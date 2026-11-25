"""C++ bba38b77c..2f0e62225 增量移植端到端一致性回归测试。

覆盖范围：
1. Battle V2 & TCP：PVP 第 5 回合投降/投票（479/480）、论剑 V2（20 回合/战意渐盛/凝锋/临时竞技宠）、
   首领试炼 V2（18s 行动窗/双卫同阵/试炼志 13 & 14 条件）与瑞兽迎春单人首领战；
2. Scene V2：瑞兽迎春真实配表（FestivalBeastActivity + FestivalBeastTier）、人物志配表校验、
   专属神兽流转与栏位隔离、赛季排位败局防挂机门槛、备战隔离快照与瑞兽结算摘要；
3. Match V2：论剑 V2 规则/6 胜锋芒称号/云章起发荣誉/有效参赛防挂机，以及 MatchInternal 瑞兽迎春
   StartActivityBattle（scene 独立签名、Kind=5 幂等、时间窗门禁与普通队列隔离）。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import struct
import time
import uuid
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables
from xuanming.data.asset_inventory import AssetInventory
from xuanming.match.config import MatchConfig
from xuanming.match.gather import (
    GatherPipeline,
    MemoryPresenceProvider,
    StaticBattleNodeResolver,
)
from xuanming.match.match_service import MatchGrpcService
from xuanming.match.model import (
    MODE_PVE_SOLO,
    MODE_PVP_TEAM,
    BattleNodeEntry,
    CallerIdentity,
    PlayerPresence,
    sign_service_call,
)
from xuanming.match.ranked import (
    MILESTONE_BIT_WIN,
    RANKED_MATCH_RATING_CONFIRMED,
    RANKED_REWARD_WIN_MILESTONE,
    RANKED_TIER_CHUSHI,
    RANKED_TIER_QINGFENG,
    RANKED_TIER_TIANSHU,
    RANKED_TIER_WUSHUANG,
    RANKED_TIER_YUNZHANG,
    RankedManager,
    RankedRuleConfig,
    evaluate_ranked_participation,
    ranked_honor_tier_code,
    tier_for_rating,
)
from xuanming.match.spectate import SpectateManager
from xuanming.match.store import MemoryMatchStore
from xuanming.nodes.battle import CLIENT_REQUESTS, BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BOSS_TRIAL_ACTION_TIMEOUT_MS,
    BOSS_TRIAL_KIND_FESTIVAL_BEAST,
    ERR_SURRENDER_COOLDOWN,
    ERR_SURRENDER_NOT_ALLOWED,
    ERR_SURRENDER_TOO_EARLY,
)
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.battle_tables import BattleTables
from xuanming.nodes.battle_tcp import BattleClients
from xuanming.nodes.scene_appearance import SceneAppearance, biography_fields_valid
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_battle import SceneBattle
from xuanming.nodes.scene_festival_beast import (
    CLAIM_APPLIED,
    DAY_MS,
    FESTIVAL_BEAST_PET_CHANGED,
    FESTIVAL_BEAST_PET_DEAD,
    FestivalBeastActivitySpec,
    LAUNCH_ADMITTED,
    PHASE_OPEN,
    REQUIRED_OPEN_DURATION_MS,
    SceneFestivalBeast,
    is_valid_reward_pet_row,
    validate_activity_row,
    validate_tiers,
)
from xuanming.nodes.scene_pets import PET_NOT_TRANSFERABLE, ScenePets
from xuanming.nodes.scene_season import SceneSeason, is_eligible_battle
from xuanming.nodes.scene_trial_journal import evaluate_seal_condition
from xuanming.protocol import Protocol

SCENE_SECRET = b"S" * 32
GUILD_SECRET = b"G" * 32


class _FakeClock:
    def __init__(self, now_ms: int = 1_760_000_000_000) -> None:
        self.now = now_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> int:
        self.now += delta_ms
        return self.now


class _FakeRpcContext:
    def __init__(self, metadata: tuple[tuple[str, str], ...] = ()) -> None:
        self._metadata = metadata

    def invocation_metadata(self):
        return self._metadata

    async def abort(self, code, details):
        raise RuntimeError(f"grpc abort {code}: {details}")


def _player_snapshot(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    health: int = 2000,
    mana: int = 500,
    speed: int = 120,
    strength: int = 20,
    skills: tuple[int, ...] = (),
    pet: dict | None = None,
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=f"P{player_id}",
        level=30,
        max_health=health,
        max_mana=mana,
        physical_attack=200,
        magic_attack=180,
        defense=100,
        team_index=team_index,
        skill_table_ids=list(skills),
    )
    snap.base_attributes.health = health
    snap.base_attributes.mana = mana
    snap.base_attributes.speed = speed
    snap.base_attributes.strength = strength
    snap.routing.session_id = (7 << 17) + player_id
    snap.routing.gate_node_id = 7
    snap.routing.gate_instance_id = "gate-1"
    snap.routing.scene_node_id = 10
    snap.routing.scene_instance_id = "scene-1"
    snap.routing.zone_id = 1
    if pet is not None:
        p = snap.pets.add(
            pet_id=pet.get("pet_id", 0),
            owner_player_id=player_id,
            pet_name=pet.get("pet_name", "灵狐"),
            pet_table_id=pet.get("pet_table_id", 1),
            level=30,
            max_health=pet.get("health", 800),
            max_mana=pet.get("mana", 400),
            physical_attack=120,
            magic_attack=100,
            defense=80,
            competitive_temporary=pet.get("competitive_temporary", False),
        )
        p.base_attributes.health = pet.get("health", 800)
        p.base_attributes.mana = pet.get("mana", 400)
        p.base_attributes.speed = 110
    return snap


def _create_req(
    protocol: Protocol,
    battle_id: int,
    *,
    mode: int = MODE_PVP_TEAM,
    config_id: int = 5,
    players: list,
    now_ms: int = 1_000,
):
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        battle_config_id=config_id,
        seed=42,
        match_mode=mode,
        created_at_ms=now_ms,
        deadline_ms=now_ms + 600_000,
        required_physical_region=1,
        placement_operation_id=str(uuid.uuid4()),
        target_instance_id="battle-inst-1",
        admit_before_ms=now_ms + 30_000,
        outcome_protocol_version=1,
        spectate_visibility=2,
        record_replay=True,
    )
    for pl in players:
        req.players.add().CopyFrom(pl)
    return req


# ── 1. Battle V2 & TCP：投降 479/480、论剑 V2、首领试炼 V2、瑞兽迎春战斗 ──────────────────────


def test_pvp_surrender_479_480_1v1_2v2_and_tcp_wiring():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(10_000)
    outcomes: list[tuple[int, int]] = []
    node = BattleNode(
        protocol,
        node_id=2,
        instance_id="battle-inst-1",
        host="127.0.0.1",
        port=19002,
        ticket_secret=b"T" * 32,
        now_ms=clock,
        steady_ms=clock,
        on_outcome=lambda bid, out: outcomes.append((bid, out)),
        tables=tables,
    )

    # BattleClients TCP 路由必须注册 479 SurrenderBattleRequest。
    assert CLIENT_REQUESTS[479] == "SurrenderBattleRequest"
    clients = BattleClients(protocol, node)
    assert isinstance(clients._limits, dict)

    # 1) 1V1 PVP：第 5 回合前拒投降；第 5 回合发起即通过，仅向本队推 480，不泄密给对手与观众。
    req_1v1 = _create_req(
        protocol,
        5001,
        mode=MODE_PVP_TEAM,
        players=[
            _player_snapshot(protocol, 101, 0),
            _player_snapshot(protocol, 201, 1),
        ],
        now_ms=clock(),
    )
    ranked_ctx = protocol.message("RankedMatchContext", season_id=1, season_key="s1", rule_version=2, team_size=1)
    res = node.create(req_1v1, ranked_context=ranked_ctx)
    assert not res.error_message.id
    room = node.rooms[5001]
    assert room.engine.max_rounds == 20
    assert room.engine.escalation_start_round == 13
    assert room.engine.surrender_unlock_round == 5

    frames: dict[int, list[tuple[int, object]]] = {101: [], 201: []}
    tickets = {}
    for pid in (101, 201):
        issued = node.issue_ticket(
            protocol.message("IssueBattleTicketRequest", battle_id=5001, player_id=pid, target_instance_id="battle-inst-1")
        )
        tk = node.authorize(issued.assignment.token_payload, issued.assignment.token_signature)
        tickets[pid] = tk
        node.attach(tk, lambda mid, msg, p=pid: frames[p].append((mid, msg)), lambda: None)

    surrender_req = protocol.message("SurrenderBattleRequest", battle_id=5001, agree=True)
    early = node.dispatch(tickets[101], 479, surrender_req)
    assert early.error_message.id == ERR_SURRENDER_TOO_EARLY

    # 推进至第 5 回合（双方各防 4 轮）。
    defend = protocol.message("SubmitBattleActionRequest", battle_id=5001)
    defend.action.action_type = 3
    for _ in range(4):
        assert not node.dispatch(tickets[101], 149, defend).error_message.id
        assert not node.dispatch(tickets[201], 149, defend).error_message.id
    assert room.engine.round_index == 5

    # 第 5 回合 101 投降 -> 秒认输（team 1 胜），480 仅推给 101，对手 201 与观众视角不含 surrender_vote。
    ok = node.dispatch(tickets[101], 479, surrender_req)
    assert not ok.error_message.id
    assert ok.vote_state.passed is True and ok.vote_state.team_index == 0
    assert any(mid == 480 and msg.vote_state.passed for mid, msg in frames[101])
    assert not any(mid == 480 for mid, _ in frames[201])
    assert outcomes == [(5001, 2)]

    # 校验终局原件中 surrendered=True 仅标记在投降方，且对手与观众快照不暴露投降态。
    commit_req = node.outcome_request(5001)
    assert commit_req is not None
    result_event = protocol.parse("contracts.kafka.BattleResultEvent", commit_req.raw_result_payload)
    by_pid = {s.player_id: s for s in result_event.settlements}
    assert by_pid[101].surrendered is True and by_pid[201].surrendered is False
    assert not room.engine.snapshot(0, viewer=201).HasField("surrender_vote")
    assert not room.engine.snapshot(0, viewer=0).HasField("surrender_vote")
    assert node.decide_outcome(5001, committed=True)

    # 2) 2V2 PVP：队友否决触发 3 回合冷却；连续挂机 >=2 回合队友自动不计入 required_agrees。
    req_2v2 = _create_req(
        protocol,
        5002,
        mode=MODE_PVP_TEAM,
        players=[
            _player_snapshot(protocol, 101, 0),
            _player_snapshot(protocol, 102, 0),
            _player_snapshot(protocol, 201, 1),
            _player_snapshot(protocol, 202, 1),
        ],
        now_ms=clock(),
    )
    engine2 = TurnEngine(protocol, req_2v2, skills={}, items={}, max_rounds=20, ranked_context=ranked_ctx)
    engine2.round_index = 5
    err, vote1 = engine2.submit_surrender_vote(101, True)
    assert err == 0 and vote1.active and vote1.required_agrees == 2 and list(vote1.agreed_player_ids) == [101]
    # 对手视图不可见本队投票
    assert not engine2.snapshot(0, viewer=201).HasField("surrender_vote")
    assert engine2.snapshot(0, viewer=102).surrender_vote.active

    err_rej, vote_rej = engine2.submit_surrender_vote(102, False)
    assert err_rej == 0 and vote_rej.rejected and not vote_rej.active and vote_rej.next_allowed_round == 8
    err_cd, _ = engine2.submit_surrender_vote(101, True)
    assert err_cd == ERR_SURRENDER_COOLDOWN

    # 冷却结束且 102 连续挂机 2 回合 -> 101 单人即可通过投降。
    engine2.round_index = 8
    engine2.consecutive_idle_rounds_by_player[102] = 2
    err_pass, vote_pass = engine2.submit_surrender_vote(101, True)
    assert err_pass == 0 and vote_pass.passed and vote_pass.required_agrees == 1 and engine2.outcome == 2


def test_lunjian_v2_escalation_primary_focus_temp_pet_and_boss_trial_v2():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)

    # 1) 临时竞技宠：仅 use_free_preset=True 允许 pet_id=0，use_free_preset=False 严禁入场。
    temp_pet_player = _player_snapshot(
        protocol, 101, 0,
        skills=(10006,),
        pet={"pet_id": 0, "pet_table_id": 1, "competitive_temporary": True},
    )
    opp = _player_snapshot(protocol, 201, 1)
    req_temp = _create_req(protocol, 6001, mode=MODE_PVP_TEAM, players=[temp_pet_player, opp])
    formal_ctx = protocol.message("RankedMatchContext", season_id=1, rule_version=2, use_free_preset=False)
    with pytest.raises(BattleError):
        TurnEngine(protocol, req_temp, skills=bt.skills, items=bt.items, buffs=bt.buffs, max_rounds=20, ranked_context=formal_ctx)

    free_ctx = protocol.message("RankedMatchContext", season_id=1, rule_version=2, use_free_preset=True)
    eng_free = TurnEngine(protocol, req_temp, skills=bt.skills, items=bt.items, buffs=bt.buffs, max_rounds=20, ranked_context=free_ctx)
    assert eng_free.max_rounds == 20 and eng_free.escalation_start_round == 13
    # 战意渐盛：第 13 回合起每层伤 +10%、疗盾 -10%（下限 20%）。
    eng_free.round_index = 12
    assert eng_free.escalation_stacks == 0 and eng_free.escalation_damage_pct == 100 and eng_free.escalation_support_pct == 100
    eng_free.round_index = 15
    assert eng_free.escalation_stacks == 3 and eng_free.escalation_damage_pct == 130 and eng_free.escalation_support_pct == 70
    # 凝锋：竞技 PVP 多目标技能在敌方人数不足上限时，显式主目标触发 is_primary_focus=True。
    eng_free.round_index = 1
    eng_free.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10006, target_id=201)
    eng_free.pending[201] = protocol.message("BattleAction", action_type=3)
    turn = eng_free.resolve()
    focus_hits = [ev for ev in turn.events if ev.event_type == 3 and ev.source_id == 101 and ev.target_id == 201 and ev.is_primary_focus]
    assert len(focus_hits) >= 1

    # 2) 首领试炼 V2：双卫同阵（boss_trial_kind=8）与 18s 手动窗 + 试炼志条件 13/14。
    dual_dungeon = next(r for r in tables["Dungeon"].rows() if int(getattr(r, "boss_trial_kind", 0)) == 8)
    req_dual = _create_req(
        protocol, 6002, mode=MODE_PVE_SOLO, config_id=dual_dungeon.id,
        players=[_player_snapshot(protocol, 101, 0), _player_snapshot(protocol, 102, 0), _player_snapshot(protocol, 103, 0)],
    )
    monsters_3p = list(bt.monsters(req_dual))
    assert any(m.monster_table_id == 109 for m in monsters_3p)
    assert BOSS_TRIAL_ACTION_TIMEOUT_MS == 18_000

    assert evaluate_seal_condition(13, 0, won=True, participated=True, total_rounds=5,
                                   guard_stance_dispelled=True, boss_guard_heal_total=0)
    assert evaluate_seal_condition(13, 0, won=True, participated=True, total_rounds=5,
                                   team_dealt_physical_direct_damage=True, team_dealt_magic_direct_damage=True,
                                   boss_guard_heal_total=500, heal_interrupt_backlash_count=1)
    assert not evaluate_seal_condition(13, 0, won=True, participated=True, total_rounds=5,
                                       guard_stance_dispelled=True, boss_guard_heal_total=500,
                                       heal_interrupt_backlash_count=0)
    assert evaluate_seal_condition(14, 0, won=True, participated=True, total_rounds=6, all_self_cleaves_parried=True)
    assert not evaluate_seal_condition(14, 0, won=True, participated=True, total_rounds=6, all_self_cleaves_parried=False)

    # 3) 瑞兽迎春单人战：冻结首领属性覆盖 + 禁道具 + PVE 不可投降 + R2 预告 / R3 慢速重击。
    fb_player = _player_snapshot(protocol, 101, 0, pet={"pet_id": 8801, "pet_table_id": 1})
    req_fb = _create_req(protocol, 6003, mode=MODE_PVE_SOLO, config_id=111, players=[fb_player])
    req_fb.activity_context.kind = 2
    req_fb.activity_context.initiator_player_id = 101
    fb = req_fb.activity_context.festival_beast
    fb.activity_instance_id = 202601
    fb.config_revision = 1
    fb.reward_identity = "festival_beast_chunxishou_v1"
    fb.reward_definition_version = 1
    fb.reward_pet_table_id = 101
    fb.required_wins = 3
    fb.locked_pet_id = 8801
    fb.locked_owner_level = 30
    fb.tier_id = 3
    fb.round_limit = 12
    fb.cycle_rounds = 3
    fb.first_heavy_round = 3
    fb.telegraph_lead_rounds = 1
    fb.heavy_damage_percent = 180
    fb.boss_name = "春禧兽"
    fb.boss_health = 26000
    fb.boss_physical_attack = 1100
    fb.boss_defense = 1300
    fb.boss_resistance = 12
    fb.boss_speed = 760
    fb.effective_start_at_ms = 1_000
    fb.effective_end_at_ms = 1_000 + REQUIRED_OPEN_DURATION_MS
    fb.client_request_id = str(uuid.uuid4())

    eng_fb = TurnEngine(
        protocol, req_fb, monsters=bt.monsters(req_fb), skills=bt.skills, items=bt.items, buffs=bt.buffs,
        max_rounds=12, boss_trial_kind=BOSS_TRIAL_KIND_FESTIVAL_BEAST,
    )
    assert eng_fb.is_festival_beast_battle and eng_fb.max_rounds == 12
    boss = next(a for a in eng_fb.actors.values() if a.actor_type == 2)
    assert boss.name == "春禧兽" and boss.max_health == 26000 and boss.physical_attack == 1100
    assert eng_fb.submit_surrender_vote(101, True)[0] == ERR_SURRENDER_NOT_ALLOWED
    # R1 -> R2 产生重击预告，R3 触发重击
    eng_fb.pending[101] = protocol.message("BattleAction", action_type=3)
    t1 = eng_fb.resolve()
    assert eng_fb.round_index == 2
    eng_fb.pending[101] = protocol.message("BattleAction", action_type=3)
    t2 = eng_fb.resolve()
    assert len(t2.state.telegraphs) >= 1 or len(t1.state.telegraphs) >= 1


# ── 2. Scene V2：瑞兽迎春表驱动、人物志校验、专属神兽流转、赛季防挂机、备战与结算 ────────────


def test_scene_v2_festival_beast_appearance_pets_season_and_battle_snapshot():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(1_770_000_000_000)

    # 1) 真实表校验：FestivalBeastActivity + FestivalBeastTier + Pet 101 + Dungeon 111
    act_rows = tables["FestivalBeastActivity"].rows()
    tier_rows = tables["FestivalBeastTier"].rows()
    assert len(act_rows) == 1 and len(tier_rows) == 8
    assert is_valid_reward_pet_row(tables["Pet"].get(101))
    assert validate_tiers(list(tier_rows))
    assert validate_activity_row(act_rows[0], tables["Dungeon"].get(111), tables["Pet"].get(101), list(tier_rows))

    # 开启状态必须严格 14 天；非 14 天拒绝。
    bad_spec = FestivalBeastActivitySpec(
        enabled=True,
        effective_start_at_ms=clock(),
        effective_end_at_ms=clock() + 7 * DAY_MS,
    )
    assert not validate_activity_row(bad_spec, tables["Dungeon"].get(111), tables["Pet"].get(101), list(tier_rows))
    good_spec = FestivalBeastActivitySpec(
        enabled=True,
        effective_start_at_ms=clock(),
        effective_end_at_ms=clock() + REQUIRED_OPEN_DURATION_MS,
    )
    assert validate_activity_row(good_spec, tables["Dungeon"].get(111), tables["Pet"].get(101), list(tier_rows))

    # 2) Appearance V2 人物志校验：全部 9 个启用外观均具备完整人物志且不含移除旧名「闻砚生」。
    assert biography_fields_valid("", "", "", "", "")
    assert biography_fields_valid("霜见雪", "", "小传", "霜阶扫雪", "正文")
    assert not biography_fields_valid("霜见雪", "", "", "霜阶扫雪", "正文")
    assert not biography_fields_valid("", "小名", "", "", "")

    mock_scene_app = SimpleNamespace(protocol=protocol)
    appearance = SceneAppearance(mock_scene_app, tables, AssetInventory(protocol, tables))
    assert appearance.validate_tables() is True
    active_apps = [r for r in tables["CharacterAppearance"].rows() if r.enabled]
    assert len(active_apps) == 9
    for row in active_apps:
        assert biography_fields_valid(row.archive_name, row.nickname, row.biography_summary, row.story_title, row.story_body)
        assert "闻砚生" not in row.biography_summary and "闻砚生" not in row.story_body

    # 3) Pet V2 专属栏位与禁交易流转策略 + SceneAssets 拒绝交易绑定神兽 101（26018）。
    pet_scene = SimpleNamespace(protocol=protocol, assets=None, battle=None, mutable=lambda _: True)
    pets = ScenePets(pet_scene, tables)
    assert pets.is_transferable_species(1) is True
    assert pets.is_transferable_species(101) is False
    assert pets.is_exclusive_gift_species(101) is True
    data = protocol.message("player_database", player_id=101, level_component={"level": 30})
    data.derived_attributes_component.health = 0  # 测试隔离战 0 血仍可入场
    data.derived_attributes_component.mana = 0
    # 放满普通栏位 + 1 只专属神兽 101：专属神兽不计入普通栏位计数。
    for idx in range(pets.max_pets):
        data.pet_component.pets.add(pet_id=1000 + idx, pet_table_id=1, level=20, health=500, mana=200)
    assert not pets.has_normal_pet_slot(data.pet_component)
    assert pets.has_exclusive_gift_slot(data.pet_component, 101)
    data.pet_component.pets.add(pet_id=9901, pet_table_id=101, name="春禧兽", level=30, health=1000, mana=500)
    assert pets.count_normal_pets(data.pet_component) == pets.max_pets
    assert not pets.has_exclusive_gift_slot(data.pet_component, 101)

    # SceneAssets._apply_debit 拒绝扣走禁交易宠物 101 -> PET_NOT_TRANSFERABLE (26018)
    assets_sys = SceneAssets(SimpleNamespace(protocol=protocol, register=lambda *a, **k: None), tables)
    debit_req = protocol.message("AssetOpRequest", correlation_id=1)
    debit_req.bundle.pet_id = 9901
    snap_out = protocol.message("AssetOpResponse").snapshot
    outcome, tip, _ = assets_sys._apply_debit(data, debit_req, snap_out)
    assert tip == PET_NOT_TRANSFERABLE

    # 4) Season V2 排位败局防挂机门槛。
    def _mk_settlement(mode: int, won: bool, rounds: int, manual: int, contrib: bool, *, ranked: bool = True, participated: bool = True):
        return protocol.message(
            "BattleSettlementData",
            battle_id=7001,
            player_id=101,
            outcome=1 if won else 2,
            player_team_index=0,
            total_rounds=rounds,
            match_mode=mode,
            competitive_ranked=ranked,
            participated=participated,
            manual_action_rounds=manual,
            effective_combat_contribution=contrib,
        )

    assert not is_eligible_battle(_mk_settlement(MODE_PVP_TEAM, False, 2, 2, True))
    assert not is_eligible_battle(_mk_settlement(MODE_PVP_TEAM, False, 3, 1, True))
    assert not is_eligible_battle(_mk_settlement(MODE_PVP_TEAM, False, 3, 2, False))
    assert is_eligible_battle(_mk_settlement(MODE_PVP_TEAM, False, 3, 2, True))
    assert is_eligible_battle(_mk_settlement(MODE_PVP_TEAM, True, 1, 1, True))

    # 5) SceneBattle 瑞兽迎春准入隔离 + 锁定宠物复核 + 战后隔离结算与 BattleEndS2C 摘要。
    async def exercise_scene_battle():
        player = SimpleNamespace(
            player_id=101,
            snapshot=data,
            gate_uuid="gate-1",
            session_id=(7 << 17) + 1,
            disconnect_at=None,
            motion=SimpleNamespace(velocity=(1.0, 0.0, 0.0)),
            seen_battles=set(),
        )
        pushes: list[tuple[str, object]] = []
        lock = asyncio.Lock()

        async def flush(_):
            return None

        async def commit(pl, saved):
            pl.snapshot = saved

        next_id = [50_000]

        async def alloc_id():
            next_id[0] += 1
            return next_id[0]

        scene_stub = SimpleNamespace(
            protocol=protocol,
            rows=SimpleNamespace(assemble=lambda s: s),
            _lock=lambda _: lock,
            locations={101: SimpleNamespace(players={101: player})},
            mutable=lambda _: True,
            flush_player_rows=flush,
            commit_player_data=commit,
            info=SimpleNamespace(node_id=10, node_uuid="scene-1", zone_id=1),
            _push=lambda pl, rpc, msg: pushes.append((rpc, msg)),
            battle=None,
            festival_beast=None,
            pets=None,
            season=None,
            trial_journal=None,
            equip_craft=None,
        )
        scene_stub.assets = SimpleNamespace(allocate_item_id=alloc_id)
        fb_sys = SceneFestivalBeast(scene_stub, tables, scene_stub.assets, clock=clock, activities=(good_spec,))
        scene_stub.festival_beast = fb_sys
        sb = SceneBattle(scene_stub, tables, now_ms=clock)
        scene_stub.battle = sb

        # 5a) dungeon=111 无 festival_beast 上下文 -> 拒绝（硬隔离）
        bad_prep = protocol.message(
            "PrepareBattleRequest",
            player_id=101, battle_id=801, battle_node_id=2,
            deadline_ms=clock() + 60_000, prepare_deadline_ms=clock() + 10_000,
            battle_config_id=111, match_mode=MODE_PVE_SOLO,
        )
        assert (await sb.prepare(bad_prep)).error_message.id != 0

        # 5b) 带 festival_beast 上下文但出战宠不一致 -> FESTIVAL_BEAST_PET_CHANGED
        data.pet_component.active_pet_id = 1000
        prep = protocol.message(
            "PrepareBattleRequest",
            player_id=101, battle_id=802, battle_node_id=2,
            deadline_ms=clock() + 60_000, prepare_deadline_ms=clock() + 10_000,
            battle_config_id=111, match_mode=MODE_PVE_SOLO,
        )
        prep.activity_context.kind = 2
        prep.activity_context.festival_beast.activity_instance_id = 202601
        prep.activity_context.festival_beast.locked_pet_id = 9999
        assert (await sb.prepare(prep)).error_message.id == FESTIVAL_BEAST_PET_CHANGED

        # 5c) 锁定正确存活宠 1000，即使角色场外 0 血也以隔离满血蓝快照入场，且无道具扣留。
        prep.activity_context.festival_beast.locked_pet_id = 1000
        ok_prep = await sb.prepare(prep)
        assert not ok_prep.error_message.id
        assert ok_prep.snapshot.base_attributes.health == ok_prep.snapshot.max_health > 0
        assert len(ok_prep.snapshot.items) == 0 and len(ok_prep.snapshot.pets) == 1

        # 5d) 瑞兽结算隔离：不回写场外血蓝，累计胜场并输出 BattleEndS2C.festival_beast_settlement。
        settlement_sys = BattleSettlement(scene_stub, tables, AssetInventory(protocol, tables), missions=None, clock=clock)
        st = protocol.message(
            "BattleSettlementData",
            battle_id=802, player_id=101, outcome=1, player_team_index=0,
            health=10, mana=10, total_rounds=4, match_mode=MODE_PVE_SOLO,
            battle_config_id=111, boss_trial_kind=BOSS_TRIAL_KIND_FESTIVAL_BEAST,
        )
        st.festival_beast.activity_instance_id = 202601
        st.festival_beast.reward_identity = good_spec.reward_identity
        st.festival_beast.reward_definition_version = 1
        st.festival_beast.reward_pet_table_id = 101
        st.festival_beast.required_wins = 3
        assert await settlement_sys.apply(player, data, st)
        assert data.derived_attributes_component.health == 0  # 场外血量严格未改写
        assert data.festival_beast_component.period_progress[0].wins == 1
        await settlement_sys.complete(player, st)
        end_pushes = [msg for rpc, msg in pushes if rpc == "BattleClientPlayerNotifyBattleEnd"]
        assert len(end_pushes) == 1
        assert end_pushes[0].festival_beast_settlement.activity_instance_id == 202601
        assert end_pushes[0].festival_beast_settlement.wins_before == 0
        assert end_pushes[0].festival_beast_settlement.wins_after == 1
        assert end_pushes[0].festival_beast_settlement.required_wins == 3

    asyncio.run(exercise_scene_battle())


# ── 3. Match V2：论剑 V2 规则/6 胜称号/荣誉与瑞兽迎春 StartActivityBattle ──────────────────────


def test_match_v2_ranked_rules_and_festival_beast_start_activity_battle():
    protocol = Protocol()
    clock = _FakeClock(1_770_000_000_000)

    # 1) Ranked V2 纯规则：段位线 152k/160k/172k/185k，云章起发荣誉，有效参赛防挂机。
    rule = RankedRuleConfig.from_config_tables(ConfigTables())
    assert rule.rule_version == 2 and rule.milestone_win_matches == 6
    assert tier_for_rating(151_999, rule=rule) == RANKED_TIER_CHUSHI
    assert tier_for_rating(152_000, rule=rule) == RANKED_TIER_QINGFENG
    assert tier_for_rating(160_000, rule=rule) == RANKED_TIER_YUNZHANG
    assert tier_for_rating(172_000, rule=rule) == RANKED_TIER_TIANSHU
    assert tier_for_rating(185_000, rule=rule) == RANKED_TIER_WUSHUANG
    assert ranked_honor_tier_code(RANKED_TIER_QINGFENG) == ("", False)
    assert ranked_honor_tier_code(RANKED_TIER_YUNZHANG) == ("yunzhang", True)
    assert ranked_honor_tier_code(RANKED_TIER_TIANSHU) == ("tianshu", True)
    assert ranked_honor_tier_code(RANKED_TIER_WUSHUANG) == ("wushuang", True)

    fact_manual = protocol.message("BattleParticipationFact", player_id=1001, entered_room=True, manual_action_accepted=True)
    fact_no_opp = protocol.message("BattleParticipationFact", player_id=1001, entered_room=True, no_action_opportunity=True)
    st_bad_loss = protocol.message("BattleSettlementData", manual_action_rounds=1, effective_combat_contribution=True)
    st_good_loss = protocol.message("BattleSettlementData", manual_action_rounds=2, effective_combat_contribution=True)
    assert not evaluate_ranked_participation(fact_manual, st_bad_loss, 3, player_won=False)
    assert evaluate_ranked_participation(fact_manual, st_good_loss, 3, player_won=False)
    assert evaluate_ranked_participation(fact_no_opp, st_bad_loss, 1, player_won=False)
    assert evaluate_ranked_participation(fact_manual, st_bad_loss, 1, player_won=True)

    async def exercise_match():
        store = MemoryMatchStore(protocol, clock_ms=clock)
        ranked = RankedManager(protocol, store, clock_ms=clock)
        caller = CallerIdentity(
            player_id=1001, account="acc:1001", owner_revision=1,
            session_id=1, gate_node_id=1, gate_instance_id="g1", session_incarnation=1,
        )

        # 6 胜解锁 RANKED_REWARD_WIN_MILESTONE(7) 并允许作为称号佩戴。
        for i in range(1, 7):
            clock.advance(1000)
            summary = await ranked.record_battle_settlement(
                battle_id=9000 + i, player_id=1001, season_id=1, team_size=1,
                status=RANKED_MATCH_RATING_CONFIRMED, outcome=1, player_team_index=0,
                total_rounds=5, participated=True, fled=False,
                rating_before_centi=150_000 + (i - 1) * 2000, rating_after_centi=150_000 + i * 2000,
            )
            if i == 6:
                assert any(r.kind == RANKED_REWARD_WIN_MILESTONE and r.item_code == "s1_win_milestone_6" for r in summary.newly_unlocked_rewards)
        prog = await store.get_ranked_player_season(1, 1001)
        assert prog is not None and (prog.milestone_bits & MILESTONE_BIT_WIN) != 0

        cos_req = protocol.message("match.UpdateRankedCosmeticRequest")
        cos_req.loadout.equipped_title_code = "s1_win_milestone_6"
        cos_resp = await ranked.update_ranked_cosmetic(caller, cos_req)
        assert cos_resp.error_message.id == 0 and cos_resp.loadout.equipped_title_code == "s1_win_milestone_6"

        # GetRankedProfile 回显 V2 字段（tier_*_centi / milestone_win_matches / uses_own_progression）。
        prof = await ranked.get_ranked_profile(caller, protocol.message("match.GetRankedProfileRequest"))
        assert prof.current_season.rule_version == 2
        assert prof.current_season.tier_qingfeng_centi == 152_000
        assert prof.current_season.tier_yunzhang_centi == 160_000
        assert prof.current_season.milestone_win_matches == 6
        assert prof.current_season.uses_own_progression is True

        # 2) MatchGrpcService.StartActivityBattle (瑞兽迎春 Kind=2)
        presence = MemoryPresenceProvider()
        presence.set_presence(PlayerPresence(player_id=1001, online=True, scene_grpc_endpoint="127.0.0.1:50051", zone_id=1))
        nodes = StaticBattleNodeResolver([
            BattleNodeEntry(node_id=2, node_uuid="battle-1", grpc_endpoint="127.0.0.1:50061", physical_region=1, client_host="127.0.0.1", client_port=19002),
        ])
        cfg = MatchConfig(scene_secret=SCENE_SECRET, guild_secret=GUILD_SECRET)
        gather = GatherPipeline(protocol, store, cfg, rpc=None, presence=presence, battle_nodes=nodes)
        spectate = SpectateManager(protocol, store, cfg, rpc=None, presence=presence)
        svc = MatchGrpcService(
            protocol, store, cfg, gate_verifier=None, presence=presence, battle_nodes=nodes,
            gather=gather, spectate=spectate, ranked=ranked, inline_gather=False, clock_ms=clock,
        )

        act_path = "/match.MatchInternal/StartActivityBattle"
        start_ms = clock() - 10_000
        end_ms = start_ms + REQUIRED_OPEN_DURATION_MS
        req_uuid = uuid.uuid4()
        req_hi, req_lo = struct.unpack(">QQ", req_uuid.bytes)

        def _make_fb_req(*, start_at: int = start_ms, end_at: int = end_ms, locked_pet: int = 8801, hi: int = req_hi, lo: int = req_lo):
            r = protocol.message(
                "match.StartActivityBattleRequest",
                battle_config_id=111,
                member_player_ids=[1001],
                request_high=hi,
                request_low=lo,
            )
            r.activity_context.kind = 2
            r.activity_context.initiator_player_id = 1001
            f = r.activity_context.festival_beast
            f.activity_instance_id = 202601
            f.config_revision = 1
            f.reward_identity = "festival_beast_chunxishou_v1"
            f.reward_definition_version = 1
            f.reward_pet_table_id = 101
            f.required_wins = 3
            f.locked_pet_id = locked_pet
            f.locked_owner_level = 30
            f.tier_id = 3
            f.round_limit = 12
            f.cycle_rounds = 3
            f.first_heavy_round = 3
            f.telegraph_lead_rounds = 1
            f.heavy_damage_percent = 180
            f.boss_name = "春禧兽"
            f.boss_health = 26000
            f.boss_physical_attack = 1100
            f.boss_defense = 1300
            f.boss_resistance = 12
            f.boss_speed = 760
            f.effective_start_at_ms = start_at
            f.effective_end_at_ms = end_at
            f.client_request_id = str(req_uuid)
            return r

        # 2a) 用 guild 密钥签名瑞兽迎春 -> 严格分域拒绝 (reject=5)
        bad_sig_req = _make_fb_req()
        sign_service_call(act_path, bad_sig_req, "guild", clock(), GUILD_SECRET)
        bad_sig_resp = await svc.start_activity_battle(bad_sig_req, _FakeRpcContext(), act_path)
        assert bad_sig_resp.reject == 5

        # 2b) 不在开放时间窗内 -> OUTSIDE_WINDOW (reject=6)
        out_req = _make_fb_req(start_at=clock() + 60_000, end_at=clock() + 60_000 + REQUIRED_OPEN_DURATION_MS)
        sign_service_call(act_path, out_req, "scene", clock(), SCENE_SECRET)
        out_resp = await svc.start_activity_battle(out_req, _FakeRpcContext(), act_path)
        assert out_resp.reject == 6

        # 2c) 合法 scene 签名 + 开放窗口内 -> 准入成功并回填 admitted_at_ms；同 UUID 重放幂等；改正文冲突 (reject=1)。
        ok_req = _make_fb_req()
        sign_service_call(act_path, ok_req, "scene", clock(), SCENE_SECRET)
        ok_resp = await svc.start_activity_battle(ok_req, _FakeRpcContext(), act_path)
        assert ok_resp.reject == 0 and ok_resp.battle_id > 0 and ok_resp.admitted_at_ms == clock()

        # 幂等重放（重新签名时间戳不同但正文相同）
        clock.advance(500)
        rep_req = _make_fb_req()
        sign_service_call(act_path, rep_req, "scene", clock(), SCENE_SECRET)
        rep_resp = await svc.start_activity_battle(rep_req, _FakeRpcContext(), act_path)
        assert rep_resp.reject == 0 and rep_resp.battle_id == ok_resp.battle_id and rep_resp.admitted_at_ms == ok_resp.admitted_at_ms

        # 同 UUID 改正文（更换 locked_pet_id）-> INVALID_ARGUMENT (reject=1)
        conflict_req = _make_fb_req(locked_pet=9999)
        sign_service_call(act_path, conflict_req, "scene", clock(), SCENE_SECRET)
        conflict_resp = await svc.start_activity_battle(conflict_req, _FakeRpcContext(), act_path)
        assert conflict_resp.reject == 1

        await svc.close()

    asyncio.run(exercise_match())
