"""赛季玩法 V3（S1 四组技印共鸣 + 战后本场赛季印功劳摘要）与 C++/Java 对齐回归测试。"""

from __future__ import annotations

from xuanming.data import ConfigTables
from xuanming.nodes.battle import BattleNode, TurnEngine, _Room
from xuanming.nodes.battle_dropin_effects import SeasonBattleRules
from xuanming.nodes.battle_tables import BattleTables
from xuanming.protocol import Protocol

_ACTION_ATTACK = 1
_ACTION_SKILL = 2
_ACTION_DEFEND = 3
_EVENT_DAMAGE = 3
_EVENT_SHIELD = 20
_EVENT_ARTIFACT = 21


def _player(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    health: int = 20_000,
    max_health: int = 20_000,
    mana: int = 10_000,
    max_mana: int = 10_000,
    speed: int = 100,
    physical_attack: int = 2000,
    magic_attack: int = 2000,
    active_season_id: int = 1,
    skills: dict[int, int] | None = None,
    seal: dict | None = None,
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=f"P{player_id}",
        team_index=team_index,
        level=30,
        max_health=max_health,
        max_mana=max_mana,
        physical_attack=physical_attack,
        magic_attack=magic_attack,
        defense=0,
        active_season_id=active_season_id,
        base_attributes=protocol.message(
            "BaseAttributesComp",
            health=health,
            mana=mana,
            strength=0,
            armor=0,
            speed=speed,
        ),
    )
    for sid, rank in (skills or {}).items():
        snap.skill_ranks[sid] = rank
        snap.skill_table_ids.append(sid)
    if seal is not None:
        snap.equipment_effects.season_seal.CopyFrom(protocol.message("SeasonSealEffect", **seal))
    return snap


def _make_engine(protocol: Protocol, tables: BattleTables, p1, p2, *, seed: int = 101) -> TurnEngine:
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=96001,
        match_mode=3,
        seed=seed,
        players=[p1, p2],
    )
    return TurnEngine(
        protocol,
        req,
        skills=tables.skills,
        items=tables.items,
        buffs=tables.buffs,
        silence_permissions=tables.silence_permissions,
        max_rounds=20,
    )


def _cmd(protocol: Protocol, action_type: int, target_id: int = 0, skill_id: int = 0):
    return protocol.message(
        "BattleAction",
        action_type=action_type,
        target_id=target_id,
        skill_table_id=skill_id,
    )


def _submit(engine: TurnEngine, actor_id: int, action) -> None:
    assert engine.validate(engine.actors[actor_id], action) == 0
    engine.pending[actor_id] = action


def test_season_v3_all_four_resonances_and_settlement_summary():
    protocol = Protocol()
    tables = BattleTables(protocol, ConfigTables())

    # 1. 101 破阵锐印 × 15001 破阵裂锋：厚盾未碎不返蓝；精确碎盾返还 30% 耗蓝（600 -> +180）
    seal_101 = dict(season_id=1, seal_id=101, effect_id=1, bonus_bps=1000, per_round_limit=1)
    p1 = _player(protocol, 101, 0, speed=200, skills={15001: 1}, seal=seal_101)
    p2 = _player(protocol, 201, 1, speed=50)
    eng_thick = _make_engine(protocol, tables, p1, p2, seed=101)
    eng_thick.equipment_effects.grant_shield(eng_thick.actors[201], 50_000.0, buff_table_id=150030, remain_rounds=3)
    _submit(eng_thick, 101, _cmd(protocol, _ACTION_SKILL, 201, 15001))
    _submit(eng_thick, 201, _cmd(protocol, _ACTION_DEFEND))
    r_thick = eng_thick.resolve()
    direct_absorbed = sum(
        ev.absorbed
        for ev in r_thick.events
        if ev.event_type == _EVENT_SHIELD and ev.season_seal_id == 0 and ev.artifact_item_id == 0
    )
    seal_stripped = next(
        (ev.value for ev in r_thick.events if ev.event_type == _EVENT_ARTIFACT and ev.season_seal_id == 101 and ev.artifact_effect_id == 1),
        0,
    )
    assert direct_absorbed > 0
    assert seal_stripped > 0
    assert not any(
        ev.event_type == _EVENT_ARTIFACT and ev.artifact_effect_id == SeasonBattleRules.RESONANCE_EVENT_SHARP_MANA_REFUND
        for ev in r_thick.events
    )
    assert eng_thick.actors[101].attributes.mana == 9_400
    rt_thick = eng_thick.equipment_effects.find_season_seal_runtime(101)
    assert rt_thick is not None and rt_thick.trigger_count == 1 and rt_thick.resonance_count == 0
    assert rt_thick.contribution_value == seal_stripped

    # 精确由锐印削盾击碎最后一点护盾
    exact_cap = direct_absorbed + min(seal_stripped, 10)
    eng_shatter = _make_engine(protocol, tables, p1, p2, seed=101)
    eng_shatter.equipment_effects.grant_shield(eng_shatter.actors[201], float(exact_cap), buff_table_id=150030, remain_rounds=3)
    _submit(eng_shatter, 101, _cmd(protocol, _ACTION_SKILL, 201, 15001))
    _submit(eng_shatter, 201, _cmd(protocol, _ACTION_DEFEND))
    r_shatter = eng_shatter.resolve()
    refund_events = [
        ev.value
        for ev in r_shatter.events
        if ev.event_type == _EVENT_ARTIFACT
        and ev.season_seal_id == 101
        and ev.artifact_effect_id == SeasonBattleRules.RESONANCE_EVENT_SHARP_MANA_REFUND
    ]
    assert refund_events == [180]
    assert eng_shatter.actors[101].attributes.mana == 9_580
    rt_shatter = eng_shatter.equipment_effects.find_season_seal_runtime(101)
    assert rt_shatter is not None and rt_shatter.trigger_count == 1 and rt_shatter.resonance_count == 1
    assert rt_shatter.contribution_value == min(seal_stripped, 10)

    # 2. 102 破阵玄印 × 15002 破阵灵雷：破盾后敌方减防回合 2 -> 3 + 共鸣事件 102
    seal_102 = dict(season_id=1, seal_id=102, effect_id=2, bonus_bps=1000, per_round_limit=1)
    p1_m = _player(protocol, 101, 0, speed=200, skills={15002: 1}, seal=seal_102)
    eng_m = _make_engine(protocol, tables, p1_m, p2, seed=102)
    eng_m.equipment_effects.grant_shield(eng_m.actors[201], 50_000.0, buff_table_id=150030, remain_rounds=3)
    _submit(eng_m, 101, _cmd(protocol, _ACTION_SKILL, 201, 15002))
    _submit(eng_m, 201, _cmd(protocol, _ACTION_DEFEND))
    r_m = eng_m.resolve()
    debuff_remain = next((b.remain_rounds for b in eng_m.actors[201].buffs if b.buff_table_id == 150021), 0)
    assert debuff_remain == 3
    assert any(
        ev.event_type == _EVENT_ARTIFACT
        and ev.season_seal_id == 102
        and ev.artifact_effect_id == SeasonBattleRules.RESONANCE_EVENT_MYSTIC_EXTEND_DEBUFF
        and ev.value == 1
        for ev in r_m.events
    )
    rt_m = eng_m.equipment_effects.find_season_seal_runtime(101)
    assert rt_m is not None and rt_m.contribution_value > 0 and rt_m.resonance_count == 1

    # 3. 103 固阵守印 × 15003 固阵灵障：R1 减伤 -> R2 施放灵障护盾量 +10%（对比无共鸣基线）
    seal_103 = dict(season_id=1, seal_id=103, effect_id=6, bonus_bps=1000, cap_bps=300, active_rounds=2, battle_limit=1)
    p1_g = _player(protocol, 101, 0, speed=200, skills={15003: 1}, seal=seal_103)
    p2_g = _player(protocol, 201, 1, speed=50, physical_attack=2000)

    # 无共鸣基线（R1 未被打，直接施放 15003）
    eng_g_base = _make_engine(protocol, tables, p1_g, p2_g, seed=103)
    _submit(eng_g_base, 101, _cmd(protocol, _ACTION_SKILL, 101, 15003))
    _submit(eng_g_base, 201, _cmd(protocol, _ACTION_DEFEND))
    eng_g_base.resolve()
    base_shield = sum(b.remaining_capacity for b in eng_g_base.actors[101].buffs if b.buff_table_id == 150030)
    assert base_shield > 0

    # 触发守印后次轮施放 15003 -> 护盾量 +10%
    eng_g = _make_engine(protocol, tables, p1_g, p2_g, seed=103)
    _submit(eng_g, 101, _cmd(protocol, _ACTION_DEFEND))
    _submit(eng_g, 201, _cmd(protocol, _ACTION_ATTACK, 101))
    r1_g = eng_g.resolve()
    mitigated = next(
        (ev.value for ev in r1_g.events if ev.event_type == _EVENT_ARTIFACT and ev.season_seal_id == 103 and ev.artifact_effect_id == 6),
        0,
    )
    assert mitigated > 0
    _submit(eng_g, 101, _cmd(protocol, _ACTION_SKILL, 101, 15003))
    _submit(eng_g, 201, _cmd(protocol, _ACTION_DEFEND))
    r2_g = eng_g.resolve()
    boosted_shield = sum(b.remaining_capacity for b in eng_g.actors[101].buffs if b.buff_table_id == 150030)
    boost_events = [
        ev.value
        for ev in r2_g.events
        if ev.event_type == _EVENT_ARTIFACT
        and ev.season_seal_id == 103
        and ev.artifact_effect_id == SeasonBattleRules.RESONANCE_EVENT_GUARD_SHIELD_BOOST
    ]
    assert len(boost_events) == 1 and boost_events[0] == boosted_shield - base_shield
    assert boosted_shield > base_shield
    rt_g = eng_g.equipment_effects.find_season_seal_runtime(101)
    assert rt_g is not None and rt_g.contribution_value == mitigated + boost_events[0]
    assert rt_g.trigger_count == 1 and rt_g.resonance_count == 1

    # 4. 104 聚息援印 × 15004 破阵蓄势：R1 防御回蓝 400 -> R2 破阵蓄势半价耗蓝(500->250)，R3 恢复全价 500 + 结算透传 52..54
    seal_104 = dict(season_id=1, seal_id=104, effect_id=7, bonus_bps=400, per_round_limit=1, battle_limit=3)
    p1_b = _player(protocol, 101, 0, mana=5_000, speed=200, skills={15004: 1}, seal=seal_104)
    p2_b = _player(protocol, 201, 1, speed=50)
    node = BattleNode(
        protocol,
        tables=ConfigTables(),
        node_id=1,
        instance_id="battle-test",
        host="127.0.0.1",
        port=0,
        ticket_secret="test-secret",
    )
    req_b = protocol.message("CreateBattleRequest", battle_id=96004, match_mode=3, seed=104, players=[p1_b, p2_b])
    eng_b = TurnEngine(
        protocol,
        req_b,
        skills=tables.skills,
        items=tables.items,
        buffs=tables.buffs,
        silence_permissions=tables.silence_permissions,
        max_rounds=20,
    )
    room = _Room(eng_b, req_b, 0, 0, {101: p1_b.routing, 201: p2_b.routing})

    _submit(eng_b, 101, _cmd(protocol, _ACTION_DEFEND))
    _submit(eng_b, 201, _cmd(protocol, _ACTION_DEFEND))
    eng_b.resolve()
    assert eng_b.actors[101].attributes.mana == 5_400

    _submit(eng_b, 101, _cmd(protocol, _ACTION_SKILL, 101, 15004))
    _submit(eng_b, 201, _cmd(protocol, _ACTION_DEFEND))
    r2_b = eng_b.resolve()
    assert eng_b.actors[101].attributes.mana == 5_150
    assert [
        ev.value
        for ev in r2_b.events
        if ev.event_type == _EVENT_ARTIFACT
        and ev.season_seal_id == 104
        and ev.artifact_effect_id == SeasonBattleRules.RESONANCE_EVENT_BREATH_HALF_MANA
    ] == [250]

    # 等真实技能冷却自然归零（期间普攻不触发聚息援印），再施放 15004 恢复全价 500
    while eng_b.actors[101].skill_cooldown_rounds.get(15004, 0) > 0:
        _submit(eng_b, 101, _cmd(protocol, _ACTION_ATTACK, 201))
        _submit(eng_b, 201, _cmd(protocol, _ACTION_DEFEND))
        eng_b.resolve()
    _submit(eng_b, 101, _cmd(protocol, _ACTION_SKILL, 101, 15004))
    _submit(eng_b, 201, _cmd(protocol, _ACTION_DEFEND))
    eng_b.resolve()
    assert eng_b.actors[101].attributes.mana == 4_650

    room.proposed_outcome = 1
    result_event = node._result(room)
    s101 = next(s for s in result_event.settlements if s.player_id == 101)
    assert s101.final_season_seal_id == 104
    assert s101.season_seal_contribution_value == 650
    assert s101.season_seal_trigger_count == 1
    assert s101.season_seal_resonance_count == 1
