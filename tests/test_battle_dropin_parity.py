"""与 C++ 战斗子系统对齐的五项规则单元测试（装备法宝 / 宠物修炼 / 赛季印 / 副本怪物 AI / 试炼志事实）。"""

from __future__ import annotations

import pytest

from xuanming.nodes.battle import BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BossPhaseId,
    DungeonMonsterAiProfile,
    LinearUtilityModel,
    ModifierSelection,
    PetAndSupportEffects,
    SeasonBattleRules,
    SealConditionType,
    TrialContext,
    ailment_chance,
    build_trained_default_monster_ai_profile,
    choose_monster_action,
    discount_mana,
    evaluate_seal_condition,
    floor_bps,
    is_valid_trial_settlement,
    received_ratio,
)
from xuanming.nodes.battle_tables import BattleBuff, BattleSkill
from xuanming.protocol import Protocol


def _player_snapshot(
    protocol: Protocol,
    player_id: int,
    team_index: int = 0,
    *,
    name: str | None = None,
    level: int = 20,
    health: int = 1000,
    mana: int = 200,
    speed: int = 100,
    strength: int = 10,
    physical_attack: int = 100,
    magic_attack: int = 120,
    defense: int = 100,
    class_id: int = 1,
    skills: tuple[int, ...] = (),
    skill_ranks: dict[int, int] | None = None,
    equipment_effects=None,
    pets: tuple = (),
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=name or f"P{player_id}",
        level=level,
        base_attributes=protocol.message(
            "BaseAttributesComp",
            health=health,
            mana=mana,
            strength=strength,
            armor=20,
            speed=speed,
        ),
        max_health=health,
        max_mana=mana,
        physical_attack=physical_attack,
        magic_attack=magic_attack,
        defense=defense,
        team_index=team_index,
        class_id=class_id,
        skill_table_ids=list(skills),
        routing=protocol.message("BattleRouting", gate_instance_id="g1", scene_instance_id="s1"),
    )
    if skill_ranks:
        for sid, rank in skill_ranks.items():
            snap.skill_ranks[sid] = rank
    if equipment_effects is not None:
        snap.equipment_effects.CopyFrom(equipment_effects)
    for pet in pets:
        snap.pets.add().CopyFrom(pet)
    return snap


def _monster_state(
    protocol: Protocol,
    monster_table_id: int,
    *,
    health: int = 500,
    mana: int = 100,
    speed: int = 60,
    strength: int = 5,
    armor: int = 10,
    defense: int = 20,
    is_boss: bool = False,
    name: str = "Monster",
    skills: tuple[int, ...] = (),
):
    return protocol.message(
        "BattleActorState",
        name=name,
        level=20,
        monster_table_id=monster_table_id,
        is_boss=is_boss,
        max_health=health,
        max_mana=mana,
        defense=defense,
        skill_table_ids=list(skills),
        attributes=protocol.message(
            "BaseAttributesComp",
            health=health,
            mana=mana,
            strength=strength,
            armor=armor,
            speed=speed,
        ),
    )


def _create_request(protocol: Protocol, players, *, match_mode: int = 4, seed: int = 42, battle_id: int = 7001):
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        battle_config_id=101,
        seed=seed,
        match_mode=match_mode,
        created_at_ms=10_000,
        deadline_ms=1_000_000,
        admit_before_ms=50_000,
        required_physical_region=1,
        placement_operation_id="op-parity-1",
        target_instance_id="battle-1",
        outcome_protocol_version=1,
    )
    for snap in players:
        req.players.add().CopyFrom(snap)
    return req


# ---------------------------------------------------------------------------
# 1. 装备与法宝战斗效果测试
# ---------------------------------------------------------------------------


def test_equipment_passive_stats_resist_crit_and_validation_caps():
    p = Protocol()
    equip = p.message(
        "EquipmentEffects",
        physical_attack_bps=2000,  # +20%
        magic_attack_bps=1500,     # +15%
        max_health_bps=500,        # +5%
        speed_bps=200,             # +2%
        physical_crit_bps=800,     # +8%
        physical_resist_bps=600,   # +6% 物理抗性
    )
    req = _create_request(p, [_player_snapshot(p, 1, 0, equipment_effects=equip), _player_snapshot(p, 2, 1)], match_mode=1)
    engine = TurnEngine(p, req, skills={}, items={}, max_rounds=10)
    a1 = engine.actors[1]
    assert a1.physical_attack == 120
    assert a1.magic_attack == 138
    assert a1.max_health == 1050
    assert a1.attributes.health == 1050
    assert a1.attributes.speed == 102
    assert a1.entry_max_health == 1050
    assert engine._critical_chance(a1, BattleSkill(10, physical=True)) == pytest.approx(0.08)

    # 验证装备抗性降低承伤且遵守 0.4 保底比例
    plain_target = engine.actors[2]
    dmg_to_plain = engine._damage_before_critical(a1, plain_target, BattleSkill(10, physical=True), 10.0)
    dmg_to_armored = engine._damage_before_critical(plain_target, a1, BattleSkill(10, physical=True), 10.0)
    assert dmg_to_armored < dmg_to_plain
    assert received_ratio(10_000, 10_000, 10_000.0, 20) == pytest.approx(0.4)

    # 非法越界快照必须被拒绝
    bad_equip = p.message("EquipmentEffects", physical_attack_bps=3201)
    bad_req = _create_request(p, [_player_snapshot(p, 1, 0, equipment_effects=bad_equip), _player_snapshot(p, 2, 1)], match_mode=1)
    with pytest.raises(BattleError):
        TurnEngine(p, bad_req, skills={}, items={}, max_rounds=10)


def test_equipment_artifacts_defend_mana_damage_reduction_shield_break_and_lifesteal():
    p = Protocol()
    # 玩家 1：法宝 1（物理削盾 50%）、法宝 7（防御回蓝 20%）
    eq1 = p.message("EquipmentEffects")
    eq1.artifacts.add(item_id=1002, effect_id=7, bonus_bps=2000, per_round_limit=1, battle_limit=2)
    eq1.artifacts.add(item_id=1001, effect_id=1, bonus_bps=5000, per_round_limit=1, battle_limit=2)

    # 玩家 2：法宝 6（开局前 2 回合减伤 40%，单次上限 15% 入战血量）
    eq2 = p.message("EquipmentEffects")
    eq2.artifacts.add(item_id=2001, effect_id=6, bonus_bps=4000, cap_bps=1500, battle_limit=2, active_rounds=2)

    p1 = _player_snapshot(p, 1, 0, mana=100, physical_attack=300, speed=120, equipment_effects=eq1)
    p1.max_mana = 200
    p2 = _player_snapshot(p, 2, 1, health=1000, speed=90, equipment_effects=eq2)
    engine = TurnEngine(p, _create_request(p, [p1, p2], match_mode=4), skills={}, items={}, max_rounds=10)

    # 法宝按 item_id 升序冻结
    assert [a.item_id for a in engine.actors[1].equipment_effects.artifacts] == [1001, 1002]

    # 给玩家 1 挂载吸血，给玩家 2 挂载 500 点护盾（确保吸收直伤后仍有残余护盾供破盾法宝 1 削减）
    engine.actors[1].attributes.health = 700
    engine.equipment_effects.set_actor_bonus(1, lifesteal_bps=2500)
    shield_cap = engine.equipment_effects.grant_shield(engine.actors[2], 500.0)
    assert shield_cap == 500

    # 回合 1：玩家 1 普攻玩家 2 -> 触发法宝 6 减伤 + 护盾吸收 + 法宝 1 物理削盾
    engine.pending[1] = p.message("BattleAction", action_type=1, target_id=2)
    engine.pending[2] = p.message("BattleAction", action_type=3)
    turn1 = engine.resolve()

    artifact_events = [e for e in turn1.events if e.event_type == 21]
    shield_events = [e for e in turn1.events if e.event_type == 20]
    assert any(e.source_id == 2 and e.artifact_effect_id == 6 and e.value > 0 for e in artifact_events)
    assert len(shield_events) >= 2
    assert any(e.source_id == 1 and e.artifact_effect_id == 1 and e.value > 0 for e in artifact_events)
    assert 1 in engine.trial_tracker.shield_break_players

    # 清除残余护盾后回合 2：玩家 1 普攻命中气血触发吸血回复
    engine.actors[2].ClearField("buffs")
    engine.pending[1] = p.message("BattleAction", action_type=1, target_id=2)
    engine.pending[2] = p.message("BattleAction", action_type=3)
    turn2_ls = engine.resolve()
    assert any(e.event_type == 4 and e.source_id == 1 and e.target_id == 1 and e.value > 0 for e in turn2_ls.events)
    assert engine.actors[1].attributes.health > 700

    # 回合 3：玩家 1 防御 -> 触发法宝 7 回蓝 200 * 20% = 40
    mana_before = engine.actors[1].attributes.mana
    engine.pending[1] = p.message("BattleAction", action_type=3)
    engine.pending[2] = p.message("BattleAction", action_type=3)
    turn3 = engine.resolve()
    assert engine.actors[1].attributes.mana == mana_before + 40
    assert any(e.event_type == 21 and e.artifact_effect_id == 7 and e.value == 40 for e in turn3.events)
    assert any(e.event_type == 14 and e.mana_delta == 40 for e in turn3.events)


def test_equipment_mana_discount_emergency_heal_and_ailment_artifacts():
    p = Protocol()
    heal_skill = BattleSkill(
        0,
        class_id=1,
        unlock_level=1,
        max_rank=10,
        target_team=2,
        max_targets=1,
        mana_percent=0.20,  # 200 * 20% = 40 蓝耗
        heal_multiplier=1.0,
    )
    eq = p.message("EquipmentEffects", outgoing_heal_bps=2000)
    eq.mana_reduction_bps[3] = 1200  # 治疗类技能减耗 12% -> 40 - 4 = 36
    eq.artifacts.add(
        item_id=3001,
        effect_id=4,
        bonus_bps=3000,
        per_round_limit=1,
        battle_limit=2,
        health_threshold_bps=5000,
    )
    eq.artifacts.add(item_id=3002, effect_id=8, bonus_bps=2500, per_round_limit=1, battle_limit=2)

    # 玩家 1 仅剩 36 蓝（低于原耗 40，刚好够折后 36）且当前残血 40%
    p1 = _player_snapshot(
        p, 1, 0, health=1000, mana=36, magic_attack=100, skills=(10,), skill_ranks={10: 1}, equipment_effects=eq
    )
    p1.max_mana = 200
    p1.base_attributes.health = 400
    p2 = _player_snapshot(p, 2, 1, health=1000)
    engine = TurnEngine(p, _create_request(p, [p1, p2], match_mode=1), skills={10: heal_skill}, items={}, max_rounds=10)

    cast = p.message("BattleAction", action_type=2, skill_table_id=10, target_id=1)
    assert discount_mana(40, 1200.0) == 36
    assert engine.validate(engine.actors[1], cast) == 0

    engine.pending[1] = cast
    engine.pending[2] = p.message("BattleAction", action_type=3)
    turn = engine.resolve()

    # 精确消耗 36 蓝，治疗享受 +20% 装备常驻 + 30% 低血法宝 4 急救增幅（100 -> 150）
    assert engine.actors[1].attributes.mana == 0
    heal_ev = next(e for e in turn.events if e.event_type == 4 and e.target_id == 1)
    assert heal_ev.value == 150
    assert any(e.event_type == 21 and e.artifact_effect_id == 4 for e in turn.events)

    # 法宝 8（清心防控制）在异常判定时触发并计入试炼事实
    dummy_turn = p.message("TurnResultS2C")
    engine.equipment_effects.roll_ailment_with_equipment(
        engine.actors[2], engine.actors[1], 0.5, 5, 900, engine.round_index, dummy_turn, 1
    )
    assert any(e.event_type == 21 and e.artifact_effect_id == 8 for e in dummy_turn.events)
    assert 1 in engine.trial_tracker.anti_control_or_season_skill_players
    assert ailment_chance(0.5, 1000.0, 2000.0, 0.0, 0.0) == pytest.approx(0.4)


# ---------------------------------------------------------------------------
# 2. 宠物修炼与辅助效果投影测试
# ---------------------------------------------------------------------------


def test_pet_cultivation_damage_poison_shield_and_support_summary():
    p = Protocol()
    poison_buff = BattleBuff(buff_type=50, max_layer=1, duration_rounds=2, interval_rounds=1, interval_damage=100.0)
    pet_skill = BattleSkill(50.0, physical=False, effects=(500,))

    def _build_with_pet(phys_bps: int, magic_bps: int, poison_bps: int, shield_bps: int):
        pet = p.message(
            "BattlePetSnapshot",
            pet_id=9001,
            owner_player_id=1,
            pet_name="SpiritFox",
            pet_table_id=501,
            level=20,
            base_attributes=p.message("BaseAttributesComp", health=500, mana=200, strength=10, armor=10, speed=150),
            max_health=500,
            max_mana=200,
            physical_attack=100,
            magic_attack=100,
            defense=50,
            skill_table_ids=[77],
        )
        pet.pet_cultivation.physical_damage_bps = phys_bps
        pet.pet_cultivation.magic_damage_bps = magic_bps
        pet.pet_cultivation.poison_damage_bps = poison_bps
        pet.pet_cultivation.shield_capacity_bps = shield_bps
        eq = p.message("EquipmentEffects", outgoing_heal_bps=1000, shield_capacity_bps=500, support_sources_complete=True)
        eq.support_sources.add(
            kind=1, item_id=11, affix_id=1, effect_kind=1, raw_bonus_bps=1000.0, effective_bonus_bps=1000.0
        )
        eq.support_sources.add(
            kind=1, item_id=12, affix_id=2, effect_kind=2, raw_bonus_bps=500.0, effective_bonus_bps=500.0
        )
        req = _create_request(
            p,
            [_player_snapshot(p, 1, 0, equipment_effects=eq, pets=(pet,)), _player_snapshot(p, 2, 1, health=2000)],
            match_mode=4,
            seed=99,
        )
        return TurnEngine(p, req, skills={77: pet_skill}, items={}, buffs={500: poison_buff}, max_rounds=10)

    base_engine = _build_with_pet(0, 0, 0, 0)
    boosted_engine = _build_with_pet(5000, 5000, 4000, 3000)

    for eng in (base_engine, boosted_engine):
        eng.pending[1] = p.message("BattleAction", action_type=3)
        eng.pending[2] = p.message("BattleAction", action_type=3)

    t_base = base_engine.resolve()
    t_boost = boosted_engine.resolve()

    base_dmg = next(e.value for e in t_base.events if e.event_type == 3 and e.target_id == 2)
    boost_dmg = next(e.value for e in t_boost.events if e.event_type == 3 and e.target_id == 2)
    assert boost_dmg > base_dmg

    base_tick = next(e.value for e in t_base.events if e.event_type == 7 and e.target_id == 2)
    boost_tick = next(e.value for e in t_boost.events if e.event_type == 7 and e.target_id == 2)
    assert boost_tick > base_tick

    pet_actor = next(a for a in boosted_engine.actors.values() if a.actor_type == 3)
    assert PetAndSupportEffects.pet_damage_bonus(pet_actor, 1, hit_kind=2) == 0.0  # 反击不吃加成
    granted = boosted_engine.equipment_effects.grant_shield(pet_actor, 100.0, source=pet_actor)
    assert granted == 130

    # 校验辅助效果只读投影与 ModifierSelection
    summary = boosted_engine.support_summary(1)
    assert summary is not None and summary.in_battle is True
    assert summary.healing.complete is True
    assert summary.healing.total_bonus_bps == pytest.approx(1000.0)
    assert summary.shield.total_bonus_bps == pytest.approx(500.0)

    sel = ModifierSelection()
    sel.consider(0.20)
    sel.consider(0.35)
    sel.consider(-0.15)
    assert sel.total() == pytest.approx(0.20)

    # 非法宠物修炼万分比必须拒绝
    bad_pet = p.message(
        "BattlePetSnapshot",
        pet_id=9999,
        owner_player_id=1,
        pet_table_id=501,
        level=20,
        base_attributes=p.message("BaseAttributesComp", health=500, mana=100, speed=100),
    )
    bad_pet.pet_cultivation.physical_damage_bps = 10_001
    with pytest.raises(BattleError):
        TurnEngine(
            p,
            _create_request(p, [_player_snapshot(p, 1, 0, pets=(bad_pet,)), _player_snapshot(p, 2, 1)]),
            skills={},
            items={},
            max_rounds=10,
        )


# ---------------------------------------------------------------------------
# 3. 赛季印战斗规则测试
# ---------------------------------------------------------------------------


def test_season_seal_caps_init_bonuses_and_runtime_triggers():
    # 纯规则：硬上限 500 bps（即使配置表填 800 也必须被夹到 500）
    assert SeasonBattleRules.scaled_bonus_bps(1000, 1000, 800) == 500
    assert SeasonBattleRules.scaled_bonus_bps(500, 1000, 400) == 200
    assert SeasonBattleRules.is_window_active(1000, 5000, True, 2500) is True
    assert SeasonBattleRules.is_window_active(1000, 5000, True, 5000) is False
    assert SeasonBattleRules.compute_catchup_plan(400, 1000, 0, 50) == (600, 30_000)

    p = Protocol()
    # 超过 500 bps 硬上限必须拒绝开局
    bad_eq = p.message("EquipmentEffects", season_max_health_bps=501)
    with pytest.raises(BattleError):
        TurnEngine(
            p,
            _create_request(p, [_player_snapshot(p, 1, 0, equipment_effects=bad_eq), _player_snapshot(p, 2, 1)]),
            skills={},
            items={},
            max_rounds=10,
        )

    # 合法赛季印：+500 bps 气血、+500 bps 防御 + 赛季印效果 6（开局减伤）
    eq1 = p.message("EquipmentEffects", season_max_health_bps=500, season_defense_bps=500)
    eq1.season_seal.CopyFrom(
        p.message(
            "SeasonSealEffect",
            season_id=1,
            seal_id=101,
            effect_id=6,
            bonus_bps=3000,
            cap_bps=1000,
            battle_limit=1,
            active_rounds=2,
            name="守心季印",
        )
    )
    eq2 = p.message("EquipmentEffects")
    eq2.season_seal.CopyFrom(
        p.message(
            "SeasonSealEffect",
            season_id=1,
            seal_id=102,
            effect_id=7,
            bonus_bps=2500,
            per_round_limit=1,
            battle_limit=2,
            name="回灵季印",
        )
    )
    p1 = _player_snapshot(p, 1, 0, health=1000, defense=200, speed=80, equipment_effects=eq1)
    p2 = _player_snapshot(p, 2, 1, health=1000, mana=100, physical_attack=250, speed=120, equipment_effects=eq2)
    p2.max_mana = 200
    engine = TurnEngine(p, _create_request(p, [p1, p2], match_mode=4), skills={}, items={}, max_rounds=10)

    assert engine.actors[1].max_health == 1050
    assert engine.actors[1].attributes.health == 1050
    assert engine.actors[1].defense == 210

    # 回合 1：P2 攻 P1 -> P1 触发赛季印 101 减伤；P1 防御
    engine.pending[1] = p.message("BattleAction", action_type=3)
    engine.pending[2] = p.message("BattleAction", action_type=1, target_id=1)
    t1 = engine.resolve()
    seal_dmg_ev = [e for e in t1.events if e.event_type == 21 and e.season_seal_id == 101]
    assert seal_dmg_ev and seal_dmg_ev[0].artifact_item_id == 0 and seal_dmg_ev[0].value > 0

    # 回合 2：P2 防御 -> 触发赛季印 102 回蓝 200 * 25% = 50
    engine.pending[1] = p.message("BattleAction", action_type=3)
    engine.pending[2] = p.message("BattleAction", action_type=3)
    t2 = engine.resolve()
    assert engine.actors[2].attributes.mana == 150
    assert any(e.event_type == 21 and e.season_seal_id == 102 and e.value == 50 for e in t2.events)

    # 下发客户端状态快照前必须清除私有 season_seal_state
    public_state = engine.snapshot(0, viewer=1)
    assert all(not actor.HasField("season_seal_state") for actor in public_state.actors)


# ---------------------------------------------------------------------------
# 4. 副本怪物战术 AI 测试（HFSM 阶段 + 行为树自保/自愈 + 效用选招 + 无配置回退）
# ---------------------------------------------------------------------------


def test_dungeon_monster_ai_tactical_selection_desperate_defend_heal_and_fallback():
    p = Protocol()
    strike = BattleSkill(45.0, mana_cost=0, physical=True, requires_target=True)
    m_heal = BattleSkill(0.0, mana_cost=0, heal_multiplier=1.0, heal_per_level=2.0)

    # 玩家 1 满血高防；玩家 2 残血且高速度（威胁更高）
    p1 = _player_snapshot(p, 1, 0, health=1000, speed=80)
    p2 = _player_snapshot(p, 2, 0, health=1000, speed=150)
    p2.base_attributes.health = 200
    boss = _monster_state(p, 3001, health=1000, speed=100, is_boss=True, name="Boss")

    # (A) 未配置 monster_ai_profiles 时：choose_monster_action 返回 None，走原版普攻兜底
    plain_engine = TurnEngine(p, _create_request(p, [p1, p2]), monsters=[boss], skills={501: strike}, items={}, max_rounds=10)
    plain_boss = next(a for a in plain_engine.actors.values() if a.actor_type == 2)
    assert choose_monster_action(plain_engine, plain_boss) is None

    # (B) 挂载默认训练好的副本战术档案
    profile = build_trained_default_monster_ai_profile(
        monster_skill_ids=(501,),
        enrage_hp_permyriad=6000,
        desperate_hp_permyriad=3000,
        emergency_defend_hp_permyriad=2500,
        defend_cooldown_rounds=3,
    )
    ai_engine = TurnEngine(
        p,
        _create_request(p, [p1, p2]),
        monsters=[boss],
        skills={501: strike},
        items={},
        max_rounds=10,
        monster_ai_profiles={3001: profile},
    )
    ai_boss = next(a for a in ai_engine.actors.values() if a.actor_type == 2)
    assert 501 in ai_boss.skill_table_ids

    # 开局阶段优先用技能 501 集火残血高威胁玩家 2
    action1 = choose_monster_action(ai_engine, ai_boss)
    assert action1 is not None
    assert action1.action_type == 2 and action1.skill_table_id == 501 and action1.target_id == 2
    assert ai_engine.monster_ai.phase_of(ai_boss.actor_id) == BossPhaseId.OPENING

    # 血线跌至 50% -> 切入 ENRAGE（共享父态 Combat 不重复进入）
    ai_boss.attributes.health = 500
    action2 = choose_monster_action(ai_engine, ai_boss)
    assert action2 is not None and action2.action_type == 2
    ctrl = ai_engine.monster_ai.controllers[ai_boss.actor_id]
    assert ctrl.current_phase == BossPhaseId.ENRAGE
    assert ctrl.combat_enter_count == 1 and ctrl.combat_exit_count == 0

    # 血线跌至 20%（<= 25% 绝境自保线）-> 切入 DESPERATE 并触发 DEFEND + 3 回合冷却
    ai_boss.attributes.health = 200
    action3 = choose_monster_action(ai_engine, ai_boss)
    assert action3 is not None and action3.action_type == 3
    assert ctrl.current_phase == BossPhaseId.DESPERATE

    # 同回合冷却尚未就绪 -> 退回效用选招释放技能
    action4 = choose_monster_action(ai_engine, ai_boss)
    assert action4 is not None and action4.action_type == 2

    # 3 回合后冷却恢复 -> 再次触发绝境 DEFEND
    ai_engine.round_index += 3
    action5 = choose_monster_action(ai_engine, ai_boss)
    assert action5 is not None and action5.action_type == 3

    # (C) 战术自愈分支测试：低于急救血线且携带治疗技能时优先治疗自身
    heal_profile = DungeonMonsterAiProfile(
        emergency_heal_hp_permyriad=4000,
        monster_skill_ids=(502,),
        utility_model=LinearUtilityModel(weights=(100, 100, 100, 100, 0, 100)),
    )
    heal_engine = TurnEngine(
        p,
        _create_request(p, [p1]),
        monsters=[_monster_state(p, 3002, health=1000, is_boss=True)],
        skills={502: m_heal},
        items={},
        max_rounds=10,
        monster_ai_profiles={3002: heal_profile},
    )
    heal_boss = next(a for a in heal_engine.actors.values() if a.actor_type == 2)
    heal_boss.attributes.health = 350
    heal_act = choose_monster_action(heal_engine, heal_boss)
    assert heal_act is not None and heal_act.action_type == 2 and heal_act.skill_table_id == 502 and heal_act.target_id == heal_boss.actor_id


# ---------------------------------------------------------------------------
# 5. 五行试炼志事实追踪与 BattleNode 终局结算集成测试
# ---------------------------------------------------------------------------


def test_trial_fact_tracker_populates_battle_settlement_end_to_end():
    p = Protocol()
    outcomes: list[tuple[int, int]] = []
    node = BattleNode(
        p,
        node_id=1,
        instance_id="battle-1",
        host="127.0.0.1",
        port=9001,
        ticket_secret="secret",
        now_ms=lambda: 20_000,
        steady_ms=lambda: 20_000,
        on_outcome=lambda bid, out: outcomes.append((bid, out)),
        monster_factory=lambda req: [_monster_state(p, 4001, health=120, speed=40, is_boss=True, name="TrialBoss")],
        skills={
            801: BattleSkill(80.0, class_id=1, unlock_level=1, max_rank=10, target_team=1, max_targets=1, physical=False),
        },
        items={},
    )

    eq = p.message("EquipmentEffects", season_max_health_bps=300)
    eq.artifacts.add(item_id=5002, effect_id=6, bonus_bps=2000, cap_bps=1000, battle_limit=1, active_rounds=2)
    eq.artifacts.add(item_id=5001, effect_id=1, bonus_bps=3000, per_round_limit=1, battle_limit=2)
    eq.season_seal.CopyFrom(
        p.message(
            "SeasonSealEffect",
            season_id=2,
            seal_id=205,
            effect_id=7,
            bonus_bps=1500,
            per_round_limit=1,
            battle_limit=1,
        )
    )
    pet = p.message(
        "BattlePetSnapshot",
        pet_id=8801,
        owner_player_id=101,
        pet_name="JadeDragon",
        pet_table_id=601,
        level=20,
        base_attributes=p.message("BaseAttributesComp", health=400, mana=100, speed=110),
        max_health=400,
        max_mana=100,
        physical_attack=80,
    )
    p1 = _player_snapshot(
        p, 101, 0, name="HeroA", physical_attack=180, magic_attack=180, speed=140,
        skills=(801,), skill_ranks={801: 3}, equipment_effects=eq, pets=(pet,),
    )
    p2 = _player_snapshot(p, 102, 0, name="MateB", physical_attack=150, speed=130)
    req = _create_request(p, [p1, p2], match_mode=4, battle_id=9011)

    trial_ctx = TrialContext(journal_entry_id=1001, trial_rule_version=2, season_id=2, guard_monster_table_id=4002)
    resp = node.create(req, trial_context=trial_ctx)
    assert resp.error_message.id == 0

    room = node.rooms[9011]
    boss = next(a for a in room.engine.actors.values() if a.actor_type == 2)

    # 通过直连 dispatch 提交手动指令：P101 施放法术技能 + P102 物理普攻，首回合击杀 Boss
    t101 = node.authorize(
        node._assignment(room, 101, 1).token_payload,
        node._assignment(room, 101, 1).token_signature,
    )
    t102 = node.authorize(
        node._assignment(room, 102, 1).token_payload,
        node._assignment(room, 102, 1).token_signature,
    )
    r1 = node.dispatch(
        t101,
        149,
        p.message(
            "SubmitBattleActionRequest",
            battle_id=9011,
            action=p.message("BattleAction", action_type=2, skill_table_id=801, target_id=boss.actor_id),
        ),
    )
    r2 = node.dispatch(
        t102,
        149,
        p.message(
            "SubmitBattleActionRequest",
            battle_id=9011,
            action=p.message("BattleAction", action_type=1, target_id=boss.actor_id),
        ),
    )
    assert r1.error_message.id == 0 and r2.error_message.id == 0
    assert outcomes == [(9011, 1)]

    commit_req = node.outcome_request(9011)
    assert commit_req is not None
    result_event = p.parse("contracts.kafka.BattleResultEvent", commit_req.raw_result_payload)
    s101 = next(s for s in result_event.settlements if s.player_id == 101)

    assert is_valid_trial_settlement(s101) is True
    facts = s101.trial_facts
    assert facts.journal_entry_id == 1001
    assert facts.trial_rule_version == 2
    assert facts.season_id == 2
    assert facts.initial_human_count == 2
    assert list(facts.teammate_names) == ["MateB"]
    assert facts.used_pet_table_id == 601
    assert list(facts.used_artifact_effect_ids) == [1, 6]
    assert facts.used_season_seal_id == 205
    assert facts.self_or_pet_ever_died is False
    assert facts.boss_guard_heal_total == 0
    assert facts.team_dealt_magic_direct_damage is True
    assert s101.manual_action_rounds == 1
    assert s101.effective_combat_contribution is True
    assert room.engine.trial_tracker.total_rounds == 1
    assert room.engine.trial_tracker.player_deaths[101] == 0
    assert room.engine.trial_tracker.min_hp_bps[101] == 10_000

    # 校验战法印权威判定函数
    assert evaluate_seal_condition(SealConditionType.CLEAR, 0, s101) is True
    assert evaluate_seal_condition(SealConditionType.SPEED_CLEAR, 3, s101) is True
    assert evaluate_seal_condition(SealConditionType.FLAWLESS_SELF_AND_PET, 0, s101) is True
    assert evaluate_seal_condition(SealConditionType.NO_BOSS_GUARD_HEAL, 0, s101) is True

    # 对照：普通非试炼局不写入 trial_facts，确保向后兼容
    plain_req = _create_request(p, [_player_snapshot(p, 201, 0, physical_attack=300)], match_mode=4, battle_id=9012)
    assert node.create(plain_req).error_message.id == 0
    plain_room = node.rooms[9012]
    plain_boss = next(a for a in plain_room.engine.actors.values() if a.actor_type == 2)
    t201 = node.authorize(
        node._assignment(plain_room, 201, 1).token_payload,
        node._assignment(plain_room, 201, 1).token_signature,
    )
    node.dispatch(
        t201,
        149,
        p.message(
            "SubmitBattleActionRequest",
            battle_id=9012,
            action=p.message("BattleAction", action_type=1, target_id=plain_boss.actor_id),
        ),
    )
    plain_event = p.parse("contracts.kafka.BattleResultEvent", node.outcome_request(9012).raw_result_payload)
    assert not plain_event.settlements[0].HasField("trial_facts")
    assert floor_bps(1000, 333) == 33


def test_trial_fact_tracker_guard_heal_stance_dispel_and_pet_death():
    p = Protocol()
    guard_buff = BattleBuff(buff_type=11, max_layer=1, duration_rounds=3, tags=frozenset({"Debuff"}))
    dispel_skill = BattleSkill(
        0.0, class_id=1, unlock_level=1, max_rank=10, target_team=1, max_targets=1, dispel_count=1
    )
    regen_buff = BattleBuff(
        buff_type=40, max_layer=1, duration_rounds=2, interval_rounds=1, health_regeneration=lambda lvl, miss: 80.0
    )

    pet = p.message(
        "BattlePetSnapshot",
        pet_id=7701,
        owner_player_id=1,
        pet_name="FragilePet",
        pet_table_id=602,
        level=10,
        base_attributes=p.message("BaseAttributesComp", health=5, mana=50, speed=50),
        max_health=100,
        max_mana=50,
    )
    p1 = _player_snapshot(p, 1, 0, skills=(901,), skill_ranks={901: 1}, pets=(pet,))
    boss = _monster_state(p, 4001, health=1000, speed=80, is_boss=True, name="Boss")
    boss.attributes.health = 600
    guard = _monster_state(p, 4002, health=500, speed=90, is_boss=False, name="Guard")

    engine = TurnEngine(
        p,
        _create_request(p, [p1]),
        monsters=[boss, guard],
        skills={901: dispel_skill},
        items={},
        buffs={701: guard_buff, 702: regen_buff},
        max_rounds=10,
        trial_context=TrialContext(
            journal_entry_id=2001, trial_rule_version=1, guard_monster_table_id=4002, guard_stance_buff_id=701
        ),
    )
    boss_actor = next(a for a in engine.actors.values() if a.monster_table_id == 4001)
    guard_actor = next(a for a in engine.actors.values() if a.monster_table_id == 4002)
    pet_actor = next(a for a in engine.actors.values() if a.actor_type == 3)

    # 给首领挂护势 buff 与来自护卫的持续回血 buff
    boss_actor.buffs.add(buff_id=1, buff_table_id=701, caster_id=guard_actor.actor_id, layer=1, remain_rounds=3)
    boss_actor.buffs.add(buff_id=2, buff_table_id=702, caster_id=guard_actor.actor_id, layer=1, remain_rounds=2)

    # 玩家 1 驱散首领护势；首领与护卫集火击杀脆皮宠物
    engine.pending[1] = p.message("BattleAction", action_type=2, skill_table_id=901, target_id=boss_actor.actor_id)
    engine.pending[boss_actor.actor_id] = p.message("BattleAction", action_type=1, target_id=pet_actor.actor_id)
    engine.pending[guard_actor.actor_id] = p.message("BattleAction", action_type=3)
    engine.resolve()

    assert engine.trial_tracker.guard_stance_dispelled is True
    assert engine.trial_tracker.boss_guard_heal_total == 80
    assert pet_actor.is_dead is True

    settlement = p.message(
        "BattleSettlementData",
        battle_id=7001,
        player_id=1,
        outcome=1,
        player_team_index=0,
        total_rounds=1,
    )
    engine.trial_tracker.populate_settlement(settlement, 1)
    assert settlement.trial_facts.guard_stance_dispelled is True
    assert settlement.trial_facts.boss_guard_heal_total == 80
    assert settlement.trial_facts.self_or_pet_ever_died is True
    assert settlement.trial_facts.guard_survived is True
    assert evaluate_seal_condition(SealConditionType.BREAK_GUARD_STANCE, 0, settlement) is True
    assert evaluate_seal_condition(SealConditionType.NO_BOSS_GUARD_HEAL, 0, settlement) is False
    assert evaluate_seal_condition(SealConditionType.FLAWLESS_SELF_AND_PET, 0, settlement) is False
    assert evaluate_seal_condition(SealConditionType.GUARD_RESCUED, 0, settlement) is True
