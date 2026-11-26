"""A41 玄镜映法（skill=10041, buff=100410, op=CopyTargetSkill）与 C++/Java 三端对齐回归测试。"""

from __future__ import annotations

from xuanming.data import ConfigTables
from xuanming.nodes.battle import TurnEngine
from xuanming.nodes.battle_tables import BattleBuff, BattleTables
from xuanming.protocol import Protocol


def _player(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    health: int = 5000,
    max_health: int = 10000,
    mana: int = 10000,
    max_mana: int = 10000,
    speed: int = 100,
    physical_attack: int = 500,
    magic_attack: int = 600,
    level: int = 30,
    skills: dict[int, int] | None = None,
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=f"P{player_id}",
        team_index=team_index,
        level=level,
        max_health=max_health,
        max_mana=max_mana,
        physical_attack=physical_attack,
        magic_attack=magic_attack,
        defense=0,
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
    return snap


def _build_pvp(
    protocol: Protocol,
    tables: BattleTables,
    *,
    p1_skills: dict[int, int],
    p2_skills: dict[int, int],
    p1_hp: int = 5000,
    p2_hp: int = 5000,
    p1_magic: int = 600,
    p2_magic: int = 600,
    p1_speed: int = 200,
    p2_speed: int = 50,
    seed: int = 20261126,
    extra_buffs: dict[int, BattleBuff] | None = None,
    p2_buff_ids: tuple[int, ...] = (),
) -> TurnEngine:
    p1 = _player(protocol, 101, 0, health=p1_hp, magic_attack=p1_magic, speed=p1_speed, skills=p1_skills)
    p2 = _player(protocol, 201, 1, health=p2_hp, magic_attack=p2_magic, speed=p2_speed, skills=p2_skills)
    for idx, bid in enumerate(p2_buff_ids, start=1):
        p2.buffs.add(buff_id=idx, buff_table_id=bid)
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=90041,
        match_mode=3,
        seed=seed,
        players=[p1, p2],
    )
    buffs = dict(tables.buffs)
    if extra_buffs:
        buffs.update(extra_buffs)
    return TurnEngine(
        protocol,
        req,
        skills=tables.skills,
        items=tables.items,
        buffs=buffs,
        silence_permissions=tables.silence_permissions,
        max_rounds=20,
    )


def _submit(engine: TurnEngine, actor_id: int, action) -> None:
    assert engine.validate(engine.actors[actor_id], action) == 0
    engine.pending[actor_id] = action


def test_copy_target_skill_mirrors_planned_enemy_skill_with_own_stats_and_no_extra_cost() -> None:
    protocol = Protocol()
    tables = BattleTables(protocol, ConfigTables())
    engine = _build_pvp(
        protocol,
        tables,
        p1_skills={10041: 3},
        p2_skills={10002: 1, 10006: 1},
        p1_magic=1000,
        p2_magic=100,
        p1_speed=300,
        p2_speed=50,
    )
    _submit(engine, 101, protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=201))
    _submit(engine, 201, protocol.message("BattleAction", action_type=2, skill_table_id=10006, target_id=101))
    turn = engine.resolve()

    assert any(
        ev.event_type == 2 and ev.source_id == 101 and ev.skill_table_id == 10041 and ev.hit_kind == 0
        for ev in turn.events
    )
    assert any(
        ev.event_type == 2 and ev.source_id == 101 and ev.skill_table_id == 10006 and ev.hit_kind == 5
        for ev in turn.events
    )
    mirrored_dmg = [
        ev for ev in turn.events
        if ev.event_type == 3 and ev.source_id == 101 and ev.target_id == 201
        and ev.skill_table_id == 10006 and ev.hit_kind == 5 and ev.damage_origin == 6
    ]
    assert mirrored_dmg and mirrored_dmg[0].value > 0

    # 10041 瞬发不滞留 100410 Buff；被复制技能 10006 不额外扣蓝、不进施法者冷却。
    enemy_after = engine.actors[201]
    assert all(b.buff_table_id != 100410 for b in enemy_after.buffs)
    self_after = engine.actors[101]
    assert self_after.skill_cooldown_rounds.get(10041, 0) == 3
    assert self_after.skill_cooldown_rounds.get(10006, 0) == 0
    mana_events = [ev for ev in turn.events if ev.event_type == 14 and ev.source_id == 101]
    assert len(mana_events) == 1


def test_copy_target_skill_falls_back_to_learned_pool_and_resists_when_uncopyable() -> None:
    protocol = Protocol()
    tables = BattleTables(protocol, ConfigTables())

    # 1. 目标防御 -> 方案 A 兜底从已学池抽出 10006 释放。
    fallback = _build_pvp(protocol, tables, p1_skills={10041: 1}, p2_skills={10006: 1})
    _submit(fallback, 101, protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=201))
    _submit(fallback, 201, protocol.message("BattleAction", action_type=3))
    r1 = fallback.resolve()
    assert any(
        ev.event_type == 2 and ev.source_id == 101 and ev.skill_table_id == 10006 and ev.hit_kind == 5
        for ev in r1.events
    )

    # 2. 目标仅会不可复制技能（10041 自身与 forbid_copy=1 的 10115）-> 发出 RESIST(100410) 且无镜像。
    blocked = _build_pvp(protocol, tables, p1_skills={10041: 1}, p2_skills={10041: 1, 10115: 1})
    _submit(blocked, 101, protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=201))
    _submit(blocked, 201, protocol.message("BattleAction", action_type=2, skill_table_id=10115, target_id=101))
    r2 = blocked.resolve()
    assert any(
        ev.event_type == 15 and ev.source_id == 101 and ev.target_id == 201 and ev.buff_table_id == 100410
        for ev in r2.events
    )
    assert not any(ev.event_type == 2 and ev.source_id == 101 and ev.hit_kind == 5 for ev in r2.events)


def test_copy_target_skill_remaps_heal_and_shield_to_caster_team_with_scale() -> None:
    protocol = Protocol()
    tables = BattleTables(protocol, ConfigTables())

    # 1. 复制治疗 10010：等级 30 原值 600 × 0.85 = 510，且治疗施法者自己而非敌方。
    heal_engine = _build_pvp(
        protocol, tables, p1_skills={10041: 1}, p2_skills={10010: 1}, p1_hp=1000, p2_hp=1000,
    )
    _submit(heal_engine, 101, protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=201))
    _submit(heal_engine, 201, protocol.message("BattleAction", action_type=2, skill_table_id=10010, target_id=201))
    r_heal = heal_engine.resolve()
    heals = [ev for ev in r_heal.events if ev.event_type == 4 and ev.source_id == 101 and ev.skill_table_id == 10010]
    assert len(heals) == 1
    assert heals[0].target_id == 101
    assert heals[0].value == 510

    # 2. 复制护盾 10040：切回己方并按 0.85 折算容量。
    shield_engine = _build_pvp(protocol, tables, p1_skills={10041: 1}, p2_skills={10040: 1})
    _submit(shield_engine, 101, protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=201))
    _submit(shield_engine, 201, protocol.message("BattleAction", action_type=2, skill_table_id=10040, target_id=201))
    shield_engine.resolve()
    caster_shields = [b for b in shield_engine.actors[101].buffs if b.buff_table_id == 100400]
    assert len(caster_shields) == 1
    # 等级 30 原值 16*30 = 480，经 0.85 折算 = 408。
    assert caster_shields[0].remaining_capacity == 408


def test_copy_target_skill_rejects_boss_and_resists_skill_copy_immune_target() -> None:
    protocol = Protocol()
    tables = BattleTables(protocol, ConfigTables())

    # 1. PVE 副本 3 首领（Monster.id=16, is_boss=1）不可被 10041 选定。
    pve_req = protocol.message(
        "CreateBattleRequest",
        battle_id=90042,
        match_mode=4,
        battle_config_id=3,
        seed=20261126,
        players=[_player(protocol, 101, 0, skills={10041: 1})],
    )
    monsters = list(tables.monsters(pve_req))
    pve_engine = TurnEngine(
        protocol,
        pve_req,
        monsters=monsters,
        skills=tables.skills,
        items=tables.items,
        buffs=tables.buffs,
        silence_permissions=tables.silence_permissions,
        max_rounds=20,
    )
    boss = next(a for a in pve_engine.actors.values() if a.is_boss)
    assert pve_engine.validate(
        pve_engine.actors[101],
        protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=boss.actor_id),
    ) == 7002

    # 2. 携带 SkillCopy 免疫标签的非首领目标 -> 发出 RESIST(100410) 且无镜像。
    immune_id = 990041
    immune_engine = _build_pvp(
        protocol,
        tables,
        p1_skills={10041: 1},
        p2_skills={10006: 1},
        extra_buffs={immune_id: BattleBuff(infinite=True, immune_tags=frozenset({"SkillCopy"}))},
        p2_buff_ids=(immune_id,),
    )
    _submit(immune_engine, 101, protocol.message("BattleAction", action_type=2, skill_table_id=10041, target_id=201))
    _submit(immune_engine, 201, protocol.message("BattleAction", action_type=2, skill_table_id=10006, target_id=101))
    r_imm = immune_engine.resolve()
    assert any(
        ev.event_type == 15 and ev.source_id == 101 and ev.target_id == 201 and ev.buff_table_id == 100410
        for ev in r_imm.events
    )
    assert not any(ev.event_type == 2 and ev.source_id == 101 and ev.hit_kind == 5 for ev in r_imm.events)
