"""C++ Working Tree 五行玩法扩展（515..521 阵法/幻境/配装收藏）与 Battle V4（锦标赛灵脉/MVP/阵法/相生/爬塔/自定义切磋）回归测试。"""

from __future__ import annotations

import asyncio
import dataclasses

import pytest
from test_monopoly import TOKEN, _enter, _settings

from xuanming.data import ConfigTables
from xuanming.nodes.battle import BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    EVENT_WUXING_SYNERGY,
    MATCH_MODE_PVE_SOLO,
    MATCH_MODE_PVP_CHALLENGE,
    MATCH_MODE_SEASON_TOURNAMENT,
    MAX_SAVED_BUILD_BOOKMARKS,
    TOURNAMENT_LEYLINE_EARTH,
    TOURNAMENT_LEYLINE_FIRE,
    TOURNAMENT_LEYLINE_METAL,
    TOURNAMENT_LEYLINE_UNSPECIFIED,
    TOURNAMENT_LEYLINE_WATER,
    TOURNAMENT_LEYLINE_WOOD,
    WUXING_ELEMENT_EARTH,
    WUXING_ELEMENT_FIRE,
    WUXING_ELEMENT_METAL,
    WUXING_ELEMENT_WATER,
    WUXING_ELEMENT_WOOD,
    WUXING_FIRE_CRIT_BURN_CAP_PER_HIT,
    WUXING_FORMATIONS,
    WUXING_SYNERGY_BONUS_PCT,
    WUXING_TOWER_MAX_FLOORS,
    WUXING_TOWER_SIGILS,
    are_elements_mutually_generating,
    element_generates,
    element_of_class,
    element_of_pet,
    formation_counter_bonus_pct,
    formation_of,
    is_synergy_round,
    is_valid_custom_duel_round_limit,
    roll_tower_sigil_offer,
    tower_floor_dungeon_id,
    tower_floor_leyline_element,
    tower_monster_attack_pct,
    tower_monster_health_pct,
    wuxing_week_key,
)
from xuanming.nodes.battle_tables import BattleBuff, BattleItem, BattleSkill
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime


async def _tcp_call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    message_id = p.message_ids[message_name]
    request = p.message(p.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def _make_player(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    hp: int = 10_000,
    mana: int = 1_000,
    max_mana: int | None = None,
    speed: int = 200,
    phys: int = 1_000,
    magic: int = 1_000,
    defense: int = 0,
    level: int = 40,
    class_id: int = 1,
    formation_id: int = 0,
    crit: int = 0,
    skills: tuple[int, ...] = (),
    items: tuple[tuple[int, int], ...] = (),
    pets: tuple = (),
    tower_context=None,
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=f"P{player_id}",
        class_id=class_id,
        level=level,
        max_health=hp,
        max_mana=max_mana if max_mana is not None else mana,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        team_index=team_index,
        formation_id=formation_id,
        skill_table_ids=list(skills),
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = mana
    snap.base_attributes.speed = speed
    snap.base_attributes.critchance = crit
    snap.routing.session_id = (7 << 17) + player_id
    snap.routing.gate_node_id = 7
    snap.routing.gate_instance_id = "gate-1"
    snap.routing.scene_node_id = 10
    snap.routing.scene_instance_id = "scene-1"
    snap.routing.zone_id = 1
    for item_id, count in items:
        snap.items.add(item_table_id=item_id, count=count)
    for pet in pets:
        snap.pets.add().CopyFrom(pet)
    if tower_context is not None:
        snap.tower_context.CopyFrom(tower_context)
    return snap


def _make_pet(
    protocol: Protocol,
    pet_id: int,
    owner_id: int,
    pet_table_id: int,
    *,
    name: str = "玄灵宠",
    hp: int = 5_000,
    mana: int = 500,
    speed: int = 150,
    phys: int = 800,
    magic: int = 400,
    defense: int = 0,
    competitive_temporary: bool = False,
):
    snap = protocol.message(
        "BattlePetSnapshot",
        pet_id=pet_id,
        owner_player_id=owner_id,
        pet_table_id=pet_table_id,
        pet_name=name,
        level=40,
        max_health=hp,
        max_mana=mana,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        competitive_temporary=competitive_temporary,
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = mana
    snap.base_attributes.speed = speed
    return snap


def _make_monster(
    protocol: Protocol,
    *,
    hp: int = 10_000,
    phys: int = 400,
    magic: int = 300,
    defense: int = 0,
    speed: int = 50,
    table_id: int = 101,
):
    m = protocol.message(
        "BattleActorState",
        name="幻境妖将",
        level=40,
        max_health=hp,
        max_mana=500,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        monster_table_id=table_id,
    )
    m.attributes.health = hp
    m.attributes.mana = 500
    m.attributes.speed = speed
    return m


def _create_req(
    protocol: Protocol,
    battle_id: int,
    players: list,
    *,
    match_mode: int = MATCH_MODE_SEASON_TOURNAMENT,
    seed: int = 42,
    tournament_context=None,
    custom_duel_rules=None,
    record_replay: bool = False,
):
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        match_mode=match_mode,
        battle_config_id=101 if match_mode == MATCH_MODE_PVE_SOLO else 0,
        seed=seed,
        created_at_ms=10_000,
        deadline_ms=1_000_000,
        admit_before_ms=100_000,
        placement_operation_id=f"op-{battle_id}",
        target_instance_id="battle-1",
        required_physical_region=1,
        outcome_protocol_version=1,
        record_replay=record_replay,
    )
    for p in players:
        req.players.add().CopyFrom(p)
    if tournament_context is not None:
        req.tournament_context.CopyFrom(tournament_context)
    if custom_duel_rules is not None:
        req.custom_duel_rules.CopyFrom(custom_duel_rules)
    return req


# ==============================================================================
# 1. 五行玩法纯规则校验（对照 wuxing_expansion_rules.h）
# ==============================================================================


def test_wuxing_expansion_pure_rules_match_cpp_header():
    assert element_of_class(0) == 0
    assert [element_of_class(i) for i in range(1, 6)] == [1, 2, 3, 4, 5]
    assert element_of_class(6) == WUXING_ELEMENT_METAL
    assert element_of_pet(3) == WUXING_ELEMENT_WATER

    # 相生链：金(1)->水(3)->木(2)->火(4)->土(5)->金(1)
    assert element_generates(WUXING_ELEMENT_METAL, WUXING_ELEMENT_WATER)
    assert element_generates(WUXING_ELEMENT_WATER, WUXING_ELEMENT_WOOD)
    assert element_generates(WUXING_ELEMENT_WOOD, WUXING_ELEMENT_FIRE)
    assert element_generates(WUXING_ELEMENT_FIRE, WUXING_ELEMENT_EARTH)
    assert element_generates(WUXING_ELEMENT_EARTH, WUXING_ELEMENT_METAL)
    assert not element_generates(WUXING_ELEMENT_METAL, WUXING_ELEMENT_WOOD)
    assert are_elements_mutually_generating(WUXING_ELEMENT_WATER, WUXING_ELEMENT_METAL)
    assert [is_synergy_round(r) for r in range(0, 7)] == [False, False, False, True, False, False, True]

    # 5 套阵法与克制环 1>2>3>4>5>1
    assert len(WUXING_FORMATIONS) == 5
    for atk, defn in ((1, 2), (2, 3), (3, 4), (4, 5), (5, 1)):
        assert formation_counter_bonus_pct(atk, defn) == 5
        assert formation_counter_bonus_pct(defn, atk) == 0
    assert formation_of(1).damage_bonus_pct == 5 and formation_of(1).speed_bonus_pct == 2
    assert formation_of(2).damage_taken_reduce_pct == 6 and formation_of(2).opening_shield_pct == 4
    assert formation_of(3).speed_bonus_pct == 6 and formation_of(3).heal_shield_bonus_pct == 4
    assert formation_of(4).control_resist_pct == 8 and formation_of(4).round_mana_regen_pct == 3
    assert formation_of(5).crit_bonus_pct == 4 and formation_of(5).pet_damage_bonus_pct == 8

    # 15 枚幻境符印与层数缩放（含第 5/10 层首领额外强化）
    assert len(WUXING_TOWER_SIGILS) == 15
    assert tower_floor_dungeon_id(1) == 101 and tower_floor_dungeon_id(10) == 107
    assert tower_monster_health_pct(1) == 85 and tower_monster_attack_pct(1) == 85
    assert tower_monster_health_pct(5) == 85 + 32 + 10 and tower_monster_attack_pct(5) == 85 + 20 + 6
    assert tower_monster_health_pct(10) == 85 + 72 + 20 and tower_monster_attack_pct(10) == 85 + 45 + 12

    # 确定性三选一：前 3 层不含仙品（rarity==3），且相同种子完全确定
    early_a = roll_tower_sigil_offer(2026, 1, 1, [])
    early_b = roll_tower_sigil_offer(2026, 1, 1, [])
    assert early_a == early_b and len(early_a) == 3 and len(set(early_a)) == 3
    sigil_map = {s.id: s for s in WUXING_TOWER_SIGILS}
    assert all(sigil_map[sid].rarity < 3 for sid in early_a)

    # 排除已持有符印
    next_offer = roll_tower_sigil_offer(2026, 1, 4, early_a)
    assert len(next_offer) == 3 and not (set(next_offer) & set(early_a))

    # 自定义切磋回合上限白名单
    assert [is_valid_custom_duel_round_limit(v) for v in (0, 8, 12, 20, 1, 7, 15, 99)] == [
        True, True, True, True, False, False, False, False,
    ]


# ==============================================================================
# 2. 真实 TCP Runtime("local") 端到端验证全部 7 条 SceneSeason RPC（515..521）与 PrepareBattle
# ==============================================================================


def test_scene_wuxing_rpcs_515_to_521_and_prepare_battle_over_local_tcp(tmp_path):
    async def scenario():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="wuxing-taoist") as (client, player_id):
                protocol = client.protocol
                # 确认 515..521 全部已注册并可经真实 TCP Gate -> Scene 路由
                assert protocol.message_ids["SceneSeasonClientPlayerEquipWuxingFormation"] == 515
                assert protocol.message_ids["SceneSeasonClientPlayerGetWuxingTowerState"] == 516
                assert protocol.message_ids["SceneSeasonClientPlayerStartWuxingTowerRun"] == 517
                assert protocol.message_ids["SceneSeasonClientPlayerPickWuxingTowerSigil"] == 518
                assert protocol.message_ids["SceneSeasonClientPlayerAbandonWuxingTowerRun"] == 519
                assert protocol.message_ids["SceneSeasonClientPlayerSaveBattleBuildBookmark"] == 520
                assert protocol.message_ids["SceneSeasonClientPlayerDeleteBattleBuildBookmark"] == 521

                # --- 515 EquipWuxingFormation + GetSeasonState 同步 ---
                bad_form = await _tcp_call(client, 1, "SceneSeasonClientPlayerEquipWuxingFormation", formation_id=6)
                assert bad_form.error_message.id == 1005

                eq_form = await _tcp_call(client, 2, "SceneSeasonClientPlayerEquipWuxingFormation", formation_id=3)
                assert not eq_form.error_message.id
                assert eq_form.equipped_formation_id == 3
                assert eq_form.state.equipped_formation_id == 3

                season_state = await _tcp_call(client, 3, "SceneSeasonClientPlayerGetSeasonState")
                assert not season_state.error_message.id
                assert season_state.state.equipped_formation_id == 3

                # --- 516 GetWuxingTowerState & 517 StartWuxingTowerRun ---
                t0 = await _tcp_call(client, 4, "SceneSeasonClientPlayerGetWuxingTowerState")
                assert not t0.error_message.id
                assert not t0.state.run_active
                assert t0.state.max_floors == WUXING_TOWER_MAX_FLOORS
                assert t0.state.current_floor == 1
                assert t0.state.current_floor_dungeon_id == 101
                assert 1 <= t0.state.current_floor_leyline_element <= 5

                started = await _tcp_call(client, 5, "SceneSeasonClientPlayerStartWuxingTowerRun")
                assert not started.error_message.id
                assert started.state.run_active is True
                assert started.state.run_id == 1
                assert started.state.current_floor == 1

                # 进行中重复开局被拒
                dup_start = await _tcp_call(client, 6, "SceneSeasonClientPlayerStartWuxingTowerRun")
                assert dup_start.error_message.id == 1005

                # --- Scene.PrepareBattle 权威注入 formation_id 与 tower_context ---
                now_ms_val = runtime.scene.battle._clock()
                prep_bad_cfg = await runtime.scene.battle.prepare(
                    protocol.message(
                        "PrepareBattleRequest",
                        player_id=player_id,
                        battle_id=88001,
                        battle_node_id=1,
                        battle_config_id=103,  # 第 1 层必须是 101
                        deadline_ms=now_ms_val + 120_000,
                        match_mode=MATCH_MODE_PVE_SOLO,
                        is_wuxing_tower=True,
                    )
                )
                assert prep_bad_cfg.error_message.id == 1006

                prep_ok = await runtime.scene.battle.prepare(
                    protocol.message(
                        "PrepareBattleRequest",
                        player_id=player_id,
                        battle_id=88002,
                        battle_node_id=1,
                        battle_config_id=101,
                        deadline_ms=now_ms_val + 120_000,
                        match_mode=MATCH_MODE_PVE_SOLO,
                        is_wuxing_tower=True,
                    )
                )
                assert not prep_ok.error_message.id
                assert prep_ok.snapshot.formation_id == 3
                assert prep_ok.snapshot.tower_context.run_id == 1
                assert prep_ok.snapshot.tower_context.floor == 1
                assert prep_ok.snapshot.tower_context.week_key == started.state.week_key
                assert prep_ok.snapshot.tower_context.leyline_element == started.state.current_floor_leyline_element
                await runtime.scene.battle.cancel(
                    protocol.message("CancelBattlePrepareRequest", player_id=player_id, battle_id=88002)
                )

                # --- 单层胜利结算推进至第 2 层并生成三选一符印（幂等校验 + 选符前禁止进第 2 层） ---
                scene_player = runtime.scene.locations[player_id].players[player_id]
                pdata = runtime.scene.rows.assemble(scene_player.snapshot)
                settlement1 = protocol.message(
                    "BattleSettlementData",
                    battle_id=88002,
                    player_id=player_id,
                    outcome=1,
                    player_team_index=0,
                    total_rounds=4,
                )
                settlement1.tower_settlement.week_key = started.state.week_key
                settlement1.tower_settlement.run_id = 1
                settlement1.tower_settlement.floor = 1
                settlement1.tower_settlement.total_rounds = 4
                assert runtime.scene.season.settle_wuxing_tower_battle(pdata, settlement1) is True
                # 重复投递同一结算幂等跳过
                assert runtime.scene.season.settle_wuxing_tower_battle(pdata, settlement1) is False
                await runtime.scene.commit_player_data(scene_player, pdata)

                t_after_f1 = await _tcp_call(client, 7, "SceneSeasonClientPlayerGetWuxingTowerState")
                assert not t_after_f1.error_message.id
                assert t_after_f1.state.current_floor == 2
                assert t_after_f1.state.weekly_cleared_mask == 0b1
                assert t_after_f1.state.weekly_best_floor == 1
                assert len(t_after_f1.state.pending_offer_sigil_ids) == 3

                # 待选符印未选前禁止直接 PrepareBattle 第 2 层
                prep_blocked = await runtime.scene.battle.prepare(
                    protocol.message(
                        "PrepareBattleRequest",
                        player_id=player_id,
                        battle_id=88003,
                        battle_node_id=1,
                        battle_config_id=103,
                        deadline_ms=now_ms_val + 120_000,
                        match_mode=MATCH_MODE_PVE_SOLO,
                        is_wuxing_tower=True,
                    )
                )
                assert prep_blocked.error_message.id == 1006

                # --- 518 PickWuxingTowerSigil ---
                bad_run_pick = await _tcp_call(
                    client, 8, "SceneSeasonClientPlayerPickWuxingTowerSigil",
                    expected_run_id=999, sigil_id=t_after_f1.state.pending_offer_sigil_ids[0],
                )
                assert bad_run_pick.error_message.id == 1005

                chosen_sigil = t_after_f1.state.pending_offer_sigil_ids[0]
                picked = await _tcp_call(
                    client, 9, "SceneSeasonClientPlayerPickWuxingTowerSigil",
                    expected_run_id=1, sigil_id=chosen_sigil,
                )
                assert not picked.error_message.id
                assert list(picked.state.active_sigil_ids) == [chosen_sigil]
                assert not picked.state.pending_offer_sigil_ids

                # 选符后可合法 PrepareBattle 第 2 层且权威注入已选符印
                prep_f2 = await runtime.scene.battle.prepare(
                    protocol.message(
                        "PrepareBattleRequest",
                        player_id=player_id,
                        battle_id=88004,
                        battle_node_id=1,
                        battle_config_id=103,
                        deadline_ms=now_ms_val + 120_000,
                        match_mode=MATCH_MODE_PVE_SOLO,
                        is_wuxing_tower=True,
                    )
                )
                assert not prep_f2.error_message.id
                assert prep_f2.snapshot.tower_context.floor == 2
                assert list(prep_f2.snapshot.tower_context.active_sigil_ids) == [chosen_sigil]
                await runtime.scene.battle.cancel(
                    protocol.message("CancelBattlePrepareRequest", player_id=player_id, battle_id=88004)
                )

                # --- 519 AbandonWuxingTowerRun ---
                bad_abandon = await _tcp_call(
                    client, 10, "SceneSeasonClientPlayerAbandonWuxingTowerRun", expected_run_id=999,
                )
                assert bad_abandon.error_message.id == 1005

                abandoned = await _tcp_call(
                    client, 11, "SceneSeasonClientPlayerAbandonWuxingTowerRun", expected_run_id=1,
                )
                assert not abandoned.error_message.id
                assert abandoned.state.run_active is False
                assert not abandoned.state.active_sigil_ids
                assert abandoned.state.weekly_cleared_mask == 0b1  # 首通位图不因放弃清空

                # 放弃后可重新开第 2 轮并模拟通关第 10 层入本周最速榜
                started2 = await _tcp_call(client, 12, "SceneSeasonClientPlayerStartWuxingTowerRun")
                assert not started2.error_message.id and started2.state.run_id == 2
                pdata2 = runtime.scene.rows.assemble(scene_player.snapshot)
                pdata2.season_component.wuxing_tower.current_floor = 10
                pdata2.season_component.wuxing_tower.run_total_rounds = 25
                pdata2.season_component.wuxing_tower.active_sigil_ids.extend([101, 202, 403])
                settlement10 = protocol.message(
                    "BattleSettlementData",
                    battle_id=88010,
                    player_id=player_id,
                    outcome=1,
                    player_team_index=0,
                    total_rounds=5,
                )
                settlement10.tower_settlement.week_key = started2.state.week_key
                settlement10.tower_settlement.run_id = 2
                settlement10.tower_settlement.floor = 10
                settlement10.tower_settlement.total_rounds = 5
                assert runtime.scene.season.settle_wuxing_tower_battle(pdata2, settlement10) is True
                await runtime.scene.commit_player_data(scene_player, pdata2)

                t_final = await _tcp_call(client, 13, "SceneSeasonClientPlayerGetWuxingTowerState")
                assert not t_final.error_message.id
                assert t_final.state.run_active is False
                assert t_final.state.weekly_best_floor == 10
                assert t_final.state.weekly_best_rounds == 30
                assert (t_final.state.weekly_cleared_mask & (1 << 9)) != 0
                assert len(t_final.state.weekly_leaderboard) == 1
                assert t_final.state.weekly_leaderboard[0].player_id == player_id
                assert t_final.state.weekly_leaderboard[0].best_rounds == 30
                assert list(t_final.state.weekly_leaderboard[0].sigil_ids) == [101, 202, 403]

                # --- 520 SaveBattleBuildBookmark & 521 DeleteBattleBuildBookmark ---
                bm = protocol.message(
                    "SavedBattleBuildBookmarkView",
                    title="天覆剑阵速推套路",
                    source_battle_id=99001,
                    source_player_id=player_id,
                    source_player_name="太清道友",
                    class_id=1,
                    formation_id=1,
                    season_seal_id=101,
                    active_pet_table_id=3,
                    active_pet_name="碧水玄龟",
                    skill_table_ids=list(range(10001, 10020)),  # 超 12 条应自动截断为 12
                    wuxing_sigil_ids=[101, 99999, 202, 101],    # 过滤非法并去重
                )
                saved = await _tcp_call(client, 14, "SceneSeasonClientPlayerSaveBattleBuildBookmark", bookmark=bm)
                assert not saved.error_message.id
                assert saved.saved.bookmark_id == 1
                assert len(saved.saved.skill_table_ids) == 12
                assert list(saved.saved.wuxing_sigil_ids) == [101, 202]
                assert len(saved.all_bookmarks) == 1

                # 同 ID 覆盖更新
                bm_upd = protocol.message("SavedBattleBuildBookmarkView", bookmark_id=1, title="修订版阵法", formation_id=5)
                updated = await _tcp_call(client, 15, "SceneSeasonClientPlayerSaveBattleBuildBookmark", bookmark=bm_upd)
                assert not updated.error_message.id
                assert len(updated.all_bookmarks) == 1
                assert updated.saved.title == "修订版阵法" and updated.saved.formation_id == 5

                # GetSeasonState 同步返回 saved_builds
                st_bm = await _tcp_call(client, 16, "SceneSeasonClientPlayerGetSeasonState")
                assert len(st_bm.state.saved_builds) == 1 and st_bm.state.saved_builds[0].title == "修订版阵法"

                # 填满 12 条后第 13 条被拒绝（预埋 2..12 以避开单秒限流阀并真测 12 条硬上限）
                pdata_bm = runtime.scene.rows.assemble(scene_player.snapshot)
                for idx in range(2, MAX_SAVED_BUILD_BOOKMARKS + 1):
                    pdata_bm.season_component.saved_builds.add(bookmark_id=idx, title=f"套路{idx}", formation_id=2)
                await runtime.scene.commit_player_data(scene_player, pdata_bm)
                overflow = await _tcp_call(
                    client, 40, "SceneSeasonClientPlayerSaveBattleBuildBookmark",
                    bookmark=protocol.message("SavedBattleBuildBookmarkView", title="超限13", formation_id=2),
                )
                assert overflow.error_message.id == 1005

                # 521 删除收藏与不存在 ID 校验
                bad_del = await _tcp_call(client, 41, "SceneSeasonClientPlayerDeleteBattleBuildBookmark", bookmark_id=9999)
                assert bad_del.error_message.id == 1005
                ok_del = await _tcp_call(client, 42, "SceneSeasonClientPlayerDeleteBattleBuildBookmark", bookmark_id=1)
                assert not ok_del.error_message.id
                assert len(ok_del.all_bookmarks) == MAX_SAVED_BUILD_BOOKMARKS - 1

                # --- PrepareBattle 自定义切磋规则白名单与满血蓝隔离 ---
                bad_duel = await runtime.scene.battle.prepare(
                    protocol.message(
                        "PrepareBattleRequest",
                        player_id=player_id,
                        battle_id=88020,
                        battle_node_id=1,
                        deadline_ms=now_ms_val + 120_000,
                        match_mode=MATCH_MODE_PVP_CHALLENGE,
                        custom_duel_rules=protocol.message("CustomDuelRules", round_limit_override=15),
                    )
                )
                assert bad_duel.error_message.id == 1005

                ok_duel = await runtime.scene.battle.prepare(
                    protocol.message(
                        "PrepareBattleRequest",
                        player_id=player_id,
                        battle_id=88021,
                        battle_node_id=1,
                        deadline_ms=now_ms_val + 120_000,
                        match_mode=MATCH_MODE_PVP_CHALLENGE,
                        custom_duel_rules=protocol.message("CustomDuelRules", round_limit_override=8, ban_control_skills=True),
                    )
                )
                assert not ok_duel.error_message.id
                assert ok_duel.snapshot.base_attributes.health == ok_duel.snapshot.max_health
                assert ok_duel.snapshot.base_attributes.mana == ok_duel.snapshot.max_mana
                await runtime.scene.battle.cancel(
                    protocol.message("CancelBattlePrepareRequest", player_id=player_id, battle_id=88021)
                )

    asyncio.run(scenario())


# ==============================================================================
# 3. Battle V4：锦标赛 V2 五行灵脉与 V3 MVP 结算统计（对齐 C++ turn_battle_engine_test）
# ==============================================================================


def test_tournament_v2_leylines_and_snapshot_propagation():
    protocol = Protocol()

    def run_single_attack(element: int) -> tuple[int, int]:
        ctx = protocol.message(
            "TournamentBattleContext",
            season_id=1,
            match_id=88001,
            best_of=3,
            game_index=2,
            wins_a=1,
            wins_b=0,
            leyline_element=element,
            squad_a_id=101,
            squad_a_name="青云剑派",
            squad_b_id=102,
            squad_b_name="天墉问道",
        )
        p_a = _make_player(protocol, 101, 0, hp=10_000, phys=1_000, speed=200)
        p_b = _make_player(protocol, 201, 1, hp=10_000, phys=100, speed=50)
        req = _create_req(protocol, 9950 + element, [p_a, p_b], tournament_context=ctx)
        engine = TurnEngine(protocol, req, skills={}, items={}, max_rounds=20)

        snap = engine.snapshot(0, viewer=101)
        assert snap.tournament_context.match_id == 88001
        assert snap.tournament_context.best_of == 3
        assert snap.tournament_context.leyline_element == element
        assert snap.tournament_context.squad_a_name == "青云剑派"

        engine.pending[101] = protocol.message("BattleAction", action_type=1, target_id=201)
        engine.pending[201] = protocol.message("BattleAction", action_type=3)
        turn = engine.resolve()
        hits = [ev for ev in turn.events if ev.event_type == 3 and ev.source_id == 101]
        assert len(hits) == 1
        return hits[0].value, engine.tournament_earth_shield_of(201)

    base_dmg, _ = run_single_attack(TOURNAMENT_LEYLINE_UNSPECIFIED)
    metal_dmg, _ = run_single_attack(TOURNAMENT_LEYLINE_METAL)
    fire_dmg, _ = run_single_attack(TOURNAMENT_LEYLINE_FIRE)
    earth_dmg, earth_shield_after = run_single_attack(TOURNAMENT_LEYLINE_EARTH)

    assert base_dmg > 0
    assert metal_dmg > base_dmg
    assert fire_dmg > metal_dmg
    # 土厚灵脉首回合 8% 气血护盾(800)完整吸收普攻伤害，掉血为 0 且护盾余额下降
    assert earth_dmg == 0
    assert 0 < earth_shield_after < 800

    # 木生灵脉：药水治疗量 +10%（100 -> 110）
    wood_ctx = protocol.message("TournamentBattleContext", match_id=88002, leyline_element=TOURNAMENT_LEYLINE_WOOD)
    p_a = _make_player(protocol, 101, 0, hp=1000, items=((5001, 1),))
    p_a.base_attributes.health = 500
    p_b = _make_player(protocol, 201, 1, hp=1000, speed=50)
    wood_engine = TurnEngine(
        protocol,
        _create_req(protocol, 9960, [p_a, p_b], tournament_context=wood_ctx),
        skills={},
        items={5001: BattleItem(usable=True, heal_health=100, heal_mana=0)},
        max_rounds=20,
    )
    wood_engine.pending[101] = protocol.message("BattleAction", action_type=4, item_table_id=5001, target_id=101)
    wood_engine.pending[201] = protocol.message("BattleAction", action_type=3)
    wood_turn = wood_engine.resolve()
    item_ev = next(ev for ev in wood_turn.events if ev.event_type == 10)
    assert item_ev.value == 110

    # 水润灵脉：回合初额外回复 5% 最大法力（200 -> +10，50 -> 60）
    water_ctx = protocol.message("TournamentBattleContext", match_id=88003, leyline_element=TOURNAMENT_LEYLINE_WATER)
    p_w = _make_player(protocol, 101, 0, hp=1000, mana=50, max_mana=200)
    water_engine = TurnEngine(
        protocol,
        _create_req(protocol, 9961, [p_w, _make_player(protocol, 201, 1, hp=1000, speed=50)], tournament_context=water_ctx),
        skills={},
        items={},
        max_rounds=20,
    )
    water_engine.pending[101] = protocol.message("BattleAction", action_type=3)
    water_engine.pending[201] = protocol.message("BattleAction", action_type=3)
    water_turn = water_engine.resolve()
    actor_a = next(a for a in water_turn.state.actors if a.actor_id == 101)
    assert actor_a.attributes.mana == 60


def test_tournament_v3_mvp_metrics_and_replay_actor_builds():
    protocol = Protocol()
    silence_buff = BattleBuff(buff_type=31, duration_rounds=1)
    regen_buff = BattleBuff(buff_type=40, duration_rounds=2, interval_rounds=1, health_regeneration=lambda _lvl, _m: 80.0)
    skills = {
        109: BattleSkill(base_damage=0, target_modes=(1,), effects=(901,)),
        110: BattleSkill(base_damage=0, target_modes=(1,), effects=(902,)),
    }
    buffs = {901: silence_buff, 902: regen_buff}

    ctx = protocol.message(
        "TournamentBattleContext",
        season_id=1,
        match_id=88010,
        best_of=1,
        game_index=1,
        squad_a_emblem_id=2,
        squad_b_emblem_id=5,
        is_third_place=True,
    )
    p_a = _make_player(
        protocol, 101, 0, hp=1000, phys=1000, speed=200, formation_id=1, skills=(109, 110),
        pets=(_make_pet(protocol, 501, 101, 3, name="水灵宠"),),
    )
    p_a.base_attributes.health = 500
    p_b = _make_player(protocol, 201, 1, hp=5000, phys=100, speed=50, formation_id=2)
    req = _create_req(protocol, 9970, [p_a, p_b], tournament_context=ctx, record_replay=True)

    node = BattleNode(
        protocol,
        node_id=1,
        instance_id="battle-1",
        host="127.0.0.1",
        port=9001,
        ticket_secret=b"secret",
        now_ms=lambda: 10_000,
        skills=skills,
        items={},
        buffs=buffs,
        on_outcome=lambda *_: None,
    )
    resp = node.create(req)
    assert not resp.error_message.id
    room = node.rooms[9970]
    engine = room.engine

    # 校验战报冻结脱敏公开配装 actor_builds
    assert room.replay is not None
    assert len(room.replay._replay.actor_builds) == 2
    build_a = room.replay._replay.actor_builds[0]
    assert build_a.player_id == 101 and build_a.formation_id == 1 and build_a.wuxing_element == WUXING_ELEMENT_METAL
    assert build_a.active_pet_table_id == 3 and build_a.active_pet_element == WUXING_ELEMENT_WATER

    # 回合 1 普攻，回合 2 沉默控制，回合 3 周期回血
    engine.pending[101] = protocol.message("BattleAction", action_type=1, target_id=201)
    engine.pending[201] = protocol.message("BattleAction", action_type=3)
    engine.resolve()

    engine.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=109, target_id=201)
    engine.pending[201] = protocol.message("BattleAction", action_type=3)
    engine.resolve()

    engine.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=110, target_id=101)
    engine.pending[201] = protocol.message("BattleAction", action_type=3)
    engine.resolve()

    room.proposed_outcome = 1
    result = node._result(room)
    settle_a = next(s for s in result.settlements if s.player_id == 101)
    assert settle_a.total_damage_dealt > 0
    assert settle_a.total_healing_done > 0
    assert settle_a.total_controls_landed == 1

    snap = engine.snapshot(0, viewer=101)
    assert snap.tournament_context.squad_a_emblem_id == 2
    assert snap.tournament_context.squad_b_emblem_id == 5
    assert snap.tournament_context.is_third_place is True


# ==============================================================================
# 4. Battle V4：五行阵法与克制环、人宠相生合击（23）、五行幻境符印与层数缩放、自定义切磋规则
# ==============================================================================


def test_wuxing_formations_counter_ring_and_human_pet_synergy_event_23():
    protocol = Protocol()

    # 1) 阵法 1(天覆 +5%直伤 +2%速, 克2 +5%) vs 阵法 2(地载 -6%受击 +4%开局护盾) vs 无阵
    def duel_first_hit(form_a: int, form_b: int) -> tuple[int, int, int]:
        p_a = _make_player(protocol, 101, 0, hp=10_000, phys=2000, speed=200, formation_id=form_a)
        p_b = _make_player(protocol, 201, 1, hp=10_000, phys=100, speed=100, formation_id=form_b)
        eng = TurnEngine(protocol, _create_req(protocol, 7000 + form_a * 10 + form_b, [p_a, p_b]), skills={}, items={}, max_rounds=20)
        snap = eng.snapshot(0)
        assert snap.team_a_formation_id == form_a and snap.team_b_formation_id == form_b
        eng.pending[101] = protocol.message("BattleAction", action_type=1, target_id=201)
        eng.pending[201] = protocol.message("BattleAction", action_type=3)
        turn = eng.resolve()
        hit = next(ev for ev in turn.events if ev.event_type == 3 and ev.source_id == 101)
        return hit.value + hit.absorbed, eng.configured.effective_speed(eng.actors[101]), eng.tournament_earth_shield_of(201)

    base_total, base_spd, _ = duel_first_hit(0, 0)
    f1_total, f1_spd, _ = duel_first_hit(1, 0)
    f1_vs_f2_total, _, f2_shield_after = duel_first_hit(1, 2)
    f0_vs_f2_total, _, _ = duel_first_hit(0, 2)
    assert f1_total > base_total
    assert f1_spd > base_spd
    assert f0_vs_f2_total < base_total  # 地载阵 -6% 减伤
    assert f1_vs_f2_total > f0_vs_f2_total  # 天覆阵直伤 + 克地载阵 +5%
    assert 0 <= f2_shield_after < 400  # 地载阵 4% 护盾(400)参与了吸收

    # 2) 人宠五行相生合击（金 class_id=1 + 水 pet_table_id=3）：第 3 回合触发 BATTLE_EVENT_WUXING_SYNERGY = 23 且增伤 +10%
    pet = _make_pet(protocol, 501, 101, pet_table_id=3, phys=1000, speed=150)
    p_a = _make_player(protocol, 101, 0, hp=20_000, phys=1000, speed=200, class_id=1, pets=(pet,))
    p_b = _make_player(protocol, 201, 1, hp=50_000, phys=10, speed=50)
    eng = TurnEngine(protocol, _create_req(protocol, 7100, [p_a, p_b]), skills={}, items={}, max_rounds=20)

    damages_by_round: list[int] = []
    synergy_events_by_round: list[int] = []
    for r in range(1, 4):
        snap_before = eng.snapshot(0)
        assert snap_before.wuxing_synergy_active_this_round is (r == 3)
        eng.pending[101] = protocol.message("BattleAction", action_type=1, target_id=201)
        eng.pending[201] = protocol.message("BattleAction", action_type=3)
        turn = eng.resolve()
        syn_evs = [ev for ev in turn.events if ev.event_type == EVENT_WUXING_SYNERGY]
        synergy_events_by_round.append(len(syn_evs))
        if r == 3:
            assert syn_evs[0].value == WUXING_SYNERGY_BONUS_PCT
            assert syn_evs[0].source_id == 101
        hit = next(ev for ev in turn.events if ev.event_type == 3 and ev.source_id == 101)
        damages_by_round.append(hit.value)

    assert synergy_events_by_round == [0, 0, 1]
    assert damages_by_round[2] > damages_by_round[0]


def test_wuxing_tower_battle_scaling_sigils_and_settlement_echo():
    protocol = Protocol()
    tc = protocol.message(
        "WuxingTowerBattleContext",
        week_key=2026,
        run_id=7,
        floor=10,
        leyline_element=TOURNAMENT_LEYLINE_FIRE,
        active_sigil_ids=[101, 103, 203, 301, 302, 401, 403, 501, 502],
    )
    pet = _make_pet(protocol, 501, 101, pet_table_id=2, hp=4_000, phys=600, speed=180)
    p_a = _make_player(
        protocol, 101, 0, hp=10_000, mana=200, max_mana=1_000, phys=2_000, speed=200, crit=100,
        pets=(pet,), tower_context=tc,
    )
    p_a.base_attributes.health = 8_000
    monster = _make_monster(protocol, hp=100_000, phys=1_000, speed=50)
    req = _create_req(protocol, 7200, [p_a], match_mode=MATCH_MODE_PVE_SOLO)

    node = BattleNode(
        protocol,
        node_id=1,
        instance_id="battle-1",
        host="127.0.0.1",
        port=9001,
        ticket_secret=b"secret",
        now_ms=lambda: 10_000,
        monster_factory=lambda _r: [monster],
        skills={},
        items={},
        on_outcome=lambda *_: None,
    )
    assert not node.create(req).error_message.id
    room = node.rooms[7200]
    eng = room.engine

    # 1) 第 10 层怪物属性缩放：HP=177%, ATK=142%
    m_actor = next(a for a in eng.actors.values() if a.actor_type == 2)
    assert m_actor.max_health == 177_000
    assert m_actor.physical_attack == 1_420

    # 2) BattleStateS2C 透传 tower_context 且 501 厚土筑垒提供 12% 开局护盾(1200)
    snap = eng.snapshot(0, viewer=101)
    assert snap.tower_context.floor == 10 and snap.tower_context.run_id == 7
    assert eng.tournament_earth_shield_of(101) == 1_200

    # 3) 第 1 回合：203 回合初回复 5% HP(8000->8500)、301 回合初回复 8% MP(200->280)、403 暴击灼伤(封顶 1500)、502 反震
    eng.pending[101] = protocol.message("BattleAction", action_type=1, target_id=m_actor.actor_id)
    turn = eng.resolve()
    p_after = next(a for a in turn.state.actors if a.actor_id == 101)
    assert p_after.attributes.mana >= 280
    # 暴击触发 403 祝融烈焰额外灼伤段（单段封顶 1500）
    player_hits = [ev for ev in turn.events if ev.event_type == 3 and ev.source_id == 101]
    assert len(player_hits) >= 2
    assert player_hits[1].value == WUXING_FIRE_CRIT_BURN_CAP_PER_HIT

    # 4) 终局结算回传 WuxingTowerSettlementEcho
    room.proposed_outcome = 1
    result = node._result(room)
    echo = result.settlements[0].tower_settlement
    assert echo.week_key == 2026 and echo.run_id == 7 and echo.floor == 10 and echo.total_rounds >= 1


def test_custom_duel_rules_round_limit_polarity_ban_control_and_free_preset():
    protocol = Protocol()
    p_a = _make_player(protocol, 101, 0, hp=5000, phys=500, speed=200, skills=(201,))
    p_b = _make_player(protocol, 201, 1, hp=5000, phys=500, speed=100)

    # 非法回合上限被引擎拒绝
    bad_rules = protocol.message("CustomDuelRules", round_limit_override=7)
    with pytest.raises(BattleError):
        TurnEngine(
            protocol,
            _create_req(protocol, 7300, [p_a, p_b], match_mode=MATCH_MODE_PVP_CHALLENGE, custom_duel_rules=bad_rules),
            skills={},
            items={},
            max_rounds=20,
        )

    # 合法自定义规则：8 回合 + 物法易势 + 禁控 + 公平免费临时宠
    ctrl_buff = BattleBuff(buff_type=30, duration_rounds=1)  # STUN
    ctrl_skill = BattleSkill(base_damage=0, target_modes=(1,), effects=(950,))
    temp_pet = _make_pet(protocol, 0, 101, 1, competitive_temporary=True)
    p_a_free = _make_player(protocol, 101, 0, hp=5000, phys=500, speed=200, skills=(201,), pets=(temp_pet,))
    rules = protocol.message(
        "CustomDuelRules",
        use_free_preset=True,
        enable_polarity_rotation=True,
        round_limit_override=8,
        ban_control_skills=True,
        allow_public_spectate=True,
    )
    eng = TurnEngine(
        protocol,
        _create_req(protocol, 7301, [p_a_free, p_b], match_mode=MATCH_MODE_PVP_CHALLENGE, custom_duel_rules=rules),
        skills={201: ctrl_skill},
        items={},
        buffs={950: ctrl_buff},
        max_rounds=20,
    )
    snap1 = eng.snapshot(0)
    assert eng.max_rounds == 8 and snap1.round_limit == 8
    assert snap1.custom_duel_rules.ban_control_skills is True
    assert snap1.custom_duel_rules.allow_public_spectate is True
    assert snap1.active_polarity in (1, 2)
    initial_pol = snap1.active_polarity

    # 禁控校验：提交控制技能被 7004 拒绝
    ctrl_action = protocol.message("BattleAction", action_type=2, skill_table_id=201, target_id=201)
    assert eng.validate(eng.actors[101], ctrl_action) == 7004

    # 推进 2 回合，第 3 回合物法易势切换极性
    for _ in range(2):
        eng.pending[101] = protocol.message("BattleAction", action_type=3)
        eng.pending[201] = protocol.message("BattleAction", action_type=3)
        eng.resolve()
    snap3 = eng.snapshot(0)
    assert snap3.round_index == 3
    assert snap3.active_polarity != initial_pol
