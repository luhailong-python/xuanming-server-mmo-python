"""C++ f4947968a..da97bfb11 五行幻境 V2（RPC 585/586）、战斗引擎 V2 奇策共鸣与 BattleNode 指标加固回归测试。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables
from xuanming.nodes.battle import BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BOON_EARTH_CLUTCH_SHIELD,
    BOON_EARTH_DOUBLE_REFLECT,
    BOON_EARTH_SHIELD_STRIKE,
    BOON_FIRE_LOW_HP_FURY,
    BOON_FIRE_ROUND_END_BURN,
    BOON_FIRE_SHIELD_BLAST,
    BOON_METAL_BREAK_REFUND,
    BOON_METAL_CRIT_SPLASH,
    BOON_METAL_FIRST_PIERCE,
    BOON_ROUND_END_BURN_CAP,
    BOON_SHIELD_SHATTER_DAMAGE_CAP,
    BOON_WATER_EXTEND_CONTROL,
    BOON_WATER_FREE_FIRST_CAST,
    BOON_WATER_SKILL_ECHO,
    BOON_WOOD_DOT_SPREAD,
    BOON_WOOD_HEAL_EMPOWER,
    BOON_WOOD_PET_FOLLOW_UP,
    DEFAULT_FAIR_CHAR_PRESET_ID,
    DEFAULT_FAIR_PET_PRESET_ID,
    MATCH_MODE_PVE_SOLO,
    MATCH_MODE_PVP_CHALLENGE,
    REALM_BRANCH_ELITE,
    REALM_BRANCH_SPECIAL,
    REALM_EVENT_ENCOUNTER,
    REALM_EVENT_NONE,
    REALM_EVENT_REST,
    REALM_NODE_BOSS,
    REALM_NODE_ELITE,
    REALM_NODE_EVENT,
    REALM_NODE_NEED_BRANCH,
    REALM_NODE_NORMAL,
    REALM_NODE_REST,
    TOURNAMENT_LEYLINE_FIRE,
    TOURNAMENT_LEYLINE_WATER,
    WUXING_ELEMENT_EARTH,
    WUXING_ELEMENT_FIRE,
    WUXING_ELEMENT_METAL,
    WUXING_ELEMENT_NONE,
    WUXING_ELEMENT_WATER,
    WUXING_ELEMENT_WOOD,
    WUXING_REALM_BOONS,
    WUXING_REALM_V2_MAX_NODES,
    WUXING_REALM_V2_VERSION,
    BattleEffectOp,
    BattleStateKind,
    compute_active_resonance_element,
    compute_realm_boon_mask,
    dominant_boon_element,
    find_realm_boon,
    is_realm_combat_node_kind,
    is_valid_fair_char_preset_id,
    is_valid_fair_pet_preset_id,
    is_valid_realm_boon_id,
    is_valid_tower_or_boon_id,
    next_upgrade_boon_id,
    realm_boon_bit,
    realm_monster_attack_pct,
    realm_monster_health_pct,
    realm_node_default_kind,
    realm_node_dungeon_id,
    roll_realm_boon_offer,
    roll_realm_encounter_boon,
    wuxing_week_key,
)
from xuanming.nodes.battle_tables import BattleBuff, BattleSkill
from xuanming.nodes.scene_season import (
    SceneSeason,
    realm_leaderboard_meta_key,
    realm_leaderboard_zset_key,
)
from xuanming.protocol import Protocol


# ==============================================================================
# 辅助构造器
# ==============================================================================


class _StubRows:
    def __init__(self, protocol: Protocol) -> None:
        self._protocol = protocol

    def assemble(self, snapshot):
        copy = self._protocol.message("player_database")
        copy.CopyFrom(snapshot)
        return copy


class _StubScene:
    def __init__(self, protocol: Protocol) -> None:
        self.protocol = protocol
        self.rows = _StubRows(protocol)
        self.registered: dict[tuple[str, str], tuple[object, bool]] = {}
        self.commit_count = 0
        self.frozen_ids: set[int] = set()
        self.battle = SimpleNamespace(frozen=lambda pid: pid in self.frozen_ids)

    def register(self, service: str, method: str, handler, *, mutating: bool = False) -> None:
        route = next(
            (
                r for r in self.protocol.routes.values()
                if r.service == service and r.method == method
            ),
            None,
        )
        assert route is not None, f"route not found in schema.pb: {service}/{method}"
        self.registered[(service, method)] = (handler, mutating)

    def mutable(self, player) -> bool:
        return player.player_id not in self.frozen_ids

    async def commit_player_data(self, player, data, *, on_committed=None) -> None:
        player.snapshot.CopyFrom(data)
        self.commit_count += 1
        if on_committed is not None:
            result = on_committed(player.snapshot)
            if asyncio.iscoroutine(result):
                await result


def _make_scene_player(protocol: Protocol, player_id: int = 99001, name: str = "太清道友"):
    data = protocol.message("player_database", player_id=player_id)
    data.level_component.level = 45
    data.profile_component.name = name
    data.season_component.SetInParent()
    return SimpleNamespace(player_id=player_id, snapshot=data)


def _make_player(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    hp: int = 10_000,
    max_hp: int | None = None,
    mana: int = 1_000,
    max_mana: int | None = None,
    speed: int = 200,
    phys: int = 1_000,
    magic: int = 1_000,
    defense: int = 0,
    level: int = 40,
    class_id: int = 1,
    crit: int = 0,
    skills: tuple[int, ...] = (),
    pets: tuple = (),
    tower_context=None,
    reflect_rate: int = 0,
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=f"P{player_id}",
        class_id=class_id,
        level=level,
        max_health=max_hp if max_hp is not None else hp,
        max_mana=max_mana if max_mana is not None else mana,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        team_index=team_index,
        skill_table_ids=list(skills),
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = mana
    snap.base_attributes.speed = speed
    snap.base_attributes.critchance = crit
    snap.combat.reflect_rate = reflect_rate
    snap.routing.session_id = (7 << 17) + player_id
    snap.routing.gate_node_id = 7
    snap.routing.gate_instance_id = "gate-1"
    snap.routing.scene_node_id = 10
    snap.routing.scene_instance_id = "scene-1"
    snap.routing.zone_id = 1
    for pet in pets:
        snap.pets.add().CopyFrom(pet)
    if tower_context is not None:
        snap.tower_context.CopyFrom(tower_context)
    return snap


def _make_pet(
    protocol: Protocol,
    pet_id: int,
    owner_id: int,
    pet_table_id: int = 1,
    *,
    hp: int = 5_000,
    phys: int = 800,
    magic: int = 400,
    speed: int = 150,
    competitive_temporary: bool = False,
):
    snap = protocol.message(
        "BattlePetSnapshot",
        pet_id=pet_id,
        owner_player_id=owner_id,
        pet_table_id=pet_table_id,
        pet_name="玄灵宠",
        level=40,
        max_health=hp,
        max_mana=500,
        physical_attack=phys,
        magic_attack=magic,
        defense=0,
        competitive_temporary=competitive_temporary,
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = 500
    snap.base_attributes.speed = speed
    return snap


def _make_monster(
    protocol: Protocol,
    *,
    hp: int = 5_000,
    phys: int = 300,
    magic: int = 200,
    defense: int = 0,
    speed: int = 50,
    table_id: int = 101,
    is_boss: bool = False,
    name: str = "秘境妖将",
):
    m = protocol.message(
        "BattleActorState",
        name=name,
        level=40,
        max_health=hp,
        max_mana=500,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        monster_table_id=table_id,
        is_boss=is_boss,
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
    match_mode: int = MATCH_MODE_PVE_SOLO,
    battle_config_id: int = 101,
    seed: int = 42,
    spectate_visibility: int = 2,
    spectate_chat_enabled: bool = False,
    table_fingerprint: str = "",
):
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        match_mode=match_mode,
        battle_config_id=battle_config_id,
        seed=seed,
        created_at_ms=10_000,
        deadline_ms=1_000_000,
        admit_before_ms=100_000,
        placement_operation_id=f"op-{battle_id}",
        target_instance_id="battle-1",
        required_physical_region=1,
        outcome_protocol_version=1,
        spectate_visibility=spectate_visibility,
        spectate_chat_enabled=spectate_chat_enabled,
        table_fingerprint=table_fingerprint,
    )
    for p in players:
        req.players.add().CopyFrom(p)
    return req


def _metric_value(text: str, series: str) -> int:
    prefix = f"{series} "
    for line in text.splitlines():
        if line.startswith(prefix):
            return int(line[len(prefix):].strip())
    raise AssertionError(f"metric series not found: {series}\n{text}")


# ==============================================================================
# 1. 五行幻境 V2 纯规则对齐测试（对照 wuxing_expansion_rules.h）
# ==============================================================================


def test_wuxing_realm_v2_pure_rules_match_cpp_header():
    assert len(WUXING_REALM_BOONS) == 15
    assert WUXING_REALM_V2_VERSION == 2
    assert WUXING_REALM_V2_MAX_NODES == 6
    assert is_valid_fair_char_preset_id(DEFAULT_FAIR_CHAR_PRESET_ID)
    assert is_valid_fair_pet_preset_id(DEFAULT_FAIR_PET_PRESET_ID)
    assert [is_valid_fair_char_preset_id(x) for x in (100, 101, 102, 103, 104)] == [False, True, True, True, False]
    assert [is_valid_fair_pet_preset_id(x) for x in (200, 201, 202, 203, 204)] == [False, True, True, True, False]

    # 15 枚奇策位图互不重叠且号段与 V1 101..503 物理隔离
    mask_all = compute_realm_boon_mask(b.id for b in WUXING_REALM_BOONS)
    assert mask_all == (1 << 15) - 1
    assert is_valid_realm_boon_id(BOON_METAL_BREAK_REFUND) and not is_valid_realm_boon_id(101)
    assert is_valid_tower_or_boon_id(101) and is_valid_tower_or_boon_id(643) and not is_valid_tower_or_boon_id(999)
    assert realm_boon_bit(BOON_EARTH_SHIELD_STRIKE) == (1 << 14)
    assert find_realm_boon(BOON_FIRE_ROUND_END_BURN).element == WUXING_ELEMENT_FIRE

    # 3 同系共鸣 & 主导五行 & 淬炼升品
    assert compute_active_resonance_element([601, 602]) == WUXING_ELEMENT_NONE
    assert compute_active_resonance_element([631, 632, 633, 601]) == WUXING_ELEMENT_FIRE
    assert compute_active_resonance_element([601, 602, 603, 631, 632, 633]) == WUXING_ELEMENT_METAL
    assert dominant_boon_element([631, 632, 601]) == WUXING_ELEMENT_FIRE
    assert dominant_boon_element([631, 632, 633, 601]) == WUXING_ELEMENT_METAL  # 已有满共鸣优先选未满最高
    assert next_upgrade_boon_id(601, [601]) == 602
    assert next_upgrade_boon_id(601, [601, 602]) == 603
    assert next_upgrade_boon_id(603, [603]) == 0

    # 节点默认类型、副本映射与怪物缩放
    assert [realm_node_default_kind(i) for i in range(1, 7)] == [
        REALM_NODE_NORMAL, REALM_NODE_NEED_BRANCH, REALM_NODE_NORMAL,
        REALM_NODE_NEED_BRANCH, REALM_NODE_ELITE, REALM_NODE_BOSS,
    ]
    assert is_realm_combat_node_kind(REALM_NODE_NORMAL)
    assert is_realm_combat_node_kind(REALM_NODE_ELITE)
    assert is_realm_combat_node_kind(REALM_NODE_BOSS)
    assert not is_realm_combat_node_kind(REALM_NODE_NEED_BRANCH)
    assert not is_realm_combat_node_kind(REALM_NODE_EVENT)
    assert not is_realm_combat_node_kind(REALM_NODE_REST)

    assert realm_node_dungeon_id(1, REALM_NODE_NORMAL) == 101
    assert realm_node_dungeon_id(2, REALM_NODE_ELITE) == 103
    assert realm_node_dungeon_id(3, REALM_NODE_NORMAL) == 105
    assert realm_node_dungeon_id(5, REALM_NODE_ELITE) == 105
    assert realm_node_dungeon_id(6, REALM_NODE_BOSS) == 107

    # 5 层精英：HP = 82 + 4*10 + 16 = 138%, ATK = 82 + 4*7 + 10 = 120%
    assert realm_monster_health_pct(5, REALM_NODE_ELITE) == 138
    assert realm_monster_attack_pct(5, REALM_NODE_ELITE) == 120
    # 6 层首领：HP = 82 + 5*10 + 16 = 148%, ATK = 82 + 5*7 + 8 = 125%
    assert realm_monster_health_pct(6, REALM_NODE_BOSS) == 148
    assert realm_monster_attack_pct(6, REALM_NODE_BOSS) == 125

    # 确定性抽策（第 1 关不含仙品、精英保底灵/仙、同源寻策偏向目标五行）
    o1 = roll_realm_boon_offer(202648, 1, 1, REALM_NODE_NORMAL, 0, [])
    assert o1 == roll_realm_boon_offer(202648, 1, 1, REALM_NODE_NORMAL, 0, [])
    assert len(o1) == 3 and len(set(o1)) == 3
    assert all(find_realm_boon(bid).rarity < 3 for bid in o1)

    o_elite = roll_realm_boon_offer(202648, 2, 5, REALM_NODE_ELITE, 0, [])
    assert len(o_elite) == 3 and any(find_realm_boon(bid).rarity >= 2 for bid in o_elite)

    o_pref = roll_realm_boon_offer(202648, 3, 4, REALM_NODE_ELITE, 1, [BOON_FIRE_SHIELD_BLAST], WUXING_ELEMENT_FIRE)
    fire_in_offer = [bid for bid in o_pref if find_realm_boon(bid).element == WUXING_ELEMENT_FIRE]
    assert len(fire_in_offer) == 2

    enc = roll_realm_encounter_boon(202648, 7, [])
    assert enc == roll_realm_encounter_boon(202648, 7, [])
    assert find_realm_boon(enc).rarity == 2


# ==============================================================================
# 2. SceneSeason V2 全流程：585 分岔 / 586 事件 / 公平预设 / 跨周保留 / 双榜防倒退
# ==============================================================================


def test_scene_season_wuxing_realm_v2_full_flow_branch_events_cross_week_and_leaderboards():
    protocol = Protocol()
    assert protocol.message_ids["SceneSeasonClientPlayerChooseWuxingRealmBranch"] == 585
    assert protocol.message_ids["SceneSeasonClientPlayerResolveWuxingRealmEvent"] == 586

    # 校验水平扩展 Redis Cluster 键规范
    assert realm_leaderboard_zset_key(202648, False) == "wuxing:realm:lb:{202648}:self"
    assert realm_leaderboard_meta_key(202648, False) == "wuxing:realm:lb_meta:{202648}:self"
    assert realm_leaderboard_zset_key(202648, True) == "wuxing:realm:lb:{202648}:preset"
    assert realm_leaderboard_meta_key(202648, True) == "wuxing:realm:lb_meta:{202648}:preset"

    now_box = [1_760_000_000_000]
    one_week_ms = 7 * 24 * 3600 * 1000
    scene = _StubScene(protocol)
    tables = ConfigTables()
    season = SceneSeason(scene, tables, clock=lambda: now_box[0])
    season.register()
    assert ("SceneSeasonClientPlayer", "ChooseWuxingRealmBranch") in scene.registered
    assert ("SceneSeasonClientPlayer", "ResolveWuxingRealmEvent") in scene.registered

    player = _make_scene_player(protocol, 99001, "青云道友")

    async def scenario():
        # 1. 非法公平预设拒收
        bad_start = await season.start_wuxing_tower_run(
            None, player, None,
            protocol.message("StartWuxingTowerRunRequest", use_free_preset=True, character_preset_id=999),
        )
        assert bad_start.error_message.id == 1005

        # 2. 开启公平 3x3 试炼 6 关 V2 轮次（人物 102 + 灵宠 202）
        start_req = protocol.message(
            "StartWuxingTowerRunRequest", use_free_preset=True, character_preset_id=102, pet_preset_id=202,
        )
        started = await season.start_wuxing_tower_run(None, player, None, start_req)
        assert not started.error_message.id
        assert started.state.run_active is True
        assert started.state.realm_version == WUXING_REALM_V2_VERSION
        assert started.state.max_floors == WUXING_REALM_V2_MAX_NODES
        assert started.state.use_free_preset is True
        assert started.state.character_preset_id == 102
        assert started.state.pet_preset_id == 202
        assert started.state.current_floor == 1
        assert started.state.current_node_kind == REALM_NODE_NORMAL
        run_id = started.state.run_id

        async def win_node(expected_floor: int, expected_dungeon: int, rounds: int):
            pdata = scene.rows.assemble(player.snapshot)
            err, ctx, char_id, pet_id = season.prepare_wuxing_tower_context(pdata, expected_dungeon, now_ms_val=now_box[0])
            assert err == 0 and ctx is not None
            assert ctx.floor == expected_floor
            assert ctx.realm_version == 2
            assert ctx.use_free_preset is True
            assert char_id == 102 and pet_id == 202

            settle = protocol.message(
                "BattleSettlementData",
                battle_id=92000 + expected_floor,
                player_id=player.player_id,
                player_team_index=0,
                match_mode=MATCH_MODE_PVE_SOLO,
                outcome=1,
                total_rounds=rounds,
            )
            settle.tower_settlement.week_key = ctx.week_key
            settle.tower_settlement.run_id = ctx.run_id
            settle.tower_settlement.floor = expected_floor
            settle.tower_settlement.total_rounds = rounds
            settle.tower_settlement.realm_version = 2
            settle.tower_settlement.node_kind = ctx.node_kind
            settle.tower_settlement.use_free_preset = True

            assert season.settle_wuxing_tower_battle(pdata, settle, now_ms_val=now_box[0]) is True
            # 重复结算严格幂等跳过
            assert season.settle_wuxing_tower_battle(pdata, settle, now_ms_val=now_box[0]) is False
            await scene.commit_player_data(player, pdata)

        # 第 1 关胜利 -> 3 选 1 奇策 -> 写公平榜不污染本人榜 -> 选策后推进第 2 关（待选分岔）
        await win_node(1, 101, 3)
        st1 = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        assert len(st1.state.pending_offer_sigil_ids) == 3
        assert all(is_valid_realm_boon_id(x) for x in st1.state.pending_offer_sigil_ids)
        assert len(st1.state.weekly_leaderboard) == 0
        assert len(st1.state.weekly_leaderboard_preset) == 1

        pick1 = await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=run_id, sigil_id=st1.state.pending_offer_sigil_ids[0]),
        )
        assert not pick1.error_message.id
        assert pick1.state.current_floor == 2
        assert pick1.state.current_node_kind == REALM_NODE_NEED_BRANCH

        # 未选分岔前禁止直接进战斗
        err_blocked, _, _, _ = season.prepare_wuxing_tower_context(scene.rows.assemble(player.snapshot), 103)
        assert err_blocked == 1006

        # 585 非法参数 / 错误 run_id 校验
        bad_br = await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=9999, branch=REALM_BRANCH_SPECIAL),
        )
        assert bad_br.error_message.id == 1005
        bad_br2 = await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=run_id, branch=9),
        )
        assert bad_br2.error_message.id == 1005

        # 585 第 2 关选右路「奇遇事件」
        br2 = await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=run_id, branch=REALM_BRANCH_SPECIAL),
        )
        assert not br2.error_message.id
        assert br2.state.current_node_kind == REALM_NODE_EVENT
        assert br2.state.pending_event_kind == REALM_EVENT_ENCOUNTER
        assert br2.state.route_choice_mask == 1
        # 已离开 NEED_BRANCH 状态重复选分岔被拒
        dup_br2 = await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=run_id, branch=REALM_BRANCH_ELITE),
        )
        assert dup_br2.error_message.id == 1005

        # 586 奇遇选项 1「道缘寻策」直接领灵品奇策并推进至第 3 关
        bad_ev = await season.resolve_wuxing_realm_event(
            None, player, None,
            protocol.message("ResolveWuxingRealmEventRequest", expected_run_id=run_id, option_index=3),
        )
        assert bad_ev.error_message.id == 1005

        ev2 = await season.resolve_wuxing_realm_event(
            None, player, None,
            protocol.message("ResolveWuxingRealmEventRequest", expected_run_id=run_id, option_index=1),
        )
        assert not ev2.error_message.id
        assert is_valid_realm_boon_id(ev2.granted_boon_id)
        assert ev2.state.current_floor == 3
        assert ev2.state.current_node_kind == REALM_NODE_NORMAL
        assert ev2.state.pending_event_kind == REALM_EVENT_NONE
        assert len(ev2.state.active_sigil_ids) == 2

        # 跨周一 05:00 进入下一周：在途进度必须完整保留，本周首通位图归零！
        now_box[0] += one_week_ms
        st_next = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        assert st_next.state.run_active is True
        assert st_next.state.current_floor == 3
        assert len(st_next.state.active_sigil_ids) == 2
        assert st_next.state.weekly_cleared_mask == 0

        # 跨周继续打通第 3 关 -> 选策 -> 第 4 关右路休整「奇策淬炼升品」 -> 第 5 关精英 -> 第 6 关首领全通
        await win_node(3, 105, 2)
        st3 = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        pick3 = await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=run_id, sigil_id=st3.state.pending_offer_sigil_ids[0]),
        )
        assert not pick3.error_message.id
        assert pick3.state.current_floor == 4
        assert pick3.state.current_node_kind == REALM_NODE_NEED_BRANCH

        br4 = await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=run_id, branch=REALM_BRANCH_SPECIAL),
        )
        assert not br4.error_message.id
        assert br4.state.current_node_kind == REALM_NODE_REST
        assert br4.state.pending_event_kind == REALM_EVENT_REST
        assert br4.state.route_choice_mask == 0b11

        ev4 = await season.resolve_wuxing_realm_event(
            None, player, None,
            protocol.message("ResolveWuxingRealmEventRequest", expected_run_id=run_id, option_index=1),
        )
        assert not ev4.error_message.id
        assert is_valid_realm_boon_id(ev4.granted_boon_id)
        assert ev4.state.current_floor == 5
        assert ev4.state.current_node_kind == REALM_NODE_ELITE

        await win_node(5, 105, 3)
        st5 = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        pick5 = await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=run_id, sigil_id=st5.state.pending_offer_sigil_ids[0]),
        )
        assert not pick5.error_message.id
        assert pick5.state.current_floor == 6
        assert pick5.state.current_node_kind == REALM_NODE_BOSS

        await win_node(6, 107, 4)
        st_done = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        assert st_done.state.run_active is False
        assert st_done.state.best_rounds_preset == 12  # 3 + 2 + 3 + 4
        assert len(st_done.state.weekly_leaderboard_preset) == 0  # 上周旧局绝不污染新周榜

        # 新周开局验证：单调榜防倒退（第 2 关成绩与奇策不被重开第 1 关覆盖）+ 第 2/4 关选项 2（五行悟道 / 同源寻策）
        nw_start = await season.start_wuxing_tower_run(None, player, None, start_req)
        assert not nw_start.error_message.id
        nw_run = nw_start.state.run_id
        await win_node(1, 101, 2)
        st_nw1 = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        pick_nw1 = await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=nw_run, sigil_id=st_nw1.state.pending_offer_sigil_ids[0]),
        )
        assert not pick_nw1.error_message.id
        # 左路精英战
        br_nw2 = await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=nw_run, branch=REALM_BRANCH_ELITE),
        )
        assert not br_nw2.error_message.id and br_nw2.state.current_node_kind == REALM_NODE_ELITE
        await win_node(2, 103, 2)
        st_nw2 = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        assert len(st_nw2.state.weekly_leaderboard_preset) == 1
        assert st_nw2.state.weekly_leaderboard_preset[0].best_floor == 2
        saved_best_boon = st_nw2.state.weekly_leaderboard_preset[0].sigil_ids[0]

        # 同周重开仅打通第 1 关（0 奇策），榜单仍保留第 2 关成绩与已选奇策
        rerun = await season.start_wuxing_tower_run(None, player, None, start_req)
        assert not rerun.error_message.id
        await win_node(1, 101, 1)
        st_rerun = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        assert st_rerun.state.weekly_leaderboard_preset[0].best_floor == 2
        assert st_rerun.state.weekly_leaderboard_preset[0].sigil_ids[0] == saved_best_boon

        # 验证 586 选项 2（第 2 关五行悟道三选一 & 第 4 关同源寻策偏向共鸣三选一）
        run3 = rerun.state.run_id
        p_o1 = await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=run3, sigil_id=st_rerun.state.pending_offer_sigil_ids[0]),
        )
        assert not p_o1.error_message.id
        await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=run3, branch=REALM_BRANCH_SPECIAL),
        )
        ev2_opt2 = await season.resolve_wuxing_realm_event(
            None, player, None,
            protocol.message("ResolveWuxingRealmEventRequest", expected_run_id=run3, option_index=2),
        )
        assert not ev2_opt2.error_message.id
        assert len(ev2_opt2.state.pending_offer_sigil_ids) == 3
        p_o2 = await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=run3, sigil_id=ev2_opt2.state.pending_offer_sigil_ids[0]),
        )
        assert not p_o2.error_message.id and p_o2.state.current_floor == 3

        await win_node(3, 105, 2)
        st_r3 = season.get_wuxing_tower_state(None, player, None, protocol.message("GetWuxingTowerStateRequest"))
        await season.pick_wuxing_tower_sigil(
            None, player, None,
            protocol.message("PickWuxingTowerSigilRequest", expected_run_id=run3, sigil_id=st_r3.state.pending_offer_sigil_ids[0]),
        )
        await season.choose_wuxing_realm_branch(
            None, player, None,
            protocol.message("ChooseWuxingRealmBranchRequest", expected_run_id=run3, branch=REALM_BRANCH_SPECIAL),
        )
        ev4_opt2 = await season.resolve_wuxing_realm_event(
            None, player, None,
            protocol.message("ResolveWuxingRealmEventRequest", expected_run_id=run3, option_index=2),
        )
        assert not ev4_opt2.error_message.id
        assert len(ev4_opt2.state.pending_offer_sigil_ids) == 3

        # 验证 V2 奇策 ID（601..643）可合法保存到战报配装收藏夹
        bm_req = protocol.message(
            "SaveBattleBuildBookmarkRequest",
            bookmark=protocol.message(
                "SavedBattleBuildBookmarkView",
                title="离火三共鸣",
                formation_id=5,
                wuxing_sigil_ids=[BOON_FIRE_SHIELD_BLAST, BOON_FIRE_LOW_HP_FURY, BOON_FIRE_ROUND_END_BURN, 9999],
            ),
        )
        bm_resp = await season.save_battle_build_bookmark(None, player, None, bm_req)
        assert not bm_resp.error_message.id
        assert list(bm_resp.saved.wuxing_sigil_ids) == [
            BOON_FIRE_SHIELD_BLAST, BOON_FIRE_LOW_HP_FURY, BOON_FIRE_ROUND_END_BURN,
        ]

    asyncio.run(scenario())


# ==============================================================================
# 3. 战斗引擎 V2 奇策、共鸣、递归守卫与结算回传
# ==============================================================================


def test_battle_engine_wuxing_realm_v2_boons_resonance_and_settlement_echo():
    protocol = Protocol()
    cfg_skill = BattleSkill(
        base_damage=400,
        mana_percent=0.20,
        battle_model=1,
        caster_mask=1,
        ability_kind=1,
        target_team=1,
        max_targets=1,
        effects=(980010,),
    )
    cfg_dmg_buff = BattleBuff(battle_op=BattleEffectOp.DAMAGE, flat_value=400.0)

    tc = protocol.message(
        "WuxingTowerBattleContext",
        week_key=202648,
        run_id=19,
        floor=5,
        realm_version=WUXING_REALM_V2_VERSION,
        node_kind=REALM_NODE_ELITE,
        use_free_preset=True,
        leyline_element=TOURNAMENT_LEYLINE_FIRE,
        active_sigil_ids=[
            BOON_FIRE_SHIELD_BLAST,
            BOON_FIRE_LOW_HP_FURY,
            BOON_FIRE_ROUND_END_BURN,
            BOON_WATER_FREE_FIRST_CAST,
            BOON_EARTH_SHIELD_STRIKE,
        ],
    )
    p = _make_player(
        protocol, 30001, 0, hp=20_000, mana=1_000, phys=500, magic=500, speed=200,
        skills=(98001,), tower_context=tc,
    )
    boss = _make_monster(protocol, hp=5_000, phys=200, speed=50, is_boss=True, name="玄火妖王")
    minion = _make_monster(protocol, hp=5_000, phys=100, speed=40, is_boss=False, name="玄火妖兵")
    req = _create_req(protocol, 6801, [p], match_mode=MATCH_MODE_PVE_SOLO, battle_config_id=105)

    node = BattleNode(
        protocol, node_id=1, instance_id="battle-1", host="127.0.0.1", port=9001,
        ticket_secret=b"secret", now_ms=lambda: 10_000,
        monster_factory=lambda _r: [boss, minion],
        skills={98001: cfg_skill}, items={}, buffs={980010: cfg_dmg_buff},
        on_outcome=lambda *_: None,
    )
    assert not node.create(req).error_message.id
    room = node.rooms[6801]
    eng = room.engine

    assert eng.uses_free_preset is True
    # 643 坤元借力赋予 18% 最大气血开局护盾（20000 * 18% = 3600）
    assert eng.tournament_earth_shield_of(30001) == 3600

    init_state = eng.snapshot(0, viewer=30001)
    assert init_state.tower_context.realm_version == 2
    assert init_state.tower_context.node_kind == REALM_NODE_ELITE
    assert init_state.tower_context.active_resonance_element == WUXING_ELEMENT_FIRE

    boss_actor = next(a for a in eng.actors.values() if a.team_index == 1 and a.is_boss)
    minion_actor = next(a for a in eng.actors.values() if a.team_index == 1 and not a.is_boss)
    # 5 层精英气血倍率：138%（5000 -> 6900）
    assert boss_actor.max_health == 6900

    # 回合 1：施放耗蓝 200 的技能 98001 -> 621 润脉免耗使蓝量保持 1000；3 火共鸣溅射小怪；回合末 633 离火燃魂灼伤最高血敌人
    eng.pending[30001] = protocol.message("BattleAction", action_type=2, skill_table_id=98001, target_id=boss_actor.actor_id)
    r1 = eng.resolve()
    p_after = next(a for a in r1.state.actors if a.actor_id == 30001)
    assert p_after.attributes.mana == 1000
    assert boss_actor.attributes.health < 6900
    assert minion_actor.attributes.health < 6900  # 3 火共鸣溅射 15% 或 633 灼伤

    # 结算回传携带 V2 字段
    room.proposed_outcome = 1
    result = node._result(room)
    settle = result.settlements[0]
    assert settle.use_free_preset is True
    assert settle.tower_settlement.realm_version == 2
    assert settle.tower_settlement.node_kind == REALM_NODE_ELITE
    assert settle.tower_settlement.use_free_preset is True

    # 非战斗节点类型（如 4=奇遇事件）必须被 TurnEngine 拒绝建房
    bad_req = protocol.message("CreateBattleRequest")
    bad_req.CopyFrom(req)
    bad_req.players[0].tower_context.node_kind = REALM_NODE_EVENT
    with pytest.raises(BattleError):
        TurnEngine(protocol, bad_req, monsters=[boss], skills={98001: cfg_skill}, items={}, buffs={980010: cfg_dmg_buff}, max_rounds=12)


def test_battle_engine_wuxing_v2_all_core_boons_and_resonances():
    protocol = Protocol()

    # 1) 金系 3 奇策（601 破盾返元 + 602 绝锋连斩 + 603 破甲先锋）+ 3 金共鸣斩杀 + 631 焚盾爆炎 + 613 灵宠协战
    tc_metal = protocol.message(
        "WuxingTowerBattleContext",
        week_key=202648, run_id=21, floor=1, realm_version=2, node_kind=REALM_NODE_NORMAL,
        active_sigil_ids=[
            BOON_METAL_BREAK_REFUND,
            BOON_METAL_CRIT_SPLASH,
            BOON_METAL_FIRST_PIERCE,
            BOON_FIRE_SHIELD_BLAST,
            BOON_WOOD_PET_FOLLOW_UP,
        ],
    )
    pet = _make_pet(protocol, 501, 101, phys=1_000, speed=150)
    p = _make_player(protocol, 101, 0, hp=10_000, mana=200, max_mana=1_000, phys=2_000, crit=100, speed=200, pets=(pet,), tower_context=tc_metal)
    m1 = _make_monster(protocol, hp=5_000, phys=100, speed=40, name="盾妖")
    m2 = _make_monster(protocol, hp=5_000, phys=100, speed=30, name="旁妖")
    eng = TurnEngine(
        protocol, _create_req(protocol, 6810, [p]),
        monsters=[m1, m2], skills={}, items={}, max_rounds=12,
    )
    assert eng.has_realm_resonance(WUXING_ELEMENT_METAL)
    m1_actor, m2_actor = [a for a in eng.actors.values() if a.team_index == 1]
    # 给 m1 挂 500 护盾以便一击击碎触发 601 + 631
    eng.opening_shield_by_actor[m1_actor.actor_id] = 500
    eng.pending[101] = protocol.message("BattleAction", action_type=1, target_id=m1_actor.actor_id)
    eng.resolve()
    # 601 破盾返还 50% 最大法力（200 -> 700）
    assert eng.actors[101].attributes.mana >= 700
    # 631 焚盾爆炎 + 602 暴击溅射对 m2 造成额外真伤
    assert m2_actor.attributes.health < m2_actor.max_health

    # 3 金共鸣斩杀（非首领 <= 20% 血量受击直接斩杀）
    m2_actor.attributes.health = max(1, (m2_actor.max_health * 19) // 100)
    eng.actors[101].physical_attack = 10
    eng.pending[101] = protocol.message("BattleAction", action_type=1, target_id=m2_actor.actor_id)
    eng.resolve()
    assert m2_actor.is_dead is True

    # 2) 水系 3 奇策（621 免耗 + 622 延控/免疫易伤 + 623 第 3 招连发）+ 3 水共鸣（冷却额外 -1 & 回蓝 +30%）
    ctrl_buff = BattleBuff(battle_op=BattleEffectOp.CONTROL, state_kind=BattleStateKind.FREEZE, config_duration_rounds=1, application_chance=1.0)
    ctrl_skill = BattleSkill(
        base_damage=0, mana_percent=0.10, cooldown_rounds=3, battle_model=1,
        caster_mask=1, ability_kind=1, target_team=1, max_targets=1, effects=(9901,),
    )
    atk_skill = BattleSkill(
        base_damage=300, mana_percent=0.05, battle_model=1,
        caster_mask=1, ability_kind=1, target_team=1, max_targets=1, effects=(9902,),
    )
    atk_buff = BattleBuff(battle_op=BattleEffectOp.DAMAGE, flat_value=300.0)
    tc_water = protocol.message(
        "WuxingTowerBattleContext",
        week_key=202648, run_id=22, floor=1, realm_version=2, node_kind=REALM_NODE_NORMAL,
        leyline_element=TOURNAMENT_LEYLINE_WATER,
        active_sigil_ids=[BOON_WATER_FREE_FIRST_CAST, BOON_WATER_EXTEND_CONTROL, BOON_WATER_SKILL_ECHO],
    )
    p_w = _make_player(protocol, 102, 0, hp=10_000, mana=500, max_mana=1_000, magic=500, speed=200, skills=(9911, 9912), tower_context=tc_water)
    m_w = _make_monster(protocol, hp=50_000, phys=10, speed=40)
    eng_w = TurnEngine(
        protocol, _create_req(protocol, 6811, [p_w]),
        monsters=[m_w], skills={9911: ctrl_skill, 9912: atk_skill}, items={},
        buffs={9901: ctrl_buff, 9902: atk_buff}, max_rounds=12,
    )
    assert eng_w.has_realm_resonance(WUXING_ELEMENT_WATER)
    mw_actor = next(a for a in eng_w.actors.values() if a.team_index == 1)
    # 回合 1：水灵脉回蓝 5%(50) × 3 水共鸣 130% = 65 -> 565；621 首招免耗；622 延控 1+1=2 回合（当回合 applied_round 不扣减）；冷却 3+1=4 回合末 3 水共鸣 -2 剩 2
    eng_w.pending[102] = protocol.message("BattleAction", action_type=2, skill_table_id=9911, target_id=mw_actor.actor_id)
    eng_w.resolve()
    assert eng_w.actors[102].attributes.mana == 565
    assert eng_w.actors[102].skill_cooldown_rounds[9911] == 2
    assert (
        any(b.buff_table_id == 9901 and b.remain_rounds == 2 for b in mw_actor.buffs)
        or eng_w.boon_runtime.vulnerable_until_round.get(mw_actor.actor_id, 0) == 3
    )

    # 622 免控触发 2 回合 18% 易伤验证（先清空上回合冰冻，挂免控护阵）
    mw_actor.ClearField("buffs")
    imm_buff = BattleBuff(battle_op=BattleEffectOp.CONTROL_IMMUNITY, config_duration_rounds=2, hit_limit=1)
    eng_w.effects.rules[9909] = imm_buff
    mw_actor.buffs.add(buff_id=999901, buff_table_id=9909, caster_id=mw_actor.actor_id, layer=1, remain_rounds=2, remaining_hits=1)
    eng_w.actors[102].skill_cooldown_rounds[9911] = 0
    eng_w.boon_runtime.vulnerable_until_round.clear()
    eng_w.pending[102] = protocol.message("BattleAction", action_type=2, skill_table_id=9911, target_id=mw_actor.actor_id)
    eng_w.resolve()
    assert eng_w.boon_runtime.vulnerable_until_round.get(mw_actor.actor_id, 0) == 4

    # 连续 3 次进攻技能验证 623 镜花水月第 3 招连发 55% 额外伤害事件
    echo_hits = []
    for _ in range(3):
        eng_w.pending[102] = protocol.message("BattleAction", action_type=2, skill_table_id=9912, target_id=mw_actor.actor_id)
        t = eng_w.resolve()
        dmg_evs = [ev for ev in t.events if ev.event_type == 3 and ev.source_id == 102 and ev.target_id == mw_actor.actor_id]
        echo_hits.append(len(dmg_evs))
    assert echo_hits == [1, 1, 2]

    # 3) 土系 3 奇策（641 倍反破甲 + 642 绝境磐石 + 643 坤元借力）+ 3 土共鸣（+10% 护盾 & 护盾承伤亦反震）
    tc_earth = protocol.message(
        "WuxingTowerBattleContext",
        week_key=202648, run_id=23, floor=1, realm_version=2, node_kind=REALM_NODE_NORMAL,
        active_sigil_ids=[BOON_EARTH_DOUBLE_REFLECT, BOON_EARTH_CLUTCH_SHIELD, BOON_EARTH_SHIELD_STRIKE],
    )
    p_e = _make_player(protocol, 103, 0, hp=4_000, max_hp=10_000, phys=100, speed=40, tower_context=tc_earth)
    m_e = _make_monster(protocol, hp=20_000, phys=3_000, speed=200)
    eng_e = TurnEngine(protocol, _create_req(protocol, 6812, [p_e]), monsters=[m_e], skills={}, items={}, max_rounds=12)
    assert eng_e.has_realm_resonance(WUXING_ELEMENT_EARTH)
    # 643(+18%) + 3土共鸣(+10%) = 28% 开局护盾（2800）
    assert eng_e.tournament_earth_shield_of(103) == 2800
    me_actor = next(a for a in eng_e.actors.values() if a.team_index == 1)
    eng_e.pending[103] = protocol.message("BattleAction", action_type=3)
    eng_e.pending[me_actor.actor_id] = protocol.message("BattleAction", action_type=1, target_id=103)
    t_e = eng_e.resolve()
    # 3 土共鸣护盾承伤亦反震 + 641 破甲标记
    assert me_actor.attributes.health < me_actor.max_health
    assert eng_e.boon_runtime.armor_broken_until_round.get(me_actor.actor_id, 0) >= 3
    assert any(ev.event_type == 3 and ev.source_id == 103 and ev.target_id == me_actor.actor_id for ev in t_e.events)

    # 4) 木系 3 奇策（611 传染 + 612 治疗蓄势 + 613 灵宠协战）+ 3 木共鸣（回合初 +6% 回血 & 灼伤/毒额外结算 50%）+ 633 灼伤
    tc_wood = protocol.message(
        "WuxingTowerBattleContext",
        week_key=202648, run_id=24, floor=1, realm_version=2, node_kind=REALM_NODE_NORMAL,
        active_sigil_ids=[BOON_WOOD_DOT_SPREAD, BOON_WOOD_HEAL_EMPOWER, BOON_WOOD_PET_FOLLOW_UP, BOON_FIRE_ROUND_END_BURN],
    )
    p_wd = _make_player(protocol, 104, 0, hp=5_000, max_hp=10_000, phys=100, speed=200, tower_context=tc_wood)
    mw1 = _make_monster(protocol, hp=20_000, phys=10, speed=50)
    mw2 = _make_monster(protocol, hp=20_000, phys=10, speed=40)
    eng_wd = TurnEngine(protocol, _create_req(protocol, 6813, [p_wd]), monsters=[mw1, mw2], skills={}, items={}, max_rounds=12)
    assert eng_wd.has_realm_resonance(WUXING_ELEMENT_WOOD)
    eng_wd.pending[104] = protocol.message("BattleAction", action_type=3)
    eng_wd.resolve()
    # 回合初 3 木共鸣回血 6%（5000 -> 5600），回合末 633 灼伤主目标并由 611 传染给副目标
    assert eng_wd.actors[104].attributes.health >= 5500
    a1, a2 = [a for a in eng_wd.actors.values() if a.team_index == 1]
    assert a1.attributes.health < a1.max_health
    assert a2.attributes.health < a2.max_health


# ==============================================================================
# 4. BattleNode 指标导出、观战交流开关透传与房间加固
# ==============================================================================


def test_battle_node_spectate_chat_redaction_and_prometheus_metrics():
    protocol = Protocol()
    tables = ConfigTables()
    clock = [10_000]
    steady = [10_000]
    fail_spectate = [False]

    def _spectate_sink(_channel, _gate_id, _battle_id, _event):
        if fail_spectate[0]:
            raise RuntimeError("kafka broker unavailable")

    node = BattleNode(
        protocol,
        node_id=1,
        instance_id="battle-1",
        host="127.0.0.1",
        port=9001,
        ticket_secret=b"secret",
        now_ms=lambda: clock[0],
        steady_ms=lambda: steady[0],
        tables=tables,
        fingerprint_mode="warn",
        on_outcome=lambda *_: None,
        on_spectate_frame=_spectate_sink,
    )

    p1 = _make_player(protocol, 101, 0, hp=5_000, phys=500, speed=200)
    p2 = _make_player(protocol, 201, 1, hp=5_000, phys=500, speed=100)
    req = _create_req(
        protocol, 9100, [p1, p2], match_mode=MATCH_MODE_PVP_CHALLENGE, battle_config_id=0,
        spectate_chat_enabled=True, table_fingerprint="stale-fp-warn",
    )
    resp = node.create(req)
    assert not resp.error_message.id
    room = node.rooms[9100]

    # 1) spectate_chat_enabled 透传给观众，同时严格脱敏私密字段
    viewer_state = room.engine.snapshot(0, viewer=0)
    assert viewer_state.spectate_chat_enabled is True
    assert len(viewer_state.self_items) == 0
    assert len(viewer_state.team_pings) == 0
    assert all(len(a.skill_cooldown_rounds) == 0 for a in viewer_state.actors)

    # 2) warn 模式指纹不一致计入 continued；enforce 模式计入 rejected
    text1 = node.collect_metrics_text()
    assert _metric_value(text1, "battle_rooms") == 1
    assert _metric_value(text1, "battle_sql_outcome_stuck_rooms") == 0
    assert _metric_value(text1, 'battle_table_fingerprint_mismatches_total{result="continued"}') == 1
    assert _metric_value(text1, 'battle_table_fingerprint_mismatches_total{result="rejected"}') == 0

    node.fingerprint_mode = "enforce"
    rej_req = _create_req(
        protocol, 9101, [p1, p2], match_mode=MATCH_MODE_PVP_CHALLENGE, battle_config_id=0,
        table_fingerprint="stale-fp-enforce",
    )
    rej = node.create(rej_req)
    assert rej.error_message.id == 1006
    assert _metric_value(node.collect_metrics_text(), 'battle_table_fingerprint_mismatches_total{result="rejected"}') == 1

    # 3) 观众席未连超时回收计入 connect_timeout；连过断线超时计入 reconnect_timeout
    op_a = "11111111-1111-1111-1111-111111111111"
    op_b = "22222222-2222-2222-2222-222222222222"
    add_a = protocol.message(
        "AddObserverRequest", battle_id=9100, observer_player_id=9001,
        spectator_operation_id=op_a, target_instance_id="battle-1", admit_before_ms=clock[0] + 20_000,
    )
    add_a.routing.gate_instance_id = "gate-1"
    assert not node.add_observer(add_a).error_message.id

    add_b = protocol.message(
        "AddObserverRequest", battle_id=9100, observer_player_id=9002,
        spectator_operation_id=op_b, target_instance_id="battle-1", admit_before_ms=clock[0] + 20_000,
    )
    add_b.routing.gate_instance_id = "gate-1"
    assert not node.add_observer(add_b).error_message.id
    t_b = node.issue_ticket(protocol.message(
        "IssueBattleTicketRequest", battle_id=9100, player_id=9002,
        target_instance_id="battle-1", spectator_operation_id=op_b,
    ))
    ticket_b = node.authorize(t_b.assignment.token_payload, t_b.assignment.token_signature)
    close_b = lambda: None
    node.attach(ticket_b, lambda _m, _p: None, close_b)
    node.detach(ticket_b, close_b)

    steady[0] += 35_000
    node.tick()
    text2 = node.collect_metrics_text()
    assert _metric_value(text2, 'battle_observer_seats_reclaimed_total{cause="connect_timeout"}') == 1
    assert _metric_value(text2, 'battle_observer_seats_reclaimed_total{cause="reconnect_timeout"}') == 1

    # 4) 无活直连回合广播计入 battle_frames_dropped_no_direct_total
    assert _metric_value(text2, "battle_frames_dropped_no_direct_total") >= 2

    # 5) 观战中继发送失败计入 battle_spectate_frames_not_sent_total
    fail_spectate[0] = True
    attach_relay = protocol.message(
        "AttachRelayGateRequest", battle_id=9100, target_instance_id="battle-1",
        gate_node_id=7, gate_instance_id="gate-1",
    )
    assert not node.attach_relay_gate(attach_relay).error_message.id
    assert _metric_value(node.collect_metrics_text(), "battle_spectate_frames_not_sent_total") >= 1

    # 6) SQL 终局滞留房间计入 battle_sql_outcome_stuck_rooms
    room.proposed_outcome = 1
    room.outcome_since_ms = steady[0]
    room.outcome_retry_ms = steady[0]
    steady[0] += 6 * 3_600_000 + 1_000
    node._propose_outcome(room, 1, clock[0])
    assert _metric_value(node.collect_metrics_text(), "battle_sql_outcome_stuck_rooms") == 1


def test_wuxing_realm_v2_rpcs_585_586_over_local_tcp_and_v1_inflight_compat(tmp_path):
    """验证 585/586 经真实 TCP Gate -> Scene 路由落盘，以及存量 V1（realm_version=1）在途局平稳完局。"""
    from test_monopoly import TOKEN, _enter, _settings
    from xuanming.runtime import Runtime

    async def _tcp_call(client, call_id: int, message_name: str, **fields):
        p = client.protocol
        mid = p.message_ids[message_name]
        req = p.message(p.routes[mid].request_type, **fields)
        return await client.rpc(mid, call_id, req)

    async def scenario():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="wuxing-v2-e2e") as (client, player_id):
                p = client.protocol
                # 1) 开启 V2 公平试炼轮次
                started = await _tcp_call(
                    client, 1, "SceneSeasonClientPlayerStartWuxingTowerRun",
                    use_free_preset=True, character_preset_id=101, pet_preset_id=201,
                )
                assert not started.error_message.id
                assert started.state.realm_version == 2
                assert started.state.max_floors == 6
                run_id = started.state.run_id

                # 模拟通关第 1 关 -> 选奇策 -> 到达第 2 关待选分岔
                scene_player = runtime.scene.locations[player_id].players[player_id]
                pdata = runtime.scene.rows.assemble(scene_player.snapshot)
                settle1 = p.message(
                    "BattleSettlementData", battle_id=95001, player_id=player_id,
                    outcome=1, player_team_index=0, total_rounds=2,
                )
                settle1.tower_settlement.week_key = started.state.run_week_key
                settle1.tower_settlement.run_id = run_id
                settle1.tower_settlement.floor = 1
                settle1.tower_settlement.total_rounds = 2
                settle1.tower_settlement.realm_version = 2
                settle1.tower_settlement.node_kind = REALM_NODE_NORMAL
                settle1.tower_settlement.use_free_preset = True
                assert runtime.scene.season.settle_wuxing_tower_battle(pdata, settle1) is True
                await runtime.scene.commit_player_data(scene_player, pdata)

                st1 = await _tcp_call(client, 2, "SceneSeasonClientPlayerGetWuxingTowerState")
                assert len(st1.state.pending_offer_sigil_ids) == 3

                picked = await _tcp_call(
                    client, 3, "SceneSeasonClientPlayerPickWuxingTowerSigil",
                    expected_run_id=run_id, sigil_id=st1.state.pending_offer_sigil_ids[0],
                )
                assert not picked.error_message.id
                assert picked.state.current_floor == 2
                assert picked.state.current_node_kind == REALM_NODE_NEED_BRANCH

                # 2) 真实 TCP RPC 585 选右路奇遇 + RPC 586 道缘寻策直接领灵品奇策推进至第 3 关
                br = await _tcp_call(
                    client, 4, "SceneSeasonClientPlayerChooseWuxingRealmBranch",
                    expected_run_id=run_id, branch=REALM_BRANCH_SPECIAL,
                )
                assert not br.error_message.id
                assert br.state.current_node_kind == REALM_NODE_EVENT
                assert br.state.pending_event_kind == REALM_EVENT_ENCOUNTER

                ev = await _tcp_call(
                    client, 5, "SceneSeasonClientPlayerResolveWuxingRealmEvent",
                    expected_run_id=run_id, option_index=1,
                )
                assert not ev.error_message.id
                assert is_valid_realm_boon_id(ev.granted_boon_id)
                assert ev.state.current_floor == 3
                assert ev.state.current_node_kind == REALM_NODE_NORMAL
                assert len(ev.state.active_sigil_ids) == 2

                # 3) 存量 V1 在途局（realm_version=1）仍按 10 层与 V1 符印平滑完局
                pdata_v1 = runtime.scene.rows.assemble(scene_player.snapshot)
                tower = pdata_v1.season_component.wuxing_tower
                wk = wuxing_week_key(runtime.scene.season.clock())
                tower.week_key = wk
                tower.run_week_key = wk
                tower.realm_version = 1
                tower.run_active = True
                tower.run_id = 77
                tower.current_floor = 10
                tower.current_node_kind = REALM_NODE_NORMAL
                tower.pending_event_kind = REALM_EVENT_NONE
                tower.use_free_preset = False
                tower.run_total_rounds = 20
                tower.ClearField("active_sigil_ids")
                tower.ClearField("pending_offer_sigil_ids")
                tower.active_sigil_ids.extend([101, 202, 403])
                settle_v1 = p.message(
                    "BattleSettlementData", battle_id=95010, player_id=player_id,
                    outcome=1, player_team_index=0, total_rounds=5,
                )
                settle_v1.tower_settlement.week_key = wk
                settle_v1.tower_settlement.run_id = 77
                settle_v1.tower_settlement.floor = 10
                settle_v1.tower_settlement.total_rounds = 5
                assert runtime.scene.season.settle_wuxing_tower_battle(pdata_v1, settle_v1) is True
                assert tower.run_active is False
                assert tower.weekly_best_floor == 10
                assert tower.weekly_best_rounds == 25

    asyncio.run(scenario())

