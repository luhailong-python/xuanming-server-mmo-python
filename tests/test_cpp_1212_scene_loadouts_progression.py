"""C++ 77e2e06d2..1212e017e (+5a3313106) 增量移植验证：
- 装备流派预设方案 536..539（0 空位 8v8 原子互换、关联战法/加点联动、扩容符/双灵石解锁幂等）
- 技能书封卷/开卷 543/544、成长被动 V2 与轻享卡 3->6 战法方案扩容
- 战斗平滑升级曲线（PlayerLevelExp / BattleProgressionRule / BattleProgressionReward）、周银两软/硬上限与帮战防双发
- 属性面板 EquipmentEffects / SupportEffectSummary / 经验进度与轻享卡 +3 加点方案
- 挂机月卡/轻享卡设置 522 与离线托管收获简报 523
- 装备品相打磨/重铸/无损升档 UPGRADE_TIER=7、双轨银两 CURRENCY_TYPE_BIND_GOLD=3 隔离与永久绑定道具防洗白
- 灵石百宝阁 V2（定时窗口 37011 / 等级门槛 37012 / 多选一 37013）、赛季荣誉与周词缀、试炼志灵宠外观/技能特效发奖
"""

import asyncio
from dataclasses import replace
from itertools import count
from types import SimpleNamespace

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.data.asset_inventory import balance
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_afk_pass import (
    LIGHT_CARD_LOADOUT_SLOTS,
    SceneAfkPass,
)
from xuanming.nodes.scene_assets import (
    ALWAYS_BOUND_ITEM_IDS,
    CURRENCY_BIND_DIAMOND,
    CURRENCY_BIND_GOLD,
    CURRENCY_DIAMOND,
    CURRENCY_GOLD,
    SEALED_SKILL_BOOK_CONFIG_ID,
    SceneAssets,
)
from xuanming.nodes.scene_attributes import (
    BattleProgressionDelta,
    SceneAttributes,
    WeeklySilverRule,
    evaluate_weekly_silver_cap,
)
from xuanming.nodes.scene_battle import (
    SceneBattle,
    is_npc_isolated_encounter_kind,
    is_practice_dummy_kind,
    is_world_tide_active_on_scene,
)
from xuanming.nodes.scene_equip_craft import (
    CRAFTING_MATERIAL_ID,
    EQUIP_AFFIX_ALREADY_MAX_QUALITY,
    EQUIP_AFFIX_POLISH_EXEMPT,
    EQUIP_CRAFT_GOLD_NOT_ENOUGH,
    GUARANTEE_CHARM_ID,
    POLISH_SAND_ID,
    SceneEquipCraft,
    upgrade_tier_gold_cost,
)
from xuanming.nodes.scene_equip_loadout import (
    EQUIP_LOADOUT_DIAMOND_NOT_ENOUGH,
    EQUIP_LOADOUT_DUPLICATE_ITEM,
    EQUIP_LOADOUT_NAME_INVALID,
    EQUIP_LOADOUT_SCHEME_INVALID,
    EQUIP_LOADOUT_VERSION_CHANGED,
    SceneEquipLoadout,
    UNLOCK_ITEM_CONFIG_ID,
)
from xuanming.nodes.scene_mission import Missions
from xuanming.nodes.scene_pets import ScenePets
from xuanming.nodes.scene_season import SceneSeason, is_eligible_battle
from xuanming.nodes.scene_skill_craft import (
    CRAFT_MODE_TARGETED_RARE,
    CRAFT_MODE_TARGETED_REGULAR,
    RARE_SCROLL_INK_ITEM_ID,
    RARE_SCROLL_PAGE_ITEM_ID,
    SCROLL_FRAGMENT_ITEM_ID,
    SceneSkillCraft,
)
from xuanming.nodes.scene_skills import SceneSkills
from xuanming.nodes.scene_spirit_shop import (
    SPIRIT_SHOP_INVALID_CHOICE,
    SPIRIT_SHOP_LEVEL_NOT_MET,
    SPIRIT_SHOP_NOT_IN_SALE_WINDOW,
    SceneSpiritShop,
)
from xuanming.nodes.scene_trial_journal import SceneTrialJournal
from xuanming.protocol import Protocol


async def _make_full_scene(
    tmp_path,
    *,
    level: int = 60,
    gold: int = 100_000,
    diamond: int = 10_000,
    bind_diamond: int = 10_000,
    bind_gold: int = 0,
    now_ms_ref: list[int] | None = None,
    equip_roll_fn=None,
):
    protocol = Protocol()
    tables = ConfigTables()
    store = SqlitePlayerStore(tmp_path / "players.sqlite3")
    codec = PlayerRowCodec(protocol)
    seed = await store.load_and_claim(1, b"seed")
    data = protocol.message("player_database", player_id=1)
    setattr(data.uint32_pb_component, "class", 1)
    data.level_component.level = level
    data.derived_attributes_component.health = 500
    data.derived_attributes_component.mana = 300
    data.currency.values.extend([gold, diamond, bind_diamond, bind_gold])
    data.attribute_component.schemes.add(scheme_id=1, name="方案一")
    data.attribute_component.active_scheme_id = 1
    data.attribute_component.next_scheme_id = 2
    data.skill_list.SetInParent()
    data.pet_component.SetInParent()
    data.season_component.SetInParent()
    data.craft_component.SetInParent()
    data.equip_loadout_component.SetInParent()
    data.spirit_shop.SetInParent()
    data.trial_journal_component.SetInParent()
    data.mission_component.SetInParent()
    await store.commit(seed, codec.project(data), release=True)

    node = protocol.message("NodeInfo", node_id=7, node_uuid="cpp-1212-scene", zone_id=1)
    scene = SceneNode(protocol, node, store, codec, lambda *_: None)
    scene.tables = tables
    guids = count(30_000)

    async def alloc_guid():
        return next(guids)

    clock_ms = (lambda: now_ms_ref[0]) if now_ms_ref is not None else (lambda: 1_760_000_000_000)
    clock_s = lambda: clock_ms() // 1000

    assets = SceneAssets(scene, tables, {}, allow_gm=True, allocate_item_id=alloc_guid)
    scene.assets = assets
    missions = Missions(scene, tables, assets, clock=clock_ms)
    scene.missions = missions
    attributes = SceneAttributes(scene, tables, assets, missions=missions, allow_gm=True, now_s=clock_s)
    scene.attributes = attributes
    skills = SceneSkills(scene, tables)
    scene.skills = skills
    skill_craft = SceneSkillCraft(scene, tables, assets, clock=clock_ms)
    scene.skill_craft = skill_craft
    pets = ScenePets(scene, tables)
    scene.pets = pets
    equip_craft = SceneEquipCraft(scene, tables, assets, clock=clock_ms, roll_fn=equip_roll_fn)
    scene.equip_craft = equip_craft
    equip_loadout = SceneEquipLoadout(scene, tables, assets)
    scene.equip_loadout = equip_loadout
    afk_pass = SceneAfkPass(scene, tables, assets, allow_gm=True, now_s=clock_s)
    scene.afk_pass = afk_pass
    season = SceneSeason(scene, tables, assets, clock=clock_ms)
    scene.season = season
    spirit_shop = SceneSpiritShop(scene, tables, assets, now_ms=clock_ms)
    scene.spirit_shop = spirit_shop
    trial_journal = SceneTrialJournal(scene, tables, assets, clock=clock_ms)
    scene.trial_journal = trial_journal
    battle = SceneBattle(scene, tables, now_ms=clock_ms)
    scene.battle = battle
    settlement = BattleSettlement(scene, tables, assets, missions, clock=clock_ms)
    scene.settlement = settlement

    assets.register_client_handlers()
    attributes.register()
    skills.register()
    skill_craft.register()
    pets.register()
    equip_craft.register()
    equip_loadout.register()
    afk_pass.register()
    season.register()
    spirit_shop.register()
    trial_journal.register()
    settlement.register()

    scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
    await scene.enter(
        protocol.message(
            "PlayerEnterGameNodeRequest",
            player_id=1,
            session_id=(1 << 17) | 7,
            scene_id=1,
            owner_revision=1,
            login_generation=1,
        ),
        "gate",
    )

    async def rpc(service: str, method: str, **fields):
        msg_id = protocol.message_ids[f"{service}{method}"]
        route = protocol.routes[msg_id]
        body = protocol.message(route.request_type, **fields).SerializeToString()
        req = protocol.message(
            "ProcessClientPlayerMessageRequest",
            session_id=(1 << 17) | 7,
            player_id=1,
            message_content=protocol.content(msg_id, 1, body=body),
        )
        res = await scene.process(req, "gate")
        assert not res.message_content.error_message.id
        return protocol.parse(route.response_type, res.message_content.serialized_message)

    return SimpleNamespace(
        protocol=protocol,
        tables=tables,
        scene=scene,
        assets=assets,
        attributes=attributes,
        skills=skills,
        skill_craft=skill_craft,
        pets=pets,
        equip_craft=equip_craft,
        equip_loadout=equip_loadout,
        afk_pass=afk_pass,
        season=season,
        spirit_shop=spirit_shop,
        trial_journal=trial_journal,
        battle=battle,
        settlement=settlement,
        rpc=rpc,
    )


def _add_item(env, data, *, item_id: int, config_id: int, size: int = 1, bound: bool = False,
              bag_type: int = 0, pos: int | None = None, equip: bool = False, enhance_level: int = 0):
    used = {entry.pos for entry in data.bag_component.items if entry.bag_type == bag_type}
    slot = pos if pos is not None else next(i for i in range(100) if i not in used)
    inst = env.protocol.message("ItemComp", item_id=item_id, config_id=config_id, size=size, bound=bound)
    if equip:
        inst.equip.enhance_level = enhance_level
    entry = data.bag_component.items.add(pos=slot, bag_type=bag_type)
    env.assets.write_entry(entry, inst)


# ── 1. 装备流派预设方案 536..539（含 0 空位 8v8 原子互换、关联联动、扩容解锁与幂等） ──


def test_equip_loadout_save_atomic_zero_free_slot_swap_linked_and_unlock(tmp_path):
    async def run():
        env = await _make_full_scene(tmp_path, level=60, gold=50_000, diamond=1000, bind_diamond=500)
        player = env.scene.locations[1].players[1]

        # 方案 A 统一 7 格席位：60 级免费解锁 2 套，上限 7 套；旧独立 UnlockEquipLoadout 退役拒绝
        get_resp = await env.rpc("SceneBagClientPlayer", "GetEquipLoadouts")
        assert not get_resp.error_message.id
        assert get_resp.snapshot.unlocked_scheme_count == 2
        assert get_resp.snapshot.max_scheme_count == 7
        assert get_resp.snapshot.state_version == 1

        retired = await env.rpc(
            "SceneBagClientPlayer", "UnlockEquipLoadout",
            client_request_id=9001, expected_unlocked_count=2, use_bind_diamond_first=True,
        )
        assert retired.error_message.id != 0
        assert retired.snapshot.unlocked_scheme_count == 2

        # 开通轻享卡后装备方案同步扩容至 4 套（基础 2 + 轻享卡 2）
        gm_pass = await env.rpc("SceneAfkPassClientPlayer", "GmSetAfkPass", remaining_seconds=86400)
        assert not gm_pass.error_message.id
        after_pass = await env.rpc("SceneBagClientPlayer", "GetEquipLoadouts")
        assert not after_pass.error_message.id
        assert after_pass.snapshot.unlocked_scheme_count == 4
        assert after_pass.snapshot.light_card_bonus_schemes == 2

        # 准备第 2 个加点方案以便联动切换
        create_attr = await env.rpc(
            "SceneAttributeClientPlayer", "CreateAttributeScheme",
            name="法系方案", client_request_id=9010,
        )
        assert not create_attr.error_message.id
        assert create_attr.scheme_id == 2

        # 塞满主背包 60/60 格（0 空位）：放 4 件新装备 + 56 格占位物品，身上穿 4 件旧装备，验证 0 空位原子互换！
        data = env.scene.rows.assemble(player.snapshot)
        # 清空背包确保精确控制 60 格
        del data.bag_component.items[:]
        worn_ids = []
        bag_equip_ids = []
        formal_slots = (3, 4, 5, 6)
        for idx, (eq_slot, cfg) in enumerate(zip(formal_slots, (3111, 3112, 3113, 3114))):
            wid = 5001 + idx
            bid = 6001 + idx
            _add_item(env, data, item_id=wid, config_id=cfg, bag_type=2, pos=eq_slot, equip=True)
            _add_item(env, data, item_id=bid, config_id=cfg, bag_type=0, pos=idx, equip=True)
            worn_ids.append(wid)
            bag_equip_ids.append(bid)
        for slot_i in range(4, 60):
            _add_item(env, data, item_id=7000 + slot_i, config_id=CRAFTING_MATERIAL_ID, size=1, bag_type=0, pos=slot_i)
        await env.scene.commit_player_data(player, data)

        # 保存方案 1（自动采集当前穿戴 5001..5004）
        cur_ver = (await env.rpc("SceneBagClientPlayer", "GetEquipLoadouts")).snapshot.state_version
        save1 = await env.rpc(
            "SceneBagClientPlayer", "SaveEquipLoadout",
            scheme_id=1, name="输出套", state_version=cur_ver,
            request_id="req-save-loadout-0001", capture_current_equipped=True,
            linked_skill_scheme_id=1, linked_attribute_scheme_id=1,
        )
        assert not save1.error_message.id
        assert len(save1.snapshot.schemes[0].slots) == 8
        assert sum(1 for s in save1.snapshot.schemes[0].slots if s.item_guid) == 4

        # 保存方案 2（显式指定背包里的 6001..6004，并联动战法 2 + 加点 2）
        slots2 = [
            env.protocol.message("EquipLoadoutSlotBinding", slot=eq_slot, item_guid=bid)
            for eq_slot, bid in zip(formal_slots, bag_equip_ids)
        ]
        save2 = await env.rpc(
            "SceneBagClientPlayer", "SaveEquipLoadout",
            scheme_id=2, name="辅助套", state_version=save1.snapshot.state_version,
            request_id="req-save-loadout-0002", slots=slots2,
            linked_skill_scheme_id=2, linked_attribute_scheme_id=2,
        )
        assert not save2.error_message.id

        # 校验无效名称 / 重复 item_id / 错误版本号 / 越界 scheme_id
        bad_name = await env.rpc(
            "SceneBagClientPlayer", "SaveEquipLoadout",
            scheme_id=1, name="   ", state_version=save2.snapshot.state_version,
            request_id="req-bad-name-000001", capture_current_equipped=True,
        )
        assert bad_name.error_message.id == EQUIP_LOADOUT_NAME_INVALID
        bad_ver = await env.rpc(
            "SceneBagClientPlayer", "SaveEquipLoadout",
            scheme_id=1, name="有效名", state_version=1,
            request_id="req-bad-ver-0000001", capture_current_equipped=True,
        )
        assert bad_ver.error_message.id == EQUIP_LOADOUT_VERSION_CHANGED
        bad_scheme = await env.rpc(
            "SceneBagClientPlayer", "SwitchEquipLoadout",
            scheme_id=5, state_version=save2.snapshot.state_version,
            request_id="req-bad-scheme-0001",
        )
        assert bad_scheme.error_message.id == EQUIP_LOADOUT_SCHEME_INVALID
        dup_slots = [
            env.protocol.message("EquipLoadoutSlotBinding", slot=3, item_guid=bag_equip_ids[0]),
            env.protocol.message("EquipLoadoutSlotBinding", slot=4, item_guid=bag_equip_ids[0]),
        ]
        bad_dup = await env.rpc(
            "SceneBagClientPlayer", "SaveEquipLoadout",
            scheme_id=2, name="重复套", state_version=save2.snapshot.state_version,
            request_id="req-bad-dup-0000001", slots=dup_slots,
        )
        assert bad_dup.error_message.id == EQUIP_LOADOUT_DUPLICATE_ITEM

        # 0 空位下切换至方案 2：6001..6004 全部上身，5001..5004 原子换回主背包 0..3 格，同时联动切换战法 2 与加点 2！
        sw2 = await env.rpc(
            "SceneBagClientPlayer", "SwitchEquipLoadout",
            scheme_id=2, state_version=save2.snapshot.state_version,
            request_id="req-switch-loadout-2", apply_linked_schemes=True,
        )
        assert not sw2.error_message.id
        assert not sw2.missing_slots
        assert not sw2.linked_notices
        after = env.scene.rows.assemble(player.snapshot)
        worn_after = {item.item_id for _, item in env.assets.entries(after.bag_component, 2)}
        bag_after = {item.item_id for _, item in env.assets.entries(after.bag_component, 0)}
        assert worn_after == set(bag_equip_ids)
        assert set(worn_ids).issubset(bag_after)
        assert after.skill_list.active_loadout_scheme_id == 2
        assert after.attribute_component.active_scheme_id == 2

    asyncio.run(run())


# ── 2. 技能书封卷/开卷 543/544、轻享卡 3->6 战法方案扩容与双轨银两升级 ──


def test_skill_book_seal_unseal_and_light_card_6_schemes(tmp_path):
    async def run():
        now_ref = [1_760_000_000_000]
        env = await _make_full_scene(tmp_path, level=60, gold=500, bind_gold=5000, now_ms_ref=now_ref)
        player = env.scene.locations[1].players[1]

        # 免费期（<85级）仅解锁 2 套战法方案，最高 7 套（含等级/轻享卡/月租扩容）
        skills0 = await env.rpc("SceneSkillClientPlayer", "ListSkills")
        assert skills0.loadout.unlocked_schemes == 2
        assert skills0.loadout.max_schemes == 7
        assert skills0.currency_balance == 5500  # 500 流通 + 5000 绑银

        # 切到方案 4 拒绝
        fail4 = await env.rpc(
            "SceneSkillClientPlayer", "SwitchSkillLoadout",
            scheme_id=4, state_version=1, request_id="req-skill-slot4-fail",
        )
        assert fail4.error_message.id == 7026

        # GM 开通轻享卡后解锁 4 套战法方案（基础 2 + 轻享卡 2）
        gm_pass = await env.rpc("SceneAfkPassClientPlayer", "GmSetAfkPass", remaining_seconds=86400)
        assert not gm_pass.error_message.id
        assert gm_pass.info.unlocked_loadout_slots == LIGHT_CARD_LOADOUT_SLOTS
        assert gm_pass.info.stage2_tradable_enabled is True
        skills1 = await env.rpc("SceneSkillClientPlayer", "ListSkills")
        assert skills1.loadout.unlocked_schemes == 4
        ok4 = await env.rpc(
            "SceneSkillClientPlayer", "SwitchSkillLoadout",
            scheme_id=4, state_version=1, request_id="req-skill-slot4-pass",
        )
        assert not ok4.error_message.id
        assert ok4.state.loadout.active_scheme_id == 4

        # 准备可交易悟道残页（3406）+ 稀世残页/墨（3407/3408）制书
        data = env.scene.rows.assemble(player.snapshot)
        _add_item(env, data, item_id=8001, config_id=SCROLL_FRAGMENT_ITEM_ID, size=50, bound=False)
        _add_item(env, data, item_id=8002, config_id=RARE_SCROLL_PAGE_ITEM_ID, size=5, bound=False)
        _add_item(env, data, item_id=8003, config_id=RARE_SCROLL_INK_ITEM_ID, size=5, bound=True)
        await env.scene.commit_player_data(player, data)

        state0 = await env.rpc("SceneSkillClientPlayer", "GetSkillCraftingState")
        scroll_reg = next(s for s in state0.state.scrolls if s.rarity_tier == 0)
        scroll_rare = next(s for s in state0.state.scrolls if s.rarity_tier == 1)

        # 1) 常规制书（用纯可交易残页，严格扣流通银两，产物未绑定）
        craft_reg = await env.rpc(
            "SceneSkillClientPlayer", "CraftSkillBook",
            client_request_id=7101,
            expected_revision=state0.state.scroll_craft_revision,
            scroll_id=scroll_reg.scroll_id,
            mode=CRAFT_MODE_TARGETED_REGULAR,
            target_skill_table_id=scroll_reg.skill_table_ids[0],
        )
        assert not craft_reg.error_message.id
        assert not craft_reg.crafted_book.bound
        book_id = craft_reg.crafted_book.book_id

        # 2) 封卷入包（543 SealSkillBook）-> 主背包产出 3414 封卷道籍且 GetBag 展示 sealed_skill_book 元数据
        seal = await env.rpc(
            "SceneSkillClientPlayer", "SealSkillBook",
            client_request_id=7102,
            expected_revision=craft_reg.state.scroll_craft_revision,
            book_id=book_id,
        )
        assert not seal.error_message.id
        assert seal.item_id > 0
        assert seal.skill_table_id == scroll_reg.skill_table_ids[0]
        # 幂等重放
        seal_replay = await env.rpc(
            "SceneSkillClientPlayer", "SealSkillBook",
            client_request_id=7102,
            expected_revision=craft_reg.state.scroll_craft_revision,
            book_id=book_id,
        )
        assert not seal_replay.error_message.id and seal_replay.replayed
        assert seal_replay.item_id == seal.item_id

        bag_resp = await env.rpc("SceneBagClientPlayer", "GetBag", bag_type=0)
        sealed_info = next(it for it in bag_resp.bag.items if it.item_id == seal.item_id)
        assert sealed_info.config_id == SEALED_SKILL_BOOK_CONFIG_ID
        assert sealed_info.sealed_skill_book.skill_table_id == scroll_reg.skill_table_ids[0]
        assert sealed_info.sealed_skill_book.skill_name

        # 3) 开卷回书架（544 UnsealSkillBook）并验证绑定态防洗白继承
        data = env.scene.rows.assemble(player.snapshot)
        for entry, inst in env.assets.entries(data.bag_component, 0):
            if inst.item_id == seal.item_id:
                inst.bound = True
                env.assets.write_entry(entry, inst)
        await env.scene.commit_player_data(player, data)

        unseal = await env.rpc(
            "SceneSkillClientPlayer", "UnsealSkillBook",
            client_request_id=7103,
            expected_revision=seal.state.scroll_craft_revision,
            item_id=seal.item_id,
        )
        assert not unseal.error_message.id
        assert unseal.unsealed_book.skill_table_id == scroll_reg.skill_table_ids[0]
        assert unseal.unsealed_book.bound is True  # 继承绑定态

        # 4) 绑定书严禁再次封卷入包
        refuse_bound = await env.rpc(
            "SceneSkillClientPlayer", "SealSkillBook",
            client_request_id=7104,
            expected_revision=unseal.state.scroll_craft_revision,
            book_id=unseal.unsealed_book.book_id,
        )
        assert refuse_bound.error_message.id != 0

        # 5) 稀世绝学定向制书允许优先扣绑定银两（流通银两清 0 也能用绑银制稀世）
        data = env.scene.rows.assemble(player.snapshot)
        data.currency.values[CURRENCY_GOLD] = 0
        data.currency.values[CURRENCY_BIND_GOLD] = 5000
        await env.scene.commit_player_data(player, data)
        craft_rare = await env.rpc(
            "SceneSkillClientPlayer", "CraftSkillBook",
            client_request_id=7105,
            expected_revision=unseal.state.scroll_craft_revision,
            scroll_id=scroll_rare.scroll_id,
            mode=CRAFT_MODE_TARGETED_RARE,
            target_skill_table_id=scroll_rare.skill_table_ids[0],
        )
        assert not craft_rare.error_message.id
        assert craft_rare.crafted_book.bound is True
        assert craft_rare.crafted_book.rarity_tier == 1
        after_rare = env.scene.rows.assemble(player.snapshot)
        assert balance(after_rare, CURRENCY_BIND_GOLD) < 5000

    asyncio.run(run())


# ── 3. 战斗平滑升级曲线、周银两软/硬上限、帮战防双发与 NPC 隔离 ──


def test_battle_progression_level_up_weekly_silver_cap_and_npc_isolation(tmp_path):
    async def run():
        now_ref = [1_760_000_000_000]
        env = await _make_full_scene(tmp_path, level=1, gold=0, now_ms_ref=now_ref)
        assert env.attributes.validate_progression_tables() is True

        # 纯函数验证跨 30000 软上限（衰减 20%）与 45000 硬封顶精确拆分，以及最多 3 周银两滚存蓄水池（90k/135k）
        rule = WeeklySilverRule(soft_cap=30_000, diminished_rate_permyriad=2000, hard_cap=45_000)
        ev1 = evaluate_weekly_silver_cap(2000, 29_000, rule)
        assert ev1.credited_silver == 1200  # 1000 满额 + 1000 * 20% = 1200
        assert ev1.used_after == 30_200 and ev1.diminished is True
        ev2 = evaluate_weekly_silver_cap(1000, 45_000, rule)
        assert ev2.credited_silver == 0 and ev2.diminished is True

        from xuanming.nodes.scene_attributes import prepare_economy_ledger
        from xuanming.nodes.scene_equip_craft import _MaterialRewardState, resolve_stage2_week_state, WEEK_MS
        pve_probe = env.protocol.message(
            "BattleSettlementData",
            player_id=1, battle_id=1999, outcome=1, player_team_index=0,
            total_rounds=3, match_mode=4, battle_config_id=1,
        )
        rows = env.attributes.load_battle_progression_rewards()
        empty_econ = env.protocol.message("BattleEconomyLedger")
        wk1 = 1_795_381_200_000
        cand1, d1 = prepare_economy_ledger(empty_econ, wk1, pve_probe, 0, 0, rows, rule)
        assert int(cand1.silver_rollover_soft_cap) == 30_000
        assert int(cand1.silver_rollover_hard_cap) == 45_000
        cand1.battle_silver_this_week = 10_000
        cand2, d2 = prepare_economy_ledger(cand1, wk1 + WEEK_MS, pve_probe, 0, 0, rows, rule)
        assert int(cand2.silver_rollover_soft_cap) == 50_000
        assert int(cand2.silver_rollover_hard_cap) == 80_000
        assert d2.weekly_silver_soft_cap == 50_000
        cand10, d10 = prepare_economy_ledger(cand2, wk1 + 10 * WEEK_MS, pve_probe, 0, 0, rows, rule)
        assert int(cand10.silver_rollover_soft_cap) == 90_000
        assert int(cand10.silver_rollover_hard_cap) == 135_000
        assert d10.weekly_silver_soft_cap == 90_000

        # 阶段二材料 4 周滚存蓄水池（+150/周，封顶 600，抗时钟回拨）
        st = _MaterialRewardState(week_starts_at_ms=wk1, earned_this_week=15, stage2_pool_cap=150)
        assert resolve_stage2_week_state(st, wk1 - WEEK_MS) == (15, 150, True)
        assert resolve_stage2_week_state(st, wk1 + 2 * WEEK_MS) == (0, 435, False)
        assert resolve_stage2_week_state(st, wk1 + 10 * WEEK_MS) == (0, 600, False)

        player = env.scene.locations[1].players[1]
        data = env.scene.rows.assemble(player.snapshot)
        # 1) 归元城演武木桩（boss_trial_kind=9）严格隔离：0 经验、0 银两、不推进赛季
        npc_settle = env.protocol.message(
            "BattleSettlementData",
            player_id=1, battle_id=1001, outcome=1, player_team_index=0,
            total_rounds=4, match_mode=4, battle_config_id=1, boss_trial_kind=9,
        )
        assert not is_eligible_battle(npc_settle)
        assert is_npc_isolated_encounter_kind(9) and is_practice_dummy_kind(9)
        assert await env.settlement.apply(player, data, npc_settle)
        assert data.level_component.level == 1
        assert data.level_component.total_exp == 0
        assert balance(data, CURRENCY_GOLD) == 0

        # 2) 帮战（match_mode=9）纵深防御：给经验（800）但银两强制压为 0
        gw_settle = env.protocol.message(
            "BattleSettlementData",
            player_id=1, battle_id=1002, outcome=1, player_team_index=0,
            total_rounds=5, match_mode=9, battle_config_id=1,
        )
        assert await env.settlement.apply(player, data, gw_settle)
        assert data.level_component.total_exp == 800
        assert data.level_component.level == 3  # L3=500, L4=900
        assert balance(data, CURRENCY_GOLD) == 0

        # 3) PVE 胜局（match_mode=4）：额外 +1000 经验（总 1800 -> L5=1400）+ 600 银两，写入 BattleResultEntry 进度字段
        pve_settle = env.protocol.message(
            "BattleSettlementData",
            player_id=1, battle_id=1003, outcome=1, player_team_index=0,
            total_rounds=3, match_mode=4, battle_config_id=1,
        )
        assert await env.settlement.apply(player, data, pve_settle)
        assert data.level_component.total_exp == 1800
        assert data.level_component.level == 5
        assert balance(data, CURRENCY_GOLD) == 600
        entry = next(e for e in data.battle_results.entries if e.battle_id == 1003)
        assert entry.level_before == 3 and entry.level_after == 5
        assert entry.exp_gain == 1000 and entry.total_exp_after == 1800

        await env.scene.commit_player_data(player, data)
        # 属性面板正确下发经验进度与 EquipmentEffects / SupportEffectSummary
        panel_resp = await env.rpc("SceneAttributeClientPlayer", "GetAttributePanel")
        assert not panel_resp.error_message.id
        assert panel_resp.panel.level == 5
        assert panel_resp.panel.total_exp == 1800
        assert panel_resp.panel.current_level_exp == 400  # 1800 - 1400
        assert panel_resp.panel.next_level_exp == 550     # 1950 - 1400
        assert panel_resp.panel.is_max_level is False

        # 野外灵潮窗口纯规则验证（[2,3,5], 3600s/2400s）
        assert is_world_tide_active_on_scene(0, 2) is True
        assert is_world_tide_active_on_scene(2400, 2) is False
        assert is_world_tide_active_on_scene(3600, 3) is True

    asyncio.run(run())


# ── 4. 挂机月卡偏好设置 522 与离线托管收获简报 523 ──


def test_afk_pass_settings_and_hosted_recap_lifecycle(tmp_path):
    async def run():
        now_ref = [1_760_000_000_000]
        env = await _make_full_scene(tmp_path, now_ms_ref=now_ref)
        player = env.scene.locations[1].players[1]

        # 522 设置离队自动回主城
        set_resp = await env.rpc(
            "SceneAfkPassClientPlayer", "SetAfkPassSettings",
            auto_return_city_on_team_end=True,
        )
        assert not set_resp.error_message.id
        assert set_resp.info.auto_return_city_on_team_end is True

        # 模拟托管开始 -> 托管期间战斗结算汇总 -> 托管结束 -> 523 按 started_at 幂等确认清零
        data = env.scene.rows.assemble(player.snapshot)
        started = now_ref[0] // 1000
        assert SceneAfkPass.on_hosting_started(data, started) is True
        assert SceneAfkPass.record_hosted_battle_settlement(
            data, hosting_active=True, won=True, exp_gained=1000, silver_gained=600, item_count=3,
            now_unix_sec=started + 10,
        ) is True
        assert SceneAfkPass.on_hosting_ended(data, started + 120) is True
        await env.scene.commit_player_data(player, data)

        info_resp = await env.rpc("SceneAfkPassClientPlayer", "GetAfkPass")
        recap = info_resp.info.hosted_recap
        assert recap.pending_ack is True
        assert recap.started_at == started and recap.ended_at == started + 120
        assert recap.battle_count == 1 and recap.win_count == 1
        assert recap.exp_gained == 1000 and recap.silver_gained == 600 and recap.item_count == 3

        # 错误 started_at 不误清
        wrong_ack = await env.rpc("SceneAfkPassClientPlayer", "AckHostedRecap", started_at=started + 999)
        assert not wrong_ack.error_message.id
        assert wrong_ack.info.hosted_recap.pending_ack is True

        # 正确 started_at 清零
        ok_ack = await env.rpc("SceneAfkPassClientPlayer", "AckHostedRecap", started_at=started)
        assert not ok_ack.error_message.id
        assert ok_ack.info.hosted_recap.pending_ack is False
        assert ok_ack.info.hosted_recap.battle_count == 0

    asyncio.run(run())


# ── 5. 装备品相打磨/重铸/无损升档 UPGRADE_TIER=7、双轨银两与永久绑定道具 ──


def test_equip_craft_polish_reforge_upgrade_tier_and_split_gold(tmp_path):
    async def run():
        # 固定品相掷骰为 2 档（良品）以验证打磨保底与定星符必达 5 档
        env = await _make_full_scene(
            tmp_path, level=60, gold=500, bind_gold=500, equip_roll_fn=lambda _tier: 2,
        )
        player = env.scene.locations[1].players[1]
        data = env.scene.rows.assemble(player.snapshot)
        # 放入 10 级基础武器（3111，绑定）、未绑定 10 级武器（3111）、材料（3001）、重铸砂（3403）、定星符（3405）
        _add_item(env, data, item_id=9101, config_id=3111, bound=True, equip=True, enhance_level=7)
        _add_item(env, data, item_id=9102, config_id=3111, bound=False, equip=True, enhance_level=5)
        _add_item(env, data, item_id=9201, config_id=CRAFTING_MATERIAL_ID, size=50, bound=False)
        _add_item(env, data, item_id=9202, config_id=POLISH_SAND_ID, size=20, bound=True)
        _add_item(env, data, item_id=9203, config_id=GUARANTEE_CHARM_ID, size=2, bound=True)
        await env.scene.commit_player_data(player, data)

        rules = await env.rpc("SceneBagClientPlayer", "GetEquipmentCraftRules")
        assert not rules.error_message.id
        fp = rules.rules_fingerprint
        rev = rules.forge_revision
        pool1_affixes = [a for a in rules.affixes if a.pool == 1]
        assert len(pool1_affixes) >= 2
        affix_a, affix_b = pool1_affixes[0], pool1_affixes[1]

        # 1) 给绑定武器 9101 安装 1 号槽词条 -> 掷得 2 档（良品）
        inst_resp = await env.rpc(
            "SceneBagClientPlayer", "CommitEquipmentCraft",
            client_request_id=8101, rules_fingerprint=fp, expected_forge_revision=rev,
            choice=env.protocol.message(
                "EquipmentCraftChoice", operation=2, item_id=9101, affix_slot=1, affix_id=affix_a.affix_id,
            ),
        )
        assert not inst_resp.error_message.id
        assert inst_resp.result_quality_tier == 2

        # 2) 重铸词条 REFORGE_AFFIX（op=3）：绑定装优先扣绑银（20），原样保留品相 2 档
        reforge = await env.rpc(
            "SceneBagClientPlayer", "CommitEquipmentCraft",
            client_request_id=8102, rules_fingerprint=fp, expected_forge_revision=inst_resp.result_forge_revision,
            choice=env.protocol.message(
                "EquipmentCraftChoice", operation=3, item_id=9101, affix_slot=1, affix_id=affix_b.affix_id,
            ),
        )
        assert not reforge.error_message.id
        assert reforge.result_quality_tier == 2
        after_rf = env.scene.rows.assemble(player.snapshot)
        assert balance(after_rf, CURRENCY_BIND_GOLD) == 480 and balance(after_rf, CURRENCY_GOLD) == 500

        # 3) 打磨词条 POLISH_AFFIX（op=4）+ 定星符（3405）-> 必达 5 档极品且保底归 0
        polish = await env.rpc(
            "SceneBagClientPlayer", "CommitEquipmentCraft",
            client_request_id=8103, rules_fingerprint=fp, expected_forge_revision=reforge.result_forge_revision,
            choice=env.protocol.message(
                "EquipmentCraftChoice", operation=4, item_id=9101, affix_slot=1, use_guarantee_charm=True,
            ),
        )
        assert not polish.error_message.id
        assert polish.result_quality_tier == 5
        assert polish.guaranteed_by_pity is True
        assert polish.result_polish_pity_count == 0

        # 再次打磨满档词条被拒 28023
        polish_max = await env.rpc(
            "SceneBagClientPlayer", "CommitEquipmentCraft",
            client_request_id=8104, rules_fingerprint=fp, expected_forge_revision=polish.result_forge_revision,
            choice=env.protocol.message("EquipmentCraftChoice", operation=4, item_id=9101, affix_slot=1),
        )
        assert polish_max.error_message.id == EQUIP_AFFIX_ALREADY_MAX_QUALITY

        # 4) 原地无损升档 UPGRADE_TIER（op=7）：绑定 10 级 3111 -> 30 级 3131（2 档 * 100 = 200 绑银），保留强化 +7 与词条！
        assert upgrade_tier_gold_cost(1, 10, 30) == 200
        up_bound = await env.rpc(
            "SceneBagClientPlayer", "CommitEquipmentCraft",
            client_request_id=8105, rules_fingerprint=fp, expected_forge_revision=polish.result_forge_revision,
            choice=env.protocol.message("EquipmentCraftChoice", operation=7, item_id=9101, config_id=3131),
        )
        assert not up_bound.error_message.id
        after_up = env.scene.rows.assemble(player.snapshot)
        upgraded = next(it for _, it in env.assets.entries(after_up.bag_component, 0) if it.item_id == 9101)
        assert upgraded.config_id == 3131
        assert upgraded.equip.enhance_level == 7
        assert upgraded.equip.fixed_affixes[0].quality_tier == 5
        assert balance(after_up, CURRENCY_BIND_GOLD) == 260  # 480 - 20(polish) - 200(upgrade)
        assert balance(after_up, CURRENCY_GOLD) == 500

        # 5) 未绑定武器 9102 升档严格只扣流通银两（把流通银两设为 50 < 100 但绑银仍有 260，必须拒 28029！）
        after_up.currency.values[CURRENCY_GOLD] = 50
        await env.scene.commit_player_data(player, after_up)
        up_fail = await env.rpc(
            "SceneBagClientPlayer", "CommitEquipmentCraft",
            client_request_id=8106, rules_fingerprint=fp, expected_forge_revision=up_bound.result_forge_revision,
            choice=env.protocol.message("EquipmentCraftChoice", operation=7, item_id=9102, config_id=3121),
        )
        assert up_fail.error_message.id == EQUIP_CRAFT_GOLD_NOT_ENOUGH

        # 6) 永久绑定道具集合防洗白（AssetCredit 3405 即使请求未标 bound 也必须强制 bound=True）
        assert GUARANTEE_CHARM_ID in ALWAYS_BOUND_ITEM_IDS
        data2 = env.scene.rows.assemble(player.snapshot)
        err = await env.assets.add_bound_items(data2, {3404: 2})
        assert err == 0
        vouchers = [it for _, it in env.assets.entries(data2.bag_component, 0) if it.config_id == 3404]
        assert vouchers and all(it.bound for it in vouchers)

    asyncio.run(run())


# ── 6. 灵石百宝阁 V2（定时窗口 / 等级门槛 / 多选一）、赛季荣誉与周词缀、试炼志外观发奖、宠物绑银 ──


def test_spirit_shop_v2_season_honors_trial_journal_and_pet_bind_gold(tmp_path):
    async def run():
        # 1) 灵石百宝阁 V2：在节日窗口外隐藏 401/402，且直购 401 返回 37011；进入窗口但 5 级返回 37012；15 级购买 402 CHOOSE_ONE 验证 37013 与正确单选发放
        now_ref = [1_760_000_000_000]  # 窗口外（401/402 为 1801756800000..1803139199000）
        env = await _make_full_scene(tmp_path, level=5, gold=0, diamond=5000, bind_gold=10_000, now_ms_ref=now_ref)
        player = env.scene.locations[1].players[1]

        info_out = await env.rpc("SceneSpiritShopClientPlayer", "GetSpiritShopInfo")
        assert len(info_out.info.goods) == 11
        buy_out = await env.rpc(
            "SceneSpiritShopClientPlayer", "BuySpiritShopGoods",
            client_request_id=6101, goods_id=401, count=1,
            expected_currency_type=1, expected_unit_price=120,
        )
        assert buy_out.error_message.id == SPIRIT_SHOP_NOT_IN_SALE_WINDOW

        # 进入节日窗口，但 5 级不满足 min_level=10 -> 37012
        now_ref[0] = 1_802_000_000_000
        info_low = await env.rpc("SceneSpiritShopClientPlayer", "GetSpiritShopInfo")
        assert all(g.goods_id not in (401, 402) for g in info_low.info.goods)
        buy_low = await env.rpc(
            "SceneSpiritShopClientPlayer", "BuySpiritShopGoods",
            client_request_id=6102, goods_id=402, count=1,
            expected_currency_type=1, expected_unit_price=180,
        )
        assert buy_low.error_message.id == SPIRIT_SHOP_LEVEL_NOT_MET

        # 提升到 15 级 -> 401 与 402 上架可见；402 为 CHOOSE_ONE（grant_mode=1），非法索引返回 37013，合法索引仅发所选 1 项
        data = env.scene.rows.assemble(player.snapshot)
        data.level_component.level = 15
        await env.scene.commit_player_data(player, data)
        info_in = await env.rpc("SceneSpiritShopClientPlayer", "GetSpiritShopInfo")
        g402 = next(g for g in info_in.info.goods if g.goods_id == 402)
        assert g402.grant_mode == 1 and len(g402.items) >= 2
        exp_items = [
            env.protocol.message(
                "SpiritShopItemEntry",
                item_config_id=it.item_config_id, item_count=it.item_count, bound=it.bound,
            )
            for it in g402.items
        ]
        bad_choice = await env.rpc(
            "SceneSpiritShopClientPlayer", "BuySpiritShopGoods",
            client_request_id=6103, goods_id=402, count=1,
            expected_currency_type=g402.currency_type, expected_unit_price=g402.unit_price,
            expected_items=exp_items, chosen_item_index=99,
        )
        assert bad_choice.error_message.id == SPIRIT_SHOP_INVALID_CHOICE

        ok_choice = await env.rpc(
            "SceneSpiritShopClientPlayer", "BuySpiritShopGoods",
            client_request_id=6104, goods_id=402, count=1,
            expected_currency_type=g402.currency_type, expected_unit_price=g402.unit_price,
            expected_items=exp_items, chosen_item_index=1,
        )
        assert not ok_choice.error_message.id
        chosen = g402.items[1]
        after_shop = env.scene.rows.assemble(player.snapshot)
        assert sum(it.size for _, it in env.assets.entries(after_shop.bag_component, 0) if it.config_id == chosen.item_config_id) == chosen.item_count

        # 2) 赛季荣誉视图（SeasonHonorView）与周环境词缀（SeasonWeeklyAffixView）
        season_resp = await env.rpc("SceneSeasonClientPlayer", "GetSeasonState")
        assert not season_resp.error_message.id
        if season_resp.state.available:
            assert season_resp.state.weekly_affix.affix_id in (1, 2, 3, 4)
            assert season_resp.state.weekly_affix.name
            assert any(h.season_id == season_resp.state.season_id and h.is_current_season for h in season_resp.state.honors)

        # 3) 宠物购买优先消耗绑定银两（流通银两为 0，绑银 10000 也能购买）
        shop_pets = await env.rpc("ScenePetClientPlayer", "GetPetShop")
        assert not shop_pets.error_message.id
        offer = next(o for o in shop_pets.offers if o.unlock_level <= 15)
        buy_pet = await env.rpc(
            "ScenePetClientPlayer", "BuyPet",
            pet_table_id=offer.pet_table_id,
            purchase_sequence=shop_pets.next_purchase_sequence,
            price_gold=offer.price_gold,
        )
        assert not buy_pet.error_message.id
        assert buy_pet.pet_id > 0
        after_pet = env.scene.rows.assemble(player.snapshot)
        assert balance(after_pet, CURRENCY_BIND_GOLD) == 10_000 - offer.price_gold

        # 4) 试炼志表校验 + 独占灵宠外观/技能特效发放辅助验证
        assert env.trial_journal.validate_tables() is True
        tj_resp = await env.rpc("SceneTrialJournalClientPlayer", "GetTrialJournal")
        assert not tj_resp.error_message.id

    asyncio.run(run())
