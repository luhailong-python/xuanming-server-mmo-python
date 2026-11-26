"""C++ 0f9bc81d6 增量移植验证：战法构筑 502/503、灵宠条目方案与灵阶突破 500/501、装备强化 T/B 隔离与解封/继承、论剑 V3 备选灵宠与赛季印快照。"""

import asyncio
from itertools import count

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_battle import SceneBattle
from xuanming.nodes.scene_equip_craft import (
    CRAFTING_MATERIAL_ID,
    ENHANCEMENT_STONE_ID,
    EQUIP_BIND_CONFIRM_REQUIRED,
    EQUIP_LEVEL_CHANGED,
    INHERIT_TALISMAN_ID,
    UNSEAL_TALISMAN_ID,
    SceneEquipCraft,
)
from xuanming.nodes.scene_pets import (
    PET_ENTRY_PRESET_INVALID,
    PET_TIER_CHANGED,
    PET_TIER_LEVEL_NOT_ENOUGH,
    PET_TIER_MATERIAL_NOT_ENOUGH,
    PET_TIER_MAX_REACHED,
    ScenePets,
)
from xuanming.nodes.scene_season import SceneSeason
from xuanming.nodes.scene_skills import SceneSkills
from xuanming.nodes.scene_stats import battle_skills, player_stats
from xuanming.protocol import Protocol


async def _make_scene(tmp_path, *, level: int = 60, gold: int = 100_000, now_ref=None):
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
    data.currency.values.extend([gold, 0, 0])
    data.skill_list.SetInParent()
    data.pet_component.SetInParent()
    data.season_component.SetInParent()
    data.craft_component.SetInParent()
    await store.commit(seed, codec.project(data), release=True)

    node = protocol.message("NodeInfo", node_id=7, node_uuid="v3-build-scene", zone_id=1)
    scene = SceneNode(protocol, node, store, codec, lambda *_: None)
    scene.tables = tables
    guids = count(20_000)

    async def alloc_guid():
        return next(guids)

    clock = (lambda: now_ref[0]) if now_ref is not None else (lambda: 1_760_000_000_000)
    assets = SceneAssets(scene, tables, {}, allow_gm=True, allocate_item_id=alloc_guid)
    scene.assets = assets
    skills = SceneSkills(scene, tables)
    scene.skills = skills
    pets = ScenePets(scene, tables)
    scene.pets = pets
    craft = SceneEquipCraft(scene, tables, assets, clock=clock)
    season = SceneSeason(scene, tables, assets, clock=clock)
    scene.season = season
    battle = SceneBattle(scene, tables, now_ms=clock)
    scene.battle = battle

    assets.register_client_handlers()
    skills.register()
    pets.register()
    craft.register()
    season.register()

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

    return protocol, tables, scene, assets, skills, pets, craft, battle, rpc


def _add_bag_item(protocol, assets, data, *, item_id: int, config_id: int, size: int = 1, bound: bool = False,
                  pos: int | None = None, equip: bool = False, enhance_level: int = 0, enhance_pity: int = 0):
    used = {entry.pos for entry in data.bag_component.items if entry.bag_type == 0}
    slot = pos if pos is not None else next(i for i in range(100) if i not in used)
    item = protocol.message("ItemComp", item_id=item_id, config_id=config_id, size=size, bound=bound, acquire_seq=item_id)
    if equip:
        item.equip.enhance_level = enhance_level
        item.equip.enhance_pity_count = enhance_pity
    assets.write_entry(data.bag_component.items.add(pos=slot, bag_type=0), item)
    return item


# ── 1. 人物战法构筑（502 SaveSkillLoadout / 503 SwitchSkillLoadout） ───────────────────────────────────


def test_skill_loadout_502_503_exclusive_group_stats_and_snapshot(tmp_path):
    async def exercise():
        protocol, tables, scene, _, skills, _, _, _, rpc = await _make_scene(tmp_path, level=60, gold=100_000)
        try:
            assert protocol.message_ids["SceneSkillClientPlayerSaveSkillLoadout"] == 502
            assert protocol.message_ids["SceneSkillClientPlayerSwitchSkillLoadout"] == 503

            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            for sid in (10002, 12001, 12002, 12003, 12004, 12101, 12102, 13101, 14101):
                data.skill_list.skill_list.add(skill_table_id=sid, rank=1)
            await scene.commit_player_data(player, data)

            # 1. ListSkills 返回 loadout 快照、互斥组、定向策略和装配态。
            initial = await rpc("SceneSkillClientPlayer", "ListSkills")
            assert not initial.error_message.id
            assert initial.HasField("loadout")
            assert not initial.loadout.custom_enabled
            assert initial.loadout.active_scheme_id == 1
            assert initial.loadout.state_version == 1
            assert (
                initial.loadout.max_passive_slots,
                initial.loadout.max_pursuit_slots,
                initial.loadout.max_opening_slots,
            ) == (3, 1, 1)
            assert len(initial.loadout.schemes) == 2
            by_id = {e.skill_table_id: e for e in initial.catalog}
            assert by_id[12101].exclusive_group == 1 and by_id[12102].exclusive_group == 1
            assert by_id[12101].equipped and not by_id[12102].equipped  # 兼容态同互斥组按 ID 升序取首条
            assert any(e.secondary_target_policy > 0 for e in initial.catalog)

            # 2. 边界拦截：槽位超限(7023)、互斥组冲突(7024)、未修习(7025)、非法方案/标签(7026)、过期版本(7027)。
            too_many = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=1,
                passive_skill_ids=[12001, 12002, 12003, 12004],
                state_version=1, request_id="loadout-too-many-0001",
            )
            assert too_many.error_message.id == 7023

            conflict = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=1,
                passive_skill_ids=[12101, 12102],
                state_version=1, request_id="loadout-conflict-0001",
            )
            assert conflict.error_message.id == 7024

            unlearned = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=1,
                passive_skill_ids=[12005],
                state_version=1, request_id="loadout-unlearn-0001",
            )
            assert unlearned.error_message.id == 7025

            bad_scheme = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=4, role_tag=0,
                state_version=1, request_id="loadout-badscheme-01",
            )
            assert bad_scheme.error_message.id == 7026

            stale_ver = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=1,
                passive_skill_ids=[12001],
                state_version=99, request_id="loadout-stalever-001",
            )
            assert stale_ver.error_message.id == 7027

            # 3. 保存合法方案 1（12001 物攻 + 12003 防御 + 12101 灵法 / 13101 追击 / 14101 开场）。
            saved = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=1,
                passive_skill_ids=[12101, 12003, 12001],
                pursuit_skill_ids=[13101],
                opening_skill_ids=[14101],
                state_version=1, request_id="loadout-valid-scheme1-01",
            )
            assert not saved.error_message.id and not saved.replayed
            assert saved.state.loadout.custom_enabled and saved.state.loadout.active_scheme_id == 1
            assert saved.state.loadout.state_version == 2
            assert list(saved.state.loadout.schemes[0].passive_skill_ids) == [12001, 12003, 12101]

            # 幂等重放与参数冲突检查。
            replay = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=1,
                passive_skill_ids=[12001, 12003, 12101],
                pursuit_skill_ids=[13101],
                opening_skill_ids=[14101],
                state_version=1, request_id="loadout-valid-scheme1-01",
            )
            assert not replay.error_message.id and replay.replayed
            bad_replay = await rpc(
                "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=2, role_tag=1,
                state_version=1, request_id="loadout-valid-scheme1-01",
            )
            assert bad_replay.error_message.id == 1005

            # 仅已装配的被动生效于二级属性与战斗快照，未装配的 12002/12004/12102 不入战。
            cur_data = scene.rows.assemble(player.snapshot)
            stats_s1 = player_stats(protocol, tables, cur_data)
            snap = protocol.message("BattlePlayerSnapshot")
            battle_skills(tables, cur_data, snap)
            assert 10002 in snap.skill_ranks and 10002 in snap.skill_table_ids
            assert 12001 in snap.skill_ranks and 12003 in snap.skill_ranks and 12101 in snap.skill_ranks
            assert 13101 in snap.skill_ranks and 14101 in snap.skill_ranks
            assert 12002 not in snap.skill_ranks and 12004 not in snap.skill_ranks and 12102 not in snap.skill_ranks
            assert skills.is_skill_equipped(player, 12001) and not skills.is_skill_equipped(player, 12002)

            # 4. SwitchSkillLoadout 切至空白方案 2 后被动加成归零，且同键重放幂等。
            sw = await rpc(
                "SceneSkillClientPlayer", "SwitchSkillLoadout",
                scheme_id=2, state_version=2, request_id="loadout-switch-s2-0001",
            )
            assert not sw.error_message.id and not sw.replayed
            assert sw.state.loadout.active_scheme_id == 2 and sw.state.loadout.state_version == 3
            stats_s2 = player_stats(protocol, tables, scene.rows.assemble(player.snapshot))
            assert stats_s2.physical_attack < stats_s1.physical_attack
            assert stats_s2.defense < stats_s1.defense

            sw_rep = await rpc(
                "SceneSkillClientPlayer", "SwitchSkillLoadout",
                scheme_id=2, state_version=2, request_id="loadout-switch-s2-0001",
            )
            assert not sw_rep.error_message.id and sw_rep.replayed

            # 5. 启用自定义方案 2（当前为空）时，新学非主动战法 14001 会自动装配进当前方案。
            learned = await rpc(
                "SceneSkillClientPlayer", "UpgradeSkill",
                skill_table_id=14001, expected_rank=0, count=1,
            )
            assert not learned.error_message.id and learned.upgraded_count == 1
            assert 14001 in list(learned.state.loadout.schemes[1].opening_skill_ids)
        finally:
            await skills.close()
            await scene.close()

    asyncio.run(exercise())


# ── 2. 宝宝条目多方案切换（500）与五阶灵阶突破（501） ───────────────────────────────────────────────


def test_pet_entry_presets_500_and_tier_ascension_501(tmp_path):
    async def exercise():
        protocol, _, scene, assets, _, _, _, _, rpc = await _make_scene(tmp_path, level=60, gold=20_000)
        try:
            assert protocol.message_ids["ScenePetClientPlayerSwitchPetEntryPreset"] == 500
            assert protocol.message_ids["ScenePetClientPlayerAscendPetTier"] == 501

            bought = await rpc("ScenePetClientPlayer", "BuyPet", pet_table_id=1, purchase_sequence=1, price_gold=1000)
            assert not bought.error_message.id
            pet_id = bought.pet_id
            pet_info = bought.pets.pets[0]
            assert pet_info.cultivation_tier == 1
            # 初始缺少突破材料 3406 -> next_tier 展示清晰前置与 block_tip_id=26022。
            assert pet_info.next_tier.next_tier == 2
            assert pet_info.next_tier.next_tier_name == "变异"
            assert pet_info.next_tier.cost_item_id == 3406 and pet_info.next_tier.cost_item_count == 10
            assert pet_info.next_tier.bonus_entry_points == 2
            assert not pet_info.next_tier.can_ascend and pet_info.next_tier.block_tip_id == PET_TIER_MATERIAL_NOT_ENOUGH

            # 1. 条目多方案切换（500）：在槽 1 分配 -> 切至槽 2（tag=2 控场）-> 槽 2 分配 -> 切回槽 1（保持原标签）。
            s1 = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=1, config_version=1,
                target={501: 12, 502: 8}, request_id="pet-preset-save-slot1",
            )
            assert not s1.error_message.id and s1.entries.active_preset_slot == 1
            assert len(s1.entries.presets) == 3 and s1.entries.presets[0].used_points == 20

            bad_slot = await rpc(
                "ScenePetClientPlayer", "SwitchPetEntryPreset",
                pet_id=pet_id, slot_id=4, tag=1, state_version=2, config_version=1,
                request_id="pet-preset-bad-slot01",
            )
            assert bad_slot.error_message.id == PET_ENTRY_PRESET_INVALID

            sw2 = await rpc(
                "ScenePetClientPlayer", "SwitchPetEntryPreset",
                pet_id=pet_id, slot_id=2, tag=2, state_version=2, config_version=1,
                request_id="pet-preset-switch-s02",
            )
            assert not sw2.error_message.id and not sw2.replayed
            assert sw2.entries.active_preset_slot == 2 and sw2.entries.state_version == 3
            assert sw2.entries.remaining_points == 20
            assert sw2.entries.presets[0].used_points == 20 and sw2.entries.presets[1].tag == 2

            # 幂等重放。
            sw2_rep = await rpc(
                "ScenePetClientPlayer", "SwitchPetEntryPreset",
                pet_id=pet_id, slot_id=2, tag=2, state_version=2, config_version=1,
                request_id="pet-preset-switch-s02",
            )
            assert not sw2_rep.error_message.id and sw2_rep.replayed

            # 槽 2 保存点位后切回槽 1（tag=0xFFFFFFFF 保持槽 1 原标签 0）。
            s2 = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=3, config_version=1,
                target={511: 10}, request_id="pet-preset-save-slot2",
            )
            assert not s2.error_message.id and s2.entries.presets[1].used_points == 10

            sw1 = await rpc(
                "ScenePetClientPlayer", "SwitchPetEntryPreset",
                pet_id=pet_id, slot_id=1, tag=0xFFFFFFFF, state_version=4, config_version=1,
                request_id="pet-preset-switch-s01",
            )
            assert not sw1.error_message.id and sw1.entries.active_preset_slot == 1
            assert sw1.entries.effects.physical_damage_bps == 600 and sw1.entries.presets[0].tag == 0
            assert sw1.entries.presets[1].used_points == 10

            # 2. 灵阶突破（501）：缺材料拒绝 -> 等级不足拒绝 -> 备齐材料与等级后逐阶突破至 5 阶圣兽。
            no_mat = await rpc(
                "ScenePetClientPlayer", "AscendPetTier",
                pet_id=pet_id, expected_tier=1, client_request_id=9001,
            )
            assert no_mat.error_message.id == PET_TIER_MATERIAL_NOT_ENOUGH

            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            data.pet_component.pets[0].level = 5
            _add_bag_item(protocol, assets, data, item_id=70001, config_id=3406, size=200)
            await scene.commit_player_data(player, data)

            low_lvl = await rpc(
                "ScenePetClientPlayer", "AscendPetTier",
                pet_id=pet_id, expected_tier=1, client_request_id=9002,
            )
            assert low_lvl.error_message.id == PET_TIER_LEVEL_NOT_ENOUGH

            data = scene.rows.assemble(player.snapshot)
            data.pet_component.pets[0].level = 60
            await scene.commit_player_data(player, data)

            stale_tier = await rpc(
                "ScenePetClientPlayer", "AscendPetTier",
                pet_id=pet_id, expected_tier=2, client_request_id=9003,
            )
            assert stale_tier.error_message.id == PET_TIER_CHANGED

            # 1 -> 2（扣 10 残页 + 500 银两，预算 20 -> 22）。
            asc2 = await rpc(
                "ScenePetClientPlayer", "AscendPetTier",
                pet_id=pet_id, expected_tier=1, client_request_id=9004,
            )
            assert not asc2.error_message.id and not asc2.replayed and asc2.new_tier == 2
            assert asc2.gold == 19_000 - 500
            assert asc2.pets.pets[0].cultivation_tier == 2
            assert asc2.pets.pets[0].entries.total_points == 22

            # 幂等重放不重复扣钱扣料。
            asc2_rep = await rpc(
                "ScenePetClientPlayer", "AscendPetTier",
                pet_id=pet_id, expected_tier=1, client_request_id=9004,
            )
            assert not asc2_rep.error_message.id and asc2_rep.replayed and asc2_rep.new_tier == 2
            assert asc2_rep.gold == 18_500

            # 连续突破 2->3, 3->4, 4->5（圣兽），满阶后预算 28，next_tier 清空且再突报 26019。
            for idx, cur_tier in enumerate((2, 3, 4), start=1):
                res = await rpc(
                    "ScenePetClientPlayer", "AscendPetTier",
                    pet_id=pet_id, expected_tier=cur_tier, client_request_id=9010 + idx,
                )
                assert not res.error_message.id and res.new_tier == cur_tier + 1
            listed = await rpc("ScenePetClientPlayer", "GetPetList")
            top = listed.pets.pets[0]
            assert top.cultivation_tier == 5 and top.cultivation_tier_name == "圣兽"
            assert top.entries.total_points == 28 and top.next_tier.next_tier == 0

            maxed = await rpc(
                "ScenePetClientPlayer", "AscendPetTier",
                pet_id=pet_id, expected_tier=5, client_request_id=9020,
            )
            assert maxed.error_message.id == PET_TIER_MAX_REACHED
        finally:
            await scene.close()

    asyncio.run(exercise())


# ── 3. 装备强化 T/B 隔离、+11 保底、解封剥离符、副套继承符、账号级阶段一 60 配额确认 ───────────────────


def test_equip_enhance_tb_isolation_unseal_inherit_and_stage1_confirm(tmp_path):
    async def exercise():
        day_ms = 1_767_320_000_000
        now = [day_ms]
        protocol, _, scene, assets, _, _, craft, _, rpc = await _make_scene(tmp_path, level=60, gold=50_000, now_ref=now)
        try:
            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            # 放入两件未绑定 V1 1级定制武器（3101）：#80001 与 #80002，以及 2 颗绑定百炼石（3002）。
            _add_bag_item(protocol, assets, data, item_id=80001, config_id=3101, bound=False, equip=True)
            _add_bag_item(protocol, assets, data, item_id=80002, config_id=3101, bound=False, equip=True)
            _add_bag_item(protocol, assets, data, item_id=80010, config_id=ENHANCEMENT_STONE_ID, size=2, bound=True)
            await scene.commit_player_data(player, data)

            # 1. GetBag 预览：未绑定装备仅剩绑定强化石时标 will_bind_on_enhance=True、tradable_material_available=0。
            bag0 = await rpc("SceneBagClientPlayer", "GetBag", bag_type=0)
            w1 = next(it for it in bag0.bag.items if it.item_id == 80001)
            assert w1.enhance_next.material_config_id == ENHANCEMENT_STONE_ID
            assert w1.enhance_next.material_count == 2
            assert w1.enhance_next.tradable_material_available == 0
            assert w1.enhance_next.will_bind_on_enhance is True

            # 未确认 allow_bound_materials=False 时拒回 28027，装备仍未绑。
            blocked = await rpc(
                "SceneBagClientPlayer", "EnhanceEquip",
                item_id=80001, client_request_id=6001, expected_level=0, allow_bound_materials=False,
            )
            assert blocked.error_message.id == EQUIP_BIND_CONFIRM_REQUIRED

            # 确认 allow_bound_materials=True 后强化 +1 成功并传染为 bound=True。
            bound_enh = await rpc(
                "SceneBagClientPlayer", "EnhanceEquip",
                item_id=80001, client_request_id=6002, expected_level=0, allow_bound_materials=True,
            )
            assert not bound_enh.error_message.id and bound_enh.success and bound_enh.enhance_level == 1
            w1_after = next(it for it in bound_enh.bag.items if it.item_id == 80001)
            assert w1_after.bound is True

            # 2. 纯 T 隔离：同时持有充足可交易石 + 绑定石，即使透传 allow_bound_materials=True 也不误绑 #80002。
            data = scene.rows.assemble(player.snapshot)
            _add_bag_item(protocol, assets, data, item_id=80011, config_id=ENHANCEMENT_STONE_ID, size=2, bound=False)
            _add_bag_item(protocol, assets, data, item_id=80012, config_id=ENHANCEMENT_STONE_ID, size=2, bound=True)
            await scene.commit_player_data(player, data)

            bag1 = await rpc("SceneBagClientPlayer", "GetBag", bag_type=0)
            w2 = next(it for it in bag1.bag.items if it.item_id == 80002)
            assert w2.enhance_next.tradable_material_available == 2 and not w2.enhance_next.will_bind_on_enhance

            pure_enh = await rpc(
                "SceneBagClientPlayer", "EnhanceEquip",
                item_id=80002, client_request_id=6003, expected_level=0, allow_bound_materials=True,
            )
            assert not pure_enh.error_message.id and pure_enh.enhance_level == 1
            w2_after = next(it for it in pure_enh.bag.items if it.item_id == 80002)
            assert w2_after.bound is False

            # 3. +10 -> +11 保底机制与 expected_pity_count 乐观校验。
            data = scene.rows.assemble(player.snapshot)
            for entry, it in assets.entries(data.bag_component, 0):
                if it.item_id == 80001:
                    it.equip.enhance_level = 10
                    it.equip.enhance_pity_count = 19
                    it.equip.fixed_affixes.add(slot=1, affix_id=101)
                    it.equip.crafter_name = "陆大师"
                    it.equip.crafter_player_id = 99
                    assets.write_entry(entry, it)
            _add_bag_item(protocol, assets, data, item_id=80013, config_id=ENHANCEMENT_STONE_ID, size=100, bound=True)
            await scene.commit_player_data(player, data)

            bag_pity = await rpc("SceneBagClientPlayer", "GetBag", bag_type=0)
            w1_pity = next(it for it in bag_pity.bag.items if it.item_id == 80001)
            assert w1_pity.enhance_next.pity_limit == 20 and w1_pity.enhance_next.pity_guaranteed_next

            stale_pity = await rpc(
                "SceneBagClientPlayer", "EnhanceEquip",
                item_id=80001, client_request_id=6004, expected_level=10, expected_pity_count=18,
            )
            assert stale_pity.error_message.id == EQUIP_LEVEL_CHANGED

            pity_ok = await rpc(
                "SceneBagClientPlayer", "EnhanceEquip",
                item_id=80001, client_request_id=6005, expected_level=10, expected_pity_count=19,
            )
            assert not pity_ok.error_message.id and pity_ok.success and pity_ok.enhance_level == 11

            # 4. 流派副套继承符（INHERIT_ENHANCE=6, 3410）：#80002 从 #80001 无损继承 +11 并转为绑定。
            rules = await rpc("SceneBagClientPlayer", "GetEquipmentCraftRules")
            data = scene.rows.assemble(player.snapshot)
            _add_bag_item(protocol, assets, data, item_id=80020, config_id=INHERIT_TALISMAN_ID, size=1, bound=True)
            _add_bag_item(protocol, assets, data, item_id=80021, config_id=UNSEAL_TALISMAN_ID, size=1, bound=True)
            await scene.commit_player_data(player, data)

            inherit_choice = protocol.message(
                "EquipmentCraftChoice", operation=6, item_id=80002, source_item_id=80001,
            )
            prev_inh = await rpc("SceneBagClientPlayer", "PreviewEquipmentCraft", choice=inherit_choice)
            assert not prev_inh.error_message.id and prev_inh.preview.available
            assert prev_inh.preview.will_bind_item and prev_inh.preview.result.enhance_level == 11

            comm_inh = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=inherit_choice, rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=0, client_request_id=6010,
            )
            assert not comm_inh.error_message.id and comm_inh.outcome == 2
            w2_inh = next(it for it in comm_inh.bag.items if it.item_id == 80002)
            assert w2_inh.enhance_level == 11 and w2_inh.bound is True

            # 5. 解封剥离符（UNSEAL_TO_TRADABLE=5, 3409）：消耗绑定解封符清空 #80001 绑定养成，恢复纯 T 原胚。
            unseal_choice = protocol.message("EquipmentCraftChoice", operation=5, item_id=80001)
            comm_unseal = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=unseal_choice, rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=1, client_request_id=6011,
            )
            assert not comm_unseal.error_message.id and comm_unseal.outcome == 2
            w1_unsealed = next(it for it in comm_unseal.bag.items if it.item_id == 80001)
            assert w1_unsealed.bound is False and w1_unsealed.enhance_level == 0
            assert w1_unsealed.crafter_name == "" and w1_unsealed.crafter_player_id == 0
            assert all(not slot.HasField("selected") for slot in w1_unsealed.fixed_affix_slots)

            # 6. 账号级阶段一 60 可交易配额（stage1_tradable_confirmed）fail-closed 拦截与确认后分堆发放。
            sync_calls = []
            craft.set_account_quota_sync_transport(lambda pid, desired, ts: sync_calls.append((pid, desired)))
            data = scene.rows.assemble(player.snapshot)
            added_total, added_tradable = craft.credit_battle_material_reward_v2(
                1, is_win=True, now_ms_val=day_ms, data=data,
            )
            assert (added_total, added_tradable) == (10, 1)
            await scene.commit_player_data(player, data)

            # 未确认 stage1_tradable_confirmed 前，含可交易批次拒绝发放（fail-closed）并触发异步同步。
            unconfirmed = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=6020, expected_revision=0,
                crafting_material_count=10, enhancement_stone_count=0,
            )
            assert unconfirmed.error_message.id == 1003 and unconfirmed.outcome == 0
            assert sync_calls == [(1, 1)]

            # 账号目录回执确认 stage1_tradable_confirmed=1 后，成功分堆发放：1 可交易 + 9 绑定，绝不混堆。
            data = scene.rows.assemble(player.snapshot)
            assert craft.apply_account_quota_sync_reply(
                1, data, desired_char_total=1, found=True, account_used_total=1, confirmed_char_total=1,
            )
            await scene.commit_player_data(player, data)

            claimed = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=6021, expected_revision=0,
                crafting_material_count=10, enhancement_stone_count=0,
            )
            assert not claimed.error_message.id and claimed.outcome == 2
            saved_data = scene.rows.assemble(player.snapshot)
            mat_stacks = [
                (it.size, it.bound) for _, it in assets.entries(saved_data.bag_component, 0)
                if it.config_id == CRAFTING_MATERIAL_ID
            ]
            assert sorted(mat_stacks) == [(1, False), (9, True)]
            assert saved_data.client_request_ledger.material_reward.stage1_tradable_confirmed == 1

            # 7. 阶段二（lifetime_earned=660）：每周 150 份池，无轻享卡全绑定，有轻享卡（afk_pass_expires_at）胜局解锁 1 份可交易。
            data = scene.rows.assemble(player.snapshot)
            data.client_request_ledger.material_reward.lifetime_earned = 660
            data.uint64_pb_component.afk_pass_expires_at = 0
            t_no_card, tr_no_card = craft.credit_battle_material_reward_v2(
                1, is_win=True, now_ms_val=day_ms, data=data, light_card_active=False,
            )
            assert (t_no_card, tr_no_card) == (10, 0)
            data.uint64_pb_component.afk_pass_expires_at = (day_ms // 1000) + 3600
            t_card, tr_card = craft.credit_battle_material_reward_v2(
                1, is_win=True, now_ms_val=day_ms, data=data, light_card_active=True,
            )
            assert (t_card, tr_card) == (10, 1)
            await scene.commit_player_data(player, data)
            mr_s2 = (await rpc("SceneBagClientPlayer", "GetEquipmentMaterialReward")).snapshot
            assert mr_s2.weekly_phase_active and mr_s2.weekly_limit == 150 and mr_s2.earned_this_week == 20
        finally:
            await scene.close()

    asyncio.run(exercise())


# ── 4. 论剑 V3 PrepareBattle 快照：备选灵宠（至多 5 只）、当季全部赛季印与生效战法方案 ────────────────


def test_prepare_battle_v3_standby_pets_available_seals_and_active_loadout(tmp_path):
    async def exercise():
        now = [1_760_000_000_000]
        protocol, _, scene, _, skills, _, _, battle, rpc = await _make_scene(tmp_path, level=60, gold=50_000, now_ref=now)
        try:
            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            # 装备自定义战法方案 1：仅装配 12001，不装配 12002。
            for sid in (10002, 12001, 12002):
                data.skill_list.skill_list.add(skill_table_id=sid, rank=1)
            scheme = data.skill_list.loadout_schemes.add(scheme_id=1, role_tag=1)
            scheme.passive_skill_ids.append(12001)
            data.skill_list.active_loadout_scheme_id = 1

            # 配置 7 只灵宠：1 只出战 + 6 只备选（其中 1 只场外 0 血，竞技隔离态应满血打包且上限 5 只）。
            data.pet_component.active_pet_id = 5001
            for idx in range(7):
                pet = data.pet_component.pets.add(
                    pet_id=5001 + idx,
                    pet_table_id=1 + (idx % 2),
                    level=30,
                    health=0 if idx == 2 else 300,
                    mana=100,
                    cultivation_tier=1,
                )
                pet.entry_allocated[501] = 10
            await scene.commit_player_data(player, data)

            # 正式自养论剑（ranked_context.season_id=1, use_free_preset=False）：打包至多 5 只备选灵宠 + 当季全部赛季印。
            req_ranked = protocol.message(
                "PrepareBattleRequest",
                player_id=1, battle_id=901, battle_node_id=1,
                deadline_ms=now[0] + 60_000, prepare_deadline_ms=now[0] + 30_000,
                match_mode=3,
                ranked_context={"season_id": 1, "use_free_preset": False},
            )
            resp_ranked = await battle.prepare(req_ranked)
            assert not resp_ranked.error_message.id
            snap = resp_ranked.snapshot
            assert [p.pet_id for p in snap.pets] == [5001]
            assert snap.pets[0].pet_cultivation.physical_damage_bps == 500
            assert [p.pet_id for p in snap.standby_pets] == [5002, 5003, 5004, 5005, 5006]
            assert snap.standby_pets[1].base_attributes.health == snap.standby_pets[1].max_health > 0  # 0 血备宠竞技隔离态满血
            assert [s.seal_id for s in snap.available_season_seals] == [101, 102, 103, 104]
            assert 10002 in snap.skill_ranks and 12001 in snap.skill_ranks and 12002 not in snap.skill_ranks

            await battle.cancel(protocol.message("CancelBattlePrepareRequest", player_id=1, battle_id=901))

            # 免费公平预设或普通 PVE：不打包备选灵宠与当季全部赛季印。
            req_free = protocol.message(
                "PrepareBattleRequest",
                player_id=1, battle_id=902, battle_node_id=1,
                deadline_ms=now[0] + 60_000, prepare_deadline_ms=now[0] + 30_000,
                match_mode=3,
                ranked_context={"season_id": 1, "use_free_preset": True},
            )
            resp_free = await battle.prepare(req_free)
            assert not resp_free.error_message.id
            assert len(resp_free.snapshot.standby_pets) == 0
            assert len(resp_free.snapshot.available_season_seals) == 0

            await battle.cancel(protocol.message("CancelBattlePrepareRequest", player_id=1, battle_id=902))

            # 赛季锦标赛（MATCH_MODE_SEASON_TOURNAMENT=11）同样打包备选灵宠与当季全部赛季印。
            req_tour = protocol.message(
                "PrepareBattleRequest",
                player_id=1, battle_id=903, battle_node_id=1,
                deadline_ms=now[0] + 60_000, prepare_deadline_ms=now[0] + 30_000,
                match_mode=11,
            )
            resp_tour = await battle.prepare(req_tour)
            assert not resp_tour.error_message.id
            assert len(resp_tour.snapshot.standby_pets) == 5
            assert [s.seal_id for s in resp_tour.snapshot.available_season_seals] == [101, 102, 103, 104]
        finally:
            await skills.close()
            await battle.close()
            await scene.close()

    asyncio.run(exercise())
