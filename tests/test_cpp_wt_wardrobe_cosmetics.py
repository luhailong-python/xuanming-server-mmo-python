"""衣柜灵宠幻化与技能特效（530/531）及百宝阁外观券（303/304）端到端闭环测试。"""

from __future__ import annotations

import asyncio

from test_cpp_wt_spirit_shop import _build_buy_req_from_info, _make_scene_harness
from xuanming.data.asset_inventory import INVALID
from xuanming.nodes.scene_appearance import (
    APPEARANCE_INVALID,
    APPEARANCE_NO_CARD,
    APPEARANCE_NOT_CARD_UNLOCKABLE,
    APPEARANCE_NOT_OWNED,
    PET_NOT_FOUND,
    SceneAppearance,
    fill_battle_skill_vfx,
    resolve_pet_cosmetic_visual,
)
from xuanming.nodes.scene_pets import build_single_pet_battle_snapshot


async def _dispatch_service(scene, player, service: str, method: str, request):
    route = next(
        r for r in scene.protocol.routes.values()
        if r.service == service and r.method == method
    )
    handler = scene._handlers[service][method]
    world = scene.locations[player.player_id]
    res = handler(world, player, route, request)
    if asyncio.iscoroutine(res):
        res = await res
    return res


def test_wardrobe_cosmetics_spirit_shop_voucher_unlock_and_battle_snapshot(tmp_path):
    async def exercise():
        protocol, tables, scene, assets, shop, player, _ = await _make_scene_harness(
            tmp_path, diamond=500, bind_diamond=200,
        )
        appearance = SceneAppearance(scene, tables, assets, now_s=lambda: 1_800_000_000)
        appearance.register()
        scene.appearance = appearance
        try:
            assert appearance.validate_tables() is True
            assert protocol.message_ids["SceneAppearanceClientPlayerEquipPetCosmetic"] == 530
            assert protocol.message_ids["SceneAppearanceClientPlayerEquipSkillVfx"] == 531

            # 准备 1 只灵宠与已习得技能 1201/1203
            data = scene.rows.assemble(player.snapshot)
            setattr(data.uint32_pb_component, "class", 2)
            data.level_component.level = 30
            pet = data.pet_component.pets.add(pet_id=71001, pet_table_id=6, level=20, health=100, mana=50)
            data.pet_component.active_pet_id = pet.pet_id
            data.skill_list.skill_list.add(skill_table_id=1201, rank=1)
            data.skill_list.skill_list.add(skill_table_id=1203, rank=1)
            await scene.commit_player_data(player, data)

            # 1. 无券时：expect_free_equip=True 拒回 NOT_OWNED，False 拒回 NO_CARD；不存在宠物拒回 PET_NOT_FOUND
            r_not_owned = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic",
                protocol.message(
                    "EquipPetCosmeticRequest",
                    client_request_id=8001, pet_id=71001, cosmetic_id=1, expect_free_equip=True,
                ),
            )
            assert r_not_owned.error_message.id == APPEARANCE_NOT_OWNED

            r_no_card = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic",
                protocol.message(
                    "EquipPetCosmeticRequest",
                    client_request_id=8002, pet_id=71001, cosmetic_id=1, expect_free_equip=False,
                ),
            )
            assert r_no_card.error_message.id == APPEARANCE_NO_CARD

            r_missing_pet = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic",
                protocol.message(
                    "EquipPetCosmeticRequest",
                    client_request_id=8003, pet_id=99999, cosmetic_id=1, expect_free_equip=False,
                ),
            )
            assert r_missing_pet.error_message.id == PET_NOT_FOUND

            # 2. 在灵石百宝阁购买 303(灵宠幻化符, 120灵石) 与 304(技能特效卷, 180灵石)
            shop_info_resp = await _dispatch_service(
                scene, player, "SceneSpiritShopClientPlayer", "GetSpiritShopInfo",
                protocol.message("GetSpiritShopInfoRequest"),
            )
            assert not shop_info_resp.error_message.id
            buy_pet_card = await _dispatch_service(
                scene, player, "SceneSpiritShopClientPlayer", "BuySpiritShopGoods",
                _build_buy_req_from_info(protocol, shop_info_resp.info, 303, 1, 8101),
            )
            assert not buy_pet_card.error_message.id
            assert buy_pet_card.info.diamond_balance == 380

            buy_vfx_card = await _dispatch_service(
                scene, player, "SceneSpiritShopClientPlayer", "BuySpiritShopGoods",
                _build_buy_req_from_info(protocol, buy_pet_card.info, 304, 1, 8102),
            )
            assert not buy_vfx_card.error_message.id
            assert buy_vfx_card.info.diamond_balance == 200

            # 3. 查询衣柜：可见 3 项幻化 + 3 项特效、券余量与百宝阁商品 ID
            app_info = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "GetAppearanceInfo",
                protocol.message("GetAppearanceInfoRequest"),
            )
            assert not app_info.error_message.id
            assert len(app_info.info.pet_cosmetics) == 3
            assert app_info.info.pet_cosmetics[0].card_item_id == 3411
            assert app_info.info.pet_cosmetics[0].owned_card_count == 1
            assert app_info.info.pet_cosmetics[0].spirit_shop_goods_id == 303
            assert app_info.info.pet_cosmetics[2].card_item_id == 0
            assert len(app_info.info.skill_vfx_cosmetics) == 3
            assert app_info.info.skill_vfx_cosmetics[0].card_item_id == 3412
            assert app_info.info.skill_vfx_cosmetics[0].owned_card_count == 1
            assert app_info.info.skill_vfx_cosmetics[0].spirit_shop_goods_id == 304

            # 4. 专属外观 #3 拒绝耗券解锁；#1 原子耗券解锁并穿戴 + 同键幂等重放 + 换参拒回
            r_excl_pet = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic",
                protocol.message(
                    "EquipPetCosmeticRequest",
                    client_request_id=8201, pet_id=71001, cosmetic_id=3, expect_free_equip=False,
                ),
            )
            assert r_excl_pet.error_message.id == APPEARANCE_NOT_CARD_UNLOCKABLE

            unlock_pet_req = protocol.message(
                "EquipPetCosmeticRequest",
                client_request_id=8202, pet_id=71001, cosmetic_id=1, expect_free_equip=False,
            )
            r_unlock_pet = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic", unlock_pet_req,
            )
            assert not r_unlock_pet.error_message.id and not r_unlock_pet.replayed
            assert r_unlock_pet.equipped_cosmetic_id == 1
            assert r_unlock_pet.info.pet_cosmetics[0].owned is True
            assert r_unlock_pet.info.pet_cosmetics[0].owned_card_count == 0

            r_replay_pet = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic", unlock_pet_req,
            )
            assert not r_replay_pet.error_message.id and r_replay_pet.replayed

            r_mismatch_pet = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic",
                protocol.message(
                    "EquipPetCosmeticRequest",
                    client_request_id=8202, pet_id=71001, cosmetic_id=2, expect_free_equip=False,
                ),
            )
            assert r_mismatch_pet.error_message.id == INVALID

            # 5. 技能特效专属拦截、技能不匹配拦截、原子耗券解锁 + 幂等重放
            r_excl_vfx = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipSkillVfx",
                protocol.message(
                    "EquipSkillVfxRequest",
                    client_request_id=8301, skill_table_id=1203, cosmetic_id=3, expect_free_equip=False,
                ),
            )
            assert r_excl_vfx.error_message.id == APPEARANCE_NOT_CARD_UNLOCKABLE

            r_bad_skill = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipSkillVfx",
                protocol.message(
                    "EquipSkillVfxRequest",
                    client_request_id=8302, skill_table_id=1202, cosmetic_id=1, expect_free_equip=False,
                ),
            )
            assert r_bad_skill.error_message.id == APPEARANCE_INVALID

            unlock_vfx_req = protocol.message(
                "EquipSkillVfxRequest",
                client_request_id=8303, skill_table_id=1201, cosmetic_id=1, expect_free_equip=False,
            )
            r_unlock_vfx = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipSkillVfx", unlock_vfx_req,
            )
            assert not r_unlock_vfx.error_message.id and not r_unlock_vfx.replayed
            assert r_unlock_vfx.equipped_cosmetic_id == 1
            assert r_unlock_vfx.info.skill_vfx_cosmetics[0].owned is True
            assert r_unlock_vfx.info.skill_vfx_cosmetics[0].owned_card_count == 0
            assert r_unlock_vfx.info.equipped_skill_vfx[1201] == 1

            r_replay_vfx = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipSkillVfx", unlock_vfx_req,
            )
            assert not r_replay_vfx.error_message.id and r_replay_vfx.replayed

            # 6. 验证灵宠与战斗快照透传（零数值改动）
            latest = scene.rows.assemble(player.snapshot)
            cid, art = resolve_pet_cosmetic_visual(
                tables, bytes(latest.profile_component.pet_cosmetic_owned_bits), latest.pet_component.pets[0],
            )
            assert (cid, art) == (1, "07-cangzhanglin")

            pet_snap = protocol.message("BattlePetSnapshot")
            assert build_single_pet_battle_snapshot(
                tables, player.player_id, latest.pet_component.pets[0], 30,
                full_resources=True, out=pet_snap,
                pet_owned_bits=bytes(latest.profile_component.pet_cosmetic_owned_bits),
            )
            assert pet_snap.cosmetic_id == 1 and pet_snap.cosmetic_art_key == "07-cangzhanglin"

            battle_snap = protocol.message("BattlePlayerSnapshot")
            fill_battle_skill_vfx(tables, latest, battle_snap)
            assert battle_snap.equipped_skill_vfx[1201] == 1

            # 7. 恢复原形与默认特效
            r_clear_pet = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipPetCosmetic",
                protocol.message(
                    "EquipPetCosmeticRequest",
                    client_request_id=8401, pet_id=71001, cosmetic_id=0, expect_free_equip=True,
                ),
            )
            assert not r_clear_pet.error_message.id and r_clear_pet.equipped_cosmetic_id == 0

            r_clear_vfx = await _dispatch_service(
                scene, player, "SceneAppearanceClientPlayer", "EquipSkillVfx",
                protocol.message(
                    "EquipSkillVfxRequest",
                    client_request_id=8402, skill_table_id=1201, cosmetic_id=0, expect_free_equip=True,
                ),
            )
            assert not r_clear_vfx.error_message.id and r_clear_vfx.equipped_cosmetic_id == 0
        finally:
            await assets.close()
            await scene.close()

    asyncio.run(exercise())
