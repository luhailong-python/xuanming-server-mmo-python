import asyncio
from itertools import count

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_equip_craft import (
    CRAFTING_MATERIAL_ID,
    DAY_MS,
    DAY_SHIFT_MS,
    EQUIP_LEVEL_CHANGED,
    SceneEquipCraft,
)
from xuanming.nodes.scene_pets import ScenePets
from xuanming.nodes.scene_season import (
    SEASON_CATCHUP_CURRENCY_NOT_ENOUGH,
    SEASON_CATCHUP_PRICE_CHANGED,
    SEASON_MISMATCH,
    SEASON_SEAL_ALREADY_EQUIPPED,
    SEASON_SEAL_ALREADY_MAX,
    SEASON_SEAL_INVALID,
    SceneSeason,
)
from xuanming.nodes.scene_trial_journal import (
    BOUND_ARTIFACT_VOUCHER_ITEM_ID,
    BOUND_REFORGE_SAND_ITEM_ID,
    TRIAL_JOURNAL_ARTIFACT_INVALID,
    TRIAL_JOURNAL_MILESTONE_ALREADY_CLAIMED,
    TRIAL_JOURNAL_MILESTONE_NOT_FOUND,
    TRIAL_JOURNAL_MILESTONE_NOT_REACHED,
    TRIAL_JOURNAL_VOUCHER_NOT_ENOUGH,
    SceneTrialJournal,
    has_packed_bit,
    set_packed_bit,
)
from xuanming.protocol import Protocol


async def _make_scene(tmp_path, *, level: int = 1, gold: int = 0, diamond: int = 0, now_ref=None):
    protocol = Protocol()
    tables = ConfigTables()
    store = SqlitePlayerStore(tmp_path / "players.sqlite3")
    codec = PlayerRowCodec(protocol)
    seed = await store.load_and_claim(1, b"seed")
    data = protocol.message("player_database", player_id=1)
    data.level_component.level = level
    data.currency.values.extend([gold, diamond, 0])
    data.pet_component.SetInParent()
    data.season_component.SetInParent()
    data.trial_journal_component.SetInParent()
    await store.commit(seed, codec.project(data), release=True)

    node = protocol.message("NodeInfo", node_id=5, node_uuid="progression-scene", zone_id=1)
    scene = SceneNode(protocol, node, store, codec, lambda *_: None)
    scene.tables = tables
    guids = count(10_000)

    async def alloc_guid():
        return next(guids)

    assets = SceneAssets(scene, tables, {}, allow_gm=True, allocate_item_id=alloc_guid)
    scene.assets = assets
    clock = (lambda: now_ref[0]) if now_ref is not None else (lambda: 1_760_000_000_000)

    season = SceneSeason(scene, tables, assets, clock=clock)
    journal = SceneTrialJournal(scene, tables, assets, clock=clock)
    craft = SceneEquipCraft(scene, tables, assets, clock=clock)
    pets = ScenePets(scene, tables)
    assets.register_client_handlers()
    season.register()
    journal.register()
    craft.register()
    pets.register()

    scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))
    await scene.enter(
        protocol.message(
            "PlayerEnterGameNodeRequest",
            player_id=1,
            session_id=7,
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
            session_id=7,
            player_id=1,
            message_content=protocol.content(msg_id, 1, body=body),
        )
        res = await scene.process(req, "gate")
        assert not res.message_content.error_message.id
        return protocol.parse(route.response_type, res.message_content.serialized_message)

    return protocol, tables, scene, assets, season, journal, craft, pets, rpc


def test_season_state_equip_battle_progress_catchup_and_archive(tmp_path):
    async def exercise():
        now = [1_760_000_000_000]
        protocol, _, scene, _, season, _, _, _, rpc = await _make_scene(
            tmp_path, level=20, gold=500, diamond=100, now_ref=now,
        )
        try:
            assert season.validate_tables()
            assert season.active_season_id() == 1

            # 预埋一条历史赛季档案，验证当前赛季操作不破坏历史记录。
            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            data.season_component.records.add(season_id=99, equipped_seal_id=999, progress_points=7, revision=4)
            await scene.commit_player_data(player, data)

            state_resp = await rpc("SceneSeasonClientPlayer", "GetSeasonState")
            assert not state_resp.error_message.id
            st = state_resp.state
            assert st.available and st.season_id == 1 and st.equipped_seal_id == 101
            assert (st.progress_points, st.max_points) == (0, 12)
            assert (st.catchup_currency_type, st.catchup_price_per_point, st.catchup_remaining_total_price) == (1, 10, 120)
            assert st.currency_balance == 100 and st.can_switch_seal and st.can_catchup
            assert [s.seal_id for s in st.seals] == [101, 102, 103, 104]
            assert sum(1 for s in st.seals if s.equipped) == 1

            # 切印校验：错赛季、非法印、已佩戴印均拒绝。
            assert (await rpc("SceneSeasonClientPlayer", "EquipSeasonSeal", season_id=2, seal_id=102)).error_message.id == SEASON_MISMATCH
            assert (await rpc("SceneSeasonClientPlayer", "EquipSeasonSeal", season_id=1, seal_id=999)).error_message.id == SEASON_SEAL_INVALID
            assert (await rpc("SceneSeasonClientPlayer", "EquipSeasonSeal", season_id=1, seal_id=101)).error_message.id == SEASON_SEAL_ALREADY_EQUIPPED

            equipped = await rpc("SceneSeasonClientPlayer", "EquipSeasonSeal", season_id=1, seal_id=103)
            assert not equipped.error_message.id
            assert equipped.state.equipped_seal_id == 103 and equipped.state.revision == 1

            # 战斗进度累计（PVE 胜 +1、PVP 胜 +2、PVP 负 +1 => 共 4 点）与效果汇总。
            data = scene.rows.assemble(player.snapshot)
            assert season.record_battle_progress(data, match_mode=1, player_won=True) == 1
            assert season.record_battle_progress(data, match_mode=3, player_won=True) == 2
            assert season.record_battle_progress(data, match_mode=4, player_won=False) == 1
            await scene.commit_player_data(player, data)

            effects = protocol.message("EquipmentEffects")
            season.collect_effects(data, effects)
            assert effects.season_seal.seal_id == 103
            assert effects.season_max_health_bps == 100 and effects.season_defense_bps == 100

            # 快捷追赶校验：空幂等键、过期报价、余额不足。
            assert (await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=0, season_id=1, points_to_buy=2, expected_currency_type=1, expected_total_price=20,
            )).error_message.id == 1005
            assert (await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=101, season_id=1, points_to_buy=2, expected_currency_type=1, expected_total_price=99,
            )).error_message.id == SEASON_CATCHUP_PRICE_CHANGED

            # 正常追赶 3 点（花费 30 灵石）+ 同键重放幂等 + 同键改参拒绝。
            catchup1 = await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=102, season_id=1, points_to_buy=3, expected_currency_type=1, expected_total_price=30,
            )
            assert not catchup1.error_message.id and not catchup1.replayed
            assert (catchup1.added_points, catchup1.spent_currency) == (3, 30)
            assert (catchup1.state.progress_points, catchup1.state.battle_points, catchup1.state.catchup_points) == (7, 4, 3)
            assert catchup1.state.currency_balance == 70

            replay = await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=102, season_id=1, points_to_buy=3, expected_currency_type=1, expected_total_price=30,
            )
            assert not replay.error_message.id and replay.replayed
            assert (replay.added_points, replay.spent_currency) == (3, 30)
            assert replay.state.currency_balance == 70

            mismatch = await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=102, season_id=1, points_to_buy=1, expected_currency_type=1, expected_total_price=10,
            )
            assert mismatch.error_message.id == 1005

            # 一键补满剩余 5 点（花费 50 灵石）-> 达到上限 12/12，满级后再次追赶报错。
            full = await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=103, season_id=1, points_to_buy=0, expected_currency_type=1, expected_total_price=50,
            )
            assert not full.error_message.id and (full.added_points, full.spent_currency) == (5, 50)
            assert (full.state.progress_points, full.state.current_max_health_bps, full.state.current_defense_bps) == (12, 300, 300)
            assert not full.state.can_catchup

            over = await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=104, season_id=1, points_to_buy=1, expected_currency_type=1, expected_total_price=10,
            )
            assert over.error_message.id == SEASON_SEAL_ALREADY_MAX

            # 验证历史赛季 99 档案原样保留。
            saved = scene.rows.assemble(player.snapshot)
            legacy = next(r for r in saved.season_component.records if r.season_id == 99)
            assert (legacy.equipped_seal_id, legacy.progress_points, legacy.revision) == (999, 7, 4)

            # 灵石不足场景校验（切到余额 10 测新角色/减余额）。
            saved.season_component.records[1].progress_points = 0
            saved.currency.values[1] = 10
            await scene.commit_player_data(player, saved)
            poor = await rpc(
                "SceneSeasonClientPlayer", "CatchupSeasonSeal",
                client_request_id=105, season_id=1, points_to_buy=5, expected_currency_type=1, expected_total_price=50,
            )
            assert poor.error_message.id == SEASON_CATCHUP_CURRENCY_NOT_ENOUGH
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_trial_journal_progress_milestones_and_bound_artifact_voucher(tmp_path):
    async def exercise():
        _, _, scene, assets, _, journal, _, _, rpc = await _make_scene(tmp_path, level=30)
        try:
            initial = await rpc("SceneTrialJournalClientPlayer", "GetTrialJournal")
            assert not initial.error_message.id
            snap = initial.snapshot
            assert snap.available and snap.total_permanent_lit_seals == 0 and snap.total_permanent_seals == 27
            assert len(snap.chapters) == 3 and len(snap.chapters[0].entries) == 5
            assert len(snap.milestones) == 11 and len(snap.redeemable_artifacts) == 8
            assert not snap.hard_boss_trial_unlocked
            # 困难条目 103/104 未过普通前不可进入。
            by_entry = {e.entry_id: e for e in snap.chapters[0].entries}
            assert by_entry[101].can_enter and not by_entry[103].can_enter

            # 注入高位未知 bit（bit 40），验证有效计数不受影响且写回不截断高位。
            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            data.trial_journal_component.lit_seal_bits = set_packed_bit(b"", 40)

            # 推进卷一普通·回春（单人 5 回合、无首领回春治疗 -> 点亮 1011/1012/1013 共 3 印）。
            delta1 = journal.record_trial_progress(
                data,
                101,
                won=True,
                total_rounds=5,
                initial_human_count=1,
                boss_guard_heal_total=0,
                used_pet_table_id=1,
                used_artifact_effect_ids=(6, 1, 99),
                used_season_seal_id=101,
            )
            assert delta1["changed"] and delta1["new_solo_best"]
            assert delta1["newly_lit_seal_ids"] == [1011, 1012, 1013]

            # 推进卷一普通·护势（好友 3 人 6 回合、驱散护势 -> 点亮 1021/1022/1023 共 3 印）。
            delta2 = journal.record_trial_progress(
                data,
                102,
                won=True,
                total_rounds=6,
                initial_human_count=3,
                guard_stance_dispelled=True,
                used_pet_table_id=2,
                teammate_names=("青衣", "赤羽"),
            )
            assert delta2["changed"] and delta2["new_coop_best"]
            assert delta2["newly_lit_seal_ids"] == [1021, 1022, 1023]
            await scene.commit_player_data(player, data)

            updated = (await rpc("SceneTrialJournalClientPlayer", "GetTrialJournal")).snapshot
            assert updated.total_permanent_lit_seals == 6 and updated.state_revision == 2
            assert updated.hard_boss_trial_unlocked
            by_entry = {e.entry_id: e for e in updated.chapters[0].entries}
            assert by_entry[103].can_enter  # 普通通关后困难开启
            assert by_entry[101].solo_recorded and by_entry[101].solo_best.best_rounds == 5
            assert list(by_entry[101].solo_best.loadout.artifact_effect_ids) == [1, 6]
            assert by_entry[102].coop_recorded and list(by_entry[102].coop_best.teammate_names) == ["青衣", "赤羽"]

            # 里程碑边界校验：不存在 / 未达标。
            assert (await rpc(
                "SceneTrialJournalClientPlayer", "ClaimTrialJournalMilestone",
                client_request_id=200, milestone_id=999,
            )).error_message.id == TRIAL_JOURNAL_MILESTONE_NOT_FOUND
            assert (await rpc(
                "SceneTrialJournalClientPlayer", "ClaimTrialJournalMilestone",
                client_request_id=201, milestone_id=103,
            )).error_message.id == TRIAL_JOURNAL_MILESTONE_NOT_REACHED

            # 领取里程碑 101（2 份仅自用重铸砂）与 102（1 张自用法宝拓印）。
            m101 = await rpc(
                "SceneTrialJournalClientPlayer", "ClaimTrialJournalMilestone",
                client_request_id=202, milestone_id=101,
            )
            assert not m101.error_message.id and not m101.replayed
            assert m101.granted_sand_count == 2 and m101.snapshot.bound_sand_balance == 2

            m102 = await rpc(
                "SceneTrialJournalClientPlayer", "ClaimTrialJournalMilestone",
                client_request_id=203, milestone_id=102,
            )
            assert not m102.error_message.id and m102.granted_voucher_count == 1
            assert m102.snapshot.voucher_balance == 1

            # 同键重放幂等，不重复发拓印；新键重复领返回已领取。
            m102_replay = await rpc(
                "SceneTrialJournalClientPlayer", "ClaimTrialJournalMilestone",
                client_request_id=203, milestone_id=102,
            )
            assert not m102_replay.error_message.id and m102_replay.replayed
            assert m102_replay.granted_voucher_count == 1 and m102_replay.snapshot.voucher_balance == 1

            m102_again = await rpc(
                "SceneTrialJournalClientPlayer", "ClaimTrialJournalMilestone",
                client_request_id=204, milestone_id=102,
            )
            assert m102_again.error_message.id == TRIAL_JOURNAL_MILESTONE_ALREADY_CLAIMED

            # 兑换法宝：非法 ID 拒绝；兑换 3301（破阵刃）扣 1 张拓印并在背包产出 bound=True 装备实例。
            assert (await rpc(
                "SceneTrialJournalClientPlayer", "RedeemBoundArtifactVoucher",
                client_request_id=300, artifact_item_config_id=9999,
            )).error_message.id == TRIAL_JOURNAL_ARTIFACT_INVALID

            redeem = await rpc(
                "SceneTrialJournalClientPlayer", "RedeemBoundArtifactVoucher",
                client_request_id=301, artifact_item_config_id=3301,
            )
            assert not redeem.error_message.id and not redeem.replayed
            assert redeem.created_item_id > 0 and redeem.artifact_item_config_id == 3301
            assert redeem.snapshot.voucher_balance == 0
            opt_3301 = next(a for a in redeem.snapshot.redeemable_artifacts if a.item_config_id == 3301)
            assert opt_3301.owned_count == 1 and opt_3301.artifact_effect_id == 1

            # 检查背包中的实体确实为绑定装备且保留了未知高位 bit 40。
            saved = scene.rows.assemble(player.snapshot)
            assert has_packed_bit(saved.trial_journal_component.lit_seal_bits, 40)
            bag_items = {item.item_id: item for _, item in assets.entries(saved.bag_component, 0)}
            created = bag_items[redeem.created_item_id]
            assert created.config_id == 3301 and created.bound is True and created.HasField("equip")
            assert any(item.config_id == BOUND_REFORGE_SAND_ITEM_ID and item.size == 2 for item in bag_items.values())
            assert not any(item.config_id == BOUND_ARTIFACT_VOUCHER_ITEM_ID for item in bag_items.values())

            # 重放兑换返回同一 item_id；无拓印再次兑换报错。
            redeem_replay = await rpc(
                "SceneTrialJournalClientPlayer", "RedeemBoundArtifactVoucher",
                client_request_id=301, artifact_item_config_id=3301,
            )
            assert not redeem_replay.error_message.id and redeem_replay.replayed
            assert redeem_replay.created_item_id == redeem.created_item_id

            no_voucher = await rpc(
                "SceneTrialJournalClientPlayer", "RedeemBoundArtifactVoucher",
                client_request_id=302, artifact_item_config_id=3302,
            )
            assert no_voucher.error_message.id == TRIAL_JOURNAL_VOUCHER_NOT_ENOUGH
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_equip_craft_rules_preview_commit_and_material_reward(tmp_path):
    async def exercise():
        # 2026-01-02 10:00:00 UTC+8（确保在 UTC+8 05:00 之后）
        day1_ms = 1_767_320_000_000
        now = [day1_ms]
        protocol, _, scene, assets, _, _, craft, _, rpc = await _make_scene(tmp_path, level=20, now_ref=now)
        try:
            rules = await rpc("SceneBagClientPlayer", "GetEquipmentCraftRules")
            assert not rules.error_message.id
            assert len(rules.rules_fingerprint) == 16 and rules.forge_revision == 0
            assert len(rules.recipes) >= 1 and len(rules.affixes) == 36

            # 初始材料自选余额为 0。
            mr0 = await rpc("SceneBagClientPlayer", "GetEquipmentMaterialReward")
            assert not mr0.error_message.id and mr0.snapshot.available
            assert mr0.snapshot.available_units == 0 and not mr0.snapshot.can_claim

            # 同日两场战斗各入账 10 份达到日上限 20，第三场同日不再入账；跨日后可再入账 10 份且保留旧余额。
            assert craft.credit_battle_material_reward(1, now_ms_val=day1_ms) == 10
            assert craft.credit_battle_material_reward(1, now_ms_val=day1_ms) == 10
            assert craft.credit_battle_material_reward(1, now_ms_val=day1_ms) == 0
            now[0] = day1_ms + DAY_MS
            assert craft.credit_battle_material_reward(1, now_ms_val=now[0]) == 10

            mr1 = (await rpc("SceneBagClientPlayer", "GetEquipmentMaterialReward")).snapshot
            assert (mr1.available_units, mr1.batch_units, mr1.earned_today, mr1.can_claim) == (30, 20, 10, True)

            # 错误分配总数拒绝；水位过期返回 UNKNOWN。
            bad_split = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=401, expected_revision=0, crafting_material_count=10, enhancement_stone_count=5,
            )
            assert bad_split.outcome == 1 and bad_split.error_message.id == 1005
            stale_rev = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=402, expected_revision=99, crafting_material_count=20, enhancement_stone_count=0,
            )
            assert stale_rev.outcome == 0 and stale_rev.error_message.id == 1006

            # 领取第一批 20 份（20 打造材料）+ 重放幂等。
            claim1 = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=403, expected_revision=0, crafting_material_count=20, enhancement_stone_count=0,
            )
            assert not claim1.error_message.id and claim1.outcome == 2 and claim1.durable and not claim1.replayed
            assert claim1.result_revision == 1 and claim1.snapshot.available_units == 10
            assert claim1.snapshot.crafting_material.owned == 20

            claim1_replay = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=403, expected_revision=0, crafting_material_count=20, enhancement_stone_count=0,
            )
            assert not claim1_replay.error_message.id and claim1_replay.replayed and claim1_replay.outcome == 2
            assert claim1_replay.snapshot.crafting_material.owned == 20

            # 预览打造 1级定制武器（3101，消耗 20 打造材料 3001）。
            create_choice = protocol.message("EquipmentCraftChoice", operation=1, config_id=3101)
            prev_create = await rpc("SceneBagClientPlayer", "PreviewEquipmentCraft", choice=create_choice)
            assert not prev_create.error_message.id
            pview = prev_create.preview
            assert pview.available and pview.rules_fingerprint == rules.rules_fingerprint
            assert pview.expected_forge_revision == 0
            assert pview.material.config_id == CRAFTING_MATERIAL_ID and pview.material.count == 20 and pview.material.owned == 20
            assert pview.result.config_id == 3101 and pview.result.equip_profile == 1 and len(pview.result.fixed_affix_slots) == 2

            # 指纹不匹配拒绝；正式打造成功 + 幂等重放 + 旧水位返回 UNKNOWN(28022)。
            bad_fp = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=create_choice, rules_fingerprint=b"0" * 16, expected_forge_revision=0, client_request_id=500,
            )
            assert bad_fp.outcome == 1 and bad_fp.error_message.id == 1005

            crafted = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=create_choice, rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=0, client_request_id=501,
            )
            assert not crafted.error_message.id and crafted.outcome == 2 and crafted.durable and not crafted.replayed
            assert crafted.item_id > 0 and crafted.result_forge_revision == 1 and crafted.current_forge_revision == 1
            created_item_id = crafted.item_id

            crafted_replay = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=create_choice, rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=0, client_request_id=501,
            )
            assert not crafted_replay.error_message.id and crafted_replay.replayed and crafted_replay.item_id == created_item_id

            stale_craft = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=create_choice, rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=0, client_request_id=502,
            )
            assert stale_craft.outcome == 0 and stale_craft.error_message.id == EQUIP_LEVEL_CHANGED

            # 领取剩余 10 份打造材料，然后给新装备安装槽位 1 固定词条 101（物理攻击）。
            claim2 = await rpc(
                "SceneBagClientPlayer", "ClaimEquipmentMaterialReward",
                client_request_id=404, expected_revision=1, crafting_material_count=10, enhancement_stone_count=0,
            )
            assert not claim2.error_message.id and claim2.snapshot.crafting_material.owned == 10

            # 池不匹配（槽 1 选池 2 的 201）拒绝。
            bad_pool = await rpc(
                "SceneBagClientPlayer", "PreviewEquipmentCraft",
                choice=protocol.message("EquipmentCraftChoice", operation=2, item_id=created_item_id, affix_slot=1, affix_id=201),
            )
            assert bad_pool.error_message.id == 1005

            affix_choice = protocol.message(
                "EquipmentCraftChoice", operation=2, item_id=created_item_id, affix_slot=1, affix_id=101,
            )
            prev_affix = await rpc("SceneBagClientPlayer", "PreviewEquipmentCraft", choice=affix_choice)
            assert not prev_affix.error_message.id and prev_affix.preview.available
            assert prev_affix.preview.result.fixed_affix_slots[0].selected.affix_id == 101

            installed = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=affix_choice, rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=1, client_request_id=503,
            )
            assert not installed.error_message.id and installed.outcome == 2 and installed.result_forge_revision == 2
            bag_item = next(it for it in installed.bag.items if it.item_id == created_item_id)
            assert bag_item.fixed_affix_slots[0].selected.affix_id == 101

            # 已有词条的槽位 1 不可覆盖安装。
            dup_slot = await rpc(
                "SceneBagClientPlayer", "CommitEquipmentCraft",
                choice=protocol.message("EquipmentCraftChoice", operation=2, item_id=created_item_id, affix_slot=1, affix_id=102),
                rules_fingerprint=rules.rules_fingerprint,
                expected_forge_revision=2,
                client_request_id=504,
            )
            assert dup_slot.outcome == 1 and dup_slot.error_message.id == 1005

            # 持久化核验。
            player = scene.locations[1].players[1]
            saved = scene.rows.assemble(player.snapshot)
            stored_item = next(item for _, item in assets.entries(saved.bag_component, 0) if item.item_id == created_item_id)
            assert [(a.slot, a.affix_id) for a in stored_item.equip.fixed_affixes] == [(1, 101)]
            assert DAY_SHIFT_MS == 10_800_000
        finally:
            await scene.close()

    asyncio.run(exercise())


def test_pet_shop_buy_and_entries_preview_save_replay(tmp_path):
    async def exercise():
        _, _, scene, _, _, _, _, _, rpc = await _make_scene(tmp_path, level=5, gold=2500)
        try:
            shop = await rpc("ScenePetClientPlayer", "GetPetShop")
            assert not shop.error_message.id
            assert len(shop.offers) == 22 and shop.gold == 2500 and shop.next_purchase_sequence == 1
            offer1 = next(o for o in shop.offers if o.pet_table_id == 1)
            offer3 = next(o for o in shop.offers if o.pet_table_id == 3)
            assert offer1.price_gold == 1000 and offer3.price_gold == 20_000

            # 报价过期拒绝、主人等级不足拒绝（pet 3 需要 10 级）、序号错误拒绝。
            assert (await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=1, purchase_sequence=1, price_gold=999,
            )).error_message.id == 1005
            assert (await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=3, purchase_sequence=1, price_gold=20_000,
            )).error_message.id == 26003
            assert (await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=1, purchase_sequence=2, price_gold=1000,
            )).error_message.id == 1005

            # 购买宠物 1 成功，扣 1000 银两，序号推进至 2；同序号重放返回同一 pet_id 不双扣。
            bought = await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=1, purchase_sequence=1, price_gold=1000,
            )
            assert not bought.error_message.id and bought.pet_id > 0
            assert bought.gold == 1500 and bought.next_purchase_sequence == 2
            pet_id = bought.pet_id
            assert bought.pets.pets[0].cultivation_tier == 1 and bought.pets.pets[0].cultivation_tier_name == "普通"

            replay_buy = await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=1, purchase_sequence=1, price_gold=1000,
            )
            assert not replay_buy.error_message.id and replay_buy.pet_id == pet_id and replay_buy.gold == 1500

            bad_replay = await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=2, purchase_sequence=1, price_gold=1000,
            )
            assert bad_replay.error_message.id == 1005

            # 再买第二只并耗尽余额，验证银两不足（26013）。
            bought2 = await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=2, purchase_sequence=2, price_gold=1000,
            )
            assert not bought2.error_message.id and bought2.gold == 500
            poor = await rpc(
                "ScenePetClientPlayer", "BuyPet", pet_table_id=1, purchase_sequence=3, price_gold=1000,
            )
            assert poor.error_message.id == 26013

            # 查询独立培养条目池。
            entries0 = await rpc("ScenePetClientPlayer", "GetPetEntries", pet_id=pet_id)
            assert not entries0.error_message.id
            e0 = entries0.entries
            assert (e0.pet_id, e0.state_version, e0.config_version, e0.total_points, e0.remaining_points, e0.effective) == (
                pet_id, 1, 1, 20, 20, True,
            )
            assert len(e0.entries) == 21

            # 预览条目池：超预算（26009）、未知条目（26012）、版本过期（1005）。
            over_budget = await rpc(
                "ScenePetClientPlayer", "PreviewPetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={501: 15, 502: 10},
            )
            assert over_budget.error_message.id == 26009 and over_budget.entries.remaining_points == 20

            unknown_entry = await rpc(
                "ScenePetClientPlayer", "PreviewPetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={9999: 1},
            )
            assert unknown_entry.error_message.id == 26012

            preview = await rpc(
                "ScenePetClientPlayer", "PreviewPetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={501: 10, 511: 10},
            )
            assert not preview.error_message.id
            assert preview.entries.remaining_points == 0
            assert preview.entries.effects.physical_damage_bps == 500
            assert preview.entries.effects.ailment_hit_bps[1] == 250

            # 预览不落盘。
            after_preview = await rpc("ScenePetClientPlayer", "GetPetEntries", pet_id=pet_id)
            assert after_preview.entries.remaining_points == 20 and after_preview.entries.state_version == 1

            # 保存条目池：非法 request_id 拒绝；正常保存成功 + 同 request_id 幂等重放 + 同 ID 换目标拒绝。
            bad_req_id = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={501: 12, 511: 8}, request_id="short",
            )
            assert bad_req_id.error_message.id == 1005

            req_id_1 = "pet_entry_req_000001"
            saved1 = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={501: 12, 511: 8}, request_id=req_id_1,
            )
            assert not saved1.error_message.id and not saved1.replayed
            assert saved1.entries.state_version == 2 and saved1.entries.remaining_points == 0
            assert saved1.entries.effects.physical_damage_bps == 600
            assert saved1.entries.effects.ailment_hit_bps[1] == 200
            assert saved1.entries.last_request_id == req_id_1

            replay_save = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={501: 12, 511: 8}, request_id=req_id_1,
            )
            assert not replay_save.error_message.id and replay_save.replayed and replay_save.entries.state_version == 2

            conflict_save = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=1, config_version=1, target={501: 10, 511: 10}, request_id=req_id_1,
            )
            assert conflict_save.error_message.id == 1005

            # 注入未知存档条目键 9999=4，并免费减点重分配，验证未知键原样保留且占用预算。
            player = scene.locations[1].players[1]
            data = scene.rows.assemble(player.snapshot)
            data.pet_component.pets[0].entry_allocated[9999] = 4
            await scene.commit_player_data(player, data)

            req_id_2 = "pet_entry_req_000002"
            saved2 = await rpc(
                "ScenePetClientPlayer", "SavePetEntries",
                pet_id=pet_id, state_version=2, config_version=1, target={501: 6, 502: 10}, request_id=req_id_2,
            )
            assert not saved2.error_message.id and saved2.entries.state_version == 3
            assert saved2.entries.unknown_used_points == 4 and saved2.entries.remaining_points == 0
            assert saved2.entries.effects.physical_damage_bps == 300
            assert saved2.entries.effects.magic_damage_bps == 500
            persisted = scene.rows.assemble(player.snapshot).pet_component.pets[0]
            assert dict(persisted.entry_allocated) == {9999: 4, 501: 6, 502: 10}
        finally:
            await scene.close()

    asyncio.run(exercise())
