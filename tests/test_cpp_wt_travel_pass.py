"""青云游历手记（526-529）与活动 UI 低打扰种草聚合单测（对齐 C++ 权威规则）。"""

from __future__ import annotations

import asyncio

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.data.asset_inventory import INVALID, balance
from xuanming.data.id_segments import IdSegments
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_activity import Activities
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_mission import Missions
from xuanming.nodes.scene_travel_pass import (
    ACTIVITY_ID_APPEARANCE_SHOWCASE,
    ACTIVITY_ID_SPIRIT_SHOP_SHOWCASE,
    ACTIVITY_ID_TRAVEL_PASS,
    COSMETIC_SLOT_FRAME,
    COSMETIC_SLOT_TITLE,
    CURRENCY_BIND_DIAMOND,
    CURRENCY_DIAMOND,
    CURRENCY_GOLD,
    ENTRY_KIND_APPEARANCE_SHOWCASE,
    ENTRY_KIND_SPIRIT_SHOP_SHOWCASE,
    ENTRY_KIND_TRAVEL_PASS,
    TRAVEL_PASS_ALREADY_UNLOCKED,
    TRAVEL_PASS_BAG_FULL,
    TRAVEL_PASS_COSMETIC_NOT_OWNED,
    TRAVEL_PASS_CURRENCY_NOT_ENOUGH,
    TRAVEL_PASS_NOT_REACHED,
    TRAVEL_PASS_PREMIUM_REQUIRED,
    TRAVEL_PASS_PRICE_CHANGED,
    SceneTravelPass,
    evaluate_battle_points,
    is_valid_cosmetic_key,
)
from xuanming.protocol import Protocol

# 2026-11-26 00:00:00 UTC（处于 202601 期开放窗口内）
_NOW_MS = 1_795_564_800_000


async def _make_harness(tmp_path, *, diamond: int = 300, bind_diamond: int = 5000, clock_box=None):
    protocol = Protocol()
    tables = ConfigTables()
    db_path = tmp_path / "scene_travel_pass.sqlite"
    store = SqlitePlayerStore(db_path)
    rows = PlayerRowCodec(protocol)
    emitted_audits = []
    tx_seq = iter(range(910_000, 999_999))

    async def emit_audit(entry):
        emitted_audits.append(entry)

    async def alloc_tx():
        return next(tx_seq)

    info = protocol.message("NodeInfo", node_id=1, node_type=3, zone_id=1, node_uuid="scene-travel-pass")
    scene = SceneNode(protocol, info, store, rows, lambda *args, **kwargs: None)
    scene.tables = tables
    scene.create(protocol.message("CreateSceneRequest", config_id=1, scene_id=1))
    assets = SceneAssets(
        scene,
        tables,
        allow_gm=True,
        allocate_item_id=IdSegments.local(db_path, "item"),
        emit_audit=emit_audit,
        allocate_tx_id=alloc_tx,
        anomaly_journal_path=tmp_path / "anomaly.json",
    )
    await assets.anomaly.start()
    assets.register_client_handlers()
    scene.assets = assets

    clock_val = clock_box if clock_box is not None else [_NOW_MS]
    tp = SceneTravelPass(scene, tables, assets, clock=lambda: clock_val[0])
    tp.register()
    scene.travel_pass = tp

    missions = Missions(scene, tables, assets, clock=lambda: clock_val[0])
    activities = Activities(missions, clock=lambda: clock_val[0])
    activities.register()

    enter_req = protocol.message(
        "PlayerEnterGameNodeRequest",
        player_id=2001,
        scene_id=1,
        home_zone_id=1,
        owner_epoch=1,
        session_id=1,
        login_generation=1,
        owner_revision=1,
    )
    await scene.enter(enter_req, "gate-test")
    player = scene.locations[2001].players[2001]
    data = scene.rows.assemble(player.snapshot)
    if diamond:
        assert assets.add_currency(data, CURRENCY_DIAMOND, diamond) == 0
    if bind_diamond:
        assert assets.add_currency(data, CURRENCY_BIND_DIAMOND, bind_diamond) == 0
    await scene.commit_player_data(player, data)
    return protocol, tables, scene, assets, tp, player, emitted_audits


async def _dispatch(scene: SceneNode, player, method: str, request):
    route = next(
        r for r in scene.protocol.routes.values()
        if r.service == "SceneActivityClientPlayer" and r.method == method
    )
    handler = scene._handlers["SceneActivityClientPlayer"][method]
    world = scene.locations[player.player_id]
    res = handler(world, player, route, request)
    if asyncio.iscoroutine(res):
        res = await res
    return res


def test_travel_pass_tables_and_battle_rules(tmp_path):
    async def run():
        protocol, tables, scene, assets, tp, player, _ = await _make_harness(tmp_path)
        assert tp.validate_tables() is True
        assert is_valid_cosmetic_key("frame_travel_qingyun_s1")
        assert is_valid_cosmetic_key("title_travel_qingyun_s1")
        assert not is_valid_cosmetic_key("bad_key")

        season = tp._active_season(_NOW_MS)
        assert season is not None
        assert season.pass_id == 1
        assert season.premium_price == 180
        assert season.max_points == 300
        assert len(season.tiers) == 10

        # 常规 PVE 胜 +20，高难 PVE 胜 +30，PVP 有效负场 +15，逃跑/挂机 0
        s_pve = protocol.message("BattleSettlementData", battle_id=10, player_id=2001, match_mode=4, battle_difficulty=1, outcome=1, player_team_index=0, total_rounds=4)
        assert evaluate_battle_points(season, s_pve) == 20
        s_hard = protocol.message("BattleSettlementData", battle_id=11, player_id=2001, match_mode=4, battle_difficulty=2, outcome=1, player_team_index=0, total_rounds=5)
        assert evaluate_battle_points(season, s_hard) == 30
        s_pvp_qual = protocol.message(
            "BattleSettlementData", battle_id=12, player_id=2001, match_mode=3, outcome=2, player_team_index=0, total_rounds=4,
            participated=True, manual_action_rounds=2, effective_combat_contribution=True,
        )
        assert evaluate_battle_points(season, s_pvp_qual) == 15
        s_pvp_afk = protocol.message(
            "BattleSettlementData", battle_id=13, player_id=2001, match_mode=3, outcome=2, player_team_index=0, total_rounds=4,
            participated=True, manual_action_rounds=1, effective_combat_contribution=True,
        )
        assert evaluate_battle_points(season, s_pvp_afk) == 0
        s_fled = protocol.message("BattleSettlementData", battle_id=14, player_id=2001, match_mode=4, battle_difficulty=2, outcome=1, player_team_index=0, total_rounds=5, fled=True)
        assert evaluate_battle_points(season, s_fled) == 0

    asyncio.run(run())


def test_play_first_buy_later_bound_rewards_and_never_expires(tmp_path):
    async def run():
        clock_box = [_NOW_MS]
        protocol, tables, scene, assets, tp, player, audits = await _make_harness(
            tmp_path, diamond=200, bind_diamond=9999, clock_box=clock_box,
        )
        # 先玩：打 10 场高难 PVE 胜直接满 300 点
        data = scene.rows.assemble(player.snapshot)
        s_hard = protocol.message("BattleSettlementData", battle_id=20, player_id=2001, match_mode=4, battle_difficulty=2, outcome=1, player_team_index=0, total_rounds=5)
        for _ in range(12):
            tp.apply_battle_credit(data, s_hard, clock_box[0])
        await scene.commit_player_data(player, data)

        # 查状态：300/300，10 档免费可领，10 档典藏待解锁可补领，不混淆绑定灵石
        st = await _dispatch(scene, player, "GetTravelPassState", protocol.message("GetTravelPassStateRequest"))
        assert st.error_message.id == 0
        assert st.snapshot.progress_points == 300
        assert st.snapshot.claimable_free_count == 10
        assert st.snapshot.claimable_premium_count == 0
        assert st.snapshot.retroactive_premium_count == 10
        assert st.snapshot.diamond_balance == 200
        assert st.snapshot.bind_diamond_balance == 9999

        # 查活动目录：出现游历手记(有红点) + 两张低打扰种草卡(无红点)
        act = await _dispatch(scene, player, "GetActivityList", protocol.message("GetActivityListRequest"))
        by_id = {a.activity_id: a for a in act.activities}
        assert ACTIVITY_ID_TRAVEL_PASS in by_id
        assert by_id[ACTIVITY_ID_TRAVEL_PASS].entry_kind == ENTRY_KIND_TRAVEL_PASS
        assert by_id[ACTIVITY_ID_TRAVEL_PASS].has_claimable_reward is True
        assert ACTIVITY_ID_APPEARANCE_SHOWCASE in by_id
        assert by_id[ACTIVITY_ID_APPEARANCE_SHOWCASE].entry_kind == ENTRY_KIND_APPEARANCE_SHOWCASE
        assert by_id[ACTIVITY_ID_APPEARANCE_SHOWCASE].has_claimable_reward is False
        assert ACTIVITY_ID_SPIRIT_SHOP_SHOWCASE in by_id
        assert by_id[ACTIVITY_ID_SPIRIT_SHOP_SHOWCASE].entry_kind == ENTRY_KIND_SPIRIT_SHOP_SHOWCASE
        assert by_id[ACTIVITY_ID_SPIRIT_SHOP_SHOWCASE].has_claimable_reward is False

        # 未解锁领典藏档被拒
        bad_prem = await _dispatch(
            scene, player, "ClaimTravelPassReward",
            protocol.message("ClaimTravelPassRewardRequest", client_request_id=101, pass_id=1, tier_index=1, premium_track=True),
        )
        assert bad_prem.error_message.id == TRAVEL_PASS_PREMIUM_REQUIRED

        # 报价防呆：错价被拒
        bad_price = await _dispatch(
            scene, player, "UnlockTravelPassPremium",
            protocol.message(
                "UnlockTravelPassPremiumRequest",
                client_request_id=102, pass_id=1, expected_currency_type=1, expected_price=100, expected_config_revision=1,
            ),
        )
        assert bad_price.error_message.id == TRAVEL_PASS_PRICE_CHANGED

        # 正确解锁典藏（扣 180 真实灵石，绑定灵石丝毫不动）
        unlock_req = protocol.message(
            "UnlockTravelPassPremiumRequest",
            client_request_id=103, pass_id=1, expected_currency_type=1, expected_price=180, expected_config_revision=1,
        )
        unlock_res = await _dispatch(scene, player, "UnlockTravelPassPremium", unlock_req)
        assert unlock_res.error_message.id == 0
        assert unlock_res.replayed is False
        assert unlock_res.snapshot.premium_unlocked is True
        assert unlock_res.snapshot.diamond_balance == 20
        assert unlock_res.snapshot.bind_diamond_balance == 9999
        assert unlock_res.snapshot.claimable_premium_count == 10

        # 同幂等键重放不二次扣灵石
        replay = await _dispatch(scene, player, "UnlockTravelPassPremium", unlock_req)
        assert replay.error_message.id == 0
        assert replay.replayed is True
        assert replay.snapshot.diamond_balance == 20

        # 换键重复买被拒
        dup = await _dispatch(
            scene, player, "UnlockTravelPassPremium",
            protocol.message(
                "UnlockTravelPassPremiumRequest",
                client_request_id=104, pass_id=1, expected_currency_type=1, expected_price=180, expected_config_revision=1,
            ),
        )
        assert dup.error_message.id == TRAVEL_PASS_ALREADY_UNLOCKED

        # 模拟活动窗口早已过期（2099 年）：已解锁典藏永不过期，仍可一键领完全部 20 轨奖励！
        clock_box[0] = 4_000_000_000_000
        claim_all = await _dispatch(
            scene, player, "ClaimTravelPassReward",
            protocol.message("ClaimTravelPassRewardRequest", client_request_id=201, pass_id=1, tier_index=0),
        )
        assert claim_all.error_message.id == 0
        assert claim_all.claimed_count == 20
        assert claim_all.snapshot.claimable_free_count == 0
        assert claim_all.snapshot.claimable_premium_count == 0
        # 所有游历手记发放道具必须 bound=True 且份数严格对齐配表
        tp_item_ids = {3401, 3402, 3403, 3405, 3406, 3407, 3408}
        tp_items = [item for item in claim_all.bag.items if item.config_id in tp_item_ids]
        assert len(tp_items) > 0
        assert all(item.bound for item in tp_items)
        by_cfg = {}
        for item in tp_items:
            by_cfg[item.config_id] = by_cfg.get(item.config_id, 0) + item.count
        assert by_cfg[3401] == 1   # 改名卡
        assert by_cfg[3402] == 2   # 角色形象卡
        assert by_cfg[3403] == 12  # 重铸砂（免费6+典藏6）
        assert by_cfg[3405] == 3   # 定星保底符（免费1+典藏2）
        assert by_cfg[3406] == 25  # 悟道残页
        assert by_cfg[3407] == 1   # 绝学残卷
        assert by_cfg[3408] == 1   # 天工秘墨
        # 银两正确到账（免费 38000 + 典藏 10000 = 48000）
        after_data = scene.rows.assemble(player.snapshot)
        assert balance(after_data, CURRENCY_GOLD) == 48_000

        # 领完后活动目录游历手记不再有红点
        act2 = await _dispatch(scene, player, "GetActivityList", protocol.message("GetActivityListRequest"))
        by_id2 = {a.activity_id: a for a in act2.activities}
        assert by_id2[ACTIVITY_ID_TRAVEL_PASS].has_claimable_reward is False

        # 零战力纪念头像框与称谓佩戴/卸下
        eq1 = await _dispatch(
            scene, player, "EquipTravelPassCosmetic",
            protocol.message("EquipTravelPassCosmeticRequest", cosmetic_slot=COSMETIC_SLOT_FRAME, cosmetic_key="frame_travel_qingyun_s1"),
        )
        assert eq1.error_message.id == 0
        assert eq1.snapshot.equipped_frame_key == "frame_travel_qingyun_s1"

        eq_bad = await _dispatch(
            scene, player, "EquipTravelPassCosmetic",
            protocol.message("EquipTravelPassCosmeticRequest", cosmetic_slot=COSMETIC_SLOT_TITLE, cosmetic_key="title_not_owned"),
        )
        assert eq_bad.error_message.id == TRAVEL_PASS_COSMETIC_NOT_OWNED

        eq2 = await _dispatch(
            scene, player, "EquipTravelPassCosmetic",
            protocol.message("EquipTravelPassCosmeticRequest", cosmetic_slot=COSMETIC_SLOT_TITLE, cosmetic_key="title_travel_qingyun_s1"),
        )
        assert eq2.error_message.id == 0
        assert eq2.snapshot.equipped_title_key == "title_travel_qingyun_s1"

        unequip = await _dispatch(
            scene, player, "EquipTravelPassCosmetic",
            protocol.message("EquipTravelPassCosmeticRequest", cosmetic_slot=COSMETIC_SLOT_FRAME, cosmetic_key=""),
        )
        assert unequip.error_message.id == 0
        assert unequip.snapshot.equipped_frame_key == ""

    asyncio.run(run())


def test_bind_diamond_never_pays_for_premium_and_zero_key_rejected(tmp_path):
    async def run():
        protocol, _, scene, _, _, player, _ = await _make_harness(tmp_path, diamond=100, bind_diamond=50_000)
        # 真实灵石 100 < 180，即使绑定灵石有 50,000 也严禁扣款
        res = await _dispatch(
            scene, player, "UnlockTravelPassPremium",
            protocol.message(
                "UnlockTravelPassPremiumRequest",
                client_request_id=301, pass_id=1, expected_currency_type=1, expected_price=180, expected_config_revision=1,
            ),
        )
        assert res.error_message.id == TRAVEL_PASS_CURRENCY_NOT_ENOUGH

        # client_request_id == 0 必须拒
        zero_req = await _dispatch(
            scene, player, "UnlockTravelPassPremium",
            protocol.message(
                "UnlockTravelPassPremiumRequest",
                client_request_id=0, pass_id=1, expected_currency_type=1, expected_price=180, expected_config_revision=1,
            ),
        )
        assert zero_req.error_message.id == INVALID

        # 未达档位领取被拒
        unreached = await _dispatch(
            scene, player, "ClaimTravelPassReward",
            protocol.message("ClaimTravelPassRewardRequest", client_request_id=302, pass_id=1, tier_index=1, premium_track=False),
        )
        assert unreached.error_message.id == TRAVEL_PASS_NOT_REACHED

    asyncio.run(run())
