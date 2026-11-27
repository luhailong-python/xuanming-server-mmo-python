"""60b0bd6b3 增量移植全量单测：江湖恩怨生死状、试炼志 V3 誓约与明雷首领、装备智能定档连磨。"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import replace

import pytest

from xuanming.data import ConfigTables
from xuanming.data.asset_inventory import INVALID, balance
from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes.battle import BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BOSS_TRIAL_KIND_BREAK_REJUV,
    BOSS_TRIAL_KIND_WILD_BELL,
    BOSS_TRIAL_KIND_WILD_TIDE,
    BOSS_TRIAL_KIND_WILD_WIND,
    JIANGHU_DUEL_DEFAULT_MAX_ROUNDS,
    MATCH_MODE_JIANGHU_DUEL,
    MATCH_MODE_PVE_SOLO,
    TELEGRAPH_KIND_WILD_WIND_GUST,
    VOW_BAN_CONTROL,
    VOW_NO_PET,
    VOW_SINGLE_ARTIFACT,
    VOW_TIGHT_ROUNDS,
    BattleEffectOp,
    BattleStateKind,
    TrialContext,
    count_vows,
    is_boss_trial_kind,
    is_valid_vow_mask,
    is_wild_bell_yang_round,
    is_wild_boss_trial_kind,
    is_wild_tide_ebb_round,
    tighten_round_limit,
)
from xuanming.nodes.battle_tables import BattleBuff, BattleSkill, BattleTables
from xuanming.nodes.player_inbox import BATTLE as INBOX_BATTLE
from xuanming.nodes.scene_equip_craft import POLISH_GOLD_COST, POLISH_SAND_ID
from xuanming.nodes.scene_jianghu import (
    CLEAR_MODE_ITEM_CHARM,
    CLEAR_MODE_SILVER,
    DEFAULT_CLEAR_ITEM_ID,
    DEFAULT_GOLD_WRIT_ITEM_ID,
    JIANGHU_INSUFFICIENT_WRIT_OR_SILVER,
    JIANGHU_NO_ACTIVE_DEFEAT_MARK,
    WRIT_TIER_GOLD_ITEM,
    WRIT_TIER_SILVER,
)
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime, grpc_address
from xuanming.settings import Settings
from xuanming.smoke import connect


def _make_runtime_settings(tmp_path) -> Settings:
    defaults = Settings()
    return replace(
        defaults,
        database=tmp_path / "players.db",
        gate=replace(defaults.gate, port=0),
        scene=replace(defaults.scene, port=0, grpc_port=0),
        battle=replace(defaults.battle, port=0, grpc_port=0),
    )


def _player_snap(protocol: Protocol, player_id: int, team_index: int = 0, **kwargs):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=kwargs.pop("player_name", f"P{player_id}"),
        team_index=team_index,
        level=kwargs.pop("level", 40),
        class_id=kwargs.pop("class_id", 1),
        max_health=kwargs.pop("max_health", 5000),
        max_mana=kwargs.pop("max_mana", 1000),
        physical_attack=kwargs.pop("physical_attack", 300),
        magic_attack=kwargs.pop("magic_attack", 300),
        defense=kwargs.pop("defense", 100),
        base_attributes={
            "health": kwargs.pop("health", 5000),
            "mana": kwargs.pop("mana", 1000),
            "speed": kwargs.pop("speed", 120),
            "strength": 10,
            "armor": 50,
            "resistance": 0,
        },
        routing={
            "session_id": player_id,
            "gate_node_id": 1,
            "gate_instance_id": "gate-1",
            "scene_node_id": 1,
            "scene_instance_id": "scene-1",
            "zone_id": 1,
        },
        **kwargs,
    )
    return snap


def _boss_monster(protocol: Protocol, *, health: int = 5000, physical_attack: int = 200, magic_attack: int = 200):
    return protocol.message(
        "BattleActorState",
        actor_type=2,
        team_index=1,
        is_boss=True,
        monster_table_id=101,
        name="明雷首领",
        level=40,
        max_health=health,
        max_mana=500,
        physical_attack=physical_attack,
        magic_attack=magic_attack,
        defense=100,
        attributes={"health": health, "mana": 500, "speed": 90, "strength": 5, "armor": 40},
    )


def _create_battle_req(protocol: Protocol, players, *, battle_id: int = 9001, match_mode: int = MATCH_MODE_PVE_SOLO, **extra):
    now_ms = 1_750_000_000_000
    return protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        match_mode=match_mode,
        seed=12345,
        players=players,
        outcome_protocol_version=1,
        placement_operation_id=str(uuid.uuid4()),
        target_instance_id="battle-inst-1",
        required_physical_region=1,
        admit_before_ms=now_ms + 30_000,
        deadline_ms=now_ms + 300_000,
        created_at_ms=now_ms,
        **extra,
    )


# ---------------------------------------------------------------------------
# 1. 江湖恩怨与天墉生死状一期：实时 TCP 555..557、gRPC 566、备战/取消/作废/结算/复仇/过户
# ---------------------------------------------------------------------------


def test_jianghu_tcp_rpc_eligibility_fee_settlement_and_clear_mark(tmp_path):
    """端到端覆盖 555/556/557 TCP RPC、566 gRPC 资格预检、银两/金帖生死状备战/取消/作废/结算/复仇/洗尘与过户清标。"""

    async def run():
        settings = _make_runtime_settings(tmp_path)
        async with Runtime(settings, "local", dev_token="test-only") as runtime:
            p, scene, gate = runtime.protocol, runtime.scene, runtime.gate
            rpc_id = p.message_ids
            address = grpc_address(scene.info.grpc_endpoint.ip, scene.info.grpc_endpoint.port)
            control = GrpcClient(address)
            try:
                async with connect(p, gate.info.endpoint) as client:
                    await client.send(gate.issue_ticket())
                    assert (await client.receive("ClientTokenVerifyResponse")).success
                    login = await client.rpc(
                        rpc_id["ClientPlayerLoginLogin"], 1,
                        p.message("loginpb.LoginRequest", account="local", password="test-only"),
                    )
                    assert not login.error_message.id
                    enter = await client.rpc(
                        rpc_id["ClientPlayerLoginEnterGame"], 2,
                        p.message("loginpb.EnterGameRequest", player_id=1, request_id="jh-enter-1"),
                    )
                    assert enter.player_id == 1
                    await client.content(rpc_id["SceneSceneClientPlayerNotifyEnterScene"])

                    # 预置资产：5000 流通银两 + 2 张金帖(3415) + 2 张清风洗尘符(3416)
                    player = scene.locations[1].players[1]
                    data = scene.rows.assemble(player.snapshot)
                    assert scene.assets.add_currency(data, 0, 5000) == 0
                    assert await scene.assets.add_items(data, {DEFAULT_GOLD_WRIT_ITEM_ID: 2, DEFAULT_CLEAR_ITEM_ID: 2}) == 0
                    await scene.commit_player_data(player, data)

                    # 555: GetJianghuProfile 初始状态
                    prof_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerGetJianghuProfile"], 3,
                        p.message("GetJianghuProfileRequest"),
                    )
                    assert not prof_resp.error_message.id
                    assert not prof_resp.profile.duel_refuse_strangers
                    assert not prof_resp.profile.has_active_defeat_mark
                    assert prof_resp.profile.silver_balance == 5000
                    assert prof_resp.profile.gold_writ_item_count == 2
                    assert prof_resp.profile.clear_charm_item_count == 2
                    assert prof_resp.profile.silver_writ_cost == 500
                    assert prof_resp.profile.silver_clear_cost == 1000

                    # 556: SetJianghuRefuseStrangers 切换闭关免战
                    set_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerSetJianghuRefuseStrangers"], 4,
                        p.message("SetJianghuRefuseStrangersRequest", refuse_strangers=True),
                    )
                    assert not set_resp.error_message.id
                    assert set_resp.profile.duel_refuse_strangers is True

                    # 566: SceneNodeGrpc/CheckJianghuDuelEligibility
                    elig_req = p.message(
                        "CheckJianghuDuelEligibilityRequest",
                        player_ids=[1, 9999],
                        payer_player_id=1,
                        writ_tier=WRIT_TIER_SILVER,
                        silver_cost=500,
                    )
                    raw_elig = await control.call(
                        "/scene_node.SceneNodeGrpc/CheckJianghuDuelEligibility",
                        elig_req.SerializeToString(),
                    )
                    elig = p.parse("CheckJianghuDuelEligibilityResponse", raw_elig)
                    assert not elig.error_message.id
                    assert len(elig.players) == 2
                    assert elig.players[0].player_id == 1
                    assert elig.players[0].online_on_node is True
                    assert elig.players[0].duel_refuse_strangers is True
                    assert elig.players[1].player_id == 9999
                    assert elig.players[1].online_on_node is False
                    assert elig.payer_has_sufficient_fee is True

                    # 566: 金帖预检与超限 11 人参数拒绝
                    raw_gold = await control.call(
                        "/scene_node.SceneNodeGrpc/CheckJianghuDuelEligibility",
                        p.message(
                            "CheckJianghuDuelEligibilityRequest",
                            player_ids=[1],
                            payer_player_id=1,
                            writ_tier=WRIT_TIER_GOLD_ITEM,
                            item_config_id=DEFAULT_GOLD_WRIT_ITEM_ID,
                        ).SerializeToString(),
                    )
                    assert p.parse("CheckJianghuDuelEligibilityResponse", raw_gold).payer_has_sufficient_fee is True
                    raw_too_many = await control.call(
                        "/scene_node.SceneNodeGrpc/CheckJianghuDuelEligibility",
                        p.message("CheckJianghuDuelEligibilityRequest", player_ids=list(range(1, 12))).SerializeToString(),
                    )
                    assert p.parse("CheckJianghuDuelEligibilityResponse", raw_too_many).error_message.id == INVALID

                    # 普通战书 500 银两备战预扣 + CancelBattlePrepare 退还
                    now = time.time_ns() // 1_000_000
                    prep_silver = p.message(
                        "PrepareBattleRequest",
                        player_id=1,
                        battle_id=5001,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        deadline_ms=now + 60_000,
                        prepare_deadline_ms=now + 30_000,
                        jianghu_duel_fee={
                            "duel_invite_id": 7001,
                            "writ_tier": WRIT_TIER_SILVER,
                            "silver_cost": 500,
                        },
                    )
                    ready_silver = p.parse(
                        "PrepareBattleResponse",
                        await control.call("/scene_node.SceneNodeGrpc/PrepareBattle", prep_silver.SerializeToString()),
                    )
                    assert not ready_silver.error_message.id
                    assert ready_silver.jianghu_silver_fee_charged is True
                    saved = scene.rows.assemble(player.snapshot)
                    assert balance(saved, 0) == 4500
                    assert saved.jianghu_component.pending_silver_fee_battle_id == 5001
                    assert saved.jianghu_component.pending_silver_fee_amount == 500

                    # CancelBattlePrepare(skip=False) 当场退回 500 银两
                    await control.call(
                        "/scene_node.SceneNodeGrpc/CancelBattlePrepare",
                        p.message("CancelBattlePrepareRequest", player_id=1, battle_id=5001).SerializeToString(),
                    )
                    saved = scene.rows.assemble(player.snapshot)
                    assert balance(saved, 0) == 5000
                    assert saved.jianghu_component.pending_silver_fee_battle_id == 0

                    # 再次预扣 500 银两 + CancelBattlePrepare(skip=True) 不双退
                    prep_silver2 = p.message(
                        "PrepareBattleRequest",
                        player_id=1,
                        battle_id=5002,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        deadline_ms=now + 60_000,
                        prepare_deadline_ms=now + 30_000,
                        jianghu_duel_fee={"duel_invite_id": 7002, "writ_tier": WRIT_TIER_SILVER, "silver_cost": 500},
                    )
                    ready2 = p.parse(
                        "PrepareBattleResponse",
                        await control.call("/scene_node.SceneNodeGrpc/PrepareBattle", prep_silver2.SerializeToString()),
                    )
                    assert not ready2.error_message.id and ready2.jianghu_silver_fee_charged is True
                    assert balance(scene.rows.assemble(player.snapshot), 0) == 4500
                    await control.call(
                        "/scene_node.SceneNodeGrpc/CancelBattlePrepare",
                        p.message(
                            "CancelBattlePrepareRequest",
                            player_id=1,
                            battle_id=5002,
                            skip_jianghu_silver_refund=True,
                        ).SerializeToString(),
                    )
                    saved = scene.rows.assemble(player.snapshot)
                    # 6b4ae7852 废除客户端/上游 skip_jianghu_silver_refund：CancelBattlePrepare 始终幂等退还 500 银两防扣留
                    assert balance(saved, 0) == 5000
                    assert saved.jianghu_component.pending_silver_fee_battle_id == 0

                    # 再次预扣 500 银两 + 作废局（outcome=4）原子退还银两
                    prep_void = p.message(
                        "PrepareBattleRequest",
                        player_id=1,
                        battle_id=5003,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        deadline_ms=now + 60_000,
                        prepare_deadline_ms=now + 30_000,
                        jianghu_duel_fee={"duel_invite_id": 7003, "writ_tier": WRIT_TIER_SILVER, "silver_cost": 500},
                    )
                    ready_void = p.parse(
                        "PrepareBattleResponse",
                        await control.call("/scene_node.SceneNodeGrpc/PrepareBattle", prep_void.SerializeToString()),
                    )
                    assert not ready_void.error_message.id
                    assert balance(scene.rows.assemble(player.snapshot), 0) == 4500
                    await scene.battle.confirm(
                        p.message("BattleConfirmedEvent", player_id=1, battle_id=5003, deadline_ms=now + 60_000)
                    )
                    void_settlement = p.message(
                        "BattleSettlementData",
                        player_id=1,
                        battle_id=5003,
                        outcome=4,
                        total_rounds=0,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                    )
                    await runtime.local_inbox.enqueue([void_settlement])
                    scene.inbox.wake(1, INBOX_BATTLE)
                    await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    async with asyncio.timeout(4):
                        while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                            await asyncio.sleep(0.02)
                    saved = scene.rows.assemble(player.snapshot)
                    assert balance(saved, 0) == 5000
                    assert saved.jianghu_component.pending_silver_fee_battle_id == 0
                    assert saved.jianghu_component.defeat_mark_expires_at_ms == 0

                    # 金帖生死状备战（冻结 1 张 3415）-> 战败结算：核销 1 张金帖、挂 30 分钟金帖印记(style=2)、零经验零银两
                    prep_gold = p.message(
                        "PrepareBattleRequest",
                        player_id=1,
                        battle_id=5004,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        deadline_ms=now + 60_000,
                        prepare_deadline_ms=now + 30_000,
                        jianghu_duel_fee={
                            "duel_invite_id": 7004,
                            "writ_tier": WRIT_TIER_GOLD_ITEM,
                            "item_config_id": DEFAULT_GOLD_WRIT_ITEM_ID,
                        },
                    )
                    ready_gold = p.parse(
                        "PrepareBattleResponse",
                        await control.call("/scene_node.SceneNodeGrpc/PrepareBattle", prep_gold.SerializeToString()),
                    )
                    assert not ready_gold.error_message.id
                    assert ready_gold.jianghu_silver_fee_charged is False
                    # 冻结后可动用金帖数从 2 降为 1
                    assert scene.assets.removable_count(scene.rows.assemble(player.snapshot), DEFAULT_GOLD_WRIT_ITEM_ID) == 1

                    await scene.battle.confirm(
                        p.message("BattleConfirmedEvent", player_id=1, battle_id=5004, deadline_ms=now + 60_000)
                    )
                    level_before = scene.rows.assemble(player.snapshot).level_component.level
                    exp_before = scene.rows.assemble(player.snapshot).level_component.total_exp
                    gold_lose_settlement = p.message(
                        "BattleSettlementData",
                        player_id=1,
                        battle_id=5004,
                        outcome=2,  # B 方胜，player 1 在 A 方（team_index=0）落败
                        player_team_index=0,
                        health=0,
                        is_dead=True,
                        total_rounds=4,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        currency_gains=[{"currency_type": 0, "amount": 9999}],
                        exp_gain=9999,
                        jianghu_duel={
                            "duel_invite_id": 7004,
                            "writ_tier": WRIT_TIER_GOLD_ITEM,
                            "challenger_captain_id": 1,
                            "challenger_captain_name": "剑客甲",
                            "target_captain_id": 2002,
                            "target_captain_name": "刀客乙",
                            "round_limit_override": 30,
                            "team_size": 1,
                        },
                    )
                    await runtime.local_inbox.enqueue([gold_lose_settlement])
                    scene.inbox.wake(1, INBOX_BATTLE)
                    await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    async with asyncio.timeout(4):
                        while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                            await asyncio.sleep(0.02)

                    saved = scene.rows.assemble(player.snapshot)
                    assert balance(saved, 0) == 5000  # 生死状零银两产出
                    assert saved.level_component.level == level_before
                    assert saved.level_component.total_exp == exp_before  # 零经验产出
                    assert scene.assets.removable_count(saved, DEFAULT_GOLD_WRIT_ITEM_ID) == 1  # 消耗 1 张金帖
                    assert saved.jianghu_component.defeat_mark_winner_id == 2002
                    assert saved.jianghu_component.defeat_mark_winner_name == "刀客乙"
                    assert saved.jianghu_component.defeat_mark_style_id == 2
                    assert saved.jianghu_component.defeat_mark_battle_id == 5004

                    # 557: 银两清除战败印记 + 幂等重放 + 同键异参拒绝
                    clear_silver_req = p.message(
                        "ClearJianghuDefeatMarkRequest",
                        client_request_id=91001,
                        mode=CLEAR_MODE_SILVER,
                    )
                    clear_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerClearJianghuDefeatMark"], 5, clear_silver_req,
                    )
                    assert not clear_resp.error_message.id
                    assert clear_resp.replayed is False
                    assert clear_resp.profile.has_active_defeat_mark is False
                    assert clear_resp.profile.silver_balance == 4000

                    replay_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerClearJianghuDefeatMark"], 6, clear_silver_req,
                    )
                    assert not replay_resp.error_message.id
                    assert replay_resp.replayed is True
                    assert replay_resp.profile.silver_balance == 4000

                    mismatch_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerClearJianghuDefeatMark"], 7,
                        p.message("ClearJianghuDefeatMarkRequest", client_request_id=91001, mode=CLEAR_MODE_ITEM_CHARM),
                    )
                    assert mismatch_resp.error_message.id == INVALID

                    # 无印记再次清除 -> 40011（测试内重置 Gate 速率窗口以免连发触发 1008）
                    for sess in runtime.gate.sessions.values():
                        sess.message_timestamps.clear()
                    no_mark_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerClearJianghuDefeatMark"], 8,
                        p.message("ClearJianghuDefeatMarkRequest", client_request_id=91002, mode=CLEAR_MODE_SILVER),
                    )
                    assert no_mark_resp.error_message.id == JIANGHU_NO_ACTIVE_DEFEAT_MARK

                    # 帮会生死状落败（双帮不同）-> 挂帮会印记 -> 清风洗尘符清除并附赠 30 分钟洒脱徽记
                    guild_lose = p.message(
                        "BattleSettlementData",
                        player_id=1,
                        battle_id=5005,
                        outcome=2,
                        player_team_index=0,
                        total_rounds=3,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        jianghu_duel={
                            "duel_invite_id": 7005,
                            "writ_tier": WRIT_TIER_SILVER,
                            "challenger_captain_id": 1,
                            "challenger_captain_name": "剑客甲",
                            "target_captain_id": 2002,
                            "target_captain_name": "刀客乙",
                            "side_a_guild_id": 10,
                            "side_a_guild_name": "青云宗",
                            "side_b_guild_id": 20,
                            "side_b_guild_name": "天罡门",
                        },
                    )
                    await runtime.local_inbox.enqueue([guild_lose])
                    scene.inbox.wake(1, INBOX_BATTLE)
                    await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    async with asyncio.timeout(4):
                        while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                            await asyncio.sleep(0.02)

                    prof_guild = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerGetJianghuProfile"], 9,
                        p.message("GetJianghuProfileRequest"),
                    )
                    assert prof_guild.profile.has_active_defeat_mark is True
                    assert prof_guild.profile.defeat_mark_is_guild_name is True
                    assert prof_guild.profile.defeat_mark_winner_id == 20
                    assert prof_guild.profile.defeat_mark_winner_name == "天罡门"
                    assert prof_guild.profile.defeat_mark_style_id == 1

                    # 557: 清风洗尘符(3416) 清除并附赠 30 分钟洒脱徽记 + 返回 bag
                    clear_charm_resp = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerClearJianghuDefeatMark"], 10,
                        p.message("ClearJianghuDefeatMarkRequest", client_request_id=91003, mode=CLEAR_MODE_ITEM_CHARM),
                    )
                    assert not clear_charm_resp.error_message.id
                    assert clear_charm_resp.replayed is False
                    assert clear_charm_resp.profile.has_active_defeat_mark is False
                    assert clear_charm_resp.profile.has_active_clear_badge is True
                    assert clear_charm_resp.profile.clear_badge_style_id == 1
                    assert clear_charm_resp.profile.clear_charm_item_count == 1
                    assert clear_charm_resp.HasField("bag")

                    # 复仇清除：再挂个人印记 -> 击败同一对手(2002)自动清标
                    lose_again = p.message(
                        "BattleSettlementData",
                        player_id=1,
                        battle_id=5006,
                        outcome=2,
                        player_team_index=0,
                        total_rounds=2,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        jianghu_duel={
                            "duel_invite_id": 7006,
                            "writ_tier": WRIT_TIER_SILVER,
                            "challenger_captain_id": 1,
                            "challenger_captain_name": "剑客甲",
                            "target_captain_id": 2002,
                            "target_captain_name": "刀客乙",
                        },
                    )
                    revenge_win = p.message(
                        "BattleSettlementData",
                        player_id=1,
                        battle_id=5007,
                        outcome=1,
                        player_team_index=0,
                        total_rounds=2,
                        match_mode=MATCH_MODE_JIANGHU_DUEL,
                        jianghu_duel={
                            "duel_invite_id": 7007,
                            "writ_tier": WRIT_TIER_SILVER,
                            "challenger_captain_id": 1,
                            "challenger_captain_name": "剑客甲",
                            "target_captain_id": 2002,
                            "target_captain_name": "刀客乙",
                        },
                    )
                    await runtime.local_inbox.enqueue([lose_again, revenge_win])
                    scene.inbox.wake(1, INBOX_BATTLE)
                    await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    async with asyncio.timeout(4):
                        while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                            await asyncio.sleep(0.02)
                    saved = scene.rows.assemble(player.snapshot)
                    assert saved.jianghu_component.defeat_mark_expires_at_ms == 0

                    # 角色过户 owner_revision 递增后，旧印记与旧洒脱徽记在面板上自动隐去
                    player.owner_revision += 1
                    prof_after_trade = await client.rpc(
                        rpc_id["SceneJianghuClientPlayerGetJianghuProfile"], 11,
                        p.message("GetJianghuProfileRequest"),
                    )
                    assert prof_after_trade.profile.has_active_defeat_mark is False
                    assert prof_after_trade.profile.has_active_clear_badge is False
            finally:
                await control.close()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# 2. Battle 江湖生死状（round_limit_override、30 回合平局、上下文透传）与试炼誓约 V3 & 明雷首领
# ---------------------------------------------------------------------------


def test_battle_jianghu_duel_draw_and_context_propagation():
    """天墉生死状默认 30 回合 / round_limit_override 生效、打满判平、且回传 jianghu_duel_context 到终局原件与个人结算。"""
    p = Protocol()
    p1 = _player_snap(p, 101, team_index=0, max_health=90_000, health=90_000, physical_attack=10, defense=5000)
    p2 = _player_snap(p, 202, team_index=1, max_health=90_000, health=90_000, physical_attack=10, defense=5000)

    # 默认回合上限 30
    req_default = _create_battle_req(
        p, [p1, p2], battle_id=8801, match_mode=MATCH_MODE_JIANGHU_DUEL,
        jianghu_duel_context={
            "duel_invite_id": 6001,
            "writ_tier": WRIT_TIER_GOLD_ITEM,
            "challenger_captain_id": 101,
            "challenger_captain_name": "甲",
            "target_captain_id": 202,
            "target_captain_name": "乙",
            "team_size": 1,
        },
    )
    eng_default = TurnEngine(p, req_default, skills={}, items={}, max_rounds=300)
    assert eng_default.is_jianghu_duel_battle is True
    assert eng_default.is_competitive_pvp_battle is True
    assert eng_default.max_rounds == JIANGHU_DUEL_DEFAULT_MAX_ROUNDS

    # round_limit_override = 2 -> 打满 2 回合双方均存活权威判平(outcome=3)
    req_short = _create_battle_req(
        p, [p1, p2], battle_id=8802, match_mode=MATCH_MODE_JIANGHU_DUEL,
        jianghu_duel_context={
            "duel_invite_id": 6002,
            "writ_tier": WRIT_TIER_GOLD_ITEM,
            "challenger_captain_id": 101,
            "challenger_captain_name": "甲",
            "target_captain_id": 202,
            "target_captain_name": "乙",
            "round_limit_override": 2,
            "team_size": 1,
        },
    )
    outcomes = []
    node = BattleNode(
        p,
        node_id=1,
        instance_id="battle-inst-1",
        host="127.0.0.1",
        port=19001,
        ticket_secret=b"x" * 32,
        now_ms=lambda: 1_750_000_000_000,
        steady_ms=lambda: 1000,
        on_outcome=lambda bid, out: outcomes.append((bid, out)),
    )
    create_resp = node.create(req_short)
    assert not create_resp.error_message.id
    room = node.rooms[8802]
    assert room.engine.max_rounds == 2
    node._resolve(room)
    assert room.engine.outcome == 0
    node._resolve(room)
    assert room.engine.outcome == 3
    assert outcomes == [(8802, 3)]

    outcome_req = node.outcome_request(8802)
    assert outcome_req is not None
    result_event = p.parse("contracts.kafka.BattleResultEvent", outcome_req.raw_result_payload)
    assert result_event.jianghu_duel_context.duel_invite_id == 6002
    assert result_event.jianghu_duel_context.writ_tier == WRIT_TIER_GOLD_ITEM
    assert len(result_event.settlements) == 2
    for st in result_event.settlements:
        assert st.jianghu_duel.duel_invite_id == 6002
        assert st.jianghu_duel.challenger_captain_id == 101
        assert st.jianghu_duel.target_captain_id == 202


def test_battle_trial_challenge_vows_and_wild_boss_mechanics():
    """校验试炼誓约 V3 四项规则（禁控/单宝/无宠/紧缩回合）、非法誓约拒绝与卷四三境明雷机制。"""
    assert is_valid_vow_mask(0)
    assert is_valid_vow_mask(0x0F)
    assert not is_valid_vow_mask(0x10)
    assert count_vows(VOW_BAN_CONTROL | VOW_NO_PET | VOW_TIGHT_ROUNDS) == 3
    assert tighten_round_limit(15) == 11
    assert tighten_round_limit(10) == 7
    assert tighten_round_limit(6) == 6
    assert is_wild_boss_trial_kind(BOSS_TRIAL_KIND_WILD_WIND)
    assert is_boss_trial_kind(BOSS_TRIAL_KIND_WILD_BELL)
    assert is_boss_trial_kind(BOSS_TRIAL_KIND_WILD_TIDE)
    assert is_wild_bell_yang_round(1) and is_wild_bell_yang_round(2) and not is_wild_bell_yang_round(3)
    assert not is_wild_tide_ebb_round(1) and is_wild_tide_ebb_round(3) and is_wild_tide_ebb_round(4)

    p = Protocol()
    ctrl_buff = BattleBuff(
        buff_type=30, duration_rounds=1,
        battle_op=BattleEffectOp.CONTROL, state_kind=BattleStateKind.SLEEP,
        application_chance=1.0, config_duration_rounds=1,
    )
    ctrl_skill = BattleSkill(
        base_damage=0.0, mana_cost=10, class_id=1, effects=(9001,),
        battle_model=1, ability_kind=1, caster_mask=1, target_team=1, max_targets=1,
    )
    p_snap = _player_snap(
        p, 11, team_index=0,
        skill_table_ids=[8001],
        skill_ranks={8001: 1},
        pets=[{
            "pet_id": 501,
            "pet_table_id": 201,
            "pet_name": "小灵宠",
            "level": 30,
            "max_health": 1000,
            "max_mana": 200,
            "physical_attack": 100,
            "magic_attack": 100,
            "defense": 50,
            "base_attributes": {"health": 1000, "mana": 200, "speed": 100},
        }],
        equipment_effects={
            "artifacts": [
                {"item_id": 1001, "effect_id": 1, "bonus_bps": 500, "per_round_limit": 1, "battle_limit": 3},
                {"item_id": 1002, "effect_id": 2, "bonus_bps": 500, "per_round_limit": 1, "battle_limit": 3},
            ],
        },
    )
    boss = _boss_monster(p)

    # 非法位掩码拒绝
    with pytest.raises(BattleError):
        TurnEngine(
            p,
            _create_battle_req(p, [p_snap], trial_context={"journal_entry_id": 101, "trial_rule_version": 1, "challenge_vow_mask": 0x10}),
            monsters=[boss], skills={8001: ctrl_skill}, buffs={9001: ctrl_buff}, items={},
            max_rounds=15, boss_trial_kind=BOSS_TRIAL_KIND_BREAK_REJUV,
        )

    # 非首领试炼拒绝携带誓约
    with pytest.raises(BattleError):
        TurnEngine(
            p,
            _create_battle_req(p, [p_snap], trial_context={"journal_entry_id": 101, "trial_rule_version": 1, "challenge_vow_mask": VOW_NO_PET}),
            monsters=[boss], skills={8001: ctrl_skill}, buffs={9001: ctrl_buff}, items={},
            max_rounds=15, boss_trial_kind=0,
        )

    # 启用全部 4 项誓约（禁控 + 单宝 + 无宠 + 紧缩回合）
    all_vows = VOW_BAN_CONTROL | VOW_SINGLE_ARTIFACT | VOW_NO_PET | VOW_TIGHT_ROUNDS
    eng = TurnEngine(
        p,
        _create_battle_req(p, [p_snap], trial_context={"journal_entry_id": 101, "trial_rule_version": 1, "challenge_vow_mask": all_vows}),
        monsters=[boss], skills={8001: ctrl_skill}, buffs={9001: ctrl_buff}, items={},
        max_rounds=15, boss_trial_kind=BOSS_TRIAL_KIND_BREAK_REJUV,
    )
    assert eng.trial_vow_mask == all_vows
    assert eng.max_rounds == 11  # ceil(15 * 0.7)
    assert eng.snapshot(0).trial_vow_mask == all_vows
    # 单宝：2 法宝裁剪为 1 法宝
    assert len(eng.actors[11].equipment_effects.artifacts) == 1
    assert eng.actors[11].equipment_effects.artifacts[0].item_id == 1001
    # 无宠：不生成玩家灵宠单位，且试炼事实 used_pet_table_id == 0
    assert not any(a.actor_type == 3 for a in eng.actors.values())
    # 禁控：玩家(team 0)施放控制技能被 7004 拒绝
    boss_actor = next(a for a in eng.actors.values() if a.actor_type == 2)
    ctrl_action = p.message("BattleAction", action_type=2, skill_table_id=8001, target_id=boss_actor.actor_id)
    assert eng.validate(eng.actors[11], ctrl_action) == 7004
    # 结算事实携带 challenge_vow_mask 与 used_pet_table_id == 0
    settlement = p.message("BattleSettlementData", battle_id=9001, player_id=11, outcome=1, total_rounds=5)
    facts = eng.trial_tracker.populate_settlement(settlement, 11)
    assert facts.challenge_vow_mask == all_vows
    assert facts.used_pet_table_id == 0

    # 卷四·风眼引风：首回合发出 TELEGRAPH_KIND_WILD_WIND_GUST (10)
    wind_eng = TurnEngine(
        p, _create_battle_req(p, [_player_snap(p, 12)]),
        monsters=[_boss_monster(p, health=3000)], skills={}, items={},
        max_rounds=12, boss_trial_kind=BOSS_TRIAL_KIND_WILD_WIND,
    )
    assert any(t.intent_kind == TELEGRAPH_KIND_WILD_WIND_GUST for t in wind_eng.encounter.active_telegraphs)

    # 卷四·碧渊水宫阳钟(R1)/阴鼓(R3) 伤害倍率与事实追踪
    bell_eng = TurnEngine(
        p,
        _create_battle_req(p, [_player_snap(p, 13)], trial_context={"journal_entry_id": 101, "trial_rule_version": 1}),
        monsters=[_boss_monster(p, health=5000)], skills={}, items={},
        max_rounds=12, boss_trial_kind=BOSS_TRIAL_KIND_WILD_BELL,
    )
    bell_boss = next(a for a in bell_eng.actors.values() if a.actor_type == 2)
    turn_dummy = p.message("TurnResultS2C")
    phys_r1, _ = bell_eng.encounter.apply_encounter_damage_modifiers(
        bell_eng.actors[13], bell_boss, 1000, 1, True, turn_dummy, 1,
    )
    magic_r1, _ = bell_eng.encounter.apply_encounter_damage_modifiers(
        bell_eng.actors[13], bell_boss, 1000, 0, True, turn_dummy, 1,
    )
    assert phys_r1 == 1150 and magic_r1 == 880
    bell_eng.trial_tracker.record_direct_damage(bell_eng.actors[13], bell_boss, 1, 500)
    bell_eng.round_index = 3
    phys_r3, _ = bell_eng.encounter.apply_encounter_damage_modifiers(
        bell_eng.actors[13], bell_boss, 1000, 1, True, turn_dummy, 1,
    )
    magic_r3, _ = bell_eng.encounter.apply_encounter_damage_modifiers(
        bell_eng.actors[13], bell_boss, 1000, 0, True, turn_dummy, 1,
    )
    assert phys_r3 == 880 and magic_r3 == 1150
    bell_eng.trial_tracker.record_direct_damage(bell_eng.actors[13], bell_boss, 0, 500)
    assert bell_eng.trial_tracker.dealt_yang_bell_physical_damage is True
    assert bell_eng.trial_tracker.dealt_yin_drum_magic_damage is True

    # 卷四·月汐湾涨潮(R1: 75%)/退潮(R3: 120%) + 退潮终结标记
    tide_eng = TurnEngine(
        p,
        _create_battle_req(p, [_player_snap(p, 14)], trial_context={"journal_entry_id": 101, "trial_rule_version": 1}),
        monsters=[_boss_monster(p, health=5000)], skills={}, items={},
        max_rounds=12, boss_trial_kind=BOSS_TRIAL_KIND_WILD_TIDE,
    )
    tide_boss = next(a for a in tide_eng.actors.values() if a.actor_type == 2)
    dmg_flood, _ = tide_eng.encounter.apply_encounter_damage_modifiers(
        tide_eng.actors[14], tide_boss, 1000, 1, True, turn_dummy, 1,
    )
    assert dmg_flood == 750
    tide_eng.round_index = 3
    dmg_ebb, _ = tide_eng.encounter.apply_encounter_damage_modifiers(
        tide_eng.actors[14], tide_boss, 1000, 1, True, turn_dummy, 1,
    )
    assert dmg_ebb == 1200
    tide_eng.outcome = 1
    st_tide = p.message("BattleSettlementData", battle_id=9002, player_id=14, outcome=1, total_rounds=3)
    assert tide_eng.trial_tracker.populate_settlement(st_tide, 14).finished_in_ebb_tide is True


# ---------------------------------------------------------------------------
# 3. 五行试炼志 V3 誓约纪录、回放卡 battle_id 绑定与野外明雷跨图校验 + 装备智能定档连磨（实时 TCP）
# ---------------------------------------------------------------------------


def test_trial_journal_v3_vow_records_and_batch_polish_tcp(tmp_path):
    """覆盖试炼志 V3 誓约纪录刷新优先级、battle_id 绑定、PlayerBattleResult 回写及装备智能定档连磨全流程。"""

    async def run():
        settings = _make_runtime_settings(tmp_path)
        async with Runtime(settings, "local", dev_token="test-only") as runtime:
            p, scene, gate = runtime.protocol, runtime.scene, runtime.gate
            rpc_id = p.message_ids
            address = grpc_address(scene.info.grpc_endpoint.ip, scene.info.grpc_endpoint.port)
            control = GrpcClient(address)
            try:
                async with connect(p, gate.info.endpoint) as client:
                    await client.send(gate.issue_ticket())
                    assert (await client.receive("ClientTokenVerifyResponse")).success
                    login = await client.rpc(
                        rpc_id["ClientPlayerLoginLogin"], 1,
                        p.message("loginpb.LoginRequest", account="local", password="test-only"),
                    )
                    assert not login.error_message.id
                    enter = await client.rpc(
                        rpc_id["ClientPlayerLoginEnterGame"], 2,
                        p.message("loginpb.EnterGameRequest", player_id=1, request_id="tp-enter-1"),
                    )
                    assert enter.player_id == 1
                    await client.content(rpc_id["SceneSceneClientPlayerNotifyEnterScene"])

                    # 1) 卷四明雷首领试炼跨图校验：玩家在主城(scene_id=1)尝试开野外图(scene_id>1)明雷首领被拒
                    wild_dungeon = next(
                        (d for d in scene.tables["Dungeon"].rows() if 12 <= int(getattr(d, "boss_trial_kind", 0)) <= 14 and int(getattr(d, "scene_id", 0)) > 1),
                        None,
                    )
                    if wild_dungeon is not None:
                        now = time.time_ns() // 1_000_000
                        wild_prep = p.parse(
                            "PrepareBattleResponse",
                            await control.call(
                                "/scene_node.SceneNodeGrpc/PrepareBattle",
                                p.message(
                                    "PrepareBattleRequest",
                                    player_id=1,
                                    battle_id=6100,
                                    battle_config_id=wild_dungeon.id,
                                    match_mode=MATCH_MODE_PVE_SOLO,
                                    deadline_ms=now + 60_000,
                                    prepare_deadline_ms=now + 30_000,
                                ).SerializeToString(),
                            ),
                        )
                        assert wild_prep.error_message.id == 1006

                    # 2) 试炼志 V3 誓约结算：先 1 条誓约 5 回合 -> 再 2 条誓约 7 回合覆盖 -> 再 2 条誓约 4 回合覆盖 -> 再 1 条誓约 2 回合不覆盖
                    for bid, rounds, mask in (
                        (7101, 5, VOW_NO_PET),
                        (7102, 7, VOW_NO_PET | VOW_BAN_CONTROL),
                        (7103, 4, VOW_NO_PET | VOW_BAN_CONTROL),
                        (7104, 2, VOW_NO_PET),
                    ):
                        st = p.message(
                            "BattleSettlementData",
                            player_id=1,
                            battle_id=bid,
                            outcome=1,
                            player_team_index=0,
                            total_rounds=rounds,
                            match_mode=MATCH_MODE_PVE_SOLO,
                            battle_config_id=101,
                            boss_trial_kind=BOSS_TRIAL_KIND_BREAK_REJUV,
                            trial_facts={
                                "journal_entry_id": 101,
                                "trial_rule_version": 1,
                                "initial_human_count": 1,
                                "challenge_vow_mask": mask,
                            },
                        )
                        await runtime.local_inbox.enqueue([st])
                        scene.inbox.wake(1, INBOX_BATTLE)
                        await client.content(rpc_id["BattleClientPlayerNotifyBattleEnd"])
                    async with asyncio.timeout(4):
                        while await asyncio.to_thread(runtime.local_inbox._fetch, 1):
                            await asyncio.sleep(0.02)

                    journal_resp = await client.rpc(
                        rpc_id["SceneTrialJournalClientPlayerGetTrialJournal"], 3,
                        p.message("GetTrialJournalRequest"),
                    )
                    assert not journal_resp.error_message.id
                    entry_101 = next(
                        e for ch in journal_resp.snapshot.chapters for e in ch.entries if e.entry_id == 101
                    )
                    assert entry_101.vow_supported is True
                    assert entry_101.best_vow_count == 2
                    assert entry_101.best_vow_mask == (VOW_NO_PET | VOW_BAN_CONTROL)
                    assert entry_101.best_vow_rounds == 4
                    assert entry_101.best_vow_battle_id == 7103
                    assert entry_101.solo_best.best_rounds == 2
                    assert entry_101.solo_best.battle_id == 7104

                    # 校验 PlayerBattleResult 持久回显 trial_vow_mask / trial_new_vow_best / trial_battle_id
                    player = scene.locations[1].players[1]
                    saved = scene.rows.assemble(player.snapshot)
                    res_7103 = next(r for r in saved.battle_results.entries if r.battle_id == 7103)
                    assert res_7103.trial_vow_mask == (VOW_NO_PET | VOW_BAN_CONTROL)
                    assert res_7103.trial_new_vow_best is True
                    assert res_7103.trial_battle_id == 7103
                    res_7104 = next(r for r in saved.battle_results.entries if r.battle_id == 7104)
                    assert res_7104.trial_new_vow_best is False
                    assert res_7104.trial_battle_id == 7104

                    # 3) 装备智能定档连磨（V3）实时 TCP 测试
                    rules_resp = await client.rpc(
                        rpc_id["SceneBagClientPlayerGetEquipmentCraftRules"], 4,
                        p.message("GetEquipmentCraftRulesRequest"),
                    )
                    assert not rules_resp.error_message.id
                    # 选择一件 profile=1 且等级需求 <= 当前角色等级的配方，以及一条非速度(effect!=4)的 pool=1 词条
                    recipe = next(r for r in rules_resp.recipes if r.product.equip_profile == 1)
                    affix = next(a for a in rules_resp.affixes if a.pool == 1 and a.effect != 4)
                    # 升满角色等级、发打造材料 + 词条材料 + 20 份重铸砂 + 5000 银两（同材料 ID 累加，防止字典覆写）
                    data = scene.rows.assemble(player.snapshot)
                    data.level_component.level = max(int(data.level_component.level), 85)
                    assert scene.assets.add_currency(data, 0, 5000) == 0
                    mats: dict[int, int] = {}
                    for mid, cnt in (
                        (recipe.material_config_id, recipe.material_count),
                        (affix.material_config_id, affix.material_count),
                        (POLISH_SAND_ID, 20),
                    ):
                        if mid > 0 and cnt > 0:
                            mats[mid] = mats.get(mid, 0) + cnt
                    assert await scene.assets.add_items(data, mats) == 0
                    await scene.commit_player_data(player, data)

                    # 固定伪随机骰子：安装词条掷 1 档(凡品)，随后连磨 3 手依次掷 [2, 3, 4]，目标 4 档在第 3 手精准停下
                    rolls = iter([1, 2, 3, 4])
                    scene.equip_craft._roll_fn = lambda _tier: next(rolls)

                    # 打造装备
                    forge_resp = await client.rpc(
                        rpc_id["SceneBagClientPlayerCommitEquipmentCraft"], 5,
                        p.message(
                            "CommitEquipmentCraftRequest",
                            client_request_id=92001,
                            rules_fingerprint=rules_resp.rules_fingerprint,
                            expected_forge_revision=rules_resp.forge_revision,
                            choice={"operation": 1, "config_id": recipe.product.config_id},
                        ),
                    )
                    assert not forge_resp.error_message.id
                    eq_item_id = forge_resp.item_id

                    # 安装非速度词条 -> 1 档
                    install_resp = await client.rpc(
                        rpc_id["SceneBagClientPlayerCommitEquipmentCraft"], 6,
                        p.message(
                            "CommitEquipmentCraftRequest",
                            client_request_id=92002,
                            rules_fingerprint=rules_resp.rules_fingerprint,
                            expected_forge_revision=forge_resp.result_forge_revision,
                            choice={"operation": 2, "item_id": eq_item_id, "affix_slot": 1, "affix_id": affix.affix_id},
                        ),
                    )
                    assert not install_resp.error_message.id
                    assert install_resp.result_quality_tier == 1

                    # 非法参数校验：max_polish_attempts=9 拒绝、target_quality_tier<=current_tier(1) 拒绝、保底符+连磨拒绝
                    bad_prev = await client.rpc(
                        rpc_id["SceneBagClientPlayerPreviewEquipmentCraft"], 7,
                        p.message(
                            "PreviewEquipmentCraftRequest",
                            choice={"operation": 4, "item_id": eq_item_id, "affix_slot": 1, "max_polish_attempts": 9},
                        ),
                    )
                    assert bad_prev.error_message.id == INVALID

                    bad_charm = await client.rpc(
                        rpc_id["SceneBagClientPlayerPreviewEquipmentCraft"], 8,
                        p.message(
                            "PreviewEquipmentCraftRequest",
                            choice={
                                "operation": 4, "item_id": eq_item_id, "affix_slot": 1,
                                "use_guarantee_charm": True, "target_quality_tier": 4,
                            },
                        ),
                    )
                    assert bad_charm.error_message.id == INVALID

                    # 正常预览：目标 4 档、最多 6 手 -> max_polish_attempts=6, sand=6, gold=120
                    prev_resp = await client.rpc(
                        rpc_id["SceneBagClientPlayerPreviewEquipmentCraft"], 9,
                        p.message(
                            "PreviewEquipmentCraftRequest",
                            choice={
                                "operation": 4,
                                "item_id": eq_item_id,
                                "affix_slot": 1,
                                "target_quality_tier": 4,
                                "max_polish_attempts": 6,
                            },
                        ),
                    )
                    assert not prev_resp.error_message.id
                    assert prev_resp.preview.available is True
                    assert prev_resp.preview.max_polish_attempts == 6
                    assert prev_resp.preview.max_polish_sand_cost == 6
                    assert prev_resp.preview.max_polish_gold_cost == 6 * POLISH_GOLD_COST

                    # 执行智能定档连磨：第 3 手掷出 4 档立即停手，精准只扣 3 砂 60 银两，熟练度 +3，返回 3 条 PolishStepTrace
                    exp_before = scene.rows.assemble(player.snapshot).craft_component.forge_mastery_exp
                    gold_before = balance(scene.rows.assemble(player.snapshot), 0)
                    batch_req = p.message(
                        "CommitEquipmentCraftRequest",
                        client_request_id=92003,
                        rules_fingerprint=rules_resp.rules_fingerprint,
                        expected_forge_revision=install_resp.result_forge_revision,
                        choice={
                            "operation": 4,
                            "item_id": eq_item_id,
                            "affix_slot": 1,
                            "target_quality_tier": 4,
                            "max_polish_attempts": 6,
                        },
                    )
                    commit_resp = await client.rpc(
                        rpc_id["SceneBagClientPlayerCommitEquipmentCraft"], 10, batch_req,
                    )
                    assert not commit_resp.error_message.id
                    assert commit_resp.replayed is False
                    assert commit_resp.result_quality_tier == 4
                    assert commit_resp.result_polish_pity_count == 3
                    assert commit_resp.guaranteed_by_pity is False
                    assert commit_resp.polish_attempts_used == 3
                    assert commit_resp.actual_gold_spent == 3 * POLISH_GOLD_COST
                    assert len(commit_resp.polish_steps) == 3
                    assert [(s.attempt_index, s.tier_before, s.tier_after) for s in commit_resp.polish_steps] == [
                        (1, 1, 2), (2, 2, 3), (3, 3, 4),
                    ]
                    after_data = scene.rows.assemble(player.snapshot)
                    assert balance(after_data, 0) == gold_before - 3 * POLISH_GOLD_COST
                    assert scene.assets.removable_count(after_data, POLISH_SAND_ID) == 17
                    assert after_data.craft_component.forge_mastery_exp == exp_before + 3

                    # 幂等重放返回首次手数与实际银两消耗，不重复扣砂扣银
                    replay_commit = await client.rpc(
                        rpc_id["SceneBagClientPlayerCommitEquipmentCraft"], 11, batch_req,
                    )
                    assert not replay_commit.error_message.id
                    assert replay_commit.replayed is True
                    assert replay_commit.polish_attempts_used == 3
                    assert replay_commit.actual_gold_spent == 3 * POLISH_GOLD_COST
                    assert balance(scene.rows.assemble(player.snapshot), 0) == gold_before - 3 * POLISH_GOLD_COST
                    assert scene.assets.removable_count(scene.rows.assemble(player.snapshot), POLISH_SAND_ID) == 17

                    # 继续连磨触发第 8 次硬保底直出极品(5 档)：剩余保底 5 手，全掷 1 档也在第 5 手保底极品
                    scene.equip_craft._roll_fn = lambda _tier: 1
                    pity_resp = await client.rpc(
                        rpc_id["SceneBagClientPlayerCommitEquipmentCraft"], 12,
                        p.message(
                            "CommitEquipmentCraftRequest",
                            client_request_id=92004,
                            rules_fingerprint=rules_resp.rules_fingerprint,
                            expected_forge_revision=commit_resp.result_forge_revision,
                            choice={
                                "operation": 4,
                                "item_id": eq_item_id,
                                "affix_slot": 1,
                                "target_quality_tier": 5,
                                "max_polish_attempts": 8,
                            },
                        ),
                    )
                    assert not pity_resp.error_message.id
                    assert pity_resp.result_quality_tier == 5
                    assert pity_resp.result_polish_pity_count == 0
                    assert pity_resp.guaranteed_by_pity is True
                    assert pity_resp.polish_attempts_used == 5
                    assert pity_resp.polish_steps[-1].guaranteed_by_pity is True
                    assert pity_resp.polish_steps[-1].tier_after == 5
            finally:
                await control.close()

    asyncio.run(run())
