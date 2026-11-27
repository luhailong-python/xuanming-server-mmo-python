"""C++ 60b0bd6b3..6b4ae7852 场景/帮战/数据服务/场景管理器增量移植全覆盖回归测试。

覆盖范围：
1. Scene 配装预设方案 A（7 套三合一共享席位、到期只锁不删、退役旧买断 539、linked_pet_id & linked_formation_id 联动与快照 pets/formations）+ SceneSeason.try_equip_linked_formation；
2. Scene 改名加固（未知枚举按瞬时保留、改名卡扣减失败拒绝白改、加载清理孤立改名卡预留、不可写态延迟）；
3. Scene 神秘商人启动表校验 `validate_tables() -> bool`；
4. Scene 战斗准入与统一复核（314 豁免场景检查、pending_silver_fee_review_after_ms 落盘、废弃 skip_jianghu_silver_refund 照退、ABORT 双清理与 COMPLETE 已应用清残标）；
5. Guild 帮战入账分流与规则加固（r2-gw#1..#8：目标提示收窄、BannerDelta.basis_holds、追杀令可见性隔离、到期原路退本、终态清理、guild_war_credit_owed 分流/护栏阻断/游标转换/幂等原 op_id/墓碑放弃）；
6. DataService 甲级回收双人复核（AdminApproval 校验、生产默认 fail-closed、留痕 trail）与合服映射 etcd 副本一致性状态机（MergedZoneSource / decide_merged_zone_publish）；
7. SceneManager 会话 gate_instance_id 身份裁决（§14.8 写 location + 托管落点透传）与 OwnerAuthority mode=enforce 拒启（r2-oa#1）。
"""

from __future__ import annotations

import asyncio
from itertools import count
from types import SimpleNamespace

import grpc
import pytest

from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.data.asset_inventory import FEATURE_UNAVAILABLE, INVALID, balance
from xuanming.data_service.config import DataServiceConfig
from xuanming.data_service.merged_zone_copy import (
    MemoryMergedZoneCopies,
    MergedZoneMapping,
    MergedZoneSource,
    ShardZeroUnreachableError,
    decode_merged_zone_copy,
    decide_merged_zone_publish,
    encode_merged_zone_copy,
    merged_zone_copy_key,
)
from xuanming.data_service.recall import approval_trail, validate_admin_approval, with_approval_trail
from xuanming.data_service.service import DataService
from xuanming.guild import (
    CreditFenceVerdict,
    GuildTableCatalog,
    MemoryGuildStore,
    ROLE_BLOCKER_WAR_CREDIT_OWED,
    RoleFenceRow,
    RoleOriginal,
    WarCreditOwed,
    WarCreditOwedCursor,
    WarOwedConvertStatus,
    classify_credit_fence,
)
from xuanming.guild.model import (
    ASSET_STREAM_GUILD_CREDIT,
    OP_KIND_BOUNTY_PAYOUT,
    OP_KIND_WAR_REWARD,
    TX_GUILD_BOUNTY_PAYOUT,
    TX_GUILD_WAR_REWARD,
)
from xuanming.guild.war import (
    BOUNTY_STATUS_ACTIVE,
    BOUNTY_STATUS_COMPLETED,
    BOUNTY_STATUS_EXPIRED,
    BOUNTY_TIER_GUILD_PUBLIC,
    BOUNTY_TIER_NORMAL_SILVER,
    BannerDelta,
    GuildWarManager,
    TARGET_KIND_BOUNTY,
    TARGET_KIND_FIELD_PLAYER,
    TARGET_KIND_LEYLINE_GARRISON,
    TARGET_KIND_LIVE_PLAYER,
    _LeylineState,
    bounty_expiry_refund,
    bounty_huntable,
    client_war_target_hints,
    field_target_bounty,
    find_leyline_basis,
)
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_afk_pass import SceneAfkPass
from xuanming.nodes.scene_assets import CURRENCY_GOLD, SceneAssets
from xuanming.nodes.scene_attributes import SceneAttributes
from xuanming.nodes.scene_battle import SceneBattle
from xuanming.nodes.scene_equip_loadout import (
    EQUIP_LOADOUT_SCHEME_INVALID,
    FREE_SCHEMES,
    MAX_SCHEMES,
    SceneEquipLoadout,
)
from xuanming.nodes.scene_jianghu import MATCH_MODE_JIANGHU_DUEL, SceneJianghu
from xuanming.nodes.scene_merchant import SceneMerchant
from xuanming.nodes.scene_pets import ScenePets
from xuanming.nodes.scene_rename import (
    ITEM_HOLD_KIND_RENAME,
    RENAME_ROLE_BUSY,
    RENAME_ROLE_OK,
    RENAME_ROLE_TAKEN,
    RENAME_ROLE_UNSPECIFIED,
    SceneRename,
    is_transient_result,
)
from xuanming.nodes.scene_season import SceneSeason
from xuanming.nodes.scene_skills import SceneSkills
from xuanming.protocol import Protocol
from xuanming.scene_manager.owner_authority import OwnerAuthorityConfig
from xuanming.scene_manager.service import (
    PlacementIdentity,
    PlacementVerdict,
    decide_placement_identity,
    identity_is_hosted_login,
    same_gate_instance,
)


# ── 1. Scene 配装预设方案 A（7 套统一席位 + 灵宠/阵法联动） ────────────────────────────────────


async def _make_scene_env(tmp_path, *, level: int = 60, gold: int = 50_000, now_ms_ref: list[int] | None = None):
    protocol = Protocol()
    tables = ConfigTables()
    store = SqlitePlayerStore(tmp_path / "players.sqlite3")
    codec = PlayerRowCodec(protocol)
    seed = await store.load_and_claim(1, b"seed")
    data = protocol.message("player_database", player_id=1)
    data.profile_component.name = "青衫侠"
    setattr(data.uint32_pb_component, "class", 1)
    data.level_component.level = level
    data.derived_attributes_component.health = 500
    data.derived_attributes_component.mana = 300
    data.currency.values.extend([gold, 1000, 500, 0])
    data.attribute_component.schemes.add(scheme_id=1, name="默认加点")
    data.attribute_component.active_scheme_id = 1
    data.attribute_component.next_scheme_id = 2
    data.skill_list.SetInParent()
    data.pet_component.SetInParent()
    data.season_component.SetInParent()
    data.equip_loadout_component.SetInParent()
    data.jianghu_component.SetInParent()
    await store.commit(seed, codec.project(data), release=True)

    node = protocol.message("NodeInfo", node_id=7, node_uuid="scene-6b4a", zone_id=1)
    scene = SceneNode(protocol, node, store, codec, lambda *_: None)
    scene.tables = tables
    guids = count(50_000)

    async def alloc_guid():
        return next(guids)

    clock_ms = (lambda: now_ms_ref[0]) if now_ms_ref is not None else (lambda: 1_800_000_000_000)
    clock_s = lambda: clock_ms() // 1000

    assets = scene.assets = SceneAssets(scene, tables, {}, allow_gm=True, allocate_item_id=alloc_guid)
    attributes = scene.attributes = SceneAttributes(scene, tables, assets, allow_gm=True, now_s=clock_s)
    skills = scene.skills = SceneSkills(scene, tables, now_s=clock_s)
    pets = scene.pets = ScenePets(scene, tables)
    afk_pass = scene.afk_pass = SceneAfkPass(scene, tables, assets, allow_gm=True, now_s=clock_s)
    season = scene.season = SceneSeason(scene, tables, assets, clock=clock_ms)
    equip_loadout = scene.equip_loadout = SceneEquipLoadout(scene, tables, assets, now_s=clock_s)
    jianghu = scene.jianghu = SceneJianghu(scene, tables, assets, clock=clock_ms)
    battle = scene.battle = SceneBattle(scene, tables, now_ms=clock_ms)

    assets.register_client_handlers()
    attributes.register()
    skills.register()
    pets.register()
    afk_pass.register()
    season.register()
    equip_loadout.register()
    jianghu.register()

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
        pets=pets,
        afk_pass=afk_pass,
        season=season,
        equip_loadout=equip_loadout,
        jianghu=jianghu,
        battle=battle,
        player=scene.locations[1].players[1],
        rpc=rpc,
    )


def test_equip_loadout_plan_a_7_slots_linked_pet_and_formation(tmp_path):
    async def run():
        now_ms = [1_800_000_000_000]
        env = await _make_scene_env(tmp_path, level=60, now_ms_ref=now_ms)
        assert FREE_SCHEMES == 2 and MAX_SCHEMES == 7

        # 注入 2 只灵宠（表首行）
        pet_row = env.tables["Pet"].rows()[0]
        data = env.scene.rows.assemble(env.player.snapshot)
        p1 = data.pet_component.pets.add(pet_id=701, pet_table_id=int(pet_row.id), name="青霜狐", level=1)
        p2 = data.pet_component.pets.add(pet_id=702, pet_table_id=int(pet_row.id), name="赤炎麟", level=1)
        data.pet_component.active_pet_id = p1.pet_id
        await env.scene.commit_player_data(env.player, data)

        # 初始 2 免费套 + 上限 7 套 + 灵宠与 5 套免费五行阵法列表
        snap0 = (await env.rpc("SceneBagClientPlayer", "GetEquipLoadouts")).snapshot
        assert snap0.unlocked_scheme_count == 2
        assert snap0.max_scheme_count == 7
        assert snap0.free_schemes == 2
        assert snap0.light_card_bonus_schemes == 0
        assert snap0.rent_unit_price_diamond == 30
        assert snap0.rent_period_days == 30
        assert snap0.rent_max_remaining_days == 180
        assert snap0.next_unlock_cost_diamond == 0
        assert len(snap0.schemes) == 7
        assert [p.pet_id for p in snap0.pets] == [701, 702]
        assert snap0.pets[0].active is True and snap0.pets[1].active is False
        assert [(f.scheme_id, f.name) for f in snap0.formations] == [
            (1, "天覆阵"),
            (2, "地载阵"),
            (3, "风扬阵"),
            (4, "云垂阵"),
            (5, "虎翼阵"),
        ]

        # 旧独立买断入口 539 退役，返回 REJECTED + FEATURE_UNAVAILABLE
        unlock = await env.rpc(
            "SceneBagClientPlayer",
            "UnlockEquipLoadout",
            client_request_id=9901,
            expected_unlocked_count=2,
        )
        assert unlock.outcome == 1
        assert unlock.error_message.id == FEATURE_UNAVAILABLE

        # 非法 linked_pet_id / linked_formation_id 拒绝
        bad_pet = await env.rpc(
            "SceneBagClientPlayer",
            "SaveEquipLoadout",
            scheme_id=2,
            name="错宠套",
            state_version=snap0.state_version,
            request_id="req-6b4a-bad-pet-0001",
            capture_current_equipped=True,
            linked_pet_id=99999,
        )
        assert bad_pet.error_message.id == INVALID
        bad_form = await env.rpc(
            "SceneBagClientPlayer",
            "SaveEquipLoadout",
            scheme_id=2,
            name="错阵套",
            state_version=snap0.state_version,
            request_id="req-6b4a-bad-form-001",
            capture_current_equipped=True,
            linked_formation_id=6,
        )
        assert bad_form.error_message.id == INVALID

        # 保存方案 2 绑定灵宠 702 + 云垂阵(4)，切换后自动出战 702 并切换五行阵法 4
        save2 = await env.rpc(
            "SceneBagClientPlayer",
            "SaveEquipLoadout",
            scheme_id=2,
            name="灵宠阵法套",
            state_version=snap0.state_version,
            request_id="req-6b4a-save-2-00001",
            capture_current_equipped=True,
            linked_pet_id=702,
            linked_formation_id=4,
        )
        assert not save2.error_message.id
        assert save2.snapshot.schemes[1].linked_pet_id == 702
        assert save2.snapshot.schemes[1].linked_formation_id == 4

        sw2 = await env.rpc(
            "SceneBagClientPlayer",
            "SwitchEquipLoadout",
            scheme_id=2,
            state_version=save2.snapshot.state_version,
            request_id="req-6b4a-switch-2-001",
            apply_linked_schemes=True,
        )
        assert not sw2.error_message.id
        assert not sw2.linked_notices
        after = env.scene.rows.assemble(env.player.snapshot)
        assert int(after.pet_component.active_pet_id) == 702
        assert int(after.season_component.equipped_formation_id) == 4

        # 开通轻享卡（+2 套 -> 4 套），切到方案 4 后轻享卡过期：到期只锁不删（active 保留 4，再切 4 被拒）
        await env.rpc("SceneAfkPassClientPlayer", "GmSetAfkPass", remaining_seconds=600)
        snap_pass = (await env.rpc("SceneBagClientPlayer", "GetEquipLoadouts")).snapshot
        assert snap_pass.unlocked_scheme_count == 4
        assert snap_pass.light_card_bonus_schemes == 2

        sw4 = await env.rpc(
            "SceneBagClientPlayer",
            "SwitchEquipLoadout",
            scheme_id=4,
            state_version=snap_pass.state_version,
            request_id="req-6b4a-switch-4-001",
        )
        assert not sw4.error_message.id
        assert sw4.snapshot.active_scheme_id == 4

        # 时间推进超过轻享卡有效期
        now_ms[0] += 1_000_000
        snap_exp = (await env.rpc("SceneBagClientPlayer", "GetEquipLoadouts")).snapshot
        assert snap_exp.unlocked_scheme_count == 2
        assert snap_exp.active_scheme_id == 4  # 到期只锁不删

        locked_save = await env.rpc(
            "SceneBagClientPlayer",
            "SaveEquipLoadout",
            scheme_id=4,
            name="已锁方案",
            state_version=snap_exp.state_version,
            request_id="req-6b4a-locked-4-001",
            capture_current_equipped=True,
        )
        assert locked_save.error_message.id == EQUIP_LOADOUT_SCHEME_INVALID

        # 绑定灵宠被移走时切换不失败，仅追加中文提示
        data = env.scene.rows.assemble(env.player.snapshot)
        del data.pet_component.pets[1]  # 移除 702
        data.pet_component.active_pet_id = 701
        await env.scene.commit_player_data(env.player, data)

        sw2_missing = await env.rpc(
            "SceneBagClientPlayer",
            "SwitchEquipLoadout",
            scheme_id=2,
            state_version=snap_exp.state_version,
            request_id="req-6b4a-switch-2-002",
            apply_linked_schemes=True,
        )
        assert not sw2_missing.error_message.id
        assert any("首发灵宠已不在灵宠栏" in n for n in sw2_missing.linked_notices)

    asyncio.run(run())


# ── 2. Scene 改名加固 & 神秘商人表校验 & 战斗待退银两统一复核 ────────────────────────────────


def test_scene_rename_hardening_unknown_enum_card_consume_and_orphan_hold(tmp_path):
    assert is_transient_result(RENAME_ROLE_OK) is False
    assert is_transient_result(RENAME_ROLE_TAKEN) is False
    assert is_transient_result(RENAME_ROLE_UNSPECIFIED) is True
    assert is_transient_result(RENAME_ROLE_BUSY) is True
    assert is_transient_result(99) is True

    async def run():
        env = await _make_scene_env(tmp_path)

        async def fake_unknown_result(_path, _raw):
            return env.protocol.message("data_service.RenameRoleResponse", result=99).SerializeToString()

        rename = SceneRename(env.scene, env.tables, env.assets, data_service_call=fake_unknown_result)
        # 1) 未知枚举 99 按瞬时处理：保留 rename_op 与改名卡预留
        data = env.scene.rows.assemble(env.player.snapshot)
        data.profile_component.rename_seq = 1
        data.profile_component.rename_op.seq = 1
        data.profile_component.rename_op.name = "新名侠"
        data.profile_component.rename_op.hold_id = 8801
        data.profile_component.rename_op.card_item_id = 3401
        data.profile_component.rename_op.card_count = 1
        h = data.item_holds.holds.add(hold_id=8801, kind=ITEM_HOLD_KIND_RENAME)
        h.lines.add(config_id=3401, count=1)
        await env.scene.commit_player_data(env.player, data)

        await rename._complete_rename(1, 1)
        after_unknown = env.scene.rows.assemble(env.player.snapshot)
        assert after_unknown.profile_component.HasField("rename_op")
        assert len(after_unknown.item_holds.holds) == 1

        # 2) data_service 返回 OK 但背包无改名卡（扣卡失败）：绝不白改，保留 rename_op 与预留
        await rename._commit_success(1, 1, "新名侠", 1_800_000_000_000, 604_800)
        after_fail_consume = env.scene.rows.assemble(env.player.snapshot)
        assert after_fail_consume.profile_component.name == "青衫侠"
        assert after_fail_consume.profile_component.HasField("rename_op")
        assert len(after_fail_consume.item_holds.holds) == 1

        # 3) 无在途 rename_op 却残留 kind=2 孤立预留：resume_pending 自动释放并落盘
        after_fail_consume.profile_component.ClearField("rename_op")
        await env.scene.commit_player_data(env.player, after_fail_consume)
        task = rename.resume_pending(env.player)
        assert task is not None
        await task
        after_clean = env.scene.rows.assemble(env.player.snapshot)
        assert len(after_clean.item_holds.holds) == 0

    asyncio.run(run())


def test_mystery_merchant_validate_tables(tmp_path):
    async def run():
        env = await _make_scene_env(tmp_path)
        merchant = SceneMerchant(env.scene, env.tables, env.assets)
        assert merchant.validate_tables() is True

        # 构造坏表验证返回 False
        class _BadTables:
            def __init__(self):
                self.tables = {
                    "MysteryMerchantRule": SimpleNamespace(rows=lambda: [SimpleNamespace(interact_range=0)]),
                    "MysteryMerchantGoods": SimpleNamespace(
                        rows=lambda: [
                            SimpleNamespace(id=1, weight=0, price=100, limit_count=1, currency_type=0, item_id=1, item_count=1)
                        ]
                    ),
                    "MysteryMerchantSpot": SimpleNamespace(rows=lambda: []),
                    "Item": SimpleNamespace(get=lambda _id: None),
                }

            def __getitem__(self, key):
                return self.tables[key]

        bad_merchant = SceneMerchant(env.scene, _BadTables(), env.assets)
        assert bad_merchant.validate_tables() is False

    asyncio.run(run())


def test_scene_battle_silver_fee_review_and_cancel_ignores_skip_flag(tmp_path):
    async def run():
        now_ms = [1_800_000_000_000]
        env = await _make_scene_env(tmp_path, gold=10_000, now_ms_ref=now_ms)
        # 1) 备战扣普通生死状银两并落盘 pending_silver_fee_review_after_ms；取消即使带 skip=True 也照退
        data = env.scene.rows.assemble(env.player.snapshot)
        assert env.jianghu.commit_prepare_silver_fee(data, 5001, 500) == 0
        data.jianghu_component.pending_silver_fee_review_after_ms = now_ms[0] - 1
        await env.scene.commit_player_data(env.player, data)
        assert balance(env.scene.rows.assemble(env.player.snapshot), CURRENCY_GOLD) == 9500

        cancel_req = env.protocol.message(
            "CancelBattlePrepareRequest",
            player_id=1,
            battle_id=5001,
            skip_jianghu_silver_refund=True,
        )
        await env.battle.cancel(cancel_req)
        after_cancel = env.scene.rows.assemble(env.player.snapshot)
        assert balance(after_cancel, CURRENCY_GOLD) == 10_000
        assert after_cancel.jianghu_component.pending_silver_fee_battle_id == 0
        assert after_cancel.jianghu_component.pending_silver_fee_review_after_ms == 0

        # 2) 统一复核 _review_holds：ABORT 同时释放道具预留与退还待退银两
        data2 = env.scene.rows.assemble(env.player.snapshot)
        assert env.jianghu.commit_prepare_silver_fee(data2, 5002, 500) == 0
        data2.jianghu_component.pending_silver_fee_review_after_ms = now_ms[0] - 10
        h = data2.item_holds.holds.add(hold_id=5002, kind=1, created_at_ms=now_ms[0] - 100, review_after_ms=now_ms[0] - 10)
        h.lines.add(config_id=3401, count=1)
        await env.scene.commit_player_data(env.player, data2)

        async def decision_abort(_bid):
            return (2, False)

        env.battle._decision = decision_abort
        await env.battle._review_holds(1)
        after_abort = env.scene.rows.assemble(env.player.snapshot)
        assert balance(after_abort, CURRENCY_GOLD) == 10_000
        assert len(after_abort.item_holds.holds) == 0
        assert after_abort.jianghu_component.pending_silver_fee_battle_id == 0

        # 3) 统一复核 COMPLETE 且本地已应用结算：清掉残留待退银两标记但不退钱
        data3 = env.scene.rows.assemble(env.player.snapshot)
        assert env.jianghu.commit_prepare_silver_fee(data3, 5003, 500) == 0
        data3.jianghu_component.pending_silver_fee_review_after_ms = now_ms[0] - 10
        cursor = data3.inbox_ledger.cursors.add(domain=2)
        cursor.recent.add(request_id=5003, msg_type=4, outcome=1)
        await env.scene.commit_player_data(env.player, data3)

        async def decision_complete(_bid):
            return (1, False)

        env.battle._decision = decision_complete
        await env.battle._review_holds(1)
        after_complete = env.scene.rows.assemble(env.player.snapshot)
        assert balance(after_complete, CURRENCY_GOLD) == 9500
        assert after_complete.jianghu_component.pending_silver_fee_battle_id == 0

    asyncio.run(run())


# ── 3. Guild War Credit Owed、规则收窄、到期退本与终态清理（r2-gw#1..#8） ──────────────────────


def test_guild_war_rules_credit_owed_fence_and_bounty_lifecycle():
    # 纯规则单元测试：r2-gw#1 / #3 / #8
    assert client_war_target_hints(TARGET_KIND_LEYLINE_GARRISON, 2, 99) == (2, 0)
    assert client_war_target_hints(TARGET_KIND_BOUNTY, 2, 99) == (0, 99)
    assert client_war_target_hints(TARGET_KIND_LIVE_PLAYER, 2, 99) == (0, 0)
    assert client_war_target_hints(TARGET_KIND_FIELD_PLAYER, 2, 99) == (0, 0)

    ll1 = _LeylineState(zone_id=1, leyline_id=1, owner_guild_id=10, banner_hp=80, shield_until_ms=5000)
    ll2 = _LeylineState(zone_id=1, leyline_id=2, owner_guild_id=20, banner_hp=100, shield_until_ms=0)
    cur, owns_other = find_leyline_basis([ll1, ll2], 1, 20)
    assert cur is ll1 and owns_other is True
    delta = BannerDelta(hp_before=80, owner_before=10, shield_until_before=5000, attacker_owns_other=True)
    assert delta.basis_holds(cur, True) is True
    assert delta.basis_holds(cur, False) is False
    ll1.banner_hp = 70
    assert delta.basis_holds(cur, True) is False

    # classify_credit_fence
    assert classify_credit_fence(None, healed=False, fence_enabled=True) == (CreditFenceVerdict.OPEN, True)
    removed_fence = RoleFenceRow(player_id=1, account="", owner_revision=1, freeze_token=0, freeze_final=False, version=2, home_zone=1)
    assert classify_credit_fence(removed_fence, healed=False, fence_enabled=True) == (CreditFenceVerdict.REMOVED, False)
    frozen_fence = RoleFenceRow(player_id=1, account="acc", owner_revision=1, freeze_token=77, freeze_final=False, version=2, home_zone=1)
    assert classify_credit_fence(frozen_fence, healed=False, fence_enabled=True) == (CreditFenceVerdict.OPEN, True)
    assert classify_credit_fence(frozen_fence, healed=True, fence_enabled=True) == (CreditFenceVerdict.FROZEN, False)

    async def run():
        protocol = Protocol()
        now = [1_800_000_000_000]
        store = MemoryGuildStore(protocol, clock_ms=lambda: now[0])
        war = GuildWarManager(protocol, store, clock_ms=lambda: now[0])

        # 创建两个帮会（帮主 101 -> 帮会 A，帮主 201 -> 帮会 B）
        a_orig = RoleOriginal(account="acc-a", player_id=101, owner_revision=1)
        b_orig = RoleOriginal(account="acc-b", player_id=201, owner_revision=1)
        c_orig = RoleOriginal(account="acc-c", player_id=301, owner_revision=1)
        g_a = await store.create_guild(a_orig, name="青云阁", name_norm="青云阁", zone_id=1, now_ms=now[0])
        g_b = await store.create_guild(b_orig, name="玄冥殿", name_norm="玄冥殿", zone_id=1, now_ms=now[0])
        g_c = await store.create_guild(c_orig, name="落星宗", name_norm="落星宗", zone_id=1, now_ms=now[0])
        await war._credit_guild_funds(g_a.guild_id, 5000)

        # 发布帮内私有追杀令（public_board=False）
        post_priv = await war.post_bounty(
            a_orig,
            protocol.message(
                "guildpb.PostGuildWarBountyRequest",
                target_player_id=201,
                tier=BOUNTY_TIER_NORMAL_SILVER,
                public_board=False,
                client_request_id=7001,
            ),
        )
        assert not post_priv.error_message.id
        bid_priv = post_priv.bounty.bounty_id

        # r2-gw#8：第三帮 C 查野外目标不应看到帮 A 的私有追杀令，帮 A 自己可见
        insp_c = await war.inspect_field_targets(
            c_orig,
            protocol.message("guildpb.InspectFieldWarTargetsRequest", player_ids=[201]),
        )
        assert insp_c.targets[0].active_bounty_id == 0
        assert insp_c.targets[0].active_bounty_reward_silver == 0
        insp_a = await war.inspect_field_targets(
            a_orig,
            protocol.message("guildpb.InspectFieldWarTargetsRequest", player_ids=[201]),
        )
        assert insp_a.targets[0].active_bounty_id == bid_priv
        assert insp_a.targets[0].active_bounty_reward_silver > 0

        # 冻结 101 的角色护栏，让私有银两追杀令到期 -> 退本自动转入 guild_war_credit_owed
        await store.apply_role_fence(
            protocol.message(
                "rolefence.RoleFenceState",
                player_id=101,
                account="acc-a",
                owner_revision=1,
                freeze_token=888,
                version=1,
                home_zone=1,
            )
        )
        now[0] += war.rule.bounty_ttl_sec * 1000 + 1000
        exp_stats = await war.expire_due_bounties(now[0])
        assert exp_stats["expired"] == 1
        assert exp_stats["owed_count"] == 1
        assert exp_stats["ops_created"] == 0

        owed_rows = await store.list_war_credit_owed()
        assert len(owed_rows) == 1
        owed_op_id = owed_rows[0].op_id
        assert owed_rows[0].player_id == 101
        assert owed_rows[0].kind == OP_KIND_BOUNTY_PAYOUT
        assert owed_rows[0].tx_type == TX_GUILD_BOUNTY_PAYOUT

        # r2-gw#5 / #6：存在 guild_war_credit_owed 时阻断过户/删角护栏检查
        # 先退帮确保不被 guild_member 抢先阻断，检验 guild_war_credit_owed 阻断器
        await store.apply_role_fence(
            protocol.message(
                "rolefence.RoleFenceState",
                player_id=999,
                account="acc-999",
                owner_revision=1,
                freeze_token=123,
                version=1,
                home_zone=1,
            )
        )
        _, owed_999, verdict_999 = await war.credit_war_asset(
            player_id=999,
            guild_id=g_a.guild_id,
            silver=1500,
            kind=OP_KIND_WAR_REWARD,
            tx_type=TX_GUILD_WAR_REWARD,
            source_id=42,
            now_ms=now[0],
        )
        assert owed_999 is not None and verdict_999 == CreditFenceVerdict.FROZEN
        freeze_out = await store.prepare_role_freeze(
            protocol.message(
                "rolefence.RoleFenceState",
                player_id=999,
                account="acc-999",
                owner_revision=1,
                freeze_token=124,
                version=2,
                home_zone=1,
            ),
            dry_run=True,
        )
        assert freeze_out.clean is False
        assert freeze_out.blocker == ROLE_BLOCKER_WAR_CREDIT_OWED

        # 冻结期内跑转换轮：保持在 owed 表中（stats.frozen == 2）
        cursor, stats_frozen = await war.convert_war_credit_owed_round(WarCreditOwedCursor(), now[0])
        assert stats_frozen.frozen == 2 and stats_frozen.converted == 0
        assert cursor == WarCreditOwedCursor()

        # 解冻 101，将 999 置墓碑 -> 101 转为 GUILD_CREDIT 指令（保留原 op_id！），999 放弃并删行
        await store.apply_role_fence(
            protocol.message(
                "rolefence.RoleFenceState",
                player_id=101,
                account="acc-a",
                owner_revision=1,
                freeze_token=0,
                version=2,
                home_zone=1,
            )
        )
        await store.apply_role_fence(
            protocol.message(
                "rolefence.RoleFenceState",
                player_id=999,
                account="",
                owner_revision=1,
                freeze_token=0,
                version=3,
                home_zone=1,
            )
        )
        _, stats_done = await war.convert_war_credit_owed_round(WarCreditOwedCursor(), now[0] + 1000)
        assert stats_done.converted == 1
        assert stats_done.abandoned == 1
        assert await store.list_war_credit_owed() == []
        assert owed_op_id in store.state.asset_ops
        assert store.state.asset_ops[owed_op_id].stream == ASSET_STREAM_GUILD_CREDIT
        assert store.state.asset_ops[owed_op_id].player_id == 101

        # r2-gw#2 公费追杀令到期退帮会资金 + r2-gw#7 终态保留期清理
        funds_before = (await store.get_guild(g_a.guild_id)).funds
        post_pub = await war.post_bounty(
            a_orig,
            protocol.message(
                "guildpb.PostGuildWarBountyRequest",
                target_player_id=301,
                tier=BOUNTY_TIER_GUILD_PUBLIC,
                public_board=True,
                client_request_id=7002,
            ),
        )
        assert not post_pub.error_message.id
        assert (await store.get_guild(g_a.guild_id)).funds == funds_before - war.rule.bounty_public_funds
        now[0] += war.rule.bounty_ttl_sec * 1000 + 1000
        exp_pub = await war.expire_due_bounties(now[0])
        assert exp_pub["funds_refunded"] == war.rule.bounty_public_funds
        assert (await store.get_guild(g_a.guild_id)).funds == funds_before

        # 终态未过 30 天保留期不删；超过 30 天批量清理
        assert await war.purge_terminal_bounties(now[0]) == 0
        purged = await war.purge_terminal_bounties(now[0] + 31 * 86_400_000)
        assert purged == 2

    asyncio.run(run())


# ── 4. DataService 双人复核批准记录 & 合服映射 etcd 副本一致性 ─────────────────────────────────


def test_data_service_dual_control_approval_and_merged_zone_copy():
    protocol = Protocol()
    ok_approval = protocol.message(
        "common.base.AdminApproval",
        approval_id="7001",
        proposer="alice",
        approver="bob",
        approved_at_ms=1_800_000_000_000,
    )
    assert validate_admin_approval(ok_approval, "alice", request_id=7001) == (None, None)
    assert approval_trail(ok_approval) == 'approval_id="7001" approver="bob"'
    assert with_approval_trail("rollback dup", ok_approval) == 'rollback dup [approval_id="7001" approver="bob"]'

    # 各类非法批准记录
    assert validate_admin_approval(None, "alice")[0] == "missing"
    same_person = protocol.message(
        "common.base.AdminApproval",
        approval_id="7001",
        proposer="alice",
        approver="alice",
        approved_at_ms=1,
    )
    assert validate_admin_approval(same_person, "alice")[0] == "invalid"
    assert validate_admin_approval(ok_approval, "charlie", request_id=7001)[0] == "invalid"
    assert validate_admin_approval(ok_approval, "alice", request_id=7002)[0] == "invalid"

    # DataServiceConfig 默认关闭 admin_allow_unapproved（fail-closed）
    assert DataServiceConfig(node_id=1).admin_allow_unapproved is False

    # DataService 服务入口实测：admin_allow_unapproved=False 拒绝未批准执行；带非法批准一律拒
    class _FakeCtx:
        def __init__(self, token: str):
            self._token = token

        def invocation_metadata(self):
            return [("x-admin-token", self._token)]

        def peer(self):
            return "ipv4:127.0.0.1:9999"

        async def abort(self, code, details):
            raise RuntimeError(f"{code.name}:{details}")

    async def run():
        from test_data_service import MemoryStore, SECRETS

        store = MemoryStore(protocol)
        svc = DataService(
            protocol,
            store,
            SECRETS,
            admin_token="secret-admin",
            admin_allow_unapproved=False,
        )
        ctx = _FakeCtx("secret-admin")

        # 未带 approval 调用 CancelRecallBatch -> PERMISSION_DENIED
        cancel_req = protocol.message(
            "data_service.CancelRecallBatchRequest",
            request_id=7001,
            operator="alice",
            reason="fix",
        )
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            await svc.cancel_recall_batch(cancel_req, ctx, "/data_service.DataService/CancelRecallBatch")

        # 演练 dry_run=True 不带 approval 放行，但带自批 approval 仍被拒
        dry_bad = protocol.message(
            "data_service.BatchRecallItemsRequest",
            recall_kind=1,
            currency_type=0,
            time_start=1000,
            time_end=2000,
            dry_run=True,
            operator="alice",
            approval=same_person,
        )
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            await svc.batch_recall_items(dry_bad, ctx, "/data_service.DataService/BatchRecallItems")

        # 合格双人复核通过准入（到达业务逻辑返回 NOT_FOUND 而非 PERMISSION_DENIED）
        cancel_ok = protocol.message(
            "data_service.CancelRecallBatchRequest",
            request_id=7001,
            operator="alice",
            reason="fix",
            approval=ok_approval,
        )
        resp_ok = await svc.cancel_recall_batch(cancel_ok, ctx, "/data_service.DataService/CancelRecallBatch")
        assert resp_ok.error_code != 0  # batch 7001 不存在，但已通过双人复核准入

        # 合服映射 etcd 副本（MergedZoneSource + decide_merged_zone_publish）
        assert merged_zone_copy_key("mmorpg_global_00") == "/mmorpg/merged_zone_copy/mmorpg_global_00"
        m1 = MergedZoneMapping(generation="gen-1", version=1, merges={2: 1})
        assert decode_merged_zone_copy(encode_merged_zone_copy(m1)) == m1
        assert decide_merged_zone_publish("k", None, m1) is True
        assert decide_merged_zone_publish("k", m1, m1) is False
        with pytest.raises(ValueError, match="directory generation"):
            decide_merged_zone_publish("k", m1, MergedZoneMapping("gen-2", 1, {2: 1}))
        with pytest.raises(ValueError, match="behind version"):
            decide_merged_zone_publish("k", MergedZoneMapping("gen-1", 3, {2: 1}), m1)
        with pytest.raises(ValueError, match="different contents"):
            decide_merged_zone_publish("k", m1, MergedZoneMapping("gen-1", 1, {3: 1}))

        class _FakeTruth:
            def __init__(self):
                self.mapping = m1
                self.err: Exception | None = None

            async def read_merged_zone_mapping(self) -> MergedZoneMapping:
                if self.err is not None:
                    raise self.err
                return self.mapping

        truth = _FakeTruth()
        copies = MemoryMergedZoneCopies("mmorpg_global")
        source = MergedZoneSource(truth, copies)
        assert await source.read() == {2: 1}
        published, rev = await copies.get()
        assert published == m1 and rev == 1

        # 新副本在 shard 0 连接不可达时可从 etcd 副本启动
        truth.err = ShardZeroUnreachableError("connection refused")
        cold = MergedZoneSource(truth, copies)
        assert await cold.read() == {2: 1}

        # 新副本在非连接类错误（如缺表）时拒绝从副本启动
        truth.err = RuntimeError("table missing")
        cold_bad = MergedZoneSource(truth, copies)
        with pytest.raises(RuntimeError, match="non-connectivity error"):
            await cold_bad.read()

        # 运行期 shard 0 不可读且副本有更高版本 -> 采纳更高版本；同版本则报错触发刷新告警
        m2 = MergedZoneMapping(generation="gen-1", version=2, merges={2: 1, 3: 1})
        assert await copies.put(rev, m2) is True
        assert await cold.read() == {2: 1, 3: 1}
        with pytest.raises(RuntimeError, match="keeping version 2"):
            await cold.read()

    asyncio.run(run())


# ── 5. SceneManager gate_instance_id 身份裁决与 OwnerAuthority enforce 拒启 ────────────────────


def test_scene_manager_gate_instance_identity_hosted_forward_and_oa_enforce_refused():
    # §14.8 same_gate_instance & decide_placement_identity
    assert same_gate_instance("", "gate-a") is True
    assert same_gate_instance("gate-a", "") is True
    assert same_gate_instance("gate-a", "gate-a") is True
    assert same_gate_instance("gate-a", "gate-b") is False

    cur = PlacementIdentity(session_id=100, login_generation=5, gate_instance_id="gate-a")
    # 托管落点不带 gate_instance_id：沿用 location 记下的 gate-a
    v_hosted, rec_hosted = decide_placement_identity(cur, True, PlacementIdentity(100, 5, ""))
    assert v_hosted == PlacementVerdict.SAME_SESSION
    assert rec_hosted == PlacementIdentity(100, 5, "gate-a")
    assert identity_is_hosted_login(cur, PlacementIdentity(100, 5, "")) is True

    # 同会话号但不同 gate 实例、同代次 -> CONFLICT；低代次 -> STALE；高代次 -> NEWER
    v_conflict, _ = decide_placement_identity(cur, True, PlacementIdentity(100, 5, "gate-b"))
    assert v_conflict == PlacementVerdict.CONFLICT
    v_stale, _ = decide_placement_identity(cur, True, PlacementIdentity(100, 4, "gate-b"))
    assert v_stale == PlacementVerdict.STALE
    v_newer, rec_newer = decide_placement_identity(cur, True, PlacementIdentity(100, 6, "gate-b"))
    assert v_newer == PlacementVerdict.NEWER
    assert rec_newer == PlacementIdentity(100, 6, "gate-b")

    # r2-oa#1：OwnerAuthority.mode == "enforce" 在准入屏障做实前拒启，shadow 正常放行
    with pytest.raises(ValueError, match="Mode=enforce is refused"):
        OwnerAuthorityConfig(mode="enforce", dsn="user:pass@tcp(127.0.0.1:3306)/owner", backend="mysql").resolve_dsn()
    assert (
        OwnerAuthorityConfig(mode="shadow", dsn="user:pass@tcp(127.0.0.1:3306)/owner", backend="mysql").resolve_dsn()
        == "user:pass@tcp(127.0.0.1:3306)/owner"
    )

    # 端到端 EnterScene：先常规带 gate_instance_id 入场写 location，再 hosted_placement 透传 gate_instance_id
    async def run():
        from test_scene_manager import Harness, SCENE_NODE_TYPE_MAIN_WORLD, SCENE_TYPE_MAIN_WORLD

        async with Harness() as h:
            h.registry.add_scene_node(
                zone_id=1, node_id=10, scene_node_type=SCENE_NODE_TYPE_MAIN_WORLD, node_uuid="scene-uuid-10",
            )
            await h.service.sync_nodes_to_control_store(1)
            h.data.register_player(42, revision=1, home_zone=1)
            created = await h.create_scene(scene_conf_id=1, scene_type=SCENE_TYPE_MAIN_WORLD, zone_id=1)
            assert created.scene_id > 0

            # 1) 常规带 gate_instance_id="gate-uuid-1" 入场
            r1 = await h.enter_scene(
                player_id=42,
                scene_id=created.scene_id,
                zone_id=1,
                gate_id="7",
                gate_instance_id="gate-uuid-1",
                gate_zone_id=1,
                session_id=(7 << 17) | 1,
                login_generation=10,
                owner_revision=1,
            )
            assert r1.error_code == 0
            loc1 = await h.read_location(42)
            assert loc1 is not None and loc1.gate_instance_id == "gate-uuid-1"

            # 2) 托管落点（不带 gate_instance_id）同场景重推 -> PlayerHostedPlacementEvent 携带 "gate-uuid-1"
            r2 = await h.enter_scene(
                player_id=42,
                scene_id=created.scene_id,
                zone_id=1,
                hosted_placement=True,
                session_id=(7 << 17) | 1,
                login_generation=10,
                owner_revision=1,
            )
            assert r2.error_code == 0
            assert len(h.publisher.scene_commands) == 1
            hosted_ev = h.protocol.parse("PlayerHostedPlacementEvent", h.publisher.scene_commands[0][2].payload)
            assert hosted_ev.gate_instance_id == "gate-uuid-1"

    asyncio.run(run())
