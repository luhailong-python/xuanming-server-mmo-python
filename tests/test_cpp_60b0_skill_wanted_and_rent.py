"""C++ 60b0bd6b3 / bc6e70850 / da97bfb11 技能制书扩展与 7 套流派方案租用回归测试（558, 559, 565, 573；560..563 退役验真）。

覆盖范围：
1. 558 ClaimScrollCollectionMilestone：典籍藏经阁小成(>=50%->20 绑残页)/圆满(100%->40 绑残页)位图领奖、去尾0与高位未知bit透传、防重与幂等重放。
2. 559 BatchDismantleSkillBooks：一次拆解 1..20 本未学常规书（禁稀世、禁重复、禁超 20）、返还绑定残页与幂等重放。
3. 560..563 退役验真：Scene 旧悬赏求书接口已退役并墓碑保留，悬赏求书改由 trade（ClientPlayerJubaozhai 582..584）承载。
4. 573 AckCraftProxyDigest + V2/V5 宗师口碑/雅号印款/同队 0 费情义代制/24h 极品热榜 Top 3/离线回账与未读战报清零。
5. 565 RentSkillLoadoutSlots + 7 套战法·加点一体化方案：2 免费 + 85 级满级赠第 3 套 + 轻享卡+2 + 按月真实灵石租用（防溢买、补差向上取整、180 天封顶、TX_SKILL_LOADOUT_RENT=48 审计、到期只锁不删、方案名与敏感词校验、一体化四维加点与洗点费/切换冷却联动）。
6. 真实 TCP Runtime("local") 端到端贯通 558, 559, 565, 573 全部在役新 RPC。
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from test_monopoly import TOKEN, _enter, _settings
from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_afk_pass import SceneAfkPass
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_attributes import SceneAttributes, initialize_attribute_schemes
from xuanming.nodes.scene_mail_outbox import SceneMailOutbox
from xuanming.nodes.scene_skill_craft import (
    CRAFT_MODE_RANDOM_REGULAR,
    CRAFT_MODE_TARGETED_RARE,
    CRAFT_MODE_TARGETED_REGULAR,
    EQUIP_PROXY_MASTER_UNAVAILABLE,
    RARE_SCROLL_INK_ITEM_ID,
    RARE_SCROLL_PAGE_ITEM_ID,
    SCROLL_FRAGMENT_ITEM_ID,
    SEALED_SKILL_BOOK_ITEM_ID,
    SKILL_BOOK_NOT_DISMANTLABLE,
    SKILL_BOOK_NOT_FOUND,
    SKILL_CRAFT_REVISION_CHANGED,
    SKILL_GOLD_NOT_ENOUGH,
    SKILL_SCROLL_INVALID,
    SceneSkillCraft,
    get_proxy_stall_directory,
    has_bit,
    set_bit,
)
from xuanming.nodes.scene_skills import SceneSkills, initialize_skill_list
from xuanming.nodes.scene_stats import (
    LOADOUT_RENT_PERIOD_SECONDS,
    LoadoutRentError,
    plan_skill_loadout_rent,
    validate_loadout_scheme_name,
)
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime


async def _make_two_player_scene(
    tmp_path,
    *,
    level: int = 60,
    gold: int = 100_000,
    diamond: int = 500,
    bind_gold: int = 0,
    bind_diamond: int = 0,
    now_sec_ref: list[int] | None = None,
    roll_fn=None,
):
    protocol = Protocol()
    tables = ConfigTables(fmt="json")
    store = SqlitePlayerStore(tmp_path / "players.sqlite")
    rows = PlayerRowCodec(protocol)
    node = protocol.message("NodeInfo", node_id=7, node_uuid="cpp-60b0-scene", zone_id=1)
    scene = SceneNode(protocol, node, store, rows, lambda *_: None)
    scene.tables = tables
    now_ref = now_sec_ref if now_sec_ref is not None else [1_800_000_000]
    audit_entries = []
    tx_counter = [1000]
    item_counter = [90000]

    def alloc_tx():
        tx_counter[0] += 1
        return tx_counter[0]

    async def alloc_item():
        item_counter[0] += 1
        return item_counter[0]

    assets = scene.assets = SceneAssets(
        scene,
        tables,
        {},
        emit_audit=audit_entries.append,
        allocate_tx_id=alloc_tx,
        allocate_item_id=alloc_item,
    )
    assets.register_client_handlers()

    afk_pass = scene.afk_pass = SceneAfkPass(
        scene, tables, assets, now_s=lambda: now_ref[0], allow_gm=True,
    )
    afk_pass.register()

    attributes = scene.attributes = SceneAttributes(
        scene, tables, assets, allow_gm=True, now_s=lambda: now_ref[0],
    )
    attributes.register()

    mail_id_seq = [5000]

    async def alloc_mail_id():
        mail_id_seq[0] += 1
        return mail_id_seq[0]

    class _FakeMailSender:
        def secret(self):
            return b"0" * 32

        async def __call__(self, _pid, _entry):
            return True

        async def close(self):
            return None

    scene.mail_outbox = SceneMailOutbox(scene, alloc_mail_id, _FakeMailSender())

    skills = SceneSkills(scene, tables, now_s=lambda: now_ref[0])
    skills.register()

    skill_craft = scene.skill_craft = SceneSkillCraft(
        scene, tables, assets, clock=lambda: now_ref[0] * 1000, roll_fn=roll_fn,
    )
    skill_craft.register()

    scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))

    async def seed_player(pid: int, name: str, *, lvl: int = level, g: int = gold, d: int = diamond, bg: int = bind_gold, bd: int = bind_diamond):
        seed = await store.load_and_claim(pid, f"seed-{pid}".encode())
        data = protocol.message("player_database", player_id=pid)
        data.profile_component.name = name
        setattr(data.uint32_pb_component, "class", 1)
        data.level_component.level = lvl
        data.uint64_pb_component.afk_pass_expires_at = 0
        data.derived_attributes_component.health = 100
        data.derived_attributes_component.mana = 100
        data.currency.values.extend([g, d, bd, bg])
        data.bag_component.SetInParent()
        data.craft_component.SetInParent()
        initialize_attribute_schemes(data)
        initialize_skill_list(tables, data)
        await store.commit(seed, rows.project(data), release=True)
        enter_req = protocol.message(
            "PlayerEnterGameNodeRequest",
            player_id=pid,
            session_id=(pid << 17) | 7,
            scene_id=1,
            enter_gs_type=1,
            owner_revision=1,
        )
        await scene.enter(enter_req, "gate")
        return scene.locations[pid].players[pid]

    p1 = await seed_player(1, "甲侠士")
    p2 = await seed_player(2, "乙宗师")

    call_seq = [0]

    async def rpc(pid: int, service: str, method: str, **fields):
        call_seq[0] += 1
        msg_id = protocol.message_ids[f"{service}{method}"]
        route = protocol.routes[msg_id]
        body = protocol.message(route.request_type, **fields).SerializeToString()
        req = protocol.message(
            "ProcessClientPlayerMessageRequest",
            session_id=(pid << 17) | 7,
            player_id=pid,
            message_content=protocol.content(msg_id, call_seq[0], body=body),
        )
        reply = (await scene.process(req, "gate")).message_content
        assert not reply.error_message.id and reply.id == call_seq[0]
        return protocol.parse(route.response_type, reply.serialized_message)

    return SimpleNamespace(
        protocol=protocol,
        tables=tables,
        store=store,
        scene=scene,
        assets=assets,
        afk_pass=afk_pass,
        attributes=attributes,
        skills=skills,
        skill_craft=skill_craft,
        p1=p1,
        p2=p2,
        now_ref=now_ref,
        audit_entries=audit_entries,
        rpc=rpc,
        seed_player=seed_player,
    )


def _add_item(env, data, *, item_id: int, config_id: int, size: int, bound: bool = False):
    occupied = {e.pos for e in data.bag_component.items if e.bag_type == 0}
    slot = next(s for s in range(100) if s not in occupied)
    item = env.protocol.message(
        "ItemComp", item_id=item_id, config_id=config_id, size=size, bound=bound, acquire_seq=item_id,
    )
    env.assets.write_entry(data.bag_component.items.add(pos=slot, bag_type=0), item)


def test_new_skill_and_craft_message_ids_registered():
    p = Protocol()
    assert p.message_ids["SceneSkillClientPlayerClaimScrollCollectionMilestone"] == 558
    assert p.message_ids["SceneSkillClientPlayerBatchDismantleSkillBooks"] == 559
    for retired_id in (560, 561, 562, 563):
        assert retired_id not in p.routes
    for retired_name in (
        "SceneSkillClientPlayerPostSkillBookWantedOrder",
        "SceneSkillClientPlayerCancelSkillBookWantedOrder",
        "SceneSkillClientPlayerListSkillBookWantedOrders",
        "SceneSkillClientPlayerFulfillSkillBookWantedOrder",
    ):
        assert retired_name not in p.message_ids
    assert p.message_ids["ClientPlayerJubaozhaiCreateWantedSkillBookOrder"] == 582
    assert p.message_ids["ClientPlayerJubaozhaiFulfillWantedSkillBookOrder"] == 583
    assert p.message_ids["ClientPlayerJubaozhaiListWantedSkillBookOrders"] == 584
    assert p.message_ids["SceneSkillClientPlayerRentSkillLoadoutSlots"] == 565
    assert p.message_ids["SceneSkillClientPlayerAckCraftProxyDigest"] == 573


def test_558_claim_scroll_milestone_and_559_batch_dismantle(tmp_path):
    async def run():
        env = await _make_two_player_scene(tmp_path, gold=50_000)
        try:
            state0 = await env.rpc(1, "SceneSkillClientPlayer", "GetSkillCraftingState")
            assert not state0.error_message.id
            scroll = next(s for s in state0.state.scrolls if s.rarity_tier == 0)
            pool = list(scroll.skill_table_ids)
            assert len(pool) >= 2
            half_need = (len(pool) + 1) // 2

            # 未达标领小成被拒
            too_early = await env.rpc(
                1, "SceneSkillClientPlayer", "ClaimScrollCollectionMilestone",
                scroll_id=scroll.scroll_id, milestone_tier=1,
                expected_revision=state0.state.scroll_craft_revision, client_request_id=5801,
            )
            assert too_early.error_message.id == 1005

            # 写入未知高位 bit 200 验证透传不丢失，并解锁 >=50% 技能
            data = env.scene.rows.assemble(env.p1.snapshot)
            data.craft_component.claimed_scroll_milestone_bits = set_bit(b"", 200)
            data.craft_component.book_unlocked_skill_ids.extend(pool[:half_need])
            await env.scene.commit_player_data(env.p1, data)

            # 版本号错误返回 7018
            bad_rev = await env.rpc(
                1, "SceneSkillClientPlayer", "ClaimScrollCollectionMilestone",
                scroll_id=scroll.scroll_id, milestone_tier=1,
                expected_revision=999, client_request_id=5802,
            )
            assert bad_rev.error_message.id == SKILL_CRAFT_REVISION_CHANGED

            # 领取小成（20 绑定 3406 残页）
            half_resp = await env.rpc(
                1, "SceneSkillClientPlayer", "ClaimScrollCollectionMilestone",
                scroll_id=scroll.scroll_id, milestone_tier=1,
                expected_revision=0, client_request_id=5803,
            )
            assert not half_resp.error_message.id and not half_resp.replayed
            assert half_resp.rewarded_fragments == 20
            assert half_resp.state.fragment_owned == 20
            bits1 = bytes(half_resp.state.claimed_scroll_milestone_bits)
            assert has_bit(bits1, (scroll.scroll_id - 1) * 2 + 0)
            assert has_bit(bits1, 200)  # 高位未知 bit 透传保留

            # 幂等重放返回原结果不重复发残页
            half_replay = await env.rpc(
                1, "SceneSkillClientPlayer", "ClaimScrollCollectionMilestone",
                scroll_id=scroll.scroll_id, milestone_tier=1,
                expected_revision=0, client_request_id=5803,
            )
            assert not half_replay.error_message.id and half_replay.replayed
            assert half_replay.rewarded_fragments == 20
            assert half_replay.state.fragment_owned == 20

            # 新请求重复领已领档位被拒
            dup_claim = await env.rpc(
                1, "SceneSkillClientPlayer", "ClaimScrollCollectionMilestone",
                scroll_id=scroll.scroll_id, milestone_tier=1,
                expected_revision=half_resp.state.scroll_craft_revision, client_request_id=5804,
            )
            assert dup_claim.error_message.id == 1005

            # 解锁 100% 后领圆满（40 绑定残页）
            data = env.scene.rows.assemble(env.p1.snapshot)
            for sid in pool[half_need:]:
                data.craft_component.book_unlocked_skill_ids.append(sid)
            await env.scene.commit_player_data(env.p1, data)
            full_resp = await env.rpc(
                1, "SceneSkillClientPlayer", "ClaimScrollCollectionMilestone",
                scroll_id=scroll.scroll_id, milestone_tier=2,
                expected_revision=half_resp.state.scroll_craft_revision, client_request_id=5805,
            )
            assert not full_resp.error_message.id and full_resp.rewarded_fragments == 40
            assert full_resp.state.fragment_owned == 60
            bits2 = bytes(full_resp.state.claimed_scroll_milestone_bits)
            assert has_bit(bits2, (scroll.scroll_id - 1) * 2 + 0)
            assert has_bit(bits2, (scroll.scroll_id - 1) * 2 + 1)

            # ── 559 BatchDismantleSkillBooks ──
            data = env.scene.rows.assemble(env.p1.snapshot)
            rare_scroll = next(s for s in state0.state.scrolls if s.rarity_tier == 1)
            data.craft_component.skill_books.add(
                book_id=101, skill_table_id=pool[0], scroll_id=scroll.scroll_id, rarity_tier=0, bound=False,
            )
            data.craft_component.skill_books.add(
                book_id=102, skill_table_id=pool[1], scroll_id=scroll.scroll_id, rarity_tier=0, bound=True,
            )
            data.craft_component.skill_books.add(
                book_id=103, skill_table_id=rare_scroll.skill_table_ids[0],
                scroll_id=rare_scroll.scroll_id, rarity_tier=1, bound=True,
            )
            data.craft_component.next_book_seq = 104
            await env.scene.commit_player_data(env.p1, data)
            rev = full_resp.state.scroll_craft_revision

            # 含稀世书拒回 7016；含重复 book_id 拒回 1005；含不存在 book_id 拒回 7015
            bad_rare = await env.rpc(
                1, "SceneSkillClientPlayer", "BatchDismantleSkillBooks",
                book_ids=[101, 103], expected_revision=rev, client_request_id=5901,
            )
            assert bad_rare.error_message.id == SKILL_BOOK_NOT_DISMANTLABLE

            bad_dup = await env.rpc(
                1, "SceneSkillClientPlayer", "BatchDismantleSkillBooks",
                book_ids=[101, 101], expected_revision=rev, client_request_id=5902,
            )
            assert bad_dup.error_message.id == 1005

            bad_missing = await env.rpc(
                1, "SceneSkillClientPlayer", "BatchDismantleSkillBooks",
                book_ids=[999], expected_revision=rev, client_request_id=5903,
            )
            assert bad_missing.error_message.id == SKILL_BOOK_NOT_FOUND

            # 合法批量拆解 2 本常规书（返还 2 * dismantle_fragment_count 张绑定残页）
            dismantle = await env.rpc(
                1, "SceneSkillClientPlayer", "BatchDismantleSkillBooks",
                book_ids=[101, 102], expected_revision=rev, client_request_id=5904,
            )
            expected_refund = scroll.dismantle_fragment_count * 2
            assert not dismantle.error_message.id and not dismantle.replayed
            assert dismantle.dismantled_count == 2
            assert dismantle.refunded_fragments == expected_refund
            assert dismantle.state.fragment_owned == 60 + expected_refund
            assert [b.book_id for b in dismantle.state.skill_books] == [103]

            # 幂等重放
            dismantle_rep = await env.rpc(
                1, "SceneSkillClientPlayer", "BatchDismantleSkillBooks",
                book_ids=[101, 102], expected_revision=rev, client_request_id=5904,
            )
            assert not dismantle_rep.error_message.id and dismantle_rep.replayed
            assert dismantle_rep.dismantled_count == 2
            assert dismantle_rep.refunded_fragments == expected_refund
        finally:
            await env.skills.close()
            await env.scene.close()
            await env.assets.close()

    asyncio.run(run())


def test_560_to_563_retired_and_craft_state_clean(tmp_path):
    """验证 560..563 旧 Scene 悬赏求书接口彻底退役，保留字段号不复用，GetSkillCraftingState 与封卷/开卷正常运作。"""
    async def run():
        env = await _make_two_player_scene(tmp_path, gold=5_000, bind_gold=50_000)
        try:
            for retired_method in (
                "post_wanted_order",
                "cancel_wanted_order",
                "list_wanted_orders",
                "fulfill_wanted_order",
            ):
                assert not hasattr(env.skill_craft, retired_method)

            st = await env.rpc(1, "SceneSkillClientPlayer", "GetSkillCraftingState")
            assert not st.error_message.id
            assert "my_wanted_orders" not in st.state.DESCRIPTOR.fields_by_name
            d1 = env.scene.rows.assemble(env.p1.snapshot)
            assert "wanted_orders" not in d1.craft_component.DESCRIPTOR.fields_by_name
            assert "next_wanted_order_seq" not in d1.craft_component.DESCRIPTOR.fields_by_name
            rec_desc = env.protocol.message("ClientRequestRecord").DESCRIPTOR
            assert "wanted_order_id" not in rec_desc.fields_by_name
            assert "wanted_order_gold" not in rec_desc.fields_by_name

            # 封卷入包（3414）链路仍完整可用，供 trade 582..584 求购单揭榜流通。
            reg_scroll = next(s for s in st.state.scrolls if s.rarity_tier == 0)
            reg_skill_a = reg_scroll.skill_table_ids[0]
            d2 = env.scene.rows.assemble(env.p2.snapshot)
            d2.craft_component.skill_books.add(
                book_id=203, skill_table_id=reg_skill_a, scroll_id=reg_scroll.scroll_id, rarity_tier=0, bound=False,
            )
            d2.craft_component.next_book_seq = 204
            await env.scene.commit_player_data(env.p2, d2)

            sealed = await env.rpc(
                2, "SceneSkillClientPlayer", "SealSkillBook",
                book_id=203, expected_revision=0, client_request_id=6303,
            )
            assert not sealed.error_message.id and sealed.item_id > 0 and sealed.skill_table_id == reg_skill_a
        finally:
            await env.skills.close()
            await env.scene.close()
            await env.assets.close()

    asyncio.run(run())


def test_573_proxy_stall_v2_v5_team_free_reputation_hot_rank_and_ack_digest(tmp_path):
    async def run():
        env = await _make_two_player_scene(tmp_path, gold=50_000, roll_fn=lambda _b: 0)
        try:
            # 宗师 2 升至制卷宗师（exp=60），解锁 4 号雅号「制卷宗师」
            d2 = env.scene.rows.assemble(env.p2.snapshot)
            d2.craft_component.scroll_mastery_exp = 60
            await env.scene.commit_player_data(env.p2, d2)

            # 佩戴未解锁雅号 6 被拒；合法设置招牌 2、雅号 4、同队 0 费开启
            bad_title = await env.rpc(
                2, "SceneSkillClientPlayer", "SetCraftProxyStall",
                open=True, proxy_scroll_fee_gold=80, master_showcase_tag=2, equipped_craft_title_id=6,
            )
            assert bad_title.error_message.id == 1005

            opened = await env.rpc(
                2, "SceneSkillClientPlayer", "SetCraftProxyStall",
                open=True, proxy_scroll_fee_gold=80, master_showcase_tag=2, equipped_craft_title_id=4,
                team_free_proxy=True,
            )
            assert not opened.error_message.id
            assert opened.state.equipped_craft_title_id == 4
            assert opened.state.team_free_proxy is True
            assert 4 in list(opened.state.unlocked_craft_title_ids)

            # 1) 同队玩家查询摊位可见 viewer_gets_free_proxy=True，且代制免手工费
            team_map = {1: 888, 2: 888}
            env.scene.team = SimpleNamespace(
                team_id=lambda pid: team_map.get(pid, 0),
                departed=lambda _p: None,
            )
            stalls_team = await env.rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")
            assert len(stalls_team.stalls) == 1
            assert stalls_team.stalls[0].viewer_gets_free_proxy is True
            assert stalls_team.stalls[0].equipped_craft_title_id == 4

            # 给买家 1 材料制书
            d1 = env.scene.rows.assemble(env.p1.snapshot)
            _add_item(env, d1, item_id=7001, config_id=SCROLL_FRAGMENT_ITEM_ID, size=100, bound=False)
            await env.scene.commit_player_data(env.p1, d1)
            s1 = await env.rpc(1, "SceneSkillClientPlayer", "GetSkillCraftingState")
            reg_scroll = next(s for s in s1.state.scrolls if s.rarity_tier == 0)

            gold_before = s1.state.gold_balance
            crafted_free = await env.rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                scroll_id=reg_scroll.scroll_id,
                mode=CRAFT_MODE_RANDOM_REGULAR,
                proxy_master_player_id=2,
                expected_revision=s1.state.scroll_craft_revision,
                client_request_id=7301,
            )
            assert not crafted_free.error_message.id
            # 同队免 80 手工费，仅扣基底 random_gold_cost
            assert crafted_free.state.gold_balance == gold_before - reg_scroll.random_gold_cost
            assert crafted_free.crafted_book.crafter_name == "制卷宗师·乙宗师"

            # 2) 拆队后再代制：收取 80 手工费（95% 净银两 76 寄出并记入未读营业战报 + 24h 热榜排名 1）
            team_map.clear()
            stalls_solo = await env.rpc(1, "SceneSkillClientPlayer", "ListCraftProxyStalls")
            assert stalls_solo.stalls[0].viewer_gets_free_proxy is False
            assert stalls_solo.stalls[0].hot_rank == 1
            assert stalls_solo.stalls[0].recent_24h_orders == 1

            crafted_paid = await env.rpc(
                1, "SceneSkillClientPlayer", "CraftSkillBook",
                scroll_id=reg_scroll.scroll_id,
                mode=CRAFT_MODE_RANDOM_REGULAR,
                proxy_master_player_id=2,
                expected_revision=crafted_free.state.scroll_craft_revision,
                client_request_id=7302,
            )
            assert not crafted_paid.error_message.id
            assert crafted_paid.state.gold_balance == crafted_free.state.gold_balance - reg_scroll.random_gold_cost - 80

            # 宗师 2 查询状态应看到累计 2 单、未读营业战报含 76 净银两与下单人「甲侠士」
            m_state = await env.rpc(2, "SceneSkillClientPlayer", "GetSkillCraftingState")
            assert m_state.state.proxy_orders_completed == 2
            assert m_state.state.HasField("unread_proxy_digest")
            assert m_state.state.unread_proxy_digest.orders_completed == 2
            assert m_state.state.unread_proxy_digest.net_gold_earned == 76
            assert m_state.state.unread_proxy_digest.last_buyer_name == "甲侠士"

            # 模拟离线/跨线待回账口碑后调用 573 AckCraftProxyDigest 幂等回账并清零未读战报
            await get_proxy_stall_directory(env.scene).record_order(
                2, supreme_delta=1, rare_delta=0, net_gold_earned=95, buyer_name="丙侠士",
            )
            acked = await env.rpc(2, "SceneSkillClientPlayer", "AckCraftProxyDigest")
            assert not acked.error_message.id
            assert acked.state.proxy_orders_completed == 3
            assert acked.state.supreme_forged_count == 1
            assert not acked.state.HasField("unread_proxy_digest")

            # 二次调用 AckCraftProxyDigest 幂等安全
            acked2 = await env.rpc(2, "SceneSkillClientPlayer", "AckCraftProxyDigest")
            assert not acked2.error_message.id
            assert not acked2.state.HasField("unread_proxy_digest")
        finally:
            await env.skills.close()
            await env.scene.close()
            await env.assets.close()

    asyncio.run(run())


def test_565_rent_skill_loadout_slots_and_unified_attribute_schemes(tmp_path):
    async def run():
        # 纯规则边界：名称校验 & 补差价向上取整
        assert validate_loadout_scheme_name("剑宗爆发")
        assert not validate_loadout_scheme_name("")
        assert not validate_loadout_scheme_name("   \u3000 ")
        assert not validate_loadout_scheme_name("<script>")
        assert not validate_loadout_scheme_name("官方客服")
        assert not validate_loadout_scheme_name("GM测试")
        assert not validate_loadout_scheme_name("一二三四五六七八九十甲乙丙")

        now_sec = 1_800_000_000
        # 剩余半个周期（15 天）加租 1 套 -> 15 灵石；剩 1 秒加租 1 套 -> 向上取整至少 1 灵石防拆单套利
        half_plan = plan_skill_loadout_rent(
            False, 1, now_sec + LOADOUT_RENT_PERIOD_SECONDS // 2, now_sec, target_rented_slots=2, add_periods=0,
        )
        assert half_plan.error == LoadoutRentError.NONE and half_plan.total_cost_diamond == 15
        one_sec_plan = plan_skill_loadout_rent(
            False, 1, now_sec + 1, now_sec, target_rented_slots=2, add_periods=0,
        )
        assert one_sec_plan.error == LoadoutRentError.NONE and one_sec_plan.total_cost_diamond == 1

        now_ref = [now_sec]
        # 严格校验只扣真实灵石：给 200 真实灵石 + 1000 绑定灵石，避免混扣
        env = await _make_two_player_scene(
            tmp_path, level=84, gold=50_000, diamond=200, bind_diamond=1000, now_sec_ref=now_ref,
        )
        try:
            d1 = env.scene.rows.assemble(env.p1.snapshot)
            d1.skill_list.skill_list.add(skill_table_id=12001, rank=1)
            await env.scene.commit_player_data(env.p1, d1)

            # 1) 84 级无轻享卡：免费 2 套，max_schemes=7，方案 3 被拒
            book0 = await env.rpc(1, "SceneSkillClientPlayer", "ListSkills")
            assert book0.loadout.free_schemes == 2
            assert book0.loadout.light_card_bonus_schemes == 0
            assert book0.loadout.unlocked_schemes == 2
            assert book0.loadout.max_schemes == 7
            assert book0.loadout.diamond_balance == 200
            assert book0.loadout.schemes[0].name == "方案一"

            # 2) 非法方案名被拒 7029
            bad_name = await env.rpc(
                1, "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=1, role_tag=4, name="官方专用",
                passive_skill_ids=[12001], state_version=1, request_id="rent-bad-name-0001",
            )
            assert bad_name.error_message.id == 7029

            # 3) 565 RentSkillLoadoutSlots 错误边界：报价不符(7030)、超买(7033)、叠超 180 天(7032)、灵石不足不混扣绑灵石(7031)
            assert (await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6501, target_rented_slots=2, add_periods=1, expected_cost_diamond=59,
            )).error_message.id == 7030
            assert (await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6502, target_rented_slots=6, add_periods=1, expected_cost_diamond=180,
            )).error_message.id == 7033
            assert (await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6503, target_rented_slots=1, add_periods=7, expected_cost_diamond=210,
            )).error_message.id == 7032
            assert (await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6504, target_rented_slots=5, add_periods=2, expected_cost_diamond=300,
            )).error_message.id == 7031  # 200 真实灵石 < 300，绝不混扣 1000 绑定灵石

            # 4) 正常首租 2 套 × 1 期（60 真实灵石）-> 解锁至 4 套，写 TX_SKILL_LOADOUT_RENT=48 审计日志
            rent1 = await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6510, target_rented_slots=2, add_periods=1, expected_cost_diamond=60,
            )
            assert not rent1.error_message.id and not rent1.replayed
            assert rent1.rented_extra_slots == 2
            assert rent1.spent_diamond == 60
            assert rent1.rent_expires_at_sec == now_sec + LOADOUT_RENT_PERIOD_SECONDS
            assert rent1.state.loadout.unlocked_schemes == 4
            assert rent1.state.loadout.diamond_balance == 140
            await asyncio.sleep(0)
            rent_audits = [e for e in env.audit_entries if e.tx_type == 48]
            assert len(rent_audits) == 1
            assert rent_audits[0].correlation_id == 6510
            assert rent_audits[0].currency_type == 1 and rent_audits[0].currency_delta == -60

            # 幂等重放（即使状态已变仍返回首次回执，不二次扣灵石）
            rent1_rep = await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6510, target_rented_slots=2, add_periods=1, expected_cost_diamond=60,
            )
            assert not rent1_rep.error_message.id and rent1_rep.replayed
            assert rent1_rep.spent_diamond == 60 and rent1_rep.state.loadout.diamond_balance == 140

            # 5) 租期内 15 天后加租至 3 套（add_periods=0，补差价 15 真实灵石，到期日不变）
            now_ref[0] = now_sec + LOADOUT_RENT_PERIOD_SECONDS // 2
            expand = await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6511, target_rented_slots=3, add_periods=0, expected_cost_diamond=15,
            )
            assert not expand.error_message.id
            assert expand.rented_extra_slots == 3 and expand.spent_diamond == 15
            assert expand.rent_expires_at_sec == now_sec + LOADOUT_RENT_PERIOD_SECONDS
            assert expand.state.loadout.unlocked_schemes == 5
            assert expand.state.loadout.diamond_balance == 125

            # 6) 保存方案 4（含自定义名、role_tag=4 生存、一体化四维加点）
            ver = expand.state.loadout.state_version
            save4 = await env.rpc(
                1, "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=4, role_tag=4, name="龟速生存流",
                passive_skill_ids=[12001],
                state_version=ver, request_id="loadout-save4-attr-001",
                update_attribute_allocation=True,
                attribute_allocated={101: 30, 102: 20},
            )
            assert not save4.error_message.id
            s4_info = next(s for s in save4.state.loadout.schemes if s.scheme_id == 4)
            assert s4_info.name == "龟速生存流" and s4_info.role_tag == 4
            assert s4_info.attribute_allocated[101] == 30 and s4_info.attribute_allocated[102] == 20
            assert save4.state.loadout.active_attribute_scheme_id == 4

            # 降点但不允许洗点返回 7035；允许洗点后自动扣银两重配成功
            dec_deny = await env.rpc(
                1, "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=4, role_tag=4, passive_skill_ids=[12001],
                state_version=save4.state.loadout.state_version, request_id="loadout-dec-deny-0001",
                update_attribute_allocation=True,
                attribute_allocated={101: 10, 102: 20},
                reset_attribute_if_needed=False,
            )
            assert dec_deny.error_message.id == 7035

            dec_ok = await env.rpc(
                1, "SceneSkillClientPlayer", "SaveSkillLoadout",
                scheme_id=4, role_tag=4, passive_skill_ids=[12001],
                state_version=save4.state.loadout.state_version, request_id="loadout-dec-okay-0002",
                update_attribute_allocation=True,
                attribute_allocated={101: 10, 102: 20},
                reset_attribute_if_needed=True,
            )
            assert not dec_ok.error_message.id
            s4_after = next(s for s in dec_ok.state.loadout.schemes if s.scheme_id == 4)
            assert s4_after.attribute_allocated[101] == 10

            # 刚切到属性方案 4 后立即切回方案 1：战法切换成功，属性处于冷却返回提示且保留方案 4
            sw1 = await env.rpc(
                1, "SceneSkillClientPlayer", "SwitchSkillLoadout",
                scheme_id=1, state_version=dec_ok.state.loadout.state_version, request_id="loadout-sw1-cool-0001",
            )
            assert not sw1.error_message.id
            assert sw1.state.loadout.active_scheme_id == 1
            assert sw1.attribute_sync_notice
            assert sw1.state.loadout.active_attribute_scheme_id == 4

            # 7) 到期只锁不删：时间推进过期后 unlocked_schemes 回落 2，切方案 4 被拒 7026，但方案 4 数据仍在快照中保留
            now_ref[0] = now_sec + LOADOUT_RENT_PERIOD_SECONDS + 10
            expired_book = await env.rpc(1, "SceneSkillClientPlayer", "ListSkills")
            assert expired_book.loadout.rented_extra_slots == 0
            assert expired_book.loadout.rent_expires_at_sec == 0
            assert expired_book.loadout.unlocked_schemes == 2
            assert any(s.scheme_id == 4 and s.name == "龟速生存流" for s in expired_book.loadout.schemes)
            locked_sw = await env.rpc(
                1, "SceneSkillClientPlayer", "SwitchSkillLoadout",
                scheme_id=4, state_version=expired_book.loadout.state_version, request_id="loadout-locked-s4-001",
            )
            assert locked_sw.error_message.id == 7026

            # 8) 升至 85 级满级自动永久再赠第 3 套免费槽（free_schemes=3），且防溢买上限收紧（无轻享卡最多租 4 套）
            d1 = env.scene.rows.assemble(env.p1.snapshot)
            d1.level_component.level = 85
            await env.scene.commit_player_data(env.p1, d1)
            lv85_book = await env.rpc(1, "SceneSkillClientPlayer", "ListSkills")
            assert lv85_book.loadout.free_schemes == 3
            assert lv85_book.loadout.unlocked_schemes == 3
            overbuy_85 = await env.rpc(
                1, "SceneSkillClientPlayer", "RentSkillLoadoutSlots",
                client_request_id=6520, target_rented_slots=5, add_periods=1, expected_cost_diamond=150,
            )
            assert overbuy_85.error_message.id == 7033
        finally:
            await env.skills.close()
            await env.scene.close()
            await env.assets.close()

    asyncio.run(run())


async def _tcp_call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    msg_id = p.message_ids[message_name]
    req = p.message(p.routes[msg_id].request_type, **fields)
    return await client.rpc(msg_id, call_id, req)


def test_real_tcp_local_runtime_all_active_new_skill_rpcs(tmp_path):
    """通过真实 TCP Runtime('local') 端到端验证 558, 559, 565, 573 全部在役新 RPC 路由与持久化（560..563 已退役）。"""
    async def scenario():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="craft-buyer") as (client_a, pid_a):
                scene = runtime.scene
                pa = scene.locations[pid_a].players[pid_a]

                # 预置银两、灵石、已解锁卷宗技能与待拆书籍
                st0 = await _tcp_call(client_a, 1, "SceneSkillClientPlayerGetSkillCraftingState")
                assert not st0.error_message.id
                reg_scroll = next(s for s in st0.state.scrolls if s.rarity_tier == 0)
                pool = list(reg_scroll.skill_table_ids)
                half_need = (len(pool) + 1) // 2

                da = scene.rows.assemble(pa.snapshot)
                while len(da.currency.values) < 4:
                    da.currency.values.append(0)
                da.currency.values[0] = 20_000
                da.currency.values[1] = 200
                da.craft_component.book_unlocked_skill_ids.extend(pool[:half_need])
                da.craft_component.skill_books.add(
                    book_id=10, skill_table_id=pool[0], scroll_id=reg_scroll.scroll_id, rarity_tier=0, bound=False,
                )
                da.craft_component.next_book_seq = 11
                da.craft_component.unread_proxy_digest_orders = 1
                da.craft_component.unread_proxy_digest_net_gold = 76
                da.craft_component.unread_proxy_digest_last_buyer = "路人甲"
                await scene.commit_player_data(pa, da)

                # 558 ClaimScrollCollectionMilestone
                r558 = await _tcp_call(
                    client_a, 2, "SceneSkillClientPlayerClaimScrollCollectionMilestone",
                    scroll_id=reg_scroll.scroll_id, milestone_tier=1, expected_revision=0, client_request_id=8558,
                )
                assert not r558.error_message.id and r558.rewarded_fragments == 20

                # 559 BatchDismantleSkillBooks
                r559 = await _tcp_call(
                    client_a, 3, "SceneSkillClientPlayerBatchDismantleSkillBooks",
                    book_ids=[10], expected_revision=r558.state.scroll_craft_revision, client_request_id=8559,
                )
                assert not r559.error_message.id and r559.dismantled_count == 1

                # 565 RentSkillLoadoutSlots
                r565 = await _tcp_call(
                    client_a, 4, "SceneSkillClientPlayerRentSkillLoadoutSlots",
                    client_request_id=8565, target_rented_slots=2, add_periods=1, expected_cost_diamond=60,
                )
                assert not r565.error_message.id and r565.rented_extra_slots == 2 and r565.spent_diamond == 60
                assert r565.state.loadout.unlocked_schemes == 4

                # 573 AckCraftProxyDigest
                r573 = await _tcp_call(client_a, 5, "SceneSkillClientPlayerAckCraftProxyDigest")
                assert not r573.error_message.id and not r573.state.HasField("unread_proxy_digest")

    asyncio.run(scenario())
