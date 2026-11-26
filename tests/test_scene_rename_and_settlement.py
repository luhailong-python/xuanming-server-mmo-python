"""Scene 角色改名系统与战斗结算成长挂钩的单元测试与回环集成测试。"""

from __future__ import annotations

import asyncio
import time
from contextlib import AsyncExitStack
from itertools import count
from types import SimpleNamespace

from data_service_fakes import MemoryStore

from xuanming.content_guard import Category, ContentGuardService, Engine, Entry, Lexicon, Mode
from xuanming.data import ConfigTables, PlayerRowCodec, SqlitePlayerStore
from xuanming.data.asset_inventory import held_item_count
from xuanming.data_service.service import DataService
from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.login.directory import DataService as DirectoryClient
from xuanming.nodes.battle_settlement import BattleSettlement
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_equip_craft import MATERIAL_REWARD_BATTLE_UNITS, SceneEquipCraft
from xuanming.nodes.scene_mission import Missions
from xuanming.nodes.scene_rename import (
    DEFAULT_RENAME_COOLDOWN_SECONDS,
    DEFAULT_RENAME_ITEM_ID,
    ITEM_HOLD_KIND_RENAME,
    ROLE_NAME_INVALID,
    ROLE_NAME_SENSITIVE,
    ROLE_NAME_TAKEN,
    ROLE_RENAME_COOLDOWN,
    ROLE_RENAME_IN_PROGRESS,
    ROLE_RENAME_NO_CARD,
    ROLE_RENAME_SAME_NAME,
    SceneRename,
)
from xuanming.nodes.scene_season import SceneSeason
from xuanming.nodes.scene_trial_journal import SceneTrialJournal, has_packed_bit
from xuanming.protocol import Protocol

_SECRETS = {
    "login": b"login-directory-secret-0123456789",
    "scene": b"scene-directory-secret-0123456789",
}


def _grant_bag_item(protocol: Protocol, data, *, item_id: int, config_id: int, size: int, pos: int = 0) -> None:
    """向角色主背包写入可堆叠道具（兼容 Item 表尚未同步改名卡 3401 的场景）。"""
    item = protocol.message("ItemComp", item_id=item_id, config_id=config_id, size=size, acquire_seq=item_id)
    entry = data.bag_component.items.add(pos=pos, bag_type=0)
    entry.instance.CopyFrom(item)
    entry.item_uuid = item.item_id
    entry.config_id = item.config_id
    entry.stack_size = item.size
    entry.acquire_seq = item.acquire_seq


def _count_bag_item(data, config_id: int) -> int:
    total = 0
    for entry in data.bag_component.items:
        if entry.bag_type != 0:
            continue
        if entry.HasField("instance"):
            if entry.instance.config_id == config_id:
                total += entry.instance.size
        elif entry.config_id == config_id:
            total += entry.stack_size
    return total


def _decode_rename_notifies(protocol: Protocol, pushes: list[tuple[int, int, bytes]], player_id: int) -> list:
    notify_id = protocol.message_ids["SceneClientPlayerCommonNotifyPlayerRenamed"]
    return [
        protocol.parse("PlayerRenamedNotify", body)
        for pid, msg_id, body in pushes
        if pid == player_id and msg_id == notify_id
    ]


async def _make_scene_env(
    tmp_path,
    *,
    player_id: int = 1001,
    initial_name: str = "初名",
    rename_cards: int = 0,
    now_ref: list[int] | None = None,
    store: SqlitePlayerStore | None = None,
    seed_player: bool = True,
    emit_audit=None,
    allocate_tx_id=None,
):
    protocol = Protocol()
    tables = ConfigTables()
    codec = PlayerRowCodec(protocol)
    db_store = store if store is not None else SqlitePlayerStore(tmp_path / "players.sqlite3")

    if seed_player:
        seed = await db_store.load_and_claim(player_id, b"seed")
        data = protocol.message("player_database", player_id=player_id)
        data.level_component.level = 10
        data.currency.values.extend([1000, 100, 0])
        data.profile_component.name = initial_name
        data.pet_component.SetInParent()
        data.season_component.SetInParent()
        data.trial_journal_component.SetInParent()
        if rename_cards > 0:
            _grant_bag_item(protocol, data, item_id=90_001, config_id=DEFAULT_RENAME_ITEM_ID, size=rename_cards)
        await db_store.commit(seed, codec.project(data), release=True)

    pushes: list[tuple[int, int, bytes]] = []

    def emit(player, content) -> None:
        pushes.append((player.player_id, content.message_id, bytes(content.serialized_message)))

    info = protocol.message("NodeInfo", node_id=7, node_type=2, zone_id=1, node_uuid=f"scene-{id(pushes)}")
    scene = SceneNode(protocol, info, db_store, codec, emit)
    scene.tables = tables
    guids = count(50_000)

    async def alloc_id() -> int:
        return next(guids)

    assets = SceneAssets(
        scene,
        tables,
        {},
        allow_gm=True,
        allocate_item_id=alloc_id,
        emit_audit=emit_audit,
        allocate_tx_id=allocate_tx_id,
    )
    scene.assets = assets
    scene.create(protocol.message("CreateSceneRequest", scene_id=1, config_id=1))

    await scene.enter(
        protocol.message(
            "PlayerEnterGameNodeRequest",
            player_id=player_id,
            session_id=77,
            scene_id=1,
            home_zone_id=1,
            owner_revision=1,
            login_generation=1,
        ),
        "gate-1",
    )
    player = scene.locations[player_id].players[player_id]

    async def rpc(service: str, method: str, **fields):
        msg_id = protocol.message_ids[f"{service}{method}"]
        route = protocol.routes[msg_id]
        body = protocol.message(route.request_type, **fields).SerializeToString()
        req = protocol.message(
            "ProcessClientPlayerMessageRequest",
            session_id=player.session_id,
            player_id=player.player_id,
            message_content=protocol.content(msg_id, 1, body=body),
        )
        res = await scene.process(req, player.gate_uuid)
        assert not res.message_content.error_message.id
        return protocol.parse(route.response_type, res.message_content.serialized_message)

    clock = (lambda: now_ref[0]) if now_ref is not None else (lambda: 1_760_000_000_000)
    return SimpleNamespace(
        protocol=protocol,
        tables=tables,
        store=db_store,
        scene=scene,
        assets=assets,
        player=player,
        pushes=pushes,
        rpc=rpc,
        clock=clock,
    )


async def _wait_rename_tasks(rename: SceneRename) -> None:
    while rename._tasks:
        pending = [task for _, task in rename._tasks.values()]
        await asyncio.gather(*pending)


def test_scene_rename_local_flow_cooldown_card_and_validation(tmp_path):
    """本地模式：参数校验、缺改名卡拒收、成功扣卡与冷却期拦截。"""

    async def exercise():
        now = [1_760_000_000_000]
        env = await _make_scene_env(tmp_path, initial_name="旧名字", rename_cards=0, now_ref=now)
        rename = SceneRename(env.scene, env.tables, env.assets, now_ms=env.clock)
        rename.register()
        try:
            # 1. 空名、过短、过长、同名均同步拒绝。
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="   ")).error_message.id == ROLE_NAME_INVALID
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="单")).error_message.id == ROLE_NAME_INVALID
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="甲" * 13)).error_message.id == ROLE_NAME_INVALID
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name=" 旧名字 ")).error_message.id == ROLE_RENAME_SAME_NAME

            # 2. 缺少改名卡同步拒绝。
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="青云剑修")).error_message.id == ROLE_RENAME_NO_CARD

            # 3. 发放 2 张改名卡后改名成功，扣 1 张卡并推送 NotifyPlayerRenamed。
            data = env.scene.rows.assemble(env.player.snapshot)
            _grant_bag_item(env.protocol, data, item_id=91_001, config_id=DEFAULT_RENAME_ITEM_ID, size=2)
            await env.scene.commit_player_data(env.player, data)

            resp = await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="  青云剑修  ")
            assert resp.error_message.id == 0
            await _wait_rename_tasks(rename)

            data = env.scene.rows.assemble(env.player.snapshot)
            assert data.profile_component.name == "青云剑修"
            assert data.profile_component.renamed_ms == now[0]
            assert data.profile_component.rename_seq == 1
            assert not data.profile_component.HasField("rename_op")
            assert held_item_count(data, DEFAULT_RENAME_ITEM_ID) == 0
            assert _count_bag_item(data, DEFAULT_RENAME_ITEM_ID) == 1

            notifies = _decode_rename_notifies(env.protocol, env.pushes, env.player.player_id)
            assert len(notifies) == 1
            assert notifies[0].error_message.id == 0
            assert notifies[0].name == "青云剑修"
            expected_next = (now[0] + DEFAULT_RENAME_COOLDOWN_SECONDS * 1000) // 1000
            assert notifies[0].next_rename_time == expected_next

            # 4. 冷却期内再次改名被拒，不扣卡。
            now[0] += 10_000
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="再改一次")).error_message.id == ROLE_RENAME_COOLDOWN
            data = env.scene.rows.assemble(env.player.snapshot)
            assert _count_bag_item(data, DEFAULT_RENAME_ITEM_ID) == 1

            # 5. 越过冷却期后第二次改名成功，剩余 1 张卡扣清；再过冷却期后因无卡被拒。
            now[0] += DEFAULT_RENAME_COOLDOWN_SECONDS * 1000
            resp2 = await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="御剑乘风")
            assert resp2.error_message.id == 0
            await _wait_rename_tasks(rename)
            data = env.scene.rows.assemble(env.player.snapshot)
            assert data.profile_component.name == "御剑乘风"
            assert _count_bag_item(data, DEFAULT_RENAME_ITEM_ID) == 0
            now[0] += DEFAULT_RENAME_COOLDOWN_SECONDS * 1000
            assert (await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="无卡再改")).error_message.id == ROLE_RENAME_NO_CARD
        finally:
            await rename.close()
            await env.scene.close()

    asyncio.run(exercise())


def test_scene_rename_loopback_data_service_and_content_guard(tmp_path):
    """回环 gRPC：真实 DataService.RenameRole（含签名鉴权）与 ContentGuard 拦截/放行、并发拦截及卡扣账。"""

    async def exercise():
        now = [time.time_ns() // 1_000_000]
        audits = []
        tx_seq = count(80_000)

        async def emit_audit(entry) -> None:
            audits.append(entry)

        async def alloc_tx() -> int:
            return next(tx_seq)

        async with AsyncExitStack() as stack:
            env = await _make_scene_env(
                tmp_path,
                player_id=1001,
                initial_name="初名",
                rename_cards=2,
                now_ref=now,
                emit_audit=emit_audit,
                allocate_tx_id=alloc_tx,
            )
            stack.push_async_callback(env.scene.close)

            # 启动真实 loopback DataService gRPC 服务并登记两名角色（1001 与 1002）。
            ds_store = MemoryStore(env.protocol, clock_ms=env.clock)
            ds_service = DataService(env.protocol, ds_store, _SECRETS, zone_refresh=60.0, clock_ms=env.clock)
            await ds_service.start()
            stack.push_async_callback(ds_service.close)
            ds_server, ds_port = await serve_grpc(env.protocol, "127.0.0.1:0", ds_service.services(), pass_context=True)
            stack.push_async_callback(ds_server.stop, 1)
            ds_client = GrpcClient(f"127.0.0.1:{ds_port}")
            stack.push_async_callback(ds_client.close)

            directory = DirectoryClient(env.protocol, ds_client.call, _SECRETS["login"])
            for pid, account, role_name in ((1001, "acct-1", "初名"), (1002, "acct-2", "别家名")):
                await directory.register_player_zone(pid, 1, 1_000_000)
                assert await directory.reserve_name(pid, role_name) == (0, 0)
                await directory.create_role(
                    account,
                    env.protocol.message(
                        "AccountSimplePlayer",
                        player_id=pid,
                        class_id=1,
                        gender=1,
                        zone_id=1,
                        name=role_name,
                        appearance_id="01_hero",
                    ),
                )

            # 启动真实 loopback ContentGuard gRPC 服务并装载自定义禁词 "禁词名"。
            engine = Engine()
            engine.swap(
                1,
                Lexicon(deny=(Entry(word="禁词名", category=Category.ABUSE, mode=Mode.SUB),)),
            )
            cg_service = ContentGuardService(env.protocol, engine, require_lexicon=True)
            cg_server, cg_port = await serve_grpc(env.protocol, "127.0.0.1:0", cg_service.services(), pass_context=True)
            stack.push_async_callback(cg_server.stop, 1)
            cg_client = GrpcClient(f"127.0.0.1:{cg_port}")
            stack.push_async_callback(cg_client.close)

            # 可选门闸：用于验证在途并发改名被拒。
            ds_entered = asyncio.Event()
            ds_release = asyncio.Event()
            ds_release.set()

            async def gated_ds_call(path: str, payload: bytes) -> bytes:
                if not ds_release.is_set():
                    ds_entered.set()
                    await ds_release.wait()
                return await ds_client.call(path, payload)

            rename = SceneRename(
                env.scene,
                env.tables,
                env.assets,
                data_service_call=gated_ds_call,
                directory_secret=_SECRETS["scene"],
                content_guard_call=cg_client.call,
                now_ms=env.clock,
            )
            rename.register()
            stack.push_async_callback(rename.close)

            # Case 1: ContentGuard 拦截 -> 推送 ROLE_NAME_SENSITIVE，释放预留，改名卡未消耗！
            r1 = await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="禁词名")
            assert r1.error_message.id == 0
            await _wait_rename_tasks(rename)
            data = env.scene.rows.assemble(env.player.snapshot)
            assert data.profile_component.name == "初名"
            assert not data.profile_component.HasField("rename_op")
            assert held_item_count(data, DEFAULT_RENAME_ITEM_ID) == 0
            assert _count_bag_item(data, DEFAULT_RENAME_ITEM_ID) == 2
            notifies = _decode_rename_notifies(env.protocol, env.pushes, 1001)
            assert notifies[-1].error_message.id == ROLE_NAME_SENSITIVE

            # Case 2: DataService 重名拒收 -> 推送 ROLE_NAME_TAKEN，释放预留，改名卡未消耗！
            r2 = await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="别家名")
            assert r2.error_message.id == 0
            await _wait_rename_tasks(rename)
            data = env.scene.rows.assemble(env.player.snapshot)
            assert data.profile_component.name == "初名"
            assert not data.profile_component.HasField("rename_op")
            assert held_item_count(data, DEFAULT_RENAME_ITEM_ID) == 0
            assert _count_bag_item(data, DEFAULT_RENAME_ITEM_ID) == 2
            notifies = _decode_rename_notifies(env.protocol, env.pushes, 1001)
            assert notifies[-1].error_message.id == ROLE_NAME_TAKEN

            # Case 3: 在途并发改名拒收，随后放行 ContentGuard + DataService 成功提交并扣 1 张卡！
            ds_release.clear()
            r3 = await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="云海归客")
            assert r3.error_message.id == 0
            await asyncio.wait_for(ds_entered.wait(), timeout=2.0)

            # 在途期间：rename_op 与 item_holds 已落盘，并发请求立拒 ROLE_RENAME_IN_PROGRESS。
            staged = env.scene.rows.assemble(env.player.snapshot)
            assert staged.profile_component.HasField("rename_op")
            assert staged.profile_component.rename_op.name == "云海归客"
            assert any(h.kind == ITEM_HOLD_KIND_RENAME for h in staged.item_holds.holds)
            assert held_item_count(staged, DEFAULT_RENAME_ITEM_ID) == 1
            concurrent = await env.rpc("SceneClientPlayerCommon", "RenamePlayer", name="并发名")
            assert concurrent.error_message.id == ROLE_RENAME_IN_PROGRESS

            ds_release.set()
            await _wait_rename_tasks(rename)

            final = env.scene.rows.assemble(env.player.snapshot)
            assert final.profile_component.name == "云海归客"
            assert not final.profile_component.HasField("rename_op")
            assert held_item_count(final, DEFAULT_RENAME_ITEM_ID) == 0
            assert _count_bag_item(final, DEFAULT_RENAME_ITEM_ID) == 1
            assert await directory.player_names([1001]) == {1001: "云海归客"}

            notifies = _decode_rename_notifies(env.protocol, env.pushes, 1001)
            assert notifies[-1].error_message.id == 0
            assert notifies[-1].name == "云海归客"
            assert notifies[-1].next_rename_time > 0

            # 验证审计流水记录了 TX_ROLE_RENAME (33)。
            await env.assets.close()
            assert any(entry.tx_type == 33 and entry.item_config_id == DEFAULT_RENAME_ITEM_ID for entry in audits)

    asyncio.run(exercise())


def test_scene_rename_crash_recovery_resume_pending(tmp_path):
    """崩溃恢复：rename_op 与 item_holds 落盘后进程关闭，重启重载调用 resume_pending 幂等完成改名。"""

    async def exercise():
        now = [time.time_ns() // 1_000_000]
        shared_store = SqlitePlayerStore(tmp_path / "shared_players.sqlite3")

        async with AsyncExitStack() as stack:
            protocol = Protocol()
            ds_store = MemoryStore(protocol, clock_ms=lambda: now[0])
            ds_service = DataService(protocol, ds_store, _SECRETS, zone_refresh=60.0, clock_ms=lambda: now[0])
            await ds_service.start()
            stack.push_async_callback(ds_service.close)
            ds_server, ds_port = await serve_grpc(protocol, "127.0.0.1:0", ds_service.services(), pass_context=True)
            stack.push_async_callback(ds_server.stop, 1)
            ds_client = GrpcClient(f"127.0.0.1:{ds_port}")
            stack.push_async_callback(ds_client.close)

            directory = DirectoryClient(protocol, ds_client.call, _SECRETS["login"])
            await directory.register_player_zone(2001, 1, 1_000_000)
            assert await directory.reserve_name(2001, "崩溃前名") == (0, 0)
            await directory.create_role(
                "acct-crash",
                protocol.message(
                    "AccountSimplePlayer",
                    player_id=2001,
                    class_id=1,
                    gender=1,
                    zone_id=1,
                    name="崩溃前名",
                    appearance_id="01_hero",
                ),
            )

            # 实例 1：改名受理并持久化 rename_op + item_holds，远端调用尚未返回即停机（模拟崩溃）。
            env1 = await _make_scene_env(
                tmp_path,
                player_id=2001,
                initial_name="崩溃前名",
                rename_cards=1,
                now_ref=now,
                store=shared_store,
                seed_player=True,
            )
            entered = asyncio.Event()
            never_returns = asyncio.Event()

            async def stalled_ds(path: str, payload: bytes) -> bytes:
                entered.set()
                await never_returns.wait()
                return b""

            rename1 = SceneRename(
                env1.scene,
                env1.tables,
                env1.assets,
                data_service_call=stalled_ds,
                directory_secret=_SECRETS["scene"],
                now_ms=env1.clock,
            )
            rename1.register()

            accepted = await env1.rpc("SceneClientPlayerCommon", "RenamePlayer", name="重启新名")
            assert accepted.error_message.id == 0
            await asyncio.wait_for(entered.wait(), timeout=2.0)

            await rename1.close()
            await env1.scene.close()

            # 实例 2：全新 SceneNode 从同一持久化存储重新载入角色，调用 resume_pending 接续完成。
            env2 = await _make_scene_env(
                tmp_path,
                player_id=2001,
                now_ref=now,
                store=shared_store,
                seed_player=False,
            )
            stack.push_async_callback(env2.scene.close)

            before_resume = env2.scene.rows.assemble(env2.player.snapshot)
            assert before_resume.profile_component.HasField("rename_op")
            assert before_resume.profile_component.rename_op.name == "重启新名"
            assert before_resume.profile_component.rename_op.seq == 1
            assert held_item_count(before_resume, DEFAULT_RENAME_ITEM_ID) == 1
            assert _count_bag_item(before_resume, DEFAULT_RENAME_ITEM_ID) == 1

            rename2 = SceneRename(
                env2.scene,
                env2.tables,
                env2.assets,
                data_service_call=ds_client.call,
                directory_secret=_SECRETS["scene"],
                now_ms=env2.clock,
            )
            rename2.register()
            stack.push_async_callback(rename2.close)

            task = rename2.resume_pending(env2.player)
            assert task is not None
            await task

            after_resume = env2.scene.rows.assemble(env2.player.snapshot)
            assert after_resume.profile_component.name == "重启新名"
            assert not after_resume.profile_component.HasField("rename_op")
            assert held_item_count(after_resume, DEFAULT_RENAME_ITEM_ID) == 0
            assert _count_bag_item(after_resume, DEFAULT_RENAME_ITEM_ID) == 0
            assert await directory.player_names([2001]) == {2001: "重启新名"}

            notifies = _decode_rename_notifies(env2.protocol, env2.pushes, 2001)
            assert len(notifies) == 1
            assert notifies[0].error_message.id == 0
            assert notifies[0].name == "重启新名"

            # 无在途 rename_op 时 resume_pending 返回 None。
            assert rename2.resume_pending(env2.player) is None

    asyncio.run(exercise())


def test_battle_settlement_progression_hooks_season_trial_and_material(tmp_path):
    """BattleSettlement.apply 原子推进赛季印、试炼志与装备每日材料自选额度，并保持幂等与逃跑/作废隔离。"""

    async def exercise():
        now = [1_760_000_000_000]
        env = await _make_scene_env(tmp_path, player_id=3001, initial_name="试炼侠客", now_ref=now)
        try:
            missions = Missions(env.scene, env.tables, env.assets)
            env.scene.season = SceneSeason(env.scene, env.tables, env.assets, clock=env.clock)
            env.scene.trial_journal = SceneTrialJournal(env.scene, env.tables, env.assets, clock=env.clock)
            env.scene.equip_craft = SceneEquipCraft(env.scene, env.tables, env.assets, clock=env.clock)
            env.scene.season.register()
            env.scene.trial_journal.register()
            env.scene.equip_craft.register()
            settlement_svc = BattleSettlement(env.scene, env.tables, env.assets, missions, clock=env.clock)

            # 1. PVE 试炼首领（entry 101）5 回合胜出：同时更新赛季、试炼志与材料自选额度。
            data = env.scene.rows.assemble(env.player.snapshot)
            pve_win = env.protocol.message(
                "BattleSettlementData",
                battle_id=7001,
                player_id=3001,
                outcome=1,
                player_team_index=0,
                health=100,
                mana=50,
                total_rounds=5,
                finished_at_ms=now[0],
                match_mode=4,
                battle_config_id=101,
                participated=True,
            )
            assert await settlement_svc.apply(env.player, data, pve_win) is True
            await env.scene.commit_player_data(env.player, data)

            saved = env.scene.rows.assemble(env.player.snapshot)
            assert len(saved.season_component.records) == 1
            assert saved.season_component.records[0].season_id == 1
            assert saved.season_component.records[0].progress_points == 1
            assert saved.season_component.records[0].battle_points == 1

            # 试炼志卷一 entry 101 的破阵(bit 0)与速破(bit 1)点亮，单人最佳回合记为 5。
            assert has_packed_bit(saved.trial_journal_component.lit_seal_bits, 0)
            assert has_packed_bit(saved.trial_journal_component.lit_seal_bits, 1)
            rec_101 = next(r for r in saved.trial_journal_component.records if r.entry_id == 101)
            assert rec_101.solo_best.best_rounds == 5

            # 装备材料自选额度入账 10 份。
            mat_resp = await env.rpc("SceneBagClientPlayer", "GetEquipmentMaterialReward")
            assert mat_resp.error_message.id == 0
            assert mat_resp.snapshot.available_units == MATERIAL_REWARD_BATTLE_UNITS
            assert mat_resp.snapshot.earned_today == MATERIAL_REWARD_BATTLE_UNITS

            # 2. 相同 battle_id 幂等重放不重复加分或发额度。
            data_dup = env.scene.rows.assemble(env.player.snapshot)
            assert await settlement_svc.apply(env.player, data_dup, pve_win) is True
            assert data_dup.season_component.records[0].progress_points == 1
            mat_dup = await env.rpc("SceneBagClientPlayer", "GetEquipmentMaterialReward")
            assert mat_dup.snapshot.available_units == MATERIAL_REWARD_BATTLE_UNITS

            # 3. PVP 败北（match_mode=3, outcome=2, player_team_index=0）：赛季 +1 (pvp_other_points)、材料 +10，不碰试炼志。
            data_pvp = env.scene.rows.assemble(env.player.snapshot)
            pvp_loss = env.protocol.message(
                "BattleSettlementData",
                battle_id=7002,
                player_id=3001,
                outcome=2,
                player_team_index=0,
                health=100,
                mana=50,
                total_rounds=4,
                finished_at_ms=now[0] + 5_000,
                match_mode=3,
                battle_config_id=1,
            )
            assert await settlement_svc.apply(env.player, data_pvp, pvp_loss) is True
            await env.scene.commit_player_data(env.player, data_pvp)
            assert data_pvp.season_component.records[0].progress_points == 2
            mat_pvp = await env.rpc("SceneBagClientPlayer", "GetEquipmentMaterialReward")
            assert mat_pvp.snapshot.available_units == MATERIAL_REWARD_BATTLE_UNITS * 2

            # 4. 逃跑与作废（outcome=4）不推进任何成长进度。
            data_skip = env.scene.rows.assemble(env.player.snapshot)
            fled_battle = env.protocol.message(
                "BattleSettlementData",
                battle_id=7003,
                player_id=3001,
                outcome=1,
                player_team_index=0,
                health=100,
                mana=50,
                fled=True,
                total_rounds=2,
                match_mode=4,
                battle_config_id=102,
            )
            assert await settlement_svc.apply(env.player, data_skip, fled_battle) is True
            assert data_skip.season_component.records[0].progress_points == 2
            assert not any(r.entry_id == 102 for r in data_skip.trial_journal_component.records)

            void_battle = env.protocol.message(
                "BattleSettlementData",
                battle_id=7004,
                player_id=3001,
                outcome=4,
                player_team_index=0,
                match_mode=4,
                battle_config_id=102,
            )
            assert await settlement_svc.apply(env.player, data_skip, void_battle) is True
            assert data_skip.season_component.records[0].progress_points == 2

            # 5. 带 trial_facts 的结算对象走精确试炼事实口径（含合战最佳记录）。
            class _TrialFactsSettlement:
                def __init__(self, base, facts):
                    self._base = base
                    self.trial_facts = facts

                def __getattr__(self, item):
                    return getattr(self._base, item)

                def HasField(self, name: str) -> bool:
                    return True if name == "trial_facts" else self._base.HasField(name)

                def ListFields(self):
                    return self._base.ListFields()

            base_coop = env.protocol.message(
                "BattleSettlementData",
                battle_id=7005,
                player_id=3001,
                outcome=1,
                player_team_index=0,
                health=100,
                mana=50,
                total_rounds=6,
                finished_at_ms=now[0] + 10_000,
                match_mode=5,
                battle_config_id=0,
                participated=True,
            )
            facts = SimpleNamespace(
                journal_entry_id=102,
                trial_rule_version=1,
                initial_human_count=2,
                self_or_pet_ever_died=False,
                boss_guard_heal_total=0,
                guard_stance_dispelled=True,
                team_dealt_physical_direct_damage=True,
                team_dealt_magic_direct_damage=False,
                boss_wind_hit_unprotected_death=False,
                dealt_yang_bell_physical_damage=False,
                dealt_yin_drum_magic_damage=False,
                finished_in_ebb_tide=False,
                guard_survived=False,
                self_triggered_shield_break=False,
                self_triggered_anti_control_or_season_skill=False,
                used_pet_table_id=1001,
                used_artifact_effect_ids=(1,),
                used_season_seal_id=101,
                teammate_names=("道友乙",),
            )
            data_coop = env.scene.rows.assemble(env.player.snapshot)
            settlement_svc._apply_progression_hooks(
                env.player, data_coop, _TrialFactsSettlement(base_coop, facts), now[0] + 10_000,
            )
            rec_102 = next(r for r in data_coop.trial_journal_component.records if r.entry_id == 102)
            assert rec_102.coop_best.best_rounds == 6
            assert rec_102.coop_best.party_size == 2
            assert list(rec_102.coop_best.teammate_names) == ["道友乙"]
            # entry 102 的破阵(bit 3)、速破(bit 4)、破势(bit 5)均已点亮。
            assert has_packed_bit(data_coop.trial_journal_component.lit_seal_bits, 3)
            assert has_packed_bit(data_coop.trial_journal_component.lit_seal_bits, 4)
            assert has_packed_bit(data_coop.trial_journal_component.lit_seal_bits, 5)
        finally:
            await env.scene.close()

    asyncio.run(exercise())
