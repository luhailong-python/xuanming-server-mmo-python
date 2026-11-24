"""Scene GM 命令、挂机月卡与角色形象系统的单元测试与端到端联调测试。"""

from __future__ import annotations

import asyncio
from itertools import count
from types import SimpleNamespace

import pytest

from xuanming.data import ConfigTables, JsonPlayerStore, PlayerRowCodec, PlayerRowKind
from xuanming.data.asset_inventory import BLOCKED, FEATURE_UNAVAILABLE, balance
from xuanming.gm_admin.clients import GamePlayerAdmin, sign_player_command
from xuanming.gm_admin.journal import CallFailure
from xuanming.grpc_runtime import GrpcClient, serve_grpc
from xuanming.nodes.scene import SceneNode
from xuanming.nodes.scene_afk_pass import (
    AFK_PASS_CURRENCY_NOT_ENOUGH,
    AFK_PASS_PRICE_CHANGED,
    AFK_PASS_STACK_LIMIT,
    SECONDS_PER_DAY,
    SceneAfkPass,
    renewed_expiry,
)
from xuanming.nodes.scene_appearance import (
    APPEARANCE_ALREADY_OWNED,
    APPEARANCE_NO_ACTIVE_TRIAL,
    APPEARANCE_NO_CARD,
    APPEARANCE_NOT_OWNED,
    APPEARANCE_TRIAL_ALREADY_USED,
    SceneAppearance,
    clear_packed_bit,
    effective_appearance_id,
    set_packed_bit,
    test_packed_bit,
)
from xuanming.nodes.scene_assets import SceneAssets
from xuanming.nodes.scene_attributes import SceneAttributes, initialize_attribute_schemes
from xuanming.nodes.scene_connection_hold import afk_pass_key, parse_afk_pass
from xuanming.nodes.scene_gm_commands import (
    GM_PLAYER_COMMAND_METHOD,
    SceneGmCommands,
    command_digest,
)
from xuanming.nodes.scene_pets import ScenePets
from xuanming.nodes.scene_skills import initialize_skill_list
from xuanming.nodes.scene_stats import initialize_base
from xuanming.protocol import Protocol


class _FakeAsyncRedis:
    """记录 SET/DEL 调用的内存异步 Redis 替身。"""

    def __init__(self) -> None:
        self.kv: dict[str, tuple[bytes, int | None]] = {}
        self.ops: list[tuple] = []

    async def set(self, key: str, value: bytes, *, ex: int | None = None) -> bool:
        self.kv[key] = (value, ex)
        self.ops.append(("SET", key, value, ex))
        return True

    async def delete(self, key: str) -> int:
        existed = key in self.kv
        self.kv.pop(key, None)
        self.ops.append(("DEL", key))
        return int(existed)

    async def get(self, key: str) -> bytes | None:
        entry = self.kv.get(key)
        return entry[0] if entry is not None else None


async def _make_scene_env(tmp_path, *, allow_gm: bool = True, now_sec: int = 1_800_000_000):
    """构造带完整资产/属性/宠物子系统的真实 SceneNode 夹具，并入场一名在线玩家。"""
    protocol = Protocol()
    tables = ConfigTables()
    rows = PlayerRowCodec(protocol)
    store = JsonPlayerStore(tmp_path / "players.json")
    info = protocol.message("NodeInfo", node_id=7, node_type=2, zone_id=1, node_uuid="scene-test-uuid")
    pushes: list[tuple[int, int, bytes]] = []

    def emit(player, content) -> None:
        pushes.append((player.player_id, content.message_id, bytes(content.serialized_message)))

    scene = SceneNode(protocol, info, store, rows, emit)
    scene.tables = tables
    scene.create(protocol.message("CreateSceneRequest", config_id=1, scene_id=1))

    ids = count(50_000)

    async def alloc_id() -> int:
        return next(ids)

    scene.assets = SceneAssets(
        scene,
        tables,
        allow_gm=allow_gm,
        allocate_item_id=alloc_id,
        anomaly_journal_path=tmp_path / "anomaly.json",
    )
    scene.assets.register_client_handlers()
    scene.attributes = SceneAttributes(scene, tables, scene.assets, allow_gm=allow_gm, now_s=lambda: now_sec)
    scene.attributes.register()
    scene.pets = ScenePets(scene, tables)

    def init_player(data) -> None:
        data.level_component.level = 5
        setattr(data.uint32_pb_component, "class", 1)
        data.profile_component.name = "测试角色"
        data.profile_component.gender = 1
        data.profile_component.appearance_id = "00_reference_topright_boy"
        initialize_attribute_schemes(data)
        scene.pets.initialize_on_load(data)
        initialize_skill_list(tables, data)
        initialize_base(protocol, tables, data)

    scene.initialize_player = init_player
    return SimpleNamespace(
        protocol=protocol,
        tables=tables,
        store=store,
        scene=scene,
        pushes=pushes,
    )


async def _enter_player(env, *, player_id: int = 1001, session_id: int = 11, generation: int = 1, gate_uuid: str = "gate-1"):
    req = env.protocol.message(
        "PlayerEnterGameNodeRequest",
        player_id=player_id,
        scene_id=1,
        home_zone_id=1,
        owner_epoch=1,
        session_id=session_id,
        login_generation=generation,
        owner_revision=1,
    )
    await env.scene.enter(req, gate_uuid)
    world = env.scene.locations[player_id]
    return world, world.players[player_id]


async def _invoke_client_rpc(env, player, service: str, method: str, **fields):
    """通过 SceneNode 正式的客户端 RPC 派发入口调用已注册方法。"""
    p = env.protocol
    msg_id = p.message_ids[f"{service}{method}"]
    route = p.routes[msg_id]
    req = p.message(route.request_type, **fields)
    envelope = p.message(
        "ProcessClientPlayerMessageRequest",
        session_id=player.session_id,
        player_id=player.player_id,
        message_content=p.content(msg_id, 1, body=req.SerializeToString()),
    )
    reply = await env.scene.process(envelope, player.gate_uuid)
    assert reply.message_content.error_message.id == 0, f"unexpected transport error {reply.message_content.error_message.id}"
    return p.parse(route.response_type, reply.message_content.serialized_message)


def test_gm_command_signature_vector_matches_cpp_and_gm_admin():
    """验证与 C++ gm_player_command_test.cpp 逐字节一致的摘要与签名向量。"""
    protocol = Protocol()
    secret = b"0123456789abcdef0123456789abcdef"
    request = protocol.message(
        "scene_node.GmExecutePlayerCommandRequest",
        command_id=9007199254740993,
        player_id=9007199254740995,
        operator="gm-test",
        reason="test-only",
        target_node_id=7,
        expected_session_id=10,
        expected_login_generation=9007199254740999,
        issued_at_ms=1700000000123,
        add_currency={"currency_type": 2, "amount": 123456},
    )
    assert command_digest(request) == "596cbd6cc4d472b1ee7bcf964d1f8c990b99ee5d734b3e93c63b4b46b6bd1078"
    sign_player_command(GM_PLAYER_COMMAND_METHOD, request, secret, 1700000000, "9007199254740993")
    assert request.auth == (
        "gm-test|1700000000|9007199254740993|"
        "dedbaa0b050bd6dba806c474571f1adf395fc840e9470f347932fc8d44fea056"
    )


def test_gm_admin_live_wire_all_seven_commands_replay_and_security(tmp_path):
    """通过真实 loopback grpc.aio 验证 GamePlayerAdmin 与 SceneGmCommands 的全量联调。"""
    secret = b"0123456789abcdef0123456789abcdef"
    now_sec = 1_800_000_000
    now_ms = now_sec * 1000 + 250

    async def exercise():
        env = await _make_scene_env(tmp_path, allow_gm=True, now_sec=now_sec)
        _, player = await _enter_player(env, player_id=2001, session_id=22, generation=9)
        gm = SceneGmCommands(env.scene, secret=secret, allow_gm=True, clock=lambda: float(now_sec))

        def get_session(request):
            return env.protocol.message(
                "playerlocator.GetSessionResponse",
                found=True,
                session={
                    "player_id": request.player_id,
                    "session_id": player.session_id,
                    "login_generation": player.generation,
                    "state": 1,
                    "scene_node_id": str(env.scene.info.node_id),
                    "gate_zone_id": env.scene.info.zone_id,
                },
                placement={
                    "found": True,
                    "node_id": str(env.scene.info.node_id),
                    "zone_id": env.scene.info.zone_id,
                    "session_id": player.session_id,
                    "login_generation": player.generation,
                },
            )

        server, port = await serve_grpc(
            env.protocol,
            "127.0.0.1:0",
            {
                "playerlocator.PlayerLocator": {"GetSession": get_session},
                "scene_node.SceneNodeGrpc": gm.grpc_methods(),
            },
        )
        endpoint = f"127.0.0.1:{port}"
        locator_client = GrpcClient(endpoint, timeout=5.0, enable_retries=False)
        admin = GamePlayerAdmin(
            env.protocol,
            locator=locator_client,
            static_targets={f"1/{env.scene.info.node_id}": endpoint},
            scene_etcd_url="",
            scene_admin_secret=secret,
            plaintext=True,
            clock_ms=lambda: now_ms,
        )
        scene_client = GrpcClient(endpoint, timeout=5.0, enable_retries=False)

        try:
            def frozen(request_id: int, operation: str, arguments: dict) -> dict:
                return {
                    "operation": operation,
                    "arguments": arguments,
                    "requestId": str(request_id),
                    "playerId": str(player.player_id),
                    "operator": "gm-alice",
                    "reason": "live-wire-verification",
                    "issuedAtMs": now_ms,
                }

            # 1. currency.add
            res_add = await admin.execute(frozen(10001, "currency.add", {"currencyType": 1, "amount": "500"}))
            assert res_add["executed"] is True and res_add["balanceAfter"] == "500"

            # 2. currency.deduct
            res_deduct = await admin.execute(frozen(10002, "currency.deduct", {"currencyType": 1, "amount": "120"}))
            assert res_deduct["executed"] is True and res_deduct["balanceAfter"] == "380"

            # 3. currency.block + 验证封禁后加币被拒绝
            res_block = await admin.execute(frozen(10003, "currency.block", {"currencyType": 1}))
            assert res_block["executed"] is True
            with pytest.raises(CallFailure) as blocked_exc:
                await admin.execute(frozen(10004, "currency.add", {"currencyType": 1, "amount": "10"}))
            assert blocked_exc.value.tip_id == BLOCKED

            # 4. currency.unblock + 验证解封后可正常加币
            res_unblock = await admin.execute(frozen(10005, "currency.unblock", {"currencyType": 1}))
            assert res_unblock["executed"] is True
            res_after_unblock = await admin.execute(frozen(10006, "currency.add", {"currencyType": 1, "amount": "20"}))
            assert res_after_unblock["balanceAfter"] == "400"

            # 5. player.level
            res_level = await admin.execute(frozen(10007, "player.level", {"level": 20}))
            assert res_level["executed"] is True
            data = env.scene.rows.assemble(player.snapshot)
            assert data.level_component.level == 20

            # 6. pet.grant
            res_pet = await admin.execute(frozen(10008, "pet.grant", {"petTableId": 1}))
            assert res_pet["executed"] is True and int(res_pet["petId"]) > 0
            data = env.scene.rows.assemble(player.snapshot)
            assert any(pet.pet_id == int(res_pet["petId"]) for pet in data.pet_component.pets)

            # 7. item.grant
            first_item_id = next(row.id for row in env.tables["Item"].rows() if not row.equip_kind)
            res_item = await admin.execute(frozen(10009, "item.grant", {"configId": first_item_id, "count": 3}))
            assert res_item["executed"] is True
            data = env.scene.rows.assemble(player.snapshot)
            assert env.scene.assets.removable_count(data, first_item_id) == 3

            # 8. 幂等重放：同一 command_id 带相同载荷重放返回缓存响应且不重复入账
            replay_same = await admin.execute(frozen(10006, "currency.add", {"currencyType": 1, "amount": "20"}))
            assert replay_same["executed"] is True and replay_same["balanceAfter"] == "400"
            data = env.scene.rows.assemble(player.snapshot)
            assert balance(data, 1) == 400

            # 同 command_id 换新 nonce 重放同样命中缓存
            resigned = env.protocol.message(
                "scene_node.GmExecutePlayerCommandRequest",
                command_id=10006,
                player_id=player.player_id,
                operator="gm-alice",
                reason="live-wire-verification",
                target_node_id=env.scene.info.node_id,
                expected_session_id=player.session_id,
                expected_login_generation=player.generation,
                issued_at_ms=now_ms,
                add_currency={"currency_type": 1, "amount": 20},
            )
            sign_player_command(GM_PLAYER_COMMAND_METHOD, resigned, secret, now_ms // 1000, "fresh-nonce-10006")
            raw = await scene_client.call(GM_PLAYER_COMMAND_METHOD, resigned.SerializeToString())
            parsed_replay = env.protocol.parse("scene_node.GmExecutePlayerCommandResponse", raw)
            assert parsed_replay.executed is True and parsed_replay.balance_after == 400
            assert balance(env.scene.rows.assemble(player.snapshot), 1) == 400

            # 9. 非法签名被直接拒绝（gRPC 层拒绝）
            bad_admin = GamePlayerAdmin(
                env.protocol,
                locator=locator_client,
                static_targets={f"1/{env.scene.info.node_id}": endpoint},
                scene_etcd_url="",
                scene_admin_secret=b"wrong-secret-0123456789abcdef0123",
                plaintext=True,
                clock_ms=lambda: now_ms,
            )
            try:
                with pytest.raises(CallFailure) as sig_exc:
                    await bad_admin.execute(frozen(10010, "currency.add", {"currencyType": 1, "amount": "1"}))
                assert sig_exc.value.code != "SERVER_TIP"
            finally:
                await bad_admin.close()

            # 10. prod 模式闸关闭时拒绝执行且不改资产
            gm_prod = SceneGmCommands(env.scene, secret=secret, allow_gm=False, clock=lambda: float(now_sec))
            prod_req = env.protocol.message(
                "scene_node.GmExecutePlayerCommandRequest",
                command_id=10011,
                player_id=player.player_id,
                operator="gm-alice",
                reason="prod-check",
                target_node_id=env.scene.info.node_id,
                expected_session_id=player.session_id,
                expected_login_generation=player.generation,
                issued_at_ms=now_ms,
                add_currency={"currency_type": 1, "amount": 99},
            )
            sign_player_command(GM_PLAYER_COMMAND_METHOD, prod_req, secret, now_ms // 1000, "10011")
            prod_reply = await gm_prod.execute(prod_req)
            assert not prod_reply.executed and prod_reply.error_message.id == FEATURE_UNAVAILABLE
            assert balance(env.scene.rows.assemble(player.snapshot), 1) == 400

            # 11. 会话不匹配拒绝执行
            bad_session_req = env.protocol.message(
                "scene_node.GmExecutePlayerCommandRequest",
                command_id=10012,
                player_id=player.player_id,
                operator="gm-alice",
                reason="session-mismatch",
                target_node_id=env.scene.info.node_id,
                expected_session_id=player.session_id + 1,
                expected_login_generation=player.generation,
                issued_at_ms=now_ms,
                add_currency={"currency_type": 1, "amount": 99},
            )
            sign_player_command(GM_PLAYER_COMMAND_METHOD, bad_session_req, secret, now_ms // 1000, "10012")
            mismatch_reply = await gm.execute(bad_session_req)
            assert not mismatch_reply.executed and mismatch_reply.error_message.id == FEATURE_UNAVAILABLE
        finally:
            await scene_client.close()
            await admin.close()
            await locator_client.close()
            await server.stop(0)
            await env.scene.close()
            await env.scene.assets.close()

    asyncio.run(exercise())


def test_afk_pass_buy_stack_replay_gm_set_and_redis_projection(tmp_path):
    """验证挂机月卡开通、叠加、上限拒绝、余额不足、幂等重放、GM 设定与 Redis 投影。"""
    now = [1_800_000_000]

    assert renewed_expiry(0, now[0], 30, 180) == now[0] + 30 * SECONDS_PER_DAY
    assert renewed_expiry(now[0] + 150 * SECONDS_PER_DAY, now[0], 30, 180) == now[0] + 180 * SECONDS_PER_DAY
    assert renewed_expiry(now[0] + 150 * SECONDS_PER_DAY + 1, now[0], 30, 180) is None

    async def exercise():
        env = await _make_scene_env(tmp_path, allow_gm=False, now_sec=now[0])
        redis = _FakeAsyncRedis()
        afk = SceneAfkPass(
            env.scene,
            env.tables,
            env.scene.assets,
            player_redis=redis,
            allow_gm=False,
            now_s=lambda: now[0],
        )
        afk.register()
        assert afk.validate_tables()

        _, player = await _enter_player(env, player_id=3001, session_id=31, generation=1)
        try:
            # 1. 初始查询
            initial = await _invoke_client_rpc(env, player, "SceneAfkPassClientPlayer", "GetAfkPass")
            assert not initial.error_message.id
            rule_info = initial.info
            assert rule_info.expires_at == 0
            assert rule_info.server_now == now[0]
            assert rule_info.price_currency_type == 1
            assert rule_info.price == 300
            assert rule_info.period_days == 30
            assert rule_info.max_remaining_days == 180
            assert rule_info.balance == 0

            # 2. 报价防呆与余额不足拒绝
            bad_price = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "BuyAfkPass",
                client_request_id=100, expected_currency_type=1, expected_price=299,
            )
            assert bad_price.error_message.id == AFK_PASS_PRICE_CHANGED

            no_money = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "BuyAfkPass",
                client_request_id=101, expected_currency_type=1, expected_price=300,
            )
            assert no_money.error_message.id == AFK_PASS_CURRENCY_NOT_ENOUGH
            assert not redis.ops

            # 给玩家充 2500 灵石
            data = env.scene.rows.assemble(player.snapshot)
            assert env.scene.assets.add_currency(data, 1, 2500) == 0
            await env.scene.commit_player_data(player, data)

            # 3. 首次开通 + Redis EX 投影
            first = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "BuyAfkPass",
                client_request_id=201, expected_currency_type=1, expected_price=300,
            )
            assert not first.error_message.id and not first.replayed
            expected_exp = now[0] + 30 * SECONDS_PER_DAY
            assert first.info.expires_at == expected_exp
            assert first.info.balance == 2200
            key = afk_pass_key(player.player_id)
            assert redis.kv[key] == (str(expected_exp).encode("ascii"), 30 * SECONDS_PER_DAY)
            assert parse_afk_pass(await redis.get(key), now[0]) == expected_exp

            # 4. 幂等重放不重复扣费不重复续期
            replayed = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "BuyAfkPass",
                client_request_id=201, expected_currency_type=1, expected_price=300,
            )
            assert not replayed.error_message.id and replayed.replayed
            assert replayed.info.expires_at == expected_exp and replayed.info.balance == 2200

            # 5. 连续叠加至 180 天上限，第 7 次被拒
            for req_id in range(202, 207):
                stacked = await _invoke_client_rpc(
                    env, player, "SceneAfkPassClientPlayer", "BuyAfkPass",
                    client_request_id=req_id, expected_currency_type=1, expected_price=300,
                )
                assert not stacked.error_message.id
            assert stacked.info.expires_at == now[0] + 180 * SECONDS_PER_DAY
            assert stacked.info.balance == 700
            assert redis.kv[key] == (str(now[0] + 180 * SECONDS_PER_DAY).encode("ascii"), 180 * SECONDS_PER_DAY)

            over_cap = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "BuyAfkPass",
                client_request_id=207, expected_currency_type=1, expected_price=300,
            )
            assert over_cap.error_message.id == AFK_PASS_STACK_LIMIT
            assert balance(env.scene.rows.assemble(player.snapshot), 1) == 700

            # 6. GM 设定（关闭时拒绝，开启后可精确设定与清零并同步 DEL Redis）
            gm_denied = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "GmSetAfkPass", remaining_seconds=120,
            )
            assert gm_denied.error_message.id == FEATURE_UNAVAILABLE

            afk._allow_gm = True
            gm_set = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "GmSetAfkPass", remaining_seconds=120,
            )
            assert not gm_set.error_message.id and gm_set.info.expires_at == now[0] + 120
            assert redis.kv[key] == (str(now[0] + 120).encode("ascii"), 120)

            gm_clear = await _invoke_client_rpc(
                env, player, "SceneAfkPassClientPlayer", "GmSetAfkPass", remaining_seconds=0,
            )
            assert not gm_clear.error_message.id and gm_clear.info.expires_at == 0
            assert key not in redis.kv
        finally:
            await env.scene.close()
            await env.scene.assets.close()

    asyncio.run(exercise())


def test_appearance_bitset_trial_cancel_expiry_purchase_and_free_switch(tmp_path):
    """验证位图往返与未知高位保留、1 小时免费试用/取消/到期广播、形象卡解锁与已拥有免费回切。"""
    # 1. 纯位图往返、去尾零与未知高位保留
    unknown_high = bytes([0x00, 0x00, 0x02])  # bit 17
    bits = set_packed_bit(unknown_high, 0)
    bits = set_packed_bit(bits, 10)
    assert test_packed_bit(bits, 0) and test_packed_bit(bits, 10) and test_packed_bit(bits, 17)
    assert not test_packed_bit(bits, 1)
    assert bits == bytes([0x01, 0x04, 0x02])
    cleared = clear_packed_bit(clear_packed_bit(clear_packed_bit(bits, 0), 10), 17)
    assert cleared == b""

    now = [1_800_000_000]

    async def exercise():
        env = await _make_scene_env(tmp_path, allow_gm=True, now_sec=now[0])
        appearance = SceneAppearance(env.scene, env.tables, env.scene.assets, now_s=lambda: now[0])
        appearance.register()

        world, player = await _enter_player(env, player_id=4001, session_id=41, generation=1)
        _, watcher = await _enter_player(env, player_id=4002, session_id=42, generation=1)
        assert watcher.player_id in {p.player_id for p in env.scene.watchers(world, player)}

        try:
            # 预置未知高位 bit 17 于 owned_bits，确认后续切换绝不截断
            data = env.scene.rows.assemble(player.snapshot)
            data.profile_component.appearance_owned_bits = bytes([0x00, 0x00, 0x02])
            await env.scene.commit_player_data(player, data)

            # 2. GetAppearanceInfo
            info_resp = await _invoke_client_rpc(env, player, "SceneAppearanceClientPlayer", "GetAppearanceInfo")
            assert not info_resp.error_message.id
            info = info_resp.info
            assert info.permanent_appearance_id == "00_reference_topright_boy"
            assert info.active_appearance_id == "00_reference_topright_boy"
            assert info.card_item_id == 3402 and info.card_item_count == 1 and info.owned_card_count == 0
            assert info.trial_duration_seconds == 3600
            assert test_packed_bit(info.owned_bits, 17)
            entries = {item.appearance_id: item for item in info.appearances}
            assert entries["00_reference_topright_boy"].owned is True
            assert entries["01_ice_sword_girl"].owned is False and entries["01_ice_sword_girl"].trial_used is False
            series = {item.series_key: item for item in info.series_progress}
            assert series["lantern_courtyard"].owned_count == 1 and series["lantern_courtyard"].total_count == 3
            assert series["elemental_blades"].owned_count == 0 and series["elemental_blades"].completed is False

            # 3. 开启 1 小时免费试用 + AOI 广播 + 幂等重放
            env.pushes.clear()
            trial = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "StartAppearanceTrial",
                client_request_id=501, appearance_id="01_ice_sword_girl",
            )
            assert not trial.error_message.id and not trial.replayed
            assert trial.info.active_appearance_id == "01_ice_sword_girl"
            assert trial.info.trial_appearance_id == "01_ice_sword_girl"
            assert trial.info.trial_expires_at == now[0] + 3600
            assert effective_appearance_id(env.scene.rows.assemble(player.snapshot), now[0]) == "01_ice_sword_girl"
            assert effective_appearance_id(env.scene.rows.assemble(player.snapshot), now[0] + 3600) == "00_reference_topright_boy"
            # 试用不计入系列已拥有数
            trial_series = {item.series_key: item for item in trial.info.series_progress}
            assert trial_series["elemental_blades"].owned_count == 0

            changed_msg_id = env.protocol.message_ids["SceneAppearanceClientPlayerNotifyActorAppearanceChanged"]
            state_msg_id = env.protocol.message_ids["SceneAppearanceClientPlayerNotifyAppearanceState"]
            changed_recipients = {pid for pid, mid, _ in env.pushes if mid == changed_msg_id}
            assert {player.player_id, watcher.player_id} <= changed_recipients

            trial_replay = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "StartAppearanceTrial",
                client_request_id=501, appearance_id="01_ice_sword_girl",
            )
            assert not trial_replay.error_message.id and trial_replay.replayed

            # 4. 提前取消试用 + 再次取消被拒 + 一次性试用限制
            cancelled = await _invoke_client_rpc(env, player, "SceneAppearanceClientPlayer", "CancelAppearanceTrial")
            assert not cancelled.error_message.id
            assert cancelled.info.active_appearance_id == "00_reference_topright_boy"
            assert cancelled.info.trial_appearance_id == "" and cancelled.info.trial_expires_at == 0

            cancel_again = await _invoke_client_rpc(env, player, "SceneAppearanceClientPlayer", "CancelAppearanceTrial")
            assert cancel_again.error_message.id == APPEARANCE_NO_ACTIVE_TRIAL

            retrial = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "StartAppearanceTrial",
                client_request_id=502, appearance_id="01_ice_sword_girl",
            )
            assert retrial.error_message.id == APPEARANCE_TRIAL_ALREADY_USED

            # 5. 试用自然到期：expire_trials 还原并同时广播变更与单播状态通知
            trial2 = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "StartAppearanceTrial",
                client_request_id=503, appearance_id="02_fire_talisman_boy",
            )
            assert not trial2.error_message.id
            assert await appearance.expire_trials(now[0] + 3599) == []

            env.pushes.clear()
            now[0] += 3600
            expired = await appearance.expire_trials(now[0])
            assert expired == [player.player_id]
            changed_msgs = [
                (pid, env.protocol.parse("ActorAppearanceChangedS2C", body))
                for pid, mid, body in env.pushes if mid == changed_msg_id
            ]
            assert {pid for pid, _ in changed_msgs} >= {player.player_id, watcher.player_id}
            assert all(not msg.is_trial and msg.appearance_id == "00_reference_topright_boy" for _, msg in changed_msgs)
            state_msgs = [
                env.protocol.parse("AppearanceStateNotify", body)
                for pid, mid, body in env.pushes if pid == player.player_id and mid == state_msg_id
            ]
            assert len(state_msgs) == 1 and state_msgs[0].trial_expired is True
            assert state_msgs[0].info.active_appearance_id == "00_reference_topright_boy"

            # 6. 永久切换：未拥有免费切换被拒；无卡买断被拒；发卡后消耗 1 张永久解锁并可 0 消耗回切
            free_refused = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "SwitchAppearance",
                client_request_id=601, appearance_id="01_ice_sword_girl", expect_free_switch=True,
            )
            assert free_refused.error_message.id == APPEARANCE_NOT_OWNED

            no_card = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "SwitchAppearance",
                client_request_id=602, appearance_id="01_ice_sword_girl", expect_free_switch=False,
            )
            assert no_card.error_message.id == APPEARANCE_NO_CARD

            # 放入 1 张角色形象卡 (config_id=3402) 到人物背包 (bag_type=0)
            data = env.scene.rows.assemble(player.snapshot)
            card_entry = data.bag_component.items.add(bag_type=0, pos=0)
            card_item = env.protocol.message("ItemComp", item_id=990001, config_id=3402, size=1, acquire_seq=1)
            env.scene.assets.write_entry(card_entry, card_item)
            await env.scene.commit_player_data(player, data)

            unlocked = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "SwitchAppearance",
                client_request_id=603, appearance_id="01_ice_sword_girl", expect_free_switch=False,
            )
            assert not unlocked.error_message.id and not unlocked.replayed
            assert unlocked.info.permanent_appearance_id == "01_ice_sword_girl"
            assert unlocked.info.owned_card_count == 0
            assert test_packed_bit(unlocked.info.owned_bits, 17)  # 未知高位原样保留
            saved_profile = env.scene.rows.assemble(player.snapshot).profile_component
            assert saved_profile.appearance_switch_seq == 1

            # 幂等重放不会再扣卡
            switch_replay = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "SwitchAppearance",
                client_request_id=603, appearance_id="01_ice_sword_girl", expect_free_switch=False,
            )
            assert not switch_replay.error_message.id and switch_replay.replayed

            # 0 消耗免费切回原形象
            free_back = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "SwitchAppearance",
                client_request_id=604, appearance_id="00_reference_topright_boy", expect_free_switch=True,
            )
            assert not free_back.error_message.id
            assert free_back.info.permanent_appearance_id == "00_reference_topright_boy"
            assert env.scene.rows.assemble(player.snapshot).profile_component.appearance_switch_seq == 2

            # 对已永久拥有的形象发起试用应被拒
            trial_owned = await _invoke_client_rpc(
                env, player, "SceneAppearanceClientPlayer", "StartAppearanceTrial",
                client_request_id=605, appearance_id="01_ice_sword_girl",
            )
            assert trial_owned.error_message.id == APPEARANCE_ALREADY_OWNED
        finally:
            await env.scene.close()
            await env.scene.assets.close()

    asyncio.run(exercise())
