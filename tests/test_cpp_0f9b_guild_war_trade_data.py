"""C++ 提交 0f9bc81d6 增量移植回归测试：

覆盖四大领域：
1. DataService.SyncAccountStage1Tradable (498)：鉴权、参数校验、跨角色终身 60 份硬上限与删角不重置；
2. TradeAdmin.RecordSellerEnvironment (496) & RecordSellerViolation (497)：环境信号、仅换 IP 不清零、异地换机清 0 保黑历史、生产态鉴权；
3. MatchInternal.StartGuildWarBattle (495) + Mode 9/10：无匹配池真人直打、守旗 AI 快照强制托管、公开观战与并发占坑返回 offender_battle_id；
4. GuildService 全天帮战 V2 (485..494, 499)：三灵脉每帮限占 1 座、守旗阵、4 层奖励与防互刷递减、追杀令/追踪/反杀撕单、换帮不重领周箱与分红，以及 Runtime("local") Gate TCP 端到端贯通。
"""

from __future__ import annotations

import asyncio
import time
from contextlib import AsyncExitStack

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from data_service_fakes import MemoryStore as BaseDataMemoryStore
from test_monopoly import TOKEN, _enter, _settings
from xuanming.data_service.errors import Invalid
from xuanming.data_service.model import (
    STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT,
    Stage1TradableQuotaLedger,
)
from xuanming.data_service.service import DataService
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata, sign_service_call
from xuanming.guild.errors import (
    ERR_GUILD_WAR_BOUNTY_ALREADY_ACTIVE,
    ERR_GUILD_WAR_BOUNTY_LIMIT,
    ERR_GUILD_WAR_GARRISON_COOLDOWN,
    ERR_GUILD_WAR_MILESTONE_CLAIMED,
    ERR_GUILD_WAR_MILESTONE_LOCKED,
    ERR_GUILD_WAR_PEACE_COOLDOWN,
    ERR_GUILD_WAR_SAME_GUILD,
    ERR_GUILD_WAR_SELF_PEACE_MODE,
    ERR_GUILD_WAR_TARGET_IN_BATTLE,
    ERR_GUILD_WAR_TARGET_PROTECTED,
    ERR_GUILD_WAR_TRACK_COOLDOWN,
)
from xuanming.guild.model import BATTLE_OUTCOME_SIDE_A_WIN, BATTLE_OUTCOME_SIDE_B_WIN
from xuanming.guild.service import GuildService
from xuanming.guild.store import MemoryGuildStore
from xuanming.guild.war import (
    BATTLE_KIND_BOUNTY_HUNT,
    BATTLE_KIND_GARRISON_SIEGE,
    BATTLE_KIND_PANEL_PVP,
    BOUNTY_STATUS_COMPLETED,
    BOUNTY_STATUS_TORN_BY_TARGET,
    BOUNTY_TAG_BANNER_BREAK,
    BOUNTY_TAG_COUNTER_TEAR,
    BOUNTY_TAG_HUNT_WIN,
    BOUNTY_TAG_STREAK_END,
    BOUNTY_TIER_GUILD_PUBLIC,
    BOUNTY_TIER_NORMAL_SILVER,
    TARGET_KIND_BOUNTY,
    TARGET_KIND_LEYLINE_GARRISON,
    TARGET_KIND_LIVE_PLAYER,
    TARGET_KIND_QUICK_AUTO,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match.config import MatchConfig
from xuanming.match.gather import (
    BattleNodeEntry,
    GatherPipeline,
    MemoryPresenceProvider,
    StaticBattleNodeResolver,
)
from xuanming.match.match_service import MatchGrpcService
from xuanming.match.model import (
    BATTLE_ACTIVITY_KIND_GUILD_WAR,
    MODE_GUILD_WAR_PVP,
    MODE_GUILD_WAR_SIEGE,
    SPECTATE_VISIBILITY_PUBLIC,
    PlayerPresence,
    record_replay_for,
    spectate_visibility_for,
)
from xuanming.match.spectate import SpectateManager
from xuanming.match.store import MemoryMatchStore
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.trade.economy_credit import SELLER_RISK_TIER_RESTRICTED
from xuanming.trade.model import MANUAL_RESOLUTION_CALLER
from xuanming.trade.service import TradeService
from xuanming.trade.store import MemoryTradeStore


class _QuotaMemoryStore(BaseDataMemoryStore):
    """在既有 DataService 内存替身上叠加账号级阶段一可交易材料终身配额账本。"""

    def __init__(self, protocol, **kwargs) -> None:
        super().__init__(protocol, **kwargs)
        self.quota_ledger = Stage1TradableQuotaLedger()

    async def sync_account_stage1_tradable(self, player_id: int, desired_char_total: int) -> tuple[bool, int, int]:
        if not player_id or desired_char_total > STAGE1_TRADABLE_MATERIAL_LIFETIME_LIMIT:
            raise Invalid("stage1_tradable")
        row = self.roles.get(int(player_id))
        if row is None or not row["account"] or row["account"] not in self.accounts:
            return False, 0, 0
        confirmed, used = self.quota_ledger.sync(row["account"], int(player_id), int(desired_char_total))
        return True, confirmed, used


# ==================================================================================================
# 1. DataService.SyncAccountStage1Tradable (498)
# ==================================================================================================


def test_data_service_sync_account_stage1_tradable():
    async def scenario():
        protocol = Protocol()
        now_ms = 1_760_000_000_000
        secrets = {
            "login": b"login-secret-0123456789abcdef0123",
            "scene": b"scene-secret-0123456789abcdef0123",
        }
        store = _QuotaMemoryStore(protocol, clock_ms=lambda: now_ms)
        svc = DataService(protocol, store, secrets, zone_refresh=60.0, clock_ms=lambda: now_ms)
        await svc.start()

        async with AsyncExitStack() as stack:
            stack.push_async_callback(svc.close)
            server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
            stack.push_async_callback(server.stop, 0)
            client = GrpcClient(f"127.0.0.1:{port}")
            stack.push_async_callback(client.close)

            path = "/data_service.DataService/SyncAccountStage1Tradable"

            async def call_sync(pid: int, desired: int, *, caller: str = "scene", tamper: bool = False, sign: bool = True):
                req = protocol.message(
                    "data_service.SyncAccountStage1TradableRequest",
                    player_id=pid,
                    desired_char_total=desired,
                )
                if sign:
                    sign_service_call(path, req, caller, now_ms, secrets[caller])
                if tamper:
                    req.desired_char_total = desired + 1
                raw = await client.call(path, req.SerializeToString(deterministic=True))
                return protocol.parse("data_service.SyncAccountStage1TradableResponse", raw)

            # 1) 鉴权与参数校验
            for kwargs, expected_code in (
                ({"sign": False}, grpc.StatusCode.PERMISSION_DENIED),
                ({"caller": "login"}, grpc.StatusCode.PERMISSION_DENIED),
                ({"tamper": True}, grpc.StatusCode.PERMISSION_DENIED),
            ):
                with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                    await call_sync(1001, 5, **kwargs)
                assert exc_info.value.code() == expected_code

            for pid, desired in ((0, 1), (1001, 61)):
                with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                    await call_sync(pid, desired)
                assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

            # 2) 不存在的角色返回 found=False
            missing = await call_sync(99999, 5)
            assert not missing.found
            assert missing.confirmed_char_total == 0
            assert missing.account_used_total == 0

            # 3) 同账号两角色共享终身 60 份硬上限，且删角不重置
            await store.read_account("acct-quota", True)
            for pid in (1001, 1002, 1003):
                await store.register_zone(pid, 1, 1_000_000)
                role = protocol.message(
                    "AccountSimplePlayer", player_id=pid, class_id=1, gender=1, zone_id=1, name=f"r{pid}"
                )
                await store.create_role("acct-quota", role)

            r1 = await call_sync(1001, 40)
            assert r1.found and r1.confirmed_char_total == 40 and r1.account_used_total == 40

            # 幂等重放与只查询 (desired=0) 不重加
            r1_replay = await call_sync(1001, 40)
            assert r1_replay.confirmed_char_total == 40 and r1_replay.account_used_total == 40
            r1_query = await call_sync(1001, 0)
            assert r1_query.confirmed_char_total == 40 and r1_query.account_used_total == 40

            # 角色 2 申请 35 份 -> 被截断到剩余 20 份，账号总额封顶 60
            r2 = await call_sync(1002, 35)
            assert r2.found and r2.confirmed_char_total == 20 and r2.account_used_total == 60

            # 删除角色 1 后：角色 1 返回 found=False；角色 3 申请仍为 0 / 60（删角不重置）
            await store.remove_roles("acct-quota", [(1001, 1)])
            r1_gone = await call_sync(1001, 45)
            assert not r1_gone.found
            r3 = await call_sync(1003, 10)
            assert r3.found and r3.confirmed_char_total == 0 and r3.account_used_total == 60

            # 4) 未实现配额接口的 store 必须返回 UNAVAILABLE
            bare_svc = DataService(protocol, BaseDataMemoryStore(protocol), secrets, clock_ms=lambda: now_ms)
            await bare_svc.start()
            stack.push_async_callback(bare_svc.close)
            b_server, b_port = await serve_grpc(protocol, "127.0.0.1:0", bare_svc.services(), pass_context=True)
            stack.push_async_callback(b_server.stop, 0)
            b_client = GrpcClient(f"127.0.0.1:{b_port}")
            stack.push_async_callback(b_client.close)
            req_b = protocol.message(
                "data_service.SyncAccountStage1TradableRequest", player_id=1002, desired_char_total=1
            )
            sign_service_call(path, req_b, "scene", now_ms, secrets["scene"])
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await b_client.call(path, req_b.SerializeToString(deterministic=True))
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE

    asyncio.run(scenario())


# ==================================================================================================
# 2. TradeAdmin.RecordSellerEnvironment (496) & RecordSellerViolation (497)
# ==================================================================================================


def test_trade_admin_seller_credit_ops():
    async def scenario():
        protocol = Protocol()
        now_ms = 1_760_000_000_000
        signer = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([signer.public_key()], allowed_callers=("gate",))
        coord_secret = b"trade-coordinator-secret-01234567"
        manual_secret = b"trade-manual-admin-secret-0123456"

        async def _serve(mode: str, *, with_manual_secret: bool = False):
            store = MemoryTradeStore(protocol)
            svc = TradeService(
                protocol,
                store,
                caller_verifier=verifier,
                coordinator_secret=coord_secret,
                mode=mode,
                manual_resolution_secret=manual_secret if with_manual_secret else b"",
                manual_resolution_environment="staging" if with_manual_secret else "",
                clock_ms=lambda: now_ms,
            )
            server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
            client = GrpcClient(f"127.0.0.1:{port}")
            return server, client

        async with AsyncExitStack() as stack:
            dev_server, dev_client = await _serve("dev")
            stack.push_async_callback(dev_server.stop, 0)
            stack.push_async_callback(dev_client.close)

            env_path = "/trade.TradeAdmin/RecordSellerEnvironment"
            vio_path = "/trade.TradeAdmin/RecordSellerViolation"

            # 1) 记录常用设备与 IP
            r1_raw = await dev_client.call(
                env_path,
                protocol.message(
                    "trade.RecordSellerEnvironmentRequest",
                    seller_player_id=7010,
                    owner_account="acct-7010",
                    owner_revision=1,
                    client_ip="203.0.113.5",
                    device_id="phone-a",
                ).SerializeToString(deterministic=True),
            )
            r1 = protocol.parse("trade.RecordSellerEnvironmentResponse", r1_raw)
            assert not r1.error_message.id and not r1.env_credit_cleared

            # 2) 仅换 IP（同设备）不扣分、不清零
            r2_raw = await dev_client.call(
                env_path,
                protocol.message(
                    "trade.RecordSellerEnvironmentRequest",
                    seller_player_id=7010,
                    owner_account="acct-7010",
                    owner_revision=1,
                    client_ip="198.51.100.88",
                    device_id="phone-a",
                ).SerializeToString(deterministic=True),
            )
            r2 = protocol.parse("trade.RecordSellerEnvironmentResponse", r2_raw)
            assert not r2.error_message.id and not r2.env_credit_cleared

            # 3) 记录违约黑历史（按 case_id 幂等）
            vio_req = protocol.message(
                "trade.RecordSellerViolationRequest",
                case_id="DISPUTE-9001",
                order_id="commerce:9001",
                seller_player_id=7010,
                seller_account="acct-7010",
                seller_owner_revision=1,
                reason="verified_chargeback",
            )
            v1 = protocol.parse(
                "trade.RecordSellerViolationResponse",
                await dev_client.call(vio_path, vio_req.SerializeToString(deterministic=True)),
            )
            v1_dup = protocol.parse(
                "trade.RecordSellerViolationResponse",
                await dev_client.call(vio_path, vio_req.SerializeToString(deterministic=True)),
            )
            assert not v1.error_message.id and v1.profile.violation_count == 1
            assert v1_dup.profile.violation_count == 1

            # 4) 异地新 IP + 陌生新设备同时命中 -> 清 0 正面信用并永久保留违约黑历史
            r3 = protocol.parse(
                "trade.RecordSellerEnvironmentResponse",
                await dev_client.call(
                    env_path,
                    protocol.message(
                        "trade.RecordSellerEnvironmentRequest",
                        seller_player_id=7010,
                        owner_account="acct-7010",
                        owner_revision=1,
                        client_ip="192.0.2.99",
                        device_id="stranger-pc",
                    ).SerializeToString(deterministic=True),
                ),
            )
            assert not r3.error_message.id
            assert r3.env_credit_cleared
            assert r3.profile.credit_score == 0
            assert r3.profile.violation_count == 1
            assert r3.profile.risk_tier == SELLER_RISK_TIER_RESTRICTED

            # 5) 携带客户端会话头一律拒绝
            details = protocol.message(
                "SessionDetails", player_id=7010, authenticated_account="acct-7010", owner_revision=1
            )
            md = session_metadata(details.SerializeToString(), env_path, signer)
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await dev_client.call(
                    env_path,
                    protocol.message(
                        "trade.RecordSellerEnvironmentRequest",
                        seller_player_id=7010,
                        owner_account="acct-7010",
                        owner_revision=1,
                        client_ip="203.0.113.5",
                        device_id="phone-a",
                    ).SerializeToString(deterministic=True),
                    metadata=md,
                )
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 6) 生产模式无签名必拒；配置 manual_secret 后带合法签名放行
            prod_server, prod_client = await _serve("pro", with_manual_secret=False)
            stack.push_async_callback(prod_server.stop, 0)
            stack.push_async_callback(prod_client.close)
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await prod_client.call(
                    env_path,
                    protocol.message(
                        "trade.RecordSellerEnvironmentRequest",
                        seller_player_id=7010,
                        owner_account="acct-7010",
                        owner_revision=1,
                        client_ip="203.0.113.5",
                        device_id="phone-a",
                    ).SerializeToString(deterministic=True),
                )
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

            signed_server, signed_client = await _serve("pro", with_manual_secret=True)
            stack.push_async_callback(signed_server.stop, 0)
            stack.push_async_callback(signed_client.close)
            s_req = protocol.message(
                "trade.RecordSellerEnvironmentRequest",
                seller_player_id=7011,
                owner_account="acct-7011",
                owner_revision=1,
                client_ip="203.0.113.9",
                device_id="phone-z",
            )
            sign_service_call(env_path, s_req, MANUAL_RESOLUTION_CALLER, now_ms, manual_secret)
            s_resp = protocol.parse(
                "trade.RecordSellerEnvironmentResponse",
                await signed_client.call(env_path, s_req.SerializeToString(deterministic=True)),
            )
            assert not s_resp.error_message.id and not s_resp.env_credit_cleared

    asyncio.run(scenario())


# ==================================================================================================
# 3. MatchInternal.StartGuildWarBattle (495) + MatchMode 9/10
# ==================================================================================================


class _FakeGatherRpc:
    """记录 PrepareBattle / CreateBattle 请求供断言的最小内存端点桩。"""

    def __init__(self, protocol: Protocol, battle_uuid: str) -> None:
        self.protocol = protocol
        self.battle_uuid = battle_uuid
        self.prepared_players: list[int] = []
        self.create_requests: list = []

    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes:
        if path.endswith("/PrepareBattle"):
            req = self.protocol.parse("PrepareBattleRequest", data)
            self.prepared_players.append(int(req.player_id))
            resp = self.protocol.message("PrepareBattleResponse", table_fingerprint="fp-v1")
            resp.snapshot.player_id = int(req.player_id)
            resp.snapshot.player_name = f"hero-{int(req.player_id)}"
            resp.snapshot.level = 30
            resp.snapshot.table_fingerprint = "fp-v1"
            return resp.SerializeToString(deterministic=True)
        if path.endswith("/QueryBattleCreation"):
            resp = self.protocol.message(
                "QueryBattleCreationResponse",
                target_instance_id=self.battle_uuid,
                supported_outcome_protocol_version=1,
                rejects_new_battles=False,
            )
            return resp.SerializeToString(deterministic=True)
        if path.endswith("/CreateBattle"):
            req = self.protocol.parse("CreateBattleRequest", data)
            self.create_requests.append(req)
            resp = self.protocol.message(
                "CreateBattleResponse",
                adopted_outcome_protocol_version=1,
                room_count=len(self.create_requests),
            )
            return resp.SerializeToString(deterministic=True)
        return b""

    async def close(self) -> None:
        return None


def test_match_internal_start_guild_war_battle_pvp_and_siege():
    async def scenario():
        protocol = Protocol()
        now_ms = 1_760_000_000_000
        guild_secret = b"match-guild-internal-secret-01234"
        battle_uuid = "11111111-1111-1111-1111-111111111111"

        assert spectate_visibility_for(MODE_GUILD_WAR_PVP, True) == SPECTATE_VISIBILITY_PUBLIC
        assert spectate_visibility_for(MODE_GUILD_WAR_SIEGE, True) == SPECTATE_VISIBILITY_PUBLIC
        assert record_replay_for(MODE_GUILD_WAR_PVP) is True
        assert record_replay_for(MODE_GUILD_WAR_SIEGE) is False

        store = MemoryMatchStore(protocol, clock_ms=lambda: now_ms)
        cfg = MatchConfig(guild_secret=guild_secret, battle_deadline_sec=720)
        presence = MemoryPresenceProvider()
        for pid in (101, 102, 103, 104):
            presence.set_presence(
                PlayerPresence(
                    player_id=pid,
                    online=True,
                    in_battle=False,
                    zone_id=1,
                    scene_grpc_endpoint="fake://scene",
                    name=f"p{pid}",
                    level=30,
                )
            )
        nodes = StaticBattleNodeResolver(
            [
                BattleNodeEntry(
                    node_id=1,
                    node_uuid=battle_uuid,
                    grpc_endpoint="fake://battle",
                    physical_region=1,
                    admitting=True,
                )
            ]
        )
        rpc = _FakeGatherRpc(protocol, battle_uuid)
        gather = GatherPipeline(protocol, store, cfg, rpc, presence, nodes)
        spectate = SpectateManager(protocol, store, cfg, rpc, presence)
        svc = MatchGrpcService(
            protocol,
            store,
            cfg,
            gate_verifier=None,
            presence=presence,
            battle_nodes=nodes,
            gather=gather,
            spectate=spectate,
            inline_gather=True,
            clock_ms=lambda: now_ms,
        )

        async with AsyncExitStack() as stack:
            stack.push_async_callback(svc.close)
            server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
            stack.push_async_callback(server.stop, 0)
            client = GrpcClient(f"127.0.0.1:{port}")
            stack.push_async_callback(client.close)
            path = "/match.MatchInternal/StartGuildWarBattle"

            def make_req(mode: int, req_id: int, attackers: list[int], defenders: list[int], ai_ids: list[int] = ()):
                req = protocol.message(
                    "match.StartGuildWarBattleRequest",
                    battle_config_id=1,
                    match_mode=mode,
                    attacker_player_ids=attackers,
                    defender_player_ids=defenders,
                    request_id=req_id,
                )
                req.activity_context.kind = BATTLE_ACTIVITY_KIND_GUILD_WAR
                req.activity_context.guild_id = 5001
                req.activity_context.initiator_player_id = attackers[0]
                req.activity_context.guild_war.zone_id = 1
                req.activity_context.guild_war.attacker_guild_id = 5001
                req.activity_context.guild_war.defender_guild_id = 5002
                req.activity_context.guild_war.leyline_id = 1
                req.activity_context.guild_war.war_battle_kind = 1 if mode == MODE_GUILD_WAR_PVP else 2
                for aid in ai_ids:
                    snap = req.ai_defender_snapshots.add()
                    snap.player_id = aid
                    snap.player_name = f"guard-{aid}"
                    snap.level = 30
                    snap.team_index = 0  # 刻意传错，验证服务端强制覆盖为 team_index=1, ai_controlled=True
                    snap.ai_controlled = False
                sign_service_call(path, req, "guild", now_ms, guild_secret)
                return req

            # 1) 真人帮战直打（MODE_GUILD_WAR_PVP = 9）+ 幂等重放
            pvp_req = make_req(MODE_GUILD_WAR_PVP, 8801, [101], [102])
            r1 = protocol.parse(
                "match.StartGuildWarBattleResponse",
                await client.call(path, pvp_req.SerializeToString(deterministic=True)),
            )
            assert r1.reject == 0 and r1.battle_id > 0
            r1_dup = protocol.parse(
                "match.StartGuildWarBattleResponse",
                await client.call(path, pvp_req.SerializeToString(deterministic=True)),
            )
            assert r1_dup.reject == 0 and r1_dup.battle_id == r1.battle_id

            # 校验 CreateBattleRequest 冻结公开观战、开启回放与双侧站位
            assert len(rpc.create_requests) == 1
            c_pvp = rpc.create_requests[0]
            assert c_pvp.match_mode == MODE_GUILD_WAR_PVP
            assert c_pvp.spectate_visibility == SPECTATE_VISIBILITY_PUBLIC
            assert c_pvp.record_replay is True
            assert {int(p.player_id): int(p.team_index) for p in c_pvp.players} == {101: 0, 102: 1}

            # 2) 另一攻方 103 并发抢点已在战中的 102 -> 返回 MEMBER_IN_BATTLE + offender_battle_id 供一键观战
            r_busy = protocol.parse(
                "match.StartGuildWarBattleResponse",
                await client.call(path, make_req(MODE_GUILD_WAR_PVP, 8802, [103], [102]).SerializeToString(deterministic=True)),
            )
            assert r_busy.reject == 3
            assert r_busy.offender_player_id == 102
            assert r_busy.offender_battle_id == r1.battle_id

            # 3) 守旗踢馆（MODE_GUILD_WAR_SIEGE = 10）：不 Prepare 守旗快照主人，强制置 team_index=1 & ai_controlled=True
            rpc.prepared_players.clear()
            siege_req = make_req(MODE_GUILD_WAR_SIEGE, 8803, [104], [], ai_ids=[9001, 9002])
            r_siege = protocol.parse(
                "match.StartGuildWarBattleResponse",
                await client.call(path, siege_req.SerializeToString(deterministic=True)),
            )
            assert r_siege.reject == 0 and r_siege.battle_id > 0
            assert rpc.prepared_players == [104]  # 守旗快照拥有者 9001/9002 绝不被冻结或拉入 PrepareBattle
            c_siege = rpc.create_requests[-1]
            assert c_siege.match_mode == MODE_GUILD_WAR_SIEGE
            assert c_siege.spectate_visibility == SPECTATE_VISIBILITY_PUBLIC
            assert c_siege.record_replay is False
            ai_defs = [p for p in c_siege.players if int(p.player_id) in (9001, 9002)]
            assert len(ai_defs) == 2
            assert all(int(p.team_index) == 1 and bool(p.ai_controlled) for p in ai_defs)

    asyncio.run(scenario())


# ==================================================================================================
# 4. GuildService 全天帮战 V2 (485..494, 499) 规则与状态机完整回归
# ==================================================================================================


class _StaticGuildEnv:
    def __init__(self, protocol: Protocol, zone_id: int = 1) -> None:
        self.protocol = protocol
        self.zone_id = zone_id
        self.online: dict[int, bool] = {}

    async def get_role_fence_state(self, player_id: int):
        return self.protocol.message(
            "rolefence.RoleFenceState",
            player_id=int(player_id),
            account=f"acct-{int(player_id)}",
            owner_revision=1,
            freeze_token=0,
            version=1,
            home_zone=self.zone_id,
            freeze_final=False,
        )

    async def get_home_zone(self, player_id: int) -> int:
        return self.zone_id

    async def batch_get_names(self, player_ids):
        return {int(pid): f"侠客{int(pid)}" for pid in player_ids}

    async def batch_online(self, player_ids, *, strict: bool = False):
        return {int(pid): self.online.get(int(pid), True) for pid in player_ids}


def test_guild_war_v2_full_rules_and_invariants():
    async def scenario():
        protocol = Protocol()
        now = [1_760_000_000_000]  # 可推进虚拟时钟
        signer = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([signer.public_key()], allowed_callers=("gate",))
        env = _StaticGuildEnv(protocol, zone_id=1)
        store = MemoryGuildStore(protocol, role_directory=env)
        svc = GuildService(
            protocol,
            store,
            caller_verifier=verifier,
            coordinator_secret=b"guild-war-coordinator-secret-0123",
            home_zones=env,
            player_names=env,
            online_resolver=env,
            clock_ms=lambda: now[0],
        )

        async with AsyncExitStack() as stack:
            server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
            stack.push_async_callback(server.stop, 0)
            client = GrpcClient(f"127.0.0.1:{port}")
            stack.push_async_callback(client.close)

            async def call_guild(pid: int, method: str, req_type: str, resp_type: str, **fields):
                path = f"/guildpb.GuildService/{method}"
                details = protocol.message(
                    "SessionDetails",
                    session_id=pid * 10 + 1,
                    gate_instance_id="gate-1",
                    session_incarnation=1,
                    player_id=pid,
                    authenticated_account=f"acct-{pid}",
                    owner_revision=1,
                )
                md = session_metadata(details.SerializeToString(deterministic=True), path, signer)
                req = protocol.message(f"guildpb.{req_type}", **fields)
                raw = await client.call(path, req.SerializeToString(deterministic=True), metadata=md)
                return protocol.parse(f"guildpb.{resp_type}", raw)

            # 创建青云阁 (101, 102) 与沧澜盟 (201, 202, 203)
            g1 = await call_guild(101, "CreateGuild", "CreateGuildRequest", "CreateGuildResponse", name="青云阁")
            g2 = await call_guild(201, "CreateGuild", "CreateGuildRequest", "CreateGuildResponse", name="沧澜盟")
            assert not g1.error_message.id and not g2.error_message.id
            gid1, gid2 = int(g1.guild.guild_id), int(g2.guild.guild_id)

            for applicant, target_gid, leader in ((102, gid1, 101), (202, gid2, 201), (203, gid2, 201)):
                ap = await call_guild(
                    applicant, "ApplyJoinGuild", "ApplyJoinGuildRequest", "ApplyJoinGuildResponse", guild_id=target_gid
                )
                assert not ap.error_message.id
                rv = await call_guild(
                    leader,
                    "ReviewGuildApplication",
                    "ReviewGuildApplicationRequest",
                    "ReviewGuildApplicationResponse",
                    applicant_player_id=applicant,
                    approve=True,
                )
                assert not rv.error_message.id

            # 1) 总览读取配表：3 灵脉 + 4 档周战功里程碑 (200/600/1200/2000)
            ov = await call_guild(101, "GetGuildWarOverview", "GetGuildWarOverviewRequest", "GetGuildWarOverviewResponse")
            assert not ov.error_message.id
            assert len(ov.leylines) == 3
            assert [m.required_merit for m in ov.milestones] == [200, 600, 1200, 2000]
            assert 1 <= ov.gold_leyline_id <= 3
            assert ov.public_bounty_daily_limit == 2

            # 2) 守旗阵上阵、60s 刷新冷却与长老/帮主置顶
            gr1 = await call_guild(
                101, "RegisterGuildWarGarrison", "RegisterGuildWarGarrisonRequest", "RegisterGuildWarGarrisonResponse", action=1
            )
            assert not gr1.error_message.id and len(gr1.garrison_slots) == 1
            gr_cd = await call_guild(
                101, "RegisterGuildWarGarrison", "RegisterGuildWarGarrisonRequest", "RegisterGuildWarGarrisonResponse", action=1
            )
            assert gr_cd.error_message.id == ERR_GUILD_WAR_GARRISON_COOLDOWN
            gr2 = await call_guild(
                102, "RegisterGuildWarGarrison", "RegisterGuildWarGarrisonRequest", "RegisterGuildWarGarrisonResponse", action=1
            )
            assert not gr2.error_message.id and len(gr2.garrison_slots) == 2
            gr_top = await call_guild(
                101,
                "RegisterGuildWarGarrison",
                "RegisterGuildWarGarrisonRequest",
                "RegisterGuildWarGarrisonResponse",
                action=3,
                target_player_id=102,
            )
            assert not gr_top.error_message.id and gr_top.garrison_slots[0].player_id == 102

            # 3) 201 开启挂牌迎战；101 真人直打 + 并发抢点返回 offender_battle_id
            st201 = await call_guild(
                201, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_open_challenge=True
            )
            assert not st201.error_message.id and st201.my_stance.open_challenge is True

            # 同帮互打拒绝
            same_g = await call_guild(
                101,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LIVE_PLAYER,
                target_player_id=102,
            )
            assert same_g.error_message.id == ERR_GUILD_WAR_SAME_GUILD

            # 正常开战 + 幂等重放
            b1 = await call_guild(
                101,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LIVE_PLAYER,
                target_player_id=201,
                leyline_id=1,
                client_request_id=77001,
            )
            assert not b1.error_message.id and b1.battle_id > 0 and b1.war_battle_kind == BATTLE_KIND_PANEL_PVP
            b1_dup = await call_guild(
                101,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LIVE_PLAYER,
                target_player_id=201,
                leyline_id=1,
                client_request_id=77001,
            )
            assert b1_dup.battle_id == b1.battle_id

            # 102 抢点已在战中的 201 -> 返回 TARGET_IN_BATTLE + offender_battle_id
            b_busy = await call_guild(
                102,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LIVE_PLAYER,
                target_player_id=201,
            )
            assert b_busy.error_message.id == ERR_GUILD_WAR_TARGET_IN_BATTLE
            assert b_busy.offender_player_id == 201 and b_busy.offender_battle_id == b1.battle_id

            # 4) 结算 b1（101 胜 201；检验幂等重放、脱战 60s 切和平拦截、战后 10s 保护）
            await svc.war.settle_battle(b1.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=6, manual_rounds=4)
            await svc.war.settle_battle(b1.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=6, manual_rounds=4)
            res_win = await call_guild(
                101, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=b1.battle_id
            )
            assert res_win.settled and res_win.material_units_added == 15 and res_win.material_tradable_added == 1
            # r2-gw#1：野外活人点杀（TARGET_KIND_LIVE_PLAYER）不采信客户端传入的 leyline_id，不直接扣灵脉旗势
            assert res_win.diminish_percent == 100 and res_win.banner_hp_before == 0 and res_win.banner_hp_after == 0

            res_loss = await call_guild(
                201, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=b1.battle_id
            )
            assert res_loss.settled and res_loss.material_units_added == 10 and res_loss.can_post_bounty
            assert res_loss.bounty_candidate_player_id == 101

            # 脱战 60 秒冷却期内切和平被拒；推进 61 秒后成功切和平，和平态无法开战
            p_cd = await call_guild(
                201, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_peace_mode=True
            )
            assert p_cd.error_message.id == ERR_GUILD_WAR_PEACE_COOLDOWN

            # 战后 10s 保护期内点 201 被拒
            prot = await call_guild(
                102,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LIVE_PLAYER,
                target_player_id=201,
            )
            assert prot.error_message.id == ERR_GUILD_WAR_TARGET_PROTECTED

            now[0] += 61_000
            p_ok = await call_guild(
                201, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_peace_mode=True
            )
            assert not p_ok.error_message.id and p_ok.my_stance.peace_mode and not p_ok.my_stance.open_challenge
            self_peace = await call_guild(
                201,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_QUICK_AUTO,
            )
            assert self_peace.error_message.id == ERR_GUILD_WAR_SELF_PEACE_MODE
            # 切回战意 0 秒生效
            await call_guild(
                201, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_peace_mode=False
            )

            # 5) 同对手 30 分钟递减验证 (100% -> 50% -> 20% -> 0%)
            await call_guild(
                201, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_open_challenge=True
            )
            diminishes = []
            for idx in range(3):
                now[0] += 15_000
                bi = await call_guild(
                    101,
                    "StartGuildWarBattle",
                    "StartGuildWarBattleRequest",
                    "StartGuildWarBattleResponse",
                    target_kind=TARGET_KIND_LIVE_PLAYER,
                    target_player_id=201,
                )
                assert not bi.error_message.id
                await svc.war.settle_battle(bi.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=4, manual_rounds=3)
                ri = await call_guild(
                    101, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=bi.battle_id
                )
                diminishes.append(ri.diminish_percent)
            assert diminishes == [50, 20, 0]

            # 6) 连胜悬赏 (>=3 连胜且击败 >=2 不同对手) + 终结悬赏暴击
            await call_guild(
                202, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_open_challenge=True
            )
            await call_guild(
                203, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_open_challenge=True
            )
            for opp in (202, 203):
                now[0] += 15_000
                bs = await call_guild(
                    101,
                    "StartGuildWarBattle",
                    "StartGuildWarBattleRequest",
                    "StartGuildWarBattleResponse",
                    target_kind=TARGET_KIND_LIVE_PLAYER,
                    target_player_id=opp,
                )
                await svc.war.settle_battle(bs.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=5, manual_rounds=3)
            # 此时 101 已有 3 连胜且击败 201/202/203 (3 名不同对手)，202 击败 101 触发终结连胜悬赏
            await call_guild(
                101, "SetGuildWarStance", "SetGuildWarStanceRequest", "SetGuildWarStanceResponse", set_open_challenge=True
            )
            now[0] += 15_000
            b_end = await call_guild(
                202,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LIVE_PLAYER,
                target_player_id=101,
            )
            await svc.war.settle_battle(b_end.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=5, manual_rounds=3)
            r_end = await call_guild(
                202, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=b_end.battle_id
            )
            assert r_end.bounty_outcome_tag == BOUNTY_TAG_STREAK_END
            assert r_end.bounty_jackpot_contribution == 100 and r_end.bounty_jackpot_silver == 3000

            # 7) 灵脉占领、15 分钟护盾与「每帮限占 1 座」硬约束
            # 先把灵脉 1 旗势打空 -> 青云阁占领灵脉 1，初始旗势 600 + 15 分钟护盾
            svc.war._ensure_leyline(1, 1, now[0]).banner_hp = 35
            b_cap1 = await call_guild(
                101,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LEYLINE_GARRISON,
                leyline_id=1,
            )
            assert b_cap1.war_battle_kind == BATTLE_KIND_GARRISON_SIEGE
            await svc.war.settle_battle(b_cap1.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=4, manual_rounds=3)
            r_cap1 = await call_guild(
                101, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=b_cap1.battle_id
            )
            assert r_cap1.bounty_outcome_tag == BOUNTY_TAG_BANNER_BREAK
            ll1 = svc.war._ensure_leyline(1, 1, now[0])
            assert ll1.owner_guild_id == gid1 and ll1.banner_hp == 600 and ll1.shield_until_ms == now[0] + 900_000

            # 护盾内攻打灵脉 1 旗势不扣减
            b_sh = await call_guild(
                201,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LEYLINE_GARRISON,
                leyline_id=1,
            )
            await svc.war.settle_battle(b_sh.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=4, manual_rounds=3)
            r_sh = await call_guild(
                201, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=b_sh.battle_id
            )
            assert r_sh.banner_shielded and r_sh.banner_hp_after == 600

            # 已占领灵脉 1 的青云阁再把灵脉 2 旗势打空 -> 获破旗赏金但绝不同时占 2 座，灵脉 2 归无主
            svc.war._ensure_leyline(1, 2, now[0]).banner_hp = 35
            b_cap2 = await call_guild(
                101,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_LEYLINE_GARRISON,
                leyline_id=2,
            )
            await svc.war.settle_battle(b_cap2.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=4, manual_rounds=3)
            ll2 = svc.war._ensure_leyline(1, 2, now[0])
            assert ll2.owner_guild_id == 0 and ll2.banner_hp == 1000
            assert ll1.owner_guild_id == gid1

            # 8) 每小时灵脉分红 + 每周战功里程碑宝箱 + 换帮不重领防白嫖
            now[0] += 2 * 3_600_000 + 1000  # 推进 2 小时 -> 灵脉 1 产出 2 份分红
            svc.war._ensure_weekly(gid1, svc.war._ensure_player(102, 1, now[0]).weekly_week_key, now[0]).guild_merit = 900
            svc.war._ensure_weekly(gid2, svc.war._ensure_player(102, 1, now[0]).weekly_week_key, now[0]).guild_merit = 6500
            svc.war._ensure_player(102, 1, now[0]).weekly_merit = 50

            m1 = await call_guild(
                102, "ClaimGuildWarMilestone", "ClaimGuildWarMilestoneRequest", "ClaimGuildWarMilestoneResponse", milestone_id=1
            )
            assert not m1.error_message.id and m1.milestone.claimed
            m1_again = await call_guild(
                102, "ClaimGuildWarMilestone", "ClaimGuildWarMilestoneRequest", "ClaimGuildWarMilestoneResponse", milestone_id=1
            )
            assert m1_again.error_message.id == ERR_GUILD_WAR_MILESTONE_CLAIMED
            m3_locked = await call_guild(
                102, "ClaimGuildWarMilestone", "ClaimGuildWarMilestoneRequest", "ClaimGuildWarMilestoneResponse", milestone_id=3
            )
            assert m3_locked.error_message.id == ERR_GUILD_WAR_MILESTONE_LOCKED

            div1 = await call_guild(
                102, "ClaimGuildWarDividend", "ClaimGuildWarDividendRequest", "ClaimGuildWarDividendResponse"
            )
            assert not div1.error_message.id and div1.claimed_count == 2 and div1.dividend.claimed_this_week == 2

            # 102 退青云阁转入满档沧澜盟：守旗槽自动清除，不能重领第 1 档，但可补领第 2/3/4 档
            await call_guild(102, "LeaveGuild", "LeaveGuildRequest", "LeaveGuildResponse", expected_guild_id=gid1)
            assert all(s.player_id != 102 for s in svc.war._garrisons.get(gid1, ()))
            await call_guild(102, "ApplyJoinGuild", "ApplyJoinGuildRequest", "ApplyJoinGuildResponse", guild_id=gid2)
            await call_guild(
                201,
                "ReviewGuildApplication",
                "ReviewGuildApplicationRequest",
                "ReviewGuildApplicationResponse",
                applicant_player_id=102,
                approve=True,
            )
            m1_cross = await call_guild(
                102, "ClaimGuildWarMilestone", "ClaimGuildWarMilestoneRequest", "ClaimGuildWarMilestoneResponse", milestone_id=1
            )
            assert m1_cross.error_message.id == ERR_GUILD_WAR_MILESTONE_CLAIMED
            m4_ok = await call_guild(
                102, "ClaimGuildWarMilestone", "ClaimGuildWarMilestoneRequest", "ClaimGuildWarMilestoneResponse", milestone_id=4
            )
            assert not m4_ok.error_message.id and m4_ok.milestone.claimed

            # 9) 江湖追杀令（10% 税、单目标限 1、15s 追踪冷却、追杀销单 + 30m 保护、反杀 2 人撕单、公费每日限 2）
            bp = await call_guild(
                201,
                "PostGuildWarBounty",
                "PostGuildWarBountyRequest",
                "PostGuildWarBountyResponse",
                target_player_id=101,
                tier=BOUNTY_TIER_NORMAL_SILVER,
                public_board=True,
            )
            assert not bp.error_message.id and bp.bounty.reward_silver == 4500  # 5000 扣 10% 税
            bp_dup = await call_guild(
                202,
                "PostGuildWarBounty",
                "PostGuildWarBountyRequest",
                "PostGuildWarBountyResponse",
                target_player_id=101,
                tier=BOUNTY_TIER_NORMAL_SILVER,
                public_board=False,
            )
            assert bp_dup.error_message.id == ERR_GUILD_WAR_BOUNTY_ALREADY_ACTIVE

            tr1 = await call_guild(
                201, "TrackGuildWarBounty", "TrackGuildWarBountyRequest", "TrackGuildWarBountyResponse", bounty_id=bp.bounty.bounty_id
            )
            assert not tr1.error_message.id and tr1.target_online and tr1.scene_id > 0
            tr_cd = await call_guild(
                201, "TrackGuildWarBounty", "TrackGuildWarBountyRequest", "TrackGuildWarBountyResponse", bounty_id=bp.bounty.bounty_id
            )
            assert tr_cd.error_message.id == ERR_GUILD_WAR_TRACK_COOLDOWN

            # 101 连续反杀 2 名不同追兵 (201, 202) -> 直接撕毁追杀令并没收 50% 赏金
            for hunter in (201, 202):
                now[0] += 15_000
                bh = await call_guild(
                    hunter,
                    "StartGuildWarBattle",
                    "StartGuildWarBattleRequest",
                    "StartGuildWarBattleResponse",
                    target_kind=TARGET_KIND_BOUNTY,
                    bounty_id=bp.bounty.bounty_id,
                )
                assert not bh.error_message.id and bh.war_battle_kind == BATTLE_KIND_BOUNTY_HUNT
                await svc.war.settle_battle(bh.battle_id, outcome=BATTLE_OUTCOME_SIDE_B_WIN, rounds=5, manual_rounds=3)
            assert svc.war._bounties[bp.bounty.bounty_id].status == BOUNTY_STATUS_TORN_BY_TARGET
            r_tear = await call_guild(
                101, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=bh.battle_id
            )
            assert r_tear.bounty_outcome_tag == BOUNTY_TAG_COUNTER_TEAR
            assert r_tear.bounty_jackpot_silver == 3000 + 2250

            # 帮会公费追杀令：201 给沧澜盟充资金，挂单 101 后由 203 击杀销单，触发 30 分钟免再挂保护
            await svc.war._credit_guild_funds(gid2, 10_000)
            bp2 = await call_guild(
                201,
                "PostGuildWarBounty",
                "PostGuildWarBountyRequest",
                "PostGuildWarBountyResponse",
                target_player_id=101,
                tier=BOUNTY_TIER_GUILD_PUBLIC,
                public_board=True,
            )
            assert not bp2.error_message.id
            now[0] += 15_000
            bh_win = await call_guild(
                203,
                "StartGuildWarBattle",
                "StartGuildWarBattleRequest",
                "StartGuildWarBattleResponse",
                target_kind=TARGET_KIND_BOUNTY,
                bounty_id=bp2.bounty.bounty_id,
            )
            await svc.war.settle_battle(bh_win.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=5, manual_rounds=3)
            assert svc.war._bounties[bp2.bounty.bounty_id].status == BOUNTY_STATUS_COMPLETED
            r_hunt = await call_guild(
                203, "GetGuildWarBattleResult", "GetGuildWarBattleResultRequest", "GetGuildWarBattleResultResponse", battle_id=bh_win.battle_id
            )
            assert r_hunt.bounty_outcome_tag == BOUNTY_TAG_HUNT_WIN and r_hunt.bounty_jackpot_silver == 4500

            # 30 分钟免再挂保护期内对 101 再挂追杀令被拒
            bp_prot = await call_guild(
                201,
                "PostGuildWarBounty",
                "PostGuildWarBountyRequest",
                "PostGuildWarBountyResponse",
                target_player_id=101,
                tier=BOUNTY_TIER_NORMAL_SILVER,
            )
            assert bp_prot.error_message.id == ERR_GUILD_WAR_TARGET_PROTECTED

            # 公费每日限 2 张（此前已用 1 张；补创帮会 3 挂第 2 张成功，第 3 张被限）
            g3 = await call_guild(301, "CreateGuild", "CreateGuildRequest", "CreateGuildResponse", name="天剑门")
            assert not g3.error_message.id
            g4 = await call_guild(302, "CreateGuild", "CreateGuildRequest", "CreateGuildResponse", name="落霞谷")
            assert not g4.error_message.id
            bp_pub2 = await call_guild(
                201,
                "PostGuildWarBounty",
                "PostGuildWarBountyRequest",
                "PostGuildWarBountyResponse",
                target_player_id=301,
                tier=BOUNTY_TIER_GUILD_PUBLIC,
            )
            assert not bp_pub2.error_message.id
            bp_pub3 = await call_guild(
                201,
                "PostGuildWarBounty",
                "PostGuildWarBountyRequest",
                "PostGuildWarBountyResponse",
                target_player_id=302,
                tier=BOUNTY_TIER_GUILD_PUBLIC,
            )
            assert bp_pub3.error_message.id == ERR_GUILD_WAR_BOUNTY_LIMIT

            # 10) InspectFieldWarTargets (499)
            now[0] += 15_000
            insp = await call_guild(
                201,
                "InspectFieldWarTargets",
                "InspectFieldWarTargetsRequest",
                "InspectFieldWarTargetsResponse",
                player_ids=[101, 202, 301],
            )
            assert not insp.error_message.id
            by_pid = {int(t.player_id): t for t in insp.targets}
            assert by_pid[101].can_attack_now is True and by_pid[101].is_rival_guild is True
            assert by_pid[202].can_attack_now is False  # 同帮
            assert by_pid[301].active_bounty_id == bp_pub2.bounty.bounty_id

            # 11) NotifyGuildWarAlert (494) 拒绝客户端会话直调
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await call_guild(101, "NotifyGuildWarAlert", "GuildWarAlertS2C", "Empty", kind=1)
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

    asyncio.run(scenario())


# ==================================================================================================
# 5. Runtime("local") Gate TCP 端到端贯通全天帮战 RPC（485..493, 499）
# ==================================================================================================


async def _tcp_call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    message_id = p.message_ids[message_name]
    request = p.message(p.routes[message_id].request_type, **fields)
    return await client.rpc(message_id, call_id, request)


def test_guild_war_end_to_end_via_local_runtime_gate_tcp(tmp_path):
    async def scenario():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="war-alpha") as (client_a, player_a):
                async with _enter(runtime, account="war-beta") as (client_b, player_b):
                    # 双方各自建帮
                    ga = await _tcp_call(client_a, 100, "GuildServiceCreateGuild", name="玄冥剑宗")
                    gb = await _tcp_call(client_b, 101, "GuildServiceCreateGuild", name="太虚仙盟")
                    assert not ga.error_message.id and not gb.error_message.id

                    # 485 总览
                    ov = await _tcp_call(client_a, 102, "GuildServiceGetGuildWarOverview")
                    assert not ov.error_message.id
                    assert len(ov.leylines) == 3 and len(ov.milestones) == 4

                    # 486 姿态开关（beta 挂牌迎战）
                    st_b = await _tcp_call(client_b, 103, "GuildServiceSetGuildWarStance", set_open_challenge=True)
                    assert not st_b.error_message.id and st_b.my_stance.open_challenge is True

                    # 487 登记守旗阵
                    gar = await _tcp_call(client_a, 104, "GuildServiceRegisterGuildWarGarrison", action=1)
                    assert not gar.error_message.id and len(gar.garrison_slots) == 1

                    # 499 野外同屏目标探测
                    insp = await _tcp_call(
                        client_a, 105, "GuildServiceInspectFieldWarTargets", player_ids=[player_b]
                    )
                    assert not insp.error_message.id and len(insp.targets) == 1
                    assert insp.targets[0].can_attack_now is True

                    # 488 一键快速开战（优先直连挂牌活人 beta）
                    btl = await _tcp_call(
                        client_a,
                        106,
                        "GuildServiceStartGuildWarBattle",
                        target_kind=TARGET_KIND_QUICK_AUTO,
                        client_request_id=55001,
                    )
                    assert not btl.error_message.id and btl.battle_id > 0
                    assert btl.war_battle_kind == BATTLE_KIND_PANEL_PVP

                    # 493 未结算前查询 settled=False；结算后查询拿到权威收益
                    res_pre = await _tcp_call(
                        client_a, 107, "GuildServiceGetGuildWarBattleResult", battle_id=btl.battle_id
                    )
                    assert not res_pre.error_message.id and res_pre.settled is False

                    await runtime.local_services.guild.war.settle_battle(
                        btl.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=5, manual_rounds=3
                    )
                    res_post = await _tcp_call(
                        client_a, 108, "GuildServiceGetGuildWarBattleResult", battle_id=btl.battle_id
                    )
                    assert not res_post.error_message.id and res_post.settled is True
                    assert res_post.material_units_added == 15 and res_post.contribution_gain >= 80

                    # 491 发布追杀令 + 492 追踪行踪
                    bounty = await _tcp_call(
                        client_b,
                        109,
                        "GuildServicePostGuildWarBounty",
                        target_player_id=player_a,
                        tier=BOUNTY_TIER_NORMAL_SILVER,
                        public_board=True,
                    )
                    assert not bounty.error_message.id and bounty.bounty.reward_silver == 4500

                    track = await _tcp_call(
                        client_b, 110, "GuildServiceTrackGuildWarBounty", bounty_id=bounty.bounty.bounty_id
                    )
                    assert not track.error_message.id and track.target_online is True and track.scene_id > 0

                    # 489 领取里程碑 + 490 领取灵脉分红（达标校验通过）
                    wk = runtime.local_services.guild.war._ensure_player(
                        player_a, 1, int(time.time() * 1000)
                    ).weekly_week_key
                    runtime.local_services.guild.war._ensure_weekly(int(ga.guild.guild_id), wk, int(time.time() * 1000)).guild_merit = 500
                    runtime.local_services.guild.war._ensure_weekly(int(ga.guild.guild_id), wk, int(time.time() * 1000)).dividend_pool_count = 3
                    m_resp = await _tcp_call(client_a, 111, "GuildServiceClaimGuildWarMilestone", milestone_id=1)
                    assert not m_resp.error_message.id and m_resp.milestone.claimed is True
                    d_resp = await _tcp_call(client_a, 112, "GuildServiceClaimGuildWarDividend")
                    assert not d_resp.error_message.id and d_resp.claimed_count == 3

    asyncio.run(scenario())
