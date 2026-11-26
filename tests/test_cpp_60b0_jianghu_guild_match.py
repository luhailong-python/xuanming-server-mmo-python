"""C++ 60b0bd6b3 + 工作树增量移植全链路测试：

覆盖：
- Chat 545: ClientPlayerChat.PublishSystemWorldNotice（白名单模板、幂等去重、禁止客户端调用、世界历史可见）
- Friend 546: ClientPlayerFriend.BatchCheckSocialRelations（批量拉黑/双向好友只读核验、禁止客户端调用、零容量行副作用）
- Guild 547/567/568: GetGuildJianghuGrudgeWall（帮派生死状恩怨墙）与 IssueGuildWarFeud / RespondGuildWarFeud（2h 宿敌约战）
- Match 548..554: 天墉生死状 1V1/2V2 状态机、JianghuRule 驱动、冷却与每日上限、Gather 扣费末位顺序与上下文透传、恩怨录与高光播报
- Match 570..572: 五行试炼志 V3 好友约战帖、受控 PVE 录像分享、FIFO 10 张淘汰与 ref_count 联动、trial_vow_mask 透传
- 真实 Gate TCP（Runtime("local")）帧路由端到端冒烟
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager

import grpc
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from test_monopoly import TOKEN, _enter, _settings
from xuanming.chat.errors import ERR_INVALID_PARAMETER as CHAT_ERR_INVALID_PARAMETER
from xuanming.chat.model import (
    CHANNEL_WORLD,
    ChatRules,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_DRAW,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_GOLD,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_SILVER,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_BROKEN,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_MILESTONE,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_GOLD,
    SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_SILVER,
)
from xuanming.chat.service import ChatService
from xuanming.chat.store import MemoryChatStore
from xuanming.friend.errors import ERR_INVALID_PARAMETER as FRIEND_ERR_INVALID_PARAMETER
from xuanming.friend.model import ActorIdentity as FriendActor, FriendRules, PlayerBrief
from xuanming.friend.service import FriendService
from xuanming.friend.store import MemoryFriendStore
from xuanming.grpc_runtime import GrpcClient, serve_grpc, session_metadata
from xuanming.guild.errors import (
    ERR_GUILD_NOT_IN_GUILD,
    ERR_GUILD_RANK_TOO_LOW,
    ERR_GUILD_WAR_FEUD_ALREADY_ACTIVE,
    ERR_GUILD_WAR_FEUD_COOLDOWN,
    ERR_GUILD_WAR_FEUD_INVALID_TARGET,
    ERR_GUILD_WAR_FEUD_NOT_FOUND,
)
from xuanming.guild.model import BATTLE_OUTCOME_SIDE_A_WIN, BATTLE_OUTCOME_SIDE_B_WIN, MATCH_MODE_JIANGHU_DUEL, RoleOriginal
from xuanming.guild.service import GuildService
from xuanming.guild.store import MemoryGuildStore
from xuanming.guild.war import (
    ALERT_FEUD_CHALLENGE,
    ALERT_FEUD_ENDED,
    ALERT_FEUD_STARTED,
    FEUD_COOLDOWN_SEC,
    FEUD_DURATION_SEC,
    FEUD_STATUS_ACTIVE,
    FEUD_STATUS_DECLINED,
    FEUD_STATUS_PENDING,
    TARGET_KIND_LIVE_PLAYER,
    _diminish_percent_for_count,
)
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    BattleNodeEntry,
    GatherPipeline,
    GrpcEndpointPool,
    JianghuManager,
    MatchConfig,
    MatchGrpcService,
    Matchmaker,
    MemoryMatchStore,
    MemoryPresenceProvider,
    PlayerPresence,
    SpectateManager,
    StaticBattleNodeResolver,
    TeamGrpcService,
    TrialChallengeCardManager,
    load_jianghu_rule,
    supports_trial_vows,
)
from xuanming.match.errors import (
    ERR_JIANGHU_BLOCKED_BY_PEER,
    ERR_JIANGHU_DAILY_INVITE_LIMIT_REACHED,
    ERR_JIANGHU_GRUDGE_NOT_FOUND,
    ERR_JIANGHU_INVITE_PENDING_OR_EXPIRED,
    ERR_JIANGHU_SAME_TARGET_COOLDOWN,
    ERR_JIANGHU_TARGET_REFUSES_STRANGERS,
    ERR_JIANGHU_TEAM_CHANGED_ABORTED,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_MODE_NOT_OPEN,
    ERR_TEAM_DUNGEON_NOT_OPEN,
)
from xuanming.match.jianghu import (
    INVITE_STATE_ABORTED,
    INVITE_STATE_AWAITING_CHALLENGER_TEAM,
    INVITE_STATE_AWAITING_TARGET_TEAM,
    INVITE_STATE_DECLINED,
    INVITE_STATE_MATCHED,
    INVITE_STATE_PENDING_TARGET,
    MSG_NOTIFY_JIANGHU_DUEL_INVITE,
    MSG_NOTIFY_JIANGHU_DUEL_STATUS,
    WRIT_TIER_GOLD_ITEM,
    WRIT_TIER_SILVER,
)
from xuanming.match.model import (
    MODE_JIANGHU_DUEL,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    SPECTATE_VISIBILITY_PUBLIC,
)
from xuanming.match.trial_cards import MAX_TRIAL_CHALLENGE_CARDS_PER_PLAYER, TRIAL_CHALLENGE_CARD_TTL_MS
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime


# ── 测试辅助替身 ─────────────────────────────────────────────────────────────────────────────────────


class _AllowAllChatActors:
    async def verify_actor(self, identity) -> None:
        return None


class _RecordingWorldPusher:
    def __init__(self) -> None:
        self.messages: list = []
        self.retracts: list = []

    async def push_world(self, message) -> None:
        self.messages.append(message)

    async def push_world_retract(self, notify) -> None:
        self.retracts.append(notify)


class _StubFriendDataService:
    def __init__(self, protocol: Protocol) -> None:
        self._protocol = protocol
        self._seq = 100

    async def get_role_fence_state(self, player_id: int):
        return self._protocol.message(
            "rolefence.RoleFenceState",
            player_id=player_id,
            account=f"acc_{player_id}",
            owner_revision=1,
            freeze_token=0,
            freeze_final=False,
            version=1,
            home_zone=1,
        )

    async def allocate_request_id(self) -> int:
        self._seq += 1
        return self._seq

    async def batch_get_player_display(self, player_ids):
        return {pid: PlayerBrief(name=f"P{pid}", level=30, class_id=1) for pid in player_ids}

    async def batch_get_player_name(self, player_ids):
        return {pid: f"P{pid}" for pid in player_ids}

    async def batch_get_player_home_zone(self, player_ids):
        return {pid: 1 for pid in player_ids}


class _StubHomeZones:
    async def get_home_zone(self, player_id: int) -> int:
        return 1


class _RecordingGatherRpc:
    """捕获 PrepareBattle / QueryBattleCreation / CreateBattle 请求并回成功响应。"""

    def __init__(self, protocol: Protocol) -> None:
        self._p = protocol
        self.prepare_reqs: list = []
        self.create_reqs: list = []

    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes:
        if path.endswith("/PrepareBattle"):
            req = self._p.parse("PrepareBattleRequest", data)
            self.prepare_reqs.append(req)
            snap = self._p.message(
                "BattlePlayerSnapshot",
                player_id=req.player_id,
                player_name=f"道友{req.player_id}",
                level=45,
                class_id=1,
            )
            return self._p.message("PrepareBattleResponse", snapshot=snap, table_fingerprint="fp-v1").SerializeToString()
        if path.endswith("/QueryBattleCreation"):
            req = self._p.parse("QueryBattleCreationRequest", data)
            return self._p.message(
                "QueryBattleCreationResponse",
                target_instance_id=req.target_instance_id,
                supported_outcome_protocol_version=1,
                rejects_new_battles=False,
            ).SerializeToString()
        if path.endswith("/CreateBattle"):
            req = self._p.parse("CreateBattleRequest", data)
            self.create_reqs.append(req)
            return self._p.message(
                "CreateBattleResponse", adopted_outcome_protocol_version=1, room_count=1
            ).SerializeToString()
        if path.endswith("/CancelBattlePrepare") or path.endswith("/DestroyBattle"):
            return b""
        raise AssertionError(f"unexpected RPC path: {path}")

    async def close(self) -> None:
        return None


def _client_meta(protocol: Protocol, key: Ed25519PrivateKey, path: str, player_id: int, rev: int = 1):
    details = protocol.message(
        "SessionDetails",
        session_id=player_id * 10,
        player_id=player_id,
        gate_node_id=1,
        gate_instance_id="gate-1",
        session_incarnation=1,
        owner_revision=rev,
        authenticated_account=f"acc_{player_id}",
    )
    return session_metadata(details.SerializeToString(deterministic=True), path, key)


# ── 1. Chat 545: PublishSystemWorldNotice ───────────────────────────────────────────────────────────


def test_chat_publish_system_world_notice_templates_idempotency_and_auth():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
        now = [1_700_000_000_000]
        store = MemoryChatStore(protocol, _AllowAllChatActors(), ChatRules(), clock_ms=lambda: now[0])
        pusher = _RecordingWorldPusher()
        svc = ChatService(protocol, store, verifier, world_pusher=pusher)
        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
        path = "/chatpb.ClientPlayerChat/PublishSystemWorldNotice"
        try:
            # 1. 客户端携带会话头调用必须被 PERMISSION_DENIED 拒绝
            req_start = protocol.message(
                "chatpb.PublishSystemWorldNoticeRequest",
                notice_id="jh_duel:1001:start",
                template_id=SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_SILVER,
                battle_id=1001,
                actor_a_name="青云\n剑客",
                actor_b_name="烈火刀修",
                team_size=1,
            )
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await client.call(
                    path,
                    req_start.SerializeToString(),
                    metadata=_client_meta(protocol, key, path, 1001),
                )
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 2. 内部无会话调用成功渲染白名单模板并广播至世界频道
            raw = await client.call(path, req_start.SerializeToString())
            resp = protocol.parse("chatpb.PublishSystemWorldNoticeResponse", raw)
            assert resp.error_message.id == 0 and not resp.deduplicated
            assert len(pusher.messages) == 1
            assert pusher.messages[0].sender_player_id == 0
            assert pusher.messages[0].content == "【天墉生死擂】青云剑客 向 烈火刀修 下达江湖战书，点击观战！"

            # 3. 同 notice_id 重放命中幂等：deduplicated=True 且不二次广播
            raw_dup = await client.call(path, req_start.SerializeToString())
            resp_dup = protocol.parse("chatpb.PublishSystemWorldNoticeResponse", raw_dup)
            assert resp_dup.error_message.id == 0 and resp_dup.deduplicated
            assert len(pusher.messages) == 1

            # 4. 校验 3V3 金帖、胜负、平局、连胜与雪耻模板
            cases = [
                ("n2", SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_GOLD, "甲", "乙", 0, 0, 3, "【金帖生死状】甲 向 乙 掷出金帖生死状（3V3），名动天墉，点击观战！"),
                ("n3", SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_SILVER, "甲", "乙", 7, 0, 1, "【天墉生死擂】甲 历经 7 回合击败 乙，略胜一筹！"),
                ("n4", SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_WIN_GOLD, "甲", "乙", 9, 0, 1, "【金帖生死状】甲 历经 9 回合大破 乙，快意恩仇！"),
                ("n5", SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_DRAW, "甲", "乙", 30, 0, 1, "【天墉生死擂】甲 与 乙 大战 30 回合难分胜负，各自收剑！"),
                ("n6", SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_MILESTONE, "甲", "乙", 0, 4, 1, "【宿敌连胜】甲 再度力克宿敌 乙，豪取 4 连胜！"),
                ("n7", SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_BROKEN, "乙", "甲", 0, 4, 1, "【快意雪耻】乙 终结了宿敌 甲 的 4 连胜，扬眉吐气！"),
            ]
            for nid, tmpl, a, b, rnds, streak, tsz, expected in cases:
                now[0] += 10
                r = protocol.message(
                    "chatpb.PublishSystemWorldNoticeRequest",
                    notice_id=nid,
                    template_id=tmpl,
                    battle_id=2000,
                    actor_a_name=a,
                    actor_b_name=b,
                    total_rounds=rnds,
                    streak_count=streak,
                    team_size=tsz,
                )
                out = protocol.parse("chatpb.PublishSystemWorldNoticeResponse", await client.call(path, r.SerializeToString()))
                assert out.error_message.id == 0 and not out.deduplicated
                assert pusher.messages[-1].content == expected

            # 5. 非法参数（连胜 < 3 / team_size > 5 / 未知模板）均返回 in-band ERR_INVALID_PARAMETER
            bad_req = protocol.message(
                "chatpb.PublishSystemWorldNoticeRequest",
                notice_id="bad_streak",
                template_id=SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_MILESTONE,
                actor_a_name="甲",
                actor_b_name="乙",
                streak_count=2,
            )
            bad_resp = protocol.parse("chatpb.PublishSystemWorldNoticeResponse", await client.call(path, bad_req.SerializeToString()))
            assert bad_resp.error_message.id == CHAT_ERR_INVALID_PARAMETER

            # 6. 系统播报落入世界频道历史且可由玩家 PullChatHistory 拉到
            pull_path = "/chatpb.ClientPlayerChat/PullChatHistory"
            pull_req = protocol.message("chatpb.PullChatHistoryRequest", channel=CHANNEL_WORLD, limit=20)
            pull_raw = await client.call(
                pull_path,
                pull_req.SerializeToString(),
                metadata=_client_meta(protocol, key, pull_path, 1001),
            )
            pull_resp = protocol.parse("chatpb.PullChatHistoryResponse", pull_raw)
            assert pull_resp.error_message.id == 0
            assert any("【金帖生死状】" in m.content and m.sender_player_id == 0 for m in pull_resp.messages)
        finally:
            await client.close()
            await svc.close()
            await server.stop(0)

    asyncio.run(exercise())


# ── 2. Friend 546: BatchCheckSocialRelations ────────────────────────────────────────────────────────


def test_friend_batch_check_social_relations_and_readonly_invariant():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
        ds = _StubFriendDataService(protocol)
        store = MemoryFriendStore(protocol, ds, FriendRules())
        svc = FriendService(protocol, store, verifier, coordinator_secret=b"x" * 32)
        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)
        path = "/friendpb.ClientPlayerFriend/BatchCheckSocialRelations"
        try:
            # 1. 客户端携带会话头调用拒绝 PERMISSION_DENIED
            req0 = protocol.message(
                "friendpb.BatchCheckSocialRelationsRequest",
                side_a_player_ids=[101],
                side_b_player_ids=[201],
            )
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                await client.call(path, req0.SerializeToString(), metadata=_client_meta(protocol, key, path, 101))
            assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

            # 2. 陌生人只读检查：无拉黑、非全员好友，且绝不创建容量行
            resp0 = protocol.parse("friendpb.BatchCheckSocialRelationsResponse", await client.call(path, req0.SerializeToString()))
            assert resp0.error_message.id == 0
            assert not resp0.has_any_block and not resp0.all_are_friends
            assert len(store.mem_state.capacities) == 0

            # 3. 101 与 201 互加双向好友 -> all_are_friends=True
            a101 = FriendActor(player_id=101, account="acc_101", owner_revision=1)
            a201 = FriendActor(player_id=201, account="acc_201", owner_revision=1)
            ch = await store.create_request(a101, 201, store.rules.add_limits() if hasattr(store.rules, "add_limits") else __import__("xuanming.friend.model", fromlist=["AddFriendLimits"]).AddFriendLimits(200, 50, 50))
            await store.accept_request(a201, ch.request_id, 200)
            resp1 = protocol.parse("friendpb.BatchCheckSocialRelationsResponse", await client.call(path, req0.SerializeToString()))
            assert resp1.error_message.id == 0 and not resp1.has_any_block and resp1.all_are_friends

            # 4. 混入非好友 202 -> all_are_friends=False
            req_mix = protocol.message(
                "friendpb.BatchCheckSocialRelationsRequest",
                side_a_player_ids=[101],
                side_b_player_ids=[201, 202],
            )
            resp_mix = protocol.parse("friendpb.BatchCheckSocialRelationsResponse", await client.call(path, req_mix.SerializeToString()))
            assert not resp_mix.has_any_block and not resp_mix.all_are_friends

            # 5. 单向拉黑（202 拉黑 101）-> has_any_block=True, all_are_friends=False
            a202 = FriendActor(player_id=202, account="acc_202", owner_revision=1)
            await store.block(a202, 101, 100)
            resp_blk = protocol.parse("friendpb.BatchCheckSocialRelationsResponse", await client.call(path, req_mix.SerializeToString()))
            assert resp_blk.has_any_block and not resp_blk.all_are_friends

            # 6. 非法输入（重叠成员 / 0 ID / 超 5 人）-> in-band ERR_INVALID_PARAMETER
            for bad_a, bad_b in (([101], [101]), ([0], [201]), ([1, 2, 3, 4, 5, 6], [201]), ([], [201])):
                bad_r = protocol.message(
                    "friendpb.BatchCheckSocialRelationsRequest", side_a_player_ids=bad_a, side_b_player_ids=bad_b
                )
                out = protocol.parse("friendpb.BatchCheckSocialRelationsResponse", await client.call(path, bad_r.SerializeToString()))
                assert out.error_message.id == FRIEND_ERR_INVALID_PARAMETER
        finally:
            await client.close()
            await server.stop(0)

    asyncio.run(exercise())


# ── 3. Guild 547/567/568: 帮派生死状恩怨墙 + 2 小时宿敌约战战书 ────────────────────────────────────


def test_guild_jianghu_grudge_wall_and_war_feud_lifecycle():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
        now = [1_700_000_000_000]
        store = MemoryGuildStore(protocol, clock_ms=lambda: now[0])
        svc = GuildService(
            protocol,
            store,
            caller_verifier=verifier,
            home_zones=_StubHomeZones(),
            clock_ms=lambda: now[0],
        )
        server, port = await serve_grpc(protocol, "127.0.0.1:0", svc.services(), pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)

        async def call(player_id: int, method: str, req):
            path = f"/guildpb.GuildService/{method}"
            raw = await client.call(path, req.SerializeToString(), metadata=_client_meta(protocol, key, path, player_id))
            resp_type = protocol.pool.FindMethodByName(f"guildpb.GuildService.{method}").output_type.full_name
            return protocol.parse(resp_type, raw)

        try:
            # 建两帮：A 帮（帮主 1001、普通成员 1003），B 帮（帮主 2001）
            g_a = await call(1001, "CreateGuild", protocol.message("guildpb.CreateGuildRequest", name="青云阁"))
            g_b = await call(2001, "CreateGuild", protocol.message("guildpb.CreateGuildRequest", name="天煞盟"))
            gid_a, gid_b = int(g_a.guild.guild_id), int(g_b.guild.guild_id)
            assert gid_a > 0 and gid_b > 0

            # 1003 加入青云阁（普通成员）
            await call(1003, "ApplyJoinGuild", protocol.message("guildpb.ApplyJoinGuildRequest", guild_id=gid_a))
            await call(1001, "ReviewGuildApplication", protocol.message("guildpb.ReviewGuildApplicationRequest", applicant_player_id=1003, approve=True))

            # --- Part A: 547 GetGuildJianghuGrudgeWall ---
            # 散人 9999 被拒
            no_guild_wall = await call(9999, "GetGuildJianghuGrudgeWall", protocol.message("guildpb.GetGuildJianghuGrudgeWallRequest"))
            assert no_guild_wall.error_message.id == ERR_GUILD_NOT_IN_GUILD

            # 录入 22 场纯同帮生死状（验证幂等 + 20 条上限裁剪）
            for idx in range(1, 23):
                ev = protocol.message(
                    "contracts.kafka.BattleResultEvent",
                    battle_id=50_000 + idx,
                    match_mode=MATCH_MODE_JIANGHU_DUEL,
                    outcome=BATTLE_OUTCOME_SIDE_A_WIN if idx % 2 == 1 else BATTLE_OUTCOME_SIDE_B_WIN,
                    total_rounds=6,
                    finished_at_ms=now[0] + idx * 1000,
                    jianghu_duel_context=protocol.message(
                        "JianghuDuelBattleContext",
                        duel_invite_id=idx,
                        writ_tier=WRIT_TIER_GOLD_ITEM if idx == 22 else WRIT_TIER_SILVER,
                        challenger_captain_id=1001,
                        challenger_captain_name="青云子",
                        target_captain_id=2001,
                        target_captain_name="煞星",
                        side_a_guild_id=gid_a,
                        side_a_guild_name="青云阁",
                        side_b_guild_id=gid_b,
                        side_b_guild_name="天煞盟",
                        team_size=3,
                    ),
                )
                assert await svc.handle_battle_result_event(ev) is True
                # 幂等重放返回 False
                assert await svc.handle_battle_result_event(ev) is False

            wall_a = await call(1003, "GetGuildJianghuGrudgeWall", protocol.message("guildpb.GetGuildJianghuGrudgeWallRequest"))
            assert wall_a.error_message.id == 0 and wall_a.guild_id == gid_a
            assert len(wall_a.rivals) == 1
            assert wall_a.rivals[0].rival_guild_id == gid_b
            assert wall_a.rivals[0].wins == 11 and wall_a.rivals[0].losses == 11
            assert len(wall_a.recent_records) == 20
            assert wall_a.recent_records[0].battle_id == 50_022 and wall_a.recent_records[0].writ_tier == WRIT_TIER_GOLD_ITEM

            # --- Part B: 567/568 IssueGuildWarFeud & RespondGuildWarFeud ---
            # 普通成员 1003 发起战书 -> ERR_GUILD_RANK_TOO_LOW
            r_low = await call(1003, "IssueGuildWarFeud", protocol.message("guildpb.IssueGuildWarFeudRequest", target_guild_id=gid_b))
            assert r_low.error_message.id == ERR_GUILD_RANK_TOO_LOW

            # 对自己发起 -> ERR_GUILD_WAR_FEUD_INVALID_TARGET
            r_self = await call(1001, "IssueGuildWarFeud", protocol.message("guildpb.IssueGuildWarFeudRequest", target_guild_id=gid_a))
            assert r_self.error_message.id == ERR_GUILD_WAR_FEUD_INVALID_TARGET

            # 青云阁帮主 1001 向天煞盟发起约战战书 -> PENDING + ALERT_FEUD_CHALLENGE
            issued = await call(1001, "IssueGuildWarFeud", protocol.message("guildpb.IssueGuildWarFeudRequest", target_guild_id=gid_b))
            assert issued.error_message.id == 0
            assert issued.feud.status == FEUD_STATUS_PENDING
            assert issued.next_issue_at_ms == now[0] + FEUD_COOLDOWN_SEC * 1000
            assert any(int(a.kind) == ALERT_FEUD_CHALLENGE for a in svc.war.alerts)

            # 已有在途战书不能重复发 -> ERR_GUILD_WAR_FEUD_ALREADY_ACTIVE
            dup_issue = await call(1001, "IssueGuildWarFeud", protocol.message("guildpb.IssueGuildWarFeudRequest", target_guild_id=gid_b))
            assert dup_issue.error_message.id == ERR_GUILD_WAR_FEUD_ALREADY_ACTIVE

            # 发起方不能自己 accept -> ERR_GUILD_WAR_FEUD_NOT_FOUND
            self_acc = await call(1001, "RespondGuildWarFeud", protocol.message("guildpb.RespondGuildWarFeudRequest", feud_id=issued.feud.feud_id, accept=True))
            assert self_acc.error_message.id == ERR_GUILD_WAR_FEUD_NOT_FOUND

            # 天煞盟帮主 2001 应战 -> ACTIVE + ALERT_FEUD_STARTED
            accepted = await call(2001, "RespondGuildWarFeud", protocol.message("guildpb.RespondGuildWarFeudRequest", feud_id=issued.feud.feud_id, accept=True))
            assert accepted.error_message.id == 0 and accepted.feud.status == FEUD_STATUS_ACTIVE
            assert any(int(a.kind) == ALERT_FEUD_STARTED for a in svc.war.alerts)

            # 约战递减曲线验证：前 3 场 100%，第 4 场 20%，第 5 场 0%
            assert [_diminish_percent_for_count(c, feud_active=True) for c in range(5)] == [100, 100, 100, 20, 0]

            # 在 2 小时约战窗口内打一场真人帮战 -> feud_bonus=True + 计入比分
            await call(1001, "SetGuildWarStance", protocol.message("guildpb.SetGuildWarStanceRequest", set_peace_mode=False, set_open_challenge=True))
            await call(2001, "SetGuildWarStance", protocol.message("guildpb.SetGuildWarStanceRequest", set_peace_mode=False, set_open_challenge=True))
            started = await call(
                1001,
                "StartGuildWarBattle",
                protocol.message("guildpb.StartGuildWarBattleRequest", target_kind=TARGET_KIND_LIVE_PLAYER, target_player_id=2001),
            )
            assert started.error_message.id == 0 and started.battle_id > 0
            settled = await svc.war.settle_battle(started.battle_id, outcome=BATTLE_OUTCOME_SIDE_A_WIN, rounds=5)
            assert settled is not None and settled.feud_bonus is True
            res_view = await call(1001, "GetGuildWarBattleResult", protocol.message("guildpb.GetGuildWarBattleResultRequest", battle_id=started.battle_id))
            assert res_view.settled and res_view.feud_bonus is True

            ov_active = await call(1001, "GetGuildWarOverview", protocol.message("guildpb.GetGuildWarOverviewRequest"))
            assert ov_active.active_feud.feud_id == issued.feud.feud_id and ov_active.active_feud.issuer_wins == 1

            # 快进超过 2 小时 -> GetGuildWarOverview 惰性收官 + ALERT_FEUD_ENDED + last_feud_summary
            now[0] += FEUD_DURATION_SEC * 1000 + 1000
            ov_done = await call(1001, "GetGuildWarOverview", protocol.message("guildpb.GetGuildWarOverviewRequest"))
            assert ov_done.active_feud.feud_id == 0
            assert "青云阁 1:0 胜 天煞盟" in ov_done.last_feud_summary
            assert any(int(a.kind) == ALERT_FEUD_ENDED for a in svc.war.alerts)

            # 24 小时冷却内再次发起 -> ERR_GUILD_WAR_FEUD_COOLDOWN
            cd_resp = await call(1001, "IssueGuildWarFeud", protocol.message("guildpb.IssueGuildWarFeudRequest", target_guild_id=gid_b))
            assert cd_resp.error_message.id == ERR_GUILD_WAR_FEUD_COOLDOWN

            # 超过 24 小时冷却后发起并撤回 -> DECLINED
            now[0] += FEUD_COOLDOWN_SEC * 1000 + 1000
            issued2 = await call(1001, "IssueGuildWarFeud", protocol.message("guildpb.IssueGuildWarFeudRequest", target_guild_id=gid_b))
            assert issued2.error_message.id == 0
            declined = await call(1001, "RespondGuildWarFeud", protocol.message("guildpb.RespondGuildWarFeudRequest", feud_id=issued2.feud.feud_id, accept=False))
            assert declined.error_message.id == 0 and declined.feud.status == FEUD_STATUS_DECLINED
        finally:
            await client.close()
            await server.stop(0)

    asyncio.run(exercise())


# ── 4. Match 548..554: 江湖生死状 1V1/2V2、Gather 透传与恩怨录 ───────────────────────────────────────


def test_match_jianghu_duel_1v1_2v2_gather_and_grudges():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
        now = [1_700_000_000_000]

        # 确认从真实 jianghurule.json 加载
        rule = load_jianghu_rule()
        assert rule.enabled is True
        assert rule.silver_writ_cost == 500
        assert rule.gold_writ_item_id == 3415
        assert rule.invite_ttl_ms == 60_000
        assert rule.team_confirm_ms == 30_000
        assert rule.same_target_cooldown_ms == 300_000
        assert rule.daily_unmatched_limit == 20
        assert rule.duel_round_limit == 30
        assert rule.streak_highlight_threshold == 3

        store = MemoryMatchStore(protocol, clock_ms=lambda: now[0])
        config = MatchConfig(require_network_probe=False, pve_team_sizes={101: 5})
        presence = MemoryPresenceProvider()
        for pid, name in ((1001, "青云子"), (1002, "清风"), (2001, "煞星"), (2002, "黑煞")):
            presence.set_presence(
                PlayerPresence(
                    player_id=pid,
                    online=True,
                    in_battle=False,
                    zone_id=1,
                    scene_grpc_endpoint="scene-1:9001",
                    name=name,
                )
            )
        nodes = StaticBattleNodeResolver(
            [BattleNodeEntry(node_id=10, node_uuid="battle-uuid-1", grpc_endpoint="battle-1:9100", physical_region=1)]
        )
        rpc = _RecordingGatherRpc(protocol)
        gather = GatherPipeline(protocol, store, config, rpc, presence, nodes)
        spectate = SpectateManager(protocol, store, config, rpc, presence)

        social_state = {"block": False, "all_friends": False}
        refuse_strangers: set[int] = set()
        notices: list = []

        async def _social(side_a, side_b):
            return social_state["block"], social_state["all_friends"]

        async def _eligibility(side_a, side_b, payer_id, tier, r, all_friends):
            for pid in side_b:
                if pid in refuse_strangers and not all_friends:
                    return ERR_JIANGHU_TARGET_REFUSES_STRANGERS, "对方开启了闭关免战"
            return 0, ""

        async def _guild_lookup(pid: int):
            return (10, "青云阁") if pid < 2000 else (20, "天煞盟")

        async def _publish_notice(req):
            notices.append(req)

        jianghu = JianghuManager(
            protocol,
            store,
            presence,
            rule=rule,
            social_checker=_social,
            eligibility_checker=_eligibility,
            guild_resolver=_guild_lookup,
            world_notice_publisher=_publish_notice,
            clock_ms=lambda: now[0],
        )
        team_svc = TeamGrpcService(
            protocol, store, config, gate_verifier=verifier, presence=presence, battle_nodes=nodes, gather=gather, inline_gather=True, clock_ms=lambda: now[0]
        )
        match_svc = MatchGrpcService(
            protocol, store, config, gate_verifier=verifier, presence=presence, battle_nodes=nodes, gather=gather, spectate=spectate, jianghu=jianghu, inline_gather=True, clock_ms=lambda: now[0]
        )
        merged = dict(match_svc.services())
        merged.update(team_svc.services())
        server, port = await serve_grpc(protocol, "127.0.0.1:0", merged, pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)

        async def call_match(player_id: int, method: str, req, *, rev: int = 1):
            path = f"/match.MatchService/{method}"
            raw = await client.call(path, req.SerializeToString(), metadata=_client_meta(protocol, key, path, player_id, rev=rev))
            resp_type = protocol.pool.FindMethodByName(f"match.MatchService.{method}").output_type.full_name
            return protocol.parse(resp_type, raw)

        async def call_team(player_id: int, method: str, req):
            path = f"/teampb.ClientPlayerTeam/{method}"
            raw = await client.call(path, req.SerializeToString(), metadata=_client_meta(protocol, key, path, player_id))
            resp_type = protocol.pool.FindMethodByName(f"teampb.ClientPlayerTeam.{method}").output_type.full_name
            return protocol.parse(resp_type, raw)

        try:
            # 1. 拉黑与闭关免战校验（全员好友可绕过闭关免战）
            social_state["block"] = True
            r_blk = await call_match(1001, "CreateJianghuDuelInvite", protocol.message("match.CreateJianghuDuelInviteRequest", target_player_id=2001, writ_tier=WRIT_TIER_SILVER))
            assert r_blk.error_message.id == ERR_JIANGHU_BLOCKED_BY_PEER
            social_state["block"] = False

            refuse_strangers.add(2001)
            r_ref = await call_match(1001, "CreateJianghuDuelInvite", protocol.message("match.CreateJianghuDuelInviteRequest", target_player_id=2001, writ_tier=WRIT_TIER_SILVER))
            assert r_ref.error_message.id == ERR_JIANGHU_TARGET_REFUSES_STRANGERS

            social_state["all_friends"] = True
            # 2. 1V1 金帖生死状全链路：Create -> Respond(accept=True) -> Gather(末位备战发起队长 + 扣费参数 + 开战播报)
            inv1 = await call_match(1001, "CreateJianghuDuelInvite", protocol.message("match.CreateJianghuDuelInviteRequest", target_player_id=2001, writ_tier=WRIT_TIER_GOLD_ITEM))
            assert inv1.error_message.id == 0
            assert inv1.invite.state == INVITE_STATE_PENDING_TARGET
            assert inv1.invite.side_a_guild_id == 10 and inv1.invite.side_b_guild_id == 20
            assert any(pid == 2001 and mid == MSG_NOTIFY_JIANGHU_DUEL_INVITE for pid, mid, _ in jianghu.pushed_events)

            # 同目标 5 分钟冷却生效
            cd_hit = await call_match(1001, "CreateJianghuDuelInvite", protocol.message("match.CreateJianghuDuelInviteRequest", target_player_id=2001, writ_tier=WRIT_TIER_SILVER))
            assert cd_hit.error_message.id == ERR_JIANGHU_SAME_TARGET_COOLDOWN

            resp1 = await call_match(2001, "RespondJianghuDuelInvite", protocol.message("match.RespondJianghuDuelInviteRequest", duel_invite_id=inv1.invite.duel_invite_id, accept=True))
            assert resp1.error_message.id == 0 and resp1.invite.state == INVITE_STATE_MATCHED and resp1.invite.battle_id > 0

            # 验证 Gather 备战顺序：非发起队长(2001)先备战，发起队长(1001)最后备战且携带 jianghu_duel_fee
            assert [int(r.player_id) for r in rpc.prepare_reqs[-2:]] == [2001, 1001]
            assert int(rpc.prepare_reqs[-1].jianghu_duel_fee.duel_invite_id) == int(inv1.invite.duel_invite_id)
            assert int(rpc.prepare_reqs[-1].jianghu_duel_fee.item_config_id) == 3415
            # CreateBattleRequest 透传 jianghu_duel_context、公开观战、开启回放录制
            c_req = rpc.create_reqs[-1]
            assert int(c_req.match_mode) == MODE_JIANGHU_DUEL
            assert int(c_req.spectate_visibility) == SPECTATE_VISIBILITY_PUBLIC
            assert c_req.record_replay is True
            assert int(c_req.jianghu_duel_context.round_limit_override) == 30
            assert int(c_req.jianghu_duel_context.side_a_guild_id) == 10 and int(c_req.jianghu_duel_context.side_b_guild_id) == 20
            # 开战全服系统播报已触发
            assert any(n.notice_id == f"jh_duel:{resp1.invite.battle_id}:start" and int(n.template_id) == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_START_GOLD for n in notices)

            # 3. 2V2 完整双方确认流程（发起方确认 -> PENDING_TARGET -> 目标队长应战 -> 目标队确认 -> MATCHED）
            now[0] += 400_000  # 越过同目标 5 分钟冷却
            t_a = await call_team(1001, "CreateTeam", protocol.message("teampb.CreateTeamRequest", request_id=str(uuid.uuid4())))
            await call_team(1002, "ApplyJoinTeam", protocol.message("teampb.ApplyJoinTeamRequest", target_player_id=1001, request_id=str(uuid.uuid4())))
            await call_team(1001, "HandleApplication", protocol.message("teampb.HandleApplicationRequest", applicant_id=1002, approve=True, expected_team_id=t_a.team.team_id, request_id=str(uuid.uuid4())))
            t_b = await call_team(2001, "CreateTeam", protocol.message("teampb.CreateTeamRequest", request_id=str(uuid.uuid4())))
            await call_team(2002, "ApplyJoinTeam", protocol.message("teampb.ApplyJoinTeamRequest", target_player_id=2001, request_id=str(uuid.uuid4())))
            await call_team(2001, "HandleApplication", protocol.message("teampb.HandleApplicationRequest", applicant_id=2002, approve=True, expected_team_id=t_b.team.team_id, request_id=str(uuid.uuid4())))

            inv2 = await call_match(1001, "CreateJianghuDuelInvite", protocol.message("match.CreateJianghuDuelInviteRequest", target_player_id=2001, writ_tier=WRIT_TIER_SILVER))
            assert inv2.error_message.id == 0 and inv2.invite.state == INVITE_STATE_AWAITING_CHALLENGER_TEAM and inv2.invite.team_size == 2

            # ListJianghuGrudges 应包含在途战书供重连恢复
            g_pending = await call_match(1002, "ListJianghuGrudges", protocol.message("match.ListJianghuGrudgesRequest"))
            assert any(int(iv.duel_invite_id) == int(inv2.invite.duel_invite_id) for iv in g_pending.active_invites)

            c1 = await call_match(1002, "ConfirmJianghuDuelMember", protocol.message("match.ConfirmJianghuDuelMemberRequest", duel_invite_id=inv2.invite.duel_invite_id, agree=True))
            assert c1.error_message.id == 0 and c1.invite.state == INVITE_STATE_PENDING_TARGET

            r2 = await call_match(2001, "RespondJianghuDuelInvite", protocol.message("match.RespondJianghuDuelInviteRequest", duel_invite_id=inv2.invite.duel_invite_id, accept=True))
            assert r2.error_message.id == 0 and r2.invite.state == INVITE_STATE_AWAITING_TARGET_TEAM

            c2 = await call_match(2002, "ConfirmJianghuDuelMember", protocol.message("match.ConfirmJianghuDuelMemberRequest", duel_invite_id=inv2.invite.duel_invite_id, agree=True))
            assert c2.error_message.id == 0 and c2.invite.state == INVITE_STATE_MATCHED and c2.invite.battle_id > 0
            assert any(mid == MSG_NOTIFY_JIANGHU_DUEL_STATUS and msg.reason_code == "matched" for _, mid, msg in jianghu.pushed_events)

            # 4. 终局恩怨入账：3 连胜触发 STREAK_MILESTONE，第 4 局翻盘触发 STREAK_BROKEN，一笔勾销后再交手自动显现
            duel_ctx = c_req.jianghu_duel_context
            for seq, outcome in ((1, 1), (2, 1), (3, 1), (4, 2)):
                ev = protocol.message(
                    "contracts.kafka.BattleResultEvent",
                    battle_id=80_000 + seq,
                    match_mode=MODE_JIANGHU_DUEL,
                    outcome=outcome,
                    total_rounds=5 + seq,
                    finished_at_ms=now[0] + seq * 1000,
                    jianghu_duel_context=duel_ctx,
                )
                summary, applied = await jianghu.apply_battle_result(ev, owner_revs={1001: 1, 2001: 1})
                assert applied is True
                if seq == 3:
                    assert summary.winner_new_win_streak == 3
                if seq == 4:
                    assert summary.loser_prev_win_streak == 3

            assert any(int(n.template_id) == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_MILESTONE and int(n.streak_count) == 3 for n in notices)
            assert any(int(n.template_id) == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_STREAK_BROKEN and int(n.streak_count) == 3 for n in notices)

            lst = await call_match(1001, "ListJianghuGrudges", protocol.message("match.ListJianghuGrudgesRequest"))
            assert len(lst.grudges) == 1
            assert lst.grudges[0].wins == 3 and lst.grudges[0].losses == 1 and lst.grudges[0].peer_online is True
            assert len(lst.grudges[0].recent_logs) == 4

            # 一笔勾销后不可见；重复勾销返回 ERR_JIANGHU_GRUDGE_NOT_FOUND
            d1 = await call_match(1001, "DismissJianghuGrudge", protocol.message("match.DismissJianghuGrudgeRequest", peer_player_id=2001))
            assert d1.error_message.id == 0
            assert len((await call_match(1001, "ListJianghuGrudges", protocol.message("match.ListJianghuGrudgesRequest"))).grudges) == 0
            d2 = await call_match(1001, "DismissJianghuGrudge", protocol.message("match.DismissJianghuGrudgeRequest", peer_player_id=2001))
            assert d2.error_message.id == ERR_JIANGHU_GRUDGE_NOT_FOUND

            # 再次交手后恩怨重新显现
            ev5 = protocol.message(
                "contracts.kafka.BattleResultEvent",
                battle_id=80_005,
                match_mode=MODE_JIANGHU_DUEL,
                outcome=3,
                total_rounds=30,
                finished_at_ms=now[0] + 9000,
                jianghu_duel_context=duel_ctx,
            )
            _, applied5 = await jianghu.apply_battle_result(ev5, owner_revs={1001: 1, 2001: 1})
            assert applied5 is True
            lst2 = await call_match(1001, "ListJianghuGrudges", protocol.message("match.ListJianghuGrudgesRequest"))
            assert len(lst2.grudges) == 1 and lst2.grudges[0].draws == 1
            assert any(int(n.template_id) == SYSTEM_WORLD_NOTICE_JIANGHU_DUEL_DRAW for n in notices)

            # 过户换人（owner_revision=2）隔离旧恩怨
            await store.apply_role_fence(
                protocol.message("rolefence.RoleFenceState", player_id=1001, account="acc_1001", owner_revision=2, version=2, home_zone=1)
            )
            lst_rev2 = await call_match(1001, "ListJianghuGrudges", protocol.message("match.ListJianghuGrudgesRequest"), rev=2)
            assert len(lst_rev2.grudges) == 0
        finally:
            await client.close()
            await match_svc.close()
            await team_svc.close()
            await server.stop(0)

    asyncio.run(exercise())


# ── 5. Match 570..572: 试炼志 V3 好友约战帖、受控录像分享与 trial_vow_mask 透传 ────────────────────


def test_match_trial_challenge_cards_and_vow_mask_propagation():
    async def exercise():
        protocol = Protocol()
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()], allowed_callers=("gate",))
        now = [1_700_000_000_000]
        assert supports_trial_vows(101) is True

        store = MemoryMatchStore(protocol, clock_ms=lambda: now[0])
        config = MatchConfig(require_network_probe=False, pve_team_sizes={101: 5})
        presence = MemoryPresenceProvider()
        for pid, name in ((1001, "试炼先锋"), (1002, "好友观摩")):
            presence.set_presence(
                PlayerPresence(player_id=pid, online=True, zone_id=1, scene_grpc_endpoint="scene-1:9001", name=name)
            )
        nodes = StaticBattleNodeResolver(
            [BattleNodeEntry(node_id=10, node_uuid="battle-uuid-1", grpc_endpoint="battle-1:9100", physical_region=1)]
        )
        rpc = _RecordingGatherRpc(protocol)
        gather = GatherPipeline(protocol, store, config, rpc, presence, nodes)
        spectate = SpectateManager(protocol, store, config, rpc, presence)
        team_svc = TeamGrpcService(
            protocol, store, config, gate_verifier=verifier, presence=presence, battle_nodes=nodes, gather=gather, inline_gather=True, clock_ms=lambda: now[0]
        )
        match_svc = MatchGrpcService(
            protocol, store, config, gate_verifier=verifier, presence=presence, battle_nodes=nodes, gather=gather, spectate=spectate, inline_gather=True, clock_ms=lambda: now[0]
        )
        merged = dict(match_svc.services())
        merged.update(team_svc.services())
        server, port = await serve_grpc(protocol, "127.0.0.1:0", merged, pass_context=True)
        client = GrpcClient(f"127.0.0.1:{port}", timeout=5.0)

        async def call_match(player_id: int, method: str, req):
            path = f"/match.MatchService/{method}"
            raw = await client.call(path, req.SerializeToString(), metadata=_client_meta(protocol, key, path, player_id))
            resp_type = protocol.pool.FindMethodByName(f"match.MatchService.{method}").output_type.full_name
            return protocol.parse(resp_type, raw)

        async def call_team(player_id: int, method: str, req):
            path = f"/teampb.ClientPlayerTeam/{method}"
            raw = await client.call(path, req.SerializeToString(), metadata=_client_meta(protocol, key, path, player_id))
            resp_type = protocol.pool.FindMethodByName(f"teampb.ClientPlayerTeam.{method}").output_type.full_name
            return protocol.parse(resp_type, raw)

        try:
            # 1. JoinQueue 校验 trial_vow_mask：非法位(0x10) 与 爬塔互斥 拒绝；合法位(0x05) 透传至 CreateBattleRequest 且开启回放录制
            bad_vow = await call_match(
                1001,
                "JoinQueue",
                protocol.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=101, request_id=str(uuid.uuid4()), trial_vow_mask=0x10),
            )
            assert bad_vow.error_code == ERR_MATCH_MODE_NOT_OPEN

            bad_tower = await call_match(
                1001,
                "JoinQueue",
                protocol.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=101, request_id=str(uuid.uuid4()), is_wuxing_tower=True, trial_vow_mask=0x03),
            )
            assert bad_tower.error_code == ERR_MATCH_MODE_NOT_OPEN

            ok_jq = await call_match(
                1001,
                "JoinQueue",
                protocol.message("match.JoinQueueRequest", mode=MODE_PVE_SOLO, battle_config_id=101, request_id=str(uuid.uuid4()), trial_vow_mask=0x05),
            )
            assert ok_jq.error_code == 0
            mm = Matchmaker(protocol, store, config, gather, nodes)
            formed = await mm.run_once()
            assert len(formed) == 1
            solo_bid = formed[0]
            assert rpc.create_reqs[-1].record_replay is True
            assert int(rpc.create_reqs[-1].trial_context.journal_entry_id) == 101
            assert int(rpc.create_reqs[-1].trial_context.challenge_vow_mask) == 0x05

            # 2. StartTeamMatch 透传 trial_vow_mask（拒绝非法掩码，放行整队直开合法掩码 0x0A）
            t = await call_team(1002, "CreateTeam", protocol.message("teampb.CreateTeamRequest", request_id=str(uuid.uuid4())))
            bad_tm = await call_team(
                1002,
                "StartTeamMatch",
                protocol.message(
                    "teampb.StartTeamMatchRequest",
                    expected_team_id=t.team.team_id,
                    battle_config_id=101,
                    match_mode=MODE_PVE_TEAM,
                    target_team_size=1,
                    request_id=str(uuid.uuid4()),
                    trial_vow_mask=0x20,
                ),
            )
            assert bad_tm.error_message.id == ERR_TEAM_DUNGEON_NOT_OPEN

            ok_tm = await call_team(
                1002,
                "StartTeamMatch",
                protocol.message(
                    "teampb.StartTeamMatchRequest",
                    expected_team_id=t.team.team_id,
                    battle_config_id=101,
                    match_mode=MODE_PVE_TEAM,
                    target_team_size=1,
                    request_id=str(uuid.uuid4()),
                    trial_vow_mask=0x0A,
                ),
            )
            assert ok_tm.error_message.id == 0
            assert int(rpc.create_reqs[-1].trial_context.challenge_vow_mask) == 0x0A
            assert rpc.create_reqs[-1].record_replay is True

            # 3. 写入 11 场试炼志 PVE 胜局与录像，验证 570/571/572（幂等复用、好友受控播放、GetBattleReport 隐私边界不变、10 张 FIFO 淘汰与过期失效）
            async def _seed_trial_win(bid: int, *, vow: int = 0x05):
                f = protocol.message(
                    "match.MatchFormationRecord",
                    battle_id=bid,
                    operation_id=f"op-{bid}",
                    state=4,
                    mode=MODE_PVE_SOLO,
                    config_id=101,
                    created_ms=now[0],
                    deadline_ms=now[0] + 60_000,
                    target_instance_id="battle-uuid-1",
                    preflight_instance_id="battle-uuid-1",
                    preflight_supported_version=1,
                    outcome_protocol_version=1,
                    adopted_outcome_observed=True,
                    adopted_outcome_protocol_version=1,
                )
                f.members.add(player_id=1001, ticket=f"tk-{bid}", team_index=0, captain=True)
                creq = protocol.message(
                    "CreateBattleRequest",
                    battle_id=bid,
                    battle_config_id=101,
                    match_mode=MODE_PVE_SOLO,
                    created_at_ms=now[0],
                    deadline_ms=now[0] + 60_000,
                    placement_operation_id=f"op-{bid}",
                    target_instance_id="battle-uuid-1",
                    outcome_protocol_version=1,
                    record_replay=True,
                )
                creq.players.add(player_id=1001, player_name="试炼先锋", team_index=0, level=45, class_id=1)
                f.create_request = creq.SerializeToString(deterministic=True)
                store._state.formations[bid] = f
                store._state.tickets[1001] = protocol.message(
                    "match.MatchAuthorityTicketRecord",
                    ticket=f"tk-{bid}",
                    player_id=1001,
                    mode=MODE_PVE_SOLO,
                    config_id=101,
                    state=3,
                    formation_id=bid,
                    owner_account="acc-1001",
                    owner_revision=1,
                )

                ev = protocol.message(
                    "contracts.kafka.BattleResultEvent",
                    battle_id=bid,
                    battle_config_id=101,
                    match_mode=MODE_PVE_SOLO,
                    outcome=1,
                    total_rounds=6,
                    finished_at_ms=now[0] + 1000,
                    settlement_format_version=1,
                )
                ev.teams.add(team_index=0, player_ids=[1001])
                s = ev.settlements.add(
                    player_id=1001,
                    battle_id=bid,
                    outcome=1,
                    player_team_index=0,
                    total_rounds=6,
                )
                s.trial_facts.journal_entry_id = 101
                s.trial_facts.trial_rule_version = 1
                s.trial_facts.initial_human_count = 1
                s.trial_facts.challenge_vow_mask = vow
                s.trial_facts.used_pet_table_id = 203
                s.trial_facts.used_artifact_effect_ids.extend([402, 401])
                s.trial_facts.used_season_seal_id = 501
                env = protocol.message("BattleReplayEnvelope", format_version=1, codec=1, payload=f"replay-{bid}".encode())
                commit_req = protocol.message(
                    "match.CommitBattleOutcomeRequest",
                    battle_id=bid,
                    placement_operation_id=f"op-{bid}",
                    target_instance_id="battle-uuid-1",
                    raw_result_payload=ev.SerializeToString(deterministic=True),
                    replay=env,
                )
                await store.commit_battle_outcome(commit_req)

            for i in range(1, 12):
                now[0] += 1000
                await _seed_trial_win(90_000 + i)

            # 570 首次签发 + 幂等复用
            c1 = await call_match(1001, "CreateTrialChallengeCard", protocol.message("match.CreateTrialChallengeCardRequest", battle_id=90_001))
            assert c1.error_message.id == 0
            assert c1.card.owner_player_id == 1001 and c1.card.owner_name == "试炼先锋"
            assert c1.card.journal_entry_id == 101 and c1.card.vow_mask == 0x05 and c1.card.has_replay is True
            assert list(c1.card.artifact_effect_ids) == [401, 402]
            ref_after_first = store._state.reports[90_001].ref_count

            c1_dup = await call_match(1001, "CreateTrialChallengeCard", protocol.message("match.CreateTrialChallengeCardRequest", battle_id=90_001))
            assert c1_dup.card.card_id == c1.card.card_id
            assert store._state.reports[90_001].ref_count == ref_after_first

            # 非参战好友 1002 调 GetBattleReport 被隐私边界拒绝，但凭有效 card_id 调 571/572 成功！
            priv_rep = await call_match(1002, "GetBattleReport", protocol.message("match.GetBattleReportRequest", battle_id=90_001))
            assert priv_rep.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

            g_card = await call_match(1002, "GetTrialChallengeCard", protocol.message("match.GetTrialChallengeCardRequest", card_id=c1.card.card_id))
            assert g_card.error_message.id == 0 and g_card.card.battle_id == 90_001

            g_rep = await call_match(1002, "GetSharedTrialReplay", protocol.message("match.GetSharedTrialReplayRequest", card_id=c1.card.card_id))
            assert g_rep.error_message.id == 0 and g_rep.replay.payload == b"replay-90001"

            # 1002 越权为 1001 的对局签卡被拒
            unauth = await call_match(1002, "CreateTrialChallengeCard", protocol.message("match.CreateTrialChallengeCardRequest", battle_id=90_001))
            assert unauth.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

            # 继续签发 10 张卡 -> 达到 10 张上限，最旧的 c1 被 FIFO 淘汰且 ref_count 减 1
            last_card_id = 0
            for i in range(2, 12):
                now[0] += 100
                ci = await call_match(1001, "CreateTrialChallengeCard", protocol.message("match.CreateTrialChallengeCardRequest", battle_id=90_000 + i))
                assert ci.error_message.id == 0
                last_card_id = int(ci.card.card_id)
            assert store._state.reports[90_001].ref_count == ref_after_first - 1
            evicted = await call_match(1002, "GetTrialChallengeCard", protocol.message("match.GetTrialChallengeCardRequest", card_id=c1.card.card_id))
            assert evicted.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

            # 快进超过 14 天 -> 最新卡片也过期失效
            now[0] += TRIAL_CHALLENGE_CARD_TTL_MS + 1000
            exp_card = await call_match(1002, "GetTrialChallengeCard", protocol.message("match.GetTrialChallengeCardRequest", card_id=last_card_id))
            assert exp_card.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND
            exp_rep = await call_match(1002, "GetSharedTrialReplay", protocol.message("match.GetSharedTrialReplayRequest", card_id=last_card_id))
            assert exp_rep.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND
        finally:
            await client.close()
            await match_svc.close()
            await team_svc.close()
            await server.stop(0)

    asyncio.run(exercise())


# ── 6. 真实 Gate TCP 帧链路冒烟：Runtime("local") 全量路由校验 ─────────────────────────────────────


async def _gate_call(client, call_id: int, message_name: str, **fields):
    p = client.protocol
    mid = p.message_ids[message_name]
    req = p.message(p.routes[mid].request_type, **fields)
    return await client.rpc(mid, call_id, req)


def test_real_gate_tcp_routes_jianghu_guild_feud_and_trial_cards(tmp_path):
    async def exercise():
        settings = _settings(tmp_path)
        async with Runtime(settings, "local", dev_token=TOKEN) as runtime:
            async with _enter(runtime, account="jh_alpha") as (client_a, player_a):
                async with _enter(runtime, account="jh_beta") as (client_b, player_b):
                    # Guild 547 / 567 / 568 真实 Gate TCP 路由
                    ga = await _gate_call(client_a, 201, "GuildServiceCreateGuild", name="天墉正道")
                    gb = await _gate_call(client_b, 202, "GuildServiceCreateGuild", name="苍冥玄门")
                    assert ga.error_message.id == 0 and gb.error_message.id == 0

                    wall = await _gate_call(client_a, 203, "GuildServiceGetGuildJianghuGrudgeWall")
                    assert wall.error_message.id == 0 and wall.guild_id == ga.guild.guild_id

                    feud = await _gate_call(
                        client_a, 204, "GuildServiceIssueGuildWarFeud", target_guild_id=gb.guild.guild_id
                    )
                    assert feud.error_message.id == 0 and feud.feud.status == FEUD_STATUS_PENDING

                    feud_acc = await _gate_call(
                        client_b, 205, "GuildServiceRespondGuildWarFeud", feud_id=feud.feud.feud_id, accept=True
                    )
                    assert feud_acc.error_message.id == 0 and feud_acc.feud.status == FEUD_STATUS_ACTIVE

                    # Match 548 / 550 / 551 / 552 真实 Gate TCP 路由
                    inv = await _gate_call(
                        client_a,
                        206,
                        "MatchServiceCreateJianghuDuelInvite",
                        target_player_id=player_b,
                        writ_tier=WRIT_TIER_SILVER,
                    )
                    assert inv.error_message.id == 0 and inv.invite.state == INVITE_STATE_PENDING_TARGET

                    decl = await _gate_call(
                        client_b,
                        207,
                        "MatchServiceRespondJianghuDuelInvite",
                        duel_invite_id=inv.invite.duel_invite_id,
                        accept=False,
                    )
                    assert decl.error_message.id == 0 and decl.invite.state == INVITE_STATE_DECLINED

                    grudges = await _gate_call(client_a, 208, "MatchServiceListJianghuGrudges")
                    assert grudges.error_message.id == 0 and grudges.server_time_ms > 0

                    dism = await _gate_call(client_a, 209, "MatchServiceDismissJianghuGrudge", peer_player_id=player_b)
                    assert dism.error_message.id == ERR_JIANGHU_GRUDGE_NOT_FOUND

                    # Match 570 / 571 / 572 真实 Gate TCP 路由
                    c_miss = await _gate_call(client_a, 210, "MatchServiceCreateTrialChallengeCard", battle_id=999999)
                    assert c_miss.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

                    g_miss = await _gate_call(client_a, 211, "MatchServiceGetTrialChallengeCard", card_id=999999)
                    assert g_miss.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

                    r_miss = await _gate_call(client_a, 212, "MatchServiceGetSharedTrialReplay", card_id=999999)
                    assert r_miss.error_message.id == ERR_MATCH_BATTLE_REPORT_NOT_FOUND

    asyncio.run(exercise())
