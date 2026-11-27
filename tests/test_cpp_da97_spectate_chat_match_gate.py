"""C++ f4947968a..da97bfb11 增量移植全链路测试：观战交流 587..590、Match/Scene/Gate 加固。

覆盖：
1. Match 587 SendBattleSpectateChat & 588 PullBattleSpectateChat：
   - 全 MatchMode 开局冻结决策表（未知模式 fail-closed、关闭不影响观战、PVE/PVP/帮战/锦标赛矩阵）；
   - Gather 建房冻结 `CreateBattleRequest.spectate_chat_enabled` 与观战大厅摘要透出；
   - 自由文本校验（空串/超长/非法 UTF-8）、ContentGuard 审核（PASS / MASKED / REJECT / UNAVAILABLE fail-closed + 真 Engine 适配）；
   - 参战者/观战者权威角色与差异化冷却、单房每秒总流速上限、60 条环形队列淘汰、游标分页拉取、终局封房与尾帧拉取；
2. Scene 589 GetSpectateChatSetting & 590 SetSpectateChatSetting 端到端真 TCP `Runtime("local")`：
   - 默认值、改写、参数/可写性校验、跨下线重登持久化，以及通过 `LocalServiceHub` 实连调用 587/588；
3. Match 加固：论剑热门构筑 D-14 SHA-256 复合主键哈希、江湖生死状待退银两预检（40014）、锦标赛满箱（17002）保留可领；
4. Scene 生命周期/围栏/save_seq 与 Gate 请求/场景通道/进场补发加固及单源指标汇总对账。
"""

from __future__ import annotations

import asyncio
import hashlib
import struct
import time
import uuid
from dataclasses import replace
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.content_guard.engine import Engine as ContentGuardEngine
from xuanming.content_guard.wordlist import Category, Entry, Lexicon, Mode
from xuanming.data import PlayerSnapshot, StoreRejected
from xuanming.grpc_runtime import session_metadata, sign_service_call
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match import (
    BattleNodeEntry,
    CallerIdentity,
    GatherPipeline,
    MatchConfig,
    MatchGrpcService,
    MemoryMatchStore,
    MemoryPresenceProvider,
    PlayerPresence,
    RankedFeaturedBuildRow,
    SpectateChatConfig,
    SpectateChatGuardResult,
    SpectateChatGuardVerdict,
    SpectateChatHub,
    SpectateChatManager,
    SpectateChatModeRule,
    SpectateChatRoomInfo,
    SpectateManager,
    StaticBattleNodeResolver,
    TournamentManager,
    adapt_content_guard_engine,
    evaluate_scene_eligibility_response,
    featured_build_key_hash,
    sanitize_sender_name,
    spectate_chat_enabled_for_battle,
    spectate_chat_rule_of_mode,
)
from xuanming.match.errors import (
    ERR_INVALID_PARAMETER,
    ERR_JIANGHU_DUEL_SILVER_FEE_PENDING,
    ERR_JIANGHU_INSUFFICIENT_WRIT_OR_SILVER,
    ERR_MAIL_RECIPIENT_BOX_FULL,
    ERR_MATCH_BATTLE_NOT_WATCHABLE,
    ERR_MATCH_SPECTATE_CHAT_CHECK_UNAVAILABLE,
    ERR_MATCH_SPECTATE_CHAT_CONTENT_REJECTED,
    ERR_MATCH_SPECTATE_CHAT_COOLDOWN,
    ERR_MATCH_SPECTATE_CHAT_DISABLED,
    ERR_MATCH_SPECTATE_CHAT_ENDED,
    ERR_MATCH_SPECTATE_CHAT_ROOM_BUSY,
)
from xuanming.match.model import (
    BATTLE_ACTIVITY_KIND_GUILD_WAR,
    MODE_GUILD_WAR_SIEGE,
    MODE_JIANGHU_DUEL,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    MODE_PVP_CHALLENGE,
    MODE_PVP_TEAM,
    MODE_SEASON_TOURNAMENT,
    OUTCOME_DECISION_COMPLETE,
    SPECTATE_VISIBILITY_PRIVATE,
    SPECTATE_VISIBILITY_PUBLIC,
)
from xuanming.match.spectate_chat import (
    RING_LIMIT,
    SPECTATE_CHAT_PREF_ALLOW,
    SPECTATE_CHAT_PREF_FORBID,
    SPECTATE_CHAT_PREF_UNSPECIFIED,
    SPECTATE_CHAT_ROLE_PARTICIPANT,
    SPECTATE_CHAT_ROLE_SPECTATOR,
)
from xuanming.match.tournament import DELIVERY_CLAIMABLE, DELIVERY_CLAIMED, _RewardRecord
from xuanming.node_link import SceneLinkNotReady
from xuanming.nodes.gate import (
    GATE_BROADCAST_METHODS,
    GateNode,
    ROUTER_LANE_DEPTH,
    ROUTER_LANE_MAX_WAIT,
    RouterLaneItem,
    Session,
)
from xuanming.nodes.scene import SceneNode, _may_retry_handoff
from xuanming.protocol import Protocol
from xuanming.runtime import Runtime
from xuanming.settings import Settings
from xuanming.smoke import connect

_OUTCOME_SECRET = b"outcome-hmac-secret-32-bytes-ok!"
_GATE_SECRET = b"gate-ticket-secret-32-bytes-ok!!"


class _Clock:
    def __init__(self, now: int = 1_700_000_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


class _FakeContext:
    def __init__(self, md=()) -> None:
        self._md = tuple(md)

    def invocation_metadata(self):
        return self._md

    async def abort(self, code, details):
        raise RuntimeError(f"grpc abort {code}: {details}")


def _signed_ctx(
    protocol: Protocol,
    key: Ed25519PrivateKey,
    method: str,
    player_id: int,
    account: str = "",
) -> _FakeContext:
    details = protocol.message(
        "SessionDetails",
        session_id=(1 << 17) + player_id,
        player_id=player_id,
        gate_node_id=1,
        gate_instance_id="gate-1",
        session_incarnation=1,
        gate_call_seq=1,
        owner_revision=1,
        authenticated_account=account or f"acc-{player_id}",
    )
    path = f"/match.MatchService/{method}"
    md = session_metadata(details.SerializeToString(), path, key)
    return _FakeContext(md)


class _DummyRpc:
    def __init__(self, protocol: Protocol, *, pref_by_player: dict[int, int] | None = None) -> None:
        self.protocol = protocol
        self.pref_by_player = dict(pref_by_player or {})
        self.created_requests: list = []

    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes:
        if path == "/scene_node.SceneNodeGrpc/PrepareBattle":
            req = self.protocol.parse("PrepareBattleRequest", data)
            snap = self.protocol.message(
                "BattlePlayerSnapshot",
                player_id=req.player_id,
                player_name=f"侠客{req.player_id}",
                is_team_captain=True,
                spectate_chat_pref=self.pref_by_player.get(int(req.player_id), SPECTATE_CHAT_PREF_UNSPECIFIED),
            )
            resp = self.protocol.message("PrepareBattleResponse", snapshot=snap, table_fingerprint="fp-1")
            return resp.SerializeToString()
        if path == "/BattleNode/QueryBattleCreation":
            req = self.protocol.parse("QueryBattleCreationRequest", data)
            resp = self.protocol.message(
                "QueryBattleCreationResponse",
                target_instance_id=req.target_instance_id,
                supported_outcome_protocol_version=1,
                rejects_new_battles=False,
            )
            return resp.SerializeToString()
        if path == "/BattleNode/CreateBattle":
            req = self.protocol.parse("CreateBattleRequest", data)
            self.created_requests.append(req)
            resp = self.protocol.message("CreateBattleResponse", adopted_outcome_protocol_version=1, room_count=1)
            return resp.SerializeToString()
        return self.protocol.message("Empty").SerializeToString()

    async def close(self) -> None:
        return None


# -- 1. 观战交流开局冻结策略矩阵与 Gather/Spectate 集成 ----------------------------------------------


def test_spectate_chat_policy_covers_all_match_modes_and_matrix():
    p = Protocol()
    mode_enum = p.pool.FindEnumTypeByName("match.MatchMode")
    for value in mode_enum.values:
        _, decided = spectate_chat_rule_of_mode(value.number)
        assert decided is True, f"MatchMode.{value.name} ({value.number}) missing from spectate_chat_rule_of_mode"

    unknown_rule, unknown_decided = spectate_chat_rule_of_mode(9999)
    assert unknown_decided is False
    assert unknown_rule == SpectateChatModeRule.FORBID

    def snap(pid: int, team: int, captain: bool, pref: int, *, ai: bool = False):
        return p.message(
            "BattlePlayerSnapshot",
            player_id=pid,
            team_index=team,
            is_team_captain=captain,
            ai_controlled=ai,
            spectate_chat_pref=pref,
        )

    default_cfg = SpectateChatConfig()

    # 1. 私密不可观战局恒 False
    assert not spectate_chat_enabled_for_battle(
        default_cfg,
        MODE_PVP_CHALLENGE,
        SPECTATE_VISIBILITY_PRIVATE,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW), snap(2, 1, True, SPECTATE_CHAT_PREF_ALLOW)],
    )

    # 2. 全局 Disabled=True 恒 False
    assert not spectate_chat_enabled_for_battle(
        SpectateChatConfig(disabled=True),
        MODE_PVE_SOLO,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW)],
    )

    # 3. 单人 PVE：未设默认允许；显式关闭禁止交流
    assert spectate_chat_enabled_for_battle(
        default_cfg, MODE_PVE_SOLO, SPECTATE_VISIBILITY_PUBLIC, None, [snap(1, 0, False, SPECTATE_CHAT_PREF_UNSPECIFIED)]
    )
    assert not spectate_chat_enabled_for_battle(
        default_cfg, MODE_PVE_SOLO, SPECTATE_VISIBILITY_PUBLIC, None, [snap(1, 0, False, SPECTATE_CHAT_PREF_FORBID)]
    )

    # 4. 组队 PVE：以队长为准
    assert spectate_chat_enabled_for_battle(
        default_cfg,
        MODE_PVE_TEAM,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW), snap(2, 0, False, SPECTATE_CHAT_PREF_FORBID)],
    )
    assert not spectate_chat_enabled_for_battle(
        default_cfg,
        MODE_PVE_TEAM,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_FORBID), snap(2, 0, False, SPECTATE_CHAT_PREF_ALLOW)],
    )

    # 5. 江湖生死状 / 1v1 切磋：全员一票否决
    assert spectate_chat_enabled_for_battle(
        default_cfg,
        MODE_JIANGHU_DUEL,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW), snap(2, 1, True, SPECTATE_CHAT_PREF_UNSPECIFIED)],
    )
    assert not spectate_chat_enabled_for_battle(
        default_cfg,
        MODE_JIANGHU_DUEL,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW), snap(2, 1, True, SPECTATE_CHAT_PREF_FORBID)],
    )

    # 6. 帮战踢馆：守方 AI 不参与投票
    gw_act = p.message("BattleActivityContext", kind=BATTLE_ACTIVITY_KIND_GUILD_WAR)
    assert spectate_chat_enabled_for_battle(
        default_cfg,
        MODE_GUILD_WAR_SIEGE,
        SPECTATE_VISIBILITY_PUBLIC,
        gw_act,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW), snap(99, 1, False, SPECTATE_CHAT_PREF_FORBID, ai=True)],
    )

    # 7. 多人团队 PVP：默认全员一票否决；切 team_pvp_captain_only=True 时按双方队长裁决
    team_3v3 = [
        snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW),
        snap(2, 0, False, SPECTATE_CHAT_PREF_FORBID),
        snap(3, 1, True, SPECTATE_CHAT_PREF_ALLOW),
        snap(4, 1, False, SPECTATE_CHAT_PREF_ALLOW),
    ]
    assert not spectate_chat_enabled_for_battle(default_cfg, MODE_PVP_TEAM, SPECTATE_VISIBILITY_PUBLIC, None, team_3v3)
    assert spectate_chat_enabled_for_battle(
        SpectateChatConfig(team_pvp_captain_only=True), MODE_PVP_TEAM, SPECTATE_VISIBILITY_PUBLIC, None, team_3v3
    )

    # 8. 锦标赛可单独禁用自由文本交流 & default_forbid_when_unset
    assert not spectate_chat_enabled_for_battle(
        SpectateChatConfig(tournament_disabled=True),
        MODE_SEASON_TOURNAMENT,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, True, SPECTATE_CHAT_PREF_ALLOW), snap(2, 1, True, SPECTATE_CHAT_PREF_ALLOW)],
    )
    assert not spectate_chat_enabled_for_battle(
        SpectateChatConfig(default_forbid_when_unset=True),
        MODE_PVE_SOLO,
        SPECTATE_VISIBILITY_PUBLIC,
        None,
        [snap(1, 0, False, SPECTATE_CHAT_PREF_UNSPECIFIED)],
    )


def test_spectate_chat_587_588_full_lifecycle_guard_rate_limit_ring_and_cleanup():
    async def run():
        p = Protocol()
        clock = _Clock(1_700_000_000_000)
        store = MemoryMatchStore(p, clock_ms=clock)
        key = Ed25519PrivateKey.generate()
        verifier = CallerVerifier([key.public_key()])
        presence = MemoryPresenceProvider()
        for pid, name in (
            (1001, "参战甲"),
            (1002, "参战乙"),
            (2001, "热情观众一二三四五六七八九十超长名字"),
            (2002, ""),
            (2003, "观众丙"),
            (2004, "观众丁"),
        ):
            presence.set_presence(
                PlayerPresence(player_id=pid, online=True, scene_grpc_endpoint="scene:1", name=name)
            )
            await store.verify_original_owner(pid, f"acc-{pid}", 1)

        nodes = StaticBattleNodeResolver(
            [BattleNodeEntry(node_id=1, node_uuid="b1", grpc_endpoint="battle:1", physical_region=1)]
        )
        rpc = _DummyRpc(
            p,
            pref_by_player={
                1001: SPECTATE_CHAT_PREF_ALLOW,
                1002: SPECTATE_CHAT_PREF_ALLOW,
                1003: SPECTATE_CHAT_PREF_FORBID,
            },
        )
        cfg = MatchConfig(
            battle_outcome_secret=_OUTCOME_SECRET,
            spectate_chat=SpectateChatConfig(
                participant_cooldown_ms=2000,
                spectator_cooldown_ms=4000,
                room_rate_limit_per_sec=3,
            ),
        )
        gather = GatherPipeline(p, store, cfg, rpc, presence, nodes)
        spectate = SpectateManager(p, store, cfg, rpc, presence)

        # 用内置 ContentGuardEngine + 自定义拦截测试 PASS / MASKED / REJECT / UNAVAILABLE
        cg_engine = ContentGuardEngine()
        cg_engine.swap(
            1,
            Lexicon(
                deny=(
                    Entry("粗口", Category.ABUSE, Mode.SUB),
                    Entry("违禁广告", Category.AD, Mode.SUB),
                )
            ),
        )
        engine_adapter = adapt_content_guard_engine(cg_engine)

        def guard_fn(player_id: int, text: str) -> SpectateChatGuardResult:
            if text == "审核故障":
                return SpectateChatGuardResult(SpectateChatGuardVerdict.UNAVAILABLE)
            if text == "异常故障":
                raise RuntimeError("boom")
            return engine_adapter(player_id, text)

        chat_mgr = SpectateChatManager(
            p,
            store,
            cfg.spectate_chat,
            content_guard=guard_fn,
            display_name_lookup=lambda pid: _lookup_name(presence, pid),
            clock_ms=clock,
        )
        svc = MatchGrpcService(
            p,
            store,
            cfg,
            gate_verifier=verifier,
            presence=presence,
            battle_nodes=nodes,
            gather=gather,
            spectate=spectate,
            spectate_chat=chat_mgr,
            inline_gather=True,
            clock_ms=clock,
        )

        # 通过真实 ChallengePlayer + RespondChallenge 建一场开放观战交流的公开江湖切磋局（1001 vs 1002）
        ch_open = await svc.challenge_player(
            p.message(
                "match.ChallengePlayerRequest",
                target_player_id=1002,
                battle_config_id=1,
                custom_rules=p.message("CustomDuelRules", allow_public_spectate=True),
            ),
            _signed_ctx(p, key, "ChallengePlayer", 1001),
            "/match.MatchService/ChallengePlayer",
        )
        assert ch_open.error_message.id == 0
        acc_open = await svc.respond_challenge(
            p.message("match.RespondChallengeRequest", challenge_id=ch_open.challenge_id, accept=True),
            _signed_ctx(p, key, "RespondChallenge", 1002),
            "/match.MatchService/RespondChallenge",
        )
        assert acc_open.error_message.id == 0
        open_bid = int(rpc.created_requests[-1].battle_id)
        f_open = await store.get_formation(open_bid)
        assert f_open is not None and rpc.created_requests[-1].spectate_chat_enabled is True

        # 建一场关闭观战交流但仍可正常观战的局（1004 vs 1003，1003 关闭了观战交流）
        presence.set_presence(PlayerPresence(player_id=1003, online=True, scene_grpc_endpoint="scene:1", name="静心道友"))
        presence.set_presence(PlayerPresence(player_id=1004, online=True, scene_grpc_endpoint="scene:1", name="切磋道友"))
        await store.verify_original_owner(1003, "acc-1003", 1)
        await store.verify_original_owner(1004, "acc-1004", 1)
        ch_muted = await svc.challenge_player(
            p.message(
                "match.ChallengePlayerRequest",
                target_player_id=1003,
                battle_config_id=1,
                custom_rules=p.message("CustomDuelRules", allow_public_spectate=True),
            ),
            _signed_ctx(p, key, "ChallengePlayer", 1004),
            "/match.MatchService/ChallengePlayer",
        )
        assert ch_muted.error_message.id == 0
        acc_muted = await svc.respond_challenge(
            p.message("match.RespondChallengeRequest", challenge_id=ch_muted.challenge_id, accept=True),
            _signed_ctx(p, key, "RespondChallenge", 1003),
            "/match.MatchService/RespondChallenge",
        )
        assert acc_muted.error_message.id == 0
        muted_bid = int(rpc.created_requests[-1].battle_id)
        assert rpc.created_requests[-1].spectate_chat_enabled is False

        # ListWatchableBattles 同时包含两场公开局，且准确暴露各自的 spectate_chat_enabled
        watch_list = await svc.list_watchable_battles(
            p.message("match.ListWatchableBattlesRequest", limit=20),
            _signed_ctx(p, key, "ListWatchableBattles", 2001),
            "/match.MatchService/ListWatchableBattles",
        )
        by_bid = {int(b.battle_id): b for b in watch_list.battles}
        assert open_bid in by_bid and by_bid[open_bid].spectate_chat_enabled is True
        assert muted_bid in by_bid and by_bid[muted_bid].spectate_chat_enabled is False

        # 关闭交流的局：Send 硬拒 16073，Pull 静默回 enabled=False 无错误码
        send_muted = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=muted_bid, content="集火右边"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert send_muted.error_message.id == ERR_MATCH_SPECTATE_CHAT_DISABLED
        assert send_muted.enabled is False

        pull_muted = await svc.pull_battle_spectate_chat(
            p.message("match.PullBattleSpectateChatRequest", battle_id=muted_bid),
            _signed_ctx(p, key, "PullBattleSpectateChat", 2001),
            "/match.MatchService/PullBattleSpectateChat",
        )
        assert pull_muted.error_message.id == 0
        assert pull_muted.enabled is False and not pull_muted.messages

        # 参数边界校验：空串 / 超长（>180 字节） / 不存在战斗
        bad_empty = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="   "),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert bad_empty.error_message.id == ERR_INVALID_PARAMETER

        bad_len = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="道" * 61),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert bad_len.error_message.id == ERR_INVALID_PARAMETER

        bad_bid = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=999_999, content="你好"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert bad_bid.error_message.id == ERR_MATCH_BATTLE_NOT_WATCHABLE
        assert bad_bid.battle_ended is True

        # 1. 观战者 2001 发言成功 -> 角色 SPECTATOR、展示名截断 16 字、冷却 4000ms
        r1 = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="这波反杀太帅了"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r1.error_message.id == 0
        assert r1.message.seq == 1
        assert r1.message.role == SPECTATE_CHAT_ROLE_SPECTATOR
        assert len(r1.message.sender_name) == 16
        assert r1.cooldown_until_ms == clock.now + 4000

        # 2. 观战者 2001 连发命中冷却 16075
        r_cd = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="再来一句"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r_cd.error_message.id == ERR_MATCH_SPECTATE_CHAT_COOLDOWN
        assert r_cd.cooldown_until_ms == clock.now + 4000

        # 3. 参战者 1001 发言命中辱骂打码 -> 角色 PARTICIPANT、冷却 2000ms
        r2 = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="粗口测试"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 1001),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r2.error_message.id == 0
        assert r2.message.seq == 2
        assert r2.message.role == SPECTATE_CHAT_ROLE_PARTICIPANT
        assert r2.message.content == "**测试"
        assert r2.cooldown_until_ms == clock.now + 2000

        # 4. ContentGuard 拒绝（16077）与审核不可用/异常 fail-closed（16078）均不消耗冷却
        r_rej = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="违禁广告"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2002),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r_rej.error_message.id == ERR_MATCH_SPECTATE_CHAT_CONTENT_REJECTED

        r_unavail = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="审核故障"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2002),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r_unavail.error_message.id == ERR_MATCH_SPECTATE_CHAT_CHECK_UNAVAILABLE

        r_exc = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="异常故障"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2002),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r_exc.error_message.id == ERR_MATCH_SPECTATE_CHAT_CHECK_UNAVAILABLE

        # 5. 同秒第 3 条（2002，空昵称兜底为“观战道友”）成功，第 4 条（2003）命中单房总流速 16076
        r3 = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="稳住阵脚"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2002),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r3.error_message.id == 0
        assert r3.message.seq == 3
        assert r3.message.sender_name == "观战道友"

        r_busy = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="挤一挤"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2003),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r_busy.error_message.id == ERR_MATCH_SPECTATE_CHAT_ROOM_BUSY

        # 6. 增量分页拉取 after_seq=1 返回 seq 2、3
        pull = await svc.pull_battle_spectate_chat(
            p.message("match.PullBattleSpectateChatRequest", battle_id=open_bid, after_seq=1),
            _signed_ctx(p, key, "PullBattleSpectateChat", 2001),
            "/match.MatchService/PullBattleSpectateChat",
        )
        assert pull.error_message.id == 0
        assert pull.enabled is True and pull.battle_ended is False
        assert pull.latest_seq == 3
        assert pull.cooldown_until_ms == clock.now + 4000
        assert [m.seq for m in pull.messages] == [2, 3]

        # 7. 终局 CommitBattleOutcome 自动联动关闭观战交流室：禁发 16074，尾帧仍可拉取且 battle_ended=True
        result_ev = p.message(
            "contracts.kafka.BattleResultEvent",
            battle_id=open_bid,
            battle_config_id=1,
            match_mode=MODE_PVP_CHALLENGE,
            settlement_format_version=1,
            outcome=1,
            total_rounds=1,
            finished_at_ms=clock.now,
        )
        result_ev.teams.add(team_index=0, player_ids=[1001])
        result_ev.teams.add(team_index=1, player_ids=[1002])
        result_ev.settlements.add(
            player_id=1001,
            battle_id=open_bid,
            battle_config_id=1,
            outcome=1,
            total_rounds=1,
            player_team_index=0,
            match_mode=MODE_PVP_CHALLENGE,
        )
        result_ev.settlements.add(
            player_id=1002,
            battle_id=open_bid,
            battle_config_id=1,
            outcome=1,
            total_rounds=1,
            player_team_index=1,
            match_mode=MODE_PVP_CHALLENGE,
        )
        outcome_req = p.message(
            "match.CommitBattleOutcomeRequest",
            battle_id=open_bid,
            placement_operation_id=f_open.operation_id,
            target_instance_id="b1",
            raw_result_payload=result_ev.SerializeToString(),
        )
        sign_service_call("/match.MatchInternal/CommitBattleOutcome", outcome_req, "battle", clock.now, _OUTCOME_SECRET)
        co_resp = await svc.commit_battle_outcome(
            outcome_req,
            _FakeContext(),
            "/match.MatchInternal/CommitBattleOutcome",
        )
        assert co_resp.decision == OUTCOME_DECISION_COMPLETE

        r_ended = await svc.send_battle_spectate_chat(
            p.message("match.SendBattleSpectateChatRequest", battle_id=open_bid, content="赛后聊天"),
            _signed_ctx(p, key, "SendBattleSpectateChat", 2004),
            "/match.MatchService/SendBattleSpectateChat",
        )
        assert r_ended.error_message.id == ERR_MATCH_SPECTATE_CHAT_ENDED
        assert r_ended.battle_ended is True

        pull_tail = await svc.pull_battle_spectate_chat(
            p.message("match.PullBattleSpectateChatRequest", battle_id=open_bid, after_seq=0, limit=2),
            _signed_ctx(p, key, "PullBattleSpectateChat", 2004),
            "/match.MatchService/PullBattleSpectateChat",
        )
        assert pull_tail.error_message.id == 0
        assert pull_tail.enabled is True and pull_tail.battle_ended is True
        assert [m.seq for m in pull_tail.messages] == [2, 3]

    asyncio.run(run())


async def _lookup_name(presence: MemoryPresenceProvider, pid: int) -> str:
    return (await presence.get_presence(pid)).name


def test_spectate_chat_hub_ring_buffer_eviction_and_cursor_pagination():
    hub = SpectateChatHub()
    base_ms = 1_700_000_000_000
    for idx in range(75):
        res = hub.send(
            battle_id=5001,
            player_id=100 + idx,
            sender_name=f"道友{idx}",
            role=SPECTATE_CHAT_ROLE_SPECTATOR,
            content=f"msg-{idx + 1}",
            now_ms=base_ms + idx * 1000,
            cooldown_ms=4000,
            room_rate_limit=8,
        )
        assert res.message is not None and res.message.seq == idx + 1

    # 默认 after_seq=0 返回最近 20 条（56..75）
    first = hub.pull(5001, 999, 0, 0, base_ms + 80_000)
    assert first.latest_seq == 75
    assert len(first.messages) == 20
    assert first.messages[0].seq == 56 and first.messages[-1].seq == 75

    # 全量拉取（limit=100 封顶 60 条）：仅保留 16..75
    full = hub.pull(5001, 999, 0, 100, base_ms + 80_000)
    assert len(full.messages) == RING_LIMIT
    assert full.messages[0].seq == 16 and full.messages[-1].seq == 75

    # 游标分页 after_seq=70 仅返回 71..75
    inc = hub.pull(5001, 999, 70, 0, base_ms + 80_000)
    assert [m.seq for m in inc.messages] == [71, 72, 73, 74, 75]

    assert sanitize_sender_name("   ", SPECTATE_CHAT_ROLE_PARTICIPANT) == "参战道友"
    assert sanitize_sender_name("   ", SPECTATE_CHAT_ROLE_SPECTATOR) == "观战道友"


# -- 2. Scene 589/590 个人开关 + LocalServiceHub 587/588 端到端真 TCP 测试 ----------------------------


def test_scene_589_590_spectate_chat_setting_persistence_and_local_hub_587_588(tmp_path):
    async def run():
        defaults = Settings()
        settings = replace(
            defaults,
            database=tmp_path / "players.db",
            gate=replace(defaults.gate, port=0),
            scene=replace(defaults.scene, port=0, grpc_port=0),
            battle=replace(defaults.battle, port=0, grpc_port=0),
        )
        async with Runtime(settings, "local", dev_token="test-only") as runtime:
            p, scene, gate = runtime.protocol, runtime.scene, runtime.gate
            rpc_id = p.message_ids
            assert rpc_id["MatchServiceSendBattleSpectateChat"] == 587
            assert rpc_id["MatchServicePullBattleSpectateChat"] == 588
            assert rpc_id["SceneSceneClientPlayerGetSpectateChatSetting"] == 589
            assert rpc_id["SceneSceneClientPlayerSetSpectateChatSetting"] == 590

            # 跨节点交接门禁应允许 589/590 重投
            assert _may_retry_handoff(p.routes[589]) is True
            assert _may_retry_handoff(p.routes[590]) is True

            async with connect(p, gate.info.endpoint) as client:
                await client.send(gate.issue_ticket())
                assert (await client.receive("ClientTokenVerifyResponse")).success
                login = await client.rpc(
                    rpc_id["ClientPlayerLoginLogin"],
                    1,
                    p.message("loginpb.LoginRequest", account="local", password="test-only"),
                )
                assert not login.error_message.id
                enter = await client.rpc(
                    rpc_id["ClientPlayerLoginEnterGame"],
                    2,
                    p.message("loginpb.EnterGameRequest", player_id=1, request_id="spectate-setting-enter-1"),
                )
                assert enter.player_id == 1
                await client.content(rpc_id["SceneSceneClientPlayerNotifyEnterScene"])

                # 1. 首次查询 589：默认 UNSPECIFIED(0) -> effective_allow=True
                g0 = await client.rpc(589, 3, p.message("GetSpectateChatSettingRequest"))
                assert g0.error_message.id == 0
                assert g0.preference == SPECTATE_CHAT_PREF_UNSPECIFIED
                assert g0.effective_allow is True

                # 2. 非法参数（0 / 99）被 590 拒绝为 1005
                bad = await client.rpc(590, 4, p.message("SetSpectateChatSettingRequest", preference=0))
                assert bad.error_message.id == ERR_INVALID_PARAMETER

                # 3. 置为 FORBID(2) 成功落盘
                s1 = await client.rpc(590, 5, p.message("SetSpectateChatSettingRequest", preference=SPECTATE_CHAT_PREF_FORBID))
                assert s1.error_message.id == 0
                assert s1.preference == SPECTATE_CHAT_PREF_FORBID
                assert s1.effective_allow is False

                # 4. 立刻读 589 与数据库快照均已生效
                g1 = await client.rpc(589, 6, p.message("GetSpectateChatSettingRequest"))
                assert g1.preference == SPECTATE_CHAT_PREF_FORBID and g1.effective_allow is False

                # 5. 通过 LocalServiceHub 实连调用 587/588（对不存在战斗返回 16018 + battle_ended=True，证明链路贯通）
                send_587 = await client.rpc(
                    587,
                    7,
                    p.message("match.SendBattleSpectateChatRequest", battle_id=888_888, content="测试发言"),
                )
                assert send_587.error_message.id == ERR_MATCH_BATTLE_NOT_WATCHABLE
                assert send_587.battle_ended is True

                pull_588 = await client.rpc(
                    588,
                    8,
                    p.message("match.PullBattleSpectateChatRequest", battle_id=888_888),
                )
                assert pull_588.error_message.id == ERR_MATCH_BATTLE_NOT_WATCHABLE
                assert pull_588.battle_ended is True

                # 6. 登出并校验持久库留存，再重登验证跨重载持久化
                await client.rpc(rpc_id["ClientPlayerLoginLeaveGame"], 9, p.message("loginpb.LeaveGameRequest"))

            persisted = await scene.store.load_and_claim(1, b"verify-spectate-pref")
            assert scene.rows.assemble(persisted).profile_component.spectate_chat_pref == SPECTATE_CHAT_PREF_FORBID
            await scene.store.release(persisted)

            async with connect(p, gate.info.endpoint) as client2:
                await client2.send(gate.issue_ticket())
                assert (await client2.receive("ClientTokenVerifyResponse")).success
                await client2.rpc(
                    rpc_id["ClientPlayerLoginLogin"],
                    1,
                    p.message("loginpb.LoginRequest", account="local", password="test-only"),
                )
                await client2.rpc(
                    rpc_id["ClientPlayerLoginEnterGame"],
                    2,
                    p.message("loginpb.EnterGameRequest", player_id=1, request_id="spectate-setting-enter-2"),
                )
                await client2.content(rpc_id["SceneSceneClientPlayerNotifyEnterScene"])
                g2 = await client2.rpc(589, 3, p.message("GetSpectateChatSettingRequest"))
                assert g2.preference == SPECTATE_CHAT_PREF_FORBID and g2.effective_allow is False

                # 切回 ALLOW(1)
                s2 = await client2.rpc(590, 4, p.message("SetSpectateChatSettingRequest", preference=SPECTATE_CHAT_PREF_ALLOW))
                assert s2.error_message.id == 0
                assert s2.preference == SPECTATE_CHAT_PREF_ALLOW and s2.effective_allow is True
                await client2.rpc(rpc_id["ClientPlayerLoginLeaveGame"], 5, p.message("loginpb.LeaveGameRequest"))

    asyncio.run(run())


# -- 3. Match 增量加固（Ranked D-14 哈希 / Jianghu 40014 / Tournament 满箱 17002）----------------------


def test_match_ranked_jianghu_and_tournament_da97_hardening():
    p = Protocol()

    # 3.1 Ranked featured_build_key_hash（与 Go SHA-256 BigEndian uint64 逐比特一致）
    build_key = "3:203:10002,10025,10035,10039"
    digest = hashlib.sha256(build_key.encode("utf-8")).digest()
    exp_high, exp_low = struct.unpack(">QQ", digest[:16])
    assert featured_build_key_hash(build_key) == (exp_high, exp_low)

    row = RankedFeaturedBuildRow(
        season_id=1,
        build_key=build_key,
        season_seal_id=3,
        starter_pet_table_id=203,
        core_skill_ids="10002,10025,10035,10039",
        appearances=5,
        wins=4,
        high_tier_wins=3,
        featured_battle_id=9001,
        featured_player_id=1001,
        featured_player_name="问道剑客",
        featured_rating_centi=175_000,
        featured_team_size=1,
        featured_total_rounds=7,
        updated_ms=1000,
    )
    assert (row.build_key_high, row.build_key_low) == (exp_high, exp_low)

    # 3.2 Jianghu CheckJianghuDuelEligibilityResponse 优先拦截 payer_silver_fee_pending -> 40014
    pending_resp = p.message(
        "CheckJianghuDuelEligibilityResponse",
        payer_has_sufficient_fee=False,
        payer_silver_fee_pending=True,
    )
    code, _ = evaluate_scene_eligibility_response(pending_resp, payer_player_id=1001, side_a_ids=[1001])
    assert code == ERR_JIANGHU_DUEL_SILVER_FEE_PENDING

    insufficient_resp = p.message(
        "CheckJianghuDuelEligibilityResponse",
        payer_has_sufficient_fee=False,
        payer_silver_fee_pending=False,
    )
    code2, _ = evaluate_scene_eligibility_response(insufficient_resp, payer_player_id=1001, side_a_ids=[1001])
    assert code2 == ERR_JIANGHU_INSUFFICIENT_WRIT_OR_SILVER

    # 3.3 Tournament 领奖遇收件箱满（17002）：透传 17002 且保留可领状态，清空后重试成功
    async def run_tourn():
        box_full = [True]

        async def mail_sender(player_id: int, reward: _RewardRecord) -> tuple[int, int]:
            if box_full[0]:
                return 0, ERR_MAIL_RECIPIENT_BOX_FULL
            return 77_001, 0

        mgr = TournamentManager(p, reward_mail_sender=mail_sender)
        mgr._rewards[(1, 1001, 1)] = _RewardRecord(
            reward_id=501,
            season_id=1,
            player_id=1001,
            tier_kind=1,
            squad_id=10,
            final_rank=1,
            tier_id=1,
            gold_amount=5000,
            delivery_state=DELIVERY_CLAIMABLE,
        )
        caller = CallerIdentity(
            player_id=1001,
            session_id=1,
            session_incarnation=1,
            gate_node_id=1,
            gate_instance_id="g1",
            account="a1",
            owner_revision=1,
        )
        req = p.message("match.ClaimTournamentRewardRequest", season_id=1)

        r_full = await mgr.claim_tournament_reward(caller, req)
        assert r_full.error_message.id == ERR_MAIL_RECIPIENT_BOX_FULL
        assert not r_full.claimed_rewards
        assert mgr._rewards[(1, 1001, 1)].delivery_state == DELIVERY_CLAIMABLE

        box_full[0] = False
        r_ok = await mgr.claim_tournament_reward(caller, req)
        assert r_ok.error_message.id == 0
        assert len(r_ok.claimed_rewards) == 1
        assert r_ok.claimed_rewards[0].mail_id == 77_001
        assert mgr._rewards[(1, 1001, 1)].delivery_state == DELIVERY_CLAIMED

    asyncio.run(run_tourn())


# -- 4. Scene 生命周期围栏/save_seq 与 Gate 通道加固测试 ----------------------------------------------


def test_scene_lifecycle_fence_counters_keep_held_and_save_seq_saturation():
    async def run():
        p = Protocol()
        from xuanming.data import PlayerRowCodec

        rows = PlayerRowCodec(p)

        class _MemStore:
            def __init__(self):
                self.saved: dict[int, PlayerSnapshot] = {}

            async def load_and_claim(self, player_id, node_uuid, owner_epoch=0):
                snap = self.saved.get(player_id)
                if snap is None:
                    snap = PlayerSnapshot(
                        player_id=player_id,
                        owner=bytes(node_uuid),
                        owner_epoch=owner_epoch or 1,
                        save_seq=1,
                        schema_gen=1,
                        rows={},
                    )
                else:
                    snap = PlayerSnapshot(
                        player_id=snap.player_id,
                        owner=bytes(node_uuid),
                        owner_epoch=owner_epoch or snap.owner_epoch + 1,
                        save_seq=snap.save_seq + 1,
                        schema_gen=snap.schema_gen,
                        rows=dict(snap.rows),
                        released=False,
                    )
                self.saved[player_id] = snap
                return snap

            async def commit(self, snapshot, new_rows, *, force=False, periodic=False, release=False):
                updated = PlayerSnapshot(
                    player_id=snapshot.player_id,
                    owner=snapshot.owner,
                    owner_epoch=snapshot.owner_epoch,
                    save_seq=snapshot.save_seq + 1,
                    schema_gen=snapshot.schema_gen,
                    rows=dict(new_rows),
                    released=release,
                )
                self.saved[snapshot.player_id] = updated
                return updated

            async def release(self, snapshot):
                return await self.commit(snapshot, snapshot.rows, release=True)

        info = p.message("NodeInfo", node_id=1, node_type=3, zone_id=1, node_uuid="scene-u1")
        scene = SceneNode(p, info, _MemStore(), rows, lambda *_: None)
        scene.create(p.message("CreateSceneRequest", scene_id=100, config_id=1))

        # 1. 不完整入场 -> reject_incomplete
        with pytest.raises(ValueError):
            await scene.enter(p.message("PlayerEnterGameNodeRequest", player_id=0, session_id=1), "g1")
        assert scene.enter_fence_counters["reject_incomplete"] == 1

        # 2. 正常入场 -> admit_new
        enter_req = p.message(
            "PlayerEnterGameNodeRequest",
            player_id=10,
            session_id=101,
            scene_id=100,
            owner_epoch=1,
            login_generation=1,
            owner_revision=1,
        )
        await scene.enter(enter_req, "g1")
        assert scene.enter_fence_counters["admit_new"] == 1

        # 3. 过期的代次入场 -> reject_stale_generation
        stale_req = p.message(
            "PlayerEnterGameNodeRequest",
            player_id=10,
            session_id=102,
            scene_id=100,
            owner_epoch=1,
            login_generation=1,
            owner_revision=1,
        )
        with pytest.raises(ValueError):
            await scene.enter(stale_req, "g2")
        assert scene.enter_fence_counters["reject_stale_generation"] == 1

        # 4. 断线保留 + 带 keep_held 的 session_ended 释放 -> session_ended_kept_held（保留不踢）
        world = scene.locations[10]
        player = world.players[10]
        await scene._connection_lost(world, player)
        assert scene.connection_lost_hold_counters["held"] == 1
        assert scene.connection_holds.held(player)

        keep_held_rel = SimpleNamespace(
            player_id=10,
            expected_owner_epoch=1,
            session_ended=True,
            covered_login_generation=1,
            covered_session_id=101,
            keep_held=True,
        )
        await scene.release(keep_held_rel)
        assert scene.release_fence_counters["session_ended_kept_held"] == 1
        assert 10 in scene.locations

        # 5. 非可写态调用 590 SetSpectateChatSetting -> 返回 1006
        scene._closing = True
        non_mut = await scene._set_spectate_chat_setting(
            world, player, None, p.message("SetSpectateChatSettingRequest", preference=SPECTATE_CHAT_PREF_FORBID)
        )
        assert non_mut.error_message.id == 1006
        scene._closing = False

        # 6. save_seq 饱和保护 -> StoreRejected("SAVE_SEQ_EXHAUSTED") + save_seq_counters["exhausted"]
        player.snapshot = SimpleNamespace(
            player_id=10, owner_epoch=1, schema_gen=1, rows=dict(player.snapshot.rows), released=False, save_seq=(1 << 64) - 1
        )
        with pytest.raises(StoreRejected):
            await scene.commit_player_rows(player, player.snapshot.rows)
        assert scene.save_seq_counters["exhausted"] == 1

        # 7. 单源指标与格式化摘要包含全部键
        metrics = scene.collect_lifecycle_metrics()
        summary = scene.format_lifecycle_summary()
        assert "[EnterFence]" in summary["enter_fence"] and "admit_new=1" in summary["enter_fence"]
        assert "[ReleaseFence]" in summary["release_fence"] and "session_ended_kept_held=1" in summary["release_fence"]
        assert "[SaveSeq]" in summary["save_seq"] and "exhausted=1" in summary["save_seq"]
        assert metrics["enter_fence"]["admit_new"] == 1
        assert metrics["save_seq"]["exhausted"] == 1

    asyncio.run(run())


def test_gate_router_lane_and_scene_entry_hardening_metrics():
    async def run():
        p = Protocol()
        info = p.message("NodeInfo", node_id=1, node_type=4, zone_id=1, node_uuid="gate-u1")

        class _FakeWriter:
            def __init__(self):
                self.closed = False

            def is_closing(self):
                return self.closed

            def close(self):
                self.closed = True

        async def _forward(*args, **kwargs):
            return p.content(1, 1)

        gate = GateNode(p, info, _GATE_SECRET, _forward)
        session = Session(
            session_id=(1 << 17) | 1,
            incarnation=1,
            writer=_FakeWriter(),
            verified=True,
            logged_in=True,
            enter_requested=True,
            player_id=77,
            account="acc-77",
            owner_revision=1,
        )
        gate.sessions[session.session_id] = session

        # 1. Router lane 满队背压与深度统计
        msg_id = p.message_ids["ClientPlayerLoginCreatePlayer"]
        now = time.monotonic()
        for seq in range(ROUTER_LANE_DEPTH):
            gate._admit_router(session, p.message("ClientRequest", message_id=msg_id, id=seq + 1), now)
        assert gate.router_lane_counters["admitted"] == ROUTER_LANE_DEPTH
        assert gate.collect_lane_metrics()["router_lane"]["max_depth_seen"] == ROUTER_LANE_DEPTH

        gate._admit_router(session, p.message("ClientRequest", message_id=msg_id, id=999), now)
        assert gate.router_lane_counters["rejected_full"] == 1

        # 2. _drain_redirect 清理在途 pending_route 并排空 router lane（计入 dropped_on_redirect）
        route_ev = p.message(
            "contracts.kafka.RoutePlayerEvent",
            session_id=session.session_id,
            player_id=77,
            scene_id=50,
            target_node_id=2,
            owner_epoch=1,
            login_generation=1,
            owner_revision=1,
        )

        class _FlakyScene:
            def __init__(self):
                self.calls = 0

            async def prepare_entry(self, node_id):
                self.calls += 1
                if self.calls == 1:
                    raise SceneLinkNotReady("not ready yet")
                return SimpleNamespace(info=SimpleNamespace(node_uuid="scene-2"))

            async def send_entry(self, link, request, *, timeout, can_send):
                return None

        gate.scene = _FlakyScene()
        await gate.route(route_ev, enter_type=1, background=False)
        assert gate.scene_entry_counters["deferred"] == 1
        assert gate.scene_entry_counters["recovered"] == 1

        # 发起新路由并立即被 _drain_redirect 取代 + 排空 router lane
        ticket = gate.issue_ticket(player_id=77)
        other_token_payload = p.message(
            "GateTokenPayload",
            gate_node_id=2,
            zone_id=1,
            expire_timestamp=int(time.time()) + 600,
            player_id=77,
        ).SerializeToString()
        redirect_resp = SimpleNamespace(
            target_gate_ip="127.0.0.1",
            target_gate_port=9002,
            token_payload=other_token_payload,
            token_signature=ticket.signature,
            token_deadline=int(time.time()) + 600,
        )
        assert gate._drain_redirect(session, redirect_resp, expected_player=77) is True
        assert session.requests.empty()
        assert gate.router_lane_counters["dropped_on_redirect"] == ROUTER_LANE_DEPTH

        # 3. 单源汇总与指标键一致
        summary = gate.format_lane_summary()
        metrics = gate.collect_lane_metrics()
        assert "[lane] summary" in summary["router_lane"] and f"dropped_on_redirect={ROUTER_LANE_DEPTH}" in summary["router_lane"]
        assert "[scene_lane] summary" in summary["scene_lane"]
        assert "[SceneEntry]" in summary["scene_entry"] and "deferred=1 recovered=1" in summary["scene_entry"]
        assert metrics["scene_entry"]["events"]["recovered"] == 1

        await gate.close()

    asyncio.run(run())
