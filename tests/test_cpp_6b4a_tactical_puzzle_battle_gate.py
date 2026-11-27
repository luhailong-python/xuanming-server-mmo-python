"""C++ 60b0bd6b3..6b4ae7852 增量移植全链路测试（万象残局坊 575..581 / 战斗引擎与防沉迷 / 网关准入与观战排位加固）。

覆盖：
1. Match 575..581 万象残局坊：草稿校验/敏感词 fail-closed/10 题上限、作者自证开战（确定性种子 + 自动录播）->
   终局提交自动发布并钉住标准解回放、挑战者攻关与破局统计、通关后幂等点赞、5 页签列表与荣誉称谓、
   编辑已发布题回落草稿与删除清理；
2. Battle 引擎残局演武规则与防沉迷加固：Dungeon 314 准入门禁、玩家/敌方自定义气血/法力/护盾/增减益/速度与阵法、
   回合耗尽权威判负（绝不判平）、通关零掉落与结算回显、未成年人战斗直连截止时刻（票据封顶、到点断连、拒握、观战清理）；
3. Gate 与 Match 观战/排位加固：Gate 未验签池预算/首帧上限/验签前心跳拦截/单票单连/登录后期限/ticket_account_digest/
   租约代次围栏/防沉迷 2049 立即下线、Match 公开观战可见性矩阵（帮战/高段排位/切磋）/观战大厅缓存与忘记、
   论剑热门构筑缓存与换季释放代表战报 ref_count。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from xuanming.data import ConfigTables
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
    SpectateManager,
    StaticBattleNodeResolver,
    TacticalPuzzleManager,
    derive_honor,
    deterministic_puzzle_seed,
)
from xuanming.match.errors import (
    ERR_MATCH_INTERNAL,
    ERR_MATCH_PUZZLE_CANNOT_LIKE_OWN,
    ERR_MATCH_PUZZLE_CLEAR_REQUIRED_TO_LIKE,
    ERR_MATCH_PUZZLE_LIMIT_REACHED,
    ERR_MATCH_PUZZLE_NOT_FOUND,
    ERR_MATCH_PUZZLE_NOT_OWNER,
    ERR_MATCH_PUZZLE_NOT_PUBLISHED,
    ERR_MATCH_PUZZLE_SOLUTION_REPLAY_NOT_FOUND,
    ERR_MATCH_PUZZLE_SPEC_INVALID,
    ERR_MATCH_PUZZLE_TEXT_REJECTED,
)
from xuanming.match.model import (
    MODE_GUILD_WAR_PVP,
    MODE_PVE_SOLO,
    MODE_PVP_CHALLENGE,
    MODE_PVP_TEAM,
    OUTCOME_DECISION_COMPLETE,
    SPECTATE_VISIBILITY_PRIVATE,
    SPECTATE_VISIBILITY_PUBLIC,
    record_replay_for,
    spectate_visibility_for,
)
from xuanming.match.tactical_puzzle import (
    STATE_DRAFT,
    STATE_PUBLISHED,
    TAB_HARDEST,
    TAB_HOT,
    TAB_MY_CLEARED,
    TAB_MY_CREATED,
    TAB_NEWEST,
    WORKSHOP_DUNGEON_ID,
)
from xuanming.nodes.battle import BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BOSS_TRIAL_KIND_TACTICAL_PUZZLE,
    MINOR_PLAY_CLOSE_LEAD_MS,
    TACTICAL_PUZZLE_BUFF_DEF_UP_BUFF_ID,
    TACTICAL_PUZZLE_DEBUFF_POISON_BUFF_ID,
    TACTICAL_PUZZLE_SHIELD_BUFF_ID,
    minor_direct_close_at_ms,
    minor_play_deadline_reached,
    minor_ticket_expire_at_ms,
)
from xuanming.nodes.gate import GateNode, LOGIN_MINOR_PLAY_TIME_OVER, Session, issue_gate_ticket
from xuanming.protocol import Protocol
from xuanming.transport import Packet, read_packet, write_packet

_BATTLE_SECRET = b"battle-ticket-secret-32-bytes-ok!"
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


def _signed_ctx(protocol: Protocol, key: Ed25519PrivateKey, method: str, player_id: int, account: str = "") -> _FakeContext:
    details = protocol.message(
        "SessionDetails",
        session_id=(1 << 17) + player_id,
        player_id=player_id,
        gate_node_id=1,
        gate_instance_id="gate-1",
        session_incarnation=player_id,
        owner_revision=1,
        authenticated_account=account or f"acct_{player_id}",
    )
    md = session_metadata(details.SerializeToString(), method, key)
    return _FakeContext(md)


def _valid_spec(protocol: Protocol, *, round_limit: int = 3, enemy_hp_pct: int = 50, monster_id: int = 311):
    spec = protocol.message(
        "TacticalPuzzleSpec",
        round_limit=round_limit,
        character_preset_id=101,
        pet_preset_id=201,
        formation_id=2,
        player_initial_hp_pct=60,
        player_initial_mp_pct=80,
        player_initial_shield_pct=20,
        player_initial_debuff_kind=3,
    )
    spec.enemies.add(
        monster_table_id=monster_id,
        initial_hp_pct=enemy_hp_pct,
        initial_shield_pct=15,
        speed_delta_pct=10,
        initial_buff_kind=1,
    )
    return spec


class _InProcessRpc:
    """将 GatherPipeline 的 Scene/Battle gRPC 调用路由到内存中的 BattleNode。"""

    def __init__(self, protocol: Protocol, battle_node: BattleNode) -> None:
        self.protocol = protocol
        self.battle_node = battle_node
        self.created_requests: list = []
        self.add_observer_requests: list = []

    async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes:
        if path == "/scene_node.SceneNodeGrpc/PrepareBattle":
            req = self.protocol.parse("PrepareBattleRequest", data)
            resp = self.protocol.message("PrepareBattleResponse")
            snap = resp.snapshot
            snap.player_id = req.player_id
            snap.player_name = f"侠士{req.player_id}"
            snap.level = 30
            snap.class_id = 1
            snap.base_attributes.health = 2000
            snap.base_attributes.mana = 500
            snap.base_attributes.strength = 180
            snap.base_attributes.armor = 50
            snap.base_attributes.speed = 120
            snap.routing.session_id = (1 << 17) + req.player_id
            snap.routing.gate_node_id = 1
            snap.routing.gate_instance_id = "gate-1"
            snap.routing.scene_node_id = 10
            snap.routing.scene_instance_id = "scene-10"
            snap.routing.zone_id = 1
            if self.battle_node.tables is not None:
                resp.table_fingerprint = self.battle_node.tables.fingerprint
                snap.table_fingerprint = self.battle_node.tables.fingerprint
            return resp.SerializeToString(deterministic=True)
        if path == "/scene_node.SceneNodeGrpc/CancelBattlePrepare":
            return self.protocol.message("Empty").SerializeToString(deterministic=True)
        if path == "/BattleNode/QueryBattleCreation":
            req = self.protocol.parse("QueryBattleCreationRequest", data)
            return self.battle_node.query_creation(req).SerializeToString(deterministic=True)
        if path == "/BattleNode/CreateBattle":
            req = self.protocol.parse("CreateBattleRequest", data)
            self.created_requests.append(req)
            return self.battle_node.create(req).SerializeToString(deterministic=True)
        if path == "/BattleNode/DestroyBattle":
            req = self.protocol.parse("DestroyBattleRequest", data)
            return self.battle_node.destroy(req).SerializeToString(deterministic=True)
        if path == "/BattleNode/AddObserver":
            req = self.protocol.parse("AddObserverRequest", data)
            self.add_observer_requests.append(req)
            return self.battle_node.add_observer(req).SerializeToString(deterministic=True)
        raise AssertionError(f"unexpected rpc path {path}")

    async def close(self) -> None:
        return None


def _build_match_stack(*, clock: _Clock | None = None, content_guard=None, lobby_cache_ttl_sec: float = 0.0):
    protocol = Protocol()
    tables = ConfigTables()
    clk = clock or _Clock()
    key = Ed25519PrivateKey.generate()
    verifier = CallerVerifier((key.public_key(),))
    store = MemoryMatchStore(protocol, clock_ms=clk)
    config = MatchConfig(
        battle_outcome_secret=_OUTCOME_SECRET,
        spectate_secret=_GATE_SECRET,
        table_fingerprint_mode="off",
    )
    outcomes: list[tuple[int, int]] = []
    battle_node = BattleNode(
        protocol,
        node_id=20,
        instance_id="battle-20",
        host="127.0.0.1",
        port=19020,
        ticket_secret=_BATTLE_SECRET,
        now_ms=clk,
        steady_ms=clk,
        on_outcome=lambda bid, out: outcomes.append((bid, out)),
        tables=tables,
        fingerprint_mode="off",
    )
    rpc = _InProcessRpc(protocol, battle_node)
    presence = MemoryPresenceProvider()
    for pid, name in ((1001, "残局宗师甲"), (1002, "破局侠客乙"), (1003, "围观路人丙")):
        presence.set_presence(
            PlayerPresence(
                player_id=pid,
                online=True,
                in_battle=False,
                zone_id=1,
                scene_node_id=10,
                scene_instance_id="scene-10",
                scene_grpc_endpoint="127.0.0.1:19010",
                session_id=(1 << 17) + pid,
                gate_node_id=1,
                gate_instance_id="gate-1",
                name=name,
            )
        )
    battle_nodes = StaticBattleNodeResolver(
        [
            BattleNodeEntry(
                node_id=20,
                node_uuid="battle-20",
                grpc_endpoint="127.0.0.1:19020",
                physical_region=1,
                client_host="127.0.0.1",
                client_port=19020,
                release_track="stable",
                admitting=True,
            )
        ]
    )
    gather = GatherPipeline(protocol, store, config, rpc, presence, battle_nodes)
    spectate = SpectateManager(protocol, store, config, rpc, presence, lobby_cache_ttl_sec=lobby_cache_ttl_sec)
    puzzles = TacticalPuzzleManager(
        protocol,
        store,
        author_name_lookup=lambda pid: _lookup_name(presence, pid),
        content_guard=content_guard,
        clock_ms=clk,
    )
    service = MatchGrpcService(
        protocol,
        store,
        config,
        gate_verifier=verifier,
        presence=presence,
        battle_nodes=battle_nodes,
        gather=gather,
        spectate=spectate,
        tactical_puzzles=puzzles,
        inline_gather=True,
        clock_ms=clk,
    )
    return SimpleNamespace(
        protocol=protocol,
        tables=tables,
        clock=clk,
        key=key,
        store=store,
        battle_node=battle_node,
        rpc=rpc,
        presence=presence,
        spectate=spectate,
        service=service,
        outcomes=outcomes,
    )


async def _lookup_name(presence: MemoryPresenceProvider, pid: int) -> str:
    return (await presence.get_presence(pid)).name


# ── 1. 万象残局坊 575..581 全链路测试 ────────────────────────────────────────────────────────────────


def test_tactical_puzzle_full_lifecycle_all_seven_rpcs():
    """验证 575..581 全部 7 条 RPC：建草稿 -> 作者自证发布与标准解回放 -> 挑战者破局与点赞 -> 编辑回落与删除。"""

    async def exercise():
        env = _build_match_stack()
        p, svc, key = env.protocol, env.service, env.key

        def ctx(pid: int, method: str):
            return _signed_ctx(p, key, f"/match.MatchService/{method}", pid)

        # 577 SaveTacticalPuzzleDraft：新建草稿
        save_req = p.message(
            "match.SaveTacticalPuzzleDraftRequest",
            puzzle_id=0,
            title="  三回合破盾斩将  ",
            hint_text="先手破盾再爆发",
            tag_mask=0b000011,
        )
        save_req.spec.CopyFrom(_valid_spec(p, round_limit=3, enemy_hp_pct=40, monster_id=311))
        save_resp = await svc.save_tactical_puzzle_draft(save_req, ctx(1001, "SaveTacticalPuzzleDraft"), "/match.MatchService/SaveTacticalPuzzleDraft")
        assert save_resp.error_message.id == 0
        puzzle_id = save_resp.puzzle.puzzle_id
        assert puzzle_id > 0
        assert save_resp.puzzle.state == STATE_DRAFT
        assert save_resp.puzzle.spec_revision == 1
        assert save_resp.puzzle.title == "三回合破盾斩将"
        assert save_resp.puzzle.author_name == "残局宗师甲"

        # 576 GetTacticalPuzzle：草稿仅作者可见，他人返回 NOT_FOUND
        get_author = await svc.get_tactical_puzzle(
            p.message("match.GetTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1001, "GetTacticalPuzzle"),
            "/match.MatchService/GetTacticalPuzzle",
        )
        assert get_author.error_message.id == 0 and get_author.puzzle.puzzle_id == puzzle_id

        get_other = await svc.get_tactical_puzzle(
            p.message("match.GetTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1002, "GetTacticalPuzzle"),
            "/match.MatchService/GetTacticalPuzzle",
        )
        assert get_other.error_message.id == ERR_MATCH_PUZZLE_NOT_FOUND

        # 579 StartTacticalPuzzleBattle：未发布前挑战者被拒；非作者发起自证被拒
        start_draft_challenger = await svc.start_tactical_puzzle_battle(
            p.message("match.StartTacticalPuzzleBattleRequest", puzzle_id=puzzle_id, is_author_verify=False),
            ctx(1002, "StartTacticalPuzzleBattle"),
            "/match.MatchService/StartTacticalPuzzleBattle",
        )
        assert start_draft_challenger.error_message.id == ERR_MATCH_PUZZLE_NOT_PUBLISHED

        start_fake_verify = await svc.start_tactical_puzzle_battle(
            p.message("match.StartTacticalPuzzleBattleRequest", puzzle_id=puzzle_id, is_author_verify=True),
            ctx(1002, "StartTacticalPuzzleBattle"),
            "/match.MatchService/StartTacticalPuzzleBattle",
        )
        assert start_fake_verify.error_message.id == ERR_MATCH_PUZZLE_NOT_OWNER

        # 579 作者自证开战：校验副本 314、确定性种子、自动录播与免费预设
        verify_req_id = str(uuid.uuid4())
        start_verify = await svc.start_tactical_puzzle_battle(
            p.message(
                "match.StartTacticalPuzzleBattleRequest",
                puzzle_id=puzzle_id,
                is_author_verify=True,
                request_id=verify_req_id,
            ),
            ctx(1001, "StartTacticalPuzzleBattle"),
            "/match.MatchService/StartTacticalPuzzleBattle",
        )
        assert start_verify.error_message.id == 0
        verify_battle_id = start_verify.battle_id
        assert start_verify.spec_revision == 1
        assert len(env.rpc.created_requests) == 1
        create_req = env.rpc.created_requests[0]
        assert create_req.battle_config_id == WORKSHOP_DUNGEON_ID
        assert create_req.seed == deterministic_puzzle_seed(puzzle_id, 1)
        assert create_req.record_replay is True
        assert create_req.ranked_context.use_free_preset is True
        assert create_req.tactical_puzzle_context.is_author_verify is True

        # 同 request_id 幂等重试返回同一 battle_id
        start_verify_dup = await svc.start_tactical_puzzle_battle(
            p.message(
                "match.StartTacticalPuzzleBattleRequest",
                puzzle_id=puzzle_id,
                is_author_verify=True,
                request_id=verify_req_id,
            ),
            ctx(1001, "StartTacticalPuzzleBattle"),
            "/match.MatchService/StartTacticalPuzzleBattle",
        )
        assert start_verify_dup.error_message.id == 0 and start_verify_dup.battle_id == verify_battle_id

        # 在真实 BattleNode 上直接完成该自证战斗并提交通关终局（含录像）
        room = env.battle_node.rooms[verify_battle_id]
        for actor in room.engine.actors.values():
            if actor.team_index == 1:
                actor.attributes.health = 1
                actor.ClearField("buffs")
        room.engine.actors[1001].is_auto = True
        env.battle_node._resolve(room)
        assert room.proposed_outcome == 1
        commit_req = env.battle_node.outcome_request(verify_battle_id)
        assert commit_req is not None and commit_req.HasField("replay") and commit_req.replay.payload
        commit_path = "/match.MatchInternal/CommitBattleOutcome"
        sign_service_call(commit_path, commit_req, "battle", env.clock(), _OUTCOME_SECRET)
        commit_resp = await svc.commit_battle_outcome(commit_req, _FakeContext(), commit_path)
        assert commit_resp.decision == OUTCOME_DECISION_COMPLETE
        env.battle_node.decide_outcome(verify_battle_id, committed=True)

        # 作者自证通关后：自动发布 + 钉住标准解回放 + 更新 author_best_rounds / global_best_rounds
        pub_view = await svc.get_tactical_puzzle(
            p.message("match.GetTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1002, "GetTacticalPuzzle"),
            "/match.MatchService/GetTacticalPuzzle",
        )
        assert pub_view.error_message.id == 0
        assert pub_view.puzzle.state == STATE_PUBLISHED
        assert pub_view.puzzle.has_solution_replay is True
        assert pub_view.puzzle.author_best_rounds == 1
        assert pub_view.puzzle.global_best_rounds == 1

        # 581 GetTacticalPuzzleSolutionReplay：挑战者凭 puzzle_id 读取标准解回放
        replay_resp = await svc.get_tactical_puzzle_solution_replay(
            p.message("match.GetTacticalPuzzleSolutionReplayRequest", puzzle_id=puzzle_id),
            ctx(1002, "GetTacticalPuzzleSolutionReplay"),
            "/match.MatchService/GetTacticalPuzzleSolutionReplay",
        )
        assert replay_resp.error_message.id == 0
        assert len(replay_resp.replay.payload) > 0

        # 580 LikeTacticalPuzzle：未通关不可点赞；作者不可自赞
        like_before_clear = await svc.like_tactical_puzzle(
            p.message("match.LikeTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1002, "LikeTacticalPuzzle"),
            "/match.MatchService/LikeTacticalPuzzle",
        )
        assert like_before_clear.error_message.id == ERR_MATCH_PUZZLE_CLEAR_REQUIRED_TO_LIKE

        like_own = await svc.like_tactical_puzzle(
            p.message("match.LikeTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1001, "LikeTacticalPuzzle"),
            "/match.MatchService/LikeTacticalPuzzle",
        )
        assert like_own.error_message.id == ERR_MATCH_PUZZLE_CANNOT_LIKE_OWN

        # 挑战者 1002 开战并破局（非自证局不录回放）
        start_chal = await svc.start_tactical_puzzle_battle(
            p.message("match.StartTacticalPuzzleBattleRequest", puzzle_id=puzzle_id, is_author_verify=False),
            ctx(1002, "StartTacticalPuzzleBattle"),
            "/match.MatchService/StartTacticalPuzzleBattle",
        )
        assert start_chal.error_message.id == 0
        chal_battle_id = start_chal.battle_id
        assert env.rpc.created_requests[-1].record_replay is False
        assert env.rpc.created_requests[-1].seed == deterministic_puzzle_seed(puzzle_id, 1)

        chal_room = env.battle_node.rooms[chal_battle_id]
        for actor in chal_room.engine.actors.values():
            if actor.team_index == 1:
                actor.attributes.health = 1
                actor.ClearField("buffs")
        chal_room.engine.actors[1002].is_auto = True
        env.battle_node._resolve(chal_room)
        chal_commit = env.battle_node.outcome_request(chal_battle_id)
        sign_service_call(commit_path, chal_commit, "battle", env.clock(), _OUTCOME_SECRET)
        await svc.commit_battle_outcome(chal_commit, _FakeContext(), commit_path)
        env.battle_node.decide_outcome(chal_battle_id, committed=True)

        # 580 通关后点赞 + 幂等重复点赞不累加
        like1 = await svc.like_tactical_puzzle(
            p.message("match.LikeTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1002, "LikeTacticalPuzzle"),
            "/match.MatchService/LikeTacticalPuzzle",
        )
        assert like1.error_message.id == 0 and like1.like_count == 1 and like1.self_liked is True
        like2 = await svc.like_tactical_puzzle(
            p.message("match.LikeTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1002, "LikeTacticalPuzzle"),
            "/match.MatchService/LikeTacticalPuzzle",
        )
        assert like2.error_message.id == 0 and like2.like_count == 1 and like2.self_liked is True

        # 575 ListTacticalPuzzles：验证 5 大页签、标签过滤与个人荣誉档案
        for tab in (TAB_HOT, TAB_NEWEST, TAB_HARDEST, TAB_MY_CLEARED):
            lst = await svc.list_tactical_puzzles(
                p.message("match.ListTacticalPuzzlesRequest", tab=tab, tag_filter_mask=0b000001, limit=10),
                ctx(1002, "ListTacticalPuzzles"),
                "/match.MatchService/ListTacticalPuzzles",
            )
            assert lst.error_message.id == 0 and lst.total_count == 1
            assert lst.puzzles[0].self_cleared is True and lst.puzzles[0].self_liked is True
            assert lst.puzzles[0].attempt_count == 1 and lst.puzzles[0].clear_count == 1
            assert lst.profile.total_cleared_count == 1
            assert lst.profile.honor_tier == 1 and lst.profile.honor_title == "初试棋局"

        author_list = await svc.list_tactical_puzzles(
            p.message("match.ListTacticalPuzzlesRequest", tab=TAB_MY_CREATED),
            ctx(1001, "ListTacticalPuzzles"),
            "/match.MatchService/ListTacticalPuzzles",
        )
        assert author_list.profile.published_count == 1
        assert author_list.profile.total_likes_received == 1
        assert author_list.profile.honor_tier == 2 and author_list.profile.honor_title == "妙手破局"

        # 编辑已发布残局：spec_revision 递增，状态回落 DRAFT，释放标准解回放
        edit_req = p.message(
            "match.SaveTacticalPuzzleDraftRequest",
            puzzle_id=puzzle_id,
            title="四回合奇门残局",
            hint_text="留怒破阵",
            tag_mask=0b000100,
        )
        edit_req.spec.CopyFrom(_valid_spec(p, round_limit=4, enemy_hp_pct=60, monster_id=312))
        edit_resp = await svc.save_tactical_puzzle_draft(
            edit_req, ctx(1001, "SaveTacticalPuzzleDraft"), "/match.MatchService/SaveTacticalPuzzleDraft"
        )
        assert edit_resp.error_message.id == 0
        assert edit_resp.puzzle.state == STATE_DRAFT
        assert edit_resp.puzzle.spec_revision == 2
        assert edit_resp.puzzle.has_solution_replay is False

        no_replay = await svc.get_tactical_puzzle_solution_replay(
            p.message("match.GetTacticalPuzzleSolutionReplayRequest", puzzle_id=puzzle_id),
            ctx(1001, "GetTacticalPuzzleSolutionReplay"),
            "/match.MatchService/GetTacticalPuzzleSolutionReplay",
        )
        assert no_replay.error_message.id == ERR_MATCH_PUZZLE_SOLUTION_REPLAY_NOT_FOUND

        # 578 DeleteTacticalPuzzle：非作者被拒，作者成功删除
        del_other = await svc.delete_tactical_puzzle(
            p.message("match.DeleteTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1002, "DeleteTacticalPuzzle"),
            "/match.MatchService/DeleteTacticalPuzzle",
        )
        assert del_other.error_message.id == ERR_MATCH_PUZZLE_NOT_OWNER

        del_ok = await svc.delete_tactical_puzzle(
            p.message("match.DeleteTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1001, "DeleteTacticalPuzzle"),
            "/match.MatchService/DeleteTacticalPuzzle",
        )
        assert del_ok.error_message.id == 0

        after_del = await svc.get_tactical_puzzle(
            p.message("match.GetTacticalPuzzleRequest", puzzle_id=puzzle_id),
            ctx(1001, "GetTacticalPuzzle"),
            "/match.MatchService/GetTacticalPuzzle",
        )
        assert after_del.error_message.id == ERR_MATCH_PUZZLE_NOT_FOUND

    asyncio.run(exercise())


def test_tactical_puzzle_validation_content_guard_and_limit():
    """验证残局文本/标签/题面规格边界、敏感词 fail-closed、10 题上限与荣誉档位派生。"""

    async def exercise():
        guard_mode = {"state": "pass"}

        def fake_guard(_pid: int, text: str) -> tuple[bool, bool]:
            if guard_mode["state"] == "unavailable":
                return False, True
            if "违禁" in text:
                return False, False
            return True, False

        env = _build_match_stack(content_guard=fake_guard)
        p, svc, key = env.protocol, env.service, env.key
        path = "/match.MatchService/SaveTacticalPuzzleDraft"

        def c1():
            return _signed_ctx(p, key, path, 1001)

        # 标题过短 / 含控制字符
        bad_short = p.message("match.SaveTacticalPuzzleDraftRequest", title="短标", spec=_valid_spec(p))
        assert (await svc.save_tactical_puzzle_draft(bad_short, c1(), path)).error_message.id == ERR_MATCH_PUZZLE_TEXT_REJECTED

        bad_ctrl = p.message("match.SaveTacticalPuzzleDraftRequest", title="残局\n注入标题", spec=_valid_spec(p))
        assert (await svc.save_tactical_puzzle_draft(bad_ctrl, c1(), path)).error_message.id == ERR_MATCH_PUZZLE_TEXT_REJECTED

        # 标签超过 2 个或越界位
        bad_tags = p.message("match.SaveTacticalPuzzleDraftRequest", title="合法残局标题", tag_mask=0b111, spec=_valid_spec(p))
        assert (await svc.save_tactical_puzzle_draft(bad_tags, c1(), path)).error_message.id == ERR_MATCH_PUZZLE_SPEC_INVALID

        # 题面回合上限越界（7 > 6）
        bad_spec = p.message("match.SaveTacticalPuzzleDraftRequest", title="合法残局标题", spec=_valid_spec(p, round_limit=7))
        assert (await svc.save_tactical_puzzle_draft(bad_spec, c1(), path)).error_message.id == ERR_MATCH_PUZZLE_SPEC_INVALID

        # 内容安全拒绝与服务不可达 fail-closed
        bad_text = p.message("match.SaveTacticalPuzzleDraftRequest", title="含违禁词残局", spec=_valid_spec(p))
        assert (await svc.save_tactical_puzzle_draft(bad_text, c1(), path)).error_message.id == ERR_MATCH_PUZZLE_TEXT_REJECTED

        guard_mode["state"] = "unavailable"
        unavail_req = p.message("match.SaveTacticalPuzzleDraftRequest", title="正常合法标题", spec=_valid_spec(p))
        assert (await svc.save_tactical_puzzle_draft(unavail_req, c1(), path)).error_message.id == ERR_MATCH_INTERNAL
        guard_mode["state"] = "pass"

        # 每人最多保留 10 道残局
        for idx in range(10):
            req = p.message("match.SaveTacticalPuzzleDraftRequest", title=f"残局考验第{idx + 1}关", spec=_valid_spec(p))
            assert (await svc.save_tactical_puzzle_draft(req, c1(), path)).error_message.id == 0
        req11 = p.message("match.SaveTacticalPuzzleDraftRequest", title="残局考验第十一关", spec=_valid_spec(p))
        assert (await svc.save_tactical_puzzle_draft(req11, c1(), path)).error_message.id == ERR_MATCH_PUZZLE_LIMIT_REACHED

        # 荣誉档位派生覆盖全 5 档
        assert derive_honor(0, 0, 0) == (0, "")
        assert derive_honor(0, 1, 0) == (1, "初试棋局")
        assert derive_honor(1, 0, 0) == (2, "妙手破局")
        assert derive_honor(1, 0, 5) == (3, "破局国手")
        assert derive_honor(3, 0, 20) == (4, "奇局宗师")
        assert derive_honor(0, 30, 0) == (4, "奇局宗师")

    asyncio.run(exercise())


# ── 2. 战斗引擎残局规则与未成年人防沉迷直连截止测试 ──────────────────────────────────────────────────


def test_battle_engine_tactical_puzzle_setup_round_limit_loss_and_no_rewards():
    """验证战斗引擎残局初始化（气血/法力/护盾/增减益/敌方速度与阵法）、回合耗尽判负与通关零掉落。"""
    protocol = Protocol()
    tables = ConfigTables()
    clock = _Clock(10_000)
    node = BattleNode(
        protocol,
        node_id=21,
        instance_id="battle-21",
        host="127.0.0.1",
        port=19021,
        ticket_secret=_BATTLE_SECRET,
        now_ms=clock,
        steady_ms=clock,
        on_outcome=lambda *_: None,
        tables=tables,
        fingerprint_mode="off",
    )

    def _make_puzzle_create_req(battle_id: int, *, round_limit: int = 1, enemy_hp_pct: int = 140, use_free_preset: bool = True):
        req = protocol.message(
            "CreateBattleRequest",
            battle_id=battle_id,
            battle_config_id=WORKSHOP_DUNGEON_ID,
            seed=deterministic_puzzle_seed(7001, 1),
            match_mode=MODE_PVE_SOLO,
            created_at_ms=clock(),
            deadline_ms=clock() + 120_000,
            required_physical_region=1,
            placement_operation_id=str(uuid.uuid4()),
            target_instance_id="battle-21",
            admit_before_ms=clock() + 30_000,
            outcome_protocol_version=1,
            spectate_visibility=SPECTATE_VISIBILITY_PRIVATE,
            record_replay=True,
        )
        req.ranked_context.use_free_preset = use_free_preset
        req.tactical_puzzle_context.puzzle_id = 7001
        req.tactical_puzzle_context.spec_revision = 1
        req.tactical_puzzle_context.is_author_verify = True
        req.tactical_puzzle_context.puzzle_title = "残局回合门禁"
        req.tactical_puzzle_context.spec.CopyFrom(
            _valid_spec(protocol, round_limit=round_limit, enemy_hp_pct=enemy_hp_pct, monster_id=311)
        )
        p1 = req.players.add(player_id=501, player_name="棋手", level=30, class_id=1, team_index=0)
        p1.base_attributes.health = 1000
        p1.base_attributes.mana = 500
        p1.base_attributes.strength = 20
        p1.base_attributes.armor = 40
        p1.base_attributes.speed = 100
        p1.routing.session_id = 100
        p1.routing.gate_node_id = 1
        p1.routing.gate_instance_id = "gate-1"
        p1.routing.scene_node_id = 10
        p1.routing.scene_instance_id = "scene-10"
        return req

    # 门禁：非免费预设被拒绝建房
    bad_req = _make_puzzle_create_req(8001, use_free_preset=False)
    assert node.create(bad_req).error_message.id != 0

    # 合法残局建房（1 回合上限 + 敌方 140% 气血）
    req = _make_puzzle_create_req(8002, round_limit=1, enemy_hp_pct=140, use_free_preset=True)
    assert node.create(req).error_message.id == 0
    room = node.rooms[8002]
    engine = room.engine
    assert engine.is_tactical_puzzle_battle is True
    assert engine.max_rounds == 1
    assert engine.team_formation_ids == [2, 0]

    # 校验玩家气血 60%、法力 80%、护盾 20% (100400) 与玄毒减益 (100190)
    player_actor = engine.actors[501]
    assert player_actor.attributes.health == (player_actor.max_health * 60) // 100
    assert player_actor.attributes.mana == (player_actor.max_mana * 80) // 100
    player_buff_ids = {b.buff_table_id for b in player_actor.buffs}
    assert TACTICAL_PUZZLE_SHIELD_BUFF_ID in player_buff_ids
    assert TACTICAL_PUZZLE_DEBUFF_POISON_BUFF_ID in player_buff_ids

    # 校验敌方单位气血放大至 140%（max_health 同步抬升）、护盾与固守增益 (100140)
    monster_actor = next(a for a in engine.actors.values() if a.team_index == 1)
    monster_row = tables["Monster"].get(311)
    expected_hp = (int(monster_row.health) * 140) // 100
    assert monster_actor.attributes.health == expected_hp
    assert monster_actor.max_health == expected_hp
    monster_buff_ids = {b.buff_table_id for b in monster_actor.buffs}
    assert TACTICAL_PUZZLE_SHIELD_BUFF_ID in monster_buff_ids
    assert TACTICAL_PUZZLE_BUFF_DEF_UP_BUFF_ID in monster_buff_ids

    # State snapshot 包含 tactical_puzzle_context
    snap = engine.snapshot(0, 501)
    assert snap.tactical_puzzle_context.puzzle_id == 7001

    # 玩家防御耗尽第 1 回合 -> 权威判负（SIDE_B_WIN = 2，绝不判平）
    engine.pending[501] = protocol.message("BattleAction", action_type=3)  # DEFEND
    node._resolve(room)
    assert engine.outcome == 2

    # 另一场残局通关：验证零 PVE 奖励掉落且结算携带 tactical_puzzle_context
    win_req = _make_puzzle_create_req(8003, round_limit=3, enemy_hp_pct=10, use_free_preset=True)
    win_req.players[0].base_attributes.strength = 5000
    assert node.create(win_req).error_message.id == 0
    win_room = node.rooms[8003]
    for a in win_room.engine.actors.values():
        if a.team_index == 1:
            a.attributes.health = 1
            a.ClearField("buffs")
    win_room.engine.actors[501].is_auto = True
    node._resolve(win_room)
    assert win_room.engine.outcome == 1
    result_event = protocol.parse("contracts.kafka.BattleResultEvent", win_room.outcome_payload)
    assert result_event.outcome == 1
    st = result_event.settlements[0]
    assert st.exp_gain == 0 and len(st.currency_gains) == 0 and len(st.items_gained) == 0
    assert st.tactical_puzzle_context.puzzle_id == 7001


def test_battle_minor_play_deadline_ticket_cap_attach_reject_and_tick_close():
    """验证未成年人战斗直连截止时刻：票据封顶、到点 tick 断连且战斗继续、到点后拒握及观众限制。"""
    protocol = Protocol()
    tables = ConfigTables()
    clock = _Clock(100_000)
    pushes: list[tuple[int, int, object]] = []
    node = BattleNode(
        protocol,
        node_id=22,
        instance_id="battle-22",
        host="127.0.0.1",
        port=19022,
        ticket_secret=_BATTLE_SECRET,
        now_ms=clock,
        steady_ms=clock,
        on_lobby_push=lambda _r, pid, mid, msg: pushes.append((pid, mid, msg)),
        on_outcome=lambda *_: None,
        tables=tables,
        fingerprint_mode="off",
    )

    # 纯函数边界校验
    assert minor_direct_close_at_ms(50_000) == 50_000 - MINOR_PLAY_CLOSE_LEAD_MS
    assert minor_direct_close_at_ms(3_000) == 0
    assert minor_play_deadline_reached(0, 999_999) is False
    assert minor_play_deadline_reached(120_000, 114_999) is False
    assert minor_play_deadline_reached(120_000, 115_000) is True
    assert minor_ticket_expire_at_ms(200_000, 120_000) == 115_000
    assert minor_ticket_expire_at_ms(110_000, 120_000) == 110_000

    # 建房：玩家 601 有未成年截止时刻 120_000（直连截止 115_000），房间截止 200_000
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=9001,
        battle_config_id=1,
        seed=42,
        match_mode=MODE_PVE_SOLO,
        created_at_ms=100_000,
        deadline_ms=200_000,
        required_physical_region=1,
        placement_operation_id=str(uuid.uuid4()),
        target_instance_id="battle-22",
        admit_before_ms=130_000,
        outcome_protocol_version=1,
        spectate_visibility=SPECTATE_VISIBILITY_PUBLIC,
    )
    p1 = req.players.add(
        player_id=601,
        player_name="少年侠客",
        level=10,
        team_index=0,
        minor_play_deadline_ms=120_000,
    )
    p1.base_attributes.health = 500
    p1.base_attributes.strength = 30
    p1.base_attributes.speed = 60
    p1.routing.session_id = 10
    p1.routing.gate_node_id = 1
    p1.routing.gate_instance_id = "gate-1"
    p1.routing.scene_node_id = 10
    p1.routing.scene_instance_id = "scene-10"
    assert node.create(req).error_message.id == 0

    # 校验下发给 601 的票据期限被封顶在 115_000（而非 200_000）
    assign_msg = next(msg for pid, mid, msg in pushes if pid == 601 and mid == 177)
    assert assign_msg.expire_at_ms == 115_000

    # 未到点前可正常挂接直连
    ticket = node.authorize(bytes(assign_msg.token_payload), bytes(assign_msg.token_signature))
    closed = {"flag": False}
    node.attach(ticket, lambda *_: None, lambda: closed.__setitem__("flag", True))
    assert 601 in node.rooms[9001].connections

    # 添加未成年观众 602（截止时刻 112_000 -> 直连截止 107_000）
    obs_op = str(uuid.uuid4())
    obs_req = protocol.message(
        "AddObserverRequest",
        battle_id=9001,
        observer_player_id=602,
        observer_name="小观众",
        spectator_operation_id=obs_op,
        admit_before_ms=120_000,
        target_instance_id="battle-22",
        minor_play_deadline_ms=112_000,
    )
    obs_req.routing.gate_node_id = 1
    obs_req.routing.gate_instance_id = "gate-1"
    assert node.add_observer(obs_req).error_message.id == 0
    obs_assign = next(msg for pid, mid, msg in pushes if pid == 602 and mid == 177)
    assert obs_assign.expire_at_ms == 107_000

    # 推进墙钟至 115_000：tick 强制断开 601 直连，但房间仍在运行；此后 601 不可再挂接直连
    clock.now = 115_000
    node.tick()
    assert closed["flag"] is True
    assert 9001 in node.rooms
    assert 601 not in node.rooms[9001].connections
    with pytest.raises(BattleError):
        node.attach(ticket, lambda *_: None, lambda: None)


# ── 3. Gate 准入/心跳/登录期限/租约围栏/2049 踢线与 Match 观战/排位加固测试 ────────────────────────


def test_gate_preauth_admission_login_deadline_and_event_fences():
    """验证 Gate 未验签池预算、验签前心跳/超长帧拦截、单票单连、登录后超时、租约代次围栏与 2049 立即下线。"""

    async def exercise():
        protocol = Protocol()
        info = SimpleNamespace(node_id=3, zone_id=1, node_uuid="gate-3", release_track="stable")

        async def noop_forward(*_args, **_kwargs):
            return None

        gate = GateNode(
            protocol,
            info,
            _GATE_SECRET,
            noop_forward,
            max_connections=4,
            max_pre_auth_connections=1,
            handshake_timeout=0.2,
            login_deadline_after_verify=0.15,
            pre_auth_max_message_bytes=512,
        )

        # 使用真实本地 loopback TCP 验证未验签预算与验签前心跳拒收
        server = await asyncio.start_server(gate.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            # 1) 验签前发送心跳 -> 断开并计 heartbeat_before_verify
            r1, w1 = await asyncio.open_connection("127.0.0.1", port)
            await write_packet(w1, Packet.from_message(protocol.message("ClientHeartbeatRequest", seq=1)))
            assert await read_packet(r1) is None
            w1.close()
            await w1.wait_closed()
            await asyncio.sleep(0.05)
            assert gate.admission_counters["heartbeat_before_verify"] >= 1

            # 2) 未验签连接池预算为 1：第 1 条连接占住未验签池后，第 2 条被 rejected_preauth_budget 拒绝
            r_hold, w_hold = await asyncio.open_connection("127.0.0.1", port)
            await asyncio.sleep(0.05)
            assert gate.pre_auth_count == 1
            r_rej, w_rej = await asyncio.open_connection("127.0.0.1", port)
            assert await read_packet(r_rej) is None
            w_rej.close()
            await w_rej.wait_closed()
            assert gate.admission_counters["rejected_preauth_budget"] >= 1
            w_hold.close()
            await w_hold.wait_closed()
            await asyncio.sleep(0.05)

            # 3) 单票单连 + 验签后不登录只发心跳仍触发 login_deadline_after_verify
            ticket = issue_gate_ticket(protocol, info.node_id, info.zone_id, _GATE_SECRET, ttl=600, player_id=77)
            r_a, w_a = await asyncio.open_connection("127.0.0.1", port)
            await write_packet(w_a, Packet.from_message(ticket))
            v_pkt = await read_packet(r_a)
            assert v_pkt is not None
            v_resp = protocol.parse("ClientTokenVerifyResponse", v_pkt.payload)
            assert v_resp.success is True

            # 同一张票开启第二条连接被拒（rejected_ticket_in_use）
            r_b, w_b = await asyncio.open_connection("127.0.0.1", port)
            await write_packet(w_b, Packet.from_message(ticket))
            v_pkt_b = await read_packet(r_b)
            assert v_pkt_b is not None
            v_resp_b = protocol.parse("ClientTokenVerifyResponse", v_pkt_b.payload)
            assert v_resp_b.success is False
            assert gate.admission_counters["rejected_ticket_in_use"] >= 1
            w_b.close()
            await w_b.wait_closed()

            # 持续发心跳但不登录 -> 超过 login_deadline_after_verify(0.15s) 被断开
            await write_packet(w_a, Packet.from_message(protocol.message("ClientHeartbeatRequest", seq=2)))
            hb_pkt = await read_packet(r_a)
            assert hb_pkt is not None and hb_pkt.type_name == "ClientHeartbeatResponse"
            await asyncio.sleep(0.22)
            assert await read_packet(r_a) is None
            assert gate.admission_counters["login_timeouts"] >= 1
            w_a.close()
            await w_a.wait_closed()
        finally:
            server.close()
            await server.wait_closed()
            await gate.close()

        # 4) PlayerLeaseExpiredEvent 代次围栏 & KickPlayerEvent(2049) 立即下线 & BoundPlayerIdOf 选角态隔离
        class _DummyTransport:
            def __init__(self) -> None:
                self.aborted = False

            def abort(self) -> None:
                self.aborted = True

            def get_write_buffer_size(self) -> int:
                return 0

        class _DummyWriter:
            def __init__(self) -> None:
                self.transport = _DummyTransport()
                self.closed = False
                self.frames: list[bytes] = []

            def is_closing(self) -> bool:
                return self.closed

            def write(self, data: bytes) -> None:
                self.frames.append(data)

            def close(self) -> None:
                self.closed = True

        w = _DummyWriter()
        sess = Session(
            session_id=(3 << 17) + 9,
            incarnation=42,
            writer=w,
            verified=True,
            player_id=900,
            account="minor_acc",
            scene_node_id=10,
            scene_id=1,
            ticket_account_digest=b"digest-123",
        )
        gate.sessions[sess.session_id] = sess

        # details() 透传 ticket_account_digest
        det = gate.details(sess, 1)
        if "ticket_account_digest" in det.DESCRIPTOR.fields_by_name:
            assert bytes(det.ticket_account_digest) == b"digest-123"

        # 旧代次 LeaseExpired 被忽略
        stale_lease = protocol.message(
            "contracts.kafka.PlayerLeaseExpiredEvent",
            session_id=sess.session_id,
            player_id=900,
            session_incarnation=41,
        )
        cmd_stale = protocol.message(
            "contracts.kafka.GateCommand",
            target_gate_id=info.node_id,
            target_instance_id=info.node_uuid,
            event_id=protocol.event_ids["ContractsKafkaPlayerLeaseExpiredEvent"],
            payload=stale_lease.SerializeToString(),
        )
        await gate.command(cmd_stale.SerializeToString())
        assert not sess.lease_expired and not w.transport.aborted

        # 选角态（scene_route_reset_pending=True 且 account=""）不收定向推送
        sess.scene_route_reset_pending = True
        sess.account = ""
        gate._deliver_to_session(sess.session_id, 900, protocol.content(143, 0))
        assert sess.outbound.buffered_bytes == 0
        sess.scene_route_reset_pending = False
        sess.account = "minor_acc"

        # 防沉迷 2049 踢线：置 exit_scene_immediately_on_close=True 且下发原因 2049
        kick_minor = protocol.message(
            "contracts.kafka.KickPlayerEvent",
            session_id=sess.session_id,
            player_id=900,
            reason_tip=LOGIN_MINOR_PLAY_TIME_OVER,
        )
        cmd_kick = protocol.message(
            "contracts.kafka.GateCommand",
            target_gate_id=info.node_id,
            target_instance_id=info.node_uuid,
            event_id=protocol.event_ids["ContractsKafkaKickPlayerEvent"],
            payload=kick_minor.SerializeToString(),
        )
        await gate.command(cmd_kick.SerializeToString())
        assert sess.exit_scene_immediately_on_close is True
        assert w.closed is True
        assert len(w.frames) == 1

    asyncio.run(exercise())


def test_match_spectate_visibility_lobby_cache_and_ranked_featured_cache_rollover():
    """验证观战可见性矩阵（帮战/高段公开排位/普通活动）、观战缓存与未成年截止透传、论剑缓存与换季释放。"""

    async def exercise():
        # 1) 观战可见性矩阵与回放录制策略
        assert spectate_visibility_for(MODE_GUILD_WAR_PVP, True, is_guild_war_activity=True) == SPECTATE_VISIBILITY_PUBLIC
        assert spectate_visibility_for(MODE_PVE_SOLO, True, is_guild_war_activity=False) == SPECTATE_VISIBILITY_PRIVATE
        assert spectate_visibility_for(MODE_PVP_CHALLENGE, False, allow_public_spectate=True) == SPECTATE_VISIBILITY_PUBLIC
        assert spectate_visibility_for(MODE_PVP_TEAM, False, ranked_public_spectate=True) == SPECTATE_VISIBILITY_PUBLIC
        assert spectate_visibility_for(MODE_PVP_TEAM, False, ranked_public_spectate=False) == SPECTATE_VISIBILITY_PRIVATE
        assert record_replay_for(MODE_PVE_SOLO, tactical_puzzle_author_verify=True) is True
        assert record_replay_for(MODE_PVE_SOLO, tactical_puzzle_author_verify=False) is False

        # 2) SpectateManager：高段公开排位字段填充、ranked_only 过滤、SpectateLobbyCache 命中与 forget、AddObserver 透传 minor_play_deadline_ms
        env = _build_match_stack(lobby_cache_ttl_sec=5.0)
        p, store, spectate = env.protocol, env.store, env.spectate
        env.presence.set_presence(
            PlayerPresence(
                player_id=1003,
                online=True,
                in_battle=False,
                zone_id=1,
                scene_node_id=10,
                scene_instance_id="scene-10",
                scene_grpc_endpoint="127.0.0.1:19010",
                session_id=(1 << 17) + 1003,
                gate_node_id=1,
                gate_instance_id="gate-1",
                name="小观众丙",
                minor_play_deadline_ms=env.clock() + 30_000,
            )
        )

        # 写入一场公开的高段自养排位成局
        ranked_ctx = p.message(
            "RankedMatchContext",
            season_id=1,
            rule_version=2,
            team_size=1,
            use_free_preset=False,
            allow_public_spectate=True,
            peak_rating_centi=175_000,
        )
        formation = await store.claim_formation(
            mode=MODE_PVP_TEAM,
            config_id=5,
            members=[(1001, "tk-1", 0, True), (1002, "tk-2", 1, True)],
            physical_region=1,
            target_node_id=20,
            target_instance_id="battle-20",
            target_endpoint="127.0.0.1:19020",
            release_track="stable",
            battle_deadline_ms=120_000,
            requeue_on_failure=False,
            ranked_context=ranked_ctx.SerializeToString(deterministic=True),
            ranked_season_id=1,
            skip_ticket_check=True,
        )
        assert await env.service.gather.run_formation(formation) is True
        viewer = CallerIdentity(
            player_id=1003,
            account="acct_1003",
            owner_revision=1,
            session_id=(1 << 17) + 1003,
            gate_node_id=1,
            gate_instance_id="gate-1",
            session_incarnation=1003,
        )
        lst = await spectate.list_watchable_battles(viewer, 10, ranked_only=True)
        assert len(lst.battles) == 1
        assert lst.battles[0].battle_id == formation.battle_id
        if "ranked_peak_rating_centi" in lst.battles[0].DESCRIPTOR.fields_by_name:
            assert lst.battles[0].ranked_peak_rating_centi == 175_000

        # 观战接入透传 minor_play_deadline_ms 到 BattleNode.AddObserver
        watch_resp = await spectate.watch_battle(
            viewer, p.message("match.WatchBattleRequest", battle_id=formation.battle_id)
        )
        assert watch_resp.error_message.id == 0
        assert len(env.rpc.add_observer_requests) == 1
        assert env.rpc.add_observer_requests[0].minor_play_deadline_ms == env.clock() + 30_000

        # forget_battle 清空缓存
        spectate.forget_battle(formation.battle_id)
        assert spectate.lobby_cache.get("stable", True) is None

        # 3) RankedManager 热门流派缓存与换季释放代表战报 ref_count
        ranked = env.service.ranked
        season = await ranked.ensure_current_season()
        # 在 MemoryMatchStore 放一条代表战报并模拟被 featured_build 钉住
        env.store._state.reports[7777] = SimpleNamespace(
            battle_id=7777,
            created_ms=env.clock(),
            match_mode=MODE_PVP_TEAM,
            battle_config_id=5,
            envelope=b"replay-bytes",
            envelope_digest=b"0" * 32,
            ref_count=1,
        )
        await env.store.upsert_ranked_featured_build(
            season_id=season.season_id,
            build_key="seal1:pet201:10002",
            season_seal_id=1,
            starter_pet_table_id=201,
            core_skill_ids_csv="10002",
            tier_qingfeng_centi=152_000,
            battle_id=7777,
            player_id=1001,
            player_name="残局宗师甲",
            after_rating_centi=175_000,
            team_size=1,
            total_rounds=5,
            player_won=True,
            settled_at_ms=env.clock(),
        )
        prof = await ranked.get_ranked_profile(viewer, p.message("match.GetRankedProfileRequest"))
        assert len(prof.featured_builds) == 1
        assert ranked.featured_cache.get(season.season_id, 5) is not None

        released = await ranked.release_season_featured_replays(season.season_id)
        assert released == 1
        assert ranked.featured_cache.get(season.season_id, 5) is None
        assert 7777 not in env.store._state.reports

    asyncio.run(exercise())


def test_tactical_puzzle_v15_leyline_and_banned_skill():
    """验证万象残局坊 V1.5 盘面加深：五行灵脉（含土厚开局护盾）、预设单招禁用与越界/跨预设拒绝。"""
    from xuanming.match.tactical_puzzle import validate_tactical_puzzle_spec
    from xuanming.nodes.battle_dropin_effects import (
        TOURNAMENT_LEYLINE_EARTH,
        TOURNAMENT_LEYLINE_METAL,
        is_valid_tactical_puzzle_spec,
    )

    protocol = Protocol()
    tables = ConfigTables()
    clock = _Clock(20_000)
    node = BattleNode(
        protocol,
        node_id=23,
        instance_id="battle-23",
        host="127.0.0.1",
        port=19023,
        ticket_secret=_BATTLE_SECRET,
        now_ms=clock,
        steady_ms=clock,
        on_outcome=lambda *_: None,
        tables=tables,
        fingerprint_mode="off",
    )

    # 1. 纯规格校验：合法人物禁招(10039) & 灵宠禁招(13005)通过；跨预设(10020)、无宠禁宠招(13005)、越界灵脉(6)拒绝
    valid_char_ban = _valid_spec(protocol, round_limit=2)
    valid_char_ban.leyline_element = TOURNAMENT_LEYLINE_METAL
    valid_char_ban.banned_skill_id = 10039
    assert validate_tactical_puzzle_spec(valid_char_ban) is True
    assert is_valid_tactical_puzzle_spec(valid_char_ban) is True

    valid_pet_ban = _valid_spec(protocol, round_limit=2)
    valid_pet_ban.leyline_element = TOURNAMENT_LEYLINE_EARTH
    valid_pet_ban.banned_skill_id = 13005
    assert validate_tactical_puzzle_spec(valid_pet_ban) is True
    assert is_valid_tactical_puzzle_spec(valid_pet_ban) is True

    bad_cross_preset = _valid_spec(protocol, round_limit=2)
    bad_cross_preset.banned_skill_id = 10020  # 属于 102 控场，不属于 101 破阵
    assert validate_tactical_puzzle_spec(bad_cross_preset) is False
    assert is_valid_tactical_puzzle_spec(bad_cross_preset) is False

    bad_no_pet_ban = _valid_spec(protocol, round_limit=2)
    bad_no_pet_ban.pet_preset_id = 0
    bad_no_pet_ban.banned_skill_id = 13005  # 属于 201 灵宠，但当前无宠
    assert validate_tactical_puzzle_spec(bad_no_pet_ban) is False
    assert is_valid_tactical_puzzle_spec(bad_no_pet_ban) is False

    bad_leyline = _valid_spec(protocol, round_limit=2)
    bad_leyline.leyline_element = 6
    assert validate_tactical_puzzle_spec(bad_leyline) is False
    assert is_valid_tactical_puzzle_spec(bad_leyline) is False

    # 2. BattleNode 建房端到端：金锐灵脉 + 禁招 10039 生效且写入快照；土厚灵脉赋予开场护盾；非法禁招拒建
    def _build_req(battle_id: int, spec):
        req = protocol.message(
            "CreateBattleRequest",
            battle_id=battle_id,
            battle_config_id=WORKSHOP_DUNGEON_ID,
            seed=deterministic_puzzle_seed(7701, 1),
            match_mode=MODE_PVE_SOLO,
            created_at_ms=clock(),
            deadline_ms=clock() + 120_000,
            required_physical_region=1,
            placement_operation_id=str(uuid.uuid4()),
            target_instance_id="battle-23",
            admit_before_ms=clock() + 30_000,
            outcome_protocol_version=1,
            spectate_visibility=SPECTATE_VISIBILITY_PRIVATE,
            record_replay=True,
        )
        req.ranked_context.use_free_preset = True
        req.tactical_puzzle_context.puzzle_id = 7701
        req.tactical_puzzle_context.spec_revision = 1
        req.tactical_puzzle_context.is_author_verify = True
        req.tactical_puzzle_context.puzzle_title = "V1.5灵脉禁招局"
        req.tactical_puzzle_context.spec.CopyFrom(spec)
        p1 = req.players.add(player_id=601, player_name="棋手", level=30, class_id=1, team_index=0)
        p1.base_attributes.health = 1000
        p1.base_attributes.mana = 500
        p1.base_attributes.strength = 100
        p1.base_attributes.speed = 120
        p1.routing.session_id = 200
        p1.routing.gate_node_id = 1
        p1.routing.gate_instance_id = "gate-1"
        p1.routing.scene_node_id = 10
        p1.routing.scene_instance_id = "scene-10"
        return req

    assert node.create(_build_req(9101, bad_cross_preset)).error_message.id != 0

    assert node.create(_build_req(9102, valid_char_ban)).error_message.id == 0
    eng_metal = node.rooms[9102].engine
    assert eng_metal.active_leyline == TOURNAMENT_LEYLINE_METAL
    assert eng_metal.is_skill_banned(10039) is True
    assert 10039 in list(eng_metal.snapshot(0, 601).banned_skill_ids)

    earth_spec = _valid_spec(protocol, round_limit=2)
    earth_spec.formation_id = 0
    earth_spec.leyline_element = TOURNAMENT_LEYLINE_EARTH
    earth_spec.banned_skill_id = 13005
    assert node.create(_build_req(9103, earth_spec)).error_message.id == 0
    eng_earth = node.rooms[9103].engine
    assert eng_earth.active_leyline == TOURNAMENT_LEYLINE_EARTH
    assert eng_earth.is_skill_banned(13005) is True
    assert eng_earth.tournament_earth_shield_of(601) > 0
