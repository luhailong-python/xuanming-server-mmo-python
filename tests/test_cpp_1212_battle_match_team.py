"""Battle V5、Match V4 与 Team V4 跨版本对账全量单测（对应 C++ 77e2e06d2..1212e017e）。"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass

import pytest

from xuanming.data import ConfigTables
from xuanming.grpc_runtime import session_metadata
from xuanming.login.caller_auth import CallerVerifier
from xuanming.match.config import MatchConfig
from xuanming.match.errors import (
    ERR_INVALID_PARAMETER,
    ERR_MATCH_TOURNAMENT_MATCH_NOT_READY,
    ERR_MATCH_TOURNAMENT_NOT_CAPTAIN,
    ERR_RATE_LIMIT_EXCEEDED,
    ERR_TEAM_NOT_LEADER,
)
from xuanming.match.gather import GatherPipeline, MemoryPresenceProvider, StaticBattleNodeResolver
from xuanming.match.match_service import MatchGrpcService
from xuanming.match.model import (
    MODE_1V1,
    MODE_PVE_SOLO,
    MODE_PVP_TEAM,
    MODE_SEASON_TOURNAMENT,
    BattleNodeEntry,
    CallerIdentity,
    PlayerPresence,
    validate_custom_duel_rules,
)
from xuanming.match.ranked import (
    RANKED_MATCH_RATING_CONFIRMED,
    RankedManager,
    RankedRuleConfig,
)
from xuanming.match.spectate import SpectateManager
from xuanming.match.store import MemoryMatchStore
from xuanming.match.team_service import (
    MSG_NOTIFY_TEAM_EVENT,
    MSG_NOTIFY_TEAM_SNAPSHOT,
    SERVICE_CLIENT_TEAM,
    TeamGrpcService,
)
from xuanming.match.tournament import (
    HONOR_BIT_CHAMPION,
    HONOR_BIT_DEBUT,
    HONOR_BIT_SWEEP,
    LEYLINE_EARTH,
    LEYLINE_FIRE,
    LEYLINE_METAL,
    LEYLINE_WATER,
    LEYLINE_WOOD,
    MATCH_FINISHED,
    MATCH_IN_BATTLE,
    MATCH_WAITING_OPPONENT,
    OUTCOME_SIDE_A_WIN,
    OUTCOME_SIDE_B_WIN,
    PHASE_KNOCKOUT,
    PHASE_QUALIFIER,
    STAGE_KNOCKOUT,
    STAGE_QUALIFIER,
    TournamentCatalog,
    TournamentManager,
    draw_leylines_with_bans,
)
from xuanming.nodes.battle import CLIENT_REQUESTS, BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BOSS_AFFIX_EARTH,
    BOSS_AFFIX_FIRE,
    BOSS_AFFIX_METAL,
    BOSS_AFFIX_WATER,
    BOSS_AFFIX_WOOD,
    BOSS_TRIAL_KIND_BREAK_REJUV,
    BOSS_TRIAL_KIND_BREAK_STANCE,
    ERR_SKILL_BANNED_IN_BATTLE,
    EVENT_SUMMON_PET,
    MATCH_MODE_PVP_CHALLENGE,
    MATCH_MODE_SEASON_TOURNAMENT,
    PVP_SUMMON_PET_UNLOCK_ROUND,
    TACTICAL_PREP_WITH_BAN_DURATION_SECONDS,
    TACTICAL_PREP_WITH_BAN_TIMEOUT_MS,
    BattleEffectOp,
    BattleModifier,
    SeasonBattleRules,
)
from xuanming.nodes.battle_replay import decode_replay
from xuanming.nodes.battle_tables import BattleBuff, BattleSkill, BattleTables
from xuanming.protocol import Protocol


class _FakeClock:
    def __init__(self, initial_ms: int = 1_750_000_000_000) -> None:
        self.now = initial_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> None:
        self.now += delta_ms


class _FakeContext:
    def __init__(self, metadata: list[tuple[str, str | bytes]]) -> None:
        self._md = metadata

    def invocation_metadata(self):
        return self._md

    async def abort(self, code, details):
        raise RuntimeError(f"grpc abort {code}: {details}")


@dataclass
class _RecordingNotifier:
    pushes: list[tuple[int, int, object]]
    refreshes: list[int]

    async def push_to_player(self, player_id: int, message_id: int, payload_msg) -> None:
        self.pushes.append((player_id, message_id, payload_msg))

    async def refresh_scene(self, player_id: int) -> None:
        self.refreshes.append(player_id)


def _seal(protocol: Protocol, seal_id: int, effect_id: int, name: str, **extra):
    params = dict(
        season_id=1,
        seal_id=seal_id,
        effect_id=effect_id,
        bonus_bps=1200,
        cap_bps=600,
        per_round_limit=1,
        battle_limit=2,
        active_rounds=2 if effect_id == 6 else 0,
        health_threshold_bps=4000 if effect_id == 4 else 0,
        name=name,
    )
    params.update(extra)
    return protocol.message("SeasonSealEffect", **params)


def _pet(
    protocol: Protocol,
    pet_id: int,
    owner_id: int,
    pet_table_id: int,
    name: str,
    *,
    hp: int = 3000,
    speed: int = 200,
    skills: tuple[int, ...] = (),
):
    p = protocol.message(
        "BattlePetSnapshot",
        pet_id=pet_id,
        owner_player_id=owner_id,
        pet_table_id=pet_table_id,
        pet_name=name,
        level=40,
        max_health=hp,
        max_mana=1000,
        physical_attack=300,
        magic_attack=300,
        defense=120,
    )
    p.base_attributes.health = hp
    p.base_attributes.mana = 1000
    p.base_attributes.speed = speed
    for sid in skills:
        p.skill_table_ids.append(sid)
        p.skill_ranks[sid] = 1
    return p


def _player(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    name: str | None = None,
    hp: int = 6000,
    mana: int = 2000,
    speed: int = 180,
    class_id: int = 1,
    skills: tuple[int, ...] = (),
    equipped_seal=None,
    available_seals=(),
    pets=(),
    standby_pets=(),
    is_team_captain: bool = False,
    season_weekly_affix_id: int = 0,
    ai_controlled: bool = False,
    with_routing: bool = True,
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=name or f"道友{player_id}",
        team_index=team_index,
        level=50,
        class_id=class_id,
        max_health=hp,
        max_mana=mana,
        physical_attack=450,
        magic_attack=450,
        defense=180,
        active_season_id=1,
        is_team_captain=is_team_captain,
        season_weekly_affix_id=season_weekly_affix_id,
        ai_controlled=ai_controlled,
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = mana
    snap.base_attributes.speed = speed
    if with_routing:
        snap.routing.gate_instance_id = "gate-1"
        snap.routing.scene_instance_id = "scene-1"
    for sid in skills:
        snap.skill_table_ids.append(sid)
        snap.skill_ranks[sid] = 1
    if equipped_seal is not None:
        snap.equipment_effects.season_seal.CopyFrom(equipped_seal)
    for seal in available_seals:
        snap.available_season_seals.add().CopyFrom(seal)
    for pet in pets:
        snap.pets.add().CopyFrom(pet)
    for sp in standby_pets:
        snap.standby_pets.add().CopyFrom(sp)
    return snap


def _create_ranked_request(
    protocol: Protocol,
    battle_id: int,
    players: list,
    *,
    now_ms: int = 20_000,
    skill_ban_slots: int = 0,
    win_streaks: dict[int, int] | None = None,
    rivalries: list[tuple[int, int, str, int, int, bool]] | None = None,
    seed: int = 20261212,
):
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        battle_config_id=5,
        seed=seed,
        match_mode=MODE_PVP_TEAM,
        created_at_ms=now_ms,
        deadline_ms=now_ms + 300_000,
        required_physical_region=1,
        placement_operation_id=f"op-{battle_id}",
        target_instance_id="battle-inst-1",
        admit_before_ms=now_ms + 30_000,
        outcome_protocol_version=1,
        spectate_visibility=2,
        record_replay=True,
    )
    req.ranked_context.season_id = 1
    req.ranked_context.rule_version = 2
    req.ranked_context.use_free_preset = False
    req.ranked_context.skill_ban_slots_per_team = skill_ban_slots
    for pid, streak in (win_streaks or {}).items():
        req.ranked_context.member_win_streaks[pid] = streak
    for pid, opp_id, opp_name, wins, losses, rev in (rivalries or ()):
        req.ranked_context.rivalry_encounters.add(
            player_id=pid,
            opponent_id=opp_id,
            opponent_name=opp_name,
            wins=wins,
            losses=losses,
            revenge_target=rev,
        )
    for p in players:
        req.players.add().CopyFrom(p)
    return req


def _make_node(protocol: Protocol, tables: ConfigTables, clock: _FakeClock, lobby_pushes: list, outcomes: list) -> BattleNode:
    return BattleNode(
        protocol,
        node_id=2,
        instance_id="battle-inst-1",
        host="127.0.0.1",
        port=19002,
        ticket_secret=b"T" * 32,
        now_ms=clock,
        steady_ms=clock,
        on_lobby_push=lambda _route, pid, mid, msg: lobby_pushes.append((pid, mid, msg)),
        on_outcome=lambda bid, out: outcomes.append((bid, out)),
        tables=tables,
    )


def _attach_player(node: BattleNode, protocol: Protocol, battle_id: int, player_id: int, inbox: list):
    issued = node.issue_ticket(
        protocol.message(
            "IssueBattleTicketRequest",
            battle_id=battle_id,
            player_id=player_id,
            target_instance_id=node.instance_id,
        )
    )
    tk = node.authorize(issued.assignment.token_payload, issued.assignment.token_signature)
    node.attach(tk, lambda mid, msg: inbox.append((mid, msg)), lambda: None)
    return tk


# ===========================================================================
# 1. Battle V5: 战前 3-Ban + 队长决胜 + 禁用强拦截(7028) + 25s 倒计时
# ===========================================================================


def test_battle_v5_pre_battle_3_ban_captain_tiebreak_and_7028_enforcement():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)
    candidates = bt.ban_eligible_skill_ids(active_season_id=1)
    assert len(candidates) >= 5
    s1, s2, s3, s4, s5 = candidates[:5]

    clock = _FakeClock(50_000)
    lobby_pushes: list = []
    outcomes: list = []
    node = _make_node(protocol, tables, clock, lobby_pushes, outcomes)

    # A 队 101(队长) + 102；B 队 201(队长) + 202
    p101 = _player(protocol, 101, 0, skills=(s1, s2, s3, s4), is_team_captain=True)
    p102 = _player(protocol, 102, 0, skills=(s1, s2, s3, s4))
    p201 = _player(protocol, 201, 1, skills=(s1, s2, s3, s5), is_team_captain=True)
    p202 = _player(protocol, 202, 1, skills=(s1, s2, s3, s5))
    req = _create_ranked_request(protocol, 55001, [p101, p102, p201, p202], now_ms=clock(), skill_ban_slots=3)

    res = node.create(req)
    assert not res.error_message.id
    room = node.rooms[55001]
    assert room.engine.is_in_prep_phase
    assert room.engine.prep_duration_seconds == TACTICAL_PREP_WITH_BAN_DURATION_SECONDS
    assert room.prep_deadline_ms == clock() + TACTICAL_PREP_WITH_BAN_TIMEOUT_MS

    inbox = {pid: [] for pid in (101, 102, 201, 202)}
    tks = {pid: _attach_player(node, protocol, 55001, pid, inbox[pid]) for pid in inbox}

    st101 = node.dispatch(tks[101], 140, protocol.message("GetBattleStateRequest", battle_id=55001))
    assert st101.prep_state.skill_ban_slots_per_team == 3
    assert st101.prep_state.duration_seconds == 25
    assert s1 in st101.prep_state.ban_candidate_skill_ids

    # 非法候选 / 重复提名 / 超额 4 招 -> 1005
    bad_dup = protocol.message(
        "UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True, banned_skill_ids=[s1, s1],
    )
    assert node.dispatch(tks[101], 504, bad_dup).error_message.id == 1005
    bad_over = protocol.message(
        "UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True, banned_skill_ids=[s1, s2, s3, s4],
    )
    assert node.dispatch(tks[101], 504, bad_over).error_message.id == 1005
    bad_unknown = protocol.message(
        "UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True, banned_skill_ids=[999999],
    )
    assert node.dispatch(tks[101], 504, bad_unknown).error_message.id == 1005

    # A 队：101(队长) 提名 [s1, s2, s3]，102 提名 [s1, s4, s5]
    # 得票：s1=2 票(第 1)，s2(slot1,队长) vs s4(slot1,非队长) -> 队长优先 s2 > s4；s3(slot2,队长) > s5(slot2,非队长)
    # 预期 A 队聚合：[s1, s2, s4]（因为 s4 slotRank=1 优于 s3 slotRank=2！）
    r101 = node.dispatch(
        tks[101], 504,
        protocol.message(
            "UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True,
            banned_skill_ids=[s1, s2, s3], ready=True,
        ),
    )
    assert not r101.error_message.id
    # 锁定后再改禁招被拒(1006)
    assert node.dispatch(
        tks[101], 504,
        protocol.message("UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True, banned_skill_ids=[s4]),
    ).error_message.id == 1006

    r102 = node.dispatch(
        tks[102], 504,
        protocol.message(
            "UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True,
            banned_skill_ids=[s1, s4, s5], ready=True,
        ),
    )
    assert not r102.error_message.id
    assert list(r102.prep_state.team_effective_ban_skill_ids) == [s1, s2, s4]

    # B 队锁阵只禁 [s5] -> 全员 ready 立即结算锁阵
    assert not node.dispatch(
        tks[201], 504,
        protocol.message(
            "UpdateBattlePrepRequest", battle_id=55001, update_banned_skills=True,
            banned_skill_ids=[s5], ready=True,
        ),
    ).error_message.id
    assert not node.dispatch(
        tks[202], 504, protocol.message("UpdateBattlePrepRequest", battle_id=55001, ready=True),
    ).error_message.id

    assert not room.engine.is_in_prep_phase
    live = node.dispatch(tks[101], 140, protocol.message("GetBattleStateRequest", battle_id=55001))
    assert list(live.team_a_banned_skill_ids) == [s1, s2, s4]
    assert list(live.team_b_banned_skill_ids) == [s5]
    assert sorted(live.banned_skill_ids) == sorted({s1, s2, s4, s5})

    # 尝试释放被禁技能 s1 -> 7028 (ERR_SKILL_BANNED_IN_BATTLE)
    cast_banned = protocol.message("SubmitBattleActionRequest", battle_id=55001)
    cast_banned.action.action_type = 2
    cast_banned.action.skill_table_id = s1
    cast_banned.action.target_id = 201
    assert node.dispatch(tks[101], 149, cast_banned).error_message.id == ERR_SKILL_BANNED_IN_BATTLE

    # 玄镜映法复制校验必须跳过被禁技能
    assert not room.engine.configured.is_skill_copyable_for(room.engine.actors[101], bt.skills[s1], s1)
    assert room.engine.configured.is_skill_copyable_for(room.engine.actors[101], bt.skills[s3], s3)

    # 自定义切磋规则：3-Ban 与 use_free_preset 互斥
    duel_bad = protocol.message("CustomDuelRules", use_free_preset=True, skill_ban_slots_per_team=3)
    assert not validate_custom_duel_rules(duel_bad)
    duel_ok = protocol.message("CustomDuelRules", use_free_preset=False, skill_ban_slots_per_team=3)
    assert validate_custom_duel_rules(duel_ok)


# ===========================================================================
# 2. Battle V5: 532/533 队内战术标点（同队推送、阵营校验、每回合限 2 次、回放裁剪）
# ===========================================================================


def test_battle_v5_tactical_ping_532_533_team_scoping_and_rate_limit():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(60_000)
    lobby_pushes: list = []
    outcomes: list = []
    node = _make_node(protocol, tables, clock, lobby_pushes, outcomes)
    assert CLIENT_REQUESTS[532] == "SendBattlePingRequest"

    p101 = _player(protocol, 101, 0)
    p102 = _player(protocol, 102, 0)
    p201 = _player(protocol, 201, 1)
    p202 = _player(protocol, 202, 1)
    req = _create_ranked_request(protocol, 55002, [p101, p102, p201, p202], now_ms=clock())
    assert not node.create(req).error_message.id

    inbox = {pid: [] for pid in (101, 102, 201, 202)}
    tks = {pid: _attach_player(node, protocol, 55002, pid, inbox[pid]) for pid in inbox}

    # 集火(1) 对友军非法 -> 1005；保人(3) 对敌军非法 -> 1005
    assert node.dispatch(
        tks[101], 532, protocol.message("SendBattlePingRequest", battle_id=55002, target_actor_id=102, kind=1),
    ).error_message.id == 1005
    assert node.dispatch(
        tks[101], 532, protocol.message("SendBattlePingRequest", battle_id=55002, target_actor_id=201, kind=3),
    ).error_message.id == 1005

    # 第 1 次：101 标点集火敌方 201 -> 仅同队 101/102 收到 533，敌方 201/202 零泄露
    ok1 = node.dispatch(
        tks[101], 532, protocol.message("SendBattlePingRequest", battle_id=55002, target_actor_id=201, kind=1),
    )
    assert not ok1.error_message.id
    assert ok1.ping.sender_player_id == 101 and ok1.ping.target_actor_id == 201 and ok1.ping.kind == 1
    assert any(mid == 533 for mid, _ in inbox[101])
    assert any(mid == 533 for mid, _ in inbox[102])
    assert not any(mid == 533 for mid, _ in inbox[201])
    assert not any(mid == 533 for mid, _ in inbox[202])

    # 重连快照同样仅对同队可见
    st102 = node.dispatch(tks[102], 140, protocol.message("GetBattleStateRequest", battle_id=55002))
    st201 = node.dispatch(tks[201], 140, protocol.message("GetBattleStateRequest", battle_id=55002))
    assert len(st102.team_pings) == 1 and st102.team_pings[0].target_actor_id == 201
    assert len(st201.team_pings) == 0

    # 第 2 次成功，第 3 次触发限流 1008
    assert not node.dispatch(
        tks[101], 532, protocol.message("SendBattlePingRequest", battle_id=55002, target_actor_id=102, kind=3),
    ).error_message.id
    assert node.dispatch(
        tks[101], 532, protocol.message("SendBattlePingRequest", battle_id=55002, target_actor_id=202, kind=2),
    ).error_message.id == ERR_RATE_LIMIT_EXCEEDED

    # 推进一回合后次数重置，可再次标点
    room = node.rooms[55002]
    room.engine.resolve()
    assert not node.dispatch(
        tks[101], 532, protocol.message("SendBattlePingRequest", battle_id=55002, target_actor_id=202, kind=2),
    ).error_message.id


# ===========================================================================
# 3. Battle V5: 临阵唤灵 (BATTLE_ACTION_SUMMON_PET=8 / BATTLE_EVENT_SUMMON_PET=24)
# ===========================================================================


def test_battle_v5_mid_battle_ladder_pet_summon_and_redaction():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(70_000)
    lobby_pushes: list = []
    outcomes: list = []
    node = _make_node(protocol, tables, clock, lobby_pushes, outcomes)

    pet1 = _pet(protocol, 7001, 101, 1, "灵狐", hp=3000, speed=260)
    pet2 = _pet(protocol, 7002, 101, 2, "石灵", hp=5200, speed=150)
    p101 = _player(protocol, 101, 0, pets=(pet1,), standby_pets=(pet2,))
    p201 = _player(protocol, 201, 1, pets=(_pet(protocol, 8001, 201, 3, "金猊", hp=3200, speed=240),))
    req = _create_ranked_request(protocol, 55003, [p101, p201], now_ms=clock())
    assert not node.create(req).error_message.id

    room = node.rooms[55003]
    inbox = {101: [], 201: []}
    tks = {pid: _attach_player(node, protocol, 55003, pid, inbox[pid]) for pid in (101, 201)}
    # 结束变阵进入第 1 回合
    assert not node.dispatch(tks[101], 504, protocol.message("UpdateBattlePrepRequest", battle_id=55003, ready=True)).error_message.id
    assert not node.dispatch(tks[201], 504, protocol.message("UpdateBattlePrepRequest", battle_id=55003, ready=True)).error_message.id
    assert not room.engine.is_in_prep_phase

    # 视角脱敏：101 见备选宠 7002 与剩余次数 1；201 与未脱敏回放均不可见
    st101 = node.dispatch(tks[101], 140, protocol.message("GetBattleStateRequest", battle_id=55003))
    st201 = node.dispatch(tks[201], 140, protocol.message("GetBattleStateRequest", battle_id=55003))
    assert st101.summon_pet_unlock_round == PVP_SUMMON_PET_UNLOCK_ROUND
    assert st101.self_summon_pet_remaining == 1
    assert [p.pet_id for p in st101.self_standby_pets] == [7002]
    assert st201.self_summon_pet_remaining == 0 and not st201.self_standby_pets

    # 第 1 回合禁止唤灵 -> 1006
    summon_req = protocol.message("SubmitBattleActionRequest", battle_id=55003)
    summon_req.action.action_type = 8
    summon_req.action.summon_pet_id = 7002
    assert node.dispatch(tks[101], 149, summon_req).error_message.id == 1006

    # 第 1 回合双方防御推进到第 2 回合
    defend = protocol.message("SubmitBattleActionRequest", battle_id=55003)
    defend.action.action_type = 3
    assert not node.dispatch(tks[101], 149, defend).error_message.id
    assert not node.dispatch(tks[201], 149, defend).error_message.id
    assert room.engine.round_index == 2

    old_pet = next(a for a in room.engine.actors.values() if a.actor_type == 3 and a.owner_player_id == 101)
    old_actor_id, old_slot = old_pet.actor_id, old_pet.formation_slot

    # 第 2 回合提交临阵唤灵 7002，201 防御 -> 结算产出 BATTLE_EVENT_SUMMON_PET(24) 且原位换新 actor_id
    assert not node.dispatch(tks[101], 149, summon_req).error_message.id
    assert not node.dispatch(tks[201], 149, defend).error_message.id
    assert room.engine.round_index == 3

    new_pet = next(a for a in room.engine.actors.values() if a.actor_type == 3 and a.owner_player_id == 101)
    assert new_pet.pet_id == 7002
    assert new_pet.actor_id != old_actor_id
    assert new_pet.formation_slot == old_slot
    assert room.engine.summon_pet_remaining(101) == 0

    turn_msgs = [msg for mid, msg in inbox[101] if mid == 139]
    assert any(ev.event_type == EVENT_SUMMON_PET and ev.source_id == 101 and ev.target_id == new_pet.actor_id for ev in turn_msgs[-1].events)

    # 第 3 回合配额已耗尽 -> 再次唤灵被拒(1006)
    assert node.dispatch(tks[101], 149, summon_req).error_message.id == 1006


# ===========================================================================
# 4. Battle V5: 赛季周词缀 (1..4) 与首领试炼周常五行词缀 (1..5) + 结算字段
# ===========================================================================


def test_battle_v5_season_weekly_affixes_and_boss_weekly_affixes():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)

    # 1) 赛季周词缀 2 (SpiritRecovery)：全员一致生效，防御回蓝 +300 bps；分歧则 fail-closed 归 0
    p1 = _player(protocol, 101, 0, mana=1000, season_weekly_affix_id=SeasonBattleRules.WEEKLY_AFFIX_SPIRIT_RECOVERY)
    p2 = _player(protocol, 201, 1, mana=1000, season_weekly_affix_id=SeasonBattleRules.WEEKLY_AFFIX_SPIRIT_RECOVERY)
    req_affix2 = _create_ranked_request(protocol, 55010, [p1, p2], now_ms=80_000)
    eng2 = TurnEngine(protocol, req_affix2, skills=bt.skills, items=bt.items, buffs=bt.buffs, max_rounds=10)
    assert eng2.active_season_weekly_affix_id == 2
    assert eng2.snapshot(0).season_weekly_affix_id == 2
    eng2.actors[101].attributes.mana = 500
    eng2.pending[101] = protocol.message("BattleAction", action_type=3)
    eng2.pending[201] = protocol.message("BattleAction", action_type=3)
    eng2.resolve()
    assert eng2.actors[101].attributes.mana == 530  # 500 + floor(1000 * 300 / 10000)

    # 分歧 fail-closed
    p2_diff = _player(protocol, 201, 1, season_weekly_affix_id=SeasonBattleRules.WEEKLY_AFFIX_OPENING_GUARD)
    eng_mismatch = TurnEngine(
        protocol, _create_ranked_request(protocol, 55011, [p1, p2_diff], now_ms=80_000),
        skills=bt.skills, items=bt.items, buffs=bt.buffs, max_rounds=10,
    )
    assert eng_mismatch.active_season_weekly_affix_id == 0

    # 2) 赛季周词缀 3 (OpeningGuard R1..2 -8%) vs 无词缀伤害对比
    def _single_attack_damage(affix_id: int) -> int:
        a = _player(protocol, 101, 0, season_weekly_affix_id=affix_id)
        b = _player(protocol, 201, 1, season_weekly_affix_id=affix_id)
        eng = TurnEngine(
            protocol, _create_ranked_request(protocol, 55012 + affix_id, [a, b], now_ms=80_000, seed=12345),
            skills=bt.skills, items=bt.items, buffs=bt.buffs, max_rounds=10,
        )
        eng.pending[101] = protocol.message("BattleAction", action_type=1, target_id=201)
        eng.pending[201] = protocol.message("BattleAction", action_type=3)
        turn = eng.resolve()
        return sum(ev.value for ev in turn.events if ev.event_type == 3 and ev.source_id == 101)

    dmg_none = _single_attack_damage(0)
    dmg_guard = _single_attack_damage(SeasonBattleRules.WEEKLY_AFFIX_OPENING_GUARD)
    dmg_sharp = _single_attack_damage(SeasonBattleRules.WEEKLY_AFFIX_SHARP_TEMPO)
    assert dmg_guard < dmg_none < dmg_sharp

    # 3) 首领试炼周常五行词缀：Metal 开局敌方 8% 护盾；Water 偶数回合末首领 +1 蓄势；非法词缀 6 拒绝
    boss_req = protocol.message(
        "CreateBattleRequest",
        battle_id=55020,
        battle_config_id=101,
        seed=777,
        match_mode=MODE_PVE_SOLO,
        created_at_ms=80_000,
        deadline_ms=380_000,
        boss_weekly_affix_id=BOSS_AFFIX_METAL,
    )
    boss_req.players.add().CopyFrom(_player(protocol, 101, 0))
    boss_mon = protocol.message("BattleActorState", level=50, monster_table_id=101, is_boss=True, max_health=10000)
    boss_mon.attributes.health = 10000
    boss_mon.attributes.speed = 100
    eng_metal = TurnEngine(
        protocol, boss_req, monsters=[boss_mon], skills=bt.skills, items=bt.items, buffs=bt.buffs,
        max_rounds=15, boss_trial_kind=BOSS_TRIAL_KIND_BREAK_STANCE, battle_difficulty=1,
    )
    boss_actor = next(a for a in eng_metal.actors.values() if a.is_boss)
    assert eng_metal.encounter.boss_weekly_affix_id == BOSS_AFFIX_METAL
    assert eng_metal.snapshot(0).boss_weekly_affix_id == BOSS_AFFIX_METAL
    assert eng_metal.opening_shield_by_actor.get(boss_actor.actor_id, 0) == 800  # 8% of 10000

    # Water 词缀：第 2 回合末首领 +1 蓄势
    water_req = protocol.message("CreateBattleRequest")
    water_req.CopyFrom(boss_req)
    water_req.battle_id = 55021
    water_req.boss_weekly_affix_id = BOSS_AFFIX_WATER
    eng_water = TurnEngine(
        protocol, water_req, monsters=[boss_mon], skills=bt.skills, items=bt.items, buffs=bt.buffs,
        max_rounds=15, boss_trial_kind=BOSS_TRIAL_KIND_BREAK_REJUV, battle_difficulty=1,
    )
    eng_water.pending[101] = protocol.message("BattleAction", action_type=3)
    eng_water.resolve()  # R1
    assert eng_water.encounter.boss_momentum_stacks == 0
    eng_water.pending[101] = protocol.message("BattleAction", action_type=3)
    eng_water.resolve()  # R2 偶数回合触发 +1 蓄势
    assert eng_water.encounter.boss_momentum_stacks == 1

    # 非法 boss_weekly_affix_id=6 -> BattleError
    bad_affix_req = protocol.message("CreateBattleRequest")
    bad_affix_req.CopyFrom(boss_req)
    bad_affix_req.boss_weekly_affix_id = 6
    with pytest.raises(BattleError):
        TurnEngine(protocol, bad_affix_req, monsters=[boss_mon], skills=bt.skills, items=bt.items, max_rounds=10)


def test_battle_v5_settlement_win_streak_rivalry_revenge_and_keep_on_death():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(90_000)
    lobby_pushes: list = []
    outcomes: list = []
    node = _make_node(protocol, tables, clock, lobby_pushes, outcomes)

    seal101 = _seal(protocol, 101, 1, "破阵锐印")
    p101 = _player(protocol, 101, 0, name="青衫", hp=6000, equipped_seal=seal101, skills=(10002,))
    p201 = _player(protocol, 201, 1, name="剑心", hp=1, skills=(10002,))
    req = _create_ranked_request(
        protocol, 55030, [p101, p201],
        now_ms=clock(),
        win_streaks={201: 4},
        rivalries=[(101, 201, "剑心", 1, 2, True)],
    )
    assert not node.create(req).error_message.id
    room = node.rooms[55030]
    assert room.engine.actors[201].win_streak == 4

    # 一回合秒杀对手，检验结算字段 37..50
    tks = {pid: _attach_player(node, protocol, 55030, pid, []) for pid in (101, 201)}
    atk = protocol.message("SubmitBattleActionRequest", battle_id=55030)
    atk.action.action_type = 1
    atk.action.target_id = 201
    defend = protocol.message("SubmitBattleActionRequest", battle_id=55030)
    defend.action.action_type = 3
    assert not node.dispatch(tks[101], 149, atk).error_message.id
    assert not node.dispatch(tks[201], 149, defend).error_message.id
    assert room.proposed_outcome == 1

    out_req = node.outcome_request(55030)
    assert out_req is not None
    result_event = protocol.parse("contracts.kafka.BattleResultEvent", out_req.raw_result_payload)
    s101 = next(s for s in result_event.settlements if s.player_id == 101)
    assert s101.ended_opponent_streak == 4
    assert s101.revenged_rival is True
    assert s101.revenged_opponent_name == "剑心"
    assert s101.revenged_wins_after == 2 and s101.revenged_losses_after == 2
    assert s101.final_season_seal_id == 101
    assert s101.player_display_name == "青衫"
    assert 10002 in s101.core_skill_ids

    # 验证 keep_on_death 与 reset_volatile_growing_stacks
    custom_buffs = {
        99001: BattleBuff(
            battle_op=BattleEffectOp.GROWING_AURA,
            modifier_stat=BattleModifier.SELF_GROWING_DIRECT_DAMAGE,
            ratio=0.05,
            stack_limit=5,
            keep_on_death=False,
        ),
        99002: BattleBuff(
            battle_op=BattleEffectOp.GROWING_AURA,
            modifier_stat=BattleModifier.SELF_GROWING_DIRECT_DAMAGE,
            ratio=0.05,
            stack_limit=5,
            keep_on_death=True,
        ),
    }
    eng = TurnEngine(
        protocol,
        _create_ranked_request(protocol, 55031, [_player(protocol, 1, 0), _player(protocol, 2, 1, hp=1)]),
        skills={},
        items={},
        buffs=custom_buffs,
        max_rounds=10,
    )
    a2 = eng.actors[2]
    a2.buffs.add(buff_id=10, buff_table_id=99001, caster_id=2, stacks=3)
    a2.buffs.add(buff_id=11, buff_table_id=99002, caster_id=2, stacks=4)
    eng.pending[1] = protocol.message("BattleAction", action_type=1, target_id=2)
    eng.pending[2] = protocol.message("BattleAction", action_type=3)
    eng.resolve()
    assert a2.is_dead
    # keep_on_death=True 的 99002 保留且层数不归零，99001 被移除
    remaining_ids = {b.buff_table_id: b.stacks for b in a2.buffs}
    assert 99002 in remaining_ids and remaining_ids[99002] == 4
    assert 99001 not in remaining_ids


# ===========================================================================
# 5. Match V4: 锦标赛 534 BanTournamentLeyline + 535 GetTournamentHighlights + 荣耀铭牌 + Gather 队长标
# ===========================================================================


def test_match_v4_tournament_534_535_honor_bits_and_gather_captain():
    asyncio.run(_run_match_v4_tournament_534_535_honor_bits_and_gather_captain())


async def _run_match_v4_tournament_534_535_honor_bits_and_gather_captain():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(1_750_000_000_000)
    catalog = TournamentCatalog.from_tables(tables)
    assert catalog.rule.leyline_ban_enabled is True
    assert catalog.rule.skill_ban_slots_per_team == 3

    mgr = TournamentManager(protocol, catalog=catalog, clock_ms=clock)
    store = MemoryMatchStore(protocol, clock_ms=clock)
    presence = MemoryPresenceProvider()
    nodes = StaticBattleNodeResolver([BattleNodeEntry(1, "battle-1", "127.0.0.1:19002", 1)])
    gather = GatherPipeline(protocol, store, MatchConfig(), rpc=None, presence=presence, battle_nodes=nodes)  # type: ignore[arg-type]
    svc = MatchGrpcService(
        protocol, store, MatchConfig(),
        gate_verifier=None, presence=presence, battle_nodes=nodes,
        gather=gather, spectate=SpectateManager(protocol, store, MatchConfig(), presence, nodes),
        tournament=mgr, clock_ms=clock,
    )
    match_rpcs = svc.services()["match.MatchService"]
    assert "BanTournamentLeyline" in match_rpcs
    assert "GetTournamentHighlights" in match_rpcs
    assert gather.on_tournament_captains == mgr.load_squad_captains

    def _caller(pid: int) -> CallerIdentity:
        return CallerIdentity(pid, f"acc-{pid}", 1, pid * 10, 1, "gate-1", 1)

    # 建 2 支满编 3 人战队并报名进入海选 -> 淘汰赛（BO3 决赛）
    squad_ids = []
    captains = [1001, 2001]
    for idx, cap in enumerate(captains):
        cr = await mgr.create_tournament_squad(
            _caller(cap),
            protocol.message("match.CreateTournamentSquadRequest", squad_name=f"仙盟{idx + 1}", emblem_id=idx + 1),
        )
        assert not cr.error_message.id
        sq_id = cr.squad.squad_id
        squad_ids.append(sq_id)
        for offset in (1, 2):
            mid = cap + offset
            inv = await mgr.invite_tournament_member(
                _caller(cap),
                protocol.message("match.InviteTournamentMemberRequest", target_player_id=mid),
            )
            assert not inv.error_message.id
            acc = await mgr.respond_tournament_invite(
                _caller(mid),
                protocol.message("match.RespondTournamentInviteRequest", squad_id=sq_id, accept=True),
            )
            assert not acc.error_message.id
        reg = await mgr.register_tournament_squad(_caller(cap), protocol.message("match.RegisterTournamentSquadRequest"))
        assert not reg.error_message.id

    assert mgr.load_squad_captains(squad_ids[0], squad_ids[1]) == {1001, 2001}

    # 推进至海选赛并打满 2 场，验证海选荣耀铭牌 HONOR_BIT_DEBUT
    season = mgr._ensure_season(clock())
    clock.now = season.qualifier_start_ms + 1000
    assert mgr.advance_season() == STAGE_QUALIFIER
    for g in range(2):
        r1 = await mgr.start_tournament_match(_caller(1001), protocol.message("match.StartTournamentMatchRequest"))
        r2 = await mgr.start_tournament_match(_caller(2001), protocol.message("match.StartTournamentMatchRequest"))
        assert r2.battle_id > 0
        ctx = mgr.battle_context_for(r2.battle_id)
        assert ctx is not None and ctx.skill_ban_slots_per_team == 3
        assert mgr.apply_battle_result(r2.battle_id, OUTCOME_SIDE_A_WIN)
    assert mgr._squads[squad_ids[0]].honor_bits & HONOR_BIT_DEBUT

    # 推进至淘汰赛（2 队 BO3 总决赛）
    clock.now = season.knockout_start_ms + 1000
    assert mgr.advance_season() == STAGE_KNOCKOUT
    ko_match = next(m for m in mgr._matches.values() if m.phase == PHASE_KNOCKOUT)
    assert ko_match.best_of == 3

    # 非队长禁灵脉被拒
    non_cap = mgr.ban_tournament_leyline(
        _caller(1002),
        protocol.message("match.BanTournamentLeylineRequest", match_id=ko_match.match_id, leyline_element=LEYLINE_METAL),
    )
    assert non_cap.error_message.id == ERR_MATCH_TOURNAMENT_NOT_CAPTAIN

    # 两队队长各禁 1 条灵脉（金/火）-> 重算 3 局序列绝对不含金、火且与纯规则一致
    ban_a = mgr.ban_tournament_leyline(
        _caller(1001),
        protocol.message("match.BanTournamentLeylineRequest", match_id=ko_match.match_id, leyline_element=LEYLINE_METAL),
    )
    assert not ban_a.error_message.id and ban_a.match.banned_leyline_a == LEYLINE_METAL
    ban_b = mgr.ban_tournament_leyline(
        _caller(2001),
        protocol.message("match.BanTournamentLeylineRequest", match_id=ko_match.match_id, leyline_element=LEYLINE_FIRE),
    )
    assert not ban_b.error_message.id and ban_b.match.banned_leyline_b == LEYLINE_FIRE
    expected_seq = draw_leylines_with_bans(season.season_id, ko_match.match_id, 3, LEYLINE_METAL, LEYLINE_FIRE)
    assert tuple(ban_b.match.leyline_sequence) == expected_seq
    assert LEYLINE_METAL not in expected_seq and LEYLINE_FIRE not in expected_seq
    assert set(expected_seq) == {LEYLINE_WOOD, LEYLINE_WATER, LEYLINE_EARTH}

    # 开战 2:0 完胜后：禁止再改灵脉禁选、触发 SWEEP+CHAMPION 荣耀铭牌、名局录(535) 收录总决赛巅峰局
    await mgr.start_tournament_match(_caller(1001), protocol.message("match.StartTournamentMatchRequest", match_id=ko_match.match_id))
    g1 = await mgr.start_tournament_match(_caller(2001), protocol.message("match.StartTournamentMatchRequest", match_id=ko_match.match_id))
    assert g1.battle_id > 0
    late_ban = mgr.ban_tournament_leyline(
        _caller(1001),
        protocol.message("match.BanTournamentLeylineRequest", match_id=ko_match.match_id, leyline_element=LEYLINE_WOOD),
    )
    assert late_ban.error_message.id == ERR_MATCH_TOURNAMENT_MATCH_NOT_READY
    assert mgr.apply_battle_result(g1.battle_id, OUTCOME_SIDE_A_WIN)

    await mgr.start_tournament_match(_caller(1001), protocol.message("match.StartTournamentMatchRequest", match_id=ko_match.match_id))
    g2 = await mgr.start_tournament_match(_caller(2001), protocol.message("match.StartTournamentMatchRequest", match_id=ko_match.match_id))
    assert mgr.apply_battle_result(g2.battle_id, OUTCOME_SIDE_A_WIN)

    champ_sq = mgr._squads[squad_ids[0]]
    assert (champ_sq.honor_bits & HONOR_BIT_SWEEP) and (champ_sq.honor_bits & HONOR_BIT_CHAMPION)
    assert season.champion_honor_bits == champ_sq.honor_bits

    hl = mgr.get_tournament_highlights(_caller(9999), protocol.message("match.GetTournamentHighlightsRequest", season_id=0))
    assert not hl.error_message.id
    assert len(hl.highlights) == 1
    assert hl.highlights[0].tag == 1 and hl.highlights[0].tag_label == "总决赛巅峰局"
    assert hl.highlights[0].match.banned_leyline_a == LEYLINE_METAL
    assert hl.highlights[0].match.banned_leyline_b == LEYLINE_FIRE


# ===========================================================================
# 6. Match V4: 论剑排位连胜、宿敌交手复仇、热门构筑与公开战报鉴权
# ===========================================================================


def test_match_v4_ranked_streaks_rivalry_and_featured_builds():
    asyncio.run(_run_match_v4_ranked_streaks_rivalry_and_featured_builds())


async def _run_match_v4_ranked_streaks_rivalry_and_featured_builds():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(1_760_000_000_000)
    rule = RankedRuleConfig.from_config_tables(tables)
    assert rule.skill_ban_slots_per_team == 3
    assert rule.skill_ban_min_rating_centi == 160_000

    store = MemoryMatchStore(protocol, clock_ms=clock)
    ranked = RankedManager(protocol, store, rule=rule, tables=tables, clock_ms=clock)
    season = await ranked.ensure_current_season()
    caller_a = CallerIdentity(301, "acc-301", 1, 3010, 1, "gate-1", 1)

    st_a = protocol.message(
        "BattleSettlementData",
        final_season_seal_id=101,
        starter_pet_table_id=3,
        core_skill_ids=[10025, 10002, 10035, 10039],
        player_display_name="剑仙甲",
    )

    # 第 1 局：301 负于 302 -> 宿敌账本 0胜1负，进入复仇目标
    r1 = await ranked.record_battle_settlement(
        battle_id=88001, player_id=301, season_id=season.season_id, team_size=1,
        status=RANKED_MATCH_RATING_CONFIRMED, outcome=2, player_team_index=0, total_rounds=5,
        participated=True, fled=False, rating_before_centi=155_000, rating_after_centi=153_000,
        opponents=[(302, "宿敌乙")], settlement_data=st_a,
    )
    assert r1.win_streak_after == 0 and not r1.revenged_rival

    encounters = await store.load_rivalry_encounters(season.season_id, [(301, 0), (302, 1)])
    assert len(encounters) == 1
    assert encounters[0].player_id == 301 and encounters[0].opponent_id == 302 and encounters[0].revenge_target is True

    # 第 2 局：301 复仇战胜 302 -> 触发 revenged_rival + 连胜 1 + 高段热门流派入榜
    r2 = await ranked.record_battle_settlement(
        battle_id=88002, player_id=301, season_id=season.season_id, team_size=1,
        status=RANKED_MATCH_RATING_CONFIRMED, outcome=1, player_team_index=0, total_rounds=6,
        participated=True, fled=False, rating_before_centi=153_000, rating_after_centi=158_000,
        ended_opponent_streak=3, opponents=[(302, "宿敌乙")], settlement_data=st_a,
    )
    assert r2.win_streak_after == 1
    assert r2.ended_opponent_streak == 3
    assert r2.revenged_rival is True
    assert r2.revenged_opponent_name == "宿敌乙"
    assert r2.revenged_wins_after == 1 and r2.revenged_losses_after == 1

    # 第 3 局：再胜一场 -> current_win_streak=2, best_win_streak=2
    r3 = await ranked.record_battle_settlement(
        battle_id=88003, player_id=301, season_id=season.season_id, team_size=1,
        status=RANKED_MATCH_RATING_CONFIRMED, outcome=1, player_team_index=0, total_rounds=4,
        participated=True, fled=False, rating_before_centi=158_000, rating_after_centi=162_000,
        opponents=[(302, "宿敌乙")], settlement_data=st_a,
    )
    assert r3.win_streak_after == 2

    # 热门代表战报公开观摩鉴权仅放行 featured_battle_id=88003
    assert await store.is_featured_ranked_replay_allowed(88003) is True
    assert await store.is_featured_ranked_replay_allowed(88001) is False

    prof = await ranked.get_ranked_profile(caller_a, protocol.message("match.GetRankedProfileRequest"))
    assert not prof.error_message.id
    assert prof.current_progress.current_win_streak == 2
    assert prof.current_progress.best_win_streak == 2
    assert len(prof.top_rivals) == 1
    assert prof.top_rivals[0].opponent_id == 302 and prof.top_rivals[0].wins == 2 and prof.top_rivals[0].losses == 1
    assert len(prof.featured_builds) == 1
    assert prof.featured_builds[0].featured_battle_id == 88003
    assert prof.featured_builds[0].high_tier_wins == 2


# ===========================================================================
# 7. Team V4: 524 SetTeamFormation + 525 RecallTeamMembers + is_hosted
# ===========================================================================


def test_team_v4_set_formation_524_and_recall_members_525():
    asyncio.run(_run_team_v4_set_formation_524_and_recall_members_525())


async def _run_team_v4_set_formation_524_and_recall_members_525():
    protocol = Protocol()
    clock = _FakeClock(1_770_000_000_000)
    store = MemoryMatchStore(protocol, clock_ms=clock)
    presence = MemoryPresenceProvider()
    nodes = StaticBattleNodeResolver()
    gather = GatherPipeline(protocol, store, MatchConfig(), rpc=None, presence=presence, battle_nodes=nodes)  # type: ignore[arg-type]
    notifier = _RecordingNotifier(pushes=[], refreshes=[])

    # 生成真实 Ed25519 会话签名密钥与校验器
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    priv = Ed25519PrivateKey.generate()
    verifier = CallerVerifier([priv.public_key()], allowed_callers=("gate",))

    team_svc = TeamGrpcService(
        protocol, store, MatchConfig(),
        gate_verifier=verifier, presence=presence, battle_nodes=nodes,
        gather=gather, notifier=notifier, clock_ms=clock,
    )
    rpcs = team_svc.services()[SERVICE_CLIENT_TEAM]
    assert "SetTeamFormation" in rpcs
    assert "RecallTeamMembers" in rpcs

    for pid, hosted in ((501, False), (502, True), (503, False)):
        presence.set_presence(PlayerPresence(player_id=pid, online=True, zone_id=1, name=f"队员{pid}", hosted=hosted))

    def _ctx(pid: int, method: str) -> _FakeContext:
        details = protocol.message(
            "SessionDetails",
            player_id=pid,
            authenticated_account=f"acc-{pid}",
            owner_revision=1,
            session_id=pid * 10,
            gate_node_id=1,
            gate_instance_id="gate-1",
            session_incarnation=1,
        )
        return _FakeContext(
            list(session_metadata(details.SerializeToString(deterministic=True), method, priv))
        )

    # 501 建队并拉入 502(托管中) 与 503
    p_create = f"/{SERVICE_CLIENT_TEAM}/CreateTeam"
    cr = await team_svc.create_team(protocol.message("teampb.CreateTeamRequest", request_id=str(uuid.uuid4())), _ctx(501, p_create), p_create)
    assert not cr.error_message.id
    team_id = cr.team.team_id
    for member_id in (502, 503):
        p_inv = f"/{SERVICE_CLIENT_TEAM}/InviteToTeam"
        await team_svc.invite_to_team(
            protocol.message("teampb.InviteToTeamRequest", target_player_id=member_id, expected_team_id=team_id, request_id=str(uuid.uuid4())),
            _ctx(501, p_inv), p_inv,
        )
        p_resp = f"/{SERVICE_CLIENT_TEAM}/RespondInvite"
        await team_svc.respond_invite(
            protocol.message("teampb.RespondInviteRequest", team_id=team_id, accept=True, request_id=str(uuid.uuid4())),
            _ctx(member_id, p_resp), p_resp,
        )

    notifier.pushes.clear()
    notifier.refreshes.clear()

    # 1) 524 SetTeamFormation：非队长拒(4018)，非法枚举拒(1005)
    p_form = f"/{SERVICE_CLIENT_TEAM}/SetTeamFormation"
    err_non_leader = await team_svc.set_team_formation(
        protocol.message("teampb.SetTeamFormationRequest", expected_team_id=team_id, formation=1, request_id=str(uuid.uuid4())),
        _ctx(502, p_form), p_form,
    )
    assert err_non_leader.error_message.id == ERR_TEAM_NOT_LEADER

    err_bad_enum = await team_svc.set_team_formation(
        protocol.message("teampb.SetTeamFormationRequest", expected_team_id=team_id, formation=9, request_id=str(uuid.uuid4())),
        _ctx(501, p_form), p_form,
    )
    assert err_bad_enum.error_message.id == ERR_INVALID_PARAMETER

    # 队长切换为雁行(1)：广播 reason=18 + 刷新全员 scene + is_hosted 正确投影
    ok_form = await team_svc.set_team_formation(
        protocol.message("teampb.SetTeamFormationRequest", expected_team_id=team_id, formation=1, request_id=str(uuid.uuid4())),
        _ctx(501, p_form), p_form,
    )
    assert not ok_form.error_message.id
    assert ok_form.team.follow_formation == 1
    hosted_map = {m.player_id: m.is_hosted for m in ok_form.team.members}
    assert hosted_map == {501: False, 502: True, 503: False}
    assert sorted(notifier.refreshes) == [501, 502, 503]
    snap_pushes = [(pid, msg) for pid, mid, msg in notifier.pushes if mid == MSG_NOTIFY_TEAM_SNAPSHOT]
    assert {pid for pid, _ in snap_pushes} == {502, 503}
    assert all(msg.reason == 18 and msg.team.follow_formation == 1 for _, msg in snap_pushes)

    # 幂等再设同一队形(1) -> 不发快照也不刷新场景
    notifier.pushes.clear()
    notifier.refreshes.clear()
    noop_form = await team_svc.set_team_formation(
        protocol.message("teampb.SetTeamFormationRequest", expected_team_id=team_id, formation=1, request_id=str(uuid.uuid4())),
        _ctx(501, p_form), p_form,
    )
    assert not noop_form.error_message.id
    assert not notifier.pushes and not notifier.refreshes

    # 2) 525 RecallTeamMembers：非队长拒(4018)；队长召集推 LEADER_RECALL(3) 给非队长且不推快照；15s 冷却限流(1008)
    p_recall = f"/{SERVICE_CLIENT_TEAM}/RecallTeamMembers"
    assert (
        await team_svc.recall_team_members(
            protocol.message("teampb.RecallTeamMembersRequest", expected_team_id=team_id, request_id=str(uuid.uuid4())),
            _ctx(502, p_recall), p_recall,
        )
    ).error_message.id == ERR_TEAM_NOT_LEADER

    ok_recall = await team_svc.recall_team_members(
        protocol.message("teampb.RecallTeamMembersRequest", expected_team_id=team_id, request_id=str(uuid.uuid4())),
        _ctx(501, p_recall), p_recall,
    )
    assert not ok_recall.error_message.id
    event_pushes = [(pid, msg) for pid, mid, msg in notifier.pushes if mid == MSG_NOTIFY_TEAM_EVENT]
    assert {pid for pid, _ in event_pushes} == {502, 503}
    assert all(msg.type == 3 and msg.actor_id == 501 and msg.team_id == team_id for _, msg in event_pushes)
    assert not any(mid == MSG_NOTIFY_TEAM_SNAPSHOT for _, mid, _ in notifier.pushes)

    # 10 秒内再次召集 -> 1008 (ERR_RATE_LIMIT_EXCEEDED)
    clock.advance(10_000)
    assert (
        await team_svc.recall_team_members(
            protocol.message("teampb.RecallTeamMembersRequest", expected_team_id=team_id, request_id=str(uuid.uuid4())),
            _ctx(501, p_recall), p_recall,
        )
    ).error_message.id == ERR_RATE_LIMIT_EXCEEDED

    # 满 15 秒后允许再次召集
    clock.advance(5_000)
    assert not (
        await team_svc.recall_team_members(
            protocol.message("teampb.RecallTeamMembersRequest", expected_team_id=team_id, request_id=str(uuid.uuid4())),
            _ctx(501, p_recall), p_recall,
        )
    ).error_message.id


def test_gather_marks_tournament_captains_and_forwards_boss_weekly_affix():
    asyncio.run(_run_gather_marks_tournament_captains_and_forwards_boss_weekly_affix())


async def _run_gather_marks_tournament_captains_and_forwards_boss_weekly_affix():
    protocol = Protocol()
    clock = _FakeClock(1_770_000_000_000)
    store = MemoryMatchStore(protocol, clock_ms=clock)
    presence = MemoryPresenceProvider()
    nodes = StaticBattleNodeResolver([BattleNodeEntry(1, "battle-1", "127.0.0.1:19002", 1)])
    captured_create: list = []

    class _FakeRpc:
        async def call(self, endpoint: str, path: str, data: bytes, *, timeout: float = 5.0) -> bytes:
            if path.endswith("/PrepareBattle"):
                req = protocol.parse("PrepareBattleRequest", data)
                resp = protocol.message("PrepareBattleResponse", table_fingerprint="fp-1")
                resp.snapshot.CopyFrom(_player(protocol, req.player_id, 0))
                return resp.SerializeToString(deterministic=True)
            if path.endswith("/QueryBattleCreation"):
                req = protocol.parse("QueryBattleCreationRequest", data)
                return protocol.message(
                    "QueryBattleCreationResponse",
                    target_instance_id=req.target_instance_id,
                    supported_outcome_protocol_version=1,
                ).SerializeToString(deterministic=True)
            if path.endswith("/CreateBattle"):
                creq = protocol.parse("CreateBattleRequest", data)
                captured_create.append(creq)
                return protocol.message(
                    "CreateBattleResponse",
                    battle_id=creq.battle_id,
                    adopted_outcome_protocol_version=1,
                    target_instance_id=creq.target_instance_id,
                    room_count=1,
                ).SerializeToString(deterministic=True)
            raise AssertionError(f"unexpected path {path}")

        async def close(self) -> None:
            return None

    for pid in (601, 602):
        presence.set_presence(PlayerPresence(player_id=pid, online=True, scene_grpc_endpoint="127.0.0.1:19001"))

    gather = GatherPipeline(
        protocol, store, MatchConfig(), rpc=_FakeRpc(), presence=presence, battle_nodes=nodes,
        on_tournament_captains=lambda sa, sb: {601},
    )
    tctx = protocol.message("TournamentBattleContext", season_id=1, match_id=77, squad_a_id=10, squad_b_id=20)
    tickets_for_claim: list[tuple[int, str, int, bool]] = []
    for side, pid in ((0, 601), (1, 602)):
        caller = CallerIdentity(pid, f"acc-{pid}", 1, pid * 10, 1, "gate-1", 1)
        tk = await store.join_queue(
            caller,
            mode=MODE_SEASON_TOURNAMENT,
            config_id=5,
            team_size=1,
            zone_id=1,
            request_id=str(uuid.uuid4()),
            request_payload=b"",
            network_profile=b"",
            pool_region=1,
            queue_ttl_ms=60_000,
        )
        tickets_for_claim.append((pid, tk.ticket, side, False))
    formation = await store.claim_formation(
        mode=MODE_SEASON_TOURNAMENT,
        config_id=5,
        members=tickets_for_claim,
        physical_region=1,
        target_node_id=1,
        target_instance_id="battle-1",
        target_endpoint="127.0.0.1:19002",
        release_track="stable",
        battle_deadline_ms=120_000,
        requeue_on_failure=False,
        tournament_context=tctx.SerializeToString(deterministic=True),
    )
    ok = await gather.run_formation(formation, boss_weekly_affix_id=BOSS_AFFIX_FIRE)
    assert ok is True
    assert len(captured_create) == 1
    creq = captured_create[0]
    assert creq.boss_weekly_affix_id == BOSS_AFFIX_FIRE
    captains = {p.player_id: p.is_team_captain for p in creq.players}
    assert captains == {601: True, 602: False}

