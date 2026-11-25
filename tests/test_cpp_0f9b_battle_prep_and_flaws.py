"""C++ 2f0e62225..0f9bc81d6 Battle V3（论剑变阵 504/505、首领试炼全破绽、蓄力/专修/策略副目标/易伤免控）回归测试。"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

from xuanming.data import ConfigTables
from xuanming.nodes.battle import CLIENT_REQUESTS, BattleError, BattleNode, TurnEngine
from xuanming.nodes.battle_dropin_effects import (
    BOSS_EXPOSED_FULL_PCT,
    BOSS_EXPOSED_PARTIAL_PCT,
    BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE,
    BOSS_MECHANIC_STANCE_BREAK,
    BOSS_TRIAL_KIND_BARRIER,
    BOSS_TRIAL_KIND_VENOM,
    EVENT_BOSS_MECHANIC,
    TACTICAL_PREP_DURATION_SECONDS,
    TACTICAL_PREP_TIMEOUT_MS,
    TELEGRAPH_KIND_GUARD_BIND,
    TELEGRAPH_KIND_GUARD_SHIELD,
    TELEGRAPH_STATUS_CANCELLED_BY_CONTROL,
)
from xuanming.nodes.battle_tables import BattleTables
from xuanming.nodes.battle_tcp import BattleClients
from xuanming.protocol import Protocol


class _FakeClock:
    def __init__(self, now_ms: int = 10_000) -> None:
        self.now = now_ms

    def __call__(self) -> int:
        return self.now

    def advance(self, delta_ms: int) -> int:
        self.now += delta_ms
        return self.now


def _seal(
    protocol: Protocol,
    seal_id: int,
    effect_id: int,
    name: str,
    *,
    season_id: int = 1,
    active_rounds: int = 0,
    battle_limit: int = 0,
):
    return protocol.message(
        "SeasonSealEffect",
        season_id=season_id,
        seal_id=seal_id,
        effect_id=effect_id,
        bonus_bps=1000,
        cap_bps=300 if active_rounds > 0 else 0,
        per_round_limit=1 if active_rounds == 0 else 0,
        battle_limit=battle_limit,
        active_rounds=active_rounds,
        name=name,
    )


def _pet(
    protocol: Protocol,
    pet_id: int,
    owner_id: int,
    table_id: int,
    name: str,
    *,
    hp: int = 3000,
    speed: int = 200,
    phys: int = 200,
    magic: int = 100,
    defense: int = 120,
    competitive_temporary: bool = False,
):
    snap = protocol.message(
        "BattlePetSnapshot",
        pet_id=pet_id,
        owner_player_id=owner_id,
        pet_table_id=table_id,
        pet_name=name,
        level=30,
        max_health=hp,
        max_mana=1000,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        competitive_temporary=competitive_temporary,
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = 1000
    snap.base_attributes.speed = speed
    return snap


def _player(
    protocol: Protocol,
    player_id: int,
    team_index: int,
    *,
    hp: int = 10_000,
    mana: int = 2000,
    speed: int = 200,
    phys: int = 300,
    magic: int = 300,
    defense: int = 100,
    level: int = 40,
    active_season_id: int = 1,
    ai_controlled: bool = False,
    skills: tuple[int, ...] = (),
    skill_ranks: dict[int, int] | None = None,
    equipped_seal=None,
    available_seals: tuple = (),
    pets: tuple = (),
    standby_pets: tuple = (),
):
    snap = protocol.message(
        "BattlePlayerSnapshot",
        player_id=player_id,
        player_name=f"P{player_id}",
        level=level,
        max_health=hp,
        max_mana=mana,
        physical_attack=phys,
        magic_attack=magic,
        defense=defense,
        team_index=team_index,
        active_season_id=active_season_id,
        ai_controlled=ai_controlled,
        skill_table_ids=list(skills),
    )
    snap.base_attributes.health = hp
    snap.base_attributes.mana = mana
    snap.base_attributes.speed = speed
    snap.routing.session_id = (7 << 17) + player_id
    snap.routing.gate_node_id = 7
    snap.routing.gate_instance_id = "gate-1"
    snap.routing.scene_node_id = 10
    snap.routing.scene_instance_id = "scene-1"
    snap.routing.zone_id = 1
    if skill_ranks:
        for sid, rank in skill_ranks.items():
            snap.skill_ranks[sid] = rank
    if equipped_seal is not None:
        snap.equipment_effects.season_seal.CopyFrom(equipped_seal)
    for seal in available_seals:
        snap.available_season_seals.add().CopyFrom(seal)
    for p in pets:
        snap.pets.add().CopyFrom(p)
    for sp in standby_pets:
        snap.standby_pets.add().CopyFrom(sp)
    return snap


def _create_request(
    protocol: Protocol,
    battle_id: int,
    players: list,
    *,
    mode: int = 7,
    config_id: int = 5,
    now_ms: int = 10_000,
    use_free_preset: bool = False,
    ranked: bool = True,
    record_replay: bool = True,
):
    req = protocol.message(
        "CreateBattleRequest",
        battle_id=battle_id,
        battle_config_id=config_id,
        seed=301,
        match_mode=mode,
        created_at_ms=now_ms,
        deadline_ms=now_ms + 600_000,
        required_physical_region=1,
        placement_operation_id=str(uuid.uuid4()),
        target_instance_id="battle-inst-1",
        admit_before_ms=now_ms + 30_000,
        outcome_protocol_version=1,
        spectate_visibility=2,
        record_replay=record_replay,
    )
    if ranked:
        req.ranked_context.season_id = 1
        req.ranked_context.season_key = "s1"
        req.ranked_context.rank_generation = 1
        req.ranked_context.rule_version = 2
        req.ranked_context.team_size = max(1, len(players) // 2)
        req.ranked_context.use_free_preset = use_free_preset
    for pl in players:
        req.players.add().CopyFrom(pl)
    return req


# ── 1. 论剑 V3 战前 15 秒明牌变阵（504 UpdateBattlePrep / 505 NotifyBattlePrepUpdate）──────────


def test_tactical_prep_504_505_privacy_swap_and_early_lockin():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(20_000)
    lobby_pushes: list[tuple[int, int, object]] = []
    outcomes: list[tuple[int, int]] = []
    node = BattleNode(
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

    assert CLIENT_REQUESTS[504] == "UpdateBattlePrepRequest"
    clients = BattleClients(protocol, node)
    assert isinstance(clients._limits, dict)

    seal101 = _seal(protocol, 101, 1, "破阵锐印")
    seal103 = _seal(protocol, 103, 6, "固阵守印", active_rounds=2, battle_limit=1)

    p1 = _player(
        protocol,
        101,
        0,
        equipped_seal=seal101,
        available_seals=(seal101, seal103),
        pets=(_pet(protocol, 5001, 101, 1, "灵狐", hp=3000, speed=260),),
        standby_pets=(_pet(protocol, 5002, 101, 2, "石灵", hp=5500, speed=140),),
    )
    p2 = _player(
        protocol,
        102,
        0,
        equipped_seal=seal101,
        available_seals=(seal101, seal103),
    )
    p3 = _player(
        protocol,
        201,
        1,
        equipped_seal=seal101,
        available_seals=(seal101, seal103),
        pets=(_pet(protocol, 6001, 201, 3, "金猊", hp=3200, speed=280),),
    )
    p4 = _player(protocol, 202, 1, ai_controlled=True)

    req = _create_request(protocol, 99301, [p1, p2, p3, p4], now_ms=clock())
    res = node.create(req)
    assert not res.error_message.id
    room = node.rooms[99301]
    assert room.engine.is_in_prep_phase
    assert not room.engine.all_prep_players_ready()
    # 变阵期延迟创建回放起点与第 1 回合计时器。
    assert room.replay is None
    assert room.action_deadline_ms == 0
    assert room.prep_deadline_ms == clock() + TACTICAL_PREP_TIMEOUT_MS

    # 连接 101/102/201 参战直连与 9001 观众直连。
    frames: dict[int, list[tuple[int, object]]] = {101: [], 102: [], 201: [], 9001: []}
    tickets = {}
    for pid in (101, 102, 201):
        issued = node.issue_ticket(
            protocol.message("IssueBattleTicketRequest", battle_id=99301, player_id=pid, target_instance_id="battle-inst-1")
        )
        tk = node.authorize(issued.assignment.token_payload, issued.assignment.token_signature)
        tickets[pid] = tk
        node.attach(tk, lambda mid, msg, p=pid: frames[p].append((mid, msg)), lambda: None)

    obs_op = str(uuid.uuid4())
    obs_req = protocol.message(
        "AddObserverRequest",
        battle_id=99301,
        observer_player_id=9001,
        spectator_operation_id=obs_op,
        target_instance_id="battle-inst-1",
        admit_before_ms=clock() + 10_000,
    )
    obs_req.routing.gate_instance_id = "gate-1"
    assert not node.add_observer(obs_req).error_message.id
    obs_issued = node.issue_ticket(
        protocol.message(
            "IssueBattleTicketRequest",
            battle_id=99301,
            player_id=9001,
            spectator_operation_id=obs_op,
            target_instance_id="battle-inst-1",
        )
    )
    obs_tk = node.authorize(obs_issued.assignment.token_payload, obs_issued.assignment.token_signature)
    node.attach(obs_tk, lambda mid, msg: frames[9001].append((mid, msg)), lambda: None)

    # 变阵期禁止提交战斗行动、切自动、投降，pending_actor_ids 为空。
    attack = protocol.message("SubmitBattleActionRequest", battle_id=99301)
    attack.action.action_type = 1
    attack.action.target_id = 201
    assert node.dispatch(tickets[101], 149, attack).error_message.id == 1006
    assert node.dispatch(tickets[101], 162, protocol.message("SetAutoBattleRequest", battle_id=99301, enabled=True)).error_message.id == 1006
    assert node.dispatch(tickets[101], 479, protocol.message("SurrenderBattleRequest", battle_id=99301, agree=True)).error_message.id == 1006

    # 视角脱敏校验：101 仅见本队（101, 102），观众仅见倒计时不含任何可选与队伍状态。
    state_101 = node.dispatch(tickets[101], 140, protocol.message("GetBattleStateRequest", battle_id=99301))
    assert not state_101.pending_actor_ids
    assert state_101.prep_state.active and state_101.prep_state.duration_seconds == TACTICAL_PREP_DURATION_SECONDS
    assert state_101.prep_state.deadline_ms == room.prep_deadline_ms
    assert len(state_101.prep_state.self_available_seals) == 2
    assert len(state_101.prep_state.self_available_pets) == 2
    assert [ps.player_id for ps in state_101.prep_state.team_states] == [101, 102]

    state_obs = node.dispatch(obs_tk, 140, protocol.message("GetBattleStateRequest", battle_id=99301))
    assert state_obs.prep_state.active and state_obs.prep_state.deadline_ms == room.prep_deadline_ms
    assert not state_obs.prep_state.self_available_seals
    assert not state_obs.prep_state.self_available_pets
    assert not state_obs.prep_state.team_states

    initial_pet = next(a for a in state_101.actors if a.actor_type == 3 and a.owner_player_id == 101)
    initial_pet_actor_id, initial_pet_slot = initial_pet.actor_id, initial_pet.formation_slot

    # 越权换敌方灵宠、换非法赛季印一律拒绝（1005）。
    bad_seal = node.dispatch(
        tickets[101], 504, protocol.message("UpdateBattlePrepRequest", battle_id=99301, selected_seal_id=999),
    )
    assert bad_seal.error_message.id == 1005
    bad_pet = node.dispatch(
        tickets[101], 504, protocol.message("UpdateBattlePrepRequest", battle_id=99301, selected_pet_id=6001),
    )
    assert bad_pet.error_message.id == 1005

    # 101 切换固阵守印(103) + 石灵(5002) 并锁定；同队 101/102 收到 505，对手 201 与观众 9001 绝不收到 505。
    ok_101 = node.dispatch(
        tickets[101],
        504,
        protocol.message("UpdateBattlePrepRequest", battle_id=99301, selected_seal_id=103, selected_pet_id=5002, ready=True),
    )
    assert not ok_101.error_message.id
    st101 = next(ps for ps in ok_101.prep_state.team_states if ps.player_id == 101)
    assert st101.selected_seal_id == 103 and st101.selected_pet_id == 5002 and st101.ready
    assert any(mid == 505 for mid, _ in frames[101])
    assert any(mid == 505 for mid, _ in frames[102])
    assert not any(mid == 505 for mid, _ in frames[201])
    assert not any(mid == 505 for mid, _ in frames[9001])

    # 锁定后禁止改配（1006），重复同参锁定幂等（0）。
    assert node.dispatch(
        tickets[101], 504, protocol.message("UpdateBattlePrepRequest", battle_id=99301, selected_seal_id=101, ready=True),
    ).error_message.id == 1006
    assert not node.dispatch(
        tickets[101], 504, protocol.message("UpdateBattlePrepRequest", battle_id=99301, selected_seal_id=103, selected_pet_id=5002, ready=True),
    ).error_message.id

    # 102 与 201 全部锁定（AI 单位 202 开局已自动就绪）-> 立即亮牌结束变阵、补录回放起点、推送新 143 并进入第 1 回合。
    lobby_before = len(lobby_pushes)
    assert not node.dispatch(tickets[102], 504, protocol.message("UpdateBattlePrepRequest", battle_id=99301, ready=True)).error_message.id
    assert room.engine.is_in_prep_phase
    assert not node.dispatch(tickets[201], 504, protocol.message("UpdateBattlePrepRequest", battle_id=99301, ready=True)).error_message.id
    assert not room.engine.is_in_prep_phase
    assert room.replay is not None
    assert room.action_deadline_ms > clock()
    new_starts = [(pid, mid, msg) for pid, mid, msg in lobby_pushes[lobby_before:] if mid == 143]
    assert {pid for pid, _, _ in new_starts} == {101, 102, 201, 202}
    assert any(mid == 161 for mid, _ in frames[9001])

    # 验证最终阵容：101 赛季印为 103，灵宠原地替换为石灵(5002)并保留 actor_id 与阵位，AI 单位 202 仍为自动。
    post = node.dispatch(tickets[101], 140, protocol.message("GetBattleStateRequest", battle_id=99301))
    assert not post.HasField("prep_state")
    assert set(post.pending_actor_ids) == {101, 102, 201}
    actor_101 = next(a for a in post.actors if a.actor_id == 101)
    swapped_pet = next(a for a in post.actors if a.actor_type == 3 and a.owner_player_id == 101)
    actor_202 = next(a for a in post.actors if a.actor_id == 202)
    assert actor_101.equipment_effects.season_seal.seal_id == 103
    assert swapped_pet.actor_id == initial_pet_actor_id
    assert swapped_pet.formation_slot == initial_pet_slot
    assert swapped_pet.pet_id == 5002 and swapped_pet.pet_table_id == 2 and swapped_pet.max_health == 5500
    assert actor_202.is_auto is True

    # 亮牌后第 1 回合正常接收指令并结算。
    assert not node.dispatch(tickets[101], 149, attack).error_message.id


def test_tactical_prep_timer_expiry_and_skip_rules():
    protocol = Protocol()
    tables = ConfigTables()
    clock = _FakeClock(50_000)
    node = BattleNode(
        protocol,
        node_id=2,
        instance_id="battle-inst-1",
        host="127.0.0.1",
        port=19002,
        ticket_secret=b"T" * 32,
        now_ms=clock,
        steady_ms=clock,
        on_outcome=lambda _bid, _out: None,
        tables=tables,
    )
    seal101 = _seal(protocol, 101, 1, "破阵锐印")
    seal103 = _seal(protocol, 103, 6, "固阵守印", active_rounds=2, battle_limit=1)

    # 1) 15 秒定时器到期后 tick() 自动锁阵并进入第 1 回合。
    req = _create_request(
        protocol,
        99302,
        [
            _player(protocol, 101, 0, equipped_seal=seal101, available_seals=(seal101, seal103)),
            _player(protocol, 201, 1, equipped_seal=seal101, available_seals=(seal101, seal103)),
        ],
        now_ms=clock(),
    )
    assert not node.create(req).error_message.id
    room = node.rooms[99302]
    assert room.engine.is_in_prep_phase
    clock.advance(TACTICAL_PREP_TIMEOUT_MS - 1)
    node.tick()
    assert room.engine.is_in_prep_phase
    clock.advance(1)
    node.tick()
    assert not room.engine.is_in_prep_phase
    assert room.replay is not None
    assert room.action_deadline_ms == clock() + 25_000

    # 2) 免费预设娱乐场与无选项对局自动跳过变阵期。
    req_free = _create_request(
        protocol,
        99303,
        [
            _player(protocol, 101, 0, equipped_seal=seal101, available_seals=(seal101, seal103)),
            _player(protocol, 201, 1),
        ],
        now_ms=clock(),
        use_free_preset=True,
    )
    eng_free = TurnEngine(protocol, req_free, skills={}, items={}, max_rounds=20)
    assert not eng_free.is_in_prep_phase

    req_zero = _create_request(
        protocol,
        99304,
        [_player(protocol, 101, 0), _player(protocol, 201, 1)],
        now_ms=clock(),
        use_free_preset=False,
    )
    eng_zero = TurnEngine(protocol, req_zero, skills={}, items={}, max_rounds=20)
    assert not eng_zero.is_in_prep_phase

    # 3) 非法备选赛季印（跨赛季）或非法备选灵宠（pet_id=0 / 重复 pet_id）直接拒绝开局。
    bad_seal_req = _create_request(
        protocol,
        99305,
        [
            _player(
                protocol,
                101,
                0,
                active_season_id=1,
                available_seals=(_seal(protocol, 101, 1, "跨季印", season_id=2),),
            ),
            _player(protocol, 201, 1),
        ],
    )
    with pytest.raises(BattleError):
        TurnEngine(protocol, bad_seal_req, skills={}, items={}, max_rounds=20)

    dup_pet_req = _create_request(
        protocol,
        99306,
        [
            _player(
                protocol,
                101,
                0,
                pets=(_pet(protocol, 7001, 101, 1, "灵狐"),),
            ),
            _player(
                protocol,
                201,
                1,
                standby_pets=(_pet(protocol, 7001, 201, 2, "重号宠"),),
            ),
        ],
    )
    with pytest.raises(BattleError):
        TurnEngine(protocol, dup_pet_req, skills={}, items={}, max_rounds=20)


# ── 2. 首领试炼 V2 全破绽（灵障碎障、灵障/缚灵护卫控断、幽毒 AoE 化解）──────────────────────────


def _make_boss_monster(protocol: Protocol, table_id: int, hp: int, *, is_boss: bool, speed: int = 80):
    attrs = protocol.message("BaseAttributesComp", health=hp, strength=10, armor=0, speed=speed)
    return protocol.message(
        "BattleActorState",
        level=30,
        monster_table_id=table_id,
        is_boss=is_boss,
        attributes=attrs,
        max_health=hp,
        physical_attack=120,
        magic_attack=120,
        defense=20,
        name="首领" if is_boss else "护卫",
    )


def test_boss_trial_v2_barrier_shatter_guard_interrupt_and_venom_parry_exposed():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)

    # 1) 灵障首领（kind=3）：R1 灵障护卫挂盾，R2 击破灵障护盾立即触发 +35% 完整破绽与崩势事件。
    p_barrier = _player(protocol, 30001, 0, hp=20_000, phys=5000, speed=200)
    req_barrier = _create_request(protocol, 5110, [p_barrier], mode=4, config_id=105, ranked=False)
    req_barrier.trial_context.journal_entry_id = 201
    req_barrier.trial_context.trial_rule_version = 1
    eng_barrier = TurnEngine(
        protocol,
        req_barrier,
        monsters=[
            _make_boss_monster(protocol, 105, 10_000, is_boss=True),
            _make_boss_monster(protocol, 106, 4_000, is_boss=False),
        ],
        skills=bt.skills,
        items=bt.items,
        buffs=bt.buffs,
        max_rounds=12,
        boss_trial_kind=BOSS_TRIAL_KIND_BARRIER,
        battle_difficulty=1,
    )
    boss_id = next(a.actor_id for a in eng_barrier.actors.values() if a.is_boss)
    eng_barrier.pending[30001] = protocol.message("BattleAction", action_type=3)
    eng_barrier.resolve()
    assert eng_barrier.encounter.boss_barrier_shield > 0

    eng_barrier.pending[30001] = protocol.message("BattleAction", action_type=1, target_id=boss_id)
    br2 = eng_barrier.resolve()
    assert br2.state.boss_exposed_pct == BOSS_EXPOSED_FULL_PCT
    assert any(
        ev.event_type == EVENT_BOSS_MECHANIC
        and ev.mechanic_kind == BOSS_MECHANIC_STANCE_BREAK
        and ev.value == BOSS_EXPOSED_FULL_PCT
        for ev in br2.events
    )
    assert eng_barrier.trial_tracker.stance_break_count >= 1

    # 2) 灵障护卫（GUARD_SHIELD）与缚灵护卫（GUARD_BIND）被控打断均触发首领 +20% 破绽与崩势计数。
    for trial_kind, expected_intent, expected_label in (
        (BOSS_TRIAL_KIND_BARRIER, TELEGRAPH_KIND_GUARD_SHIELD, "灵障护佑被控打断，首领陷入破绽"),
        (BOSS_TRIAL_KIND_VENOM, TELEGRAPH_KIND_GUARD_BIND, "缚灵锁脉被控打断，首领陷入破绽"),
    ):
        eng_ctrl = TurnEngine(
            protocol,
            _create_request(protocol, 5120 + trial_kind, [_player(protocol, 30001, 0, hp=20_000, speed=200)], mode=4, ranked=False),
            monsters=[
                _make_boss_monster(protocol, 105, 20_000, is_boss=True),
                _make_boss_monster(protocol, 106, 8_000, is_boss=False),
            ],
            skills=bt.skills,
            items=bt.items,
            buffs=bt.buffs,
            max_rounds=12,
            boss_trial_kind=trial_kind,
            battle_difficulty=1,
        )
        guard = next(a for a in eng_ctrl.actors.values() if a.actor_type == 2 and not a.is_boss)
        if trial_kind == BOSS_TRIAL_KIND_VENOM:
            # 缚灵锁脉在 R1 预告、R2 兑现
            eng_ctrl.pending[30001] = protocol.message("BattleAction", action_type=3)
            eng_ctrl.resolve()
        # 给护卫挂上冰冻控制（100380: battle_op=13, state_kind=6）；玩家普攻以隔离幽毒 AoE 防御破绽
        guard.buffs.add(buff_id=999, buff_table_id=100380, caster_id=30001, layer=1, remain_rounds=2, applied_round=eng_ctrl.round_index)
        eng_ctrl.pending[30001] = protocol.message("BattleAction", action_type=1, target_id=guard.actor_id)
        res_ctrl = eng_ctrl.resolve()
        assert res_ctrl.state.boss_exposed_pct == BOSS_EXPOSED_PARTIAL_PCT
        assert eng_ctrl.trial_tracker.stance_break_count >= 1
        assert any(
            t.intent_kind == expected_intent
            and t.status == TELEGRAPH_STATUS_CANCELLED_BY_CONTROL
            and t.label == expected_label
            for t in res_ctrl.resolved_telegraphs
        )

    # 3) 幽毒首领（kind=4）：2 人局 R2 玄毒冲击 1/2 防御触发 +20% 轻破绽，2/2 全员防御触发 +35% 完整破绽。
    for defend_second, expected_pct in ((False, BOSS_EXPOSED_PARTIAL_PCT), (True, BOSS_EXPOSED_FULL_PCT)):
        eng_v = TurnEngine(
            protocol,
            _create_request(
                protocol,
                5130 + int(defend_second),
                [
                    _player(protocol, 30001, 0, hp=20_000, speed=200),
                    _player(protocol, 30002, 0, hp=20_000, speed=190),
                ],
                mode=4,
                ranked=False,
            ),
            monsters=[
                _make_boss_monster(protocol, 107, 30_000, is_boss=True),
                _make_boss_monster(protocol, 108, 8_000, is_boss=False),
            ],
            skills=bt.skills,
            items=bt.items,
            buffs=bt.buffs,
            max_rounds=12,
            boss_trial_kind=BOSS_TRIAL_KIND_VENOM,
            battle_difficulty=1,
        )
        eng_v.pending[30001] = protocol.message("BattleAction", action_type=3)
        eng_v.pending[30002] = protocol.message("BattleAction", action_type=3)
        eng_v.resolve()
        eng_v.pending[30001] = protocol.message("BattleAction", action_type=3)
        eng_v.pending[30002] = protocol.message("BattleAction", action_type=3 if defend_second else 1, target_id=0)
        vr2 = eng_v.resolve()
        assert vr2.state.boss_exposed_pct == expected_pct
        assert any(
            ev.event_type == EVENT_BOSS_MECHANIC
            and ev.mechanic_kind == BOSS_MECHANIC_CLEAVE_PARRY_EXPOSE
            and ev.value == expected_pct
            for ev in vr2.events
        )


# ── 3. 战法 V3 新机制（策略副目标、单次攻击标记消耗、蓄力/打断、专修互斥、易伤与免控）──────────────


def test_strategy_secondary_targets_and_one_shot_attack_mark():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)

    # 1) S05 逐命斩(10105, LOWEST_HEALTH_VALUE): 主目标 201，其余 2 个名额按最低血量确定性选中 203(1000) 与 205(1500)。
    p1 = _player(protocol, 101, 0, skills=(10105,), skill_ranks={10105: 1}, speed=300)
    enemies = [
        _player(protocol, 201, 1, hp=9000),
        _player(protocol, 202, 1, hp=8000),
        _player(protocol, 203, 1, hp=1000),
        _player(protocol, 204, 1, hp=7000),
        _player(protocol, 205, 1, hp=1500),
    ]
    eng = TurnEngine(
        protocol,
        _create_request(protocol, 8001, [p1, *enemies], mode=6, ranked=False),
        skills=bt.skills,
        items=bt.items,
        buffs=bt.buffs,
        max_rounds=20,
    )
    for eid in (201, 202, 203, 204, 205):
        eng.pending[eid] = protocol.message("BattleAction", action_type=3)
    eng.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10105, target_id=201)
    r1 = eng.resolve()
    victims = {ev.target_id for ev in r1.events if ev.event_type == 3 and ev.source_id == 101 and ev.skill_table_id == 10105}
    assert victims == {201, 203, 205}

    # 2) S09 授锋战印(10109, HIGHEST_PHYSICAL_ATTACK + NEXT_PHYSICAL_ATTACK):
    #    给主目标与物攻最高队友挂 101090，下回合施放物理战法 10002 后一次性消耗。
    mark_players = [
        _player(protocol, 101, 0, skills=(10109, 10002), skill_ranks={10109: 1, 10002: 1}, speed=300, phys=300),
        _player(protocol, 102, 0, phys=999, speed=150),
        _player(protocol, 201, 1, speed=100),
        _player(protocol, 202, 1, speed=100),
    ]
    eng_mark = TurnEngine(
        protocol,
        _create_request(protocol, 8002, mark_players, mode=6, ranked=False),
        skills=bt.skills,
        items=bt.items,
        buffs=bt.buffs,
        max_rounds=20,
    )
    for pid in (102, 201, 202):
        eng_mark.pending[pid] = protocol.message("BattleAction", action_type=3)
    eng_mark.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10109, target_id=101)
    eng_mark.resolve()
    assert any(b.buff_table_id == 101090 for b in eng_mark.actors[101].buffs)
    assert any(b.buff_table_id == 101090 for b in eng_mark.actors[102].buffs)

    for pid in (102, 201, 202):
        eng_mark.pending[pid] = protocol.message("BattleAction", action_type=3)
    eng_mark.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10002, target_id=201)
    eng_mark.resolve()
    assert not any(b.buff_table_id == 101090 for b in eng_mark.actors[101].buffs)

    # 3) S13 掠影追锋(10113, LOWEST_ROUND_SPEED): 锁定主目标 201，按本回合开场冻结速度选中最慢的 204(50) 与 205(70)。
    spd_players = [
        _player(protocol, 101, 0, skills=(10113,), skill_ranks={10113: 1}, speed=400),
        _player(protocol, 201, 1, speed=300),
        _player(protocol, 202, 1, speed=250),
        _player(protocol, 203, 1, speed=200),
        _player(protocol, 204, 1, speed=50),
        _player(protocol, 205, 1, speed=70),
    ]
    eng_spd = TurnEngine(
        protocol,
        _create_request(protocol, 8003, spd_players, mode=6, ranked=False),
        skills=bt.skills,
        items=bt.items,
        buffs=bt.buffs,
        max_rounds=20,
    )
    for eid in (201, 202, 203, 204, 205):
        eng_spd.pending[eid] = protocol.message("BattleAction", action_type=3)
    eng_spd.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10113, target_id=201)
    r_spd = eng_spd.resolve()
    spd_victims = {ev.target_id for ev in r_spd.events if ev.event_type == 3 and ev.source_id == 101 and ev.skill_table_id == 10113}
    assert spd_victims == {201, 204, 205}


def test_prepare_cast_charging_unleash_and_freeze_interrupt():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)

    # 1) E01 蓄锋破阵(10115): R1 蓄力零伤且公开 charging_skill_id=10115，R2 自动爆发并清 0。
    eng = TurnEngine(
        protocol,
        _create_request(
            protocol,
            8010,
            [
                _player(protocol, 101, 0, skills=(10115,), skill_ranks={10115: 1}, speed=200),
                _player(protocol, 201, 1, speed=150),
            ],
            mode=6,
            ranked=False,
        ),
        skills=bt.skills,
        items=bt.items,
        buffs=bt.buffs,
        max_rounds=20,
    )
    eng.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10115, target_id=201)
    eng.pending[201] = protocol.message("BattleAction", action_type=3)
    r1 = eng.resolve()
    assert not any(ev.event_type == 3 and ev.source_id == 101 for ev in r1.events)
    assert eng.actors[101].charging_skill_id == 10115
    # 蓄力期间不允许重复提交蓄力战法（7004）。
    assert eng.validate(eng.actors[101], protocol.message("BattleAction", action_type=2, skill_table_id=10115, target_id=201)) == 7004

    eng.pending[101] = protocol.message("BattleAction", action_type=3)
    eng.pending[201] = protocol.message("BattleAction", action_type=3)
    r2 = eng.resolve()
    assert any(ev.event_type == 3 and ev.source_id == 101 and ev.skill_table_id == 10115 and ev.value > 0 for ev in r2.events)
    assert eng.actors[101].charging_skill_id == 0

    # 2) R2 出手前被敌方高速冰冻(10038)打断：不造成蓄力伤害且清除 charging_skill_id。
    buffs_guaranteed = dict(bt.buffs)
    buffs_guaranteed[100380] = dataclasses.replace(buffs_guaranteed[100380], application_chance=1.0)
    eng_int = TurnEngine(
        protocol,
        _create_request(
            protocol,
            8011,
            [
                _player(protocol, 101, 0, skills=(10115,), skill_ranks={10115: 1}, speed=100),
                _player(protocol, 201, 1, skills=(10038,), skill_ranks={10038: 1}, speed=500),
            ],
            mode=6,
            ranked=False,
        ),
        skills=bt.skills,
        items=bt.items,
        buffs=buffs_guaranteed,
        max_rounds=20,
    )
    eng_int.pending[101] = protocol.message("BattleAction", action_type=2, skill_table_id=10115, target_id=201)
    eng_int.pending[201] = protocol.message("BattleAction", action_type=3)
    eng_int.resolve()
    assert eng_int.actors[101].charging_skill_id == 10115

    eng_int.pending[101] = protocol.message("BattleAction", action_type=3)
    eng_int.pending[201] = protocol.message("BattleAction", action_type=2, skill_table_id=10038, target_id=101)
    r2_int = eng_int.resolve()
    assert not any(ev.event_type == 3 and ev.source_id == 101 and ev.skill_table_id == 10115 for ev in r2_int.events)
    assert eng_int.actors[101].charging_skill_id == 0


def test_specialization_exclusive_group_forbid_and_hit_vulnerability_control_immunity():
    protocol = Protocol()
    tables = ConfigTables()
    bt = BattleTables(protocol, tables)

    # 1) 互斥战法组 exclusive_group=1：同时携带 12101(灵法专修) 与 12102(兵锋专修) 仅保留 12101；
    #    12101 禁普攻与物理战法（7004），放行法术战法(10006)，未提交指令自动兜底防御。
    p_spec = _player(
        protocol,
        101,
        0,
        skills=(10002, 10006),
        skill_ranks={12101: 1, 12102: 1, 10002: 1, 10006: 1},
    )
    eng_spec = TurnEngine(
        protocol,
        _create_request(protocol, 8020, [p_spec, _player(protocol, 201, 1)], mode=6, ranked=False),
        skills=bt.skills,
        items=bt.items,
        buffs=bt.buffs,
        max_rounds=20,
    )
    assert 12101 in eng_spec.actors[101].skill_ranks and 12102 not in eng_spec.actors[101].skill_ranks
    assert eng_spec.validate(eng_spec.actors[101], protocol.message("BattleAction", action_type=1, target_id=201)) == 7004
    assert eng_spec.validate(eng_spec.actors[101], protocol.message("BattleAction", action_type=2, skill_table_id=10002, target_id=201)) == 7004
    assert eng_spec.validate(eng_spec.actors[101], protocol.message("BattleAction", action_type=2, skill_table_id=10006, target_id=201)) == 0

    # 2) E05 截灵追击(13101) + E07 连击破绽(14101) + E08 净心护阵(14102) 端到端闭环。
    buffs_100 = dict(bt.buffs)
    buffs_100[131010] = dataclasses.replace(buffs_100[131010], trigger_chance=1.0)
    buffs_100[131011] = dataclasses.replace(buffs_100[131011], application_chance=1.0)
    buffs_100[100380] = dataclasses.replace(buffs_100[100380], application_chance=1.0)

    eng_aura = TurnEngine(
        protocol,
        _create_request(
            protocol,
            8021,
            [
                _player(protocol, 101, 0, skill_ranks={13101: 1, 14101: 1, 14102: 1}, speed=200),
                _player(protocol, 201, 1, skills=(10038,), skill_ranks={10038: 1}, speed=300),
            ],
            mode=6,
            ranked=False,
        ),
        skills=bt.skills,
        items=bt.items,
        buffs=buffs_100,
        max_rounds=20,
    )
    assert any(b.buff_table_id == 141010 for b in eng_aura.actors[201].buffs)
    assert any(b.buff_table_id == 141020 for b in eng_aura.actors[101].buffs)

    eng_aura.pending[101] = protocol.message("BattleAction", action_type=1, target_id=201)
    eng_aura.pending[201] = protocol.message("BattleAction", action_type=2, skill_table_id=10038, target_id=101)
    r1 = eng_aura.resolve()
    # 敌方冰冻应被净心护阵挡下（发出 RESIST 并消耗 141020），101 正常出手普攻叠 1 层连击破绽并触发 131011 封法。
    assert any(ev.event_type == 15 and ev.target_id == 101 and ev.buff_table_id == 100380 for ev in r1.events)
    assert not any(b.buff_table_id == 141020 for b in eng_aura.actors[101].buffs)
    vuln = next(b for b in eng_aura.actors[201].buffs if b.buff_table_id == 141010)
    assert vuln.stacks == 1
    assert any(b.buff_table_id == 131011 for b in eng_aura.actors[201].buffs)
    assert 101 in eng_aura.trial_tracker.anti_control_or_season_skill_players
