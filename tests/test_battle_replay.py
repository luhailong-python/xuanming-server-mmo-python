import hashlib

import pytest

from xuanming.nodes import battle_replay
from xuanming.nodes.battle_replay import BattleReplayRecorder, decode_replay
from xuanming.protocol import Protocol


def _request(p):
    return p.message("CreateBattleRequest", battle_id=9001, match_mode=3, battle_config_id=17,
                     created_at_ms=1_700_000_000_000, record_replay=True)


def _state(p, round_index):
    state = p.message("BattleStateS2C", battle_id=9001, round_index=round_index,
                      action_deadline_ms=1_700_000_006_000, pending_actor_ids=[11])
    for actor_id, name, team in ((11, "甲", 0), (22, "乙", 1)):
        actor = state.actors.add(actor_id=actor_id, name=name, team_index=team, max_health=1000)
        actor.attributes.health = 1000 - round_index * 10
    return state


def _round(p, round_index):
    result = p.message("TurnResultS2C", battle_id=9001, round_index=round_index, action_order=[11, 22],
                       state=_state(p, round_index))
    result.events.add(event_type=3, source_id=11, target_id=22, value=10, target_health_after=1000 - round_index * 10)
    return result


def test_replay_matches_native_envelope_fields_and_strips_private_data():
    p = Protocol()
    start = _state(p, 1)
    start.self_items.add(item_table_id=13, count=1)
    start.actors[0].combat.physical_crit_rate = 10
    start.actors[0].skill_cooldown_rounds[15] = 2
    recorder = BattleReplayRecorder(p, _request(p), "fingerprint-v1", start)
    recorder.add_round(_round(p, 1))
    recorder.add_round(_round(p, 2))
    envelope = recorder.freeze(1, 2, 1_700_000_012_000)
    replay = decode_replay(p, envelope)
    assert envelope.format_version == envelope.codec == 1
    assert replay.battle_id == 9001 and replay.end_reason == 1 and not replay.HasField("final_state")
    assert list(replay.rounds[1].action_order) == [11, 22]
    assert not replay.start_state.pending_actor_ids and not replay.start_state.self_items
    assert not replay.start_state.actors[0].HasField("combat") and not replay.start_state.actors[0].skill_cooldown_rounds
    assert all(not frame.state.action_deadline_ms for frame in replay.rounds)
    # Native C++ protobuf, using the same source recorder fixture before gzip.
    assert hashlib.sha256(replay.SerializeToString(deterministic=True)).hexdigest() == (
        "1674bfae1c65ac49d2c67348a26362225378dce8964ae96bde187a764e453fac"
    )
    assert recorder.freeze(1, 2, 1_700_000_012_000) is None


def test_replay_stops_at_budget_and_bounds_decompression(monkeypatch):
    p = Protocol()
    monkeypatch.setattr(battle_replay, "RECORD_BUDGET_BYTES", 120)
    recorder = BattleReplayRecorder(p, _request(p), "fingerprint-v1", _state(p, 1))
    recorder.add_round(_round(p, 1))
    recorder.add_round(_round(p, 2))
    recorder.mark_deadline_draw(_state(p, 3))
    envelope = recorder.freeze(3, 2, 1_700_000_300_000)
    replay = decode_replay(p, envelope)
    assert replay.truncated and replay.total_rounds == 2 and replay.end_reason == 2
    assert len(replay.rounds) < replay.total_rounds and replay.final_state.round_index == 3
    assert replay.final_state.action_deadline_ms == 0
    envelope.raw_bytes -= 1
    with pytest.raises(ValueError):
        decode_replay(p, envelope)


def test_record_replay_policy_matches_all_pvp_modes():
    from xuanming.match.model import (
        MODE_1V1, MODE_3V3, MODE_5V5, MODE_POLARITY_5V5, MODE_PVE_SOLO,
        MODE_PVE_TEAM, MODE_PVP_CHALLENGE, MODE_PVP_TEAM, MODE_SEASON_TOURNAMENT,
        MODE_UNSPECIFIED, record_replay_for,
    )
    for pvp in (MODE_1V1, MODE_3V3, MODE_5V5, MODE_PVP_CHALLENGE, MODE_PVP_TEAM,
                MODE_POLARITY_5V5, MODE_SEASON_TOURNAMENT):
        assert record_replay_for(pvp) is True
    for non_pvp in (MODE_UNSPECIFIED, MODE_PVE_SOLO, MODE_PVE_TEAM, 99):
        assert record_replay_for(non_pvp) is False
