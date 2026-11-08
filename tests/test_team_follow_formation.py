from math import ceil, cos, radians, sin

import pytest

from xuanming.nodes.team_follow_formation import (
    FeedResult,
    TeamFollowBroadcastState,
    TeamFollowFormation,
    TeamFollowMember,
    TeamFollowTrail,
    horizontal_distance,
    should_broadcast,
)


def _point(x, y=0.0, z=0.0):
    return x, y, z


def _column(size):
    formation = TeamFollowFormation(_point(0))
    for i in range(1, 41):
        formation.tick(_point(i * 0.5), 0.05)
    for rank in reversed(range(size)):
        member = TeamFollowMember(100 + rank, rank + 1, formation.slot_odo(rank))
        assert formation.add(member)
    return formation


def _walk(formation, start, end):
    length = horizontal_distance(start, end)
    for i in range(1, ceil(length / 0.45 - 1e-9) + 1):
        t = min(1.0, 0.45 * i / length)
        point = tuple(a + (b - a) * t for a, b in zip(start, end))
        yield formation.tick(point, 0.05)


def _positions(formation):
    return [formation.trail.point_at(member.cursor_odo) for member in formation.members]


def test_followers_move_on_the_same_tick_and_take_the_leaders_corner():
    formation = _column(4)
    for steps in _walk(formation, _point(20), _point(20, 5.4)):
        assert [step.player_id for step in steps] == [100, 101, 102, 103]
        for step in steps:
            assert not step.placed and not step.suspended
            assert step.moved == pytest.approx(0.45)
            x, y, _ = step.position
            assert (y == 0 and x <= 20) or (x == 20 and y >= 0)
    for actual, expected in zip(_positions(formation), (
        _point(20, 4), _point(20, 2.6), _point(20, 1.2), _point(19.8),
    )):
        assert actual == pytest.approx(expected)
    assert all(step.moved == 0 for step in formation.tick(_point(20, 5.4), 0.05))


def test_short_rewinds_do_not_move_members_but_a_real_reversal_preserves_order():
    formation = _column(4)
    points = len(formation.trail)
    starting_positions = _positions(formation)
    for back in (1.0, 0.8) * 5:
        for start, end in ((_point(20), _point(20 - back)), (_point(20 - back), _point(20))):
            assert all(step.moved == pytest.approx(0) for steps in _walk(formation, start, end) for step in steps)
    assert _positions(formation) == starting_positions
    assert len(formation.trail) <= points + 3

    for steps in _walk(formation, _point(20), _point(10)):
        assert all(step.moved >= 0 and step.position[1] == 0 for step in steps)
    assert [point[0] for point in _positions(formation)] == pytest.approx([11.4, 12.8, 14.2, 15.6])


def test_trail_rewind_respects_navigation_and_the_leading_members_cursor():
    trail = TeamFollowTrail()
    assert trail.nearest_odo(_point(0), 1) is None
    assert trail.feed(_point(0)) is FeedResult.APPENDED
    assert trail.feed(_point(0.01, z=12)) is FeedResult.IGNORED
    for i in range(1, 5):
        assert trail.feed(_point(0.45 * i)) is FeedResult.APPENDED
    assert trail.feed(_point(1.35), 1.2) is FeedResult.REWOUND
    assert trail.feed(_point(0.9), 1.2) is FeedResult.REWOUND
    assert trail.point_at(1.2) == pytest.approx(_point(1.2))
    assert trail.head == _point(0.9) and trail.head_odo == pytest.approx(1.5)
    assert trail.nearest_odo(_point(1.1), 0.5) == pytest.approx(1.3)

    blocked = TeamFollowTrail(_point(0))
    blocked.feed(_point(0.45))
    blocked.feed(_point(0.9))
    shortcuts = []

    def cannot_walk(start, end):
        shortcuts.append((start, end))
        return False

    assert blocked.feed(_point(0.6), walkable=cannot_walk) is FeedResult.APPENDED
    assert len(shortcuts) == 1 and blocked.head_odo == pytest.approx(1.2)


def test_trail_prepend_and_prune_keep_interpolation_and_height():
    trail = TeamFollowTrail(_point(0))
    trail.feed(_point(2, z=4))
    trail.feed(_point(2, 2, 6))
    assert trail.point_at(1) == _point(1, z=2)
    assert trail.point_at(3) == _point(2, 1, 5)
    assert trail.point_at(100) == _point(2, 2, 6)
    assert trail.nearest_odo(_point(2.3, 1.5, 30), 0.5) == pytest.approx(3.5)
    assert trail.nearest_odo(_point(4, 4), 0.5) is None

    trail.trim_tail_to(1)
    assert trail.tail_odo == 1 and trail.tail == _point(1, z=2)
    trail.prepend_path((_point(1, -3, 2), _point(1, -1, 2), trail.tail))
    assert trail.tail_odo == -2 and trail.point_at(-1.5) == _point(1, -2.5, 2)
    trail.prune_before(3)
    assert trail.tail_odo == 2 and trail.point_at(3) == _point(2, 1, 5)


def test_ranked_newcomer_catches_up_and_removal_keeps_spatial_slots():
    formation = _column(2)
    rank = formation.rank_for(3, 105)
    assert rank == 2
    formation.trim_tail_for_newcomer(rank)
    assert formation.trail.tail == pytest.approx(_point(15.8))
    formation.trail.prepend_path((_point(16, 2), formation.trail.tail))
    assert formation.add(TeamFollowMember(105, 3, formation.trail.tail_odo))
    assert not formation.add(TeamFollowMember(105, 1))
    assert formation.rank_for(3, 104) == 2
    assert formation.rank_for(3, 106) == 3
    assert formation.add(TeamFollowMember(106, 3, formation.slot_odo(3)))
    assert not formation.add(TeamFollowMember(107, 4))
    assert formation.remove(106)
    for _ in range(10):
        steps = formation.tick(_point(20), 0.05)
        assert all(step.moved == 0 for step in steps[:2])
        assert 0 <= steps[2].moved <= 0.54 + 1e-9
    assert _positions(formation)[2] == pytest.approx(_point(15.8))
    assert formation.remove(100)
    for _ in range(4):
        assert all(step.moved <= 0.54 + 1e-9 for step in formation.tick(_point(20), 0.05))
    assert [point[0] for point in _positions(formation)] == pytest.approx([18.6, 17.2])


def test_suspended_follower_teleports_only_after_the_native_lag_threshold():
    formation = _column(2)
    formation.set_suspended(100, True)
    for steps in _walk(formation, _point(20), _point(44)):
        assert steps[0].suspended and not steps[0].placed
        assert not steps[1].suspended and not steps[1].placed
    member = formation.find(100)
    assert not member.needs_placement and member.cursor_odo == pytest.approx(18.6)
    assert formation.trail.tail_odo <= member.cursor_odo
    assert formation.tick(_point(44.1), 0.05)[0].suspended
    assert member.needs_placement
    assert formation.trail.length < 8

    formation.set_suspended(100, False)
    first, second = formation.tick(_point(44.1), 0.05)
    assert first.placed and first.moved == 0 and not first.suspended
    assert first.position == pytest.approx(_point(42.7))
    assert not second.placed and not member.needs_placement
    for i in range(1, 800):
        formation.tick(_point(44.1 + 0.45 * i), 0.05)
    assert len(formation.trail) <= 20 and 5.6 <= formation.trail.length < 8


def test_broadcast_throttle_matches_native_start_stop_turn_speed_and_interval():
    still = TeamFollowBroadcastState()
    east = TeamFollowBroadcastState(True, 10, 0)
    assert should_broadcast(still, east, 0) and should_broadcast(east, still, 0)
    assert not should_broadcast(still, still, 100)
    assert not should_broadcast(east, east, 4) and should_broadcast(east, east, 5)
    for angle, expected in ((10, False), (30, True)):
        now = TeamFollowBroadcastState(True, 10 * cos(radians(angle)), 10 * sin(radians(angle)))
        assert should_broadcast(east, now, 1) is expected
    assert not should_broadcast(east, TeamFollowBroadcastState(True, 11.5), 1)
    assert should_broadcast(east, TeamFollowBroadcastState(True, 11.501), 1)
    assert should_broadcast(TeamFollowBroadcastState(True), east, 1)
