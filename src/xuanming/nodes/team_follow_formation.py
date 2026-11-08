"""按队长实际走过的轨迹推进队形。坐标、里程和速度均使用米制。"""

from bisect import bisect_left, bisect_right
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from enum import IntEnum
from math import hypot, sqrt
from operator import attrgetter
from sys import float_info

FollowPoint = tuple[float, float, float]
WalkableCheck = Callable[[FollowPoint, FollowPoint], bool]

SPACING = 1.4
MAX_MEMBERS = 4
CATCH_UP_SPEED = 10.8
MAX_LAG = 24.0
MAX_LEADER_JUMP = 4.0
TRAIL_MIN_STEP = 0.05
TRAIL_REWIND_RADIUS = 0.6
TRAIL_REWIND_MIN_GAIN = 0.05
TRAIL_REWIND_SEARCH_LENGTH = 4.0
TRAIL_KEEP_BEHIND_HEAD = (MAX_MEMBERS + 1) * SPACING
BROADCAST_INTERVAL_FRAMES = 5
TURN_BROADCAST_COS = 0.94
SPEED_CHANGE_RATIO = 0.15

_NO_PROTECTION = -float_info.max
_ZERO = (0.0, 0.0, 0.0)


class FeedResult(IntEnum):
    IGNORED = 0
    APPENDED = 1
    REWOUND = 2


def horizontal_distance(a: FollowPoint, b: FollowPoint) -> float:
    dx, dy = a[0] - b[0], a[1] - b[1]
    return sqrt(dx * dx + dy * dy)


@dataclass(frozen=True, slots=True)
class _Node:
    point: FollowPoint
    odo: float


_ODO = attrgetter("odo")


def _lerp(a: _Node, b: _Node, t: float) -> _Node:
    ax, ay, az = a.point
    bx, by, bz = b.point
    return _Node((ax + (bx - ax) * t, ay + (by - ay) * t, az + (bz - az) * t), a.odo + (b.odo - a.odo) * t)


def _at_odo(a: _Node, b: _Node, odo: float) -> _Node:
    span = b.odo - a.odo
    return _lerp(a, b, min(1.0, max(0.0, (odo - a.odo) / span)) if span > 0 else 0.0)


def _project(a: _Node, b: _Node, point: FollowPoint) -> _Node:
    dx, dy = b.point[0] - a.point[0], b.point[1] - a.point[1]
    squared = dx * dx + dy * dy
    if squared <= 0:
        return a
    t = ((point[0] - a.point[0]) * dx + (point[1] - a.point[1]) * dy) / squared
    return _lerp(a, b, min(1.0, max(0.0, t)))


class TeamFollowTrail:
    def __init__(self, head: FollowPoint | None = None) -> None:
        self._nodes: list[_Node] = []
        if head is not None:
            self.reset(head)

    def __len__(self) -> int:
        return len(self._nodes)

    def reset(self, head: FollowPoint) -> None:
        self._nodes[:] = [_Node(head, 0.0)]

    @property
    def head(self) -> FollowPoint:
        return self._nodes[-1].point

    @property
    def tail(self) -> FollowPoint:
        return self._nodes[0].point

    @property
    def head_odo(self) -> float:
        return self._nodes[-1].odo

    @property
    def tail_odo(self) -> float:
        return self._nodes[0].odo

    @property
    def length(self) -> float:
        return self.head_odo - self.tail_odo

    def _append(self, point: FollowPoint) -> None:
        self._nodes.append(_Node(point, self.head_odo + horizontal_distance(self.head, point)))

    def feed(
        self, leader: FollowPoint, protect_odo: float = _NO_PROTECTION, walkable: WalkableCheck | None = None,
    ) -> FeedResult:
        if not self._nodes:
            self.reset(leader)
            return FeedResult.APPENDED
        head_odo = self.head_odo
        step = horizontal_distance(self.head, leader)
        if step < TRAIL_MIN_STEP:
            return FeedResult.IGNORED

        # 只剪队员还没走到的头部折返，并且先确认捷径可走。
        floor_odo = max(protect_odo, head_odo - TRAIL_REWIND_SEARCH_LENGTH)
        best, best_end, best_gain = None, 0, TRAIL_REWIND_MIN_GAIN
        for end in range(len(self._nodes) - 1, 0, -1):
            a, b = self._nodes[end - 1], self._nodes[end]
            if b.odo <= floor_odo:
                break
            candidate = _project(a, b, leader)
            if candidate.odo < floor_odo:
                candidate = _at_odo(a, b, floor_odo)
            shortcut = horizontal_distance(candidate.point, leader)
            gain = (head_odo - candidate.odo) + step - shortcut
            if shortcut <= TRAIL_REWIND_RADIUS and gain > best_gain:
                best, best_end, best_gain = candidate, end, gain

        if best is not None and (walkable is None or walkable(best.point, leader)):
            del self._nodes[best_end:]
            if horizontal_distance(self.head, best.point) >= TRAIL_MIN_STEP:
                self._nodes.append(best)
            if horizontal_distance(self.head, leader) >= TRAIL_MIN_STEP:
                self._append(leader)
            return FeedResult.REWOUND
        self._append(leader)
        return FeedResult.APPENDED

    def point_at(self, odo: float) -> FollowPoint:
        if odo <= self.tail_odo:
            return self.tail
        if odo >= self.head_odo:
            return self.head
        end = bisect_left(self._nodes, odo, key=_ODO)
        return _at_odo(self._nodes[end - 1], self._nodes[end], odo).point

    def nearest_odo(self, point: FollowPoint, max_distance: float) -> float | None:
        if not self._nodes:
            return None
        best_distance, best_odo = horizontal_distance(self.tail, point), self.tail_odo
        for end in range(1, len(self._nodes)):
            candidate = _project(self._nodes[end - 1], self._nodes[end], point)
            distance = horizontal_distance(candidate.point, point)
            # 发夹弯等距时，接到离队长更近的那一趟。
            if distance <= best_distance:
                best_distance, best_odo = distance, candidate.odo
        return best_odo if best_distance <= max_distance else None

    def prepend_path(self, path: Sequence[FollowPoint]) -> None:
        if not self._nodes:
            return
        tail, odo = self.tail, self.tail_odo
        added = []
        for point in reversed(path):
            distance = horizontal_distance(point, tail)
            if distance >= TRAIL_MIN_STEP:
                odo -= distance
                added.append(_Node(point, odo))
                tail = point
        self._nodes[:0] = reversed(added)

    def trim_tail_to(self, odo: float) -> None:
        if len(self._nodes) < 2 or odo <= self.tail_odo:
            return
        if odo >= self.head_odo:
            self._nodes[:] = [self._nodes[-1]]
            return
        cut = self.point_at(odo)
        self.prune_before(odo)
        self._nodes[0] = _Node(cut, odo)

    def prune_before(self, odo: float) -> None:
        # 留下覆盖该里程的线段起点，后续插值不变。
        end = bisect_right(self._nodes, odo, key=_ODO) - 1
        if end > 0:
            del self._nodes[:end]


@dataclass(slots=True)
class TeamFollowMember:
    player_id: int
    join_seq: int
    cursor_odo: float = 0.0
    suspended: bool = False
    needs_placement: bool = False


@dataclass(frozen=True, slots=True)
class TeamFollowStep:
    player_id: int
    position: FollowPoint = _ZERO
    moved: float = 0.0
    placed: bool = False
    suspended: bool = False


class TeamFollowFormation:
    def __init__(self, leader: FollowPoint, spacing: float = SPACING) -> None:
        self.trail, self.spacing = TeamFollowTrail(leader), spacing
        self._members: list[TeamFollowMember] = []

    @property
    def members(self) -> tuple[TeamFollowMember, ...]:
        return tuple(self._members)

    def rank_of(self, player_id: int) -> int | None:
        return next((i for i, member in enumerate(self._members) if member.player_id == player_id), None)

    def find(self, player_id: int) -> TeamFollowMember | None:
        rank = self.rank_of(player_id)
        return self._members[rank] if rank is not None else None

    def rank_for(self, join_seq: int, player_id: int) -> int:
        return sum(member.player_id != player_id and (member.join_seq, member.player_id) < (join_seq, player_id)
                   for member in self._members)

    def add(self, member: TeamFollowMember) -> bool:
        if not member.player_id or self.find(member.player_id) is not None or len(self._members) >= MAX_MEMBERS:
            return False
        self._members.insert(self.rank_for(member.join_seq, member.player_id), replace(member))
        return True

    def remove(self, player_id: int) -> bool:
        rank = self.rank_of(player_id)
        if rank is None:
            return False
        del self._members[rank]
        return True

    def set_suspended(self, player_id: int, suspended: bool) -> None:
        if (member := self.find(player_id)) is not None:
            member.suspended = suspended

    def _ideal_slot(self, rank: int) -> float:
        return self.trail.head_odo - (rank + 1) * self.spacing

    def slot_odo(self, rank: int) -> float:
        return max(self.trail.tail_odo, self._ideal_slot(rank))

    def protect_odo(self) -> float:
        return max((member.cursor_odo for member in self._members if not member.needs_placement), default=_NO_PROTECTION)

    def _keep_from(self, odo: float) -> float:
        for member in self._members:
            if not member.needs_placement:
                odo = min(odo, member.cursor_odo)
        return odo

    def keep_odo(self) -> float:
        return self._keep_from(self.trail.head_odo - TRAIL_KEEP_BEHIND_HEAD)

    def trim_tail_for_newcomer(self, rank: int) -> None:
        self.trail.trim_tail_to(self._keep_from(self._ideal_slot(rank)))

    def tick(
        self, leader: FollowPoint, delta_seconds: float, walkable: WalkableCheck | None = None,
    ) -> tuple[TeamFollowStep, ...]:
        head_before = self.trail.head_odo
        self.trail.feed(leader, self.protect_odo(), walkable)
        advance = max(0.0, self.trail.head_odo - head_before)
        max_step = max(advance, CATCH_UP_SPEED * max(0.0, delta_seconds))
        steps = []
        for rank, member in enumerate(self._members):
            slot = self.slot_odo(rank)
            lag = slot - member.cursor_odo
            if member.suspended:
                if lag > MAX_LAG:
                    member.needs_placement = True
                steps.append(TeamFollowStep(member.player_id, suspended=True))
            elif member.needs_placement or lag > MAX_LAG:
                member.cursor_odo, member.needs_placement = slot, False
                steps.append(TeamFollowStep(member.player_id, self.trail.point_at(slot), placed=True))
            else:
                # 槽位回退时游标不退，原地等队长继续走。
                prior = member.cursor_odo
                if lag > 0:
                    member.cursor_odo = min(slot, prior + max_step)
                steps.append(TeamFollowStep(member.player_id, self.trail.point_at(member.cursor_odo), member.cursor_odo - prior))
        self.trail.prune_before(self.keep_odo())
        return tuple(steps)


@dataclass(frozen=True, slots=True)
class TeamFollowBroadcastState:
    moving: bool = False
    velocity_x: float = 0.0
    velocity_y: float = 0.0


def should_broadcast(last: TeamFollowBroadcastState, now: TeamFollowBroadcastState, frames_since_last: int) -> bool:
    if last.moving != now.moving:
        return True
    if not now.moving:
        return False
    if frames_since_last >= BROADCAST_INTERVAL_FRAMES:
        return True
    old_speed = hypot(last.velocity_x, last.velocity_y)
    speed = hypot(now.velocity_x, now.velocity_y)
    if old_speed <= 0.0 or speed <= 0.0:
        return True
    direction = (last.velocity_x * now.velocity_x + last.velocity_y * now.velocity_y) / (old_speed * speed)
    return direction < TURN_BROADCAST_COS or abs(speed - old_speed) > SPEED_CHANGE_RATIO * old_speed
