"""原版场景 AOI 的六边形候选、有向关注与容量规则。"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import IntEnum

Position = tuple[float, float, float]
Hex = tuple[int, int]

VIEW_RADIUS = 10.0
HEX_SIZE = 20.0
DEFAULT_CAPACITY = 100
MIN_CAPACITY = 20
MAX_CAPACITY = 200
_HEX_VERTICAL_FACTOR = math.sqrt(3) / 3
_NEIGHBORS = ((0, 0), (1, 0), (1, -1), (0, -1), (-1, 0), (-1, 1), (0, 1))


class Priority(IntEnum):
    NORMAL = 0
    TEAMMATE = 1
    ATTACKER = 2
    QUEST_NPC = 3
    BOSS = 4
    PINNED = 5
    COMBAT = 6


class Policy(IntEnum):
    OPEN_WORLD = 0
    DUNGEON = 1
    PVP_ARENA = 2


_WEIGHTS = {
    Policy.OPEN_WORLD: (0, 1, 2, 3, 4, 255, 5),
    Policy.DUNGEON: (0, 2, 3, 1, 4, 255, 5),
    Policy.PVP_ARENA: (0, 2, 4, 1, 3, 255, 5),
}


def priority_weight(priority: Priority, policy: Policy = Policy.OPEN_WORLD) -> int:
    return _WEIGHTS.get(policy, _WEIGHTS[Policy.OPEN_WORLD])[priority]


def effective_capacity(desired: int | None = None, pressure: float = 0.0) -> int:
    client = DEFAULT_CAPACITY if desired is None else min(MAX_CAPACITY, max(MIN_CAPACITY, desired))
    pressure = min(1.0, max(0.0, pressure))
    return min(client, int(MAX_CAPACITY - pressure * (MAX_CAPACITY - MIN_CAPACITY)))


def _round_away(value: float) -> int:
    fraction, whole = math.modf(value)
    return int(whole) + (1 if fraction >= .5 else -1 if fraction <= -.5 else 0)


def hex_at(position: Position) -> Hex:
    """原版 flat 布局的轴向坐标，横纵尺寸均为 20 米。"""
    x, y = position[0] / HEX_SIZE, position[1] / HEX_SIZE
    q, r = (2 / 3) * x, (-1 / 3) * x + _HEX_VERTICAL_FACTOR * y
    s = -q - r
    iq, ir, is_ = _round_away(q), _round_away(r), _round_away(s)
    dq, dr, ds = abs(iq - q), abs(ir - r), abs(is_ - s)
    if dq > dr and dq > ds:
        iq = -ir - is_
    elif dr > ds:
        ir = -iq - is_
    return iq, ir


def _neighborhood(cell: Hex | None) -> set[Hex]:
    if cell is None:
        return set()
    q, r = cell
    return {(q + dq, r + dr) for dq, dr in _NEIGHBORS}


class FollowKind(IntEnum):
    TEAM = 0
    SKILL = 1
    COMBAT = 2


@dataclass(frozen=True, slots=True)
class FollowSource:
    kind: FollowKind
    id: int


@dataclass(frozen=True, slots=True)
class FollowGrant:
    source: FollowSource
    priority: Priority = Priority.NORMAL
    reveals_stealth: bool = False


@dataclass(frozen=True, slots=True)
class VisibilityChange:
    watcher: int
    target: int
    shown: bool


def net_changes(changes: Iterable[VisibilityChange]) -> tuple[VisibilityChange, ...]:
    pending: dict[tuple[int, int], VisibilityChange] = {}
    for change in changes:
        key = change.watcher, change.target
        if key in pending:
            del pending[key]
        else:
            pending[key] = change
    return tuple(sorted(pending.values(), key=lambda change: (change.shown, change.watcher, change.target)))


@dataclass(slots=True)
class _Actor:
    position: Position
    team_id: int
    radius: float
    desired_capacity: int | None
    stealthed: bool
    cell: Hex | None = None


def _within_radius(observer: _Actor, target: _Actor) -> bool:
    ax, ay, az = observer.position
    bx, by, bz = target.position
    dx, dy, dz = ax - bx, ay - by, az - bz
    return math.sqrt(dx * dx + dy * dy + dz * dz) <= observer.radius


@dataclass(slots=True)
class _Entry:
    spatial: Priority | None = None
    follows: dict[FollowSource, FollowGrant] = field(default_factory=dict)
    visible: bool = False


@dataclass(slots=True)
class _Observer:
    entries: dict[int, _Entry] = field(default_factory=dict)
    visible_count: int = 0


class SceneAoi:
    """一个场景的兴趣表，所有 ID 都是同场景 actor 网络号。"""

    def __init__(self, policy: Policy = Policy.OPEN_WORLD, *, pressure: float = 0.0) -> None:
        self.policy, self.pressure = policy, pressure
        self._actors: dict[int, _Actor] = {}
        self._grid: dict[Hex, set[int]] = {}
        self._outgoing: dict[int, _Observer] = {}
        self._incoming: dict[int, set[int]] = {}
        self._refill_needed: set[int] = set()

    def visible_targets(self, watcher: int) -> tuple[int, ...]:
        observer = self._outgoing.get(watcher)
        if observer is None:
            return ()
        return tuple(sorted(target for target, entry in observer.entries.items() if entry.visible))

    def visible_watchers(self, target: int) -> tuple[int, ...]:
        return tuple(sorted(watcher for watcher in self._incoming.get(target, ())
                            if self._outgoing[watcher].entries[target].visible))

    def sees(self, watcher: int, target: int) -> bool:
        entry = self._entry(watcher, target)
        return entry is not None and entry.visible

    def ready(self, entity: int) -> bool:
        actor = self._actors.get(entity)
        return actor is not None and actor.cell is not None

    def nearby(self, entity: int) -> set[int]:
        """读当前格及邻格的实时几何距离，不用兴趣关系和隐身筛选。"""
        actor = self._actors.get(entity)
        if actor is None or actor.cell is None:
            return set()
        return {
            candidate for cell in _neighborhood(actor.cell) for candidate in self._grid.get(cell, ())
            if candidate != entity and _within_radius(actor, self._actors[candidate])
        }

    def _capacity(self, watcher: int) -> int:
        return effective_capacity(self._actors[watcher].desired_capacity, self.pressure)

    def _eligible(self, target: int, entry: _Entry | None) -> bool:
        return not self._actors[target].stealthed or bool(
            entry and any(grant.reveals_stealth for grant in entry.follows.values())
        )

    def _weight(self, entry: _Entry) -> int:
        weight = 0 if entry.spatial is None else priority_weight(entry.spatial, self.policy)
        if not entry.follows:
            return weight
        return max(weight, max((priority_weight(grant.priority, self.policy) for grant in entry.follows.values()),
                               default=0))

    def _entry(self, watcher: int, target: int) -> _Entry | None:
        observer = self._outgoing.get(watcher)
        return observer.entries.get(target) if observer is not None else None

    def _insert(self, watcher: int, target: int, observer: _Observer, entry: _Entry) -> None:
        observer.entries[target] = entry
        self._incoming.setdefault(target, set()).add(watcher)

    def _erase_reverse(self, watcher: int, target: int) -> None:
        watchers = self._incoming[target]
        watchers.remove(watcher)
        if not watchers:
            del self._incoming[target]

    @staticmethod
    def _show(watcher: int, observer: _Observer, target: int, entry: _Entry, shown: bool,
              changes: list[VisibilityChange]) -> None:
        entry.visible = shown
        observer.visible_count += 1 if shown else -1
        changes.append(VisibilityChange(watcher, target, shown))

    def _erase(self, watcher: int, observer: _Observer, target: int, changes: list[VisibilityChange]) -> None:
        entry = observer.entries.pop(target)
        if entry.visible:
            self._show(watcher, observer, target, entry, False, changes)
        self._erase_reverse(watcher, target)

    def _make_room(self, watcher: int, observer: _Observer, weight: int, changes: list[VisibilityChange]) -> bool:
        if observer.visible_count < self._capacity(watcher):
            return True
        if not weight:
            self._refill_needed.add(watcher)
            return False
        # 同权重保留原已显示条目。淘汰候选同分时，用网络号确定先后。
        victim = min(((self._weight(entry), -target) for target, entry in observer.entries.items() if entry.visible),
                     default=None)
        if victim is None or victim[0] >= weight:
            self._refill_needed.add(watcher)
            return False
        target = -victim[1]
        entry = observer.entries[target]
        entry.spatial = None
        if entry.follows:
            self._show(watcher, observer, target, entry, False, changes)
        else:
            self._erase(watcher, observer, target, changes)
            self._refill_needed.add(watcher)
        return True

    def _fill_slots(self, watcher: int, observer: _Observer, changes: list[VisibilityChange]) -> None:
        capacity = self._capacity(watcher)
        if observer.visible_count < capacity and len(observer.entries) != observer.visible_count:
            hidden = sorted(
                (-self._weight(entry), target) for target, entry in observer.entries.items()
                if not entry.visible and self._eligible(target, entry)
            )
            for _, target in hidden[:capacity - observer.visible_count]:
                self._show(watcher, observer, target, observer.entries[target], True, changes)

    def _refresh_nearby(self, entity: int, changes: list[VisibilityChange]) -> None:
        actor = self._actors.get(entity)
        if actor is None or actor.cell is None:
            return
        for other in self._actors_in(_neighborhood(actor.cell), entity):
            self._add_spatial(entity, other, changes)
            self._add_spatial(other, entity, changes)

    def apply_pressure(self, pressure: float) -> tuple[tuple[VisibilityChange, ...], int]:
        """Apply a node-wide pressure factor, trimming by priority then distance."""
        pressure = min(1.0, max(0.0, pressure))
        if pressure == self.pressure:
            return (), 0
        expanding = pressure < self.pressure
        self.pressure = pressure
        changes: list[VisibilityChange] = []
        trimmed = 0
        for watcher, observer in tuple(self._outgoing.items()):
            capacity = self._capacity(watcher)
            if observer.visible_count > capacity:
                ranked = sorted(
                    ((-self._weight(entry), math.dist(self._actors[watcher].position,
                                                      self._actors[target].position), target)
                     for target, entry in observer.entries.items() if entry.visible),
                )
                for _, _, target in ranked[capacity:]:
                    entry = observer.entries.get(target)
                    if entry is None or not entry.visible:
                        continue
                    entry.spatial = None
                    trimmed += 1
                    if entry.follows:
                        self._show(watcher, observer, target, entry, False, changes)
                    else:
                        self._erase(watcher, observer, target, changes)
                        self._refill_needed.add(watcher)
            self._fill_slots(watcher, observer, changes)
        if expanding:
            for watcher in sorted(tuple(self._refill_needed)):
                observer = self._outgoing.get(watcher)
                if observer is None or observer.visible_count >= self._capacity(watcher):
                    continue
                self._refill_needed.discard(watcher)
                self._refresh_nearby(watcher, changes)
        return net_changes(changes), trimmed

    def _reconcile(self, watcher: int, target: int, entry: _Entry, changes: list[VisibilityChange]) -> None:
        observer = self._outgoing[watcher]
        eligible = self._eligible(target, entry)
        if entry.visible and not eligible:
            entry.spatial = None
            if entry.follows:
                self._show(watcher, observer, target, entry, False, changes)
            else:
                self._erase(watcher, observer, target, changes)
            self._fill_slots(watcher, observer, changes)
        elif not entry.visible and eligible and self._make_room(watcher, observer, self._weight(entry), changes):
            self._show(watcher, observer, target, entry, True, changes)

    def _team_priority(self, watcher: int, target: int) -> Priority:
        team = self._actors[watcher].team_id
        return Priority.TEAMMATE if team and team == self._actors[target].team_id else Priority.NORMAL

    def _add_spatial(self, watcher: int, target: int, changes: list[VisibilityChange]) -> None:
        entry = self._entry(watcher, target)
        if not _within_radius(self._actors[watcher], self._actors[target]) or not self._eligible(target, entry):
            return
        priority = self._team_priority(watcher, target)
        observer = self._outgoing.setdefault(watcher, _Observer())
        if entry is not None and entry.visible:
            if entry.spatial is None or priority_weight(priority, self.policy) > priority_weight(entry.spatial, self.policy):
                entry.spatial = priority
            return
        is_new = entry is None
        if is_new:
            entry = _Entry()
        entry.spatial = priority
        if self._make_room(watcher, observer, self._weight(entry), changes):
            if is_new:
                self._insert(watcher, target, observer, entry)
            self._show(watcher, observer, target, entry, True, changes)
        else:
            entry.spatial = None

    def _remove_spatial(self, watcher: int, target: int, changes: list[VisibilityChange]) -> None:
        entry = self._entry(watcher, target)
        if entry is None or entry.spatial is None:
            return
        entry.spatial = None
        if not entry.follows:
            observer = self._outgoing[watcher]
            self._erase(watcher, observer, target, changes)
            self._fill_slots(watcher, observer, changes)

    def _actors_in(self, cells: Iterable[Hex], excluding: int) -> list[int]:
        return sorted(actor for cell in cells for actor in self._grid.get(cell, ()) if actor != excluding)

    def _remove_cell(self, entity: int, actor: _Actor) -> None:
        if actor.cell is not None:
            occupants = self._grid[actor.cell]
            occupants.remove(entity)
            if not occupants:
                del self._grid[actor.cell]
            actor.cell = None

    def enter(
        self, entity: int, position: Position, *, team_id: int = 0, radius: float = VIEW_RADIUS,
        desired_capacity: int | None = None, stealthed: bool = False,
    ) -> tuple[VisibilityChange, ...]:
        if not entity or entity in self._actors or not all(map(math.isfinite, position)):
            raise ValueError("AOI actor must have a new, nonzero network ID")
        self._actors[entity] = _Actor(position, team_id, radius, desired_capacity, stealthed)
        return self.move(entity, position)

    def move(self, entity: int, position: Position) -> tuple[VisibilityChange, ...]:
        actor = self._actors[entity]
        cell = hex_at(position)
        actor.position = position
        if actor.cell == cell:
            return ()
        prior, current = _neighborhood(actor.cell), _neighborhood(cell)
        self._remove_cell(entity, actor)
        self._grid.setdefault(cell, set()).add(entity)
        actor.cell = cell
        changes: list[VisibilityChange] = []
        for other in self._actors_in(current - prior, entity):
            self._add_spatial(entity, other, changes)
            self._add_spatial(other, entity, changes)
        for other in self._actors_in(prior - current, entity):
            self._remove_spatial(entity, other, changes)
            self._remove_spatial(other, entity, changes)
        return net_changes(changes)

    def reset(self, entity: int, position: Position) -> tuple[VisibilityChange, ...]:
        """瞬移后撤空间来源。下一次 move 才在新位置发现邻居。"""
        actor = self._actors[entity]
        actor.position = position
        changes: list[VisibilityChange] = []
        observer = self._outgoing.get(entity)
        if observer is not None:
            for target in sorted(observer.entries):
                self._remove_spatial(entity, target, changes)
        for watcher in sorted(self._incoming.get(entity, ())):
            self._remove_spatial(watcher, entity, changes)
        self._remove_cell(entity, actor)
        return net_changes(changes)

    def leave(self, entity: int) -> tuple[VisibilityChange, ...]:
        actor = self._actors.get(entity)
        if actor is None:
            return ()
        self._refill_needed.discard(entity)
        observer = self._outgoing.pop(entity, None)
        if observer is not None:
            for target in observer.entries:
                self._erase_reverse(entity, target)
        changes: list[VisibilityChange] = []
        for watcher in sorted(self._incoming.get(entity, ())):
            other = self._outgoing[watcher]
            visible = other.entries[entity].visible
            self._erase(watcher, other, entity, changes)
            if visible:
                self._fill_slots(watcher, other, changes)
        self._remove_cell(entity, actor)
        del self._actors[entity]
        return net_changes(changes)

    def set_stealth(self, entity: int, stealthed: bool) -> tuple[VisibilityChange, ...]:
        actor = self._actors[entity]
        if actor.stealthed == stealthed:
            return ()
        actor.stealthed = stealthed
        changes: list[VisibilityChange] = []
        for watcher in sorted(self._incoming.get(entity, ())):
            self._reconcile(watcher, entity, self._outgoing[watcher].entries[entity], changes)
        if not stealthed:
            for watcher in self._actors_in(_neighborhood(actor.cell), entity):
                self._add_spatial(watcher, entity, changes)
        return net_changes(changes)

    def set_team(self, entity: int, team_id: int) -> None:
        actor = self._actors[entity]
        if actor.team_id == team_id:
            return
        actor.team_id = team_id
        for other in self._actors_in(_neighborhood(actor.cell), entity):
            priority = self._team_priority(entity, other)
            for watcher, target in ((entity, other), (other, entity)):
                entry = self._entry(watcher, target)
                if entry is None or entry.spatial is None:
                    continue
                if priority is Priority.TEAMMATE:
                    if priority_weight(priority, self.policy) > priority_weight(entry.spatial, self.policy):
                        entry.spatial = priority
                elif entry.spatial is Priority.TEAMMATE:
                    entry.spatial = Priority.NORMAL

    def follow(self, watcher: int, target: int, grant: FollowGrant) -> tuple[VisibilityChange, ...]:
        if watcher == target or watcher not in self._actors or target not in self._actors:
            return ()
        observer = self._outgoing.setdefault(watcher, _Observer())
        entry = observer.entries.get(target)
        if entry is None:
            entry = _Entry()
            self._insert(watcher, target, observer, entry)
        entry.follows[grant.source] = grant
        changes: list[VisibilityChange] = []
        self._reconcile(watcher, target, entry, changes)
        return net_changes(changes)

    def unfollow(self, watcher: int, target: int, source: FollowSource) -> tuple[VisibilityChange, ...]:
        entry = self._entry(watcher, target)
        if entry is None or source not in entry.follows:
            return ()
        del entry.follows[source]
        changes: list[VisibilityChange] = []
        if entry.follows or entry.spatial is not None:
            self._reconcile(watcher, target, entry, changes)
        else:
            observer = self._outgoing[watcher]
            visible = entry.visible
            self._erase(watcher, observer, target, changes)
            if visible:
                self._fill_slots(watcher, observer, changes)
        return net_changes(changes)
