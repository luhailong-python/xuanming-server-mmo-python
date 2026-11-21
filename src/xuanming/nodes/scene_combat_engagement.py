"""Combat-time AOI interest shared by scene players and NPCs."""

from __future__ import annotations

from dataclasses import dataclass, field

from .scene_aoi import FollowGrant, FollowKind, FollowSource, Priority


DISENGAGE_SECONDS = 10.0
MAX_NPC_ALLY_ROSTER = 40
_COMBAT_SOURCE = FollowSource(FollowKind.COMBAT, 0)


@dataclass(slots=True)
class _Record:
    other: object
    seconds_until_disengage: float = DISENGAGE_SECONDS
    hostile: bool = False


@dataclass(slots=True)
class _ActorState:
    actor: object
    records: dict[int, _Record] = field(default_factory=dict)


@dataclass(slots=True)
class _NpcRoster:
    npc: object
    members: list[int] = field(default_factory=list)


class SceneCombatEngagement:
    """Keep combatants visible outside spatial range until their engagement expires."""

    def __init__(self, scene) -> None:
        self.scene = scene
        self._actors: dict[int, _ActorState] = {}
        self._rosters: dict[int, _NpcRoster] = {}

    @staticmethod
    def _is_npc(actor) -> bool:
        return not hasattr(actor, "player_id")

    def _world(self, actor):
        if self._is_npc(actor):
            return getattr(actor, "world", None)
        return self.scene.locations.get(actor.player_id)

    @staticmethod
    def _present(world, actor) -> bool:
        return world is not None and world.actors.get(actor.entity) is actor

    def _can_engage(self, lhs, rhs) -> bool:
        if lhs is rhs or lhs.entity == rhs.entity:
            return False
        world = self._world(lhs)
        return world is self._world(rhs) and self._present(world, lhs) and self._present(world, rhs)

    def _follow(self, watcher, other) -> None:
        world = self._world(watcher)
        if self._present(world, watcher) and self._present(world, other):
            changes = world.aoi.follow(
                watcher.entity, other.entity,
                FollowGrant(_COMBAT_SOURCE, Priority.COMBAT, reveals_stealth=False),
            )
            self.scene._visibility(world, changes)

    def _unfollow(self, watcher, other) -> None:
        world = self._world(watcher)
        if self._present(world, watcher):
            changes = world.aoi.unfollow(watcher.entity, other.entity, _COMBAT_SOURCE)
            self.scene._visibility(world, changes)

    def _engage_one_side(self, watcher, other, hostile: bool) -> None:
        state = self._actors.get(watcher.entity)
        if state is None or state.actor is not watcher:
            state = _ActorState(watcher)
            self._actors[watcher.entity] = state
        record = state.records.get(other.entity)
        if record is None:
            state.records[other.entity] = _Record(other, hostile=hostile)
        else:
            record.other = other
            record.seconds_until_disengage = DISENGAGE_SECONDS
            record.hostile = record.hostile or hostile
        self._follow(watcher, other)

    def _engage(self, lhs, rhs, hostile: bool) -> None:
        self._engage_one_side(lhs, rhs, hostile)
        self._engage_one_side(rhs, lhs, hostile)

    def _share_npc_fight(self, npc, fighter) -> None:
        roster = self._rosters.get(npc.entity)
        if roster is None or roster.npc is not npc:
            roster = _NpcRoster(npc)
            self._rosters[npc.entity] = roster
        if fighter.entity not in roster.members:
            if len(roster.members) >= MAX_NPC_ALLY_ROSTER:
                return
            roster.members.append(fighter.entity)
        world = self._world(npc)
        for ally_id in tuple(roster.members):
            if ally_id == fighter.entity:
                continue
            ally = world.actors.get(ally_id) if world is not None else None
            if ally is not None and self._can_engage(fighter, ally):
                self._engage(fighter, ally, hostile=False)

    def record_damage(self, attacker, target) -> None:
        """Record an actual hit and share NPC aggro with the other active fighters."""
        if not self._can_engage(attacker, target):
            return
        self._engage(attacker, target, hostile=True)
        attacker_is_npc, target_is_npc = self._is_npc(attacker), self._is_npc(target)
        if target_is_npc and not attacker_is_npc:
            self._share_npc_fight(target, attacker)
        elif attacker_is_npc and not target_is_npc:
            self._share_npc_fight(attacker, target)

    def _leave_roster(self, npc, fighter) -> None:
        if not self._is_npc(npc):
            return
        roster = self._rosters.get(npc.entity)
        if roster is None or roster.npc is not npc:
            return
        roster.members[:] = [entity for entity in roster.members if entity != fighter.entity]
        if not roster.members:
            del self._rosters[npc.entity]

    def _end_one_side(self, watcher, other) -> None:
        state = self._actors.get(watcher.entity)
        if state is None or state.actor is not watcher or state.records.pop(other.entity, None) is None:
            return
        if not state.records:
            del self._actors[watcher.entity]
        self._unfollow(watcher, other)

    def _end_engagement(self, lhs, rhs) -> None:
        self._leave_roster(lhs, rhs)
        self._leave_roster(rhs, lhs)
        self._end_one_side(lhs, rhs)
        self._end_one_side(rhs, lhs)

    def on_death(self, actor) -> None:
        """End all relationships immediately when an actor dies."""
        self._clear_actor(actor)

    def on_leave(self, actor) -> None:
        """Drop relationships before scene removal; AOI leave handles the remaining links."""
        self._clear_actor(actor)

    def _clear_actor(self, actor) -> None:
        state = self._actors.get(actor.entity)
        if state is not None and state.actor is actor:
            for record in tuple(state.records.values()):
                self._end_engagement(actor, record.other)
        if self._is_npc(actor):
            roster = self._rosters.pop(actor.entity, None)
            if roster is not None:
                world = self._world(actor)
                for fighter_id in tuple(roster.members):
                    fighter = world.actors.get(fighter_id) if world is not None else None
                    if fighter is not None:
                        self._end_engagement(actor, fighter)

    def update(self, delta: float) -> None:
        if delta <= 0:
            return
        ended: dict[tuple[int, int], tuple[object, object]] = {}
        for state in tuple(self._actors.values()):
            watcher = state.actor
            world = self._world(watcher)
            for record in tuple(state.records.values()):
                other = record.other
                if (not self._present(world, watcher) or not self._present(world, other)
                        or self._world(other) is not world):
                    record.seconds_until_disengage = 0
                else:
                    record.seconds_until_disengage -= delta
                if record.seconds_until_disengage <= 0:
                    pair = min(watcher.entity, other.entity), max(watcher.entity, other.entity)
                    ended[pair] = watcher, other
        for lhs, rhs in ended.values():
            self._end_engagement(lhs, rhs)
