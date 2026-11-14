"""Scene 侧的权威队伍刷新、归队和服务端跟随。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import math

from .scene import ZERO
from .scene_facing import rotation_toward
from .team_follow_formation import (
    MAX_LEADER_JUMP, MAX_MEMBERS, TeamFollowBroadcastState, TeamFollowFormation,
    TeamFollowMember, horizontal_distance, should_broadcast,
)

_LOG = logging.getLogger(__name__)
_LEFT, _TEAM_CHANGED, _LEADER_CHANGED, _BECAME_LEADER, _SCENE_CHANGED, _LEADER_GONE, _LEAVING = range(1, 8)
_REGROUP_DISTANCE = 8.0
_REGROUP_TELEPORT = 5


@dataclass(frozen=True, slots=True)
class _Membership:
    team_id: int
    stamp: tuple[int, int]
    leader: int
    join_seq: int


@dataclass(slots=True)
class _Follower:
    leader: int
    last: TeamFollowBroadcastState = field(default_factory=TeamFollowBroadcastState)
    frames: int = 0


@dataclass(slots=True)
class _Formation:
    world: object
    team_id: int
    trail: TeamFollowFormation


class SceneTeam:
    def __init__(self, scene, travel) -> None:
        self.scene = scene
        self.travel = travel
        self.membership: dict[int, _Membership] = {}
        self.intents: set[int] = set()
        self._followers: dict[int, _Follower] = {}
        self._formations: dict[int, _Formation] = {}
        self._pending: dict[int, bool] = {}
        self._tasks: dict[int, asyncio.Task] = {}
        self._closing = False

    def register(self) -> None:
        self.scene.register("SceneMovementClientPlayer", "TeamFollow", self.follow)

    def follow(self, world, player, route, request) -> None:
        if request.follow:
            self.intents.add(player.player_id)
            self.refresh(player)
        else:
            self.intents.discard(player.player_id)
            self._detach(player.player_id, _LEFT)

    def _player(self, player_id):
        world = self.scene.locations.get(player_id)
        return world.players.get(player_id) if world is not None else None

    def hint(self, player_id: int) -> None:
        if (player := self._player(player_id)) is not None:
            self.refresh(player)

    def entered(self, player) -> None:
        if (follower := self._followers.get(player.player_id)) is not None:
            self._send_slot(player.player_id, follower.leader)
        self.refresh(player, fanout=True)

    def refresh(self, player, *, fanout: bool = False) -> None:
        if self._closing or self.travel is None or player.role_identity is None:
            return
        player_id = player.player_id
        self._pending[player_id] = fanout or self._pending.get(player_id, False)
        if player_id not in self._tasks:
            self._tasks[player_id] = asyncio.create_task(self._refresh(player_id), name=f"team-refresh-{player_id}")

    @staticmethod
    def _identity(player):
        identity = player.role_identity
        return identity.SerializeToString(deterministic=True) if identity is not None else None

    async def _refresh(self, player_id: int) -> None:
        try:
            while player_id in self._pending:
                fanout = self._pending.pop(player_id)
                player = self._player(player_id)
                if player is None or (identity := self._identity(player)) is None:
                    continue
                try:
                    reply = await self.travel.read_team_membership(player)
                    if self._player(player_id) is player and self._identity(player) == identity:
                        await self._apply(player, identity, reply, fanout)
                except Exception:
                    _LOG.debug("组队刷新未完成 player=%d", player_id, exc_info=True)
        finally:
            self._tasks.pop(player_id, None)

    async def _apply(self, player, identity, reply, fanout: bool) -> None:
        player_id = player.player_id
        record = reply.record
        member = next((entry for entry in record.members if entry.player_id == player_id), None)
        view = _Membership(reply.team_id, (reply.membership_epoch, reply.version),
                           record.leader_id if reply.team_id else 0, member.join_seq if member else 0)
        previous = self.membership.get(player_id)
        if previous and (previous.stamp > view.stamp or
                         (previous.stamp[0] == view.stamp[0] and previous.team_id != view.team_id)):
            return
        self.membership[player_id] = view
        self.scene.locations[player_id].aoi.set_team(player.entity, view.team_id)
        if previous and previous.team_id != view.team_id:
            self._detach(player_id, _TEAM_CHANGED)
            self._dissolve(player_id, _LEADER_CHANGED)
        if not view.team_id:
            return
        if view.leader == player_id:
            self._detach(player_id, _BECAME_LEADER)
            if fanout or previous is None or previous.leader != player_id or previous.team_id != view.team_id:
                for entry in record.members:
                    if entry.player_id != player_id:
                        self.hint(entry.player_id)
            return
        self._dissolve(player_id, _LEADER_CHANGED)
        if (follower := self._followers.get(player_id)) is not None and follower.leader != view.leader:
            self._detach(player_id, _LEADER_CHANGED)
        if self._leaving(player) or self._held(player):
            return
        location = await self.travel.team_leader_location(player_id, view.leader)
        if (self._player(player_id) is not player or self._identity(player) != identity
                or self._leaving(player) or self._held(player) or location is None
                or not location.scene_id or location.zone_id != self.scene.info.zone_id
                or location.node_id != str(self.scene.info.node_id)):
            return
        world = self.scene.locations[player_id]
        if location.scene_id != world.info.scene_id:
            self.travel.follow_scene(player, location.scene_id)
            return
        leader = world.players.get(view.leader)
        if leader is None or self._leaving(leader):
            return
        near = self._near(world, player, leader)
        leader_view = self.membership.get(view.leader)
        if player_id in self.intents and leader_view and leader_view.team_id == view.team_id:
            self._attach(world, player, leader, view, near)
        elif not near and player.disconnect_at is None:
            self.scene.teleport(world, player, leader.transform.position, leader.transform.rotation,
                                reason=_REGROUP_TELEPORT)

    def _leaving(self, player) -> bool:
        return self.scene._closing or self.scene._releasing(player) or (
            self.scene.travel is not None and self.scene.travel.frozen(player.player_id)
        )

    def _held(self, player) -> bool:
        return bool(self.scene.battle and self.scene.battle.frozen(player.player_id))

    def _near(self, world, player, leader) -> bool:
        if horizontal_distance(player.transform.position, leader.transform.position) > _REGROUP_DISTANCE:
            return False
        aoi = world.aoi
        if not (aoi.ready(player.entity) and aoi.ready(leader.entity)):
            return True
        return aoi.sees(player.entity, leader.entity) and aoi.sees(leader.entity, player.entity)

    def _attach(self, world, player, leader, view: _Membership, walk: bool) -> None:
        player_id, leader_id = player.player_id, leader.player_id
        if (follower := self._followers.get(player_id)) is not None and follower.leader == leader_id:
            self._send_slot(player_id, leader_id)
            return
        self._detach(player_id, _LEADER_CHANGED)
        self._dissolve(player_id, _LEADER_CHANGED)
        self._detach(leader_id, _BECAME_LEADER)
        group = self._formations.get(leader_id)
        if group and (group.world is not world or group.team_id != view.team_id):
            self._dissolve(leader_id, _LEADER_CHANGED)
            group = None
        if group is None:
            group = self._formations[leader_id] = _Formation(
                world, view.team_id, TeamFollowFormation(leader.transform.position),
            )
        if len(group.trail.members) < MAX_MEMBERS:
            self._place(group, player, leader, view.join_seq, walk)

    def _approach(self, group, rank, position) -> bool:
        formation = group.trail
        formation.trim_tail_for_newcomer(rank)
        tail = formation.trail.tail
        if self.scene._missing_required_navigation(group.world):
            return False
        path = group.world.nav.find_path(position, tail) if group.world.nav else (position, tail)
        if not path or horizontal_distance(path[-1], tail) > 0.5:
            return False
        points = (position, *path, tail)
        if sum(horizontal_distance(a, b) for a, b in zip(points, points[1:])) > 16.0:
            return False
        formation.trail.prepend_path(points)
        return True

    def _place(self, group, player, leader, join_seq: int, walk: bool) -> None:
        formation = group.trail
        player_id, leader_id = player.player_id, leader.player_id
        rank = formation.rank_for(join_seq, player_id)
        cursor = formation.trail.nearest_odo(player.transform.position, 0.75)
        place = needs_placement = False
        if cursor is None:
            if walk and self._approach(group, rank, player.transform.position):
                cursor = formation.trail.tail_odo
            else:
                cursor = formation.slot_odo(rank)
                needs_placement = self._held(player)
                place = not needs_placement
        formation.add(TeamFollowMember(player_id, join_seq, cursor, needs_placement=needs_placement))
        self._followers[player_id] = _Follower(leader_id)
        self.scene.set_velocity(player, ZERO)
        player.motion.teleport_until = 0.0
        if place:
            if not self.scene.teleport(group.world, player, formation.trail.point_at(cursor), leader.transform.rotation,
                                       reason=_REGROUP_TELEPORT):
                self._detach(player_id, _LEFT)
                return
        self._send_slot(player_id, leader_id)

    def _send_slot(self, player_id, leader_id) -> None:
        leader = self._player(leader_id)
        group = self._formations.get(leader_id)
        if leader is not None and group is not None and (rank := group.trail.rank_of(player_id)) is not None:
            self._state(player_id, driven=True, leader_entity=leader.entity, slot=rank + 1)

    def _state(self, player_id, **fields) -> None:
        player = self._player(player_id)
        if player is not None and player.disconnect_at is None:
            self.scene._push(player, "SceneMovementClientPlayerNotifyTeamFollowState",
                             self.scene.protocol.message("TeamFollowStateS2C", **fields))

    def _detach(self, player_id: int, reason: int) -> None:
        follower = self._followers.pop(player_id, None)
        if follower is None:
            return
        if (group := self._formations.get(follower.leader)) is not None:
            group.trail.remove(player_id)
            if not group.trail.members:
                self._formations.pop(follower.leader, None)
        player = self._player(player_id)
        if player is not None:
            player.motion.teleport_target = None
            self._motion(self.scene.locations[player_id], player, follower, ZERO)
            self._state(player_id, reason=reason)

    def _dissolve(self, leader_id: int, reason: int) -> None:
        if (group := self._formations.pop(leader_id, None)) is not None:
            for member in group.trail.members:
                self._detach(member.player_id, reason)

    def moved_scene(self, player) -> None:
        self._detach(player.player_id, _SCENE_CHANGED)
        self._dissolve(player.player_id, _SCENE_CHANGED)

    def departed(self, player) -> None:
        player_id = player.player_id
        self._detach(player_id, _LEAVING)
        self._dissolve(player_id, _LEADER_GONE)
        self.intents.discard(player_id)
        self.membership.pop(player_id, None)
        self._pending.pop(player_id, None)

    def driven(self, player_id: int) -> bool:
        return player_id in self._followers

    def team_id(self, player_id: int) -> int:
        view = self.membership.get(player_id)
        return view.team_id if view else 0

    def hosting(self, player) -> None:
        if (view := self.membership.get(player.player_id)) and view.team_id:
            self.intents.add(player.player_id)
            if not self.driven(player.player_id):
                self.refresh(player)

    def _motion(self, world, player, follower, velocity) -> None:
        self.scene.set_velocity(player, velocity)
        moving = velocity != ZERO
        if (rotation := rotation_toward(velocity[0], velocity[1])) is not None:
            player.transform.rotation = rotation
        now = TeamFollowBroadcastState(moving, velocity[0], velocity[1])
        follower.frames += 1
        if should_broadcast(follower.last, now, follower.frames):
            self.scene._broadcast_move(world, player, include_self=True)
            follower.last, follower.frames = now, 0

    def tick(self, delta: float) -> None:
        if delta <= 0:
            return
        for leader_id, group in tuple(self._formations.items()):
            world = group.world
            leader = world.players.get(leader_id)
            if leader is None or self._leaving(leader):
                self._dissolve(leader_id, _LEADER_GONE)
                continue
            for member in group.trail.members:
                player = world.players.get(member.player_id)
                if player is None or self._leaving(player):
                    self._detach(member.player_id, _LEAVING)
            if not group.trail.members:
                continue
            if self._held(leader):
                for member in group.trail.members:
                    self._motion(world, world.players[member.player_id], self._followers[member.player_id], ZERO)
                continue
            position = leader.transform.position
            if horizontal_distance(group.trail.trail.head, position) > MAX_LEADER_JUMP:
                members = group.trail.members
                group.trail = TeamFollowFormation(position)
                for member in members:
                    player = world.players[member.player_id]
                    self._place(group, player, leader, member.join_seq,
                                horizontal_distance(position, player.transform.position) <= _REGROUP_DISTANCE)
                continue
            for member in group.trail.members:
                group.trail.set_suspended(member.player_id, self._held(world.players[member.player_id]))

            def walkable(a, b):
                return world.nav.validate_move(a, b)[0] if world.nav else False

            missing_navigation = self.scene._missing_required_navigation(world)
            for step in group.trail.tick(position, delta, walkable if world.nav or missing_navigation else None):
                player, follower = world.players[step.player_id], self._followers[step.player_id]
                if step.suspended:
                    self._motion(world, player, follower, ZERO)
                elif step.placed:
                    if not self.scene.teleport(world, player, step.position, leader.transform.rotation,
                                               reason=_REGROUP_TELEPORT):
                        self._detach(step.player_id, _LEFT)
                        continue
                    follower.last, follower.frames = TeamFollowBroadcastState(), 0
                elif step.moved:
                    if missing_navigation:
                        self._detach(step.player_id, _LEFT)
                        continue
                    previous = player.transform.position
                    self.scene._move_position(world, player, step.position)
                    self._motion(world, player, follower, tuple((b - a) / delta for a, b in zip(previous, step.position)))
                else:
                    self._motion(world, player, follower, ZERO)

    async def close(self) -> None:
        self._closing = True
        tasks = tuple(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._pending.clear()
