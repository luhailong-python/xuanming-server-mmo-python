"""Coalesce dirty actor attributes for visible Scene observers."""

from dataclasses import dataclass
from typing import Literal

_Field = Literal["transform", "velocity", "combat_state_flags"]
_MESSAGE = "ScenePlayerSyncSyncBaseAttribute"


@dataclass(slots=True)
class _DirtyActor:
    actor: object
    fields: set[_Field]


class SceneActorSync:
    def __init__(self, scene, emit_many=None) -> None:
        self.scene = scene
        self.emit_many = emit_many
        self._dirty: dict[int, _DirtyActor] = {}
        self._frame = 0

    def mark(self, actor, *fields: _Field) -> None:
        pending = self._dirty.get(actor.entity)
        if pending is not None and pending.actor is actor:
            pending.fields.update(fields)
        elif fields:
            self._dirty[actor.entity] = _DirtyActor(actor, set(fields))

    def discard(self, actor) -> None:
        pending = self._dirty.get(actor.entity)
        if pending is not None and pending.actor is actor:
            del self._dirty[actor.entity]

    def tick(self) -> None:
        self._frame += 1
        if self._frame % 2 == 0:
            return
        for entity, pending in tuple(self._dirty.items()):
            actor = pending.actor
            player_id = getattr(actor, "player_id", 0)
            world = self.scene.locations.get(player_id) if player_id else actor.world
            if world is None or world.actors.get(entity) is not actor:
                self.discard(actor)
                continue
            if player_id and self.scene.travel and self.scene.travel.frozen(player_id):
                continue
            watchers = tuple(self.scene.watchers(world, actor))
            if not watchers:
                continue
            self.discard(actor)
            self._broadcast(actor, player_id, pending.fields, watchers)

    def _broadcast(self, actor, player_id: int, fields: set[_Field], watchers) -> None:
        values = {}
        if "transform" in fields:
            values["transform"] = actor.transform.wire()
        if "velocity" in fields and getattr(actor, "motion", None) is not None:
            values["velocity"] = dict(zip("xyz", actor.motion.velocity))
        if "combat_state_flags" in fields and self.scene.buffs is not None:
            values["combat_state_flags"] = {"state_flags": self.scene.buffs.state_flags(actor)}
        if not values:
            return
        protocol = self.scene.protocol
        message = protocol.message("ActorBaseAttributesS2C", entity_id=player_id, **values)
        content = protocol.content(protocol.message_ids[_MESSAGE], body=message.SerializeToString())
        if self.emit_many is not None:
            self.emit_many(watchers, content)
        else:
            for watcher in watchers:
                self.scene.emit(watcher, content)
