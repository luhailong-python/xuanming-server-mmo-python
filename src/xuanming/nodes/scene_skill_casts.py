"""Scene realtime spell phases and authoritative impacts."""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from contextlib import nullcontext
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable
from .battle_tables import BattleTables
from .scene_buffs import (
    ACTOR_COMBAT,
    EXIT_INTERRUPTED,
    USE_SKILL,
    SceneBuffs,
)
from .scene_facing import rotation_toward
from .scene_stats import effective_level, player_stats


_LOG = logging.getLogger(__name__)
_SKILL_USED = "SceneSkillClientPlayerNotifySkillUsed"
_SKILL_INTERRUPTED = "SceneSkillClientPlayerNotifySkillInterrupted"


@dataclass(slots=True)
class _Cast:
    task: asyncio.Task | None = None
    committing: bool = False


class SceneSkillCasts:
    def __init__(self, owner):
        self.owner = owner
        self.scene = owner.scene
        self.protocol = owner.protocol
        self.tables = owner.tables
        self.battle_tables = BattleTables(self.protocol, self.tables)
        self.casts: dict[int, _Cast] = {}
        self.cooldowns: dict[int, dict[int, float]] = {}
        self.buffs = SceneBuffs(self.protocol, self.tables, valid=self._live,
                                apply_allowed=lambda actor: not self._frozen(actor),
                                periodic_allowed=self._periodic_allowed)
        self.buffs.state_changed = self._publish_state
        self.buffs.self_state_changed = self._publish_self_state_flags
        self.buffs.register_exit_hook(ACTOR_COMBAT, self._on_combat_state_exited)
        self._healing: set[int] = set()
        self._tasks: set[asyncio.Task] = set()
        self._closed = False
        self._prune_at = 0.0

    @staticmethod
    def _player(actor) -> bool:
        return bool(getattr(actor, "player_id", 0))

    def _world(self, actor):
        if self._player(actor):
            return self.scene.locations.get(actor.player_id)
        world = getattr(actor, "world", None)
        if world is not None:
            return world
        return next((world for world in self.scene.worlds.values()
                     if getattr(world, "npcs", {}).get(actor.entity) is actor), None)

    def _in_world(self, world, actor) -> bool:
        if self._player(actor):
            return self.scene.locations.get(actor.player_id) is world and world.players.get(actor.player_id) is actor
        return getattr(world, "npcs", {}).get(actor.entity) is actor

    def _live(self, actor) -> bool:
        world = self._world(actor)
        return world is not None and self._in_world(world, actor)

    def _periodic_allowed(self, actor) -> bool:
        return not self._player(actor) or self.scene.mutable(actor)

    def _frozen(self, actor) -> bool:
        return not self._live(actor) or (self._player(actor) and (
            not self.scene.mutable(actor) or bool(self.scene.battle and self.scene.battle.frozen(actor.player_id))
        ))

    def _target(self, world, target_id):
        actors = getattr(world, "actors", None)
        if actors is not None:
            target = actors.get(target_id)
        else:
            target = getattr(world, "npcs", {}).get(target_id)
            if target is None:
                target = next((actor for actor in world.players.values() if actor.entity == target_id), None)
        if target is not None and self._player(target) and not self.scene.mutable(target):
            return None
        return target

    def release(self, world, player, route, request):
        response = self.protocol.message("ReleaseSkillResponse")
        error = self._start(world, player, request)
        if error:
            response.error_message.id = error
        return response

    def _start(self, world, player, request) -> int:
        row = self.owner.skills.get(request.skill_table_id)
        if row is None or not request.skill_table_id:
            return 1001
        data = self.scene.rows.assemble(player.snapshot)
        if (not self.owner._present(data) or not data.HasField("derived_attributes_component")
                or not data.HasField("level_component") or not player.entity):
            return 1003
        if not self.owner._usable(data, row):
            return 1001
        if row.passive_stat or self._closed or self._frozen(player) or not data.derived_attributes_component.health:
            return 7004
        if self.scene.locations.get(player.player_id) is not world or world.players.get(player.player_id) is not player:
            return 7004
        target = self._target(world, request.target_id) if request.target_id else None
        if row.targeting_mode and not request.target_id:
            return 7001
        for mode in row.targeting_mode:
            if mode in (0, 2):
                break
            if mode == 1:
                if target is None:
                    return 7001
                if self._frozen(target):
                    return 7002
                break
        if request.HasField("position") and not all(math.isfinite(n) for n in self._xyz(request.position)):
            return 1005
        now = time.monotonic()
        active_cooldowns = self.cooldowns.get(player.entity, {})
        if active_cooldowns.get(row.cooldown_id, 0) >= now:
            return 7003
        state_error = self.buffs.validate_skill(player, row.skill_type)
        if state_error:
            return state_error
        has_general = 1 in row.skill_type
        has_channel = 2 in row.skill_type and not has_general
        previous = self.casts.get(player.entity)
        if previous is not None:
            if not row.immediate:
                return 7000
            self._broadcast(world, player, _SKILL_INTERRUPTED, self.protocol.message(
                "SkillInterruptedS2C", entity=player.entity, skill_table_id=row.id,
            ))
            if not previous.committing:
                previous.task.cancel()
            del self.casts[player.entity]
        error = self.buffs.perform_action(player, USE_SKILL, ACTOR_COMBAT)
        if error:
            return error
        look = self._xyz(request.position) if request.HasField("position") else target.transform.position if target else None
        if look is not None:
            self._face(player, look)
        notification = self.protocol.message("SkillUsedS2C", entity=player.entity, skill_table_id=row.id,
                                             position=request.position)
        if target is not None:
            notification.target_entity.append(target.entity)
        self._broadcast(world, player, _SKILL_USED, notification)
        cooldown = self.tables["Cooldown"].get(row.cooldown_id) if row.cooldown_id else None
        if cooldown is not None and cooldown.duration:
            self.cooldowns.setdefault(player.entity, {})[row.cooldown_id] = now + cooldown.duration / 1000
        if has_general or has_channel:
            cast = _Cast()
            self.casts[player.entity] = cast
            cast.task = asyncio.create_task(self._resolve(cast, world, player, target, row, has_channel))
            self._tasks.add(cast.task)
            cast.task.add_done_callback(self._tasks.discard)
        if target is not None and not self._frozen(target):
            self.buffs.reset_combat_idle(target)
        if now >= self._prune_at:
            self._prune(now)
            self._prune_at = now + 30.0
        return 0

    @staticmethod
    def _xyz(vector) -> tuple[float, float, float]:
        return vector.x, vector.y, vector.z

    def _face(self, player, look) -> None:
        dx, dy = look[0] - player.transform.position[0], look[1] - player.transform.position[1]
        rotation = rotation_toward(dx, dy)
        if rotation is not None:
            player.transform.rotation = rotation
            actor_sync = getattr(self.scene, "actor_sync", None)
            if actor_sync is not None:
                actor_sync.mark(player, "transform")

    def _broadcast(self, world, player, message_name, message) -> None:
        watchers = getattr(self.scene, "watchers", None)
        if watchers is not None:
            receivers = watchers(world, player)
        elif self.buffs.stealthed(player):
            return
        else:
            receivers = (world.players.get(watcher) for watcher in world.nearby(player))
        for receiver in receivers:
            if receiver is not None and receiver.disconnect_at is None:
                self.scene._push(receiver, message_name, message)

    def _publish_state(self, actor) -> None:
        actor_sync = getattr(self.scene, "actor_sync", None)
        if actor_sync is not None:
            actor_sync.mark(actor, "combat_state_flags")
            return
        world = self._world(actor)
        if world is not None:
            state = self.protocol.message("ActorBaseAttributesS2C", entity_id=getattr(actor, "player_id", 0))
            state.combat_state_flags.SetInParent()
            state.combat_state_flags.state_flags.update(self.buffs.state_flags(actor))
            self._broadcast(world, actor, "ScenePlayerSyncSyncBaseAttribute", state)

    def _publish_self_state_flags(self, actor, action_mask: int, combat_mask: int, state_seq: int) -> None:
        if not self._player(actor) or self._frozen(actor) or actor.disconnect_at is not None:
            return
        msg = self.protocol.message(
            "SelfActorStateFlagsS2C",
            action_state_mask=action_mask,
            combat_state_mask=combat_mask,
            state_seq=state_seq,
        )
        self.scene._push(actor, "SceneClientPlayerCommonNotifySelfActorStateFlags", msg)

    def sync_self_state_after_entry(self, actor) -> None:
        self.buffs.sync_self_state_after_entry(actor)

    def _on_combat_state_exited(self, actor, _state: int, reason: int) -> None:
        if reason != EXIT_INTERRUPTED:
            return
        active = self.casts.get(actor.entity)
        if active is not None and not active.committing:
            active.task.cancel()
            self.casts.pop(actor.entity, None)
            if (world := self._world(actor)) is not None and self._in_world(world, actor):
                self._broadcast(world, actor, _SKILL_INTERRUPTED, self.protocol.message(
                    "SkillInterruptedS2C", entity=actor.entity,
                ))

    def _prune(self, now: float) -> None:
        present = {player.entity for world in self.scene.worlds.values() for player in world.players.values()}
        for entity, entries in tuple(self.cooldowns.items()):
            if entity not in present:
                del self.cooldowns[entity]
                continue
            for key, expires in tuple(entries.items()):
                if expires <= now:
                    del entries[key]
            if not entries:
                del self.cooldowns[entity]

    async def _resolve(self, cast, world, caster, target, row, channel: bool) -> None:
        try:
            await asyncio.sleep(max(0.0, row.cast_point))
            if self.casts.get(caster.entity) is not cast or self._frozen(caster):
                return
            if self.scene.locations.get(caster.player_id) is not world or world.players.get(caster.player_id) is not caster:
                return
            if target is not None:
                lock = self.scene._lock(target.player_id) if self._player(target) else nullcontext()
                async with lock:
                    if self.casts.get(caster.entity) is not cast or self._frozen(caster) or world.players.get(caster.player_id) is not caster:
                        return
                    if not self._frozen(target) and self._in_world(world, target):
                        await self._impact(cast, world, caster, target, row, apply_effects=not channel)
            if channel:
                deadline = time.monotonic() + row.channel_finish
                interval = row.channel_think or row.channel_finish
                while (remaining := deadline - time.monotonic()) > 0:
                    await asyncio.sleep(min(remaining, interval))
                    if self.casts.get(caster.entity) is not cast or self._frozen(caster) or not self._in_world(world, caster):
                        return
            await asyncio.sleep(max(0.0, row.recovery_time))
        except (StoreRejected, StoreUnavailable, OSError, ValueError):
            _LOG.warning("Scene skill impact save failed caster=%s skill=%s", caster.player_id, row.id, exc_info=True)
        finally:
            if self.casts.get(caster.entity) is cast:
                del self.casts[caster.entity]

    async def _impact(self, cast, world, caster, target, row, *, apply_effects: bool) -> None:
        player_target = self._player(target)
        if player_target:
            await self.scene.flush_player_rows(target)
        if (self.casts.get(caster.entity) is not cast or not self._in_world(world, caster)
                or not self._in_world(world, target) or self._frozen(caster) or self._frozen(target)):
            return
        source = self.scene.rows.assemble(caster.snapshot)
        target_data = self.scene.rows.assemble(target.snapshot) if player_target else None
        if not source.HasField("derived_attributes_component") or not source.HasField("level_component"):
            return
        if player_target and (not target_data.HasField("derived_attributes_component") or not target_data.HasField("level_component")):
            return
        source_attrs = source.derived_attributes_component
        target_attrs = target_data.derived_attributes_component if player_target else target.attributes
        if target_attrs is None:
            return
        was_alive = bool(target_attrs.health)
        dealt_damage = False

        def finish(_snapshot=None):
            if not self._in_world(world, target) or self._frozen(target):
                return
            if dealt_damage:
                self.scene.combat_engagement.record_damage(caster, target)
                if not target_attrs.health:
                    self.scene.combat_engagement.on_death(target)
            if was_alive:
                self.buffs.reset_combat_idle(target)
            if apply_effects:
                for buff_id in row.effect:
                    self.buffs.add(target, buff_id, caster.entity)

        if was_alive:
            base = self.battle_tables.skills[row.id].damage(effective_level(source))
            target_stats = player_stats(self.protocol, self.tables, target_data) if player_target else getattr(target, "derived", None)
            target_level = effective_level(target_data) if player_target else max(1, min(85, getattr(target, "level", 1) or 1))
            damage = self._base_damage(source, source_attrs, target_attrs, target_stats, target_level, row, base)
            damage = self.buffs.before_damage(caster, target, damage)
            if math.isfinite(damage) and damage > 0:
                amount = min(target_attrs.health, math.ceil(damage))
                target_attrs.health -= amount
                dealt_damage = amount > 0
                if player_target:
                    cast.committing = True
                    try:
                        await self.scene.commit_player_data(target, target_data, on_committed=finish)
                    finally:
                        cast.committing = False
                    return
        finish()

    def _base_damage(self, source, source_attrs, target_attrs, target_stats, level, row, base):
        damage = 0.0
        if math.isfinite(base) and base > 0:
            source_stats = player_stats(self.protocol, self.tables, source)
            attack = source_stats.physical_attack if row.damage_type == 1 else source_stats.magic_attack if not row.damage_type else 0
            multiplier = row.attack_multiplier or 1.0
            if not math.isfinite(multiplier) or multiplier < 0:
                multiplier = 0.0
            raw = base * (1 + float(source_attrs.strength) * 0.1) + float(attack) * multiplier
            level_factor = 360 + 120 * level
            defense = float(target_stats.defense) if target_stats is not None else 0.0
            received = max(1.0 - 0.60, level_factor / (float(target_attrs.armor) + defense + level_factor)
                           * (1 - min(100, target_attrs.resistance) / 100))
            if math.isfinite(raw) and raw > 0:
                damage = raw * received
        crit = min(100, source_attrs.critchance)
        if crit and random.random() < crit / 100:
            damage *= 2
        return damage

    async def tick(self, delta: float) -> None:
        if self._closed:
            return
        for actor, due in self.buffs.tick(delta, healing_busy=lambda actor: actor.entity in self._healing):
            if not self._player(actor):
                attributes, derived = actor.attributes, getattr(actor, "derived", None)
                if attributes is not None and derived is not None:
                    attributes.health = self.buffs.heal(actor, due, attributes.health, derived.max_health,
                                                        max(1, min(85, getattr(actor, "level", 1) or 1)))
                continue
            self._healing.add(actor.entity)
            task = asyncio.create_task(self._heal_player(actor, due))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _heal_player(self, player, due) -> None:
        try:
            async with self.scene._lock(player.player_id):
                if not self._live(player) or not self._periodic_allowed(player):
                    return
                await self.scene.flush_player_rows(player)
                if not self._live(player) or not self._periodic_allowed(player):
                    return
                data = self.scene.rows.assemble(player.snapshot)
                if not data.HasField("derived_attributes_component") or not data.HasField("level_component"):
                    return
                stats = player_stats(self.protocol, self.tables, data)
                attrs = data.derived_attributes_component
                health = self.buffs.heal(player, due, attrs.health, stats.max_health, effective_level(data))
                if health != attrs.health:
                    attrs.health = health
                    await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError, ValueError):
            _LOG.warning("Scene periodic healing did not commit player=%s", player.player_id, exc_info=True)
        finally:
            self._healing.discard(player.entity)

    async def close(self) -> None:
        self._closed = True
        for cast in tuple(self.casts.values()):
            if not cast.committing:
                cast.task.cancel()
        if self._tasks:
            try:
                async with asyncio.timeout(6):
                    await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
            except TimeoutError:
                _LOG.warning("Scene skill casts did not drain before shutdown")
        self.buffs.clear()
        self.cooldowns.clear()
