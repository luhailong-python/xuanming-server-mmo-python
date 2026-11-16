"""Authoritative in-memory battle rooms and their direct client contract."""

from __future__ import annotations

import hmac
import logging
import os
import re
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from math import isfinite
from typing import TYPE_CHECKING

from google.protobuf.message import DecodeError, Message

from .battle_effects import FREEZE, STUN, U64_MAX, BattleEffects, landed_damage, landed_heal, rank_factor, scaled
from .battle_replay import BattleReplayRecorder
from .battle_script import BattleScriptRuntime
from .battle_skills import BattleSkills, active as _active, base_stat
from .battle_spectate import END, STATE, TURN, SpectateChannels, SpectateSink
from .battle_tables import BattleBuff, BattleItem, BattleSkill, BattleTables, _Mt19937_64

if TYPE_CHECKING:
    from xuanming.data import ConfigTables
    from xuanming.protocol import Protocol

_LOG = logging.getLogger(__name__)
_ENGINE_ACTOR_FLAG = 1 << 63
_MONSTER_ACTOR_BASE = _ENGINE_ACTOR_FLAG | (1 << 32)
_PET_ACTOR_BASE = _ENGINE_ACTOR_FLAG | (2 << 32)
_PVE_MODES = {4, 5}
_PLAYER, _MONSTER, _PET = 1, 2, 3
_PARTICIPANT, _OBSERVER = 1, 2
_ATTACK, _SKILL, _DEFEND, _ITEM, _FLEE = 1, 2, 3, 4, 5
_INVALID_TABLE, _UNAVAILABLE, _MISSING, _INVALID, _FEATURE = range(1002, 1007)
_AT_CAPACITY, _INACTIVE = 1008, 1009
_INVALID_TARGET, _WRONG_TARGET = 7001, 7002
_SPECTATE_FULL = 16021
_ACTION_MS, _ROUND_MS, _MAX_ROUNDS = 25_000, 6_000, 300
_OBSERVER_GRACE_MS = 30_000
_MAX_SPECTATOR_OPERATIONS = 100_000
_MAX_OUTCOME_PAYLOAD = 960 * 1024
_UUID_PATTERN = re.compile(r"[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}")

CLIENT_REQUESTS = {140: "GetBattleStateRequest", 149: "SubmitBattleActionRequest",
                   162: "SetAutoBattleRequest", 165: "StopWatchBattleRequest"}


class BattleError(ValueError):
    """The direct battle request or its verified identity was rejected."""


@dataclass(frozen=True, slots=True)
class Ticket:
    battle_id: int
    player_id: int
    role: int
    expires_ms: int
    spectator_operation_id: str = ""


@dataclass(slots=True)
class _Connection:
    emit: Callable[[int, Message], None]
    close: Callable[[], None]
    is_live: Callable[[], bool] | None = None


@dataclass(slots=True)
class _Observer:
    operation_id: str
    expires_ms: int


@dataclass(slots=True)
class _Receipt:
    operation_id: str
    expires_ms: int
    canonical: bytes = b""
    aborted: bool = False


@dataclass(slots=True)
class _ObserverReceipt:
    expires_ms: int
    canonical: bytes = b""
    error: int = _INVALID


def _uuid(value: str) -> bool:
    return _UUID_PATTERN.fullmatch(value) is not None and int(value.replace("-", ""), 16) != 0


class TurnEngine:
    """Table-driven turn state owned by one event loop, independent of network I/O."""

    def __init__(
        self, protocol: Protocol, request: Message, *, monsters: Iterable[Message] = (),
        skills: Mapping[int, BattleSkill], items: Mapping[int, BattleItem], max_rounds: int,
        buffs: Mapping[int, BattleBuff] | None = None, silence_permissions: tuple[int, ...] | None = None,
        monster_ids: set[int] | None = None,
    ):
        if not request.battle_id or not request.players:
            raise BattleError("battle needs an identity and players")
        self.protocol = protocol
        self.battle_id = request.battle_id
        self.pve = request.match_mode in _PVE_MODES
        self.rng = _Mt19937_64(request.seed)
        self.item_rules = items
        self.max_rounds = max_rounds
        self.round_index = 1
        self.outcome = 0
        self.actors: dict[int, Message] = {}
        self.items: dict[int, list[Message]] = {}
        self.pending: dict[int, Message] = {}
        self.items_used: Counter[int] = Counter()
        self.items_consumed: dict[int, Counter[int]] = {}
        self.last_action_order: list[int] = []
        self.defeated_monsters: list[int] = []
        self.effects = BattleEffects(self, buffs or {})
        self.skills = BattleSkills(self, skills, silence_permissions)
        self._group = 0
        slots = [0, 0]
        for player in request.players:
            actor_id = player.player_id
            if not 0 < actor_id < _ENGINE_ACTOR_FLAG or actor_id in self.actors or player.team_index > 1:
                raise BattleError("invalid player snapshot")
            if slots[player.team_index] >= 5:
                raise BattleError("too many players on one side")
            actor = self._actor(player, actor_id, _PLAYER, player.team_index, slots, player.player_name)
            actor.appearance_id = player.appearance_id
            actor.class_id = player.class_id
            actor.gender = player.gender
            if player.HasField("combat"):
                actor.combat.CopyFrom(player.combat)
            actor.buffs.extend(player.buffs)
            self.skills.initialize(actor, player)
            self.items[actor_id] = []
            for item in player.items:
                if item.count:
                    entry = type(item)()
                    entry.CopyFrom(item)
                    self.items[actor_id].append(entry)

        pet_ids: set[int] = set()
        for player in request.players:
            for pet in player.pets:
                if not pet.pet_id or pet.pet_id in pet_ids or pet.owner_player_id not in (0, player.player_id):
                    raise BattleError("invalid pet snapshot")
                actor = self._actor(pet, _PET_ACTOR_BASE + len(pet_ids), _PET, player.team_index, slots, pet.pet_name)
                actor.owner_player_id = player.player_id
                actor.pet_table_id = pet.pet_table_id
                actor.pet_id = pet.pet_id
                actor.is_auto = True
                self.skills.initialize(actor, pet, pet=True)
                pet_ids.add(pet.pet_id)
        for index, monster in enumerate(monsters):
            actor = self.protocol.message("BattleActorState")
            actor.CopyFrom(monster)
            actor.actor_id = _MONSTER_ACTOR_BASE + index
            actor.actor_type = _MONSTER
            actor.team_index = 1
            actor.formation_slot = slots[1]
            slots[1] += 1
            self.actors[actor.actor_id] = actor
        if {actor.team_index for actor in self.actors.values()} != {0, 1}:
            raise BattleError("both sides need a unit")
        self.effects.sanitize()
        self.scripts = BattleScriptRuntime(self, request, monster_ids)

    def _actor(self, snapshot: Message, actor_id: int, actor_type: int, team: int,
               slots: list[int], name: str) -> Message:
        attributes = snapshot.base_attributes
        actor = self.protocol.message(
            "BattleActorState", actor_id=actor_id, actor_type=actor_type, team_index=team,
            level=snapshot.level, attributes=attributes, max_health=snapshot.max_health or attributes.health,
            max_mana=snapshot.max_mana or attributes.mana, physical_attack=snapshot.physical_attack,
            magic_attack=snapshot.magic_attack, defense=snapshot.defense, formation_slot=slots[team],
        )
        actor.name = name
        slots[team] += 1
        self.actors[actor_id] = actor
        return actor

    def pending_players(self) -> list[int]:
        if self.outcome:
            return []
        return [actor.actor_id for actor in self.actors.values()
                if actor.actor_type == _PLAYER and _active(actor) and not actor.is_auto
                and actor.actor_id not in self.pending]

    def snapshot(self, deadline_ms: int, viewer: int = 0, *, unredacted: bool = False) -> Message:
        state = self.protocol.message(
            "BattleStateS2C", battle_id=self.battle_id, round_index=self.round_index,
            action_deadline_ms=deadline_ms, outcome=self.outcome, pending_actor_ids=self.pending_players(),
        )
        state.actors.extend(self.actors.values())
        if not unredacted:
            self.personalize(state, viewer)
        return state

    def personalize(self, state: Message, viewer: int) -> None:
        for actor in state.actors:
            actor.ClearField("combat")
            if not viewer or (actor.actor_id != viewer and actor.owner_player_id != viewer):
                actor.ClearField("skill_cooldown_rounds")
        state.ClearField("self_items")
        for item in sorted(self.items.get(viewer, ()), key=lambda entry: entry.item_table_id):
            if item.count:
                state.self_items.add().CopyFrom(item)

    def validate(self, actor: Message, action: Message) -> int:
        if not _active(actor):
            return _INACTIVE
        if self.effects.has(actor, STUN, FREEZE):
            return 7006
        action_type = action.action_type
        if action_type in (_ATTACK, _DEFEND):
            return 0
        if action_type == _FLEE:
            return 0 if self.pve else _INVALID
        if action_type == _SKILL:
            return self.skills.validate(actor, action)
        if action_type == _ITEM:
            return self._validate_item(actor, action.item_table_id, action.target_id)
        return _INVALID

    def _validate_item(self, actor: Message, item_id: int, target_id: int) -> int:
        rule = self.item_rules.get(item_id)
        if rule is None:
            return 1001
        if not rule.usable or not (rule.heal_health or rule.heal_mana):
            return _INVALID
        if not any(item.item_table_id == item_id and item.count
                   for item in self.items.get(actor.actor_id, ())):
            return 6007
        if not self.pve and self.items_used[actor.actor_id] >= 5:
            return _INVALID
        if target_id in (0, actor.actor_id):
            return 0
        target = self.actors.get(target_id)
        if target is None or not _active(target):
            return _INVALID_TARGET
        return 0 if target.team_index == actor.team_index else _WRONG_TARGET

    @staticmethod
    def _enemy(actor: Message, target: Message | None) -> bool:
        return target is not None and _active(target) and actor.team_index != target.team_index

    def _target(self, actor: Message, target_id: int) -> Message | None:
        target = self.actors.get(target_id)
        if self._enemy(actor, target):
            return target
        enemies = [other for other in self.actors.values() if self._enemy(actor, other)]
        return enemies[self.rng.next() % len(enemies)] if enemies else None

    def resolve(self) -> Message:
        turn = self.protocol.message("TurnResultS2C", battle_id=self.battle_id, round_index=self.round_index)
        if self.outcome:
            turn.state.CopyFrom(self.snapshot(0, unredacted=True))
            return turn
        self._fill_default_actions()
        order = sorted((actor for actor in self.actors.values() if _active(actor)), key=self._sort_key)
        self.last_action_order = [actor.actor_id for actor in order]
        turn.action_order.extend(self.last_action_order)
        self._group = 0
        for actor in order:
            if _active(actor):
                self._group += 1
                self._execute(turn, self._group, actor, self.pending[actor.actor_id])
        for actor in self.actors.values():
            if _active(actor) and actor.buffs:
                self._group += 1
                self.effects.tick(actor, turn, self._group)
        for actor in self.actors.values():
            actor.is_defending = False
            for skill_id, remaining in list(actor.skill_cooldown_rounds.items()):
                if remaining:
                    actor.skill_cooldown_rounds[skill_id] = remaining - 1
        side_a, side_b = (any(_active(actor) and actor.team_index == side for actor in self.actors.values())
                          for side in (0, 1))
        if not side_a and not side_b:
            self.outcome = 3
        elif not side_a:
            self.outcome = 2
        elif not side_b:
            self.outcome = 1
        elif self.round_index >= self.max_rounds:
            self.outcome = 2
        self.pending.clear()
        if not self.outcome:
            self.round_index += 1
        turn.state.CopyFrom(self.snapshot(0, unredacted=True))
        return turn

    def _fill_default_actions(self) -> None:
        self.scripts.fill()
        for actor in self.actors.values():
            if not _active(actor) or actor.actor_id in self.pending:
                continue
            default = None
            if actor.actor_type == _PET and actor.skill_table_ids:
                target = next((other for other in self.actors.values() if self._enemy(actor, other)), None)
                if target is not None:
                    for skill_id in actor.skill_table_ids:
                        action = self.protocol.message("BattleAction", action_type=_SKILL, skill_table_id=skill_id,
                                                       target_id=target.actor_id)
                        if not self.validate(actor, action):
                            default = action
                            break
            elif actor.actor_type == _PLAYER and actor.is_auto:
                default = self.skills.auto_action(actor)
            self.pending[actor.actor_id] = default if default is not None else self.protocol.message("BattleAction", action_type=_ATTACK)

    def _sort_key(self, actor: Message) -> tuple[int, int, int]:
        action = self.pending.get(actor.actor_id)
        return (-self.skills.initiative(actor, action), -self._effective_stat(actor, 13), actor.actor_id)

    def _effective_stat(self, actor: Message, kind: int) -> int:
        value = base_stat(actor, kind)
        rate = self.effects.multiplier(actor, kind)
        return value if rate == 1.0 else scaled(value, rate)

    @staticmethod
    def _event(turn: Message, group: int, kind: int, source: Message, target: Message, **fields) -> Message:
        return turn.events.add(event_type=kind, source_id=source.actor_id, target_id=target.actor_id,
                               group_id=group, **fields)

    def _execute(self, turn: Message, group: int, actor: Message, action: Message) -> None:
        if self.effects.has(actor, STUN, FREEZE):
            return
        if action.action_type == _DEFEND:
            actor.is_defending = True
            self._event(turn, group, 9, actor, actor)
            return
        if action.action_type == _FLEE:
            if self.pve:
                top_speed = max((self._effective_stat(other, 13) for other in self.actors.values() if self._enemy(actor, other)), default=0)
                difference = float(self._effective_stat(actor, 13)) - float(top_speed)
                chance = max(0.05, min(0.95, 0.5 + difference * (0.01 / 12)))
                actor.fled = self.rng.unit() < chance
            self._event(turn, group, 11, actor, actor, success=actor.fled)
            return
        if action.action_type == _ITEM:
            self._use_item(turn, group, actor, action)
            return
        if action.action_type == _SKILL:
            self.skills.execute(actor, action, turn, group)
            return
        if action.action_type == _ATTACK:
            self._attack_target(turn, group, actor, action.target_id)

    def _attack_target(self, turn: Message, group: int, actor: Message, target_id: int) -> None:
        target = self._target(actor, target_id)
        if target is not None:
            self._basic_attack(turn, group, actor, target)

    def _use_item(self, turn: Message, group: int, actor: Message, action: Message) -> None:
        target_id = action.target_id
        if self._validate_item(actor, action.item_table_id, target_id):
            if target_id in (0, actor.actor_id) or self._validate_item(actor, action.item_table_id, actor.actor_id):
                return
            target_id = actor.actor_id
        rule = self.item_rules[action.item_table_id]
        target = self.actors.get(target_id, actor)
        old_health, old_mana = target.attributes.health, target.attributes.mana
        target.attributes.health = old_health + landed_heal(target, float(rule.heal_health))
        target.attributes.mana = old_mana + min(rule.heal_mana, max(0, target.max_mana - old_mana))
        item = next(entry for entry in self.items[actor.actor_id]
                    if entry.item_table_id == action.item_table_id and entry.count)
        item.count -= 1
        self.items_used[actor.actor_id] += 1
        self.items_consumed.setdefault(actor.actor_id, Counter())[action.item_table_id] += 1
        mana_delta = target.attributes.mana - old_mana
        event = self._event(turn, group, 10, actor, target, item_table_id=action.item_table_id,
                            value=(target.attributes.health - old_health) or mana_delta,
                            target_health_after=target.attributes.health, target_mana_after=target.attributes.mana)
        if rule.heal_mana:
            event.mana_delta = mana_delta

    def _basic_attack(self, turn: Message, group: int, actor: Message, target: Message, hit_kind: int = 0) -> None:
        rule = BattleSkill(10, physical=True)
        self._event(turn, group, 1, actor, target, hit_kind=hit_kind)
        self._damage(turn, group, actor, target, rule, hit_kind=hit_kind)
        if hit_kind == 0 and _active(actor) and _active(target) and self.rng.percent(actor.combat.combo_rate):
            self._damage(turn, group, actor, target, rule, hit_kind=1, hit_index=1)
        if (hit_kind == 0 and _active(actor) and _active(target) and not self.effects.has(target, STUN, FREEZE)
                and self.rng.percent(target.combat.counter_rate)):
            self._group += 1
            self._basic_attack(turn, self._group, target, actor, hit_kind=2)

    def _damage_before_critical(self, actor: Message, target: Message, rule: BattleSkill, base: float) -> float:
        if base <= 0 or not isfinite(base):
            return 0.0
        damage_kind = rule.damage_kind
        attack = self._effective_stat(actor, 10) if damage_kind == 1 else actor.magic_attack if damage_kind == 0 else 0
        resist_bonus = target.combat.physical_resist if damage_kind == 1 else target.combat.magic_resist if damage_kind == 0 else 0
        resistance = min(100, target.attributes.resistance + resist_bonus) / 100
        level_factor = 360 + 120 * min(85, max(1, target.level))
        defense = float(target.attributes.armor) + float(self._effective_stat(target, 11))
        ratio = max(1.0 - 0.60, level_factor / (defense + level_factor) * (1 - resistance))
        multiplier = rule.attack_multiplier
        if not isfinite(multiplier) or multiplier < 0:
            multiplier = 0.0
        raw = base * (1 + float(actor.attributes.strength) * 0.1) + float(attack) * multiplier
        if not isfinite(raw) or raw <= 0:
            return 0.0
        return raw * ratio * (1.0 if self.pve else 0.3)

    @staticmethod
    def _critical_chance(actor: Message, rule: BattleSkill) -> float:
        kind = rule.damage_kind
        bonus = actor.combat.physical_crit_rate if kind == 1 else actor.combat.magic_crit_rate if kind == 0 else 0
        return min(100, actor.attributes.critchance + bonus) / 100

    def _expected_damage(self, actor: Message, target: Message, rule: BattleSkill, base: float) -> float:
        return self._damage_before_critical(actor, target, rule, base) * (1 + self._critical_chance(actor, rule))

    def _damage(self, turn: Message, group: int, actor: Message, target: Message, rule: BattleSkill,
                *, skill_id: int = 0, hit_kind: int = 0, hit_index: int = 0,
                base: float | None = None, rank: int = 0) -> None:
        damage = self._damage_before_critical(actor, target, rule, rule.base_damage if base is None else base)
        critical_rate = self._critical_chance(actor, rule)
        critical = critical_rate > 0 and self.rng.unit() < critical_rate
        if critical:
            damage *= 2
        damage *= rank_factor(rank)
        if hit_kind == 1:
            damage *= 0.5
        dealt = landed_damage(target, damage)
        self._apply_damage(turn, group, actor, target, dealt, critical=critical, skill_id=skill_id,
                           hit_kind=hit_kind, hit_index=hit_index)
        if not skill_id and hit_kind != 2 and dealt and _active(actor) and _active(target) and self.rng.percent(target.combat.reflect_rate):
            reflected = min(actor.attributes.health, (dealt + 1) // 2)
            self._apply_damage(turn, group, target, actor, reflected, hit_kind=3, hit_index=hit_index)

    def _apply_damage(self, turn: Message, group: int, source: Message, target: Message, value: int,
                      *, critical: bool = False, skill_id: int = 0, hit_kind: int = 0, hit_index: int = 0) -> None:
        target.attributes.health -= value
        self._event(turn, group, 3, source, target, value=value, is_critical=critical, skill_table_id=skill_id,
                    hit_kind=hit_kind, hit_index=hit_index, target_health_after=target.attributes.health,
                    target_mana_after=target.attributes.mana)
        self._death(turn, group, target, source.actor_id, hit_index)

    def _death(self, turn: Message, group: int, target: Message, source_id: int, hit: int = 0) -> None:
        if target.is_dead or target.attributes.health:
            return
        source = self.actors.get(source_id)
        owner = self.actors.get(source.owner_player_id) if source is not None and source.actor_type == _PET else source
        if (target.actor_type == _MONSTER and target.team_index == 1 and target.monster_table_id
                and source is not None and source.team_index == 0 and owner is not None
                and owner.actor_type == _PLAYER and owner.team_index == 0):
            self.defeated_monsters.append(target.monster_table_id)
        target.is_dead = True
        target.is_defending = False
        target.ClearField("buffs")
        self._event(turn, group, 8, target, target, hit_index=hit)


@dataclass(slots=True)
class _Room:
    engine: TurnEngine
    original: Message
    deadline_ms: int
    action_deadline_ms: int
    routing: dict[int, Message]
    reward_profile: Message | None = None
    observers: dict[int, _Observer] = field(default_factory=dict)
    connections: dict[int, _Connection] = field(default_factory=dict)
    visible_before_terminal: dict[int, Message] = field(default_factory=dict)
    final_turn: Message | None = None
    proposed_outcome: int = 0
    outcome_retry_ms: int = 0
    outcome_finished_ms: int = 0
    outcome_since_ms: int = 0
    outcome_payload: bytes = b""
    confirm_retry_ms: int = 0
    confirm_until_ms: int = 0
    replay: BattleReplayRecorder | None = None
    outcome_replay: Message | None = None
    spectate: SpectateChannels = field(default_factory=SpectateChannels)


class BattleNode:
    """Own battle rooms, client identity, admissions, and their deadline-driven lifecycle."""

    def __init__(
        self, protocol: Protocol, *, node_id: int, instance_id: str, host: str, port: int,
        ticket_secret: bytes | str, physical_region: int = 1, max_rooms: int = 1024,
        max_observers: int = 20, max_connections: int = 4096, now_ms: Callable[[], int] | None = None,
        steady_ms: Callable[[], int] | None = None,
        on_lobby_push: Callable[[Message, int, int, Message], None] | None = None,
        on_confirm: Callable[[Message, int, int, int], None] | None = None,
        on_outcome: Callable[[int, int], None] | None = None,
        on_spectate_frame: SpectateSink | None = None,
        monster_factory: Callable[[Message], Iterable[Message]] | None = None,
        skills: Mapping[int, BattleSkill] | None = None, items: Mapping[int, BattleItem] | None = None,
        max_rounds: int = _MAX_ROUNDS, tables: ConfigTables | None = None,
        buffs: Mapping[int, BattleBuff] | None = None, fingerprint_mode: str | None = None,
        on_rooms_changed: Callable[[int], None] | None = None,
    ):
        if not 0 < node_id < 1 << 32 or not instance_id or not host or not 0 <= port <= 65535:
            raise ValueError("invalid battle node endpoint or identity")
        if physical_region <= 0 or min(max_rooms, max_observers, max_rounds) <= 0:
            raise ValueError("battle capacity and physical region must be positive")
        if not 1 <= max_connections <= 65535:
            raise ValueError("battle connection capacity must be between 1 and 65535")
        self.protocol = protocol
        self.node_id = node_id
        self.instance_id = instance_id
        self.host = host
        self.port = port
        self.ticket_secret = ticket_secret.encode() if isinstance(ticket_secret, str) else ticket_secret
        self.physical_region = physical_region
        self.max_rooms = max_rooms
        self.max_observers = max_observers
        self.max_connections = max_connections
        self.max_rounds = max_rounds
        self.now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)
        self._steady_ms = steady_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self.on_lobby_push = on_lobby_push
        self.on_confirm = on_confirm
        self.on_outcome = on_outcome
        self.on_spectate_frame = on_spectate_frame
        self.on_rooms_changed = on_rooms_changed
        self.tables = BattleTables(protocol, tables, rewards_path=os.environ.get("BATTLE_REWARD_CONFIG_PATH")) if tables is not None else None
        self.monster_factory = monster_factory or (self.tables.monsters if self.tables else None)
        self.skills = (self.tables.skills if self.tables else {}) | dict(skills or {})
        self.items = (self.tables.items if self.tables else {}) | dict(items or {})
        self.buffs = (self.tables.buffs if self.tables else {}) | dict(buffs or {})
        self.fingerprint_mode = fingerprint_mode or os.environ.get("BATTLE_TABLE_FINGERPRINT_MODE", "warn")
        if self.fingerprint_mode not in ("off", "warn", "enforce"):
            _LOG.warning("invalid Battle table fingerprint mode %r; falling back to warn", self.fingerprint_mode)
            self.fingerprint_mode = "warn"
        self.rooms: dict[int, _Room] = {}
        self.receipts: dict[int, _Receipt] = {}
        self.observer_receipts: dict[tuple[int, int, str], _ObserverReceipt] = {}
        self.admitting = True
        self._draining = False

    @staticmethod
    def _error(response: Message, error: int) -> Message:
        if error:
            response.error_message.id = error
        return response

    def create(self, request: Message) -> Message:
        response = self.protocol.message("CreateBattleResponse", battle_id=request.battle_id)
        if self._draining:
            return self._error(response, _UNAVAILABLE)
        now = self.now_ms()
        if (request.outcome_protocol_version != 1 or not request.placement_operation_id
                or request.target_instance_id != self.instance_id
                or request.required_physical_region != self.physical_region):
            return self._error(response, _INVALID)
        canonical = request.SerializeToString(deterministic=True)
        receipt = self.receipts.get(request.battle_id)
        if receipt is not None:
            if receipt.aborted or receipt.canonical != canonical or request.battle_id not in self.rooms:
                return self._error(response, _INVALID)
            return self._create_success(response)
        if not self.admitting or len(self.rooms) >= self.max_rooms:
            return self._error(response, _UNAVAILABLE)
        if request.admit_before_ms <= now or not request.battle_id or not request.players:
            return self._error(response, _INVALID)
        if any(not player.routing.gate_instance_id or not player.routing.scene_instance_id for player in request.players):
            return self._error(response, _INVALID)
        if self.on_outcome is None:
            return self._error(response, _FEATURE)
        if not self.ticket_secret or not self.port:
            return self._error(response, _UNAVAILABLE)
        fingerprint_mismatches = self._fingerprint_mismatches(request)
        if fingerprint_mismatches:
            mode = self.fingerprint_mode
            _LOG.warning("battle table fingerprint mismatch battle=%s mode=%s fields=%s",
                         request.battle_id, mode, fingerprint_mismatches)
            if mode == "enforce":
                response.error_message.parameters.append(
                    f"battle table fingerprint mismatch: node={self.tables.fingerprint} request={request.table_fingerprint}"
                )
                return self._error(response, _FEATURE)
        try:
            reward_profile = (self.tables.reward_profile(request.battle_config_id)
                              if self.tables and request.match_mode in _PVE_MODES else None)
            monsters = self.monster_factory(request) if self.monster_factory and request.match_mode in _PVE_MODES else ()
            max_rounds = self.tables.max_rounds(request, self.max_rounds) if self.tables else self.max_rounds
            engine = TurnEngine(self.protocol, request, monsters=monsters, skills=self.skills, items=self.items,
                                max_rounds=max_rounds, buffs=self.buffs,
                                silence_permissions=self.tables.silence_permissions if self.tables else None,
                                monster_ids={row.id for row in self.tables.tables["Monster"].rows()} if self.tables else None)
        except (BattleError, ValueError):
            return self._error(response, _INVALID_TABLE)
        if request.admit_before_ms <= self.now_ms():
            return self._error(response, _INVALID)
        deadline = request.deadline_ms if request.deadline_ms > now else now + (self.max_rounds + 2) * _ROUND_MS
        original = self.protocol.message("CreateBattleRequest")
        original.CopyFrom(request)
        routing = {player.player_id: player.routing for player in original.players}
        room = _Room(engine, original, deadline, now + _ACTION_MS, routing,
                     reward_profile=reward_profile,
                     confirm_retry_ms=now + 10_000, confirm_until_ms=now + 180_000)
        if request.record_replay:
            fingerprint = self.tables.fingerprint if self.tables else ""
            room.replay = BattleReplayRecorder(self.protocol, original, fingerprint, engine.snapshot(0))
        assignments = {player: self._assignment(room, player, _PARTICIPANT) for player in routing}
        self.rooms[request.battle_id] = room
        if self.on_rooms_changed:
            self._publish(self.on_rooms_changed, len(self.rooms))
        self.receipts[request.battle_id] = _Receipt(request.placement_operation_id, deadline + 60_000, canonical)
        for player, route in routing.items():
            if self.on_lobby_push:
                self._publish(self.on_lobby_push, route, player, 177, assignments[player])
                start = self.protocol.message("BattleStartS2C", battle_id=request.battle_id,
                                              state=engine.snapshot(room.action_deadline_ms, player))
                self._publish(self.on_lobby_push, route, player, 143, start)
            if self.on_confirm:
                self._publish(self.on_confirm, route, player, request.battle_id, deadline)
        return self._create_success(response)

    def _fingerprint_mismatches(self, request: Message) -> list[str]:
        if self.tables is None or self.fingerprint_mode == "off":
            return []
        expected = self.tables.fingerprint
        fingerprints = [("request", request.table_fingerprint)]
        fingerprints.extend((f"player_{player.player_id}", player.table_fingerprint) for player in request.players)
        return [f"{name}={fingerprint}" for name, fingerprint in fingerprints if fingerprint and fingerprint != expected]

    def _create_success(self, response: Message) -> Message:
        response.adopted_outcome_protocol_version = 1
        response.target_instance_id = self.instance_id
        response.room_count = len(self.rooms)
        return response

    def query_creation(self, request: Message) -> Message:
        response = self.protocol.message("QueryBattleCreationResponse")
        if request.target_instance_id != self.instance_id or not request.placement_operation_id:
            return response
        response.target_instance_id = self.instance_id
        response.supported_outcome_protocol_version = int(self.on_outcome is not None)
        response.rejects_new_battles = not self.admitting or len(self.rooms) >= self.max_rooms
        receipt = self.receipts.get(request.battle_id)
        if receipt is None:
            response.state = 2
        elif receipt.operation_id == request.placement_operation_id:
            if receipt.aborted:
                response.state = 3
            elif request.battle_id in self.rooms:
                response.state = 1
                response.adopted_outcome_protocol_version = 1
            else:
                response.state = 2
        return response

    def accepts_placement_operation(self, request: Message) -> bool:
        receipt = self.receipts.get(request.battle_id)
        if not request.placement_operation_id:
            return not request.target_instance_id and receipt is None
        return request.target_instance_id == self.instance_id and (
            receipt is None or receipt.operation_id == request.placement_operation_id
        )

    def destroy(self, request: Message) -> Message:
        receipt = self.receipts.get(request.battle_id)
        if (request.battle_id not in self.rooms and request.placement_operation_id
                and self.accepts_placement_operation(request)):
            expires = max(receipt.expires_ms if receipt else 0, self.now_ms() + 360_000)
            self.receipts[request.battle_id] = _Receipt(request.placement_operation_id, expires, aborted=True)
        return self.protocol.message("Empty")

    def _assignment(self, room: _Room, player: int, role: int, operation: str = "") -> Message:
        payload = self.protocol.message(
            "BattleTicketPayload", battle_id=room.engine.battle_id, player_id=player,
            battle_node_id=self.node_id, battle_instance_id=self.instance_id, expire_at_ms=room.deadline_ms,
            role=role, spectator_operation_id=operation,
        ).SerializeToString(deterministic=True)
        signature = hmac.new(self.ticket_secret, payload, "sha256").hexdigest().encode("ascii")
        return self.protocol.message(
            "BattleAssignedS2C", battle_id=room.engine.battle_id, host=self.host, port=self.port,
            token_payload=payload, token_signature=signature, expire_at_ms=room.deadline_ms, role=role,
        )

    def issue_ticket(self, request: Message) -> Message:
        response = self.protocol.message("IssueBattleTicketResponse")
        room = self.rooms.get(request.battle_id)
        if not request.player_id or request.target_instance_id != self.instance_id or room is None:
            return self._error(response, _INVALID)
        player = request.player_id
        operation = request.spectator_operation_id
        if player in room.routing and not operation:
            role = _PARTICIPANT
        elif player in room.observers and room.observers[player].operation_id == operation:
            role = _OBSERVER
        else:
            return self._error(response, _INVALID)
        if room.deadline_ms <= self.now_ms() or not self.ticket_secret or not self.port:
            return self._error(response, _UNAVAILABLE)
        response.assignment.CopyFrom(self._assignment(room, player, role, operation))
        return response

    def authorize(self, payload: bytes, signature: bytes) -> Ticket:
        expected = hmac.new(self.ticket_secret, payload, "sha256").hexdigest().encode("ascii")
        if not self.ticket_secret or not hmac.compare_digest(expected, signature):
            raise BattleError("invalid ticket signature")
        try:
            token = self.protocol.parse("BattleTicketPayload", payload)
        except (DecodeError, ValueError) as error:
            raise BattleError("malformed ticket payload") from error
        if not token.battle_id or not token.player_id:
            raise BattleError("ticket rejected: empty_identity")
        if token.battle_node_id != self.node_id:
            raise BattleError("ticket rejected: node_mismatch")
        if token.battle_instance_id != self.instance_id:
            raise BattleError("ticket rejected: instance_mismatch")
        if token.expire_at_ms <= self.now_ms():
            raise BattleError("ticket rejected: expired")
        if token.role not in (_PARTICIPANT, _OBSERVER):
            raise BattleError("ticket rejected: role_invalid")
        if token.role == _OBSERVER and not _uuid(token.spectator_operation_id) or token.role == _PARTICIPANT and token.spectator_operation_id:
            raise BattleError("ticket rejected: spectator_operation_invalid")
        ticket = Ticket(token.battle_id, token.player_id, token.role, token.expire_at_ms, token.spectator_operation_id)
        if not self._member(ticket):
            raise BattleError("battle not found or player not in this battle")
        return ticket

    def _member(self, ticket: Ticket) -> _Room | None:
        room = self.rooms.get(ticket.battle_id)
        if room is not None:
            if ticket.role == _PARTICIPANT and ticket.player_id in room.routing:
                return room
            observer = room.observers.get(ticket.player_id)
            if ticket.role == _OBSERVER and observer is not None and observer.operation_id == ticket.spectator_operation_id:
                return room
        return None

    def attach(
        self, ticket: Ticket, emit: Callable[[int, Message], None], close: Callable[[], None],
        *, is_live: Callable[[], bool] | None = None,
    ) -> None:
        room = self._member(ticket)
        if self._draining or room is None or ticket.expires_ms <= self.now_ms():
            raise BattleError("battle not found or player not in this battle")
        old = room.connections.get(ticket.player_id)
        room.connections[ticket.player_id] = _Connection(emit, close, is_live)
        if ticket.role == _OBSERVER:
            room.observers[ticket.player_id].expires_ms = 0
        if old is not None and old.close is not close:
            self._publish(old.close)

    def on_verified(self, ticket: Ticket) -> None:
        room = self._member(ticket)
        if room is not None and ticket.role == _OBSERVER:
            self._push_observer_state(room, ticket.player_id)

    def _push_observer_state(self, room: _Room, player_id: int) -> None:
        live = {
            player for player, connection in room.connections.items()
            if player in room.observers and (connection.is_live is None or connection.is_live())
        }
        if player_id in live:
            state = self.protocol.message("SpectateStateS2C", state=self._snapshot(room, 0),
                                          observer_count=len(live))
            self._emit(room, player_id, 161, state)

    def owns_connection(self, ticket: Ticket, close: Callable[[], None]) -> bool:
        room = self._member(ticket)
        connection = room.connections.get(ticket.player_id) if room is not None else None
        return connection is not None and connection.close is close

    def detach(self, ticket: Ticket, close: Callable[[], None]) -> None:
        room = self._member(ticket)
        if room is not None and self.owns_connection(ticket, close):
            del room.connections[ticket.player_id]
            if ticket.role == _OBSERVER:
                room.observers[ticket.player_id].expires_ms = self._steady_ms() + _OBSERVER_GRACE_MS

    def add_observer(self, request: Message) -> Message:
        response = self.protocol.message("AddObserverResponse")
        player, operation = request.observer_player_id, request.spectator_operation_id
        now = self.now_ms()
        if (not player or not request.battle_id or not _uuid(operation) or request.target_instance_id != self.instance_id
                or not request.routing.gate_instance_id or not now < request.admit_before_ms <= now + 30_000):
            return self._error(response, _INVALID)
        key = request.battle_id, player, operation
        canonical = request.SerializeToString(deterministic=True)
        receipt = self.observer_receipts.get(key)
        room = self.rooms.get(request.battle_id)
        if receipt is not None:
            if receipt.canonical != canonical or receipt.error:
                return self._error(response, receipt.error or _INVALID)
            if room is None or player not in room.observers or room.observers[player].operation_id != operation:
                return self._error(response, _INVALID)
        else:
            if len(self.observer_receipts) >= _MAX_SPECTATOR_OPERATIONS:
                return self._error(response, _AT_CAPACITY)
            receipt = _ObserverReceipt(request.admit_before_ms, canonical)
            self.observer_receipts[key] = receipt
            if room is None:
                receipt.error = _MISSING
            elif not self.admitting or room.proposed_outcome or room.deadline_ms <= now:
                receipt.error = _UNAVAILABLE
            elif room.original.spectate_visibility != 2 or player in room.routing or player in room.observers:
                receipt.error = _INVALID
            elif (len(room.observers) >= self.max_observers
                  or sum(len(current.observers) for current in self.rooms.values())
                  >= max(self.max_observers, self.max_connections // 4)):
                receipt.error = _SPECTATE_FULL
            elif not self.ticket_secret or not self.port:
                receipt.error = _UNAVAILABLE
            else:
                room.observers[player] = _Observer(operation, self._steady_ms() + _OBSERVER_GRACE_MS)
                receipt.error = 0
            if receipt.error:
                return self._error(response, receipt.error)
        assignment = self._assignment(room, player, _OBSERVER, operation)
        if self.on_lobby_push:
            self._publish(self.on_lobby_push, request.routing, player, 177, assignment)
        self._push_observer_state(room, player)
        return response

    def query_observer(self, request: Message) -> Message:
        response = self.protocol.message("QueryObserverResponse")
        if request.target_instance_id != self.instance_id or not _uuid(request.spectator_operation_id):
            return response
        response.target_instance_id = self.instance_id
        response.spectator_operation_id = request.spectator_operation_id
        room = self.rooms.get(request.battle_id)
        observer = room.observers.get(request.observer_player_id) if room is not None else None
        response.state = 1 if observer is not None and observer.operation_id == request.spectator_operation_id else 2
        return response

    def remove_observer(self, request: Message) -> Message:
        response = self.protocol.message("RemoveObserverResponse")
        now = self.now_ms()
        player, operation = request.observer_player_id, request.spectator_operation_id
        if (not player or not _uuid(operation) or request.target_instance_id != self.instance_id
                or not 0 < request.admit_before_ms <= now + 30_000):
            return response
        key = request.battle_id, player, operation
        previous = self.observer_receipts.get(key)
        if previous is not None and previous.expires_ms != request.admit_before_ms:
            return response
        if request.admit_before_ms > now:
            if previous is None and len(self.observer_receipts) >= _MAX_SPECTATOR_OPERATIONS:
                return response
            self.observer_receipts[key] = _ObserverReceipt(request.admit_before_ms)
        elif previous is not None:
            del self.observer_receipts[key]
        room = self.rooms.get(request.battle_id)
        if room is not None:
            observer = room.observers.get(player)
            if observer is not None and observer.operation_id == operation:
                self._drop_observer(room, player)
        response.cancelled = True
        response.target_instance_id = self.instance_id
        response.spectator_operation_id = operation
        return response

    def _drop_observer(self, room: _Room, player: int, *, notify: bool = True) -> None:
        del room.observers[player]
        if notify:
            self._emit(room, player, 166, self.protocol.message("SpectateEndS2C", battle_id=room.engine.battle_id, reason=3))
        connection = room.connections.pop(player, None)
        if connection is not None:
            self._publish(connection.close)

    def _spectate_room(self, request):
        if request.target_instance_id != self.instance_id:
            return None, _INVALID
        room = self.rooms.get(request.battle_id)
        if room is None:
            return None, _MISSING
        if not self.admitting or self.on_spectate_frame is None:
            return None, _UNAVAILABLE
        if room.original.spectate_visibility != 2:
            return None, _INVALID
        return room, 0

    def attach_relay_gate(self, request):
        response = self.protocol.message("AttachRelayGateResponse")
        room, error = self._spectate_room(request)
        if room is None:
            return self._error(response, error)
        if not request.gate_node_id or not request.gate_instance_id:
            return self._error(response, _INVALID)
        response.battle_instance_id = self.instance_id
        response.deadline_ms = room.deadline_ms
        response.broadcast = room.spectate.broadcast
        if room.spectate.attach(request.gate_node_id, request.gate_instance_id, self._steady_ms()):
            self._spectate_state(room, targets=(("relay", request.gate_node_id, request.gate_instance_id),))
        return response

    def promote_spectate_broadcast(self, request):
        response = self.protocol.message("PromoteSpectateBroadcastResponse")
        room, error = self._spectate_room(request)
        if room is None:
            return self._error(response, error)
        response.battle_instance_id = self.instance_id
        response.deadline_ms = room.deadline_ms
        if not room.spectate.broadcast:
            room.spectate.broadcast = True
            self._spectate_state(room)
        return response

    def _spectate_state(self, room: _Room, targets=None) -> None:
        state = self._snapshot(room, 0)
        message = self.protocol.message("SpectateStateS2C", state=state)
        self._spectate_frame(room, STATE, state.round_index, message, targets=targets)

    def _spectate_frame(self, room: _Room, kind: int, round_index: int, message: Message, *, targets=None) -> None:
        if self.on_spectate_frame is None:
            return
        if targets is None:
            targets = room.spectate.targets()
            if not targets:
                return
            room.spectate.last_frame_steady_ms = None if kind == END else self._steady_ms()
        original = room.spectate.frame(self.protocol, battle_id=room.engine.battle_id, instance_id=self.instance_id,
                                       deadline_ms=room.deadline_ms, kind=kind, round_index=round_index, message=message)
        for channel, gate_id, gate_instance in targets:
            event = type(original)()
            event.CopyFrom(original)
            event.target_gate_id, event.target_instance_id = gate_id, gate_instance
            self._publish(self.on_spectate_frame, channel, gate_id, event.battle_id, event)

    def dispatch(self, ticket: Ticket, message_id: int, request: Message) -> Message:
        now = self.now_ms()
        room = self._member(ticket) if request.battle_id == ticket.battle_id else None
        if message_id == 140:
            return self._snapshot(room, ticket.player_id if ticket.role == _PARTICIPANT else 0) if room is not None else self.protocol.message("BattleStateS2C")
        responses = {149: "SubmitBattleActionResponse", 162: "SetAutoBattleResponse", 165: "StopWatchBattleResponse"}
        if message_id not in responses:
            raise BattleError("message_id_not_allowed")
        response = self.protocol.message(responses[message_id])
        if message_id == 165:
            if room is not None and ticket.role == _OBSERVER:
                self._drop_observer(room, ticket.player_id, notify=False)
            return response
        if room is None:
            return self._error(response, _INVALID)
        if ticket.role != _PARTICIPANT:
            return self._error(response, _INVALID)
        if room.proposed_outcome or room.deadline_ms <= now:
            return self._error(response, _UNAVAILABLE)
        engine = room.engine
        if now >= room.action_deadline_ms:
            self._resolve(room, now)
            return self._error(response, _INVALID)
        actor = engine.actors[ticket.player_id]
        if not _active(actor):
            return self._error(response, _INACTIVE)
        if message_id == 162:
            was_ready = not engine.pending_players()
            actor.is_auto = request.enabled
            if request.enabled and not was_ready and not engine.pending_players():
                self._resolve(room, now)
        else:
            error = engine.validate(actor, request.action)
            if error:
                return self._error(response, error)
            engine.pending[ticket.player_id] = self.protocol.message("BattleAction")
            engine.pending[ticket.player_id].CopyFrom(request.action)
            if not engine.pending_players():
                self._resolve(room, now)
        return response

    @staticmethod
    def _snapshot(room: _Room, viewer: int) -> Message:
        if room.proposed_outcome:
            return room.visible_before_terminal.get(viewer, room.visible_before_terminal[0])
        return room.engine.snapshot(room.action_deadline_ms, viewer)

    def _resolve(self, room: _Room, now: int) -> None:
        visible = self._terminal_visibility(room)
        turn = room.engine.resolve()
        if room.replay is not None:
            room.replay.add_round(turn)
        if room.engine.outcome:
            room.visible_before_terminal = visible
            room.final_turn = turn
            self._propose_outcome(room, room.engine.outcome, now)
            return
        interval = _ACTION_MS if room.engine.pending_players() else max(2000, 250 * len(turn.action_order))
        room.action_deadline_ms = now + interval
        self._broadcast_turn(room, turn)

    @staticmethod
    def _terminal_visibility(room: _Room) -> dict[int, Message]:
        return {viewer: room.engine.snapshot(room.action_deadline_ms, viewer) for viewer in (0, *room.routing)}

    def _broadcast_turn(self, room: _Room, original: Message) -> None:
        for player in tuple(room.connections):
            viewer = player if player in room.routing else 0
            turn = self.protocol.message("TurnResultS2C")
            turn.CopyFrom(original)
            turn.state.CopyFrom(room.engine.snapshot(room.action_deadline_ms, viewer))
            self._emit(room, player, 139 if viewer else 158, turn)
        if room.spectate.targets():
            turn = type(original)()
            turn.CopyFrom(original)
            turn.state.CopyFrom(room.engine.snapshot(room.action_deadline_ms))
            self._spectate_frame(room, TURN, turn.round_index, turn)

    def _propose_outcome(self, room: _Room, outcome: int, now: int) -> None:
        steady = self._steady_ms()
        if not room.proposed_outcome:
            room.proposed_outcome = outcome
            room.outcome_finished_ms = now
            room.outcome_since_ms = steady
            room.action_deadline_ms = 0
            for state in room.visible_before_terminal.values():
                state.action_deadline_ms = 0
            result = self._result(room)
            room.outcome_payload = result.SerializeToString(deterministic=True)
            if room.replay is not None:
                room.outcome_replay = room.replay.freeze(outcome, result.total_rounds, now)
                room.replay = None
        age = steady - room.outcome_since_ms
        room.outcome_retry_ms = steady + (5000 if age < 6 * 3_600_000 else 60_000)
        if self.on_outcome:
            self._publish(self.on_outcome, room.engine.battle_id, outcome)

    def _result(self, room: _Room) -> Message:
        engine = room.engine
        request = room.original
        rounds = engine.round_index if engine.outcome else engine.round_index - 1
        event = self.protocol.message(
            "contracts.kafka.BattleResultEvent", battle_id=engine.battle_id, match_mode=request.match_mode,
            battle_config_id=request.battle_config_id, outcome=room.proposed_outcome,
            winner_team_index=int(room.proposed_outcome == 2), total_rounds=rounds,
            finished_at_ms=room.outcome_finished_ms, settlement_format_version=1,
        )
        if request.activity_context.kind:
            event.activity_context.CopyFrom(request.activity_context)
        teams: dict[int, list[int]] = {}
        for player in sorted(room.routing):
            actor = engine.actors[player]
            teams.setdefault(actor.team_index, []).append(player)
            settlement = event.settlements.add(
                battle_id=engine.battle_id, player_id=player, outcome=room.proposed_outcome,
                player_team_index=actor.team_index, health=actor.attributes.health, mana=actor.attributes.mana,
                is_dead=actor.is_dead, fled=actor.fled, total_rounds=rounds,
                finished_at_ms=event.finished_at_ms, match_mode=request.match_mode,
                battle_config_id=request.battle_config_id,
            )
            if actor.fled:
                event.fled_player_ids.append(player)
            if actor.is_dead:
                event.dead_player_ids.append(player)
            for item_id, count in engine.items_consumed.get(player, {}).items():
                settlement.items_consumed.add(item_table_id=item_id, count=count)
            for pet in engine.actors.values():
                if pet.actor_type == _PET and pet.owner_player_id == player:
                    settlement.pets.add(pet_id=pet.pet_id, health=pet.attributes.health,
                                        mana=pet.attributes.mana, is_dead=pet.is_dead)
            if engine.pve and engine.outcome == 1 and actor.team_index == 0 and _active(actor):
                for monster_id in engine.defeated_monsters:
                    settlement.defeated_monsters.add(monster_config_id=monster_id, count=1)
                if self.tables:
                    self.tables.add_rewards(request, settlement, profile=room.reward_profile)
        for side, players in sorted(teams.items()):
            event.teams.add(team_index=side, player_ids=players)
        return event

    def outcome_request(self, battle_id: int) -> Message | None:
        room = self.rooms.get(battle_id)
        if room is None or not 0 < len(room.outcome_payload) <= _MAX_OUTCOME_PAYLOAD:
            return None
        request = self.protocol.message(
            "match.CommitBattleOutcomeRequest", battle_id=battle_id,
            placement_operation_id=room.original.placement_operation_id,
            target_instance_id=self.instance_id, raw_result_payload=room.outcome_payload,
        )
        if room.outcome_replay is not None:
            request.replay.CopyFrom(room.outcome_replay)
        return request

    def pending_outcome_ids(self) -> tuple[int, ...]:
        return tuple(battle_id for battle_id, room in self.rooms.items()
                     if 0 < len(room.outcome_payload) <= _MAX_OUTCOME_PAYLOAD)

    def decide_outcome(self, battle_id: int, *, committed: bool) -> bool:
        """Apply the terminal decision from the outcome authority once."""
        room = self.rooms.get(battle_id)
        if room is None or not room.proposed_outcome:
            return False
        room.action_deadline_ms = 0
        if committed and room.final_turn is not None:
            self._broadcast_turn(room, room.final_turn)
        reason = 1 if committed and room.final_turn is not None else 2
        end = self.protocol.message("SpectateEndS2C", battle_id=battle_id,
                                    outcome=room.proposed_outcome if committed else 0, reason=reason)
        self._spectate_frame(room, END, room.engine.round_index, end)
        for player, connection in tuple(room.connections.items()):
            if player in room.observers:
                self._emit(room, player, 166, end)
            self._publish(connection.close)
        del self.rooms[battle_id]
        if self.on_rooms_changed:
            self._publish(self.on_rooms_changed, len(self.rooms))
        return True

    def tick(self) -> None:
        if self._draining:
            return
        now = self.now_ms()
        steady = self._steady_ms()
        for battle_id, receipt in tuple(self.receipts.items()):
            if receipt.expires_ms <= now and battle_id not in self.rooms:
                del self.receipts[battle_id]
        for key, receipt in tuple(self.observer_receipts.items()):
            if receipt.expires_ms <= now:
                del self.observer_receipts[key]
        for room in tuple(self.rooms.values()):
            for player, observer in tuple(room.observers.items()):
                if 0 < observer.expires_ms <= steady:
                    self._drop_observer(room, player)
            if room.proposed_outcome:
                if steady - room.outcome_since_ms >= 30 * 3_600_000:
                    for connection in room.connections.values():
                        self._publish(connection.close)
                    del self.rooms[room.engine.battle_id]
                    if self.on_rooms_changed:
                        self._publish(self.on_rooms_changed, len(self.rooms))
                    _LOG.error("battle outcome still unconfirmed after replay window: %s", room.engine.battle_id)
                elif room.outcome_retry_ms <= steady:
                    self._propose_outcome(room, room.proposed_outcome, now)
            elif room.deadline_ms <= now:
                room.visible_before_terminal = self._terminal_visibility(room)
                if room.replay is not None:
                    room.replay.mark_deadline_draw(room.visible_before_terminal[0])
                self._propose_outcome(room, 3, now)
            elif room.action_deadline_ms <= now:
                self._resolve(room, now)
            elif self.on_confirm and room.confirm_retry_ms <= now < room.confirm_until_ms:
                room.confirm_retry_ms = now + 10_000
                for player, route in room.routing.items():
                    self._publish(self.on_confirm, route, player, room.engine.battle_id, room.deadline_ms)
        self.pump_spectate_keepalive()

    def pump_spectate_keepalive(self) -> None:
        steady, now = self._steady_ms(), self.now_ms()
        for room in tuple(self.rooms.values()):
            if room.spectate.due(steady, now, room.deadline_ms):
                self._spectate_state(room)

    def stop_spectate_keepalive(self) -> None:
        for room in self.rooms.values():
            room.spectate.last_frame_steady_ms = None

    def begin_shutdown(self) -> None:
        if self._draining:
            return
        self._draining = True
        self.admitting = False
        for room in self.rooms.values():
            end = self.protocol.message("SpectateEndS2C", battle_id=room.engine.battle_id, reason=4)
            if not room.proposed_outcome:
                self._spectate_frame(room, END, room.engine.round_index, end)
            for player, connection in tuple(room.connections.items()):
                if player in room.observers and not room.proposed_outcome:
                    self._emit(room, player, 166, end)
                self._publish(connection.close)
            room.connections.clear()

    def close(self) -> None:
        self.begin_shutdown()

    @staticmethod
    def _emit(room: _Room, player: int, message_id: int, message: Message) -> None:
        connection = room.connections.get(player)
        if connection is not None:
            BattleNode._publish(connection.emit, message_id, message)

    @staticmethod
    def _publish(callback: Callable, *args: object) -> None:
        try:
            callback(*args)
        except Exception:
            _LOG.exception("battle output callback failed")
