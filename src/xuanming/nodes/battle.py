"""Authoritative in-memory battle rooms and their direct client contract."""

from __future__ import annotations

import hmac
import heapq
import logging
import os
import re
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from math import ceil, isfinite
from typing import TYPE_CHECKING

from google.protobuf.message import DecodeError, Message

from .battle_dropin_effects import (
    BOSS_TRIAL_ACTION_TIMEOUT_MS,
    BOSS_TRIAL_KIND_FESTIVAL_BEAST,
    BOSS_TRIAL_KIND_GUARDIAN_RESCUE,
    BOSS_TRIAL_KIND_NONE,
    BOSS_TRIAL_KIND_POLARITY_5V5,
    ERR_SURRENDER_ALREADY_VOTED,
    ERR_SURRENDER_COOLDOWN,
    ERR_SURRENDER_NOT_ALLOWED,
    ERR_SURRENDER_TOO_EARLY,
    MATCH_MODE_POLARITY_5V5,
    MATCH_MODE_PVE_SOLO,
    MATCH_MODE_SEASON_TOURNAMENT,
    POLARITY_DEFAULT_MAX_ROUNDS,
    PVP_ESCALATION_DAMAGE_STEP_PCT,
    PVP_ESCALATION_LEAD_ROUNDS,
    PVP_ESCALATION_MIN_MAX_ROUNDS,
    PVP_ESCALATION_SUPPORT_FLOOR_PCT,
    PVP_ESCALATION_SUPPORT_STEP_PCT,
    PVP_PRIMARY_FOCUS_DEN,
    PVP_PRIMARY_FOCUS_NUM,
    PVP_SURRENDER_EXPIRE_COOLDOWN_ROUNDS,
    PVP_SURRENDER_IDLE_EXCLUDE_ROUNDS,
    PVP_SURRENDER_REJECT_COOLDOWN_ROUNDS,
    PVP_SURRENDER_UNLOCK_ROUND,
    RANKED_DEFAULT_MAX_ROUNDS,
    TACTICAL_PREP_DURATION_SECONDS,
    TACTICAL_PREP_TIMEOUT_MS,
    BattleModifier,
    BattleStateKind,
    BattleTrigger,
    ConfiguredAbilitiesController,
    DungeonMonsterAiProfile,
    EncounterV2Controller,
    EquipmentBattleEffects,
    MonsterAiRuntime,
    PetAndSupportEffects,
    SeasonBattleRules,
    TrialContext,
    TrialFactTracker,
    choose_monster_action,
    is_boss_trial_kind,
)
from .battle_effects import FREEZE, STUN, U64_MAX, BattleEffects, landed_damage, landed_heal, rank_factor, scaled
from .battle_replay import BattleReplayRecorder
from .battle_script import BattleScriptRuntime
from .battle_skills import BattleSkills, active as _active, base_stat, mana_cost
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
                   162: "SetAutoBattleRequest", 165: "StopWatchBattleRequest",
                   479: "SurrenderBattleRequest", 504: "UpdateBattlePrepRequest"}


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


@dataclass(slots=True)
class _PrepPlayerOptions:
    """论剑 V3 战前 15 秒明牌变阵单人可选配置与锁定态（对照 C++ `PrepPlayerOptions`）。"""

    team_index: int
    active_season_id: int
    ai_controlled: bool
    selected_seal_id: int = 0
    selected_pet_id: int = 0
    ready: bool = False
    available_seals: list[Message] = field(default_factory=list)
    available_pets: list[Message] = field(default_factory=list)


def _uuid(value: str) -> bool:
    return _UUID_PATTERN.fullmatch(value) is not None and int(value.replace("-", ""), 16) != 0


class TurnEngine:
    """Table-driven turn state owned by one event loop, independent of network I/O."""

    def __init__(
        self, protocol: Protocol, request: Message, *, monsters: Iterable[Message] = (),
        skills: Mapping[int, BattleSkill], items: Mapping[int, BattleItem], max_rounds: int,
        buffs: Mapping[int, BattleBuff] | None = None, silence_permissions: tuple[int, ...] | None = None,
        monster_ids: set[int] | None = None,
        monster_ai_profiles: Mapping[int, DungeonMonsterAiProfile] | None = None,
        trial_context: Message | TrialContext | None = None,
        boss_trial_kind: int = BOSS_TRIAL_KIND_NONE,
        battle_difficulty: int = 0,
        has_explicit_round_limit: bool = False,
        ranked_context: Message | None = None,
    ):
        if not request.battle_id or not request.players:
            raise BattleError("battle needs an identity and players")
        self.protocol = protocol
        self.create_request = request
        if ranked_context is not None:
            self.ranked_context: Message | None = ranked_context
        elif "ranked_context" in request.DESCRIPTOR.fields_by_name and request.HasField("ranked_context"):
            self.ranked_context = request.ranked_context
        else:
            self.ranked_context = None
        self.battle_id = request.battle_id
        self.match_mode = request.match_mode
        self.pve = request.match_mode in _PVE_MODES
        self.rng = _Mt19937_64(request.seed)
        self.item_rules = items

        # 排位默认回合上限收敛到 20，物法易势 5V5 默认 16，瑞兽迎春硬性校验并取冻结 round_limit。
        if self.is_ranked_battle and not has_explicit_round_limit:
            max_rounds = min(max_rounds, RANKED_DEFAULT_MAX_ROUNDS)
        if request.match_mode == MATCH_MODE_POLARITY_5V5 and boss_trial_kind == BOSS_TRIAL_KIND_NONE:
            boss_trial_kind = BOSS_TRIAL_KIND_POLARITY_5V5
            if max_rounds == _MAX_ROUNDS:
                max_rounds = POLARITY_DEFAULT_MAX_ROUNDS

        has_festival_activity = (
            request.HasField("activity_context") and request.activity_context.kind == 2
        )
        if boss_trial_kind == BOSS_TRIAL_KIND_FESTIVAL_BEAST or has_festival_activity:
            if (
                boss_trial_kind != BOSS_TRIAL_KIND_FESTIVAL_BEAST
                or not has_festival_activity
                or request.match_mode != MATCH_MODE_PVE_SOLO
                or len(request.players) != 1
                or len(request.players[0].pets) != 1
                or not request.activity_context.HasField("festival_beast")
            ):
                raise BattleError("invalid festival beast battle admission")
            fb = request.activity_context.festival_beast
            if (
                fb.activity_instance_id == 0
                or not fb.reward_identity
                or fb.required_wins == 0
                or fb.locked_pet_id == 0
                or request.players[0].pets[0].pet_id != fb.locked_pet_id
                or fb.boss_health == 0
                or fb.round_limit == 0
                or fb.cycle_rounds == 0
                or fb.first_heavy_round == 0
                or fb.first_heavy_round > fb.cycle_rounds
                or fb.telegraph_lead_rounds == 0
                or fb.telegraph_lead_rounds >= fb.first_heavy_round
                or fb.heavy_damage_percent == 0
            ):
                raise BattleError("invalid festival beast frozen context")
            max_rounds = fb.round_limit

        self.max_rounds = max_rounds
        self.round_index = 1
        self.outcome = 0
        self.surrendered_team: int | None = None
        self.team_surrender_votes: list[Message] = [
            protocol.message("BattleSurrenderVoteState"),
            protocol.message("BattleSurrenderVoteState"),
        ]
        self.consecutive_idle_rounds_by_player: dict[int, int] = {}
        self.actors: dict[int, Message] = {}
        self.items: dict[int, list[Message]] = {}
        self.pending: dict[int, Message] = {}
        self.items_used: Counter[int] = Counter()
        self.items_consumed: dict[int, Counter[int]] = {}
        self.last_action_order: list[int] = []
        self.defeated_monsters: list[int] = []
        self.effects = BattleEffects(self, buffs or {})
        self.skills = BattleSkills(self, skills, silence_permissions)
        self.configured = ConfiguredAbilitiesController(self)
        self.equipment_effects = EquipmentBattleEffects(self)
        self.pet_support = PetAndSupportEffects(self)
        self.season_rules = SeasonBattleRules
        effective_trial_ctx = trial_context
        if effective_trial_ctx is None and hasattr(request, "trial_context") and request.HasField("trial_context"):
            effective_trial_ctx = request.trial_context
        self.trial_tracker = TrialFactTracker(self, effective_trial_ctx)
        self.monster_ai = MonsterAiRuntime(self, monster_ai_profiles)
        self.encounter = EncounterV2Controller(
            self, request, boss_trial_kind=boss_trial_kind, battle_difficulty=battle_difficulty,
        )
        self.round_start_speeds: dict[int, int] = {}
        self.ai_controlled_players: set[int] = set()
        self.prep_active: bool = False
        self._next_pet_actor_index: int = 0
        self.prep_options_by_player: dict[int, _PrepPlayerOptions] = {}
        self._group = 0
        self._team_slots = [0, 0]
        slots = self._team_slots
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
            if bool(getattr(player, "ai_controlled", False)):
                actor.is_auto = True
                self.ai_controlled_players.add(actor_id)
            if player.HasField("combat"):
                actor.combat.CopyFrom(player.combat)
            actor.buffs.extend(player.buffs)
            self.skills.initialize(actor, player)
            if player.HasField("equipment_effects"):
                if not self.equipment_effects.initialize_equipment(actor, player.equipment_effects):
                    raise BattleError("invalid equipment effects snapshot")
            self.items[actor_id] = []
            for item in player.items:
                if item.count:
                    entry = type(item)()
                    entry.CopyFrom(item)
                    self.items[actor_id].append(entry)

        pet_ids: set[int] = set()
        for player in request.players:
            for pet in player.pets:
                if pet.pet_id and pet.pet_id in pet_ids:
                    raise BattleError("invalid pet snapshot")
                actor = self._build_pet_actor(
                    player, pet, _PET_ACTOR_BASE + self._next_pet_actor_index, slots[player.team_index],
                )
                slots[player.team_index] += 1
                self._next_pet_actor_index += 1
                self.actors[actor.actor_id] = actor
                if pet.pet_id:
                    pet_ids.add(pet.pet_id)
        self._init_tactical_prep(request)
        monster_list = list(monsters)
        if self.encounter.is_festival_beast:
            if len(monster_list) != 1:
                raise BattleError("festival beast battle requires exactly 1 boss monster")
        for index, monster in enumerate(monster_list):
            actor = self.protocol.message("BattleActorState")
            actor.CopyFrom(monster)
            actor.actor_id = _MONSTER_ACTOR_BASE + index
            actor.actor_type = _MONSTER
            actor.team_index = 1
            actor.formation_slot = slots[1]
            slots[1] += 1
            if self.encounter.is_festival_beast:
                fb = request.activity_context.festival_beast
                actor.is_boss = True
                if fb.boss_name:
                    actor.name = fb.boss_name
                actor.max_health = fb.boss_health
                actor.attributes.health = fb.boss_health
                actor.attributes.strength = 0
                actor.physical_attack = fb.boss_physical_attack
                actor.magic_attack = 0
                actor.attributes.armor = 0
                actor.defense = fb.boss_defense
                actor.attributes.resistance = fb.boss_resistance
                actor.attributes.speed = fb.boss_speed
                actor.attributes.critchance = 0
                actor.ClearField("combat")
                actor.ClearField("equipment_effects")
                actor.ClearField("pet_cultivation")
                actor.ClearField("skill_table_ids")
                actor.ClearField("runtime_skills")
                actor.capture_pet_table_id = 0
            self.actors[actor.actor_id] = actor
        if {actor.team_index for actor in self.actors.values()} != {0, 1}:
            raise BattleError("both sides need a unit")
        self.effects.sanitize()
        self.scripts = BattleScriptRuntime(self, request, monster_ids)
        self.monster_ai.bind_actors()
        # 战前 15 秒明牌变阵期间延迟到 finalize_prep_phase 锁阵后再初始化开场光环/被动，防止替换首发宠残留旧光环。
        if not self.prep_active:
            self.configured.initialize_abilities()
        self.trial_tracker.initialize(request)
        self.encounter.init_special_encounter_state()

    @property
    def is_in_prep_phase(self) -> bool:
        """当前是否处于论剑 V3 战前 15 秒明牌变阵期。"""
        return self.prep_active

    def _build_pet_actor(
        self, owner_snapshot: Message, pet: Message, actor_id: int, formation_slot: int,
    ) -> Message:
        """按 C++ `BuildPetActor` 校验并构造单个灵宠战斗单位（供开局与变阵换宠复用）。"""
        allow_temp_pet = self.uses_free_preset
        is_temp_pet = bool(getattr(pet, "competitive_temporary", False))
        if is_temp_pet and not allow_temp_pet:
            raise BattleError("competitive_temporary pet rejected when use_free_preset is false")
        if not pet.pet_id and not (is_temp_pet and allow_temp_pet):
            raise BattleError("invalid pet snapshot")
        if pet.owner_player_id not in (0, owner_snapshot.player_id):
            raise BattleError("invalid pet snapshot")
        if pet.HasField("pet_cultivation") and not PetAndSupportEffects.valid_pet_cultivation(pet.pet_cultivation):
            raise BattleError("invalid pet cultivation snapshot")

        attributes = pet.base_attributes
        actor = self.protocol.message(
            "BattleActorState",
            actor_id=actor_id,
            actor_type=_PET,
            team_index=owner_snapshot.team_index,
            level=pet.level,
            attributes=attributes,
            max_health=pet.max_health or attributes.health,
            max_mana=pet.max_mana or attributes.mana,
            physical_attack=pet.physical_attack,
            magic_attack=pet.magic_attack,
            defense=pet.defense,
            formation_slot=formation_slot,
            owner_player_id=owner_snapshot.player_id,
            pet_table_id=pet.pet_table_id,
            pet_id=pet.pet_id,
            competitive_temporary=is_temp_pet,
            is_auto=True,
        )
        actor.name = pet.pet_name
        self.skills.initialize(actor, pet, pet=True)
        if pet.HasField("pet_cultivation"):
            self.pet_support.initialize_pet_cultivation(actor, pet.pet_cultivation)
        return actor

    def _init_tactical_prep(self, request: Message) -> None:
        """初始化论剑 V3 战前 15 秒明牌变阵配置（对照 C++ `InitTacticalPrep`）。"""
        self.prep_active = False
        self.prep_options_by_player.clear()

        # 仅正式自养论剑与赛季战队锦标赛开启战前 15 秒明牌变阵；PVE / 免费预设娱乐场 / 切磋 / 帮战零打扰。
        mode_eligible = (
            not self.pve
            and not self.uses_free_preset
            and (self.is_ranked_battle or request.match_mode == MATCH_MODE_SEASON_TOURNAMENT)
        )
        if not mode_eligible:
            return

        all_battle_pet_ids = {
            actor.pet_id
            for actor in self.actors.values()
            if actor.actor_type == _PET and actor.pet_id != 0
        }
        has_any_choice = False
        for snapshot in request.players:
            ai_ctrl = bool(getattr(snapshot, "ai_controlled", False))
            opt = _PrepPlayerOptions(
                team_index=snapshot.team_index,
                active_season_id=snapshot.active_season_id,
                ai_controlled=ai_ctrl,
                ready=ai_ctrl,
            )
            seen_seal_ids: set[int] = set()
            if snapshot.HasField("equipment_effects") and snapshot.equipment_effects.HasField("season_seal"):
                equipped_seal = snapshot.equipment_effects.season_seal
                opt.selected_seal_id = equipped_seal.seal_id
                if equipped_seal.seal_id not in seen_seal_ids:
                    seen_seal_ids.add(equipped_seal.seal_id)
                    opt.available_seals.append(equipped_seal)
            for seal in getattr(snapshot, "available_season_seals", ()):
                if not SeasonBattleRules.valid_seal_effect(
                    seal.season_id,
                    seal.seal_id,
                    seal.effect_id,
                    seal.bonus_bps,
                    seal.cap_bps,
                    seal.per_round_limit,
                    seal.battle_limit,
                    seal.active_rounds,
                    seal.health_threshold_bps,
                ):
                    raise BattleError("invalid available_season_seal snapshot")
                if snapshot.active_season_id != 0 and seal.season_id != snapshot.active_season_id:
                    raise BattleError("available_season_seal season mismatch")
                if seal.seal_id not in seen_seal_ids:
                    seen_seal_ids.add(seal.seal_id)
                    opt.available_seals.append(seal)
            opt.available_seals.sort(key=lambda s: s.seal_id)

            if snapshot.pets:
                opt.available_pets.append(snapshot.pets[0])
                opt.selected_pet_id = snapshot.pets[0].pet_id
            for standby in getattr(snapshot, "standby_pets", ()):
                if bool(getattr(standby, "competitive_temporary", False)) or standby.pet_id == 0:
                    raise BattleError("invalid standby pet id or temporary flag")
                if standby.owner_player_id not in (0, snapshot.player_id):
                    raise BattleError("standby pet ownership mismatch")
                if standby.HasField("pet_cultivation") and not PetAndSupportEffects.valid_pet_cultivation(standby.pet_cultivation):
                    raise BattleError("invalid standby pet_cultivation")
                if standby.pet_id == opt.selected_pet_id:
                    continue
                if standby.pet_id in all_battle_pet_ids:
                    raise BattleError("duplicate standby pet_id across battle")
                all_battle_pet_ids.add(standby.pet_id)
                opt.available_pets.append(standby)

            if not opt.ai_controlled and (
                len(opt.available_seals) > 1
                or len(opt.available_pets) > 1
                or (opt.selected_pet_id == 0 and len(opt.available_pets) > 0)
            ):
                has_any_choice = True
            self.prep_options_by_player[snapshot.player_id] = opt

        if has_any_choice:
            self.prep_active = True
        else:
            self.prep_options_by_player.clear()

    def all_prep_players_ready(self) -> bool:
        """判定全部非 AI 参战者是否已完成变阵锁定。"""
        if not self.prep_active:
            return True
        return all(opt.ready for opt in self.prep_options_by_player.values() if not opt.ai_controlled)

    def update_battle_prep(
        self,
        player_id: int,
        selected_seal_id: int,
        selected_pet_id: int,
        ready: bool,
        *,
        deadline_ms: int = 0,
    ) -> tuple[int, Message | None]:
        """处理单个参战者的战前变阵提交（换赛季印/换首发宠/锁定），返回 `(tip_error, viewer_prep_state)`。"""
        if self.outcome != 0 or not self.prep_active:
            return _FEATURE, None
        opt = self.prep_options_by_player.get(player_id)
        if opt is None or opt.ai_controlled:
            return _INVALID, None

        if opt.ready:
            seal_changed = selected_seal_id != 0 and selected_seal_id != opt.selected_seal_id
            pet_changed = selected_pet_id != 0 and selected_pet_id != opt.selected_pet_id
            if seal_changed or pet_changed:
                return _FEATURE, None
            return 0, self.build_prep_state_for_viewer(player_id, deadline_ms)

        if selected_seal_id != 0 and not any(s.seal_id == selected_seal_id for s in opt.available_seals):
            return _INVALID, None
        if selected_pet_id != 0 and not any(p.pet_id == selected_pet_id for p in opt.available_pets):
            return _INVALID, None

        if selected_seal_id != 0:
            opt.selected_seal_id = selected_seal_id
        if selected_pet_id != 0:
            opt.selected_pet_id = selected_pet_id
        if ready:
            opt.ready = True
        return 0, self.build_prep_state_for_viewer(player_id, deadline_ms)

    def finalize_prep_phase(self) -> bool:
        """锁定战前变阵：应用赛季印、原地替换或补发首发宠，并一次性初始化全部开场光环/被动。"""
        if not self.prep_active:
            return False

        snap_by_player = {snap.player_id: snap for snap in self.create_request.players}
        for player_id, opt in sorted(self.prep_options_by_player.items()):
            # 1. 仅替换单槽赛季印机制（不重算基础气血/防御万分比）。
            if opt.selected_seal_id != 0:
                actor = self.actors.get(player_id)
                if actor is not None:
                    for seal in opt.available_seals:
                        if seal.seal_id == opt.selected_seal_id:
                            if (
                                not actor.equipment_effects.HasField("season_seal")
                                or actor.equipment_effects.season_seal.seal_id != opt.selected_seal_id
                            ):
                                actor.equipment_effects.season_seal.CopyFrom(seal)
                                actor.ClearField("season_seal_state")
                            break

            # 2. 替换或补发首发灵宠（原地保留局内 actor_id 与阵位）。
            if opt.selected_pet_id != 0:
                chosen_pet = next((p for p in opt.available_pets if p.pet_id == opt.selected_pet_id), None)
                owner_snap = snap_by_player.get(player_id)
                if chosen_pet is not None and owner_snap is not None:
                    existing_pet = next(
                        (a for a in self.actors.values() if a.actor_type == _PET and a.owner_player_id == player_id),
                        None,
                    )
                    if existing_pet is not None:
                        if existing_pet.pet_id != chosen_pet.pet_id:
                            rebuilt = self._build_pet_actor(
                                owner_snap, chosen_pet, existing_pet.actor_id, existing_pet.formation_slot,
                            )
                            self.actors[existing_pet.actor_id] = rebuilt
                    else:
                        new_actor_id = _PET_ACTOR_BASE + self._next_pet_actor_index
                        self._next_pet_actor_index += 1
                        slot = self._team_slots[opt.team_index]
                        self._team_slots[opt.team_index] += 1
                        added = self._build_pet_actor(owner_snap, chosen_pet, new_actor_id, slot)
                        self.actors[new_actor_id] = added

        self.prep_active = False
        self.configured.initialize_abilities()
        return True

    def build_prep_state_for_viewer(self, viewer_player_id: int, deadline_ms: int = 0) -> Message:
        """按收信视角脱敏构建 `BattlePrepState`（本人看全部可选+同队态，观众仅看倒计时）。"""
        out = self.protocol.message("BattlePrepState")
        if not self.prep_active:
            return out
        out.active = True
        out.deadline_ms = deadline_ms
        out.duration_seconds = TACTICAL_PREP_DURATION_SECONDS
        if not viewer_player_id:
            return out
        self_opt = self.prep_options_by_player.get(viewer_player_id)
        if self_opt is None:
            return out
        for seal in self_opt.available_seals:
            out.self_available_seals.add(seal_id=seal.seal_id, effect_id=seal.effect_id, name=seal.name)
        for pet in self_opt.available_pets:
            dst = out.self_available_pets.add(
                pet_id=pet.pet_id,
                pet_table_id=pet.pet_table_id,
                pet_name=pet.pet_name,
                level=pet.level,
                max_health=pet.max_health or pet.base_attributes.health,
                speed=pet.base_attributes.speed,
                physical_attack=pet.physical_attack,
                magic_attack=pet.magic_attack,
                defense=pet.defense,
            )
            dst.skill_table_ids.extend(pet.skill_table_ids)
        for member_id, opt in sorted(self.prep_options_by_player.items()):
            if opt.team_index != self_opt.team_index:
                continue
            ps = out.team_states.add(
                player_id=member_id,
                team_index=opt.team_index,
                selected_seal_id=opt.selected_seal_id,
                selected_pet_id=opt.selected_pet_id,
                ready=opt.ready,
            )
            if opt.selected_pet_id != 0:
                chosen = next((p for p in opt.available_pets if p.pet_id == opt.selected_pet_id), None)
                if chosen is not None:
                    ps.selected_pet_table_id = chosen.pet_table_id
                    ps.selected_pet_name = chosen.pet_name
        return out

    def fill_prep_state_for_viewer(self, state: Message, viewer_player_id: int, deadline_ms: int = 0) -> None:
        state.ClearField("prep_state")
        if not self.prep_active:
            return
        state.prep_state.CopyFrom(self.build_prep_state_for_viewer(viewer_player_id, deadline_ms))

    @property
    def is_ranked_battle(self) -> bool:
        return self.ranked_context is not None and self.ranked_context.season_id != 0

    @property
    def uses_free_preset(self) -> bool:
        return self.ranked_context is not None and bool(self.ranked_context.use_free_preset)

    @property
    def is_competitive_pvp_battle(self) -> bool:
        return not self.pve and (
            self.is_ranked_battle
            or self.encounter.boss_trial_kind == BOSS_TRIAL_KIND_POLARITY_5V5
            or self.match_mode in (MATCH_MODE_POLARITY_5V5, MATCH_MODE_SEASON_TOURNAMENT)
        )

    @property
    def is_boss_trial_battle(self) -> bool:
        return self.encounter.is_boss_trial

    @property
    def is_festival_beast_battle(self) -> bool:
        return self.encounter.is_festival_beast

    @property
    def escalation_start_round(self) -> int:
        if self.is_competitive_pvp_battle and self.max_rounds >= PVP_ESCALATION_MIN_MAX_ROUNDS:
            return self.max_rounds - PVP_ESCALATION_LEAD_ROUNDS
        return 0

    @property
    def escalation_stacks(self) -> int:
        start = self.escalation_start_round
        return (self.round_index - start + 1) if (start > 0 and self.round_index >= start) else 0

    @property
    def escalation_damage_pct(self) -> int:
        return 100 + self.escalation_stacks * PVP_ESCALATION_DAMAGE_STEP_PCT

    @property
    def escalation_support_pct(self) -> int:
        drop = self.escalation_stacks * PVP_ESCALATION_SUPPORT_STEP_PCT
        max_drop = 100 - PVP_ESCALATION_SUPPORT_FLOOR_PCT
        return PVP_ESCALATION_SUPPORT_FLOOR_PCT if drop >= max_drop else (100 - drop)

    @property
    def surrender_unlock_round(self) -> int:
        return 0 if self.pve else PVP_SURRENDER_UNLOCK_ROUND

    def team_index_of_player(self, player_id: int) -> int | None:
        actor = self.actors.get(player_id)
        if actor is None or actor.actor_type != _PLAYER or actor.team_index > 1:
            return None
        return actor.team_index

    def completed_rounds(self) -> int:
        if self.outcome == 0 or self.surrendered_team is not None:
            return max(0, self.round_index - 1)
        return self.round_index

    def _expire_surrender_votes_for_round_advance(self) -> None:
        for vote in self.team_surrender_votes:
            if not vote.active:
                continue
            vote.active = False
            vote.rejected = True
            vote.passed = False
            vote.ClearField("agreed_player_ids")
            vote.next_allowed_round = self.round_index + PVP_SURRENDER_EXPIRE_COOLDOWN_ROUNDS

    def submit_surrender_vote(self, player_id: int, agree: bool) -> tuple[int, Message | None]:
        """PVP 第 5 回合起投降 / 同队投降投票表态；返回 `(tip_error, vote_state)`。"""
        if self.outcome != 0:
            return _INVALID, None
        if self.prep_active:
            return _FEATURE, None
        if self.pve:
            return ERR_SURRENDER_NOT_ALLOWED, None
        actor = self.actors.get(player_id)
        if actor is None or actor.actor_type != _PLAYER or actor.team_index > 1:
            return _INVALID, None
        if self.round_index < PVP_SURRENDER_UNLOCK_ROUND:
            return ERR_SURRENDER_TOO_EARLY, None

        team_idx = actor.team_index
        vote = self.team_surrender_votes[team_idx]
        if not vote.active:
            if not agree:
                return _INVALID, None
            if vote.next_allowed_round > 0 and self.round_index < vote.next_allowed_round:
                return ERR_SURRENDER_COOLDOWN, None
            active_voters = 0
            for member in self.actors.values():
                if member.actor_type != _PLAYER or member.team_index != team_idx:
                    continue
                idle_rounds = self.consecutive_idle_rounds_by_player.get(member.actor_id, 0)
                if member.actor_id == player_id or idle_rounds < PVP_SURRENDER_IDLE_EXCLUDE_ROUNDS:
                    active_voters += 1
            vote.Clear()
            vote.active = True
            vote.team_index = team_idx
            vote.initiator_player_id = player_id
            vote.initiated_round = self.round_index
            vote.agreed_player_ids.append(player_id)
            vote.required_agrees = max(1, active_voters)
        else:
            if player_id in vote.agreed_player_ids:
                return ERR_SURRENDER_ALREADY_VOTED, None
            if not agree:
                vote.active = False
                vote.rejected = True
                vote.passed = False
                vote.ClearField("agreed_player_ids")
                vote.next_allowed_round = self.round_index + PVP_SURRENDER_REJECT_COOLDOWN_ROUNDS
                out = self.protocol.message("BattleSurrenderVoteState")
                out.CopyFrom(vote)
                return 0, out
            agreed = sorted((*vote.agreed_player_ids, player_id))
            vote.ClearField("agreed_player_ids")
            vote.agreed_player_ids.extend(agreed)

        if vote.active and len(vote.agreed_player_ids) >= vote.required_agrees:
            vote.active = False
            vote.passed = True
            vote.rejected = False
            self.surrendered_team = team_idx
            self.outcome = 2 if team_idx == 0 else 1
            self.encounter.active_telegraphs.clear()

        out = self.protocol.message("BattleSurrenderVoteState")
        out.CopyFrom(vote)
        return 0, out

    def fill_surrender_vote_for_viewer(self, state: Message, viewer_player_id: int) -> None:
        state.ClearField("surrender_vote")
        team = self.team_index_of_player(viewer_player_id)
        if team is None:
            return
        vote = self.team_surrender_votes[team]
        if (
            vote.active
            or vote.passed
            or vote.rejected
            or (vote.next_allowed_round > 0 and self.round_index < vote.next_allowed_round)
        ):
            state.surrender_vote.CopyFrom(vote)

    def set_monster_ai_profiles(self, profiles: Mapping[int, DungeonMonsterAiProfile]) -> None:
        """为本场战斗动态挂载副本怪物战术 AI 档案。"""
        self.monster_ai.set_profiles(profiles)

    def support_summary(self, actor_id: int) -> Message | None:
        """生成指定单位当前回合的辅助效果只读投影（治疗/护盾）。"""
        actor = self.actors.get(actor_id)
        return self.pet_support.build_support_summary(actor, self.round_index) if actor is not None else None

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
        if self.outcome or self.prep_active:
            return []
        return [actor.actor_id for actor in self.actors.values()
                if actor.actor_type == _PLAYER and _active(actor) and not actor.is_auto
                and actor.actor_id not in self.pending]

    def snapshot(
        self, deadline_ms: int, viewer: int = 0, *, unredacted: bool = False, prep_deadline_ms: int = 0,
    ) -> Message:
        state = self.protocol.message(
            "BattleStateS2C", battle_id=self.battle_id, round_index=self.round_index,
            action_deadline_ms=deadline_ms, outcome=self.outcome, pending_actor_ids=self.pending_players(),
            round_limit=self.max_rounds,
        )
        state.actors.extend(self.actors.values())
        self.encounter.fill_state_snapshot(state)
        esc_start = self.escalation_start_round
        if esc_start > 0:
            state.escalation_start_round = esc_start
            state.escalation_stacks = self.escalation_stacks
        surrender_unlock = self.surrender_unlock_round
        if surrender_unlock > 0:
            state.surrender_unlock_round = surrender_unlock
        if unredacted:
            if self.prep_active:
                state.prep_state.active = True
                state.prep_state.duration_seconds = TACTICAL_PREP_DURATION_SECONDS
                if prep_deadline_ms:
                    state.prep_state.deadline_ms = prep_deadline_ms
        else:
            self.personalize(state, viewer, prep_deadline_ms=prep_deadline_ms)
        return state

    def personalize(self, state: Message, viewer: int, *, prep_deadline_ms: int = 0) -> None:
        for actor in state.actors:
            actor.ClearField("combat")
            if actor.HasField("season_seal_state"):
                actor.ClearField("season_seal_state")
            if not viewer or (actor.actor_id != viewer and actor.owner_player_id != viewer):
                actor.ClearField("skill_cooldown_rounds")
            if actor.HasField("support_effects") and (not viewer or actor.actor_id != viewer):
                actor.ClearField("support_effects")
        state.ClearField("self_items")
        state.ClearField("surrender_vote")
        state.ClearField("prep_state")
        for item in sorted(self.items.get(viewer, ()), key=lambda entry: entry.item_table_id):
            if item.count:
                state.self_items.add().CopyFrom(item)
        if viewer:
            self.fill_surrender_vote_for_viewer(state, viewer)
        if self.prep_active:
            self.fill_prep_state_for_viewer(state, viewer, prep_deadline_ms)

    def validate(self, actor: Message, action: Message) -> int:
        if self.prep_active:
            return _FEATURE
        if not _active(actor):
            return _INACTIVE
        if (
            self.effects.has(actor, STUN, FREEZE)
            or self.configured.has_state(actor, BattleStateKind.FREEZE)
            or self.configured.has_state(actor, BattleStateKind.SLEEP)
        ):
            return 7006
        action_type = action.action_type
        if action_type == _ATTACK:
            if (
                self.configured.has_state(actor, BattleStateKind.SEAL_PHYSICAL)
                or self.configured.is_physical_forbidden(actor)
            ):
                return 7004
            return 0
        if action_type == _DEFEND:
            return 0
        if action_type == _FLEE:
            return 0 if self.pve else _INVALID
        if action_type == _SKILL:
            error = self.skills.validate(actor, action)
            if error == 7004 and actor.HasField("equipment_effects") and actor.equipment_effects.mana_reduction_bps:
                rule = self.skills.rules.get(action.skill_table_id)
                if rule is not None and rule.castable:
                    discounted = self.equipment_effects.discount_skill_mana(actor, rule, mana_cost(actor, rule))
                    if discounted <= actor.attributes.mana:
                        return 0
            return error
        if action_type == _ITEM:
            if self.is_festival_beast_battle:
                return _INVALID
            return self._validate_item(actor, action.item_table_id, action.target_id)
        return _INVALID

    def _validate_item(self, actor: Message, item_id: int, target_id: int) -> int:
        if self.is_festival_beast_battle:
            return _INVALID
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

    def _update_outcome(self) -> None:
        if self.outcome:
            return
        if self.encounter.boss_trial_kind == BOSS_TRIAL_KIND_GUARDIAN_RESCUE and self.encounter.protected_guard_actor_id:
            guard = self.actors.get(self.encounter.protected_guard_actor_id)
            if guard is None or not _active(guard):
                self.outcome = 2
                return
        side_a, side_b = (any(_active(actor) and actor.team_index == side for actor in self.actors.values())
                          for side in (0, 1))
        if not side_a and not side_b:
            self.outcome = 3
        elif not side_a:
            self.outcome = 2
        elif not side_b:
            self.outcome = 1
        elif self.round_index >= self.max_rounds:
            if self.encounter.boss_trial_kind == BOSS_TRIAL_KIND_POLARITY_5V5 or self.match_mode == MATCH_MODE_POLARITY_5V5:
                self.outcome = 3
            else:
                self.outcome = 2

    def resolve(self) -> Message:
        turn = self.protocol.message("TurnResultS2C", battle_id=self.battle_id, round_index=self.round_index)
        if self.outcome:
            turn.state.CopyFrom(self.snapshot(0, unredacted=True))
            return turn
        if self.prep_active:
            self.finalize_prep_phase()
        self._fill_default_actions()
        order = sorted((actor for actor in self.actors.values() if _active(actor)), key=self._sort_key)
        self.round_start_speeds = {actor.actor_id: self.configured.effective_speed(actor) for actor in order}
        self.last_action_order = [actor.actor_id for actor in order]
        turn.action_order.extend(self.last_action_order)
        self._group = 0
        for actor in order:
            if self.encounter.is_slow_phase_actor(actor.actor_id):
                continue
            if _active(actor):
                self._group += 1
                self._execute(turn, self._group, actor, self.pending[actor.actor_id])
        self.encounter.execute_slow_and_guardian_actions(turn)
        for actor in self.actors.values():
            if _active(actor) and actor.buffs:
                self._group += 1
                tick_start = len(turn.events)
                self.effects.tick(actor, turn, self._group)
                self._post_process_buff_ticks(actor, turn, tick_start)
        for actor in self.actors.values():
            actor.is_defending = False
            for skill_id, remaining in list(actor.skill_cooldown_rounds.items()):
                if remaining:
                    actor.skill_cooldown_rounds[skill_id] = remaining - 1
        self._update_outcome()
        if not self.outcome and self.is_boss_trial_battle:
            self._group += 1
            self.encounter.process_boss_trial_round_end(turn, self._group)
            self._update_outcome()
        self.trial_tracker.on_round_complete(self.round_index)
        for actor in self.actors.values():
            if actor.actor_type != _PLAYER:
                continue
            manual_rounds = self.trial_tracker.manual_action_rounds.get(actor.actor_id)
            if manual_rounds is not None and self.round_index in manual_rounds:
                self.consecutive_idle_rounds_by_player[actor.actor_id] = 0
            else:
                self.consecutive_idle_rounds_by_player[actor.actor_id] = (
                    self.consecutive_idle_rounds_by_player.get(actor.actor_id, 0) + 1
                )
        self.pending.clear()
        if not self.outcome:
            self._expire_surrender_votes_for_round_advance()
            self.round_index += 1
            self.encounter.advance_round_cleanup()
        else:
            self.encounter.active_telegraphs.clear()
        turn.state.CopyFrom(self.snapshot(0, unredacted=True))
        return turn

    def _post_process_buff_ticks(self, target: Message, turn: Message, event_start: int) -> None:
        """对本回合周期效果事件补充宠物毒伤修炼增幅、首领破绽易伤与怪物护卫回血追踪。"""
        for event in turn.events[event_start:]:
            if event.event_type != 7 or not event.value:
                continue
            rule = self.effects.rules.get(event.buff_table_id)
            caster = self.actors.get(event.source_id)
            if rule is None or caster is None:
                continue
            if rule.buff_type == 50 and caster.actor_type == _PET and caster.HasField("pet_cultivation"):
                bonus = self.pet_support.pet_poison_bonus(caster)
                if bonus > 0.0 and not target.is_dead:
                    extra = landed_damage(target, float(event.value) * bonus)
                    if extra > 0:
                        target.attributes.health -= extra
                        event.value += extra
                        event.target_health_after = target.attributes.health
                        self._death(turn, event.group_id, target, caster.actor_id)
            if rule.buff_type in (50, 51) and self.is_boss_trial_battle and target.team_index == 1 and not target.is_dead:
                vuln_pct = 0
                exp = self.encounter.exposed_actors.get(target.actor_id)
                if exp is not None and exp[1] >= self.round_index:
                    vuln_pct += exp[0]
                if target.is_boss and self.encounter.boss_phase >= 2:
                    vuln_pct += 15
                if vuln_pct > 0:
                    extra = min(target.attributes.health, (event.value * vuln_pct) // 100)
                    if extra > 0:
                        target.attributes.health -= extra
                        event.value += extra
                        event.target_health_after = target.attributes.health
                        self._death(turn, event.group_id, target, caster.actor_id)
            elif rule.buff_type in (40, 42):
                self.trial_tracker.record_monster_heal(caster, target, event.value)

    def _fill_default_actions(self) -> None:
        self.scripts.fill()
        if self.monster_ai.active:
            self.monster_ai.fill_actions()
        for actor in self.actors.values():
            if not _active(actor) or actor.actor_id in self.pending:
                continue
            default = None
            if actor.actor_type == _MONSTER:
                default = choose_monster_action(self, actor)
            elif actor.actor_type == _PET and actor.skill_table_ids:
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
            fallback_type = _DEFEND if self.configured.is_physical_forbidden(actor) else _ATTACK
            self.pending[actor.actor_id] = (
                default if default is not None else self.protocol.message("BattleAction", action_type=fallback_type)
            )

    def _sort_key(self, actor: Message) -> tuple[int, int, int]:
        action = self.pending.get(actor.actor_id)
        return (-self.skills.initiative(actor, action), -self.configured.effective_speed(actor), actor.actor_id)

    def _effective_stat(self, actor: Message, kind: int) -> int:
        value = base_stat(actor, kind)
        rate = self.effects.multiplier(actor, kind)
        if kind == 10:
            rate *= self.encounter.effective_physical_attack_multiplier(actor)
        elif kind == 11:
            rate *= self.encounter.effective_defense_multiplier(actor)
        return value if rate == 1.0 else scaled(value, rate)

    @staticmethod
    def _event(turn: Message, group: int, kind: int, source: Message, target: Message, **fields) -> Message:
        return turn.events.add(event_type=kind, source_id=source.actor_id, target_id=target.actor_id,
                               group_id=group, **fields)

    def _execute(self, turn: Message, group: int, actor: Message, action: Message) -> None:
        if self.configured.execute_prepared_cast_if_ready(actor, turn, group):
            return
        if (
            self.effects.has(actor, STUN, FREEZE)
            or self.configured.has_state(actor, BattleStateKind.FREEZE)
            or self.configured.has_state(actor, BattleStateKind.SLEEP)
        ):
            return
        if action.action_type == _DEFEND:
            actor.is_defending = True
            self._event(turn, group, 9, actor, actor)
            if actor.HasField("equipment_effects"):
                self.equipment_effects.on_defend(actor, self.round_index, turn, group)
            return
        if action.action_type == _FLEE:
            if self.is_boss_trial_battle and actor.actor_type == _PLAYER:
                # 首领试炼确定退出：不消耗 RNG，本人及名下出战宠原子离场。
                actor.fled = True
            elif self.pve:
                top_speed = max((self._effective_stat(other, 13) for other in self.actors.values() if self._enemy(actor, other)), default=0)
                difference = float(self._effective_stat(actor, 13)) - float(top_speed)
                chance = max(0.05, min(0.95, 0.5 + difference * (0.01 / 12)))
                actor.fled = self.rng.unit() < chance
            self._event(turn, group, 11, actor, actor, success=actor.fled)
            if actor.fled and (self.is_boss_trial_battle or self.is_festival_beast_battle) and actor.actor_type == _PLAYER:
                for other in self.actors.values():
                    if other.actor_type == _PET and other.owner_player_id == actor.actor_id and not other.fled:
                        was_active = _active(other)
                        other.fled = True
                        if was_active:
                            self._event(turn, group, 11, other, other, success=True)
            return
        if action.action_type == _ITEM:
            self._use_item(turn, group, actor, action)
            return
        if action.action_type == _SKILL:
            self._execute_skill_with_dropin(turn, group, actor, action)
            return
        if action.action_type == _ATTACK:
            if (
                self.configured.has_state(actor, BattleStateKind.SEAL_PHYSICAL)
                or self.configured.is_physical_forbidden(actor)
            ):
                return
            self._attack_target(turn, group, actor, action.target_id)

    def _execute_skill_with_dropin(self, turn: Message, group: int, actor: Message, action: Message) -> None:
        """执行技能并接入装备减耗、治疗增幅、法宝触发与试炼志事件追踪。"""
        event_start = len(turn.events)
        rule = self.skills.rules.get(action.skill_table_id)
        discount_saved = 0
        original_cost = 0
        borrowed_mana = 0
        if (
            rule is not None
            and actor.HasField("equipment_effects")
            and actor.equipment_effects.mana_reduction_bps
        ):
            original_cost = mana_cost(actor, rule)
            discounted_cost = self.equipment_effects.discount_skill_mana(actor, rule, original_cost)
            discount_saved = max(0, original_cost - discounted_cost)
            if discount_saved > 0 and discounted_cost <= actor.attributes.mana < original_cost:
                borrowed_mana = original_cost - actor.attributes.mana
                actor.attributes.mana += borrowed_mana

        self.skills.execute(actor, action, turn, group)

        if discount_saved > 0:
            mana_events = [
                ev for ev in turn.events[event_start:]
                if ev.event_type == 14 and ev.source_id == actor.actor_id and ev.target_id == actor.actor_id and ev.mana_delta < 0
            ]
            if mana_events:
                refund = discount_saved - borrowed_mana
                if refund > 0:
                    actor.attributes.mana = min(actor.max_mana, actor.attributes.mana + refund)
                elif refund < 0:
                    actor.attributes.mana = max(0, actor.attributes.mana + refund)
                discounted_actual = max(1, abs(mana_events[0].mana_delta) - discount_saved)
                mana_events[0].value = discounted_actual
                mana_events[0].mana_delta = -discounted_actual
                mana_events[0].target_mana_after = actor.attributes.mana
            elif borrowed_mana > 0:
                actor.attributes.mana = max(0, actor.attributes.mana - borrowed_mana)

        new_events = list(turn.events[event_start:])
        first_heal = True
        for event in new_events:
            if event.event_type == 4 and event.value > 0:
                target = self.actors.get(event.target_id)
                if target is not None:
                    self.equipment_effects.amplify_heal_event(
                        actor, target, event, self.round_index, turn, group, is_primary_first_hit=first_heal,
                    )
                    first_heal = False
                    self.trial_tracker.record_effective_contribution(actor)
                    self.trial_tracker.record_monster_heal(actor, target, event.value)
            elif event.event_type == 6:
                target = self.actors.get(event.target_id)
                if target is not None:
                    if target.team_index != actor.team_index and target.actor_type == _MONSTER:
                        self.trial_tracker.record_guard_stance_dispelled(event.buff_table_id)
                        self.trial_tracker.record_effective_contribution(actor)
                    elif target.team_index == actor.team_index and actor.actor_type == _PLAYER:
                        self.trial_tracker.record_anti_control_or_season_skill(actor)

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
        raw_heal = float(rule.heal_health)
        if self.escalation_stacks > 0:
            raw_heal *= self.escalation_support_pct / 100.0
        target.attributes.health = old_health + landed_heal(target, raw_heal)
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
        events_before = len(turn.events)
        self._event(turn, group, 1, actor, target, hit_kind=hit_kind)
        self._damage(turn, group, actor, target, rule, hit_kind=hit_kind)
        if hit_kind == 0 and _active(actor) and _active(target) and self.rng.percent(actor.combat.combo_rate):
            self._damage(turn, group, actor, target, rule, hit_kind=1, hit_index=1)
        # 单次物理攻击标记覆盖整次普攻行动（含连击），行动末一次性消耗。
        if hit_kind == 0 and _active(actor):
            self.configured.consume_one_shot_attack_modifiers(actor, 1, turn, group)
            landed_normal = any(
                ev.event_type == 3 and ev.source_id == actor.actor_id and ev.target_id == target.actor_id and ev.value > 0
                for ev in turn.events[events_before:]
            )
            if landed_normal and _active(target):
                self.configured.trigger_buffs(actor, target, BattleTrigger.AFTER_BASIC, 1, turn, group)
        if (
            hit_kind == 0
            and _active(actor)
            and _active(target)
            and not self.effects.has(target, STUN, FREEZE)
            and not self.configured.has_state(target, BattleStateKind.FREEZE)
            and not self.configured.has_state(target, BattleStateKind.SLEEP)
            and not self.configured.is_physical_forbidden(target)
            and self.rng.percent(target.combat.counter_rate)
        ):
            self._group += 1
            self._basic_attack(turn, self._group, target, actor, hit_kind=2)

    def _damage_before_critical(self, actor: Message, target: Message, rule: BattleSkill, base: float) -> float:
        if base <= 0 or not isfinite(base):
            return 0.0
        damage_kind = rule.damage_kind
        attack = (
            self.configured.effective_physical_attack(actor)
            if damage_kind == 1
            else self.configured.effective_magic_attack(actor)
            if damage_kind == 0
            else 0
        )
        resist_bonus = target.combat.physical_resist if damage_kind == 1 else target.combat.magic_resist if damage_kind == 0 else 0
        resistance = min(100, target.attributes.resistance + resist_bonus) / 100
        level_factor = 360 + 120 * min(85, max(1, target.level))
        eff_def = self.configured.effective_defense(target)
        defense = float(target.attributes.armor) + float(eff_def)
        ratio = max(1.0 - 0.60, level_factor / (defense + level_factor) * (1 - resistance))
        multiplier = rule.attack_multiplier
        if not isfinite(multiplier) or multiplier < 0:
            multiplier = 0.0
        raw = base * (1 + float(actor.attributes.strength) * 0.1) + float(attack) * multiplier
        if not isfinite(raw) or raw <= 0:
            return 0.0
        damage = raw * ratio
        if target.HasField("equipment_effects"):
            damage = self.equipment_effects.apply_equipment_resist(
                target, damage, damage_kind, eff_def,
            )
        return damage * (1.0 if self.pve else 0.3)

    @staticmethod
    def _critical_chance(actor: Message, rule: BattleSkill) -> float:
        kind = rule.damage_kind
        bonus = actor.combat.physical_crit_rate if kind == 1 else actor.combat.magic_crit_rate if kind == 0 else 0
        base = min(100, actor.attributes.critchance + bonus) / 100
        if not actor.HasField("equipment_effects"):
            return base
        equip_crit = (
            actor.equipment_effects.physical_crit_bps
            if kind == 1
            else actor.equipment_effects.magic_crit_bps
            if kind == 0
            else 0
        )
        return max(0.0, min(1.0, base + equip_crit / 10_000.0)) if equip_crit else base

    def _expected_damage(self, actor: Message, target: Message, rule: BattleSkill, base: float) -> float:
        return self._damage_before_critical(actor, target, rule, base) * (1 + self._critical_chance(actor, rule))

    def _damage(self, turn: Message, group: int, actor: Message, target: Message, rule: BattleSkill,
                *, skill_id: int = 0, hit_kind: int = 0, hit_index: int = 0,
                base: float | None = None, rank: int = 0,
                is_multi_target: bool = False, is_primary_target: bool = True,
                is_primary_focus: bool = False) -> None:
        damage = self._damage_before_critical(actor, target, rule, rule.base_damage if base is None else base)
        critical_rate = self._critical_chance(actor, rule)
        critical = critical_rate > 0 and self.rng.unit() < critical_rate
        if critical:
            damage *= 2
        damage *= rank_factor(rank)
        if hit_kind == 1:
            damage *= 0.5
        if is_primary_focus:
            damage *= PVP_PRIMARY_FOCUS_NUM / PVP_PRIMARY_FOCUS_DEN
        if hit_kind != 3:
            typed_out = self.configured.modifier(
                actor,
                BattleModifier.OUTGOING_PHYSICAL_DAMAGE if rule.damage_kind == 1 else BattleModifier.OUTGOING_MAGIC_DAMAGE,
            )
            one_shot_out = self.configured.modifier(
                actor,
                BattleModifier.NEXT_PHYSICAL_ATTACK if rule.damage_kind == 1 else BattleModifier.NEXT_MAGIC_ATTACK,
            )
            out_bonus = self.configured.modifier(actor, BattleModifier.OUTGOING_DAMAGE) + typed_out + one_shot_out
            if out_bonus != 0.0:
                damage *= max(0.0, 1.0 + out_bonus)
            in_bonus = self.configured.modifier(target, BattleModifier.INCOMING_DAMAGE) + self.configured.hit_vulnerability_bonus(target)
            if in_bonus != 0.0:
                damage *= max(0.0, 1.0 + in_bonus)
        if actor.actor_type == _PET and actor.HasField("pet_cultivation"):
            damage = self.pet_support.apply_pet_damage_bonus(
                actor, damage, rule.damage_kind, is_direct=(hit_kind != 3), hit_kind=hit_kind,
            )
        extra_bonus = self.equipment_effects.actor_bonus(actor.actor_id)
        if extra_bonus is not None and extra_bonus.damage_bonus_bps > 0:
            damage = self.pet_support.apply_bonus(damage, extra_bonus.damage_bonus_bps / 10_000.0)
        if self.escalation_stacks > 0 and hit_kind != 3:
            damage *= self.escalation_damage_pct / 100.0

        splash_reduced = False
        if self.encounter.has_active_damage_modifiers:
            scaled_dmg = damage * (0.5 if target.is_defending else 1.0)
            remaining = ceil(scaled_dmg) if (scaled_dmg > 0 and isfinite(scaled_dmg)) else 0
            remaining, splash_reduced = self.encounter.apply_encounter_damage_modifiers(
                actor, target, remaining, rule.damage_kind, hit_kind != 2, turn, group,
                is_multi_target_skill=is_multi_target, is_primary_target=is_primary_target,
            )
            dealt = min(target.attributes.health, remaining)
        else:
            dealt = landed_damage(target, damage)
        if dealt > 0 and (
            target.HasField("equipment_effects")
            or self.equipment_effects.actor_bonus(target.actor_id) is not None
        ):
            dealt = self.equipment_effects.reduce_damage(
                actor, target, dealt, self.round_index, turn, group,
                direct_or_pursuit_or_mirror=(hit_kind != 3),
            )
        original_shields = [entry.buff_id for entry in target.buffs if entry.remaining_capacity > 0]
        absorbed = 0
        if dealt > 0 and original_shields:
            dealt, absorbed = self.equipment_effects.absorb_shields(actor, target, dealt, turn, group)
            if actor.HasField("equipment_effects") and hit_kind == 0 and hit_index == 0:
                self.equipment_effects.break_shields(
                    actor, target, dealt + absorbed, rule.damage_kind,
                    self.round_index, turn, group, original_shields,
                )
        if dealt > 0 or not absorbed:
            self._apply_damage(
                turn, group, actor, target, dealt, critical=critical, skill_id=skill_id,
                hit_kind=hit_kind, hit_index=hit_index,
                is_splash_reduced=splash_reduced, is_primary_focus=is_primary_focus,
            )
        if not skill_id:
            self.encounter.apply_breaker_sunder_on_basic_hit(actor, target, dealt, hit_kind, turn, group)
        if (dealt + absorbed) > 0 and hit_kind in (0, 1):
            self.trial_tracker.record_direct_damage(actor, target, rule.damage_kind, dealt + absorbed)
            self.encounter.record_player_direct_damage(actor, target, rule.damage_kind, dealt + absorbed)
        if (dealt + absorbed) > 0 and hit_kind != 3 and _active(target):
            self.configured.increment_hit_vulnerability(target, self.round_index, group)
        if dealt > 0:
            self.equipment_effects.apply_lifesteal(actor, dealt, turn, group)
        if not skill_id and hit_kind != 2 and dealt and _active(actor) and _active(target) and self.rng.percent(target.combat.reflect_rate):
            reflected = min(actor.attributes.health, (dealt + 1) // 2)
            self._apply_damage(turn, group, target, actor, reflected, hit_kind=3, hit_index=hit_index)

    def _apply_damage(self, turn: Message, group: int, source: Message, target: Message, value: int,
                      *, critical: bool = False, skill_id: int = 0, hit_kind: int = 0, hit_index: int = 0,
                      is_splash_reduced: bool = False, is_primary_focus: bool = False) -> None:
        target.attributes.health -= value
        ev = self._event(turn, group, 3, source, target, value=value, is_critical=critical, skill_table_id=skill_id,
                         hit_kind=hit_kind, hit_index=hit_index, target_health_after=target.attributes.health,
                         target_mana_after=target.attributes.mana)
        if is_splash_reduced:
            ev.is_splash_reduced = True
        if is_primary_focus:
            ev.is_primary_focus = True
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
        target.charging_skill_id = 0
        if target.artifact_states:
            self.equipment_effects.clear_pending_echoes_on_death(target)
        target.ClearField("buffs")
        self.trial_tracker.record_death(target)
        self.monster_ai.notify_dead(target.actor_id)
        self._event(turn, group, 8, target, target, hit_index=hit)
        self.encounter.handle_boss_trial_monster_death(target, turn, group)


@dataclass(slots=True)
class _Room:
    engine: TurnEngine
    original: Message
    deadline_ms: int
    action_deadline_ms: int
    routing: dict[int, Message]
    action_due_steady_ms: int = 0
    battle_due_steady_ms: int = 0
    prep_deadline_ms: int = 0
    prep_due_steady_ms: int = 0
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
    confirm_retry_steady_ms: int = 0
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
        monster_ai_profiles: Mapping[int, DungeonMonsterAiProfile] | None = None,
        trial_context_resolver: Callable[[Message], TrialContext | Message | None] | None = None,
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
        self.monster_ai_profiles = dict(monster_ai_profiles) if monster_ai_profiles else None
        self.trial_context_resolver = trial_context_resolver
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
        self._observer_deadlines: list[tuple[int, tuple[int, int, str]]] = []
        self.admitting = True
        self._draining = False

    @staticmethod
    def _error(response: Message, error: int) -> Message:
        if error:
            response.error_message.id = error
        return response

    def create(
        self,
        request: Message,
        *,
        trial_context: TrialContext | Message | None = None,
        monster_ai_profiles: Mapping[int, DungeonMonsterAiProfile] | None = None,
        ranked_context: Message | None = None,
    ) -> Message:
        response = self.protocol.message("CreateBattleResponse", battle_id=request.battle_id)
        if self._draining:
            return self._error(response, _UNAVAILABLE)
        now = self.now_ms()
        if (request.outcome_protocol_version != 1 or not request.placement_operation_id
                or request.target_instance_id != self.instance_id
                or request.required_physical_region != self.physical_region):
            return self._error(response, _INVALID)
        self._prune_creation_receipts(now)
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
            boss_trial_kind, battle_difficulty, explicit_round_limit = (
                self.tables.dungeon_meta(request.battle_config_id)
                if self.tables is not None else (0, 0, 0)
            )
            resolved_trial_ctx = trial_context
            if resolved_trial_ctx is None and self.trial_context_resolver is not None:
                resolved_trial_ctx = self.trial_context_resolver(request)
            resolved_ai_profiles = monster_ai_profiles if monster_ai_profiles is not None else self.monster_ai_profiles
            engine = TurnEngine(self.protocol, request, monsters=monsters, skills=self.skills, items=self.items,
                                max_rounds=max_rounds, buffs=self.buffs,
                                silence_permissions=self.tables.silence_permissions if self.tables else None,
                                monster_ids={row.id for row in self.tables.tables["Monster"].rows()} if self.tables else None,
                                monster_ai_profiles=resolved_ai_profiles,
                                trial_context=resolved_trial_ctx,
                                boss_trial_kind=boss_trial_kind,
                                battle_difficulty=battle_difficulty,
                                has_explicit_round_limit=explicit_round_limit > 0,
                                ranked_context=ranked_context)
        except (BattleError, ValueError):
            return self._error(response, _INVALID_TABLE)
        now = self.now_ms()
        if request.admit_before_ms <= now:
            return self._error(response, _INVALID)
        deadline = request.deadline_ms if request.deadline_ms > now else now + (_MAX_ROUNDS + 2) * _ROUND_MS
        manual_window_ms = BOSS_TRIAL_ACTION_TIMEOUT_MS if engine.is_boss_trial_battle else _ACTION_MS
        original = self.protocol.message("CreateBattleRequest")
        original.CopyFrom(request)
        routing = {player.player_id: player.routing for player in original.players}
        in_prep_phase = engine.is_in_prep_phase
        room = _Room(engine, original, deadline, 0 if in_prep_phase else (now + manual_window_ms), routing,
                     reward_profile=reward_profile,
                     confirm_until_ms=now + 180_000)
        # 战前 15 秒明牌变阵期延迟录制回放起点至 _finalize_battle_prep 亮牌后再录。
        if request.record_replay and not in_prep_phase:
            fingerprint = self.tables.fingerprint if self.tables else ""
            room.replay = BattleReplayRecorder(self.protocol, original, fingerprint, engine.snapshot(0))
        assignments = {player: self._assignment(room, player, _PARTICIPANT) for player in routing}
        # Preparing the engine, replay and every ticket can exhaust the
        # placement window; do not commit a room after it has expired.
        admitted_at = self.now_ms()
        if request.admit_before_ms <= admitted_at:
            return self._error(response, _INVALID)
        armed_at = self._steady_ms()
        if in_prep_phase:
            room.action_deadline_ms = 0
            room.action_due_steady_ms = 0
            room.prep_deadline_ms = admitted_at + TACTICAL_PREP_TIMEOUT_MS
            room.prep_due_steady_ms = armed_at + TACTICAL_PREP_TIMEOUT_MS
        else:
            room.action_deadline_ms = admitted_at + manual_window_ms
            room.action_due_steady_ms = armed_at + manual_window_ms
        room.battle_due_steady_ms = armed_at + deadline - now
        self.rooms[request.battle_id] = room
        if self.on_rooms_changed:
            self._publish(self.on_rooms_changed, len(self.rooms))
        self.receipts[request.battle_id] = _Receipt(
            request.placement_operation_id, request.deadline_ms + 60_000, canonical,
        )
        for player, route in routing.items():
            if self.on_lobby_push:
                self._publish(self.on_lobby_push, route, player, 177, assignments[player])
                start = self.protocol.message(
                    "BattleStartS2C",
                    battle_id=request.battle_id,
                    state=engine.snapshot(room.action_deadline_ms, player, prep_deadline_ms=room.prep_deadline_ms),
                )
                self._publish(self.on_lobby_push, route, player, 143, start)
            if self.on_confirm:
                self._publish(self.on_confirm, route, player, request.battle_id, deadline)
        room.confirm_retry_steady_ms = self._steady_ms() + 10_000
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
                or not now < request.admit_before_ms <= now + 30_000):
            return self._error(response, _INVALID)
        self._prune_observer_receipts(now)
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
            heapq.heappush(self._observer_deadlines, (receipt.expires_ms, key))
            if room is None:
                receipt.error = _MISSING
            elif not request.routing.gate_instance_id:
                receipt.error = _INVALID
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
        # A replay must be signed again, just like a new admission.  If the
        # signing endpoint becomes unavailable, Match rolls back its marker;
        # retire the local seat as well and retain the original rejection.
        if not self.ticket_secret or not self.port:
            self._drop_observer(room, player, notify=False)
            receipt.error = _UNAVAILABLE
            return self._error(response, _UNAVAILABLE)
        assignment = self._assignment(room, player, _OBSERVER, operation)
        if self.on_lobby_push:
            self._publish(self.on_lobby_push, request.routing, player, 177, assignment)
        self._push_observer_state(room, player)
        return response

    def query_observer(self, request: Message) -> Message:
        response = self.protocol.message("QueryObserverResponse")
        if (request.target_instance_id != self.instance_id or not request.battle_id or not request.observer_player_id
                or not _uuid(request.spectator_operation_id)):
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
        if (not player or not request.battle_id or not _uuid(operation) or request.target_instance_id != self.instance_id
                or not 0 < request.admit_before_ms <= now + 30_000):
            return response
        self._prune_observer_receipts(now)
        key = request.battle_id, player, operation
        previous = self.observer_receipts.get(key)
        if previous is not None and previous.expires_ms != request.admit_before_ms:
            return response
        if request.admit_before_ms > now:
            if previous is None and len(self.observer_receipts) >= _MAX_SPECTATOR_OPERATIONS:
                return response
            self.observer_receipts[key] = _ObserverReceipt(request.admit_before_ms)
            if previous is None:
                heapq.heappush(self._observer_deadlines, (request.admit_before_ms, key))
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
        observer = room.observers.pop(player)
        key = room.engine.battle_id, player, observer.operation_id
        receipt = self.observer_receipts.get(key)
        if receipt is not None:
            if receipt.expires_ms <= self.now_ms():
                del self.observer_receipts[key]
            else:
                receipt.error = _INVALID
        if notify:
            self._emit(room, player, 166, self.protocol.message("SpectateEndS2C", battle_id=room.engine.battle_id, reason=3))
        connection = room.connections.pop(player, None)
        if connection is not None:
            self._publish(connection.close)

    def _prune_observer_receipts(self, now: int) -> None:
        while self._observer_deadlines and self._observer_deadlines[0][0] <= now:
            expires, key = heapq.heappop(self._observer_deadlines)
            receipt = self.observer_receipts.get(key)
            if receipt is not None and receipt.expires_ms == expires and receipt.error:
                del self.observer_receipts[key]

    def _prune_creation_receipts(self, now: int) -> None:
        for battle_id, receipt in tuple(self.receipts.items()):
            if receipt.expires_ms < now and battle_id not in self.rooms:
                del self.receipts[battle_id]

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
        steady = self._steady_ms()
        room = self._member(ticket) if request.battle_id == ticket.battle_id else None
        if message_id == 140:
            return self._snapshot(room, ticket.player_id if ticket.role == _PARTICIPANT else 0) if room is not None else self.protocol.message("BattleStateS2C")
        responses = {
            149: "SubmitBattleActionResponse",
            162: "SetAutoBattleResponse",
            165: "StopWatchBattleResponse",
            479: "SurrenderBattleResponse",
            504: "UpdateBattlePrepResponse",
        }
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
        if room.proposed_outcome or room.battle_due_steady_ms <= steady:
            return self._error(response, _UNAVAILABLE)
        engine = room.engine
        if message_id == 504:
            if engine.is_in_prep_phase and room.prep_due_steady_ms and steady >= room.prep_due_steady_ms:
                self._finalize_battle_prep(room)
            err, prep_state = engine.update_battle_prep(
                ticket.player_id,
                request.selected_seal_id,
                request.selected_pet_id,
                bool(request.ready),
                deadline_ms=room.prep_deadline_ms,
            )
            if err:
                return self._error(response, err)
            response.prep_state.CopyFrom(prep_state)
            if engine.all_prep_players_ready():
                self._finalize_battle_prep(room)
                return response
            # 仅向同队成员推送 505 变阵同步（各自保留本人可选列表 + 共享本队选择），绝不向对手或观众泄密。
            actor_team = engine.team_index_of_player(ticket.player_id)
            if actor_team is not None:
                for member_id in tuple(room.routing):
                    if engine.team_index_of_player(member_id) != actor_team:
                        continue
                    notify = self.protocol.message("BattlePrepUpdateS2C", battle_id=engine.battle_id)
                    notify.prep_state.CopyFrom(engine.build_prep_state_for_viewer(member_id, room.prep_deadline_ms))
                    self._emit(room, member_id, 505, notify)
            return response
        if message_id == 479:
            visible_before = self._terminal_visibility(room)
            err, vote_state = engine.submit_surrender_vote(ticket.player_id, bool(request.agree))
            if err:
                return self._error(response, err)
            response.vote_state.CopyFrom(vote_state)
            notify = self.protocol.message("SurrenderVoteS2C", battle_id=engine.battle_id)
            notify.vote_state.CopyFrom(vote_state)
            for member_id in tuple(room.routing):
                if engine.team_index_of_player(member_id) == vote_state.team_index:
                    self._emit(room, member_id, 480, notify)
            if engine.outcome:
                if room.replay is not None:
                    room.replay.mark_deadline_draw(engine.snapshot(0))
                room.visible_before_terminal = visible_before
                self._propose_outcome(room, engine.outcome, self.now_ms())
            return response
        if engine.is_in_prep_phase:
            if room.prep_due_steady_ms and steady >= room.prep_due_steady_ms:
                self._finalize_battle_prep(room)
            else:
                return self._error(response, _FEATURE)
        if steady >= room.action_due_steady_ms:
            self._resolve(room)
            return self._error(response, _INVALID)
        actor = engine.actors[ticket.player_id]
        if not _active(actor):
            return self._error(response, _INACTIVE)
        if message_id == 162:
            if ticket.player_id in engine.ai_controlled_players:
                return self._error(response, _INVALID)
            was_ready = not engine.pending_players()
            actor.is_auto = request.enabled
            if request.enabled and not was_ready and not engine.pending_players():
                self._resolve(room)
        else:
            error = engine.validate(actor, request.action)
            if error:
                return self._error(response, error)
            engine.pending[ticket.player_id] = self.protocol.message("BattleAction")
            engine.pending[ticket.player_id].CopyFrom(request.action)
            engine.trial_tracker.record_manual_action(ticket.player_id, engine.round_index)
            if not engine.pending_players():
                self._resolve(room)
        return response

    def _finalize_battle_prep(self, room: _Room) -> None:
        """结束论剑 V3 战前 15 秒明牌变阵：锁阵、启动第 1 回合计时、补录回放起点并向参战者/观众亮牌。"""
        if room.proposed_outcome or not room.engine.is_in_prep_phase:
            return
        room.prep_deadline_ms = 0
        room.prep_due_steady_ms = 0
        if not room.engine.finalize_prep_phase():
            return
        manual_window_ms = BOSS_TRIAL_ACTION_TIMEOUT_MS if room.engine.is_boss_trial_battle else _ACTION_MS
        room.action_deadline_ms = self.now_ms() + manual_window_ms
        room.action_due_steady_ms = self._steady_ms() + manual_window_ms
        if room.original.record_replay and room.replay is None:
            fingerprint = self.tables.fingerprint if self.tables else ""
            room.replay = BattleReplayRecorder(self.protocol, room.original, fingerprint, room.engine.snapshot(0))
        for player, route in room.routing.items():
            start = self.protocol.message(
                "BattleStartS2C",
                battle_id=room.engine.battle_id,
                state=room.engine.snapshot(room.action_deadline_ms, player),
            )
            if self.on_lobby_push:
                self._publish(self.on_lobby_push, route, player, 143, start)
        for observer_id in tuple(room.observers):
            self._push_observer_state(room, observer_id)
        if room.spectate.targets():
            self._spectate_state(room)
        _LOG.info(
            "battle tactical prep finalized battle=%s action_deadline_ms=%s",
            room.engine.battle_id,
            room.action_deadline_ms,
        )

    @staticmethod
    def _snapshot(room: _Room, viewer: int) -> Message:
        if room.proposed_outcome:
            return room.visible_before_terminal.get(viewer, room.visible_before_terminal[0])
        return room.engine.snapshot(room.action_deadline_ms, viewer, prep_deadline_ms=room.prep_deadline_ms)

    def _resolve(self, room: _Room) -> None:
        visible = self._terminal_visibility(room)
        turn = room.engine.resolve()
        if room.replay is not None:
            room.replay.add_round(turn)
        if room.engine.outcome:
            room.visible_before_terminal = visible
            room.final_turn = turn
            self._propose_outcome(room, room.engine.outcome, self.now_ms())
            return
        manual_window_ms = BOSS_TRIAL_ACTION_TIMEOUT_MS if room.engine.is_boss_trial_battle else _ACTION_MS
        interval = manual_window_ms if room.engine.pending_players() else max(2000, 250 * len(turn.action_order))
        room.action_deadline_ms = self.now_ms() + interval
        room.action_due_steady_ms = self._steady_ms() + interval
        self._broadcast_turn(room, turn)

    @staticmethod
    def _terminal_visibility(room: _Room) -> dict[int, Message]:
        return {
            viewer: room.engine.snapshot(room.action_deadline_ms, viewer, prep_deadline_ms=room.prep_deadline_ms)
            for viewer in (0, *room.routing)
        }

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
            room.prep_deadline_ms = 0
            room.prep_due_steady_ms = 0
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
        rounds = engine.completed_rounds()
        event = self.protocol.message(
            "contracts.kafka.BattleResultEvent", battle_id=engine.battle_id, match_mode=request.match_mode,
            battle_config_id=request.battle_config_id, outcome=room.proposed_outcome,
            winner_team_index=int(room.proposed_outcome == 2), total_rounds=rounds,
            finished_at_ms=room.outcome_finished_ms, settlement_format_version=1,
        )
        if request.activity_context.kind:
            event.activity_context.CopyFrom(request.activity_context)
        if engine.is_ranked_battle and engine.ranked_context is not None:
            event.ranked_context.CopyFrom(engine.ranked_context)
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
                surrendered=(engine.surrendered_team is not None and actor.team_index == engine.surrendered_team),
                use_free_preset=engine.uses_free_preset,
                boss_trial_kind=engine.encounter.boss_trial_kind,
                battle_difficulty=engine.encounter.battle_difficulty,
                manual_action_rounds=len(engine.trial_tracker.manual_action_rounds.get(player, ())),
                effective_combat_contribution=(player in engine.trial_tracker.effective_contribution_players),
            )
            if engine.is_festival_beast_battle:
                settlement.festival_beast.CopyFrom(request.activity_context.festival_beast)
            if engine.is_ranked_battle and engine.ranked_context is not None:
                settlement.competitive_ranked = True
                settlement.season_id = engine.ranked_context.season_id
                settlement.ranked_rule_version = engine.ranked_context.rule_version
            if actor.fled:
                event.fled_player_ids.append(player)
            if actor.is_dead:
                event.dead_player_ids.append(player)
            for item_id, count in engine.items_consumed.get(player, {}).items():
                settlement.items_consumed.add(item_table_id=item_id, count=count)
            for pet in engine.actors.values():
                if pet.actor_type == _PET and pet.owner_player_id == player and not pet.competitive_temporary:
                    settlement.pets.add(pet_id=pet.pet_id, health=pet.attributes.health,
                                        mana=pet.attributes.mana, is_dead=pet.is_dead)
            if (engine.pve and not engine.is_festival_beast_battle
                    and engine.outcome == 1 and actor.team_index == 0 and _active(actor)):
                for monster_id in engine.defeated_monsters:
                    settlement.defeated_monsters.add(monster_config_id=monster_id, count=1)
                if self.tables:
                    self.tables.add_rewards(request, settlement, profile=room.reward_profile)
            if engine.trial_tracker.enabled:
                engine.trial_tracker.populate_settlement(settlement, player)
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
        reason = 1 if committed and (room.final_turn is not None or room.engine.surrendered_team is not None) else 2
        end = self.protocol.message("SpectateEndS2C", battle_id=battle_id,
                                    outcome=room.proposed_outcome if committed else 0, reason=reason)
        self._spectate_frame(room, END, room.engine.round_index, end)
        for player, connection in tuple(room.connections.items()):
            if player in room.observers:
                self._emit(room, player, 166, end)
            self._publish(connection.close)
        room.connections.clear()
        for player in tuple(room.observers):
            self._drop_observer(room, player, notify=False)
        del self.rooms[battle_id]
        if self.on_rooms_changed:
            self._publish(self.on_rooms_changed, len(self.rooms))
        return True

    def tick(self) -> None:
        if self._draining:
            return
        now = self.now_ms()
        steady = self._steady_ms()
        self._prune_creation_receipts(now)
        self._prune_observer_receipts(now)
        for room in tuple(self.rooms.values()):
            for player, observer in tuple(room.observers.items()):
                if 0 < observer.expires_ms <= steady:
                    self._drop_observer(room, player)
            if room.proposed_outcome:
                if steady - room.outcome_since_ms >= 30 * 3_600_000:
                    for connection in room.connections.values():
                        self._publish(connection.close)
                    room.connections.clear()
                    for player in tuple(room.observers):
                        self._drop_observer(room, player, notify=False)
                    del self.rooms[room.engine.battle_id]
                    if self.on_rooms_changed:
                        self._publish(self.on_rooms_changed, len(self.rooms))
                    _LOG.error("battle outcome still unconfirmed after replay window: %s", room.engine.battle_id)
                elif room.outcome_retry_ms <= steady:
                    self._propose_outcome(room, room.proposed_outcome, now)
            elif room.battle_due_steady_ms <= steady:
                room.prep_deadline_ms = 0
                room.prep_due_steady_ms = 0
                room.visible_before_terminal = self._terminal_visibility(room)
                if room.replay is not None:
                    room.replay.mark_deadline_draw(room.visible_before_terminal[0])
                self._propose_outcome(room, 3, now)
            elif room.engine.is_in_prep_phase:
                if room.prep_due_steady_ms and room.prep_due_steady_ms <= steady:
                    self._finalize_battle_prep(room)
            elif room.action_due_steady_ms <= steady:
                self._resolve(room)
            elif (self.on_confirm and room.confirm_retry_steady_ms
                  and room.confirm_retry_steady_ms <= steady):
                if now >= room.confirm_until_ms:
                    room.confirm_retry_steady_ms = 0
                else:
                    room.confirm_retry_steady_ms = steady + 10_000
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
            if not room.proposed_outcome:
                for player in tuple(room.observers):
                    self._drop_observer(room, player, notify=False)

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
