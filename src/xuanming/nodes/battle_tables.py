"""Typed C++ battle table adapters, deterministic RNG and fingerprints."""

from __future__ import annotations

import ast
import hashlib
import operator
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib.resources import files
from math import ceil, isfinite
from pathlib import Path
from typing import TYPE_CHECKING

from google.protobuf.json_format import Parse
from google.protobuf.message import Message
from google.protobuf.message_factory import GetMessageClass

if TYPE_CHECKING:
    from xuanming.data import ConfigTables
    from xuanming.protocol import Protocol

_MASK64 = (1 << 64) - 1
_ROUND_MS = 6_000
_FINGERPRINT_TABLES = ("Skill", "Buff", "Cooldown", "SkillPermission", "Dungeon", "Monster", "Item")
_BINARY_OPERATORS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv}


def rounds_from_seconds(seconds: float) -> int:
    return max(1, (int(seconds * 1000) + _ROUND_MS - 1) // _ROUND_MS) if seconds > 0 else 1


def _expression(source: str) -> Callable[[float, float], float]:
    """Compile the arithmetic formulas used by the battle table exporter."""
    def parse(node):
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            return lambda level, health: float(node.value)
        if isinstance(node, ast.Name) and node.id == "level":
            return lambda level, health: level
        if isinstance(node, ast.Name) and node.id == "health":
            return lambda level, health: health
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            value = parse(node.operand)
            return (lambda level, health: -value(level, health)) if isinstance(node.op, ast.USub) else value
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY_OPERATORS:
            apply, left, right = _BINARY_OPERATORS[type(node.op)], parse(node.left), parse(node.right)
            return lambda level, health: apply(left(level, health), right(level, health))
        raise ValueError(f"unsupported battle table expression: {source!r}")

    formula = parse(ast.parse(source.strip() or "0", mode="eval").body)

    def evaluate(level: float, health: float = 0) -> float:
        try:
            value = formula(level, health)
            return value if isfinite(value) else 0.0
        except (ArithmeticError, ValueError):
            return 0.0

    return evaluate


def battle_table_fingerprint(tables: ConfigTables) -> str:
    """C++ BattleTableFingerprint::ComputeFrom over the seven decoded wrappers."""
    digest = hashlib.sha256()
    for name in _FINGERPRINT_TABLES:
        rows = tables[name].rows()
        if rows:
            descriptor = rows[0].DESCRIPTOR.file.pool.FindMessageTypeByName(f"{name}TableData")
            data = GetMessageClass(descriptor)(data=rows).SerializeToString(deterministic=True)
        else:
            data = b""
        digest.update(name.lower().encode("ascii") + b"\0" + len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()[:32]


class _Mt19937_64:
    """The seed and output mapping used by std::mt19937_64 in the C++ engine."""

    def __init__(self, seed: int):
        self._state = [seed]
        for index in range(1, 312):
            previous = self._state[-1]
            self._state.append((6364136223846793005 * (previous ^ (previous >> 62)) + index) & _MASK64)
        self._index = 312

    def next(self) -> int:
        state = self._state
        if self._index == 312:
            lower_mask = (1 << 31) - 1
            for index in range(312):
                merged = (state[index] & ~lower_mask) | (state[(index + 1) % 312] & lower_mask)
                twist = 0xB5026F5AA96619E9 if merged & 1 else 0
                state[index] = state[(index + 156) % 312] ^ (merged >> 1) ^ twist
            self._index = 0
        value = state[self._index]
        self._index += 1
        value ^= (value >> 29) & 0x5555555555555555
        value ^= (value << 17) & 0x71D67FFFEDA60000
        value ^= (value << 37) & 0xFFF7EEE000000000
        return value ^ (value >> 43)

    def unit(self) -> float:
        return (self.next() >> 11) / (1 << 53)

    def percent(self, chance: int) -> bool:
        return chance >= 100 or (chance > 0 and self.unit() * 100 < chance)


@dataclass(frozen=True, slots=True)
class BattleSkill:
    base_damage: float
    mana_cost: int = 0
    cooldown_rounds: int = 0
    physical: bool = False
    attack_multiplier: float = 1.0
    initiative: int = 0
    damage_per_level: float = 0.0
    cooldown_group: int = 0
    requires_target: bool = False
    class_id: int = 0
    unlock_level: int = 0
    max_rank: int = 0
    skill_types: tuple[int, ...] = ()
    target_modes: tuple[int, ...] = ()
    effects: tuple[int, ...] = ()
    damage_type: int | None = None
    target_team: int = 0
    max_targets: int = 1
    pet_max_targets: int = 0
    mana_percent: float = 0.0
    heal_multiplier: float = 0.0
    heal_per_level: float = 0.0
    execute_heal_threshold: float = 0.0
    execute_heal_bonus: float = 0.0
    restore_mana_percent: float = 0.0
    dispel_count: int = 0
    passive_stat: str = ""
    passive_percent: float = 0.0
    buff_percent: float = 0.0
    damage_formula: Callable[[float, float], float] | None = None
    battle_model: int = 0
    caster_mask: int = 0
    ability_kind: int = 0
    init_effects: tuple[int, ...] = ()
    action_tags: int = 0
    allow_dead_target: bool = False
    forbid_copy: bool = False
    season_id: int = 0
    max_battle_uses: int = 0
    health_cost_mode: int = 0
    secondary_target_policy: int = 0
    exclusive_group: int = 0

    @property
    def castable(self) -> bool:
        if self.battle_model == 1:
            return self.ability_kind == 1
        return not self.passive_stat and not {0, 2, 3}.intersection(self.skill_types)

    @property
    def damage_kind(self) -> int:
        return int(self.physical) if self.damage_type is None else self.damage_type

    def damage(self, level: int) -> float:
        if self.damage_formula is not None:
            return self.damage_formula(level, 0.0)
        return self.base_damage + self.damage_per_level * level


@dataclass(frozen=True, slots=True)
class BattleItem:
    heal_health: int = 0
    heal_mana: int = 0
    usable: bool = True


@dataclass(frozen=True, slots=True)
class BattleBuff:
    buff_type: int = 0
    no_caster: bool = False
    tags: frozenset[str] = frozenset()
    immune_tags: frozenset[str] = frozenset()
    dispel_tags: frozenset[str] = frozenset()
    max_layer: int = 0
    duration_rounds: int = 0
    infinite: bool = False
    interval_rounds: int = 0
    interval_count: int = 0
    interval_damage: float | None = None
    health_regeneration: Callable[[float, float], float] | None = None
    sub_buffs: tuple[int, ...] = ()
    target_sub_buffs: tuple[int, ...] = ()
    battle_op: int = 0
    damage_filter: int = 0
    modifier_stat: int = 0
    ratio: float = 0.0
    state_kind: int = 0
    trigger_event: int = 0
    effect_target: int = 0
    flat_value: float = 0.0
    per_level: float = 0.0
    value_basis: int = 0
    config_duration_rounds: int = 0
    trigger_chance: float = 0.0
    per_round_limit: int = 0
    hit_limit: int = 0
    application_chance: float = 0.0
    required_tag: str = ""
    second_required_tag: str = ""
    stack_limit: int = 0
    start_round: int = 0
    require_living_caster: bool = False


class BattleTables:
    """Adapt the same generated rows that the C++ battle provider consumes."""

    def __init__(self, protocol: Protocol, tables: ConfigTables, *, rewards_path: str | None = None):
        self.protocol = protocol
        self.tables = tables
        self.fingerprint = battle_table_fingerprint(tables)
        self.items = {row.id: BattleItem(row.battle_heal_hp, row.battle_heal_mp, bool(row.battle_usable))
                      for row in tables["Item"].rows()}
        self.skills = self._skills()
        self.buffs = {
            row.id: BattleBuff(
                buff_type=row.buff_type, no_caster=bool(row.no_caster), tags=frozenset(row.tag),
                immune_tags=frozenset(row.immune_tag), dispel_tags=frozenset(row.dispel_tag), max_layer=row.max_layer,
                duration_rounds=rounds_from_seconds(row.duration) if row.duration > 0 else 0,
                infinite=bool(row.infinite_duration), interval_rounds=rounds_from_seconds(row.interval) if row.interval > 0 else 0,
                interval_count=row.interval_count, interval_damage=next(iter(row.interval_effect), None),
                health_regeneration=_expression(row.health_regeneration), sub_buffs=tuple(row.sub_buff),
                target_sub_buffs=tuple(row.target_sub_buff),
                battle_op=int(getattr(row, "battle_op", 0)),
                damage_filter=int(getattr(row, "damage_filter", 0)),
                modifier_stat=int(getattr(row, "modifier_stat", 0)),
                ratio=float(getattr(row, "ratio", 0.0)),
                state_kind=int(getattr(row, "state_kind", 0)),
                trigger_event=int(getattr(row, "trigger_event", 0)),
                effect_target=int(getattr(row, "effect_target", 0)),
                flat_value=float(getattr(row, "flat_value", 0.0)),
                per_level=float(getattr(row, "per_level", 0.0)),
                value_basis=int(getattr(row, "value_basis", 0)),
                config_duration_rounds=int(getattr(row, "duration_rounds", 0)),
                trigger_chance=float(getattr(row, "trigger_chance", 0.0)),
                per_round_limit=int(getattr(row, "per_round_limit", 0)),
                hit_limit=int(getattr(row, "hit_limit", 0)),
                application_chance=float(getattr(row, "application_chance", 0.0)),
                required_tag=str(getattr(row, "required_tag", "")),
                second_required_tag=str(getattr(row, "second_required_tag", "")),
                stack_limit=int(getattr(row, "stack_limit", 0)),
                start_round=int(getattr(row, "start_round", 0)),
                require_living_caster=bool(getattr(row, "require_living_caster", 0)),
            ) for row in tables["Buff"].rows()
        }
        self.silence_permissions = {
            row.id: tuple(row.skill_type) for row in tables["SkillPermission"].rows()
        }.get(1)
        resource = Path(rewards_path) if rewards_path else files("xuanming").joinpath("contracts", "battle_rewards.json")
        with resource.open("rb") as stream:
            config = stream.read((1 << 20) + 1)
        if not 0 < len(config) <= 1 << 20:
            raise ValueError("battle rewards config is empty or exceeds 1 MiB")
        parsed = Parse(config.decode("utf-8"), protocol.message("BattleRewardConfig"))
        self.rewards = self._reward_profiles(parsed)

    def _reward_profiles(self, parsed: Message) -> dict[int, Message]:
        if not 0 < len(parsed.profiles) <= 256:
            raise ValueError("invalid battle reward profile count")
        known_currencies = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType").values_by_number
        profiles = {}
        for profile in parsed.profiles:
            key = profile.battle_config_id, profile.strategy_key
            if (key in profiles or len(profile.strategy_key.encode("utf-8")) > 64 or profile.item_draws > 100
                    or len(profile.random_items) > 256 or (profile.item_draws and not profile.random_items)):
                raise ValueError("invalid or duplicate battle reward profile")
            currencies = set()
            for currency in profile.currencies:
                if (currency.currency_type in currencies or currency.currency_type not in known_currencies
                        or currency.amount > (1 << 63) - 1):
                    raise ValueError("invalid or duplicate battle reward currency")
                currencies.add(currency.currency_type)
            items = set()
            for reward in profile.random_items:
                item = self.tables["Item"].get(reward.item_table_id)
                if (not reward.item_table_id or reward.item_table_id in items or item is None
                        or not item.max_stack_size or not reward.count or not reward.weight
                        or reward.count * profile.item_draws >= 1 << 32):
                    raise ValueError("invalid or duplicate battle reward item")
                items.add(reward.item_table_id)
            profiles[key] = profile
        if (0, "") not in profiles:
            raise ValueError("battle rewards require a default profile")
        return {config: profile for (config, strategy), profile in profiles.items() if not strategy}

    def _skills(self) -> dict[int, BattleSkill]:
        result = {}
        for row in self.tables["Skill"].rows():
            cooldown = self.tables["Cooldown"].get(row.cooldown_id)
            legacy_cooldown = ceil(cooldown.duration / _ROUND_MS) if cooldown is not None else 0
            result[row.id] = BattleSkill(
                base_damage=0.0,
                mana_cost=sum(entry.cost_resource_cost for entry in row.cost_resource if entry.cost_resource_id == 1),
                cooldown_rounds=row.cooldown_rounds if (row.class_id or getattr(row, "battle_model", 0) == 1) else legacy_cooldown,
                attack_multiplier=max(0, row.attack_multiplier or 1.0), initiative=row.initiative,
                cooldown_group=row.cooldown_id, requires_target=bool(row.targeting_mode),
                class_id=row.class_id, unlock_level=row.unlock_level, max_rank=row.max_rank,
                skill_types=tuple(row.skill_type), target_modes=tuple(row.targeting_mode), effects=tuple(row.effect),
                damage_type=row.damage_type, target_team=row.target_team, max_targets=row.max_targets,
                pet_max_targets=int(getattr(row, "pet_max_targets", 0)),
                mana_percent=row.mana_percent, heal_multiplier=row.heal_multiplier, heal_per_level=row.heal_per_level,
                execute_heal_threshold=row.execute_heal_threshold, execute_heal_bonus=row.execute_heal_bonus,
                restore_mana_percent=row.restore_mana_percent, dispel_count=row.dispel_count,
                passive_stat=row.passive_stat, passive_percent=row.passive_percent, buff_percent=row.buff_percent,
                damage_formula=_expression(row.damage),
                battle_model=int(getattr(row, "battle_model", 0)),
                caster_mask=int(getattr(row, "caster_mask", 0)),
                ability_kind=int(getattr(row, "ability_kind", 0)),
                init_effects=tuple(getattr(row, "init_effect", ())),
                action_tags=int(getattr(row, "action_tags", 0)),
                allow_dead_target=bool(getattr(row, "allow_dead_target", 0)),
                forbid_copy=bool(getattr(row, "forbid_copy", 0)),
                season_id=int(getattr(row, "season_id", 0)),
                max_battle_uses=int(getattr(row, "max_battle_uses", 0)),
                health_cost_mode=int(getattr(row, "health_cost_mode", 0)),
                secondary_target_policy=int(getattr(row, "secondary_target_policy", 0)),
                exclusive_group=int(getattr(row, "exclusive_group", 0)),
            )
        return result

    def dungeon_meta(self, battle_config_id: int) -> tuple[int, int, int]:
        """返回副本表 `(boss_trial_kind, difficulty, round_limit)`。"""
        dungeon = self.tables["Dungeon"].get(battle_config_id)
        if dungeon is None:
            return 0, 0, 0
        return (
            int(getattr(dungeon, "boss_trial_kind", 0)),
            int(getattr(dungeon, "difficulty", 0)),
            int(getattr(dungeon, "round_limit", 0)),
        )

    def monsters(self, request: Message) -> Iterable[Message]:
        dungeon = self.tables["Dungeon"].get(request.battle_config_id)
        boss_trial_kind = int(getattr(dungeon, "boss_trial_kind", 0)) if dungeon is not None else 0
        is_boss_trial = (1 <= boss_trial_kind <= 4) or boss_trial_kind == 8
        is_boss_trial_or_rescue = is_boss_trial or boss_trial_kind == 5
        is_festival_beast = boss_trial_kind == 7
        attackers = sum(player.team_index == 0 for player in request.players)
        ids = [monster for monster in dungeon.monster if monster] if dungeon is not None else []
        if not ids:
            ids = [0] * len(request.players)
        elif not is_boss_trial_or_rescue and not is_festival_beast and not request.activity_context.kind:
            count = min(10, len(ids) * attackers)
            ids = [ids[index % len(ids)] for index in range(count)]
        # 首领试炼 V2 精简精英阵：单卫 2..3 人加破军卫(109)、4..5 人加鼓噪卫(110)；双卫 3..5 人加破军卫(109)。
        if is_boss_trial:
            if boss_trial_kind == 8:
                if attackers >= 3 and self.tables["Monster"].get(109) is not None:
                    ids.append(109)
            else:
                if attackers >= 2 and self.tables["Monster"].get(109) is not None:
                    ids.append(109)
                if attackers >= 4 and self.tables["Monster"].get(110) is not None:
                    ids.append(110)
        hp_scale = max(1, attackers) if is_boss_trial_or_rescue else 1
        level = max(1, max(player.level for player in request.players))
        for monster_id in ids:
            row = self.tables["Monster"].get(monster_id)
            attributes = self.protocol.message("BaseAttributesComp", health=300, strength=5, armor=24, speed=60)
            is_boss = False
            if row is not None and row.health:
                for field_name in ("health", "strength", "armor", "resistance", "critchance"):
                    setattr(attributes, field_name, getattr(row, field_name))
                attributes.speed = row.speed or 60
                is_boss = bool(getattr(row, "is_boss", 0))
            base_hp = attributes.health
            is_support_guard = monster_id in (109, 110)
            if not is_festival_beast:
                if boss_trial_kind == 8 and not is_boss and not is_support_guard:
                    base_hp = max(1, (base_hp * 75) // 100)
                unit_hp_scale = max(1, (attackers + 1) // 2) if is_support_guard else hp_scale
                scaled_hp = base_hp * unit_hp_scale
                if scaled_hp != attributes.health:
                    attributes.health = scaled_hp
            actor = self.protocol.message("BattleActorState", level=level, monster_table_id=monster_id,
                                          is_boss=is_boss, attributes=attributes, max_health=attributes.health)
            actor.name = "野怪"
            yield actor

    def max_rounds(self, request: Message, default: int) -> int:
        dungeon = self.tables["Dungeon"].get(request.battle_config_id)
        if dungeon is not None:
            round_limit = int(getattr(dungeon, "round_limit", 0))
            if round_limit > 0:
                return round_limit
            if dungeon.time_limit:
                return ceil(dungeon.time_limit * 1000 / _ROUND_MS)
        return default

    def reward_profile(self, battle_config_id: int) -> Message:
        profile = self.rewards.get(battle_config_id, self.rewards.get(0))
        if profile is None:
            raise ValueError("PVE reward profile unavailable")
        return type(profile).FromString(profile.SerializeToString())

    def add_rewards(self, request: Message, settlement: Message, *, profile: Message | None = None) -> None:
        if profile is None:
            profile = self.reward_profile(request.battle_config_id)
        settlement.exp_gain = profile.exp_gain
        settlement.currency_gains.extend(profile.currencies)
        total_weight = sum(item.weight for item in profile.random_items)
        counts: Counter[int] = Counter()
        rng = _Mt19937_64(_mix_seed(request.seed ^ _mix_seed(settlement.player_id)))
        if profile.item_draws and total_weight == 0:
            raise ValueError("PVE reward profile has no positive draw weight")
        for _ in range(profile.item_draws):
            threshold = ((1 << 64) - total_weight) % total_weight
            value = rng.next()
            while value < threshold:
                value = rng.next()
            roll = value % total_weight
            for item in profile.random_items:
                if roll < item.weight:
                    counts[item.item_table_id] += item.count
                    break
                roll -= item.weight
        for item_id, count in sorted(counts.items()):
            settlement.items_gained.add(item_table_id=item_id, count=count)


def _mix_seed(value: int) -> int:
    value = (value + 0x9E3779B97F4A7C15) & _MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _MASK64
    return value ^ (value >> 31)


