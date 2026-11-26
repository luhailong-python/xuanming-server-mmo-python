"""宠物列表、出战、加点、商店购买与独立条目池，写操作随角色存档提交。"""

import logging
from secrets import randbelow
from time import time

from ..data.asset_inventory import (
    BAG_INSUFFICIENT, FEATURE_UNAVAILABLE, INSUFFICIENT, INVALID, INVALID_DATA, UNAVAILABLE,
    balance, carries_unknown, clone,
)
from ..data.asset_ledger import INT64_MAX, UINT64_MAX
from ..data.attribute_allocation import AllocationError, distribute_points, evaluate_pool, validate_allocation
from .scene_mission import effective_level
from .scene_skills import _rescale
from .scene_stats import (
    UINT32_MAX,
    is_turn_battle_castable,
    pet_dimensions,
    pet_level,
    pet_pool,
    pet_skills,
    pet_stats,
    rank_limit,
)

logger = logging.getLogger(__name__)

_SERVICE = "ScenePetClientPlayer"
_NOT_FOUND, _ROW_MISSING, _FULL, _LEVEL_LOW, _ACTIVE, _NOT_ACTIVE = range(26000, 26006)
_INVALID_NAME, _IN_BATTLE, _POINTS_LOW, _CANNOT_DECREASE, _CAP, _UNKNOWN_DIM = range(26007, 26013)
_GOLD_LOW, _NO_AUTO_PLAN, _NO_CHANGE, _ID_UNAVAILABLE = range(26013, 26017)
PET_NOT_TRANSFERABLE = 26018
PET_TIER_MAX_REACHED = 26019
PET_TIER_CHANGED = 26020
PET_TIER_LEVEL_NOT_ENOUGH = 26021
PET_TIER_MATERIAL_NOT_ENOUGH = 26022
PET_ENTRY_PRESET_INVALID = 26023
_MAX_PRESET_SLOTS = 3
_MAX_PRESET_TAG = 3
_KEEP_PRESET_TAG = 0xFFFFFFFF
_MAX_CULTIVATION_TIER = 5
_DEFAULT_ENTRY_CONFIG_VERSION = 1
_DEFAULT_ENTRY_TOTAL_POINTS = 20
_DEFAULT_ENTRY_MAX_DIRECTIONS = 0
_REQUEST_ID_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_ALLOC_ERRORS = {
    AllocationError.POOL_LOCKED: _LEVEL_LOW,
    AllocationError.DIMENSION_NOT_IN_POOL: _UNKNOWN_DIM,
    AllocationError.CANNOT_DECREASE: _CANNOT_DECREASE,
    AllocationError.CAP_EXCEEDED: _CAP,
    AllocationError.NOT_ENOUGH_POINTS: _POINTS_LOW,
    AllocationError.NOTHING_TO_CHANGE: _NO_CHANGE,
}
_INVISIBLE = {0x20, 0xA0, 0x2028, 0x2029, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF}


def _visible_name(name, limit):
    if not 1 <= len(name) <= limit:
        return False
    points = tuple(map(ord, name))
    return (all(point >= 0x20 and point != 0x7F for point in points)
            and any(point not in _INVISIBLE and not 0x2000 <= point <= 0x200F for point in points))


def _find(comp, pet_id):
    return next((pet for pet in comp.pets if pet.pet_id == pet_id), None)


def _pet_shop_price(row) -> int:
    if "shop_price_gold" in row.DESCRIPTOR.fields_by_name:
        return row.shop_price_gold
    return 20_000 if row.quality >= 2 else 1_000


def _pet_acquisition_tier(row) -> int:
    return getattr(row, "acquisition_tier", 0)


def _pet_cultivation_tier(row) -> int:
    return getattr(row, "cultivation_tier", 1) or 1


def _pet_transfer_policy(row) -> int:
    return int(getattr(row, "transfer_policy", 0))


def _pet_exclusive_gift_slots(row) -> int:
    return int(getattr(row, "exclusive_gift_slots", 0))


def _pet_fixed_initial_aptitude(row) -> bool:
    return bool(getattr(row, "fixed_initial_aptitude", False))


def _pet_data_known(comp) -> bool:
    if carries_unknown(comp):
        return False
    ids = set()
    for pet in comp.pets:
        if carries_unknown(pet) or not pet.pet_id or not pet.pet_table_id or not pet.level or pet.pet_id in ids:
            return False
        ids.add(pet.pet_id)
    return not comp.active_pet_id or comp.active_pet_id in ids


def _next_purchase_sequence(comp) -> int:
    return 0 if comp.last_purchase_sequence >= UINT64_MAX else comp.last_purchase_sequence + 1


def _entry_state_version(value: int) -> int:
    return value or 1


def _valid_entry_request_id(request_id: str) -> bool:
    return 16 <= len(request_id) <= 64 and all(ch in _REQUEST_ID_CHARS for ch in request_id)


def _valid_preset_slot(slot_id: int) -> bool:
    return 1 <= slot_id <= _MAX_PRESET_SLOTS


def _valid_preset_tag(tag: int) -> bool:
    return tag <= _MAX_PRESET_TAG or tag == _KEEP_PRESET_TAG


def _effective_preset_slot(stored: int) -> int:
    return stored if _valid_preset_slot(stored) else 1


def _effective_cultivation_tier(tables, pet) -> int:
    tier = int(pet.cultivation_tier)
    if not tier:
        species = tables["Pet"].get(pet.pet_table_id)
        if species is not None:
            tier = _pet_cultivation_tier(species)
    return tier or 1


def _effective_budget_for_pet(tables, pet, base_budget: int) -> int:
    bonus = 0
    if "PetTier" in tables.tables:
        tier_row = tables["PetTier"].get(_effective_cultivation_tier(tables, pet))
        if tier_row is not None:
            bonus = int(getattr(tier_row, "bonus_entry_points", 0))
    return base_budget + bonus


def _find_preset(pet, slot_id: int):
    return next((preset for preset in pet.entry_presets if preset.slot_id == slot_id), None)


def _find_or_create_preset(pet, slot_id: int):
    existing = _find_preset(pet, slot_id)
    if existing is not None:
        return existing
    return pet.entry_presets.add(slot_id=slot_id, tag=0)


def _sync_active_preset_from_allocated(pet, active_slot: int) -> None:
    preset = _find_or_create_preset(pet, active_slot)
    preset.allocated.clear()
    for entry_id, points in pet.entry_allocated.items():
        if points > 0:
            preset.allocated[entry_id] = points


def _inventory_total_count(data, config_id: int) -> int:
    if data is None or not config_id:
        return 0
    total = 0
    for entry in data.bag_component.items:
        if entry.bag_type != 0:
            continue
        if entry.HasField("instance"):
            if entry.instance.config_id == config_id:
                total += entry.instance.size
        elif entry.config_id == config_id:
            total += entry.stack_size
    return total


def _accumulate_pet_entry_effects(tables, pet, effects) -> bool:
    """将单只灵宠的生效培养条目效果汇总至 PetCultivationEffects，供战斗快照复用。"""
    if "PetEntry" not in tables.tables or "PetRule" not in tables.tables:
        return False
    rule = next(iter(tables["PetRule"].rows()), None)
    if rule is None:
        return False
    fields = rule.DESCRIPTOR.fields_by_name
    base_budget = rule.entry_total_points if "entry_total_points" in fields else _DEFAULT_ENTRY_TOTAL_POINTS
    max_directions = rule.entry_max_directions if "entry_max_directions" in fields else _DEFAULT_ENTRY_MAX_DIRECTIONS
    rules: dict[int, tuple[int, int]] = {}
    for row in tables["PetEntry"].rows():
        invalid_row = (
            not row.id or not row.cap or not 1 <= row.channel <= 9
            or not row.per_point_bps or row.cap * row.per_point_bps > 10_000
            or (row.channel in (5, 6) and not 1 <= row.state_kind <= 7)
        )
        if invalid_row:
            return False
        rules[row.id] = (row.cap, row.per_point_bps)
    if not rules or not base_budget:
        return False
    budget = _effective_budget_for_pet(tables, pet, base_budget)
    values = dict(pet.entry_allocated)
    known_values = {eid: pts for eid, pts in values.items() if eid in rules}
    effective = _validate_entries(values, known_values, rules, budget, max_directions) == 0
    effects.Clear()
    for row in sorted(tables["PetEntry"].rows(), key=lambda r: (r.sort, r.id)):
        points = values.get(row.id, 0)
        bps = points * row.per_point_bps if effective else 0
        if bps:
            _accumulate_entry_effect(effects, row, bps)
    return True


def build_single_pet_battle_snapshot(
    tables,
    player_id: int,
    pet,
    owner_level: int,
    *,
    full_resources: bool,
    out,
    pet_owned_bits: bytes = b"",
) -> bool:
    """构建单只灵宠的 BattlePetSnapshot（出战主宠与论剑 V3 备选灵宠共用同一口径）。"""
    from .scene_appearance import resolve_pet_cosmetic_visual

    row = tables["Pet"].get(pet.pet_table_id)
    if row is None or owner_level < pet_level(pet.level, row):
        return False
    if not full_resources and pet.health == 0:
        return False
    stats = pet_stats(tables, pet)
    if not stats:
        return False
    eff_level = pet_level(pet.level, row)
    if not _accumulate_pet_entry_effects(tables, pet, out.pet_cultivation):
        out.ClearField("pet_cultivation")
    out.pet_id = pet.pet_id
    out.owner_player_id = player_id
    out.pet_name = pet.name or row.name
    out.pet_table_id = pet.pet_table_id
    out.level = eff_level
    cosmetic_id, cosmetic_art = resolve_pet_cosmetic_visual(tables, pet_owned_bits, pet)
    out.cosmetic_id = cosmetic_id
    out.cosmetic_art_key = cosmetic_art
    out.max_health = stats["max_health"]
    out.max_mana = stats["max_mana"]
    out.physical_attack = stats["physical_attack"]
    out.magic_attack = stats["magic_attack"]
    out.defense = stats["defense"]
    out.base_attributes.health = stats["max_health"] if full_resources else min(pet.health, stats["max_health"])
    out.base_attributes.mana = (
        stats["max_mana"] if full_resources
        else (min(pet.mana, stats["max_mana"]) if stats["max_mana"] else pet.mana)
    )
    out.base_attributes.speed = stats["speed"]
    stored_ranks = dict(pet.skill_ranks)
    for skill_id in pet_skills(pet, row):
        skill = tables["Skill"].get(skill_id)
        if skill is None:
            continue
        if is_turn_battle_castable(skill):
            out.skill_table_ids.append(skill_id)
        if getattr(skill, "battle_model", 0) == 1:
            stored_rank = stored_ranks.get(skill_id, 1)
            out.skill_ranks[skill_id] = min(stored_rank, rank_limit(eff_level, skill))
    return True


def build_standby_battle_snapshots(
    tables,
    player_id: int,
    data,
    exclude_pet_id: int,
    max_count: int,
    *,
    full_resources: bool,
    protocol,
) -> list:
    """收集除首发灵宠外至多 max_count 只合规备选灵宠快照，供论剑 V3 战前 15 秒明牌变阵使用。"""
    if max_count <= 0 or not data.HasField("pet_component") or not data.HasField("level_component"):
        return []
    owner_level = effective_level(data)
    pet_owned_bits = bytes(data.profile_component.pet_cosmetic_owned_bits) if data.HasField("profile_component") else b""
    result = []
    for pet in data.pet_component.pets:
        if len(result) >= max_count:
            break
        if not pet.pet_id or pet.pet_id == exclude_pet_id:
            continue
        snap = protocol.message("BattlePetSnapshot")
        if build_single_pet_battle_snapshot(
            tables, player_id, pet, owner_level, full_resources=full_resources, out=snap, pet_owned_bits=pet_owned_bits,
        ):
            result.append(snap)
    return result


def _validate_entries(stored: dict[int, int], target: dict[int, int], rules: dict[int, tuple[int, int]],
                      budget: int, max_directions: int) -> int:
    used = sum(points for entry_id, points in stored.items() if entry_id not in rules)
    directions = 0
    for entry_id, points in target.items():
        rule = rules.get(entry_id)
        if rule is None:
            return _UNKNOWN_DIM
        if points > rule[0]:
            return _CAP
        used += points
        if points > 0:
            directions += 1
    if used > budget:
        return _POINTS_LOW
    if max_directions > 0 and directions > max_directions:
        return INVALID
    return 0


def _replace_known_entries(stored: dict[int, int], target: dict[int, int],
                           rules: dict[int, tuple[int, int]]) -> dict[int, int]:
    result = {entry_id: points for entry_id, points in stored.items() if entry_id not in rules}
    for entry_id, points in target.items():
        if entry_id in rules and points > 0:
            result[entry_id] = points
    return result


def _accumulate_entry_effect(effects, row, bps: int) -> None:
    channel = row.channel
    if channel == 1:
        effects.physical_damage_bps += bps
    elif channel == 2:
        effects.magic_damage_bps += bps
    elif channel == 3:
        effects.outgoing_heal_bps += bps
    elif channel == 4:
        effects.shield_capacity_bps += bps
    elif channel == 5:
        effects.ailment_hit_bps[row.state_kind] = effects.ailment_hit_bps.get(row.state_kind, 0) + bps
    elif channel == 6:
        effects.ailment_resist_bps[row.state_kind] = effects.ailment_resist_bps.get(row.state_kind, 0) + bps
    elif channel == 7:
        effects.poison_damage_bps += bps
    elif channel == 8:
        effects.retreat_hit_bps += bps
    elif channel == 9:
        effects.retreat_resist_bps += bps


class ScenePets:
    def __init__(self, scene, tables):
        self.scene, self.tables = scene, tables
        self.protocol, self.assets = scene.protocol, getattr(scene, "assets", None)
        self.rule = next(iter(tables["PetRule"].rows()), None)
        self.max_pets = (self.rule.max_pets if self.rule is not None else 0) or 1
        self.rename_cost = self.rule.rename_cost_gold if self.rule is not None else 0
        self.name_limit = (self.rule.name_max_len if self.rule is not None else 0) or 8
        self.pool = pet_pool(tables)
        self.dimensions = pet_dimensions(tables, self.pool) if self.pool is not None else ()
        self._editors = {
            "SummonPet": self._summon,
            "RecallPet": self._recall,
            "AllocatePetPoints": self._allocate,
            "ResetPetPoints": self._reset,
            "RenamePet": self._rename,
        }
        currencies = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        self.gold = currencies.values_by_name["CURRENCY_TYPE_GOLD"].number
        tx_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self.debit_tx = tx_types.values_by_name["TX_CURRENCY_DEDUCT"].number
        self.shop_buy_tx = tx_types.values_by_name["TX_SHOP_BUY"].number

    def register(self):
        self.scene.register(_SERVICE, "GetPetList", self.get_list)
        self.scene.register(_SERVICE, "AutoAllocatePetPoints", self.auto_allocate)
        self.scene.register(_SERVICE, "GmGrantPet", self.grant, mutating=True)
        for method in self._editors:
            self.scene.register(_SERVICE, method, self.change, mutating=True)
        self.scene.register(_SERVICE, "GetPetShop", self.get_shop)
        self.scene.register(_SERVICE, "BuyPet", self.buy_pet, mutating=True)
        self.scene.register(_SERVICE, "GetPetEntries", self.get_entries)
        self.scene.register(_SERVICE, "PreviewPetEntries", self.preview_entries, mutating=True)
        self.scene.register(_SERVICE, "SavePetEntries", self.save_entries, mutating=True)
        self.scene.register(_SERVICE, "SwitchPetEntryPreset", self.switch_entry_preset, mutating=True)
        self.scene.register(_SERVICE, "AscendPetTier", self.ascend_tier, mutating=True)

    def _allocation(self, pet, level):
        current = {dim.id: pet.allocated.get(dim.id, 0) for dim in self.dimensions}
        foreign = sum(points for dimension, points in pet.allocated.items() if dimension not in current)
        return current, evaluate_pool(self.pool, level, current, foreign=foreign)

    def _reset_cost(self, level, allocation):
        return self.pool.reset_cost_gold if allocation.effective and level >= self.pool.reset_free_below_level else 0

    def _fill_list(self, comp, listing, *, player=None, data=None):
        listing.active_pet_id = comp.active_pet_id
        listing.max_pets, listing.rename_cost_gold = self.max_pets, self.rename_cost
        if self.pool is None:
            return
        from .scene_appearance import resolve_pet_cosmetic_visual
        pet_owned_bits = (
            bytes(data.profile_component.pet_cosmetic_owned_bits)
            if data is not None and data.HasField("profile_component")
            else b""
        )
        for pet in comp.pets:
            row = self.tables["Pet"].get(pet.pet_table_id)
            level = pet_level(pet.level, row)
            current, allocation = self._allocation(pet, level)
            cosmetic_id, cosmetic_art = resolve_pet_cosmetic_visual(self.tables, pet_owned_bits, pet)
            info = listing.pets.add(
                pet_id=pet.pet_id, pet_table_id=pet.pet_table_id, level=level,
                name=pet.name or (row.name if row is not None else ""), is_active=comp.active_pet_id == pet.pet_id,
                pool_id=self.pool.id, total_points=allocation.total, remaining_points=allocation.remaining,
                reset_cost_gold=self._reset_cost(level, allocation),
                cosmetic_id=cosmetic_id, cosmetic_art_key=cosmetic_art,
            )
            if row is not None:
                info.quality, info.model_id, info.desc = row.quality, row.model_id, row.desc
                info.skill_table_ids.extend(pet_skills(pet, row))
            self._fill_cultivation_tier(pet, info)
            self._fill_tier_ascension(player, data, pet, level, info)
            if self._build_entries_info(pet, info.entries):
                info.ClearField("entries")
            aptitude_total = 0
            for dim in self.dimensions:
                aptitude = pet.aptitude.get(dim.id, 10_000)
                allocated = current[dim.id]
                aptitude_total += aptitude or 10_000
                info.dimensions.add(
                    dimension_id=dim.id, pool_id=dim.pool_id, name=dim.name, desc=dim.desc,
                    allocated=allocated, value=dim.base_per_level * level + (allocated if allocation.effective else 0),
                    cap=self.pool.dimension_cap, sort=dim.sort, aptitude=aptitude,
                )
            info.growth = aptitude_total // len(self.dimensions) if self.dimensions else 10_000
            stats = pet_stats(self.tables, pet) if row is not None else None
            if stats is not None:
                for key, amount in stats.items():
                    setattr(info.derived, key, amount)
            info.derived.health, info.derived.mana = pet.health, pet.mana

    def _reply(self, route, error=0, data=None, *, player=None, **fields):
        reply = self.protocol.message(route.response_type, **fields)
        if error:
            reply.error_message.id = error
        elif data is not None:
            self._fill_list(data.pet_component, reply.pets, player=player, data=data)
        return reply

    def get_list(self, _world, player, route, _request):
        data = self.scene.rows.assemble(player.snapshot)
        return self._reply(route, 0 if data.HasField("pet_component") else UNAVAILABLE, data, player=player)

    def notify(self, player):
        data = self.scene.rows.assemble(player.snapshot)
        if data.HasField("pet_component"):
            message = self.protocol.message("PetListChangedS2C")
            self._fill_list(data.pet_component, message.pets, player=player, data=data)
            self.scene._push(player, "ScenePetClientPlayerNotifyPetListChanged", message)

    def initialize_on_load(self, data):
        comp = data.pet_component
        comp.SetInParent()
        if comp.active_pet_id and _find(comp, comp.active_pet_id) is None:
            comp.active_pet_id = 0
        for pet in comp.pets:
            if not pet.cultivation_tier:
                row = self.tables["Pet"].get(pet.pet_table_id)
                if row is not None:
                    pet.cultivation_tier = _pet_cultivation_tier(row)
        self.recalculate_all(data, level_changed=False)

    def recalculate_all(self, data, *, level_changed=False):
        if self.pool is None or not data.HasField("level_component"):
            return
        comp = data.pet_component
        owner_level = effective_level(data)
        for pet in comp.pets:
            row = self.tables["Pet"].get(pet.pet_table_id)
            if row is None:
                continue
            old = pet_stats(self.tables, pet)
            self._recalculate_one(pet, row, owner_level, old, level_changed=level_changed)
            if level_changed and comp.active_pet_id == pet.pet_id and owner_level < pet_level(pet.level, row):
                comp.active_pet_id = 0

    def _recalculate_one(self, pet, row, owner_level, old, *, level_changed=False):
        pet.level = max(pet.level, pet_level(owner_level, row))
        new = pet_stats(self.tables, pet)
        if level_changed:
            if pet.health and old["max_health"]:
                pet.health = min(new["max_health"], pet.health + max(0, new["max_health"] - old["max_health"]))
            if new["max_mana"]:
                pet.mana = min(new["max_mana"], pet.mana + (max(0, new["max_mana"] - old["max_mana"])
                                                           if old["max_mana"] else 0))
        else:
            pet.health = _rescale(pet.health, old["max_health"], new["max_health"])
            pet.mana = _rescale(pet.mana, old["max_mana"], new["max_mana"])

    def _writable(self, player):
        if not self.scene.mutable(player):
            return INVALID
        return _IN_BATTLE if self.scene.battle and self.scene.battle.frozen(player.player_id) else 0

    async def change(self, _world, player, route, request):
        method = route.method
        if method == "AllocatePetPoints" and (not request.pet_id or not request.allocated):
            return self._reply(route, INVALID)
        if error := self._writable(player):
            return self._reply(route, error)
        if method in {"AllocatePetPoints", "ResetPetPoints"} and self.pool is None:
            return self._reply(route, INVALID_DATA)
        if method == "RenamePet" and not _visible_name(request.name, self.name_limit):
            return self._reply(route, _INVALID_NAME)
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component"):
            return self._reply(route, UNAVAILABLE)
        if method in {"SummonPet", "AllocatePetPoints", "ResetPetPoints"} and not data.HasField("level_component"):
            return self._reply(route, UNAVAILABLE)
        fee_operation = method in {"ResetPetPoints", "RenamePet"}
        before = clone(data) if fee_operation and self.assets.audit_enabled else None
        if error := self._editors[method](data, request):
            return self._reply(route, error)
        audit = self.assets.make_audit_callback(player, before, data, self.debit_tx) if before is not None else None
        await self.scene.commit_player_data(player, data, on_committed=audit)
        return self._reply(route, data=data, player=player)

    def _summon(self, data, request):
        comp = data.pet_component
        pet = _find(comp, request.pet_id)
        if pet is None:
            return _NOT_FOUND
        if comp.active_pet_id == pet.pet_id:
            return _ACTIVE
        row = self.tables["Pet"].get(pet.pet_table_id)
        if row is None:
            return _ROW_MISSING
        if effective_level(data) < max(row.unlock_level, pet_level(pet.level, row)):
            return _LEVEL_LOW
        comp.active_pet_id = pet.pet_id
        return 0

    @staticmethod
    def _recall(data, _request):
        if not data.pet_component.active_pet_id:
            return _NOT_ACTIVE
        data.pet_component.active_pet_id = 0
        return 0

    def _allocate(self, data, request):
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            return _NOT_FOUND
        row = self.tables["Pet"].get(pet.pet_table_id)
        if row is None:
            return _ROW_MISSING
        level = pet_level(pet.level, row)
        current, allocation = self._allocation(pet, level)
        error = validate_allocation(self.pool, level, current, request.allocated, allocation.remaining)
        if error is not AllocationError.OK:
            return _ALLOC_ERRORS[error]
        old = pet_stats(self.tables, pet)
        for dim, value in request.allocated.items():
            if value != current[dim]:
                pet.allocated[dim] = value
        self._recalculate_one(pet, row, effective_level(data), old)
        return 0

    def _deduct(self, data, cost):
        if not cost:
            return 0
        error = self.assets.deduct_currency(data, self.gold, cost)
        return _GOLD_LOW if error == INSUFFICIENT else error

    def _reset(self, data, request):
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            return _NOT_FOUND
        row = self.tables["Pet"].get(pet.pet_table_id)
        if row is None:
            return _ROW_MISSING
        level = pet_level(pet.level, row)
        current, allocation = self._allocation(pet, level)
        if not allocation.known_used:
            return _NO_CHANGE
        if error := self._deduct(data, self._reset_cost(level, allocation)):
            return error
        old = pet_stats(self.tables, pet)
        for dim in current:
            if dim in pet.allocated:
                del pet.allocated[dim]
        self._recalculate_one(pet, row, effective_level(data), old)
        return 0

    def auto_allocate(self, _world, player, route, request):
        if self.pool is None:
            return self._reply(route, INVALID_DATA)
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component"):
            return self._reply(route, UNAVAILABLE)
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            return self._reply(route, _NOT_FOUND)
        plan = next((row for row in self.tables["AttributeAutoPlan"].rows()
                     if row.class_id == 0 and row.pool_id == self.pool.id), None)
        if plan is None or not plan.dimension:
            return self._reply(route, _NO_AUTO_PLAN)
        row = self.tables["Pet"].get(pet.pet_table_id)
        suggested, allocation = self._allocation(pet, pet_level(pet.level, row))
        if not allocation.remaining:
            return self._reply(route, _NO_CHANGE)
        choices = [(dim, plan.weight[i] if i < len(plan.weight) else 1)
                   for i, dim in enumerate(plan.dimension) if dim in suggested]
        distribute_points(self.pool, [dim for dim, _ in choices], [weight for _, weight in choices],
                          allocation.remaining, suggested)
        return self._reply(route, pet_id=pet.pet_id, suggested=suggested)

    def _rename(self, data, request):
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            return _NOT_FOUND
        row = self.tables["Pet"].get(pet.pet_table_id)
        current = pet.name or (row.name if row is not None else "")
        if current == request.name:
            return _NO_CHANGE
        if error := self._deduct(data, self.rename_cost):
            return error
        pet.name = request.name
        return 0

    def is_transferable_species(self, pet_table_id: int) -> bool:
        """判断宠物种类是否允许流转/交易（transfer_policy == 0）。"""
        row = self.tables["Pet"].get(pet_table_id)
        return row is not None and _pet_transfer_policy(row) == 0

    def is_exclusive_gift_species(self, pet_table_id: int) -> bool:
        """判断宠物种类是否走活动专属赠送位（不占普通 max_pets）。"""
        row = self.tables["Pet"].get(pet_table_id)
        return row is not None and _pet_exclusive_gift_slots(row) > 0

    def count_normal_pets(self, comp) -> int:
        """统计普通携带位中的宠物数量（排除活动专属赠送位种类）。"""
        return sum(1 for pet in comp.pets if not self.is_exclusive_gift_species(pet.pet_table_id))

    @staticmethod
    def count_species_pets(comp, pet_table_id: int) -> int:
        return sum(1 for pet in comp.pets if pet.pet_table_id == pet_table_id)

    def has_normal_pet_slot(self, comp) -> bool:
        return self.count_normal_pets(comp) < self.max_pets

    def has_exclusive_gift_slot(self, comp, pet_table_id: int) -> bool:
        row = self.tables["Pet"].get(pet_table_id)
        if row is None:
            return False
        slots = _pet_exclusive_gift_slots(row)
        return slots > 0 and self.count_species_pets(comp, pet_table_id) < slots

    def available_capture_slots(self, comp) -> int:
        if not _pet_data_known(comp) or self.pool is None or carries_unknown(self.pool):
            return 0
        count = self.count_normal_pets(comp)
        return self.max_pets - count if count < self.max_pets else 0

    async def prepare_exclusive_activity_pet(self, data, pet_table_id: int):
        """专属活动神兽发放预备：校验专属赠送位与固定首发资质并分配永久 pet_id，不写组件。"""
        if not data.HasField("pet_component") or not data.HasField("level_component"):
            return UNAVAILABLE, None
        comp = data.pet_component
        if not _pet_data_known(comp):
            return UNAVAILABLE, None
        row = self.tables["Pet"].get(pet_table_id)
        if row is None:
            return _ROW_MISSING, None
        if carries_unknown(row) or self.pool is None or carries_unknown(self.pool):
            return INVALID_DATA, None
        if (
            not row.name
            or not row.model_id
            or _pet_acquisition_tier(row) != 2
            or _pet_cultivation_tier(row) != 4
            or _pet_transfer_policy(row) != 1
            or _pet_exclusive_gift_slots(row) == 0
            or not _pet_fixed_initial_aptitude(row)
            or len(row.aptitude_min) < 4
            or len(row.aptitude_max) < 4
            or any(int(row.aptitude_min[i]) == 0 or int(row.aptitude_min[i]) != int(row.aptitude_max[i]) for i in range(4))
        ):
            return INVALID_DATA, None
        if not self.has_exclusive_gift_slot(comp, pet_table_id):
            return _FULL, None
        return await self._prepare_new_pet(comp, row, effective_level(data))

    @staticmethod
    def commit_prepared_activity_pet(data, candidate) -> None:
        """将预备好的专属活动神兽同步写入玩家宠物组件（与活动领取回执同帧提交）。"""
        data.pet_component.pets.add().CopyFrom(candidate)

    async def _prepare_new_pet(self, comp, row, owner_level: int):
        if self.assets is None or self.assets.allocate_item_id is None:
            return _ID_UNAVAILABLE, None
        try:
            pet_id = await self.assets.allocate_item_id()
        except (OSError, RuntimeError, ValueError):
            return _ID_UNAVAILABLE, None
        if type(pet_id) is not int or not 0 < pet_id < 1 << 63 or _find(comp, pet_id) is not None:
            return _ID_UNAVAILABLE, None
        pet = self.protocol.message(
            "PetInstance",
            pet_id=pet_id,
            pet_table_id=row.id,
            cultivation_tier=_pet_cultivation_tier(row),
            level=pet_level(owner_level, row),
            created_at=int(time()),
        )
        fixed_apt = _pet_fixed_initial_aptitude(row)
        for i, dim in enumerate(self.dimensions):
            low = row.aptitude_min[i] if i < len(row.aptitude_min) else 10_000
            high = row.aptitude_max[i] if i < len(row.aptitude_max) else 10_000
            low, high = sorted((low or high or 10_000, high or low or 10_000))
            pet.aptitude[dim.id] = low if (fixed_apt or low == high) else (low + randbelow(high - low + 1))
        stats = pet_stats(self.tables, pet)
        pet.health, pet.mana = stats["max_health"], stats["max_mana"]
        return 0, pet

    async def grant(self, _world, player, route, request):
        if self.assets is None or not self.assets.allow_gm:
            return self._reply(route, FEATURE_UNAVAILABLE)
        if error := self._writable(player):
            return self._reply(route, error)
        row = self.tables["Pet"].get(request.pet_table_id)
        if row is None:
            return self._reply(route, _ROW_MISSING)
        if _pet_transfer_policy(row) != 0 or _pet_exclusive_gift_slots(row) > 0:
            return self._reply(route, FEATURE_UNAVAILABLE)
        if self.pool is None:
            return self._reply(route, INVALID_DATA)
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component") or not data.HasField("level_component"):
            return self._reply(route, UNAVAILABLE)
        comp = data.pet_component
        if not self.has_normal_pet_slot(comp):
            return self._reply(route, _FULL)
        error, candidate = await self._prepare_new_pet(comp, row, effective_level(data))
        if error:
            return self._reply(route, error)
        comp.pets.add().CopyFrom(candidate)
        await self.scene.commit_player_data(player, data)
        return self._reply(route, data=data, player=player, pet_id=candidate.pet_id)

    # ── 宠物商店（GetPetShop / BuyPet） ──

    def get_shop(self, _world, player, _route, _request):
        """查询普通玩家可购宠物商店清单、当前银两与下一购买序号。"""
        response = self.protocol.message("GetPetShopResponse")
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component") or not _pet_data_known(data.pet_component):
            response.error_message.id = UNAVAILABLE
            return response
        if self.pool is None or carries_unknown(self.pool):
            response.error_message.id = INVALID_DATA
            return response
        for row in self.tables["Pet"].rows():
            price = _pet_shop_price(row)
            if (
                carries_unknown(row)
                or _pet_acquisition_tier(row) != 0
                or _pet_transfer_policy(row) != 0
                or _pet_exclusive_gift_slots(row) > 0
                or not 0 < price <= INT64_MAX
            ):
                continue
            response.offers.add(
                pet_table_id=row.id,
                name=row.name,
                model_id=row.model_id,
                unlock_level=row.unlock_level,
                price_gold=price,
                desc=row.desc,
            )
        response.gold = balance(data, self.gold)
        response.next_purchase_sequence = _next_purchase_sequence(data.pet_component)
        return response

    async def buy_pet(self, _world, player, _route, request):
        """普通玩家购买宠物：按单调购买序号幂等、核验报价与等级、扣银两并原子提交。"""
        response = self.protocol.message("BuyPetResponse")
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0, pet_id: int = 0):
            if error:
                response.error_message.id = error
            if data.HasField("pet_component"):
                response.next_purchase_sequence = _next_purchase_sequence(data.pet_component)
            response.gold = balance(data, self.gold)
            if not error:
                response.pet_id = pet_id
                self._fill_list(data.pet_component, response.pets, player=player, data=data)
            return response

        if error := self._writable(player):
            return finish(error)
        if not data.HasField("pet_component") or not _pet_data_known(data.pet_component):
            return finish(UNAVAILABLE)
        comp = data.pet_component
        seq = request.purchase_sequence
        if not seq:
            return finish(INVALID)
        if seq == comp.last_purchase_sequence:
            if (
                request.pet_table_id != comp.last_purchase_pet_table_id
                or request.price_gold != comp.last_purchase_price_gold
            ):
                return finish(INVALID)
            if not comp.last_purchase_pet_id:
                return finish(UNAVAILABLE)
            return finish(0, comp.last_purchase_pet_id)
        if seq != _next_purchase_sequence(comp):
            return finish(INVALID)

        row = self.tables["Pet"].get(request.pet_table_id)
        if row is None:
            return finish(_ROW_MISSING)
        if carries_unknown(row):
            return finish(INVALID_DATA)
        price = _pet_shop_price(row)
        if (
            _pet_acquisition_tier(row) != 0
            or _pet_transfer_policy(row) != 0
            or _pet_exclusive_gift_slots(row) > 0
            or price == 0
        ):
            return finish(FEATURE_UNAVAILABLE)
        if request.price_gold != price:
            return finish(INVALID)
        if price > INT64_MAX:
            return finish(INVALID_DATA)
        if self.pool is None or carries_unknown(self.pool):
            return finish(INVALID_DATA)
        if not data.HasField("level_component"):
            return finish(UNAVAILABLE)
        owner_level = effective_level(data)
        if owner_level < row.unlock_level:
            return finish(_LEVEL_LOW)
        if not self.has_normal_pet_slot(comp):
            return finish(_FULL)
        if balance(data, self.gold) < price:
            return finish(_GOLD_LOW)

        error, candidate = await self._prepare_new_pet(comp, row, owner_level)
        if error:
            return finish(error)
        before = clone(data) if self.assets is not None and self.assets.audit_enabled else None
        if self.assets is not None:
            deduct_err = self.assets.deduct_currency(data, self.gold, price)
            if deduct_err:
                return finish(_GOLD_LOW if deduct_err == INSUFFICIENT else deduct_err)
        comp.pets.add().CopyFrom(candidate)
        comp.last_purchase_sequence = seq
        comp.last_purchase_pet_table_id = request.pet_table_id
        comp.last_purchase_pet_id = candidate.pet_id
        comp.last_purchase_price_gold = price

        audit = (
            self.assets.make_audit_callback(
                player, before, data, self.shop_buy_tx, correlation=candidate.pet_id,
            )
            if before is not None else None
        )
        await self.scene.commit_player_data(player, data, on_committed=audit)
        logger.info(
            "Pet purchased player_id=%s pet_id=%s pet_table_id=%s price_gold=%s seq=%s",
            player.player_id, candidate.pet_id, request.pet_table_id, price, seq,
        )
        return finish(0, candidate.pet_id)

    # ── 宠物培养阶位与独立条目池（GetPetEntries / PreviewPetEntries / SavePetEntries） ──

    def _fill_cultivation_tier(self, pet, info) -> None:
        tier = _effective_cultivation_tier(self.tables, pet)
        info.cultivation_tier = tier
        if "PetTier" in self.tables.tables:
            tier_row = self.tables["PetTier"].get(tier)
            if tier_row is not None:
                info.cultivation_tier_name = tier_row.name
                info.cultivation_art_key = tier_row.art_key

    def _fill_tier_ascension(self, player, data, pet, eff_level: int, info) -> None:
        """填充宝宝下一灵阶突破预览（五阶满阶时清空 next_tier）。"""
        info.ClearField("next_tier")
        current_tier = _effective_cultivation_tier(self.tables, pet)
        if current_tier >= _MAX_CULTIVATION_TIER or "PetTier" not in self.tables.tables:
            return
        current_row = self.tables["PetTier"].get(current_tier)
        next_row = self.tables["PetTier"].get(current_tier + 1)
        if current_row is None or next_row is None:
            return
        out = info.next_tier
        out.next_tier = next_row.id
        out.next_tier_name = next_row.name
        out.next_art_key = next_row.art_key
        out.min_pet_level = int(getattr(current_row, "ascend_min_pet_level", 0))
        out.cost_item_id = int(getattr(current_row, "ascend_cost_item_id", 0))
        out.cost_item_count = int(getattr(current_row, "ascend_cost_item_count", 0))
        out.cost_gold = int(getattr(current_row, "ascend_cost_gold", 0))
        out.bonus_entry_points = int(getattr(next_row, "bonus_entry_points", 0))

        owned_material = _inventory_total_count(data, out.cost_item_id) if out.cost_item_id else 0
        out.owned_item_count = owned_material

        block_tip = 0
        if player is not None and self.scene.battle and self.scene.battle.frozen(player.player_id):
            block_tip = _IN_BATTLE
        elif player is not None and not self.scene.mutable(player):
            block_tip = INVALID
        elif eff_level < out.min_pet_level:
            block_tip = PET_TIER_LEVEL_NOT_ENOUGH
        elif out.cost_item_id and owned_material < out.cost_item_count:
            block_tip = PET_TIER_MATERIAL_NOT_ENOUGH
        elif out.cost_gold and (data is None or balance(data, self.gold) < out.cost_gold):
            block_tip = _GOLD_LOW
        out.can_ascend = block_tip == 0
        out.block_tip_id = block_tip

    def _read_entry_config(self):
        if self.rule is None or "PetEntry" not in self.tables.tables:
            return 0, 0, 0, {}
        fields = self.rule.DESCRIPTOR.fields_by_name
        version = self.rule.entry_config_version if "entry_config_version" in fields else _DEFAULT_ENTRY_CONFIG_VERSION
        budget = self.rule.entry_total_points if "entry_total_points" in fields else _DEFAULT_ENTRY_TOTAL_POINTS
        max_directions = (
            self.rule.entry_max_directions if "entry_max_directions" in fields else _DEFAULT_ENTRY_MAX_DIRECTIONS
        )
        rules: dict[int, tuple[int, int]] = {}
        for row in self.tables["PetEntry"].rows():
            invalid_row = (
                not row.id
                or not row.cap
                or not 1 <= row.channel <= 9
                or not row.per_point_bps
                or row.cap * row.per_point_bps > 10_000
                or (row.channel in (5, 6) and not 1 <= row.state_kind <= 7)
            )
            if invalid_row:
                return 0, 0, 0, {}
            rules[row.id] = (row.cap, row.per_point_bps)
        if not rules or not budget:
            return 0, 0, 0, {}
        return version, budget, max_directions, rules

    def _fill_entries_snapshot(self, pet, config, values: dict[int, int], info) -> int:
        info.Clear()
        version, base_budget, max_directions, rules = config
        if not version:
            return INVALID_DATA
        budget = _effective_budget_for_pet(self.tables, pet, base_budget)
        active_slot = _effective_preset_slot(int(pet.active_entry_preset_slot))
        info.pet_id = pet.pet_id
        info.state_version = _entry_state_version(pet.entry_state_version)
        info.config_version = version
        info.total_points = budget
        info.max_directions = max_directions
        info.last_request_id = pet.entry_last_request_id
        info.active_preset_slot = active_slot

        known_values = {}
        total_used = 0
        unknown_used = 0
        for entry_id, points in values.items():
            total_used += points
            if entry_id in rules:
                known_values[entry_id] = points
            else:
                unknown_used += points

        effective = _validate_entries(values, known_values, rules, budget, max_directions) == 0
        info.effective = effective
        info.unknown_used_points = unknown_used
        info.remaining_points = (budget - total_used) if effective and total_used <= budget else 0

        sorted_rows = sorted(self.tables["PetEntry"].rows(), key=lambda row: (row.sort, row.id))
        for row in sorted_rows:
            points = values.get(row.id, 0)
            effective_bps = points * row.per_point_bps if effective else 0
            info.entries.add(
                id=row.id,
                name=row.name,
                desc=row.desc,
                category=row.category,
                channel=row.channel,
                state_kind=row.state_kind,
                cap=row.cap,
                per_point_bps=row.per_point_bps,
                allocated=points,
                effective_bps=effective_bps,
                sort=row.sort,
                icon=row.icon,
            )
            _accumulate_entry_effect(info.effects, row, effective_bps)

        for slot in range(1, _MAX_PRESET_SLOTS + 1):
            summary = info.presets.add(slot_id=slot)
            stored_preset = _find_preset(pet, slot)
            summary.tag = stored_preset.tag if stored_preset is not None else 0
            preset_used = 0
            source = values if slot == active_slot else (stored_preset.allocated if stored_preset is not None else {})
            for entry_id, points in source.items():
                if points > 0:
                    summary.allocated[entry_id] = points
                    preset_used += points
            summary.used_points = min(UINT32_MAX, preset_used)
        return 0

    def _build_entries_info(self, pet, info) -> int:
        return self._fill_entries_snapshot(pet, self._read_entry_config(), dict(pet.entry_allocated), info)

    def get_entries(self, _world, player, _route, request):
        """查询指定宝宝的独立培养条目池与当前汇总效果。"""
        response = self.protocol.message("GetPetEntriesResponse")
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component"):
            response.error_message.id = UNAVAILABLE
            return response
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            response.error_message.id = _NOT_FOUND
            return response
        error = self._build_entries_info(pet, response.entries)
        if error:
            response.ClearField("entries")
            response.error_message.id = error
        return response

    def preview_entries(self, _world, player, _route, request):
        """预览宝宝独立条目池目标分配方案，不修改存档。"""
        response = self.protocol.message("PreviewPetEntriesResponse")
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component"):
            response.error_message.id = UNAVAILABLE
            return response
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            response.error_message.id = _NOT_FOUND
            return response

        config = self._read_entry_config()
        version, base_budget, max_directions, rules = config
        budget = _effective_budget_for_pet(self.tables, pet, base_budget)
        stored = dict(pet.entry_allocated)
        target = dict(request.target)

        if not version:
            error = INVALID_DATA
        elif request.config_version != version or request.state_version != _entry_state_version(pet.entry_state_version):
            error = INVALID
        else:
            error = _validate_entries(stored, target, rules, budget, max_directions)

        if error:
            response.error_message.id = error
            if self._build_entries_info(pet, response.entries):
                response.ClearField("entries")
            return response

        preview_points = _replace_known_entries(stored, target, rules)
        fill_err = self._fill_entries_snapshot(pet, config, preview_points, response.entries)
        if fill_err:
            response.ClearField("entries")
            response.error_message.id = fill_err
        return response

    async def save_entries(self, _world, player, _route, request):
        """原子保存宝宝独立条目池目标分配方案，带 state_version 乐观锁与 request_id 幂等。"""
        response = self.protocol.message("SavePetEntriesResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component"):
            response.error_message.id = UNAVAILABLE
            return response
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            response.error_message.id = _NOT_FOUND
            return response

        def fail(err: int):
            response.error_message.id = err
            if self._build_entries_info(pet, response.entries):
                response.ClearField("entries")
            return response

        if error := self._writable(player):
            return fail(error)
        if not _valid_entry_request_id(request.request_id):
            return fail(INVALID)

        target = dict(request.target)
        if pet.entry_last_request_id == request.request_id:
            same = (
                pet.entry_last_preset_slot == 0
                and pet.entry_last_base_version == request.state_version
                and pet.entry_last_config_version == request.config_version
                and dict(pet.entry_last_target) == target
            )
            if not same:
                return fail(INVALID)
            build_err = self._build_entries_info(pet, response.entries)
            if build_err:
                response.ClearField("entries")
                response.error_message.id = build_err
                return response
            response.replayed = True
            return response

        config = self._read_entry_config()
        version, base_budget, max_directions, rules = config
        if not version:
            return fail(INVALID_DATA)
        if (
            request.config_version != version
            or request.state_version != _entry_state_version(pet.entry_state_version)
            or pet.entry_state_version >= UINT64_MAX
        ):
            return fail(INVALID)

        budget = _effective_budget_for_pet(self.tables, pet, base_budget)
        stored = dict(pet.entry_allocated)
        error = _validate_entries(stored, target, rules, budget, max_directions)
        if error:
            return fail(error)

        active_slot = _effective_preset_slot(int(pet.active_entry_preset_slot))
        pet.active_entry_preset_slot = active_slot
        replaced = _replace_known_entries(stored, target, rules)
        pet.entry_allocated.clear()
        for entry_id, points in replaced.items():
            pet.entry_allocated[entry_id] = points
        _sync_active_preset_from_allocated(pet, active_slot)
        pet.entry_state_version = _entry_state_version(pet.entry_state_version) + 1
        pet.entry_last_request_id = request.request_id
        pet.entry_last_base_version = request.state_version
        pet.entry_last_config_version = request.config_version
        pet.entry_last_preset_slot = 0
        pet.entry_last_preset_tag = 0
        pet.entry_last_target.clear()
        for entry_id, points in target.items():
            pet.entry_last_target[entry_id] = points

        fill_err = self._fill_entries_snapshot(pet, config, dict(pet.entry_allocated), response.entries)
        if fill_err:
            response.ClearField("entries")
            return fail(INVALID_DATA)
        await self.scene.commit_player_data(player, data)
        logger.info(
            "Pet entries saved player_id=%s pet_id=%s preset_slot=%s state_version=%s remaining=%s",
            player.player_id, pet.pet_id, active_slot, pet.entry_state_version, response.entries.remaining_points,
        )
        return response

    async def switch_entry_preset(self, _world, player, _route, request):
        """切换宝宝条目预设槽位（1..3）或更新其用途标签，战外免费、乐观锁 + 幂等。"""
        response = self.protocol.message("SwitchPetEntryPresetResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component"):
            response.error_message.id = UNAVAILABLE
            return response
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            response.error_message.id = _NOT_FOUND
            return response

        def fail(err: int):
            response.error_message.id = err
            if self._build_entries_info(pet, response.entries):
                response.ClearField("entries")
            return response

        if error := self._writable(player):
            return fail(error)
        if not _valid_entry_request_id(request.request_id):
            return fail(INVALID)
        if not _valid_preset_slot(request.slot_id) or not _valid_preset_tag(request.tag):
            return fail(PET_ENTRY_PRESET_INVALID)

        if pet.entry_last_request_id == request.request_id:
            same = (
                pet.entry_last_preset_slot == request.slot_id
                and pet.entry_last_preset_tag == request.tag
                and pet.entry_last_base_version == request.state_version
                and pet.entry_last_config_version == request.config_version
            )
            if not same:
                return fail(INVALID)
            build_err = self._build_entries_info(pet, response.entries)
            if build_err:
                response.ClearField("entries")
                response.error_message.id = build_err
                return response
            response.replayed = True
            return response

        config = self._read_entry_config()
        version = config[0]
        if not version:
            return fail(INVALID_DATA)
        if (
            request.config_version != version
            or request.state_version != _entry_state_version(pet.entry_state_version)
            or pet.entry_state_version >= UINT64_MAX
        ):
            return fail(INVALID)

        current_slot = _effective_preset_slot(int(pet.active_entry_preset_slot))
        # 先把当前槽位的权威分配落回预设槽，确保切走时不丢当前方案点位。
        _sync_active_preset_from_allocated(pet, current_slot)

        target_preset = _find_or_create_preset(pet, request.slot_id)
        if request.tag != _KEEP_PRESET_TAG:
            target_preset.tag = request.tag
        pet.active_entry_preset_slot = request.slot_id
        pet.entry_allocated.clear()
        for entry_id, points in target_preset.allocated.items():
            if points > 0:
                pet.entry_allocated[entry_id] = points

        pet.entry_state_version = _entry_state_version(pet.entry_state_version) + 1
        pet.entry_last_request_id = request.request_id
        pet.entry_last_base_version = request.state_version
        pet.entry_last_config_version = request.config_version
        pet.entry_last_preset_slot = request.slot_id
        pet.entry_last_preset_tag = request.tag
        pet.entry_last_target.clear()

        fill_err = self._fill_entries_snapshot(pet, config, dict(pet.entry_allocated), response.entries)
        if fill_err:
            response.ClearField("entries")
            return fail(INVALID_DATA)
        await self.scene.commit_player_data(player, data)
        logger.info(
            "Pet entry preset switched player_id=%s pet_id=%s slot=%s->%s state_version=%s",
            player.player_id, pet.pet_id, current_slot, request.slot_id, pet.entry_state_version,
        )
        return response

    async def ascend_tier(self, _world, player, _route, request):
        """宝宝灵阶突破（普通→变异→灵兽→神兽→圣兽）：扣 PetTier 材料与银两，乐观锁 + 幂等。"""
        response = self.protocol.message("AscendPetTierResponse", client_request_id=request.client_request_id)
        data = self.scene.rows.assemble(player.snapshot)

        def fail(err: int):
            response.error_message.id = err
            response.gold = balance(data, self.gold)
            if data.HasField("pet_component"):
                self._fill_list(data.pet_component, response.pets, player=player, data=data)
            return response

        if not data.HasField("pet_component"):
            return fail(UNAVAILABLE)
        pet = _find(data.pet_component, request.pet_id)
        if pet is None:
            return fail(_NOT_FOUND)
        if not request.client_request_id:
            return fail(INVALID)

        if pet.tier_last_request_id == request.client_request_id:
            if pet.tier_last_expected_tier != request.expected_tier:
                return fail(INVALID)
            response.replayed = True
            response.new_tier = pet.tier_last_result_tier
            response.gold = balance(data, self.gold)
            self._fill_list(data.pet_component, response.pets, player=player, data=data)
            return response

        if error := self._writable(player):
            return fail(error)

        species = self.tables["Pet"].get(pet.pet_table_id)
        if species is None:
            return fail(_ROW_MISSING)
        current_tier = _effective_cultivation_tier(self.tables, pet)
        if current_tier >= _MAX_CULTIVATION_TIER:
            return fail(PET_TIER_MAX_REACHED)
        if request.expected_tier != current_tier:
            return fail(PET_TIER_CHANGED)

        if "PetTier" not in self.tables.tables:
            return fail(INVALID_DATA)
        current_row = self.tables["PetTier"].get(current_tier)
        next_row = self.tables["PetTier"].get(current_tier + 1)
        if current_row is None or next_row is None:
            return fail(INVALID_DATA)

        eff_level = pet_level(pet.level, species)
        if eff_level < int(getattr(current_row, "ascend_min_pet_level", 0)):
            return fail(PET_TIER_LEVEL_NOT_ENOUGH)

        cost_gold = int(getattr(current_row, "ascend_cost_gold", 0))
        if cost_gold > 0 and (cost_gold > INT64_MAX or balance(data, self.gold) < cost_gold):
            return fail(_GOLD_LOW)

        cost_item_id = int(getattr(current_row, "ascend_cost_item_id", 0))
        cost_item_count = int(getattr(current_row, "ascend_cost_item_count", 0))
        staged = clone(data)
        staged_pet = _find(staged.pet_component, request.pet_id)
        if staged_pet is None:
            return fail(_NOT_FOUND)

        if cost_item_id and cost_item_count:
            if self.assets is None:
                return fail(UNAVAILABLE)
            consume_err = self.assets.consume_items(staged, {cost_item_id: cost_item_count})
            if consume_err == BAG_INSUFFICIENT:
                return fail(PET_TIER_MATERIAL_NOT_ENOUGH)
            if consume_err:
                return fail(consume_err)

        if cost_gold > 0:
            if self.assets is None:
                return fail(UNAVAILABLE)
            deduct_err = self._deduct(staged, cost_gold)
            if deduct_err:
                return fail(deduct_err)

        staged_pet.cultivation_tier = current_tier + 1
        if staged_pet.entry_state_version < UINT64_MAX:
            staged_pet.entry_state_version = _entry_state_version(staged_pet.entry_state_version) + 1
        staged_pet.tier_last_request_id = request.client_request_id
        staged_pet.tier_last_expected_tier = current_tier
        staged_pet.tier_last_result_tier = current_tier + 1

        before = clone(data) if self.assets is not None and self.assets.audit_enabled else None
        audit = (
            self.assets.make_audit_callback(
                player, before, staged, self.debit_tx, correlation=staged_pet.pet_table_id,
            )
            if before is not None else None
        )
        await self.scene.commit_player_data(player, staged, on_committed=audit)

        response.new_tier = current_tier + 1
        response.gold = balance(staged, self.gold)
        self._fill_list(staged.pet_component, response.pets, player=player, data=staged)
        logger.info(
            "Pet tier ascended player_id=%s pet_id=%s tier=%s->%s cost_gold=%s cost_item=%sx%s",
            player.player_id, staged_pet.pet_id, current_tier, current_tier + 1,
            cost_gold, cost_item_id, cost_item_count,
        )
        return response
