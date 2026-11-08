"""宠物列表、出战和加点，写操作随角色存档提交。"""

from secrets import randbelow
from time import time

from ..data.asset_inventory import FEATURE_UNAVAILABLE, INSUFFICIENT, INVALID, INVALID_DATA, UNAVAILABLE, clone
from ..data.attribute_allocation import AllocationError, distribute_points, evaluate_pool, validate_allocation
from .scene_mission import effective_level
from .scene_skills import _rescale
from .scene_stats import pet_dimensions, pet_level, pet_pool, pet_skills, pet_stats

_SERVICE = "ScenePetClientPlayer"
_NOT_FOUND, _ROW_MISSING, _FULL, _LEVEL_LOW, _ACTIVE, _NOT_ACTIVE = range(26000, 26006)
_INVALID_NAME, _IN_BATTLE, _POINTS_LOW, _CANNOT_DECREASE, _CAP, _UNKNOWN_DIM = range(26007, 26013)
_GOLD_LOW, _NO_AUTO_PLAN, _NO_CHANGE, _ID_UNAVAILABLE = range(26013, 26017)
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


class ScenePets:
    def __init__(self, scene, tables):
        self.scene, self.tables = scene, tables
        self.protocol, self.assets = scene.protocol, scene.assets
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

    def register(self):
        self.scene.register(_SERVICE, "GetPetList", self.get_list)
        self.scene.register(_SERVICE, "AutoAllocatePetPoints", self.auto_allocate)
        self.scene.register(_SERVICE, "GmGrantPet", self.grant, mutating=True)
        for method in self._editors:
            self.scene.register(_SERVICE, method, self.change, mutating=True)

    def _allocation(self, pet, level):
        current = {dim.id: pet.allocated.get(dim.id, 0) for dim in self.dimensions}
        foreign = sum(points for dimension, points in pet.allocated.items() if dimension not in current)
        return current, evaluate_pool(self.pool, level, current, foreign=foreign)

    def _reset_cost(self, level, allocation):
        return self.pool.reset_cost_gold if allocation.effective and level >= self.pool.reset_free_below_level else 0

    def _fill_list(self, comp, listing):
        listing.active_pet_id = comp.active_pet_id
        listing.max_pets, listing.rename_cost_gold = self.max_pets, self.rename_cost
        if self.pool is None:
            return
        for pet in comp.pets:
            row = self.tables["Pet"].get(pet.pet_table_id)
            level = pet_level(pet.level, row)
            current, allocation = self._allocation(pet, level)
            info = listing.pets.add(
                pet_id=pet.pet_id, pet_table_id=pet.pet_table_id, level=level,
                name=pet.name or (row.name if row is not None else ""), is_active=comp.active_pet_id == pet.pet_id,
                pool_id=self.pool.id, total_points=allocation.total, remaining_points=allocation.remaining,
                reset_cost_gold=self._reset_cost(level, allocation),
            )
            if row is not None:
                info.quality, info.model_id, info.desc = row.quality, row.model_id, row.desc
                info.skill_table_ids.extend(pet_skills(pet, row))
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

    def _reply(self, route, error=0, data=None, **fields):
        reply = self.protocol.message(route.response_type, **fields)
        if error:
            reply.error_message.id = error
        elif data is not None:
            self._fill_list(data.pet_component, reply.pets)
        return reply

    def get_list(self, _world, player, route, _request):
        data = self.scene.rows.assemble(player.snapshot)
        return self._reply(route, 0 if data.HasField("pet_component") else UNAVAILABLE, data)

    def notify(self, player):
        data = self.scene.rows.assemble(player.snapshot)
        if data.HasField("pet_component"):
            message = self.protocol.message("PetListChangedS2C")
            self._fill_list(data.pet_component, message.pets)
            self.scene._push(player, "ScenePetClientPlayerNotifyPetListChanged", message)

    def initialize_on_load(self, data):
        comp = data.pet_component
        comp.SetInParent()
        if comp.active_pet_id and _find(comp, comp.active_pet_id) is None:
            comp.active_pet_id = 0
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
        return self._reply(route, data=data)

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

    async def grant(self, _world, player, route, request):
        if not self.assets.allow_gm:
            return self._reply(route, FEATURE_UNAVAILABLE)
        if error := self._writable(player):
            return self._reply(route, error)
        row = self.tables["Pet"].get(request.pet_table_id)
        if row is None:
            return self._reply(route, _ROW_MISSING)
        if self.pool is None:
            return self._reply(route, INVALID_DATA)
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("pet_component") or not data.HasField("level_component"):
            return self._reply(route, UNAVAILABLE)
        comp = data.pet_component
        if len(comp.pets) >= self.max_pets:
            return self._reply(route, _FULL)
        if self.assets.allocate_item_id is None:
            return self._reply(route, _ID_UNAVAILABLE)
        try:
            pet_id = await self.assets.allocate_item_id()
        except (OSError, RuntimeError, ValueError):
            return self._reply(route, _ID_UNAVAILABLE)
        if type(pet_id) is not int or not 0 < pet_id < 1 << 63 or _find(comp, pet_id) is not None:
            return self._reply(route, _ID_UNAVAILABLE)
        pet = comp.pets.add(pet_id=pet_id, pet_table_id=row.id, level=pet_level(effective_level(data), row),
                            created_at=int(time()))
        for i, dim in enumerate(self.dimensions):
            low = row.aptitude_min[i] if i < len(row.aptitude_min) else 10_000
            high = row.aptitude_max[i] if i < len(row.aptitude_max) else 10_000
            low, high = sorted((low or high or 10_000, high or low or 10_000))
            pet.aptitude[dim.id] = low + randbelow(high - low + 1) if low != high else low
        stats = pet_stats(self.tables, pet)
        pet.health, pet.mana = stats["max_health"], stats["max_mana"]
        await self.scene.commit_player_data(player, data)
        return self._reply(route, data=data, pet_id=pet_id)
