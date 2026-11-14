"""角色属性面板、加点方案及其存档入口。"""

from inspect import isawaitable
import time

from ..data import client_request
from ..data.asset_inventory import FEATURE_UNAVAILABLE, INSUFFICIENT, INVALID, UNAVAILABLE, clone
from ..data.attribute_allocation import AllocationError, distribute_points, evaluate_pool, is_unlocked, validate_allocation
from .scene_mission import LEVEL, effective_level
from .scene_skills import _rescale
from .scene_stats import STATS, UINT32_MAX, UINT64_MAX, _equipment, class_id, player_stats

_SERVICE = "SceneAttributeClientPlayer"
_CREATE_SCHEME = 3
_POOL_NOT_FOUND = 25000
_POOL_LOCKED = 25001
_SCHEME_NOT_FOUND = 25006
_SCHEME_LIMIT = 25007
_SWITCH_COOLDOWN = 25008
_INVALID_NAME = 25009
_ALREADY_ACTIVE = 25010
_IN_BATTLE = 25011
_INSUFFICIENT_GOLD = 25012
_NO_PLAN = 25013
_NO_CHANGE = 25014
_ALLOCATION_TIPS = {
    AllocationError.POOL_LOCKED: _POOL_LOCKED,
    AllocationError.DIMENSION_NOT_IN_POOL: 25002,
    AllocationError.NOT_ENOUGH_POINTS: 25003,
    AllocationError.CANNOT_DECREASE: 25004,
    AllocationError.CAP_EXCEEDED: 25005,
    AllocationError.NOTHING_TO_CHANGE: _NO_CHANGE,
}


def _default_name(ordinal: int) -> str:
    return f"方案{'一二三四五六七八九'[ordinal - 1] if 1 <= ordinal <= 9 else ordinal}"


def initialize_attribute_schemes(data) -> None:
    comp = data.attribute_component
    if not comp.schemes:
        comp.schemes.add(scheme_id=1, name=_default_name(1))
        comp.active_scheme_id = 1
    elif not comp.active_scheme_id:
        comp.active_scheme_id = comp.schemes[0].scheme_id
    if not comp.next_scheme_id:
        highest = max(scheme.scheme_id for scheme in comp.schemes)
        comp.next_scheme_id = min(highest + 1, UINT32_MAX)


class SceneAttributes:
    def __init__(self, scene, tables, assets, *, missions=None, allow_gm=False, now_s=None) -> None:
        self.scene, self.protocol, self.tables, self.assets = scene, scene.protocol, tables, assets
        self.missions, self.allow_gm = missions, allow_gm
        self.now_s = now_s or (lambda: time.time_ns() // 1_000_000_000)
        self.rule = next(iter(tables["AttributeRule"].rows()), None)
        self.pools = {row.id: row for row in tables["AttributePool"].rows() if row.owner_type == 0}
        self.dimensions = {row.id: row for row in tables["AttributeDimension"].rows() if row.pool_id in self.pools}
        self.pool_dims = {
            pool: tuple(row.id for row in self.dimensions.values() if row.pool_id == pool) for pool in self.pools
        }
        self.plans = {}
        for plan in tables["AttributeAutoPlan"].rows():
            self.plans.setdefault((plan.class_id, plan.pool_id), plan)
        self.combat_rows = {}
        for row in sorted(tables["EquipAttribute"].rows(), key=lambda row: (row.sort, row.id)):
            if row.effect == 5 and 1 <= row.effect_param <= 15:
                self.combat_rows.setdefault(row.effect_param, row)
        currency = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        self.gold = currency.values_by_name["CURRENCY_TYPE_GOLD"].number
        types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self.spend_tx = types.values_by_name["TX_CURRENCY_DEDUCT"].number
        self.mission_tx = types.values_by_name["TX_QUEST_REWARD"].number

    def register(self) -> None:
        self.scene.register(_SERVICE, "GetAttributePanel", self.get_panel)
        self.scene.register(_SERVICE, "AutoAllocateAttributePoints", self.suggest)
        for method, handler in (
            ("AllocateAttributePoints", self.allocate), ("ResetAttributePoints", self.reset),
            ("CreateAttributeScheme", self.create), ("SwitchAttributeScheme", self.switch),
            ("RenameAttributeScheme", self.rename), ("GmSetPlayerLevel", self.set_level),
        ):
            self.scene.register(_SERVICE, method, handler, mutating=True)

    @staticmethod
    def _scheme(comp, scheme_id):
        return next((scheme for scheme in comp.schemes if scheme.scheme_id == scheme_id), None)

    @staticmethod
    def _present(data) -> bool:
        return all(data.HasField(field) for field in (
            "attribute_component", "level_component", "uint32_pb_component", "derived_attributes_component",
        ))

    def _write_error(self, player, data) -> int:
        if not self._present(data):
            return UNAVAILABLE
        if not self.scene.mutable(player):
            return INVALID
        if self.scene.battle and self.scene.battle.frozen(player.player_id):
            return _IN_BATTLE
        return 0

    def _pool_state(self, comp, allocated, pool, level):
        current = {key: allocated.get(key, 0) for key in self.pool_dims[pool.id]}
        foreign = sum(value for key, value in allocated.items() if key not in self.dimensions)
        return current, evaluate_pool(pool, level, current, bonus=comp.bonus_points.get(pool.id, 0), foreign=foreign)

    @staticmethod
    def _reset_cost(pool, level, allocation) -> int:
        return pool.reset_cost_gold if allocation.effective and level >= pool.reset_free_below_level else 0

    def _scheme_cost(self, comp) -> int:
        return self.rule.create_scheme_cost_gold if self.rule and len(comp.schemes) >= self.rule.free_scheme_count else 0

    def _max_schemes(self) -> int:
        return (self.rule.max_schemes or 1) if self.rule else 1

    def _valid_name(self, name) -> bool:
        limit = (self.rule.scheme_name_max_len or 12) if self.rule else 12
        if not 0 < len(name) <= limit or any(ord(char) < 0x20 or char == "\x7f" for char in name):
            return False
        invisible = {0x20, 0xA0, 0x2028, 0x2029, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF}
        return any(ord(char) not in invisible and not 0x2000 <= ord(char) <= 0x200F for char in name)

    def _panel(self, response, data) -> None:
        if not self._present(data):
            response.error_message.id = UNAVAILABLE
            return
        comp = data.attribute_component
        level = effective_level(data)
        panel = response.panel
        panel.level, panel.active_scheme_id = level, comp.active_scheme_id
        panel.max_schemes, panel.create_scheme_cost_gold = self._max_schemes(), self._scheme_cost(comp)
        if self.rule and self.rule.switch_cooldown_seconds:
            panel.switch_cooldown_until = min(UINT64_MAX, comp.last_switch_time + self.rule.switch_cooldown_seconds)
        for scheme in comp.schemes:
            panel.schemes.add(scheme_id=scheme.scheme_id, name=scheme.name)

        active = self._scheme(comp, comp.active_scheme_id)
        allocated = active.allocated if active is not None else {}
        effective = {}
        for pool in self.pools.values():
            _, state = self._pool_state(comp, allocated, pool, level)
            effective[pool.id] = state.effective
            panel.pools.add(pool_id=pool.id, name=pool.name, total=state.total, remaining=state.remaining,
                            dimension_cap=pool.dimension_cap, unlocked=is_unlocked(pool, level),
                            unlock_level=pool.unlock_level, reset_cost_gold=self._reset_cost(pool, level, state))
        equipped, all_points, _, _ = _equipment(self.protocol, self.tables, data)
        for dim in self.dimensions.values():
            points = allocated.get(dim.id, 0)
            bonus = comp.bonus_values.get(dim.id, 0) + equipped[dim.id] + all_points
            value = min(UINT64_MAX, dim.base_per_level * level + (points if effective[dim.pool_id] else 0) + bonus)
            panel.dimensions.add(dimension_id=dim.id, pool_id=dim.pool_id, name=dim.name, desc=dim.desc,
                                 allocated=points, value=value, cap=self.pools[dim.pool_id].dimension_cap,
                                 sort=dim.sort, bonus=min(UINT32_MAX, bonus))
        stats = player_stats(self.protocol, self.tables, data)
        base = data.derived_attributes_component
        for key in STATS:
            setattr(panel.derived, key, getattr(stats, key))
        panel.derived.health, panel.derived.mana = base.health, base.mana
        for stat, row in sorted(self.combat_rows.items(), key=lambda item: (item[1].sort, item[0])):
            if not row.name:
                continue
            field = stats.combat.DESCRIPTOR.fields_by_number[stat]
            intrinsic = base.critchance if stat in (1, 2) else base.resistance if stat in (14, 15) else 0
            value = min(100 if row.percent else UINT64_MAX, intrinsic + getattr(stats.combat, field.name))
            panel.combat.add(combat_id=stat, name=row.name, value=value, percent=bool(row.percent), sort=row.sort)

    def _rescale(self, old, data, *, level_changed=False):
        new = player_stats(self.protocol, self.tables, data)
        base = data.derived_attributes_component
        base.speed = new.speed
        if level_changed:
            if base.health and 0 < old.max_health < new.max_health:
                base.health = min(new.max_health, base.health + new.max_health - old.max_health)
            if 0 < old.max_mana < new.max_mana:
                base.mana = min(new.max_mana, base.mana + new.max_mana - old.max_mana)
            base.health = min(base.health, new.max_health)
            if new.max_mana:
                base.mana = min(base.mana, new.max_mana)
        else:
            base.health = _rescale(base.health, old.max_health, new.max_health)
            base.mana = _rescale(base.mana, old.max_mana, new.max_mana)

    def _data(self, player):
        return self.scene.rows.assemble(player.snapshot)

    def _response(self, route):
        return self.protocol.message(route.response_type)

    @staticmethod
    def _reject(response, error):
        response.error_message.id = error
        return response

    def _pool(self, data, pool_id):
        pool = self.pools.get(pool_id)
        if pool is None:
            return None, None, _POOL_NOT_FOUND
        comp = data.attribute_component
        scheme = self._scheme(comp, comp.active_scheme_id)
        return pool, scheme, 0 if scheme is not None else _SCHEME_NOT_FOUND

    def _debit(self, data, cost):
        error = self.assets.deduct_currency(data, self.gold, cost) if cost else 0
        return _INSUFFICIENT_GOLD if error == INSUFFICIENT else error

    async def _save(self, player, response, data, *, audit=None):
        await self.scene.commit_player_data(player, data, on_committed=audit)
        self._panel(response, data)
        return response

    def get_panel(self, _world, player, route, _request):
        response = self._response(route)
        self._panel(response, self._data(player))
        return response

    def suggest(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if not self._present(data):
            return self._reject(response, UNAVAILABLE)
        pool = self.pools.get(request.pool_id)
        if pool is None:
            return self._reject(response, _POOL_NOT_FOUND)
        level = effective_level(data)
        if not is_unlocked(pool, level):
            return self._reject(response, _POOL_LOCKED)
        _, scheme, error = self._pool(data, pool.id)
        if error:
            return self._reject(response, error)
        plan = self.plans.get((class_id(data), pool.id)) or self.plans.get((0, pool.id))
        if plan is None or not plan.dimension:
            return self._reject(response, _NO_PLAN)
        current, state = self._pool_state(data.attribute_component, scheme.allocated, pool, level)
        if not state.remaining:
            return self._reject(response, _NO_CHANGE)
        order, weights = [], []
        for index, key in enumerate(plan.dimension):
            if key in current:
                order.append(key)
                weights.append(plan.weight[index] if index < len(plan.weight) else 1)
        distribute_points(pool, order, weights, state.remaining, current)
        response.pool_id = pool.id
        response.suggested.update(current)
        return response

    async def allocate(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if not request.pool_id or not request.allocated:
            return self._reject(response, INVALID)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        pool, scheme, error = self._pool(data, request.pool_id)
        if error:
            return self._reject(response, error)
        level = effective_level(data)
        current, state = self._pool_state(data.attribute_component, scheme.allocated, pool, level)
        outcome = validate_allocation(pool, level, current, request.allocated, state.remaining)
        if outcome is not AllocationError.OK:
            return self._reject(response, _ALLOCATION_TIPS[outcome])
        old = player_stats(self.protocol, self.tables, data)
        for key, amount in request.allocated.items():
            if amount != current[key]:
                scheme.allocated[key] = amount
        self._rescale(old, data)
        return await self._save(player, response, data)

    async def reset(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        pool, scheme, error = self._pool(data, request.pool_id)
        if error:
            return self._reject(response, error)
        level = effective_level(data)
        current, state = self._pool_state(data.attribute_component, scheme.allocated, pool, level)
        if not state.known_used:
            return self._reject(response, _NO_CHANGE)
        old = player_stats(self.protocol, self.tables, data)
        cost = self._reset_cost(pool, level, state)
        before = clone(data) if cost and self.assets.audit_enabled else None
        if error := self._debit(data, cost):
            return self._reject(response, error)
        for key in current:
            scheme.allocated.pop(key, None)
        self._rescale(old, data)
        audit = self.assets.make_audit_callback(player, before, data, self.spend_tx)
        return await self._save(player, response, data, audit=audit)

    async def create(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _CREATE_SCHEME)
        if error:
            return self._reject(response, error)
        if prior is not None:
            response.scheme_id, response.replayed = prior.created_scheme_id, True
            self._panel(response, data)
            return response
        if error := self._write_error(player, data):
            return self._reject(response, error)
        comp = data.attribute_component
        if len(comp.schemes) >= self._max_schemes():
            return self._reject(response, _SCHEME_LIMIT)
        name = request.name or _default_name(len(comp.schemes) + 1)
        if not self._valid_name(name):
            return self._reject(response, _INVALID_NAME)
        scheme_id = comp.next_scheme_id
        if not 0 < scheme_id < UINT32_MAX or self._scheme(comp, scheme_id) is not None:
            return self._reject(response, UNAVAILABLE)
        cost = self._scheme_cost(comp)
        before = clone(data) if cost and self.assets.audit_enabled else None
        if error := self._debit(data, cost):
            return self._reject(response, error)
        comp.schemes.add(scheme_id=scheme_id, name=name)
        comp.next_scheme_id += 1
        client_request.remember(ledger, request.client_request_id, _CREATE_SCHEME, fingerprint,
                                created_scheme_id=scheme_id)
        response.scheme_id = scheme_id
        audit = self.assets.make_audit_callback(player, before, data, self.spend_tx)
        return await self._save(player, response, data, audit=audit)

    async def switch(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        comp = data.attribute_component
        if self._scheme(comp, request.scheme_id) is None:
            return self._reject(response, _SCHEME_NOT_FOUND)
        if comp.active_scheme_id == request.scheme_id:
            return self._reject(response, _ALREADY_ACTIVE)
        now = self.now_s()
        if self.rule and self.rule.switch_cooldown_seconds:
            if comp.last_switch_time + self.rule.switch_cooldown_seconds > now:
                return self._reject(response, _SWITCH_COOLDOWN)
        old = player_stats(self.protocol, self.tables, data)
        comp.active_scheme_id, comp.last_switch_time = request.scheme_id, now
        self._rescale(old, data)
        return await self._save(player, response, data)

    async def rename(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        if not self._valid_name(request.name):
            return self._reject(response, _INVALID_NAME)
        scheme = self._scheme(data.attribute_component, request.scheme_id)
        if scheme is None:
            return self._reject(response, _SCHEME_NOT_FOUND)
        scheme.name = request.name
        return await self._save(player, response, data)

    async def set_level(self, _world, player, route, request):
        response, data = self._response(route), self._data(player)
        if not self.allow_gm:
            return self._reject(response, FEATURE_UNAVAILABLE)
        if error := self._write_error(player, data):
            return self._reject(response, error)
        level = request.level
        if not 1 <= level <= 85:
            return self._reject(response, INVALID)
        old = player_stats(self.protocol, self.tables, data)
        before = clone(data) if self.missions and self.assets.audit_enabled else None
        data.level_component.level = level
        self._rescale(old, data, level_changed=True)
        pets = getattr(self.scene, "pets", None)
        if pets is not None:
            pets.recalculate_all(data, level_changed=True)
        gains = []
        if self.missions is not None:
            stamp = self.missions.clock()
            await self.missions._event(data, self.missions.scopes(data)[0], LEVEL, level, level, stamp, gains=gains)
        audit = self.assets.make_audit_callback(player, before, data, self.mission_tx, gains=gains, credit=True)

        async def committed(snapshot):
            if audit is not None:
                result = audit(snapshot)
                if isawaitable(result):
                    await result
            self.notify(player)
            if pets is not None:
                pets.notify(player)

        return await self._save(player, response, data, audit=committed)

    def notify(self, player) -> None:
        response = self.protocol.message("GetAttributePanelResponse")
        self._panel(response, self._data(player))
        if not response.error_message.id:
            self.scene._push(player, "SceneAttributeClientPlayerNotifyAttributePanelChanged",
                             self.protocol.message("AttributePanelChangedS2C", panel=response.panel))
