"""对 detached player_database 执行原子的货币和背包原语。"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping
from secrets import randbelow

from google.protobuf.unknown_fields import UnknownFieldSet

from .asset_anomaly import AssetGain
from .asset_ledger import INT64_MAX, UINT64_MAX

INVALID_DATA = 1002
UNAVAILABLE = 1003
INVALID = 1005
FEATURE_UNAVAILABLE = 1006
BAG_FULL = 6005
BAG_INSUFFICIENT = 6007
INSUFFICIENT = 27000
BLOCKED = 27005
BAG_CAPACITY = (100, 200, 10, 200)
MAX_NEW_STACKS = 4096
TEMPORARY_NOT_FOUND = 6015
TEMPORARY_BAG_FULL = 6016


def clone(message):
    result = type(message)()
    result.CopyFrom(message)
    return result


def carries_unknown(message) -> bool:
    if len(UnknownFieldSet(message)):
        return True
    for field, value in message.ListFields():
        if field.message_type is None:
            continue
        if field.is_repeated and field.message_type.GetOptions().map_entry:
            if field.message_type.fields_by_name["value"].message_type is not None:
                if any(carries_unknown(item) for item in value.values()):
                    return True
        elif field.is_repeated:
            if any(carries_unknown(item) for item in value):
                return True
        elif carries_unknown(value):
            return True
    return False


def balance(data, kind: int) -> int:
    values = data.currency.values
    return values[kind] if 0 <= kind < len(values) else 0


def bag_capacity(bag, kind: int) -> int:
    return bag.capacities[kind] if kind < len(bag.capacities) else BAG_CAPACITY[kind]


class AssetInventory:
    def __init__(
        self,
        protocol,
        tables,
        allocate_item_id: Callable[[], Awaitable[int]] | None = None,
        *,
        blocked_gain: Callable[[str, int], bool] | None = None,
    ) -> None:
        self.protocol = protocol
        self.tables = tables
        self.allocate_item_id = allocate_item_id
        self.blocked_gain = blocked_gain or (lambda category, key: False)
        self.global_blocks: dict[str, set[int]] = {"currency": set(), "item": set()}
        currency = protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
        self.currency_types = frozenset(currency.values_by_number)

    def _set_balance(self, data, kind: int, amount: int) -> None:
        values = data.currency.values
        values.extend([0] * (max(self.currency_types) + 1 - len(values)))
        values[kind] = amount

    def currency_blocked(self, data, kind: int) -> bool:
        return (
            kind in data.currency.blocked_types or kind in self.global_blocks["currency"]
            or self.blocked_gain("currency", kind)
        )

    def item_gain_blocked(self, data, config: int) -> bool:
        return (
            config in data.item_gain_block.item_config_ids or config in self.global_blocks["item"]
            or self.blocked_gain("item", config)
        )

    def add_currency(self, data, kind: int, amount: int, *, gains: list[AssetGain] | None = None) -> int:
        if kind not in self.currency_types or not 1 <= amount <= INT64_MAX:
            return INVALID
        if self.currency_blocked(data, kind):
            return BLOCKED
        now = int(time.time())
        debt = next((debt for debt in data.currency.debts if debt.currency_type == kind), None)
        owed = 0
        if debt is not None and not debt.frozen and (
            not debt.expires_at or now < debt.expires_at
        ) and not carries_unknown(debt):
            owed = min(amount, max(0, debt.owed - debt.paid))
        new_balance = balance(data, kind) + amount - owed
        if new_balance > UINT64_MAX:
            return INVALID
        if owed:
            debt.paid += owed
            if debt.paid == debt.owed:
                data.currency.debts.remove(debt)
        self._set_balance(data, kind, new_balance)
        if gains is not None:
            gains.append(AssetGain("currency", kind, amount))
        return 0

    def deduct_currency(self, data, kind: int, amount: int) -> int:
        if kind not in self.currency_types or not 1 <= amount <= INT64_MAX:
            return INVALID
        previous = balance(data, kind)
        if previous < amount:
            return INSUFFICIENT
        self._set_balance(data, kind, previous - amount)
        return 0

    def attach_debt(
        self, data, kind: int, amount: int, correlation: int = 0, *,
        reason: str | None = None, operator: str = "data_service", expires_at: int = 0,
    ) -> int:
        if kind not in self.currency_types or not 1 <= amount <= INT64_MAX:
            return INVALID
        if not 0 <= expires_at <= UINT64_MAX:
            return INVALID
        now = int(time.time())
        debt = next((debt for debt in data.currency.debts if debt.currency_type == kind), None)
        if debt is not None and carries_unknown(debt):
            return FEATURE_UNAVAILABLE
        if debt is not None and debt.expires_at and now >= debt.expires_at:
            data.currency.debts.remove(debt)
            debt = None
        if debt is not None and debt.owed + amount > UINT64_MAX:
            return INVALID
        if debt is None:
            debt = data.currency.debts.add(currency_type=kind)
        merging = debt.owed > debt.paid
        if merging:
            expires_at = max(debt.expires_at, expires_at) if debt.expires_at and expires_at else 0
        debt.owed += amount
        debt.reason = f"gm_recall op={correlation}" if reason is None else reason
        debt.gm_operator = operator
        debt.expires_at = expires_at
        if not debt.created_at:
            debt.created_at = now
        data.currency.debts.sort(key=lambda item: item.currency_type)
        return 0

    def item(self, entry):
        if entry.HasField("instance"):
            return clone(entry.instance)
        result = self.protocol.message(
            "ItemComp", item_id=entry.item_uuid, config_id=entry.config_id,
            size=entry.stack_size, acquire_seq=entry.acquire_seq,
        )
        if entry.HasField("equip"):
            result.equip.CopyFrom(entry.equip)
        return result

    @staticmethod
    def write_entry(entry, item) -> None:
        entry.instance.CopyFrom(item)
        entry.item_uuid = item.item_id
        entry.config_id = item.config_id
        entry.stack_size = item.size
        entry.acquire_seq = item.acquire_seq
        if item.HasField("equip"):
            entry.equip.CopyFrom(item.equip)
        else:
            entry.ClearField("equip")

    def all_ids(self, bag) -> set[int]:
        entries = list(bag.items)
        for dynamic in bag.dynamic_bags:
            entries.extend(dynamic.items)
        return {entry.instance.item_id if entry.HasField("instance") else entry.item_uuid for entry in entries}

    def entries(self, bag, kind: int):
        return [(entry, self.item(entry)) for entry in bag.items if entry.bag_type == kind]

    @staticmethod
    def _usable(entries, capacity: int) -> bool:
        ids, slots = set(), set()
        for entry, item in entries:
            if (
                not item.item_id or not item.config_id or not item.size or item.item_id in ids
                or entry.pos >= capacity or entry.pos in slots
            ):
                return False
            ids.add(item.item_id)
            slots.add(entry.pos)
        return True

    @staticmethod
    def countable(item) -> bool:
        return not carries_unknown(item)

    def _mergeable(self, item, limit: int) -> bool:
        return limit > 1 and 0 < item.size < limit and not item.HasField("equip") and self.countable(item)

    def _drain(self, entries, config: int, amount: int) -> tuple[int, bool]:
        known = total = 0
        for entry, item in sorted(entries, key=lambda row: (row[0].pos, row[1].acquire_seq, row[1].item_id)):
            if item.config_id != config:
                continue
            total += item.size
            if not self.countable(item):
                continue
            take = min(item.size, max(0, amount - known))
            known += take
            if take:
                item.size -= take
                self.write_entry(entry, item)
        return known, known < amount <= total

    def consume_items(self, data, counts: Mapping[int, int], *, bag_type: int = 0) -> int:
        if not 0 <= bag_type < len(BAG_CAPACITY):
            return INVALID
        bag = clone(data.bag_component)
        entries = self.entries(bag, bag_type)
        if not self._usable(entries, bag_capacity(bag, bag_type)):
            return INVALID_DATA
        skew = False
        for config, amount in sorted(counts.items()):
            if not 0 < amount <= 0xFFFFFFFF or self.tables["Item"].get(config) is None:
                return INVALID
            taken, version_skew = self._drain(entries, config, amount)
            if taken != amount and not version_skew:
                return BAG_INSUFFICIENT
            skew |= version_skew
        if skew:
            return FEATURE_UNAVAILABLE
        self._drop_empty(bag.items, bag_type)
        data.bag_component.CopyFrom(bag)
        return 0

    async def add_items(
        self,
        data,
        counts: Mapping[int, int],
        *,
        bag_type: int = 0,
        overflow: bool = False,
        instances=(),
        gains: list[AssetGain] | None = None,
    ) -> int:
        if not counts and not instances:
            return 0
        if not 0 <= bag_type < len(BAG_CAPACITY) or bag_type == 2:
            return INVALID
        kinds = (bag_type, 3) if overflow and bag_type == 0 else (bag_type,)
        rows = []
        for config, amount in sorted(counts.items()):
            row = self.tables["Item"].get(config)
            if row is None or row.max_stack_size == 0 or not 0 < amount <= 0xFFFFFFFF:
                return INVALID
            if self.item_gain_blocked(data, config):
                return BLOCKED
            rows.append((row, amount))
        if any(self.item_gain_blocked(data, item.config_id) for item in instances):
            return BLOCKED
        bag = clone(data.bag_component)
        items = {kind: self.entries(bag, kind) for kind in kinds}
        for kind, entries in items.items():
            if not self._usable(entries, bag_capacity(bag, kind)):
                return INVALID_DATA
        plan = []
        occupied = {kind: {entry.pos for entry, _ in items[kind]} for kind in kinds}
        slots = {kind: self._free_slots(occupied[kind], bag_capacity(bag, kind)) for kind in kinds}

        for row, amount in rows:
            for kind in kinds:
                for entry, item in sorted(items[kind], key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
                    if item.config_id == row.id and self._mergeable(item, row.max_stack_size):
                        added = min(amount, row.max_stack_size - item.size)
                        if added:
                            item.size += added
                            amount -= added
                            self.write_entry(entry, item)
                while amount:
                    slot = next(slots[kind], None)
                    if slot is None:
                        break
                    if len(plan) >= MAX_NEW_STACKS:
                        return INVALID_DATA
                    stack = min(amount, row.max_stack_size)
                    plan.append((kind, slot, row.id, stack))
                    amount -= stack
            if amount:
                return BAG_FULL
        ids = self.all_ids(bag)
        received = []
        for item in instances:
            if item.item_id in ids or not item.item_id or item.size != 1:
                return INVALID
            ids.add(item.item_id)
            target = next(
                ((kind, slot) for kind in kinds if (slot := next(slots[kind], None)) is not None),
                None,
            )
            if target is None:
                return BAG_FULL
            received.append((*target, clone(item)))

        if plan and self.allocate_item_id is None:
            return UNAVAILABLE
        for kind, slot, config, stack in plan:
            try:
                guid = await self.allocate_item_id()
            except (OSError, RuntimeError, ValueError):
                return UNAVAILABLE
            if type(guid) is not int or not 1 <= guid <= UINT64_MAX or guid in ids:
                return UNAVAILABLE
            ids.add(guid)
            item = self.protocol.message("ItemComp", item_id=guid, config_id=config, size=stack)
            self._initialize_equipment(item)
            received.append((kind, slot, item))
        if not self._place(bag, items, received):
            return INVALID_DATA
        self._order(bag.items)
        data.bag_component.CopyFrom(bag)
        if gains is not None:
            # 托管转移的完整实例走 PutInstance 语义，不进入新发放收益窗口。
            gains.extend(AssetGain("item", row.id, amount) for row, amount in rows)
        return 0

    def _initialize_equipment(self, item) -> None:
        row = self.tables["Item"].get(item.config_id)
        if not row.equip_kind or row.max_stack_size != 1 or item.HasField("equip"):
            return
        item.equip.SetInParent()
        if not row.affix_pool or not row.affix_rule:
            return
        rule = self.tables["EquipAffixRule"].get(row.affix_rule)
        pool = self.tables["EquipAffixPool"].find_all(row.affix_pool, field="pool_id")
        if rule is None:
            return
        candidates = {}
        for entry in pool:
            if not entry.weight or self.tables["EquipAttribute"].get(entry.attr_id) is None:
                continue
            if entry.attr_id not in candidates:
                levels = self.tables["EquipAttributeCap"].find_all(entry.attr_id, field="attr_id")
                cap = max(
                    (level for level in levels if level.level <= row.equip_level),
                    key=lambda level: level.level, default=None,
                )
                candidates[entry.attr_id] = [0, cap]
            candidates[entry.attr_id][0] += entry.weight
        candidates = {
            attr_id: (weight, cap)
            for attr_id, (weight, cap) in candidates.items() if cap is not None and cap.cap
        }
        if not candidates:
            return
        blue_count = rule.blue_min + randbelow(max(1, rule.blue_max - rule.blue_min + 1))
        remaining = dict(candidates)
        for seq in range(min(blue_count, len(remaining))):
            chosen = self._roll_affix(item.equip, remaining, 1, seq)
            del remaining[chosen]
        for tier, rate in ((2, rule.pink_rate), (3, rule.yellow_rate)):
            if rate and randbelow(10_000) < rate:
                self._roll_affix(item.equip, candidates, tier, 0)

    @staticmethod
    def _roll_affix(equip, candidates, tier: int, seq: int) -> int:
        ticket = randbelow(sum(weight for weight, _ in candidates.values()))
        for attr_id, (weight, cap) in candidates.items():
            if ticket < weight:
                low = min(cap.cap, max(1, cap.min_value))
                value = low + randbelow(cap.cap - low + 1)
                equip.affixes.add(attr_id=attr_id, tier=tier, value=value, seq=seq)
                return attr_id
            ticket -= weight
        raise RuntimeError("随机词条权重计算错误")

    @staticmethod
    def _free_slots(occupied: set[int], capacity: int):
        return (index for index in range(capacity) if index not in occupied)

    def _place(self, bag, items, received) -> bool:
        watermarks = {
            kind: max((item.acquire_seq for _, item in entries), default=0)
            for kind, entries in items.items()
        }
        for kind, slot, item in received:
            if watermarks[kind] == UINT64_MAX:
                return False
            watermarks[kind] += 1
            item.acquire_seq = watermarks[kind]
            self.write_entry(bag.items.add(pos=slot, bag_type=kind), item)
        return True

    def _drop_empty(self, entries, kind: int) -> None:
        for index in range(len(entries) - 1, -1, -1):
            if entries[index].bag_type == kind and self.item(entries[index]).size == 0:
                del entries[index]

    def _order(self, entries) -> None:
        entries.sort(
            key=lambda entry: (entry.bag_type, entry.pos, entry.item_uuid)
            if entry.bag_type < len(BAG_CAPACITY) else (len(BAG_CAPACITY), 0, 0),
        )

    def sort_bag(self, data, kind: int) -> tuple[int, bool]:
        if kind not in (0, 1):
            return INVALID, False
        bag = clone(data.bag_component)
        entries = self.entries(bag, kind)
        if not self._usable(entries, bag_capacity(bag, kind)):
            return INVALID_DATA, False
        groups = {}
        for entry, item in entries:
            row = self.tables["Item"].get(item.config_id)
            if row is not None and self._mergeable(item, row.max_stack_size):
                groups.setdefault((row.id, row.max_stack_size), []).append((entry, item))
        ordered = sorted(entries, key=lambda pair: pair[0].pos)
        layout_ok = all(
            entry.pos == index
            for index, (entry, _) in enumerate(ordered)
        ) and all(
            (a.config_id, -a.size) <= (b.config_id, -b.size)
            for (_, a), (_, b) in zip(ordered, ordered[1:])
        )
        merged = False
        for (_, limit), group in groups.items():
            if len(group) < 2:
                continue
            merged = True
            remaining = sum(item.size for _, item in group)
            for entry, item in sorted(group, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
                item.size = min(limit, remaining)
                remaining -= item.size
                self.write_entry(entry, item)
        if layout_ok and not merged:
            return 0, False
        self._drop_empty(bag.items, kind)
        ordered = sorted(
            self.entries(bag, kind),
            key=lambda pair: (pair[1].config_id, -pair[1].size, pair[1].acquire_seq, pair[1].item_id),
        )
        for position, (entry, _) in enumerate(ordered):
            entry.pos = position
        self._order(bag.items)
        data.bag_component.CopyFrom(bag)
        return 0, True

    def retrieve_temporary(self, data, guid: int, take_all: bool) -> tuple[int, bool]:
        if bool(guid) == take_all:
            return INVALID, False
        bag = clone(data.bag_component)
        inventory, temporary = self.entries(bag, 0), self.entries(bag, 3)
        capacity = bag_capacity(bag, 0)
        if not self._usable(inventory, capacity) or not self._usable(temporary, bag_capacity(bag, 3)):
            return INVALID_DATA, False
        if {item.item_id for _, item in inventory}.intersection(item.item_id for _, item in temporary):
            return INVALID_DATA, False
        selected = [(entry, item) for entry, item in temporary if take_all or item.item_id == guid]
        if not selected:
            return (0 if take_all else TEMPORARY_NOT_FOUND), False
        order = lambda pair: (pair[1].acquire_seq, pair[1].item_id)
        free = self._free_slots({entry.pos for entry, _ in inventory}, capacity)
        watermark = max((item.acquire_seq for _, item in inventory), default=0)
        retired = set()
        for entry, item in sorted(selected, key=order):
            config = self.tables["Item"].get(item.config_id)
            if (config is None or not config.max_stack_size
                    or config.max_stack_size == 1 and item.size != 1):
                return INVALID_DATA, False
            # Existing stacks above the current cap retain their identity and remaining count.
            if not item.HasField("equip") and self.countable(item):
                for target, held in sorted(inventory, key=order):
                    if held.config_id == item.config_id and self._mergeable(held, config.max_stack_size):
                        added = min(item.size, config.max_stack_size - held.size)
                        held.size += added
                        item.size -= added
                        self.write_entry(target, held)
                        if not item.size:
                            break
            if not item.size:
                retired.add(item.item_id)
                continue
            slot = next(free, None)
            if slot is None:
                return TEMPORARY_BAG_FULL, False
            if watermark == UINT64_MAX:
                return INVALID_DATA, False
            watermark += 1
            item.acquire_seq = watermark
            entry.bag_type, entry.pos = 0, slot
            self.write_entry(entry, item)
            inventory.append((entry, item))
        for index in range(len(bag.items) - 1, -1, -1):
            if bag.items[index].bag_type == 3 and self.item(bag.items[index]).item_id in retired:
                del bag.items[index]
        self._order(bag.items)
        data.bag_component.CopyFrom(bag)
        return 0, True

    def equip_item(self, data, guid: int, *, wear: bool) -> int:
        if not guid:
            return INVALID
        bag = clone(data.bag_component)
        inventory, equipment = self.entries(bag, 0), self.entries(bag, 2)
        capacity, worn_capacity = bag_capacity(bag, 0), bag_capacity(bag, 2)
        if not self._usable(inventory, capacity) or not self._usable(equipment, worn_capacity):
            return 28010
        if {item.item_id for _, item in inventory}.intersection(item.item_id for _, item in equipment):
            return 28010
        source = inventory if wear else equipment
        item_pair = next((pair for pair in source if pair[1].item_id == guid), None)
        if item_pair is None:
            return 28000 if wear else 28005
        entry, item = item_pair
        row = self.tables["Item"].get(item.config_id)
        if not wear:
            if row is None or row.max_stack_size != 1 or item.size != 1:
                return 28010
            free = next(self._free_slots({slot.pos for slot, _ in inventory}, capacity), None)
            if free is None:
                return 28006
            moves = [(entry, item, 0, free, inventory)]
        else:
            if row is None or not row.equip_kind or row.max_stack_size != 1:
                return 28001
            if item.size != 1:
                return 28000
            if max(1, data.level_component.level) < row.equip_level:
                return 28002
            if row.equip_class and row.equip_class != getattr(data.uint32_pb_component, "class"):
                return 28003
            configured = self.tables["EquipSlot"]
            slots = sorted({slot.id for slot in configured.rows() if slot.id < worn_capacity})
            accepting = [slot for slot in slots if configured.get(slot).equip_kind == row.equip_kind]
            if not accepting:
                return 28004
            worn = {slot.pos: (slot, item) for slot, item in equipment}
            slot = next((slot for slot in accepting if slot not in worn), accepting[0])
            moves = [(entry, item, 2, slot, equipment)]
            if slot in worn:
                swapped_entry, swapped_item = worn[slot]
                swapped = self.tables["Item"].get(swapped_item.config_id)
                if swapped is None or swapped.max_stack_size != 1 or swapped_item.size != 1:
                    return 28010
                occupied = {other.pos for other, _ in inventory if other.pos != entry.pos}
                destination = next(self._free_slots(occupied, capacity), None)
                if destination is None:
                    return 28010
                moves.append((swapped_entry, swapped_item, 0, destination, inventory))
        for moved, instance, kind, pos, destination in moves:
            mark = max((other.acquire_seq for _, other in destination), default=0)
            if mark == UINT64_MAX:
                return 28010
            instance.acquire_seq = mark + 1
            moved.bag_type, moved.pos = kind, pos
            self.write_entry(moved, instance)
        self._order(bag.items)
        data.bag_component.CopyFrom(bag)
        return 0
