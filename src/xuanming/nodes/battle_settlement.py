"""应用 Match 的持久终局原件，奖励、消耗和近期结果同一次提交。"""

import logging
from collections import Counter

from ..data.asset_inventory import BAG_FULL
from .scene_mission import now_ms
from .scene_stats import pet_stats, player_stats

logger = logging.getLogger(__name__)
MAX_HISTORY = 10
MAX_LINES = 32
MAX_ITEM_COUNT = (1 << 32) - 1
MAX_CURRENCY_AMOUNT = (1 << 63) - 1
ASSET_BLOCKED = 27005
BAG_SPACE_DEFERRED = 6006  # Native kBagItemNotStacked executor defer reason.


def known_message(message) -> bool:
    original = message.SerializeToString(deterministic=True)
    copy = type(message)()
    copy.CopyFrom(message)
    copy.DiscardUnknownFields()
    return original == copy.SerializeToString(deterministic=True)


class BattleSettlement:
    def __init__(self, scene, tables, assets, missions, *, clock=now_ms):
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.missions = missions
        self.clock = clock
        self.currency_types = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType").values_by_number

    def register(self) -> None:
        self.scene.register("SceneBattleResultClientPlayer", "AckBattleResults", self.ack, mutating=True)

    def ack(self, world, player, route, request):
        response = self.protocol.message("AckBattleResultsResponse")
        if len(request.battle_ids) > MAX_HISTORY:
            response.error_message.id = 1015
        elif not self.scene.mutable(player):
            response.error_message.id = 1006
        else:
            data = self.scene.rows.assemble(player.snapshot)
            live = {entry.battle_id for entry in data.battle_results.entries}
            player.seen_battles.intersection_update(live)
            player.seen_battles.update(live.intersection(request.battle_ids))
        return response

    async def apply(
        self, player, data, settlement, *, audits: list | None = None, defer_reasons: list[int] | None = None,
    ) -> bool:
        if (
            not settlement.battle_id or settlement.player_id != player.player_id
            or settlement.outcome not in (1, 2, 3) or not known_message(settlement)
            or not self.scene.mutable(player)
            or sum(item.count for item in settlement.defeated_monsters) > 4096
        ):
            return False
        if self.scene.battle:
            current = self.scene.battle.current(player.player_id)
            if current and current != settlement.battle_id:
                return False
        if any(entry.battle_id == settlement.battle_id for entry in data.battle_results.entries):
            return True
        currencies = self._currencies(settlement)
        consumed = self._items(settlement.items_consumed)
        gained = self._items(settlement.items_gained)
        if currencies is None or consumed is None or gained is None:
            return False
        owned = self._owned(data, consumed)
        if owned is None:
            return False
        consumed = {item: min(count, owned[item]) for item, count in consumed.items() if owned[item]}
        gained = {key: count for key, count in gained.items() if not self.assets.item_gain_blocked(data, key)}
        checkpoint = self._checkpoint(data, audits)
        gains = []
        for kind, amount in tuple(currencies.items()):
            # A blocked reward is skipped permanently, so its debt need not
            # be interpreted by this version of the server.
            if self.assets.currency_blocked(data, kind):
                del currencies[kind]
                continue
            if any(debt.currency_type == kind and not known_message(debt) for debt in data.currency.debts):
                return False
            error = self.assets.add_currency(data, kind, amount, gains=gains)
            if error == ASSET_BLOCKED:
                del currencies[kind]
            elif error:
                return False
        self._audit(player, checkpoint, data, 8, audits, correlation=settlement.battle_id, credit=True, gains=gains)
        checkpoint = self._checkpoint(data, audits)
        if consumed and self.assets.consume_items(data, consumed):
            return False
        details = {"source": "battle-inbox"}
        self._audit(player, checkpoint, data, 13, audits, correlation=settlement.battle_id, item_extra=details)
        checkpoint = self._checkpoint(data, audits)
        gains = []
        if gained:
            error = await self.assets.add_items(data, gained, overflow=True, gains=gains)
            if error:
                if error == BAG_FULL and defer_reasons is not None:
                    defer_reasons.append(BAG_SPACE_DEFERRED)
                return False
        self._audit(player, checkpoint, data, 20, audits, correlation=settlement.battle_id, item_extra=details, gains=gains)
        stats = player_stats(self.protocol, self.tables, data)
        base = data.derived_attributes_component
        base.health = min(settlement.health, stats.max_health)
        base.mana = min(settlement.mana, stats.max_mana) if stats.max_mana else settlement.mana
        if not base.health:
            base.health, base.mana = stats.max_health, stats.max_mana
        if not self._pets(data, settlement.pets):
            return False
        stamp = self.clock()
        if settlement.defeated_monsters:
            checkpoint = self._checkpoint(data, audits)
            gains = []
            await self.missions.battle_defeats(data, settlement.defeated_monsters, stamp, gains=gains)
            self._audit(player, checkpoint, data, 7, audits, gains=gains)
        self._record(data.battle_results, settlement, currencies, gained, consumed, stamp)
        return True

    def _checkpoint(self, data, audits):
        if audits is not None and self.assets.audit_enabled:
            return type(data).FromString(data.SerializeToString())
        return None

    def _audit(self, player, before, data, kind, audits, **options):
        if audits is not None:
            callback = self.assets.make_audit_callback(player, before, data, kind, **options)
            if callback is not None:
                audits.append(callback)

    def _currencies(self, settlement):
        entries = ([(line.currency_type, line.amount) for line in settlement.currency_gains]
                   if settlement.currency_gains else [(0, settlement.gold_gain)])
        result = {}
        for currency, amount in entries:
            if currency in result or currency not in self.currency_types or amount > MAX_CURRENCY_AMOUNT:
                return None
            result[currency] = amount
        return {currency: amount for currency, amount in result.items() if amount}

    def _items(self, entries):
        result = Counter()
        for entry in entries:
            item = entry.item_table_id
            if not item or not entry.count:
                return None
            result[item] += entry.count
            if result[item] > MAX_ITEM_COUNT:
                return None
        for item in list(result):
            row = self.tables["Item"].get(item)
            if row is None:
                logger.warning("战斗原件中的物品配表已删除，跳过 config_id=%s", item)
                del result[item]
            elif not known_message(row):
                return None
        return result

    def _owned(self, data, consumed):
        owned = Counter()
        for entry in data.bag_component.items:
            if entry.bag_type:
                continue
            instance = entry.instance if entry.HasField("instance") else None
            config = instance.config_id if instance is not None else entry.config_id
            if config not in consumed:
                continue
            if instance is None:
                if not entry.item_uuid or not entry.stack_size or entry.HasField("equip"):
                    return None
                owned[config] += entry.stack_size
            else:
                if (not instance.item_id or not instance.size or not known_message(instance)
                        or not self.assets.countable(instance)):
                    return None
                owned[config] += instance.size
        return owned

    def _pets(self, data, effects) -> bool:
        if not effects:
            return True
        if not known_message(data.pet_component):
            return False
        owned = {pet.pet_id: pet for pet in data.pet_component.pets}
        if 0 in owned or len(owned) != len(data.pet_component.pets):
            return False
        seen = set()
        for effect in effects:
            if not effect.pet_id or effect.pet_id in seen:
                return False
            seen.add(effect.pet_id)
            pet = owned.get(effect.pet_id)
            if pet is None:
                continue
            stats = pet_stats(self.tables, pet)
            if stats is None:
                continue
            pet.health = min(effect.health, stats["max_health"])
            pet.mana = min(effect.mana, stats["max_mana"]) if stats["max_mana"] else effect.mana
            if effect.is_dead or not pet.health:
                pet.health, pet.mana = stats["max_health"], stats["max_mana"]
        return True

    @staticmethod
    def _record(history, settlement, currencies, gained, consumed, stamp):
        seq = max(history.last_result_seq, max((entry.result_seq for entry in history.entries), default=0)) + 1
        history.last_result_seq = seq
        entry = history.entries.add(
            result_seq=seq, battle_id=settlement.battle_id, outcome=settlement.outcome,
            team_index=settlement.player_team_index, is_dead=settlement.is_dead, fled=settlement.fled,
            total_rounds=settlement.total_rounds, finished_at_ms=settlement.finished_at_ms, settled_at_ms=stamp,
            lines_truncated=len(gained) > MAX_LINES or len(consumed) > MAX_LINES,
            match_mode=settlement.match_mode, battle_config_id=settlement.battle_config_id,
        )
        for kind, amount in sorted(currencies.items()):
            entry.currencies.add(currency_type=kind, amount=amount)
        for items, lines in ((gained, entry.items_gained), (consumed, entry.items_consumed)):
            for kind, count in sorted(items.items())[:MAX_LINES]:
                lines.add(item_table_id=kind, count=count)
        while len(history.entries) > MAX_HISTORY:
            oldest = min(range(len(history.entries)), key=lambda index: history.entries[index].result_seq)
            del history.entries[oldest]

    async def complete(self, player, settlement) -> None:
        if self.scene.battle and not await self.scene.battle.settled(player.player_id, settlement.battle_id):
            return
        pets = getattr(self.scene, "pets", None)
        if settlement.pets and pets is not None:
            pets.notify(player)
        self.scene._push(player, "BattleClientPlayerNotifyBattleEnd", self.protocol.message(
            "BattleEndS2C", battle_id=settlement.battle_id, outcome=settlement.outcome, settlement=settlement,
        ))
