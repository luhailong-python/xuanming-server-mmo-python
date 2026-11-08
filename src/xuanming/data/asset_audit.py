"""生成资产流水，在玩家存档确认后发布 C++ Kafka 消费者识别的记录。"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from inspect import isawaitable

from .asset_inventory import balance
from .asset_ledger import INT64_MAX, UINT64_MAX

logger = logging.getLogger(__name__)
_DEFERRED_CLAWBACK = 18


def _json(fields) -> str:
    return json.dumps(fields, separators=(",", ":")) if fields else ""


def _debts(data) -> dict[int, int]:
    return {row.currency_type: max(0, row.owed - row.paid) for row in data.currency.debts}


def _items(bag) -> dict[int, tuple[int, int, int, int]]:
    items = {}
    duplicates = set()
    for entries in (bag.items, *(part.items for part in bag.dynamic_bags)):
        for row in entries:
            if row.HasField("instance"):
                item = row.instance
                guid, count = item.item_id, item.size
            else:
                item = row
                guid, count = row.item_uuid, row.stack_size
            if guid and item.config_id and count:
                if guid in items:
                    duplicates.add(guid)
                items[guid] = (item.config_id, count, row.bag_type, item.acquire_seq)
    for guid in duplicates:
        items[guid] = (0, 0, 0, 0)
    return items


class AssetAudit:
    def __init__(self, protocol, zone_id, currency_types, tables, emit, allocate_tx_id) -> None:
        self.protocol = protocol
        self.zone_id = zone_id
        self.currency_types = sorted(currency_types)
        self.tables = tables
        self.emit = emit
        self.allocate_tx_id = allocate_tx_id
        self.pending = asyncio.Queue(maxsize=2048)
        self.worker: asyncio.Task | None = None
        self.closed = False

    def prepare(
        self, player_id, before, after, tx_type, *, correlation=0, credit=False,
        item_extra=None, retirement_only=False,
    ):
        batch = self.protocol.message("TransactionLogBatch")
        timestamp = int(time.time())

        def add(kind=tx_type, *, gain=False, correlation_id=correlation, **fields):
            batch.entries.add(
                tx_type=kind, timestamp=timestamp, zone_id=self.zone_id,
                from_player=0 if gain else player_id, to_player=player_id if gain else 0,
                correlation_id=correlation_id, **fields,
            )

        previous_debts, current_debts = (_debts(before), _debts(after)) if credit else ({}, {})
        for kind in self.currency_types:
            previous, current = balance(before, kind), balance(after, kind)
            remaining = current_debts.get(kind, 0)
            repaid = max(0, previous_debts.get(kind, 0) - remaining)
            delta = current - previous + repaid
            if not -INT64_MAX <= delta <= INT64_MAX or repaid > INT64_MAX:
                logger.error("资产流水金额越界 player_id=%s tx_type=%s", player_id, tx_type)
                continue
            if repaid:
                add(_DEFERRED_CLAWBACK, correlation_id=0, currency_type=kind, currency_delta=-repaid,
                    extra=_json({"debt_remaining": remaining}))
            if delta:
                add(gain=delta > 0, currency_type=kind, currency_delta=delta,
                    balance_before=previous, balance_after=current)

        old_items, new_items = _items(before.bag_component), _items(after.bag_component)
        extra = _json(item_extra)
        empty = (0, 0, 0, 0)
        grants = {}
        for guid in sorted(old_items.keys() | new_items.keys()):
            old_config, previous, _, _ = old_items.get(guid, empty)
            new_config, current, bag_type, acquired = new_items.get(guid, empty)
            if ((guid in old_items and not old_config) or (guid in new_items and not new_config)
                    or (old_config and new_config and old_config != new_config)):
                continue
            if retirement_only:
                if previous and not current:
                    add(item_uuid=guid, item_config_id=old_config, extra=extra)
            elif current > previous and (row := self.tables["Item"].get(new_config)) and row.max_stack_size > 1:
                # 可叠加发放按配置汇总，记录此次先写入的实例。
                key = (bag_type, new_config)
                first = (not previous, acquired, guid)
                primary, amount = grants.get(key, (first, 0))
                grants[key] = (min(first, primary), amount + current - previous)
            elif previous != current:
                add(gain=current > previous, item_uuid=guid, item_config_id=new_config or old_config,
                    item_quantity=abs(current - previous), extra=extra)
        for (_, config), ((_, _, guid), amount) in sorted(grants.items()):
            if amount <= 0xFFFFFFFF:
                add(gain=True, item_uuid=guid, item_config_id=config, item_quantity=amount, extra=extra)
            else:
                logger.error("资产流水物品数量越界 player_id=%s config_id=%s", player_id, config)
        return batch

    def enqueue(self, batch) -> None:
        if self.closed or self.pending.full():
            logger.error("资产流水队列不可用，无法发布 %s 条已提交记录", len(batch.entries))
            return
        self.pending.put_nowait(batch)
        if self.worker is None or self.worker.done():
            self.worker = asyncio.create_task(self._publish())

    async def _publish(self) -> None:
        while not self.pending.empty():
            batch = self.pending.get_nowait()
            for entry in batch.entries:
                try:
                    async with asyncio.timeout(5):
                        tx_id = self.allocate_tx_id()
                        if isawaitable(tx_id):
                            tx_id = await tx_id
                        if type(tx_id) is not int or not 0 < tx_id <= UINT64_MAX:
                            raise ValueError("交易流水号无效")
                        entry.tx_id = tx_id
                        sent = self.emit(entry)
                        if isawaitable(sent):
                            await sent
                except Exception:
                    logger.exception(
                        "发布已提交的资产流水失败 player_id=%s tx_id=%s correlation_id=%s",
                        entry.from_player or entry.to_player, entry.tx_id, entry.correlation_id,
                    )

    async def close(self) -> None:
        self.closed = True
        if self.worker is None:
            return
        try:
            await asyncio.wait_for(self.worker, timeout=5)
        except TimeoutError:
            logger.error("停止时资产流水仍在发布，剩余 %s 批", self.pending.qsize())
