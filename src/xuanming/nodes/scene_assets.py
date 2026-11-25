"""Scene 资产权限、结局账本以及客户端货币和背包写入口。"""

from __future__ import annotations

import logging
import os
import time
from inspect import isawaitable
from secrets import randbelow

from google.protobuf.message import DecodeError

from ..data import StoreRejected, StoreUnavailable
from ..data.asset_anomaly import AssetAnomaly
from ..data.asset_audit import AssetAudit
from ..data.asset_inventory import (
    BAG_FULL, BAG_INSUFFICIENT, BLOCKED, FEATURE_UNAVAILABLE, INSUFFICIENT,
    INVALID, UNAVAILABLE, AssetInventory, balance, carries_unknown, clone,
)
from ..data.asset_ledger import (
    CALLERS, INSTANCE_LIMIT, INT64_MAX, Outcome, authorized, classify,
    find_receipt, find_stream, receipt_matches, record, valid,
)
from ..data import client_request
from .scene_player import _bag

logger = logging.getLogger(__name__)

_ASSET_BAG_FULL = 27001
_IN_BATTLE = 27002
_FROZEN = 27003
_INVALID_BUNDLE = 27004
_NOT_HERE = 27006
_PARTIAL = 27007
_AUTH_FAILED = 27008
_EQUIP_GRANT_INVALID = 28009
_EQUIP_FROZEN = 28008
_EQUIP_IN_BATTLE = 28007
_EQUIP_INTERNAL = 28010
_ENHANCE_UNAVAILABLE = 28011
_ENHANCE_MAX = 28012
_ENHANCE_MATERIAL = 28013
_ENHANCE_GOLD = 28014
_ENHANCE_LEVEL_CHANGED = 28022
_GEM_INVALID = 28015
_GEM_NOT_OWNED = 28016
_SOCKET_INVALID = 28017
_SOCKET_OCCUPIED = 28018
_SOCKET_EMPTY = 28019
_GEM_TYPE_MISMATCH = 28020
_GEM_BAG_FULL = 28021
_TEMPORARY_BUSY = 6017
_TX_AUCTION_RETURN = 30
_RULES = {
    1: ("debit", (24,)),
    2: ("credit", (25, 26)),
    3: ("debit", (1, 3, 4)),
    4: ("credit", (1, 4, 30)),
    5: ("credit", (2, 8, 9)),
    6: ("debit", (19,)),
}


class SceneAssets(AssetInventory):
    def __init__(
        self,
        scene,
        tables,
        secrets=None,
        *,
        allow_gm: bool = False,
        allocate_item_id=None,
        blocked_gain=None,
        in_battle=None,
        emit_audit=None,
        allocate_tx_id=None,
        emit_anomaly=None,
        anomaly_journal_path=None,
    ) -> None:
        super().__init__(scene.protocol, tables, allocate_item_id, blocked_gain=blocked_gain)
        self.scene = scene
        supplied = (
            {caller: os.environ.get(f"MMORPG_ASSET_OP_SECRET_{caller.upper()}", "") for caller in set(CALLERS.values())}
            if secrets is None else secrets
        )
        self._secrets = {
            caller: (secret.encode("utf-8") if isinstance(secret, str) else secret).strip()
            for caller, secret in supplied.items()
        }
        self.allow_gm = allow_gm
        self._in_battle = in_battle
        if (emit_audit is None) != (allocate_tx_id is None):
            raise ValueError("资产流水发布器与流水号段必须同时配置")
        self._audit = None if emit_audit is None else AssetAudit(
            self.protocol, scene.info.zone_id, self.currency_types, tables, emit_audit, allocate_tx_id,
        )
        self.anomaly = AssetAnomaly(
            self._is_current_player, emit_anomaly, journal_path=anomaly_journal_path,
        )

    async def close(self) -> None:
        await self.anomaly.close()
        if self._audit is not None:
            await self._audit.close()

    def _is_current_player(self, player) -> bool:
        world = self.scene.locations.get(player.player_id)
        return world is not None and world.players.get(player.player_id) is player

    @property
    def audit_enabled(self) -> bool:
        return self._audit is not None

    def make_audit_callback(self, player, before_data, after_data, tx_type, *, gains=(), **options):
        batch = None
        if self._audit is not None and before_data is not None and tx_type:
            batch = self._audit.prepare(player.player_id, before_data, after_data, tx_type, **options)
        if not gains and (batch is None or not batch.entries):
            return None
        earned = tuple(gains)
        timestamp = self.anomaly.now()

        async def committed(_snapshot):
            if batch is not None and batch.entries:
                self._audit.enqueue(batch)
            for gain in earned:
                await self.anomaly.record(player, gain, timestamp)

        return committed

    def grpc_methods(self) -> dict:
        return {
            "AssetDebit": self.debit,
            "AssetCredit": self.credit,
            "AssetAbortDebit": self.abort_debit,
            "AssetAckReceipt": self.ack_receipt,
        }

    def register_client_handlers(self) -> None:
        register = self.scene.register
        for method in ("GmAddCurrency", "GmDeductCurrency", "GmBlockCurrency", "GmUnblockCurrency"):
            register("SceneCurrencyClientPlayer", method, self._wallet_rpc, mutating=True)
        register("SceneBagClientPlayer", "SortBag", self._sort_rpc, mutating=True)
        register("SceneBagClientPlayer", "GmGrantItem", self._grant_rpc, mutating=True)
        register("SceneBagClientPlayer", "EquipItem", self._wear_rpc, mutating=True)
        register("SceneBagClientPlayer", "UnequipItem", self._wear_rpc, mutating=True)
        register("SceneBagClientPlayer", "RetrieveTemporaryItems", self._retrieve_rpc, mutating=True)
        register("SceneBagClientPlayer", "EnhanceEquip", self._enhance_rpc, mutating=True)
        register("SceneBagClientPlayer", "InlayGem", self._gem_rpc, mutating=True)
        register("SceneBagClientPlayer", "RemoveGem", self._gem_rpc, mutating=True)

    async def debit(self, request):
        return await self._handle("debit", request)

    async def credit(self, request):
        return await self._handle("credit", request)

    async def abort_debit(self, request):
        return await self._handle("abort_debit", request)

    async def ack_receipt(self, request):
        return await self._handle("ack_receipt", request)

    def _answer(self, outcome: Outcome, reason: int = 0, *, durable=False, partial=False, snapshot=None):
        response = self.protocol.message(
            "AssetOpResponse", outcome=outcome, durable=durable, partial=partial,
        )
        response.reason.id = reason
        if snapshot is not None:
            response.snapshot.CopyFrom(snapshot)
        return response

    async def _handle(self, rpc: str, request):
        ack = rpc == "ack_receipt"
        rule = _RULES.get(request.stream)
        if not (request.player_id and request.seq and request.stream_epoch) or rule is None:
            return self._answer(Outcome.UNKNOWN, _AUTH_FAILED if ack else _INVALID_BUNDLE)
        if rpc in ("debit", "credit") and (rpc != rule[0] or request.tx_type not in rule[1]):
            return self._answer(Outcome.UNKNOWN, _INVALID_BUNDLE)
        if ack and (request.auth.caller != "trade" or not request.correlation_id):
            return self._answer(Outcome.UNKNOWN, _AUTH_FAILED)
        if not authorized(rpc, request, self._secrets, time.time_ns() // 1_000_000):
            return self._answer(Outcome.UNKNOWN, _AUTH_FAILED)
        if request.ByteSize() > 1 << 20:
            return self._answer(Outcome.UNKNOWN, _INVALID_BUNDLE)

        async with self.scene._lock(request.player_id):
            world = self.scene.locations.get(request.player_id)
            player = None if world is None else world.players.get(request.player_id)
            if player is None:
                return self._answer(Outcome.NOT_HERE, _NOT_HERE)
            try:
                await self.scene.flush_player_rows(player)
            except (StoreRejected, StoreUnavailable, OSError):
                return self._answer(Outcome.RETRY)
            base = self.scene.rows.assemble(player.snapshot)
            if not valid(base.asset_op_ledger):
                logger.error("资产账本损坏，暂停玩家资产操作 player_id=%s", request.player_id)
                return self._answer(Outcome.RETRY, _FROZEN if ack else BLOCKED)
            if ack:
                return await self._ack(player, base, request)
            return await self._decide(rpc, player, base, request)

    async def _decide(self, rpc: str, player, base, request):
        stream = find_stream(base.asset_op_ledger, request.stream)
        outcome = classify(stream, request.stream_epoch, request.seq)
        if outcome == Outcome.UNKNOWN:
            return self._answer(Outcome.UNKNOWN)
        if outcome is not None:
            return self._replay(stream, request, outcome)
        if not player.snapshot.owner_epoch:
            return self._answer(Outcome.RETRY, _FROZEN)
        if not self.scene.mutable(player):
            reason = _NOT_HERE if getattr(player, "disconnect_at", None) is not None else _FROZEN
            return self._answer(Outcome.RETRY, reason)

        data = clone(base)
        # 带完整实例的收据预检随后再次核算，防止滑窗裁掉的旧回执占预算。
        if record(data.asset_op_ledger, request, Outcome.APPLIED, 0) is None:
            return self._answer(Outcome.RETRY, _ASSET_BAG_FULL)
        data.asset_op_ledger.CopyFrom(base.asset_op_ledger)
        if rpc == "abort_debit":
            return await self._finish(player, data, request, Outcome.REJECTED)
        if self._player_in_battle(player):
            return self._answer(Outcome.RETRY, _IN_BATTLE)

        if not self._valid_bundle(rpc, request):
            return await self._finish(player, data, request, Outcome.REJECTED, _INVALID_BUNDLE)
        if carries_unknown(request):
            return self._answer(Outcome.RETRY, FEATURE_UNAVAILABLE)
        if (any(value.currency_type not in self.currency_types for value in request.bundle.currencies)
                or any(self.tables["Item"].get(value.config_id) is None
                       for value in (*request.bundle.items, *request.bundle.item_instances))
                or (request.bundle.HasField("pet_instance") and
                    self.tables["Pet"].get(request.bundle.pet_instance.pet_table_id) is None)):
            return self._answer(Outcome.RETRY, FEATURE_UNAVAILABLE)
        snapshot = self.protocol.message("AssetSnapshot")
        gains = []
        if request.stream == 6:
            outcome, reason, partial = self._recall(data, request, snapshot)
        elif rpc == "debit":
            outcome, reason, partial = self._apply_debit(data, request, snapshot)
        else:
            outcome, reason, partial = await self._apply_credit(data, request, gains)
        if outcome == Outcome.RETRY:
            return self._answer(outcome, reason)
        if outcome == Outcome.REJECTED:
            # 校验使用 detached 快照，即使已试算也只提交原数据和拒绝回执。
            data = clone(base)
            return await self._finish(player, data, request, outcome, reason)
        expose = request.stream == 6 or rpc == "debit" and not self._single_currency(request.bundle)
        return await self._finish(
            player, data, request, outcome, reason, snapshot if expose else None,
            partial=partial, gains=gains,
        )

    def _replay(self, stream, request, outcome: Outcome):
        receipt = find_receipt(stream, request.seq)
        if not receipt_matches(receipt, request):
            return self._answer(Outcome.UNKNOWN, _INVALID_BUNDLE)
        partial = request.seq in stream.partial_seqs
        reason = _PARTIAL if partial else 0
        if outcome == Outcome.REJECTED:
            reason = next((row.reason_tip_id for row in stream.rejections if row.seq == request.seq), 0)
        snapshot = receipt.snapshot if receipt.HasField("snapshot") else None
        return self._answer(outcome, reason, durable=True, partial=partial, snapshot=snapshot)

    async def _finish(self, player, data, request, outcome, reason=0, snapshot=None, *, partial=False, gains=()):
        if record(data.asset_op_ledger, request, outcome, reason, snapshot, partial=partial) is None:
            return self._answer(Outcome.RETRY, _ASSET_BAG_FULL)
        tx_type = request.tx_type if outcome == Outcome.APPLIED else 0
        if not await self._save(
            player, data, tx_type, correlation=request.correlation_id, credit=request.stream in (2, 4, 5), gains=gains,
        ):
            return self._answer(Outcome.RETRY)
        return self._answer(outcome, reason, durable=True, partial=partial, snapshot=snapshot)

    async def _ack(self, player, base, request):
        if not self.scene.mutable(player) or not player.snapshot.owner_epoch:
            return self._answer(Outcome.RETRY, _FROZEN)
        data = clone(base)
        stream = find_stream(data.asset_op_ledger, request.stream)
        if stream is None or stream.stream_epoch != request.stream_epoch:
            return self._answer(Outcome.UNKNOWN, _INVALID_BUNDLE)
        if request.seq <= stream.watermark:
            return self._answer(Outcome.APPLIED)
        receipt = find_receipt(stream, request.seq)
        if (
            not receipt_matches(receipt, request)
            or classify(stream, request.stream_epoch, request.seq) != Outcome.APPLIED
            or request.seq in stream.partial_seqs
        ):
            return self._answer(Outcome.UNKNOWN, _INVALID_BUNDLE)
        if receipt.HasField("snapshot"):
            receipt.ClearField("snapshot")
            if not await self._save(player, data):
                return self._answer(Outcome.RETRY)
        return self._answer(Outcome.APPLIED, durable=True)

    def _valid_bundle(self, rpc: str, request) -> bool:
        bundle = request.bundle
        currencies, grants = bundle.currencies, bundle.items
        guids, instances = bundle.item_uuids, bundle.item_instances
        has_pet = bundle.HasField("pet_instance")
        if any(not 1 <= entry.amount <= INT64_MAX for entry in currencies):
            return False
        if len({entry.currency_type for entry in currencies}) != len(currencies):
            return False
        if len({entry.config_id for entry in grants}) != len(grants):
            return False
        if any(not item.config_id or not item.count for item in grants):
            return False
        if guids and (0 in guids or len(set(guids)) != len(guids)):
            return False
        if request.stream == 6:
            return not (bundle.pet_id or instances or has_pet) and (
                len(currencies) == 1 and not (grants or guids)
                or not currencies and len(grants) == 1 and len(guids) <= 64
            )
        if len(currencies) > 4 or len(grants) > 16 or len(guids) > 8 or len(instances) > 8:
            return False
        if request.stream == 1:
            return self._single_currency(bundle) and not (instances or has_pet)
        if request.stream not in (3, 4) and (guids or instances or bundle.pet_id or has_pet):
            return False
        if rpc == "debit":
            if instances or has_pet or not (currencies or grants or guids or bundle.pet_id):
                return False
            return all(
                item.count <= 4096 and (row is None or row.max_stack_size > 1 and not row.equip_kind)
                for item in grants for row in (self.tables["Item"].get(item.config_id),)
            )
        if guids or bundle.pet_id or not (currencies or grants or instances or has_pet):
            return False
        if sum(item.count for item in grants) > 4096:
            return False
        return self._valid_transfers(bundle)

    def _valid_transfers(self, bundle) -> bool:
        ids = set()
        try:
            for transfer in bundle.item_instances:
                if not 1 <= len(transfer.instance_proto) <= INSTANCE_LIMIT:
                    return False
                item = self.protocol.parse("ItemComp", transfer.instance_proto)
                row = self.tables["Item"].get(item.config_id)
                if (
                    not item.item_id or not transfer.config_id or item.item_id in ids or item.item_id != transfer.item_uuid
                    or item.config_id != transfer.config_id or item.size != 1 or transfer.stack_size != 1
                    or (row is not None and row.max_stack_size != 1)
                ):
                    return False
                ids.add(item.item_id)
            if bundle.HasField("pet_instance"):
                transfer = bundle.pet_instance
                if not 1 <= len(transfer.instance_proto) <= INSTANCE_LIMIT:
                    return False
                pet = self.protocol.parse("PetInstance", transfer.instance_proto)
                if (
                    not pet.pet_id or pet.pet_id != transfer.pet_id or pet.pet_table_id != transfer.pet_table_id
                    or pet.level != transfer.level or pet.name != transfer.name or not pet.pet_table_id
                ):
                    return False
        except DecodeError:
            return False
        return True

    @staticmethod
    def _single_currency(bundle) -> bool:
        return len(bundle.currencies) == 1 and not (bundle.items or bundle.item_uuids or bundle.pet_id)

    def _apply_debit(self, data, request, snapshot):
        bundle = request.bundle
        if any(balance(data, entry.currency_type) < entry.amount for entry in bundle.currencies):
            return Outcome.REJECTED, INSUFFICIENT, False
        ids = set(bundle.item_uuids)
        if ids or bundle.pet_id:
            if not request.correlation_id:
                return Outcome.REJECTED, _INVALID_BUNDLE, False
        if ids:
            entries = {item.item_id: item for _, item in self.entries(data.bag_component, 0)}
            removing = {}
            for guid in bundle.item_uuids:
                item = entries.get(guid)
                row = None if item is None else self.tables["Item"].get(item.config_id)
                if row is None or row.max_stack_size != 1 or item.size != 1:
                    return Outcome.REJECTED, _INVALID_BUNDLE, False
                if self.countable(item):
                    removing[item.config_id] = removing.get(item.config_id, 0) + item.size
                payload = item.SerializeToString(deterministic=True)
                if len(payload) > INSTANCE_LIMIT:
                    return Outcome.REJECTED, _INVALID_BUNDLE, False
                snapshot.items.add(
                    item_uuid=guid, config_id=item.config_id, stack_size=item.size, instance_proto=payload,
                )
            if any(count > self.removable_count(data, config) for config, count in removing.items()):
                return Outcome.RETRY, BAG_INSUFFICIENT, False
        pet_index = None
        if bundle.pet_id:
            if data.pet_component.active_pet_id == bundle.pet_id:
                return Outcome.REJECTED, _INVALID_BUNDLE, False
            for index, pet in enumerate(data.pet_component.pets):
                if pet.pet_id != bundle.pet_id:
                    continue
                pet_row = self.tables["Pet"].get(pet.pet_table_id)
                if pet_row is None or int(getattr(pet_row, "transfer_policy", 0)) != 0:
                    return Outcome.REJECTED, 26018, False
                payload = pet.SerializeToString(deterministic=True)
                if len(payload) > INSTANCE_LIMIT:
                    return Outcome.REJECTED, _INVALID_BUNDLE, False
                snapshot.pet.CopyFrom(self.protocol.message(
                    "AssetPetInstance", pet_id=pet.pet_id, pet_table_id=pet.pet_table_id,
                    name=pet.name, level=pet.level, instance_proto=payload,
                ))
                pet_index = index
                break
            if pet_index is None:
                return Outcome.REJECTED, _INVALID_BUNDLE, False
        if bundle.items:
            result = self.consume_items(data, {item.config_id: item.count for item in bundle.items})
            if result == BAG_INSUFFICIENT:
                return Outcome.REJECTED, _INVALID_BUNDLE, False
            if result:
                return Outcome.RETRY, result, False
        if ids:
            if any(count > self.removable_count(data, config) for config, count in removing.items()):
                return Outcome.RETRY, BAG_INSUFFICIENT, False
            for index in range(len(data.bag_component.items) - 1, -1, -1):
                entry = data.bag_component.items[index]
                if entry.bag_type == 0 and self.item(entry).item_id in ids:
                    del data.bag_component.items[index]
        if pet_index is not None:
            del data.pet_component.pets[pet_index]
        for entry in bundle.currencies:
            result = self.deduct_currency(data, entry.currency_type, entry.amount)
            if result:
                return Outcome.RETRY, result, False
        return Outcome.APPLIED, 0, False

    async def _apply_credit(self, data, request, gains):
        bundle = request.bundle
        counts_as_gain = request.tx_type != _TX_AUCTION_RETURN
        if counts_as_gain and (any(self.currency_blocked(data, value.currency_type) for value in bundle.currencies) or any(
            self.item_gain_blocked(data, item.config_id) for item in (*bundle.items, *bundle.item_instances)
        )):
            return Outcome.REJECTED, BLOCKED, False
        if any(self.credit_overflows(data, entry.currency_type, entry.amount) for entry in bundle.currencies):
            return Outcome.REJECTED, _INVALID_BUNDLE, False
        items = [self.protocol.parse("ItemComp", item.instance_proto) for item in bundle.item_instances]
        stored_ids = self.all_ids(data.bag_component)
        if any(item.item_id in stored_ids for item in items):
            return Outcome.REJECTED, _INVALID_BUNDLE, False
        pet = None
        if bundle.HasField("pet_instance"):
            pet = self.protocol.parse("PetInstance", bundle.pet_instance.instance_proto)
            pet_row = self.tables["Pet"].get(pet.pet_table_id)
            if pet_row is None or int(getattr(pet_row, "transfer_policy", 0)) != 0:
                return Outcome.REJECTED, 26018, False
            rules = self.tables["PetRule"].rows()
            capacity = (rules[0].max_pets if rules else 0) or 1
            normal_pets = sum(
                1 for existing in data.pet_component.pets
                if int(getattr(self.tables["Pet"].get(existing.pet_table_id), "exclusive_gift_slots", 0)) == 0
            )
            if normal_pets >= capacity:
                return Outcome.RETRY, _ASSET_BAG_FULL, False
            if any(existing.pet_id == pet.pet_id for existing in data.pet_component.pets):
                return Outcome.REJECTED, _INVALID_BUNDLE, False
        counts = {item.config_id: item.count for item in bundle.items}
        result = await self.add_items(data, counts, instances=items, gains=gains, counts_as_gain=counts_as_gain)
        if result in (BLOCKED, INVALID):
            return Outcome.REJECTED, BLOCKED, False
        if result:
            tip = _ASSET_BAG_FULL if result == BAG_FULL else 0 if result == UNAVAILABLE else result
            return Outcome.RETRY, tip, False
        # Item allocation awaits an external source. Re-evaluate debt expiry
        # after that wait, before committing any of the detached asset changes,
        # and use one timestamp for both the check and every currency credit.
        credit_at = int(time.time())
        if any(self.credit_overflows(data, entry.currency_type, entry.amount, now_seconds=credit_at)
               for entry in bundle.currencies):
            return Outcome.REJECTED, _INVALID_BUNDLE, False
        if pet is not None:
            data.pet_component.pets.add().CopyFrom(pet)
        mutated = bool(counts or items or pet is not None)
        for entry in bundle.currencies:
            result = self.add_currency(data, entry.currency_type, entry.amount, gains=gains,
                                       counts_as_gain=counts_as_gain, now_seconds=credit_at)
            if result:
                return (Outcome.APPLIED, _PARTIAL, True) if mutated else (Outcome.RETRY, result, False)
            mutated = True
        return Outcome.APPLIED, 0, False

    def _recall(self, data, request, snapshot):
        if not request.correlation_id:
            return Outcome.REJECTED, _INVALID_BUNDLE, False
        if request.bundle.currencies:
            return self._recall_currency(data, request, snapshot)
        return self._recall_items(data, request, snapshot)

    def _recall_currency(self, data, request, snapshot):
        currency = request.bundle.currencies[0]
        kind, requested = currency.currency_type, currency.amount
        taken = min(balance(data, kind), requested)
        debt = requested - taken
        if debt:
            error = self.attach_debt(data, kind, debt, request.correlation_id)
            if error == FEATURE_UNAVAILABLE:
                return Outcome.RETRY, error, False
            if error and not taken:
                return Outcome.REJECTED, _INVALID_BUNDLE, False
        else:
            error = 0
        if taken:
            self.deduct_currency(data, kind, taken)
        snapshot.recall_lines.add(
            currency_type=kind, requested=requested, taken=taken,
            debt=0 if error else debt, shortfall=debt if error else 0,
        )
        return Outcome.APPLIED, _PARTIAL if error else 0, bool(error)

    def _recall_items(self, data, request, snapshot):
        item = request.bundle.items[0]
        config, requested = item.config_id, item.count
        # 召回跨多个背包和实例进行，预留期间保守重试，避免越过人物背包预留。
        from ..data.asset_inventory import held_item_count
        if held_item_count(data, config):
            return Outcome.RETRY, BAG_INSUFFICIENT, False
        bag = data.bag_component
        groups = [[entry for entry in bag.items if entry.bag_type == kind] for kind in (0, 1, 3)]
        groups.extend(box.items for box in sorted(bag.dynamic_bags, key=lambda box: box.bag_id))
        groups.append([entry for entry in bag.items if entry.bag_type == 2])
        ordered = [[(entry, self.item(entry)) for entry in entries] for entries in groups]
        matching = [stored for entries in ordered for _, stored in entries if stored.config_id == config]
        countable = sum(stored.size for stored in matching if not carries_unknown(stored))
        if countable < requested and any(carries_unknown(stored) for stored in matching):
            return Outcome.RETRY, FEATURE_UNAVAILABLE, False
        row = self.tables["Item"].get(config)
        taken = 0
        removed = set()
        if row.max_stack_size > 1:
            for entries in ordered:
                taken += self._drain(entries, config, requested - taken)[0]
                removed.update(stored.item_id for _, stored in entries if stored.size == 0 and stored.config_id == config)
        else:
            available = {
                stored.item_id for stored in matching if stored.size == 1 and not carries_unknown(stored)
            }
            hints = [guid for guid in request.bundle.item_uuids if guid in available]
            removed.update(hints[:requested])
            taken = len(removed)
        for entries in (bag.items, *(box.items for box in bag.dynamic_bags)):
            for index in range(len(entries) - 1, -1, -1):
                if self.item(entries[index]).item_id in removed:
                    del entries[index]
        snapshot.recall_lines.add(config_id=config, requested=requested, taken=taken, shortfall=requested - taken)
        return Outcome.APPLIED, 0, False

    def _player_in_battle(self, player) -> bool:
        if self._in_battle is not None:
            return self._in_battle(player)
        battle = getattr(self.scene, "battle", None)
        return battle is not None and battle.frozen(player.player_id)

    async def _save(self, player, data, tx_type=0, *, on_committed=None, **audit_options) -> bool:
        before = self.scene.rows.assemble(player.snapshot)
        worn_changed = self.entries(before.bag_component, 2) != self.entries(data.bag_component, 2)
        if worn_changed:
            self._update_worn_stats(before, data)
        audit = self.make_audit_callback(player, before, data, tx_type, **audit_options)
        attributes = getattr(self.scene, "attributes", None) if worn_changed else None

        async def committed(snapshot):
            if audit is not None:
                result = audit(snapshot)
                if isawaitable(result):
                    await result
            if attributes is not None:
                attributes.notify(player)
            if on_committed is not None:
                result = on_committed(snapshot)
                if isawaitable(result):
                    await result

        callback = committed if attributes is not None or on_committed is not None else audit
        try:
            await self.scene.commit_player_data(player, data, on_committed=callback)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("玩家资产存盘未获确认 player_id=%s", player.player_id, exc_info=True)
            return False
        return True

    def _update_worn_stats(self, before, data) -> None:
        from .scene_skills import _rescale
        from .scene_stats import player_stats

        old = player_stats(self.protocol, self.tables, before)
        new = player_stats(self.protocol, self.tables, data)
        base = data.derived_attributes_component
        base.health = _rescale(base.health, old.max_health, new.max_health)
        base.mana = _rescale(base.mana, old.max_mana, new.max_mana)
        base.speed = new.speed

    def _client_answer(self, route, tip=0):
        response = self.protocol.message(route.response_type)
        if tip:
            response.error_message.id = tip
        return response

    async def _wallet_rpc(self, world, player, route, request):
        if not self.allow_gm:
            return self._client_answer(route, FEATURE_UNAVAILABLE)
        data = self.scene.rows.assemble(player.snapshot)
        kind = request.currency_type
        if kind not in self.currency_types:
            return self._client_answer(route, INVALID)
        method = route.method
        if not self.scene.mutable(player):
            error = _FROZEN if method in ("GmAddCurrency", "GmDeductCurrency") else INVALID
            return self._client_answer(route, error)
        gains = []
        if method == "GmAddCurrency":
            error = self.add_currency(data, kind, request.amount, gains=gains)
        elif method == "GmDeductCurrency":
            error = self.deduct_currency(data, kind, request.amount)
        elif method == "GmBlockCurrency":
            if kind not in data.currency.blocked_types:
                data.currency.blocked_types.append(kind)
            error = 0
        else:
            if kind in data.currency.blocked_types:
                data.currency.blocked_types.remove(kind)
            error = 0
        if error:
            return self._client_answer(route, error)
        tx_type = {"GmAddCurrency": 9, "GmDeductCurrency": 10}.get(method, 0)
        if not await self._save(player, data, tx_type, credit=method == "GmAddCurrency", gains=gains):
            return self._client_answer(route, UNAVAILABLE)
        response = self._client_answer(route)
        if method in ("GmAddCurrency", "GmDeductCurrency"):
            response.balance_after = balance(data, kind)
        return response

    async def _sort_rpc(self, world, player, route, request):
        if not self.scene.mutable(player) or self._player_in_battle(player):
            return self._client_answer(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        error, changed = self.sort_bag(data, request.bag_type)
        if error:
            return self._client_answer(route, error)
        if changed and not await self._save(player, data, 13, retirement_only=True):
            return self._client_answer(route, UNAVAILABLE)
        return self._bag_reply(route, player, request.bag_type, changed=changed)

    async def _grant_rpc(self, world, player, route, request):
        if not self.allow_gm:
            return self._client_answer(route, FEATURE_UNAVAILABLE)
        if not self.scene.mutable(player):
            return self._client_answer(route, _EQUIP_FROZEN)
        if self._player_in_battle(player):
            return self._client_answer(route, _EQUIP_IN_BATTLE)
        if (
            not 0 < request.count <= 99
            or not request.config_id
            or self.tables["Item"].get(request.config_id) is None
        ):
            return self._client_answer(route, _EQUIP_GRANT_INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        gains = []
        error = await self.add_items(data, {request.config_id: request.count}, gains=gains)
        if error:
            return self._client_answer(route, error)
        if not await self._save(player, data, 9, gains=gains):
            return self._client_answer(route, UNAVAILABLE)
        return self._bag_reply(route, player, 0)

    async def _wear_rpc(self, world, player, route, request):
        if not request.item_id:
            return self._client_answer(route, INVALID)
        if not self.scene.mutable(player):
            return self._client_answer(route, _EQUIP_FROZEN)
        if self._player_in_battle(player):
            return self._client_answer(route, _EQUIP_IN_BATTLE)
        data = self.scene.rows.assemble(player.snapshot)
        error = self.equip_item(data, request.item_id, wear=route.method == "EquipItem")
        if error:
            return self._client_answer(route, error)
        if not await self._save(player, data):
            return self._client_answer(route, UNAVAILABLE)
        return self._paired_bags(route, player, 2, "equipment")

    async def _retrieve_rpc(self, world, player, route, request):
        if bool(request.item_id) == request.take_all:
            return self._client_answer(route, INVALID)
        if not self.scene.mutable(player):
            return self._client_answer(route, _TEMPORARY_BUSY)
        data = self.scene.rows.assemble(player.snapshot)
        error, changed = self.retrieve_temporary(data, request.item_id, request.take_all)
        if error:
            return self._client_answer(route, error)
        inbox = self.scene.inbox
        if changed and not await self._save(
            player, data, 13, retirement_only=True, item_extra={"source": "temporary-retrieve"},
            on_committed=(lambda _snapshot: inbox.on_bag_space_freed(player)) if inbox is not None else None,
        ):
            return self._client_answer(route, UNAVAILABLE)
        return self._paired_bags(route, player, 3, "temporary")

    def _equipment_entry(self, data, item_id):
        for kind in (0, 2):
            for entry, item in self.entries(data.bag_component, kind):
                if item.item_id != item_id:
                    continue
                row = self.tables["Item"].get(item.config_id)
                if row is None or not row.equip_kind or row.max_stack_size != 1:
                    return 28001, None, None, None
                if item.size != 1:
                    return 28000, None, None, None
                return 0, entry, item, row
        return 28000, None, None, None

    def _gear_gate(self, player) -> int:
        if not self.scene.mutable(player):
            return _EQUIP_FROZEN
        return _EQUIP_IN_BATTLE if self._player_in_battle(player) else 0

    async def _enhance_rpc(self, world, player, route, request):
        if not request.item_id:
            return self._client_answer(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, 1)
        if error:
            return self._client_answer(route, error)
        if prior is not None:
            response = self._paired_bags(route, player, 2, "equipment")
            response.replayed = True
            if not response.error_message.id:
                response.success, response.enhance_level = prior.enhance_success, prior.enhance_level
            return response
        error = self._gear_gate(player)
        if error:
            return self._client_answer(route, error)
        error, success, level = self._enhance(data, request)
        if error:
            return self._client_answer(route, error)
        client_request.remember(
            ledger, request.client_request_id, 1, fingerprint,
            enhance_success=success, enhance_level=level,
        )
        if not await self._save(
            player, data, 27, correlation=request.item_id,
            item_extra={"from": request.expected_level, "success": success},
        ):
            return self._client_answer(route, UNAVAILABLE)
        response = self._paired_bags(route, player, 2, "equipment")
        if not response.error_message.id:
            response.success, response.enhance_level = success, level
        return response

    def _enhance(self, data, request) -> tuple[int, bool, int]:
        error, _, item, _ = self._equipment_entry(data, request.item_id)
        if error:
            return error, False, 0
        table = self.tables["EquipEnhance"]
        maximum = 0
        while maximum < len(table) and table.get(maximum + 1) is not None:
            maximum += 1
        if not maximum or not item.HasField("equip"):
            return _ENHANCE_UNAVAILABLE, False, 0
        current = min(item.equip.enhance_level, maximum)
        if request.expected_level != current:
            return _ENHANCE_LEVEL_CHANGED, False, 0
        if current >= maximum:
            return _ENHANCE_MAX, False, 0
        rule = table.get(current + 1)
        if not rule.success_rate:
            return _ENHANCE_UNAVAILABLE, False, 0
        if rule.material_id and rule.material_count:
            material = self.tables["Item"].get(rule.material_id)
            if material is None or material.equip_kind:
                return _EQUIP_INTERNAL, False, 0
            error = self.consume_items(data, {rule.material_id: rule.material_count})
            if error == BAG_INSUFFICIENT:
                return _ENHANCE_MATERIAL, False, 0
            if error:
                return error, False, 0
        if rule.cost_gold and self.deduct_currency(data, 0, rule.cost_gold):
            return _ENHANCE_GOLD, False, 0
        error, entry, item, _ = self._equipment_entry(data, request.item_id)
        if error:
            return _EQUIP_INTERNAL, False, 0
        success = randbelow(10_000) < rule.success_rate
        level = current + 1 if success else max(0, current - rule.fail_level_loss)
        item.equip.enhance_level = level
        self.write_entry(entry, item)
        return 0, success, level

    async def _gem_rpc(self, world, player, route, request):
        inlay = route.method == "InlayGem"
        if not request.item_id or inlay and not request.gem_config_id:
            return self._client_answer(route, INVALID)
        error = self._gear_gate(player)
        if error:
            return self._client_answer(route, error)
        data = self.scene.rows.assemble(player.snapshot)
        error, _, item, row = self._equipment_entry(data, request.item_id)
        if error:
            return self._client_answer(route, error)
        socket = request.socket
        occupied = next((gem for gem in item.equip.gems if gem.socket == socket and gem.gem_config_id), None)
        gains = []
        if inlay:
            error = self._inlay(data, item, row, socket, occupied, request.gem_config_id)
        else:
            error = await self._remove_gem(data, occupied, gains)
        if error:
            return self._client_answer(route, error)
        error, entry, item, _ = self._equipment_entry(data, request.item_id)
        if error:
            return self._client_answer(route, _EQUIP_INTERNAL)
        if inlay:
            item.equip.gems.add(socket=socket, gem_config_id=request.gem_config_id)
        else:
            for index in range(len(item.equip.gems) - 1, -1, -1):
                if item.equip.gems[index].socket == socket:
                    del item.equip.gems[index]
        self.write_entry(entry, item)
        if not await self._save(
            player, data, 28 if inlay else 29, correlation=request.item_id, item_extra={"socket": socket}, gains=gains,
        ):
            return self._client_answer(route, UNAVAILABLE)
        return self._paired_bags(route, player, 2, "equipment")

    def _inlay(self, data, item, row, socket: int, occupied, config: int) -> int:
        if not item.HasField("equip"):
            return _SOCKET_INVALID
        gem = self.tables["EquipGem"].get(config)
        gem_item = self.tables["Item"].get(config)
        if gem is None or gem_item is None or gem_item.equip_kind:
            return _GEM_INVALID
        definition = next(
            (slot for slot in self.tables["EquipGemSocket"].find_all(row.equip_kind, field="equip_kind")
             if slot.socket == socket), None,
        )
        if definition is None:
            return _SOCKET_INVALID
        if occupied is not None:
            return _SOCKET_OCCUPIED
        if definition.gem_type and definition.gem_type != gem.gem_type:
            return _GEM_TYPE_MISMATCH
        if len(item.equip.gems) >= 8:
            return _EQUIP_INTERNAL
        error = self.consume_items(data, {config: 1})
        return _GEM_NOT_OWNED if error == BAG_INSUFFICIENT else error

    async def _remove_gem(self, data, occupied, gains) -> int:
        if occupied is None:
            return _SOCKET_EMPTY
        config = occupied.gem_config_id
        item = self.tables["Item"].get(config)
        if item is None or item.equip_kind:
            return _EQUIP_INTERNAL
        error = await self.add_items(data, {config: 1}, gains=gains)
        if error == BAG_FULL:
            return _GEM_BAG_FULL
        return error if error in (0, BLOCKED) else _EQUIP_INTERNAL

    def _paired_bags(self, route, player, bag_type: int, field: str):
        first = _bag(self.scene, self.tables, player, 0)
        second = _bag(self.scene, self.tables, player, bag_type)
        response = self._client_answer(route, first.error_message.id or second.error_message.id)
        if not response.error_message.id:
            response.bag.CopyFrom(first.bag)
            getattr(response, field).CopyFrom(second.bag)
        return response

    def _bag_reply(self, route, player, bag_type, *, changed=None):
        result = _bag(self.scene, self.tables, player, bag_type)
        response = self._client_answer(route, result.error_message.id)
        if result.HasField("bag"):
            response.bag.CopyFrom(result.bag)
        if changed is not None:
            response.changed = changed
        return response
