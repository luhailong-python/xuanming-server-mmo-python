"""Scene 神秘商人读模型和原子购买。"""

from __future__ import annotations

import asyncio
import logging
import math
import time

from google.protobuf.message import DecodeError

from ..data import client_request
from ..data.asset_inventory import BAG_FULL, BLOCKED, UNAVAILABLE, balance, clone
from .scene_player import _bag

logger = logging.getLogger(__name__)

_SERVICE = "SceneMysteryMerchantClientPlayer"
_NOT_PRESENT = 30000
_NOT_HERE = 30001
_TOO_FAR = 30002
_SHELF_CHANGED = 30003
_LIMIT_REACHED = 30004
_CURRENCY_NOT_ENOUGH = 30005
_BAG_FULL = 30006
_IN_BATTLE = 30007
_FROZEN = 30008
_INVALID_COUNT = 30009
_ITEM_BLOCKED = 30010
_INTERNAL = 30011


def redis_key(zone: int) -> str:
    return f"mystery_merchant:zone:{zone}"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _version(state) -> int:
    return state.round_id * 1000 + state.revision


def _active(state, now_ms: int) -> bool:
    return state is not None and state.appear_at_ms <= now_ms < state.expire_at_ms


def _next_appearance(state, now_ms: int) -> int:
    interval = state.interval_seconds * 1000
    if not interval:
        return 0
    if now_ms < state.appear_at_ms:
        return state.appear_at_ms
    rounds, remainder = divmod(now_ms - state.appear_at_ms, interval)
    previous = now_ms - remainder
    return previous if rounds and remainder < 30_000 else previous + interval


class SceneMerchant:
    def __init__(self, scene, tables, assets, *, read_state=None, clock=_now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.read_state = read_state
        self.clock = clock
        self.state = None
        self._announced = 0

    def register(self) -> None:
        self.scene.register(_SERVICE, "GetMysteryMerchantInfo", self.query)
        self.scene.register(_SERVICE, "BuyMysteryMerchantGoods", self.buy, mutating=True)

    async def refresh_loop(self) -> None:
        if self.read_state is None:
            raise RuntimeError("神秘商人缺少 SceneControlRedis 读取器")
        while True:
            try:
                async with asyncio.timeout(3):
                    payload = await self.read_state()
                if payload is None:
                    self.apply_state(None)
                elif isinstance(payload, bytes):
                    self.apply_state(self.protocol.parse("MysteryMerchantState", payload))
                else:
                    logger.warning("神秘商人 Redis 记录类型无效")
            except (OSError, DecodeError, ValueError):
                logger.warning("读取神秘商人记录失败，保留现有记录", exc_info=True)
            except Exception as exc:
                if not type(exc).__module__.startswith("redis"):
                    raise
                logger.warning("神秘商人 Redis 读失败，保留现有记录", exc_info=True)
            await asyncio.sleep(5)

    def apply_state(self, state) -> bool:
        if state is None:
            self.state = None
            return True
        if state.zone_id != self.scene.info.zone_id:
            return False
        now = self.clock()
        version = _version(state)
        if self.state is not None and now < self.state.expire_at_ms and version < _version(self.state):
            return False
        self.state = clone(state)
        if version <= self._announced or now < state.appear_at_ms:
            return True
        self._announced = version
        if _active(state, now):
            self._broadcast(now)
        return True

    def _broadcast(self, now: int) -> None:
        push = self.protocol.message("MysteryMerchantNotify", info=self._info(None, None, now))
        for player in tuple(self.scene.sessions.values()):
            if player.session_id and player.disconnect_at is None:
                self.scene._push(player, _SERVICE + "NotifyMysteryMerchant", push)

    def _range(self) -> int:
        rules = self.tables["MysteryMerchantRule"].rows()
        return min(100, rules[0].interact_range or 8) if rules else 8

    def validate_tables(self) -> bool:
        """校验神秘商人规则与商品表可售性（对齐 C++ PlayerMysteryMerchantSystem::ValidateTables）。"""
        all_valid = True
        rules = self.tables["MysteryMerchantRule"].rows() if "MysteryMerchantRule" in self.tables.tables else ()
        if not rules:
            logger.error("MysteryMerchantRule table is empty; purchase range falls back to 8")
            all_valid = False
        else:
            interact_range = int(rules[0].interact_range)
            if not 1 <= interact_range <= 100:
                logger.error(
                    "MysteryMerchantRule.interact_range=%d is not in 1..100, effective value %d",
                    interact_range, self._range(),
                )
                all_valid = False

        sellable = 0
        goods_rows = self.tables["MysteryMerchantGoods"].rows() if "MysteryMerchantGoods" in self.tables.tables else ()
        for row in goods_rows:
            reason = self._goods_row_invalid_reason(row)
            if reason is not None:
                logger.error(
                    "MysteryMerchantGoods row is not sellable: id=%d reason=%s",
                    int(row.id), reason,
                )
                all_valid = False
                continue
            sellable += 1
        spots = len(self.tables["MysteryMerchantSpot"].rows()) if "MysteryMerchantSpot" in self.tables.tables else 0
        logger.info(
            "MysteryMerchant table check finished: sellable_goods=%d spots=%d interact_range=%d",
            sellable, spots, self._range(),
        )
        return all_valid

    def _goods_row_invalid_reason(self, row) -> str | None:
        if int(row.weight) <= 0:
            return "weight<=0"
        if not 1 <= int(row.price) <= 1_000_000_000:
            return "price_out_of_range"
        if not 1 <= int(row.limit_count) <= 99:
            return "limit_count_out_of_range"
        if int(row.currency_type) not in self.assets.currency_types:
            return "invalid_currency_type"
        item = self.tables["Item"].get(int(row.item_id))
        if item is None:
            return "item_not_found"
        if not 1 <= int(row.item_count) <= int(item.max_stack_size):
            return "item_count_out_of_range"
        return None

    def _goods(self, entry):
        goods = self.tables["MysteryMerchantGoods"].get(entry.goods_id)
        if goods is None or not goods.weight or not 1 <= entry.discount_percent <= 100:
            return None
        item = self.tables["Item"].get(goods.item_id)
        if (
            not 1 <= goods.price <= 1_000_000_000 or not 1 <= goods.limit_count <= 99
            or goods.currency_type not in self.assets.currency_types
            or item is None or not 1 <= goods.item_count <= item.max_stack_size
        ):
            return None
        return self.protocol.message(
            "MysteryMerchantGoodsInfo", goods_id=goods.id, item_config_id=goods.item_id,
            item_count=goods.item_count, name=goods.name or item.name, description=item.description,
            icon_key=item.icon_key, currency_type=goods.currency_type, original_price=goods.price,
            price=(goods.price * entry.discount_percent + 99) // 100,
            discount_percent=entry.discount_percent, limit_count=goods.limit_count,
        )

    @staticmethod
    def _purchased(record, round_id: int, goods_id: int):
        if record.round_id != round_id:
            return None
        return next((row for row in record.purchases if row.goods_id == goods_id), None)

    def _info(self, world, data, now: int):
        state = self.state
        info = self.protocol.message("MysteryMerchantInfo", server_time_ms=now)
        if state is None:
            return info
        info.next_appear_at_ms = _next_appearance(state, now)
        if not _active(state, now):
            return info
        info.active = True
        info.round_id, info.scene_id = state.round_id, state.scene_id
        info.scene_config_id, info.channel_no = state.scene_config_id, state.channel_no
        info.position.CopyFrom(state.position)
        info.appear_at_ms, info.expire_at_ms = state.appear_at_ms, state.expire_at_ms
        info.interact_range = self._range()
        info.here = world is not None and world.info.scene_id == state.scene_id
        if info.here and data is not None:
            for entry in state.shelf:
                goods = self._goods(entry)
                if goods is None:
                    continue
                purchased = self._purchased(data.mystery_merchant, state.round_id, goods.goods_id)
                goods.bought_count = purchased.count if purchased is not None else 0
                info.goods.add().CopyFrom(goods)
        return info

    def query(self, world, player, route, request):
        data = self.scene.rows.assemble(player.snapshot)
        return self.protocol.message(
            "GetMysteryMerchantInfoResponse", info=self._info(world, data, self.clock()),
        )

    async def buy(self, world, player, route, request):
        data = self.scene.rows.assemble(player.snapshot)
        response = self.protocol.message("BuyMysteryMerchantGoodsResponse")
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, 2)
        if error:
            response.error_message.id = error
            return response
        if prior is not None:
            response.replayed = True
            return self._success(response, world, player, data)

        state = self.state
        error, goods = self._validate(world, player, data, state, request)
        if error:
            response.error_message.id = error
            return response
        cost = goods.price * request.count
        units = goods.item_count * request.count
        if not 0 < cost < 1 << 63 or not 0 < units < 1 << 32:
            response.error_message.id = _INTERNAL
            return response
        if self.assets.item_gain_blocked(data, goods.item_config_id):
            response.error_message.id = _ITEM_BLOCKED
            return response
        if balance(data, goods.currency_type) < cost:
            response.error_message.id = _CURRENCY_NOT_ENOUGH
            return response
        gains = []
        error = await self.assets.add_items(data, {goods.item_config_id: units}, gains=gains)
        if error:
            response.error_message.id = _BAG_FULL if error == BAG_FULL else _ITEM_BLOCKED if error == BLOCKED else _INTERNAL
            return response
        # Python 可在发号时挂起，防止一笔请求等待期间跨过截止线或遇到换线。
        current = self.state
        if not _active(current, self.clock()):
            response.error_message.id = _NOT_PRESENT
            return response
        if _version(current) != _version(state) or current.scene_id != state.scene_id:
            response.error_message.id = _SHELF_CHANGED
            return response
        if self.assets.deduct_currency(data, goods.currency_type, cost):
            response.error_message.id = _INTERNAL
            return response
        record = data.mystery_merchant
        if record.round_id != state.round_id:
            record.Clear()
            record.round_id = state.round_id
        purchased = self._purchased(record, state.round_id, goods.goods_id)
        if purchased is None:
            purchased = record.purchases.add(goods_id=goods.goods_id)
        purchased.count += request.count
        client_request.remember(ledger, request.client_request_id, 2, fingerprint)
        if not await self.assets._save(
            player, data, 11, correlation=state.round_id, gains=gains,
            item_extra={
                "source": "mystery_merchant", "round_id": state.round_id, "goods_id": goods.goods_id,
                "count": request.count, "unit_price": goods.price, "discount_percent": goods.discount_percent,
            },
        ):
            response.error_message.id = UNAVAILABLE
            return response
        return self._success(response, world, player, data)

    def _validate(self, world, player, data, state, request):
        if not self.scene.mutable(player):
            return _FROZEN, None
        if self.assets._player_in_battle(player):
            return _IN_BATTLE, None
        if not request.goods_id or not 1 <= request.count <= 99:
            return _INVALID_COUNT, None
        if not _active(state, self.clock()):
            return _NOT_PRESENT, None
        if request.round_id != state.round_id:
            return _SHELF_CHANGED, None
        if world.info.scene_id != state.scene_id:
            return _NOT_HERE, None
        x, y, _ = player.transform.position
        distance = math.hypot(x - state.position.x, y - state.position.y)
        if not distance <= self._range():
            return _TOO_FAR, None
        entry = next((entry for entry in state.shelf if entry.goods_id == request.goods_id), None)
        if entry is None:
            return _SHELF_CHANGED, None
        goods = self._goods(entry)
        if goods is None:
            return _INTERNAL, None
        if (
            request.expected_price != goods.price or request.expected_currency_type != goods.currency_type
            or request.expected_item_config_id != goods.item_config_id or request.expected_item_count != goods.item_count
        ):
            return _SHELF_CHANGED, None
        purchased = self._purchased(data.mystery_merchant, state.round_id, goods.goods_id)
        already_bought = purchased.count if purchased is not None else 0
        if already_bought + request.count > goods.limit_count:
            return _LIMIT_REACHED, None
        return 0, goods

    def _success(self, response, world, player, data):
        response.info.CopyFrom(self._info(world, data, self.clock()))
        bag = _bag(self.scene, self.tables, player, 0)
        if not bag.error_message.id:
            response.bag.CopyFrom(bag.bag)
        return response
