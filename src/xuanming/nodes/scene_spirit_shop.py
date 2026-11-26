"""Scene 官方灵石百宝阁系统（对齐 C++ spirit_shop_rules.h 与 player_spirit_shop.cpp）。

承接真钱充值灵石（CURRENCY_TYPE_DIAMOND = 1）的官方绑定货架：
- 支持单物品与双物品原子直发并按 ``(config_id, bound)`` 严格分堆防洗白；
- 支持单商品限购与共享限购组权重上限（终身 / 每周 UTC+8 周一 05:00 重置，抗时钟回拨）；
- 强制非 0 ``client_request_id`` 接入 ``ClientRequestLedgerComp``（kind = 15），报价防呆，
  同一次存盘事务原子提交 ``WALLET``（扣灵石 + 限购 + 幂等）与 ``BAG``（发物），写 ``TX_SPIRIT_SHOP_BUY = 44`` 审计流水。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..data import client_request
from ..data.asset_anomaly import AssetGain
from ..data.asset_inventory import (
    BAG_FULL,
    BLOCKED,
    INSUFFICIENT,
    INVALID,
    MAX_NEW_STACKS,
    UNAVAILABLE,
    bag_capacity,
    balance,
    carries_unknown,
    clone,
)
from ..data.asset_ledger import INT64_MAX, UINT64_MAX
from .scene_mission import effective_level
from .scene_player import _bag

logger = logging.getLogger(__name__)

_SERVICE = "SceneSpiritShopClientPlayer"
CLIENT_REQUEST_KIND_BUY_SPIRIT_SHOP = 15

CURRENCY_DIAMOND = 1
CURRENCY_BIND_DIAMOND = 2
TX_SPIRIT_SHOP_BUY = 44

RESET_LIFETIME = 0
RESET_WEEKLY = 1

GRANT_MODE_ALL = 0
GRANT_MODE_CHOOSE_ONE = 1
MAX_GOODS_ITEMS = 4

MAX_UNIT_PRICE = 1_000_000_000
MAX_BUY_COUNT_PER_REQUEST = 99
MAX_LIMIT_GROUP_WEIGHT = 1_000_000
MAX_GOODS_RECORDS = 512
MAX_GROUP_RECORDS = 128

_MAX_UINT32 = 0xFFFFFFFF

# 与 C++ PlayerEquipmentMaterialReward / spirit_shop_rules 同一游戏周口径：UTC+8 周一 05:00。
# 1970-01-05 05:00:00 +08:00 == 1970-01-04 21:00:00 UTC == 334,800,000 ms。
WEEK_MS = 7 * 24 * 3600 * 1000
FIRST_MONDAY_05_UTC8_MS = 334_800_000

# 对应 C++ spirit_shop_error_tip.pb.h 错误码（37000..37013）。
SPIRIT_SHOP_UNAVAILABLE = 37000
SPIRIT_SHOP_GOODS_OFF_SHELF = 37001
SPIRIT_SHOP_PRICE_CHANGED = 37002
SPIRIT_SHOP_INVALID_COUNT = 37003
SPIRIT_SHOP_LIMIT_REACHED = 37004
SPIRIT_SHOP_GROUP_LIMIT_REACHED = 37005
SPIRIT_SHOP_CURRENCY_NOT_ENOUGH = 37006
SPIRIT_SHOP_BAG_FULL = 37007
SPIRIT_SHOP_FROZEN = 37008
SPIRIT_SHOP_IN_BATTLE = 37009
SPIRIT_SHOP_ITEM_BLOCKED = 37010
SPIRIT_SHOP_NOT_IN_SALE_WINDOW = 37011
SPIRIT_SHOP_LEVEL_NOT_MET = 37012
SPIRIT_SHOP_INVALID_CHOICE = 37013


@dataclass(frozen=True, slots=True)
class ResolvedItem:
    """百宝阁单件发放条目快照。"""

    item_config_id: int
    item_count: int
    bound: bool


@dataclass(frozen=True, slots=True)
class ResolvedLimitGroup:
    """百宝阁共享限购组配置快照。"""

    group_id: int
    name: str
    reset_kind: int
    max_weight: int


@dataclass(frozen=True, slots=True)
class ResolvedGoods:
    """百宝阁单行在售商品快照（支持 V2 定时上下架、等级阶段、多物品与多选一）。"""

    goods_id: int
    tab_id: int
    sort_order: int
    name: str
    description: str
    icon_key: str
    badge_text: str
    currency_type: int
    unit_price: int
    max_buy_per_request: int
    items: tuple[ResolvedItem, ...]
    limit_reset_kind: int
    limit_count: int
    limit_group_id: int
    limit_group_weight: int
    start_time_ms: int = 0
    end_time_ms: int = 0
    grant_mode: int = GRANT_MODE_ALL
    original_price: int = 0
    min_level: int = 0
    max_level: int = 0


def is_valid_reset_kind(reset_kind: int) -> bool:
    """校验限购重置周期：0=终身，1=每周。"""
    return reset_kind in (RESET_LIFETIME, RESET_WEEKLY)


def is_valid_grant_mode(grant_mode: int) -> bool:
    """校验发放模式：0=ALL 全部发放，1=CHOOSE_ONE 多选一。"""
    return grant_mode in (GRANT_MODE_ALL, GRANT_MODE_CHOOSE_ONE)


def is_in_sale_window(start_time_ms: int, end_time_ms: int, now_ms: int) -> bool:
    """定时上下架判定：左闭右开 [start_time_ms, end_time_ms)，任一端为 0 表示该侧不限。"""
    if start_time_ms > 0 and now_ms < start_time_ms:
        return False
    if end_time_ms > 0 and now_ms >= end_time_ms:
        return False
    return True


def is_level_eligible(eff_level: int, min_level: int, max_level: int) -> bool:
    """角色有效等级阶段解锁判定：任一端为 0 表示该侧不限。"""
    if min_level > 0 and eff_level < min_level:
        return False
    if max_level > 0 and eff_level > max_level:
        return False
    return True


def resolve_selected_items(goods: ResolvedGoods, chosen_item_index: int) -> tuple[ResolvedItem, ...] | None:
    """根据发放模式解析本次实际发入背包的物品列表。"""
    if not goods.items:
        return None
    if goods.grant_mode == GRANT_MODE_ALL:
        if chosen_item_index != 0:
            return None
        return goods.items
    if goods.grant_mode == GRANT_MODE_CHOOSE_ONE:
        if len(goods.items) < 2 or not 0 <= chosen_item_index < len(goods.items):
            return None
        return (goods.items[chosen_item_index],)
    return None


def resolve_week(now_ms: int) -> tuple[int, int] | None:
    """计算当前 UTC+8 周一 05:00 游戏周区间 ``(start_ms, next_reset_ms)``。"""
    if now_ms < FIRST_MONDAY_05_UTC8_MS:
        return None
    offset = (now_ms - FIRST_MONDAY_05_UTC8_MS) % WEEK_MS
    start = now_ms - offset
    if start > UINT64_MAX - WEEK_MS:
        return None
    return start, start + WEEK_MS


def total_cost(unit_price: int, count: int) -> int | None:
    """计算总价并校验上下界与 int64 溢出。"""
    if not 1 <= unit_price <= MAX_UNIT_PRICE or not 1 <= count <= MAX_BUY_COUNT_PER_REQUEST:
        return None
    total = unit_price * count
    return total if total <= INT64_MAX else None


def total_group_weight(weight_per_unit: int, count: int) -> int | None:
    """计算本次购买占用的共享组总权重并防 uint32 溢出。"""
    if not 1 <= weight_per_unit <= MAX_LIMIT_GROUP_WEIGHT or not 1 <= count <= MAX_BUY_COUNT_PER_REQUEST:
        return None
    total = weight_per_unit * count
    return total if total <= _MAX_UINT32 else None


def bought_count(comp, goods_id: int, reset_kind: int, week_start_ms: int) -> int:
    """读取单商品当前生效已购份数；周限购遇时钟回拨保守沿用旧值不重置。"""
    for rec in comp.goods_records:
        if rec.goods_id != goods_id:
            continue
        if reset_kind == RESET_WEEKLY:
            return 0 if week_start_ms > rec.week_starts_at_ms else int(rec.weekly_bought)
        return int(rec.lifetime_bought)
    return 0


def used_group_weight(comp, group_id: int, reset_kind: int, week_start_ms: int) -> int:
    """读取共享限购组当前生效已用权重（抗时钟回拨同上）。"""
    for rec in comp.group_records:
        if rec.group_id != group_id:
            continue
        if reset_kind == RESET_WEEKLY:
            return 0 if week_start_ms > rec.week_starts_at_ms else int(rec.weekly_used)
        return int(rec.lifetime_used)
    return 0


def would_exceed_limit(used: int, delta: int, limit: int) -> bool:
    """检查累加 ``delta`` 后是否超出上限（``limit == 0`` 表示不限购）。"""
    return limit > 0 and used + delta > limit


def expected_items_match(request, actual_items: tuple[ResolvedItem, ...]) -> bool:
    """核对客户端报价防呆中的物品清单：条数、顺序、模板 ID、数量与绑定态须逐项一致。"""
    if len(request.expected_items) != len(actual_items):
        return False
    for exp, act in zip(request.expected_items, actual_items):
        if (
            int(exp.item_config_id) != act.item_config_id
            or int(exp.item_count) != act.item_count
            or bool(exp.bound) != act.bound
        ):
            return False
    return True


def validate_comp(comp) -> str | None:
    """校验 ``PlayerSpiritShopComp`` 结构合法性；合法返回 ``None``，否则返回英文原因。"""
    if len(comp.goods_records) > MAX_GOODS_RECORDS:
        return "goods_records exceeds capacity"
    if len(comp.group_records) > MAX_GROUP_RECORDS:
        return "group_records exceeds capacity"
    seen_goods: set[int] = set()
    for rec in comp.goods_records:
        gid = int(rec.goods_id)
        if gid == 0:
            return "goods_records contains zero goods_id"
        if gid in seen_goods:
            return "goods_records contains duplicate goods_id"
        seen_goods.add(gid)
    seen_groups: set[int] = set()
    for rec in comp.group_records:
        grid = int(rec.group_id)
        if grid == 0:
            return "group_records contains zero group_id"
        if grid in seen_groups:
            return "group_records contains duplicate group_id"
        seen_groups.add(grid)
    return None


def record_purchase(
    comp,
    goods_id: int,
    count: int,
    goods_limit_count: int,
    group_id: int,
    add_group_weight: int,
    week_start_ms: int,
) -> bool:
    """在候选组件上记录一次成交（单品计数、共享组权重、revision + 1）。"""
    if int(comp.shop_revision) >= UINT64_MAX:
        return False
    if goods_limit_count > 0:
        target = next((rec for rec in comp.goods_records if rec.goods_id == goods_id), None)
        if target is None:
            if len(comp.goods_records) >= MAX_GOODS_RECORDS:
                return False
            target = comp.goods_records.add(goods_id=goods_id)
        next_lifetime = int(target.lifetime_bought) + count
        if next_lifetime > _MAX_UINT32:
            return False
        target.lifetime_bought = next_lifetime
        if week_start_ms > int(target.week_starts_at_ms):
            target.week_starts_at_ms = week_start_ms
            target.weekly_bought = count
        else:
            next_weekly = int(target.weekly_bought) + count
            if next_weekly > _MAX_UINT32:
                return False
            target.weekly_bought = next_weekly

    if group_id > 0 and add_group_weight > 0:
        target = next((rec for rec in comp.group_records if rec.group_id == group_id), None)
        if target is None:
            if len(comp.group_records) >= MAX_GROUP_RECORDS:
                return False
            target = comp.group_records.add(group_id=group_id)
        next_lifetime = int(target.lifetime_used) + add_group_weight
        if next_lifetime > _MAX_UINT32:
            return False
        target.lifetime_used = next_lifetime
        if week_start_ms > int(target.week_starts_at_ms):
            target.week_starts_at_ms = week_start_ms
            target.weekly_used = add_group_weight
        else:
            next_weekly = int(target.weekly_used) + add_group_weight
            if next_weekly > _MAX_UINT32:
                return False
            target.weekly_used = next_weekly

    comp.shop_revision = int(comp.shop_revision) + 1
    return True


def _default_now_ms() -> int:
    return time.time_ns() // 1_000_000


class SceneSpiritShop:
    """官方灵石百宝阁客户端 RPC 处理器（509 查询 / 510 原子购买）。"""

    def __init__(
        self,
        scene,
        tables,
        assets,
        *,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.now_ms = now_ms or _default_now_ms
        tx_enum = self.protocol.pool.FindEnumTypeByName("TransactionType")
        buy_tx = tx_enum.values_by_name.get("TX_SPIRIT_SHOP_BUY")
        self.buy_tx = buy_tx.number if buy_tx is not None else TX_SPIRIT_SHOP_BUY

    def register(self) -> None:
        """向 SceneNode 注册 509 / 510 客户端 RPC 入口。"""
        self.scene.register(_SERVICE, "GetSpiritShopInfo", self.get_info)
        self.scene.register(_SERVICE, "BuySpiritShopGoods", self.buy_goods, mutating=True)

    def _table(self, name: str):
        try:
            return self.tables[name]
        except KeyError:
            return None

    def _resolve_limit_group(self, group_id: int) -> tuple[ResolvedLimitGroup | None, str]:
        table = self._table("SpiritShopLimitGroup")
        row = None if table is None else table.get(group_id)
        if row is None:
            return None, "limit group row not found"
        gid = int(row.id)
        if gid == 0:
            return None, "limit group id is 0"
        reset_kind = int(row.reset_kind)
        if not is_valid_reset_kind(reset_kind):
            return None, "limit group reset_kind is invalid"
        max_weight = int(row.max_weight)
        if max_weight == 0:
            return None, "limit group max_weight is 0"
        return ResolvedLimitGroup(
            group_id=gid,
            name=str(row.name),
            reset_kind=reset_kind,
            max_weight=max_weight,
        ), ""

    def _resolve_goods_row(self, row) -> tuple[ResolvedGoods | None, str]:
        gid = int(row.id)
        if gid == 0:
            return None, "goods id is 0"
        if int(row.enabled) == 0:
            return None, "goods disabled"
        tab_id = int(row.tab_id)
        if tab_id == 0:
            return None, "tab_id is 0"
        currency_type = int(row.currency_type)
        if currency_type not in self.assets.currency_types:
            return None, "unknown currency_type"
        price = int(row.price)
        if not 1 <= price <= MAX_UNIT_PRICE:
            return None, "price not in 1..1000000000"
        original_price = int(getattr(row, "original_price", 0))
        if original_price > 0 and (original_price <= price or original_price > MAX_UNIT_PRICE):
            return None, "original_price must be 0 or strictly greater than price and <= 1000000000"
        start_time_ms = int(getattr(row, "start_time_ms", 0))
        end_time_ms = int(getattr(row, "end_time_ms", 0))
        if start_time_ms > 0 and end_time_ms > 0 and end_time_ms <= start_time_ms:
            return None, "end_time_ms must be greater than start_time_ms"
        min_level = int(getattr(row, "min_level", 0))
        max_level = int(getattr(row, "max_level", 0))
        if min_level > 0 and max_level > 0 and max_level < min_level:
            return None, "max_level must be >= min_level"
        grant_mode = int(getattr(row, "grant_mode", GRANT_MODE_ALL))
        if not is_valid_grant_mode(grant_mode):
            return None, "grant_mode must be 0 (ALL) or 1 (CHOOSE_ONE)"
        max_buy = int(row.max_buy_per_request)
        if not 1 <= max_buy <= MAX_BUY_COUNT_PER_REQUEST:
            return None, "max_buy_per_request not in 1..99"
        limit_reset_kind = int(row.limit_reset_kind)
        if not is_valid_reset_kind(limit_reset_kind):
            return None, "limit_reset_kind is invalid"

        items_table = self._table("Item")
        raw_slots = (
            (int(row.item_id_1), int(row.item_count_1), int(row.item_bound_1)),
            (int(row.item_id_2), int(row.item_count_2), int(row.item_bound_2)),
            (int(getattr(row, "item_id_3", 0)), int(getattr(row, "item_count_3", 0)), int(getattr(row, "item_bound_3", 0))),
            (int(getattr(row, "item_id_4", 0)), int(getattr(row, "item_count_4", 0)), int(getattr(row, "item_bound_4", 0))),
        )
        resolved_items: list[ResolvedItem] = []
        first_item = None
        seen_empty = False
        for item_id, item_count, item_bound in raw_slots:
            if item_bound not in (0, 1):
                return None, "item_bound must be 0 or 1"
            if item_id == 0:
                if item_count != 0 or item_bound != 0:
                    return None, "empty item slot has non-zero count or bound"
                seen_empty = True
                continue
            if seen_empty:
                return None, "item slots must not contain gaps"
            item_row = None if items_table is None else items_table.get(item_id)
            if item_row is None or item_count == 0:
                return None, "item_id missing or item_count is 0"
            if int(item_row.max_stack_size) == 0 or item_count * max_buy > _MAX_UINT32:
                return None, "item stack size invalid or count overflows"
            bound = bool(item_bound)
            for existing in resolved_items:
                if grant_mode == GRANT_MODE_ALL:
                    if existing.item_config_id == item_id and existing.bound == bound:
                        return None, "ALL bundle has duplicate (item_id, bound)"
                elif existing.item_config_id == item_id and existing.item_count == item_count and existing.bound == bound:
                    return None, "CHOOSE_ONE bundle has duplicate option"
            if first_item is None:
                first_item = item_row
            resolved_items.append(ResolvedItem(item_id, item_count, bound))

        if not resolved_items or first_item is None:
            return None, "item_id_1 missing or item_count_1 is 0"
        if grant_mode == GRANT_MODE_CHOOSE_ONE and len(resolved_items) < 2:
            return None, "CHOOSE_ONE requires at least 2 item options"

        group_id = int(row.limit_group_id)
        group_weight = 0
        if group_id != 0:
            group, reason = self._resolve_limit_group(group_id)
            if group is None:
                return None, reason
            group_weight = int(row.limit_group_weight)
            if not 1 <= group_weight <= MAX_LIMIT_GROUP_WEIGHT:
                return None, "limit_group_weight not in 1..1000000"

        return ResolvedGoods(
            goods_id=gid,
            tab_id=tab_id,
            sort_order=int(row.sort_order),
            name=str(row.name) or str(first_item.name),
            description=str(row.description) or str(first_item.description),
            icon_key=str(row.icon_key) or str(first_item.icon_key),
            badge_text=str(row.badge_text),
            currency_type=currency_type,
            unit_price=price,
            max_buy_per_request=max_buy,
            items=tuple(resolved_items),
            limit_reset_kind=limit_reset_kind,
            limit_count=int(row.limit_count),
            limit_group_id=group_id,
            limit_group_weight=group_weight,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            grant_mode=grant_mode,
            original_price=original_price,
            min_level=min_level,
            max_level=max_level,
        ), ""

    def validate_tables(self) -> bool:
        """校验 ``SpiritShopLimitGroup`` 与在售 ``SpiritShopGoods`` 配表行（fail-closed，不崩服）。"""
        all_valid = True
        valid_groups = 0
        group_table = self._table("SpiritShopLimitGroup")
        if group_table is None:
            logger.error("[SpiritShop] SpiritShopLimitGroup table is missing")
            all_valid = False
        else:
            for row in group_table.rows():
                group, reason = self._resolve_limit_group(int(row.id))
                if group is None:
                    logger.error("[SpiritShop] Invalid SpiritShopLimitGroup row: id=%d reason=%s", row.id, reason)
                    all_valid = False
                else:
                    valid_groups += 1

        sellable_goods = 0
        goods_table = self._table("SpiritShopGoods")
        if goods_table is None:
            logger.error("[SpiritShop] SpiritShopGoods table is missing")
            all_valid = False
        else:
            for row in goods_table.rows():
                if int(row.enabled) == 0:
                    continue
                goods, reason = self._resolve_goods_row(row)
                if goods is None:
                    logger.error("[SpiritShop] Invalid enabled SpiritShopGoods row: id=%d reason=%s", row.id, reason)
                    all_valid = False
                else:
                    sellable_goods += 1

        logger.info(
            "[SpiritShop] Table check finished: valid_groups=%d sellable_goods=%d ok=%s",
            valid_groups,
            sellable_goods,
            "true" if all_valid else "false",
        )
        return all_valid

    def _fill_info(self, data, now_val: int, out) -> None:
        out.Clear()
        out.server_time_ms = int(now_val)
        week = resolve_week(now_val)
        week_ok = week is not None
        week_start_ms, week_next_ms = week if week_ok else (0, 0)

        comp = data.spirit_shop
        out.shop_revision = int(comp.shop_revision)
        out.diamond_balance = balance(data, CURRENCY_DIAMOND)
        out.bind_diamond_balance = balance(data, CURRENCY_BIND_DIAMOND)

        group_table = self._table("SpiritShopLimitGroup")
        groups: list[ResolvedLimitGroup] = []
        if group_table is not None:
            for row in group_table.rows():
                group, _ = self._resolve_limit_group(int(row.id))
                if group is not None:
                    groups.append(group)
        groups.sort(key=lambda g: g.group_id)
        for group in groups:
            used = (
                used_group_weight(comp, group.group_id, group.reset_kind, week_start_ms)
                if (group.reset_kind == RESET_LIFETIME or week_ok)
                else 0
            )
            out.limit_groups.add(
                group_id=group.group_id,
                name=group.name,
                reset_kind=group.reset_kind,
                max_weight=group.max_weight,
                used_weight=used,
                next_reset_ms=week_next_ms if (group.reset_kind == RESET_WEEKLY and week_ok) else 0,
            )

        player_level = effective_level(data) if data.HasField("level_component") else 1
        goods_table = self._table("SpiritShopGoods")
        goods_list: list[ResolvedGoods] = []
        if goods_table is not None:
            for row in goods_table.rows():
                if int(row.enabled) == 0:
                    continue
                resolved, _ = self._resolve_goods_row(row)
                if resolved is None:
                    continue
                if not is_in_sale_window(resolved.start_time_ms, resolved.end_time_ms, now_val):
                    continue
                if not is_level_eligible(player_level, resolved.min_level, resolved.max_level):
                    continue
                goods_list.append(resolved)
        goods_list.sort(key=lambda g: (g.tab_id, g.sort_order, g.goods_id))
        for goods in goods_list:
            bought = (
                bought_count(comp, goods.goods_id, goods.limit_reset_kind, week_start_ms)
                if (goods.limit_reset_kind == RESET_LIFETIME or week_ok)
                else 0
            )
            view = out.goods.add(
                goods_id=goods.goods_id,
                tab_id=goods.tab_id,
                sort_order=goods.sort_order,
                name=goods.name,
                description=goods.description,
                icon_key=goods.icon_key,
                badge_text=goods.badge_text,
                currency_type=goods.currency_type,
                unit_price=goods.unit_price,
                max_buy_per_request=goods.max_buy_per_request,
                limit_reset_kind=goods.limit_reset_kind,
                limit_count=goods.limit_count,
                bought_count=bought,
                limit_group_id=goods.limit_group_id,
                limit_group_weight=goods.limit_group_weight,
                next_reset_ms=(
                    week_next_ms
                    if (goods.limit_count > 0 and goods.limit_reset_kind == RESET_WEEKLY and week_ok)
                    else 0
                ),
                start_time_ms=goods.start_time_ms,
                end_time_ms=goods.end_time_ms,
                grant_mode=goods.grant_mode,
                original_price=goods.original_price,
                min_level=goods.min_level,
                max_level=goods.max_level,
            )
            for item in goods.items:
                view.items.add(
                    item_config_id=item.item_config_id,
                    item_count=item.item_count,
                    bound=item.bound,
                )

    def _reject(self, route, tip: int):
        response = self.protocol.message(route.response_type)
        response.error_message.id = tip
        return response

    def _fill_success_reply(self, player, data, now_val: int, response) -> None:
        self._fill_info(data, now_val, response.info)
        bag_resp = _bag(self.scene, self.tables, player, 0)
        if not bag_resp.error_message.id and bag_resp.HasField("bag"):
            response.bag.CopyFrom(bag_resp.bag)
            self.assets.enrich_bag_enhance_preview(data, 0, response.bag)
            equip_craft = getattr(self.scene, "equip_craft", None)
            if equip_craft is not None:
                equip_craft._enrich_bag_snapshot(data, 0, response.bag)
        else:
            response.ClearField("bag")
            logger.error(
                "[SpiritShop] Purchase succeeded but bag snapshot failed; replying without bag: player_id=%d err=%d",
                player.player_id,
                bag_resp.error_message.id,
            )

    async def _grant_items_exact(
        self,
        data,
        items_to_grant: list[tuple[int, int, bool]],
        *,
        gains: list[AssetGain],
    ) -> int:
        """在候选背包中原子规划并按 ``(config_id, bound)`` 严格分堆发放全部商品条目，任一失败零副作用。"""
        items_table = self._table("Item")
        if items_table is None:
            return SPIRIT_SHOP_UNAVAILABLE

        rows: list[tuple[object, int, bool]] = []
        for config_id, total_qty, bound in items_to_grant:
            if not 0 < total_qty <= _MAX_UINT32:
                return SPIRIT_SHOP_INVALID_COUNT
            if self.assets.item_gain_blocked(data, config_id):
                return SPIRIT_SHOP_ITEM_BLOCKED
            row = items_table.get(config_id)
            if row is None or int(row.max_stack_size) == 0:
                return SPIRIT_SHOP_UNAVAILABLE
            rows.append((row, total_qty, bound))

        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        cap = bag_capacity(bag, 0)
        if not self.assets._usable(entries, cap):
            return SPIRIT_SHOP_UNAVAILABLE

        occupied = {entry.pos for entry, _ in entries}
        free_slots = (slot for slot in range(cap) if slot not in occupied)
        plan: list[tuple[int, int, int, bool]] = []

        for row, total_qty, bound in rows:
            max_stack = int(row.max_stack_size)
            remaining = total_qty
            # 仅合并到同 config_id 且 bound 一致的既有堆，绝不将绑定道具混入可交易堆。
            for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
                if (
                    item.config_id == row.id
                    and bool(item.bound) == bound
                    and self.assets._mergeable(item, max_stack)
                ):
                    added = min(remaining, max_stack - item.size)
                    if added:
                        item.size += added
                        remaining -= added
                        self.assets.write_entry(entry, item)
                        if remaining == 0:
                            break
            while remaining > 0:
                slot = next(free_slots, None)
                if slot is None:
                    return SPIRIT_SHOP_BAG_FULL
                if len(plan) >= MAX_NEW_STACKS:
                    return SPIRIT_SHOP_UNAVAILABLE
                stack = min(remaining, max_stack)
                plan.append((slot, int(row.id), stack, bound))
                remaining -= stack

        if plan and self.assets.allocate_item_id is None:
            return SPIRIT_SHOP_UNAVAILABLE

        ids = self.assets.all_ids(bag)
        watermark = max((int(item.acquire_seq) for _, item in entries), default=0)
        for slot, config_id, stack, bound in plan:
            try:
                guid = await self.assets.allocate_item_id()
            except (OSError, RuntimeError, ValueError):
                return SPIRIT_SHOP_UNAVAILABLE
            if type(guid) is not int or not 1 <= guid <= UINT64_MAX or guid in ids:
                return SPIRIT_SHOP_UNAVAILABLE
            if watermark >= UINT64_MAX:
                return SPIRIT_SHOP_UNAVAILABLE
            ids.add(guid)
            watermark += 1
            item = self.protocol.message(
                "ItemComp",
                item_id=guid,
                config_id=config_id,
                size=stack,
                bound=bound,
                acquire_seq=watermark,
            )
            self.assets._initialize_equipment(item)
            self.assets.write_entry(bag.items.add(pos=slot, bag_type=0), item)

        self.assets._order(bag.items)
        data.bag_component.CopyFrom(bag)
        gains.extend(AssetGain("item", int(row.id), total_qty) for row, total_qty, _ in rows)
        return 0

    def get_info(self, _world, player, route, _request):
        """509 查询当前灵石百宝阁货架、双余额与个人/共享组限购状态（纯只读）。"""
        data = self.scene.rows.assemble(player.snapshot)
        response = self.protocol.message(route.response_type)
        self._fill_info(data, int(self.now_ms()), response.info)
        return response

    async def buy_goods(self, _world, player, route, request):
        """510 购买百宝阁商品：幂等查重 -> 闸口与报价防呆 -> 限购与余额预检 -> 原子发物扣款存盘。"""
        now_val = int(self.now_ms())
        if carries_unknown(request):
            return self._reject(route, INVALID)

        # 1. 幂等查重排在一切状态预检之前：强制非 0 client_request_id。
        client_request_id = int(request.client_request_id)
        if client_request_id == 0:
            return self._reject(route, INVALID)

        data = self.scene.rows.assemble(player.snapshot)
        error, fingerprint, prior = client_request.check(
            data.client_request_ledger, request, CLIENT_REQUEST_KIND_BUY_SPIRIT_SHOP,
        )
        if error:
            return self._reject(route, error)
        if prior is not None:
            response = self.protocol.message(route.response_type, replayed=True)
            self._fill_success_reply(player, data, now_val, response)
            return response

        # 2. 可写闸与战斗闸。
        if not self.scene.mutable(player):
            return self._reject(route, SPIRIT_SHOP_FROZEN)
        if self.assets._player_in_battle(player):
            return self._reject(route, SPIRIT_SHOP_IN_BATTLE)

        # 3. 查表、上架态、份数与报价防呆。
        goods_id = int(request.goods_id)
        if goods_id == 0:
            return self._reject(route, SPIRIT_SHOP_GOODS_OFF_SHELF)
        goods_table = self._table("SpiritShopGoods")
        goods_row = None if goods_table is None else goods_table.get(goods_id)
        if goods_row is None or int(goods_row.enabled) == 0:
            return self._reject(route, SPIRIT_SHOP_GOODS_OFF_SHELF)
        goods, reason = self._resolve_goods_row(goods_row)
        if goods is None:
            logger.error(
                "[SpiritShop] Purchase rejected: goods row invalid player_id=%d goods_id=%d reason=%s",
                player.player_id,
                goods_id,
                reason,
            )
            return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)

        if not is_in_sale_window(goods.start_time_ms, goods.end_time_ms, now_val):
            return self._reject(route, SPIRIT_SHOP_NOT_IN_SALE_WINDOW)
        if goods.min_level > 0 or goods.max_level > 0:
            if not data.HasField("level_component"):
                return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)
            if not is_level_eligible(effective_level(data), goods.min_level, goods.max_level):
                return self._reject(route, SPIRIT_SHOP_LEVEL_NOT_MET)

        count = int(request.count)
        if count == 0 or count > goods.max_buy_per_request:
            return self._reject(route, SPIRIT_SHOP_INVALID_COUNT)
        chosen_idx = int(getattr(request, "chosen_item_index", 0))
        selected_items = resolve_selected_items(goods, chosen_idx)
        if selected_items is None:
            return self._reject(route, SPIRIT_SHOP_INVALID_CHOICE)
        if (
            int(request.expected_currency_type) != goods.currency_type
            or int(request.expected_unit_price) != goods.unit_price
            or not expected_items_match(request, goods.items)
        ):
            return self._reject(route, SPIRIT_SHOP_PRICE_CHANGED)

        # 4. 周起点、持久限购组件结构与单品/共享组限购预检。
        week = resolve_week(now_val)
        if week is None:
            logger.error(
                "[SpiritShop] Purchase rejected: invalid wall clock now_ms=%d player_id=%d",
                now_val,
                player.player_id,
            )
            return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)
        week_start_ms, _ = week

        comp_err = validate_comp(data.spirit_shop)
        if comp_err is not None:
            logger.error(
                "[SpiritShop] Purchase rejected: PlayerSpiritShopComp invalid (%s) player_id=%d",
                comp_err,
                player.player_id,
            )
            return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)

        if goods.limit_count > 0:
            bought = bought_count(data.spirit_shop, goods.goods_id, goods.limit_reset_kind, week_start_ms)
            if would_exceed_limit(bought, count, goods.limit_count):
                return self._reject(route, SPIRIT_SHOP_LIMIT_REACHED)

        add_group_weight = 0
        if goods.limit_group_id > 0:
            group, reason = self._resolve_limit_group(goods.limit_group_id)
            if group is None:
                logger.error(
                    "[SpiritShop] Purchase rejected: limit group invalid player_id=%d group_id=%d reason=%s",
                    player.player_id,
                    goods.limit_group_id,
                    reason,
                )
                return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)
            weight_val = total_group_weight(goods.limit_group_weight, count)
            if weight_val is None:
                return self._reject(route, SPIRIT_SHOP_INVALID_COUNT)
            add_group_weight = weight_val
            used = used_group_weight(data.spirit_shop, group.group_id, group.reset_kind, week_start_ms)
            if would_exceed_limit(used, add_group_weight, group.max_weight):
                return self._reject(route, SPIRIT_SHOP_GROUP_LIMIT_REACHED)

        # 5. 总价、物品封禁与余额预检（严禁绑定灵石混扣）。
        cost = total_cost(goods.unit_price, count)
        if cost is None:
            return self._reject(route, SPIRIT_SHOP_INVALID_COUNT)
        for item in selected_items:
            if self.assets.item_gain_blocked(data, item.item_config_id):
                return self._reject(route, SPIRIT_SHOP_ITEM_BLOCKED)
        if balance(data, goods.currency_type) < cost:
            return self._reject(route, SPIRIT_SHOP_CURRENCY_NOT_ENOUGH)

        items_to_grant: list[tuple[int, int, bool]] = []
        for item in selected_items:
            qty = item.item_count * count
            if not 0 < qty <= _MAX_UINT32:
                return self._reject(route, SPIRIT_SHOP_INVALID_COUNT)
            # 策划口径永久仅自用道具无论表内 item_bound 配置如何均强制 bound=True。
            is_bound = item.bound or self.assets.is_always_bound_item(item.item_config_id)
            items_to_grant.append((item.item_config_id, qty, is_bound))

        # 6. 候选副本原子装配：先完成全部分堆空间规划与发号，再复核闸口并扣款、记限购与幂等。
        staged = clone(data)
        gains: list[AssetGain] = []
        grant_err = await self._grant_items_exact(staged, items_to_grant, gains=gains)
        if grant_err:
            if grant_err == BLOCKED:
                return self._reject(route, SPIRIT_SHOP_ITEM_BLOCKED)
            if grant_err == BAG_FULL:
                return self._reject(route, SPIRIT_SHOP_BAG_FULL)
            return self._reject(route, grant_err)

        # 发号 await 后复核可写闸与战斗闸，防止挂起期间切态。
        if not self.scene.mutable(player):
            return self._reject(route, SPIRIT_SHOP_FROZEN)
        if self.assets._player_in_battle(player):
            return self._reject(route, SPIRIT_SHOP_IN_BATTLE)

        deduct_err = self.assets.deduct_currency(staged, goods.currency_type, cost)
        if deduct_err == INSUFFICIENT:
            return self._reject(route, SPIRIT_SHOP_CURRENCY_NOT_ENOUGH)
        if deduct_err:
            logger.error(
                "[SpiritShop] Purchase rejected: currency deduction failed player_id=%d goods_id=%d cost=%d currency_type=%d err=%d",
                player.player_id,
                goods.goods_id,
                cost,
                goods.currency_type,
                deduct_err,
            )
            return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)

        if (
            not record_purchase(
                staged.spirit_shop,
                goods.goods_id,
                count,
                goods.limit_count,
                goods.limit_group_id,
                add_group_weight,
                week_start_ms,
            )
            or validate_comp(staged.spirit_shop) is not None
        ):
            return self._reject(route, SPIRIT_SHOP_UNAVAILABLE)

        client_request.remember(
            staged.client_request_ledger,
            client_request_id,
            CLIENT_REQUEST_KIND_BUY_SPIRIT_SHOP,
            fingerprint,
        )

        audit_extra = {
            "source": "spirit_shop",
            "goods_id": goods.goods_id,
            "count": count,
            "unit_price": goods.unit_price,
            "currency_type": goods.currency_type,
        }
        if goods.grant_mode != GRANT_MODE_ALL or chosen_idx != 0:
            audit_extra["grant_mode"] = goods.grant_mode
            audit_extra["chosen_item_index"] = chosen_idx

        # 7. WALLET（扣灵石 + 限购 + 幂等）与 BAG（绑定物品）同事务落盘并写 TX_SPIRIT_SHOP_BUY=44 流水。
        saved = await self.assets._save(
            player,
            staged,
            self.buy_tx,
            correlation=client_request_id,
            gains=gains,
            item_extra=audit_extra,
        )
        if not saved:
            return self._reject(route, UNAVAILABLE)

        logger.info(
            "[SpiritShop] metric=spirit_shop_bought player_id=%d goods_id=%d count=%d cost=%d currency_type=%d group_id=%d group_weight_added=%d client_request_id=%d",
            player.player_id,
            goods.goods_id,
            count,
            cost,
            goods.currency_type,
            goods.limit_group_id,
            add_group_weight,
            client_request_id,
        )
        response = self.protocol.message(route.response_type)
        self._fill_success_reply(player, staged, now_val, response)
        return response
