"""率土式分卷宗永久技能制书、手艺熟练度与主城宗师原子代打摊位系统。"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    BAG_FULL,
    BAG_INSUFFICIENT,
    BLOCKED,
    FEATURE_UNAVAILABLE,
    INVALID,
    UNAVAILABLE,
    bag_capacity,
    clone,
    held_item_count,
)
from .scene_mission import now_ms
from .scene_stats import try_auto_slot_newly_learned

logger = logging.getLogger(__name__)

_SERVICE = "SceneSkillClientPlayer"
_MAX_UINT64 = (1 << 64) - 1
_MAX_UINT32 = (1 << 32) - 1

# 客户端幂等请求种类（与 C++ ClientRequestKind 严格对齐）。
_CLIENT_REQUEST_KIND_SKILL_SCROLL_CRAFT = 14
_CLIENT_REQUEST_KIND_LEARN_SKILL_BOOK = 15
_CLIENT_REQUEST_KIND_DISMANTLE_SKILL_BOOK = 16

# 物品配置 ID（与 C++ equip_craft_quality_rules.h 对齐）。
SCROLL_FRAGMENT_ITEM_ID = 3406
RARE_SCROLL_PAGE_ITEM_ID = 3407
RARE_SCROLL_INK_ITEM_ID = 3408

# 手艺熟练度档位常量。
MASTERY_APPRENTICE = 1
MASTERY_ARTISAN = 2
MASTERY_MASTER = 3
ARTISAN_EXP_THRESHOLD = 20
MASTER_EXP_THRESHOLD = 60

# 代打摊位手续费与税率。
PROXY_FEE_MIN_GOLD = 50
PROXY_FEE_MAX_GOLD = 100
PROXY_TAX_BPS = 500  # 5% 银两税销毁
SCROLL_BONUS_FRAGMENTS_COUNT = 2
MAX_RARE_SKILL_RATIO_BPS = 1000  # 稀世绝学硬上限 <= 10%

# 制书模式（与 CraftSkillBookRequest.CraftMode 一致）。
CRAFT_MODE_RANDOM_REGULAR = 1
CRAFT_MODE_TARGETED_REGULAR = 2
CRAFT_MODE_TARGETED_RARE = 3

# 错误码（与 C++ skill_error_tip.pb.h / equip_error_tip.pb.h 严格对齐）。
SKILL_NOT_IN_CLASS = 7007
SKILL_GOLD_NOT_ENOUGH = 7012
SKILL_UPGRADE_BUSY = 7013
SKILL_SCROLL_INVALID = 7014
SKILL_BOOK_NOT_FOUND = 7015
SKILL_BOOK_NOT_DISMANTLABLE = 7016
SKILL_CRAFT_MATERIAL_NOT_ENOUGH = 7017
SKILL_CRAFT_REVISION_CHANGED = 7018
SKILL_CRAFT_BOOK_REQUIRED = 7019
SKILL_ALREADY_BOOK_UNLOCKED = 7020
SKILL_PROXY_STALL_REQUIRES_MASTER = 7021
SKILL_PROXY_FEE_INVALID = 7022
EQUIP_PROXY_MASTER_UNAVAILABLE = 28030
INVALID_TABLE_ID = 1002
CURRENCY_GOLD = 0  # currency.values 下标 0 为银两（与 SceneAssets 统一）

# 权威卷宗技能清单兜底（当 Skill 表尚未包含 scroll_id 字段时启用，与 C++ skill.json 严格一致）。
# 元素格式：(skill_table_id, caster_mask)，caster_mask: 1=人物, 2=宝宝, 3=通用。
_DEFAULT_SCROLL_SKILLS: dict[int, tuple[tuple[int, int], ...]] = {
    1: (
        (10002, 3), (10003, 1), (10006, 3), (10008, 1), (10010, 3), (10012, 1),
        (10014, 3), (10015, 1), (10016, 3), (10018, 2), (10019, 3), (10020, 1),
        (10021, 1), (10022, 1), (10023, 1), (10024, 1), (10025, 3), (10027, 3),
        (10029, 1), (10030, 1), (10031, 1), (10033, 3), (10034, 3), (10035, 1),
        (10036, 1), (10038, 1), (10039, 1), (10040, 1), (11005, 2), (11006, 2),
        (11007, 2), (11008, 2), (11011, 2), (11012, 2), (11013, 2), (11014, 2),
        (11015, 2), (12001, 1), (12002, 1), (12003, 1), (12004, 3), (12005, 1),
        (12006, 3), (12007, 1), (12008, 1), (12014, 2), (12015, 2), (12016, 2),
        (12017, 2), (12018, 2), (13001, 2), (13005, 2), (13006, 2), (14001, 1),
        (14002, 1), (14003, 2),
    ),
    2: (
        (10026, 3), (10041, 1), (11009, 2), (11016, 2), (12009, 1), (12019, 1),
    ),
    3: (
        (12020, 1), (12021, 1), (12022, 1),
    ),
}


def mastery_tier_for_exp(exp: int) -> int:
    if exp >= MASTER_EXP_THRESHOLD:
        return MASTERY_MASTER
    if exp >= ARTISAN_EXP_THRESHOLD:
        return MASTERY_ARTISAN
    return MASTERY_APPRENTICE


def mastery_next_exp(exp: int) -> int:
    if exp >= MASTER_EXP_THRESHOLD:
        return 0
    if exp >= ARTISAN_EXP_THRESHOLD:
        return MASTER_EXP_THRESHOLD
    return ARTISAN_EXP_THRESHOLD


def mastery_tier_name(tier: int) -> str:
    if tier == MASTERY_MASTER:
        return "宗师"
    if tier == MASTERY_ARTISAN:
        return "巧匠"
    return "学徒"


def forge_top_quality_rate_bps(tier: int) -> int:
    if tier == MASTERY_MASTER:
        return 1000
    if tier == MASTERY_ARTISAN:
        return 700
    return 500


def scroll_bonus_fragment_rate_bps(tier: int) -> int:
    if tier == MASTERY_MASTER:
        return 1000
    if tier == MASTERY_ARTISAN:
        return 500
    return 0


def valid_proxy_fee(gross_fee: int) -> bool:
    return PROXY_FEE_MIN_GOLD <= gross_fee <= PROXY_FEE_MAX_GOLD


def proxy_tax_gold(gross_fee: int) -> int:
    return 0 if gross_fee == 0 else (gross_fee * PROXY_TAX_BPS + 9999) // 10000


def proxy_net_gold(gross_fee: int) -> int:
    tax = proxy_tax_gold(gross_fee)
    return gross_fee - tax if gross_fee > tax else 0


class SceneSkillCraft:
    """Scene 分卷宗永久技能制书、研习、拆页与主城宗师代打摊位模块。"""

    def __init__(
        self,
        scene,
        tables,
        assets=None,
        *,
        clock=now_ms,
        roll_fn: Callable[[int], int] | None = None,
        craft_unlock_gate: bool = False,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._roll_fn = roll_fn
        self._craft_unlock_gate = bool(craft_unlock_gate)
        self._skill_has_scroll = (
            "Skill" in tables.tables
            and len(tables["Skill"]) > 0
            and hasattr(tables["Skill"].rows()[0], "scroll_id")
        )

    def register(self) -> None:
        """向 SceneNode 注册 6 条技能制书与代打摊位客户端 RPC。"""
        self.scene.register(_SERVICE, "GetSkillCraftingState", self.get_state)
        self.scene.register(_SERVICE, "CraftSkillBook", self.craft_skill_book, mutating=True)
        self.scene.register(_SERVICE, "LearnSkillBook", self.learn_skill_book, mutating=True)
        self.scene.register(_SERVICE, "DismantleSkillBook", self.dismantle_skill_book, mutating=True)
        self.scene.register(_SERVICE, "SetCraftProxyStall", self.set_proxy_stall, mutating=True)
        self.scene.register(_SERVICE, "ListCraftProxyStalls", self.list_proxy_stalls)

    # ── 配置与校验辅助 ──

    def _roll_below(self, bound: int) -> int:
        if bound <= 1:
            return 0
        if self._roll_fn is not None:
            return int(self._roll_fn(bound)) % bound
        return random.randrange(bound)

    def _scrolls(self) -> list:
        if "SkillScrollCraft" not in self.tables.tables:
            return []
        rows = [row for row in self.tables["SkillScrollCraft"].rows() if row.enabled]
        rows.sort(key=lambda r: r.id)
        return rows

    def _scroll_by_id(self, scroll_id: int):
        if "SkillScrollCraft" not in self.tables.tables:
            return None
        row = self.tables["SkillScrollCraft"].get(scroll_id)
        return row if row is not None and row.enabled else None

    def _collect_scroll_skills(self, scroll_id: int) -> list[int]:
        if self._skill_has_scroll:
            ids = [
                row.id for row in self.tables["Skill"].rows()
                if getattr(row, "scroll_id", 0) == scroll_id
                and getattr(row, "season_id", 0) == 0
                and getattr(row, "battle_model", 1) == 1
            ]
            ids.sort()
            return ids
        return sorted(sid for sid, _ in _DEFAULT_SCROLL_SKILLS.get(scroll_id, ()))

    def _skill_caster_mask(self, skill_id: int) -> int:
        if self._skill_has_scroll:
            row = self.tables["Skill"].get(skill_id)
            if row is None or getattr(row, "season_id", 0) != 0:
                return 0
            return int(getattr(row, "caster_mask", 1))
        for entries in _DEFAULT_SCROLL_SKILLS.values():
            for sid, mask in entries:
                if sid == skill_id:
                    return mask
        return 0

    def validate_tables(self) -> bool:
        """校验 SkillScrollCraft 配方与稀世绝学占比 <= 10% 硬红线。"""
        scrolls = self._scrolls()
        if not scrolls:
            return False
        total_skills = 0
        rare_skills = 0
        for row in scrolls:
            if row.id == 0 or row.rarity_tier > 1:
                return False
            pool = self._collect_scroll_skills(row.id)
            if not pool:
                return False
            if row.rarity_tier == 0:
                if (
                    not row.fragment_item_id
                    or not row.random_fragment_count
                    or row.targeted_fragment_count < row.random_fragment_count
                    or not row.dismantle_fragment_count
                    or row.dismantle_fragment_count > row.random_fragment_count
                ):
                    return False
            else:
                if (
                    not row.rare_scroll_item_id
                    or not row.rare_scroll_count
                    or not row.rare_ink_item_id
                    or not row.rare_ink_count
                ):
                    return False
            total_skills += len(pool)
            if row.rarity_tier == 1:
                rare_skills += len(pool)
        if total_skills == 0:
            return False
        return rare_skills * 10_000 <= total_skills * MAX_RARE_SKILL_RATIO_BPS

    def _writable_error(self, player) -> int:
        if not self.scene.mutable(player):
            return SKILL_UPGRADE_BUSY
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return SKILL_UPGRADE_BUSY
        return 0

    def _find_online_player(self, player_id: int):
        for location in getattr(self.scene, "locations", {}).values():
            target = location.players.get(player_id)
            if target is not None:
                return target
        return None

    @staticmethod
    def _display_name(data, player_id: int) -> str:
        name = getattr(data.profile_component, "name", "") if data.HasField("profile_component") else ""
        return name or f"宗师#{player_id}"

    # ── 背包材料与货币辅助（兼容 Item 表未含 3406/3407/3408 时走同口径原子逻辑） ──

    @staticmethod
    def _gold_balance(data) -> int:
        return int(data.currency.values[CURRENCY_GOLD]) if len(data.currency.values) > CURRENCY_GOLD else 0

    @staticmethod
    def _deduct_gold(data, amount: int) -> bool:
        if amount == 0:
            return True
        if len(data.currency.values) <= CURRENCY_GOLD or data.currency.values[CURRENCY_GOLD] < amount:
            return False
        data.currency.values[CURRENCY_GOLD] -= amount
        return True

    @staticmethod
    def _add_gold(data, amount: int) -> bool:
        if amount == 0:
            return True
        while len(data.currency.values) <= CURRENCY_GOLD:
            data.currency.values.append(0)
        if data.currency.values[CURRENCY_GOLD] > _MAX_UINT64 - amount:
            return False
        data.currency.values[CURRENCY_GOLD] += amount
        return True

    def _owned_count(self, data, config_id: int) -> int:
        total = 0
        for entry in data.bag_component.items:
            if entry.bag_type != 0:
                continue
            item = self.assets.item(entry)
            if item.config_id == config_id:
                total += item.size
        return total

    def _consume_materials(self, data, counts: dict[int, int]) -> tuple[int, bool]:
        """原子扣减所需材料，返回 (err, consumed_any_bound)。"""
        if not counts:
            return 0, False
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return FEATURE_UNAVAILABLE, False
        consumed_bound = False
        for cid, need in sorted(counts.items()):
            if need <= 0:
                continue
            held = held_item_count(data, cid)
            known = sum(
                item.size for _, item in entries
                if item.config_id == cid and self.assets.countable(item)
            )
            if known < need or (held and need > max(0, known - held)):
                return SKILL_CRAFT_MATERIAL_NOT_ENOUGH, False
            # 优先扣非绑定，再扣绑定；任一扣到绑定即标 consumed_bound=True（防洗白）。
            ordered = sorted(
                ((e, it) for e, it in entries if it.config_id == cid and self.assets.countable(it)),
                key=lambda pair: (1 if pair[1].bound else 0, pair[1].acquire_seq, pair[1].item_id),
            )
            remaining = need
            for entry, item in ordered:
                if remaining <= 0:
                    break
                take = min(remaining, item.size)
                if take > 0:
                    if item.bound:
                        consumed_bound = True
                    item.size -= take
                    remaining -= take
                    self.assets.write_entry(entry, item)
            if remaining > 0:
                return SKILL_CRAFT_MATERIAL_NOT_ENOUGH, False
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0, consumed_bound

    async def _grant_stack(self, data, config_id: int, count: int, *, bound: bool) -> int:
        """向主背包发放可堆叠材料（保留 bound 绑定属性）。"""
        if count <= 0:
            return 0
        if self.assets.item_gain_blocked(data, config_id):
            return BLOCKED
        row = self.tables["Item"].get(config_id)
        max_stack = row.max_stack_size if row is not None and row.max_stack_size > 0 else 999
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        cap = bag_capacity(bag, 0)
        if not self.assets._usable(entries, cap):
            return UNAVAILABLE
        occupied = {entry.pos for entry, _ in entries}
        free_slots = (slot for slot in range(cap) if slot not in occupied)
        remaining = count
        for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
            if item.config_id == config_id and item.bound == bound and self.assets._mergeable(item, max_stack):
                added = min(remaining, max_stack - item.size)
                if added > 0:
                    item.size += added
                    remaining -= added
                    self.assets.write_entry(entry, item)
        plan = []
        while remaining > 0:
            slot = next(free_slots, None)
            if slot is None:
                return BAG_FULL
            stack = min(remaining, max_stack)
            plan.append((slot, stack))
            remaining -= stack
        if plan and self.assets.allocate_item_id is None:
            return UNAVAILABLE
        ids = self.assets.all_ids(bag)
        watermark = max((item.acquire_seq for _, item in entries), default=0)
        for slot, stack in plan:
            try:
                guid = await self.assets.allocate_item_id()
            except (OSError, RuntimeError, ValueError):
                return UNAVAILABLE
            if type(guid) is not int or not 1 <= guid <= _MAX_UINT64 or guid in ids:
                return UNAVAILABLE
            ids.add(guid)
            watermark += 1
            item = self.protocol.message(
                "ItemComp", item_id=guid, config_id=config_id, size=stack, bound=bound, acquire_seq=watermark,
            )
            self.assets.write_entry(bag.items.add(pos=slot, bag_type=0), item)
        self.assets._order(bag.items)
        data.bag_component.CopyFrom(bag)
        return 0

    # ── 快照组装 ──

    def _fill_mastery(self, craft, forge_out, scroll_out) -> None:
        f_exp = int(craft.forge_mastery_exp)
        f_tier = mastery_tier_for_exp(f_exp)
        forge_out.tier = f_tier
        forge_out.tier_name = mastery_tier_name(f_tier)
        forge_out.exp = f_exp
        forge_out.next_tier_exp = mastery_next_exp(f_exp)
        forge_out.bonus_rate_bps = forge_top_quality_rate_bps(f_tier)

        s_exp = int(craft.scroll_mastery_exp)
        s_tier = mastery_tier_for_exp(s_exp)
        scroll_out.tier = s_tier
        scroll_out.tier_name = mastery_tier_name(s_tier)
        scroll_out.exp = s_exp
        scroll_out.next_tier_exp = mastery_next_exp(s_exp)
        scroll_out.bonus_rate_bps = scroll_bonus_fragment_rate_bps(s_tier)

    def _fill_state(self, data, out) -> None:
        out.Clear()
        craft = data.craft_component
        out.scroll_craft_revision = craft.scroll_craft_revision
        self._fill_mastery(craft, out.forge_mastery, out.scroll_mastery)
        for row in self._scrolls():
            entry = out.scrolls.add(
                scroll_id=row.id,
                name=row.name,
                description=row.description,
                rarity_tier=row.rarity_tier,
                fragment_item_id=row.fragment_item_id,
                random_fragment_count=row.random_fragment_count,
                random_gold_cost=row.random_gold_cost,
                targeted_fragment_count=row.targeted_fragment_count,
                targeted_gold_cost=row.targeted_gold_cost,
                dismantle_fragment_count=row.dismantle_fragment_count,
                rare_scroll_item_id=row.rare_scroll_item_id,
                rare_scroll_count=row.rare_scroll_count,
                rare_ink_item_id=row.rare_ink_item_id,
                rare_ink_count=row.rare_ink_count,
                rare_gold_cost=row.rare_gold_cost,
            )
            entry.skill_table_ids.extend(self._collect_scroll_skills(row.id))
        for book in sorted(craft.skill_books, key=lambda b: b.book_id):
            out.skill_books.add().CopyFrom(book)
        out.book_unlocked_skill_ids.extend(sorted(set(craft.book_unlocked_skill_ids)))
        out.gold_balance = self._gold_balance(data)
        out.fragment_owned = self._owned_count(data, SCROLL_FRAGMENT_ITEM_ID)
        out.rare_scroll_owned = self._owned_count(data, RARE_SCROLL_PAGE_ITEM_ID)
        out.rare_ink_owned = self._owned_count(data, RARE_SCROLL_INK_ITEM_ID)
        out.proxy_stall_open = bool(craft.proxy_stall_open)
        out.proxy_forge_fee_gold = craft.proxy_forge_fee_gold
        out.proxy_scroll_fee_gold = craft.proxy_scroll_fee_gold
        out.craft_unlock_gate_enabled = self._craft_unlock_gate

    def _finish(self, response, data, error: int = 0):
        if error:
            response.error_message.id = error
        self._fill_state(data, response.state)
        return response

    # ── 6 条客户端 RPC ──

    def get_state(self, _world, player, _route, _request):
        """查询技能制书卷宗目录、未学技能书架、熟练度与代打摊位状态。"""
        response = self.protocol.message("GetSkillCraftingStateResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        return self._finish(response, data)

    async def craft_skill_book(self, _world, player, _route, request):
        """分卷宗随机/定向/稀世制书，支持主城宗师代制与 client_request_id 幂等。"""
        response = self.protocol.message(
            "CraftSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_SKILL_SCROLL_CRAFT)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.bonus_fragments_granted = prior.scroll_bonus_fragments
            found = next((b for b in data.craft_component.skill_books if b.book_id == prior.crafted_skill_book_id), None)
            if found is not None:
                response.crafted_book.CopyFrom(found)
            else:
                response.crafted_book.book_id = prior.crafted_skill_book_id
                response.crafted_book.skill_table_id = prior.crafted_skill_table_id
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if craft.scroll_craft_revision == _MAX_UINT64:
            return self._finish(response, data, UNAVAILABLE)
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        scroll = self._scroll_by_id(request.scroll_id)
        if scroll is None:
            return self._finish(response, data, SKILL_SCROLL_INVALID)
        pool = self._collect_scroll_skills(scroll.id)
        if not pool:
            return self._finish(response, data, SKILL_SCROLL_INVALID)

        consumed: dict[int, int] = {}
        mode = request.mode
        if mode == CRAFT_MODE_RANDOM_REGULAR:
            if scroll.rarity_tier != 0 or request.target_skill_table_id != 0:
                return self._finish(response, data, SKILL_SCROLL_INVALID)
            chosen_skill_id = pool[self._roll_below(len(pool))]
            consumed[scroll.fragment_item_id] = scroll.random_fragment_count
            base_gold = int(scroll.random_gold_cost)
        elif mode == CRAFT_MODE_TARGETED_REGULAR:
            if scroll.rarity_tier != 0 or request.target_skill_table_id not in pool:
                return self._finish(response, data, SKILL_SCROLL_INVALID)
            chosen_skill_id = int(request.target_skill_table_id)
            consumed[scroll.fragment_item_id] = scroll.targeted_fragment_count
            base_gold = int(scroll.targeted_gold_cost)
        elif mode == CRAFT_MODE_TARGETED_RARE:
            if scroll.rarity_tier != 1 or request.target_skill_table_id not in pool:
                return self._finish(response, data, SKILL_SCROLL_INVALID)
            chosen_skill_id = int(request.target_skill_table_id)
            consumed[scroll.rare_scroll_item_id] = scroll.rare_scroll_count
            consumed[scroll.rare_ink_item_id] = scroll.rare_ink_count
            base_gold = int(scroll.rare_gold_cost)
        else:
            return self._finish(response, data, INVALID)

        proxy_master_id = int(request.proxy_master_player_id)
        proxy_master = None
        proxy_master_data = None
        proxy_fee_gold = 0
        proxy_master_name = ""
        effective_tier = mastery_tier_for_exp(int(craft.scroll_mastery_exp))
        if proxy_master_id != 0:
            if proxy_master_id == player.player_id:
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)
            proxy_master = self._find_online_player(proxy_master_id)
            if proxy_master is None or not self.scene.mutable(proxy_master):
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)
            proxy_master_data = self.scene.rows.assemble(proxy_master.snapshot)
            mc = proxy_master_data.craft_component
            if (
                not mc.proxy_stall_open
                or mastery_tier_for_exp(int(mc.scroll_mastery_exp)) < MASTERY_MASTER
                or not valid_proxy_fee(int(mc.proxy_scroll_fee_gold))
            ):
                return self._finish(response, data, EQUIP_PROXY_MASTER_UNAVAILABLE)
            proxy_fee_gold = int(mc.proxy_scroll_fee_gold)
            proxy_master_name = self._display_name(proxy_master_data, proxy_master_id)
            effective_tier = MASTERY_MASTER

        total_gold = base_gold + proxy_fee_gold
        if total_gold > 0 and self._gold_balance(data) < total_gold:
            return self._finish(response, data, SKILL_GOLD_NOT_ENOUGH)

        staged = clone(data)
        consume_err, consumed_bound = self._consume_materials(staged, consumed)
        if consume_err:
            return self._finish(response, data, consume_err)

        if not self._deduct_gold(staged, total_gold):
            return self._finish(response, data, SKILL_GOLD_NOT_ENOUGH)

        bonus_fragments = 0
        if scroll.rarity_tier == 0:
            bonus_rate = scroll_bonus_fragment_rate_bps(effective_tier)
            if bonus_rate > 0 and self._roll_below(10_000) < bonus_rate:
                grant_err = await self._grant_stack(
                    staged, scroll.fragment_item_id, SCROLL_BONUS_FRAGMENTS_COUNT, bound=True,
                )
                if grant_err == 0:
                    bonus_fragments = SCROLL_BONUS_FRAGMENTS_COUNT

        sc = staged.craft_component
        book_id = sc.next_book_seq or 1
        new_book = sc.skill_books.add(
            book_id=book_id,
            skill_table_id=chosen_skill_id,
            scroll_id=scroll.id,
            rarity_tier=scroll.rarity_tier,
            bound=True if mode == CRAFT_MODE_TARGETED_RARE else consumed_bound,
        )
        if effective_tier == MASTERY_MASTER:
            signer_id = proxy_master_id if proxy_master_id else player.player_id
            new_book.crafter_player_id = signer_id
            new_book.crafter_name = (
                proxy_master_name if proxy_master_id else self._display_name(staged, signer_id)
            )
        sc.next_book_seq = book_id + 1
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev
        if proxy_master_id == 0 and sc.scroll_mastery_exp < _MAX_UINT32:
            sc.scroll_mastery_exp += 1

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_SKILL_SCROLL_CRAFT,
            fingerprint,
            crafted_skill_book_id=book_id,
            crafted_skill_table_id=chosen_skill_id,
            scroll_craft_revision=next_rev,
            scroll_bonus_fragments=bonus_fragments,
        )

        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        if proxy_master is not None and proxy_master_data is not None:
            net_fee = proxy_net_gold(proxy_fee_gold)
            if net_fee > 0:
                master_staged = clone(proxy_master_data)
                if self._add_gold(master_staged, net_fee):
                    try:
                        await self.scene.commit_player_data(proxy_master, master_staged)
                    except (StoreRejected, StoreUnavailable, OSError):
                        logger.warning(
                            "Proxy master fee credit failed master_id=%s fee=%s", proxy_master_id, net_fee,
                        )

        logger.info(
            "Skill book crafted player_id=%s book_id=%s skill_id=%s scroll_id=%s bound=%s bonus=%s",
            player.player_id, book_id, chosen_skill_id, scroll.id, new_book.bound, bonus_fragments,
        )
        response.crafted_book.CopyFrom(new_book)
        response.bonus_fragments_granted = bonus_fragments
        return self._finish(response, staged)

    async def learn_skill_book(self, _world, player, _route, request):
        """研习未学技能书：永久解锁对应技能（绝不顶掉已有技能），支持人物与宝宝。"""
        response = self.protocol.message(
            "LearnSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.book_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_LEARN_SKILL_BOOK)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.learned_skill_table_id = prior.crafted_skill_table_id
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        book_idx = next((i for i, b in enumerate(craft.skill_books) if b.book_id == request.book_id), -1)
        if book_idx < 0:
            return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
        skill_id = int(craft.skill_books[book_idx].skill_table_id)
        caster_mask = self._skill_caster_mask(skill_id)
        if caster_mask == 0:
            return self._finish(response, data, INVALID_TABLE_ID)
        required_mask = 1 if request.pet_id == 0 else 2
        if (caster_mask & required_mask) == 0:
            return self._finish(response, data, SKILL_NOT_IN_CLASS)
        if skill_id in craft.book_unlocked_skill_ids:
            return self._finish(response, data, SKILL_ALREADY_BOOK_UNLOCKED)

        staged = clone(data)
        if request.pet_id != 0:
            pet = next((p for p in staged.pet_component.pets if p.pet_id == request.pet_id), None)
            if pet is None:
                return self._finish(response, data, INVALID)
            if skill_id not in pet.skill_table_ids:
                pet.skill_table_ids.append(skill_id)
            if pet.skill_ranks.get(skill_id, 0) == 0:
                pet.skill_ranks[skill_id] = 1
        else:
            existing = next(
                (s for s in staged.skill_list.skill_list if s.skill_table_id == skill_id), None,
            )
            newly_learned = existing is None or existing.rank == 0
            if existing is None:
                staged.skill_list.skill_list.add(skill_table_id=skill_id, rank=1)
            elif existing.rank == 0:
                existing.rank = 1
            if newly_learned:
                skill_row = self.tables["Skill"].get(skill_id)
                if skill_row is not None:
                    try_auto_slot_newly_learned(self.tables, staged.skill_list, skill_row)

        sc = staged.craft_component
        del sc.skill_books[book_idx]
        sc.book_unlocked_skill_ids.append(skill_id)
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_LEARN_SKILL_BOOK,
            fingerprint,
            crafted_skill_book_id=request.book_id,
            crafted_skill_table_id=skill_id,
            scroll_craft_revision=next_rev,
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill book learned player_id=%s book_id=%s skill_id=%s pet_id=%s",
            player.player_id, request.book_id, skill_id, request.pet_id,
        )
        response.learned_skill_table_id = skill_id
        return self._finish(response, staged)

    async def dismantle_skill_book(self, _world, player, _route, request):
        """拆解多余未学常规技能书，固定返还 80%（16 张）同绑定属性悟道残页。"""
        response = self.protocol.message(
            "DismantleSkillBookResponse", client_request_id=request.client_request_id,
        )
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.book_id:
            return self._finish(response, data, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_DISMANTLE_SKILL_BOOK)
        if err:
            return self._finish(response, data, err)
        if prior is not None:
            response.replayed = True
            response.refunded_fragments = prior.scroll_bonus_fragments
            return self._finish(response, data)

        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        craft = data.craft_component
        if request.expected_revision != craft.scroll_craft_revision:
            return self._finish(response, data, SKILL_CRAFT_REVISION_CHANGED)

        book_idx = next((i for i, b in enumerate(craft.skill_books) if b.book_id == request.book_id), -1)
        if book_idx < 0:
            return self._finish(response, data, SKILL_BOOK_NOT_FOUND)
        book = craft.skill_books[book_idx]
        if book.rarity_tier != 0:
            return self._finish(response, data, SKILL_BOOK_NOT_DISMANTLABLE)
        scroll = self._scroll_by_id(book.scroll_id)
        if scroll is None or scroll.dismantle_fragment_count == 0:
            return self._finish(response, data, SKILL_BOOK_NOT_DISMANTLABLE)

        staged = clone(data)
        grant_err = await self._grant_stack(
            staged, scroll.fragment_item_id, int(scroll.dismantle_fragment_count), bound=bool(book.bound),
        )
        if grant_err:
            return self._finish(response, data, grant_err)

        sc = staged.craft_component
        del sc.skill_books[book_idx]
        next_rev = sc.scroll_craft_revision + 1
        sc.scroll_craft_revision = next_rev

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_DISMANTLE_SKILL_BOOK,
            fingerprint,
            crafted_skill_book_id=book.book_id,
            crafted_skill_table_id=book.skill_table_id,
            scroll_craft_revision=next_rev,
            scroll_bonus_fragments=int(scroll.dismantle_fragment_count),
        )
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)

        logger.info(
            "Skill book dismantled player_id=%s book_id=%s refunded=%s",
            player.player_id, book.book_id, scroll.dismantle_fragment_count,
        )
        response.refunded_fragments = int(scroll.dismantle_fragment_count)
        return self._finish(response, staged)

    async def set_proxy_stall(self, _world, player, _route, request):
        """宗师开启或关闭主城原子代打/代制摊位并设置固定银两手工费（50..100）。"""
        response = self.protocol.message("SetCraftProxyStallResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        if busy := self._writable_error(player):
            return self._finish(response, data, busy)

        staged = clone(data)
        sc = staged.craft_component
        if not request.open:
            sc.proxy_stall_open = False
            try:
                await self.scene.commit_player_data(player, staged)
            except (StoreRejected, StoreUnavailable, OSError):
                return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)
            return self._finish(response, staged)

        forge_master = mastery_tier_for_exp(int(sc.forge_mastery_exp)) >= MASTERY_MASTER
        scroll_master = mastery_tier_for_exp(int(sc.scroll_mastery_exp)) >= MASTERY_MASTER
        if not forge_master and not scroll_master:
            return self._finish(response, data, SKILL_PROXY_STALL_REQUIRES_MASTER)

        forge_fee = int(request.proxy_forge_fee_gold)
        scroll_fee = int(request.proxy_scroll_fee_gold)
        if forge_master and not valid_proxy_fee(forge_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)
        if not forge_master and forge_fee != 0 and not valid_proxy_fee(forge_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)
        if scroll_master and not valid_proxy_fee(scroll_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)
        if not scroll_master and scroll_fee != 0 and not valid_proxy_fee(scroll_fee):
            return self._finish(response, data, SKILL_PROXY_FEE_INVALID)

        sc.proxy_stall_open = True
        sc.proxy_forge_fee_gold = forge_fee if forge_master else 0
        sc.proxy_scroll_fee_gold = scroll_fee if scroll_master else 0
        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, self.scene.rows.assemble(player.snapshot), UNAVAILABLE)
        return self._finish(response, staged)

    def list_proxy_stalls(self, _world, _player, _route, _request):
        """列出当前在线且处于可交易状态的宗师原子代打/代制摊位（按 player_id 升序）。"""
        response = self.protocol.message("ListCraftProxyStallsResponse")
        masters = []
        seen_pids: set[int] = set()
        for location in getattr(self.scene, "locations", {}).values():
            for pid, target in location.players.items():
                if not pid or pid in seen_pids or not self.scene.mutable(target):
                    continue
                seen_pids.add(pid)
                tdata = self.scene.rows.assemble(target.snapshot)
                craft = tdata.craft_component
                if not craft.proxy_stall_open:
                    continue
                f_tier = mastery_tier_for_exp(int(craft.forge_mastery_exp))
                s_tier = mastery_tier_for_exp(int(craft.scroll_mastery_exp))
                if f_tier < MASTERY_MASTER and s_tier < MASTERY_MASTER:
                    continue
                masters.append((pid, tdata, craft, f_tier, s_tier))
        masters.sort(key=lambda item: item[0])
        for pid, tdata, craft, f_tier, s_tier in masters:
            response.stalls.add(
                master_player_id=pid,
                master_name=self._display_name(tdata, pid),
                forge_mastery_tier=f_tier,
                scroll_mastery_tier=s_tier,
                top_quality_rate_bps=forge_top_quality_rate_bps(f_tier),
                scroll_bonus_rate_bps=scroll_bonus_fragment_rate_bps(s_tier),
                proxy_forge_fee_gold=craft.proxy_forge_fee_gold if f_tier >= MASTERY_MASTER else 0,
                proxy_scroll_fee_gold=craft.proxy_scroll_fee_gold if s_tier >= MASTERY_MASTER else 0,
            )
        return response
