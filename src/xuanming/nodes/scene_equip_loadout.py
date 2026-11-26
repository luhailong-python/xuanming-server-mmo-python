"""装备流派方案预设系统（536..539，对齐 C++ equip_loadout_rules.h 与 player_equip_loadout.cpp）。

实例与布局严格分离：方案只记录 8 个正式槽位（EquipSlot 3..10）指向的真实装备 guid，
不复制 ItemComp、不另建隐藏背包；支持 0 空格 8v8 原位原子互换、战法/加点联动与扩容符/灵石解锁。
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import IntEnum

from ..data import client_request
from ..data.asset_inventory import (
    FEATURE_UNAVAILABLE,
    INVALID,
    UNAVAILABLE,
    bag_capacity,
    balance,
    clone,
)
from ..data.asset_ledger import UINT64_MAX
from .scene_mission import effective_level
from .scene_player import _bag, _fill_item
from .scene_stats import class_id

logger = logging.getLogger(__name__)

_SERVICE = "SceneBagClientPlayer"
CLIENT_REQUEST_KIND_EQUIP_LOADOUT_UNLOCK = 20

# EquipSlot.xlsx 正式 8 槽：3 武器 / 4 帽子 / 5 衣服 / 6 鞋子 / 7 首饰一 / 8 首饰二 / 9 法宝一 / 10 法宝二。
FORMAL_SLOTS: tuple[int, ...] = (3, 4, 5, 6, 7, 8, 9, 10)
FREE_SCHEMES = 1
MAX_SCHEMES = 5
MAX_ROLE_TAG = 4
MAX_NAME_CODE_POINTS = 12
UNLOCK_ITEM_CONFIG_ID = 3413

CURRENCY_DIAMOND = 1
CURRENCY_BIND_DIAMOND = 2

# 错误码（与 C++ equip_error_tip.pb.h 严格对齐）。
EQUIP_ITEM_NOT_FOUND = 28000
EQUIP_NOT_EQUIPMENT = 28001
EQUIP_LEVEL_NOT_ENOUGH = 28002
EQUIP_CLASS_MISMATCH = 28003
EQUIP_IN_BATTLE = 28007
EQUIP_FROZEN = 28008
EQUIP_INTERNAL_ERROR = 28010
EQUIP_LOADOUT_SCHEME_INVALID = 28031
EQUIP_LOADOUT_MAX_UNLOCKED = 28032
EQUIP_LOADOUT_VERSION_CHANGED = 28033
EQUIP_LOADOUT_DIAMOND_NOT_ENOUGH = 28034
EQUIP_LOADOUT_NAME_INVALID = 28035
EQUIP_LOADOUT_DUPLICATE_ITEM = 28036
EQUIP_LOADOUT_SLOT_UNAVAILABLE = 28037
EQUIP_LOADOUT_UNLOCK_COUNT_CHANGED = 28038

_OUTCOME_UNKNOWN = 0
_OUTCOME_REJECTED = 1
_OUTCOME_APPLIED = 2

_SLOT_EMPTY = 0
_SLOT_EQUIPPED = 1
_SLOT_IN_BAG = 2
_SLOT_MISSING_OR_STORED = 3

_FALLBACK_SLOT_META: dict[int, tuple[str, int]] = {
    3: ("武器", 11),
    4: ("帽子", 12),
    5: ("衣服", 13),
    6: ("鞋子", 14),
    7: ("首饰一", 15),
    8: ("首饰二", 15),
    9: ("法宝一", 16),
    10: ("法宝二", 16),
}
_REQUEST_ID_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_BLOCKED_NAME_TERMS = ("官方", "客服", "管理员", "系统消息", "外挂", "代充", "私服", "gm", "admin")
_INVISIBLE_CODE_POINTS = frozenset({
    0x20, 0xA0, 0x2028, 0x2029, 0x202F, 0x205F, 0x2060, 0x3000, 0xFEFF,
})


class SwapPlanError(IntEnum):
    NONE = 0
    INVALID_SLOT = 1
    DUPLICATE_ITEM = 2


@dataclass(frozen=True, slots=True)
class UnlockSpec:
    valid: bool = False
    target_scheme_id: int = 0
    cost_diamond: int = 0
    allow_bind_diamond: bool = False


@dataclass(frozen=True, slots=True)
class UnlockPaymentPlan:
    can_afford: bool = False
    use_talisman: bool = False
    spend_bind_diamond: int = 0
    spend_diamond: int = 0


@dataclass(frozen=True, slots=True)
class EquipFromBagMove:
    target_slot: int
    item_guid: int


@dataclass(frozen=True, slots=True)
class MoveWithinEquip:
    from_slot: int
    target_slot: int
    item_guid: int


@dataclass(frozen=True, slots=True)
class UnequipMove:
    from_slot: int
    item_guid: int
    paired_incoming_bag_guid: int = 0


@dataclass(slots=True)
class AtomicSwapPlan:
    error: SwapPlanError = SwapPlanError.NONE
    to_equip_from_bag: list[EquipFromBagMove] = field(default_factory=list)
    to_move_within_equip: list[MoveWithinEquip] = field(default_factory=list)
    to_unequip: list[UnequipMove] = field(default_factory=list)
    missing_slots: list[int] = field(default_factory=list)
    net_extra_bag_slots_needed: int = 0


def is_formal_slot(slot: int) -> bool:
    return 3 <= slot <= 10


def valid_role_tag(role_tag: int) -> bool:
    return 0 <= role_tag <= MAX_ROLE_TAG


def effective_unlocked_count(stored: int) -> int:
    if stored < FREE_SCHEMES:
        return FREE_SCHEMES
    return min(stored, MAX_SCHEMES)


def effective_active_scheme_id(stored: int, unlocked_count: int) -> int:
    unlocked = effective_unlocked_count(unlocked_count)
    return stored if 1 <= stored <= unlocked else 1


def effective_state_version(stored: int) -> int:
    return stored or 1


def valid_scheme_id(scheme_id: int, unlocked_count: int) -> bool:
    return 1 <= scheme_id <= effective_unlocked_count(unlocked_count)


def default_scheme_name(scheme_id: int) -> str:
    names = {1: "方案一", 2: "方案二", 3: "方案三", 4: "方案四", 5: "方案五"}
    return names.get(scheme_id, f"方案{scheme_id}")


def valid_request_id(request_id: str) -> bool:
    return 16 <= len(request_id) <= 64 and all(ch in _REQUEST_ID_CHARS for ch in request_id)


def unlock_spec_for_target(target_scheme_id: int) -> UnlockSpec:
    """方案位付费阶梯（零战力纯便利，上限 5 套）：方案 2 60 灵石可绑灵，3/4/5 为 120/180/240 非绑灵石。"""
    specs = {
        2: UnlockSpec(True, 2, 60, True),
        3: UnlockSpec(True, 3, 120, False),
        4: UnlockSpec(True, 4, 180, False),
        5: UnlockSpec(True, 5, 240, False),
    }
    return specs.get(target_scheme_id, UnlockSpec())


def plan_unlock_payment(
    spec: UnlockSpec,
    talisman_available: int,
    bind_diamond_balance: int,
    diamond_balance: int,
    use_bind_diamond_first: bool,
) -> UnlockPaymentPlan:
    """规划扩容支付路径：优先消耗 3413 扩容符，否则按档位扣灵石/绑定灵石。"""
    if not spec.valid:
        return UnlockPaymentPlan()
    if talisman_available >= 1:
        return UnlockPaymentPlan(can_afford=True, use_talisman=True)
    if spec.allow_bind_diamond and use_bind_diamond_first:
        from_bind = min(bind_diamond_balance, spec.cost_diamond)
        need_diamond = spec.cost_diamond - from_bind
        if diamond_balance < need_diamond:
            return UnlockPaymentPlan()
        return UnlockPaymentPlan(can_afford=True, spend_bind_diamond=from_bind, spend_diamond=need_diamond)
    if diamond_balance < spec.cost_diamond:
        return UnlockPaymentPlan()
    return UnlockPaymentPlan(can_afford=True, spend_diamond=spec.cost_diamond)


def default_sensitive_word_filter(text: str) -> bool:
    lower = text.lower()
    return not any(term in lower for term in _BLOCKED_NAME_TERMS)


def validate_scheme_name(
    name: str,
    sensitive_word_filter: Callable[[str], bool] | None = None,
) -> bool:
    """校验方案名称：UTF-8 1..12 字符、非全空白、无控制字符与尖括号、过敏感词。"""
    if not name or len(name) > MAX_NAME_CODE_POINTS:
        return False
    has_visible = False
    for ch in name:
        cp = ord(ch)
        if cp < 0x20 or cp == 0x7F or 0x80 <= cp <= 0x9F or ch in ("<", ">"):
            return False
        if cp not in _INVISIBLE_CODE_POINTS and not (0x2000 <= cp <= 0x200F):
            has_visible = True
    if not has_visible:
        return False
    checker = sensitive_word_filter or default_sensitive_word_filter
    return bool(checker(name))


def plan_atomic_swap(
    current_equipped_by_slot: dict[int, int],
    desired_by_slot: dict[int, int],
    in_inventory: Callable[[int], bool],
) -> AtomicSwapPlan:
    """两阶段原子换装纯规划：0 空格可完成 8v8 整套互换，缺失槽位保持原件不动。"""
    plan = AtomicSwapPlan()
    current_slot_of_guid = {
        guid: slot for slot, guid in current_equipped_by_slot.items() if is_formal_slot(slot) and guid
    }

    seen_desired: set[int] = set()
    for slot, guid in desired_by_slot.items():
        if not is_formal_slot(slot):
            plan.error = SwapPlanError.INVALID_SLOT
            return plan
        if guid:
            if guid in seen_desired:
                plan.error = SwapPlanError.DUPLICATE_ITEM
                return plan
            seen_desired.add(guid)

    present_target_by_slot: dict[int, int] = {}
    claimed_by_target: set[int] = set()
    missing_set: set[int] = set()
    for slot in FORMAL_SLOTS:
        guid = desired_by_slot.get(slot, 0)
        if not guid:
            continue
        if guid in current_slot_of_guid or in_inventory(guid):
            present_target_by_slot[slot] = guid
            claimed_by_target.add(guid)
        else:
            plan.missing_slots.append(slot)
            missing_set.add(slot)

    final_by_slot = dict(present_target_by_slot)
    final_guids = set(claimed_by_target)
    for slot in missing_set:
        cur_guid = current_equipped_by_slot.get(slot, 0)
        if cur_guid and cur_guid not in claimed_by_target:
            final_by_slot[slot] = cur_guid
            final_guids.add(cur_guid)

    incoming_bag_guid_by_slot: dict[int, int] = {}
    for slot in FORMAL_SLOTS:
        target_guid = final_by_slot.get(slot, 0)
        if not target_guid:
            continue
        cur_slot = current_slot_of_guid.get(target_guid)
        if cur_slot is None:
            plan.to_equip_from_bag.append(EquipFromBagMove(slot, target_guid))
            incoming_bag_guid_by_slot[slot] = target_guid
        elif cur_slot != slot:
            plan.to_move_within_equip.append(MoveWithinEquip(cur_slot, slot, target_guid))

    for slot in FORMAL_SLOTS:
        old_guid = current_equipped_by_slot.get(slot, 0)
        if not old_guid or old_guid in final_guids:
            continue
        paired = incoming_bag_guid_by_slot.get(slot, 0)
        plan.to_unequip.append(UnequipMove(slot, old_guid, paired))

    plan.net_extra_bag_slots_needed = max(0, len(plan.to_unequip) - len(plan.to_equip_from_bag))
    return plan


def _find_scheme(comp, scheme_id: int):
    return next((s for s in comp.schemes if s.scheme_id == scheme_id), None)


def _find_or_create_scheme(comp, scheme_id: int):
    existing = _find_scheme(comp, scheme_id)
    if existing is not None:
        return existing
    return comp.schemes.add(scheme_id=scheme_id, name=default_scheme_name(scheme_id), role_tag=0)


def _skill_scheme_display_name(scheme_id: int) -> str:
    names = {1: "战法方案一", 2: "战法方案二", 3: "战法方案三", 4: "战法方案四", 5: "战法方案五", 6: "战法方案六"}
    return names.get(scheme_id, f"战法方案{scheme_id}")


class SceneEquipLoadout:
    """装备流派方案预设客户端 RPC 处理器（536..539）。"""

    def __init__(
        self,
        scene,
        tables,
        assets=None,
        *,
        name_filter: Callable[[str], bool] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.name_filter = name_filter
        tx_enum = self.protocol.pool.FindEnumTypeByName("TransactionType")
        unlock_tx = (
            tx_enum.values_by_name.get("TX_EQUIP_LOADOUT_UNLOCK")
            or tx_enum.values_by_name["TX_CURRENCY_DEDUCT"]
        )
        self.unlock_tx = unlock_tx.number

    def register(self) -> None:
        """向 SceneNode 注册 536..539 装备方案预设 RPC。"""
        self.scene.register(_SERVICE, "GetEquipLoadouts", self.get_loadouts)
        self.scene.register(_SERVICE, "SaveEquipLoadout", self.save_loadout, mutating=True)
        self.scene.register(_SERVICE, "SwitchEquipLoadout", self.switch_loadout, mutating=True)
        self.scene.register(_SERVICE, "UnlockEquipLoadout", self.unlock_loadout, mutating=True)

    # ── 校验与槽位辅助 ──

    @staticmethod
    def _required_present(data) -> bool:
        return data.HasField("level_component") and data.HasField("uint32_pb_component")

    def _writable_error(self, player, data) -> int:
        if not self._required_present(data):
            return UNAVAILABLE
        if not self.scene.mutable(player):
            return EQUIP_FROZEN
        if self.assets is not None and self.assets._player_in_battle(player):
            return EQUIP_IN_BATTLE
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return EQUIP_IN_BATTLE
        return 0

    def _slot_meta(self, slot: int) -> tuple[str, int]:
        if "EquipSlot" in self.tables.tables:
            row = self.tables["EquipSlot"].get(slot)
            if row is not None:
                return str(row.name), int(row.equip_kind)
        return _FALLBACK_SLOT_META.get(slot, ("", 0))

    def _slot_accepts_kind(self, slot: int, equip_kind: int) -> bool:
        if not is_formal_slot(slot) or not equip_kind:
            return False
        _, accepted_kind = self._slot_meta(slot)
        return accepted_kind == equip_kind

    def _unlocked_skill_scheme_count(self, data) -> int:
        skills = getattr(self.scene, "skills", None)
        if skills is not None and hasattr(skills, "unlocked_loadout_scheme_count"):
            return int(skills.unlocked_loadout_scheme_count(data))
        afk_pass = getattr(self.scene, "afk_pass", None)
        if afk_pass is not None and hasattr(afk_pass, "is_active_data"):
            return 6 if afk_pass.is_active_data(data) else 3
        return 3

    @staticmethod
    def _attribute_scheme_exists(data, scheme_id: int) -> bool:
        if not data.HasField("attribute_component"):
            return False
        return any(s.scheme_id == scheme_id for s in data.attribute_component.schemes)

    def _validate_candidate(self, data, item, target_slot: int) -> int:
        row = self.tables["Item"].get(item.config_id)
        if row is None or not row.equip_kind or row.max_stack_size != 1:
            return EQUIP_NOT_EQUIPMENT
        profile = int(getattr(row, "equip_profile", 0))
        if profile > 3:
            return FEATURE_UNAVAILABLE
        if item.size != 1:
            return EQUIP_ITEM_NOT_FOUND
        if effective_level(data) < int(row.equip_level):
            return EQUIP_LEVEL_NOT_ENOUGH
        if row.equip_class and row.equip_class != class_id(data):
            return EQUIP_CLASS_MISMATCH
        if not self._slot_accepts_kind(target_slot, int(row.equip_kind)):
            return EQUIP_LOADOUT_SLOT_UNAVAILABLE
        return 0

    def _fill_slot_item_meta(self, data, item, slot_view) -> None:
        out = slot_view.item
        out.Clear()
        out.item_id = item.item_id
        out.config_id = item.config_id
        out.count = item.size
        out.bound = bool(item.bound)
        _fill_item(self.tables, out, item.equip if item.HasField("equip") else None)
        if self.assets is not None and hasattr(self.assets, "enrich_bag_enhance_preview"):
            tmp = self.protocol.message("BagInfo")
            tmp.items.add().CopyFrom(out)
            bag_type = 2 if slot_view.status == _SLOT_EQUIPPED else 0
            self.assets.enrich_bag_enhance_preview(data, bag_type, tmp)
            equip_craft = getattr(self.scene, "equip_craft", None)
            if equip_craft is not None:
                equip_craft.fill_v1_display(item, tmp.items[0])
            out.CopyFrom(tmp.items[0])

    def build_snapshot(self, data, snapshot) -> int:
        """构建完整装备方案快照（1..5 套位、实时 8 槽状态与可选联动列表）。"""
        snapshot.Clear()
        if not self._required_present(data) or self.assets is None:
            return UNAVAILABLE
        comp = data.equip_loadout_component
        unlocked = effective_unlocked_count(int(comp.unlocked_scheme_count))
        active = effective_active_scheme_id(int(comp.active_scheme_id), unlocked)
        version = effective_state_version(int(comp.state_version))

        snapshot.unlocked_scheme_count = unlocked
        snapshot.max_scheme_count = MAX_SCHEMES
        snapshot.active_scheme_id = active
        snapshot.state_version = version
        snapshot.unlock_item_config_id = UNLOCK_ITEM_CONFIG_ID
        snapshot.unlock_item_owned = self.assets.removable_count(data, UNLOCK_ITEM_CONFIG_ID)
        snapshot.diamond_balance = balance(data, CURRENCY_DIAMOND)
        snapshot.bind_diamond_balance = balance(data, CURRENCY_BIND_DIAMOND)

        if unlocked < MAX_SCHEMES:
            spec = unlock_spec_for_target(unlocked + 1)
            snapshot.next_unlock_cost_diamond = spec.cost_diamond
            snapshot.next_unlock_allow_bind_diamond = spec.allow_bind_diamond
        else:
            snapshot.next_unlock_cost_diamond = 0
            snapshot.next_unlock_allow_bind_diamond = False

        worn_by_guid = {item.item_id: item for _, item in self.assets.entries(data.bag_component, 2)}
        bag_by_guid = {item.item_id: item for _, item in self.assets.entries(data.bag_component, 0)}

        for scheme_id in range(1, MAX_SCHEMES + 1):
            info = snapshot.schemes.add(scheme_id=scheme_id, unlocked=(scheme_id <= unlocked))
            stored = _find_scheme(comp, scheme_id)
            info.name = stored.name if (stored is not None and stored.name) else default_scheme_name(scheme_id)
            info.role_tag = int(stored.role_tag) if stored is not None else 0
            info.linked_skill_scheme_id = int(stored.linked_skill_scheme_id) if stored is not None else 0
            info.linked_attribute_scheme_id = int(stored.linked_attribute_scheme_id) if stored is not None else 0

            bound_by_slot: dict[int, int] = {}
            if stored is not None:
                for binding in stored.slots:
                    if is_formal_slot(int(binding.slot)) and binding.item_guid:
                        bound_by_slot[int(binding.slot)] = int(binding.item_guid)

            missing_count = 0
            for slot in FORMAL_SLOTS:
                slot_view = info.slots.add()
                slot_name, equip_kind = self._slot_meta(slot)
                slot_view.slot = slot
                slot_view.slot_name = slot_name
                slot_view.equip_kind = equip_kind
                guid = bound_by_slot.get(slot, 0)
                if not guid:
                    slot_view.status = _SLOT_EMPTY
                    continue
                slot_view.item_guid = guid
                if guid in worn_by_guid:
                    slot_view.status = _SLOT_EQUIPPED
                    self._fill_slot_item_meta(data, worn_by_guid[guid], slot_view)
                elif guid in bag_by_guid:
                    slot_view.status = _SLOT_IN_BAG
                    self._fill_slot_item_meta(data, bag_by_guid[guid], slot_view)
                else:
                    slot_view.status = _SLOT_MISSING_OR_STORED
                    missing_count += 1
            info.missing_count = missing_count

        skill_schemes = self._unlocked_skill_scheme_count(data)
        for sid in range(1, skill_schemes + 1):
            snapshot.skill_schemes.add(scheme_id=sid, name=_skill_scheme_display_name(sid))
        if data.HasField("attribute_component"):
            for s in data.attribute_component.schemes:
                snapshot.attribute_schemes.add(
                    scheme_id=s.scheme_id,
                    name=s.name or f"加点方案{s.scheme_id}",
                )
        return 0

    def _fill_bags(self, player, data, response, *, include_equipment: bool) -> int:
        inv = _bag(self.scene, self.tables, player, 0)
        if inv.error_message.id:
            return inv.error_message.id
        response.bag.CopyFrom(inv.bag)
        self.assets.enrich_bag_enhance_preview(data, 0, response.bag)
        equip_craft = getattr(self.scene, "equip_craft", None)
        if equip_craft is not None:
            equip_craft._enrich_bag_snapshot(data, 0, response.bag)
        if include_equipment:
            eq = _bag(self.scene, self.tables, player, 2)
            if eq.error_message.id:
                response.ClearField("bag")
                return eq.error_message.id
            response.equipment.CopyFrom(eq.bag)
            self.assets.enrich_bag_enhance_preview(data, 2, response.equipment)
            if equip_craft is not None:
                equip_craft._enrich_bag_snapshot(data, 2, response.equipment)
        return 0

    # ── 4 个客户端 RPC 处理入口 ──

    def get_loadouts(self, _world, player, _route, _request):
        """536 查询玩家装备方案预设快照（纯只读）。"""
        response = self.protocol.message("GetEquipLoadoutsResponse")
        data = self.scene.rows.assemble(player.snapshot)
        err = self.build_snapshot(data, response.snapshot)
        if err:
            response.ClearField("snapshot")
            response.error_message.id = err
        return response

    async def save_loadout(self, _world, player, _route, request):
        """537 保存装备流派方案（支持一键抓拍当前身上 8 槽或显式绑定 guid）。"""
        response = self.protocol.message("SaveEquipLoadoutResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0, current_data=None):
            if error:
                response.error_message.id = error
            target_data = current_data if current_data is not None else self.scene.rows.assemble(player.snapshot)
            snap_err = self.build_snapshot(target_data, response.snapshot)
            if snap_err:
                response.ClearField("snapshot")
                if not error:
                    response.error_message.id = snap_err
            return response

        if not self._required_present(data) or self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        if not valid_request_id(request.request_id):
            return finish(INVALID, data)

        comp = data.equip_loadout_component
        if comp.last_request_id == request.request_id:
            same = (
                comp.last_base_version == request.state_version
                and comp.last_scheme_id == request.scheme_id
            )
            if not same:
                return finish(INVALID, data)
            response.replayed = True
            return finish(0, data)

        if err := self._writable_error(player, data):
            return finish(err, data)

        unlocked = effective_unlocked_count(int(comp.unlocked_scheme_count))
        if not valid_scheme_id(int(request.scheme_id), unlocked):
            return finish(EQUIP_LOADOUT_SCHEME_INVALID, data)
        current_ver = effective_state_version(int(comp.state_version))
        if request.state_version != current_ver or int(comp.state_version) >= UINT64_MAX:
            return finish(EQUIP_LOADOUT_VERSION_CHANGED, data)
        if not valid_role_tag(int(request.role_tag)):
            return finish(INVALID, data)

        final_name = request.name or default_scheme_name(int(request.scheme_id))
        if request.name and not validate_scheme_name(final_name, self.name_filter):
            return finish(EQUIP_LOADOUT_NAME_INVALID, data)

        if request.linked_skill_scheme_id and request.linked_skill_scheme_id > self._unlocked_skill_scheme_count(data):
            return finish(INVALID, data)
        if request.linked_attribute_scheme_id and not self._attribute_scheme_exists(data, int(request.linked_attribute_scheme_id)):
            return finish(INVALID, data)

        worn_by_slot = {
            entry.pos: item.item_id
            for entry, item in self.assets.entries(data.bag_component, 2)
            if is_formal_slot(entry.pos)
        }
        worn_by_guid = {item.item_id: item for _, item in self.assets.entries(data.bag_component, 2)}
        bag_by_guid = {item.item_id: item for _, item in self.assets.entries(data.bag_component, 0)}

        validated_slots: dict[int, int] = {}
        if request.capture_current_equipped:
            for slot in FORMAL_SLOTS:
                guid = worn_by_slot.get(slot, 0)
                if guid:
                    validated_slots[slot] = guid
        else:
            seen_guids: set[int] = set()
            for binding in request.slots:
                slot = int(binding.slot)
                guid = int(binding.item_guid)
                if not is_formal_slot(slot):
                    return finish(EQUIP_LOADOUT_SLOT_UNAVAILABLE, data)
                if slot in validated_slots:
                    return finish(INVALID, data)
                if not guid:
                    validated_slots[slot] = 0
                    continue
                if guid in seen_guids:
                    return finish(EQUIP_LOADOUT_DUPLICATE_ITEM, data)
                seen_guids.add(guid)
                item = worn_by_guid.get(guid) or bag_by_guid.get(guid)
                if item is None:
                    return finish(EQUIP_ITEM_NOT_FOUND, data)
                row = self.tables["Item"].get(item.config_id)
                if (
                    row is None
                    or not row.equip_kind
                    or row.max_stack_size != 1
                    or int(getattr(row, "equip_profile", 0)) > 3
                    or item.size != 1
                    or not self._slot_accepts_kind(slot, int(row.equip_kind))
                ):
                    return finish(EQUIP_LOADOUT_SLOT_UNAVAILABLE, data)
                validated_slots[slot] = guid

        staged = clone(data)
        sc = staged.equip_loadout_component
        scheme = _find_or_create_scheme(sc, int(request.scheme_id))
        scheme.name = final_name
        scheme.role_tag = int(request.role_tag)
        scheme.linked_skill_scheme_id = int(request.linked_skill_scheme_id)
        scheme.linked_attribute_scheme_id = int(request.linked_attribute_scheme_id)
        del scheme.slots[:]
        for slot in sorted(validated_slots):
            guid = validated_slots[slot]
            if guid:
                scheme.slots.add(slot=slot, item_guid=guid)

        if sc.unlocked_scheme_count < FREE_SCHEMES:
            sc.unlocked_scheme_count = FREE_SCHEMES
        if not sc.active_scheme_id:
            sc.active_scheme_id = 1
        sc.state_version = current_ver + 1
        sc.last_request_id = request.request_id
        sc.last_base_version = request.state_version
        sc.last_scheme_id = request.scheme_id

        if not await self.assets._save(player, staged):
            return finish(UNAVAILABLE)
        logger.info(
            "Equip loadout saved player_id=%s scheme_id=%s slots=%s capture_current=%s version=%s",
            player.player_id, request.scheme_id, len(scheme.slots),
            request.capture_current_equipped, sc.state_version,
        )
        return finish(0, staged)

    async def switch_loadout(self, _world, player, _route, request):
        """538 两阶段原子切换装备方案，支持 0 空格 8v8 原位互换与战法/加点联动。"""
        response = self.protocol.message("SwitchEquipLoadoutResponse", request_id=request.request_id)
        data = self.scene.rows.assemble(player.snapshot)

        def finish(error: int = 0, *, include_bags: bool = False, current_data=None):
            if error:
                response.error_message.id = error
            target_data = current_data if current_data is not None else self.scene.rows.assemble(player.snapshot)
            if include_bags:
                bag_err = self._fill_bags(player, target_data, response, include_equipment=True)
                if bag_err and not error:
                    response.error_message.id = bag_err
            snap_err = self.build_snapshot(target_data, response.snapshot)
            if snap_err:
                response.ClearField("snapshot")
                if not error and not response.error_message.id:
                    response.error_message.id = snap_err
            return response

        if not self._required_present(data) or self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        if not valid_request_id(request.request_id):
            return finish(INVALID, current_data=data)

        comp = data.equip_loadout_component
        if comp.last_request_id == request.request_id:
            same = (
                comp.last_base_version == request.state_version
                and comp.last_scheme_id == request.scheme_id
            )
            if not same:
                return finish(INVALID, current_data=data)
            response.replayed = True
            return finish(0, include_bags=True, current_data=data)

        if err := self._writable_error(player, data):
            return finish(err, current_data=data)

        unlocked = effective_unlocked_count(int(comp.unlocked_scheme_count))
        if not valid_scheme_id(int(request.scheme_id), unlocked):
            return finish(EQUIP_LOADOUT_SCHEME_INVALID, current_data=data)
        current_ver = effective_state_version(int(comp.state_version))
        if request.state_version != current_ver or int(comp.state_version) >= UINT64_MAX:
            return finish(EQUIP_LOADOUT_VERSION_CHANGED, current_data=data)

        # 阶段 1：只读规划与前置校验（任一失败零副作用）。
        current_by_slot: dict[int, int] = {}
        worn_by_guid = {}
        for entry, item in self.assets.entries(data.bag_component, 2):
            worn_by_guid[item.item_id] = (entry, item)
            if is_formal_slot(entry.pos):
                current_by_slot[entry.pos] = item.item_id

        bag_by_guid = {}
        occupied_bag_slots: set[int] = set()
        for entry, item in self.assets.entries(data.bag_component, 0):
            bag_by_guid[item.item_id] = (entry, item)
            occupied_bag_slots.add(entry.pos)

        desired_by_slot: dict[int, int] = {}
        seen_desired: set[int] = set()
        stored_scheme = _find_scheme(comp, int(request.scheme_id))
        if stored_scheme is not None:
            for binding in stored_scheme.slots:
                slot = int(binding.slot)
                guid = int(binding.item_guid)
                if not is_formal_slot(slot):
                    return finish(EQUIP_LOADOUT_SLOT_UNAVAILABLE, current_data=data)
                if guid:
                    if guid in seen_desired:
                        return finish(EQUIP_LOADOUT_DUPLICATE_ITEM, current_data=data)
                    seen_desired.add(guid)
                    desired_by_slot[slot] = guid

        plan = plan_atomic_swap(current_by_slot, desired_by_slot, lambda guid: guid in bag_by_guid)
        if plan.error == SwapPlanError.DUPLICATE_ITEM:
            return finish(EQUIP_LOADOUT_DUPLICATE_ITEM, current_data=data)
        if plan.error != SwapPlanError.NONE:
            return finish(EQUIP_LOADOUT_SLOT_UNAVAILABLE, current_data=data)

        bag_config_needs: Counter[int] = Counter()
        for move in plan.to_equip_from_bag:
            pair = bag_by_guid.get(move.item_guid)
            if pair is None:
                return finish(EQUIP_ITEM_NOT_FOUND, current_data=data)
            err = self._validate_candidate(data, pair[1], move.target_slot)
            if err:
                return finish(err, current_data=data)
            bag_config_needs[pair[1].config_id] += 1

        for move in plan.to_move_within_equip:
            pair = worn_by_guid.get(move.item_guid)
            if pair is None:
                return finish(EQUIP_INTERNAL_ERROR, current_data=data)
            err = self._validate_candidate(data, pair[1], move.target_slot)
            if err:
                return finish(err, current_data=data)

        for cfg_id, needed in bag_config_needs.items():
            if self.assets.removable_count(data, cfg_id) < needed:
                logger.info(
                    "Equip loadout switch refused: equipment held by unsettled battle player_id=%s scheme_id=%s",
                    player.player_id, request.scheme_id,
                )
                return finish(EQUIP_LOADOUT_SLOT_UNAVAILABLE, current_data=data)

        for move in plan.to_unequip:
            pair = worn_by_guid.get(move.item_guid)
            row = self.tables["Item"].get(pair[1].config_id) if pair is not None else None
            if pair is None or row is None or row.max_stack_size != 1 or pair[1].size != 1:
                return finish(EQUIP_INTERNAL_ERROR, current_data=data)

        inv_cap = bag_capacity(data.bag_component, 0)
        free_cells = max(0, inv_cap - len(occupied_bag_slots))
        if free_cells < plan.net_extra_bag_slots_needed:
            return finish(EQUIP_LOADOUT_SLOT_UNAVAILABLE, current_data=data)

        # 阶段 2：在 detached 副本上原子搬运（先全取再全落位）。
        staged = clone(data)
        taken_from_bag: list[tuple[int, int, object]] = []
        incoming_bag_pos_by_guid: dict[int, int] = {}
        taken_unequip: list[tuple[int | None, object]] = []
        taken_move_within: list[tuple[int, object]] = []

        moving_bag_guids = {m.item_guid for m in plan.to_equip_from_bag}
        moving_equip_guids = {m.item_guid for m in plan.to_unequip} | {m.item_guid for m in plan.to_move_within_equip}

        extracted_bag: dict[int, tuple[int, object]] = {}
        extracted_equip: dict[int, object] = {}
        for idx in range(len(staged.bag_component.items) - 1, -1, -1):
            entry = staged.bag_component.items[idx]
            item = self.assets.item(entry)
            if entry.bag_type == 0 and item.item_id in moving_bag_guids:
                extracted_bag[item.item_id] = (entry.pos, item)
                del staged.bag_component.items[idx]
            elif entry.bag_type == 2 and item.item_id in moving_equip_guids:
                extracted_equip[item.item_id] = item
                del staged.bag_component.items[idx]

        for move in plan.to_equip_from_bag:
            pos, item = extracted_bag[move.item_guid]
            incoming_bag_pos_by_guid[move.item_guid] = pos
            taken_from_bag.append((move.target_slot, pos, item))
        for move in plan.to_unequip:
            pref_pos = incoming_bag_pos_by_guid.get(move.paired_incoming_bag_guid) if move.paired_incoming_bag_guid else None
            taken_unequip.append((pref_pos, extracted_equip[move.item_guid]))
        for move in plan.to_move_within_equip:
            taken_move_within.append((move.target_slot, extracted_equip[move.item_guid]))

        for target_slot, _src_pos, item in taken_from_bag:
            self.assets.write_entry(staged.bag_component.items.add(pos=target_slot, bag_type=2), item)
        for target_slot, item in taken_move_within:
            self.assets.write_entry(staged.bag_component.items.add(pos=target_slot, bag_type=2), item)

        remaining_bag_slots = {e.pos for e in staged.bag_component.items if e.bag_type == 0}
        for pref_pos, item in taken_unequip:
            if pref_pos is not None and pref_pos < inv_cap and pref_pos not in remaining_bag_slots:
                pos = pref_pos
            else:
                pos = next((s for s in range(inv_cap) if s not in remaining_bag_slots), None)
            if pos is None:
                return finish(EQUIP_INTERNAL_ERROR, current_data=data)
            remaining_bag_slots.add(pos)
            self.assets.write_entry(staged.bag_component.items.add(pos=pos, bag_type=0), item)
        self.assets._order(staged.bag_component.items)

        # 状态更新与跨系统流派联动。
        sc = staged.equip_loadout_component
        if sc.unlocked_scheme_count < FREE_SCHEMES:
            sc.unlocked_scheme_count = FREE_SCHEMES
        sc.active_scheme_id = int(request.scheme_id)
        sc.state_version = current_ver + 1
        sc.last_request_id = request.request_id
        sc.last_base_version = request.state_version
        sc.last_scheme_id = int(request.scheme_id)

        linked_changed = False
        if request.apply_linked_schemes and stored_scheme is not None:
            if stored_scheme.linked_skill_scheme_id:
                skills = getattr(self.scene, "skills", None)
                if skills is not None and hasattr(skills, "try_switch_linked_loadout"):
                    ok, notice = skills.try_switch_linked_loadout(staged, int(stored_scheme.linked_skill_scheme_id))
                    if notice:
                        response.linked_notices.append(notice)
                    if ok:
                        linked_changed = True
                else:
                    response.linked_notices.append("联动战法方案暂不可用，本次仅完成装备切换")
            if stored_scheme.linked_attribute_scheme_id:
                attributes = getattr(self.scene, "attributes", None)
                if attributes is not None and hasattr(attributes, "try_switch_linked_scheme"):
                    attr_ok, attr_notice = attributes.try_switch_linked_scheme(
                        staged, int(stored_scheme.linked_attribute_scheme_id),
                    )
                    if attr_notice:
                        response.linked_notices.append(attr_notice)
                    if attr_ok:
                        linked_changed = True
                else:
                    response.linked_notices.append("属性加点方案暂不可切换，本次仅完成装备切换")

        if linked_changed:
            self.assets._update_worn_stats(data, staged)

        inbox = getattr(self.scene, "inbox", None)
        space_freed = len(plan.to_equip_from_bag) > len(plan.to_unequip)
        attributes = getattr(self.scene, "attributes", None)

        async def _on_committed(_snapshot):
            if linked_changed and attributes is not None:
                attributes.notify(player)
            if space_freed and inbox is not None:
                inbox.on_bag_space_freed(player)

        if not await self.assets._save(player, staged, on_committed=_on_committed):
            return finish(UNAVAILABLE)

        for slot in plan.missing_slots:
            response.missing_slots.append(slot)
        logger.info(
            "Equip loadout switched player_id=%s scheme_id=%s from_bag=%s moved=%s unequipped=%s missing=%s version=%s",
            player.player_id, request.scheme_id, len(plan.to_equip_from_bag),
            len(plan.to_move_within_equip), len(plan.to_unequip), len(plan.missing_slots), sc.state_version,
        )
        return finish(0, include_bags=True, current_data=staged)

    async def unlock_loadout(self, _world, player, _route, request):
        """539 解锁下一个装备流派方案位（优先扣 3413 扩容符，其次按阶梯扣灵石/绑灵，幂等落盘）。"""
        response = self.protocol.message(
            "UnlockEquipLoadoutResponse", client_request_id=request.client_request_id,
        )
        data = self.scene.rows.assemble(player.snapshot)

        def reject(tip: int, *, known: bool = True, current_data=None):
            response.outcome = _OUTCOME_REJECTED if known else _OUTCOME_UNKNOWN
            response.error_message.id = tip
            target_data = current_data if current_data is not None else self.scene.rows.assemble(player.snapshot)
            response.current_unlocked_count = effective_unlocked_count(
                int(target_data.equip_loadout_component.unlocked_scheme_count),
            )
            if self.build_snapshot(target_data, response.snapshot):
                response.ClearField("snapshot")
            return response

        def fill_applied(current_data, unlocked_count: int, used_item_id: int, spent_bind: int, spent_diamond: int, replayed: bool):
            response.outcome = _OUTCOME_APPLIED
            response.replayed = replayed
            response.durable = True
            response.result_unlocked_count = unlocked_count
            response.current_unlocked_count = effective_unlocked_count(
                int(current_data.equip_loadout_component.unlocked_scheme_count),
            )
            response.used_item_config_id = used_item_id
            response.spent_bind_diamond = spent_bind
            response.spent_diamond = spent_diamond
            snap_err = self.build_snapshot(current_data, response.snapshot)
            if snap_err:
                response.ClearField("snapshot")
                response.error_message.id = snap_err
            bag_err = self._fill_bags(player, current_data, response, include_equipment=False)
            if bag_err:
                response.ClearField("bag")
                if not response.error_message.id:
                    response.error_message.id = bag_err
            return response

        if not self._required_present(data) or self.assets is None:
            return reject(UNAVAILABLE, known=False, current_data=data)
        if not request.client_request_id:
            return reject(INVALID, known=True, current_data=data)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, CLIENT_REQUEST_KIND_EQUIP_LOADOUT_UNLOCK)
        if err:
            return reject(err, known=(err == INVALID), current_data=data)
        if prior is not None:
            return fill_applied(
                data,
                int(prior.equip_loadout_unlocked_count),
                int(prior.equip_loadout_used_item_id),
                int(prior.equip_loadout_spent_bind_diamond),
                int(prior.equip_loadout_spent_diamond),
                True,
            )

        if err := self._writable_error(player, data):
            return reject(err, known=True, current_data=data)

        comp = data.equip_loadout_component
        current_unlocked = effective_unlocked_count(int(comp.unlocked_scheme_count))
        if request.expected_unlocked_count != current_unlocked:
            known = current_unlocked < request.expected_unlocked_count
            return reject(EQUIP_LOADOUT_UNLOCK_COUNT_CHANGED, known=known, current_data=data)
        if current_unlocked >= MAX_SCHEMES:
            return reject(EQUIP_LOADOUT_MAX_UNLOCKED, known=True, current_data=data)
        if int(ledger.next_seq) >= UINT64_MAX or int(comp.state_version) >= UINT64_MAX:
            return reject(UNAVAILABLE, known=False, current_data=data)

        target_scheme_id = current_unlocked + 1
        spec = unlock_spec_for_target(target_scheme_id)
        if not spec.valid:
            return reject(EQUIP_LOADOUT_MAX_UNLOCKED, known=True, current_data=data)

        talisman_avail = self.assets.removable_count(data, UNLOCK_ITEM_CONFIG_ID)
        bind_bal = balance(data, CURRENCY_BIND_DIAMOND)
        diamond_bal = balance(data, CURRENCY_DIAMOND)
        payment = plan_unlock_payment(
            spec, talisman_avail, bind_bal, diamond_bal, bool(request.use_bind_diamond_first),
        )
        if not payment.can_afford:
            return reject(EQUIP_LOADOUT_DIAMOND_NOT_ENOUGH, known=True, current_data=data)

        staged = clone(data)
        used_item_id = 0
        if payment.use_talisman:
            consume_err = self.assets.consume_items(staged, {UNLOCK_ITEM_CONFIG_ID: 1})
            if consume_err:
                return reject(EQUIP_LOADOUT_DIAMOND_NOT_ENOUGH, known=True, current_data=data)
            used_item_id = UNLOCK_ITEM_CONFIG_ID
        else:
            if payment.spend_bind_diamond > 0:
                if self.assets.deduct_currency(staged, CURRENCY_BIND_DIAMOND, payment.spend_bind_diamond):
                    return reject(EQUIP_LOADOUT_DIAMOND_NOT_ENOUGH, known=True, current_data=data)
            if payment.spend_diamond > 0:
                if self.assets.deduct_currency(staged, CURRENCY_DIAMOND, payment.spend_diamond):
                    return reject(EQUIP_LOADOUT_DIAMOND_NOT_ENOUGH, known=True, current_data=data)

        sc = staged.equip_loadout_component
        sc.unlocked_scheme_count = target_scheme_id
        if not sc.active_scheme_id:
            sc.active_scheme_id = 1
        _find_or_create_scheme(sc, target_scheme_id)
        sc.state_version = effective_state_version(int(sc.state_version)) + 1

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            CLIENT_REQUEST_KIND_EQUIP_LOADOUT_UNLOCK,
            fingerprint,
            equip_loadout_unlocked_count=target_scheme_id,
            equip_loadout_used_item_id=used_item_id,
            equip_loadout_spent_bind_diamond=payment.spend_bind_diamond,
            equip_loadout_spent_diamond=payment.spend_diamond,
        )

        if not await self.assets._save(
            player,
            staged,
            self.unlock_tx,
            correlation=request.client_request_id,
            item_extra={"source": "equip_loadout_unlock", "target_scheme": target_scheme_id},
        ):
            return reject(UNAVAILABLE, known=False)

        logger.info(
            "Equip loadout unlocked player_id=%s unlocked=%s used_item=%s spent_bind=%s spent_diamond=%s req=%s",
            player.player_id, target_scheme_id, used_item_id,
            payment.spend_bind_diamond, payment.spend_diamond, request.client_request_id,
        )
        return fill_applied(
            staged, target_scheme_id, used_item_id, payment.spend_bind_diamond, payment.spend_diamond, False,
        )
