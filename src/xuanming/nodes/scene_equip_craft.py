"""装备 V1 打造、固定词条安装与每日材料自选领取系统，协作 scene.assets 提交存档。"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    BAG_FULL, BAG_INSUFFICIENT, BLOCKED, FEATURE_UNAVAILABLE, INVALID,
    INVALID_DATA, UNAVAILABLE, bag_capacity, carries_unknown, clone, held_item_count,
)
from .scene_mission import effective_level, now_ms
from .scene_player import _bag, _fill_item
from .scene_season import valid_artifact_definition

logger = logging.getLogger(__name__)

_SERVICE = "SceneBagClientPlayer"
_CLIENT_REQUEST_KIND_CRAFT = 4
_CLIENT_REQUEST_KIND_MATERIAL_REWARD = 5
_MAX_UINT64 = (1 << 64) - 1
_MAX_CACHED_RECEIPTS = 256

# 装备与材料错误码（与 C++ equip_error_tip.pb.h 对齐）。
EQUIP_ITEM_NOT_FOUND = 28000
EQUIP_LEVEL_NOT_ENOUGH = 28002
EQUIP_IN_BATTLE = 28007
EQUIP_FROZEN = 28008
EQUIP_INTERNAL_ERROR = 28010
EQUIP_MATERIAL_NOT_ENOUGH = 28013
EQUIP_LEVEL_CHANGED = 28022

# 材料自选与游戏日契约（UTC+8 05:00 换日，与 C++ PlayerEquipmentMaterialRewardSystem 一致）。
DAY_MS = 86_400_000
DAY_SHIFT_MS = 10_800_000
MATERIAL_REWARD_DAILY_LIMIT = 20
MATERIAL_REWARD_BATTLE_UNITS = 10
CRAFTING_MATERIAL_ID = 3001
ENHANCEMENT_STONE_ID = 3002
LEGACY_ENHANCEMENT_STONE_ID = 2001
DEFAULT_FORGE_MATERIAL_COUNT = 10

_OUTCOME_UNKNOWN = 0
_OUTCOME_REJECTED = 1
_OUTCOME_APPLIED = 2

_OP_CREATE_ITEM = 1
_OP_INSTALL_AFFIX = 2

_MAX_SOURCE_BPS = {
    1: 3200, 2: 3200, 5: 3200, 6: 3200,
    3: 800, 7: 800, 8: 800,
    4: 200,
    9: 600, 10: 600,
    11: 1600, 12: 1600,
    13: 1200,
}


@dataclass
class _MaterialRewardState:
    day_starts_at_ms: int = 0
    earned_today: int = 0
    available_units: int = 0
    revision: int = 0


def resolve_game_day(now_ms_val: int) -> tuple[int, int] | None:
    """按 UTC+8 05:00 计算当前游戏日 [start_ms, next_reset_ms)。"""
    offset = (now_ms_val % DAY_MS + DAY_SHIFT_MS) % DAY_MS
    if now_ms_val < offset:
        return None
    start = now_ms_val - offset
    if start > _MAX_UINT64 - DAY_MS:
        return None
    return start, start + DAY_MS


def maximum_source_bps(effect: int) -> int:
    """返回装备同类固定词条全身上限（万分比）。"""
    return _MAX_SOURCE_BPS.get(effect, 0)


def pool_for(profile: int, slot: int) -> int:
    """按装备分型（1=常规四部位，2=首饰）和词条槽位（1/2）返回合法词条池号。"""
    if slot not in (1, 2):
        return 0
    if profile == 1:
        return slot
    return 3 if profile == 2 else 0


def valid_affix_row(row) -> bool:
    """校验单条 EquipV1Affix 规则行是否合法（与 C++ ValidAffix 一致）。"""
    if (
        not row.id
        or not row.value_permyriad
        or row.source_cap < row.value_permyriad
        or row.source_cap > 10_000
        or not row.material_id
        or not row.material_count
    ):
        return False
    effect, param = row.effect, row.effect_param
    if not 1 <= effect <= 13 or row.source_cap > maximum_source_bps(effect):
        return False
    if effect <= 10 and param != 0:
        return False
    if effect in (11, 12) and not 1 <= param <= 7:
        return False
    if effect == 13 and not 1 <= param <= 12:
        return False
    if row.pool == 1:
        return effect in (1, 2, 5, 6, 11)
    if row.pool == 2:
        return effect in (7, 8, 13)
    return row.pool == 3 and effect in (3, 4, 9, 10, 12)


def _valid_artifact_row(row) -> bool:
    return valid_artifact_definition(
        row.id, row.bonus_bps, row.cap_bps,
        row.per_round_limit, row.battle_limit, row.active_rounds, row.health_threshold_bps,
    )


def _fill_affix_info(row, out) -> None:
    out.affix_id = row.id
    out.pool = row.pool
    out.name = row.name
    out.description = row.description
    out.value_bps = row.value_permyriad
    out.source_cap_bps = row.source_cap
    out.effect = row.effect
    out.effect_param = row.effect_param
    out.unit = 2 if 7 <= row.effect <= 12 else 1
    out.material_config_id = row.material_id
    out.material_count = row.material_count


def _fill_artifact_info(row, out) -> None:
    out.effect_id = row.id
    out.name = row.name
    out.description = row.description
    out.bonus_bps = row.bonus_bps
    out.cap_bps = row.cap_bps
    out.per_round_limit = row.per_round_limit
    out.battle_limit = row.battle_limit
    out.active_rounds = row.active_rounds
    out.health_threshold_bps = row.health_threshold_bps


class SceneEquipCraft:
    """Scene 装备 V1 打造、固定词条安装与每日材料自选领取伴生模块。"""

    def __init__(self, scene, tables, assets=None, *, clock=now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._explicit_profiles = any(
            getattr(row, "equip_profile", 0) > 0 for row in tables["Item"].rows()
        )
        self._craft_rules_cache = None
        self._forge_revisions: dict[int, int] = {}
        self._material_rewards: dict[int, _MaterialRewardState] = {}
        self._craft_receipts: dict[tuple[int, int], tuple[int, int]] = {}
        self._reward_receipts: dict[tuple[int, int], int] = {}

    def register(self) -> None:
        """向 SceneNode 注册 5 个装备打造与材料自选 RPC。"""
        self.scene.register(_SERVICE, "GetEquipmentCraftRules", self.get_rules)
        self.scene.register(_SERVICE, "PreviewEquipmentCraft", self.preview_craft, mutating=True)
        self.scene.register(_SERVICE, "CommitEquipmentCraft", self.commit_craft, mutating=True)
        self.scene.register(_SERVICE, "GetEquipmentMaterialReward", self.get_material_reward)
        self.scene.register(_SERVICE, "ClaimEquipmentMaterialReward", self.claim_material_reward, mutating=True)

    # ── 配置规则解析与展示补充 ──

    def _item_profile(self, row) -> int:
        explicit = getattr(row, "equip_profile", 0)
        if explicit:
            return explicit
        if self._explicit_profiles or row.max_stack_size != 1:
            return 0
        if 11 <= row.equip_kind <= 14:
            return 1
        if row.equip_kind == 15:
            return 2
        if row.equip_kind == 16:
            return 3
        return 0

    def _item_forge_material(self, row) -> tuple[int, int]:
        mat_id = getattr(row, "forge_material_id", 0) or CRAFTING_MATERIAL_ID
        mat_count = getattr(row, "forge_material_count", 0) or DEFAULT_FORGE_MATERIAL_COUNT
        return mat_id, mat_count

    def _enhancement_stone_id(self) -> int:
        if self.tables["Item"].get(ENHANCEMENT_STONE_ID) is not None:
            return ENHANCEMENT_STONE_ID
        if self.tables["Item"].get(LEGACY_ENHANCEMENT_STONE_ID) is not None:
            return LEGACY_ENHANCEMENT_STONE_ID
        return ENHANCEMENT_STONE_ID

    def _valid_material(self, config_id: int, count: int) -> bool:
        if not config_id or not count:
            return False
        row = self.tables["Item"].get(config_id)
        if row is not None:
            return row.max_stack_size != 0 and not row.equip_kind and not row.battle_usable
        # 若 Item 表尚未同步 3001/3002，允许标准打造材料 ID 通过规则校验。
        return config_id in (CRAFTING_MATERIAL_ID, ENHANCEMENT_STONE_ID)

    def _valid_template(self, row) -> bool:
        profile = self._item_profile(row)
        if not profile or row.max_stack_size != 1:
            return False
        mat_id, mat_count = self._item_forge_material(row)
        if not self._valid_material(mat_id, mat_count):
            return False
        if self._explicit_profiles and (row.affix_pool or row.affix_rule):
            return False
        artifact_id = getattr(row, "artifact_id", 0)
        if profile == 1:
            return 11 <= row.equip_kind <= 14 and artifact_id == 0
        if profile == 2:
            return row.equip_kind == 15 and artifact_id == 0
        if profile == 3:
            artifact = self.tables["EquipArtifact"].get(artifact_id) if "EquipArtifact" in self.tables.tables else None
            return row.equip_kind == 16 and artifact is not None and _valid_artifact_row(artifact)
        return False

    def _valid_selections(self, item, profile: int) -> bool:
        if not item.HasField("equip") or len(item.equip.fixed_affixes) > 2:
            return False
        slots, ids = set(), set()
        for selected in item.equip.fixed_affixes:
            affix = self.tables["EquipV1Affix"].get(selected.affix_id)
            if (
                affix is None
                or not valid_affix_row(affix)
                or pool_for(profile, selected.slot) != affix.pool
                or selected.slot in slots
                or selected.affix_id in ids
            ):
                return False
            slots.add(selected.slot)
            ids.add(selected.affix_id)
        return True

    def fill_v1_display(self, item, info) -> None:
        """为 BagItemInfo 补充 V1 固定词条槽位与法宝展示数据。"""
        row = self.tables["Item"].get(item.config_id)
        if row is None:
            return
        profile = self._item_profile(row)
        info.equip_profile = profile
        info.ClearField("fixed_affix_slots")
        info.ClearField("artifact")
        if profile in (1, 2):
            valid = self._valid_selections(item, profile)
            for slot in (1, 2):
                shown = info.fixed_affix_slots.add(slot=slot, pool=pool_for(profile, slot))
                if not valid:
                    continue
                for selected in item.equip.fixed_affixes:
                    if selected.slot == slot:
                        affix = self.tables["EquipV1Affix"].get(selected.affix_id)
                        if affix is not None:
                            _fill_affix_info(affix, shown.selected)
        elif profile == 3 and "EquipArtifact" in self.tables.tables:
            artifact = self.tables["EquipArtifact"].get(getattr(row, "artifact_id", 0))
            if artifact is not None and _valid_artifact_row(artifact):
                _fill_artifact_info(artifact, info.artifact)

    def _fill_product(self, item, info) -> None:
        info.Clear()
        row = self.tables["Item"].get(item.config_id)
        if row is None:
            return
        info.item_id = item.item_id
        info.config_id = item.config_id
        info.count = item.size
        _fill_item(self.tables, info, item.equip if item.HasField("equip") else None)
        self.fill_v1_display(item, info)

    def _build_rules(self):
        if self._craft_rules_cache is not None:
            return self._craft_rules_cache
        rules = self.protocol.message("GetEquipmentCraftRulesResponse")
        recipes = []
        for row in self.tables["Item"].rows():
            profile = self._item_profile(row)
            if not profile:
                continue
            if not self._valid_template(row):
                return INVALID_DATA, None
            recipes.append(row)
        recipes.sort(key=lambda row: row.id)
        for row in recipes:
            item = self.protocol.message("ItemComp", config_id=row.id, size=1)
            item.equip.SetInParent()
            recipe = rules.recipes.add()
            self._fill_product(item, recipe.product)
            mat_id, mat_count = self._item_forge_material(row)
            recipe.material_config_id = mat_id
            recipe.material_count = mat_count

        if "EquipV1Affix" not in self.tables.tables:
            return FEATURE_UNAVAILABLE, None
        affixes = []
        caps: dict[tuple[int, int], int] = {}
        for row in self.tables["EquipV1Affix"].rows():
            if not valid_affix_row(row) or not self._valid_material(row.material_id, row.material_count):
                return INVALID_DATA, None
            key = (row.effect, row.effect_param)
            if key in caps and caps[key] != row.source_cap:
                return INVALID_DATA, None
            caps[key] = row.source_cap
            affixes.append(row)
        if not recipes or not affixes:
            return FEATURE_UNAVAILABLE, None
        affixes.sort(key=lambda row: row.id)
        for row in affixes:
            _fill_affix_info(row, rules.affixes.add())

        if "EquipAffixQuality" in self.tables.tables:
            for row in sorted(self.tables["EquipAffixQuality"].rows(), key=lambda r: r.id):
                rules.quality_tiers.add(
                    quality_tier=row.id,
                    name=row.name,
                    ratio_pct=row.ratio_pct,
                    apprentice_rate_bps=row.apprentice_weight,
                    artisan_rate_bps=row.artisan_weight,
                    master_rate_bps=row.master_weight,
                )

        digest = hashlib.sha256()
        for table_name in ("Item", "EquipAttribute", "EquipEnhance", "EquipV1Affix", "EquipArtifact", "EquipSlot"):
            if table_name in self.tables.tables:
                for row in self.tables[table_name].rows():
                    digest.update(row.SerializeToString(deterministic=True))
        digest.update(rules.SerializeToString(deterministic=True))
        rules.rules_fingerprint = digest.digest()[:16]
        self._craft_rules_cache = (0, rules)
        return self._craft_rules_cache

    # ── 状态修订号与材料自选状态（优先读 proto 字段，未扩展或同步入账时协调内存态） ──

    def _get_forge_revision(self, player_id: int, data) -> int:
        ledger = data.client_request_ledger
        if hasattr(ledger, "forge_revision"):
            return ledger.forge_revision
        return self._forge_revisions.get(player_id, 0)

    def _set_forge_revision(self, player_id: int, data, revision: int) -> None:
        ledger = data.client_request_ledger
        if hasattr(ledger, "forge_revision"):
            ledger.forge_revision = revision
        else:
            self._forge_revisions[player_id] = revision

    def _online_player_data(self, player_id: int):
        for location in getattr(self.scene, "locations", {}).values():
            player = location.players.get(player_id)
            if player is not None:
                return self.scene.rows.assemble(player.snapshot)
        return None

    def _get_reward_state(self, player_id: int, data) -> _MaterialRewardState:
        ledger = data.client_request_ledger
        mem = self._material_rewards.get(player_id)
        if hasattr(ledger, "material_reward"):
            mr = ledger.material_reward
            if mem is not None and mem.revision >= mr.revision:
                return _MaterialRewardState(
                    day_starts_at_ms=mem.day_starts_at_ms,
                    earned_today=mem.earned_today,
                    available_units=mem.available_units,
                    revision=mem.revision,
                )
            return _MaterialRewardState(
                day_starts_at_ms=mr.day_starts_at_ms,
                earned_today=mr.earned_today,
                available_units=mr.available_units,
                revision=mr.revision,
            )
        if mem is None:
            mem = _MaterialRewardState()
            self._material_rewards[player_id] = mem
        return mem

    def _save_reward_state(
        self, player_id: int, data, state: _MaterialRewardState, *, persisted: bool = False,
    ) -> None:
        ledger = data.client_request_ledger
        if hasattr(ledger, "material_reward"):
            mr = ledger.material_reward
            mr.day_starts_at_ms = state.day_starts_at_ms
            mr.earned_today = state.earned_today
            mr.available_units = state.available_units
            mr.revision = state.revision
            if persisted:
                self._material_rewards.pop(player_id, None)
            elif player_id in self._material_rewards:
                self._material_rewards[player_id] = _MaterialRewardState(
                    day_starts_at_ms=state.day_starts_at_ms,
                    earned_today=state.earned_today,
                    available_units=state.available_units,
                    revision=state.revision,
                )
        else:
            self._material_rewards[player_id] = state

    def credit_battle_material_reward(
        self,
        player_id: int,
        *,
        now_ms_val: int | None = None,
        units: int = MATERIAL_REWARD_BATTLE_UNITS,
        eligible: bool = True,
        data=None,
    ) -> int:
        """向玩家入账权威战斗结算产出的材料自选额度，返回实际入账份数。"""
        if not eligible or units <= 0:
            return 0
        stamp = self.clock() if now_ms_val is None else now_ms_val
        day = resolve_game_day(stamp)
        if day is None:
            return 0
        start_ms, _ = day
        if data is not None:
            state = self._get_reward_state(player_id, data)
        else:
            state = self._material_rewards.get(player_id)
            if state is None:
                online = self._online_player_data(player_id)
                if online is not None and hasattr(online.client_request_ledger, "material_reward"):
                    mr = online.client_request_ledger.material_reward
                    state = _MaterialRewardState(
                        day_starts_at_ms=mr.day_starts_at_ms,
                        earned_today=mr.earned_today,
                        available_units=mr.available_units,
                        revision=mr.revision,
                    )
                else:
                    state = _MaterialRewardState()
                self._material_rewards[player_id] = state
        # 时钟回拨不重新开放旧日额度；向前换日仅重置当日计数，未领余额跨日保留。
        if start_ms < state.day_starts_at_ms:
            return 0
        earned = 0 if start_ms > state.day_starts_at_ms else state.earned_today
        if earned >= MATERIAL_REWARD_DAILY_LIMIT:
            return 0
        added = min(units, MATERIAL_REWARD_DAILY_LIMIT - earned)
        if state.available_units > _MAX_UINT64 - added:
            return 0
        state.day_starts_at_ms = start_ms
        state.earned_today = earned + added
        state.available_units += added
        if data is not None:
            self._save_reward_state(player_id, data, state)
        return added

    # ── 背包材料读/扣/发辅助（协作 scene.assets，兼容表未同步的 3001/3002） ──

    def _owned_material(self, data, config_id: int) -> int:
        total = 0
        for entry in data.bag_component.items:
            if entry.bag_type != 0:
                continue
            item = self.assets.item(entry)
            if item.config_id == config_id:
                total += item.size
        return total

    def _available_material(self, data, config_id: int) -> int:
        return self.assets.removable_count(data, config_id)

    def _consume_material(self, data, config_id: int, count: int) -> int:
        if self.tables["Item"].get(config_id) is not None:
            return self.assets.consume_items(data, {config_id: count})
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return INVALID_DATA
        held = held_item_count(data, config_id)
        known = sum(
            item.size for _, item in entries
            if item.config_id == config_id and self.assets.countable(item)
        )
        if known < count or (held and count > max(0, known - held)):
            return BAG_INSUFFICIENT
        taken, version_skew = self.assets._drain(entries, config_id, count)
        if taken != count:
            return FEATURE_UNAVAILABLE if version_skew else BAG_INSUFFICIENT
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0

    async def _grant_materials(self, data, counts: dict[int, int], *, gains: list) -> int:
        known = {cid: qty for cid, qty in counts.items() if qty > 0 and self.tables["Item"].get(cid) is not None}
        synthetic = {cid: qty for cid, qty in counts.items() if qty > 0 and cid not in known}
        staged = clone(data)
        if known:
            err = await self.assets.add_items(staged, known, gains=gains)
            if err:
                return err
        if synthetic:
            for cid in synthetic:
                if self.assets.item_gain_blocked(staged, cid):
                    return BLOCKED
            bag = clone(staged.bag_component)
            entries = self.assets.entries(bag, 0)
            cap = bag_capacity(bag, 0)
            if not self.assets._usable(entries, cap):
                return INVALID_DATA
            occupied = {entry.pos for entry, _ in entries}
            free_slots = (slot for slot in range(cap) if slot not in occupied)
            plan = []
            for cid, amount in sorted(synthetic.items()):
                remaining = amount
                for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
                    if item.config_id == cid and self.assets._mergeable(item, 999):
                        added = min(remaining, 999 - item.size)
                        if added:
                            item.size += added
                            remaining -= added
                            self.assets.write_entry(entry, item)
                while remaining:
                    slot = next(free_slots, None)
                    if slot is None:
                        return BAG_FULL
                    stack = min(remaining, 999)
                    plan.append((slot, cid, stack))
                    remaining -= stack
            if plan and self.assets.allocate_item_id is None:
                return UNAVAILABLE
            ids = self.assets.all_ids(bag)
            watermark = max((item.acquire_seq for _, item in entries), default=0)
            for slot, cid, stack in plan:
                try:
                    guid = await self.assets.allocate_item_id()
                except (OSError, RuntimeError, ValueError):
                    return UNAVAILABLE
                if type(guid) is not int or not 1 <= guid <= _MAX_UINT64 or guid in ids:
                    return UNAVAILABLE
                ids.add(guid)
                watermark += 1
                item = self.protocol.message("ItemComp", item_id=guid, config_id=cid, size=stack, acquire_seq=watermark)
                self.assets.write_entry(bag.items.add(pos=slot, bag_type=0), item)
            self.assets._order(bag.items)
            staged.bag_component.CopyFrom(bag)
        data.bag_component.CopyFrom(staged.bag_component)
        return 0

    def _fill_material_display(self, data, config_id: int, count: int, out) -> None:
        out.config_id = config_id
        out.count = count
        row = self.tables["Item"].get(config_id)
        if row is not None:
            out.name = row.name
            out.icon_key = row.icon_key
        elif config_id == CRAFTING_MATERIAL_ID:
            out.name = "打造材料"
            out.icon_key = "UI/Items/3001"
        elif config_id == ENHANCEMENT_STONE_ID:
            out.name = "强化石"
            out.icon_key = "UI/Items/3002"
        out.owned = self._owned_material(data, config_id)
        out.available = self._available_material(data, config_id)

    def _writable_error(self, player) -> int:
        if not self.scene.mutable(player):
            return EQUIP_FROZEN
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return EQUIP_IN_BATTLE
        return 0

    def _enrich_bag_snapshot(self, data, bag_type: int, bag_info) -> None:
        """为 BagInfo 中的装备项补充 V1 fixed_affix_slots 与 artifact 展示信息。"""
        by_id = {item.item_id: item for _, item in self.assets.entries(data.bag_component, bag_type)}
        for info in bag_info.items:
            stored = by_id.get(info.item_id)
            if stored is not None:
                self.fill_v1_display(stored, info)

    # ── 打造试算与校验 ──

    def _prepare_choice(self, data, choice):
        if carries_unknown(choice):
            return FEATURE_UNAVAILABLE, None
        if choice.operation == _OP_CREATE_ITEM:
            if choice.item_id or choice.affix_slot or choice.affix_id:
                return INVALID, None
            row = self.tables["Item"].get(choice.config_id)
            if row is None or not self._valid_template(row):
                return INVALID, None
            result = self.protocol.message("ItemComp", config_id=row.id, size=1)
            result.equip.SetInParent()
            mat_id, mat_count = self._item_forge_material(row)
            return 0, {
                "result": result,
                "original": None,
                "holder": 0,
                "material_id": mat_id,
                "material_count": mat_count,
            }
        if choice.operation != _OP_INSTALL_AFFIX or choice.config_id or not choice.item_id or not choice.affix_id:
            return INVALID, None
        found_item = None
        holder = 0
        for bag_type in (0, 2):
            for _, item in self.assets.entries(data.bag_component, bag_type):
                if item.item_id == choice.item_id:
                    found_item = item
                    holder = bag_type
                    break
            if found_item is not None:
                break
        if found_item is None:
            return EQUIP_ITEM_NOT_FOUND, None
        item_row = self.tables["Item"].get(found_item.config_id)
        affix = self.tables["EquipV1Affix"].get(choice.affix_id)
        if item_row is None or affix is None or not valid_affix_row(affix):
            return INVALID, None
        profile = self._item_profile(item_row)
        if (
            pool_for(profile, choice.affix_slot) != affix.pool
            or found_item.size != 1
            or not self._valid_selections(found_item, profile)
        ):
            return INVALID, None
        if carries_unknown(found_item) or carries_unknown(found_item.equip):
            return FEATURE_UNAVAILABLE, None
        for selected in found_item.equip.fixed_affixes:
            if carries_unknown(selected):
                return FEATURE_UNAVAILABLE, None
            if selected.slot == choice.affix_slot or selected.affix_id == choice.affix_id:
                return INVALID, None
        result = clone(found_item)
        result.equip.fixed_affixes.add(slot=choice.affix_slot, affix_id=choice.affix_id)
        return 0, {
            "result": result,
            "original": found_item,
            "holder": holder,
            "material_id": affix.material_id,
            "material_count": affix.material_count,
        }

    def _check_availability(self, player, data, prepared) -> int:
        if writable_err := self._writable_error(player):
            return writable_err
        if self._get_forge_revision(player.player_id, data) == _MAX_UINT64:
            return UNAVAILABLE
        if not data.HasField("level_component"):
            return UNAVAILABLE
        row = self.tables["Item"].get(prepared["result"].config_id)
        if row is None:
            return UNAVAILABLE
        if effective_level(data) < row.equip_level:
            return EQUIP_LEVEL_NOT_ENOUGH
        available = self._available_material(data, prepared["material_id"])
        if available < prepared["material_count"]:
            return EQUIP_MATERIAL_NOT_ENOUGH
        if prepared["original"] is None:
            # 预检扣料后主背包是否至少有 1 个空位容纳新打造装备。
            staged = clone(data)
            err = self._consume_material(staged, prepared["material_id"], prepared["material_count"])
            if err == BAG_INSUFFICIENT:
                return EQUIP_MATERIAL_NOT_ENOUGH
            if err:
                return err
            entries = self.assets.entries(staged.bag_component, 0)
            if len(entries) >= bag_capacity(staged.bag_component, 0):
                return BAG_FULL
        return 0

    # ── 5 个客户端 RPC 处理入口 ──

    def get_rules(self, _world, player, _route, _request):
        """查询权威打造配方、固定词条池、规则指纹与当前打造修订号。"""
        response = self.protocol.message("GetEquipmentCraftRulesResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        err, cached = self._build_rules()
        if err:
            response.error_message.id = err
            return response
        data = self.scene.rows.assemble(player.snapshot)
        response.CopyFrom(cached)
        response.forge_revision = self._get_forge_revision(player.player_id, data)
        return response

    def preview_craft(self, _world, player, _route, request):
        """试算装备打造或词条安装结果与所需材料，不落盘不扣料。"""
        response = self.protocol.message("PreviewEquipmentCraftResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        if not request.HasField("choice") or carries_unknown(request):
            response.error_message.id = INVALID
            return response
        err, cached = self._build_rules()
        if err:
            response.error_message.id = err
            return response
        data = self.scene.rows.assemble(player.snapshot)
        err, prepared = self._prepare_choice(data, request.choice)
        if err:
            response.error_message.id = err
            return response
        preview = response.preview
        preview.choice.CopyFrom(request.choice)
        preview.rules_fingerprint = cached.rules_fingerprint
        preview.expected_forge_revision = self._get_forge_revision(player.player_id, data)
        self._fill_product(prepared["result"], preview.result)
        self._fill_material_display(data, prepared["material_id"], prepared["material_count"], preview.material)
        unavailable = self._check_availability(player, data, prepared)
        preview.available = unavailable == 0
        if unavailable:
            preview.unavailable_tip = unavailable
        return response

    def _remember_craft(self, player_id: int, request_id: int, item_id: int, forge_revision: int) -> None:
        if len(self._craft_receipts) >= _MAX_CACHED_RECEIPTS:
            oldest = next(iter(self._craft_receipts))
            del self._craft_receipts[oldest]
        self._craft_receipts[(player_id, request_id)] = (item_id, forge_revision)

    def _fill_craft_applied(self, player, data, response, *, item_id: int, result_rev: int, replayed: bool):
        response.outcome = _OUTCOME_APPLIED
        response.replayed = replayed
        response.durable = True
        response.item_id = item_id
        response.result_forge_revision = result_rev
        response.current_forge_revision = self._get_forge_revision(player.player_id, data)
        inv = _bag(self.scene, self.tables, player, 0)
        eq = _bag(self.scene, self.tables, player, 2)
        if inv.error_message.id or eq.error_message.id:
            response.error_message.id = inv.error_message.id or eq.error_message.id
        else:
            response.bag.CopyFrom(inv.bag)
            response.equipment.CopyFrom(eq.bag)
            self._enrich_bag_snapshot(data, 0, response.bag)
            self._enrich_bag_snapshot(data, 2, response.equipment)
        return response

    async def commit_craft(self, _world, player, _route, request):
        """原子提交装备打造或首次固定词条安装，带水位乐观锁与 client_request_id 幂等。"""
        response = self.protocol.message(
            "CommitEquipmentCraftResponse", client_request_id=request.client_request_id,
        )

        def reject(tip: int, *, known: bool = True):
            response.outcome = _OUTCOME_REJECTED if known else _OUTCOME_UNKNOWN
            response.error_message.id = tip
            return response

        if self.assets is None:
            return reject(UNAVAILABLE, known=False)
        data = self.scene.rows.assemble(player.snapshot)
        current_rev = self._get_forge_revision(player.player_id, data)
        response.current_forge_revision = current_rev
        if not request.client_request_id or not request.HasField("choice") or carries_unknown(request):
            return reject(INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_CRAFT)
        if err:
            return reject(err, known=err == INVALID)
        if prior is not None:
            cached = self._craft_receipts.get((player.player_id, request.client_request_id))
            item_id = (
                getattr(prior, "equipment_item_id", 0)
                or (cached[0] if cached is not None else request.choice.item_id)
            )
            result_rev = (
                getattr(prior, "forge_revision", 0)
                or (cached[1] if cached is not None else current_rev)
            )
            return self._fill_craft_applied(
                player, data, response, item_id=item_id, result_rev=result_rev, replayed=True,
            )

        if request.expected_forge_revision != current_rev:
            return reject(EQUIP_LEVEL_CHANGED, known=False)
        err, cached = self._build_rules()
        if err:
            return reject(err)
        if request.rules_fingerprint != cached.rules_fingerprint:
            return reject(INVALID)

        err, prepared = self._prepare_choice(data, request.choice)
        if err:
            return reject(err)
        err = self._check_availability(player, data, prepared)
        if err:
            return reject(err)

        staged = clone(data)
        consume_err = self._consume_material(staged, prepared["material_id"], prepared["material_count"])
        if consume_err == BAG_INSUFFICIENT:
            return reject(EQUIP_MATERIAL_NOT_ENOUGH)
        if consume_err:
            return reject(consume_err)

        if request.choice.operation == _OP_CREATE_ITEM:
            if self.assets.allocate_item_id is None:
                return reject(UNAVAILABLE)
            try:
                created_guid = await self.assets.allocate_item_id()
            except (OSError, RuntimeError, ValueError):
                return reject(UNAVAILABLE)
            if type(created_guid) is not int or not 1 <= created_guid <= _MAX_UINT64:
                return reject(EQUIP_INTERNAL_ERROR)
            new_item = clone(prepared["result"])
            new_item.item_id = created_guid
            add_err = await self.assets.add_items(staged, {}, instances=(new_item,))
            if add_err:
                return reject(add_err)
            target_item_id = created_guid
        else:
            replaced = False
            for entry, stored in self.assets.entries(staged.bag_component, prepared["holder"]):
                if stored.item_id == request.choice.item_id:
                    self.assets.write_entry(entry, prepared["result"])
                    replaced = True
                    break
            if not replaced:
                return reject(EQUIP_ITEM_NOT_FOUND)
            target_item_id = request.choice.item_id

        next_rev = current_rev + 1
        if hasattr(staged.client_request_ledger, "forge_revision"):
            staged.client_request_ledger.forge_revision = next_rev
        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_CRAFT,
            fingerprint,
            equipment_item_id=target_item_id,
            forge_revision=next_rev,
        )
        # 若修改的是已穿戴装备，commit 成功后通知属性重算。
        saved = await self.assets._save(
            player, staged, 27, correlation=request.client_request_id,
            item_extra={"operation": "equipment_craft", "item_id": target_item_id},
        )
        if not saved:
            return reject(UNAVAILABLE, known=False)
        self._set_forge_revision(player.player_id, staged, next_rev)
        if request.choice.operation == _OP_INSTALL_AFFIX and prepared["holder"] == 2:
            attributes = getattr(self.scene, "attributes", None)
            if attributes is not None:
                attributes.notify(player)
        self._remember_craft(player.player_id, request.client_request_id, target_item_id, next_rev)
        logger.info(
            "Equipment craft committed player_id=%s op=%s item_id=%s revision=%s",
            player.player_id, request.choice.operation, target_item_id, next_rev,
        )
        return self._fill_craft_applied(
            player, staged, response, item_id=target_item_id, result_rev=next_rev, replayed=False,
        )

    def _fill_material_reward_snapshot(self, player, data, stamp: int, out) -> int:
        out.Clear()
        day = resolve_game_day(stamp)
        if day is None:
            return INVALID
        start_ms, next_reset_ms = day
        state = self._get_reward_state(player.player_id, data)
        out.available = True
        out.day_starts_at_ms = start_ms
        out.next_reset_at_ms = next_reset_ms
        out.earned_today = state.earned_today if state.day_starts_at_ms == start_ms else 0
        out.daily_limit = MATERIAL_REWARD_DAILY_LIMIT
        out.available_units = state.available_units
        out.batch_units = min(MATERIAL_REWARD_DAILY_LIMIT, state.available_units)
        out.revision = state.revision
        self._fill_material_display(data, CRAFTING_MATERIAL_ID, 0, out.crafting_material)
        self._fill_material_display(data, self._enhancement_stone_id(), 0, out.enhancement_stone)
        writable = self._writable_error(player)
        out.can_claim = out.batch_units > 0 and writable == 0 and state.revision < _MAX_UINT64
        if out.batch_units == 0:
            out.unavailable_reason = "完成主要战斗可获得材料自选份数，每日最多20份。"
        elif writable == EQUIP_IN_BATTLE:
            out.unavailable_reason = "战斗中暂不能领取，请在结算完成后重试。"
        elif writable or not out.can_claim:
            out.unavailable_reason = "当前状态暂不能领取，请稍后重试。"
        return 0

    def get_material_reward(self, _world, player, _route, _request):
        """查询当前材料自选余额、当日获取进度与可领批次快照。"""
        response = self.protocol.message("GetEquipmentMaterialRewardResponse")
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response
        data = self.scene.rows.assemble(player.snapshot)
        err = self._fill_material_reward_snapshot(player, data, self.clock(), response.snapshot)
        if err:
            response.ClearField("snapshot")
            response.error_message.id = err
        return response

    def _remember_reward(self, player_id: int, request_id: int, result_rev: int) -> None:
        if len(self._reward_receipts) >= _MAX_CACHED_RECEIPTS:
            oldest = next(iter(self._reward_receipts))
            del self._reward_receipts[oldest]
        self._reward_receipts[(player_id, request_id)] = result_rev

    def _fill_reward_applied(self, player, data, stamp: int, response, *, result_rev: int, replayed: bool):
        response.outcome = _OUTCOME_APPLIED
        response.replayed = replayed
        response.durable = True
        response.result_revision = result_rev
        response.current_revision = self._get_reward_state(player.player_id, data).revision
        snap_err = self._fill_material_reward_snapshot(player, data, stamp, response.snapshot)
        inv = _bag(self.scene, self.tables, player, 0) if not snap_err else None
        if snap_err or inv.error_message.id:
            response.ClearField("bag")
            response.ClearField("snapshot")
            response.error_message.id = snap_err or inv.error_message.id
        else:
            response.bag.CopyFrom(inv.bag)
            self._enrich_bag_snapshot(data, 0, response.bag)
        return response

    async def claim_material_reward(self, _world, player, _route, request):
        """按批次自选比例领取打造材料与强化石，带水位防重与 client_request_id 幂等。"""
        response = self.protocol.message(
            "ClaimEquipmentMaterialRewardResponse", client_request_id=request.client_request_id,
        )
        stamp = self.clock()

        def reject(tip: int, *, known: bool = True):
            response.outcome = _OUTCOME_REJECTED if known else _OUTCOME_UNKNOWN
            response.error_message.id = tip
            return response

        if self.assets is None:
            return reject(UNAVAILABLE, known=False)
        data = self.scene.rows.assemble(player.snapshot)
        state = self._get_reward_state(player.player_id, data)
        response.current_revision = state.revision
        if not request.client_request_id or carries_unknown(request):
            return reject(INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_MATERIAL_REWARD)
        if err:
            return reject(err, known=err == INVALID)
        if prior is not None:
            result_rev = (
                getattr(prior, "material_reward_revision", 0)
                or self._reward_receipts.get((player.player_id, request.client_request_id), state.revision)
            )
            return self._fill_reward_applied(player, data, stamp, response, result_rev=result_rev, replayed=True)

        if request.expected_revision != state.revision:
            return reject(FEATURE_UNAVAILABLE, known=False)
        snap_err = self._fill_material_reward_snapshot(player, data, stamp, response.snapshot)
        if snap_err:
            return reject(snap_err)
        total = request.crafting_material_count + request.enhancement_stone_count
        if total == 0 or total != response.snapshot.batch_units:
            return reject(INVALID)
        if writable_err := self._writable_error(player):
            return reject(writable_err)
        if state.revision == _MAX_UINT64:
            return reject(FEATURE_UNAVAILABLE)

        staged = clone(data)
        gains = []
        grants = {}
        if request.crafting_material_count:
            grants[CRAFTING_MATERIAL_ID] = request.crafting_material_count
        if request.enhancement_stone_count:
            stone_id = self._enhancement_stone_id()
            grants[stone_id] = grants.get(stone_id, 0) + request.enhancement_stone_count
        grant_err = await self._grant_materials(staged, grants, gains=gains)
        if grant_err:
            return reject(grant_err)

        next_state = _MaterialRewardState(
            day_starts_at_ms=state.day_starts_at_ms,
            earned_today=state.earned_today,
            available_units=state.available_units - total,
            revision=state.revision + 1,
        )
        if hasattr(staged.client_request_ledger, "material_reward"):
            self._save_reward_state(player.player_id, staged, next_state)
        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_MATERIAL_REWARD,
            fingerprint,
            material_reward_revision=next_state.revision,
            crafting_material_count=request.crafting_material_count,
            enhancement_stone_count=request.enhancement_stone_count,
        )
        saved = await self.assets._save(
            player, staged, 8, correlation=request.client_request_id, gains=gains,
            item_extra={"operation": "equipment_material_reward"},
        )
        if not saved:
            return reject(UNAVAILABLE, known=False)
        self._save_reward_state(player.player_id, staged, next_state, persisted=True)
        self._remember_reward(player.player_id, request.client_request_id, next_state.revision)
        logger.info(
            "Equipment material reward claimed player_id=%s craft_mat=%s stone=%s revision=%s",
            player.player_id, request.crafting_material_count, request.enhancement_stone_count, next_state.revision,
        )
        return self._fill_reward_applied(player, staged, stamp, response, result_rev=next_state.revision, replayed=False)
