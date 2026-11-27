"""装备 V1 打造、固定词条安装与每日材料自选领取系统，协作 scene.assets 提交存档。"""

from __future__ import annotations

import hashlib
import logging
import random
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    BAG_FULL, BAG_INSUFFICIENT, BLOCKED, FEATURE_UNAVAILABLE, INVALID,
    INVALID_DATA, UNAVAILABLE, bag_capacity, carries_unknown, clone, held_item_count,
)
from .scene_mission import effective_level, now_ms
from .scene_player import _bag, _fill_item
from .scene_season import valid_artifact_definition
from .scene_skill_craft import (
    MASTERY_APPRENTICE,
    MASTERY_ARTISAN,
    MASTERY_MASTER,
    forge_top_quality_rate_bps,
    get_proxy_stall_directory,
    mastery_next_exp,
    mastery_tier_for_exp,
    mastery_tier_name,
    proxy_net_gold,
    record_proxy_order_reputation,
    valid_proxy_fee,
)

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
EQUIP_AFFIX_ALREADY_MAX_QUALITY = 28023
EQUIP_AFFIX_POLISH_EXEMPT = 28024
EQUIP_AFFIX_SLOT_EMPTY = 28025
EQUIP_AFFIX_REFORGE_SAME = 28026
EQUIP_BIND_CONFIRM_REQUIRED = 28027
EQUIP_POLISH_PITY_CHANGED = 28028
EQUIP_CRAFT_GOLD_NOT_ENOUGH = 28029
EQUIP_PROXY_MASTER_UNAVAILABLE = 28030

# 材料自选与游戏日/周契约（UTC+8 05:00 换日，UTC+8 周一 05:00 换周，与 C++ PlayerEquipmentMaterialRewardSystem 一致）。
DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
DAY_SHIFT_MS = 10_800_000
FIRST_MONDAY_05_UTC8_MS = 3 * DAY_MS + 21 * 3_600_000
MATERIAL_REWARD_DAILY_LIMIT = 20
MATERIAL_REWARD_MIN_STEP_UNITS = 5
MATERIAL_REWARD_BATTLE_UNITS = 10
MATERIAL_REWARD_BONUS_UNITS = 5
MATERIAL_REWARD_LIFETIME_LIMIT = 660
MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT = 60
MATERIAL_REWARD_WEEKLY_LIMIT = 150
MATERIAL_REWARD_WEEKLY_ROLLOVER_MAX_WEEKS = 4
MATERIAL_REWARD_WEEKLY_ROLLOVER_MAX_POOL = (
    MATERIAL_REWARD_WEEKLY_LIMIT * MATERIAL_REWARD_WEEKLY_ROLLOVER_MAX_WEEKS
)
CRAFTING_MATERIAL_ID = 3001
ENHANCEMENT_STONE_ID = 3002
LEGACY_ENHANCEMENT_STONE_ID = 2001
POLISH_SAND_ID = 3403
BOUND_ARTIFACT_VOUCHER_ID = 3404
GUARANTEE_CHARM_ID = 3405
UNSEAL_TALISMAN_ID = 3409
INHERIT_TALISMAN_ID = 3410
DEFAULT_FORGE_MATERIAL_COUNT = 10

POLISH_GOLD_COST = 20
REFORGE_GOLD_COST = 20
UPGRADE_TIER_BASE_GOLD_PER_STEP = 100
UPGRADE_TIER_ACCESSORY_GOLD_PER_STEP = 60
MIN_QUALITY_TIER = 1
MAX_QUALITY_TIER = 5
POLISH_PITY_LIMIT = 8
MAX_BATCH_POLISH_ATTEMPTS = POLISH_PITY_LIMIT
SPEED_AFFIX_EFFECT = 4

_OUTCOME_UNKNOWN = 0
_OUTCOME_REJECTED = 1
_OUTCOME_APPLIED = 2

_OP_CREATE_ITEM = 1
_OP_INSTALL_AFFIX = 2
_OP_REFORGE_AFFIX = 3
_OP_POLISH_AFFIX = 4
_OP_UNSEAL_TO_TRADABLE = 5
_OP_INHERIT_ENHANCE = 6
_OP_UPGRADE_TIER = 7

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
    lifetime_earned: int = 0
    tradable_earned_total: int = 0
    available_tradable_units: int = 0
    week_starts_at_ms: int = 0
    earned_this_week: int = 0
    migrated_to_v2: bool = False
    stage1_tradable_confirmed: int = 0
    stage2_pool_cap: int = 0


def resolve_stage2_week_state(state: _MaterialRewardState, week_start: int) -> tuple[int, int, bool]:
    """解析阶段二滚存蓄水池状态 -> (earned, pool_cap, clock_rollback)，4 周上限 600，抗时钟回拨。"""
    if state.week_starts_at_ms == 0:
        return 0, MATERIAL_REWARD_WEEKLY_LIMIT, False
    prev_cap = (
        max(MATERIAL_REWARD_WEEKLY_LIMIT, min(MATERIAL_REWARD_WEEKLY_ROLLOVER_MAX_POOL, state.stage2_pool_cap))
        if state.stage2_pool_cap > 0
        else MATERIAL_REWARD_WEEKLY_LIMIT
    )
    if week_start < state.week_starts_at_ms:
        return state.earned_this_week, prev_cap, True
    if week_start == state.week_starts_at_ms:
        return state.earned_this_week, prev_cap, False
    elapsed_weeks = max(1, (week_start - state.week_starts_at_ms) // WEEK_MS)
    carry = max(0, prev_cap - state.earned_this_week)
    added_quota = min(elapsed_weeks, MATERIAL_REWARD_WEEKLY_ROLLOVER_MAX_WEEKS) * MATERIAL_REWARD_WEEKLY_LIMIT
    pool_cap = min(MATERIAL_REWARD_WEEKLY_ROLLOVER_MAX_POOL, max(MATERIAL_REWARD_WEEKLY_LIMIT, carry + added_quota))
    return 0, pool_cap, False


def resolve_game_day(now_ms_val: int) -> tuple[int, int] | None:
    """按 UTC+8 05:00 计算当前游戏日 [start_ms, next_reset_ms)。"""
    offset = (now_ms_val % DAY_MS + DAY_SHIFT_MS) % DAY_MS
    if now_ms_val < offset:
        return None
    start = now_ms_val - offset
    if start > _MAX_UINT64 - DAY_MS:
        return None
    return start, start + DAY_MS


def resolve_game_week(now_ms_val: int) -> tuple[int, int] | None:
    """按 UTC+8 周一 05:00 计算当前游戏周 [start_ms, next_reset_ms)。"""
    if now_ms_val < FIRST_MONDAY_05_UTC8_MS:
        return None
    elapsed = now_ms_val - FIRST_MONDAY_05_UTC8_MS
    start = FIRST_MONDAY_05_UTC8_MS + (elapsed // WEEK_MS) * WEEK_MS
    if start > _MAX_UINT64 - WEEK_MS:
        return None
    return start, start + WEEK_MS


def maximum_source_bps(effect: int) -> int:
    """返回装备同类固定词条全身上限（未强化表上限，万分比）。"""
    return _MAX_SOURCE_BPS.get(effect, 0)


# 控辅 V1 基础装固定词条强化克制份额（与 C++ equip_attribute_rules.h / Java EquipmentRules 逐比特一致）。
V1_SUPPORT_ENHANCE_SHARE_PERMYRIAD = 5000  # 50%：施放治疗(5)、护盾容量(6)
V1_CONTROL_ENHANCE_SHARE_PERMYRIAD = 2500  # 25%：控场命中(11)
V1_TOP_ENHANCE_BONUS_PERMYRIAD = 2400      # +11 顶级强化增幅 24%


def v1_fixed_affix_enhance_share_permyriad(equip_profile: int, effect: int) -> int:
    if equip_profile != 1:
        return 0
    if effect in (5, 6):
        return V1_SUPPORT_ENHANCE_SHARE_PERMYRIAD
    if effect == 11:
        return V1_CONTROL_ENHANCE_SHARE_PERMYRIAD
    return 0


def v1_fixed_affix_enhance_bonus_permyriad(
    equip_profile: int, effect: int, item_enhance_bonus_permyriad: int,
) -> int:
    share = v1_fixed_affix_enhance_share_permyriad(equip_profile, effect)
    if not share or item_enhance_bonus_permyriad <= 0:
        return 0
    return (int(item_enhance_bonus_permyriad) * share) // 10_000


def scale_v1_fixed_affix_by_enhance(
    equip_profile: int, effect: int, value_bps: int, item_enhance_bonus_permyriad: int,
) -> int:
    affix_bonus = v1_fixed_affix_enhance_bonus_permyriad(
        equip_profile, effect, item_enhance_bonus_permyriad,
    )
    if affix_bonus <= 0 or value_bps <= 0:
        return int(value_bps)
    val = int(value_bps)
    return val + (val * affix_bonus) // 10_000


def max_v1_fixed_affix_effective_cap(effect: int, base_cap_bps: int) -> int:
    return scale_v1_fixed_affix_by_enhance(1, effect, base_cap_bps, V1_TOP_ENHANCE_BONUS_PERMYRIAD)


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


def is_polish_exempt(effect: int) -> bool:
    """速度词条（G02, effect=4）豁免品相随机，固定满值。"""
    return effect == SPEED_AFFIX_EFFECT


def effective_quality_tier(stored_tier: int, exempt: bool) -> int:
    """兼容旧存档 quality_tier==0 与豁免词条均视作 5 档极品。"""
    if exempt or stored_tier == 0:
        return MAX_QUALITY_TIER
    return max(MIN_QUALITY_TIER, min(MAX_QUALITY_TIER, stored_tier))


def default_quality_ratio_pct(tier: int) -> int:
    return {1: 80, 2: 85, 3: 90, 4: 95}.get(tier, 100)


def default_quality_name(tier: int) -> str:
    return {1: "凡品", 2: "良品", 3: "上品", 4: "珍品"}.get(tier, "极品")


def scale_value_by_quality(max_value_bps: int, ratio_pct: int) -> int:
    clamped = max(1, min(100, int(ratio_pct)))
    return (int(max_value_bps) * clamped) // 100


def roll_quality_tier(weights_bps: tuple[int, ...], roll_bps: int) -> int:
    acc = 0
    for idx, weight in enumerate(weights_bps):
        acc += weight
        if roll_bps < acc:
            return idx + 1
    return MAX_QUALITY_TIER


def resolve_polish(current_tier: int, current_pity: int, rolled_tier: int, use_guarantee_charm: bool) -> tuple[int, int, bool]:
    """只升不降 + 累计第 8 次必达极品（或定星保底符直接必达极品），返回 (new_tier, new_pity, guaranteed)。"""
    base_tier = max(MIN_QUALITY_TIER, min(MAX_QUALITY_TIER, current_tier))
    rolled = max(MIN_QUALITY_TIER, min(MAX_QUALITY_TIER, rolled_tier))
    if use_guarantee_charm or current_pity + 1 >= POLISH_PITY_LIMIT:
        return MAX_QUALITY_TIER, 0, True
    next_tier = max(base_tier, rolled)
    if next_tier >= MAX_QUALITY_TIER:
        return MAX_QUALITY_TIER, 0, False
    return next_tier, current_pity + 1, False


def can_upgrade_tier(
    source_profile: int, source_kind: int, source_level: int, source_class: int,
    target_profile: int, target_kind: int, target_level: int, target_class: int,
) -> bool:
    """V1 装备原地无损升档合法性校验（仅 profile 1/2，同部位同职业，目标等级高于源等级）。"""
    if source_profile not in (1, 2):
        return False
    if target_profile != source_profile or target_kind != source_kind or target_class != source_class:
        return False
    return target_level > source_level


def upgrade_tier_gold_cost(equip_profile: int, source_level: int, target_level: int) -> int:
    """V1 装备原地无损升档银两消耗：每跨 10 级一档，基础装 100 银/档，首饰 60 银/档。"""
    if equip_profile not in (1, 2) or target_level <= source_level:
        return 0
    src_tier = source_level // 10
    dst_tier = target_level // 10
    steps = (dst_tier - src_tier) if dst_tier > src_tier else 1
    per_step = UPGRADE_TIER_BASE_GOLD_PER_STEP if equip_profile == 1 else UPGRADE_TIER_ACCESSORY_GOLD_PER_STEP
    return steps * per_step


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

    def __init__(self, scene, tables, assets=None, *, clock=now_ms, roll_fn=None) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._roll_fn = roll_fn
        self._explicit_profiles = any(
            getattr(row, "equip_profile", 0) > 0 for row in tables["Item"].rows()
        )
        self._craft_rules_cache = None
        self._forge_revisions: dict[int, int] = {}
        self._material_rewards: dict[int, _MaterialRewardState] = {}
        self._craft_receipts: dict[tuple[int, int], tuple[int, int]] = {}
        self._reward_receipts: dict[tuple[int, int], int] = {}
        self._account_stage1_used: dict[int, int] = {}
        self._account_quota_transport = None
        self.scene.equip_craft = self

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

    def _quality_row(self, tier: int):
        if "EquipAffixQuality" not in self.tables.tables:
            return None
        return self.tables["EquipAffixQuality"].get(tier)

    def _quality_ratio_pct(self, tier: int) -> int:
        row = self._quality_row(tier)
        if row is not None and row.ratio_pct:
            return int(row.ratio_pct)
        return default_quality_ratio_pct(tier)

    def _quality_name(self, tier: int) -> str:
        row = self._quality_row(tier)
        if row is not None and row.name:
            return row.name
        return default_quality_name(tier)

    def _roll_quality_for_mastery(self, mastery_tier: int) -> int:
        if self._roll_fn is not None:
            return max(MIN_QUALITY_TIER, min(MAX_QUALITY_TIER, int(self._roll_fn(mastery_tier))))
        weights = []
        for tier in range(MIN_QUALITY_TIER, MAX_QUALITY_TIER + 1):
            row = self._quality_row(tier)
            if row is not None:
                if mastery_tier >= MASTERY_MASTER:
                    weights.append(int(row.master_weight))
                elif mastery_tier >= MASTERY_ARTISAN:
                    weights.append(int(row.artisan_weight))
                else:
                    weights.append(int(row.apprentice_weight))
        if len(weights) == MAX_QUALITY_TIER and sum(weights) > 0:
            total = sum(weights)
            return roll_quality_tier(tuple(weights), random.randrange(total))
        return MAX_QUALITY_TIER

    def _v1_item_enhance_bonus(self, item, row) -> int:
        if (
            self._item_profile(row) != 1
            or not item.HasField("equip")
            or not item.equip.enhance_level
        ):
            return 0
        from .scene_player import _enhance_row_for_profile, _max_enhance_level_for_profile
        max_lvl = _max_enhance_level_for_profile(self.tables, 1)
        if not max_lvl:
            return 0
        lvl = min(int(item.equip.enhance_level), max_lvl)
        cur = _enhance_row_for_profile(self.tables, 1, lvl)
        if cur is None or cur.bonus_permyriad > V1_TOP_ENHANCE_BONUS_PERMYRIAD:
            return 0
        return int(cur.bonus_permyriad)

    def _fill_affix_instance(
        self, affix_row, selected, out, *, equip_profile: int = 0, enhance_bonus: int = 0,
    ) -> None:
        _fill_affix_info(affix_row, out)
        exempt = is_polish_exempt(affix_row.effect)
        tier = effective_quality_tier(int(selected.quality_tier), exempt)
        ratio = self._quality_ratio_pct(tier)
        quality_scaled = scale_value_by_quality(affix_row.value_permyriad, ratio)
        effective_val = scale_v1_fixed_affix_by_enhance(
            equip_profile, affix_row.effect, quality_scaled, enhance_bonus,
        )
        out.max_value_bps = int(affix_row.value_permyriad)
        out.value_bps = effective_val
        if enhance_bonus > 0 and v1_fixed_affix_enhance_share_permyriad(equip_profile, affix_row.effect) > 0:
            out.source_cap_bps = scale_v1_fixed_affix_by_enhance(
                equip_profile, affix_row.effect, int(affix_row.source_cap), enhance_bonus,
            )
        out.quality_tier = tier
        out.quality_name = self._quality_name(tier)
        out.quality_ratio_pct = ratio
        out.polish_pity_count = 0 if (exempt or tier >= MAX_QUALITY_TIER) else int(selected.polish_pity_count)
        out.polish_pity_limit = 0 if exempt else POLISH_PITY_LIMIT
        out.polish_exempt = exempt

    def _fill_forge_mastery(self, data, out) -> None:
        out.Clear()
        exp = int(data.craft_component.forge_mastery_exp) if data.HasField("craft_component") else 0
        tier = mastery_tier_for_exp(exp)
        out.tier = tier
        out.tier_name = mastery_tier_name(tier)
        out.exp = exp
        out.next_tier_exp = mastery_next_exp(exp)
        out.top_quality_rate_bps = forge_top_quality_rate_bps(tier)

    def fill_v1_display(self, item, info) -> None:
        """为 BagItemInfo 补充 V1 固定词条槽位（含品相与控辅强化缩放、保底进度）与法宝展示数据。"""
        row = self.tables["Item"].get(item.config_id)
        if row is None:
            return
        profile = self._item_profile(row)
        info.equip_profile = profile
        info.ClearField("fixed_affix_slots")
        info.ClearField("artifact")
        if profile in (1, 2):
            valid = self._valid_selections(item, profile)
            enhance_bonus = self._v1_item_enhance_bonus(item, row) if (valid and profile == 1) else 0
            for slot in (1, 2):
                shown = info.fixed_affix_slots.add(slot=slot, pool=pool_for(profile, slot))
                if not valid:
                    continue
                for selected in item.equip.fixed_affixes:
                    if selected.slot == slot:
                        affix = self.tables["EquipV1Affix"].get(selected.affix_id)
                        if affix is not None:
                            self._fill_affix_instance(
                                affix, selected, shown.selected,
                                equip_profile=profile, enhance_bonus=enhance_bonus,
                            )
        elif profile == 3 and "EquipArtifact" in self.tables.tables:
            artifact = self.tables["EquipArtifact"].get(getattr(row, "artifact_id", 0))
            if artifact is not None and _valid_artifact_row(artifact):
                _fill_artifact_info(artifact, info.artifact)

    def collect_effects(self, data, effects) -> None:
        """汇总已穿戴 V1 装备固定词条（含品相与控辅强化克制缩放、动态上限）、法宝效果及当季赛季印轻属性。"""
        effects.Clear()
        if self.assets is None:
            return
        effects.support_sources_complete = True
        values: dict[tuple[int, int], list[int]] = {}
        artifacts: list[tuple[int, object]] = []
        for _, item in self.assets.entries(data.bag_component, 2):
            if item.size != 1:
                continue
            row = self.tables["Item"].get(item.config_id)
            if row is None:
                effects.support_sources_complete = False
                continue
            profile = self._item_profile(row)
            if not 1 <= profile <= 3:
                continue
            if not item.HasField("equip") or not self._valid_selections(item, profile):
                effects.support_sources_complete = False
                continue
            item_enhance_bonus = self._v1_item_enhance_bonus(item, row) if profile == 1 else 0
            for selected in item.equip.fixed_affixes:
                affix = self.tables["EquipV1Affix"].get(selected.affix_id)
                if affix is None:
                    effects.support_sources_complete = False
                    continue
                exempt = is_polish_exempt(affix.effect)
                tier = effective_quality_tier(int(selected.quality_tier), exempt)
                quality_scaled = scale_value_by_quality(affix.value_permyriad, self._quality_ratio_pct(tier))
                scaled_val = scale_v1_fixed_affix_by_enhance(
                    profile, affix.effect, quality_scaled, item_enhance_bonus,
                )
                chan_enhance = (
                    item_enhance_bonus
                    if v1_fixed_affix_enhance_share_permyriad(profile, affix.effect) > 0
                    else 0
                )
                key = (int(affix.effect), int(affix.effect_param))
                if key not in values:
                    values[key] = [scaled_val, int(affix.source_cap), chan_enhance]
                else:
                    values[key][0] += scaled_val
                    values[key][1] = min(values[key][1], int(affix.source_cap))
                    values[key][2] = max(values[key][2], chan_enhance)
                if affix.effect in (5, 6):
                    src = effects.support_sources.add()
                    src.kind = 1
                    src.item_id = int(item.item_id)
                    src.affix_id = int(affix.id)
                    src.name = f"{row.name} · {affix.name}"
                    src.effect_kind = 2 if affix.effect == 6 else 1
                    src.raw_bonus_bps = scaled_val
                    src.source_cap_bps = int(affix.source_cap)
            if profile == 3:
                artifacts.append((int(item.item_id), row))

        support_effective_caps = {1: 3200, 2: 3200}
        for (eff, param), (total_val, cap_val, max_enh) in values.items():
            effective_cap = (
                scale_v1_fixed_affix_by_enhance(1, eff, cap_val, max_enh)
                if max_enh > 0 else cap_val
            )
            capped = min(total_val, effective_cap)
            if eff == 1:
                effects.physical_attack_bps = capped
            elif eff == 2:
                effects.magic_attack_bps = capped
            elif eff == 3:
                effects.max_health_bps = capped
            elif eff == 4:
                effects.speed_bps = capped
            elif eff == 5:
                effects.outgoing_heal_bps = capped
                support_effective_caps[1] = effective_cap
            elif eff == 6:
                effects.shield_capacity_bps = capped
                support_effective_caps[2] = effective_cap
            elif eff == 7:
                effects.physical_crit_bps = capped
            elif eff == 8:
                effects.magic_crit_bps = capped
            elif eff == 9:
                effects.physical_resist_bps = capped
            elif eff == 10:
                effects.magic_resist_bps = capped
            elif eff == 11:
                effects.ailment_hit_bps[param] = capped
            elif eff == 12:
                effects.ailment_resist_bps[param] = capped
            elif eff == 13:
                effects.mana_reduction_bps[param] = capped

        support_list = sorted(effects.support_sources, key=lambda s: (s.item_id, s.affix_id))
        if support_list:
            del effects.support_sources[:]
            healing_left = int(effects.outgoing_heal_bps)
            shield_left = int(effects.shield_capacity_bps)
            for s in support_list:
                out_s = effects.support_sources.add()
                out_s.CopyFrom(s)
                out_s.source_cap_bps = support_effective_caps[2 if out_s.effect_kind == 2 else 1]
                if out_s.effect_kind == 2:
                    contrib = min(shield_left, int(out_s.raw_bonus_bps))
                    shield_left -= contrib
                else:
                    contrib = min(healing_left, int(out_s.raw_bonus_bps))
                    healing_left -= contrib
                out_s.effective_bonus_bps = contrib
                if contrib < out_s.raw_bonus_bps:
                    out_s.reason = "装备同类来源上限已截取"

        artifacts.sort(key=lambda pair: pair[0])
        if "EquipArtifact" in self.tables.tables:
            for item_id, row in artifacts:
                art = self.tables["EquipArtifact"].get(getattr(row, "artifact_id", 0))
                if art is None or not _valid_artifact_row(art):
                    effects.support_sources_complete = False
                    continue
                effects.artifacts.add(
                    item_id=item_id,
                    effect_id=art.id,
                    bonus_bps=art.bonus_bps,
                    cap_bps=art.cap_bps,
                    per_round_limit=art.per_round_limit,
                    battle_limit=art.battle_limit,
                    active_rounds=art.active_rounds,
                    health_threshold_bps=art.health_threshold_bps,
                    name=art.name,
                )

        season = getattr(self.scene, "season", None)
        if season is not None and hasattr(season, "collect_effects"):
            season.collect_effects(data, effects, now_ms_val=self.clock())

    def _fill_product(self, item, info) -> None:
        info.Clear()
        row = self.tables["Item"].get(item.config_id)
        if row is None:
            return
        info.item_id = item.item_id
        info.config_id = item.config_id
        info.count = item.size
        info.bound = bool(item.bound)
        _fill_item(self.tables, info, item.equip if item.HasField("equip") else None)
        if item.HasField("equip"):
            info.enhance_pity_count = int(item.equip.enhance_pity_count)
            info.crafter_name = item.equip.crafter_name
            info.crafter_player_id = int(item.equip.crafter_player_id)
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
                    lifetime_earned=mem.lifetime_earned,
                    tradable_earned_total=mem.tradable_earned_total,
                    available_tradable_units=mem.available_tradable_units,
                    week_starts_at_ms=mem.week_starts_at_ms,
                    earned_this_week=mem.earned_this_week,
                    migrated_to_v2=mem.migrated_to_v2,
                    stage1_tradable_confirmed=mem.stage1_tradable_confirmed,
                    stage2_pool_cap=mem.stage2_pool_cap,
                )
            return _MaterialRewardState(
                day_starts_at_ms=mr.day_starts_at_ms,
                earned_today=mr.earned_today,
                available_units=mr.available_units,
                revision=mr.revision,
                lifetime_earned=int(getattr(mr, "lifetime_earned", 0)),
                tradable_earned_total=int(getattr(mr, "tradable_earned_total", 0)),
                available_tradable_units=int(getattr(mr, "available_tradable_units", 0)),
                week_starts_at_ms=int(getattr(mr, "week_starts_at_ms", 0)),
                earned_this_week=int(getattr(mr, "earned_this_week", 0)),
                migrated_to_v2=bool(getattr(mr, "migrated_to_v2", False)),
                stage1_tradable_confirmed=int(getattr(mr, "stage1_tradable_confirmed", 0)),
                stage2_pool_cap=int(getattr(mr, "stage2_pool_cap", 0)),
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
            mr.lifetime_earned = state.lifetime_earned
            mr.tradable_earned_total = state.tradable_earned_total
            mr.available_tradable_units = state.available_tradable_units
            mr.week_starts_at_ms = state.week_starts_at_ms
            mr.earned_this_week = state.earned_this_week
            mr.migrated_to_v2 = state.migrated_to_v2
            mr.stage1_tradable_confirmed = state.stage1_tradable_confirmed
            if hasattr(mr, "stage2_pool_cap"):
                mr.stage2_pool_cap = state.stage2_pool_cap
            if persisted:
                self._material_rewards.pop(player_id, None)
            elif player_id in self._material_rewards:
                self._material_rewards[player_id] = _MaterialRewardState(
                    day_starts_at_ms=state.day_starts_at_ms,
                    earned_today=state.earned_today,
                    available_units=state.available_units,
                    revision=state.revision,
                    lifetime_earned=state.lifetime_earned,
                    tradable_earned_total=state.tradable_earned_total,
                    available_tradable_units=state.available_tradable_units,
                    week_starts_at_ms=state.week_starts_at_ms,
                    earned_this_week=state.earned_this_week,
                    migrated_to_v2=state.migrated_to_v2,
                    stage1_tradable_confirmed=state.stage1_tradable_confirmed,
                    stage2_pool_cap=state.stage2_pool_cap,
                )
        else:
            self._material_rewards[player_id] = state

    def set_account_quota_sync_transport(self, transport) -> None:
        """注入账号级阶段一 60 份可交易配额异步同步传输钩子（测试或 data_service 客户端）。"""
        self._account_quota_transport = transport
        if transport is None:
            self._account_stage1_used.clear()

    def known_account_stage1_tradable_used(self, player_id: int, data=None) -> int:
        """返回当前场景已知的账号级阶段一可交易已用份数（封顶 60）。"""
        used = 0
        if data is not None and hasattr(data.client_request_ledger, "material_reward"):
            mr = data.client_request_ledger.material_reward
            if mr.migrated_to_v2:
                used = int(mr.tradable_earned_total)
        used = max(used, self._account_stage1_used.get(player_id, 0))
        return min(used, MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT)

    def apply_account_quota_sync_reply(
        self,
        player_id: int,
        data,
        *,
        desired_char_total: int,
        found: bool,
        account_used_total: int,
        confirmed_char_total: int,
    ) -> bool:
        """应用 data_service 账号目录同步回执：超额未确认可交易额平滑降级为绑定自用，更新 stage1_tradable_confirmed。"""
        if not found:
            return False
        used = min(MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT, int(account_used_total))
        self._account_stage1_used[player_id] = max(self._account_stage1_used.get(player_id, 0), used)
        state = self._get_reward_state(player_id, data)
        if not state.migrated_to_v2:
            return False
        confirmed_by_dir = min(int(confirmed_char_total), MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT)
        changed = False
        if state.tradable_earned_total > confirmed_by_dir and (
            confirmed_by_dir < desired_char_total
            or self._account_stage1_used[player_id] >= MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT
        ):
            revoked = state.tradable_earned_total - confirmed_by_dir
            state.tradable_earned_total = confirmed_by_dir
            state.available_tradable_units = max(0, state.available_tradable_units - revoked)
            changed = True
        next_confirmed = min(
            state.tradable_earned_total,
            max(state.stage1_tradable_confirmed, confirmed_by_dir),
        )
        if next_confirmed != state.stage1_tradable_confirmed:
            state.stage1_tradable_confirmed = next_confirmed
            changed = True
        if changed:
            self._save_reward_state(player_id, data, state)
        return changed

    def credit_battle_material_reward_v2(
        self,
        player_id: int,
        *,
        is_win: bool,
        bonus_units: int = 0,
        now_ms_val: int | None = None,
        data=None,
        light_card_active: bool = False,
        account_stage1_tradable_used: int | None = None,
    ) -> tuple[int, int]:
        """两阶段 V2 账本入账（首套 660 份 + 账号级 60 可交易；二阶段每周 150 份 + 轻享卡胜局可交易），返回 (总入账, 可交易入账)。"""
        stamp = self.clock() if now_ms_val is None else now_ms_val
        week = resolve_game_week(stamp)
        if week is None:
            return 0, 0
        week_start, _ = week
        if data is not None:
            state = self._get_reward_state(player_id, data)
        else:
            state = self._material_rewards.get(player_id)
            if state is None:
                state = _MaterialRewardState()
                self._material_rewards[player_id] = state
        gross_units = MATERIAL_REWARD_BATTLE_UNITS + (MATERIAL_REWARD_BONUS_UNITS if bonus_units > 0 else 0)
        prev_lifetime = state.lifetime_earned if state.migrated_to_v2 else 0
        prev_tradable_total = state.tradable_earned_total if state.migrated_to_v2 else 0
        prev_avail_tradable = state.available_tradable_units if state.migrated_to_v2 else 0

        add_total = 0
        add_tradable = 0
        if prev_lifetime < MATERIAL_REWARD_LIFETIME_LIMIT:
            stage1_remain = MATERIAL_REWARD_LIFETIME_LIMIT - prev_lifetime
            add_total = min(gross_units, stage1_remain)
            known_used = (
                self.known_account_stage1_tradable_used(player_id, data)
                if account_stage1_tradable_used is None
                else min(int(account_stage1_tradable_used), MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT)
            )
            effective_account_used = max(prev_tradable_total, known_used)
            if is_win and add_total >= MATERIAL_REWARD_BATTLE_UNITS and effective_account_used < MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT:
                add_tradable = 1
            if state.available_units > _MAX_UINT64 - add_total:
                return 0, 0
            state.migrated_to_v2 = True
            state.lifetime_earned = prev_lifetime + add_total
            state.tradable_earned_total = prev_tradable_total + add_tradable
            state.available_units += add_total
            state.available_tradable_units = prev_avail_tradable + add_tradable
            if add_tradable > 0:
                self._account_stage1_used[player_id] = min(
                    MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT,
                    max(self._account_stage1_used.get(player_id, 0) + 1, state.tradable_earned_total),
                )
        else:
            earned_week, pool_cap, clock_rollback = resolve_stage2_week_state(state, week_start)
            if clock_rollback or earned_week >= pool_cap:
                return 0, 0
            add_total = min(gross_units, pool_cap - earned_week)
            add_tradable = 1 if (light_card_active and is_win and add_total >= MATERIAL_REWARD_BATTLE_UNITS) else 0
            if state.available_units > _MAX_UINT64 - add_total:
                return 0, 0
            state.migrated_to_v2 = True
            state.week_starts_at_ms = week_start
            state.stage2_pool_cap = pool_cap
            state.earned_this_week = earned_week + add_total
            state.available_units += add_total
            if add_tradable > 0:
                state.available_tradable_units = prev_avail_tradable + add_tradable

        if data is not None:
            self._save_reward_state(player_id, data, state)
        return add_total, add_tradable

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

    def _consume_material_tb(self, data, config_id: int, count: int, *, tradable_only: bool = False) -> tuple[int, bool]:
        """按 T/B 隔离扣减打造/解封/继承材料，返回 (error, consumed_bound)。"""
        if self.tables["Item"].get(config_id) is not None and hasattr(self.assets, "consume_items_tb"):
            return self.assets.consume_items_tb(data, {config_id: count}, tradable_only=tradable_only)
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return INVALID_DATA, False
        held = held_item_count(data, config_id)
        known = sum(
            item.size for _, item in entries
            if item.config_id == config_id and self.assets.countable(item)
            and (not tradable_only or not item.bound)
        )
        if known < count or (held and count > max(0, known - held)):
            return BAG_INSUFFICIENT, False
        ordered = sorted(
            (
                (entry, item) for entry, item in entries
                if item.config_id == config_id and self.assets.countable(item)
                and (not tradable_only or not item.bound)
            ),
            key=lambda pair: (1 if pair[1].bound else 0, pair[0].pos, pair[1].acquire_seq, pair[1].item_id),
        )
        remaining = count
        consumed_bound = False
        for entry, item in ordered:
            if remaining <= 0:
                break
            take = min(remaining, item.size)
            if take:
                if item.bound:
                    consumed_bound = True
                item.size -= take
                remaining -= take
                self.assets.write_entry(entry, item)
        if remaining:
            return BAG_INSUFFICIENT, False
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0, consumed_bound

    def _consume_material(self, data, config_id: int, count: int) -> int:
        err, _ = self._consume_material_tb(data, config_id, count, tradable_only=False)
        return err

    async def _grant_stack_with_bound(self, data, config_id: int, count: int, *, bound: bool, gains: list) -> int:
        """按 bound 属性分堆发放材料，绝不把可交易堆与绑定堆混合。"""
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
            return INVALID_DATA
        occupied = {entry.pos for entry, _ in entries}
        free_slots = (slot for slot in range(cap) if slot not in occupied)
        remaining = count
        for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
            if item.config_id == config_id and bool(item.bound) == bound and self.assets._mergeable(item, max_stack):
                added = min(remaining, max_stack - item.size)
                if added:
                    item.size += added
                    remaining -= added
                    self.assets.write_entry(entry, item)
        plan = []
        while remaining:
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

    async def _grant_materials(self, data, counts: dict[int, int], *, gains: list, bound: bool = False) -> int:
        staged = clone(data)
        for cid, qty in sorted(counts.items()):
            if qty <= 0:
                continue
            if not bound and self.tables["Item"].get(cid) is not None:
                # 仅合并到未绑定堆，避免普通发放误并入已有绑定堆。
                err = await self._grant_stack_with_bound(staged, cid, qty, bound=False, gains=gains)
            else:
                err = await self._grant_stack_with_bound(staged, cid, qty, bound=bound, gains=gains)
            if err:
                return err
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
        """为 BagInfo 中的装备项补充 V1 fixed_affix_slots、artifact 与强化 T/B 预览信息。"""
        if hasattr(self.assets, "enrich_bag_enhance_preview"):
            self.assets.enrich_bag_enhance_preview(data, bag_type, bag_info)
        by_id = {item.item_id: item for _, item in self.assets.entries(data.bag_component, bag_type)}
        for info in bag_info.items:
            stored = by_id.get(info.item_id)
            if stored is not None:
                self.fill_v1_display(stored, info)

    # ── 打造试算与校验 ──

    def _locate_equipment(self, data, item_id: int):
        for bag_type in (0, 2):
            for _, item in self.assets.entries(data.bag_component, bag_type):
                if item.item_id == item_id:
                    return item, bag_type
        return None, 0

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

    def _resolve_proxy_master(self, buyer_id: int, buyer_data, proxy_master_id: int, prepared: dict) -> int:
        if not proxy_master_id:
            exp = int(buyer_data.craft_component.forge_mastery_exp) if buyer_data.HasField("craft_component") else 0
            prepared["effective_mastery_tier"] = mastery_tier_for_exp(exp)
            return 0
        if proxy_master_id == buyer_id:
            return EQUIP_PROXY_MASTER_UNAVAILABLE
        stall = get_proxy_stall_directory(self.scene).find(proxy_master_id)
        if (
            stall is None
            or stall.forge_mastery_tier < MASTERY_MASTER
            or not valid_proxy_fee(int(stall.proxy_forge_fee_gold))
        ):
            return EQUIP_PROXY_MASTER_UNAVAILABLE
        fee = int(stall.proxy_forge_fee_gold)
        net_fee = proxy_net_gold(fee)
        outbox = getattr(self.scene, "mail_outbox", None)
        if net_fee > 0 and (
            outbox is None
            or not outbox.can_accept(buyer_data)
            or (hasattr(self.assets, "currency_globally_blocked") and self.assets.currency_globally_blocked(0))
        ):
            return EQUIP_PROXY_MASTER_UNAVAILABLE
        prepared["proxy_master_player_id"] = proxy_master_id
        prepared["proxy_fee_gold"] = fee
        prepared["proxy_master_name"] = stall.master_name or f"宗师#{proxy_master_id}"
        prepared["effective_mastery_tier"] = MASTERY_MASTER
        return 0

    def _plan_bound_policy(self, data, choice, *, is_create_new: bool, prepared: dict) -> None:
        """规划装备打造/装条/重铸/打磨的 T/B 隔离策略，与 C++ PlanBoundPolicy 保持一致。"""
        consumed = prepared.get("consumed") or {}
        all_tradable_enough = True
        any_countable_bound = False
        for mat_id, mat_count in consumed.items():
            if mat_count <= 0:
                continue
            tradable_avail = (
                self.assets.removable_tradable_count(data, mat_id)
                if hasattr(self.assets, "removable_tradable_count")
                else self._available_material(data, mat_id)
            )
            if tradable_avail < mat_count:
                all_tradable_enough = False
            total_owned = self._owned_material(data, mat_id)
            tradable_owned = (
                self.assets.total_tradable_count(data, mat_id)
                if hasattr(self.assets, "total_tradable_count")
                else total_owned
            )
            if total_owned > tradable_owned:
                any_countable_bound = True

        prepared["tradable_only"] = False
        prepared["will_bind_item"] = False
        prepared["bind_confirm_blocked"] = False

        if is_create_new:
            if not choice.allow_bound_materials and all_tradable_enough:
                prepared["tradable_only"] = True
                prepared["will_bind_item"] = False
                return
            prepared["tradable_only"] = False
            prepared["will_bind_item"] = any_countable_bound
            return

        if prepared["original"].bound:
            prepared["tradable_only"] = False
            prepared["will_bind_item"] = False
            return

        if all_tradable_enough:
            prepared["tradable_only"] = True
            prepared["will_bind_item"] = False
            return
        if not choice.allow_bound_materials:
            prepared["tradable_only"] = True
            prepared["will_bind_item"] = True
            prepared["bind_confirm_blocked"] = True
            return
        prepared["tradable_only"] = False
        prepared["will_bind_item"] = any_countable_bound

    def _prepare_choice(self, buyer_id: int, data, choice):
        if carries_unknown(choice):
            return FEATURE_UNAVAILABLE, None
        op = choice.operation
        if op != _OP_POLISH_AFFIX and (
            int(getattr(choice, "target_quality_tier", 0)) != 0
            or int(getattr(choice, "max_polish_attempts", 0)) != 0
        ):
            return INVALID, None
        if op == _OP_CREATE_ITEM:
            if (
                choice.item_id or choice.affix_slot or choice.affix_id or choice.source_item_id
                or choice.use_guarantee_charm or choice.HasField("expected_polish_pity_count")
            ):
                return INVALID, None
            row = self.tables["Item"].get(choice.config_id)
            if row is None or not self._valid_template(row):
                return INVALID, None
            result = self.protocol.message("ItemComp", config_id=row.id, size=1)
            result.equip.SetInParent()
            mat_id, mat_count = self._item_forge_material(row)
            prepared = {
                "result": result,
                "original": None,
                "holder": 0,
                "material_id": mat_id,
                "material_count": mat_count,
                "extra_material_id": 0,
                "extra_material_count": 0,
                "consumed": {mat_id: mat_count},
                "base_gold_cost": 0,
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": MASTERY_APPRENTICE,
                "target_affix_index": -1,
                "polish_exempt": False,
                "reroll_quality_on_reforge": False,
                "bind_on_consumed_bound": True,
            }
            if err := self._resolve_proxy_master(buyer_id, data, int(choice.proxy_master_player_id), prepared):
                return err, None
            self._plan_bound_policy(data, choice, is_create_new=True, prepared=prepared)
            return 0, prepared

        if op == _OP_INSTALL_AFFIX:
            if (
                choice.config_id or not choice.item_id or not choice.affix_id or choice.source_item_id
                or choice.use_guarantee_charm or choice.HasField("expected_polish_pity_count")
            ):
                return INVALID, None
            found_item, holder = self._locate_equipment(data, choice.item_id)
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
            prepared = {
                "result": result,
                "original": found_item,
                "holder": holder,
                "material_id": affix.material_id,
                "material_count": affix.material_count,
                "extra_material_id": 0,
                "extra_material_count": 0,
                "consumed": {affix.material_id: affix.material_count},
                "base_gold_cost": 0,
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": MASTERY_APPRENTICE,
                "target_affix_index": len(result.equip.fixed_affixes) - 1,
                "polish_exempt": is_polish_exempt(affix.effect),
                "reroll_quality_on_reforge": False,
                "bind_on_consumed_bound": True,
            }
            if err := self._resolve_proxy_master(buyer_id, data, int(choice.proxy_master_player_id), prepared):
                return err, None
            self._plan_bound_policy(data, choice, is_create_new=False, prepared=prepared)
            return 0, prepared

        if op == _OP_REFORGE_AFFIX:
            if (
                choice.config_id or not choice.item_id or not choice.affix_id or choice.source_item_id
                or choice.use_guarantee_charm or choice.proxy_master_player_id
                or choice.HasField("expected_polish_pity_count")
            ):
                return INVALID, None
            found_item, holder = self._locate_equipment(data, choice.item_id)
            if found_item is None:
                return EQUIP_ITEM_NOT_FOUND, None
            item_row = self.tables["Item"].get(found_item.config_id)
            new_affix = self.tables["EquipV1Affix"].get(choice.affix_id)
            if item_row is None or new_affix is None or not valid_affix_row(new_affix):
                return INVALID, None
            profile = self._item_profile(item_row)
            if (
                pool_for(profile, choice.affix_slot) != new_affix.pool
                or found_item.size != 1
                or not self._valid_selections(found_item, profile)
            ):
                return INVALID, None
            if carries_unknown(found_item) or carries_unknown(found_item.equip):
                return FEATURE_UNAVAILABLE, None
            found_idx = -1
            for idx, selected in enumerate(found_item.equip.fixed_affixes):
                if carries_unknown(selected):
                    return FEATURE_UNAVAILABLE, None
                if selected.slot == choice.affix_slot:
                    if selected.affix_id == choice.affix_id:
                        return EQUIP_AFFIX_REFORGE_SAME, None
                    found_idx = idx
                elif selected.affix_id == choice.affix_id:
                    return INVALID, None
            if found_idx < 0:
                return EQUIP_AFFIX_SLOT_EMPTY, None
            result = clone(found_item)
            target = result.equip.fixed_affixes[found_idx]
            old_affix = self.tables["EquipV1Affix"].get(target.affix_id)
            old_exempt = old_affix is not None and is_polish_exempt(old_affix.effect)
            new_exempt = is_polish_exempt(new_affix.effect)
            target.affix_id = choice.affix_id
            reroll_quality = False
            if new_exempt:
                target.quality_tier = MAX_QUALITY_TIER
                target.polish_pity_count = 0
            elif old_exempt:
                reroll_quality = True
            exp = int(data.craft_component.forge_mastery_exp) if data.HasField("craft_component") else 0
            prepared = {
                "result": result,
                "original": found_item,
                "holder": holder,
                "material_id": POLISH_SAND_ID,
                "material_count": 1,
                "extra_material_id": 0,
                "extra_material_count": 0,
                "consumed": {POLISH_SAND_ID: 1},
                "base_gold_cost": REFORGE_GOLD_COST,
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": mastery_tier_for_exp(exp),
                "target_affix_index": found_idx,
                "polish_exempt": new_exempt,
                "reroll_quality_on_reforge": reroll_quality,
                "bind_on_consumed_bound": True,
            }
            self._plan_bound_policy(data, choice, is_create_new=False, prepared=prepared)
            return 0, prepared

        if op == _OP_POLISH_AFFIX:
            if choice.config_id or not choice.item_id or choice.affix_id or choice.source_item_id:
                return INVALID, None
            found_item, holder = self._locate_equipment(data, choice.item_id)
            if found_item is None:
                return EQUIP_ITEM_NOT_FOUND, None
            item_row = self.tables["Item"].get(found_item.config_id)
            if item_row is None:
                return INVALID, None
            profile = self._item_profile(item_row)
            if (
                pool_for(profile, choice.affix_slot) == 0
                or found_item.size != 1
                or not self._valid_selections(found_item, profile)
            ):
                return INVALID, None
            if carries_unknown(found_item) or carries_unknown(found_item.equip):
                return FEATURE_UNAVAILABLE, None
            found_idx = -1
            for idx, selected in enumerate(found_item.equip.fixed_affixes):
                if carries_unknown(selected):
                    return FEATURE_UNAVAILABLE, None
                if selected.slot == choice.affix_slot:
                    found_idx = idx
                    break
            if found_idx < 0:
                return EQUIP_AFFIX_SLOT_EMPTY, None
            current = found_item.equip.fixed_affixes[found_idx]
            affix = self.tables["EquipV1Affix"].get(current.affix_id)
            if affix is None:
                return INVALID, None
            if is_polish_exempt(affix.effect):
                return EQUIP_AFFIX_POLISH_EXEMPT, None
            current_tier = effective_quality_tier(int(current.quality_tier), False)
            if current_tier >= MAX_QUALITY_TIER:
                return EQUIP_AFFIX_ALREADY_MAX_QUALITY, None
            if choice.HasField("expected_polish_pity_count") and choice.expected_polish_pity_count != current.polish_pity_count:
                return EQUIP_POLISH_PITY_CHANGED, None
            req_target_tier = int(getattr(choice, "target_quality_tier", 0))
            req_max_attempts = int(getattr(choice, "max_polish_attempts", 0))
            if req_max_attempts > MAX_BATCH_POLISH_ATTEMPTS:
                return INVALID, None
            if req_target_tier != 0 and (
                req_target_tier < 2 or req_target_tier > MAX_QUALITY_TIER or req_target_tier <= current_tier
            ):
                return INVALID, None
            if choice.use_guarantee_charm and (req_target_tier != 0 or req_max_attempts > 1):
                return INVALID, None
            consumed = {POLISH_SAND_ID: 1}
            extra_id = GUARANTEE_CHARM_ID if choice.use_guarantee_charm else 0
            extra_count = 1 if choice.use_guarantee_charm else 0
            if extra_id:
                consumed[extra_id] = consumed.get(extra_id, 0) + extra_count
            prepared = {
                "result": clone(found_item),
                "original": found_item,
                "holder": holder,
                "material_id": POLISH_SAND_ID,
                "material_count": 1,
                "extra_material_id": extra_id,
                "extra_material_count": extra_count,
                "consumed": consumed,
                "base_gold_cost": POLISH_GOLD_COST,
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": MASTERY_APPRENTICE,
                "target_affix_index": found_idx,
                "polish_exempt": False,
                "reroll_quality_on_reforge": False,
                "bind_on_consumed_bound": True,
            }
            if err := self._resolve_proxy_master(buyer_id, data, int(choice.proxy_master_player_id), prepared):
                return err, None
            self._plan_bound_policy(data, choice, is_create_new=False, prepared=prepared)

            pity_remaining = (
                POLISH_PITY_LIMIT - int(current.polish_pity_count)
                if int(current.polish_pity_count) < POLISH_PITY_LIMIT
                else 1
            )
            batch_requested = (not choice.use_guarantee_charm) and (req_target_tier != 0 or req_max_attempts > 1)
            if batch_requested:
                polish_target_tier = req_target_tier if req_target_tier != 0 else MAX_QUALITY_TIER
                requested_cap = req_max_attempts if req_max_attempts != 0 else MAX_BATCH_POLISH_ATTEMPTS
                max_by_pity = min(requested_cap, pity_remaining)
                sand_avail = (
                    self.assets.removable_tradable_count(data, POLISH_SAND_ID)
                    if prepared.get("tradable_only") and hasattr(self.assets, "removable_tradable_count")
                    else self._available_material(data, POLISH_SAND_ID)
                )
                max_by_sand = min(max_by_pity, sand_avail)
                allow_bind_for_base = bool(prepared["result"].bound)
                unit_strict = prepared["proxy_fee_gold"] + (0 if allow_bind_for_base else prepared["base_gold_cost"])
                unit_prefer_bind = prepared["base_gold_cost"] if allow_bind_for_base else 0
                max_by_gold = 0
                for n in range(1, max_by_sand + 1):
                    if (unit_strict == 0 and unit_prefer_bind == 0) or self.assets.can_afford_split_gold(
                        data, unit_strict * n, unit_prefer_bind * n,
                    ):
                        max_by_gold = n
                    else:
                        break
                max_polish_attempts = max(1, max_by_gold)
            else:
                polish_target_tier = 0
                max_polish_attempts = 1
            prepared["polish_target_tier"] = polish_target_tier
            prepared["max_polish_attempts"] = max_polish_attempts
            prepared["max_polish_sand_cost"] = max_polish_attempts
            prepared["max_polish_gold_cost"] = max_polish_attempts * (
                prepared["base_gold_cost"] + prepared["proxy_fee_gold"]
            )
            return 0, prepared

        if op == _OP_UNSEAL_TO_TRADABLE:
            if (
                choice.config_id or not choice.item_id or choice.affix_slot or choice.affix_id
                or choice.source_item_id or choice.use_guarantee_charm or choice.proxy_master_player_id
                or choice.HasField("expected_polish_pity_count")
            ):
                return INVALID, None
            if self.tables["Item"].get(UNSEAL_TALISMAN_ID) is None:
                return INVALID_DATA, None
            found_item, holder = self._locate_equipment(data, choice.item_id)
            if found_item is None:
                return EQUIP_ITEM_NOT_FOUND, None
            item_row = self.tables["Item"].get(found_item.config_id)
            if item_row is None or not item_row.equip_kind or found_item.size != 1 or not found_item.HasField("equip"):
                return INVALID, None
            if carries_unknown(found_item) or carries_unknown(found_item.equip):
                return FEATURE_UNAVAILABLE, None
            if not found_item.bound:
                return INVALID, None
            for gem in found_item.equip.gems:
                if gem.gem_config_id != 0:
                    return INVALID, None
            result = clone(found_item)
            result.bound = False
            result.equip.enhance_level = 0
            result.equip.enhance_pity_count = 0
            del result.equip.fixed_affixes[:]
            result.equip.crafter_name = ""
            result.equip.crafter_player_id = 0
            return 0, {
                "result": result,
                "original": found_item,
                "holder": holder,
                "material_id": UNSEAL_TALISMAN_ID,
                "material_count": 1,
                "extra_material_id": 0,
                "extra_material_count": 0,
                "consumed": {UNSEAL_TALISMAN_ID: 1},
                "base_gold_cost": 0,
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": MASTERY_APPRENTICE,
                "target_affix_index": -1,
                "polish_exempt": False,
                "reroll_quality_on_reforge": False,
                "tradable_only": False,
                "will_bind_item": False,
                "bind_confirm_blocked": False,
                "bind_on_consumed_bound": False,
            }

        if op == _OP_INHERIT_ENHANCE:
            if (
                choice.config_id or not choice.item_id or not choice.source_item_id
                or choice.source_item_id == choice.item_id or choice.affix_slot or choice.affix_id
                or choice.use_guarantee_charm or choice.proxy_master_player_id
                or choice.HasField("expected_polish_pity_count")
            ):
                return INVALID, None
            if self.tables["Item"].get(INHERIT_TALISMAN_ID) is None:
                return INVALID_DATA, None
            found_item, holder = self._locate_equipment(data, choice.item_id)
            if found_item is None:
                return EQUIP_ITEM_NOT_FOUND, None
            source_item, _ = self._locate_equipment(data, choice.source_item_id)
            if source_item is None:
                return EQUIP_ITEM_NOT_FOUND, None
            target_row = self.tables["Item"].get(found_item.config_id)
            source_row = self.tables["Item"].get(source_item.config_id)
            if (
                target_row is None or source_row is None or not target_row.equip_kind
                or target_row.equip_kind != source_row.equip_kind
                or self._item_profile(target_row) != self._item_profile(source_row)
                or target_row.equip_level != source_row.equip_level
                or found_item.size != 1 or source_item.size != 1
                or not found_item.HasField("equip") or not source_item.HasField("equip")
            ):
                return INVALID, None
            if (
                carries_unknown(found_item) or carries_unknown(found_item.equip)
                or carries_unknown(source_item) or carries_unknown(source_item.equip)
            ):
                return FEATURE_UNAVAILABLE, None
            source_level = int(source_item.equip.enhance_level)
            target_level = int(found_item.equip.enhance_level)
            if source_level == 0 or source_level <= target_level:
                return INVALID, None
            result = clone(found_item)
            result.equip.enhance_level = source_level
            result.equip.enhance_pity_count = int(source_item.equip.enhance_pity_count)
            result.bound = True
            return 0, {
                "result": result,
                "original": found_item,
                "holder": holder,
                "material_id": INHERIT_TALISMAN_ID,
                "material_count": 1,
                "extra_material_id": 0,
                "extra_material_count": 0,
                "consumed": {INHERIT_TALISMAN_ID: 1},
                "base_gold_cost": 0,
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": MASTERY_APPRENTICE,
                "target_affix_index": -1,
                "polish_exempt": False,
                "reroll_quality_on_reforge": False,
                "tradable_only": False,
                "will_bind_item": not found_item.bound,
                "bind_confirm_blocked": False,
                "bind_on_consumed_bound": False,
            }

        if op == _OP_UPGRADE_TIER:
            if (
                not choice.config_id or not choice.item_id or choice.source_item_id
                or choice.affix_slot or choice.affix_id or choice.use_guarantee_charm
                or choice.proxy_master_player_id or choice.HasField("expected_polish_pity_count")
            ):
                return INVALID, None
            found_item, holder = self._locate_equipment(data, choice.item_id)
            if found_item is None:
                return EQUIP_ITEM_NOT_FOUND, None
            source_row = self.tables["Item"].get(found_item.config_id)
            target_row = self.tables["Item"].get(choice.config_id)
            if (
                source_row is None or target_row is None
                or not self._valid_template(source_row) or not self._valid_template(target_row)
                or not can_upgrade_tier(
                    self._item_profile(source_row), source_row.equip_kind, source_row.equip_level,
                    getattr(source_row, "equip_class", 0),
                    self._item_profile(target_row), target_row.equip_kind, target_row.equip_level,
                    getattr(target_row, "equip_class", 0),
                )
                or found_item.size != 1
                or not self._valid_selections(found_item, self._item_profile(source_row))
            ):
                return INVALID, None
            if carries_unknown(found_item) or carries_unknown(found_item.equip):
                return FEATURE_UNAVAILABLE, None
            result = clone(found_item)
            result.config_id = target_row.id
            return 0, {
                "result": result,
                "original": found_item,
                "holder": holder,
                "material_id": 0,
                "material_count": 0,
                "extra_material_id": 0,
                "extra_material_count": 0,
                "consumed": {},
                "base_gold_cost": upgrade_tier_gold_cost(
                    self._item_profile(source_row), source_row.equip_level, target_row.equip_level,
                ),
                "proxy_fee_gold": 0,
                "proxy_master_player_id": 0,
                "proxy_master_name": "",
                "effective_mastery_tier": MASTERY_APPRENTICE,
                "target_affix_index": -1,
                "polish_exempt": False,
                "reroll_quality_on_reforge": False,
                "tradable_only": False,
                "will_bind_item": False,
                "bind_confirm_blocked": False,
                "bind_on_consumed_bound": False,
            }

        return INVALID, None

    def _split_gold_costs(
        self, op: int, prepared: dict, *, base_gold_override: int | None = None, proxy_fee_override: int | None = None,
    ) -> tuple[int, int]:
        allow_bind_for_base_fee = (op != _OP_UNSEAL_TO_TRADABLE) and bool(prepared["result"].bound)
        base_gold = int(prepared.get("base_gold_cost", 0)) if base_gold_override is None else int(base_gold_override)
        proxy_fee = int(prepared.get("proxy_fee_gold", 0)) if proxy_fee_override is None else int(proxy_fee_override)
        strict_gold = proxy_fee + (0 if allow_bind_for_base_fee else base_gold)
        prefer_bind_gold = base_gold if allow_bind_for_base_fee else 0
        return strict_gold, prefer_bind_gold

    def _check_availability(self, player, data, op: int, prepared) -> int:
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
        for mat_id, mat_count in sorted((prepared.get("consumed") or {}).items()):
            if mat_count > 0 and self._available_material(data, mat_id) < mat_count:
                return EQUIP_MATERIAL_NOT_ENOUGH
        if prepared.get("bind_confirm_blocked"):
            return EQUIP_BIND_CONFIRM_REQUIRED
        strict_gold, prefer_bind_gold = self._split_gold_costs(op, prepared)
        if strict_gold > 0 or prefer_bind_gold > 0:
            if not self.assets.can_afford_split_gold(data, strict_gold, prefer_bind_gold):
                return EQUIP_CRAFT_GOLD_NOT_ENOUGH
        if prepared["original"] is None:
            staged = clone(data)
            for mat_id, mat_count in sorted((prepared.get("consumed") or {}).items()):
                if mat_count <= 0:
                    continue
                err, _ = self._consume_material_tb(
                    staged, mat_id, mat_count,
                    tradable_only=bool(prepared.get("tradable_only")),
                )
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
        """查询权威打造配方、固定词条池、品相权重、规则指纹与当前打造修订号。"""
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
        self._fill_forge_mastery(data, response.forge_mastery)
        return response

    def preview_craft(self, _world, player, _route, request):
        """试算装备打造、装条、重铸、打磨、解封、继承或无损升档结果与所需材料，不落盘不扣料。"""
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
        err, prepared = self._prepare_choice(player.player_id, data, request.choice)
        if err:
            response.error_message.id = err
            return response
        preview = response.preview
        preview.choice.CopyFrom(request.choice)
        preview.rules_fingerprint = cached.rules_fingerprint
        preview.expected_forge_revision = self._get_forge_revision(player.player_id, data)
        preview_item = clone(prepared["result"])
        if prepared.get("will_bind_item"):
            preview_item.bound = True
        if request.choice.operation == _OP_POLISH_AFFIX and prepared["target_affix_index"] >= 0:
            target = preview_item.equip.fixed_affixes[prepared["target_affix_index"]]
            if request.choice.use_guarantee_charm or target.polish_pity_count + 1 >= POLISH_PITY_LIMIT:
                target.quality_tier = MAX_QUALITY_TIER
                target.polish_pity_count = 0
        preview.will_bind_item = bool(prepared.get("will_bind_item"))
        self._fill_product(preview_item, preview.result)
        if prepared["material_id"] and prepared["material_count"]:
            self._fill_material_display(data, prepared["material_id"], prepared["material_count"], preview.material)
        if prepared["extra_material_id"] and prepared["extra_material_count"]:
            self._fill_material_display(
                data, prepared["extra_material_id"], prepared["extra_material_count"], preview.extra_material,
            )
        preview.gold_cost = int(prepared.get("base_gold_cost", 0))
        preview.proxy_fee_gold = int(prepared.get("proxy_fee_gold", 0))
        if prepared.get("proxy_master_name"):
            preview.proxy_master_name = prepared["proxy_master_name"]
        preview.max_polish_attempts = int(prepared.get("max_polish_attempts", 1))
        preview.max_polish_sand_cost = int(prepared.get("max_polish_sand_cost", 0))
        preview.max_polish_gold_cost = int(prepared.get("max_polish_gold_cost", 0))
        unavailable = self._check_availability(player, data, request.choice.operation, prepared)
        preview.available = unavailable == 0
        if unavailable:
            preview.unavailable_tip = unavailable
        return response

    def _remember_craft(self, player_id: int, request_id: int, item_id: int, forge_revision: int) -> None:
        if len(self._craft_receipts) >= _MAX_CACHED_RECEIPTS:
            oldest = next(iter(self._craft_receipts))
            del self._craft_receipts[oldest]
        self._craft_receipts[(player_id, request_id)] = (item_id, forge_revision)

    def _fill_craft_applied(
        self, player, data, response, *, item_id: int, result_rev: int, replayed: bool,
        quality_tier: int = 0, polish_pity_count: int = 0, guaranteed_by_pity: bool = False,
        polish_attempts_used: int = 0, actual_gold_spent: int = 0, polish_steps: list | None = None,
    ):
        response.outcome = _OUTCOME_APPLIED
        response.replayed = replayed
        response.durable = True
        response.item_id = item_id
        response.result_forge_revision = result_rev
        response.current_forge_revision = self._get_forge_revision(player.player_id, data)
        response.result_quality_tier = quality_tier
        response.result_polish_pity_count = polish_pity_count
        response.guaranteed_by_pity = guaranteed_by_pity
        response.polish_attempts_used = int(polish_attempts_used)
        response.actual_gold_spent = int(actual_gold_spent)
        if polish_steps:
            for step in polish_steps:
                response.polish_steps.add().CopyFrom(step)
        self._fill_forge_mastery(data, response.forge_mastery)
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
        """原子提交装备打造/装条/重铸/打磨/解封/继承/无损升档，带水位乐观锁、拆分银两与 client_request_id 幂等。"""
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
                quality_tier=int(getattr(prior, "equipment_quality_tier", 0)),
                polish_pity_count=int(getattr(prior, "equipment_polish_pity_count", 0)),
                guaranteed_by_pity=bool(getattr(prior, "equipment_guaranteed_by_pity", False)),
                polish_attempts_used=int(getattr(prior, "equipment_polish_attempts_used", 0)),
                actual_gold_spent=int(getattr(prior, "equipment_actual_gold_spent", 0)),
            )

        if request.expected_forge_revision != current_rev:
            return reject(EQUIP_LEVEL_CHANGED, known=False)
        err, cached = self._build_rules()
        if err:
            return reject(err)
        if request.rules_fingerprint != cached.rules_fingerprint:
            return reject(INVALID)

        op = request.choice.operation
        err, prepared = self._prepare_choice(player.player_id, data, request.choice)
        if err:
            return reject(err)
        err = self._check_availability(player, data, op, prepared)
        if err:
            return reject(err)

        res_quality_tier = 0
        res_polish_pity = 0
        res_guaranteed = False
        actual_polish_attempts = 1
        polish_steps = []
        target_idx = prepared["target_affix_index"]
        if (op == _OP_INSTALL_AFFIX or prepared["reroll_quality_on_reforge"]) and target_idx >= 0:
            target = prepared["result"].equip.fixed_affixes[target_idx]
            tier = MAX_QUALITY_TIER if prepared["polish_exempt"] else self._roll_quality_for_mastery(
                prepared["effective_mastery_tier"],
            )
            target.quality_tier = tier
            target.polish_pity_count = 0
            res_quality_tier = tier
            res_polish_pity = 0
            res_guaranteed = False
        elif op == _OP_REFORGE_AFFIX and target_idx >= 0:
            target = prepared["result"].equip.fixed_affixes[target_idx]
            res_quality_tier = effective_quality_tier(int(target.quality_tier), prepared["polish_exempt"])
            res_polish_pity = int(target.polish_pity_count)
        elif op == _OP_POLISH_AFFIX and target_idx >= 0:
            target = prepared["result"].equip.fixed_affixes[target_idx]
            tier = effective_quality_tier(int(target.quality_tier), False)
            pity = int(target.polish_pity_count)
            guaranteed = False
            actual_polish_attempts = 0
            max_attempts = int(prepared.get("max_polish_attempts", 1))
            polish_target_tier = int(prepared.get("polish_target_tier", 0))
            for step_idx in range(1, max_attempts + 1):
                rolled_tier = self._roll_quality_for_mastery(prepared["effective_mastery_tier"])
                new_tier, new_pity, step_guaranteed = resolve_polish(
                    tier, pity, rolled_tier, bool(request.choice.use_guarantee_charm),
                )
                polish_steps.append(
                    self.protocol.message(
                        "PolishStepTrace",
                        attempt_index=step_idx,
                        rolled_tier=rolled_tier,
                        tier_before=tier,
                        tier_after=new_tier,
                        guaranteed_by_pity=step_guaranteed,
                    )
                )
                tier = new_tier
                pity = new_pity
                guaranteed = guaranteed or step_guaranteed
                actual_polish_attempts = step_idx
                if tier >= MAX_QUALITY_TIER or (polish_target_tier > 0 and tier >= polish_target_tier):
                    break
            target.quality_tier = tier
            target.polish_pity_count = pity
            res_quality_tier = tier
            res_polish_pity = pity
            res_guaranteed = guaranteed
            # 仅按实际执行手数精准扣除重铸砂，绝不多扣未使用手数。
            prepared["consumed"][POLISH_SAND_ID] = actual_polish_attempts

        supreme_produced = (
            op in (_OP_INSTALL_AFFIX, _OP_POLISH_AFFIX)
            and not prepared["polish_exempt"]
            and res_quality_tier == MAX_QUALITY_TIER
        )
        if supreme_produced and prepared["effective_mastery_tier"] == MASTERY_MASTER:
            signer_id = prepared["proxy_master_player_id"] or player.player_id
            signer_name = (
                prepared["proxy_master_name"]
                if prepared["proxy_master_player_id"]
                else self._display_name(data, signer_id)
            )
            prepared["result"].equip.crafter_player_id = signer_id
            prepared["result"].equip.crafter_name = signer_name

        staged = clone(data)
        consumed_bound = False
        for mat_id, mat_count in sorted((prepared.get("consumed") or {}).items()):
            if mat_count <= 0:
                continue
            consume_err, used_bound = self._consume_material_tb(
                staged, mat_id, mat_count,
                tradable_only=bool(prepared.get("tradable_only")),
            )
            if consume_err == BAG_INSUFFICIENT:
                return reject(EQUIP_MATERIAL_NOT_ENOUGH)
            if consume_err:
                return reject(consume_err)
            if used_bound:
                consumed_bound = True

        total_base_gold = (
            actual_polish_attempts * int(prepared.get("base_gold_cost", 0))
            if op == _OP_POLISH_AFFIX
            else int(prepared.get("base_gold_cost", 0))
        )
        total_proxy_fee = (
            actual_polish_attempts * int(prepared.get("proxy_fee_gold", 0))
            if op == _OP_POLISH_AFFIX
            else int(prepared.get("proxy_fee_gold", 0))
        )
        actual_gold_spent = total_base_gold + total_proxy_fee
        strict_gold, prefer_bind_gold = self._split_gold_costs(
            op, prepared, base_gold_override=total_base_gold, proxy_fee_override=total_proxy_fee,
        )
        if strict_gold > 0 or prefer_bind_gold > 0:
            if self.assets.deduct_split_gold(staged, strict_gold, prefer_bind_gold):
                return reject(EQUIP_CRAFT_GOLD_NOT_ENOUGH)

        bind_on_consumed_bound = bool(prepared.get("bind_on_consumed_bound"))
        if op == _OP_CREATE_ITEM:
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
            if bind_on_consumed_bound and consumed_bound:
                new_item.bound = True
            add_err = await self.assets.add_items(staged, {}, instances=(new_item,))
            if add_err:
                return reject(add_err)
            target_item_id = created_guid
        else:
            replaced = False
            final_item = clone(prepared["result"])
            if bind_on_consumed_bound and consumed_bound:
                final_item.bound = True
            for entry, stored in self.assets.entries(staged.bag_component, prepared["holder"]):
                if stored.item_id == request.choice.item_id:
                    self.assets.write_entry(entry, final_item)
                    replaced = True
                    break
            if not replaced:
                return reject(EQUIP_ITEM_NOT_FOUND)
            target_item_id = request.choice.item_id

        next_rev = current_rev + 1
        if hasattr(staged.client_request_ledger, "forge_revision"):
            staged.client_request_ledger.forge_revision = next_rev
        # 本人打造/装条/打磨自然涨铸器熟练度（按实际手数累加）；重铸/解封/继承/无损升档不涨熟练度。
        if op in (_OP_CREATE_ITEM, _OP_INSTALL_AFFIX, _OP_POLISH_AFFIX) and not prepared["proxy_master_player_id"]:
            if hasattr(staged, "craft_component"):
                exp_gain = actual_polish_attempts if op == _OP_POLISH_AFFIX else 1
                staged.craft_component.forge_mastery_exp = min(
                    (1 << 32) - 1, int(staged.craft_component.forge_mastery_exp) + exp_gain,
                )
                if supreme_produced and hasattr(staged.craft_component, "supreme_forged_count"):
                    staged.craft_component.supreme_forged_count = min(
                        (1 << 32) - 1, int(staged.craft_component.supreme_forged_count) + 1,
                    )
        if prepared["proxy_master_player_id"]:
            net_fee = proxy_net_gold(total_proxy_fee)
            if net_fee > 0:
                outbox = getattr(self.scene, "mail_outbox", None)
                if outbox is None or not await outbox.prepare_proxy_fee(
                    staged,
                    recipient_player_id=prepared["proxy_master_player_id"],
                    net_gold=net_fee,
                    source_id=request.client_request_id,
                    buyer_name=self._display_name(staged, player.player_id),
                ):
                    return reject(EQUIP_PROXY_MASTER_UNAVAILABLE)

        client_request.remember(
            staged.client_request_ledger,
            request.client_request_id,
            _CLIENT_REQUEST_KIND_CRAFT,
            fingerprint,
            equipment_item_id=target_item_id,
            forge_revision=next_rev,
            equipment_quality_tier=res_quality_tier,
            equipment_polish_pity_count=res_polish_pity,
            equipment_guaranteed_by_pity=res_guaranteed,
            equipment_polish_attempts_used=actual_polish_attempts if op == _OP_POLISH_AFFIX else 0,
            equipment_actual_gold_spent=actual_gold_spent,
        )
        saved = await self.assets._save(
            player, staged, 27, correlation=request.client_request_id,
            item_extra={"operation": "equipment_craft", "item_id": target_item_id},
        )
        if not saved:
            return reject(UNAVAILABLE, known=False)

        if prepared["proxy_master_player_id"]:
            await record_proxy_order_reputation(
                self.scene,
                int(prepared["proxy_master_player_id"]),
                supreme_delta=1 if supreme_produced else 0,
                rare_delta=0,
                net_gold_earned=proxy_net_gold(total_proxy_fee),
                buyer_name=self._display_name(staged, player.player_id),
            )

        self._set_forge_revision(player.player_id, staged, next_rev)
        if op != _OP_CREATE_ITEM and prepared["holder"] == 2:
            attributes = getattr(self.scene, "attributes", None)
            if attributes is not None:
                attributes.notify(player)
        self._remember_craft(player.player_id, request.client_request_id, target_item_id, next_rev)
        logger.info(
            "Equipment craft committed player_id=%s op=%s item_id=%s revision=%s quality=%s attempts=%s",
            player.player_id, op, target_item_id, next_rev, res_quality_tier,
            actual_polish_attempts if op == _OP_POLISH_AFFIX else 0,
        )
        return self._fill_craft_applied(
            player, staged, response, item_id=target_item_id, result_rev=next_rev, replayed=False,
            quality_tier=res_quality_tier, polish_pity_count=res_polish_pity, guaranteed_by_pity=res_guaranteed,
            polish_attempts_used=actual_polish_attempts if op == _OP_POLISH_AFFIX else 0,
            actual_gold_spent=actual_gold_spent,
            polish_steps=polish_steps,
        )

    def _light_card_active(self, data, stamp: int) -> bool:
        if hasattr(data, "uint64_pb_component"):
            return int(getattr(data.uint64_pb_component, "afk_pass_expires_at", 0)) > (stamp // 1000)
        return False

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

        if state.migrated_to_v2:
            week = resolve_game_week(stamp)
            week_start, next_week = week if week is not None else (0, 0)
            avail_tradable = min(state.available_tradable_units, state.available_units)
            batch_tradable = min(out.batch_units, avail_tradable)
            weekly_phase = state.lifetime_earned >= MATERIAL_REWARD_LIFETIME_LIMIT
            earned_week, pool_cap, _ = (
                resolve_stage2_week_state(state, week_start)
                if weekly_phase
                else (0, MATERIAL_REWARD_WEEKLY_LIMIT, False)
            )
            out.lifetime_earned = state.lifetime_earned
            out.lifetime_limit = MATERIAL_REWARD_LIFETIME_LIMIT
            out.tradable_earned_total = self.known_account_stage1_tradable_used(player.player_id, data)
            out.tradable_lifetime_limit = MATERIAL_REWARD_TRADABLE_LIFETIME_LIMIT
            out.available_tradable_units = avail_tradable
            out.batch_tradable_units = batch_tradable
            out.weekly_phase_active = weekly_phase
            out.week_starts_at_ms = week_start
            out.next_week_reset_at_ms = next_week
            out.earned_this_week = earned_week
            out.weekly_limit = pool_cap
        else:
            weekly_phase = False

        self._fill_material_display(data, CRAFTING_MATERIAL_ID, 0, out.crafting_material)
        self._fill_material_display(data, self._enhancement_stone_id(), 0, out.enhancement_stone)
        writable = self._writable_error(player)
        out.can_claim = out.batch_units > 0 and writable == 0 and state.revision < _MAX_UINT64
        if out.batch_units == 0:
            if state.migrated_to_v2:
                light_card = self._light_card_active(data, stamp)
                out.unavailable_reason = (
                    (
                        "完成主要战斗可获得材料自选份数（每周一 05:00 +150 滚存蓄水，最多存 4 周至 600 份；仙途轻享卡生效中：胜局含 1 份可交易）。"
                        if light_card else
                        "完成主要战斗可获得材料自选份数（每周一 05:00 +150 滚存蓄水，最多存 4 周至 600 份，全绑定；开通仙途轻享卡可解锁胜局可交易产出）。"
                    )
                    if weekly_phase else
                    "完成主要战斗可获得首套材料自选份数（首套总池660份，无每日限制）。"
                )
            else:
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
        stone_id = self._enhancement_stone_id()
        batch_tradable = 0
        if state.migrated_to_v2:
            avail_tradable = min(state.available_tradable_units, state.available_units)
            batch_tradable = min(total, avail_tradable)
            enforce_account_quota = (
                self._account_quota_transport is not None
                or getattr(player, "role_identity", None) is not None
            )
            if (
                enforce_account_quota
                and batch_tradable > 0
                and state.tradable_earned_total > state.stage1_tradable_confirmed
            ):
                if self._account_quota_transport is not None:
                    self._account_quota_transport(player.player_id, state.tradable_earned_total, stamp)
                return reject(UNAVAILABLE, known=False)
            # 按本批可交易份数拆分：可交易优先分配给打造材料（3001），余量给强化石（3002）；其余全绑定，分堆发放永不混堆。
            crafting_tradable = min(request.crafting_material_count, batch_tradable)
            crafting_bound = request.crafting_material_count - crafting_tradable
            stone_tradable = min(request.enhancement_stone_count, batch_tradable - crafting_tradable)
            stone_bound = request.enhancement_stone_count - stone_tradable
            tradable_grants = {}
            if crafting_tradable:
                tradable_grants[CRAFTING_MATERIAL_ID] = crafting_tradable
            if stone_tradable:
                tradable_grants[stone_id] = tradable_grants.get(stone_id, 0) + stone_tradable
            bound_grants = {}
            if crafting_bound:
                bound_grants[CRAFTING_MATERIAL_ID] = crafting_bound
            if stone_bound:
                bound_grants[stone_id] = bound_grants.get(stone_id, 0) + stone_bound
            if tradable_grants:
                grant_err = await self._grant_materials(staged, tradable_grants, gains=gains, bound=False)
                if grant_err:
                    return reject(grant_err)
            if bound_grants:
                grant_err = await self._grant_materials(staged, bound_grants, gains=gains, bound=True)
                if grant_err:
                    return reject(grant_err)
        else:
            grants = {}
            if request.crafting_material_count:
                grants[CRAFTING_MATERIAL_ID] = request.crafting_material_count
            if request.enhancement_stone_count:
                grants[stone_id] = grants.get(stone_id, 0) + request.enhancement_stone_count
            grant_err = await self._grant_materials(staged, grants, gains=gains, bound=False)
            if grant_err:
                return reject(grant_err)

        next_state = _MaterialRewardState(
            day_starts_at_ms=state.day_starts_at_ms,
            earned_today=state.earned_today,
            available_units=state.available_units - total,
            revision=state.revision + 1,
            lifetime_earned=state.lifetime_earned,
            tradable_earned_total=state.tradable_earned_total,
            available_tradable_units=max(0, state.available_tradable_units - batch_tradable),
            week_starts_at_ms=state.week_starts_at_ms,
            earned_this_week=state.earned_this_week,
            migrated_to_v2=state.migrated_to_v2,
            stage1_tradable_confirmed=state.stage1_tradable_confirmed,
            stage2_pool_cap=state.stage2_pool_cap,
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
