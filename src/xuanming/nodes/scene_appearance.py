"""Scene 角色形象衣柜、形象卡永久解锁与一小时免费试用系统（对齐 C++ player_appearance.cpp 与 appearance_rules.h）。

业务规则：
- 永久拥有位图 ``appearance_owned_bits`` 与试用位图 ``appearance_trial_used_bits`` 采用低位在前紧凑编码，
  置位时保留未知高位并去除尾部全 0 字节；
- 已拥有形象（含当前永久形象）可 0 消耗免费切换；未拥有且 ``card_unlockable=1`` 的形象消耗角色形象卡解锁入柜；
- 每个未拥有形象每角色限免费试用 1 次（默认 3600s），可提前取消或到期自动还原并向本人与 AOI 观察者广播。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    BAG_INSUFFICIENT,
    FEATURE_UNAVAILABLE,
    INVALID,
    INVALID_DATA,
    UNAVAILABLE,
    bag_capacity,
    clone,
    held_item_count,
)

logger = logging.getLogger(__name__)

_SERVICE = "SceneAppearanceClientPlayer"
_NOTIFY_ACTOR_APPEARANCE_CHANGED = "SceneAppearanceClientPlayerNotifyActorAppearanceChanged"
_NOTIFY_APPEARANCE_STATE = "SceneAppearanceClientPlayerNotifyAppearanceState"

_SWITCH_APPEARANCE_KIND = 7
_START_TRIAL_KIND = 8
_EQUIP_PET_COSMETIC_KIND = 18
_EQUIP_SKILL_VFX_KIND = 19

MIN_TRIAL_DURATION_SECONDS = 60
MAX_TRIAL_DURATION_SECONDS = 86_400
MAX_CARD_ITEM_COUNT = 99
MAX_MALL_PRICE_FEN = 1_000_000
_UINT32_MAX = 0xFFFF_FFFF

# 对应 C++ appearance_error_tip.pb.h / pet_error_tip.pb.h 错误码。
PET_NOT_FOUND = 26000
APPEARANCE_INVALID = 33000
APPEARANCE_ALREADY_CURRENT = 33001
APPEARANCE_NO_CARD = 33002
APPEARANCE_TRIAL_ALREADY_USED = 33003
APPEARANCE_NO_ACTIVE_TRIAL = 33004
APPEARANCE_FROZEN = 33005
APPEARANCE_UNAVAILABLE = 33006
APPEARANCE_NOT_CARD_UNLOCKABLE = 33007
APPEARANCE_ALREADY_OWNED = 33008
APPEARANCE_NOT_OWNED = 33009


class SwitchPlan(Enum):
    FREE_SWITCH = auto()
    CARD_UNLOCK = auto()
    REFUSE_NOT_OWNED = auto()
    REFUSE_NOT_CARD_UNLOCKABLE = auto()


@dataclass(frozen=True, slots=True)
class AppearanceRuleData:
    """角色形象切换与试用单行规则快照。"""

    card_item_id: int
    card_item_count: int
    trial_duration_seconds: int
    mall_sku_id: int
    mall_price_fen: int


def rule_problem(rule: AppearanceRuleData) -> str | None:
    """校验 AppearanceRule 单行配置；合法返回 ``None``。"""
    if rule.card_item_id == 0:
        return "card_item_id must be non-zero"
    if not 1 <= rule.card_item_count <= MAX_CARD_ITEM_COUNT:
        return "card_item_count out of range"
    if not MIN_TRIAL_DURATION_SECONDS <= rule.trial_duration_seconds <= MAX_TRIAL_DURATION_SECONDS:
        return "trial_duration_seconds out of range"
    if rule.mall_price_fen > MAX_MALL_PRICE_FEN:
        return "mall_price_fen out of range"
    return None


def test_packed_bit(bits: bytes, index: int) -> bool:
    """测试低位在前紧凑位图中 ``index`` 位是否为 1。"""
    if index < 0:
        return False
    byte_idx, bit_offset = divmod(index, 8)
    return byte_idx < len(bits) and bool((bits[byte_idx] >> bit_offset) & 1)


test_packed_bit.__test__ = False  # type: ignore[attr-defined]


def set_packed_bit(bits: bytes, index: int) -> bytes:
    """置位低位在前紧凑位图的 ``index`` 位，保留所有高位未知 bit 并去掉尾部全 0 字节。"""
    if index < 0:
        raise ValueError("位图索引不能为负数")
    byte_idx, bit_offset = divmod(index, 8)
    buf = bytearray(bits)
    if len(buf) <= byte_idx:
        buf.extend(b"\x00" * (byte_idx + 1 - len(buf)))
    buf[byte_idx] |= 1 << bit_offset
    return bytes(buf).rstrip(b"\x00")


def clear_packed_bit(bits: bytes, index: int) -> bytes:
    """清除低位在前紧凑位图的 ``index`` 位，保留其他高位并去掉尾部全 0 字节。"""
    if index < 0:
        raise ValueError("位图索引不能为负数")
    byte_idx, bit_offset = divmod(index, 8)
    if byte_idx >= len(bits):
        return bits.rstrip(b"\x00")
    buf = bytearray(bits)
    buf[byte_idx] &= ~(1 << bit_offset)
    return bytes(buf).rstrip(b"\x00")


def is_trial_active(trial_appearance_id: str, trial_expires_at: int, now_sec: int) -> bool:
    """判断当前是否处于有效试用期（到期时刻 <= now 视为过期）。"""
    return bool(trial_appearance_id) and trial_expires_at > now_sec


def effective_appearance_id(data, now_sec: int) -> str:
    """返回此刻实际对外展示的外观 ID：试用有效取试用形象，否则取永久形象。"""
    profile = data.profile_component
    if is_trial_active(profile.trial_appearance_id, int(profile.trial_expires_at), now_sec):
        return profile.trial_appearance_id
    return profile.appearance_id


def plan_switch(already_owned: bool, expect_free_switch: bool, card_unlockable: bool) -> SwitchPlan:
    """判定永久形象切换计划：免费切换、扣形象卡买断或 fail-closed 拒绝。"""
    if already_owned:
        return SwitchPlan.FREE_SWITCH
    if expect_free_switch:
        return SwitchPlan.REFUSE_NOT_OWNED
    if not card_unlockable:
        return SwitchPlan.REFUSE_NOT_CARD_UNLOCKABLE
    return SwitchPlan.CARD_UNLOCK


def is_owned_appearance(
    owned_bits: bytes,
    bit_index: int | None,
    permanent_appearance_id: str,
    candidate_appearance_id: str,
) -> bool:
    """查询角色是否已永久拥有目标形象（含当前永久形象的零迁移兼容）。"""
    if candidate_appearance_id and candidate_appearance_id == permanent_appearance_id:
        return True
    return bit_index is not None and test_packed_bit(owned_bits, bit_index)


def is_blank_text(value: str) -> bool:
    """判断字符串是否为空或仅含 ASCII 空白符（对齐 C++ appearance_rules.h）。"""
    return not value or value.strip(" \t\r\n") == ""


def biography_fields_valid(
    archive_name: str,
    nickname: str,
    biography_summary: str,
    story_title: str,
    story_body: str,
) -> bool:
    """人物志字段必须全空或四项核心字段（档案名/小传/篇名/正文）同时非空，禁止半残行上线。"""
    has_archive = not is_blank_text(archive_name)
    has_nickname = not is_blank_text(nickname)
    has_summary = not is_blank_text(biography_summary)
    has_title = not is_blank_text(story_title)
    has_body = not is_blank_text(story_body)
    if not (has_archive or has_nickname or has_summary or has_title or has_body):
        return True
    return has_archive and has_summary and has_title and has_body


def is_valid_tint_hex(value: str) -> bool:
    """特效色调覆盖必须为空串或严格 #RRGGBB。"""
    if not value:
        return True
    if len(value) != 7 or value[0] != "#":
        return False
    return all(ch in "0123456789abcdefABCDEF" for ch in value[1:])


def _cosmetic_bit_index(tables, table_name: str, config_id: int) -> int | None:
    try:
        table = tables[table_name]
    except KeyError:
        return None
    try:
        return table.bit_index(config_id)
    except KeyError:
        return None


def resolve_pet_cosmetic_visual(tables, pet_owned_bits: bytes, pet) -> tuple[int, str]:
    """解析灵宠当前权威幻化展示 ID 与资源键；未拥有/停用/不适配一律回落原形 (0, '')。"""
    cosmetic_id = int(getattr(pet, "cosmetic_id", 0))
    if cosmetic_id <= 0:
        return 0, ""
    try:
        pet_cosmetic_table = tables["PetCosmetic"]
    except KeyError:
        return 0, ""
    row = pet_cosmetic_table.get(cosmetic_id)
    if row is None or not getattr(row, "enabled", 0):
        return 0, ""
    bit_idx = _cosmetic_bit_index(tables, "PetCosmetic", int(row.id))
    if bit_idx is None or not test_packed_bit(pet_owned_bits, bit_idx):
        return 0, ""
    target_pet_id = int(getattr(row, "target_pet_table_id", 0))
    if target_pet_id and target_pet_id != int(pet.pet_table_id):
        return 0, ""
    art_key = str(getattr(row, "visual_art_key", "") or "")
    return int(row.id), art_key


def fill_battle_skill_vfx(tables, data, snapshot) -> None:
    """将角色已拥有且启用的技能特效透传至 BattlePlayerSnapshot（纯战斗画面零数值）。"""
    if not data.HasField("profile_component"):
        return
    try:
        vfx_table = tables["SkillVfxCosmetic"]
    except KeyError:
        return
    profile = data.profile_component
    vfx_bits = bytes(getattr(profile, "skill_vfx_owned_bits", b""))
    for skill_id, cosmetic_id in dict(getattr(profile, "equipped_skill_vfx", {})).items():
        if not skill_id or not cosmetic_id:
            continue
        row = vfx_table.get(int(cosmetic_id))
        if row is None or not getattr(row, "enabled", 0) or int(row.skill_table_id) != int(skill_id):
            continue
        bit_idx = _cosmetic_bit_index(tables, "SkillVfxCosmetic", int(row.id))
        if bit_idx is None or not test_packed_bit(vfx_bits, bit_idx):
            continue
        snapshot.equipped_skill_vfx[int(skill_id)] = int(cosmetic_id)


class SceneAppearance:
    """角色形象衣柜、形象卡永久解锁与 1 小时免费试用处理器。"""

    def __init__(
        self,
        scene,
        tables,
        assets,
        *,
        now_s: Callable[[], int] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.now_s = now_s or (lambda: time.time_ns() // 1_000_000_000)
        self._active_trials: dict[int, int] = {}
        tx_enum = self.protocol.pool.FindEnumTypeByName("TransactionType")
        switch_tx = tx_enum.values_by_name.get("TX_APPEARANCE_SWITCH") or tx_enum.values_by_name["TX_ITEM_DESTROY"]
        self.switch_tx = switch_tx.number

    def register(self) -> None:
        self.scene.register(_SERVICE, "GetAppearanceInfo", self.get_info)
        self.scene.register(_SERVICE, "SwitchAppearance", self.switch_appearance, mutating=True)
        self.scene.register(_SERVICE, "StartAppearanceTrial", self.start_trial, mutating=True)
        self.scene.register(_SERVICE, "CancelAppearanceTrial", self.cancel_trial, mutating=True)
        self.scene.register(_SERVICE, "EquipPetCosmetic", self.equip_pet_cosmetic, mutating=True)
        self.scene.register(_SERVICE, "EquipSkillVfx", self.equip_skill_vfx, mutating=True)

    def _table(self, name: str):
        try:
            return self.tables[name]
        except KeyError:
            return None

    def _bit_index(self, config_id: int) -> int | None:
        table = self._table("CharacterAppearance")
        if table is None:
            return None
        try:
            return table.bit_index(config_id)
        except KeyError:
            return None

    def current_rule(self) -> tuple[AppearanceRuleData | None, str | None]:
        table = self._table("AppearanceRule")
        rows = table.rows() if table is not None else ()
        if not rows:
            return None, "AppearanceRule table is empty"
        row = rows[0]
        rule = AppearanceRuleData(
            card_item_id=int(row.card_item_id),
            card_item_count=int(row.card_item_count),
            trial_duration_seconds=int(row.trial_duration_seconds),
            mall_sku_id=int(row.mall_sku_id),
            mall_price_fen=int(row.mall_price_fen),
        )
        problem = rule_problem(rule)
        if problem is not None:
            return None, problem
        return rule, None

    def validate_tables(self) -> bool:
        """校验 AppearanceRule / CharacterAppearance / AppearanceSeries 配表与人物志完整性。"""
        rule, problem = self.current_rule()
        if rule is None:
            logger.error("Appearance table validation failed: %s", problem)
            return False
        table = self._table("CharacterAppearance")
        if table is None:
            logger.error("Appearance table validation failed: CharacterAppearance missing")
            return False
        series_table = self._table("AppearanceSeries")
        enabled_series = {
            int(row.id) for row in (series_table.rows() if series_table is not None else ())
            if getattr(row, "enabled", 0)
        }
        seen_ids: set[str] = set()
        for row in table.rows():
            if not row.id or not row.appearance_id or row.appearance_id in seen_ids:
                logger.error("Appearance table validation failed: invalid or duplicate appearance id=%s", row.id)
                return False
            seen_ids.add(row.appearance_id)
            if self._bit_index(int(row.id)) is None:
                logger.error("Appearance table validation failed: missing bit_index for id=%s", row.id)
                return False
            if not biography_fields_valid(
                str(getattr(row, "archive_name", "")),
                str(getattr(row, "nickname", "")),
                str(getattr(row, "biography_summary", "")),
                str(getattr(row, "story_title", "")),
                str(getattr(row, "story_body", "")),
            ):
                logger.error("Appearance table validation failed: incomplete biography fields for id=%s", row.id)
                return False
            if getattr(row, "enabled", 0):
                series_id = int(getattr(row, "series_id", 0))
                if series_id and series_id not in enabled_series:
                    logger.error("Appearance table validation failed: missing enabled series_id=%s for id=%s", series_id, row.id)
                    return False

        item_table = self._table("Item")
        pet_table = self._table("Pet")
        skill_table = self._table("Skill")
        shop_table = self._table("SpiritShopGoods")
        pet_cosmetic_table = self._table("PetCosmetic")
        if pet_cosmetic_table is not None:
            seen_pet_keys: set[str] = set()
            for row in pet_cosmetic_table.rows():
                if not row.id or is_blank_text(str(row.cosmetic_key)) or is_blank_text(str(row.name)):
                    logger.error("PetCosmetic table validation failed: invalid row id=%s", row.id)
                    return False
                if _cosmetic_bit_index(self.tables, "PetCosmetic", int(row.id)) is None:
                    logger.error("PetCosmetic table validation failed: missing bit_index id=%s", row.id)
                    return False
                if row.target_pet_table_id and pet_table is not None and pet_table.get(int(row.target_pet_table_id)) is None:
                    return False
                if not row.visual_pet_table_id and is_blank_text(str(row.visual_art_key)):
                    return False
                if row.visual_pet_table_id and pet_table is not None and pet_table.get(int(row.visual_pet_table_id)) is None:
                    return False
                card_id = int(getattr(row, "card_item_id", 0))
                card_cnt = int(getattr(row, "card_item_count", 0))
                if card_id:
                    if (item_table is not None and item_table.get(card_id) is None) or not (1 <= card_cnt <= MAX_CARD_ITEM_COUNT):
                        return False
                elif card_cnt:
                    return False
                goods_id = int(getattr(row, "spirit_shop_goods_id", 0))
                if goods_id and shop_table is not None:
                    goods_row = shop_table.get(goods_id)
                    if goods_row is None or (card_id and int(goods_row.item_id_1) != card_id):
                        return False
                if row.enabled:
                    if row.cosmetic_key in seen_pet_keys:
                        return False
                    seen_pet_keys.add(row.cosmetic_key)

        vfx_table = self._table("SkillVfxCosmetic")
        if vfx_table is not None:
            seen_vfx_keys: set[str] = set()
            for row in vfx_table.rows():
                if not row.id or is_blank_text(str(row.vfx_key)) or is_blank_text(str(row.name)) or is_blank_text(str(row.fx_id)):
                    return False
                if _cosmetic_bit_index(self.tables, "SkillVfxCosmetic", int(row.id)) is None:
                    return False
                if skill_table is not None and skill_table.get(int(row.skill_table_id)) is None:
                    return False
                if not is_valid_tint_hex(str(getattr(row, "tint_hex", ""))):
                    return False
                card_id = int(getattr(row, "card_item_id", 0))
                card_cnt = int(getattr(row, "card_item_count", 0))
                if card_id:
                    if (item_table is not None and item_table.get(card_id) is None) or not (1 <= card_cnt <= MAX_CARD_ITEM_COUNT):
                        return False
                elif card_cnt:
                    return False
                goods_id = int(getattr(row, "spirit_shop_goods_id", 0))
                if goods_id and shop_table is not None:
                    goods_row = shop_table.get(goods_id)
                    if goods_row is None or (card_id and int(goods_row.item_id_1) != card_id):
                        return False
                if row.enabled:
                    if row.vfx_key in seen_vfx_keys:
                        return False
                    seen_vfx_keys.add(row.vfx_key)
        return True

    def _find_enabled_appearance(self, appearance_id: str):
        table = self._table("CharacterAppearance")
        if not appearance_id or table is None:
            return None
        for row in table.rows():
            if row.enabled and row.appearance_id == appearance_id:
                return row
        return None

    def _find_config_id_any(self, appearance_id: str) -> int:
        table = self._table("CharacterAppearance")
        if not appearance_id or table is None:
            return 0
        for row in table.rows():
            if row.appearance_id == appearance_id:
                return int(row.id)
        return 0

    def _check_mutability(self, player) -> int:
        if self.scene.mutable(player):
            return 0
        if not player.snapshot.owner_epoch:
            return UNAVAILABLE
        return APPEARANCE_FROZEN

    def _reject(self, route, tip: int):
        response = self.protocol.message(route.response_type)
        response.error_message.id = tip
        return response

    def _owned_card_count(self, data, card_item_id: int) -> int:
        if not card_item_id:
            return 0
        return min(_UINT32_MAX, self.assets.removable_count(data, card_item_id))

    def _consume_card(self, data, card_item_id: int, count: int) -> int:
        """从人物背包扣除指定数量的形象卡，支持配表已同步或尚未同步 Item 行的两种情形。"""
        if self.tables["Item"].get(card_item_id) is not None:
            return self.assets.consume_items(data, {card_item_id: count})
        # 配表尚未同步 card_item_id 时，复用与 AssetInventory.consume_items 一致的背包扣除与预留校验。
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return INVALID_DATA
        held = held_item_count(data, card_item_id)
        known = sum(item.size for _, item in entries if item.config_id == card_item_id and self.assets.countable(item))
        if held and known >= count and count > max(0, known - held):
            return BAG_INSUFFICIENT
        taken, version_skew = self.assets._drain(entries, card_item_id, count)
        if taken != count and not version_skew:
            return BAG_INSUFFICIENT
        if version_skew:
            return FEATURE_UNAVAILABLE
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0

    def fill_info(self, data, now_sec: int, info) -> None:
        """按权威存档与当前配置表填充 ``PlayerAppearanceInfo``（只读，不产生脏写）。"""
        info.Clear()
        info.server_now = int(now_sec)

        profile = data.profile_component
        permanent = profile.appearance_id
        trial_active = is_trial_active(profile.trial_appearance_id, int(profile.trial_expires_at), now_sec)
        info.permanent_appearance_id = permanent
        info.active_appearance_id = profile.trial_appearance_id if trial_active else permanent
        if trial_active:
            info.trial_appearance_id = profile.trial_appearance_id
            info.trial_expires_at = int(profile.trial_expires_at)

        trial_bits = bytes(profile.appearance_trial_used_bits)
        info.trial_used_bits = trial_bits

        effective_owned = bytes(profile.appearance_owned_bits)
        permanent_cfg_id = self._find_config_id_any(permanent)
        if permanent_cfg_id:
            perm_bit = self._bit_index(permanent_cfg_id)
            if perm_bit is not None:
                effective_owned = set_packed_bit(effective_owned, perm_bit)
        info.owned_bits = effective_owned

        rule, _ = self.current_rule()
        if rule is not None:
            info.card_item_id = rule.card_item_id
            info.card_item_count = rule.card_item_count
            info.owned_card_count = self._owned_card_count(data, rule.card_item_id)
            info.trial_duration_seconds = rule.trial_duration_seconds
            info.mall_sku_id = rule.mall_sku_id
            info.mall_price_fen = rule.mall_price_fen

        entries = []
        appearances_table = self._table("CharacterAppearance")
        if appearances_table is not None:
            for row in appearances_table.rows():
                if not row.enabled:
                    continue
                bit_idx = self._bit_index(int(row.id))
                entries.append((
                    int(row.sort_order),
                    int(row.id),
                    row.appearance_id,
                    row.name,
                    bit_idx is not None and test_packed_bit(trial_bits, bit_idx),
                    is_owned_appearance(effective_owned, bit_idx, permanent, row.appearance_id),
                    bool(row.card_unlockable),
                    int(row.series_id),
                ))
        entries.sort(key=lambda item: (item[0], item[1]))
        for sort_order, cfg_id, app_id, name, trial_used, owned, card_unlockable, series_id in entries:
            info.appearances.add(
                config_id=cfg_id,
                appearance_id=app_id,
                name=name,
                sort_order=sort_order,
                trial_used=trial_used,
                owned=owned,
                card_unlockable=card_unlockable,
                series_id=series_id,
            )

        series_table = self._table("AppearanceSeries")
        if series_table is not None:
            series_map: dict[int, dict] = {}
            for row in series_table.rows():
                if not row.enabled:
                    continue
                series_map[int(row.id)] = {
                    "series_id": int(row.id),
                    "series_key": row.series_key,
                    "name": row.name,
                    "sort_order": int(row.sort_order),
                    "completion_badge_name": row.completion_badge_name,
                    "epilogue_title": row.epilogue_title,
                    "owned_count": 0,
                    "total_count": 0,
                    "members": [],
                }
            for _, _, app_id, _, _, owned, _, series_id in entries:
                agg = series_map.get(series_id)
                if agg is None:
                    continue
                agg["total_count"] += 1
                if owned:
                    agg["owned_count"] += 1
                agg["members"].append(app_id)
            for agg in sorted(series_map.values(), key=lambda item: (item["sort_order"], item["series_id"])):
                if not agg["total_count"]:
                    continue
                info.series_progress.add(
                    series_id=agg["series_id"],
                    series_key=agg["series_key"],
                    name=agg["name"],
                    sort_order=agg["sort_order"],
                    owned_count=agg["owned_count"],
                    total_count=agg["total_count"],
                    completed=agg["total_count"] > 0 and agg["owned_count"] >= agg["total_count"],
                    completion_badge_name=agg["completion_badge_name"],
                    epilogue_title=agg["epilogue_title"],
                    member_appearance_ids=agg["members"],
                )

        # 灵宠幻化目录与当前实例穿戴快照（纯外观零数值）
        pet_bits = bytes(getattr(profile, "pet_cosmetic_owned_bits", b""))
        info.pet_cosmetic_owned_bits = pet_bits
        pet_cosmetic_table = self._table("PetCosmetic")
        if pet_cosmetic_table is not None:
            pet_rows = [row for row in pet_cosmetic_table.rows() if getattr(row, "enabled", 0)]
            pet_rows.sort(key=lambda r: (int(r.sort_order), int(r.id)))
            for row in pet_rows:
                bit_idx = _cosmetic_bit_index(self.tables, "PetCosmetic", int(row.id))
                card_id = int(getattr(row, "card_item_id", 0))
                info.pet_cosmetics.add(
                    config_id=int(row.id),
                    cosmetic_key=str(row.cosmetic_key),
                    name=str(row.name),
                    target_pet_table_id=int(row.target_pet_table_id),
                    visual_pet_table_id=int(row.visual_pet_table_id),
                    visual_art_key=str(row.visual_art_key),
                    sort_order=int(row.sort_order),
                    owned=bit_idx is not None and test_packed_bit(pet_bits, bit_idx),
                    mall_sku_id=int(row.mall_sku_id),
                    acquire_hint=str(row.acquire_hint),
                    desc=str(row.desc),
                    card_item_id=card_id,
                    card_item_count=int(getattr(row, "card_item_count", 0)),
                    owned_card_count=self._owned_card_count(data, card_id) if card_id else 0,
                    spirit_shop_goods_id=int(getattr(row, "spirit_shop_goods_id", 0)),
                )
        if data.HasField("pet_component"):
            for pet in data.pet_component.pets:
                resolved_id, _ = resolve_pet_cosmetic_visual(self.tables, pet_bits, pet)
                info.equipped_pet_cosmetics.add(pet_id=int(pet.pet_id), cosmetic_id=resolved_id)

        # 技能特效目录与当前启用映射（纯战斗画面零数值）
        vfx_bits = bytes(getattr(profile, "skill_vfx_owned_bits", b""))
        info.skill_vfx_owned_bits = vfx_bits
        vfx_table = self._table("SkillVfxCosmetic")
        if vfx_table is not None:
            vfx_rows = [row for row in vfx_table.rows() if getattr(row, "enabled", 0)]
            vfx_rows.sort(key=lambda r: (int(r.sort_order), int(r.id)))
            for row in vfx_rows:
                bit_idx = _cosmetic_bit_index(self.tables, "SkillVfxCosmetic", int(row.id))
                card_id = int(getattr(row, "card_item_id", 0))
                info.skill_vfx_cosmetics.add(
                    config_id=int(row.id),
                    vfx_key=str(row.vfx_key),
                    name=str(row.name),
                    skill_table_id=int(row.skill_table_id),
                    fx_id=str(row.fx_id),
                    projectile_fx_id=str(row.projectile_fx_id),
                    telegraph_fx_id=str(row.telegraph_fx_id),
                    tint_hex=str(row.tint_hex),
                    sort_order=int(row.sort_order),
                    owned=bit_idx is not None and test_packed_bit(vfx_bits, bit_idx),
                    mall_sku_id=int(row.mall_sku_id),
                    acquire_hint=str(row.acquire_hint),
                    desc=str(row.desc),
                    card_item_id=card_id,
                    card_item_count=int(getattr(row, "card_item_count", 0)),
                    owned_card_count=self._owned_card_count(data, card_id) if card_id else 0,
                    spirit_shop_goods_id=int(getattr(row, "spirit_shop_goods_id", 0)),
                )
        if vfx_table is not None:
            for skill_id, cosmetic_id in dict(getattr(profile, "equipped_skill_vfx", {})).items():
                if not skill_id or not cosmetic_id:
                    continue
                row = vfx_table.get(int(cosmetic_id))
                if row is None or not getattr(row, "enabled", 0) or int(row.skill_table_id) != int(skill_id):
                    continue
                bit_idx = _cosmetic_bit_index(self.tables, "SkillVfxCosmetic", int(row.id))
                if bit_idx is not None and test_packed_bit(vfx_bits, bit_idx):
                    info.equipped_skill_vfx[int(skill_id)] = int(cosmetic_id)

    def _broadcast_appearance_changed(
        self,
        world,
        player,
        appearance_id: str,
        *,
        is_trial: bool,
        trial_expires_at: int,
    ) -> None:
        message = self.protocol.message(
            "ActorAppearanceChangedS2C",
            entity=player.entity,
            player_id=player.player_id,
            appearance_id=appearance_id,
            is_trial=is_trial,
            trial_expires_at=trial_expires_at if is_trial else 0,
        )
        self.scene._push(player, _NOTIFY_ACTOR_APPEARANCE_CHANGED, message)
        if world is not None:
            self.scene._push_many(self.scene.watchers(world, player), _NOTIFY_ACTOR_APPEARANCE_CHANGED, message)

    def _push_state_notify(self, player, data, now_sec: int, *, trial_expired: bool) -> None:
        notify = self.protocol.message("AppearanceStateNotify", trial_expired=trial_expired)
        self.fill_info(data, now_sec, notify.info)
        self.scene._push(player, _NOTIFY_APPEARANCE_STATE, notify)

    def get_info(self, _world, player, route, _request):
        data = self.scene.rows.assemble(player.snapshot)
        response = self.protocol.message(route.response_type)
        self.fill_info(data, self.now_s(), response.info)
        return response

    async def switch_appearance(self, world, player, route, request):
        now_sec = self.now_s()
        if not request.client_request_id:
            return self._reject(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _SWITCH_APPEARANCE_KIND)
        if error:
            return self._reject(route, error)
        if prior is not None:
            response = self.protocol.message(route.response_type, replayed=True)
            self.fill_info(data, now_sec, response.info)
            return response

        rule, problem = self.current_rule()
        if rule is None:
            logger.error(
                "Appearance switch refused: AppearanceRule unavailable (%s) player_id=%d",
                problem,
                player.player_id,
            )
            return self._reject(route, APPEARANCE_UNAVAILABLE)

        if mut_tip := self._check_mutability(player):
            return self._reject(route, mut_tip)

        target = self._find_enabled_appearance(request.appearance_id)
        if target is None:
            return self._reject(route, APPEARANCE_INVALID)
        target_bit = self._bit_index(int(target.id))
        if target_bit is None:
            logger.error(
                "Appearance switch refused: missing bit_index config_id=%d player_id=%d",
                target.id,
                player.player_id,
            )
            return self._reject(route, APPEARANCE_UNAVAILABLE)

        profile = data.profile_component
        if profile.appearance_id == request.appearance_id:
            return self._reject(route, APPEARANCE_ALREADY_CURRENT)

        already_owned = is_owned_appearance(
            bytes(profile.appearance_owned_bits), target_bit, profile.appearance_id, request.appearance_id,
        )
        plan = plan_switch(already_owned, bool(request.expect_free_switch), bool(target.card_unlockable))
        if plan is SwitchPlan.REFUSE_NOT_OWNED:
            return self._reject(route, APPEARANCE_NOT_OWNED)
        if plan is SwitchPlan.REFUSE_NOT_CARD_UNLOCKABLE:
            return self._reject(route, APPEARANCE_NOT_CARD_UNLOCKABLE)

        consume_card = plan is SwitchPlan.CARD_UNLOCK
        before_data = clone(data) if consume_card and self.assets.audit_enabled else None
        if consume_card:
            if self._owned_card_count(data, rule.card_item_id) < rule.card_item_count:
                return self._reject(route, APPEARANCE_NO_CARD)
            consume_err = self._consume_card(data, rule.card_item_id, rule.card_item_count)
            if consume_err == BAG_INSUFFICIENT:
                return self._reject(route, APPEARANCE_NO_CARD)
            if consume_err:
                logger.error(
                    "Appearance switch refused: card consumption failed player_id=%d item_id=%d err=%d",
                    player.player_id,
                    rule.card_item_id,
                    consume_err,
                )
                return self._reject(route, APPEARANCE_UNAVAILABLE)

        before_permanent = profile.appearance_id
        owned_bits = bytes(profile.appearance_owned_bits)
        before_cfg_id = self._find_config_id_any(before_permanent)
        if before_cfg_id:
            before_bit = self._bit_index(before_cfg_id)
            if before_bit is not None:
                owned_bits = set_packed_bit(owned_bits, before_bit)
        owned_bits = set_packed_bit(owned_bits, target_bit)

        profile.appearance_owned_bits = owned_bits
        profile.appearance_id = request.appearance_id
        profile.trial_appearance_id = ""
        profile.trial_expires_at = 0
        profile.appearance_switch_seq += 1
        client_request.remember(ledger, request.client_request_id, _SWITCH_APPEARANCE_KIND, fingerprint)

        audit = (
            self.assets.make_audit_callback(
                player, before_data, data, self.switch_tx, correlation=request.client_request_id,
            )
            if consume_card
            else None
        )
        try:
            await self.scene.commit_player_data(player, data, on_committed=audit)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("Appearance switch save failed player_id=%d", player.player_id, exc_info=True)
            return self._reject(route, UNAVAILABLE)

        self._active_trials.pop(player.player_id, None)
        logger.info(
            "Appearance switched player_id=%d before=%s after=%s free_switch=%d client_request_id=%d",
            player.player_id,
            before_permanent,
            request.appearance_id,
            int(not consume_card),
            request.client_request_id,
        )
        self._broadcast_appearance_changed(world, player, request.appearance_id, is_trial=False, trial_expires_at=0)
        response = self.protocol.message(route.response_type)
        self.fill_info(data, now_sec, response.info)
        return response

    async def start_trial(self, world, player, route, request):
        now_sec = self.now_s()
        if not request.client_request_id:
            return self._reject(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _START_TRIAL_KIND)
        if error:
            return self._reject(route, error)
        if prior is not None:
            response = self.protocol.message(route.response_type, replayed=True)
            self.fill_info(data, now_sec, response.info)
            return response

        rule, problem = self.current_rule()
        if rule is None:
            logger.error(
                "Appearance trial start refused: AppearanceRule unavailable (%s) player_id=%d",
                problem,
                player.player_id,
            )
            return self._reject(route, APPEARANCE_UNAVAILABLE)

        if mut_tip := self._check_mutability(player):
            return self._reject(route, mut_tip)

        target = self._find_enabled_appearance(request.appearance_id)
        if target is None:
            return self._reject(route, APPEARANCE_INVALID)
        target_bit = self._bit_index(int(target.id))
        if target_bit is None:
            logger.error(
                "Appearance trial start refused: missing bit_index config_id=%d player_id=%d",
                target.id,
                player.player_id,
            )
            return self._reject(route, APPEARANCE_UNAVAILABLE)

        profile = data.profile_component
        if profile.appearance_id == request.appearance_id:
            return self._reject(route, APPEARANCE_ALREADY_CURRENT)
        if is_owned_appearance(
            bytes(profile.appearance_owned_bits), target_bit, profile.appearance_id, request.appearance_id,
        ):
            return self._reject(route, APPEARANCE_ALREADY_OWNED)
        if test_packed_bit(bytes(profile.appearance_trial_used_bits), target_bit):
            return self._reject(route, APPEARANCE_TRIAL_ALREADY_USED)

        expires_at = now_sec + rule.trial_duration_seconds
        profile.appearance_trial_used_bits = set_packed_bit(bytes(profile.appearance_trial_used_bits), target_bit)
        profile.trial_appearance_id = request.appearance_id
        profile.trial_expires_at = expires_at
        client_request.remember(ledger, request.client_request_id, _START_TRIAL_KIND, fingerprint)

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("Appearance trial start save failed player_id=%d", player.player_id, exc_info=True)
            return self._reject(route, UNAVAILABLE)

        self._active_trials[player.player_id] = expires_at
        logger.info(
            "Appearance trial started player_id=%d appearance_id=%s expires_at=%d client_request_id=%d",
            player.player_id,
            request.appearance_id,
            expires_at,
            request.client_request_id,
        )
        self._broadcast_appearance_changed(
            world, player, request.appearance_id, is_trial=True, trial_expires_at=expires_at,
        )
        response = self.protocol.message(route.response_type)
        self.fill_info(data, now_sec, response.info)
        return response

    async def cancel_trial(self, world, player, route, _request):
        now_sec = self.now_s()
        if mut_tip := self._check_mutability(player):
            return self._reject(route, mut_tip)
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        if not is_trial_active(profile.trial_appearance_id, int(profile.trial_expires_at), now_sec):
            return self._reject(route, APPEARANCE_NO_ACTIVE_TRIAL)

        cancelled = profile.trial_appearance_id
        profile.trial_appearance_id = ""
        profile.trial_expires_at = 0

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("Appearance trial cancel save failed player_id=%d", player.player_id, exc_info=True)
            return self._reject(route, UNAVAILABLE)

        self._active_trials.pop(player.player_id, None)
        logger.info(
            "Appearance trial cancelled player_id=%d trial_appearance_id=%s reverted_to=%s",
            player.player_id,
            cancelled,
            profile.appearance_id,
        )
        self._broadcast_appearance_changed(world, player, profile.appearance_id, is_trial=False, trial_expires_at=0)
        response = self.protocol.message(route.response_type)
        self.fill_info(data, now_sec, response.info)
        return response

    def _player_has_usable_skill(self, data, skill_table_id: int) -> bool:
        if not skill_table_id:
            return False
        skill_table = self._table("Skill")
        row = skill_table.get(int(skill_table_id)) if skill_table is not None else None
        if row is None:
            return False
        skill_class = int(getattr(row, "class_id", 0))
        if skill_class:
            player_class = (
                int(getattr(data.uint32_pb_component, "class", 0))
                if data.HasField("uint32_pb_component")
                else 0
            )
            if player_class and player_class != skill_class:
                return False
        if int(getattr(row, "season_id", 0)) != 0:
            return True
        if not data.HasField("skill_list"):
            return False
        for entry in data.skill_list.skill_list:
            if int(entry.skill_table_id) != int(skill_table_id):
                continue
            if skill_class == 0 or int(entry.rank) > 0:
                return True
        return False

    async def equip_pet_cosmetic(self, _world, player, route, request):
        now_sec = self.now_s()
        if not request.client_request_id or not request.pet_id:
            return self._reject(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _EQUIP_PET_COSMETIC_KIND)
        if error:
            return self._reject(route, error)
        if prior is not None:
            response = self.protocol.message(
                route.response_type,
                pet_id=request.pet_id,
                equipped_cosmetic_id=request.cosmetic_id,
                replayed=True,
            )
            self.fill_info(data, now_sec, response.info)
            return response

        if mut_tip := self._check_mutability(player):
            return self._reject(route, mut_tip)

        pet = next((p for p in data.pet_component.pets if int(p.pet_id) == int(request.pet_id)), None)
        if pet is None:
            return self._reject(route, PET_NOT_FOUND)

        target_id = int(request.cosmetic_id)
        profile = data.profile_component
        consume_voucher = False
        voucher_item_id = 0
        voucher_item_count = 0
        target_bit = None
        if target_id != 0:
            pet_cosmetic_table = self._table("PetCosmetic")
            row = pet_cosmetic_table.get(target_id) if pet_cosmetic_table is not None else None
            if row is None or not getattr(row, "enabled", 0):
                return self._reject(route, APPEARANCE_INVALID)
            target_bit = _cosmetic_bit_index(self.tables, "PetCosmetic", int(row.id))
            if target_bit is None:
                return self._reject(route, APPEARANCE_UNAVAILABLE)
            target_pet_id = int(getattr(row, "target_pet_table_id", 0))
            if target_pet_id and target_pet_id != int(pet.pet_table_id):
                return self._reject(route, APPEARANCE_INVALID)
            already_owned = test_packed_bit(bytes(profile.pet_cosmetic_owned_bits), target_bit)
            voucher_item_id = int(getattr(row, "card_item_id", 0))
            voucher_item_count = int(getattr(row, "card_item_count", 0))
            plan = plan_switch(already_owned, bool(request.expect_free_equip), voucher_item_id > 0 and voucher_item_count > 0)
            if plan is SwitchPlan.REFUSE_NOT_OWNED:
                return self._reject(route, APPEARANCE_NOT_OWNED)
            if plan is SwitchPlan.REFUSE_NOT_CARD_UNLOCKABLE:
                return self._reject(route, APPEARANCE_NOT_CARD_UNLOCKABLE)
            consume_voucher = plan is SwitchPlan.CARD_UNLOCK

        if consume_voucher:
            before_data = clone(data) if self.assets.audit_enabled else None
            if self._owned_card_count(data, voucher_item_id) < voucher_item_count:
                return self._reject(route, APPEARANCE_NO_CARD)
            consume_err = self._consume_card(data, voucher_item_id, voucher_item_count)
            if consume_err == BAG_INSUFFICIENT:
                return self._reject(route, APPEARANCE_NO_CARD)
            if consume_err:
                return self._reject(route, APPEARANCE_UNAVAILABLE)
            profile.pet_cosmetic_owned_bits = set_packed_bit(bytes(profile.pet_cosmetic_owned_bits), target_bit)
            pet.cosmetic_id = target_id
            client_request.remember(ledger, request.client_request_id, _EQUIP_PET_COSMETIC_KIND, fingerprint)
            audit = self.assets.make_audit_callback(
                player, before_data, data, self.switch_tx, correlation=request.client_request_id,
            )
            try:
                await self.scene.commit_player_data(player, data, on_committed=audit)
            except (StoreRejected, StoreUnavailable, OSError):
                return self._reject(route, UNAVAILABLE)
            self._push_state_notify(player, data, now_sec, trial_expired=False)
            response = self.protocol.message(
                route.response_type, pet_id=int(pet.pet_id), equipped_cosmetic_id=target_id, replayed=False,
            )
            self.fill_info(data, now_sec, response.info)
            return response

        if int(pet.cosmetic_id) == target_id:
            response = self.protocol.message(
                route.response_type, pet_id=int(pet.pet_id), equipped_cosmetic_id=target_id, replayed=True,
            )
            self.fill_info(data, now_sec, response.info)
            return response

        pet.cosmetic_id = target_id
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._reject(route, UNAVAILABLE)
        self._push_state_notify(player, data, now_sec, trial_expired=False)
        response = self.protocol.message(
            route.response_type, pet_id=int(pet.pet_id), equipped_cosmetic_id=target_id, replayed=False,
        )
        self.fill_info(data, now_sec, response.info)
        return response

    async def equip_skill_vfx(self, _world, player, route, request):
        now_sec = self.now_s()
        if not request.client_request_id or not request.skill_table_id:
            return self._reject(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _EQUIP_SKILL_VFX_KIND)
        if error:
            return self._reject(route, error)
        if prior is not None:
            response = self.protocol.message(
                route.response_type,
                skill_table_id=request.skill_table_id,
                equipped_cosmetic_id=request.cosmetic_id,
                replayed=True,
            )
            self.fill_info(data, now_sec, response.info)
            return response

        if mut_tip := self._check_mutability(player):
            return self._reject(route, mut_tip)

        skill_table_id = int(request.skill_table_id)
        target_id = int(request.cosmetic_id)
        profile = data.profile_component
        consume_voucher = False
        voucher_item_id = 0
        voucher_item_count = 0
        target_bit = None
        if target_id != 0:
            vfx_table = self._table("SkillVfxCosmetic")
            row = vfx_table.get(target_id) if vfx_table is not None else None
            if row is None or not getattr(row, "enabled", 0) or int(row.skill_table_id) != skill_table_id:
                return self._reject(route, APPEARANCE_INVALID)
            target_bit = _cosmetic_bit_index(self.tables, "SkillVfxCosmetic", int(row.id))
            if target_bit is None:
                return self._reject(route, APPEARANCE_UNAVAILABLE)
            if not self._player_has_usable_skill(data, skill_table_id):
                return self._reject(route, APPEARANCE_INVALID)
            already_owned = test_packed_bit(bytes(profile.skill_vfx_owned_bits), target_bit)
            voucher_item_id = int(getattr(row, "card_item_id", 0))
            voucher_item_count = int(getattr(row, "card_item_count", 0))
            plan = plan_switch(already_owned, bool(request.expect_free_equip), voucher_item_id > 0 and voucher_item_count > 0)
            if plan is SwitchPlan.REFUSE_NOT_OWNED:
                return self._reject(route, APPEARANCE_NOT_OWNED)
            if plan is SwitchPlan.REFUSE_NOT_CARD_UNLOCKABLE:
                return self._reject(route, APPEARANCE_NOT_CARD_UNLOCKABLE)
            consume_voucher = plan is SwitchPlan.CARD_UNLOCK

        if consume_voucher:
            before_data = clone(data) if self.assets.audit_enabled else None
            if self._owned_card_count(data, voucher_item_id) < voucher_item_count:
                return self._reject(route, APPEARANCE_NO_CARD)
            consume_err = self._consume_card(data, voucher_item_id, voucher_item_count)
            if consume_err == BAG_INSUFFICIENT:
                return self._reject(route, APPEARANCE_NO_CARD)
            if consume_err:
                return self._reject(route, APPEARANCE_UNAVAILABLE)
            profile.skill_vfx_owned_bits = set_packed_bit(bytes(profile.skill_vfx_owned_bits), target_bit)
            profile.equipped_skill_vfx[skill_table_id] = target_id
            client_request.remember(ledger, request.client_request_id, _EQUIP_SKILL_VFX_KIND, fingerprint)
            audit = self.assets.make_audit_callback(
                player, before_data, data, self.switch_tx, correlation=request.client_request_id,
            )
            try:
                await self.scene.commit_player_data(player, data, on_committed=audit)
            except (StoreRejected, StoreUnavailable, OSError):
                return self._reject(route, UNAVAILABLE)
            self._push_state_notify(player, data, now_sec, trial_expired=False)
            response = self.protocol.message(
                route.response_type, skill_table_id=skill_table_id, equipped_cosmetic_id=target_id, replayed=False,
            )
            self.fill_info(data, now_sec, response.info)
            return response

        current_id = int(profile.equipped_skill_vfx.get(skill_table_id, 0))
        if current_id == target_id:
            response = self.protocol.message(
                route.response_type, skill_table_id=skill_table_id, equipped_cosmetic_id=target_id, replayed=True,
            )
            self.fill_info(data, now_sec, response.info)
            return response

        if target_id == 0:
            profile.equipped_skill_vfx.pop(skill_table_id, None)
        else:
            profile.equipped_skill_vfx[skill_table_id] = target_id
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._reject(route, UNAVAILABLE)
        self._push_state_notify(player, data, now_sec, trial_expired=False)
        response = self.protocol.message(
            route.response_type, skill_table_id=skill_table_id, equipped_cosmetic_id=target_id, replayed=False,
        )
        self.fill_info(data, now_sec, response.info)
        return response

    async def grant_pet_cosmetic(self, player, cosmetic_id: int) -> int:
        if mut_tip := self._check_mutability(player):
            return mut_tip
        pet_cosmetic_table = self._table("PetCosmetic")
        row = pet_cosmetic_table.get(int(cosmetic_id)) if pet_cosmetic_table is not None else None
        if row is None or not getattr(row, "enabled", 0):
            return APPEARANCE_INVALID
        bit_idx = _cosmetic_bit_index(self.tables, "PetCosmetic", int(row.id))
        if bit_idx is None:
            return APPEARANCE_UNAVAILABLE
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        if test_packed_bit(bytes(profile.pet_cosmetic_owned_bits), bit_idx):
            return 0
        profile.pet_cosmetic_owned_bits = set_packed_bit(bytes(profile.pet_cosmetic_owned_bits), bit_idx)
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return UNAVAILABLE
        self._push_state_notify(player, data, self.now_s(), trial_expired=False)
        return 0

    async def grant_skill_vfx(self, player, cosmetic_id: int) -> int:
        if mut_tip := self._check_mutability(player):
            return mut_tip
        vfx_table = self._table("SkillVfxCosmetic")
        row = vfx_table.get(int(cosmetic_id)) if vfx_table is not None else None
        if row is None or not getattr(row, "enabled", 0):
            return APPEARANCE_INVALID
        bit_idx = _cosmetic_bit_index(self.tables, "SkillVfxCosmetic", int(row.id))
        if bit_idx is None:
            return APPEARANCE_UNAVAILABLE
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        if test_packed_bit(bytes(profile.skill_vfx_owned_bits), bit_idx):
            return 0
        profile.skill_vfx_owned_bits = set_packed_bit(bytes(profile.skill_vfx_owned_bits), bit_idx)
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return UNAVAILABLE
        self._push_state_notify(player, data, self.now_s(), trial_expired=False)
        return 0

    def track_player(self, player, *, now_sec: int | None = None) -> None:
        """玩家载入后登记在途试用时刻，供 ``expire_trials`` 定时回收。"""
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.profile_component
        if profile.trial_appearance_id:
            self._active_trials[player.player_id] = int(profile.trial_expires_at)
        else:
            self._active_trials.pop(player.player_id, None)

    def departed(self, player_id: int) -> None:
        self._active_trials.pop(player_id, None)

    async def expire_trials(self, now_sec: int | None = None) -> list[int]:
        """扫描已到期的试用形象，还原为永久形象并广播变更与状态推送；返回本次还原的玩家 ID 列表。"""
        now_unix = self.now_s() if now_sec is None else int(now_sec)
        due = [player_id for player_id, expires_at in self._active_trials.items() if expires_at <= now_unix]
        if not due:
            return []

        expired_players: list[int] = []
        for player_id in due:
            self._active_trials.pop(player_id, None)
            async with self.scene._lock(player_id):
                world = self.scene.locations.get(player_id)
                player = world.players.get(player_id) if world is not None else None
                if player is None:
                    continue
                if not self.scene.mutable(player):
                    self._active_trials[player_id] = now_unix
                    continue
                try:
                    await self.scene.flush_player_rows(player)
                except (StoreRejected, StoreUnavailable, OSError):
                    self._active_trials[player_id] = now_unix
                    continue
                data = self.scene.rows.assemble(player.snapshot)
                profile = data.profile_component
                if not profile.trial_appearance_id or int(profile.trial_expires_at) > now_unix:
                    if profile.trial_appearance_id:
                        self._active_trials[player_id] = int(profile.trial_expires_at)
                    continue
                expired_trial = profile.trial_appearance_id
                profile.trial_appearance_id = ""
                profile.trial_expires_at = 0
                try:
                    await self.scene.commit_player_data(player, data)
                except (StoreRejected, StoreUnavailable, OSError):
                    logger.warning("Appearance trial expiry save failed player_id=%d", player_id, exc_info=True)
                    self._active_trials[player_id] = now_unix
                    continue
                logger.info(
                    "Appearance trial expired player_id=%d trial_appearance_id=%s reverted_to=%s",
                    player_id,
                    expired_trial,
                    profile.appearance_id,
                )
                self._broadcast_appearance_changed(
                    world, player, profile.appearance_id, is_trial=False, trial_expires_at=0,
                )
                self._push_state_notify(player, data, now_unix, trial_expired=True)
                expired_players.append(player_id)
        return expired_players
