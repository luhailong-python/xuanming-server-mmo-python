"""五行试炼志 V2 系统：战法印位图、最佳回合复盘、里程碑发奖与自用法宝拓印兑换。"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    BAG_FULL, BAG_INSUFFICIENT, BLOCKED, INVALID, UNAVAILABLE,
    bag_capacity, clone, held_item_count,
)
from .scene_mission import now_ms, reward_items
from .scene_season import find_active_season_rule, valid_artifact_definition

logger = logging.getLogger(__name__)

_SERVICE = "SceneTrialJournalClientPlayer"
_CLIENT_REQUEST_KIND_CLAIM_MILESTONE = 10
_CLIENT_REQUEST_KIND_REDEEM_VOUCHER = 11
_MAX_UINT64 = (1 << 64) - 1
_MAX_CACHED_RECEIPTS = 256

# 仅自用横向奖励物品模板 ID（与 C++ trial_journal_rules.h 对齐）。
BOUND_REFORGE_SAND_ITEM_ID = 3403
BOUND_ARTIFACT_VOUCHER_ITEM_ID = 3404
ARTIFACT_ITEM_ID_BASE = 3300
MIN_ARTIFACT_EFFECT_ID = 1
MAX_ARTIFACT_EFFECT_ID = 8
MAX_ARTIFACT_SLOTS = 2
MAX_COOP_TEAMMATES = 4

# 卷册与准入模式常量。
SCOPE_ALL_PERMANENT = 0
CHAPTER_BREAKTHROUGH = 1
CHAPTER_THREE_REALMS = 2
CHAPTER_SEASONAL = 3
UNLOCK_MODE_OPEN = 0
UNLOCK_MODE_REQUIRE_NORMAL_CLEAR = 1

# 试炼志错误码（与 C++ trial_journal_error_tip.pb.h 严格对齐）。
TRIAL_JOURNAL_UNAVAILABLE = 35000
TRIAL_JOURNAL_FROZEN = 35001
TRIAL_JOURNAL_IN_BATTLE = 35002
TRIAL_JOURNAL_MILESTONE_NOT_FOUND = 35003
TRIAL_JOURNAL_MILESTONE_NOT_REACHED = 35004
TRIAL_JOURNAL_MILESTONE_ALREADY_CLAIMED = 35005
TRIAL_JOURNAL_BAG_FULL = 35006
TRIAL_JOURNAL_VOUCHER_NOT_ENOUGH = 35007
TRIAL_JOURNAL_ARTIFACT_INVALID = 35008


def has_packed_bit(bits: bytes, index: int) -> bool:
    """读取紧凑位图（低位在前）第 index 位，不修改或截断任何未知高位。"""
    byte_idx = index // 8
    return byte_idx < len(bits) and bool((bits[byte_idx] >> (index % 8)) & 1)


def set_packed_bit(bits: bytes, index: int) -> bytes:
    """将紧凑位图（低位在前）第 index 位置 1，保留既有高位字节不变。"""
    buf = bytearray(bits)
    byte_idx = index // 8
    if len(buf) <= byte_idx:
        buf.extend(b"\x00" * (byte_idx + 1 - len(buf)))
    buf[byte_idx] |= 1 << (index % 8)
    return bytes(buf)


@dataclass(frozen=True, slots=True)
class _EntryRow:
    id: int
    chapter_id: int
    chapter_name: str
    season_id: int
    source_kind: int
    difficulty: int
    variant_key: str
    rule_version: int
    rule_version_name: str
    sort_order: int
    display_name: str
    icon_key: str
    desc: str
    unlock_mode: int
    jump_target: str
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class _SealRow:
    id: int
    entry_id: int
    seal_slot_kind: int
    condition_type: int
    condition_param: int
    title: str
    desc: str
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class _MilestoneRow:
    id: int
    scope_chapter_id: int
    season_id: int
    required_seals: int
    title: str
    desc: str
    bound_sand_count: int
    bound_artifact_voucher_count: int
    badge_key: str = ""
    badge_name: str = ""
    commentary_key: str = ""
    commentary_title: str = ""
    commentary_body: str = ""
    grant_appearance_id: str = ""
    reward_id: int = 0
    enabled: bool = True


# 默认卷一目录与位图下标（与 C++ generated/tables/trialjournal*.json 及 *_bit_index.h 严格一致；
# 若 ConfigTables 已同步对应三张表则优先读取配表数据）。
_DEFAULT_ENTRIES: tuple[_EntryRow, ...] = (
    _EntryRow(101, 1, "卷一 · 破阵试炼", 0, 1, 1, "rejuvenation", 1, "首测规则", 1, "普通·回春",
              "UI/TrialJournal/guard_rejuvenation",
              "普通破阵首领配回春护卫：重劈与治疗错峰，适合练手压回合与打断回血。",
              0, "boss_trial:normal:rejuvenation"),
    _EntryRow(102, 1, "卷一 · 破阵试炼", 0, 1, 1, "bulwark", 1, "首测规则", 2, "普通·护势",
              "UI/TrialJournal/guard_bulwark",
              "普通破阵首领配护势护卫：物法防护交替，可驱散、双系混编或先破首领。",
              0, "boss_trial:normal:bulwark"),
    _EntryRow(103, 1, "卷一 · 破阵试炼", 0, 1, 2, "rejuvenation", 1, "首测规则", 3, "困难·回春",
              "UI/TrialJournal/guard_rejuvenation",
              "困难破阵首领配回春护卫：重劈与治疗同轮重叠，检验人宠抗伤、治疗与打断节奏。",
              1, "boss_trial:hard:rejuvenation"),
    _EntryRow(104, 1, "卷一 · 破阵试炼", 0, 1, 2, "bulwark", 1, "首测规则", 4, "困难·护势",
              "UI/TrialJournal/guard_bulwark",
              "困难破阵首领配护势护卫：高压重劈与高额护势并存，检验保命续航与破势策略。",
              1, "boss_trial:hard:bulwark"),
)

_DEFAULT_SEALS: tuple[_SealRow, ...] = (
    _SealRow(1011, 101, 1, 1, 0, "破阵", "有效通关普通·回春（1～5人皆可点亮）"),
    _SealRow(1012, 101, 4, 2, 7, "速破", "7 回合内有效通关普通·回春"),
    _SealRow(1013, 101, 3, 4, 0, "断愈", "全场未让首领获得有效回春治疗并有效通关"),
    _SealRow(1021, 102, 1, 1, 0, "破阵", "有效通关普通·护势（1～5人皆可点亮）"),
    _SealRow(1022, 102, 4, 2, 7, "速破", "7 回合内有效通关普通·护势"),
    _SealRow(1023, 102, 3, 5, 0, "破势", "本场成功驱散护势，或物法双系均造成有效直伤并通关"),
    _SealRow(1031, 103, 1, 1, 0, "破阵", "有效通关困难·回春（1～5人皆可点亮）"),
    _SealRow(1032, 103, 2, 3, 0, "无损", "困难·回春中本人及本人出战宝宝全程未曾倒地并通关"),
    _SealRow(1033, 103, 3, 4, 0, "断愈", "困难·回春中首领全程未获有效回春治疗并通关"),
    _SealRow(1041, 104, 1, 1, 0, "破阵", "有效通关困难·护势（1～5人皆可点亮）"),
    _SealRow(1042, 104, 2, 3, 0, "无损", "困难·护势中本人及本人出战宝宝全程未曾倒地并通关"),
    _SealRow(1043, 104, 3, 5, 0, "破势", "困难·护势中成功驱散护势或物法双系均造成有效直伤并通关"),
)

_DEFAULT_MILESTONES: tuple[_MilestoneRow, ...] = (
    _MilestoneRow(101, 1, 0, 3, "初涉破阵",
                  "卷一累计点亮 3 枚战法印，获赠 2 份仅自用重铸砂，可自由试换一次词条并留回旋余地。", 2, 0),
    _MilestoneRow(102, 1, 0, 6, "破阵试剑",
                  "卷一累计点亮 6 枚战法印，获赠 1 张自用法宝拓印与名片印戳「破阵试剑」。", 0, 1,
                  badge_key="badge_trial_sword", badge_name="破阵试剑"),
    _MilestoneRow(103, 1, 0, 9, "攻守兼备",
                  "卷一累计点亮 9 枚战法印，获赠 1 张自用法宝拓印与 2 份仅自用重铸砂。", 2, 1),
    _MilestoneRow(104, 1, 0, 12, "破阵尽墨",
                  "卷一 12 印全亮，获赠第 3 张自用法宝拓印、名片印戳「破阵尽墨」与留灯院评语《祝停云·破阵跋》。",
                  0, 1, badge_key="badge_trial_master", badge_name="破阵尽墨",
                  commentary_key="commentary_tingyun_trial_v1", commentary_title="祝停云·破阵跋",
                  commentary_body=(
                      "枪穗上的旧船绳又沾了墨。你在卷一里把快攻、守心、断愈和破势全试了一遍——"
                      "我原先总以为赢就是抢先一步，如今在败局册末页补了一句：肯换手路的人，比只押一招的人难缠得多。"
                      "下回演武场见，可别让我半步。"
                  )),
)

_DEFAULT_SEAL_BITS: dict[int, int] = {row.id: idx for idx, row in enumerate(_DEFAULT_SEALS)}
_DEFAULT_MILESTONE_BITS: dict[int, int] = {row.id: idx for idx, row in enumerate(_DEFAULT_MILESTONES)}


def _is_season_visible(row_season_id: int, active_season_id: int) -> bool:
    return row_season_id == 0 or (active_season_id != 0 and row_season_id == active_season_id)


def _find_season_archive(comp, season_id: int):
    return next((arc for arc in comp.season_records if arc.season_id == season_id), None)


def _find_or_add_season_archive(comp, season_id: int):
    existing = _find_season_archive(comp, season_id)
    if existing is not None:
        return existing
    return comp.season_records.add(season_id=season_id)


def _find_entry_record(comp, entry_id: int, rule_version: int):
    return next(
        (rec for rec in comp.records if rec.entry_id == entry_id and rec.rule_version == rule_version),
        None,
    )


def _find_or_add_entry_record(comp, entry_id: int, rule_version: int):
    existing = _find_entry_record(comp, entry_id, rule_version)
    if existing is not None:
        return existing
    return comp.records.add(entry_id=entry_id, rule_version=rule_version)


def _has_legacy_record(comp, entry_id: int, current_rule_version: int) -> bool:
    return any(
        rec.entry_id == entry_id
        and rec.rule_version != current_rule_version
        and (rec.solo_best.best_rounds > 0 or rec.coop_best.best_rounds > 0)
        for rec in comp.records
    )


def _bump_revision(comp) -> None:
    if comp.state_revision < _MAX_UINT64:
        comp.state_revision += 1


def _normalize_artifact_effects(raw_ids) -> list[int]:
    filtered = sorted(
        effect_id for effect_id in raw_ids if MIN_ARTIFACT_EFFECT_ID <= effect_id <= MAX_ARTIFACT_EFFECT_ID
    )
    return filtered[:MAX_ARTIFACT_SLOTS]


def evaluate_seal_condition(
    condition_type: int,
    condition_param: int,
    *,
    won: bool,
    participated: bool,
    total_rounds: int,
    self_or_pet_ever_died: bool = False,
    boss_guard_heal_total: int = 0,
    guard_stance_dispelled: bool = False,
    team_dealt_physical_direct_damage: bool = False,
    team_dealt_magic_direct_damage: bool = False,
    boss_wind_hit_unprotected_death: bool = False,
    dealt_yang_bell_physical_damage: bool = False,
    dealt_yin_drum_magic_damage: bool = False,
    finished_in_ebb_tide: bool = False,
    guard_survived: bool = False,
    self_triggered_shield_break: bool = False,
    self_triggered_anti_control_or_season_skill: bool = False,
) -> bool:
    """按 C++ trial_journal_rules.h 口径判定单条战法印是否达成。"""
    if total_rounds <= 0:
        return False
    if condition_type == 1:  # kClear
        return won
    if condition_type == 2:  # kSpeedClear
        return won and condition_param > 0 and total_rounds <= condition_param
    if condition_type == 3:  # kFlawlessSelfAndPet
        return won and not self_or_pet_ever_died
    if condition_type == 4:  # kNoBossGuardHeal
        return won and boss_guard_heal_total == 0
    if condition_type == 5:  # kBreakGuardStance
        return won and (
            guard_stance_dispelled
            or (team_dealt_physical_direct_damage and team_dealt_magic_direct_damage)
        )
    if condition_type == 6:  # kWindDefend
        return won and not boss_wind_hit_unprotected_death and (
            condition_param == 0 or total_rounds <= condition_param
        )
    if condition_type == 7:  # kBellHarmony
        return won and dealt_yang_bell_physical_damage and dealt_yin_drum_magic_damage
    if condition_type == 8:  # kTideFinish
        return won and finished_in_ebb_tide
    if condition_type == 9:  # kGuardRescued
        return won and guard_survived
    if condition_type == 10:  # kPolarityWin
        return won
    if condition_type == 11:  # kArtifactShieldBreak
        return (won or participated) and self_triggered_shield_break
    if condition_type == 12:  # kAntiControlOrSeasonSkill
        return (won or participated) and self_triggered_anti_control_or_season_skill
    return False


class SceneTrialJournal:
    """Scene 五行试炼志 V2 服务：战法印进度、里程碑发奖与自用法宝拓印兑换。"""

    def __init__(self, scene, tables, assets=None, *, clock=now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._redeem_receipts: dict[tuple[int, int], tuple[int, int]] = {}

    def register(self) -> None:
        """向 SceneNode 注册试炼志客户端 RPC。"""
        self.scene.register(_SERVICE, "GetTrialJournal", self.get_journal)
        self.scene.register(_SERVICE, "ClaimTrialJournalMilestone", self.claim_milestone, mutating=True)
        self.scene.register(_SERVICE, "RedeemBoundArtifactVoucher", self.redeem_voucher, mutating=True)

    def initialize_on_load(self, data) -> None:
        """角色载入时确保 trial_journal_component 存在，原样保留位图未知高位。"""
        data.trial_journal_component.SetInParent()

    # ── 目录与位图映射解析（优先配表，缺表时回退到权威默认目录） ──

    def _entries(self) -> tuple:
        if "TrialJournalEntry" in self.tables.tables:
            return self.tables["TrialJournalEntry"].rows()
        return _DEFAULT_ENTRIES

    def _seals(self) -> tuple:
        if "TrialJournalSeal" in self.tables.tables:
            return self.tables["TrialJournalSeal"].rows()
        return _DEFAULT_SEALS

    def _milestones(self) -> tuple:
        if "TrialJournalMilestone" in self.tables.tables:
            return self.tables["TrialJournalMilestone"].rows()
        return _DEFAULT_MILESTONES

    def _entry_by_id(self, entry_id: int):
        if "TrialJournalEntry" in self.tables.tables:
            return self.tables["TrialJournalEntry"].get(entry_id)
        return next((row for row in _DEFAULT_ENTRIES if row.id == entry_id), None)

    def _milestone_by_id(self, milestone_id: int):
        if "TrialJournalMilestone" in self.tables.tables:
            return self.tables["TrialJournalMilestone"].get(milestone_id)
        return next((row for row in _DEFAULT_MILESTONES if row.id == milestone_id), None)

    def _seals_for_entry(self, entry_id: int) -> list:
        rows = [row for row in self._seals() if row.entry_id == entry_id and row.enabled]
        rows.sort(key=lambda row: row.id)
        return rows

    def _seal_bit_index(self, seal_id: int) -> int | None:
        if "TrialJournalSeal" in self.tables.tables:
            table = self.tables["TrialJournalSeal"]
            try:
                return table.bit_index(seal_id)
            except KeyError:
                pass
            ordered = sorted(row.id for row in table.rows())
            return ordered.index(seal_id) if seal_id in ordered else None
        return _DEFAULT_SEAL_BITS.get(seal_id)

    def _milestone_bit_index(self, milestone_id: int) -> int | None:
        if "TrialJournalMilestone" in self.tables.tables:
            table = self.tables["TrialJournalMilestone"]
            try:
                return table.bit_index(milestone_id)
            except KeyError:
                pass
            ordered = sorted(row.id for row in table.rows())
            return ordered.index(milestone_id) if milestone_id in ordered else None
        return _DEFAULT_MILESTONE_BITS.get(milestone_id)

    def _is_seal_lit(self, comp, seal_id: int, entry_season_id: int) -> bool:
        if entry_season_id:
            archive = _find_season_archive(comp, entry_season_id)
            return archive is not None and seal_id in archive.lit_seal_ids
        bit_idx = self._seal_bit_index(seal_id)
        return bit_idx is not None and has_packed_bit(comp.lit_seal_bits, bit_idx)

    def _mark_seal_lit(self, comp, seal_id: int, entry_season_id: int) -> bool:
        if entry_season_id:
            archive = _find_or_add_season_archive(comp, entry_season_id)
            if seal_id in archive.lit_seal_ids:
                return False
            archive.lit_seal_ids.append(seal_id)
            return True
        bit_idx = self._seal_bit_index(seal_id)
        if bit_idx is None or has_packed_bit(comp.lit_seal_bits, bit_idx):
            return False
        comp.lit_seal_bits = set_packed_bit(comp.lit_seal_bits, bit_idx)
        return True

    def _is_milestone_claimed(self, comp, milestone_id: int, milestone_season_id: int) -> bool:
        if milestone_season_id:
            archive = _find_season_archive(comp, milestone_season_id)
            return archive is not None and milestone_id in archive.claimed_milestone_ids
        bit_idx = self._milestone_bit_index(milestone_id)
        return bit_idx is not None and has_packed_bit(comp.claimed_milestone_bits, bit_idx)

    def _mark_milestone_claimed(self, comp, milestone_id: int, milestone_season_id: int) -> bool:
        if milestone_season_id:
            archive = _find_or_add_season_archive(comp, milestone_season_id)
            if milestone_id in archive.claimed_milestone_ids:
                return False
            archive.claimed_milestone_ids.append(milestone_id)
            return True
        bit_idx = self._milestone_bit_index(milestone_id)
        if bit_idx is None or has_packed_bit(comp.claimed_milestone_bits, bit_idx):
            return False
        comp.claimed_milestone_bits = set_packed_bit(comp.claimed_milestone_bits, bit_idx)
        return True

    def _active_season_id(self, stamp: int) -> int:
        rule = find_active_season_rule(self.tables, stamp)
        return rule.id if rule is not None else 0

    def _writable_error(self, player) -> int:
        if not self.scene.mutable(player):
            return TRIAL_JOURNAL_FROZEN
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return TRIAL_JOURNAL_IN_BATTLE
        return 0

    def _redeemable_artifacts(self) -> list[tuple[int, object, str]]:
        """返回 [(item_config_id, EquipArtifactRow, icon_key), ...]，按 artifact_id 升序。"""
        if "EquipArtifact" not in self.tables.tables:
            return []
        from_items = []
        for item in self.tables["Item"].rows():
            profile = getattr(item, "equip_profile", 0)
            artifact_id = getattr(item, "artifact_id", 0)
            if (
                item.max_stack_size == 1
                and profile == 3
                and item.equip_kind == 16
                and MIN_ARTIFACT_EFFECT_ID <= artifact_id <= MAX_ARTIFACT_EFFECT_ID
            ):
                artifact = self.tables["EquipArtifact"].get(artifact_id)
                if artifact is not None and valid_artifact_definition(
                    artifact.id, artifact.bonus_bps, artifact.cap_bps,
                    artifact.per_round_limit, artifact.battle_limit,
                    artifact.active_rounds, artifact.health_threshold_bps,
                ):
                    from_items.append((item.id, artifact, item.icon_key))
        if from_items:
            from_items.sort(key=lambda pair: (pair[1].id, pair[0]))
            return from_items
        # 若当前 Item 表尚未同步 3301..3308，则直接依据 EquipArtifact 表构造标准兑换选项。
        fallback = []
        for artifact in sorted(self.tables["EquipArtifact"].rows(), key=lambda row: row.id):
            if MIN_ARTIFACT_EFFECT_ID <= artifact.id <= MAX_ARTIFACT_EFFECT_ID and valid_artifact_definition(
                artifact.id, artifact.bonus_bps, artifact.cap_bps,
                artifact.per_round_limit, artifact.battle_limit,
                artifact.active_rounds, artifact.health_threshold_bps,
            ):
                item_config_id = ARTIFACT_ITEM_ID_BASE + artifact.id
                item_row = self.tables["Item"].get(item_config_id)
                icon_key = item_row.icon_key if item_row is not None else f"UI/Artifacts/{item_config_id}"
                fallback.append((item_config_id, artifact, icon_key))
        return fallback

    def _count_effective_seals(self, comp, active_season_id: int) -> dict:
        lit_by_chapter: dict[int, int] = {}
        total_by_chapter: dict[int, int] = {}
        permanent_lit = 0
        permanent_total = 0
        any_normal_clear = False
        for entry in self._entries():
            if not entry.enabled or not _is_season_visible(entry.season_id, active_season_id):
                continue
            for seal in self._seals_for_entry(entry.id):
                lit = self._is_seal_lit(comp, seal.id, entry.season_id)
                total_by_chapter[entry.chapter_id] = total_by_chapter.get(entry.chapter_id, 0) + 1
                if lit:
                    lit_by_chapter[entry.chapter_id] = lit_by_chapter.get(entry.chapter_id, 0) + 1
                if entry.season_id == 0:
                    permanent_total += 1
                    if lit:
                        permanent_lit += 1
                if (
                    lit
                    and entry.chapter_id == CHAPTER_BREAKTHROUGH
                    and entry.difficulty == 1
                    and seal.condition_type == 1
                ):
                    any_normal_clear = True
        return {
            "lit_by_chapter": lit_by_chapter,
            "total_by_chapter": total_by_chapter,
            "permanent_lit": permanent_lit,
            "permanent_total": permanent_total,
            "any_normal_clear": any_normal_clear,
        }

    def _find_appearance_name(self, appearance_id: str) -> str:
        if not appearance_id or "CharacterAppearance" not in self.tables.tables:
            return ""
        for row in self.tables["CharacterAppearance"].rows():
            if row.appearance_id == appearance_id:
                return row.name
        return ""

    def _is_valid_exclusive_appearance(self, appearance_id: str) -> bool:
        if not appearance_id:
            return True
        if "CharacterAppearance" not in self.tables.tables:
            return False
        for row in self.tables["CharacterAppearance"].rows():
            if row.appearance_id == appearance_id:
                return bool(row.enabled) and not getattr(row, "card_unlockable", 0)
        return False

    def _grant_appearance(self, data, appearance_id: str) -> None:
        if not appearance_id or "CharacterAppearance" not in self.tables.tables:
            return
        table = self.tables["CharacterAppearance"]
        target = next((row for row in table.rows() if row.appearance_id == appearance_id), None)
        if target is None:
            return
        try:
            bit_idx = table.bit_index(target.id)
        except KeyError:
            ordered = sorted(row.id for row in table.rows())
            bit_idx = ordered.index(target.id)
        if bit_idx is not None:
            data.profile_component.appearance_owned_bits = set_packed_bit(
                data.profile_component.appearance_owned_bits, bit_idx,
            )

    # ── 背包协作原语（优先走 scene.assets；表未同步 3403/3404/3301..3308 时保持同口径原子语义） ──

    def _removable_count(self, data, config_id: int) -> int:
        if self.assets is not None:
            return self.assets.removable_count(data, config_id)
        total = 0
        for entry in data.bag_component.items:
            if entry.bag_type != 0:
                continue
            item = entry.instance if entry.HasField("instance") else entry
            cid = item.config_id
            size = item.size if entry.HasField("instance") else entry.stack_size
            if cid == config_id:
                total += size
        return max(0, total - held_item_count(data, config_id))

    def _owned_across_bags(self, data, config_id: int) -> int:
        total = 0
        for entry in data.bag_component.items:
            if entry.bag_type not in (0, 2):
                continue
            if entry.HasField("instance"):
                if entry.instance.config_id == config_id:
                    total += entry.instance.size
            elif entry.config_id == config_id:
                total += entry.stack_size
        return total

    async def _grant_stackable_items(self, data, counts: dict[int, int], *, gains: list) -> int:
        """向主背包发奖；Item 表已配置时直走 assets.add_items，未同步时走同口径原子堆叠。"""
        if not counts:
            return 0
        if self.assets is None:
            return TRIAL_JOURNAL_UNAVAILABLE
        known = {cid: qty for cid, qty in counts.items() if self.tables["Item"].get(cid) is not None}
        synthetic = {cid: qty for cid, qty in counts.items() if cid not in known}
        staged = clone(data)
        if known:
            err = await self.assets.add_items(staged, known, gains=gains)
            if err:
                return err
        if synthetic:
            err = await self._add_synthetic_stacks(staged, synthetic)
            if err:
                return err
        data.bag_component.CopyFrom(staged.bag_component)
        return 0

    async def _add_synthetic_stacks(self, data, counts: dict[int, int], *, max_stack: int = 999) -> int:
        for cid in counts:
            if self.assets.item_gain_blocked(data, cid):
                return BLOCKED
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        cap = bag_capacity(bag, 0)
        if not self.assets._usable(entries, cap):
            return TRIAL_JOURNAL_UNAVAILABLE
        occupied = {entry.pos for entry, _ in entries}
        free_slots = (slot for slot in range(cap) if slot not in occupied)
        plan = []
        for cid, amount in sorted(counts.items()):
            remaining = amount
            for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
                if item.config_id == cid and self.assets._mergeable(item, max_stack):
                    added = min(remaining, max_stack - item.size)
                    if added:
                        item.size += added
                        remaining -= added
                        self.assets.write_entry(entry, item)
            while remaining:
                slot = next(free_slots, None)
                if slot is None:
                    return BAG_FULL
                stack = min(remaining, max_stack)
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
        data.bag_component.CopyFrom(bag)
        return 0

    def _consume_voucher(self, data, count: int = 1) -> int:
        if self.assets is None:
            return TRIAL_JOURNAL_UNAVAILABLE
        if self.tables["Item"].get(BOUND_ARTIFACT_VOUCHER_ITEM_ID) is not None:
            return self.assets.consume_items(data, {BOUND_ARTIFACT_VOUCHER_ITEM_ID: count})
        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        if not self.assets._usable(entries, bag_capacity(bag, 0)):
            return TRIAL_JOURNAL_UNAVAILABLE
        held = held_item_count(data, BOUND_ARTIFACT_VOUCHER_ITEM_ID)
        known = sum(
            item.size for _, item in entries
            if item.config_id == BOUND_ARTIFACT_VOUCHER_ITEM_ID and self.assets.countable(item)
        )
        if known < count or (held and count > max(0, known - held)):
            return BAG_INSUFFICIENT
        taken, version_skew = self.assets._drain(entries, BOUND_ARTIFACT_VOUCHER_ITEM_ID, count)
        if taken != count:
            return TRIAL_JOURNAL_UNAVAILABLE if version_skew else BAG_INSUFFICIENT
        self.assets._drop_empty(bag.items, 0)
        data.bag_component.CopyFrom(bag)
        return 0

    # ── 外部进度推进入口（供战斗结算或上层玩法直接调用） ──

    def record_trial_progress(
        self,
        data,
        entry_id: int = 0,
        *,
        settlement=None,
        won: bool = True,
        participated: bool = True,
        total_rounds: int = 1,
        rule_version: int = 0,
        initial_human_count: int = 1,
        self_or_pet_ever_died: bool = False,
        boss_guard_heal_total: int = 0,
        guard_stance_dispelled: bool = False,
        team_dealt_physical_direct_damage: bool = False,
        team_dealt_magic_direct_damage: bool = False,
        boss_wind_hit_unprotected_death: bool = False,
        dealt_yang_bell_physical_damage: bool = False,
        dealt_yin_drum_magic_damage: bool = False,
        finished_in_ebb_tide: bool = False,
        guard_survived: bool = False,
        self_triggered_shield_break: bool = False,
        self_triggered_anti_control_or_season_skill: bool = False,
        used_pet_table_id: int = 0,
        used_artifact_effect_ids: tuple[int, ...] = (),
        used_season_seal_id: int = 0,
        teammate_names: tuple[str, ...] = (),
        achieved_at_ms: int | None = None,
        seal_ids: tuple[int, ...] | None = None,
    ) -> dict:
        """写入试炼进度：点亮达标战法印并刷新对应可比规则版的单人/合战最佳回合。"""
        delta = {"entry_id": 0, "newly_lit_seal_ids": [], "new_solo_best": False, "new_coop_best": False, "changed": False}
        if settlement is not None:
            if (
                not settlement.battle_id or not settlement.player_id or settlement.fled
                or not settlement.total_rounds or settlement.player_team_index > 1
                or settlement.outcome not in (1, 2, 3)
                or not settlement.HasField("trial_facts")
            ):
                return delta
            facts = settlement.trial_facts
            if not facts.journal_entry_id or not facts.trial_rule_version or not 1 <= facts.initial_human_count <= 5:
                return delta
            entry_id = facts.journal_entry_id
            rule_version = facts.trial_rule_version
            initial_human_count = facts.initial_human_count
            won = (
                (settlement.outcome == 1 and settlement.player_team_index == 0)
                or (settlement.outcome == 2 and settlement.player_team_index == 1)
            )
            participated = bool(getattr(settlement, "participated", False))
            total_rounds = settlement.total_rounds
            self_or_pet_ever_died = bool(facts.self_or_pet_ever_died)
            boss_guard_heal_total = facts.boss_guard_heal_total
            guard_stance_dispelled = bool(facts.guard_stance_dispelled)
            team_dealt_physical_direct_damage = bool(facts.team_dealt_physical_direct_damage)
            team_dealt_magic_direct_damage = bool(facts.team_dealt_magic_direct_damage)
            boss_wind_hit_unprotected_death = bool(facts.boss_wind_hit_unprotected_death)
            dealt_yang_bell_physical_damage = bool(facts.dealt_yang_bell_physical_damage)
            dealt_yin_drum_magic_damage = bool(facts.dealt_yin_drum_magic_damage)
            finished_in_ebb_tide = bool(facts.finished_in_ebb_tide)
            guard_survived = bool(facts.guard_survived)
            self_triggered_shield_break = bool(facts.self_triggered_shield_break)
            self_triggered_anti_control_or_season_skill = bool(facts.self_triggered_anti_control_or_season_skill)
            used_pet_table_id = facts.used_pet_table_id
            used_artifact_effect_ids = tuple(facts.used_artifact_effect_ids)
            used_season_seal_id = facts.used_season_seal_id
            teammate_names = tuple(facts.teammate_names)
            achieved_at_ms = settlement.finished_at_ms or self.clock()

        entry = self._entry_by_id(entry_id)
        if entry is None:
            return delta
        comp = data.trial_journal_component
        comp.SetInParent()
        delta["entry_id"] = entry.id
        effective_version = rule_version or entry.rule_version

        seals = self._seals_for_entry(entry.id)
        for seal in seals:
            matched = (
                seal.id in seal_ids
                if seal_ids is not None
                else evaluate_seal_condition(
                    seal.condition_type,
                    seal.condition_param,
                    won=won,
                    participated=participated,
                    total_rounds=total_rounds,
                    self_or_pet_ever_died=self_or_pet_ever_died,
                    boss_guard_heal_total=boss_guard_heal_total,
                    guard_stance_dispelled=guard_stance_dispelled,
                    team_dealt_physical_direct_damage=team_dealt_physical_direct_damage,
                    team_dealt_magic_direct_damage=team_dealt_magic_direct_damage,
                    boss_wind_hit_unprotected_death=boss_wind_hit_unprotected_death,
                    dealt_yang_bell_physical_damage=dealt_yang_bell_physical_damage,
                    dealt_yin_drum_magic_damage=dealt_yin_drum_magic_damage,
                    finished_in_ebb_tide=finished_in_ebb_tide,
                    guard_survived=guard_survived,
                    self_triggered_shield_break=self_triggered_shield_break,
                    self_triggered_anti_control_or_season_skill=self_triggered_anti_control_or_season_skill,
                )
            )
            if matched and self._mark_seal_lit(comp, seal.id, entry.season_id):
                delta["newly_lit_seal_ids"].append(seal.id)
                delta["changed"] = True

        if won and total_rounds > 0:
            stamp = self.clock() if achieved_at_ms is None else achieved_at_ms
            if initial_human_count == 1:
                existing = _find_entry_record(comp, entry.id, effective_version)
                old_rounds = existing.solo_best.best_rounds if existing is not None else 0
                if old_rounds == 0 or total_rounds < old_rounds:
                    record = _find_or_add_entry_record(comp, entry.id, effective_version)
                    record.solo_best.Clear()
                    record.solo_best.best_rounds = total_rounds
                    record.solo_best.achieved_at_ms = stamp
                    record.solo_best.loadout.pet_table_id = used_pet_table_id
                    record.solo_best.loadout.artifact_effect_ids.extend(
                        _normalize_artifact_effects(used_artifact_effect_ids),
                    )
                    record.solo_best.loadout.season_seal_id = used_season_seal_id
                    delta["new_solo_best"] = True
                    delta["changed"] = True
            elif 2 <= initial_human_count <= 5:
                existing = _find_entry_record(comp, entry.id, effective_version)
                old_rounds = existing.coop_best.best_rounds if existing is not None else 0
                if old_rounds == 0 or total_rounds < old_rounds:
                    record = _find_or_add_entry_record(comp, entry.id, effective_version)
                    record.coop_best.Clear()
                    record.coop_best.best_rounds = total_rounds
                    record.coop_best.party_size = initial_human_count
                    record.coop_best.achieved_at_ms = stamp
                    record.coop_best.pet_table_id = used_pet_table_id
                    for name in teammate_names[:MAX_COOP_TEAMMATES]:
                        if name:
                            record.coop_best.teammate_names.append(name)
                    delta["new_coop_best"] = True
                    delta["changed"] = True

        if delta["changed"]:
            _bump_revision(comp)
        return delta

    # ── 快照组装与 RPC 入口 ──

    def _fill_snapshot(self, player, data, stamp: int, out) -> int:
        out.Clear()
        out.server_now_ms = stamp
        comp = data.trial_journal_component
        active_season_id = self._active_season_id(stamp)
        counts = self._count_effective_seals(comp, active_season_id)

        entries_by_chapter: dict[int, list] = {}
        for entry in self._entries():
            if not entry.enabled or not _is_season_visible(entry.season_id, active_season_id):
                continue
            entries_by_chapter.setdefault(entry.chapter_id, []).append(entry)

        out.state_revision = comp.state_revision
        out.total_permanent_lit_seals = counts["permanent_lit"]
        out.total_permanent_seals = counts["permanent_total"]
        out.voucher_balance = self._removable_count(data, BOUND_ARTIFACT_VOUCHER_ITEM_ID)
        out.bound_sand_balance = self._removable_count(data, BOUND_REFORGE_SAND_ITEM_ID)

        writable_err = self._writable_error(player)
        out.can_operate = writable_err == 0
        if writable_err == TRIAL_JOURNAL_IN_BATTLE:
            out.action_unavailable_reason = "战斗中暂不能领取里程碑或兑换法宝，请在结算后重试"
        elif writable_err:
            out.action_unavailable_reason = "当前角色状态暂时不能操作试炼志，请稍后再试"

        if not entries_by_chapter:
            out.available = False
            out.unavailable_reason = "试炼志暂不可用"
            return 0
        out.available = True

        for chapter_id in sorted(entries_by_chapter):
            entries = sorted(entries_by_chapter[chapter_id], key=lambda row: (row.sort_order, row.id))
            chapter_view = out.chapters.add(
                chapter_id=chapter_id,
                season_id=entries[0].season_id,
                chapter_name=entries[0].chapter_name,
                lit_seal_count=counts["lit_by_chapter"].get(chapter_id, 0),
                total_seal_count=counts["total_by_chapter"].get(chapter_id, 0),
            )
            for entry in entries:
                entry_view = chapter_view.entries.add(
                    entry_id=entry.id,
                    chapter_id=entry.chapter_id,
                    season_id=entry.season_id,
                    source_kind=entry.source_kind,
                    difficulty=entry.difficulty,
                    variant_key=entry.variant_key,
                    rule_version=entry.rule_version,
                    rule_version_name=entry.rule_version_name,
                    sort_order=entry.sort_order,
                    display_name=entry.display_name,
                    icon_key=entry.icon_key,
                    desc=entry.desc,
                    jump_target=entry.jump_target,
                )
                seals = self._seals_for_entry(entry.id)
                entry_lit = 0
                for seal in seals:
                    lit = self._is_seal_lit(comp, seal.id, entry.season_id)
                    if lit:
                        entry_lit += 1
                    entry_view.seals.add(
                        seal_id=seal.id,
                        seal_slot_kind=seal.seal_slot_kind,
                        condition_type=seal.condition_type,
                        condition_param=seal.condition_param,
                        title=seal.title,
                        desc=seal.desc,
                        lit=lit,
                    )
                entry_view.lit_seal_count = entry_lit
                entry_view.total_seal_count = len(seals)

                record = _find_entry_record(comp, entry.id, entry.rule_version)
                has_solo = record is not None and record.solo_best.best_rounds > 0
                has_coop = record is not None and record.coop_best.best_rounds > 0
                entry_view.has_solo_best = has_solo
                if has_solo:
                    entry_view.solo_best.CopyFrom(record.solo_best)
                entry_view.has_coop_best = has_coop
                if has_coop:
                    entry_view.coop_best.CopyFrom(record.coop_best)
                entry_view.has_legacy_record_only = (
                    not has_solo and not has_coop and _has_legacy_record(comp, entry.id, entry.rule_version)
                )

                if writable_err == TRIAL_JOURNAL_IN_BATTLE:
                    entry_view.can_enter = False
                    entry_view.enter_blocked_reason = "当前正在战斗中，暂不能前往试炼"
                elif entry.unlock_mode == UNLOCK_MODE_REQUIRE_NORMAL_CLEAR and not counts["any_normal_clear"]:
                    entry_view.can_enter = False
                    entry_view.enter_blocked_reason = "本人普通首领试炼通关一次后开启困难"
                else:
                    entry_view.can_enter = writable_err == 0
                    if writable_err:
                        entry_view.enter_blocked_reason = "当前状态暂不可前往试炼"

        milestones = [
            row for row in self._milestones()
            if row.enabled and _is_season_visible(row.season_id, active_season_id)
        ]
        milestones.sort(key=lambda row: (row.scope_chapter_id, row.required_seals, row.id))
        for row in milestones:
            current_seals = (
                counts["permanent_lit"]
                if row.scope_chapter_id == SCOPE_ALL_PERMANENT
                else counts["lit_by_chapter"].get(row.scope_chapter_id, 0)
            )
            reached = current_seals >= row.required_seals
            claimed = self._is_milestone_claimed(comp, row.id, row.season_id)
            out.milestones.add(
                milestone_id=row.id,
                scope_chapter_id=row.scope_chapter_id,
                season_id=row.season_id,
                required_seals=row.required_seals,
                current_seals=current_seals,
                title=row.title,
                desc=row.desc,
                bound_sand_count=row.bound_sand_count,
                bound_artifact_voucher_count=row.bound_artifact_voucher_count,
                badge_key=row.badge_key,
                badge_name=row.badge_name,
                commentary_key=row.commentary_key,
                commentary_title=row.commentary_title,
                commentary_body=row.commentary_body,
                grant_appearance_id=row.grant_appearance_id,
                grant_appearance_name=self._find_appearance_name(row.grant_appearance_id),
                reached=reached,
                claimed=claimed,
                claimable=reached and not claimed and writable_err == 0,
            )

        for item_config_id, artifact, icon_key in self._redeemable_artifacts():
            out.redeemable_artifacts.add(
                item_config_id=item_config_id,
                artifact_effect_id=artifact.id,
                name=artifact.name,
                description=artifact.description,
                icon_key=icon_key,
                owned_count=self._owned_across_bags(data, item_config_id),
            )
        return 0

    def _finish(self, response, player, data, stamp: int, error: int = 0):
        if error:
            response.error_message.id = error
        snap_err = self._fill_snapshot(player, data, stamp, response.snapshot)
        if snap_err:
            response.ClearField("snapshot")
            if not error:
                response.error_message.id = snap_err
        return response

    def get_journal(self, _world, player, _route, _request):
        """查询试炼志完整快照（只读）。"""
        response = self.protocol.message("GetTrialJournalResponse")
        data = self.scene.rows.assemble(player.snapshot)
        return self._finish(response, player, data, self.clock())

    async def claim_milestone(self, _world, player, _route, request):
        """领取已达标的一次性里程碑奖励（含背包预检、外观解锁与 client_request_id 幂等）。"""
        response = self.protocol.message("ClaimTrialJournalMilestoneResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.milestone_id:
            return self._finish(response, player, data, stamp, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_CLAIM_MILESTONE)
        if err:
            return self._finish(response, player, data, stamp, err)
        if prior is not None:
            row = self._milestone_by_id(request.milestone_id)
            response.replayed = True
            if row is not None:
                response.granted_sand_count = row.bound_sand_count
                response.granted_voucher_count = row.bound_artifact_voucher_count
                if row.grant_appearance_id:
                    response.granted_appearance_id = row.grant_appearance_id
            return self._finish(response, player, data, stamp)

        if self.assets is None:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        milestone = self._milestone_by_id(request.milestone_id)
        active_season_id = self._active_season_id(stamp)
        if milestone is None or not milestone.enabled or not _is_season_visible(milestone.season_id, active_season_id):
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_MILESTONE_NOT_FOUND)
        comp = data.trial_journal_component
        if self._is_milestone_claimed(comp, milestone.id, milestone.season_id):
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_MILESTONE_ALREADY_CLAIMED)

        counts = self._count_effective_seals(comp, active_season_id)
        current_seals = (
            counts["permanent_lit"]
            if milestone.scope_chapter_id == SCOPE_ALL_PERMANENT
            else counts["lit_by_chapter"].get(milestone.scope_chapter_id, 0)
        )
        if current_seals < milestone.required_seals:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_MILESTONE_NOT_REACHED)
        if not self._is_valid_exclusive_appearance(milestone.grant_appearance_id):
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)

        grant_counts: dict[int, int] = {}
        if milestone.bound_sand_count > 0:
            grant_counts[BOUND_REFORGE_SAND_ITEM_ID] = (
                grant_counts.get(BOUND_REFORGE_SAND_ITEM_ID, 0) + milestone.bound_sand_count
            )
        if milestone.bound_artifact_voucher_count > 0:
            grant_counts[BOUND_ARTIFACT_VOUCHER_ITEM_ID] = (
                grant_counts.get(BOUND_ARTIFACT_VOUCHER_ITEM_ID, 0) + milestone.bound_artifact_voucher_count
            )
        extra_reward_id = getattr(milestone, "reward_id", 0)
        if extra_reward_id:
            extra_items = reward_items(self.tables, extra_reward_id)
            if extra_items is None:
                return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)
            for cid, qty in extra_items.items():
                grant_counts[cid] = grant_counts.get(cid, 0) + qty

        before = clone(data) if self.assets.audit_enabled else None
        gains = []
        if grant_counts:
            grant_err = await self._grant_stackable_items(data, grant_counts, gains=gains)
            if grant_err:
                mapped = TRIAL_JOURNAL_BAG_FULL if grant_err == BAG_FULL else (
                    grant_err if grant_err == INVALID else TRIAL_JOURNAL_BAG_FULL
                )
                return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, mapped)

        if not self._mark_milestone_claimed(data.trial_journal_component, milestone.id, milestone.season_id):
            return self._finish(
                response, player, self.scene.rows.assemble(player.snapshot), stamp, TRIAL_JOURNAL_MILESTONE_ALREADY_CLAIMED,
            )
        if milestone.grant_appearance_id:
            self._grant_appearance(data, milestone.grant_appearance_id)
        _bump_revision(data.trial_journal_component)
        client_request.remember(
            data.client_request_ledger, request.client_request_id, _CLIENT_REQUEST_KIND_CLAIM_MILESTONE, fingerprint,
        )

        audit = (
            self.assets.make_audit_callback(
                player, before, data, 7, correlation=request.client_request_id, gains=gains,
            )
            if before is not None else None
        )
        try:
            await self.scene.commit_player_data(player, data, on_committed=audit)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)

        logger.info(
            "Trial journal milestone claimed player_id=%s milestone_id=%s sand=%s voucher=%s",
            player.player_id, milestone.id, milestone.bound_sand_count, milestone.bound_artifact_voucher_count,
        )
        response.granted_sand_count = milestone.bound_sand_count
        response.granted_voucher_count = milestone.bound_artifact_voucher_count
        if milestone.grant_appearance_id:
            response.granted_appearance_id = milestone.grant_appearance_id
        return self._finish(response, player, data, stamp)

    def _remember_redeem(self, player_id: int, request_id: int, item_id: int, config_id: int) -> None:
        if len(self._redeem_receipts) >= _MAX_CACHED_RECEIPTS:
            oldest = next(iter(self._redeem_receipts))
            del self._redeem_receipts[oldest]
        self._redeem_receipts[(player_id, request_id)] = (item_id, config_id)

    async def redeem_voucher(self, _world, player, _route, request):
        """消耗 1 张自用法宝拓印（3404），在背包中铸造指定绑定法宝实例（bound=True）。"""
        response = self.protocol.message("RedeemBoundArtifactVoucherResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id or not request.artifact_item_config_id:
            return self._finish(response, player, data, stamp, INVALID)

        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_REDEEM_VOUCHER)
        if err:
            return self._finish(response, player, data, stamp, err)
        if prior is not None:
            cached = self._redeem_receipts.get((player.player_id, request.client_request_id))
            response.replayed = True
            response.created_item_id = cached[0] if cached is not None else 0
            response.artifact_item_config_id = cached[1] if cached is not None else request.artifact_item_config_id
            return self._finish(response, player, data, stamp)

        if self.assets is None:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        option = next(
            (row for row in self._redeemable_artifacts() if row[0] == request.artifact_item_config_id),
            None,
        )
        if option is None:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_ARTIFACT_INVALID)
        item_config_id, artifact, _ = option

        if self._removable_count(data, BOUND_ARTIFACT_VOUCHER_ITEM_ID) < 1:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_VOUCHER_NOT_ENOUGH)

        # 离线试算消费 1 张拓印 + 铸造 1 件绑定法宝，确保满包时不扣拓印。
        staged = clone(data)
        consume_err = self._consume_voucher(staged, 1)
        if consume_err == BAG_INSUFFICIENT:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_VOUCHER_NOT_ENOUGH)
        if consume_err:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)

        if self.assets.allocate_item_id is None:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)
        try:
            created_guid = await self.assets.allocate_item_id()
        except (OSError, RuntimeError, ValueError):
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)
        if type(created_guid) is not int or not 1 <= created_guid <= _MAX_UINT64:
            return self._finish(response, player, data, stamp, TRIAL_JOURNAL_UNAVAILABLE)

        artifact_item = self.protocol.message(
            "ItemComp", item_id=created_guid, config_id=item_config_id, size=1, bound=True,
        )
        artifact_item.equip.SetInParent()
        before = clone(data) if self.assets.audit_enabled else None
        gains = []
        add_err = await self.assets.add_items(staged, {}, instances=(artifact_item,), gains=gains)
        if add_err:
            mapped = TRIAL_JOURNAL_BAG_FULL if add_err == BAG_FULL else TRIAL_JOURNAL_UNAVAILABLE
            return self._finish(response, player, data, stamp, mapped)

        _bump_revision(staged.trial_journal_component)
        client_request.remember(
            staged.client_request_ledger, request.client_request_id, _CLIENT_REQUEST_KIND_REDEEM_VOUCHER, fingerprint,
        )
        audit = (
            self.assets.make_audit_callback(
                player, before, staged, 8, correlation=request.client_request_id, gains=gains,
            )
            if before is not None else None
        )
        try:
            await self.scene.commit_player_data(player, staged, on_committed=audit)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)

        self._remember_redeem(player.player_id, request.client_request_id, created_guid, item_config_id)
        logger.info(
            "Bound artifact redeemed player_id=%s config_id=%s artifact_id=%s item_id=%s",
            player.player_id, item_config_id, artifact.id, created_guid,
        )
        response.created_item_id = created_guid
        response.artifact_item_config_id = item_config_id
        return self._finish(response, player, staged, stamp)
