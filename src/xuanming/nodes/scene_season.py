"""赛季技能与单槽赛季印（方案 A）系统，写操作随角色存档提交。"""

from __future__ import annotations

import logging
import time

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import INVALID, UNAVAILABLE, balance, clone
from .battle_dropin_effects import (
    MAX_SAVED_BUILD_BOOKMARKS,
    WUXING_TOWER_MAX_FLOORS,
    is_valid_formation_id,
    is_valid_tower_sigil_id,
    roll_tower_sigil_offer,
    tower_floor_dungeon_id,
    tower_floor_leyline_element,
    wuxing_week_key,
)
from .scene_mission import effective_level, now_ms
from .scene_stats import rank_limit

logger = logging.getLogger(__name__)

_SERVICE = "SceneSeasonClientPlayer"
_CLIENT_REQUEST_KIND_CATCHUP = 9
_MAX_UINT64 = (1 << 64) - 1
_MAX_CACHED_RECEIPTS = 256
_MAX_BOOKMARK_TITLE_BYTES = 48
_MAX_BOOKMARK_SKILLS = 12
_MAX_TOWER_ACTIVE_SIGILS = 9
_TOWER_LEADERBOARD_TOP_N = 20

# 赛季规则硬安全边界（与 C++ season_rules.h 保持一致）。
HARD_STAT_CAP_BPS = 500
HARD_MAX_PROGRESS_POINTS = 1000
HARD_MAX_CATCHUP_PRICE_PER_POINT = 1_000_000_000
MAX_VALID_CURRENCY_TYPE = 2

# 赛季错误码（与 C++ season_error_tip.pb.h 严格对齐）。
SEASON_NOT_OPEN = 34000
SEASON_MISMATCH = 34001
SEASON_SEAL_INVALID = 34002
SEASON_SEAL_ALREADY_EQUIPPED = 34003
SEASON_SEAL_ALREADY_MAX = 34004
SEASON_CATCHUP_DISABLED = 34005
SEASON_CATCHUP_PRICE_CHANGED = 34006
SEASON_CATCHUP_CURRENCY_NOT_ENOUGH = 34007
SEASON_FROZEN = 34008
SEASON_IN_BATTLE = 34009
SEASON_SKILL_NO_UPGRADE = 34010
SEASON_UNAVAILABLE = 34011

# MatchMode 枚举数值（保留既有 scene_season 映射以兼容旧单测；is_eligible_battle 严格按 proto MatchMode 判定）。
_PVE_MODES = frozenset({1, 2})
_PVP_MODES = frozenset({3, 4, 5})

# 正式排位败局/平局防挂机门槛（对齐 C++ PlayerEquipmentMaterialRewardSystem::kPvpMin*）。
PVP_MIN_ROUNDS_FOR_LOSS = 3
PVP_MIN_MANUAL_ROUNDS_FOR_LOSS = 2


def is_eligible_battle(settlement) -> bool:
    """按 C++ PlayerSeasonSystem::IsEligibleBattle 判定本场结算是否可累计赛季印进度点。"""
    if (
        not settlement.battle_id
        or not settlement.player_id
        or settlement.fled
        or settlement.total_rounds <= 0
        or settlement.player_team_index > 1
        or settlement.outcome not in (1, 2, 3)
    ):
        return False
    player_won = (
        (settlement.outcome == 1 and settlement.player_team_index == 0)
        or (settlement.outcome == 2 and settlement.player_team_index == 1)
    )
    mode = int(settlement.match_mode)
    if mode in (4, 5):  # MATCH_MODE_PVE_SOLO, MATCH_MODE_PVE_TEAM
        return player_won
    if mode in (1, 3, 7):  # MATCH_MODE_5V5, MATCH_MODE_1V1, MATCH_MODE_PVP_TEAM
        if not bool(getattr(settlement, "competitive_ranked", False)):
            return True
        if not bool(getattr(settlement, "participated", False)):
            return False
        if player_won:
            return True
        return (
            settlement.total_rounds >= PVP_MIN_ROUNDS_FOR_LOSS
            and int(getattr(settlement, "manual_action_rounds", 0)) >= PVP_MIN_MANUAL_ROUNDS_FOR_LOSS
            and bool(getattr(settlement, "effective_combat_contribution", False))
        )
    return False


def valid_artifact_definition(
    effect_id: int,
    bonus_bps: int,
    cap_bps: int,
    per_round_limit: int,
    battle_limit: int,
    active_rounds: int,
    health_threshold_bps: int,
) -> bool:
    """校验法宝/赛季印战斗机制参数是否合法（对齐 C++ equipbattle::ValidArtifactDefinition）。"""
    if (
        not 1 <= effect_id <= 8
        or not 1 <= bonus_bps <= 10_000
        or cap_bps > 10_000
        or health_threshold_bps > 10_000
        or per_round_limit > 1
        or battle_limit > 3
        or active_rounds > 2
    ):
        return False
    if effect_id != 6 and per_round_limit == 0:
        return False
    if (effect_id == 3 or effect_id >= 6) and battle_limit == 0:
        return False
    if effect_id == 6 and (cap_bps == 0 or active_rounds == 0):
        return False
    return effect_id != 4 or health_threshold_bps != 0


def is_window_active(starts_at_ms: int, ends_at_ms: int, enabled: bool, now_ms_val: int) -> bool:
    """判断给定赛季时间窗在当前时刻是否开启。"""
    if not enabled:
        return False
    if ends_at_ms and ends_at_ms <= starts_at_ms:
        return False
    if now_ms_val < starts_at_ms:
        return False
    return ends_at_ms == 0 or now_ms_val < ends_at_ms


def windows_overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> bool:
    """判断两个半开赛季窗口是否存在时间重叠。"""
    a_before_b = end_a != 0 and end_a <= start_b
    b_before_a = end_b != 0 and end_b <= start_a
    return not a_before_b and not b_before_a


def validate_rule(row) -> str | None:
    """校验单条 SeasonRule 配置行，返回错误说明或 None。"""
    if not row.id:
        return "season_id must be non-zero"
    if row.ends_at_ms and row.ends_at_ms <= row.starts_at_ms:
        return "ends_at_ms must be 0 or greater than starts_at_ms"
    if not 1 <= row.seal_max_points <= HARD_MAX_PROGRESS_POINTS:
        return "seal_max_points out of range [1, 1000]"
    if row.max_health_bps_cap > HARD_STAT_CAP_BPS or row.defense_bps_cap > HARD_STAT_CAP_BPS:
        return "season stat cap exceeds hard limit 500 bps"
    if row.catchup_currency_type > MAX_VALID_CURRENCY_TYPE:
        return "catchup_currency_type invalid"
    if row.catchup_price_per_point > HARD_MAX_CATCHUP_PRICE_PER_POINT:
        return "catchup_price_per_point exceeds limit"
    if not row.default_seal_id:
        return "default_seal_id must be non-zero"
    return None


def valid_seal_row(seal) -> bool:
    """校验单条 SeasonSeal 配置行是否合法。"""
    if not seal.season_id or not seal.id:
        return False
    return valid_artifact_definition(
        seal.effect_id,
        seal.bonus_bps,
        seal.cap_bps,
        seal.per_round_limit,
        seal.battle_limit,
        seal.active_rounds,
        seal.health_threshold_bps,
    )


def scaled_bonus_bps(progress_points: int, max_points: int, cap_bps: int) -> int:
    """按进度点线性映射轻属性万分比，向下取整并受硬上限约束。"""
    if not max_points or not cap_bps or not progress_points:
        return 0
    capped_cap = min(cap_bps, HARD_STAT_CAP_BPS)
    clamped_points = min(progress_points, max_points)
    return (capped_cap * clamped_points) // max_points


def compute_catchup_plan(
    current_points: int,
    max_points: int,
    requested_points: int,
    price_per_point: int,
) -> tuple[int, int] | None:
    """计算追赶点数与总价；requested_points=0 表示补满剩余点数。"""
    if not max_points or current_points >= max_points or not price_per_point:
        return None
    remaining = max_points - current_points
    buy = remaining if requested_points == 0 else requested_points
    if not 1 <= buy <= remaining:
        return None
    total_price = price_per_point * buy
    if total_price > _MAX_UINT64 or total_price >= (1 << 63):
        return None
    return buy, total_price


def _find_record(comp, season_id: int):
    return next((rec for rec in comp.records if rec.season_id == season_id), None)


def _find_or_add_record(comp, season_id: int):
    existing = _find_record(comp, season_id)
    if existing is not None:
        return existing
    return comp.records.add(season_id=season_id)


def find_valid_seal(tables, season_id: int, seal_id: int):
    """按 (season_id, seal_id) 查找有效赛季印行。"""
    if not season_id or not seal_id or "SeasonSeal" not in tables.tables:
        return None
    row = tables["SeasonSeal"].get(seal_id)
    if row is None or row.season_id != season_id or not valid_seal_row(row):
        return None
    return row


def find_active_season_rule(tables, now_ms_val: int):
    """查找当前时刻唯一活跃且合法的 SeasonRule；重叠或配置非法时 fail-closed 返回 None。"""
    if "SeasonRule" not in tables.tables:
        return None
    active = None
    for row in tables["SeasonRule"].rows():
        if not is_window_active(row.starts_at_ms, row.ends_at_ms, bool(row.enabled), now_ms_val):
            continue
        if validate_rule(row) is not None or find_valid_seal(tables, row.id, row.default_seal_id) is None:
            return None
        if active is not None:
            return None
        active = row
    return active


def _truncate_utf8(text: str, max_bytes: int) -> str:
    """按 UTF-8 字节上限安全截断字符串，避免截断多字节字符。"""
    cleaned = text.strip()
    raw = cleaned.encode("utf-8")
    if len(raw) <= max_bytes:
        return cleaned
    return raw[:max_bytes].decode("utf-8", errors="ignore").strip()


def _copy_bookmark_to_view(src, dst) -> None:
    """将持久化配装收藏条目投影至客户端视图消息。"""
    dst.bookmark_id = src.bookmark_id
    dst.title = src.title
    dst.source_battle_id = src.source_battle_id
    dst.source_player_id = src.source_player_id
    dst.source_player_name = src.source_player_name
    dst.class_id = src.class_id
    dst.formation_id = src.formation_id
    dst.season_seal_id = src.season_seal_id
    dst.active_pet_table_id = src.active_pet_table_id
    dst.active_pet_name = src.active_pet_name
    dst.skill_table_ids.extend(src.skill_table_ids)
    dst.wuxing_sigil_ids.extend(src.wuxing_sigil_ids)
    dst.saved_at_ms = src.saved_at_ms


def _sync_tower_week(tower, week_key: int) -> bool:
    """跨周自动重置五行幻境周位图与在途轮次，保留单调 run_id 与 revision。"""
    if tower.week_key == week_key:
        if tower.current_floor == 0:
            tower.current_floor = 1
        return False
    tower.week_key = week_key
    tower.run_active = False
    tower.current_floor = 1
    tower.run_total_rounds = 0
    tower.ClearField("active_sigil_ids")
    tower.ClearField("pending_offer_sigil_ids")
    tower.weekly_cleared_mask = 0
    tower.weekly_best_floor = 0
    tower.weekly_best_rounds = 0
    return True


class SceneSeason:
    """Scene 赛季技能与单槽赛季印服务，管理状态快照、切换、快捷追赶、五行阵法/幻境爬塔与配装收藏。"""

    def __init__(self, scene, tables, assets=None, *, clock=now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        tx_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self._debit_tx = tx_types.values_by_name["TX_CURRENCY_DEDUCT"].number
        self._catchup_receipts: dict[tuple[int, int], tuple[int, int]] = {}
        # 五行幻境周最速榜内存快照：week_key -> {player_id: (best_floor, best_rounds, player_name, formation_id, sigil_ids)}
        self._tower_leaderboards: dict[int, dict[int, tuple[int, int, str, int, tuple[int, ...]]]] = {}

    def register(self) -> None:
        """向 SceneNode 注册赛季及五行玩法扩展客户端 RPC（含 515..521）。"""
        self.scene.register(_SERVICE, "GetSeasonState", self.get_state)
        self.scene.register(_SERVICE, "EquipSeasonSeal", self.equip_seal, mutating=True)
        self.scene.register(_SERVICE, "CatchupSeasonSeal", self.catchup, mutating=True)
        self.scene.register(_SERVICE, "EquipWuxingFormation", self.equip_wuxing_formation, mutating=True)
        self.scene.register(_SERVICE, "GetWuxingTowerState", self.get_wuxing_tower_state)
        self.scene.register(_SERVICE, "StartWuxingTowerRun", self.start_wuxing_tower_run, mutating=True)
        self.scene.register(_SERVICE, "PickWuxingTowerSigil", self.pick_wuxing_tower_sigil, mutating=True)
        self.scene.register(_SERVICE, "AbandonWuxingTowerRun", self.abandon_wuxing_tower_run, mutating=True)
        self.scene.register(_SERVICE, "SaveBattleBuildBookmark", self.save_battle_build_bookmark, mutating=True)
        self.scene.register(_SERVICE, "DeleteBattleBuildBookmark", self.delete_battle_build_bookmark, mutating=True)

    def validate_tables(self) -> bool:
        """启动前校验 SeasonRule 与 SeasonSeal 配表一致性。"""
        rules = self.tables["SeasonRule"].rows() if "SeasonRule" in self.tables.tables else ()
        seals = self.tables["SeasonSeal"].rows() if "SeasonSeal" in self.tables.tables else ()
        if not rules:
            return not seals
        enabled_rules = []
        rule_ids = set()
        for rule in rules:
            if validate_rule(rule) is not None:
                return False
            if find_valid_seal(self.tables, rule.id, rule.default_seal_id) is None:
                return False
            rule_ids.add(rule.id)
            if rule.enabled:
                enabled_rules.append(rule)
        for index, first in enumerate(enabled_rules):
            for second in enabled_rules[index + 1:]:
                if windows_overlap(first.starts_at_ms, first.ends_at_ms, second.starts_at_ms, second.ends_at_ms):
                    return False
        return all(seal.season_id in rule_ids and valid_seal_row(seal) for seal in seals)

    def initialize_on_load(self, data) -> None:
        """角色载入时确保 season_component 存在，不删除历史赛季归档。"""
        data.season_component.SetInParent()

    def active_season_id(self, now_ms_val: int | None = None) -> int:
        """返回当前活跃赛季 ID，未开启或配置冲突返回 0。"""
        stamp = self.clock() if now_ms_val is None else now_ms_val
        rule = find_active_season_rule(self.tables, stamp)
        return rule.id if rule is not None else 0

    def _effective_seal_id(self, rule, record) -> int:
        if record is not None and record.equipped_seal_id:
            if find_valid_seal(self.tables, rule.id, record.equipped_seal_id) is not None:
                return record.equipped_seal_id
        return rule.default_seal_id

    def _writable_error(self, player) -> int:
        if not self.scene.mutable(player):
            return SEASON_FROZEN
        battle = getattr(self.scene, "battle", None)
        if battle is not None and battle.frozen(player.player_id):
            return SEASON_IN_BATTLE
        return 0

    def record_battle_progress(
        self,
        data,
        *,
        match_mode: int,
        player_won: bool,
        fled: bool = False,
        total_rounds: int = 1,
        now_ms_val: int | None = None,
        competitive_ranked: bool = False,
        participated: bool = True,
        manual_action_rounds: int = 0,
        effective_combat_contribution: bool = False,
    ) -> int:
        """按权威战斗结果累计当季赛季印进度点，返回实际新增点数。"""
        if fled or total_rounds <= 0:
            return 0
        if competitive_ranked:
            if not participated:
                return 0
            if not player_won and (
                total_rounds < PVP_MIN_ROUNDS_FOR_LOSS
                or manual_action_rounds < PVP_MIN_MANUAL_ROUNDS_FOR_LOSS
                or not effective_combat_contribution
            ):
                return 0
        stamp = self.clock() if now_ms_val is None else now_ms_val
        rule = find_active_season_rule(self.tables, stamp)
        if rule is None:
            return 0
        if match_mode in _PVE_MODES:
            gain = rule.pve_win_points if player_won else 0
        elif match_mode in _PVP_MODES:
            gain = rule.pvp_win_points if player_won else rule.pvp_other_points
        else:
            gain = 0
        if gain <= 0:
            return 0
        record = _find_or_add_record(data.season_component, rule.id)
        if not record.equipped_seal_id:
            record.equipped_seal_id = rule.default_seal_id
        before = min(record.progress_points, rule.seal_max_points)
        if before >= rule.seal_max_points:
            return 0
        added = min(gain, rule.seal_max_points - before)
        record.progress_points = before + added
        record.battle_points += added
        return added

    def collect_effects(self, data, effects, *, now_ms_val: int | None = None) -> None:
        """汇总当前活跃赛季印的轻属性与战斗机制至 EquipmentEffects。"""
        effects.ClearField("season_seal")
        effects.season_max_health_bps = 0
        effects.season_defense_bps = 0
        if not data.HasField("season_component"):
            return
        stamp = self.clock() if now_ms_val is None else now_ms_val
        rule = find_active_season_rule(self.tables, stamp)
        if rule is None:
            return
        record = _find_record(data.season_component, rule.id)
        seal_id = self._effective_seal_id(rule, record)
        seal = find_valid_seal(self.tables, rule.id, seal_id)
        if seal is None:
            return
        progress = min(record.progress_points, rule.seal_max_points) if record is not None else 0
        effects.season_max_health_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.max_health_bps_cap)
        effects.season_defense_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.defense_bps_cap)
        dst = effects.season_seal
        dst.season_id = rule.id
        dst.seal_id = seal.id
        dst.effect_id = seal.effect_id
        dst.bonus_bps = seal.bonus_bps
        dst.cap_bps = seal.cap_bps
        dst.per_round_limit = seal.per_round_limit
        dst.battle_limit = seal.battle_limit
        dst.active_rounds = seal.active_rounds
        dst.health_threshold_bps = seal.health_threshold_bps
        dst.name = seal.name

    def _fill_state(self, player, data, stamp: int, out) -> int:
        out.Clear()
        out.server_now_ms = stamp
        if not data.HasField("level_component"):
            return SEASON_UNAVAILABLE
        # 无论当前是否存在进行中的赛季，始终回填玩家已装配五行阵法与公开配装收藏夹。
        comp = data.season_component
        fid = int(comp.equipped_formation_id)
        out.equipped_formation_id = fid if is_valid_formation_id(fid) else 0
        for bookmark in comp.saved_builds:
            _copy_bookmark_to_view(bookmark, out.saved_builds.add())

        rule = find_active_season_rule(self.tables, stamp)
        if rule is None:
            out.available = False
            out.unavailable_reason = "当前暂无进行中的赛季"
            return 0

        record = _find_record(data.season_component, rule.id)
        equipped_id = self._effective_seal_id(rule, record)
        raw_progress = record.progress_points if record is not None else 0
        progress = min(raw_progress, rule.seal_max_points)
        battle_pts = min(record.battle_points, progress) if record is not None else 0
        catchup_pts = min(record.catchup_points, progress) if record is not None else 0

        out.available = True
        out.season_id = rule.id
        out.season_name = rule.name
        out.season_desc = rule.desc
        out.starts_at_ms = rule.starts_at_ms
        out.ends_at_ms = rule.ends_at_ms

        out.equipped_seal_id = equipped_id
        out.progress_points = progress
        out.max_points = rule.seal_max_points
        out.battle_points = battle_pts
        out.catchup_points = catchup_pts
        out.current_max_health_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.max_health_bps_cap)
        out.cap_max_health_bps = rule.max_health_bps_cap
        out.current_defense_bps = scaled_bonus_bps(progress, rule.seal_max_points, rule.defense_bps_cap)
        out.cap_defense_bps = rule.defense_bps_cap
        out.pve_win_points = rule.pve_win_points
        out.pvp_win_points = rule.pvp_win_points
        out.pvp_other_points = rule.pvp_other_points

        out.catchup_currency_type = rule.catchup_currency_type
        out.catchup_price_per_point = rule.catchup_price_per_point
        full_plan = compute_catchup_plan(progress, rule.seal_max_points, 0, rule.catchup_price_per_point)
        out.catchup_remaining_total_price = full_plan[1] if full_plan is not None else 0
        out.currency_balance = balance(data, rule.catchup_currency_type)
        out.revision = record.revision if record is not None else 0

        writable_err = self._writable_error(player)
        out.can_switch_seal = writable_err == 0
        out.can_catchup = writable_err == 0 and full_plan is not None
        if writable_err == SEASON_IN_BATTLE:
            out.action_unavailable_reason = "战斗中暂不能更换或追赶赛季印，请在结算后重试"
        elif writable_err:
            out.action_unavailable_reason = "当前角色状态暂时不能操作赛季印，请稍后再试"

        seals = [
            seal for seal in self.tables["SeasonSeal"].rows()
            if seal.season_id == rule.id and valid_seal_row(seal)
        ]
        seals.sort(key=lambda row: row.id)
        for seal in seals:
            out.seals.add(
                seal_id=seal.id,
                name=seal.name,
                description=seal.description,
                icon_key=seal.icon_key,
                effect_id=seal.effect_id,
                bonus_bps=seal.bonus_bps,
                cap_bps=seal.cap_bps,
                per_round_limit=seal.per_round_limit,
                battle_limit=seal.battle_limit,
                active_rounds=seal.active_rounds,
                health_threshold_bps=seal.health_threshold_bps,
                equipped=seal.id == equipped_id,
            )

        level = effective_level(data)
        season_skills = [
            skill for skill in self.tables["Skill"].rows()
            if getattr(skill, "season_id", 0) == rule.id
        ]
        season_skills.sort(key=lambda row: row.id)
        for skill in season_skills:
            eff_rank = rank_limit(level, skill)
            out.season_skills.add(
                skill_table_id=skill.id,
                name=skill.name,
                description=skill.description,
                icon_key=skill.icon_key,
                category=skill.category,
                target_team=skill.target_team,
                max_targets=skill.max_targets,
                mana_percent=skill.mana_percent,
                cooldown_rounds=skill.cooldown_rounds,
                unlock_level=skill.unlock_level,
                max_rank=skill.max_rank,
                effective_rank=eff_rank,
                unlocked=eff_rank > 0,
                ability_kind=getattr(skill, "ability_kind", 0),
                caster_mask=getattr(skill, "caster_mask", 0),
                action_tags=getattr(skill, "action_tags", 0),
            )
        return 0

    def _finish(self, response, player, data, stamp: int, error: int = 0):
        if error:
            response.error_message.id = error
        state_err = self._fill_state(player, data, stamp, response.state)
        if state_err:
            response.ClearField("state")
            if not error:
                response.error_message.id = state_err
        return response

    def get_state(self, _world, player, _route, _request):
        """查询当前赛季状态快照（只读）。"""
        response = self.protocol.message("GetSeasonStateResponse")
        data = self.scene.rows.assemble(player.snapshot)
        return self._finish(response, player, data, self.clock())

    async def equip_seal(self, _world, player, _route, request):
        """战外免费切换当前激活赛季的赛季印。"""
        response = self.protocol.message("EquipSeasonSealResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)

        rule = find_active_season_rule(self.tables, stamp)
        if rule is None:
            return self._finish(response, player, data, stamp, SEASON_NOT_OPEN)
        if not request.season_id or request.season_id != rule.id:
            return self._finish(response, player, data, stamp, SEASON_MISMATCH)
        if find_valid_seal(self.tables, rule.id, request.seal_id) is None:
            return self._finish(response, player, data, stamp, SEASON_SEAL_INVALID)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        current_seal_id = self._effective_seal_id(rule, _find_record(data.season_component, rule.id))
        if current_seal_id == request.seal_id:
            return self._finish(response, player, data, stamp, SEASON_SEAL_ALREADY_EQUIPPED)

        record = _find_or_add_record(data.season_component, rule.id)
        record.equipped_seal_id = request.seal_id
        if record.revision < _MAX_UINT64:
            record.revision += 1

        attributes = getattr(self.scene, "attributes", None)
        on_committed = (lambda _snapshot: attributes.notify(player)) if attributes is not None else None
        try:
            await self.scene.commit_player_data(player, data, on_committed=on_committed)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Equipped season seal player_id=%s season_id=%s seal_id=%s",
            player.player_id, rule.id, request.seal_id,
        )
        return self._finish(response, player, data, stamp)

    def _remember_receipt(self, player_id: int, request_id: int, added_points: int, spent_currency: int) -> None:
        if len(self._catchup_receipts) >= _MAX_CACHED_RECEIPTS:
            oldest = next(iter(self._catchup_receipts))
            del self._catchup_receipts[oldest]
        self._catchup_receipts[(player_id, request_id)] = (added_points, spent_currency)

    def _replay_receipt(self, player_id: int, request, rule) -> tuple[int, int]:
        cached = self._catchup_receipts.get((player_id, request.client_request_id))
        if cached is not None:
            return cached
        spent = request.expected_total_price
        if request.points_to_buy:
            added = request.points_to_buy
        elif rule is not None and rule.catchup_price_per_point:
            added = spent // rule.catchup_price_per_point
        else:
            added = 0
        return added, spent

    async def catchup(self, _world, player, _route, request):
        """灵石/货币快捷追赶当前赛季印进度，带 client_request_id 幂等与报价防呆。"""
        response = self.protocol.message("CatchupSeasonSealResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not request.client_request_id:
            return self._finish(response, player, data, stamp, INVALID)

        # 幂等查账排在一切状态校验之前，确保超时重放始终返回首次结果而不双扣。
        ledger = data.client_request_ledger
        err, fingerprint, prior = client_request.check(ledger, request, _CLIENT_REQUEST_KIND_CATCHUP)
        if err:
            return self._finish(response, player, data, stamp, err)
        if prior is not None:
            rule = find_active_season_rule(self.tables, stamp)
            added, spent = self._replay_receipt(player.player_id, request, rule)
            response.replayed = True
            response.added_points = added
            response.spent_currency = spent
            return self._finish(response, player, data, stamp)

        if not data.HasField("level_component") or self.assets is None:
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)

        rule = find_active_season_rule(self.tables, stamp)
        if rule is None:
            return self._finish(response, player, data, stamp, SEASON_NOT_OPEN)
        if not request.season_id or request.season_id != rule.id:
            return self._finish(response, player, data, stamp, SEASON_MISMATCH)
        if not rule.catchup_price_per_point:
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_DISABLED)
        if writable_err := self._writable_error(player):
            return self._finish(response, player, data, stamp, writable_err)

        existing = _find_record(data.season_component, rule.id)
        current_points = min(existing.progress_points, rule.seal_max_points) if existing is not None else 0
        if current_points >= rule.seal_max_points:
            return self._finish(response, player, data, stamp, SEASON_SEAL_ALREADY_MAX)

        plan = compute_catchup_plan(
            current_points, rule.seal_max_points, request.points_to_buy, rule.catchup_price_per_point,
        )
        if plan is None:
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_PRICE_CHANGED)
        points_to_buy, total_price = plan

        if (
            request.expected_currency_type != rule.catchup_currency_type
            or request.expected_total_price != total_price
        ):
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_PRICE_CHANGED)

        if balance(data, rule.catchup_currency_type) < total_price:
            return self._finish(response, player, data, stamp, SEASON_CATCHUP_CURRENCY_NOT_ENOUGH)

        before = clone(data) if self.assets.audit_enabled else None
        deduct_err = self.assets.deduct_currency(data, rule.catchup_currency_type, total_price)
        if deduct_err:
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, deduct_err)

        record = _find_or_add_record(data.season_component, rule.id)
        if not record.equipped_seal_id:
            record.equipped_seal_id = rule.default_seal_id
        record.progress_points = current_points + points_to_buy
        record.catchup_points += points_to_buy
        if record.revision < _MAX_UINT64:
            record.revision += 1

        client_request.remember(ledger, request.client_request_id, _CLIENT_REQUEST_KIND_CATCHUP, fingerprint)
        audit = (
            self.assets.make_audit_callback(
                player, before, data, self._debit_tx, correlation=request.client_request_id,
            )
            if before is not None else None
        )
        attributes = getattr(self.scene, "attributes", None)

        async def _committed(snapshot):
            if audit is not None:
                await audit(snapshot)
            if attributes is not None:
                attributes.notify(player)

        callback = _committed if attributes is not None else audit
        try:
            await self.scene.commit_player_data(player, data, on_committed=callback)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish(response, player, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)

        self._remember_receipt(player.player_id, request.client_request_id, points_to_buy, total_price)
        logger.info(
            "Season seal catchup succeeded player_id=%s season_id=%s added_points=%s progress=%s/%s spent=%s",
            player.player_id, rule.id, points_to_buy, record.progress_points, rule.seal_max_points, total_price,
        )
        response.added_points = points_to_buy
        response.spent_currency = total_price
        return self._finish(response, player, data, stamp)

    # ── 五行阵法 / 五行幻境周常爬塔 / 战报配装收藏夹（515..521） ──

    async def equip_wuxing_formation(self, _world, player, _route, request):
        """515 战外免费切换五行阵法（0..5，零数值付费）。"""
        response = self.protocol.message("EquipWuxingFormationResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            return self._finish(response, player, data, stamp, SEASON_UNAVAILABLE)
        fid = int(request.formation_id)
        if not is_valid_formation_id(fid):
            response.equipped_formation_id = int(data.season_component.equipped_formation_id)
            return self._finish(response, player, data, stamp, INVALID)
        if writable_err := self._writable_error(player):
            response.equipped_formation_id = int(data.season_component.equipped_formation_id)
            return self._finish(response, player, data, stamp, writable_err)

        data.season_component.equipped_formation_id = fid
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            latest = self.scene.rows.assemble(player.snapshot)
            response.equipped_formation_id = int(latest.season_component.equipped_formation_id)
            return self._finish(response, player, latest, stamp, UNAVAILABLE)
        response.equipped_formation_id = fid
        logger.info("Equipped wuxing formation player_id=%s formation_id=%s", player.player_id, fid)
        return self._finish(response, player, data, stamp)

    def _fill_tower_state(self, data, stamp: int, out) -> None:
        out.Clear()
        wk = wuxing_week_key(stamp)
        view_tower = self.protocol.message("WuxingTowerCompState")
        if data.HasField("season_component") and data.season_component.HasField("wuxing_tower"):
            view_tower.CopyFrom(data.season_component.wuxing_tower)
        _sync_tower_week(view_tower, wk)
        floor = view_tower.current_floor if 1 <= view_tower.current_floor <= WUXING_TOWER_MAX_FLOORS else 1
        out.week_key = wk
        out.run_active = bool(view_tower.run_active)
        out.run_id = view_tower.run_id
        out.current_floor = floor
        out.max_floors = WUXING_TOWER_MAX_FLOORS
        out.current_floor_dungeon_id = tower_floor_dungeon_id(floor)
        out.current_floor_leyline_element = tower_floor_leyline_element(wk, floor)
        out.run_total_rounds = view_tower.run_total_rounds
        out.active_sigil_ids.extend(view_tower.active_sigil_ids)
        out.pending_offer_sigil_ids.extend(view_tower.pending_offer_sigil_ids)
        out.weekly_cleared_mask = view_tower.weekly_cleared_mask
        out.weekly_best_floor = view_tower.weekly_best_floor
        out.weekly_best_rounds = view_tower.weekly_best_rounds
        out.revision = view_tower.revision

        board = self._tower_leaderboards.get(wk, {})
        ranked = sorted(
            board.items(),
            key=lambda kv: (-kv[1][0], kv[1][1] if kv[1][1] > 0 else (1 << 30), kv[0]),
        )[:_TOWER_LEADERBOARD_TOP_N]
        for rank_idx, (pid, (best_floor, best_rounds, pname, fid, sigils)) in enumerate(ranked, start=1):
            entry = out.weekly_leaderboard.add(
                rank=rank_idx,
                player_id=pid,
                player_name=pname,
                best_floor=best_floor,
                best_rounds=best_rounds,
                formation_id=fid,
            )
            entry.sigil_ids.extend(sigils)

    def _finish_tower(self, response, data, stamp: int, error: int = 0):
        if error:
            response.error_message.id = error
        self._fill_tower_state(data, stamp, response.state)
        return response

    def get_wuxing_tower_state(self, _world, player, _route, _request):
        """516 查询五行幻境周常爬塔状态与本周最速流派榜（只读）。"""
        response = self.protocol.message("GetWuxingTowerStateResponse")
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        return self._finish_tower(response, data, self.clock())

    async def start_wuxing_tower_run(self, _world, player, _route, _request):
        """517 开启新一轮五行幻境周常爬塔（从第 1 层起，清空旧局内符印）。"""
        response = self.protocol.message("StartWuxingTowerRunResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        wk = wuxing_week_key(stamp)
        tower = data.season_component.wuxing_tower
        _sync_tower_week(tower, wk)
        if tower.run_active:
            return self._finish_tower(response, data, stamp, INVALID)

        tower.run_active = True
        if tower.run_id < _MAX_UINT64:
            tower.run_id += 1
        tower.current_floor = 1
        tower.run_total_rounds = 0
        tower.ClearField("active_sigil_ids")
        tower.ClearField("pending_offer_sigil_ids")
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info("Started wuxing tower run player_id=%s week_key=%s run_id=%s", player.player_id, wk, tower.run_id)
        return self._finish_tower(response, data, stamp)

    async def pick_wuxing_tower_sigil(self, _world, player, _route, request):
        """518 通关一层后从三选一候选中确定选择一枚局内五行符印。"""
        response = self.protocol.message("PickWuxingTowerSigilResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        wk = wuxing_week_key(stamp)
        tower = data.season_component.wuxing_tower
        _sync_tower_week(tower, wk)
        sigil_id = int(request.sigil_id)
        if (
            not tower.run_active
            or request.expected_run_id != tower.run_id
            or not tower.pending_offer_sigil_ids
            or sigil_id not in tower.pending_offer_sigil_ids
            or not is_valid_tower_sigil_id(sigil_id)
            or sigil_id in tower.active_sigil_ids
            or len(tower.active_sigil_ids) >= _MAX_TOWER_ACTIVE_SIGILS
        ):
            return self._finish_tower(response, data, stamp, INVALID)

        tower.active_sigil_ids.append(sigil_id)
        tower.ClearField("pending_offer_sigil_ids")
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info(
            "Picked wuxing tower sigil player_id=%s run_id=%s sigil_id=%s active_count=%s",
            player.player_id, tower.run_id, sigil_id, len(tower.active_sigil_ids),
        )
        return self._finish_tower(response, data, stamp)

    async def abandon_wuxing_tower_run(self, _world, player, _route, request):
        """519 放弃当前在途五行幻境爬塔轮次并立即清空局内符印。"""
        response = self.protocol.message("AbandonWuxingTowerRunResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("level_component"):
            response.error_message.id = SEASON_UNAVAILABLE
            return response
        if writable_err := self._writable_error(player):
            return self._finish_tower(response, data, stamp, writable_err)

        wk = wuxing_week_key(stamp)
        tower = data.season_component.wuxing_tower
        _sync_tower_week(tower, wk)
        if not tower.run_active or (request.expected_run_id and request.expected_run_id != tower.run_id):
            return self._finish_tower(response, data, stamp, INVALID)

        abandoned_run = tower.run_id
        tower.run_active = False
        tower.current_floor = 1
        tower.run_total_rounds = 0
        tower.ClearField("active_sigil_ids")
        tower.ClearField("pending_offer_sigil_ids")
        if tower.revision < _MAX_UINT64:
            tower.revision += 1

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            return self._finish_tower(response, self.scene.rows.assemble(player.snapshot), stamp, UNAVAILABLE)
        logger.info("Abandoned wuxing tower run player_id=%s run_id=%s", player.player_id, abandoned_run)
        return self._finish_tower(response, data, stamp)

    def settle_wuxing_tower_battle(
        self,
        data,
        settlement,
        *,
        now_ms_val: int | None = None,
        player_name: str = "",
    ) -> bool:
        """幂等推进五行幻境单层结算：校验 (week_key, run_id, floor) 与待选符印为空，通关 1..9 层生成三选一，第 10 层入榜。"""
        if not settlement.HasField("tower_settlement"):
            return False
        player_won = (
            (settlement.outcome == 1 and settlement.player_team_index == 0)
            or (settlement.outcome == 2 and settlement.player_team_index == 1)
        )
        if not player_won or settlement.fled:
            return False
        echo = settlement.tower_settlement
        stamp = self.clock() if now_ms_val is None else now_ms_val
        wk = wuxing_week_key(stamp)
        if echo.week_key != wk or not (1 <= echo.floor <= WUXING_TOWER_MAX_FLOORS):
            return False
        tower = data.season_component.wuxing_tower
        _sync_tower_week(tower, wk)
        if (
            not tower.run_active
            or tower.run_id != echo.run_id
            or tower.current_floor != echo.floor
            or tower.pending_offer_sigil_ids
        ):
            return False

        floor = int(echo.floor)
        tower.weekly_cleared_mask |= 1 << (floor - 1)
        tower.run_total_rounds += max(1, int(echo.total_rounds or settlement.total_rounds or 1))
        if floor > tower.weekly_best_floor:
            tower.weekly_best_floor = floor

        if floor < WUXING_TOWER_MAX_FLOORS:
            tower.current_floor = floor + 1
            offers = roll_tower_sigil_offer(wk, tower.run_id, floor, list(tower.active_sigil_ids))
            tower.pending_offer_sigil_ids.extend(offers)
        else:
            if tower.weekly_best_rounds == 0 or tower.run_total_rounds < tower.weekly_best_rounds:
                tower.weekly_best_rounds = tower.run_total_rounds
            pname = player_name or (data.profile_component.name if data.HasField("profile_component") else "")
            fid = int(data.season_component.equipped_formation_id)
            sigil_tuple = tuple(tower.active_sigil_ids)
            board = self._tower_leaderboards.setdefault(wk, {})
            prev = board.get(data.player_id)
            if prev is None or floor > prev[0] or (floor == prev[0] and tower.weekly_best_rounds < prev[1]):
                board[data.player_id] = (floor, tower.weekly_best_rounds, pname, fid, sigil_tuple)
            tower.run_active = False
            tower.current_floor = 1
            tower.ClearField("active_sigil_ids")
            tower.ClearField("pending_offer_sigil_ids")

        if tower.revision < _MAX_UINT64:
            tower.revision += 1
        return True

    async def save_battle_build_bookmark(self, _world, player, _route, request):
        """520 保存或更新战报公开脱敏配装收藏（单角色硬上限 12 条，按 bookmark_id 幂等覆盖）。"""
        response = self.protocol.message("SaveBattleBuildBookmarkResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.season_component

        def _finish_bm(error: int = 0, saved_entry=None):
            if error:
                response.error_message.id = error
            if saved_entry is not None:
                _copy_bookmark_to_view(saved_entry, response.saved)
            for item in comp.saved_builds:
                _copy_bookmark_to_view(item, response.all_bookmarks.add())
            return response

        if not data.HasField("level_component"):
            return _finish_bm(SEASON_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return _finish_bm(writable_err)
        if not request.HasField("bookmark"):
            return _finish_bm(INVALID)

        incoming = request.bookmark
        existing = (
            next((b for b in comp.saved_builds if b.bookmark_id == incoming.bookmark_id), None)
            if incoming.bookmark_id
            else None
        )
        if existing is None and len(comp.saved_builds) >= MAX_SAVED_BUILD_BOOKMARKS:
            return _finish_bm(INVALID)

        if existing is not None:
            target = existing
            bookmark_id = existing.bookmark_id
        else:
            bookmark_id = incoming.bookmark_id or (max((b.bookmark_id for b in comp.saved_builds), default=0) + 1)
            target = comp.saved_builds.add()

        target.Clear()
        target.bookmark_id = bookmark_id
        target.title = _truncate_utf8(incoming.title, _MAX_BOOKMARK_TITLE_BYTES) or "配装方案"
        target.source_battle_id = incoming.source_battle_id
        target.source_player_id = incoming.source_player_id
        target.source_player_name = _truncate_utf8(incoming.source_player_name, _MAX_BOOKMARK_TITLE_BYTES)
        target.class_id = incoming.class_id
        target.formation_id = incoming.formation_id if is_valid_formation_id(incoming.formation_id) else 0
        target.season_seal_id = incoming.season_seal_id
        target.active_pet_table_id = incoming.active_pet_table_id
        target.active_pet_name = _truncate_utf8(incoming.active_pet_name, _MAX_BOOKMARK_TITLE_BYTES)
        seen_skills: set[int] = set()
        for sid in incoming.skill_table_ids:
            if sid and sid not in seen_skills:
                seen_skills.add(sid)
                target.skill_table_ids.append(sid)
                if len(target.skill_table_ids) >= _MAX_BOOKMARK_SKILLS:
                    break
        seen_sigils: set[int] = set()
        for sg in incoming.wuxing_sigil_ids:
            if is_valid_tower_sigil_id(sg) and sg not in seen_sigils:
                seen_sigils.add(sg)
                target.wuxing_sigil_ids.append(sg)
                if len(target.wuxing_sigil_ids) >= _MAX_TOWER_ACTIVE_SIGILS:
                    break
        target.saved_at_ms = stamp

        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            latest = self.scene.rows.assemble(player.snapshot)
            comp = latest.season_component
            return _finish_bm(UNAVAILABLE)
        logger.info("Saved battle build bookmark player_id=%s bookmark_id=%s", player.player_id, bookmark_id)
        return _finish_bm(0, target)

    async def delete_battle_build_bookmark(self, _world, player, _route, request):
        """521 删除指定战报配装收藏条目。"""
        response = self.protocol.message("DeleteBattleBuildBookmarkResponse")
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.season_component

        def _finish_del(error: int = 0):
            if error:
                response.error_message.id = error
            for item in comp.saved_builds:
                _copy_bookmark_to_view(item, response.all_bookmarks.add())
            return response

        if not data.HasField("level_component"):
            return _finish_del(SEASON_UNAVAILABLE)
        if writable_err := self._writable_error(player):
            return _finish_del(writable_err)
        if not request.bookmark_id:
            return _finish_del(INVALID)
        idx = next((i for i, b in enumerate(comp.saved_builds) if b.bookmark_id == request.bookmark_id), -1)
        if idx < 0:
            return _finish_del(INVALID)

        del comp.saved_builds[idx]
        try:
            await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError):
            latest = self.scene.rows.assemble(player.snapshot)
            comp = latest.season_component
            return _finish_del(UNAVAILABLE)
        logger.info("Deleted battle build bookmark player_id=%s bookmark_id=%s", player.player_id, request.bookmark_id)
        return _finish_del(0)
