"""赛季技能与单槽赛季印（方案 A）系统，写操作随角色存档提交。"""

from __future__ import annotations

import logging
import time

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import INVALID, UNAVAILABLE, balance, clone
from .scene_mission import effective_level, now_ms
from .scene_stats import rank_limit

logger = logging.getLogger(__name__)

_SERVICE = "SceneSeasonClientPlayer"
_CLIENT_REQUEST_KIND_CATCHUP = 9
_MAX_UINT64 = (1 << 64) - 1
_MAX_CACHED_RECEIPTS = 256

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


class SceneSeason:
    """Scene 赛季技能与单槽赛季印服务，管理状态快照、切换与快捷追赶。"""

    def __init__(self, scene, tables, assets=None, *, clock=now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        tx_types = self.protocol.pool.FindEnumTypeByName("TransactionType")
        self._debit_tx = tx_types.values_by_name["TX_CURRENCY_DEDUCT"].number
        self._catchup_receipts: dict[tuple[int, int], tuple[int, int]] = {}

    def register(self) -> None:
        """向 SceneNode 注册赛季客户端 RPC。"""
        self.scene.register(_SERVICE, "GetSeasonState", self.get_state)
        self.scene.register(_SERVICE, "EquipSeasonSeal", self.equip_seal, mutating=True)
        self.scene.register(_SERVICE, "CatchupSeasonSeal", self.catchup, mutating=True)

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
