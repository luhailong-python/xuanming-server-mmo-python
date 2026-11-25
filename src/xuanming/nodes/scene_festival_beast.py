"""瑞兽迎春限时活动系统：详情投影、携宠单人挑战准入、胜场进度与永久专属神兽领奖。"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable
from ..data.asset_inventory import INVALID, clone
from .scene_mission import effective_level, now_ms

logger = logging.getLogger(__name__)

_SERVICE = "SceneActivityClientPlayer"
_MAX_UINT64 = (1 << 64) - 1
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

DAY_MS = 86_400_000
REQUIRED_OPEN_DURATION_MS = 14 * DAY_MS
DEFAULT_SHOWCASE_DAYS = 7

# 阶段枚举（与 proto/scene/player_activity.proto FestivalBeastPhase 对齐）。
PHASE_UNSPECIFIED = 0
PHASE_UNSCHEDULED = 1
PHASE_UPCOMING = 2
PHASE_OPEN = 3
PHASE_CLAIM_SHOWCASE = 4
PHASE_HISTORY = 5

# 阻断码枚举（FestivalBeastActionBlockCode）。
BLOCK_NONE = 0
BLOCK_NOT_OPEN = 1
BLOCK_CONFIG_INVALID = 2
BLOCK_FROZEN = 3
BLOCK_IN_BATTLE = 4
BLOCK_PET_REQUIRED = 5
BLOCK_PET_DEAD = 6
BLOCK_LAUNCH_IN_FLIGHT = 7
BLOCK_NOT_ELIGIBLE = 8
BLOCK_ALREADY_CLAIMED = 9
BLOCK_CLAIM_CONFIRMING = 10
BLOCK_EXCLUSIVE_SLOT_OCCUPIED = 11
BLOCK_SERVICE_UNAVAILABLE = 12

# 开局状态枚举（FestivalBeastLaunchState）。
LAUNCH_UNSPECIFIED = 0
LAUNCH_LAUNCHING = 1
LAUNCH_ADMITTED = 2
LAUNCH_REJECTED = 3
LAUNCH_UNKNOWN = 4

# 领奖结果枚举（FestivalBeastClaimOutcome）。
CLAIM_UNSPECIFIED = 0
CLAIM_APPLIED = 1
CLAIM_ALREADY_CLAIMED = 2
CLAIM_REJECTED = 3
CLAIM_UNKNOWN = 4

# 业务错误码（与 C++ festival_beast_error_tip.pb.h 严格对齐）。
FESTIVAL_BEAST_NOT_OPEN = 36000
FESTIVAL_BEAST_CONFIG_INVALID = 36001
FESTIVAL_BEAST_CONFIG_REVISION_MISMATCH = 36002
FESTIVAL_BEAST_PET_REQUIRED = 36003
FESTIVAL_BEAST_PET_DEAD = 36004
FESTIVAL_BEAST_PET_CHANGED = 36005
FESTIVAL_BEAST_LAUNCH_IN_FLIGHT = 36006
FESTIVAL_BEAST_IN_BATTLE = 36007
FESTIVAL_BEAST_FROZEN = 36008
FESTIVAL_BEAST_NOT_ELIGIBLE = 36009
FESTIVAL_BEAST_ALREADY_CLAIMED = 36010
FESTIVAL_BEAST_EXCLUSIVE_SLOT_OCCUPIED = 36011
FESTIVAL_BEAST_REQUEST_CONFLICT = 36012
FESTIVAL_BEAST_SERVICE_UNAVAILABLE = 36013


@dataclass(frozen=True, slots=True)
class FestivalBeastActivitySpec:
    id: int = 202601
    config_revision: int = 1
    enabled: bool = False
    activity_name: str = "瑞兽迎春"
    boss_name: str = "春禧兽"
    banner_asset_key: str = "activity_festival_beast_banner"
    icon_key: str = "pet_chunxishou_icon"
    effective_start_at_ms: int = 0
    effective_end_at_ms: int = 0
    claim_showcase_days: int = DEFAULT_SHOWCASE_DAYS
    required_wins: int = 3
    reward_identity: str = "festival_beast_chunxishou_v1"
    reward_definition_version: int = 1
    reward_pet_table_id: int = 101
    battle_config_id: int = 111
    round_limit: int = 12
    cycle_rounds: int = 3
    first_heavy_round: int = 3
    telegraph_lead_rounds: int = 1
    heavy_damage_percent: int = 180


@dataclass(frozen=True, slots=True)
class FestivalBeastTierSpec:
    id: int
    min_level: int
    max_level: int
    boss_health: int
    boss_physical_attack: int
    boss_defense: int
    boss_resistance: int
    boss_speed: int


DEFAULT_FESTIVAL_BEAST_TIERS: tuple[FestivalBeastTierSpec, ...] = (
    FestivalBeastTierSpec(1, 1, 9, 2000, 140, 200, 5, 320),
    FestivalBeastTierSpec(2, 10, 19, 14000, 620, 700, 10, 520),
    FestivalBeastTierSpec(3, 20, 34, 26000, 1100, 1300, 12, 760),
    FestivalBeastTierSpec(4, 35, 49, 42000, 1750, 2200, 14, 1060),
    FestivalBeastTierSpec(5, 50, 64, 58000, 2400, 3200, 15, 1360),
    FestivalBeastTierSpec(6, 65, 79, 74000, 3050, 4200, 16, 1660),
    FestivalBeastTierSpec(7, 80, 99, 90000, 3750, 5200, 18, 1960),
    FestivalBeastTierSpec(8, 100, 120, 112000, 4700, 6600, 20, 2360),
)


def parse_uuid128(text: str) -> tuple[int, int] | None:
    """解析标准 36 字符 UUID 并拒绝全 0，返回 (high, low) 或 None。"""
    if not text or not _UUID_RE.match(text):
        return None
    hex_digits = text.replace("-", "")
    high = int(hex_digits[:16], 16)
    low = int(hex_digits[16:], 16)
    if high == 0 and low == 0:
        return None
    return high, low


def claim_showcase_end_at_ms(effective_end_ms: int, showcase_days: int) -> int:
    if effective_end_ms == 0:
        return 0
    days = showcase_days if showcase_days > 0 else DEFAULT_SHOWCASE_DAYS
    return effective_end_ms + days * DAY_MS


def resolve_phase(
    enabled: bool, start_ms: int, end_ms: int, showcase_days: int, now_ms_val: int,
) -> int:
    """按服务端 UTC 毫秒裁定瑞兽迎春活动阶段。"""
    if not enabled or start_ms == 0 or end_ms <= start_ms:
        return PHASE_UNSCHEDULED
    if now_ms_val < start_ms:
        return PHASE_UPCOMING
    if now_ms_val < end_ms:
        return PHASE_OPEN
    if now_ms_val < claim_showcase_end_at_ms(end_ms, showcase_days):
        return PHASE_CLAIM_SHOWCASE
    return PHASE_HISTORY


def find_tier_for_level(
    tiers: tuple[FestivalBeastTierSpec, ...], owner_level: int,
) -> FestivalBeastTierSpec | None:
    clamped = max(1, int(owner_level))
    for tier in tiers:
        if tier.min_level <= clamped <= tier.max_level:
            return tier
    return None


class SceneFestivalBeast:
    """Scene 瑞兽迎春限时活动处理器：3 条 SceneActivityClientPlayer RPC + 胜场结算推进。"""

    def __init__(
        self,
        scene,
        tables,
        assets=None,
        *,
        clock=now_ms,
        activities: tuple[FestivalBeastActivitySpec, ...] | None = None,
        tiers: tuple[FestivalBeastTierSpec, ...] | None = None,
        launch_battle: Callable[[int, FestivalBeastActivitySpec, int, int, str], Awaitable[int]] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._activities = activities
        self._tiers = tiers
        self._launch_battle = launch_battle
        self._next_battle_id = 900_000

    def register(self) -> None:
        """向 SceneNode 注册瑞兽迎春 3 条客户端 RPC。"""
        self.scene.register(_SERVICE, "GetFestivalBeastDetail", self.get_detail)
        self.scene.register(_SERVICE, "StartFestivalBeastChallenge", self.start_challenge, mutating=True)
        self.scene.register(_SERVICE, "ClaimFestivalBeastReward", self.claim_reward, mutating=True)

    # ── 配置解析辅助 ──

    def _load_activities(self) -> tuple[FestivalBeastActivitySpec, ...]:
        if self._activities is not None:
            return self._activities
        if "FestivalBeastActivity" in self.tables.tables:
            return tuple(
                FestivalBeastActivitySpec(
                    id=int(r.id),
                    config_revision=int(r.config_revision),
                    enabled=bool(r.enabled),
                    activity_name=str(r.activity_name),
                    boss_name=str(r.boss_name),
                    banner_asset_key=str(r.banner_asset_key),
                    icon_key=str(r.icon_key),
                    effective_start_at_ms=int(r.effective_start_at_ms),
                    effective_end_at_ms=int(r.effective_end_at_ms),
                    claim_showcase_days=int(r.claim_showcase_days),
                    required_wins=int(r.required_wins),
                    reward_identity=str(r.reward_identity),
                    reward_definition_version=int(r.reward_definition_version),
                    reward_pet_table_id=int(r.reward_pet_table_id),
                    battle_config_id=int(r.battle_config_id),
                    round_limit=int(r.round_limit),
                    cycle_rounds=int(r.cycle_rounds),
                    first_heavy_round=int(r.first_heavy_round),
                    telegraph_lead_rounds=int(r.telegraph_lead_rounds),
                    heavy_damage_percent=int(r.heavy_damage_percent),
                )
                for r in self.tables["FestivalBeastActivity"].rows()
            )
        return (FestivalBeastActivitySpec(),)

    def _load_tiers(self) -> tuple[FestivalBeastTierSpec, ...]:
        if self._tiers is not None:
            return self._tiers
        if "FestivalBeastTier" in self.tables.tables:
            return tuple(
                FestivalBeastTierSpec(
                    id=int(r.id),
                    min_level=int(r.min_level),
                    max_level=int(r.max_level),
                    boss_health=int(r.boss_health),
                    boss_physical_attack=int(r.boss_physical_attack),
                    boss_defense=int(r.boss_defense),
                    boss_resistance=int(r.boss_resistance),
                    boss_speed=int(r.boss_speed),
                )
                for r in self.tables["FestivalBeastTier"].rows()
            )
        return DEFAULT_FESTIVAL_BEAST_TIERS

    def _resolve_activity(
        self, requested_instance_id: int, comp,
    ) -> tuple[FestivalBeastActivitySpec | None, bool, tuple[FestivalBeastTierSpec, ...]]:
        tiers = self._load_tiers()
        all_rows = self._load_activities()
        enabled_rows = [r for r in all_rows if r.enabled]
        multiple_enabled = len(enabled_rows) > 1

        chosen: FestivalBeastActivitySpec | None = None
        if requested_instance_id != 0:
            chosen = next((r for r in all_rows if r.id == requested_instance_id), None)
        elif enabled_rows:
            chosen = enabled_rows[0]
        else:
            unclaimed = self._find_unclaimed_eligibility(comp)
            if unclaimed is not None:
                chosen = next((r for r in all_rows if r.id == unclaimed.source_activity_instance_id), None)
            if chosen is None and len(comp.eligibilities) > 0:
                last_elig = comp.eligibilities[-1]
                chosen = next((r for r in all_rows if r.id == last_elig.source_activity_instance_id), None)
            if chosen is None and all_rows:
                chosen = all_rows[-1]

        valid = False
        if chosen is not None and not multiple_enabled:
            valid = (
                chosen.id > 0
                and chosen.config_revision > 0
                and bool(chosen.reward_identity)
                and chosen.required_wins > 0
                and chosen.reward_pet_table_id > 0
                and bool(tiers)
            )
            if chosen.enabled and (
                chosen.effective_start_at_ms == 0
                or chosen.effective_end_at_ms <= chosen.effective_start_at_ms
            ):
                valid = False
        return chosen, valid, tiers

    # ── 组件查找辅助 ──

    @staticmethod
    def _find_period(comp, instance_id: int):
        return next((p for p in comp.period_progress if p.activity_instance_id == instance_id), None)

    @staticmethod
    def _find_eligibility(comp, reward_identity: str):
        return next((e for e in comp.eligibilities if e.reward_identity == reward_identity), None)

    @staticmethod
    def _find_claim_by_identity(comp, reward_identity: str):
        return next((r for r in comp.claim_receipts if r.reward_identity == reward_identity), None)

    @staticmethod
    def _find_claim_by_request(comp, request_id: str):
        return next((r for r in comp.claim_receipts if r.request_id == request_id), None)

    def _find_unclaimed_eligibility(self, comp):
        for e in comp.eligibilities:
            if self._find_claim_by_identity(comp, e.reward_identity) is None:
                return e
        return None

    @staticmethod
    def _has_active_pending_launch(comp) -> bool:
        if not comp.HasField("pending_launch") or not comp.pending_launch.request_id:
            return False
        return comp.pending_launch.launch_state in (LAUNCH_LAUNCHING, LAUNCH_ADMITTED, LAUNCH_UNKNOWN)

    def _writable_and_battle(self, player) -> tuple[bool, bool]:
        writable = self.scene.mutable(player)
        battle = getattr(self.scene, "battle", None)
        in_battle = battle is not None and battle.frozen(player.player_id)
        return writable, in_battle

    def _validate_active_pet(self, data) -> tuple[int, int]:
        """校验玩家是否有一只存活的出战宝宝，返回 (err, pet_id)。"""
        pet_comp = data.pet_component
        active_id = int(pet_comp.active_pet_id)
        if active_id == 0:
            return FESTIVAL_BEAST_PET_REQUIRED, 0
        active_pet = next((p for p in pet_comp.pets if p.pet_id == active_id), None)
        if active_pet is None:
            return FESTIVAL_BEAST_PET_REQUIRED, 0
        if active_pet.health == 0:
            return FESTIVAL_BEAST_PET_DEAD, 0
        return 0, active_id

    def _pet_meta(self, pet_table_id: int) -> tuple[str, int]:
        if "Pet" in self.tables.tables:
            row = self.tables["Pet"].get(pet_table_id)
            if row is not None:
                return str(row.name), int(getattr(row, "model_id", 0))
        return "春禧兽", 0

    def _has_exclusive_slot(self, data, pet_table_id: int) -> bool:
        return not any(p.pet_table_id == pet_table_id for p in data.pet_component.pets)

    # ── 外部战斗结算推进入口 ──

    def record_battle_result(
        self,
        data,
        *,
        activity_instance_id: int,
        won: bool,
        battle_id: int = 0,
        client_request_id: str = "",
        now_ms_val: int | None = None,
    ) -> dict:
        """写入瑞兽迎春单场战斗结果：清理在途开局、累加本期胜场、达标冻结永久待领资格。"""
        stamp = self.clock() if now_ms_val is None else now_ms_val
        comp = data.festival_beast_component
        comp.SetInParent()
        spec, _, _ = self._resolve_activity(activity_instance_id, comp)
        if spec is None:
            return {"changed": False, "wins_after": 0, "newly_eligible": False}

        changed = False
        if comp.HasField("pending_launch") and (
            (battle_id and comp.pending_launch.battle_id == battle_id)
            or (client_request_id and comp.pending_launch.request_id == client_request_id)
            or (not battle_id and not client_request_id and comp.pending_launch.activity_instance_id == spec.id)
        ):
            comp.ClearField("pending_launch")
            changed = True

        period = self._find_period(comp, spec.id)
        wins_before = min(period.wins, spec.required_wins) if period is not None else 0
        wins_after = wins_before
        newly_eligible = False

        if won:
            if period is None:
                period = comp.period_progress.add(
                    activity_instance_id=spec.id,
                    required_wins=spec.required_wins,
                    reward_identity=spec.reward_identity,
                )
            wins_after = min(wins_before + 1, spec.required_wins)
            if wins_after > wins_before:
                period.wins = wins_after
                period.required_wins = spec.required_wins
                period.last_win_at_ms = stamp
                period.reward_identity = spec.reward_identity
                changed = True
            if wins_after >= spec.required_wins and self._find_eligibility(comp, spec.reward_identity) is None:
                comp.eligibilities.add(
                    reward_identity=spec.reward_identity,
                    reward_definition_version=spec.reward_definition_version,
                    reward_pet_table_id=spec.reward_pet_table_id,
                    source_activity_instance_id=spec.id,
                    earned_at_ms=stamp,
                    earned_battle_id=battle_id,
                )
                newly_eligible = True
                changed = True

        if changed and comp.state_revision < _MAX_UINT64:
            comp.state_revision += 1
        return {
            "changed": changed,
            "wins_before": wins_before,
            "wins_after": wins_after,
            "newly_eligible": newly_eligible,
            "reward_identity": spec.reward_identity,
        }

    # ── 3 条客户端 RPC ──

    def get_detail(self, _world, player, _route, request):
        """查询瑞兽迎春详情、永久资格、在途开局恢复态与可执行阻断码。"""
        response = self.protocol.message("GetFestivalBeastDetailResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.festival_beast_component

        response.server_now_ms = stamp
        response.state_revision = comp.state_revision

        spec, cfg_valid, tiers = self._resolve_activity(int(request.activity_instance_id), comp)
        if spec is None:
            response.phase = PHASE_UNSCHEDULED
            response.can_start_challenge = False
            response.start_block_code = BLOCK_NOT_OPEN
            response.start_block_reason = "瑞兽迎春活动当前未排期"
            response.can_claim_reward = False
            response.claim_block_code = BLOCK_NOT_ELIGIBLE
            response.claim_block_reason = "尚未满足领取条件"
            return response

        phase = resolve_phase(
            spec.enabled, spec.effective_start_at_ms, spec.effective_end_at_ms, spec.claim_showcase_days, stamp,
        )
        response.phase = phase
        response.activity_instance_id = spec.id
        response.config_revision = spec.config_revision
        response.activity_name = spec.activity_name
        response.boss_name = spec.boss_name
        response.banner_asset_key = spec.banner_asset_key
        response.icon_key = spec.icon_key
        response.effective_start_at_ms = spec.effective_start_at_ms
        response.effective_end_at_ms = spec.effective_end_at_ms
        response.claim_showcase_end_at_ms = claim_showcase_end_at_ms(
            spec.effective_end_at_ms, spec.claim_showcase_days,
        )
        response.required_wins = spec.required_wins
        response.round_limit = spec.round_limit
        response.heavy_damage_percent = spec.heavy_damage_percent

        tier = find_tier_for_level(tiers, effective_level(data))
        if tier is not None:
            response.matched_tier_id = tier.id

        period = self._find_period(comp, spec.id)
        if period is not None:
            response.current_wins = min(period.wins, spec.required_wins)

        elig = self._find_eligibility(comp, spec.reward_identity)
        receipt = self._find_claim_by_identity(comp, spec.reward_identity)
        eff_version = (
            receipt.reward_definition_version
            if receipt is not None
            else (elig.reward_definition_version if elig is not None else spec.reward_definition_version)
        )
        eff_pet_id = (
            receipt.reward_pet_table_id
            if receipt is not None
            else (elig.reward_pet_table_id if elig is not None else spec.reward_pet_table_id)
        )
        pet_name, pet_model = self._pet_meta(eff_pet_id)
        response.reward_identity = spec.reward_identity
        response.reward_definition_version = eff_version
        response.reward_pet_table_id = eff_pet_id
        response.reward_pet_model_id = pet_model
        response.reward_pet_name = pet_name

        if elig is not None:
            response.has_permanent_eligibility = True
            response.eligibility_source_instance_id = elig.source_activity_instance_id
            response.eligibility_earned_at_ms = elig.earned_at_ms
        if receipt is not None:
            response.has_permanent_eligibility = True
            response.reward_claimed = True
            response.reward_claim_durable = True
            response.claimed_pet_id = receipt.pet_id
            response.claimed_at_ms = receipt.claimed_at_ms
            response.claim_request_id = receipt.request_id

        if comp.HasField("pending_launch") and comp.pending_launch.request_id:
            src = comp.pending_launch
            dst = response.pending_launch
            dst.client_request_id = src.request_id
            dst.activity_instance_id = src.activity_instance_id
            dst.config_revision = src.config_revision
            dst.locked_pet_id = src.locked_pet_id
            dst.matched_tier_id = src.matched_tier_id
            dst.launch_state = src.launch_state
            dst.battle_id = src.battle_id
            dst.requested_at_ms = src.requested_at_ms
            dst.admitted_at_ms = src.admitted_at_ms
            dst.reject_tip_id = src.reject_tip_id
            dst.reject_reason = src.reject_reason

        writable, in_battle = self._writable_and_battle(player)
        pet_err, _ = self._validate_active_pet(data)
        if not cfg_valid:
            response.can_start_challenge = False
            response.start_block_code = BLOCK_CONFIG_INVALID
            response.start_block_reason = "瑞兽迎春活动配置不完整，暂未开放"
        elif phase != PHASE_OPEN:
            response.can_start_challenge = False
            response.start_block_code = BLOCK_NOT_OPEN
            response.start_block_reason = "活动尚未开始，请等待开放时间" if phase == PHASE_UPCOMING else "本期活动挑战已结束"
        elif not writable:
            response.can_start_challenge = False
            response.start_block_code = BLOCK_FROZEN
            response.start_block_reason = "当前角色状态暂时不能发起挑战，请稍后重试"
        elif in_battle:
            response.can_start_challenge = False
            response.start_block_code = BLOCK_IN_BATTLE
            response.start_block_reason = "当前正在战斗中，请在结算完成后再试"
        elif self._has_active_pending_launch(comp):
            response.can_start_challenge = False
            response.start_block_code = BLOCK_LAUNCH_IN_FLIGHT
            response.start_block_reason = "正在进入瑞兽迎春战斗或结果确认中，请稍候"
        elif pet_err == FESTIVAL_BEAST_PET_DEAD:
            response.can_start_challenge = False
            response.start_block_code = BLOCK_PET_DEAD
            response.start_block_reason = "当前出战宝宝已倒地，请先恢复气血或更换出战宝宝"
        elif pet_err:
            response.can_start_challenge = False
            response.start_block_code = BLOCK_PET_REQUIRED
            response.start_block_reason = "请先出战一只合法存活宝宝后再挑战"
        else:
            response.can_start_challenge = True
            response.start_block_code = BLOCK_NONE

        if receipt is not None:
            response.can_claim_reward = False
            response.claim_block_code = BLOCK_ALREADY_CLAIMED
            response.claim_block_reason = "已领取该专属神兽，同一角色终身限领一只"
        elif elig is None:
            response.can_claim_reward = False
            response.claim_block_code = BLOCK_NOT_ELIGIBLE
            response.claim_block_reason = (
                f"本期累计获胜 {spec.required_wins} 次后可免费领取{spec.boss_name}"
                f"（当前 {response.current_wins}/{spec.required_wins}）"
            )
        elif not writable:
            response.can_claim_reward = False
            response.claim_block_code = BLOCK_FROZEN
            response.claim_block_reason = "当前角色状态暂时不能领取，请稍后重试"
        elif in_battle:
            response.can_claim_reward = False
            response.claim_block_code = BLOCK_IN_BATTLE
            response.claim_block_reason = "战斗中暂不能领取神兽，请在结算完成后重试"
        elif not self._has_exclusive_slot(data, eff_pet_id):
            response.can_claim_reward = False
            response.claim_block_code = BLOCK_EXCLUSIVE_SLOT_OCCUPIED
            response.claim_block_reason = "已拥有该专属神兽，无需重复领取"
        else:
            response.can_claim_reward = True
            response.claim_block_code = BLOCK_NONE
        return response

    async def start_challenge(self, _world, player, _route, request):
        """发起瑞兽迎春单人挑战：锁定存活出战宝宝、持久化在途开局意图并幂等准入。"""
        response = self.protocol.message("StartFestivalBeastChallengeResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.festival_beast_component

        req_id = str(request.client_request_id)
        if parse_uuid128(req_id) is None or not request.activity_instance_id or not request.config_revision:
            response.error_message.id = INVALID
            return response
        response.client_request_id = req_id

        # 1. 同请求重放：参数不一致报错，参数一致直接回放原准入态。
        if comp.HasField("pending_launch") and comp.pending_launch.request_id == req_id:
            pending = comp.pending_launch
            if (
                pending.activity_instance_id != request.activity_instance_id
                or pending.config_revision != request.config_revision
            ):
                response.error_message.id = FESTIVAL_BEAST_REQUEST_CONFLICT
                return response
            response.replayed = True
            response.launch_state = pending.launch_state
            response.locked_pet_id = pending.locked_pet_id
            response.battle_id = pending.battle_id
            response.matched_tier_id = pending.matched_tier_id
            response.admitted_at_ms = pending.admitted_at_ms
            response.reject_reason = pending.reject_reason
            if pending.launch_state == LAUNCH_REJECTED and pending.reject_tip_id:
                response.error_message.id = pending.reject_tip_id
            return response

        # 2. 已有不同请求在途开局，禁止并发另起新局。
        if self._has_active_pending_launch(comp):
            response.error_message.id = FESTIVAL_BEAST_LAUNCH_IN_FLIGHT
            return response

        spec, cfg_valid, tiers = self._resolve_activity(int(request.activity_instance_id), comp)
        if spec is None or not cfg_valid:
            response.error_message.id = FESTIVAL_BEAST_CONFIG_INVALID
            return response
        if spec.config_revision != request.config_revision:
            response.error_message.id = FESTIVAL_BEAST_CONFIG_REVISION_MISMATCH
            return response
        phase = resolve_phase(
            spec.enabled, spec.effective_start_at_ms, spec.effective_end_at_ms, spec.claim_showcase_days, stamp,
        )
        if phase != PHASE_OPEN:
            response.error_message.id = FESTIVAL_BEAST_NOT_OPEN
            return response

        writable, in_battle = self._writable_and_battle(player)
        if not writable:
            response.error_message.id = FESTIVAL_BEAST_FROZEN
            return response
        if in_battle:
            response.error_message.id = FESTIVAL_BEAST_IN_BATTLE
            return response

        pet_err, locked_pet_id = self._validate_active_pet(data)
        if pet_err:
            response.error_message.id = pet_err
            return response

        owner_level = effective_level(data)
        tier = find_tier_for_level(tiers, owner_level)
        if tier is None:
            response.error_message.id = FESTIVAL_BEAST_CONFIG_INVALID
            return response

        if self._launch_battle is not None:
            try:
                battle_id = int(await self._launch_battle(player.player_id, spec, locked_pet_id, tier.id, req_id))
            except Exception:
                logger.warning("Festival beast launch callback failed player_id=%s", player.player_id, exc_info=True)
                response.launch_state = LAUNCH_REJECTED
                response.reject_reason = "活动开局服务暂不可用，请稍后重试"
                response.error_message.id = FESTIVAL_BEAST_SERVICE_UNAVAILABLE
                return response
        else:
            self._next_battle_id += 1
            battle_id = self._next_battle_id

        staged = clone(data)
        sc = staged.festival_beast_component
        sc.SetInParent()
        p = sc.pending_launch
        p.Clear()
        p.request_id = req_id
        p.activity_instance_id = spec.id
        p.config_revision = spec.config_revision
        p.reward_identity = spec.reward_identity
        p.locked_pet_id = locked_pet_id
        p.locked_owner_level = owner_level
        p.matched_tier_id = tier.id
        p.battle_config_id = spec.battle_config_id
        p.launch_state = LAUNCH_ADMITTED if battle_id > 0 else LAUNCH_LAUNCHING
        p.battle_id = battle_id
        p.requested_at_ms = stamp
        p.admitted_at_ms = stamp if battle_id > 0 else 0
        if sc.state_revision < _MAX_UINT64:
            sc.state_revision += 1

        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            response.error_message.id = FESTIVAL_BEAST_SERVICE_UNAVAILABLE
            return response

        logger.info(
            "Festival beast challenge started player_id=%s instance=%s pet_id=%s tier=%s battle_id=%s",
            player.player_id, spec.id, locked_pet_id, tier.id, battle_id,
        )
        response.launch_state = p.launch_state
        response.locked_pet_id = locked_pet_id
        response.battle_id = battle_id
        response.matched_tier_id = tier.id
        response.admitted_at_ms = p.admitted_at_ms
        return response

    async def claim_reward(self, _world, player, _route, request):
        """领取瑞兽迎春永久待领专属神兽：同 UUID 幂等重放，异 UUID 收敛至同一 pet_id，原子落盘。"""
        response = self.protocol.message("ClaimFestivalBeastRewardResponse")
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.festival_beast_component

        req_id = str(request.client_request_id)
        reward_identity = str(request.reward_identity)
        if parse_uuid128(req_id) is None or not reward_identity or len(reward_identity) > 64:
            response.outcome = CLAIM_REJECTED
            response.error_message.id = INVALID
            return response
        response.client_request_id = req_id
        response.reward_identity = reward_identity

        # 1. 同请求 UUID 重放。
        same_req = self._find_claim_by_request(comp, req_id)
        if same_req is not None:
            if same_req.reward_identity != reward_identity:
                response.outcome = CLAIM_REJECTED
                response.error_message.id = FESTIVAL_BEAST_REQUEST_CONFLICT
                return response
            response.outcome = CLAIM_APPLIED
            response.replayed = True
            response.durable = True
            response.reward_definition_version = same_req.reward_definition_version
            response.reward_pet_table_id = same_req.reward_pet_table_id
            response.pet_id = same_req.pet_id
            response.claimed_at_ms = same_req.claimed_at_ms
            return response

        # 2. 不同 UUID 重复领取同一 reward_identity -> 幂等收敛至同一只 pet_id。
        existing = self._find_claim_by_identity(comp, reward_identity)
        if existing is not None:
            response.outcome = CLAIM_ALREADY_CLAIMED
            response.replayed = False
            response.durable = True
            response.client_request_id = existing.request_id
            response.reward_definition_version = existing.reward_definition_version
            response.reward_pet_table_id = existing.reward_pet_table_id
            response.pet_id = existing.pet_id
            response.claimed_at_ms = existing.claimed_at_ms
            return response

        # 3. 校验永久资格、状态与专属栏位。
        elig = self._find_eligibility(comp, reward_identity)
        if elig is None:
            response.outcome = CLAIM_REJECTED
            response.error_message.id = FESTIVAL_BEAST_NOT_ELIGIBLE
            return response
        writable, in_battle = self._writable_and_battle(player)
        if not writable:
            response.outcome = CLAIM_REJECTED
            response.error_message.id = FESTIVAL_BEAST_FROZEN
            return response
        if in_battle:
            response.outcome = CLAIM_REJECTED
            response.error_message.id = FESTIVAL_BEAST_IN_BATTLE
            return response
        if not self._has_exclusive_slot(data, elig.reward_pet_table_id):
            response.outcome = CLAIM_REJECTED
            response.error_message.id = FESTIVAL_BEAST_EXCLUSIVE_SLOT_OCCUPIED
            return response

        if self.assets is None or self.assets.allocate_item_id is None:
            response.outcome = CLAIM_UNKNOWN
            response.error_message.id = FESTIVAL_BEAST_SERVICE_UNAVAILABLE
            return response
        try:
            granted_pet_id = await self.assets.allocate_item_id()
        except (OSError, RuntimeError, ValueError):
            response.outcome = CLAIM_UNKNOWN
            response.error_message.id = FESTIVAL_BEAST_SERVICE_UNAVAILABLE
            return response
        if type(granted_pet_id) is not int or not 1 <= granted_pet_id <= _MAX_UINT64:
            response.outcome = CLAIM_UNKNOWN
            response.error_message.id = FESTIVAL_BEAST_SERVICE_UNAVAILABLE
            return response

        staged = clone(data)
        pet_name, _ = self._pet_meta(elig.reward_pet_table_id)
        owner_level = max(1, effective_level(staged))
        new_pet = staged.pet_component.pets.add(
            pet_id=granted_pet_id,
            pet_table_id=elig.reward_pet_table_id,
            name=pet_name,
            level=owner_level,
            health=1000,
            mana=500,
            created_at=stamp // 1000,
            cultivation_tier=4,
        )
        for dim in (1, 2, 3, 4):
            new_pet.aptitude[dim] = 10000

        sc = staged.festival_beast_component
        sc.SetInParent()
        sc.claim_receipts.add(
            reward_identity=reward_identity,
            request_id=req_id,
            reward_definition_version=elig.reward_definition_version,
            reward_pet_table_id=elig.reward_pet_table_id,
            pet_id=granted_pet_id,
            claimed_at_ms=stamp,
        )
        if sc.state_revision < _MAX_UINT64:
            sc.state_revision += 1

        try:
            await self.scene.commit_player_data(player, staged)
        except (StoreRejected, StoreUnavailable, OSError):
            response.outcome = CLAIM_UNKNOWN
            response.error_message.id = FESTIVAL_BEAST_SERVICE_UNAVAILABLE
            return response

        logger.info(
            "Festival beast reward claimed player_id=%s reward_identity=%s pet_id=%s",
            player.player_id, reward_identity, granted_pet_id,
        )
        response.outcome = CLAIM_APPLIED
        response.replayed = False
        response.durable = True
        response.reward_definition_version = elig.reward_definition_version
        response.reward_pet_table_id = elig.reward_pet_table_id
        response.pet_id = granted_pet_id
        response.claimed_at_ms = stamp
        return response
