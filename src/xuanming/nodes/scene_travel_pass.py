"""Scene 青云游历手记系统（对齐 C++ travel_pass_rules.h 与 player_travel_pass.cpp）。

不肝不逼氪双轨游历活动：
- 零日常任务、零连续签到、零周清打卡；正常游玩结算自动累计游历点（封顶 max_points）；
- 典藏轨支持先玩后买随时补领，仅消耗真实充值灵石（CURRENCY_TYPE_DIAMOND = 1，严禁扣绑定灵石）；
- 解锁典藏后永不过期（即使超出 ends_at_ms，已解锁期次仍保留进度并允许随时领奖）；
- 所有物品强制 bound=True（不可交易/不可挂集市），纪念头像框/称谓零战力纯外显；
- 解锁与领奖均强制非 0 client_request_id 接入 ClientRequestLedgerComp（kind=16/17），
  与 WALLET / BAG 同事务原子落盘并记录 TX_TRAVEL_PASS_UNLOCK(45) / TX_TRAVEL_PASS_REWARD(46)。
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
from ..data.asset_ledger import UINT64_MAX
from .scene_player import _bag

logger = logging.getLogger(__name__)

_SERVICE = "SceneActivityClientPlayer"
CLIENT_REQUEST_KIND_UNLOCK_TRAVEL_PASS = 16
CLIENT_REQUEST_KIND_CLAIM_TRAVEL_PASS = 17

CURRENCY_GOLD = 0
CURRENCY_DIAMOND = 1
CURRENCY_BIND_DIAMOND = 2

TX_TRAVEL_PASS_UNLOCK = 45
TX_TRAVEL_PASS_REWARD = 46

COSMETIC_SLOT_FRAME = 1
COSMETIC_SLOT_TITLE = 2

ACTIVITY_ID_TRAVEL_PASS = 900001
ACTIVITY_ID_APPEARANCE_SHOWCASE = 900002
ACTIVITY_ID_SPIRIT_SHOP_SHOWCASE = 900003

ENTRY_KIND_TRAVEL_PASS = 3
ENTRY_KIND_APPEARANCE_SHOWCASE = 4
ENTRY_KIND_SPIRIT_SHOP_SHOWCASE = 5

MAX_PASS_RECORDS = 32
MAX_TIERS_PER_PASS = 64
MAX_COSMETICS_PER_SEASON = 64
MAX_COSMETIC_KEY_BYTES = 64
MAX_PRICE = 1_000_000_000
MAX_POINTS = 100_000
MAX_GOLD_PER_TIER = 100_000_000
_MAX_UINT32 = 0xFFFFFFFF

# 对应 C++ travel_pass_error_tip.pb.h 错误码（38000..38012）。
TRAVEL_PASS_UNAVAILABLE = 38000
TRAVEL_PASS_NOT_OPEN = 38001
TRAVEL_PASS_ALREADY_UNLOCKED = 38002
TRAVEL_PASS_PRICE_CHANGED = 38003
TRAVEL_PASS_CURRENCY_NOT_ENOUGH = 38004
TRAVEL_PASS_NOT_REACHED = 38005
TRAVEL_PASS_PREMIUM_REQUIRED = 38006
TRAVEL_PASS_NOTHING_TO_CLAIM = 38007
TRAVEL_PASS_BAG_FULL = 38008
TRAVEL_PASS_ITEM_BLOCKED = 38009
TRAVEL_PASS_FROZEN = 38010
TRAVEL_PASS_IN_BATTLE = 38011
TRAVEL_PASS_COSMETIC_NOT_OWNED = 38012

# 对应 match.MatchMode 枚举（与 C++ player_travel_pass.cpp 严格一致）。
_MATCH_MODE_5V5 = 1
_MATCH_MODE_1V1 = 3
_MATCH_MODE_PVE_SOLO = 4
_MATCH_MODE_PVE_TEAM = 5
_MATCH_MODE_PVP_TEAM = 7
_MATCH_MODE_POLARITY_5V5 = 8
_MATCH_MODE_GUILD_WAR_PVP = 9
_MATCH_MODE_GUILD_WAR_SIEGE = 10
_PVE_ELIGIBLE_MODES = frozenset({_MATCH_MODE_PVE_SOLO, _MATCH_MODE_PVE_TEAM})
_PVP_ELIGIBLE_MODES = frozenset({
    _MATCH_MODE_1V1,
    _MATCH_MODE_5V5,
    _MATCH_MODE_PVP_TEAM,
    _MATCH_MODE_POLARITY_5V5,
    _MATCH_MODE_GUILD_WAR_PVP,
    _MATCH_MODE_GUILD_WAR_SIEGE,
})


@dataclass(frozen=True, slots=True)
class ResolvedTier:
    tier_id: int
    pass_id: int
    tier_index: int
    required_points: int
    free_item_id: int
    free_item_count: int
    free_gold: int
    free_cosmetic_key: str
    free_cosmetic_name: str
    premium_item_id: int
    premium_item_count: int
    premium_gold: int
    premium_cosmetic_key: str
    premium_cosmetic_name: str


@dataclass(frozen=True, slots=True)
class ResolvedSeason:
    pass_id: int
    config_revision: int
    enabled: bool
    name: str
    description: str
    icon_key: str
    banner_asset_key: str
    starts_at_ms: int
    ends_at_ms: int
    premium_currency_type: int
    premium_price: int
    max_points: int
    pve_win_points: int
    hard_or_pvp_win_points: int
    pvp_qualified_other_points: int
    tiers: tuple[ResolvedTier, ...]


def is_valid_cosmetic_key(key: str) -> bool:
    if not key or len(key.encode("utf-8")) > MAX_COSMETIC_KEY_BYTES:
        return False
    if not (key.startswith("frame_") or key.startswith("title_")):
        return False
    return all(ch.isalnum() or ch in "_-" for ch in key)


def cosmetic_slot_of(key: str) -> int:
    if key.startswith("frame_"):
        return COSMETIC_SLOT_FRAME
    if key.startswith("title_"):
        return COSMETIC_SLOT_TITLE
    return 0


def has_claimed_tier(mask: int, tier_index: int) -> bool:
    if not 1 <= tier_index <= MAX_TIERS_PER_PASS:
        return False
    return (int(mask) & (1 << (tier_index - 1))) != 0


def mark_claimed_tier(mask: int, tier_index: int) -> int:
    if not 1 <= tier_index <= MAX_TIERS_PER_PASS:
        return int(mask)
    return int(mask) | (1 << (tier_index - 1))


def has_cosmetic(comp, key: str) -> bool:
    if not key:
        return False
    return any(k == key for rec in comp.seasons for k in rec.unlocked_cosmetic_keys)


def add_cosmetic(season_rec, key: str) -> bool:
    if not key:
        return True
    if not is_valid_cosmetic_key(key):
        return False
    if any(k == key for k in season_rec.unlocked_cosmetic_keys):
        return True
    if len(season_rec.unlocked_cosmetic_keys) >= MAX_COSMETICS_PER_SEASON:
        return False
    season_rec.unlocked_cosmetic_keys.append(key)
    return True


def validate_comp(comp) -> str | None:
    if len(comp.seasons) > MAX_PASS_RECORDS:
        return "seasons exceeds capacity"
    seen_passes: set[int] = set()
    owned: set[str] = set()
    for rec in comp.seasons:
        pid = int(rec.pass_id)
        if pid == 0:
            return "seasons contains zero pass_id"
        if pid in seen_passes:
            return "seasons contains duplicate pass_id"
        seen_passes.add(pid)
        if len(rec.unlocked_cosmetic_keys) > MAX_COSMETICS_PER_SEASON:
            return "season unlocked_cosmetic_keys exceeds capacity"
        season_keys: set[str] = set()
        for key in rec.unlocked_cosmetic_keys:
            if not is_valid_cosmetic_key(key):
                return "unlocked_cosmetic_keys contains invalid key"
            if key in season_keys:
                return "unlocked_cosmetic_keys contains duplicate key"
            season_keys.add(key)
            owned.add(key)
    if comp.equipped_frame_key:
        if cosmetic_slot_of(comp.equipped_frame_key) != COSMETIC_SLOT_FRAME or comp.equipped_frame_key not in owned:
            return "equipped_frame_key not owned or wrong slot"
    if comp.equipped_title_key:
        if cosmetic_slot_of(comp.equipped_title_key) != COSMETIC_SLOT_TITLE or comp.equipped_title_key not in owned:
            return "equipped_title_key not owned or wrong slot"
    return None


def is_in_window(season: ResolvedSeason, now_ms: int) -> bool:
    if not season.enabled:
        return False
    if season.starts_at_ms > 0 and now_ms < season.starts_at_ms:
        return False
    if season.ends_at_ms > 0 and now_ms >= season.ends_at_ms:
        return False
    return True


def evaluate_battle_points(season: ResolvedSeason, settlement) -> int:
    """对齐 C++ PlayerTravelPassSystem::ComputeBattlePoints（反挂机与玩法口径一致）。"""
    if (
        int(getattr(settlement, "battle_id", 0)) == 0
        or int(getattr(settlement, "player_id", 0)) == 0
        or settlement.fled
        or settlement.total_rounds <= 0
        or settlement.player_team_index > 1
        or settlement.outcome not in (1, 2, 3)
    ):
        return 0
    player_won = (
        (settlement.outcome == 1 and settlement.player_team_index == 0)
        or (settlement.outcome == 2 and settlement.player_team_index == 1)
    )
    match_mode = int(getattr(settlement, "match_mode", 0))
    if match_mode in _PVE_ELIGIBLE_MODES:
        if not player_won:
            return 0
        return season.hard_or_pvp_win_points if int(getattr(settlement, "battle_difficulty", 0)) == 2 else season.pve_win_points
    if match_mode in _PVP_ELIGIBLE_MODES:
        if not bool(getattr(settlement, "participated", False)):
            return 0
        if player_won:
            return season.hard_or_pvp_win_points
        # PVP 平/负局统一走防挂机门槛（>=3回合 + >=2手动回合 + 有效贡献）。
        manual_rounds = int(getattr(settlement, "manual_action_rounds", 0))
        effective = bool(getattr(settlement, "effective_combat_contribution", False))
        if int(settlement.total_rounds) >= 3 and manual_rounds >= 2 and effective:
            return season.pvp_qualified_other_points
        return 0
    return 0


class SceneTravelPass:
    """Scene 青云游历手记与活动 UI 低打扰种草聚合处理器。"""

    def __init__(
        self,
        scene,
        tables,
        assets,
        *,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.now_ms = clock if clock is not None else (lambda: int(time.time() * 1000))
        self.unlock_tx = self._resolve_tx("TX_TRAVEL_PASS_UNLOCK", TX_TRAVEL_PASS_UNLOCK)
        self.reward_tx = self._resolve_tx("TX_TRAVEL_PASS_REWARD", TX_TRAVEL_PASS_REWARD)

    def _resolve_tx(self, name: str, fallback: int) -> int:
        try:
            enum_desc = self.protocol.pool.FindEnumTypeByName("TransactionType")
            if enum_desc is not None and name in enum_desc.values_by_name:
                return int(enum_desc.values_by_name[name].number)
        except KeyError:
            pass
        return fallback

    def register(self) -> None:
        self.scene.register(_SERVICE, "GetTravelPassState", self.get_state)
        self.scene.register(_SERVICE, "UnlockTravelPassPremium", self.unlock_premium, mutating=True)
        self.scene.register(_SERVICE, "ClaimTravelPassReward", self.claim_reward, mutating=True)
        self.scene.register(_SERVICE, "EquipTravelPassCosmetic", self.equip_cosmetic, mutating=True)

    def _table(self, name: str):
        try:
            return self.tables[name]
        except KeyError:
            return None

    def _resolve_season(self, pass_id: int) -> tuple[ResolvedSeason | None, str]:
        season_table = self._table("TravelPassSeason")
        tier_table = self._table("TravelPassTier")
        item_table = self._table("Item")
        if season_table is None or tier_table is None:
            return None, "travel pass tables missing"
        row = season_table.get(pass_id)
        if row is None:
            return None, "season not found"
        if int(row.config_revision) == 0:
            return None, "config_revision is zero"
        if int(row.premium_currency_type) != CURRENCY_DIAMOND:
            return None, "premium_currency_type must be real diamond"
        price = int(row.premium_price)
        if not 1 <= price <= MAX_PRICE:
            return None, "premium_price out of range"
        max_pts = int(row.max_points)
        if not 1 <= max_pts <= MAX_POINTS:
            return None, "max_points out of range"
        if int(row.starts_at_ms) > 0 and int(row.ends_at_ms) > 0 and int(row.ends_at_ms) <= int(row.starts_at_ms):
            return None, "invalid time window"
        tier_rows = sorted(
            (r for r in tier_table.rows() if int(r.pass_id) == pass_id),
            key=lambda r: (int(r.tier_index), int(r.id)),
        )
        if not tier_rows or len(tier_rows) > MAX_TIERS_PER_PASS:
            return None, "tier count invalid"

        resolved_tiers: list[ResolvedTier] = []
        prev_pts = 0
        for expected_idx, tr in enumerate(tier_rows, start=1):
            tidx = int(tr.tier_index)
            req_pts = int(tr.required_points)
            if tidx != expected_idx:
                return None, "tier_index not contiguous from 1"
            if req_pts == 0 or req_pts < prev_pts or req_pts > max_pts:
                return None, "required_points invalid"
            prev_pts = req_pts
            for item_id, item_cnt in (
                (int(tr.free_item_id), int(tr.free_item_count)),
                (int(tr.premium_item_id), int(tr.premium_item_count)),
            ):
                if (item_id == 0) != (item_cnt == 0):
                    return None, "item id/count mismatch"
                if item_id != 0 and (item_table is None or item_table.get(item_id) is None):
                    return None, f"item_id {item_id} not in Item table"
            if int(tr.free_gold) > MAX_GOLD_PER_TIER or int(tr.premium_gold) > MAX_GOLD_PER_TIER:
                return None, "gold exceeds per-tier limit"
            for ck in (tr.free_cosmetic_key, tr.premium_cosmetic_key):
                if ck and not is_valid_cosmetic_key(ck):
                    return None, f"invalid cosmetic key {ck}"
            resolved_tiers.append(
                ResolvedTier(
                    tier_id=int(tr.id),
                    pass_id=pass_id,
                    tier_index=tidx,
                    required_points=req_pts,
                    free_item_id=int(tr.free_item_id),
                    free_item_count=int(tr.free_item_count),
                    free_gold=int(tr.free_gold),
                    free_cosmetic_key=str(tr.free_cosmetic_key),
                    free_cosmetic_name=str(tr.free_cosmetic_name),
                    premium_item_id=int(tr.premium_item_id),
                    premium_item_count=int(tr.premium_item_count),
                    premium_gold=int(tr.premium_gold),
                    premium_cosmetic_key=str(tr.premium_cosmetic_key),
                    premium_cosmetic_name=str(tr.premium_cosmetic_name),
                )
            )
        return (
            ResolvedSeason(
                pass_id=pass_id,
                config_revision=int(row.config_revision),
                enabled=bool(int(row.enabled)),
                name=str(row.name),
                description=str(row.description),
                icon_key=str(row.icon_key),
                banner_asset_key=str(row.banner_asset_key),
                starts_at_ms=int(row.starts_at_ms),
                ends_at_ms=int(row.ends_at_ms),
                premium_currency_type=int(row.premium_currency_type),
                premium_price=price,
                max_points=max_pts,
                pve_win_points=int(row.pve_win_points),
                hard_or_pvp_win_points=int(row.hard_or_pvp_win_points),
                pvp_qualified_other_points=int(row.pvp_qualified_other_points),
                tiers=tuple(resolved_tiers),
            ),
            "",
        )

    def _active_season(self, now_ms: int) -> ResolvedSeason | None:
        table = self._table("TravelPassSeason")
        if table is None:
            return None
        for row in sorted(table.rows(), key=lambda r: int(r.id)):
            if int(row.enabled) == 0:
                continue
            season, reason = self._resolve_season(int(row.id))
            if season is None:
                logger.error("[TravelPass] Invalid enabled season id=%d reason=%s", int(row.id), reason)
                continue
            if is_in_window(season, now_ms):
                return season
        return None

    def _select_season_for_view(self, comp, requested_pass_id: int, now_ms: int) -> ResolvedSeason | None:
        if requested_pass_id != 0:
            season, _ = self._resolve_season(requested_pass_id)
            return season
        active = self._active_season(now_ms)
        if active is not None:
            return active
        # 没有当前期次时，仍向已解锁玩家展示其未过期/未领完的历史典藏期次。
        for rec in comp.seasons:
            if not rec.premium_unlocked:
                continue
            season, _ = self._resolve_season(int(rec.pass_id))
            if season is not None:
                return season
        return None

    def _find_record(self, comp, pass_id: int):
        for rec in comp.seasons:
            if int(rec.pass_id) == pass_id:
                return rec
        return None

    def _find_or_create_record(self, comp, pass_id: int):
        rec = self._find_record(comp, pass_id)
        if rec is not None:
            return rec
        if len(comp.seasons) >= MAX_PASS_RECORDS:
            return None
        return comp.seasons.add(pass_id=pass_id)

    def validate_tables(self) -> bool:
        season_table = self._table("TravelPassSeason")
        tier_table = self._table("TravelPassTier")
        if season_table is None or tier_table is None:
            logger.error("[TravelPass] TravelPassSeason or TravelPassTier table missing")
            return False
        ok = True
        for row in season_table.rows():
            if int(row.enabled) == 0:
                continue
            season, reason = self._resolve_season(int(row.id))
            if season is None:
                logger.error("[TravelPass] Invalid TravelPassSeason id=%d reason=%s", int(row.id), reason)
                ok = False
        return ok

    def apply_battle_credit(self, data, settlement, now_ms: int) -> int:
        """在战斗结算事务内原子累加游历点（不产生单独存盘；已解锁期次永不过期）。"""
        comp = data.travel_pass
        if validate_comp(comp) is not None:
            return 0
        # 若有活跃期次则优先给活跃期次；已解锁的旧期次即使过期仍继续累计直至满档。
        targets: list[ResolvedSeason] = []
        active = self._active_season(now_ms)
        if active is not None:
            targets.append(active)
        for rec in comp.seasons:
            if not rec.premium_unlocked:
                continue
            pid = int(rec.pass_id)
            if any(s.pass_id == pid for s in targets):
                continue
            season, _ = self._resolve_season(pid)
            if season is not None and int(rec.progress_points) < season.max_points:
                targets.append(season)
        total_added = 0
        for season in targets:
            delta = evaluate_battle_points(season, settlement)
            if delta <= 0:
                continue
            rec = self._find_or_create_record(comp, season.pass_id)
            if rec is None:
                continue
            before = int(rec.progress_points)
            if before >= season.max_points:
                continue
            after = min(season.max_points, before + delta)
            if after > before:
                rec.progress_points = after
                rec.last_updated_at_ms = now_ms
                if int(comp.pass_revision) < UINT64_MAX:
                    comp.pass_revision = int(comp.pass_revision) + 1
                total_added += after - before
        return total_added

    def _fill_snapshot(self, data, requested_pass_id: int, now_ms: int, out) -> None:
        comp = data.travel_pass
        out.server_now_ms = now_ms
        out.pass_revision = int(comp.pass_revision)
        out.diamond_balance = balance(data, CURRENCY_DIAMOND)
        out.bind_diamond_balance = balance(data, CURRENCY_BIND_DIAMOND)
        all_cosmetics = sorted({k for rec in comp.seasons for k in rec.unlocked_cosmetic_keys})
        out.unlocked_cosmetic_keys.extend(all_cosmetics)
        out.equipped_frame_key = comp.equipped_frame_key
        out.equipped_title_key = comp.equipped_title_key

        if validate_comp(comp) is not None:
            out.available = False
            out.unavailable_reason = "游历手记数据异常，请联系客服处理"
            return

        season = self._select_season_for_view(comp, requested_pass_id, now_ms)
        if season is None:
            out.available = False
            out.unavailable_reason = "本期游历手记尚未开启，敬请期待"
            return

        rec = self._find_record(comp, season.pass_id)
        points = int(rec.progress_points) if rec is not None else 0
        unlocked = bool(rec.premium_unlocked) if rec is not None else False
        free_mask = int(rec.free_claimed_mask) if rec is not None else 0
        prem_mask = int(rec.premium_claimed_mask) if rec is not None else 0
        in_window = is_in_window(season, now_ms)

        out.available = True
        out.pass_id = season.pass_id
        out.config_revision = season.config_revision
        out.name = season.name
        out.description = season.description
        out.icon_key = season.icon_key
        out.banner_asset_key = season.banner_asset_key
        out.starts_at_ms = season.starts_at_ms
        out.ends_at_ms = season.ends_at_ms
        out.open_for_progress = in_window or unlocked
        out.open_for_unlock = in_window and not unlocked
        out.progress_points = points
        out.max_points = season.max_points
        out.pve_win_points = season.pve_win_points
        out.hard_or_pvp_win_points = season.hard_or_pvp_win_points
        out.pvp_qualified_other_points = season.pvp_qualified_other_points
        out.premium_unlocked = unlocked
        out.premium_unlocked_at_ms = int(rec.premium_unlocked_at_ms) if rec is not None else 0
        out.premium_currency_type = season.premium_currency_type
        out.premium_price = season.premium_price

        item_table = self._table("Item")
        claimable_free = 0
        claimable_prem = 0
        retro_prem = 0

        for tier in season.tiers:
            reached = points >= tier.required_points
            f_claimed = has_claimed_tier(free_mask, tier.tier_index)
            p_claimed = has_claimed_tier(prem_mask, tier.tier_index)
            f_can = reached and not f_claimed
            p_can = reached and unlocked and not p_claimed
            if f_can:
                claimable_free += 1
            if p_can:
                claimable_prem += 1
            if reached and not unlocked and not p_claimed:
                retro_prem += 1

            tv = out.tiers.add(
                tier_id=tier.tier_id,
                tier_index=tier.tier_index,
                required_points=tier.required_points,
                reached=reached,
                free_item_id=tier.free_item_id,
                free_item_count=tier.free_item_count,
                free_gold=tier.free_gold,
                free_cosmetic_key=tier.free_cosmetic_key,
                free_cosmetic_name=tier.free_cosmetic_name,
                free_claimed=f_claimed,
                free_can_claim=f_can,
                premium_item_id=tier.premium_item_id,
                premium_item_count=tier.premium_item_count,
                premium_gold=tier.premium_gold,
                premium_cosmetic_key=tier.premium_cosmetic_key,
                premium_cosmetic_name=tier.premium_cosmetic_name,
                premium_claimed=p_claimed,
                premium_can_claim=p_can,
            )
            if tier.free_item_id and item_table is not None:
                item_row = item_table.get(tier.free_item_id)
                if item_row is not None:
                    tv.free_item_name = str(item_row.name)
                    tv.free_item_icon_key = str(item_row.icon_key)
            if tier.premium_item_id and item_table is not None:
                item_row = item_table.get(tier.premium_item_id)
                if item_row is not None:
                    tv.premium_item_name = str(item_row.name)
                    tv.premium_item_icon_key = str(item_row.icon_key)

        out.claimable_free_count = claimable_free
        out.claimable_premium_count = claimable_prem
        out.retroactive_premium_count = retro_prem

    def append_activity_catalog_entries(self, _player, data, now_ms: int, response) -> None:
        """向 GetActivityListResponse 聚合游历手记与两张低打扰种草卡片（严禁假红点）。"""
        snap = self.protocol.message("TravelPassSnapshot")
        self._fill_snapshot(data, 0, now_ms, snap)
        if snap.available:
            tp = response.activities.add(
                activity_id=ACTIVITY_ID_TRAVEL_PASS,
                definition_key=f"travel_pass:{snap.pass_id}",
                configuration_revision=snap.config_revision,
                category=3,  # 福利活动
                name=snap.name,
                description=snap.description,
                icon_key=snap.icon_key or "icon_scroll",
                status=2,
                starts_at_ms=snap.starts_at_ms,
                ends_at_ms=snap.ends_at_ms,
                completion_count_known=True,
                completed_count=snap.progress_points,
                completion_limit=snap.max_points,
                completion_period="当期游历 · 解锁典藏永不过期",
                can_participate=True,
                entry_kind=ENTRY_KIND_TRAVEL_PASS,
                has_claimable_reward=(snap.claimable_free_count + snap.claimable_premium_count) > 0,
            )
            tp.reward_types.append(3)
            # 精选免费轨代表奖励预览
            item_table = self._table("Item")
            for item_id, count in ((3405, 1), (3403, 6)):
                item_row = None if item_table is None else item_table.get(item_id)
                tp.rewards.add(
                    item_config_id=item_id,
                    count=count,
                    name=str(item_row.name) if item_row is not None else f"道具#{item_id}",
                    icon_key=str(item_row.icon_key) if item_row is not None else "",
                    binding_description="绑定仅自用",
                )

        # 低打扰体验种草 1：留灯风物 · 免费试穿（零红点，展示实时形象数）
        app_table = self._table("CharacterAppearance")
        total_appearances = len(app_table.rows()) if app_table is not None else 0
        if total_appearances > 0:
            app_card = response.activities.add(
                activity_id=ACTIVITY_ID_APPEARANCE_SHOWCASE,
                definition_key="showcase:appearance_trial",
                configuration_revision=1,
                category=3,
                name="留灯风物 · 免费试穿",
                description="全角色外观开放免费试穿 1 小时与人物背景阅读，零战力纯外观，喜欢再换绝不强买。",
                icon_key="item_appearance_card",
                status=2,
                completion_count_known=False,
                completion_period="常驻开放 · 免费体验",
                can_participate=True,
                entry_kind=ENTRY_KIND_APPEARANCE_SHOWCASE,
                has_claimable_reward=False,
            )
            app_card.reward_types.append(3)

        # 低打扰体验种草 2：灵石百宝阁 · 轻便利（零红点，展示实时在售商品数）
        shop_table = self._table("SpiritShopGoods")
        sellable = sum(1 for r in shop_table.rows() if int(r.enabled) != 0) if shop_table is not None else 0
        if sellable > 0:
            shop_card = response.activities.add(
                activity_id=ACTIVITY_ID_SPIRIT_SHOP_SHOWCASE,
                definition_key="showcase:spirit_shop",
                configuration_revision=1,
                category=3,
                name="灵石百宝阁 · 轻便利",
                description=f"在售 {sellable} 款绑定仅自用保底符与外观便利道具，不卖战力上限，按需随看。",
                icon_key="item_star_guarantee_charm",
                status=2,
                completion_count_known=False,
                completion_period="常驻开放 · 绝不强制",
                can_participate=True,
                entry_kind=ENTRY_KIND_SPIRIT_SHOP_SHOWCASE,
                has_claimable_reward=False,
            )
            shop_card.reward_types.append(3)

    def _reject(self, route, tip_id: int):
        resp = self.protocol.message(route.response_type)
        resp.error_message.id = tip_id
        return resp

    def get_state(self, _world, player, route, request):
        """526 查询游历手记双轨进度、档位领取状态、双余额与纪念外观佩戴状态。"""
        data = self.scene.rows.assemble(player.snapshot)
        resp = self.protocol.message(route.response_type)
        self._fill_snapshot(data, int(request.pass_id), int(self.now_ms()), resp.snapshot)
        return resp

    async def unlock_premium(self, _world, player, route, request):
        """527 使用真实充值灵石解锁典藏轨（幂等 + 报价防呆 + 永不过期）。"""
        now_val = int(self.now_ms())
        if carries_unknown(request):
            return self._reject(route, INVALID)
        req_id = int(request.client_request_id)
        if req_id == 0:
            return self._reject(route, INVALID)

        data = self.scene.rows.assemble(player.snapshot)
        error, fingerprint, prior = client_request.check(
            data.client_request_ledger, request, CLIENT_REQUEST_KIND_UNLOCK_TRAVEL_PASS,
        )
        if error:
            return self._reject(route, error)
        if prior is not None:
            resp = self.protocol.message(route.response_type, replayed=True)
            self._fill_snapshot(data, int(request.pass_id), now_val, resp.snapshot)
            return resp

        if not self.scene.mutable(player):
            return self._reject(route, TRAVEL_PASS_FROZEN)
        if self.assets._player_in_battle(player):
            return self._reject(route, TRAVEL_PASS_IN_BATTLE)
        if validate_comp(data.travel_pass) is not None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)

        season = self._select_season_for_view(data.travel_pass, int(request.pass_id), now_val)
        if season is None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
        rec = self._find_record(data.travel_pass, season.pass_id)
        if rec is not None and rec.premium_unlocked:
            return self._reject(route, TRAVEL_PASS_ALREADY_UNLOCKED)
        if not is_in_window(season, now_val):
            return self._reject(route, TRAVEL_PASS_NOT_OPEN)
        if (
            int(request.expected_currency_type) != season.premium_currency_type
            or int(request.expected_price) != season.premium_price
            or int(request.expected_config_revision) != season.config_revision
        ):
            return self._reject(route, TRAVEL_PASS_PRICE_CHANGED)
        # 严禁绑定灵石混扣：只检查真实灵石余额。
        if balance(data, CURRENCY_DIAMOND) < season.premium_price:
            return self._reject(route, TRAVEL_PASS_CURRENCY_NOT_ENOUGH)

        staged = clone(data)
        deduct_err = self.assets.deduct_currency(staged, CURRENCY_DIAMOND, season.premium_price)
        if deduct_err == INSUFFICIENT:
            return self._reject(route, TRAVEL_PASS_CURRENCY_NOT_ENOUGH)
        if deduct_err:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)

        staged_rec = self._find_or_create_record(staged.travel_pass, season.pass_id)
        if staged_rec is None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
        staged_rec.premium_unlocked = True
        staged_rec.premium_unlocked_at_ms = now_val
        if int(staged.travel_pass.pass_revision) < UINT64_MAX:
            staged.travel_pass.pass_revision = int(staged.travel_pass.pass_revision) + 1
        if validate_comp(staged.travel_pass) is not None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)

        client_request.remember(
            staged.client_request_ledger, req_id, CLIENT_REQUEST_KIND_UNLOCK_TRAVEL_PASS, fingerprint,
        )
        saved = await self.assets._save(
            player,
            staged,
            self.unlock_tx,
            correlation=req_id,
            item_extra={"source": "travel_pass_unlock", "pass_id": season.pass_id, "price": season.premium_price},
        )
        if not saved:
            return self._reject(route, UNAVAILABLE)

        logger.info(
            "[TravelPass] metric=travel_pass_unlocked player_id=%d pass_id=%d price=%d client_request_id=%d",
            player.player_id, season.pass_id, season.premium_price, req_id,
        )
        resp = self.protocol.message(route.response_type)
        self._fill_snapshot(staged, season.pass_id, now_val, resp.snapshot)
        return resp

    async def _grant_bound_items_exact(self, data, items: list[tuple[int, int]], *, gains: list[AssetGain]) -> int:
        if not items:
            return 0
        item_table = self._table("Item")
        if item_table is None:
            return TRAVEL_PASS_UNAVAILABLE
        merged: dict[int, int] = {}
        for item_id, count in items:
            if item_id == 0 or count <= 0:
                continue
            merged[item_id] = merged.get(item_id, 0) + count
        if not merged:
            return 0

        bag = clone(data.bag_component)
        entries = self.assets.entries(bag, 0)
        capacity = bag_capacity(bag, 0)
        if not self.assets._usable(entries, capacity):
            return TRAVEL_PASS_UNAVAILABLE
        occupied = {entry.pos for entry, _ in entries}
        free_slots = iter(slot for slot in range(capacity) if slot not in occupied)
        plan: list[tuple[int, int, int]] = []

        for item_id, total_qty in sorted(merged.items()):
            row = item_table.get(item_id)
            if row is None or int(row.max_stack_size) <= 0:
                return TRAVEL_PASS_UNAVAILABLE
            if self.assets.item_gain_blocked(data, item_id):
                return TRAVEL_PASS_ITEM_BLOCKED
            max_stack = int(row.max_stack_size)
            remaining = total_qty
            if max_stack > 1:
                for entry, item in sorted(entries, key=lambda pair: (pair[1].acquire_seq, pair[1].item_id)):
                    if int(item.config_id) != item_id or not bool(item.bound) or not self.assets._mergeable(item, max_stack):
                        continue
                    added = min(remaining, max_stack - int(item.size))
                    if added:
                        item.size += added
                        remaining -= added
                        self.assets.write_entry(entry, item)
                        if remaining == 0:
                            break
            while remaining > 0:
                slot = next(free_slots, None)
                if slot is None:
                    return TRAVEL_PASS_BAG_FULL
                if len(plan) >= MAX_NEW_STACKS:
                    return TRAVEL_PASS_UNAVAILABLE
                stack = min(remaining, max_stack)
                plan.append((slot, item_id, stack))
                remaining -= stack

        if plan and self.assets.allocate_item_id is None:
            return TRAVEL_PASS_UNAVAILABLE

        ids = self.assets.all_ids(bag)
        watermark = max((int(item.acquire_seq) for _, item in entries), default=0)
        for slot, config_id, stack in plan:
            try:
                guid = await self.assets.allocate_item_id()
            except (OSError, RuntimeError, ValueError):
                return TRAVEL_PASS_UNAVAILABLE
            if type(guid) is not int or not 1 <= guid <= UINT64_MAX or guid in ids or watermark >= UINT64_MAX:
                return TRAVEL_PASS_UNAVAILABLE
            ids.add(guid)
            watermark += 1
            item = self.protocol.message(
                "ItemComp",
                item_id=guid,
                config_id=config_id,
                size=stack,
                bound=True,  # 游历手记全部物品强制永久绑定仅自用
                acquire_seq=watermark,
            )
            self.assets._initialize_equipment(item)
            self.assets.write_entry(bag.items.add(pos=slot, bag_type=0), item)

        self.assets._order(bag.items)
        data.bag_component.CopyFrom(bag)
        gains.extend(AssetGain("item", item_id, qty) for item_id, qty in sorted(merged.items()))
        return 0

    async def claim_reward(self, _world, player, route, request):
        """528 领取单档或一键领取全部可领奖励（全绑定道具 + 银两 + 纪念外显原子提交）。"""
        now_val = int(self.now_ms())
        if carries_unknown(request):
            return self._reject(route, INVALID)
        req_id = int(request.client_request_id)
        if req_id == 0:
            return self._reject(route, INVALID)

        data = self.scene.rows.assemble(player.snapshot)
        error, fingerprint, prior = client_request.check(
            data.client_request_ledger, request, CLIENT_REQUEST_KIND_CLAIM_TRAVEL_PASS,
        )
        if error:
            return self._reject(route, error)
        if prior is not None:
            resp = self.protocol.message(
                route.response_type, replayed=True, claimed_count=int(prior.travel_pass_claimed_count),
            )
            self._fill_snapshot(data, int(request.pass_id), now_val, resp.snapshot)
            bag_resp = _bag(self.scene, self.tables, player, 0)
            if not bag_resp.error_message.id and bag_resp.HasField("bag"):
                resp.bag.CopyFrom(bag_resp.bag)
                self.assets.enrich_bag_enhance_preview(data, 0, resp.bag)
            return resp

        if not self.scene.mutable(player):
            return self._reject(route, TRAVEL_PASS_FROZEN)
        if self.assets._player_in_battle(player):
            return self._reject(route, TRAVEL_PASS_IN_BATTLE)
        if validate_comp(data.travel_pass) is not None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)

        season = self._select_season_for_view(data.travel_pass, int(request.pass_id), now_val)
        if season is None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
        rec = self._find_record(data.travel_pass, season.pass_id)
        points = int(rec.progress_points) if rec is not None else 0
        unlocked = bool(rec.premium_unlocked) if rec is not None else False
        free_mask = int(rec.free_claimed_mask) if rec is not None else 0
        prem_mask = int(rec.premium_claimed_mask) if rec is not None else 0

        targets: list[tuple[ResolvedTier, bool]] = []
        tier_index = int(request.tier_index)
        if tier_index > 0:
            tier = next((t for t in season.tiers if t.tier_index == tier_index), None)
            if tier is None:
                return self._reject(route, INVALID)
            if points < tier.required_points:
                return self._reject(route, TRAVEL_PASS_NOT_REACHED)
            if request.premium_track:
                if not unlocked:
                    return self._reject(route, TRAVEL_PASS_PREMIUM_REQUIRED)
                if has_claimed_tier(prem_mask, tier.tier_index):
                    return self._reject(route, TRAVEL_PASS_NOTHING_TO_CLAIM)
                targets.append((tier, True))
            else:
                if has_claimed_tier(free_mask, tier.tier_index):
                    return self._reject(route, TRAVEL_PASS_NOTHING_TO_CLAIM)
                targets.append((tier, False))
        else:
            for tier in season.tiers:
                if points < tier.required_points:
                    continue
                if not has_claimed_tier(free_mask, tier.tier_index):
                    targets.append((tier, False))
                if unlocked and not has_claimed_tier(prem_mask, tier.tier_index):
                    targets.append((tier, True))
            if not targets:
                return self._reject(route, TRAVEL_PASS_NOTHING_TO_CLAIM)

        items_to_grant: list[tuple[int, int]] = []
        total_gold = 0
        cosmetics_to_add: list[str] = []
        for tier, is_prem in targets:
            if is_prem:
                if tier.premium_item_id and tier.premium_item_count:
                    items_to_grant.append((tier.premium_item_id, tier.premium_item_count))
                total_gold += tier.premium_gold
                if tier.premium_cosmetic_key:
                    cosmetics_to_add.append(tier.premium_cosmetic_key)
            else:
                if tier.free_item_id and tier.free_item_count:
                    items_to_grant.append((tier.free_item_id, tier.free_item_count))
                total_gold += tier.free_gold
                if tier.free_cosmetic_key:
                    cosmetics_to_add.append(tier.free_cosmetic_key)

        staged = clone(data)
        gains: list[AssetGain] = []
        grant_err = await self._grant_bound_items_exact(staged, items_to_grant, gains=gains)
        if grant_err:
            return self._reject(route, grant_err)

        if not self.scene.mutable(player):
            return self._reject(route, TRAVEL_PASS_FROZEN)
        if self.assets._player_in_battle(player):
            return self._reject(route, TRAVEL_PASS_IN_BATTLE)

        if total_gold > 0:
            if self.assets.credit_overflows(staged, CURRENCY_GOLD, total_gold):
                return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
            gold_err = self.assets.add_currency(staged, CURRENCY_GOLD, total_gold, gains=gains)
            if gold_err:
                return self._reject(route, TRAVEL_PASS_UNAVAILABLE)

        staged_rec = self._find_or_create_record(staged.travel_pass, season.pass_id)
        if staged_rec is None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
        for tier, is_prem in targets:
            if is_prem:
                staged_rec.premium_claimed_mask = mark_claimed_tier(int(staged_rec.premium_claimed_mask), tier.tier_index)
            else:
                staged_rec.free_claimed_mask = mark_claimed_tier(int(staged_rec.free_claimed_mask), tier.tier_index)
        for key in cosmetics_to_add:
            if not add_cosmetic(staged_rec, key):
                return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
        staged_rec.last_updated_at_ms = now_val
        if int(staged.travel_pass.pass_revision) < UINT64_MAX:
            staged.travel_pass.pass_revision = int(staged.travel_pass.pass_revision) + 1
        if validate_comp(staged.travel_pass) is not None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)

        claimed_count = len(targets)
        client_request.remember(
            staged.client_request_ledger,
            req_id,
            CLIENT_REQUEST_KIND_CLAIM_TRAVEL_PASS,
            fingerprint,
            travel_pass_claimed_count=claimed_count,
        )
        saved = await self.assets._save(
            player,
            staged,
            self.reward_tx,
            correlation=req_id,
            gains=gains,
            item_extra={"source": "travel_pass_reward", "pass_id": season.pass_id, "claimed_count": claimed_count},
        )
        if not saved:
            return self._reject(route, UNAVAILABLE)

        logger.info(
            "[TravelPass] metric=travel_pass_claimed player_id=%d pass_id=%d claimed_count=%d gold=%d client_request_id=%d",
            player.player_id, season.pass_id, claimed_count, total_gold, req_id,
        )
        resp = self.protocol.message(route.response_type, claimed_count=claimed_count)
        self._fill_snapshot(staged, season.pass_id, now_val, resp.snapshot)
        bag_resp = _bag(self.scene, self.tables, player, 0)
        if not bag_resp.error_message.id and bag_resp.HasField("bag"):
            resp.bag.CopyFrom(bag_resp.bag)
            self.assets.enrich_bag_enhance_preview(staged, 0, resp.bag)
        return resp

    async def equip_cosmetic(self, _world, player, route, request):
        """529 零消耗佩戴/卸下已解锁的游历纪念头像框或称谓（零战力纯外显）。"""
        now_val = int(self.now_ms())
        if carries_unknown(request):
            return self._reject(route, INVALID)
        slot = int(request.cosmetic_slot)
        key = str(request.cosmetic_key)
        if slot not in (COSMETIC_SLOT_FRAME, COSMETIC_SLOT_TITLE):
            return self._reject(route, INVALID)
        if key and (not is_valid_cosmetic_key(key) or cosmetic_slot_of(key) != slot):
            return self._reject(route, INVALID)

        if not self.scene.mutable(player):
            return self._reject(route, TRAVEL_PASS_FROZEN)
        if self.assets._player_in_battle(player):
            return self._reject(route, TRAVEL_PASS_IN_BATTLE)

        data = self.scene.rows.assemble(player.snapshot)
        if validate_comp(data.travel_pass) is not None:
            return self._reject(route, TRAVEL_PASS_UNAVAILABLE)
        if key and not has_cosmetic(data.travel_pass, key):
            return self._reject(route, TRAVEL_PASS_COSMETIC_NOT_OWNED)

        staged = clone(data)
        if slot == COSMETIC_SLOT_FRAME:
            staged.travel_pass.equipped_frame_key = key
        else:
            staged.travel_pass.equipped_title_key = key
        if int(staged.travel_pass.pass_revision) < UINT64_MAX:
            staged.travel_pass.pass_revision = int(staged.travel_pass.pass_revision) + 1

        saved = await self.assets._save(player, staged, 0)
        if not saved:
            return self._reject(route, UNAVAILABLE)

        resp = self.protocol.message(route.response_type)
        self._fill_snapshot(staged, 0, now_val, resp.snapshot)
        return resp
