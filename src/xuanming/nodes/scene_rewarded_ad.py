"""Scene 零拒审激励视频广告系统（对齐 C++ rewarded_ad_rules.h 与 player_rewarded_ad.cpp）。

合规红线与经济隔离：
- 服务端总开关默认关闭（RewardedAdSlot.enabled = 0），未开启时 GetRewardedAdInfo 返回 enabled=False，开播/领奖均拒绝；
- 奖励严格限定物理不可交易的绑定银两（CURRENCY_TYPE_BIND_GOLD = 3），严禁误配为流通银两(0)或灵石(1)；
- 复用 ClientRequestLedgerComp（WALLET 存盘组）保存短效一次性会话与幂等回执（kind = 21），
  同帧原子落盘以抗 30 秒全屏广告暂停心跳引发的断线重连补领与重复领奖；
- 切日点为 UTC+8 每日 05:00，严格抗时钟回拨（仅前进不倒退）。
"""

from __future__ import annotations

import logging
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..data import client_request
from ..data.asset_anomaly import AssetGain
from ..data.asset_inventory import BLOCKED, INVALID, UNAVAILABLE, balance, carries_unknown, clone
from ..data.asset_ledger import UINT64_MAX

logger = logging.getLogger(__name__)

_SERVICE = "SceneRewardedAdClientPlayer"
CLIENT_REQUEST_KIND_CLAIM_REWARDED_AD = 21

CURRENCY_GOLD = 0
CURRENCY_DIAMOND = 1
CURRENCY_BIND_DIAMOND = 2
CURRENCY_BIND_GOLD = 3

TX_SYSTEM_GRANT = 8

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
SHIFT_MS = 10_800_000  # UTC+8 05:00 切日
FIRST_MONDAY_05_UTC8_MS = 3 * DAY_MS + 21 * 3_600_000  # 1970-01-05 05:00 UTC+8 切周基准
MAX_DAILY_LIMIT = 20
MAX_SESSION_TTL_SEC = 3600
MAX_REWARD_AMOUNT = 100_000
ALL_PLATFORM_MASK = 0x0F  # bits 0..3: 微信小游戏 / 抖音小游戏 / iOS / Android

# 对应 C++ rewarded_ad_error_tip.pb.h 错误码（39000..39012）。
REWARDED_AD_UNAVAILABLE = 39000
REWARDED_AD_DISABLED = 39001
REWARDED_AD_PLATFORM_UNSUPPORTED = 39002
REWARDED_AD_LEVEL_NOT_ENOUGH = 39003
REWARDED_AD_DAILY_LIMIT_REACHED = 39004
REWARDED_AD_COOLDOWN = 39005
REWARDED_AD_SESSION_INVALID = 39006
REWARDED_AD_SESSION_EXPIRED = 39007
REWARDED_AD_WATCH_TOO_SHORT = 39008
REWARDED_AD_CURRENCY_BLOCKED = 39009
REWARDED_AD_IN_BATTLE = 39010
REWARDED_AD_FROZEN = 39011
REWARDED_AD_SSV_REQUIRED = 39012

SESSION_OK = "ok"
SESSION_INVALID = "invalid"
SESSION_TOO_SHORT = "too_short"
SESSION_EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class SlotConfig:
    """单条 RewardedAdSlot 配置表的强类型规则快照。"""

    slot_id: int
    enabled: bool
    min_level: int
    daily_limit: int
    cooldown_sec: int
    min_watch_sec: int
    session_ttl_sec: int
    currency_type: int
    reward_amount: int
    require_ssv: bool
    platform_mask: int


def resolve_game_day(now_ms: int) -> tuple[int, int] | None:
    """解析 now_ms 所属游戏日区间 [day_start, next_day)，切点 UTC+8 每日 05:00。"""
    if now_ms < 0 or now_ms > UINT64_MAX - SHIFT_MS - DAY_MS:
        return None
    shifted = now_ms + SHIFT_MS
    day_index = shifted // DAY_MS
    day_start = 0 if day_index == 0 else day_index * DAY_MS - SHIFT_MS
    next_day = (day_index + 1) * DAY_MS - SHIFT_MS
    return day_start, next_day


def resolve_game_week(now_ms: int) -> tuple[int, int] | None:
    """解析 now_ms 所属游戏周区间 [week_start, next_week)，切点 UTC+8 每周一 05:00。"""
    if now_ms < FIRST_MONDAY_05_UTC8_MS:
        return None
    offset = (now_ms - FIRST_MONDAY_05_UTC8_MS) % WEEK_MS
    week_start = now_ms - offset
    if week_start > UINT64_MAX - WEEK_MS:
        return None
    return week_start, week_start + WEEK_MS


def platform_bit(client_platform: int) -> int:
    """将 AdClientPlatform(1..4) 映射为平台位掩码。"""
    if not 1 <= client_platform <= 4:
        return 0
    return 1 << (client_platform - 1)


def is_platform_allowed(client_platform: int, platform_mask: int) -> bool:
    bit = platform_bit(client_platform)
    return bit != 0 and (platform_mask & bit) != 0


def validate_slot_config(cfg: SlotConfig) -> str | None:
    """强校验配置表行：激励视频仅允许发放物理不可交易的 CURRENCY_TYPE_BIND_GOLD(3)。"""
    if cfg.slot_id == 0:
        return "slot id is 0"
    if cfg.daily_limit == 0 or cfg.daily_limit > MAX_DAILY_LIMIT:
        return "daily_limit out of range 1..20"
    if (
        cfg.min_watch_sec == 0
        or cfg.session_ttl_sec == 0
        or cfg.min_watch_sec > cfg.session_ttl_sec
        or cfg.session_ttl_sec > MAX_SESSION_TTL_SEC
    ):
        return "invalid min_watch_sec or session_ttl_sec"
    if cfg.currency_type != CURRENCY_BIND_GOLD:
        return "rewarded ad slot must grant CURRENCY_TYPE_BIND_GOLD (3) only"
    if cfg.reward_amount == 0 or cfg.reward_amount > MAX_REWARD_AMOUNT:
        return "reward_amount out of range 1..100000"
    if (cfg.platform_mask & ~ALL_PLATFORM_MASK) != 0:
        return "platform_mask has unknown bits"
    return None


def effective_claimed_today(ad_ledger, current_day_start_ms: int) -> int:
    """抗时钟回拨的今日已领次数查询：仅当游戏日起点严格前进才归零。"""
    if current_day_start_ms > int(ad_ledger.day_starts_at_ms):
        return 0
    return int(ad_ledger.claimed_today)


def advance_day_if_needed(ad_ledger, current_day_start_ms: int) -> None:
    """推进日账本（仅前进不清倒退）。"""
    if current_day_start_ms > int(ad_ledger.day_starts_at_ms):
        ad_ledger.day_starts_at_ms = current_day_start_ms
        ad_ledger.claimed_today = 0


def cooldown_until_ms(ad_ledger, cooldown_sec: int) -> int:
    """计算冷却截止时刻（UTC 毫秒，0 表示无冷却）。"""
    last_claim = int(ad_ledger.last_claim_at_ms)
    if last_claim == 0 or cooldown_sec == 0:
        return 0
    delta_ms = cooldown_sec * 1000
    if last_claim > UINT64_MAX - delta_ms:
        return UINT64_MAX
    return last_claim + delta_ms


def verify_active_session(
    ad_ledger,
    slot_id: int,
    ad_session_id: int,
    now_ms: int,
    min_watch_sec: int,
    session_ttl_sec: int,
) -> str:
    """核验一次性广告会话是否匹配、达到最短观看时长且未过期。"""
    issued_ms = int(ad_ledger.active_session_issued_ms)
    if (
        ad_session_id == 0
        or int(ad_ledger.active_session_id) != ad_session_id
        or int(ad_ledger.active_session_slot_id) != slot_id
        or issued_ms == 0
    ):
        return SESSION_INVALID
    if now_ms < issued_ms:
        return SESSION_TOO_SHORT
    elapsed_ms = now_ms - issued_ms
    if elapsed_ms > session_ttl_sec * 1000:
        return SESSION_EXPIRED
    if elapsed_ms < min_watch_sec * 1000:
        return SESSION_TOO_SHORT
    return SESSION_OK


def _ledger_is_valid(ledger) -> bool:
    """与 client_request.check 同口径校验幂等账本自洽性，损坏时 fail-closed。"""
    next_seq = int(ledger.next_seq) or 1
    if len(ledger.records) > 256:
        return False
    keys: set[int] = set()
    seqs: set[int] = set()
    for rec in ledger.records:
        key = int(rec.key)
        seq = int(rec.seq)
        if (
            not key
            or key in keys
            or len(rec.fingerprint) != 16
            or not 1 <= seq < next_seq
            or seq in seqs
            or not int(rec.kind)
        ):
            return False
        keys.add(key)
        seqs.add(seq)
    return True


def _generate_session_id(avoid_id: int) -> int:
    """生成 63 位非 0 随机一次性会话号，避免与前一次挂起会话号重复。"""
    for _ in range(8):
        candidate = secrets.randbits(63)
        if candidate != 0 and candidate != avoid_id:
            return candidate
    return 2 if avoid_id == 1 else 1


def _row_to_config(row) -> tuple[SlotConfig | None, str | None]:
    enabled_raw = int(row.enabled)
    ssv_raw = int(row.require_ssv)
    if enabled_raw not in (0, 1) or ssv_raw not in (0, 1):
        return None, "enabled or require_ssv must be 0 or 1"
    cfg = SlotConfig(
        slot_id=int(row.id),
        enabled=enabled_raw != 0,
        min_level=int(row.min_level),
        daily_limit=int(row.daily_limit),
        cooldown_sec=int(row.cooldown_sec),
        min_watch_sec=int(row.min_watch_sec),
        session_ttl_sec=int(row.session_ttl_sec),
        currency_type=int(row.currency_type),
        reward_amount=int(row.reward_amount),
        require_ssv=ssv_raw != 0,
        platform_mask=int(row.platform_mask),
    )
    reason = validate_slot_config(cfg)
    if reason is not None:
        return None, reason
    return cfg, None


def _fill_slot_view(
    cfg: SlotConfig,
    ad_ledger,
    client_platform: int,
    player_level: int,
    day_start_ms: int,
    next_day_ms: int,
    now_ms: int,
    out,
) -> None:
    out.Clear()
    out.slot_id = cfg.slot_id
    platform_ok = client_platform == 0 or is_platform_allowed(client_platform, cfg.platform_mask)
    out.enabled = bool(cfg.enabled and platform_ok and player_level >= cfg.min_level)
    out.min_level = cfg.min_level
    out.daily_limit = cfg.daily_limit
    out.claimed_today = effective_claimed_today(ad_ledger, day_start_ms)
    out.next_reset_ms = next_day_ms
    cd = cooldown_until_ms(ad_ledger, cfg.cooldown_sec)
    out.cooldown_until_ms = cd if cd > now_ms else 0
    out.currency_type = cfg.currency_type
    out.reward_amount = cfg.reward_amount


class SceneRewardedAd:
    """Scene 激励视频广告 RPC 处理器（540 GetInfo / 541 BeginSession / 542 Claim）。"""

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
        self.now_ms = clock or (lambda: time.time_ns() // 1_000_000)

    def register(self) -> None:
        self.validate_tables()
        self.scene.register(_SERVICE, "GetRewardedAdInfo", self.get_info)
        self.scene.register(_SERVICE, "BeginRewardedAdSession", self.begin_session, mutating=True)
        self.scene.register(_SERVICE, "ClaimRewardedAd", self.claim, mutating=True)

    def _table(self):
        return self.tables.tables.get("RewardedAdSlot")

    def validate_tables(self) -> bool:
        """校验 RewardedAdSlot 全表行（强断言仅发 CURRENCY_TYPE_BIND_GOLD），非法行记 ERROR 并在运行期拒载。"""
        table = self._table()
        if table is None:
            return True
        ok = True
        for row in table.rows():
            cfg, reason = _row_to_config(row)
            if cfg is None:
                logger.error(
                    "[RewardedAd] invalid RewardedAdSlot row: id=%d reason=%s",
                    int(row.id),
                    reason or "unknown",
                )
                ok = False
        return ok

    def _resolve_slot(self, slot_id: int) -> tuple[SlotConfig | None, str | None]:
        table = self._table()
        row = None if table is None else table.get(slot_id)
        if row is None:
            return None, "slot row not found"
        return _row_to_config(row)

    def _reject(self, route, tip_id: int):
        resp = self.protocol.message(route.response_type)
        resp.error_message.id = tip_id
        return resp

    def get_info(self, _world, player, route, request):
        """540 查询可用激励视频槽位与双银两余额（纯只读）。"""
        now_val = int(self.now_ms())
        data = self.scene.rows.assemble(player.snapshot)
        if not _ledger_is_valid(data.client_request_ledger):
            return self._reject(route, REWARDED_AD_UNAVAILABLE)
        day_pair = resolve_game_week(now_val)
        if day_pair is None:
            return self._reject(route, REWARDED_AD_UNAVAILABLE)
        day_start_ms, next_day_ms = day_pair

        player_level = int(data.level_component.level)
        client_platform = int(request.client_platform)
        resp = self.protocol.message(
            route.response_type,
            server_time_ms=now_val,
            bind_gold_balance=balance(data, CURRENCY_BIND_GOLD),
            gold_balance=balance(data, CURRENCY_GOLD),
        )
        table = self._table()
        valid_slots: list[SlotConfig] = []
        if table is not None:
            for row in table.rows():
                cfg, _ = _row_to_config(row)
                if cfg is not None:
                    valid_slots.append(cfg)
        valid_slots.sort(key=lambda item: item.slot_id)
        ad_ledger = data.client_request_ledger.rewarded_ad
        for cfg in valid_slots:
            _fill_slot_view(
                cfg,
                ad_ledger,
                client_platform,
                player_level,
                day_start_ms,
                next_day_ms,
                now_val,
                resp.slots.add(),
            )
        return resp

    async def begin_session(self, _world, player, route, request):
        """541 开播前签发一次性短效会话并随 WALLET 组落盘。"""
        now_val = int(self.now_ms())
        if carries_unknown(request):
            return self._reject(route, INVALID)
        if self.assets._player_in_battle(player):
            return self._reject(route, REWARDED_AD_IN_BATTLE)
        if not self.scene.mutable(player):
            return self._reject(route, REWARDED_AD_FROZEN)

        data = self.scene.rows.assemble(player.snapshot)
        if not _ledger_is_valid(data.client_request_ledger):
            return self._reject(route, REWARDED_AD_UNAVAILABLE)

        slot_id = int(request.slot_id)
        client_platform = int(request.client_platform)
        cfg, reason = self._resolve_slot(slot_id)
        if cfg is None:
            logger.warning(
                "[RewardedAd] metric=rewarded_ad_session_rejected reason=invalid_slot player_id=%d slot_id=%d detail=%s",
                player.player_id,
                slot_id,
                reason or "unknown",
            )
            return self._reject(route, REWARDED_AD_UNAVAILABLE)
        if not cfg.enabled:
            return self._reject(route, REWARDED_AD_DISABLED)
        if not is_platform_allowed(client_platform, cfg.platform_mask):
            return self._reject(route, REWARDED_AD_PLATFORM_UNSUPPORTED)
        if int(data.level_component.level) < cfg.min_level:
            return self._reject(route, REWARDED_AD_LEVEL_NOT_ENOUGH)

        day_pair = resolve_game_week(now_val)
        if day_pair is None:
            return self._reject(route, REWARDED_AD_UNAVAILABLE)
        day_start_ms, _ = day_pair

        ad_ledger = data.client_request_ledger.rewarded_ad
        if effective_claimed_today(ad_ledger, day_start_ms) >= cfg.daily_limit:
            return self._reject(route, REWARDED_AD_DAILY_LIMIT_REACHED)
        cd = cooldown_until_ms(ad_ledger, cfg.cooldown_sec)
        if cd > 0 and now_val < cd:
            return self._reject(route, REWARDED_AD_COOLDOWN)
        if self.assets.currency_blocked(data, CURRENCY_BIND_GOLD):
            return self._reject(route, REWARDED_AD_CURRENCY_BLOCKED)
        if self.assets.credit_overflows(data, CURRENCY_BIND_GOLD, cfg.reward_amount, now_seconds=now_val // 1000):
            return self._reject(route, REWARDED_AD_UNAVAILABLE)

        staged = clone(data)
        staged_ad = staged.client_request_ledger.rewarded_ad
        advance_day_if_needed(staged_ad, day_start_ms)
        session_id = _generate_session_id(int(staged_ad.active_session_id))
        staged_ad.active_session_id = session_id
        staged_ad.active_session_issued_ms = now_val
        staged_ad.active_session_slot_id = cfg.slot_id

        saved = await self.assets._save(player, staged, 0)
        if not saved:
            return self._reject(route, UNAVAILABLE)

        ttl_ms = cfg.session_ttl_sec * 1000
        expires_at = UINT64_MAX if now_val > UINT64_MAX - ttl_ms else now_val + ttl_ms
        logger.info(
            "[RewardedAd] metric=rewarded_ad_session_issued player_id=%d slot_id=%d platform=%d ad_session_id=%d",
            player.player_id,
            cfg.slot_id,
            client_platform,
            session_id,
        )
        return self.protocol.message(
            route.response_type,
            ad_session_id=session_id,
            expires_at_ms=expires_at,
        )

    async def claim(self, _world, player, route, request):
        """542 完播后幂等核销会话、仅发放绑定银两并原子提交 WALLET 存盘。"""
        now_val = int(self.now_ms())
        if carries_unknown(request):
            return self._reject(route, INVALID)
        req_id = int(request.client_request_id)
        if req_id == 0:
            return self._reject(route, INVALID)

        data = self.scene.rows.assemble(player.snapshot)
        # 1. 幂等查重排在一切状态校验之前（首次已成功，重放直接回首次结果）。
        error, fingerprint, prior = client_request.check(
            data.client_request_ledger,
            request,
            CLIENT_REQUEST_KIND_CLAIM_REWARDED_AD,
        )
        if error:
            return self._reject(route, error)
        if prior is not None:
            resp = self.protocol.message(
                route.response_type,
                currency_type=int(prior.rewarded_ad_currency_type),
                reward_amount=int(prior.rewarded_ad_amount),
                currency_balance=balance(data, CURRENCY_BIND_GOLD),
                gold_balance=balance(data, CURRENCY_GOLD),
                replayed=True,
            )
            day_pair = resolve_game_week(now_val)
            cfg, _ = self._resolve_slot(int(request.slot_id))
            if day_pair is not None and cfg is not None:
                _fill_slot_view(
                    cfg,
                    data.client_request_ledger.rewarded_ad,
                    0,
                    int(data.level_component.level),
                    day_pair[0],
                    day_pair[1],
                    now_val,
                    resp.slot,
                )
            return resp

        # 2. 运行状态校验。
        if self.assets._player_in_battle(player):
            return self._reject(route, REWARDED_AD_IN_BATTLE)
        if not self.scene.mutable(player):
            return self._reject(route, REWARDED_AD_FROZEN)

        # 3. 配表、等级、日限与冷却校验。
        slot_id = int(request.slot_id)
        ad_session_id = int(request.ad_session_id)
        cfg, _ = self._resolve_slot(slot_id)
        if cfg is None:
            return self._reject(route, REWARDED_AD_UNAVAILABLE)
        if not cfg.enabled:
            return self._reject(route, REWARDED_AD_DISABLED)
        player_level = int(data.level_component.level)
        if player_level < cfg.min_level:
            return self._reject(route, REWARDED_AD_LEVEL_NOT_ENOUGH)

        day_pair = resolve_game_week(now_val)
        if day_pair is None:
            return self._reject(route, REWARDED_AD_UNAVAILABLE)
        day_start_ms, next_day_ms = day_pair

        ad_ledger = data.client_request_ledger.rewarded_ad
        if effective_claimed_today(ad_ledger, day_start_ms) >= cfg.daily_limit:
            return self._reject(route, REWARDED_AD_DAILY_LIMIT_REACHED)
        cd = cooldown_until_ms(ad_ledger, cfg.cooldown_sec)
        if cd > 0 and now_val < cd:
            return self._reject(route, REWARDED_AD_COOLDOWN)

        # 4. 一次性会话、最短观看时长、TTL 与可选 SSV 校验。
        verdict = verify_active_session(
            ad_ledger,
            cfg.slot_id,
            ad_session_id,
            now_val,
            cfg.min_watch_sec,
            cfg.session_ttl_sec,
        )
        if verdict == SESSION_INVALID:
            logger.warning(
                "[RewardedAd] metric=rewarded_ad_claim_rejected reason=session_invalid player_id=%d slot_id=%d ad_session_id=%d",
                player.player_id,
                cfg.slot_id,
                ad_session_id,
            )
            return self._reject(route, REWARDED_AD_SESSION_INVALID)
        if verdict == SESSION_EXPIRED:
            logger.warning(
                "[RewardedAd] metric=rewarded_ad_claim_rejected reason=session_expired player_id=%d slot_id=%d ad_session_id=%d",
                player.player_id,
                cfg.slot_id,
                ad_session_id,
            )
            return self._reject(route, REWARDED_AD_SESSION_EXPIRED)
        if verdict == SESSION_TOO_SHORT:
            logger.warning(
                "[RewardedAd] metric=rewarded_ad_claim_rejected reason=too_fast player_id=%d slot_id=%d ad_session_id=%d",
                player.player_id,
                cfg.slot_id,
                ad_session_id,
            )
            return self._reject(route, REWARDED_AD_WATCH_TOO_SHORT)
        if cfg.require_ssv and not str(request.ssv_proof):
            return self._reject(route, REWARDED_AD_SSV_REQUIRED)

        # 5. 产出封禁与溢出预检。
        if self.assets.currency_blocked(data, CURRENCY_BIND_GOLD):
            return self._reject(route, REWARDED_AD_CURRENCY_BLOCKED)
        if self.assets.credit_overflows(data, CURRENCY_BIND_GOLD, cfg.reward_amount, now_seconds=now_val // 1000):
            return self._reject(route, REWARDED_AD_UNAVAILABLE)

        # 6. 候选账本预演 + 仅发放绑定银两 + 同帧原子提交。
        staged = clone(data)
        staged_ad = staged.client_request_ledger.rewarded_ad
        advance_day_if_needed(staged_ad, day_start_ms)
        staged_ad.claimed_today = int(staged_ad.claimed_today) + 1
        staged_ad.last_claim_at_ms = now_val
        if int(staged_ad.total_claimed_lifetime) < UINT64_MAX:
            staged_ad.total_claimed_lifetime = int(staged_ad.total_claimed_lifetime) + 1
        staged_ad.active_session_id = 0
        staged_ad.active_session_issued_ms = 0
        staged_ad.active_session_slot_id = 0

        client_request.remember(
            staged.client_request_ledger,
            req_id,
            CLIENT_REQUEST_KIND_CLAIM_REWARDED_AD,
            fingerprint,
            rewarded_ad_currency_type=CURRENCY_BIND_GOLD,
            rewarded_ad_amount=cfg.reward_amount,
        )
        if not _ledger_is_valid(staged.client_request_ledger):
            return self._reject(route, UNAVAILABLE)

        gains: list[AssetGain] = []
        add_err = self.assets.add_currency(
            staged,
            CURRENCY_BIND_GOLD,
            cfg.reward_amount,
            gains=gains,
            now_seconds=now_val // 1000,
        )
        if add_err == BLOCKED:
            return self._reject(route, REWARDED_AD_CURRENCY_BLOCKED)
        if add_err:
            logger.error(
                "[RewardedAd] AddCurrency failed after precheck player_id=%d slot_id=%d amount=%d err=%d",
                player.player_id,
                cfg.slot_id,
                cfg.reward_amount,
                add_err,
            )
            return self._reject(route, REWARDED_AD_UNAVAILABLE)

        saved = await self.assets._save(
            player,
            staged,
            TX_SYSTEM_GRANT,
            correlation=ad_session_id,
            gains=gains,
        )
        if not saved:
            return self._reject(route, UNAVAILABLE)

        resp = self.protocol.message(
            route.response_type,
            currency_type=CURRENCY_BIND_GOLD,
            reward_amount=cfg.reward_amount,
            currency_balance=balance(staged, CURRENCY_BIND_GOLD),
            gold_balance=balance(staged, CURRENCY_GOLD),
            replayed=False,
        )
        _fill_slot_view(
            cfg,
            staged.client_request_ledger.rewarded_ad,
            0,
            player_level,
            day_start_ms,
            next_day_ms,
            now_val,
            resp.slot,
        )
        logger.info(
            "[RewardedAd] metric=rewarded_ad_claimed player_id=%d slot_id=%d currency_type=%d reward_amount=%d claimed_today=%d client_request_id=%d",
            player.player_id,
            cfg.slot_id,
            CURRENCY_BIND_GOLD,
            cfg.reward_amount,
            int(staged.client_request_ledger.rewarded_ad.claimed_today),
            req_id,
        )
        return resp
