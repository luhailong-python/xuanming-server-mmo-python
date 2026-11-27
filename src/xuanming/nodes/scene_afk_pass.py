"""Scene 挂机月卡系统（对齐 C++ afk_pass_rules.h 与 afk_pass.cpp）。

事实源为存档 ``PlayerUint64Comp.afk_pass_expires_at``（Unix 秒）；开通/续费经客户端幂等键
（``ClientRequestKind::kBuyAfkPass = 6``）与货币扣款同一次存盘提交，成功后异步投影到玩家域
Redis 键 ``player:afk_pass:{<player_id>}``（带 ``EX`` TTL）。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    FEATURE_UNAVAILABLE,
    INSUFFICIENT,
    INVALID,
    UNAVAILABLE,
    balance,
    clone,
)
from .scene_connection_hold import afk_pass_key

logger = logging.getLogger(__name__)

_SERVICE = "SceneAfkPassClientPlayer"
_BUY_AFK_PASS_KIND = 6

SECONDS_PER_DAY = 86_400
MAX_PERIOD_DAYS = 366
MAX_PRICE = 1_000_000_000
MAX_REMAINING_DAYS_CAP = 3660
MAX_REMIND_BEFORE_HOURS = 720
GM_MAX_REMAINING_SECONDS = 366 * SECONDS_PER_DAY
BASE_LOADOUT_SLOTS = 2
LIGHT_CARD_BONUS_LOADOUT_SLOTS = 2
LIGHT_CARD_LOADOUT_SLOTS = BASE_LOADOUT_SLOTS + LIGHT_CARD_BONUS_LOADOUT_SLOTS
_UINT32_MAX = 0xFFFFFFFF
_UINT64_MAX = (1 << 64) - 1

# 对应 C++ afk_pass_error_tip.pb.h 错误码。
AFK_PASS_PRICE_CHANGED = 32000
AFK_PASS_CURRENCY_NOT_ENOUGH = 32001
AFK_PASS_STACK_LIMIT = 32002
AFK_PASS_FROZEN = 32003
AFK_PASS_UNAVAILABLE = 32004


def is_active(expires_at: int, now_unix_sec: int) -> bool:
    """判断挂机月卡/轻享卡此刻是否处于有效期内。"""
    return int(expires_at) > int(now_unix_sec)


def unlocked_loadout_slots(active: bool) -> int:
    """返回当前生效方案槽位数（免费基础 2 格，轻享卡 +2 共 4 格，可另租扩至 7 格）。"""
    return LIGHT_CARD_LOADOUT_SLOTS if active else BASE_LOADOUT_SLOTS


def clear_hosted_recap_fields(profile) -> None:
    """清空离线托管收获简报持久化字段。"""
    profile.hosted_recap_started_at = 0
    profile.hosted_recap_ended_at = 0
    profile.hosted_recap_battle_count = 0
    profile.hosted_recap_win_count = 0
    profile.hosted_recap_exp_gained = 0
    profile.hosted_recap_silver_gained = 0
    profile.hosted_recap_item_count = 0
    profile.hosted_recap_pending_ack = False


@dataclass(frozen=True, slots=True)
class AfkPassRuleData:
    """挂机月卡单行规则快照。"""

    period_days: int
    price_currency_type: int
    price: int
    max_remaining_days: int
    remind_before_hours: int


def rule_problem(rule: AfkPassRuleData, valid_currencies: frozenset[int]) -> str | None:
    """校验规则表取值；合法返回 ``None``，否则返回英文原因供日志记录。"""
    if not 1 <= rule.period_days <= MAX_PERIOD_DAYS:
        return "period_days is not in 1..366"
    if rule.price_currency_type not in valid_currencies:
        return "price_currency_type is not a known CurrencyType"
    if not 1 <= rule.price <= MAX_PRICE:
        return "price is not in 1..1000000000"
    if not rule.period_days <= rule.max_remaining_days <= MAX_REMAINING_DAYS_CAP:
        return "max_remaining_days is not in period_days..3660"
    if not 0 <= rule.remind_before_hours <= MAX_REMIND_BEFORE_HOURS:
        return "remind_before_hours is not in 0..720"
    return None


def renewed_expiry(
    current_expires_at: int,
    now_unix_sec: int,
    period_days: int,
    max_remaining_days: int,
) -> int | None:
    """计算续费后的到期 Unix 秒；超过最大叠加天数上限时返回 ``None``。"""
    remaining = current_expires_at - now_unix_sec if current_expires_at > now_unix_sec else 0
    period = period_days * SECONDS_PER_DAY
    cap = max_remaining_days * SECONDS_PER_DAY
    if remaining > cap or cap - remaining < period:
        return None
    return now_unix_sec + remaining + period


def gm_expiry(now_unix_sec: int, remaining_seconds: int) -> int:
    """GM 设定到期时刻：0 表示清除，正值表示 ``now + remaining_seconds``。"""
    return 0 if remaining_seconds == 0 else now_unix_sec + remaining_seconds


class SceneAfkPass:
    """挂机月卡客户端 RPC 处理器与 Redis 投影同步器。"""

    def __init__(
        self,
        scene,
        tables,
        assets,
        *,
        player_redis=None,
        allow_gm: bool | None = None,
        now_s: Callable[[], int] | None = None,
    ) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.player_redis = player_redis
        self._allow_gm = allow_gm
        self.now_s = now_s or (lambda: time.time_ns() // 1_000_000_000)
        self._projection_failures: int = 0
        tx_enum = self.protocol.pool.FindEnumTypeByName("TransactionType")
        buy_tx = tx_enum.values_by_name.get("TX_AFK_PASS_BUY") or tx_enum.values_by_name["TX_CURRENCY_DEDUCT"]
        self.buy_tx = buy_tx.number

    @property
    def projection_failures(self) -> int:
        """累计挂机月卡 Redis 投影写失败次数（对齐 C++ AfkPassSystem::ProjectionFailures）。"""
        return self._projection_failures

    def render_metrics(self, hosting_events: dict[str, int] | None = None) -> str:
        """导出挂机托管与月卡投影失败计数 Prometheus 文本指标（对齐 C++ AfkHostingSystem::WriteMetrics）。"""
        events = hosting_events or {}
        lines = [
            "# HELP scene_afk_hosting_events_total Offline hosting events on this scene node since process start.",
            "# TYPE scene_afk_hosting_events_total counter",
        ]
        for name in ("probe", "probe_failed", "hosted", "renewed", "ended"):
            lines.append(f'scene_afk_hosting_events_total{{event="{name}"}} {int(events.get(name, 0))}')
        lines.extend([
            "# HELP scene_afk_pass_projection_failures_total AFK pass Redis projection writes that failed since process start.",
            "# TYPE scene_afk_pass_projection_failures_total counter",
            f"scene_afk_pass_projection_failures_total {self._projection_failures}",
        ])
        return "\n".join(lines) + "\n"

    @property
    def allow_gm(self) -> bool:
        if self._allow_gm is not None:
            return self._allow_gm
        return bool(getattr(self.assets, "allow_gm", False))

    def register(self) -> None:
        self.scene.register(_SERVICE, "GetAfkPass", self.get_pass)
        self.scene.register(_SERVICE, "BuyAfkPass", self.buy_pass, mutating=True)
        self.scene.register(_SERVICE, "GmSetAfkPass", self.gm_set_pass, mutating=True)
        self.scene.register(_SERVICE, "SetAfkPassSettings", self.set_settings, mutating=True)
        self.scene.register(_SERVICE, "AckHostedRecap", self.ack_hosted_recap, mutating=True)

    def is_active_data(self, data, now_sec: int | None = None) -> bool:
        """基于已装载的玩家数据判定轻享卡/挂机月卡是否生效。"""
        if not data.HasField("uint64_pb_component"):
            return False
        now_unix = self.now_s() if now_sec is None else int(now_sec)
        return is_active(int(data.uint64_pb_component.afk_pass_expires_at), now_unix)

    def current_rule(self) -> tuple[AfkPassRuleData | None, str | None]:
        try:
            table = self.tables["AfkPassRule"]
        except KeyError:
            table = None
        rows = table.rows() if table is not None else ()
        if not rows:
            return None, "AfkPassRule table is empty"
        row = rows[0]
        rule = AfkPassRuleData(
            period_days=int(row.period_days),
            price_currency_type=int(row.price_currency_type),
            price=int(row.price),
            max_remaining_days=int(row.max_remaining_days),
            remind_before_hours=int(row.remind_before_hours),
        )
        problem = rule_problem(rule, self.assets.currency_types)
        if problem is not None:
            return None, problem
        return rule, None

    def validate_tables(self) -> bool:
        _, problem = self.current_rule()
        if problem is not None:
            logger.error("AfkPass table validation failed: %s", problem)
            return False
        return True

    def _fill_info(self, data, now_sec: int, info) -> None:
        info.Clear()
        profile = data.uint64_pb_component
        expires_at = int(profile.afk_pass_expires_at)
        active = is_active(expires_at, now_sec)
        info.expires_at = expires_at
        info.server_now = int(now_sec)
        info.unlocked_loadout_slots = unlocked_loadout_slots(active)
        info.stage2_tradable_enabled = active
        info.auto_return_city_on_team_end = bool(profile.afk_auto_return_city_on_team_end)
        if profile.hosted_recap_pending_ack or profile.hosted_recap_started_at:
            recap = info.hosted_recap
            recap.started_at = int(profile.hosted_recap_started_at)
            recap.ended_at = int(profile.hosted_recap_ended_at)
            recap.battle_count = int(profile.hosted_recap_battle_count)
            recap.win_count = int(profile.hosted_recap_win_count)
            recap.exp_gained = int(profile.hosted_recap_exp_gained)
            recap.silver_gained = int(profile.hosted_recap_silver_gained)
            recap.item_count = int(profile.hosted_recap_item_count)
            recap.pending_ack = bool(profile.hosted_recap_pending_ack)
        rule, _ = self.current_rule()
        if rule is None:
            return
        info.price_currency_type = rule.price_currency_type
        info.price = rule.price
        info.period_days = rule.period_days
        info.max_remaining_days = rule.max_remaining_days
        info.remind_before_hours = rule.remind_before_hours
        info.balance = balance(data, rule.price_currency_type)

    def _reject(self, route, tip: int):
        response = self.protocol.message(route.response_type)
        response.error_message.id = tip
        return response

    async def sync_projection(self, player_id: int, expires_at: int, *, now_sec: int | None = None) -> bool:
        """将已提交的月卡到期时刻投影到可选 ``player_redis``（未来时刻写 ``EX`` TTL，否则 ``DEL``）。"""
        if self.player_redis is None:
            return False
        now_unix = self.now_s() if now_sec is None else int(now_sec)
        key = afk_pass_key(player_id)
        try:
            if expires_at > now_unix:
                ttl = max(1, int(expires_at - now_unix))
                await self.player_redis.set(key, str(int(expires_at)).encode("ascii"), ex=ttl)
            else:
                await self.player_redis.delete(key)
            return True
        except Exception:
            self._projection_failures += 1
            logger.warning(
                "AfkPass redis projection failed player_id=%d expires_at=%d failures_total=%d",
                player_id,
                expires_at,
                self._projection_failures,
                exc_info=True,
            )
            return False

    def get_pass(self, _world, player, route, _request):
        data = self.scene.rows.assemble(player.snapshot)
        response = self.protocol.message(route.response_type)
        self._fill_info(data, self.now_s(), response.info)
        return response

    async def buy_pass(self, _world, player, route, request):
        now_sec = self.now_s()
        if not request.client_request_id:
            return self._reject(route, INVALID)
        data = self.scene.rows.assemble(player.snapshot)
        ledger = data.client_request_ledger
        error, fingerprint, prior = client_request.check(ledger, request, _BUY_AFK_PASS_KIND)
        if error:
            return self._reject(route, error)
        if prior is not None:
            response = self.protocol.message(route.response_type, replayed=True)
            self._fill_info(data, now_sec, response.info)
            return response

        rule, problem = self.current_rule()
        if rule is None:
            logger.error(
                "AfkPass purchase refused: AfkPassRule is not usable (%s) player_id=%d",
                problem,
                player.player_id,
            )
            return self._reject(route, AFK_PASS_UNAVAILABLE)

        if request.expected_currency_type != rule.price_currency_type or request.expected_price != rule.price:
            return self._reject(route, AFK_PASS_PRICE_CHANGED)

        if not self.scene.mutable(player):
            return self._reject(route, AFK_PASS_FROZEN)

        before_expires = int(data.uint64_pb_component.afk_pass_expires_at)
        after_expires = renewed_expiry(before_expires, now_sec, rule.period_days, rule.max_remaining_days)
        if after_expires is None:
            return self._reject(route, AFK_PASS_STACK_LIMIT)

        if balance(data, rule.price_currency_type) < rule.price:
            return self._reject(route, AFK_PASS_CURRENCY_NOT_ENOUGH)

        before_data = clone(data) if self.assets.audit_enabled else None
        deduct_err = self.assets.deduct_currency(data, rule.price_currency_type, rule.price)
        if deduct_err == INSUFFICIENT:
            return self._reject(route, AFK_PASS_CURRENCY_NOT_ENOUGH)
        if deduct_err:
            logger.error(
                "AfkPass purchase refused: charge rejected after precheck player_id=%d currency=%d price=%d err=%d",
                player.player_id,
                rule.price_currency_type,
                rule.price,
                deduct_err,
            )
            return self._reject(route, AFK_PASS_UNAVAILABLE)

        data.uint64_pb_component.afk_pass_expires_at = after_expires
        client_request.remember(ledger, request.client_request_id, _BUY_AFK_PASS_KIND, fingerprint)
        audit = self.assets.make_audit_callback(
            player, before_data, data, self.buy_tx, correlation=request.client_request_id,
        )

        attributes = getattr(self.scene, "attributes", None)

        async def on_committed(snapshot):
            if audit is not None:
                await audit(snapshot)
            await self.sync_projection(player.player_id, after_expires, now_sec=now_sec)
            if attributes is not None:
                attributes.notify(player)

        try:
            await self.scene.commit_player_data(player, data, on_committed=on_committed)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("AfkPass purchase save failed player_id=%d", player.player_id, exc_info=True)
            return self._reject(route, UNAVAILABLE)

        logger.info(
            "AfkPass bought player_id=%d expires_before=%d expires_after=%d currency=%d price=%d client_request_id=%d",
            player.player_id,
            before_expires,
            after_expires,
            rule.price_currency_type,
            rule.price,
            request.client_request_id,
        )
        response = self.protocol.message(route.response_type)
        self._fill_info(data, now_sec, response.info)
        return response

    async def gm_set_pass(self, _world, player, route, request):
        if not self.allow_gm:
            return self._reject(route, FEATURE_UNAVAILABLE)
        if request.remaining_seconds > GM_MAX_REMAINING_SECONDS:
            return self._reject(route, INVALID)
        if not self.scene.mutable(player):
            return self._reject(route, AFK_PASS_FROZEN)

        now_sec = self.now_s()
        data = self.scene.rows.assemble(player.snapshot)
        before_expires = int(data.uint64_pb_component.afk_pass_expires_at)
        after_expires = gm_expiry(now_sec, int(request.remaining_seconds))
        data.uint64_pb_component.afk_pass_expires_at = after_expires

        attributes = getattr(self.scene, "attributes", None)

        async def on_committed(_snapshot):
            await self.sync_projection(player.player_id, after_expires, now_sec=now_sec)
            if attributes is not None:
                attributes.notify(player)

        try:
            await self.scene.commit_player_data(player, data, on_committed=on_committed)
        except (StoreRejected, StoreUnavailable, OSError):
            logger.warning("AfkPass GM set save failed player_id=%d", player.player_id, exc_info=True)
            return self._reject(route, UNAVAILABLE)

        logger.info(
            "AfkPass GM set player_id=%d expires_before=%d expires_after=%d remaining_seconds=%d",
            player.player_id,
            before_expires,
            after_expires,
            request.remaining_seconds,
        )
        response = self.protocol.message(route.response_type)
        self._fill_info(data, now_sec, response.info)
        return response

    async def set_settings(self, _world, player, route, request):
        """522 修改离线托管偏好设置（如离队/解散自动回默认主城安全区）。"""
        if not self.scene.mutable(player):
            return self._reject(route, AFK_PASS_FROZEN)
        now_sec = self.now_s()
        data = self.scene.rows.assemble(player.snapshot)
        desired = bool(request.auto_return_city_on_team_end)
        if bool(data.uint64_pb_component.afk_auto_return_city_on_team_end) != desired:
            data.uint64_pb_component.afk_auto_return_city_on_team_end = desired
            try:
                await self.scene.commit_player_data(player, data)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("AfkPass settings save failed player_id=%d", player.player_id, exc_info=True)
                return self._reject(route, UNAVAILABLE)
            logger.info(
                "AfkPass settings updated player_id=%d auto_return_city=%d",
                player.player_id, int(desired),
            )
        response = self.protocol.message(route.response_type)
        self._fill_info(data, now_sec, response.info)
        return response

    async def ack_hosted_recap(self, _world, player, route, request):
        """523 确认已阅离线托管收获简报（按 started_at 幂等清零，避免误清后续新会话）。"""
        if not self.scene.mutable(player):
            return self._reject(route, AFK_PASS_FROZEN)
        now_sec = self.now_s()
        data = self.scene.rows.assemble(player.snapshot)
        profile = data.uint64_pb_component
        started_at = int(request.started_at)
        if profile.hosted_recap_pending_ack and (started_at == 0 or int(profile.hosted_recap_started_at) == started_at):
            acked_start = int(profile.hosted_recap_started_at)
            clear_hosted_recap_fields(profile)
            try:
                await self.scene.commit_player_data(player, data)
            except (StoreRejected, StoreUnavailable, OSError):
                logger.warning("AfkPass hosted recap ack save failed player_id=%d", player.player_id, exc_info=True)
                return self._reject(route, UNAVAILABLE)
            logger.info("AfkPass hosted recap acked player_id=%d started_at=%d", player.player_id, acked_start)
        response = self.protocol.message(route.response_type)
        self._fill_info(data, now_sec, response.info)
        return response

    @staticmethod
    def on_hosting_started(data, now_unix_sec: int) -> bool:
        """进入离线托管会话时记录起点；已有未结束会话（跨节点重建）则沿用原起点与累计收益。"""
        profile = data.uint64_pb_component
        if profile.hosted_recap_started_at and not profile.hosted_recap_ended_at:
            return False
        clear_hosted_recap_fields(profile)
        profile.hosted_recap_started_at = int(now_unix_sec) if now_unix_sec else 1
        return True

    @staticmethod
    def record_hosted_battle_settlement(
        data,
        *,
        hosting_active: bool,
        won: bool,
        exp_gained: int,
        silver_gained: int,
        item_count: int,
        now_unix_sec: int,
    ) -> bool:
        """离线托管期间同帧汇总真实战斗结算收益（不额外发产出）。"""
        if not hosting_active:
            return False
        profile = data.uint64_pb_component
        if not profile.hosted_recap_started_at or profile.hosted_recap_ended_at:
            clear_hosted_recap_fields(profile)
            profile.hosted_recap_started_at = int(now_unix_sec) if now_unix_sec else 1
        profile.hosted_recap_battle_count = min(_UINT32_MAX, int(profile.hosted_recap_battle_count) + 1)
        if won:
            profile.hosted_recap_win_count = min(_UINT32_MAX, int(profile.hosted_recap_win_count) + 1)
        profile.hosted_recap_exp_gained = min(_UINT64_MAX, int(profile.hosted_recap_exp_gained) + max(0, int(exp_gained)))
        profile.hosted_recap_silver_gained = min(
            _UINT64_MAX, int(profile.hosted_recap_silver_gained) + max(0, int(silver_gained)),
        )
        profile.hosted_recap_item_count = min(
            _UINT32_MAX, int(profile.hosted_recap_item_count) + max(0, int(item_count)),
        )
        return True

    @staticmethod
    def on_hosting_ended(data, now_unix_sec: int) -> bool:
        """结束离线托管会话并置待客户端确认标记。"""
        profile = data.uint64_pb_component
        if not profile.hosted_recap_started_at or profile.hosted_recap_ended_at:
            return False
        profile.hosted_recap_ended_at = max(int(now_unix_sec), int(profile.hosted_recap_started_at))
        profile.hosted_recap_pending_ack = True
        return True
