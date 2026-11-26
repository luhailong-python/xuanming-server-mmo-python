"""Scene 江湖恩怨与天墉生死状一期系统（对齐 C++ player_jianghu.cpp 与 scene_node_service.cpp）。

负责：
- 闭关免战开关查询与切换（555 GetJianghuProfile / 556 SetJianghuRefuseStrangers）；
- 30 分钟战败社交印记与清风洗尘符洒脱徽记展示（按角色持有代次 owner_revision 自动防过户背仇）；
- 银两（默认 1000）或清风洗尘符（Item 3416）幂等清除战败印记（557 ClearJianghuDefeatMark，账本 kind=29）；
- match -> scene gRPC 约战前资格与资费预检（564/566 CheckJianghuDuelEligibility）；
- 备战期发起方队长资费预留/扣除、取消与作废退还、结算挂印记/复仇清标与消耗金帖。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from ..data import StoreRejected, StoreUnavailable, client_request
from ..data.asset_inventory import (
    FEATURE_UNAVAILABLE,
    INVALID,
    UNAVAILABLE,
    AssetInventory,
    balance,
    carries_unknown,
    clone,
)
from .scene_mission import now_ms
from .scene_player import _bag

logger = logging.getLogger(__name__)

_SERVICE = "SceneJianghuClientPlayer"
CLIENT_REQUEST_KIND_CLEAR_DEFEAT_MARK = 29

JIANGHU_RULE_ROW_ID = 1
DEFAULT_GOLD_WRIT_ITEM_ID = 3415
DEFAULT_CLEAR_ITEM_ID = 3416
DEFAULT_SILVER_WRIT_COST = 500
DEFAULT_SILVER_CLEAR_COST = 1000
DEFAULT_DEFEAT_MARK_DURATION_SEC = 1800
DEFAULT_CLEAR_BADGE_DURATION_SEC = 1800
MAX_ELIGIBILITY_PLAYERS_PER_CALL = 10

WRIT_TIER_SILVER = 1
WRIT_TIER_GOLD_ITEM = 2

CLEAR_MODE_SILVER = 1
CLEAR_MODE_ITEM_CHARM = 2

MATCH_MODE_JIANGHU_DUEL = 12
CURRENCY_GOLD = 0

TX_JIANGHU_DUEL_FEE = 49
TX_JIANGHU_DUEL_REFUND = 50
TX_JIANGHU_CLEAR_DEFEAT_MARK = 51

# 对应 C++ jianghu_error_tip.pb.h 错误码。
JIANGHU_DUEL_DISABLED = 40000
JIANGHU_INSUFFICIENT_WRIT_OR_SILVER = 40008
JIANGHU_NO_ACTIVE_DEFEAT_MARK = 40011

_INT64_MAX = (1 << 63) - 1
_UINT32_MAX = (1 << 32) - 1


@dataclass(frozen=True, slots=True)
class JianghuRuleConfig:
    """JianghuRule 配置表第 1 行解析结果。"""

    found: bool = False
    enabled: bool = False
    silver_writ_cost: int = DEFAULT_SILVER_WRIT_COST
    gold_writ_item_id: int = DEFAULT_GOLD_WRIT_ITEM_ID
    clear_item_id: int = DEFAULT_CLEAR_ITEM_ID
    silver_clear_cost: int = DEFAULT_SILVER_CLEAR_COST
    defeat_mark_duration_sec: int = DEFAULT_DEFEAT_MARK_DURATION_SEC
    clear_badge_duration_sec: int = DEFAULT_CLEAR_BADGE_DURATION_SEC


def load_jianghu_rule(tables) -> JianghuRuleConfig:
    """从 ConfigTables 读取 JianghuRule(id=1)；缺表或无行时返回 found=False。"""
    if tables is None or "JianghuRule" not in tables.tables:
        return JianghuRuleConfig()
    row = tables["JianghuRule"].get(JIANGHU_RULE_ROW_ID)
    if row is None:
        return JianghuRuleConfig()
    return JianghuRuleConfig(
        found=True,
        enabled=bool(getattr(row, "enabled", 0)),
        silver_writ_cost=int(getattr(row, "silver_writ_cost", DEFAULT_SILVER_WRIT_COST)),
        gold_writ_item_id=int(getattr(row, "gold_writ_item_id", DEFAULT_GOLD_WRIT_ITEM_ID)),
        clear_item_id=int(getattr(row, "clear_item_id", DEFAULT_CLEAR_ITEM_ID)),
        silver_clear_cost=int(getattr(row, "silver_clear_cost", DEFAULT_SILVER_CLEAR_COST)),
        defeat_mark_duration_sec=int(getattr(row, "defeat_mark_duration_sec", DEFAULT_DEFEAT_MARK_DURATION_SEC)),
        clear_badge_duration_sec=int(getattr(row, "clear_badge_duration_sec", DEFAULT_CLEAR_BADGE_DURATION_SEC)),
    )


def _current_owner_revision(player) -> int:
    return int(getattr(player, "owner_revision", 0) or 0)


def _owner_revision_mismatched(comp, current_owner_rev: int) -> bool:
    return current_owner_rev != 0 and comp.owner_revision != 0 and comp.owner_revision != current_owner_rev


def _clear_defeat_mark_fields(comp) -> None:
    comp.defeat_mark_expires_at_ms = 0
    comp.defeat_mark_winner_id = 0
    comp.defeat_mark_winner_name = ""
    comp.defeat_mark_style_id = 0
    comp.defeat_mark_is_guild_name = False
    comp.defeat_mark_battle_id = 0


def _clear_badge_fields(comp) -> None:
    comp.clear_badge_expires_at_ms = 0
    comp.clear_badge_style_id = 0


def _normalize_owner_revision(comp, current_owner_rev: int) -> None:
    if current_owner_rev == 0:
        return
    if comp.owner_revision != 0 and comp.owner_revision != current_owner_rev:
        # 官方角色过户换手后清空旧号主战败印记、洒脱徽记与未结银两痕迹，防止新号主替旧号主背仇。
        _clear_defeat_mark_fields(comp)
        _clear_badge_fields(comp)
        comp.pending_silver_fee_battle_id = 0
        comp.pending_silver_fee_amount = 0
    comp.owner_revision = current_owner_rev


class SceneJianghu:
    """Scene 江湖恩怨与天墉生死状伴生模块。"""

    def __init__(self, scene, tables, assets=None, *, clock=now_ms) -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets if assets is not None else getattr(scene, "assets", None)
        self.clock = clock
        self._fallback_inventory = AssetInventory(self.protocol, tables)
        self.scene.jianghu = self

    def register(self) -> None:
        """向 SceneNode 注册江湖面板与印记清除客户端 RPC（555..557）。"""
        self.scene.register(_SERVICE, "GetJianghuProfile", self.get_profile)
        self.scene.register(_SERVICE, "SetJianghuRefuseStrangers", self.set_refuse_strangers, mutating=True)
        self.scene.register(_SERVICE, "ClearJianghuDefeatMark", self.clear_defeat_mark, mutating=True)

    def grpc_methods(self) -> dict:
        """向 SceneNodeGrpc 暴露天墉生死状资格与资费预检入口（566）。"""
        return {"CheckJianghuDuelEligibility": self.check_duel_eligibility}

    # ── 内部工具 ──

    def _inventory(self):
        return self.assets if self.assets is not None else self._fallback_inventory

    def _removable_count(self, data, item_config_id: int) -> int:
        if not item_config_id:
            return 0
        count = int(self._inventory().removable_count(data, item_config_id))
        return min(_UINT32_MAX, max(0, count))

    def _in_battle(self, player) -> bool:
        battle = getattr(self.scene, "battle", None)
        return battle is not None and battle.frozen(player.player_id)

    def _online_player(self, player_id: int):
        world = self.scene.locations.get(player_id)
        if world is None:
            return None
        player = world.players.get(player_id)
        if player is None:
            return None
        if getattr(self.scene, "_releasing", None) and self.scene._releasing(player):
            return None
        return player

    def _fill_profile_view(self, player, data, rule: JianghuRuleConfig, stamp: int, out) -> None:
        out.Clear()
        comp = data.jianghu_component
        out.server_time_ms = stamp
        out.duel_refuse_strangers = bool(comp.duel_refuse_strangers)

        owner_mismatch = _owner_revision_mismatched(comp, _current_owner_revision(player))
        if not owner_mismatch and comp.defeat_mark_expires_at_ms > stamp:
            out.has_active_defeat_mark = True
            out.defeat_mark_expires_at_ms = comp.defeat_mark_expires_at_ms
            out.defeat_mark_winner_id = comp.defeat_mark_winner_id
            out.defeat_mark_winner_name = comp.defeat_mark_winner_name
            out.defeat_mark_style_id = comp.defeat_mark_style_id
            out.defeat_mark_is_guild_name = bool(comp.defeat_mark_is_guild_name)
            out.defeat_mark_battle_id = comp.defeat_mark_battle_id
        if not owner_mismatch and comp.clear_badge_expires_at_ms > stamp:
            out.has_active_clear_badge = True
            out.clear_badge_expires_at_ms = comp.clear_badge_expires_at_ms
            out.clear_badge_style_id = comp.clear_badge_style_id

        out.silver_balance = balance(data, CURRENCY_GOLD)
        out.gold_writ_item_count = self._removable_count(data, rule.gold_writ_item_id)
        out.clear_charm_item_count = self._removable_count(data, rule.clear_item_id)
        out.silver_writ_cost = rule.silver_writ_cost
        out.silver_clear_cost = rule.silver_clear_cost

    def _fill_clear_response(self, player, data, mode: int, rule: JianghuRuleConfig, stamp: int, response) -> None:
        self._fill_profile_view(player, data, rule, stamp, response.profile)
        if mode == CLEAR_MODE_ITEM_CHARM:
            inv = _bag(self.scene, self.tables, player, 0)
            if inv.error_message.id:
                response.ClearField("bag")
            else:
                response.bag.CopyFrom(inv.bag)
                equip_craft = getattr(self.scene, "equip_craft", None)
                if equip_craft is not None and hasattr(equip_craft, "_enrich_bag_snapshot"):
                    equip_craft._enrich_bag_snapshot(data, 0, response.bag)

    # ── 客户端 RPC（555..557） ──

    def get_profile(self, _world, player, _route, _request):
        """查询个人江湖面板（只读，绝不写盘）。"""
        response = self.protocol.message("GetJianghuProfileResponse")
        data = self.scene.rows.assemble(player.snapshot)
        rule = load_jianghu_rule(self.tables)
        self._fill_profile_view(player, data, rule, self.clock(), response.profile)
        return response

    async def set_refuse_strangers(self, _world, player, _route, request):
        """切换「闭关免战」开关并落盘。"""
        response = self.protocol.message("SetJianghuRefuseStrangersResponse")
        if not self.scene.mutable(player) or self._in_battle(player):
            response.error_message.id = FEATURE_UNAVAILABLE
            return response
        stamp = self.clock()
        data = self.scene.rows.assemble(player.snapshot)
        comp = data.jianghu_component
        comp.SetInParent()
        _normalize_owner_revision(comp, _current_owner_revision(player))
        desired = bool(request.refuse_strangers)
        if comp.duel_refuse_strangers != desired:
            comp.duel_refuse_strangers = desired
            try:
                await self.scene.commit_player_data(player, data)
            except (StoreRejected, StoreUnavailable, OSError):
                response.error_message.id = UNAVAILABLE
                return response
            logger.info(
                "Jianghu refuse strangers updated player_id=%s refuse_strangers=%s",
                player.player_id, desired,
            )
        rule = load_jianghu_rule(self.tables)
        self._fill_profile_view(player, data, rule, stamp, response.profile)
        return response

    async def clear_defeat_mark(self, _world, player, _route, request):
        """幂等清除当前生效的 30 分钟战败社交印记（银两普通清除，或清风洗尘符附赠洒脱徽记）。"""
        response = self.protocol.message("ClearJianghuDefeatMarkResponse")
        if carries_unknown(request):
            response.error_message.id = INVALID
            return response
        client_request_id = int(request.client_request_id)
        mode = int(request.mode)
        if not client_request_id or mode not in (CLEAR_MODE_SILVER, CLEAR_MODE_ITEM_CHARM):
            response.error_message.id = INVALID
            return response

        stamp = self.clock()
        rule = load_jianghu_rule(self.tables)
        data = self.scene.rows.assemble(player.snapshot)
        err, fingerprint, prior = client_request.check(
            data.client_request_ledger, request, CLIENT_REQUEST_KIND_CLEAR_DEFEAT_MARK,
        )
        if err:
            response.error_message.id = err
            return response
        if prior is not None:
            response.replayed = True
            self._fill_clear_response(player, data, mode, rule, stamp, response)
            return response

        if not self.scene.mutable(player) or self._in_battle(player):
            response.error_message.id = FEATURE_UNAVAILABLE
            return response
        if not rule.found or not rule.enabled:
            response.error_message.id = JIANGHU_DUEL_DISABLED
            return response
        if self.assets is None:
            response.error_message.id = UNAVAILABLE
            return response

        owner_rev = _current_owner_revision(player)
        comp = data.jianghu_component
        if _owner_revision_mismatched(comp, owner_rev) or comp.defeat_mark_expires_at_ms <= stamp:
            response.error_message.id = JIANGHU_NO_ACTIVE_DEFEAT_MARK
            return response

        staged = clone(data)
        staged_comp = staged.jianghu_component
        staged_comp.SetInParent()
        _normalize_owner_revision(staged_comp, owner_rev)
        _clear_defeat_mark_fields(staged_comp)
        if mode == CLEAR_MODE_ITEM_CHARM and rule.clear_badge_duration_sec > 0:
            staged_comp.clear_badge_expires_at_ms = stamp + rule.clear_badge_duration_sec * 1000
            staged_comp.clear_badge_style_id = 1

        client_request.remember(
            staged.client_request_ledger,
            client_request_id,
            CLIENT_REQUEST_KIND_CLEAR_DEFEAT_MARK,
            fingerprint,
        )

        if mode == CLEAR_MODE_SILVER:
            if not 0 < rule.silver_clear_cost <= _INT64_MAX:
                response.error_message.id = UNAVAILABLE
                return response
            if balance(staged, CURRENCY_GOLD) < rule.silver_clear_cost:
                response.error_message.id = JIANGHU_INSUFFICIENT_WRIT_OR_SILVER
                return response
            deduct_err = self.assets.deduct_currency(staged, CURRENCY_GOLD, rule.silver_clear_cost)
            if deduct_err:
                response.error_message.id = (
                    JIANGHU_INSUFFICIENT_WRIT_OR_SILVER if deduct_err == 27001 else FEATURE_UNAVAILABLE
                )
                return response
            saved = await self.assets._save(
                player, staged, TX_JIANGHU_CLEAR_DEFEAT_MARK, correlation=client_request_id,
            )
            if not saved:
                response.error_message.id = UNAVAILABLE
                return response
            self._fill_profile_view(player, staged, rule, stamp, response.profile)
            logger.info(
                "Jianghu defeat mark cleared with silver player_id=%s cost=%s client_request_id=%s",
                player.player_id, rule.silver_clear_cost, client_request_id,
            )
            return response

        # CLEAR_MODE_ITEM_CHARM：消耗 1 张 Item 3416「清风洗尘符」，附赠洒脱徽记。
        if not rule.clear_item_id:
            response.error_message.id = UNAVAILABLE
            return response
        if self._removable_count(staged, rule.clear_item_id) < 1:
            response.error_message.id = JIANGHU_INSUFFICIENT_WRIT_OR_SILVER
            return response
        consume_err = self.assets.consume_items(staged, {rule.clear_item_id: 1})
        if consume_err:
            response.error_message.id = (
                consume_err if consume_err in (FEATURE_UNAVAILABLE, UNAVAILABLE)
                else JIANGHU_INSUFFICIENT_WRIT_OR_SILVER
            )
            return response
        saved = await self.assets._save(
            player,
            staged,
            TX_JIANGHU_CLEAR_DEFEAT_MARK,
            correlation=client_request_id,
            item_extra={"source": "jianghu_clear_defeat_mark"},
        )
        if not saved:
            response.error_message.id = UNAVAILABLE
            return response
        self._fill_clear_response(player, staged, mode, rule, stamp, response)
        logger.info(
            "Jianghu defeat mark cleared with charm player_id=%s item_id=%s client_request_id=%s",
            player.player_id, rule.clear_item_id, client_request_id,
        )
        return response

    # ── match -> scene gRPC 约战前资格与资费预检（564/566） ──

    def check_duel_eligibility(self, request):
        """只读检查成员在线状态、闭关免战开关与发起方队长银两/金帖余额（无副作用）。"""
        response = self.protocol.message("CheckJianghuDuelEligibilityResponse")
        rule = load_jianghu_rule(self.tables)
        if not rule.found or not rule.enabled:
            response.error_message.id = JIANGHU_DUEL_DISABLED
            return response
        if len(request.player_ids) > MAX_ELIGIBILITY_PLAYERS_PER_CALL:
            response.error_message.id = INVALID
            return response

        for player_id in request.player_ids:
            entry = response.players.add(player_id=int(player_id))
            player = self._online_player(int(player_id))
            entry.online_on_node = player is not None
            if player is not None:
                data = self.scene.rows.assemble(player.snapshot)
                entry.duel_refuse_strangers = bool(data.jianghu_component.duel_refuse_strangers)

        payer_id = int(request.payer_player_id)
        if not payer_id:
            return response
        payer = self._online_player(payer_id)
        if payer is None:
            response.payer_has_sufficient_fee = False
            return response
        payer_data = self.scene.rows.assemble(payer.snapshot)
        writ_tier = int(request.writ_tier)
        if writ_tier == WRIT_TIER_SILVER:
            silver_cost = int(request.silver_cost) if request.silver_cost else rule.silver_writ_cost
            response.payer_has_sufficient_fee = (
                0 < silver_cost <= _INT64_MAX and balance(payer_data, CURRENCY_GOLD) >= silver_cost
            )
            return response
        if writ_tier == WRIT_TIER_GOLD_ITEM:
            item_config_id = int(request.item_config_id) if request.item_config_id else rule.gold_writ_item_id
            response.payer_has_sufficient_fee = (
                item_config_id != 0 and self._removable_count(payer_data, item_config_id) >= 1
            )
            return response
        response.payer_has_sufficient_fee = False
        return response

    # ── PrepareBattle / CancelBattlePrepare / Void / Settlement 协同原语 ──

    def validate_and_collect_prepare_fee(self, data, battle_id: int, fee, held_counts: dict[int, int]) -> tuple[int, int]:
        """校验发起方队长生死状资费；金帖写入 held_counts，普通战书返回需扣流通银两额。"""
        if not battle_id:
            return INVALID, 0
        rule = load_jianghu_rule(self.tables)
        if not rule.found or not rule.enabled:
            return JIANGHU_DUEL_DISABLED, 0

        writ_tier = int(fee.writ_tier)
        if writ_tier == WRIT_TIER_GOLD_ITEM:
            item_config_id = int(fee.item_config_id) if fee.item_config_id else rule.gold_writ_item_id
            if not item_config_id or item_config_id != rule.gold_writ_item_id:
                return INVALID, 0
            if self._removable_count(data, item_config_id) < 1:
                return JIANGHU_INSUFFICIENT_WRIT_OR_SILVER, 0
            held_counts[item_config_id] = held_counts.get(item_config_id, 0) + 1
            return 0, 0
        if writ_tier == WRIT_TIER_SILVER:
            silver_cost = int(fee.silver_cost) if fee.silver_cost else rule.silver_writ_cost
            if not silver_cost or silver_cost != rule.silver_writ_cost or silver_cost > _INT64_MAX:
                return INVALID, 0
            if data.jianghu_component.pending_silver_fee_battle_id == battle_id:
                return 0, 0
            if balance(data, CURRENCY_GOLD) < silver_cost:
                return JIANGHU_INSUFFICIENT_WRIT_OR_SILVER, 0
            return 0, silver_cost
        return INVALID, 0

    def commit_prepare_silver_fee(self, data, battle_id: int, silver_to_deduct: int) -> int:
        """备战期在冻结道具后同步预扣发起方队长普通战书流通银两并登记待退记录。"""
        if not silver_to_deduct:
            return 0
        if not battle_id or silver_to_deduct > _INT64_MAX:
            return INVALID
        comp = data.jianghu_component
        comp.SetInParent()
        if comp.pending_silver_fee_battle_id == battle_id:
            return 0
        deduct_err = self._inventory().deduct_currency(data, CURRENCY_GOLD, silver_to_deduct)
        if deduct_err:
            return JIANGHU_INSUFFICIENT_WRIT_OR_SILVER if deduct_err == 27001 else FEATURE_UNAVAILABLE
        comp.pending_silver_fee_battle_id = battle_id
        comp.pending_silver_fee_amount = silver_to_deduct
        return 0

    def on_battle_prepare_cancelled(self, data, battle_id: int, *, skip_silver_refund: bool = False) -> tuple[bool, int]:
        """备战取消时收尾普通战书待退记录；skip_silver_refund=False 时当场退还流通银两。"""
        if not battle_id:
            return False, 0
        comp = data.jianghu_component
        if comp.pending_silver_fee_battle_id != battle_id:
            return False, 0
        refund_amount = int(comp.pending_silver_fee_amount)
        comp.pending_silver_fee_battle_id = 0
        comp.pending_silver_fee_amount = 0
        if skip_silver_refund or not 0 < refund_amount <= _INT64_MAX:
            return True, 0
        credit_err = self._inventory().add_currency(data, CURRENCY_GOLD, refund_amount, counts_as_gain=False)
        if credit_err:
            logger.error(
                "Jianghu failed to refund silver duel fee battle_id=%s amount=%s err=%s",
                battle_id, refund_amount, credit_err,
            )
            return True, 0
        return True, refund_amount

    def on_battle_void(self, data, battle_id: int) -> tuple[bool, int]:
        """作废局随收件箱原子退还普通战书银两并清理待退记录。"""
        return self.on_battle_prepare_cancelled(data, battle_id, skip_silver_refund=False)

    def prepare_battle_settlement(
        self, player, data, settlement, now_ms_val: int,
    ) -> tuple[int, object, bool, int, int]:
        """纯计算生死状结算候选态。

        返回 `(err, candidate_comp, changed, gold_writ_to_consume, silver_refund)`。
        """
        current = data.jianghu_component
        candidate = clone(current)
        if int(settlement.match_mode) != MATCH_MODE_JIANGHU_DUEL:
            return 0, candidate, False, 0, 0
        player_id = int(player.player_id)
        if not player_id or player_id != int(settlement.player_id):
            return INVALID, candidate, False, 0, 0

        before_bytes = current.SerializeToString(deterministic=True)
        _normalize_owner_revision(candidate, _current_owner_revision(player))

        pending_silver = 0
        if candidate.pending_silver_fee_battle_id == settlement.battle_id:
            pending_silver = int(candidate.pending_silver_fee_amount)
            candidate.pending_silver_fee_battle_id = 0
            candidate.pending_silver_fee_amount = 0

        # 作废或 0 回合流局：不消费金帖、不挂战败印记，普通战书银两随本收件箱原子退还。
        if int(settlement.outcome) == 4 or int(settlement.total_rounds) == 0:
            changed = candidate.SerializeToString(deterministic=True) != before_bytes
            return 0, candidate, changed, 0, pending_silver

        rule = load_jianghu_rule(self.tables)
        gold_writ_item_id = rule.gold_writ_item_id if (rule.found and rule.gold_writ_item_id) else DEFAULT_GOLD_WRIT_ITEM_ID
        defeat_duration_sec = (
            rule.defeat_mark_duration_sec if (rule.found and rule.defeat_mark_duration_sec) else DEFAULT_DEFEAT_MARK_DURATION_SEC
        )

        gold_writ_to_consume = 0
        if "jianghu_duel" in settlement.DESCRIPTOR.fields_by_name and settlement.HasField("jianghu_duel"):
            ctx = settlement.jianghu_duel
            if int(ctx.writ_tier) == WRIT_TIER_GOLD_ITEM and int(ctx.challenger_captain_id) == player_id:
                gold_writ_to_consume = gold_writ_item_id

            player_won = (
                (int(settlement.outcome) == 1 and int(settlement.player_team_index) == 0)
                or (int(settlement.outcome) == 2 and int(settlement.player_team_index) == 1)
            )
            player_lost = (
                (int(settlement.outcome) == 1 and int(settlement.player_team_index) == 1)
                or (int(settlement.outcome) == 2 and int(settlement.player_team_index) == 0)
            )

            if player_lost and candidate.defeat_mark_battle_id != settlement.battle_id:
                winner_team = 0 if int(settlement.outcome) == 1 else 1
                is_guild_duel = (
                    ctx.side_a_guild_id != 0
                    and ctx.side_b_guild_id != 0
                    and ctx.side_a_guild_id != ctx.side_b_guild_id
                )
                if is_guild_duel:
                    winner_id = int(ctx.side_a_guild_id if winner_team == 0 else ctx.side_b_guild_id)
                    winner_name = str(ctx.side_a_guild_name if winner_team == 0 else ctx.side_b_guild_name)
                else:
                    winner_id = int(ctx.challenger_captain_id if winner_team == 0 else ctx.target_captain_id)
                    winner_name = str(ctx.challenger_captain_name if winner_team == 0 else ctx.target_captain_name)
                candidate.defeat_mark_expires_at_ms = now_ms_val + defeat_duration_sec * 1000
                candidate.defeat_mark_winner_id = winner_id
                candidate.defeat_mark_winner_name = winner_name
                candidate.defeat_mark_style_id = 2 if int(ctx.writ_tier) == WRIT_TIER_GOLD_ITEM else 1
                candidate.defeat_mark_is_guild_name = is_guild_duel
                candidate.defeat_mark_battle_id = settlement.battle_id
            elif player_won and candidate.defeat_mark_expires_at_ms > now_ms_val:
                opp_captain_id = int(
                    ctx.target_captain_id if int(settlement.player_team_index) == 0 else ctx.challenger_captain_id
                )
                opp_guild_id = int(
                    ctx.side_b_guild_id if int(settlement.player_team_index) == 0 else ctx.side_a_guild_id
                )
                revenge_matched = (
                    (not candidate.defeat_mark_is_guild_name and opp_captain_id != 0 and opp_captain_id == candidate.defeat_mark_winner_id)
                    or (candidate.defeat_mark_is_guild_name and opp_guild_id != 0 and opp_guild_id == candidate.defeat_mark_winner_id)
                )
                if revenge_matched:
                    _clear_defeat_mark_fields(candidate)

        changed = candidate.SerializeToString(deterministic=True) != before_bytes
        return 0, candidate, changed, gold_writ_to_consume, 0
