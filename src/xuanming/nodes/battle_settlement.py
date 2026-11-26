"""应用 Match 的持久终局原件，奖励、消耗和近期结果同一次提交。"""

import logging
from collections import Counter

from ..data.asset_inventory import BAG_FULL, carries_unknown, release_item_hold
from .battle_dropin_effects import (
    BOSS_TRIAL_KIND_FESTIVAL_BEAST,
    BOSS_TRIAL_KIND_GUARDIAN_RESCUE,
    is_boss_trial_kind,
)
from .scene_afk_pass import SceneAfkPass
from .scene_attributes import (
    BattleProgressionDelta,
    LevelExpRow,
    ModeRewardRow,
    WeeklySilverRule,
    prepare_economy_ledger,
    resolve_level_for_exp,
)
from .scene_equip_craft import resolve_game_week
from .scene_festival_beast import SceneFestivalBeast
from .scene_mission import LEVEL, now_ms
from .scene_stats import pet_pool, pet_stats, player_stats

logger = logging.getLogger(__name__)
MAX_HISTORY = 10
MAX_LINES = 32
MAX_ITEM_COUNT = (1 << 32) - 1
MAX_CURRENCY_AMOUNT = (1 << 63) - 1
ASSET_BLOCKED = 27005
BAG_SPACE_DEFERRED = 6006  # Native kBagItemNotStacked executor defer reason.
_PVE_MODES = frozenset({1, 2})
_PVP_MODES = frozenset({3, 4, 5})


def _has_trial_facts(settlement) -> bool:
    if not hasattr(settlement, "trial_facts"):
        return False
    descriptor = getattr(settlement, "DESCRIPTOR", None)
    if descriptor is not None and "trial_facts" in descriptor.fields_by_name:
        return bool(settlement.HasField("trial_facts"))
    has_field = getattr(settlement, "HasField", None)
    if callable(has_field):
        return bool(has_field("trial_facts"))
    return settlement.trial_facts is not None


class BattleSettlement:
    def __init__(self, scene, tables, assets, missions, *, clock=now_ms):
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self.assets = assets
        self.missions = missions
        self.clock = clock
        self.currency_types = self.protocol.pool.FindEnumTypeByName("common.base.CurrencyType").values_by_number

    def register(self) -> None:
        self.scene.register("SceneBattleResultClientPlayer", "AckBattleResults", self.ack, mutating=True)

    def ack(self, world, player, route, request):
        response = self.protocol.message("AckBattleResultsResponse")
        if len(request.battle_ids) > MAX_HISTORY:
            response.error_message.id = 1015
        elif not self.scene.mutable(player):
            response.error_message.id = 1006
        else:
            data = self.scene.rows.assemble(player.snapshot)
            live = {entry.battle_id for entry in data.battle_results.entries}
            player.seen_battles.intersection_update(live)
            player.seen_battles.update(live.intersection(request.battle_ids))
        return response

    async def apply(
        self, player, data, settlement, *, audits: list | None = None, defer_reasons: list[int] | None = None,
    ) -> bool:
        if (
            not settlement.battle_id or settlement.player_id != player.player_id
            or settlement.outcome not in (1, 2, 3, 4) or carries_unknown(settlement)
            or not self.scene.mutable(player)
            or sum(item.count for item in settlement.defeated_monsters) > 4096
        ):
            return False
        if settlement.outcome == 4:
            allowed = {1, 2, 3, 4, 17, 18, 19}
            if any(field.number not in allowed for field, _ in settlement.ListFields()):
                return False
            release_item_hold(data, settlement.battle_id)
            SceneFestivalBeast.on_battle_prepare_cancelled_or_void(data, settlement.battle_id)
            if not any(entry.battle_id == settlement.battle_id for entry in data.battle_results.entries):
                self._record(data.battle_results, settlement, {}, {}, {}, self.clock())
            return True
        if self.scene.battle:
            current = self.scene.battle.current(player.player_id)
            if current and current != settlement.battle_id:
                return False
        if any(entry.battle_id == settlement.battle_id for entry in data.battle_results.entries):
            return True
        currencies = self._currencies(settlement)
        consumed = self._items(settlement.items_consumed)
        gained = self._items(settlement.items_gained)
        if currencies is None or consumed is None or gained is None:
            return False
        owned = self._owned(data, consumed)
        if owned is None:
            return False
        consumed = {item: amount for item, count in consumed.items()
                    if (amount := min(count, self.assets.removable_count(
                        data, item, except_hold_id=settlement.battle_id)))}
        gained = {key: count for key, count in gained.items() if not self.assets.item_gain_blocked(data, key)}
        stamp = self.clock()
        boss_trial_kind = int(getattr(settlement, "boss_trial_kind", 0))
        is_npc_isolated_encounter = 9 <= boss_trial_kind <= 11
        is_guild_war = int(settlement.match_mode) in (9, 10)
        is_festival_beast = (
            settlement.HasField("festival_beast") and settlement.festival_beast.activity_instance_id != 0
        ) or boss_trial_kind == BOSS_TRIAL_KIND_FESTIVAL_BEAST

        # 计算全玩法战斗经验/银两与周银两软上限候选账本（与货币入账同一次提交）。
        attributes = getattr(self.scene, "attributes", None)
        reward_rows = (
            attributes.load_battle_progression_rewards()
            if attributes is not None and hasattr(attributes, "load_battle_progression_rewards")
            else self._load_reward_rows()
        )
        silver_rule = (
            attributes.load_weekly_silver_rule()
            if attributes is not None and hasattr(attributes, "load_weekly_silver_rule")
            else self._load_silver_rule()
        )
        week = resolve_game_week(stamp)
        week_start_ms = week[0] if week is not None else 0
        explicit_silver = currencies.get(0, 0)
        explicit_exp = int(getattr(settlement, "exp_gain", 0))
        existing_economy = data.client_request_ledger.battle_economy
        candidate_economy, prog_delta = prepare_economy_ledger(
            existing_economy, week_start_ms, settlement, explicit_exp, explicit_silver, reward_rows, silver_rule,
        )
        if prog_delta.silver_gain > 0:
            currencies[0] = prog_delta.silver_gain
        else:
            currencies.pop(0, None)

        checkpoint = self._checkpoint(data, audits)
        gains = []
        for kind, amount in tuple(currencies.items()):
            # A blocked reward is skipped permanently, so its debt need not
            # be interpreted by this version of the server.
            if self.assets.currency_blocked(data, kind):
                del currencies[kind]
                if kind == 0:
                    candidate_economy.battle_silver_this_week = existing_economy.battle_silver_this_week
                    prog_delta.silver_gain = 0
                    prog_delta.weekly_silver_used_after = int(existing_economy.battle_silver_this_week)
                continue
            if any(debt.currency_type == kind and carries_unknown(debt) for debt in data.currency.debts):
                return False
            error = self.assets.add_currency(data, kind, amount, gains=gains)
            if error == ASSET_BLOCKED:
                del currencies[kind]
                if kind == 0:
                    candidate_economy.battle_silver_this_week = existing_economy.battle_silver_this_week
                    prog_delta.silver_gain = 0
                    prog_delta.weekly_silver_used_after = int(existing_economy.battle_silver_this_week)
            elif error:
                return False
        if prog_delta.ledger_changed:
            data.client_request_ledger.battle_economy.CopyFrom(candidate_economy)
        self._audit(player, checkpoint, data, 8, audits, correlation=settlement.battle_id, credit=True, gains=gains)
        checkpoint = self._checkpoint(data, audits)
        if consumed and self.assets.consume_items(data, consumed, except_hold_id=settlement.battle_id):
            return False
        release_item_hold(data, settlement.battle_id)
        details = {"source": "battle-inbox"}
        self._audit(player, checkpoint, data, 13, audits, correlation=settlement.battle_id, item_extra=details)
        checkpoint = self._checkpoint(data, audits)
        gains = []
        items_mailed = False
        if gained:
            error = await self.assets.add_items(data, gained, overflow=True, gains=gains)
            if error:
                outbox = getattr(self.scene, "mail_outbox", None)
                if error == BAG_FULL and outbox is not None and await outbox.prepare(data, settlement.battle_id, gained):
                    items_mailed = True
                else:
                    if error == BAG_FULL and defer_reasons is not None:
                        defer_reasons.append(BAG_SPACE_DEFERRED)
                    return False
        self._audit(player, checkpoint, data, 20, audits, correlation=settlement.battle_id, item_extra=details, gains=gains)
        isolate_temp_state = (
            is_festival_beast
            or is_npc_isolated_encounter
            or is_guild_war
            or bool(getattr(settlement, "use_free_preset", False))
            or bool(getattr(settlement, "competitive_ranked", False))
            or is_boss_trial_kind(boss_trial_kind)
            or boss_trial_kind == BOSS_TRIAL_KIND_GUARDIAN_RESCUE
            or settlement.HasField("tower_settlement")
        )
        if not isolate_temp_state:
            stats = player_stats(self.protocol, self.tables, data)
            base = data.derived_attributes_component
            base.health = min(settlement.health, stats.max_health)
            base.mana = min(settlement.mana, stats.max_mana) if stats.max_mana else settlement.mana
            if not base.health:
                base.health, base.mana = stats.max_health, stats.max_mana
            if not self._pets(data, settlement.pets):
                return False

        # 经验入账与等级平滑升级（同帧提交，不覆写超上限旧档）。
        if data.HasField("level_component"):
            if attributes is not None and hasattr(attributes, "apply_battle_exp_and_level_up"):
                ok_lvl, lvl_res = attributes.apply_battle_exp_and_level_up(data, prog_delta.exp_gain)
            else:
                ok_lvl, lvl_res = self._apply_exp_fallback(data, prog_delta.exp_gain)
            if not ok_lvl:
                return False
            prog_delta.level_before = lvl_res.level_before
            prog_delta.level_after = lvl_res.level_after
            prog_delta.total_exp_after = lvl_res.total_exp_after
            prog_delta.next_level_total_exp = lvl_res.next_level_total_exp
            prog_delta.leveled_up = lvl_res.leveled_up
            if lvl_res.leveled_up and self.missions is not None:
                checkpoint = self._checkpoint(data, audits)
                lvl_gains = []
                scope = self.missions.scopes(data)[0]
                for reached in range(lvl_res.level_before + 1, lvl_res.level_after + 1):
                    await self.missions._event(data, scope, LEVEL, reached, reached, stamp, gains=lvl_gains)
                self._audit(player, checkpoint, data, 7, audits, gains=lvl_gains)

        if settlement.defeated_monsters and not is_festival_beast and not is_npc_isolated_encounter:
            checkpoint = self._checkpoint(data, audits)
            gains = []
            await self.missions.battle_defeats(data, settlement.defeated_monsters, stamp, gains=gains)
            self._audit(player, checkpoint, data, 7, audits, gains=gains)
        fb_delta = self._apply_festival_beast_credit(data, settlement, stamp)
        if fb_delta is None:
            return False
        travel_pass = getattr(self.scene, "travel_pass", None)
        if not is_npc_isolated_encounter and travel_pass is not None and hasattr(travel_pass, "apply_battle_credit"):
            travel_pass.apply_battle_credit(data, settlement, stamp)
        if not is_npc_isolated_encounter:
            self._apply_progression_hooks(player, data, settlement, stamp, is_festival_beast=is_festival_beast)
        self._record(
            data.battle_results, settlement, currencies, gained, consumed, stamp,
            items_mailed=items_mailed, fb_delta=fb_delta, prog_delta=prog_delta,
        )
        # 离线托管期间的真实战斗结算同帧记入收获简报（不额外发产出）。
        hosting_active = bool(getattr(player, "hosting_active", False) or getattr(player, "disconnect_at", None) is not None)
        if hosting_active:
            won = (
                (settlement.outcome == 1 and settlement.player_team_index == 0)
                or (settlement.outcome == 2 and settlement.player_team_index == 1)
            )
            SceneAfkPass.record_hosted_battle_settlement(
                data,
                hosting_active=True,
                won=won,
                exp_gained=prog_delta.exp_gain,
                silver_gained=currencies.get(0, 0),
                item_count=sum(gained.values()),
                now_unix_sec=stamp // 1000,
            )
        return True

    def _apply_festival_beast_credit(self, data, settlement, stamp: int) -> dict | None:
        if not settlement.HasField("festival_beast") or not settlement.festival_beast.activity_instance_id:
            return {"active": False}
        fb_sys = getattr(self.scene, "festival_beast", None)
        if fb_sys is None:
            fb_sys = SceneFestivalBeast(self.scene, self.tables)
        err, candidate, delta = fb_sys.prepare_battle_credit(data.festival_beast_component, settlement, stamp)
        if err:
            return None
        if delta.get("changed"):
            data.festival_beast_component.CopyFrom(candidate)
        return delta

    def _apply_progression_hooks(
        self, player, data, settlement, stamp: int, *, is_festival_beast: bool = False,
    ) -> None:
        """在战斗终局落盘前，安全推进赛季印、试炼志与装备每日材料自选额度。"""
        if (
            settlement.fled
            or settlement.total_rounds <= 0
            or settlement.player_team_index > 1
            or settlement.outcome not in (1, 2, 3)
        ):
            return
        player_won = (
            (settlement.outcome == 1 and settlement.player_team_index == 0)
            or (settlement.outcome == 2 and settlement.player_team_index == 1)
        )

        if not is_festival_beast:
            season = getattr(self.scene, "season", None)
            if season is not None:
                try:
                    season.record_battle_progress(
                        data,
                        match_mode=settlement.match_mode,
                        player_won=player_won,
                        fled=bool(settlement.fled),
                        total_rounds=settlement.total_rounds,
                        now_ms_val=stamp,
                        competitive_ranked=bool(getattr(settlement, "competitive_ranked", False)),
                        participated=bool(getattr(settlement, "participated", True)),
                        manual_action_rounds=int(getattr(settlement, "manual_action_rounds", 0)),
                        effective_combat_contribution=bool(
                            getattr(settlement, "effective_combat_contribution", False)
                        ),
                    )
                except Exception:
                    logger.warning(
                        "Battle settlement season progress hook failed player_id=%s battle_id=%s",
                        player.player_id, settlement.battle_id, exc_info=True,
                    )
                if settlement.HasField("tower_settlement"):
                    try:
                        season.settle_wuxing_tower_battle(
                            data,
                            settlement,
                            now_ms_val=stamp,
                            player_name=data.profile_component.name,
                        )
                    except Exception:
                        logger.warning(
                            "Battle settlement wuxing tower hook failed player_id=%s battle_id=%s",
                            player.player_id, settlement.battle_id, exc_info=True,
                        )

            trial_journal = getattr(self.scene, "trial_journal", None)
            if trial_journal is not None:
                try:
                    if _has_trial_facts(settlement):
                        trial_journal.record_trial_progress(data, settlement=settlement)
                    elif settlement.battle_config_id:
                        trial_journal.record_trial_progress(
                            data,
                            settlement.battle_config_id,
                            won=player_won,
                            participated=bool(getattr(settlement, "participated", False)),
                            total_rounds=settlement.total_rounds,
                            self_or_pet_ever_died=bool(settlement.is_dead),
                            achieved_at_ms=settlement.finished_at_ms or stamp,
                        )
                except Exception:
                    logger.warning(
                        "Battle settlement trial journal hook failed player_id=%s battle_id=%s",
                        player.player_id, settlement.battle_id, exc_info=True,
                    )

        equip_craft = getattr(self.scene, "equip_craft", None)
        material_eligible = (
            player_won if is_festival_beast else (
                (settlement.match_mode in _PVE_MODES and player_won)
                or settlement.match_mode in _PVP_MODES
            )
        )
        if equip_craft is not None and material_eligible:
            try:
                equip_craft.credit_battle_material_reward(
                    player.player_id,
                    now_ms_val=stamp,
                    eligible=True,
                    data=data,
                )
            except Exception:
                logger.warning(
                    "Battle settlement material reward hook failed player_id=%s battle_id=%s",
                    player.player_id, settlement.battle_id, exc_info=True,
                )

    def _checkpoint(self, data, audits):
        if audits is not None and self.assets.audit_enabled:
            return type(data).FromString(data.SerializeToString())
        return None

    def _audit(self, player, before, data, kind, audits, **options):
        if audits is not None:
            callback = self.assets.make_audit_callback(player, before, data, kind, **options)
            if callback is not None:
                audits.append(callback)

    def currency_version_skew(self, settlement, data) -> bool:
        """Distinguish newer currency/debt fields from malformed reward lines."""
        entries = ([(line.currency_type, line.amount) for line in settlement.currency_gains]
                   if settlement.currency_gains else [(0, settlement.gold_gain)])
        seen = set()
        for currency, amount in entries:
            if currency in seen or amount > MAX_CURRENCY_AMOUNT:
                return False
            seen.add(currency)
        if any(currency not in self.currency_types for currency, _ in entries):
            return True
        return any(amount and debt.currency_type == currency and carries_unknown(debt)
                   for currency, amount in entries for debt in data.currency.debts)

    def _currencies(self, settlement):
        entries = ([(line.currency_type, line.amount) for line in settlement.currency_gains]
                   if settlement.currency_gains else [(0, settlement.gold_gain)])
        result = {}
        for currency, amount in entries:
            if currency in result or currency not in self.currency_types or amount > MAX_CURRENCY_AMOUNT:
                return None
            result[currency] = amount
        return {currency: amount for currency, amount in result.items() if amount}

    def _items(self, entries):
        result = Counter()
        for entry in entries:
            item = entry.item_table_id
            if not item or not entry.count:
                return None
            result[item] += entry.count
            if result[item] > MAX_ITEM_COUNT:
                return None
        for item in list(result):
            row = self.tables["Item"].get(item)
            if row is None:
                logger.warning("战斗物品配表尚不可用，延后结算 config_id=%s", item)
                return None
            elif carries_unknown(row):
                return None
        return result

    def _owned(self, data, consumed):
        owned = Counter()
        for entry in data.bag_component.items:
            if entry.bag_type:
                continue
            instance = entry.instance if entry.HasField("instance") else None
            config = instance.config_id if instance is not None else entry.config_id
            if config not in consumed:
                continue
            if instance is None:
                if not entry.item_uuid or not entry.stack_size or entry.HasField("equip"):
                    return None
                owned[config] += entry.stack_size
            else:
                if (not instance.item_id or not instance.size or carries_unknown(instance)
                        or not self.assets.countable(instance)):
                    return None
                owned[config] += instance.size
        return owned

    def _pets(self, data, effects) -> bool:
        if not effects:
            return True
        if carries_unknown(data.pet_component):
            return False
        owned = {pet.pet_id: pet for pet in data.pet_component.pets}
        if 0 in owned or len(owned) != len(data.pet_component.pets):
            return False
        seen = set()
        for effect in effects:
            if not effect.pet_id or effect.pet_id in seen:
                return False
            seen.add(effect.pet_id)
            pet = owned.get(effect.pet_id)
            if pet is None:
                continue
            row = self.tables["Pet"].get(pet.pet_table_id)
            if row is None or carries_unknown(row):
                return False
            pool = pet_pool(self.tables)
            if pool is None:
                continue
            if carries_unknown(pool):
                return False
            stats = pet_stats(self.tables, pet)
            if stats is None:
                continue
            pet.health = min(effect.health, stats["max_health"])
            pet.mana = min(effect.mana, stats["max_mana"]) if stats["max_mana"] else effect.mana
            if effect.is_dead or not pet.health:
                pet.health, pet.mana = stats["max_health"], stats["max_mana"]
        return True

    def _load_reward_rows(self) -> list[ModeRewardRow]:
        if "BattleProgressionReward" not in self.tables.tables:
            return []
        return [
            ModeRewardRow(
                match_mode=int(row.match_mode),
                difficulty=int(row.difficulty),
                outcome_tier=int(row.outcome_tier),
                exp_gain=int(row.exp_gain),
                silver_gain=int(row.silver_gain),
            )
            for row in self.tables["BattleProgressionReward"].rows()
        ]

    def _load_silver_rule(self) -> WeeklySilverRule | None:
        if "BattleProgressionRule" not in self.tables.tables:
            return None
        row = self.tables["BattleProgressionRule"].get(1)
        if row is None:
            return None
        return WeeklySilverRule(
            soft_cap=int(row.weekly_silver_soft_cap),
            diminished_rate_permyriad=int(row.diminished_rate_permyriad),
            hard_cap=int(row.weekly_silver_hard_cap),
        )

    def _apply_exp_fallback(self, data, exp_gain: int):
        rows = []
        if "PlayerLevelExp" in self.tables.tables:
            rows = sorted(
                (LevelExpRow(level=int(r.id), required_total_exp=int(r.required_total_exp))
                 for r in self.tables["PlayerLevelExp"].rows()),
                key=lambda r: r.level,
            )
        ok, res = resolve_level_for_exp(int(data.level_component.level), int(data.level_component.total_exp), exp_gain, rows)
        if ok:
            if exp_gain > 0:
                data.level_component.total_exp = res.total_exp_after
            if res.leveled_up:
                data.level_component.level = res.stored_level_after
        return ok, res

    @staticmethod
    def _record(
        history, settlement, currencies, gained, consumed, stamp, *,
        items_mailed=False, fb_delta=None, prog_delta: BattleProgressionDelta | None = None,
    ):
        seq = max(history.last_result_seq, max((entry.result_seq for entry in history.entries), default=0)) + 1
        history.last_result_seq = seq
        entry = history.entries.add(
            result_seq=seq, battle_id=settlement.battle_id, outcome=settlement.outcome,
            team_index=settlement.player_team_index, is_dead=settlement.is_dead, fled=settlement.fled,
            total_rounds=settlement.total_rounds, finished_at_ms=settlement.finished_at_ms, settled_at_ms=stamp,
            lines_truncated=len(gained) > MAX_LINES or len(consumed) > MAX_LINES,
            match_mode=settlement.match_mode, battle_config_id=settlement.battle_config_id, items_mailed=items_mailed,
        )
        if prog_delta is not None:
            entry.exp_gain = int(prog_delta.exp_gain)
            entry.level_before = int(prog_delta.level_before)
            entry.level_after = int(prog_delta.level_after)
            entry.total_exp_after = int(prog_delta.total_exp_after)
            entry.next_level_total_exp = int(prog_delta.next_level_total_exp)
            entry.battle_silver_weekly_used = int(prog_delta.weekly_silver_used_after)
            entry.battle_silver_weekly_soft_cap = int(prog_delta.weekly_silver_soft_cap)
            entry.battle_silver_diminished = bool(prog_delta.silver_diminished)
        if fb_delta and fb_delta.get("active"):
            entry.festival_activity_instance_id = int(fb_delta.get("activity_instance_id", 0))
            entry.festival_reward_identity = str(fb_delta.get("reward_identity", ""))
            entry.festival_wins_before = int(fb_delta.get("wins_before", 0))
            entry.festival_wins_after = int(fb_delta.get("wins_after", 0))
            entry.festival_required_wins = int(fb_delta.get("required_wins", 0))
            entry.festival_newly_eligible = bool(fb_delta.get("newly_eligible", False))
        for kind, amount in sorted(currencies.items()):
            entry.currencies.add(currency_type=kind, amount=amount)
        for items, lines in ((gained, entry.items_gained), (consumed, entry.items_consumed)):
            for kind, count in sorted(items.items())[:MAX_LINES]:
                lines.add(item_table_id=kind, count=count)
        while len(history.entries) > MAX_HISTORY:
            oldest = min(range(len(history.entries)), key=lambda index: history.entries[index].result_seq)
            del history.entries[oldest]

    async def complete(self, player, settlement) -> None:
        if self.scene.battle and not await self.scene.battle.settled(player.player_id, settlement.battle_id):
            return
        data = self.scene.rows.assemble(player.snapshot)
        result = next((entry for entry in data.battle_results.entries
                       if entry.battle_id == settlement.battle_id), None)
        leveled_up = bool(result is not None and result.level_after > result.level_before)
        pets = getattr(self.scene, "pets", None)
        if (settlement.pets or leveled_up) and pets is not None:
            pets.notify(player)
        attributes = getattr(self.scene, "attributes", None)
        if leveled_up and attributes is not None:
            attributes.notify(player)
        end_msg = self.protocol.message(
            "BattleEndS2C", battle_id=settlement.battle_id, outcome=settlement.outcome, settlement=settlement,
            items_mailed=bool(result and result.items_mailed),
        )
        if result is not None:
            end_msg.level_before = int(result.level_before)
            end_msg.level_after = int(result.level_after)
            end_msg.exp_gain = int(result.exp_gain)
            end_msg.total_exp_after = int(result.total_exp_after)
            end_msg.next_level_total_exp = int(result.next_level_total_exp)
            end_msg.battle_silver_diminished = bool(result.battle_silver_diminished)
            for cur in result.currencies:
                if cur.currency_type == 0:
                    end_msg.silver_gain = int(cur.amount)
                    break
            if result.festival_activity_instance_id:
                fb_summary = end_msg.festival_beast_settlement
                fb_summary.activity_instance_id = result.festival_activity_instance_id
                fb_summary.reward_identity = result.festival_reward_identity
                fb_summary.wins_before = result.festival_wins_before
                fb_summary.wins_after = result.festival_wins_after
                fb_summary.required_wins = result.festival_required_wins
                fb_summary.newly_eligible = result.festival_newly_eligible
                fb_summary.material_units_added = result.material_credit_units_added
                fb_summary.material_tradable_units_added = result.material_tradable_units_added
                fb_summary.material_available_units_after = result.material_available_units_after
                fb_summary.material_earnable_remaining_after = result.material_earnable_remaining_after
        self.scene._push(player, "BattleClientPlayerNotifyBattleEnd", end_msg)
