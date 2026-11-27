"""Scene 备战快照、战斗冻结和按局号围栏的 Redis 锁。"""

import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass

import grpc
from google.protobuf.message import DecodeError

from ..data import PlayerRowKind, StoreRejected, StoreUnavailable
from ..data.asset_inventory import AssetInventory, release_item_hold
from .battle_control import service_digest, sign_service_call, verify_service_call
from .battle_dropin_effects import (
    BOSS_TRIAL_KIND_FESTIVAL_BEAST,
    BOSS_TRIAL_KIND_GUARDIAN_RESCUE,
    MATCH_MODE_PVP_CHALLENGE,
    WUXING_TOWER_MAX_FLOORS,
    is_boss_trial_kind,
    is_valid_custom_duel_round_limit,
    is_valid_formation_id,
    tower_floor_dungeon_id,
    tower_floor_leyline_element,
    wuxing_week_key,
)
from .battle_tables import battle_table_fingerprint
from .scene_festival_beast import FESTIVAL_BEAST_PET_CHANGED, FESTIVAL_BEAST_PET_DEAD, SceneFestivalBeast
from .scene_jianghu import (
    MATCH_MODE_JIANGHU_DUEL,
    TX_JIANGHU_DUEL_FEE,
    TX_JIANGHU_DUEL_REFUND,
    SceneJianghu,
)
from .scene_mission import effective_level
from .scene_pets import build_single_pet_battle_snapshot, build_standby_battle_snapshots
from .scene_season import find_active_season_rule, valid_seal_row
from .scene_stats import battle_skills, class_id, player_stats

logger = logging.getLogger(__name__)
TERMINAL = "/match.MatchInternal/GetBattleTerminalDecision"
_MATCH_MODE_SEASON_TOURNAMENT = 11
_MAX_STANDBY_PETS = 5
_SETTLEMENT_GRACE_MS = 120_000  # Match 终局宽限和收件箱投递余量各 60 秒。
_HOLD_CAP_MS = 15 * 60_000
_LOCK_EXTRA_SECONDS = 60
_REAPER_INTERVAL_MS = 30_000
_UINT64_MAX = (1 << 64) - 1
_TIDE_CANDIDATES = (2, 3, 5)
_TIDE_INTERVAL_SEC = 3600
_TIDE_DURATION_SEC = 2400
_GUILD_WAR_MODES = frozenset({9, 10})


def is_world_tide_active_on_scene(now_unix_sec: int, scene_config_id: int) -> bool:
    """判定指定野外地图当前是否处于灵潮珍兽潮激活窗口（默认轮换 [2,3,5]/3600s/2400s）。"""
    # shortcut: uses default tide schedule [2,3,5]/3600s/2400s, read WorldMapOverview snapshot if ops customizes tide schedule
    if not scene_config_id or (now_unix_sec % _TIDE_INTERVAL_SEC) >= _TIDE_DURATION_SEC:
        return False
    round_index = now_unix_sec // _TIDE_INTERVAL_SEC
    return _TIDE_CANDIDATES[round_index % len(_TIDE_CANDIDATES)] == scene_config_id


def is_npc_isolated_encounter_kind(boss_trial_kind: int) -> bool:
    """归元城原创 NPC 场景玩法（演武试招木桩=9、回合残局=10、人物奇谈=11）。"""
    return 9 <= int(boss_trial_kind) <= 11


def is_practice_dummy_kind(boss_trial_kind: int) -> bool:
    """归元城演武试招木桩（boss_trial_kind=9，禁用免费预设）。"""
    return int(boss_trial_kind) == 9

_WRITE = """
local current = redis.call('GET', KEYS[1])
if current and current ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[2])
return 1
"""
_RENEW = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local ttl = math.max(redis.call('TTL', KEYS[1]), tonumber(ARGV[2]))
redis.call('SET', KEYS[2], ARGV[3], 'EX', ttl)
redis.call('EXPIRE', KEYS[1], ttl)
return 1
"""
_REMOVE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] ~= '' and redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
return redis.call('DEL', KEYS[1], KEYS[2])
"""
_READ = """
local lock = redis.call('GET', KEYS[1])
if not lock then return nil end
return {lock, redis.call('GET', KEYS[2]), redis.call('TTL', KEYS[1])}
"""


@dataclass(slots=True)
class Freeze:
    context: object
    session_id: int
    retry_at: int = 0
    cap_scheduled: bool = False

    @property
    def due(self) -> int:
        if self.context.state == 1:
            return self.context.prepare_deadline_ms or self.context.deadline_ms
        deadline = self.context.deadline_ms
        return min(_UINT64_MAX, deadline + _SETTLEMENT_GRACE_MS) if deadline else 0

    @property
    def hold_until(self) -> int:
        if self.context.state == 1 or not self.due:
            return self.due
        return min(_UINT64_MAX, self.due + _HOLD_CAP_MS)


class SceneBattle:
    def __init__(self, scene, tables, *, now_ms=None, redis=None, match_call=None, terminal_secret=b"") -> None:
        self.scene = scene
        self.protocol = scene.protocol
        self.tables = tables
        self._clock = now_ms or (lambda: time.time_ns() // 1_000_000)
        self._freezes: dict[int, Freeze] = {}
        self._redis = redis
        self._match_call = match_call
        self._terminal_secret = terminal_secret
        self._reaping: dict[int, asyncio.Task] = {}
        self._hold_reviews: dict[int, asyncio.Task] = {}
        self._hold_review_at: dict[int, int] = {}
        self._hold_checked_seq: dict[int, int] = {}
        self._inventory = AssetInventory(self.protocol, tables)

    def current(self, player_id: int) -> int:
        entry = self._freezes.get(player_id)
        if entry is None:
            return 0
        if self._redis is None and entry.due and entry.due < self._clock():
            self._thaw(player_id)
            return 0
        return entry.context.battle_id

    def frozen(self, player_id: int) -> bool:
        return bool(self.current(player_id))

    def reap(self) -> None:
        now = self._clock()
        for player_id, entry in tuple(self._freezes.items()):
            if not entry.due or entry.due >= now:
                continue
            if self._redis is None:
                self._thaw(player_id)
                continue
            past_cap = entry.context.state != 1 and entry.hold_until < now
            task = self._reaping.get(player_id)
            if task is not None:
                if not past_cap or entry.cap_scheduled:
                    continue
                task.cancel()
            if past_cap or (entry.retry_at <= now and len(self._reaping) < 16):
                entry.cap_scheduled = past_cap
                self._reaping[player_id] = asyncio.create_task(self._expire(player_id, entry))
        for world in tuple(getattr(self.scene, "worlds", {}).values()):
            for player in tuple(world.players.values()):
                player_id = player.player_id
                if (player_id in self._hold_reviews or self._hold_review_at.get(player_id, 0) > now
                        or len(self._hold_reviews) >= 16 or not self.scene.mutable(player)):
                    continue
                snapshot = player.snapshot
                save_seq = getattr(snapshot, "save_seq", None)
                if save_seq is not None and self._hold_checked_seq.get(player_id) == save_seq:
                    continue
                if hasattr(snapshot, "rows"):
                    raw = snapshot.rows.get(PlayerRowKind.PROFILE, b"")
                    profile_db = self.protocol.parse("player_database", raw) if raw else None
                    holds = profile_db.item_holds.holds if profile_db is not None else ()
                    jh_comp = profile_db.jianghu_component if profile_db is not None else None
                else:
                    assembled = self.scene.rows.assemble(snapshot)
                    holds = assembled.item_holds.holds
                    jh_comp = assembled.jianghu_component
                battle_holds = [hold for hold in holds if hold.kind == 1 and hold.hold_id]
                pending_fee_bid = int(getattr(jh_comp, "pending_silver_fee_battle_id", 0)) if jh_comp is not None else 0
                pending_fee_review = (
                    int(getattr(jh_comp, "pending_silver_fee_review_after_ms", 0)) if jh_comp is not None else 0
                )
                if not battle_holds and not pending_fee_bid:
                    if save_seq is not None:
                        self._hold_checked_seq[player_id] = save_seq
                    continue
                self._hold_checked_seq.pop(player_id, None)
                cur_bid = self.current(player_id)
                hold_due = any(hold.review_after_ms <= now and hold.hold_id != cur_bid for hold in battle_holds)
                fee_due = bool(pending_fee_bid and pending_fee_bid != cur_bid and pending_fee_review <= now)
                if hold_due or fee_due:
                    self._hold_review_at[player_id] = now + _REAPER_INTERVAL_MS
                    self._hold_reviews[player_id] = asyncio.create_task(self._review_holds(player_id))

    async def _review_holds(self, player_id: int) -> None:
        """统一复核战斗道具预留与普通生死状待退银两（对齐 C++ ReviewBattleHoldsAndFees）。"""
        try:
            player = self._player(player_id)
            if player is None:
                return
            now = self._clock()
            cur_bid = self.current(player_id)
            data = self.scene.rows.assemble(player.snapshot)
            hold_bids = {
                int(hold.hold_id)
                for hold in data.item_holds.holds
                if hold.kind == 1 and hold.hold_id and hold.review_after_ms <= now and hold.hold_id != cur_bid
            }
            fee_bid = int(data.jianghu_component.pending_silver_fee_battle_id)
            fee_review = int(data.jianghu_component.pending_silver_fee_review_after_ms)
            fee_due_bid = fee_bid if (fee_bid and fee_bid != cur_bid and fee_review <= now) else 0
            candidates = sorted(hold_bids | ({fee_due_bid} if fee_due_bid else set()))
            jianghu_sys = (
                getattr(self.scene, "jianghu", None)
                or SceneJianghu(self.scene, self.tables, getattr(self.scene, "assets", None), clock=self._clock)
            )
            for battle_id in candidates:
                has_hold = battle_id in hold_bids
                has_fee = battle_id == fee_due_bid
                applied_here = self._applied(player, battle_id)
                # 仅道具预留且本地已应用结算：直接释放预留；待退银两必须查 SQL 终局（防作废结算退款失败被误吞）。
                if has_hold and not has_fee and applied_here:
                    verdict = (2, False)
                else:
                    verdict = await self._decision(battle_id)
                releasable = verdict in ((2, False), (0, False))
                consume_stale_fee = verdict == (1, False) and applied_here and has_fee
                if not releasable and not consume_stale_fee:
                    continue
                async with self.scene._lock(player_id):
                    if self._player(player_id) is not player or not self.scene.mutable(player) or self.current(player_id) == battle_id:
                        continue
                    await self.scene.flush_player_rows(player)
                    current = self.scene.rows.assemble(player.snapshot)
                    assets = getattr(self.scene, "assets", None)
                    before = (
                        type(current).FromString(current.SerializeToString())
                        if (assets is not None and assets.audit_enabled)
                        else None
                    )
                    changed = False
                    refunded = 0
                    if releasable:
                        if release_item_hold(current, battle_id):
                            changed = True
                        if int(current.jianghu_component.pending_silver_fee_battle_id) == battle_id:
                            j_changed, refunded = jianghu_sys.on_battle_prepare_cancelled(
                                current, battle_id, skip_silver_refund=False,
                            )
                            if j_changed:
                                current.jianghu_component.pending_silver_fee_review_after_ms = 0
                                changed = True
                    elif consume_stale_fee and int(current.jianghu_component.pending_silver_fee_battle_id) == battle_id:
                        current.jianghu_component.pending_silver_fee_battle_id = 0
                        current.jianghu_component.pending_silver_fee_amount = 0
                        current.jianghu_component.pending_silver_fee_review_after_ms = 0
                        changed = True
                    if changed:
                        audit_cb = (
                            assets.make_audit_callback(
                                player, before, current, TX_JIANGHU_DUEL_REFUND, correlation=battle_id,
                            )
                            if (before is not None and refunded > 0)
                            else None
                        )
                        if audit_cb is not None:
                            await self.scene.commit_player_data(player, current, on_committed=audit_cb)
                        else:
                            await self.scene.commit_player_data(player, current)
        except (StoreRejected, StoreUnavailable, OSError, ValueError):
            logger.warning("Battle hold and silver fee review pending retry player_id=%s", player_id, exc_info=True)
        finally:
            if self._hold_reviews.get(player_id) is asyncio.current_task():
                self._hold_reviews.pop(player_id, None)

    async def _expire(self, player_id, entry) -> None:
        try:
            context = entry.context
            async with self.scene._lock(player_id):
                if self._freezes.get(player_id) is not entry or entry.due >= self._clock():
                    return
                if context.state == 1:
                    # 迟到的确认仍可凭匹配的 Redis 锁恢复正式战斗。
                    self._thaw(player_id)
                    return
                player = self._player(player_id)
                if self._applied(player, context.battle_id) or self._past_cap(player_id, entry):
                    await self._release_expired(player_id, entry, player)
                    return
            verdict = await self._decision(context.battle_id)
            async with self.scene._lock(player_id):
                if self._freezes.get(player_id) is not entry or entry.due >= self._clock():
                    return
                player = self._player(player_id)
                if (verdict in ((2, False), (0, False)) or self._applied(player, context.battle_id)
                        or self._past_cap(player_id, entry)):
                    await self._release_expired(player_id, entry, player)
                    # ABORT / 无可裁决原件：不会再有结算，一并释放道具预留并退还待退银两。
                    if (
                        verdict in ((2, False), (0, False))
                        and player is not None
                        and self.scene.mutable(player)
                    ):
                        await self._release_hold_and_fee_on_abort(player, context.battle_id)
                else:
                    entry.retry_at = self._clock() + _REAPER_INTERVAL_MS
                    await self._store(_RENEW, player_id, entry)
        except Exception:
            entry.retry_at = self._clock() + _REAPER_INTERVAL_MS
            logger.warning("战斗终局未能确认，保留冻结 player_id=%s", player_id, exc_info=True)
        finally:
            if self._reaping.get(player_id) is asyncio.current_task():
                self._reaping.pop(player_id, None)

    def _player(self, player_id):
        world = self.scene.locations.get(player_id)
        return world.players.get(player_id) if world is not None else None

    def _applied(self, player, battle_id) -> bool:
        if player is None:
            return False
        data = self.scene.rows.assemble(player.snapshot)
        return any(
            item.request_id == battle_id and item.msg_type == 4 and item.outcome == 1
            for cursor in data.inbox_ledger.cursors if cursor.domain == 2
            for item in cursor.recent
        )

    def _past_cap(self, player_id, entry) -> bool:
        if self._clock() <= entry.hold_until:
            return False
        logger.error("战斗冻结达到续期上限 player_id=%s battle_id=%s", player_id, entry.context.battle_id)
        return True

    async def _release_expired(self, player_id, entry, player) -> None:
        battle_id = entry.context.battle_id
        self._thaw(player_id)
        if player is not None:
            player.battle_expired_id = battle_id
        await self._remove_lock(player_id, battle_id)

    async def _release_hold_and_fee_on_abort(self, player, battle_id: int) -> None:
        try:
            await self.scene.flush_player_rows(player)
            data = self.scene.rows.assemble(player.snapshot)
            assets = getattr(self.scene, "assets", None)
            before = (
                type(data).FromString(data.SerializeToString())
                if (assets is not None and assets.audit_enabled)
                else None
            )
            changed = bool(release_item_hold(data, battle_id))
            refunded = 0
            if int(data.jianghu_component.pending_silver_fee_battle_id) == battle_id:
                jianghu_sys = (
                    getattr(self.scene, "jianghu", None)
                    or SceneJianghu(self.scene, self.tables, assets, clock=self._clock)
                )
                j_changed, refunded = jianghu_sys.on_battle_prepare_cancelled(
                    data, battle_id, skip_silver_refund=False,
                )
                if j_changed:
                    data.jianghu_component.pending_silver_fee_review_after_ms = 0
                    changed = True
            if changed:
                audit_cb = (
                    assets.make_audit_callback(
                        player, before, data, TX_JIANGHU_DUEL_REFUND, correlation=battle_id,
                    )
                    if (before is not None and refunded > 0)
                    else None
                )
                if audit_cb is not None:
                    await self.scene.commit_player_data(player, data, on_committed=audit_cb)
                else:
                    await self.scene.commit_player_data(player, data)
        except (StoreRejected, StoreUnavailable, OSError, ValueError):
            logger.warning(
                "Abort battle hold/fee cleanup deferred to review player_id=%s battle_id=%s",
                player.player_id, battle_id, exc_info=True,
            )

    def _thaw(self, player_id: int) -> None:
        if self._freezes.pop(player_id, None) is None:
            return
        if (player := self._player(player_id)) is not None:
            if (landing := getattr(self.scene, "landing", None)) is not None:
                landing.wake(player, coalesce=False)
            if (team := getattr(self.scene, "team", None)) is not None:
                team.refresh(player)

    async def _remove_lock(self, player_id, battle_id) -> None:
        try:
            await self._eval(_REMOVE, player_id, str(battle_id), b"")
        except Exception:
            logger.warning("战斗锁条件删除待重试 player_id=%s battle_id=%s", player_id, battle_id, exc_info=True)

    async def _decision(self, battle_id: int):
        if not self._match_call or not self._terminal_secret:
            return None
        request = self.protocol.message("match.GetBattleTerminalDecisionRequest", battle_id=battle_id)
        digest = service_digest(request)
        sign_service_call(TERMINAL, request, "scene", self._clock(), self._terminal_secret)
        try:
            async with asyncio.timeout(6):
                data = await self._match_call(TERMINAL, request.SerializeToString(deterministic=True))
            reply = self.protocol.parse("match.GetBattleTerminalDecisionResponse", data)
        except (grpc.RpcError, DecodeError, TimeoutError):
            return None
        if reply.retry or reply.request_digest != digest or not verify_service_call(
            TERMINAL + "/reply", reply, "match", self._clock(), self._terminal_secret,
        ):
            return None
        return reply.decision, reply.terminal_expected if reply.decision == 0 else False

    async def _eval(self, script, player_id, *args):
        if self._redis is None:
            return 1
        return await self._redis.eval(script, 2, f"battle:lock:{{{player_id}}}", f"battle:ctx:{{{player_id}}}", *args)

    async def _store(self, script, player_id, entry):
        ttl = max(0, entry.hold_until - self._clock()) // 1000 + _LOCK_EXTRA_SECONDS
        return await self._eval(script, player_id, str(entry.context.battle_id), ttl,
                                entry.context.SerializeToString(deterministic=True))

    async def _read(self, player_id, session_id=0):
        if self._redis is None:
            return None
        result = await self._eval(_READ, player_id)
        if not result:
            return None
        battle_id, raw, ttl = result
        battle_id = int(battle_id)
        try:
            context = self.protocol.parse("InBattleComp", raw or b"")
        except DecodeError:
            context = self.protocol.message("InBattleComp")
        if context.battle_id != battle_id:
            context.Clear()
            context.battle_id = battle_id
        if not context.state:
            context.state = 2
        if not context.deadline_ms:
            remaining = max(0, ttl) * 1000
            expires = min(_UINT64_MAX, self._clock() + remaining)
            after_grace = _HOLD_CAP_MS + _LOCK_EXTRA_SECONDS * 1000
            context.deadline_ms = (
                max(0, expires - after_grace - _SETTLEMENT_GRACE_MS) + 1500
                if remaining > after_grace else expires
            )
        return Freeze(context, session_id)

    async def restore(self, player, *, notify=True) -> None:
        entry = self._freezes.get(player.player_id) or await self._read(player.player_id, player.session_id)
        if entry is not None:
            if getattr(player, "battle_expired_id", 0) == entry.context.battle_id:
                await self._remove_lock(player.player_id, entry.context.battle_id)
                return
            self._freezes[player.player_id] = entry
            if notify:
                self.notify_reconnect(player)

    def departed(self, player) -> None:
        self._freezes.pop(player.player_id, None)
        if (task := self._reaping.pop(player.player_id, None)) is not None:
            task.cancel()
        if (task := self._hold_reviews.pop(player.player_id, None)) is not None:
            task.cancel()
        self._hold_review_at.pop(player.player_id, None)
        self._hold_checked_seq.pop(player.player_id, None)

    def notify_reconnect(self, player) -> None:
        entry = self._freezes.get(player.player_id)
        if entry is not None and entry.context.state == 2:
            self.scene._push(player, "BattleClientPlayerNotifyBattleReconnect", self.protocol.message(
                "BattleReconnectS2C", battle_id=entry.context.battle_id,
            ))

    async def prepare(self, request):
        response = self.protocol.message("PrepareBattleResponse")
        player_id = request.player_id
        deadline = request.prepare_deadline_ms or request.deadline_ms
        if not all((player_id, request.battle_id, request.deadline_ms)) or deadline <= self._clock():
            response.error_message.id = 1005
            return response
        async with self.scene._lock(player_id):
            world = self.scene.locations.get(player_id)
            player = world.players.get(player_id) if world is not None else None
            if player is None:
                error = 1004
            elif self.frozen(player_id) or not self.scene.mutable(player):
                error = 1006
            elif player.disconnect_at is not None or not player.gate_uuid or not player.session_id >> 17:
                error = 1011
            else:
                try:
                    await self.scene.flush_player_rows(player)
                    error = self._snapshot(player, response.snapshot, request=request)
                except (StoreRejected, StoreUnavailable):
                    error = 1003
                if not error and deadline <= self._clock():
                    error = 1005
                held = False
                silver_charged = False
                is_jianghu_duel = int(getattr(request, "match_mode", 0)) == MATCH_MODE_JIANGHU_DUEL
                has_jianghu_fee = (
                    is_jianghu_duel
                    and "jianghu_duel_fee" in request.DESCRIPTOR.fields_by_name
                    and request.HasField("jianghu_duel_fee")
                )
                jianghu_sys = (
                    getattr(self.scene, "jianghu", None)
                    or SceneJianghu(self.scene, self.tables, getattr(self.scene, "assets", None), clock=self._clock)
                )
                if not error and (response.snapshot.items or has_jianghu_fee):
                    data = self.scene.rows.assemble(player.snapshot)
                    assets = getattr(self.scene, "assets", None)
                    before_fee = (
                        type(data).FromString(data.SerializeToString())
                        if (has_jianghu_fee and assets is not None and assets.audit_enabled)
                        else None
                    )
                    counts = {item.item_table_id: item.count for item in response.snapshot.items}
                    jianghu_silver_to_deduct = 0
                    if has_jianghu_fee:
                        fee_err, jianghu_silver_to_deduct = jianghu_sys.validate_and_collect_prepare_fee(
                            data, request.battle_id, request.jianghu_duel_fee, counts,
                        )
                        if fee_err:
                            error = fee_err
                    hold_and_fee_review_after_ms = min(_UINT64_MAX, request.deadline_ms + _SETTLEMENT_GRACE_MS)
                    if not error and counts:
                        hold_err = self._inventory.place_item_hold(
                            data, request.battle_id, counts, created_at_ms=self._clock(),
                            review_after_ms=hold_and_fee_review_after_ms,
                        )
                        if hold_err:
                            error = 1006
                    if not error and jianghu_silver_to_deduct > 0:
                        debit_err = jianghu_sys.commit_prepare_silver_fee(
                            data, request.battle_id, jianghu_silver_to_deduct,
                        )
                        if debit_err:
                            error = debit_err
                        else:
                            data.jianghu_component.pending_silver_fee_review_after_ms = hold_and_fee_review_after_ms
                    if not error and (counts or jianghu_silver_to_deduct > 0):
                        audit_cb = (
                            assets.make_audit_callback(
                                player, before_fee, data, TX_JIANGHU_DUEL_FEE, correlation=request.battle_id,
                            )
                            if (before_fee is not None and jianghu_silver_to_deduct > 0)
                            else None
                        )
                        try:
                            if audit_cb is not None:
                                await self.scene.commit_player_data(player, data, on_committed=audit_cb)
                            else:
                                await self.scene.commit_player_data(player, data)
                        except (StoreRejected, StoreUnavailable):
                            error = 1003
                        else:
                            held = bool(counts)
                            silver_charged = jianghu_silver_to_deduct > 0
                            if silver_charged:
                                response.jianghu_silver_fee_charged = True
                            # The commit yields to the event loop. A deadline that was
                            # valid before the write may have passed while it was in flight.
                            if deadline <= self._clock():
                                error = 1005
                if not error:
                    context = self.protocol.message(
                        "InBattleComp", battle_id=request.battle_id, battle_node_id=request.battle_node_id,
                        deadline_ms=request.deadline_ms, prepare_deadline_ms=deadline, state=1,
                    )
                    entry = Freeze(context, player.session_id)
                    try:
                        wrote = await self._store(_WRITE, player_id, entry)
                    except Exception:
                        logger.warning("备战 Redis 锁未确认 player_id=%s", player_id, exc_info=True)
                        wrote = False
                    if not wrote:
                        error = 1003
                    else:
                        self._freezes[player_id] = entry
                        player.motion.velocity = (0., 0., 0.)
                if error and (held or silver_charged):
                    response.jianghu_silver_fee_charged = False
                    try:
                        data = self.scene.rows.assemble(player.snapshot)
                        changed = bool(release_item_hold(data, request.battle_id)) if held else False
                        if silver_charged:
                            j_changed, _ = jianghu_sys.on_battle_prepare_cancelled(
                                data, request.battle_id, skip_silver_refund=False,
                            )
                            changed = changed or j_changed
                        if changed:
                            await self.scene.commit_player_data(player, data)
                    except (StoreRejected, StoreUnavailable):
                        # Keep an uncertain hold for the regular terminal review.
                        logger.warning("失败备战预留释放待复核 player_id=%s battle_id=%s",
                                       player_id, request.battle_id, exc_info=True)
            if error:
                response.ClearField("snapshot")
                response.error_message.id = error
            else:
                response.table_fingerprint = response.snapshot.table_fingerprint
        return response

    def _snapshot(self, player, snapshot, *, request=None) -> int:
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("derived_attributes_component") or not data.HasField("level_component"):
            return 1003

        dungeon_row = (
            self.tables["Dungeon"].get(request.battle_config_id)
            if request is not None and request.battle_config_id and "Dungeon" in self.tables.tables
            else None
        )
        boss_trial_kind = int(getattr(dungeon_row, "boss_trial_kind", 0)) if dungeon_row is not None else 0
        is_boss_trial = is_boss_trial_kind(boss_trial_kind)
        is_guard_rescue = boss_trial_kind == BOSS_TRIAL_KIND_GUARDIAN_RESCUE
        is_npc_isolated_encounter = is_npc_isolated_encounter_kind(boss_trial_kind)
        is_guild_war = bool(
            request is not None and int(getattr(request, "match_mode", 0)) in _GUILD_WAR_MODES
        )
        is_festival_beast = bool(
            request is not None
            and request.HasField("activity_context")
            and request.activity_context.kind == 2
            and request.activity_context.HasField("festival_beast")
        )
        is_formal_ranked = bool(
            request is not None
            and request.HasField("ranked_context")
            and request.ranked_context.season_id != 0
        )
        use_free_preset = bool(
            request is not None
            and request.HasField("ranked_context")
            and request.ranked_context.use_free_preset
        )
        is_wuxing_tower = bool(request is not None and getattr(request, "is_wuxing_tower", False))
        has_custom_duel = bool(
            request is not None
            and "custom_duel_rules" in request.DESCRIPTOR.fields_by_name
            and request.HasField("custom_duel_rules")
        )
        is_pvp_challenge = bool(
            request is not None and int(getattr(request, "match_mode", 0)) == MATCH_MODE_PVP_CHALLENGE
        )
        is_jianghu_duel = bool(
            request is not None and int(getattr(request, "match_mode", 0)) == MATCH_MODE_JIANGHU_DUEL
        )
        if has_custom_duel:
            if not is_valid_custom_duel_round_limit(request.custom_duel_rules.round_limit_override):
                return 1005
            if request.custom_duel_rules.use_free_preset:
                use_free_preset = True

        now_stamp = self._clock()
        world = self.scene.locations.get(player.player_id)
        current_scene_config_id = int(getattr(world, "config_id", 0)) if world is not None else 0

        # 野外灵宠栖息地场景与灵潮珍兽潮窗口权威校验。
        if dungeon_row is not None and int(getattr(dungeon_row, "allow_pet_capture", 0)) == 1 and int(getattr(dungeon_row, "scene_id", 0)) > 1:
            req_scene = int(dungeon_row.scene_id)
            if current_scene_config_id != req_scene:
                logger.warning(
                    "PrepareBattle rejected: wild pet habitat scene mismatch player_id=%s battle_config_id=%s required=%s current=%s",
                    player.player_id, request.battle_config_id, req_scene, current_scene_config_id,
                )
                return 1006
            if 210 <= int(dungeon_row.id) <= 219 and not is_world_tide_active_on_scene(now_stamp // 1000, req_scene):
                logger.warning(
                    "PrepareBattle rejected: wild pet tide habitat inactive player_id=%s battle_config_id=%s required=%s",
                    player.player_id, request.battle_config_id, req_scene,
                )
                return 1006

        # 卷四·三境明雷野外首领试炼（boss_trial_kind 12..14）：玩家必须身处对应野外图，防原地跨图偷开。
        if 12 <= boss_trial_kind <= 14 and dungeon_row is not None and int(getattr(dungeon_row, "scene_id", 0)) > 1:
            req_scene = int(dungeon_row.scene_id)
            if current_scene_config_id != req_scene:
                logger.warning(
                    "PrepareBattle rejected: wild boss trial scene mismatch player_id=%s battle_config_id=%s required=%s current=%s",
                    player.player_id, request.battle_config_id, req_scene, current_scene_config_id,
                )
                return 1006

        # 归元城 NPC 场景玩法（木桩/残局/奇谈）场景与预设准入校验（314 豁免跨场景校验）。
        if is_npc_isolated_encounter and dungeon_row is not None:
            req_scene = int(getattr(dungeon_row, "scene_id", 0))
            if req_scene != 0 and int(getattr(request, "battle_config_id", 0)) != 314 and current_scene_config_id != req_scene:
                logger.warning(
                    "PrepareBattle rejected: npc encounter scene mismatch player_id=%s battle_config_id=%s required=%s current=%s",
                    player.player_id, request.battle_config_id, req_scene, current_scene_config_id,
                )
                return 1006
            if is_practice_dummy_kind(boss_trial_kind) and use_free_preset:
                logger.warning(
                    "PrepareBattle rejected: practice dummy forbids free preset player_id=%s battle_config_id=%s",
                    player.player_id, request.battle_config_id,
                )
                return 1006
        tower_ctx_source = None
        if is_wuxing_tower:
            wk = wuxing_week_key(now_stamp)
            if not data.HasField("season_component") or not data.season_component.HasField("wuxing_tower"):
                return 1006
            tower = data.season_component.wuxing_tower
            if (
                tower.week_key != wk
                or not tower.run_active
                or not (1 <= tower.current_floor <= WUXING_TOWER_MAX_FLOORS)
                or tower.pending_offer_sigil_ids
            ):
                return 1006
            expected_dungeon = tower_floor_dungeon_id(tower.current_floor)
            if request is not None and request.battle_config_id and request.battle_config_id != expected_dungeon:
                return 1006
            tower_ctx_source = tower

        # 瑞兽迎春准入硬隔离：boss_trial_kind=7 必须且仅能携带合法 FESTIVAL_BEAST 上下文。
        if (boss_trial_kind == BOSS_TRIAL_KIND_FESTIVAL_BEAST) != is_festival_beast:
            logger.warning(
                "PrepareBattle rejected: festival beast context mismatch player_id=%s battle_config_id=%s",
                player.player_id, request.battle_config_id if request is not None else 0,
            )
            return 1006
        if is_festival_beast:
            fb_sys = getattr(self.scene, "festival_beast", None)
            if fb_sys is not None:
                pet_err = fb_sys.verify_prepare_battle_pet(data, request.activity_context.festival_beast)
            else:
                fb_ctx = request.activity_context.festival_beast
                active_id = int(data.pet_component.active_pet_id)
                pet = next((p for p in data.pet_component.pets if p.pet_id == active_id), None)
                if not fb_ctx.locked_pet_id or active_id != fb_ctx.locked_pet_id or pet is None:
                    pet_err = FESTIVAL_BEAST_PET_CHANGED
                elif pet.health == 0:
                    pet_err = FESTIVAL_BEAST_PET_DEAD
                else:
                    pet_err = 0
            if pet_err:
                return pet_err

        isolate_temp_state = (
            use_free_preset
            or is_formal_ranked
            or is_boss_trial
            or is_guard_rescue
            or is_npc_isolated_encounter
            or is_guild_war
            or is_jianghu_duel
            or is_festival_beast
            or is_wuxing_tower
            or has_custom_duel
            or is_pvp_challenge
        )
        base = data.derived_attributes_component
        if not isolate_temp_state and not base.health:
            return 1006
        stats = player_stats(self.protocol, self.tables, data)
        profile = data.profile_component
        snapshot.player_id, snapshot.player_name = player.player_id, profile.name
        snapshot.appearance_id, snapshot.gender = profile.appearance_id, profile.gender
        from .scene_appearance import fill_battle_skill_vfx
        fill_battle_skill_vfx(self.tables, data, snapshot)
        snapshot.class_id, snapshot.level = class_id(data), effective_level(data)
        snapshot.base_attributes.CopyFrom(base)
        if isolate_temp_state:
            snapshot.base_attributes.health = stats.max_health
            snapshot.base_attributes.mana = stats.max_mana
        snapshot.base_attributes.speed = stats.speed
        for key in ("max_health", "max_mana", "physical_attack", "magic_attack", "defense"):
            setattr(snapshot, key, getattr(stats, key))
        snapshot.combat.CopyFrom(stats.combat)
        snapshot.table_fingerprint = battle_table_fingerprint(self.tables)
        # 注入玩家当前已装配五行阵法与权威五行幻境上下文（客户端不可伪造）。
        if data.HasField("season_component"):
            fid = int(data.season_component.equipped_formation_id)
            if is_valid_formation_id(fid):
                snapshot.formation_id = fid
        if tower_ctx_source is not None:
            tc = snapshot.tower_context
            tc.week_key = tower_ctx_source.week_key
            tc.run_id = tower_ctx_source.run_id
            tc.floor = tower_ctx_source.current_floor
            tc.leyline_element = tower_floor_leyline_element(tower_ctx_source.week_key, tower_ctx_source.current_floor)
            tc.active_sigil_ids.extend(tower_ctx_source.active_sigil_ids)
        routing = snapshot.routing
        routing.session_id, routing.gate_node_id, routing.gate_instance_id = player.session_id, player.session_id >> 17, player.gate_uuid
        routing.scene_node_id, routing.scene_instance_id = self.scene.info.node_id, self.scene.info.node_uuid
        routing.zone_id = self.scene.info.zone_id
        active_season_rule = find_active_season_rule(self.tables, now_stamp)
        active_season_id = active_season_rule.id if active_season_rule is not None else 0
        battle_skills(self.tables, data, snapshot, active_season_id=active_season_id)
        buffs = getattr(self.scene, "buffs", None)
        if buffs is not None:
            buffs.snapshot(player, snapshot)
        if not isolate_temp_state:
            items: Counter[int] = Counter()
            for row in data.bag_component.items:
                if row.bag_type != 0:
                    continue
                if row.HasField("instance"):
                    item = row.instance
                    guid, config, amount = item.item_id, item.config_id, item.size
                else:
                    item = row
                    guid, config, amount = row.item_uuid, row.config_id, row.stack_size
                rule = self.tables["Item"].get(config)
                if rule is not None and rule.battle_usable and guid and amount and AssetInventory.countable(item):
                    items[config] += amount
            for config, amount in sorted(items.items()):
                available = min(amount, self._inventory.removable_count(data, config))
                if available:
                    snapshot.items.add(item_table_id=config, count=min((1 << 32) - 1, available))
        if data.pet_component.active_pet_id:
            pet = next((item for item in data.pet_component.pets if item.pet_id == data.pet_component.active_pet_id), None)
            if pet is not None:
                wire = self.protocol.message("BattlePetSnapshot")
                if build_single_pet_battle_snapshot(
                    self.tables, player.player_id, pet, snapshot.level, full_resources=False, out=wire,
                    pet_owned_bits=bytes(profile.pet_cosmetic_owned_bits),
                ):
                    if isolate_temp_state:
                        wire.base_attributes.health = wire.max_health
                        wire.base_attributes.mana = wire.max_mana
                    snapshot.pets.add().CopyFrom(wire)
        # 论剑 V3 战前 15 秒明牌变阵：仅正式自养论剑与赛季锦标赛打包备选灵宠（至多 5 只）与当季全部赛季印。
        is_tournament = request is not None and int(getattr(request, "match_mode", 0)) == _MATCH_MODE_SEASON_TOURNAMENT
        if not use_free_preset and (is_formal_ranked or is_tournament):
            active_pet_id = snapshot.pets[0].pet_id if snapshot.pets else 0
            standby_pets = build_standby_battle_snapshots(
                self.tables, player.player_id, data, active_pet_id, _MAX_STANDBY_PETS,
                full_resources=isolate_temp_state, protocol=self.protocol,
            )
            for standby in standby_pets:
                snapshot.standby_pets.add().CopyFrom(standby)
            if active_season_rule is not None and "SeasonSeal" in self.tables.tables:
                seals = [
                    seal for seal in self.tables["SeasonSeal"].rows()
                    if seal.season_id == active_season_rule.id and valid_seal_row(seal)
                ]
                seals.sort(key=lambda row: row.id)
                for seal in seals:
                    snapshot.available_season_seals.add(
                        season_id=active_season_rule.id,
                        seal_id=seal.id,
                        effect_id=seal.effect_id,
                        bonus_bps=seal.bonus_bps,
                        cap_bps=seal.cap_bps,
                        per_round_limit=seal.per_round_limit,
                        battle_limit=seal.battle_limit,
                        active_rounds=seal.active_rounds,
                        health_threshold_bps=seal.health_threshold_bps,
                        name=seal.name,
                    )
        return 0

    async def cancel(self, request):
        player_id = request.player_id
        # skip_jianghu_silver_refund 已废弃：match 不再发退费邮件，普通生死状银两退还只走 scene 通道，读到 true 也照退。
        jianghu_sys = (
            getattr(self.scene, "jianghu", None)
            or SceneJianghu(self.scene, self.tables, getattr(self.scene, "assets", None), clock=self._clock)
        )
        async with self.scene._lock(player_id):
            entry = self._freezes.get(player_id)
            if entry is None and self._player(player_id) is None:
                entry = await self._read(player_id)
            if entry and entry.context.battle_id == request.battle_id and entry.context.state == 1:
                await self._eval(_REMOVE, player_id, str(request.battle_id), entry.context.SerializeToString(deterministic=True))
                self._thaw(player_id)
                if (
                    (player := self._player(player_id)) is not None
                    and callable(getattr(self.scene, "mutable", None))
                    and self.scene.mutable(player)
                ):
                    await self.scene.flush_player_rows(player)
                    data = self.scene.rows.assemble(player.snapshot)
                    assets = getattr(self.scene, "assets", None)
                    before = (
                        type(data).FromString(data.SerializeToString())
                        if (assets is not None and assets.audit_enabled)
                        else None
                    )
                    changed = bool(release_item_hold(data, request.battle_id))
                    if SceneFestivalBeast.on_battle_prepare_cancelled_or_void(data, request.battle_id):
                        changed = True
                    j_changed, refunded = jianghu_sys.on_battle_prepare_cancelled(
                        data, request.battle_id, skip_silver_refund=False,
                    )
                    if j_changed:
                        data.jianghu_component.pending_silver_fee_review_after_ms = 0
                        changed = True
                    if changed:
                        audit_cb = (
                            assets.make_audit_callback(
                                player, before, data, TX_JIANGHU_DUEL_REFUND, correlation=request.battle_id,
                            )
                            if (before is not None and refunded > 0)
                            else None
                        )
                        if audit_cb is not None:
                            await self.scene.commit_player_data(player, data, on_committed=audit_cb)
                        else:
                            await self.scene.commit_player_data(player, data)
            elif (
                entry is None
                and (player := self._player(player_id)) is not None
                and callable(getattr(self.scene, "mutable", None))
                and self.scene.mutable(player)
            ):
                # 即使 PREPARING 冻结已先被过期回收，仍须幂等核销/退还待退的普通战书银两，防 500 银两永久滞留。
                await self.scene.flush_player_rows(player)
                data = self.scene.rows.assemble(player.snapshot)
                assets = getattr(self.scene, "assets", None)
                before = (
                    type(data).FromString(data.SerializeToString())
                    if (assets is not None and assets.audit_enabled)
                    else None
                )
                j_changed, refunded = jianghu_sys.on_battle_prepare_cancelled(
                    data, request.battle_id, skip_silver_refund=False,
                )
                if j_changed:
                    data.jianghu_component.pending_silver_fee_review_after_ms = 0
                    audit_cb = (
                        assets.make_audit_callback(
                            player, before, data, TX_JIANGHU_DUEL_REFUND, correlation=request.battle_id,
                        )
                        if (before is not None and refunded > 0)
                        else None
                    )
                    if audit_cb is not None:
                        await self.scene.commit_player_data(player, data, on_committed=audit_cb)
                    else:
                        await self.scene.commit_player_data(player, data)
        return self.protocol.message("Empty")

    async def confirm(self, event) -> None:
        player_id, battle_id = event.player_id, event.battle_id
        if not player_id or not battle_id:
            return
        async with self.scene._lock(player_id):
            player = self._player(player_id)
            if player is None and not event.deadline_ms:
                return
            if player is not None and getattr(player, "battle_expired_id", 0) == battle_id:
                await self._remove_lock(player_id, battle_id)
                return
            existing = self._freezes.get(player_id)
            entry = existing or await self._read(player_id)
            if entry is None or entry.context.battle_id != battle_id:
                return
            upgrading = entry.context.state == 1
            if existing is not None and not upgrading and entry.context.state != 2:
                return
            if existing is None or upgrading:
                entry.context.state = 2
                entry.context.deadline_ms = event.deadline_ms or entry.context.deadline_ms
            renewed = await self._store(_RENEW, player_id, entry)
            if player is not None and (existing is not None or renewed):
                self._freezes[player_id] = entry
                if (existing is None or upgrading and entry.session_id != player.session_id) and player.disconnect_at is None:
                    self.notify_reconnect(player)

    async def settled(self, player_id: int, battle_id: int) -> bool:
        entry = self._freezes.get(player_id)
        if entry is not None and entry.context.battle_id != battle_id:
            return False
        await self._remove_lock(player_id, battle_id)
        self._thaw(player_id)
        return True

    async def close(self) -> None:
        tasks = tuple(self._reaping.values()) + tuple(self._hold_reviews.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
