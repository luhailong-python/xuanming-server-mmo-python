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
    is_boss_trial_kind,
)
from .battle_tables import battle_table_fingerprint
from .scene_festival_beast import FESTIVAL_BEAST_PET_CHANGED, FESTIVAL_BEAST_PET_DEAD, SceneFestivalBeast
from .scene_mission import effective_level
from .scene_stats import battle_skills, class_id, pet_level, pet_skills, pet_stats, player_stats

logger = logging.getLogger(__name__)
TERMINAL = "/match.MatchInternal/GetBattleTerminalDecision"
_SETTLEMENT_GRACE_MS = 120_000  # Match 终局宽限和收件箱投递余量各 60 秒。
_HOLD_CAP_MS = 15 * 60_000
_LOCK_EXTRA_SECONDS = 60
_REAPER_INTERVAL_MS = 30_000
_UINT64_MAX = (1 << 64) - 1

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
                    holds = self.protocol.parse("player_database", raw).item_holds.holds if raw else ()
                else:
                    holds = self.scene.rows.assemble(snapshot).item_holds.holds
                battle_holds = [hold for hold in holds if hold.kind == 1 and hold.hold_id]
                if not battle_holds:
                    if save_seq is not None:
                        self._hold_checked_seq[player_id] = save_seq
                    continue
                self._hold_checked_seq.pop(player_id, None)
                if any(hold.review_after_ms <= now and hold.hold_id != self.current(player_id)
                       for hold in battle_holds):
                    self._hold_review_at[player_id] = now + _REAPER_INTERVAL_MS
                    self._hold_reviews[player_id] = asyncio.create_task(self._review_holds(player_id))

    async def _review_holds(self, player_id: int) -> None:
        try:
            player = self._player(player_id)
            if player is None:
                return
            data = self.scene.rows.assemble(player.snapshot)
            candidates = [hold.hold_id for hold in data.item_holds.holds
                          if hold.kind == 1 and hold.hold_id and hold.review_after_ms <= self._clock()
                          and hold.hold_id != self.current(player_id)]
            for hold_id in candidates:
                verdict = (2, False) if self._applied(player, hold_id) else await self._decision(hold_id)
                if verdict not in ((2, False), (0, False)):
                    continue
                async with self.scene._lock(player_id):
                    if self._player(player_id) is not player or not self.scene.mutable(player) or self.current(player_id) == hold_id:
                        continue
                    await self.scene.flush_player_rows(player)
                    current = self.scene.rows.assemble(player.snapshot)
                    if release_item_hold(current, hold_id):
                        await self.scene.commit_player_data(player, current)
        except (StoreRejected, StoreUnavailable, OSError, ValueError):
            logger.warning("战斗道具预留复核待重试 player_id=%s", player_id, exc_info=True)
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
                if not error and response.snapshot.items:
                    data = self.scene.rows.assemble(player.snapshot)
                    counts = {item.item_table_id: item.count for item in response.snapshot.items}
                    error = self._inventory.place_item_hold(
                        data, request.battle_id, counts, created_at_ms=self._clock(),
                        review_after_ms=min(_UINT64_MAX, request.deadline_ms + _SETTLEMENT_GRACE_MS),
                    )
                    if error:
                        error = 1006
                    if not error:
                        try:
                            await self.scene.commit_player_data(player, data)
                        except (StoreRejected, StoreUnavailable):
                            error = 1003
                        else:
                            held = True
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
                if error and held:
                    try:
                        data = self.scene.rows.assemble(player.snapshot)
                        if release_item_hold(data, request.battle_id):
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

        isolate_temp_state = use_free_preset or is_formal_ranked or is_boss_trial or is_guard_rescue or is_festival_beast
        base = data.derived_attributes_component
        if not isolate_temp_state and not base.health:
            return 1006
        stats = player_stats(self.protocol, self.tables, data)
        profile = data.profile_component
        snapshot.player_id, snapshot.player_name = player.player_id, profile.name
        snapshot.appearance_id, snapshot.gender = profile.appearance_id, profile.gender
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
        routing = snapshot.routing
        routing.session_id, routing.gate_node_id, routing.gate_instance_id = player.session_id, player.session_id >> 17, player.gate_uuid
        routing.scene_node_id, routing.scene_instance_id = self.scene.info.node_id, self.scene.info.node_uuid
        routing.zone_id = self.scene.info.zone_id
        battle_skills(self.tables, data, snapshot)
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
            row = self.tables["Pet"].get(pet.pet_table_id) if pet is not None else None
            pstats = pet_stats(self.tables, pet) if pet is not None else None
            if row is not None and pstats and pet.health and snapshot.level >= pet_level(pet.level, row):
                wire = snapshot.pets.add(
                    pet_id=pet.pet_id, owner_player_id=player.player_id, pet_name=pet.name or row.name,
                    pet_table_id=pet.pet_table_id, level=pet_level(pet.level, row),
                    max_health=pstats["max_health"], max_mana=pstats["max_mana"], defense=pstats["defense"],
                    physical_attack=pstats["physical_attack"], magic_attack=pstats["magic_attack"],
                    skill_table_ids=pet_skills(pet, row),
                )
                if isolate_temp_state:
                    wire.base_attributes.health = pstats["max_health"]
                    wire.base_attributes.mana = pstats["max_mana"]
                else:
                    wire.base_attributes.health = min(pet.health, pstats["max_health"])
                    wire.base_attributes.mana = min(pet.mana, pstats["max_mana"]) if pstats["max_mana"] else pet.mana
                wire.base_attributes.speed = pstats["speed"]
        return 0

    async def cancel(self, request):
        player_id = request.player_id
        async with self.scene._lock(player_id):
            entry = self._freezes.get(player_id)
            if entry is None and self._player(player_id) is None:
                entry = await self._read(player_id)
            if entry and entry.context.battle_id == request.battle_id and entry.context.state == 1:
                await self._eval(_REMOVE, player_id, str(request.battle_id), entry.context.SerializeToString(deterministic=True))
                self._thaw(player_id)
                if (player := self._player(player_id)) is not None and self.scene.mutable(player):
                    await self.scene.flush_player_rows(player)
                    data = self.scene.rows.assemble(player.snapshot)
                    changed = bool(release_item_hold(data, request.battle_id))
                    if SceneFestivalBeast.on_battle_prepare_cancelled_or_void(data, request.battle_id):
                        changed = True
                    if changed:
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
