"""Scene 备战快照、战斗冻结和按局号围栏的 Redis 锁。"""

import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass

import grpc
from google.protobuf.message import DecodeError

from ..data import StoreRejected, StoreUnavailable
from ..data.asset_inventory import AssetInventory
from .battle_control import service_digest, sign_service_call, verify_service_call
from .battle_tables import battle_table_fingerprint
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
                    error = self._snapshot(player, response.snapshot)
                except (StoreRejected, StoreUnavailable):
                    error = 1003
                if not error and deadline <= self._clock():
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
            if error:
                response.ClearField("snapshot")
                response.error_message.id = error
            else:
                response.table_fingerprint = response.snapshot.table_fingerprint
        return response

    def _snapshot(self, player, snapshot) -> int:
        data = self.scene.rows.assemble(player.snapshot)
        if not data.HasField("derived_attributes_component") or not data.HasField("level_component"):
            return 1003
        base = data.derived_attributes_component
        if not base.health:
            return 1006
        stats = player_stats(self.protocol, self.tables, data)
        profile = data.profile_component
        snapshot.player_id, snapshot.player_name = player.player_id, profile.name
        snapshot.appearance_id, snapshot.gender = profile.appearance_id, profile.gender
        snapshot.class_id, snapshot.level = class_id(data), effective_level(data)
        snapshot.base_attributes.CopyFrom(base)
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
            snapshot.items.add(item_table_id=config, count=min((1 << 32) - 1, amount))
        if data.pet_component.active_pet_id:
            pet = next((item for item in data.pet_component.pets if item.pet_id == data.pet_component.active_pet_id), None)
            row = self.tables["Pet"].get(pet.pet_table_id) if pet is not None else None
            stats = pet_stats(self.tables, pet) if pet is not None else None
            if row is not None and stats and pet.health and snapshot.level >= pet_level(pet.level, row):
                wire = snapshot.pets.add(
                    pet_id=pet.pet_id, owner_player_id=player.player_id, pet_name=pet.name or row.name,
                    pet_table_id=pet.pet_table_id, level=pet_level(pet.level, row),
                    max_health=stats["max_health"], max_mana=stats["max_mana"], defense=stats["defense"],
                    physical_attack=stats["physical_attack"], magic_attack=stats["magic_attack"],
                    skill_table_ids=pet_skills(pet, row),
                )
                wire.base_attributes.health = min(pet.health, stats["max_health"])
                wire.base_attributes.mana = min(pet.mana, stats["max_mana"]) if stats["max_mana"] else pet.mana
                wire.base_attributes.speed = stats["speed"]
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
        for task in tuple(self._reaping.values()):
            task.cancel()
        await asyncio.gather(*tuple(self._reaping.values()), return_exceptions=True)
