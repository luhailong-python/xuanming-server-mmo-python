"""两域存储实现：SceneControlStore（控制域）与 PlayerPlacementStore（玩家域）。

每个域都同时提供：
- 基于 ``redis.asyncio`` + Go 同源 Lua 脚本的生产实现（``RedisSceneControlStore`` / ``RedisPlayerPlacementStore``）；
- 确定性同构内存孪生（``MemorySceneControlStore`` / ``MemoryPlayerPlacementStore``），使单测无需外部 Redis。

两个域在构造器与所有多键 Lua 调用处均执行域守卫，严禁把控制域键与玩家域键混在同一段脚本中。
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from . import config, keys, lua

logger = logging.getLogger(__name__)


def open_redis(url: str, *, cluster: bool = False):
    """打开 Redis 异步客户端（decode_responses=False，保留原始 protobuf 字节）。"""
    from redis.asyncio import Redis, RedisCluster

    redis_cls = RedisCluster if cluster else Redis
    return redis_cls.from_url(url, decode_responses=False, socket_connect_timeout=3, socket_timeout=3)


def _as_bytes(value) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode("utf-8")


def _as_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _as_int(value) -> int:
    if isinstance(value, int):
        return value
    return int(_as_text(value))


# ── 结果值类型 ─────────────────────────────────────────────────────────────────

class RegistrationOutcome(str, Enum):
    REGISTERED = "registered"
    CONFLICT = "conflict"
    ABORTED = "aborted"
    FENCED = "fenced"
    OVER_DESIRED = "over_desired"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class WorldChannelCandidate:
    scene_id: int
    node_id: str
    zone_id: int


@dataclass(frozen=True, slots=True)
class SceneClaimResult:
    claimed: bool = False
    busy: bool = False
    pending: bool = False
    withdrawn: bool = False
    node_id: str = ""
    agones_gs: str = ""
    source: str = ""
    mirror_flag: str = ""
    members: tuple[str, ...] = ()
    token: str = ""


@dataclass(frozen=True, slots=True)
class SceneTeardownJob:
    """拆除任务快照（对应 scene:{ctl:<z>}:<id>:teardown）。"""

    zone_id: int
    scene_id: int
    kind: str
    reason: str
    claim: SceneClaimResult


@dataclass(frozen=True, slots=True)
class DeadNodeTaskFence:
    """死节点收尾任务身份围栏，用于逐个认领与结束时的同脚本防撤销核对。"""

    tasks_key: str
    member: str
    queued_at_ms: int


@dataclass(frozen=True, slots=True)
class DeadNodeReconcileTask:
    """从 scene_manager:dead_node_reconcile:{ctl:<z>} 读出的一条死节点收尾任务。"""

    member: str
    zone_id: int
    node_id: str
    queued_at_ms: int

    def fence(self) -> DeadNodeTaskFence:
        return DeadNodeTaskFence(
            tasks_key=keys.dead_node_reconcile_tasks_key(self.zone_id),
            member=self.member,
            queued_at_ms=self.queued_at_ms,
        )


class SceneIdGenerator:
    """单调非零 uint64 场景号生成器（时间毫秒 + 节点号 + 序列号，与雪花结构兼容）。"""

    def __init__(self, node_id: int = 1, *, clock_ms: Callable[[], int] | None = None) -> None:
        self._node_bits = (node_id & 0x3FF) << 12
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._last_ms = 0
        self._seq = 0

    def next_id(self) -> int:
        now = max(1, int(self._clock_ms()))
        if now <= self._last_ms:
            self._seq = (self._seq + 1) & 0xFFF
            if self._seq == 0:
                self._last_ms += 1
        else:
            self._last_ms = now
            self._seq = 0
        # 保留 41 位时间戳低位 + 10 位节点号 + 12 位序列号，确保正 int64 / uint64 且永不为 0
        ts_part = (self._last_ms & ((1 << 41) - 1)) << 22
        return ts_part | self._node_bits | self._seq


# ── 控制域存储协议 ─────────────────────────────────────────────────────────────

class SceneControlStore(Protocol):
    async def upsert_scene_node(
        self, zone_id: int, node_id: str, *, score: float, scene_node_type: int, preserve_death_at: bool = False,
    ) -> None: ...
    async def remove_scene_node(self, zone_id: int, node_id: str, *, death_at_unix_sec: int | None = None) -> None: ...
    async def get_node_load_pairs(self, zone_id: int) -> list[tuple[str, float]]: ...
    async def is_node_in_load_set(self, zone_id: int, node_id: str) -> bool: ...
    async def get_node_scene_type(self, zone_id: int, node_id: str) -> int | None: ...
    async def get_node_scene_count(self, zone_id: int, node_id: str) -> int | None: ...
    async def get_node_death_at(self, zone_id: int, node_id: str) -> int | None: ...
    async def get_scene_node(self, zone_id: int, scene_id: int) -> str: ...
    async def scene_node_exists(self, zone_id: int, scene_id: int) -> bool: ...
    async def get_scene_mirrors(self, zone_id: int, source_scene_id: int) -> list[str]: ...
    async def remove_scene_mirror(self, zone_id: int, source_scene_id: int, mirror_scene_id: int) -> None: ...
    async def delete_scene_mirrors_index(self, zone_id: int, source_scene_id: int) -> None: ...
    async def is_scene_draining(self, zone_id: int, scene_id: int) -> bool: ...
    async def is_scene_mirror(self, zone_id: int, scene_id: int) -> bool: ...
    async def get_scene_placement_gen(self, zone_id: int, scene_id: int) -> int: ...
    async def allocate_placement_generation(self, zone_id: int) -> int: ...
    async def reassign_scene_node(
        self, *, zone_id: int, scene_id: int, old_node_id: str, new_node_id: str, placement_generation: int,
    ) -> bool: ...
    async def get_world_channels(self, zone_id: int, conf_id: int) -> list[str]: ...
    async def get_world_draining_channels(self, zone_id: int, conf_id: int) -> list[str]: ...
    async def is_world_channel_active(self, zone_id: int, conf_id: int, scene_id: int) -> bool: ...
    async def is_world_channel_draining(self, zone_id: int, conf_id: int, scene_id: int) -> bool: ...
    async def allocate_world_channel_line_numbers(
        self, zone_id: int, conf_id: int, scene_ids: Sequence[int],
    ) -> dict[int, int]: ...
    async def set_world_channel_directory(self, zone_id: int, conf_id: int, payload: bytes, ttl_sec: int) -> None: ...
    async def get_world_channel_directory(self, zone_id: int, conf_id: int) -> bytes | None: ...
    async def get_or_seed_desired_world_channels(self, zone_id: int, conf_id: int, seed_count: int) -> int: ...
    async def acquire_world_init_lock(self, zone_id: int, token: str, ttl_sec: int) -> bool: ...
    async def release_world_init_lock(self, zone_id: int, token: str) -> bool: ...
    async def register_instance(
        self, *, zone_id: int, scene_id: int, node_id: str, created_at: int,
        mirror: bool, source_scene_id: int,
    ) -> RegistrationOutcome: ...
    async def register_world_channel(
        self, *, zone_id: int, scene_id: int, node_id: str, conf_id: int, desired_channels: int,
    ) -> RegistrationOutcome: ...
    async def list_active_instances_with_scores(self, zone_id: int) -> list[tuple[str, float]]: ...
    async def touch_active_instance(self, zone_id: int, scene_id: int, now_unix_sec: int) -> None: ...
    async def is_active_instance(self, zone_id: int, scene_id: int) -> bool: ...
    async def atomic_add_scene_member(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> int: ...
    async def add_scene_member(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> None: ...
    async def remove_scene_member_keys(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> None: ...
    async def get_scene_member_count(self, zone_id: int, scene_id: int) -> int: ...
    async def reserve_best_world_channel(
        self, candidates: Sequence[WorldChannelCandidate], player_id: int,
    ) -> tuple[int, str, int] | None: ...
    async def atomic_destroy_instance(
        self, *, zone_id: int, scene_id: int, expected_owner: str, now_unix_sec: int | None = None,
    ) -> SceneClaimResult: ...
    async def atomic_destroy_instance_force(
        self, *, zone_id: int, scene_id: int, expected_owner: str, kind: str, reason: str,
        count_owner: bool = True, now_unix_sec: int | None = None,
    ) -> SceneClaimResult: ...
    async def atomic_destroy_orphan_of_dead_node(
        self, *, zone_id: int, scene_id: int, dead_node: str, task_fence: DeadNodeTaskFence,
        now_unix_sec: int | None = None,
    ) -> SceneClaimResult: ...
    async def finish_scene_teardown(self, zone_id: int, scene_id: int, token: str) -> bool: ...
    async def list_due_teardown_jobs(self, zone_id: int, max_claimed_at_sec: int, limit: int) -> list[tuple[str, float]]: ...
    async def load_scene_teardown_job(self, zone_id: int, scene_id: int) -> SceneTeardownJob | None: ...
    async def drop_dangling_teardown(self, zone_id: int, scene_id: int) -> bool: ...
    async def enqueue_dead_node_reconcile(
        self, zone_id: int, node_id: str, *, queued_at_ms: int, detach: bool = True, node_uuid: str = "",
    ) -> int: ...
    async def withdraw_dead_node_for_returning_instance(self, zone_id: int, node_id: str, node_uuid: str) -> int: ...
    async def list_dead_node_reconcile_tasks(self, zone_id: int) -> tuple[list[DeadNodeReconcileTask], list[str]]: ...
    async def remove_dead_node_reconcile_member(self, zone_id: int, member: str) -> None: ...
    async def get_node_reconcile_scenes(self, zone_id: int, node_id: str) -> list[str]: ...
    async def remove_node_reconcile_scenes(self, zone_id: int, node_id: str, scene_ids: Sequence[str]) -> None: ...
    async def remove_node_scenes(self, zone_id: int, node_id: str, scene_ids: Sequence[str]) -> None: ...
    async def finish_dead_node_reconcile(
        self, zone_id: int, node_id: str, member: str, queued_at_ms: int, *, abandon: bool = False,
    ) -> bool: ...
    async def get_gate_marks(self, zone_id: int, gate_node_ids: Sequence[int]) -> tuple[set[int], set[int]]: ...


# ── 玩家域存储协议 ─────────────────────────────────────────────────────────────

class PlayerPlacementStore(Protocol):
    async def observe_location_revision(self, player_id: int, revision: int) -> int: ...
    async def read_location_snapshot(self, player_id: int) -> tuple[bytes, str]: ...
    async def get_owner_epoch(self, player_id: int) -> int: ...
    async def setnx_owner_epoch(self, player_id: int, epoch: int) -> bool: ...
    async def get_handoff_marker(self, player_id: int) -> str: ...
    async def get_holder_lease(self, player_id: int) -> str: ...
    async def mint_epoch_and_set_location(
        self, *, player_id: int, observed_epoch: int, location_bytes: bytes,
        required_marker: str, expect_mode: str, expect_raw: bytes,
        owner_revision: int, new_tag: str, observed_tag: str, target_epoch: int,
    ) -> int: ...
    async def set_location_if_epoch(
        self, *, player_id: int, observed_epoch: int, location_bytes: bytes,
        expect_mode: str, expect_raw: bytes, owner_revision: int, new_tag: str, observed_tag: str,
    ) -> int: ...
    async def rollback_player_placement(
        self, *, player_id: int, placed_raw: bytes, restored_raw: bytes, placed_epoch: int,
        mode: str, required_marker: str, forward_marker: str, handoff_ttl_sec: int,
        owner_revision: int, placed_tag: str, restored_tag: str,
        void_epoch: int | None = None, observed_raw: bytes = b"",
        observed_tag: str = "", void_epoch_after: int | None = None,
    ) -> int: ...
    async def delete_location_if_unchanged(
        self, *, player_id: int, expected_raw: bytes, mode: str, epoch: int,
        expected_tag: str, owner_revision: int,
    ) -> int: ...
    async def note_leave_release_wait(self, player_id: int, scope: str, ttl_sec: int) -> int: ...
    async def has_channel_switch_cooldown(self, player_id: int) -> bool: ...
    async def set_channel_switch_cooldown(self, player_id: int, ttl_sec: int) -> None: ...
    async def setnx_enter_dedupe(self, player_id: int, request_id: str, value: str, ttl_sec: int) -> bool: ...
    async def get_enter_dedupe(self, player_id: int, request_id: str) -> str: ...
    async def delete_enter_dedupe_if_value(self, player_id: int, request_id: str, expected: str) -> bool: ...
    async def complete_enter_dedupe(
        self, *, player_id: int, request_id: str, expected_owner: str,
        cached_value: str, ttl_sec: int, owner_revision: int,
    ) -> bool: ...


# ==============================================================================
# 一、RedisSceneControlStore（控制域 Redis + Go 同源 Lua）
# ==============================================================================

def _parse_registration_reply(raw) -> RegistrationOutcome:
    if not raw or not isinstance(raw, (list, tuple)):
        return RegistrationOutcome.UNKNOWN
    status = _as_text(raw[0])
    try:
        return RegistrationOutcome(status)
    except ValueError:
        return RegistrationOutcome.UNKNOWN


def _parse_scene_claim_reply(raw, token: str) -> SceneClaimResult:
    if not raw or not isinstance(raw, (list, tuple)) or len(raw) < 5:
        return SceneClaimResult()
    status = _as_text(raw[0])
    if status == "busy":
        return SceneClaimResult(busy=True)
    if status == "pending":
        return SceneClaimResult(pending=True)
    if status == "withdrawn":
        return SceneClaimResult(withdrawn=True)
    if status == "claimed":
        return SceneClaimResult(
            claimed=True,
            node_id=_as_text(raw[1]),
            agones_gs=_as_text(raw[2]),
            source=_as_text(raw[3]),
            mirror_flag=_as_text(raw[4]),
            members=tuple(_as_text(item) for item in raw[5:]),
            token=token,
        )
    return SceneClaimResult()


def _sorted_unique_scene_ids(scene_ids: Sequence[int]) -> list[int]:
    """去重并过滤非正数，按 uint64 数值升序排列（与 Go sortedUniqueSceneIDs 一致）。"""
    return sorted({int(sid) for sid in scene_ids if int(sid) > 0})


def _parse_line_numbers_reply(raw, ids: Sequence[int]) -> dict[int, int]:
    """校验并解析 LUA_ALLOC_WORLD_CHANNEL_LINE_NUMBERS 返回的扁平列表。"""
    if not isinstance(raw, (list, tuple)) or len(raw) != 2 * len(ids):
        raise ValueError(f"unexpected line number reply shape: len={len(raw) if isinstance(raw, (list, tuple)) else type(raw).__name__}")
    expected = set(ids)
    line_nos: dict[int, int] = {}
    used_lines: set[int] = set()
    for i in range(0, len(raw), 2):
        sid = int(_as_text(raw[i]))
        line_no = int(_as_text(raw[i + 1]))
        if sid not in expected or sid in line_nos:
            raise ValueError(f"unexpected or duplicate scene_id {sid} in line number reply")
        if line_no <= 0 or line_no > 0xFFFFFFFF or line_no in used_lines:
            raise ValueError(f"invalid or duplicate line_no {line_no} for scene_id {sid}")
        line_nos[sid] = line_no
        used_lines.add(line_no)
    return line_nos


class RedisSceneControlStore:
    """基于 redis.asyncio 与控制域 Lua 脚本的 SceneControlStore 生产实现。"""

    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self._register_instance_script = redis_client.register_script(lua.LUA_REGISTER_INSTANCE)
        self._register_world_script = redis_client.register_script(lua.LUA_REGISTER_WORLD_CHANNEL)
        self._abort_reg_script = redis_client.register_script(lua.LUA_ABORT_SCENE_REGISTRATION)
        self._atomic_add_member_script = redis_client.register_script(lua.LUA_ATOMIC_ADD_SCENE_MEMBER)
        self._add_member_script = redis_client.register_script(lua.LUA_ADD_SCENE_MEMBER)
        self._remove_member_script = redis_client.register_script(lua.LUA_REMOVE_SCENE_MEMBER)
        self._reserve_best_script = redis_client.register_script(lua.LUA_RESERVE_BEST_WORLD_CHANNEL)
        self._destroy_idle_script = redis_client.register_script(lua.LUA_ATOMIC_DESTROY_INSTANCE)
        self._destroy_force_script = redis_client.register_script(lua.LUA_ATOMIC_DESTROY_INSTANCE_FORCE)
        self._destroy_orphan_script = redis_client.register_script(lua.LUA_ATOMIC_DESTROY_ORPHAN_OF_DEAD_NODE)
        self._finish_teardown_script = redis_client.register_script(lua.LUA_FINISH_SCENE_TEARDOWN)
        self._drop_dangling_teardown_script = redis_client.register_script(lua.LUA_DROP_DANGLING_TEARDOWN)
        self._release_init_lock_script = redis_client.register_script(lua.LUA_RELEASE_WORLD_INIT_LOCK)
        self._reassign_scene_script = redis_client.register_script(lua.LUA_REASSIGN_SCENE_NODE)
        self._alloc_lineno_script = redis_client.register_script(lua.LUA_ALLOC_WORLD_CHANNEL_LINE_NUMBERS)
        self._enqueue_dead_node_script = redis_client.register_script(lua.LUA_ENQUEUE_DEAD_NODE_RECONCILE)
        self._withdraw_dead_node_script = redis_client.register_script(lua.LUA_WITHDRAW_DEAD_NODE_FOR_RETURNING_INSTANCE)
        self._finish_dead_node_script = redis_client.register_script(lua.LUA_FINISH_DEAD_NODE_RECONCILE)

    async def upsert_scene_node(
        self, zone_id: int, node_id: str, *, score: float, scene_node_type: int, preserve_death_at: bool = False,
    ) -> None:
        load_k = keys.node_load_key(zone_id)
        type_k = keys.node_scene_node_type_key(zone_id, node_id)
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((load_k, type_k, death_k))
        await self.redis.zadd(load_k, {node_id: float(score)})
        await self.redis.set(type_k, str(scene_node_type))
        if not preserve_death_at:
            await self.redis.delete(death_k)

    async def remove_scene_node(self, zone_id: int, node_id: str, *, death_at_unix_sec: int | None = None) -> None:
        load_k = keys.node_load_key(zone_id)
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((load_k, death_k))
        if death_at_unix_sec is not None:
            await self.redis.set(death_k, str(death_at_unix_sec))
        await self.redis.zrem(load_k, node_id)

    async def get_node_load_pairs(self, zone_id: int) -> list[tuple[str, float]]:
        load_k = keys.node_load_key(zone_id)
        keys.assert_control_domain_keys((load_k,))
        rows = await self.redis.zrange(load_k, 0, -1, withscores=True)
        return [(_as_text(member), float(score)) for member, score in rows]

    async def is_node_in_load_set(self, zone_id: int, node_id: str) -> bool:
        load_k = keys.node_load_key(zone_id)
        keys.assert_control_domain_keys((load_k,))
        return (await self.redis.zscore(load_k, node_id)) is not None

    async def get_node_scene_type(self, zone_id: int, node_id: str) -> int | None:
        type_k = keys.node_scene_node_type_key(zone_id, node_id)
        keys.assert_control_domain_keys((type_k,))
        raw = await self.redis.get(type_k)
        if not raw:
            return None
        try:
            return int(_as_text(raw))
        except ValueError:
            return None

    async def get_node_scene_count(self, zone_id: int, node_id: str) -> int | None:
        cnt_k = keys.node_scene_count_key(zone_id, node_id)
        keys.assert_control_domain_keys((cnt_k,))
        raw = await self.redis.get(cnt_k)
        if not raw:
            return None
        try:
            return int(_as_text(raw))
        except ValueError:
            return None

    async def get_node_death_at(self, zone_id: int, node_id: str) -> int | None:
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((death_k,))
        raw = await self.redis.get(death_k)
        if not raw:
            return None
        try:
            return int(_as_text(raw))
        except ValueError:
            return None

    async def get_scene_node(self, zone_id: int, scene_id: int) -> str:
        k = keys.scene_node_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return _as_text(await self.redis.get(k))

    async def scene_node_exists(self, zone_id: int, scene_id: int) -> bool:
        k = keys.scene_node_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return int(await self.redis.exists(k)) == 1

    async def get_scene_mirrors(self, zone_id: int, source_scene_id: int) -> list[str]:
        k = keys.scene_mirrors_key(zone_id, source_scene_id)
        keys.assert_control_domain_keys((k,))
        return sorted(_as_text(m) for m in await self.redis.smembers(k))

    async def remove_scene_mirror(self, zone_id: int, source_scene_id: int, mirror_scene_id: int) -> None:
        k = keys.scene_mirrors_key(zone_id, source_scene_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.srem(k, str(mirror_scene_id))

    async def delete_scene_mirrors_index(self, zone_id: int, source_scene_id: int) -> None:
        k = keys.scene_mirrors_key(zone_id, source_scene_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.delete(k)

    async def is_scene_draining(self, zone_id: int, scene_id: int) -> bool:
        k = keys.scene_draining_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return int(await self.redis.exists(k)) == 1

    async def is_scene_mirror(self, zone_id: int, scene_id: int) -> bool:
        k = keys.scene_mirror_flag_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return _as_text(await self.redis.get(k)) == "1"

    async def get_scene_placement_gen(self, zone_id: int, scene_id: int) -> int:
        k = keys.scene_placement_gen_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        raw = await self.redis.get(k)
        return int(_as_text(raw)) if raw else 0

    async def allocate_placement_generation(self, zone_id: int) -> int:
        k = keys.world_placement_seq_key(zone_id)
        keys.assert_control_domain_keys((k,))
        return int(await self.redis.incr(k))

    async def reassign_scene_node(
        self, *, zone_id: int, scene_id: int, old_node_id: str, new_node_id: str, placement_generation: int,
    ) -> bool:
        script_keys = [
            keys.scene_node_key(zone_id, scene_id),
            keys.node_scenes_key(zone_id, old_node_id),
            keys.node_scene_count_key(zone_id, old_node_id),
            keys.node_scenes_key(zone_id, new_node_id),
            keys.node_scene_count_key(zone_id, new_node_id),
            keys.scene_placement_gen_key(zone_id, scene_id),
        ]
        keys.assert_control_domain_keys(script_keys)
        res = await self._reassign_scene_script(
            keys=script_keys,
            args=[old_node_id, new_node_id, str(scene_id), str(placement_generation)],
        )
        return _as_int(res) == 1

    async def get_world_channels(self, zone_id: int, conf_id: int) -> list[str]:
        k = keys.world_channels_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return sorted(_as_text(m) for m in await self.redis.smembers(k))

    async def get_world_draining_channels(self, zone_id: int, conf_id: int) -> list[str]:
        k = keys.world_draining_set_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return sorted(_as_text(m) for m in await self.redis.smembers(k))

    async def is_world_channel_active(self, zone_id: int, conf_id: int, scene_id: int) -> bool:
        k = keys.world_channels_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return bool(await self.redis.sismember(k, str(scene_id)))

    async def is_world_channel_draining(self, zone_id: int, conf_id: int, scene_id: int) -> bool:
        k = keys.world_draining_set_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return bool(await self.redis.sismember(k, str(scene_id)))

    async def allocate_world_channel_line_numbers(
        self, zone_id: int, conf_id: int, scene_ids: Sequence[int],
    ) -> dict[int, int]:
        ids = _sorted_unique_scene_ids(scene_ids)
        if not ids:
            return {}
        k = keys.world_channel_lineno_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        raw = await self._alloc_lineno_script(keys=[k], args=[str(sid) for sid in ids])
        return _parse_line_numbers_reply(raw, ids)

    async def set_world_channel_directory(self, zone_id: int, conf_id: int, payload: bytes, ttl_sec: int) -> None:
        k = keys.world_channel_directory_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.set(k, payload, ex=max(1, int(ttl_sec)))

    async def get_world_channel_directory(self, zone_id: int, conf_id: int) -> bytes | None:
        k = keys.world_channel_directory_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        raw = await self.redis.get(k)
        return None if raw is None else _as_bytes(raw)

    async def get_or_seed_desired_world_channels(self, zone_id: int, conf_id: int, seed_count: int) -> int:
        k = keys.world_desired_channels_key(zone_id)
        keys.assert_control_domain_keys((k,))
        field = str(conf_id)
        raw = await self.redis.hget(k, field)
        if raw is not None:
            try:
                parsed = int(_as_text(raw))
                if parsed >= 1:
                    return parsed
            except ValueError:
                pass
        seed = max(1, seed_count)
        await self.redis.hsetnx(k, field, str(seed))
        raw_after = await self.redis.hget(k, field)
        if raw_after is not None:
            try:
                parsed = int(_as_text(raw_after))
                if parsed >= 1:
                    return parsed
            except ValueError:
                pass
        return seed

    async def acquire_world_init_lock(self, zone_id: int, token: str, ttl_sec: int) -> bool:
        k = keys.world_init_lock_key(zone_id)
        keys.assert_control_domain_keys((k,))
        ok = await self.redis.set(k, token, ex=ttl_sec, nx=True)
        if ok:
            return True
        holder = _as_text(await self.redis.get(k))
        return holder == token

    async def release_world_init_lock(self, zone_id: int, token: str) -> bool:
        k = keys.world_init_lock_key(zone_id)
        keys.assert_control_domain_keys((k,))
        res = await self._release_init_lock_script(keys=[k], args=[token])
        return _as_int(res) == 1

    async def _run_registration(self, script, script_keys: list[str], zone_id: int, scene_id: int,
                                node_id: str, extra_args: list[object]) -> RegistrationOutcome:
        keys.assert_control_domain_keys(script_keys)
        token = str(uuid.uuid4())
        args = [str(scene_id), token, config.SCENE_REGISTRATION_RECORD_TTL_SEC, node_id, "", "", *extra_args]
        try:
            raw = await script(keys=script_keys, args=args)
            return _parse_registration_reply(raw)
        except Exception as exc:
            logger.error(
                "scene registration outcome unknown zone=%d scene=%d node=%s error=%s",
                zone_id, scene_id, node_id, exc,
            )
            abort_keys = [keys.scene_registration_record_key(zone_id, scene_id), keys.scene_node_key(zone_id, scene_id)]
            keys.assert_control_domain_keys(abort_keys)
            try:
                raw = await self._abort_reg_script(
                    keys=abort_keys,
                    args=[token, config.SCENE_REGISTRATION_RECORD_TTL_SEC, node_id],
                )
                return _parse_registration_reply(raw)
            except Exception as abort_exc:
                logger.error(
                    "scene registration abort failed zone=%d scene=%d node=%s error=%s",
                    zone_id, scene_id, node_id, abort_exc,
                )
                return RegistrationOutcome.UNKNOWN

    def _base_registration_keys(self, zone_id: int, scene_id: int, node_id: str) -> list[str]:
        return [
            keys.scene_node_key(zone_id, scene_id),
            keys.node_scene_count_key(zone_id, node_id),
            keys.node_scenes_key(zone_id, node_id),
            keys.scene_agones_gs_key(zone_id, scene_id),
            keys.scene_registration_record_key(zone_id, scene_id),
            keys.scene_teardown_job_key(zone_id, scene_id),
            keys.agones_room_holds_key(zone_id),
        ]

    async def register_instance(
        self, *, zone_id: int, scene_id: int, node_id: str, created_at: int,
        mirror: bool, source_scene_id: int,
    ) -> RegistrationOutcome:
        script_keys = self._base_registration_keys(zone_id, scene_id, node_id) + [
            keys.active_instances_key(zone_id),
            keys.scene_mirror_flag_key(zone_id, scene_id),
            keys.scene_source_key(zone_id, scene_id),
        ]
        if source_scene_id > 0:
            script_keys.append(keys.scene_mirrors_key(zone_id, source_scene_id))
        return await self._run_registration(
            self._register_instance_script,
            script_keys,
            zone_id,
            scene_id,
            node_id,
            [created_at, "1" if mirror else "0", str(source_scene_id)],
        )

    async def register_world_channel(
        self, *, zone_id: int, scene_id: int, node_id: str, conf_id: int, desired_channels: int,
    ) -> RegistrationOutcome:
        script_keys = self._base_registration_keys(zone_id, scene_id, node_id) + [
            keys.world_channels_key(zone_id, conf_id),
            keys.world_desired_channels_key(zone_id),
        ]
        return await self._run_registration(
            self._register_world_script,
            script_keys,
            zone_id,
            scene_id,
            node_id,
            [str(conf_id), desired_channels],
        )

    async def atomic_add_scene_member(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> int:
        script_keys = [keys.scene_node_key(zone_id, scene_id), keys.instance_members_key(zone_id, scene_id)]
        if node_id:
            script_keys.append(keys.node_members_key(zone_id, node_id))
        keys.assert_control_domain_keys(script_keys)
        return _as_int(await self._atomic_add_member_script(keys=script_keys, args=[str(player_id)]))

    async def add_scene_member(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> None:
        script_keys = _member_keys(zone_id, scene_id, node_id)
        if not script_keys:
            return
        keys.assert_control_domain_keys(script_keys)
        await self._add_member_script(keys=script_keys, args=[str(player_id)])

    async def remove_scene_member_keys(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> None:
        script_keys = _member_keys(zone_id, scene_id, node_id)
        if not script_keys:
            return
        keys.assert_control_domain_keys(script_keys)
        await self._remove_member_script(keys=script_keys, args=[str(player_id)])

    async def get_scene_member_count(self, zone_id: int, scene_id: int) -> int:
        k = keys.instance_members_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return int(await self.redis.scard(k))

    async def reserve_best_world_channel(
        self, candidates: Sequence[WorldChannelCandidate], player_id: int,
    ) -> tuple[int, str, int] | None:
        if not candidates:
            return None
        script_keys: list[str] = []
        for cand in candidates:
            script_keys.append(keys.scene_node_key(cand.zone_id, cand.scene_id))
            script_keys.append(keys.instance_members_key(cand.zone_id, cand.scene_id))
        for cand in candidates:
            script_keys.append(keys.node_members_key(cand.zone_id, cand.node_id))
        keys.assert_control_domain_keys(script_keys)
        args = [cand.node_id for cand in candidates] + [str(player_id)]
        raw = await self._reserve_best_script(keys=script_keys, args=args)
        if not raw or len(raw) < 3:
            return None
        best_idx = int(_as_text(raw[0]))
        if not 1 <= best_idx <= len(candidates):
            return None
        winner = candidates[best_idx - 1]
        return winner.scene_id, winner.node_id, int(_as_text(raw[2]))

    async def list_active_instances_with_scores(self, zone_id: int) -> list[tuple[str, float]]:
        k = keys.active_instances_key(zone_id)
        keys.assert_control_domain_keys((k,))
        rows = await self.redis.zrange(k, 0, -1, withscores=True)
        return [(_as_text(member), float(score)) for member, score in rows]

    async def touch_active_instance(self, zone_id: int, scene_id: int, now_unix_sec: int) -> None:
        k = keys.active_instances_key(zone_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.zadd(k, {str(scene_id): float(now_unix_sec)}, xx=True)

    async def is_active_instance(self, zone_id: int, scene_id: int) -> bool:
        k = keys.active_instances_key(zone_id)
        keys.assert_control_domain_keys((k,))
        return (await self.redis.zscore(k, str(scene_id))) is not None

    def _build_claim_keys(
        self, zone_id: int, scene_id: int, expected_owner: str,
        *, task_fence: DeadNodeTaskFence | None = None,
    ) -> list[str]:
        script_keys = [
            keys.scene_node_key(zone_id, scene_id),
            keys.instance_members_key(zone_id, scene_id),
            keys.active_instances_key(zone_id),
            keys.scene_mirror_flag_key(zone_id, scene_id),
            keys.scene_source_key(zone_id, scene_id),
            keys.scene_agones_gs_key(zone_id, scene_id),
            keys.scene_teardown_job_key(zone_id, scene_id),
            keys.scene_teardown_pending_key(zone_id),
        ]
        if expected_owner:
            script_keys.extend((
                keys.node_scenes_key(zone_id, expected_owner),
                keys.node_scene_count_key(zone_id, expected_owner),
            ))
        if task_fence is not None:
            script_keys.append(task_fence.tasks_key)
        return script_keys

    async def atomic_destroy_instance(
        self, *, zone_id: int, scene_id: int, expected_owner: str, now_unix_sec: int | None = None,
    ) -> SceneClaimResult:
        token = str(uuid.uuid4())
        now_sec = int(time.time()) if now_unix_sec is None else int(now_unix_sec)
        script_keys = self._build_claim_keys(zone_id, scene_id, expected_owner)
        keys.assert_control_domain_keys(script_keys)
        args = [str(scene_id), token, now_sec, "instance", "idle", expected_owner, "1", "0", "", ""]
        raw = await self._destroy_idle_script(keys=script_keys, args=args)
        return _parse_scene_claim_reply(raw, token)

    async def atomic_destroy_instance_force(
        self, *, zone_id: int, scene_id: int, expected_owner: str, kind: str, reason: str,
        count_owner: bool = True, now_unix_sec: int | None = None,
    ) -> SceneClaimResult:
        token = str(uuid.uuid4())
        now_sec = int(time.time()) if now_unix_sec is None else int(now_unix_sec)
        script_keys = self._build_claim_keys(zone_id, scene_id, expected_owner)
        keys.assert_control_domain_keys(script_keys)
        args = [
            str(scene_id), token, now_sec, kind, reason, expected_owner,
            "1" if count_owner else "0", "0", "", "",
        ]
        raw = await self._destroy_force_script(keys=script_keys, args=args)
        return _parse_scene_claim_reply(raw, token)

    async def atomic_destroy_orphan_of_dead_node(
        self, *, zone_id: int, scene_id: int, dead_node: str, task_fence: DeadNodeTaskFence,
        now_unix_sec: int | None = None,
    ) -> SceneClaimResult:
        if not dead_node:
            raise ValueError("dead_node must be non-empty")
        token = str(uuid.uuid4())
        now_sec = int(time.time()) if now_unix_sec is None else int(now_unix_sec)
        script_keys = self._build_claim_keys(zone_id, scene_id, dead_node, task_fence=task_fence)
        keys.assert_control_domain_keys(script_keys)
        args = [
            str(scene_id), token, now_sec, "instance", "node_death", dead_node,
            "1", "1", task_fence.member, str(task_fence.queued_at_ms),
        ]
        raw = await self._destroy_orphan_script(keys=script_keys, args=args)
        return _parse_scene_claim_reply(raw, token)

    async def finish_scene_teardown(self, zone_id: int, scene_id: int, token: str) -> bool:
        script_keys = [keys.scene_teardown_job_key(zone_id, scene_id), keys.scene_teardown_pending_key(zone_id)]
        keys.assert_control_domain_keys(script_keys)
        res = await self._finish_teardown_script(keys=script_keys, args=[str(scene_id), token])
        return _as_int(res) == 1

    async def list_due_teardown_jobs(self, zone_id: int, max_claimed_at_sec: int, limit: int) -> list[tuple[str, float]]:
        k = keys.scene_teardown_pending_key(zone_id)
        keys.assert_control_domain_keys((k,))
        rows = await self.redis.zrangebyscore(
            k, "-inf", float(max_claimed_at_sec), start=0, num=max(1, int(limit)), withscores=True,
        )
        return [(_as_text(member), float(score)) for member, score in rows]

    async def load_scene_teardown_job(self, zone_id: int, scene_id: int) -> SceneTeardownJob | None:
        k = keys.scene_teardown_job_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        fields = await self.redis.hmget(k, "token", "kind", "reason", "reply")
        if not fields or len(fields) < 4 or not fields[0]:
            return None
        token = _as_text(fields[0])
        kind = _as_text(fields[1])
        reason = _as_text(fields[2])
        reply = json.loads(_as_text(fields[3]))
        claim = _parse_scene_claim_reply(reply, token)
        if not claim.claimed:
            raise ValueError(f"teardown job for scene {scene_id} does not hold a valid claim")
        return SceneTeardownJob(zone_id=zone_id, scene_id=scene_id, kind=kind, reason=reason, claim=claim)

    async def drop_dangling_teardown(self, zone_id: int, scene_id: int) -> bool:
        script_keys = [keys.scene_teardown_job_key(zone_id, scene_id), keys.scene_teardown_pending_key(zone_id)]
        keys.assert_control_domain_keys(script_keys)
        res = await self._drop_dangling_teardown_script(keys=script_keys, args=[str(scene_id)])
        return _as_int(res) == 1

    async def enqueue_dead_node_reconcile(
        self, zone_id: int, node_id: str, *, queued_at_ms: int, detach: bool = True, node_uuid: str = "",
    ) -> int:
        script_keys = [
            keys.dead_node_reconcile_tasks_key(zone_id),
            keys.node_reconcile_scenes_key(zone_id, node_id),
            keys.node_scenes_key(zone_id, node_id),
            keys.node_load_key(zone_id),
            keys.node_reconcile_instance_key(zone_id, node_id),
        ]
        keys.assert_control_domain_keys(script_keys)
        args = [
            keys.pending_dead_node_member(zone_id, node_id),
            str(int(queued_at_ms)),
            "index",
            node_id if detach else "",
            node_uuid,
        ]
        return _as_int(await self._enqueue_dead_node_script(keys=script_keys, args=args))

    async def withdraw_dead_node_for_returning_instance(self, zone_id: int, node_id: str, node_uuid: str) -> int:
        script_keys = [
            keys.dead_node_reconcile_tasks_key(zone_id),
            keys.node_reconcile_scenes_key(zone_id, node_id),
            keys.node_reconcile_instance_key(zone_id, node_id),
            keys.node_death_at_key(zone_id, node_id),
        ]
        keys.assert_control_domain_keys(script_keys)
        args = [keys.pending_dead_node_member(zone_id, node_id), node_uuid]
        return _as_int(await self._withdraw_dead_node_script(keys=script_keys, args=args))

    async def list_dead_node_reconcile_tasks(self, zone_id: int) -> tuple[list[DeadNodeReconcileTask], list[str]]:
        k = keys.dead_node_reconcile_tasks_key(zone_id)
        keys.assert_control_domain_keys((k,))
        rows = await self.redis.zrange(k, 0, -1, withscores=True)
        tasks: list[DeadNodeReconcileTask] = []
        malformed: list[str] = []
        for raw_member, score in rows:
            member = _as_text(raw_member)
            parsed = keys.parse_dead_node_reconcile_member(member)
            if parsed is None or parsed[0] != zone_id:
                malformed.append(member)
                continue
            tasks.append(DeadNodeReconcileTask(
                member=member, zone_id=parsed[0], node_id=parsed[1], queued_at_ms=int(score),
            ))
        return tasks, malformed

    async def remove_dead_node_reconcile_member(self, zone_id: int, member: str) -> None:
        k = keys.dead_node_reconcile_tasks_key(zone_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.zrem(k, member)

    async def get_node_reconcile_scenes(self, zone_id: int, node_id: str) -> list[str]:
        k = keys.node_reconcile_scenes_key(zone_id, node_id)
        keys.assert_control_domain_keys((k,))
        return sorted(_as_text(m) for m in await self.redis.smembers(k))

    async def remove_node_reconcile_scenes(self, zone_id: int, node_id: str, scene_ids: Sequence[str]) -> None:
        if not scene_ids:
            return
        k = keys.node_reconcile_scenes_key(zone_id, node_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.srem(k, *scene_ids)

    async def remove_node_scenes(self, zone_id: int, node_id: str, scene_ids: Sequence[str]) -> None:
        if not scene_ids:
            return
        k = keys.node_scenes_key(zone_id, node_id)
        keys.assert_control_domain_keys((k,))
        await self.redis.srem(k, *scene_ids)

    async def finish_dead_node_reconcile(
        self, zone_id: int, node_id: str, member: str, queued_at_ms: int, *, abandon: bool = False,
    ) -> bool:
        script_keys = [
            keys.node_reconcile_scenes_key(zone_id, node_id),
            keys.node_scene_count_key(zone_id, node_id),
            keys.node_members_key(zone_id, node_id),
            keys.dead_node_reconcile_tasks_key(zone_id),
            keys.node_reconcile_instance_key(zone_id, node_id),
        ]
        keys.assert_control_domain_keys(script_keys)
        args = [member, "1" if abandon else "0", str(int(queued_at_ms))]
        return _as_int(await self._finish_dead_node_script(keys=script_keys, args=args)) == 1

    async def get_gate_marks(self, zone_id: int, gate_node_ids: Sequence[int]) -> tuple[set[int], set[int]]:
        if not gate_node_ids:
            return set(), set()
        mget_keys = [keys.gate_draining_key(zone_id, gid) for gid in gate_node_ids] + [
            keys.gate_stale_key(zone_id, gid) for gid in gate_node_ids
        ]
        keys.assert_control_domain_keys(mget_keys)
        values = await self.redis.mget(*mget_keys)
        draining: set[int] = set()
        stale: set[int] = set()
        n = len(gate_node_ids)
        for idx, raw in enumerate(values):
            if not raw:
                continue
            if idx < n:
                draining.add(gate_node_ids[idx])
            else:
                stale.add(gate_node_ids[idx - n])
        return draining, stale


# ==============================================================================
# 二、RedisPlayerPlacementStore（玩家域 Redis + Go 同源 Lua）
# ==============================================================================

class RedisPlayerPlacementStore:
    """基于 redis.asyncio 与玩家域 Lua 脚本的 PlayerPlacementStore 生产实现。"""

    def __init__(self, redis_client) -> None:
        self.redis = redis_client
        self._observe_rev_script = redis_client.register_script(lua.LUA_OBSERVE_LOCATION_REVISION)
        self._snapshot_script = redis_client.register_script(lua.LUA_READ_LOCATION_SNAPSHOT)
        self._mint_script = redis_client.register_script(lua.LUA_MINT_EPOCH_AND_SET_LOCATION)
        self._set_loc_script = redis_client.register_script(lua.LUA_SET_LOCATION_IF_EPOCH)
        self._rollback_script = redis_client.register_script(lua.LUA_ROLLBACK_PLAYER_PLACEMENT)
        self._delete_loc_script = redis_client.register_script(lua.LUA_DELETE_PLAYER_LOCATION_IF_UNCHANGED)
        self._note_wait_script = redis_client.register_script(lua.LUA_NOTE_LEAVE_RELEASE_WAIT)
        self._del_dedupe_script = redis_client.register_script(lua.LUA_DELETE_ENTER_SCENE_DEDUPE_IF_VALUE)
        self._complete_dedupe_script = redis_client.register_script(lua.LUA_COMPLETE_ENTER_SCENE_DEDUPE)

    async def observe_location_revision(self, player_id: int, revision: int) -> int:
        k = keys.location_revision_floor_key(player_id)
        keys.assert_player_domain_keys((k,))
        return _as_int(await self._observe_rev_script(keys=[k], args=[keys.revision_text(revision)]))

    async def read_location_snapshot(self, player_id: int) -> tuple[bytes, str]:
        script_keys = list(keys.location_snapshot_keys(player_id))
        keys.assert_player_domain_keys(script_keys)
        out = await self._snapshot_script(keys=script_keys, args=[])
        if not isinstance(out, (list, tuple)) or len(out) != 2:
            raise RuntimeError(f"unexpected location snapshot reply: {out!r}")
        return _as_bytes(out[0]), _as_text(out[1])

    async def get_owner_epoch(self, player_id: int) -> int:
        k = keys.owner_epoch_key(player_id)
        keys.assert_player_domain_keys((k,))
        return keys.parse_epoch(await self.redis.get(k))

    async def setnx_owner_epoch(self, player_id: int, epoch: int) -> bool:
        k = keys.owner_epoch_key(player_id)
        keys.assert_player_domain_keys((k,))
        return bool(await self.redis.set(k, str(epoch), nx=True))

    async def get_handoff_marker(self, player_id: int) -> str:
        k = keys.handoff_key(player_id)
        keys.assert_player_domain_keys((k,))
        return _as_text(await self.redis.get(k))

    async def get_holder_lease(self, player_id: int) -> str:
        k = keys.holder_lease_key(player_id)
        keys.assert_player_domain_keys((k,))
        return _as_text(await self.redis.get(k))

    async def mint_epoch_and_set_location(
        self, *, player_id: int, observed_epoch: int, location_bytes: bytes,
        required_marker: str, expect_mode: str, expect_raw: bytes,
        owner_revision: int, new_tag: str, observed_tag: str, target_epoch: int,
    ) -> int:
        script_keys = list(keys.placement_keys(player_id))
        keys.assert_player_domain_keys(script_keys)
        args = [
            str(observed_epoch),
            location_bytes,
            required_marker,
            expect_mode,
            expect_raw,
            keys.optional_revision_text(owner_revision),
            new_tag,
            observed_tag,
            str(target_epoch),
        ]
        return _as_int(await self._mint_script(keys=script_keys, args=args))

    async def set_location_if_epoch(
        self, *, player_id: int, observed_epoch: int, location_bytes: bytes,
        expect_mode: str, expect_raw: bytes, owner_revision: int, new_tag: str, observed_tag: str,
    ) -> int:
        script_keys = list(keys.placement_keys(player_id))
        keys.assert_player_domain_keys(script_keys)
        args = [
            str(observed_epoch),
            location_bytes,
            "",
            expect_mode,
            expect_raw,
            keys.optional_revision_text(owner_revision),
            new_tag,
            observed_tag,
        ]
        return _as_int(await self._set_loc_script(keys=script_keys, args=args))

    async def rollback_player_placement(
        self, *, player_id: int, placed_raw: bytes, restored_raw: bytes, placed_epoch: int,
        mode: str, required_marker: str, forward_marker: str, handoff_ttl_sec: int,
        owner_revision: int, placed_tag: str, restored_tag: str,
        void_epoch: int | None = None, observed_raw: bytes = b"",
        observed_tag: str = "", void_epoch_after: int | None = None,
    ) -> int:
        script_keys = list(keys.rollback_placement_keys(player_id))
        keys.assert_player_domain_keys(script_keys)
        args: list[object] = [
            placed_raw,
            restored_raw,
            str(placed_epoch),
            mode,
            required_marker,
            forward_marker,
            str(handoff_ttl_sec),
            keys.optional_revision_text(owner_revision),
            placed_tag,
            restored_tag,
        ]
        if void_epoch is not None and void_epoch_after is not None:
            args.extend((str(void_epoch), observed_raw, observed_tag, str(void_epoch_after)))
        return _as_int(await self._rollback_script(keys=script_keys, args=args))

    async def delete_location_if_unchanged(
        self, *, player_id: int, expected_raw: bytes, mode: str, epoch: int,
        expected_tag: str, owner_revision: int,
    ) -> int:
        script_keys = list(keys.leave_delete_keys(player_id))
        keys.assert_player_domain_keys(script_keys)
        args = [
            expected_raw,
            mode,
            str(epoch),
            expected_tag,
            keys.optional_revision_text(owner_revision),
        ]
        return _as_int(await self._delete_loc_script(keys=script_keys, args=args))

    async def note_leave_release_wait(self, player_id: int, scope: str, ttl_sec: int) -> int:
        k = keys.leave_release_wait_key(player_id)
        keys.assert_player_domain_keys((k,))
        return _as_int(await self._note_wait_script(keys=[k], args=[scope, str(ttl_sec)]))

    async def has_channel_switch_cooldown(self, player_id: int) -> bool:
        k = keys.channel_switch_cooldown_key(player_id)
        keys.assert_player_domain_keys((k,))
        return int(await self.redis.exists(k)) == 1

    async def set_channel_switch_cooldown(self, player_id: int, ttl_sec: int) -> None:
        k = keys.channel_switch_cooldown_key(player_id)
        keys.assert_player_domain_keys((k,))
        await self.redis.set(k, "1", ex=ttl_sec)

    async def setnx_enter_dedupe(self, player_id: int, request_id: str, value: str, ttl_sec: int) -> bool:
        k = keys.enter_scene_dedup_key(player_id, request_id)
        keys.assert_player_domain_keys((k,))
        return bool(await self.redis.set(k, value, ex=ttl_sec, nx=True))

    async def get_enter_dedupe(self, player_id: int, request_id: str) -> str:
        k = keys.enter_scene_dedup_key(player_id, request_id)
        keys.assert_player_domain_keys((k,))
        return _as_text(await self.redis.get(k))

    async def delete_enter_dedupe_if_value(self, player_id: int, request_id: str, expected: str) -> bool:
        k = keys.enter_scene_dedup_key(player_id, request_id)
        keys.assert_player_domain_keys((k,))
        return _as_int(await self._del_dedupe_script(keys=[k], args=[expected])) == 1

    async def complete_enter_dedupe(
        self, *, player_id: int, request_id: str, expected_owner: str,
        cached_value: str, ttl_sec: int, owner_revision: int,
    ) -> bool:
        script_keys = list(keys.enter_scene_dedupe_complete_keys(player_id, request_id))
        keys.assert_player_domain_keys(script_keys)
        args = [expected_owner, cached_value, ttl_sec, keys.revision_text(owner_revision)]
        return _as_int(await self._complete_dedupe_script(keys=script_keys, args=args)) == 1


# ==============================================================================
# 三、确定性内存孪生：MemorySceneControlStore 与 MemoryPlayerPlacementStore
# ==============================================================================

def _member_keys(zone_id: int, scene_id: int, node_id: str) -> list[str]:
    if zone_id <= 0:
        return []
    out: list[str] = []
    if scene_id > 0:
        out.append(keys.instance_members_key(zone_id, scene_id))
    if node_id:
        out.append(keys.node_members_key(zone_id, node_id))
    return out


class MemorySceneControlStore:
    """与 RedisSceneControlStore 语义等价的内存孪生，用于确定性单测。"""

    def __init__(self, *, clock_ms: Callable[[], int] | None = None) -> None:
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.strings: dict[str, bytes] = {}
        self.expires_ms: dict[str, int] = {}
        self.sets: dict[str, set[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}

    def _purge_if_expired(self, key: str) -> None:
        exp = self.expires_ms.get(key)
        if exp is not None and self._clock_ms() >= exp:
            self.expires_ms.pop(key, None)
            self.strings.pop(key, None)
            self.sets.pop(key, None)
            self.zsets.pop(key, None)
            self.hashes.pop(key, None)

    def _get_str(self, key: str) -> str | None:
        self._purge_if_expired(key)
        raw = self.strings.get(key)
        return None if raw is None else raw.decode("utf-8", errors="replace")

    def _set_str(self, key: str, value: str, *, ex: int | None = None) -> None:
        self.strings[key] = value.encode("utf-8")
        if ex is not None:
            self.expires_ms[key] = self._clock_ms() + int(ex) * 1000
        else:
            self.expires_ms.pop(key, None)

    def _del(self, *del_keys: str) -> None:
        for key in del_keys:
            self.strings.pop(key, None)
            self.sets.pop(key, None)
            self.zsets.pop(key, None)
            self.hashes.pop(key, None)
            self.expires_ms.pop(key, None)

    def _sadd(self, key: str, member: str) -> None:
        self.sets.setdefault(key, set()).add(member)

    def _srem(self, key: str, member: str) -> None:
        s = self.sets.get(key)
        if s is not None:
            s.discard(member)
            if not s:
                self.sets.pop(key, None)

    def _zadd(self, key: str, member: str, score: float) -> None:
        self.zsets.setdefault(key, {})[member] = float(score)

    def _zrem(self, key: str, member: str) -> int:
        z = self.zsets.get(key)
        if z is None or member not in z:
            return 0
        del z[member]
        if not z:
            self.zsets.pop(key, None)
        return 1

    # ── 测试辅助写入口与直接字节读（供 nodes/scene_channels.py 消费者直读） ──────
    async def get(self, key: str) -> bytes | None:
        self._purge_if_expired(key)
        return self.strings.get(key)

    def get_ttl_remaining_sec(self, key: str) -> int | None:
        self._purge_if_expired(key)
        exp = self.expires_ms.get(key)
        if exp is None:
            return None
        return max(0, (exp - self._clock_ms()) // 1000)

    def mark_channel_draining(self, zone_id: int, conf_id: int, scene_id: int) -> None:
        self._srem(keys.world_channels_key(zone_id, conf_id), str(scene_id))
        self._sadd(keys.world_draining_set_key(zone_id, conf_id), str(scene_id))
        self._set_str(keys.scene_draining_key(zone_id, scene_id), "1")

    def mark_gate_draining(self, zone_id: int, gate_node_id: int) -> None:
        self._set_str(keys.gate_draining_key(zone_id, gate_node_id), str(self._clock_ms() // 1000))

    def mark_gate_stale(self, zone_id: int, gate_node_id: int) -> None:
        self._set_str(keys.gate_stale_key(zone_id, gate_node_id), "1")

    # ── Protocol 方法实现 ─────────────────────────────────────────────────────
    async def upsert_scene_node(
        self, zone_id: int, node_id: str, *, score: float, scene_node_type: int, preserve_death_at: bool = False,
    ) -> None:
        load_k = keys.node_load_key(zone_id)
        type_k = keys.node_scene_node_type_key(zone_id, node_id)
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((load_k, type_k, death_k))
        self._zadd(load_k, node_id, score)
        self._set_str(type_k, str(scene_node_type))
        if not preserve_death_at:
            self._del(death_k)

    async def remove_scene_node(self, zone_id: int, node_id: str, *, death_at_unix_sec: int | None = None) -> None:
        load_k = keys.node_load_key(zone_id)
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((load_k, death_k))
        if death_at_unix_sec is not None:
            self._set_str(death_k, str(death_at_unix_sec))
        self._zrem(load_k, node_id)

    async def get_node_load_pairs(self, zone_id: int) -> list[tuple[str, float]]:
        load_k = keys.node_load_key(zone_id)
        keys.assert_control_domain_keys((load_k,))
        z = self.zsets.get(load_k, {})
        return sorted(z.items(), key=lambda item: (item[1], item[0]))

    async def is_node_in_load_set(self, zone_id: int, node_id: str) -> bool:
        load_k = keys.node_load_key(zone_id)
        keys.assert_control_domain_keys((load_k,))
        return node_id in self.zsets.get(load_k, {})

    async def get_node_scene_type(self, zone_id: int, node_id: str) -> int | None:
        type_k = keys.node_scene_node_type_key(zone_id, node_id)
        keys.assert_control_domain_keys((type_k,))
        val = self._get_str(type_k)
        if not val:
            return None
        try:
            return int(val)
        except ValueError:
            return None

    async def get_node_scene_count(self, zone_id: int, node_id: str) -> int | None:
        cnt_k = keys.node_scene_count_key(zone_id, node_id)
        keys.assert_control_domain_keys((cnt_k,))
        val = self._get_str(cnt_k)
        if not val:
            return None
        try:
            return int(val)
        except ValueError:
            return None

    async def get_node_death_at(self, zone_id: int, node_id: str) -> int | None:
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((death_k,))
        val = self._get_str(death_k)
        if not val:
            return None
        try:
            return int(val)
        except ValueError:
            return None

    async def get_scene_node(self, zone_id: int, scene_id: int) -> str:
        k = keys.scene_node_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return self._get_str(k) or ""

    async def scene_node_exists(self, zone_id: int, scene_id: int) -> bool:
        return bool(await self.get_scene_node(zone_id, scene_id))

    async def get_scene_mirrors(self, zone_id: int, source_scene_id: int) -> list[str]:
        k = keys.scene_mirrors_key(zone_id, source_scene_id)
        keys.assert_control_domain_keys((k,))
        return sorted(self.sets.get(k, set()))

    async def remove_scene_mirror(self, zone_id: int, source_scene_id: int, mirror_scene_id: int) -> None:
        k = keys.scene_mirrors_key(zone_id, source_scene_id)
        keys.assert_control_domain_keys((k,))
        self._srem(k, str(mirror_scene_id))

    async def delete_scene_mirrors_index(self, zone_id: int, source_scene_id: int) -> None:
        k = keys.scene_mirrors_key(zone_id, source_scene_id)
        keys.assert_control_domain_keys((k,))
        self._del(k)

    async def is_scene_draining(self, zone_id: int, scene_id: int) -> bool:
        k = keys.scene_draining_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return self._get_str(k) is not None

    async def is_scene_mirror(self, zone_id: int, scene_id: int) -> bool:
        k = keys.scene_mirror_flag_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return self._get_str(k) == "1"

    async def get_scene_placement_gen(self, zone_id: int, scene_id: int) -> int:
        k = keys.scene_placement_gen_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        val = self._get_str(k)
        return int(val) if val else 0

    async def allocate_placement_generation(self, zone_id: int) -> int:
        k = keys.world_placement_seq_key(zone_id)
        keys.assert_control_domain_keys((k,))
        nxt = int(self._get_str(k) or "0") + 1
        self._set_str(k, str(nxt))
        return nxt

    async def reassign_scene_node(
        self, *, zone_id: int, scene_id: int, old_node_id: str, new_node_id: str, placement_generation: int,
    ) -> bool:
        node_k = keys.scene_node_key(zone_id, scene_id)
        old_scenes_k = keys.node_scenes_key(zone_id, old_node_id)
        old_cnt_k = keys.node_scene_count_key(zone_id, old_node_id)
        new_scenes_k = keys.node_scenes_key(zone_id, new_node_id)
        new_cnt_k = keys.node_scene_count_key(zone_id, new_node_id)
        gen_k = keys.scene_placement_gen_key(zone_id, scene_id)
        keys.assert_control_domain_keys((node_k, old_scenes_k, old_cnt_k, new_scenes_k, new_cnt_k, gen_k))
        if self._get_str(node_k) != old_node_id:
            return False
        self._set_str(node_k, new_node_id)
        self._srem(old_scenes_k, str(scene_id))
        old_cnt = int(self._get_str(old_cnt_k) or "0")
        if old_cnt > 0:
            self._set_str(old_cnt_k, str(old_cnt - 1))
        self._sadd(new_scenes_k, str(scene_id))
        new_cnt = int(self._get_str(new_cnt_k) or "0")
        self._set_str(new_cnt_k, str(new_cnt + 1))
        if placement_generation > 0:
            self._set_str(gen_k, str(placement_generation))
        return True

    async def get_world_channels(self, zone_id: int, conf_id: int) -> list[str]:
        k = keys.world_channels_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return sorted(self.sets.get(k, set()))

    async def get_world_draining_channels(self, zone_id: int, conf_id: int) -> list[str]:
        k = keys.world_draining_set_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return sorted(self.sets.get(k, set()))

    async def is_world_channel_active(self, zone_id: int, conf_id: int, scene_id: int) -> bool:
        k = keys.world_channels_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return str(scene_id) in self.sets.get(k, set())

    async def is_world_channel_draining(self, zone_id: int, conf_id: int, scene_id: int) -> bool:
        k = keys.world_draining_set_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return str(scene_id) in self.sets.get(k, set())

    async def allocate_world_channel_line_numbers(
        self, zone_id: int, conf_id: int, scene_ids: Sequence[int],
    ) -> dict[int, int]:
        ids = _sorted_unique_scene_ids(scene_ids)
        if not ids:
            return {}
        k = keys.world_channel_lineno_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        wanted = {str(sid) for sid in ids}
        table = self.hashes.setdefault(k, {})
        kept: dict[str, int] = {}
        used: set[int] = set()
        for field in list(table.keys()):
            raw_val = _as_text(table[field])
            try:
                line_no = int(raw_val)
            except ValueError:
                line_no = 0
            if field in wanted and 1 <= line_no <= 0xFFFFFFFF and line_no not in used:
                kept[field] = line_no
                used.add(line_no)
            else:
                del table[field]
        out: dict[int, int] = {}
        candidate = 1
        for sid in ids:
            sid_str = str(sid)
            line_no = kept.get(sid_str)
            if line_no is None:
                while candidate in used:
                    candidate += 1
                line_no = candidate
                used.add(line_no)
                kept[sid_str] = line_no
                table[sid_str] = str(line_no).encode("ascii")
            out[sid] = line_no
        return out

    async def set_world_channel_directory(self, zone_id: int, conf_id: int, payload: bytes, ttl_sec: int) -> None:
        k = keys.world_channel_directory_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        self.strings[k] = bytes(payload)
        self.expires_ms[k] = self._clock_ms() + max(1, int(ttl_sec)) * 1000

    async def get_world_channel_directory(self, zone_id: int, conf_id: int) -> bytes | None:
        k = keys.world_channel_directory_key(zone_id, conf_id)
        keys.assert_control_domain_keys((k,))
        return await self.get(k)

    async def get_or_seed_desired_world_channels(self, zone_id: int, conf_id: int, seed_count: int) -> int:
        k = keys.world_desired_channels_key(zone_id)
        keys.assert_control_domain_keys((k,))
        h = self.hashes.setdefault(k, {})
        field = str(conf_id)
        existing = h.get(field)
        if existing is not None:
            try:
                parsed = int(existing.decode("utf-8"))
                if parsed >= 1:
                    return parsed
            except ValueError:
                pass
        seed = max(1, seed_count)
        h.setdefault(field, str(seed).encode("utf-8"))
        return int(h[field].decode("utf-8"))

    async def acquire_world_init_lock(self, zone_id: int, token: str, ttl_sec: int) -> bool:
        k = keys.world_init_lock_key(zone_id)
        keys.assert_control_domain_keys((k,))
        current = self._get_str(k)
        if current is None:
            self._set_str(k, token, ex=ttl_sec)
            return True
        return current == token

    async def release_world_init_lock(self, zone_id: int, token: str) -> bool:
        k = keys.world_init_lock_key(zone_id)
        keys.assert_control_domain_keys((k,))
        if self._get_str(k) == token:
            self._del(k)
            return True
        return False

    def _guard_registration(self, zone_id: int, scene_id: int, node_id: str, token: str) -> RegistrationOutcome | None:
        reg_k = keys.scene_registration_record_key(zone_id, scene_id)
        node_k = keys.scene_node_key(zone_id, scene_id)
        td_k = keys.scene_teardown_job_key(zone_id, scene_id)
        prior = self._get_str(reg_k)
        if prior is not None:
            rec = json.loads(prior)
            if rec.get("token") != token:
                return RegistrationOutcome.CONFLICT
            reply = rec.get("reply") or []
            if reply and reply[0] == "registered" and self._get_str(node_k) != node_id:
                return RegistrationOutcome.CONFLICT
            return _parse_registration_reply(reply)
        if self._get_str(node_k) is not None or td_k in self.hashes:
            return RegistrationOutcome.CONFLICT
        return None

    def _commit_registration_base(self, zone_id: int, scene_id: int, node_id: str, token: str) -> None:
        self._set_str(keys.scene_node_key(zone_id, scene_id), node_id)
        cnt_k = keys.node_scene_count_key(zone_id, node_id)
        cur_cnt = int(self._get_str(cnt_k) or "0")
        self._set_str(cnt_k, str(cur_cnt + 1))
        self._sadd(keys.node_scenes_key(zone_id, node_id), str(scene_id))
        self._set_str(
            keys.scene_registration_record_key(zone_id, scene_id),
            json.dumps({"token": token, "reply": ["registered"]}),
            ex=config.SCENE_REGISTRATION_RECORD_TTL_SEC,
        )

    async def register_instance(
        self, *, zone_id: int, scene_id: int, node_id: str, created_at: int,
        mirror: bool, source_scene_id: int,
    ) -> RegistrationOutcome:
        check_keys = [
            keys.scene_node_key(zone_id, scene_id),
            keys.node_scene_count_key(zone_id, node_id),
            keys.node_scenes_key(zone_id, node_id),
            keys.active_instances_key(zone_id),
        ]
        if source_scene_id > 0:
            check_keys.append(keys.scene_mirrors_key(zone_id, source_scene_id))
        keys.assert_control_domain_keys(check_keys)

        token = str(uuid.uuid4())
        guarded = self._guard_registration(zone_id, scene_id, node_id, token)
        if guarded is not None:
            return guarded
        self._commit_registration_base(zone_id, scene_id, node_id, token)
        self._zadd(keys.active_instances_key(zone_id), str(scene_id), float(created_at))
        if mirror:
            self._set_str(keys.scene_mirror_flag_key(zone_id, scene_id), "1")
        if source_scene_id > 0:
            self._set_str(keys.scene_source_key(zone_id, scene_id), str(source_scene_id))
            self._sadd(keys.scene_mirrors_key(zone_id, source_scene_id), str(scene_id))
        return RegistrationOutcome.REGISTERED

    async def register_world_channel(
        self, *, zone_id: int, scene_id: int, node_id: str, conf_id: int, desired_channels: int,
    ) -> RegistrationOutcome:
        ch_k = keys.world_channels_key(zone_id, conf_id)
        desired_k = keys.world_desired_channels_key(zone_id)
        keys.assert_control_domain_keys((keys.scene_node_key(zone_id, scene_id), ch_k, desired_k))

        token = str(uuid.uuid4())
        guarded = self._guard_registration(zone_id, scene_id, node_id, token)
        if guarded is not None:
            return guarded
        h = self.hashes.get(desired_k, {})
        raw_desired = h.get(str(conf_id))
        desired = int(raw_desired.decode("utf-8")) if raw_desired else desired_channels
        if desired < 1:
            desired = desired_channels
        if len(self.sets.get(ch_k, set())) >= desired:
            return RegistrationOutcome.OVER_DESIRED
        self._commit_registration_base(zone_id, scene_id, node_id, token)
        self._sadd(ch_k, str(scene_id))
        return RegistrationOutcome.REGISTERED

    async def atomic_add_scene_member(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> int:
        node_k = keys.scene_node_key(zone_id, scene_id)
        mem_k = keys.instance_members_key(zone_id, scene_id)
        check_keys = [node_k, mem_k]
        if node_id:
            check_keys.append(keys.node_members_key(zone_id, node_id))
        keys.assert_control_domain_keys(check_keys)
        if self._get_str(node_k) is None:
            return -1
        self._sadd(mem_k, str(player_id))
        if node_id:
            self._sadd(keys.node_members_key(zone_id, node_id), str(player_id))
        return len(self.sets.get(mem_k, set()))

    async def add_scene_member(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> None:
        mkeys = _member_keys(zone_id, scene_id, node_id)
        if not mkeys:
            return
        keys.assert_control_domain_keys(mkeys)
        for key in mkeys:
            self._sadd(key, str(player_id))

    async def remove_scene_member_keys(self, zone_id: int, scene_id: int, node_id: str, player_id: int) -> None:
        mkeys = _member_keys(zone_id, scene_id, node_id)
        if not mkeys:
            return
        keys.assert_control_domain_keys(mkeys)
        for key in mkeys:
            self._srem(key, str(player_id))

    async def get_scene_member_count(self, zone_id: int, scene_id: int) -> int:
        k = keys.instance_members_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        return len(self.sets.get(k, set()))

    async def reserve_best_world_channel(
        self, candidates: Sequence[WorldChannelCandidate], player_id: int,
    ) -> tuple[int, str, int] | None:
        if not candidates:
            return None
        check_keys = [keys.scene_node_key(c.zone_id, c.scene_id) for c in candidates]
        keys.assert_control_domain_keys(check_keys)
        best: WorldChannelCandidate | None = None
        best_count: int | None = None
        for cand in candidates:
            if self._get_str(keys.scene_node_key(cand.zone_id, cand.scene_id)) == cand.node_id:
                cnt = len(self.sets.get(keys.instance_members_key(cand.zone_id, cand.scene_id), set()))
                if best_count is None or cnt < best_count:
                    best = cand
                    best_count = cnt
        if best is None:
            return None
        mem_k = keys.instance_members_key(best.zone_id, best.scene_id)
        node_mem_k = keys.node_members_key(best.zone_id, best.node_id)
        self._sadd(mem_k, str(player_id))
        self._sadd(node_mem_k, str(player_id))
        return best.scene_id, best.node_id, len(self.sets.get(mem_k, set()))

    async def list_active_instances_with_scores(self, zone_id: int) -> list[tuple[str, float]]:
        k = keys.active_instances_key(zone_id)
        keys.assert_control_domain_keys((k,))
        z = self.zsets.get(k, {})
        return sorted(z.items(), key=lambda item: (item[1], item[0]))

    async def touch_active_instance(self, zone_id: int, scene_id: int, now_unix_sec: int) -> None:
        k = keys.active_instances_key(zone_id)
        keys.assert_control_domain_keys((k,))
        z = self.zsets.get(k)
        if z is not None and str(scene_id) in z:
            z[str(scene_id)] = float(now_unix_sec)

    async def is_active_instance(self, zone_id: int, scene_id: int) -> bool:
        k = keys.active_instances_key(zone_id)
        keys.assert_control_domain_keys((k,))
        return str(scene_id) in self.zsets.get(k, {})

    def _claim_scene_teardown(
        self,
        *,
        zone_id: int,
        scene_id: int,
        expected_owner: str,
        kind: str,
        reason: str,
        count_owner: bool,
        require_idle: bool,
        task_fence: DeadNodeTaskFence | None,
        now_unix_sec: int | None,
    ) -> SceneClaimResult:
        token = str(uuid.uuid4())
        now_sec = (self._clock_ms() // 1000) if now_unix_sec is None else int(now_unix_sec)
        node_k = keys.scene_node_key(zone_id, scene_id)
        mem_k = keys.instance_members_key(zone_id, scene_id)
        active_k = keys.active_instances_key(zone_id)
        mirror_k = keys.scene_mirror_flag_key(zone_id, scene_id)
        source_k = keys.scene_source_key(zone_id, scene_id)
        agones_k = keys.scene_agones_gs_key(zone_id, scene_id)
        td_k = keys.scene_teardown_job_key(zone_id, scene_id)
        pending_k = keys.scene_teardown_pending_key(zone_id)
        check_keys = [node_k, mem_k, active_k, td_k, pending_k]
        if task_fence is not None:
            check_keys.append(task_fence.tasks_key)
        keys.assert_control_domain_keys(check_keys)

        td_hash = self.hashes.get(td_k)
        if td_hash and "token" in td_hash:
            existing_token = _as_text(td_hash.get("token"))
            if existing_token == token:
                return _parse_scene_claim_reply(json.loads(_as_text(td_hash.get("reply"))), token)
            return SceneClaimResult(pending=True)

        if task_fence is not None:
            z = self.zsets.get(task_fence.tasks_key, {})
            queued = z.get(task_fence.member)
            if queued is None or int(queued) != int(task_fence.queued_at_ms):
                return SceneClaimResult(withdrawn=True)

        current_owner = self._get_str(node_k)
        if expected_owner == "":
            if current_owner is not None:
                return SceneClaimResult()
            owner_val = ""
        else:
            if current_owner != expected_owner:
                return SceneClaimResult()
            owner_val = current_owner

        if require_idle and len(self.sets.get(mem_k, set())) > 0:
            z = self.zsets.get(active_k)
            if z is not None and str(scene_id) in z:
                z[str(scene_id)] = float(now_sec)
            return SceneClaimResult(busy=True)

        agones_gs = self._get_str(agones_k) or ""
        source = self._get_str(source_k) or ""
        mirror_flag = self._get_str(mirror_k) or ""
        members = tuple(sorted(self.sets.get(mem_k, set())))
        self._del(node_k, mem_k, mirror_k, source_k, agones_k)
        self._zrem(active_k, str(scene_id))
        if owner_val:
            self._srem(keys.node_scenes_key(zone_id, owner_val), str(scene_id))
            if count_owner:
                cnt_k = keys.node_scene_count_key(zone_id, owner_val)
                cur_cnt = int(self._get_str(cnt_k) or "0")
                if cur_cnt > 0:
                    self._set_str(cnt_k, str(cur_cnt - 1))
        reply = ["claimed", owner_val, agones_gs, source, mirror_flag, *members]
        self.hashes[td_k] = {
            "token": token.encode("utf-8"),
            "kind": kind.encode("utf-8"),
            "reason": reason.encode("utf-8"),
            "claimed_at": str(now_sec).encode("utf-8"),
            "reply": json.dumps(reply).encode("utf-8"),
        }
        self._zadd(pending_k, str(scene_id), float(now_sec))
        return _parse_scene_claim_reply(reply, token)

    async def atomic_destroy_instance(
        self, *, zone_id: int, scene_id: int, expected_owner: str, now_unix_sec: int | None = None,
    ) -> SceneClaimResult:
        return self._claim_scene_teardown(
            zone_id=zone_id,
            scene_id=scene_id,
            expected_owner=expected_owner,
            kind="instance",
            reason="idle",
            count_owner=True,
            require_idle=True,
            task_fence=None,
            now_unix_sec=now_unix_sec,
        )

    async def atomic_destroy_instance_force(
        self, *, zone_id: int, scene_id: int, expected_owner: str, kind: str, reason: str,
        count_owner: bool = True, now_unix_sec: int | None = None,
    ) -> SceneClaimResult:
        return self._claim_scene_teardown(
            zone_id=zone_id,
            scene_id=scene_id,
            expected_owner=expected_owner,
            kind=kind,
            reason=reason,
            count_owner=count_owner,
            require_idle=False,
            task_fence=None,
            now_unix_sec=now_unix_sec,
        )

    async def atomic_destroy_orphan_of_dead_node(
        self, *, zone_id: int, scene_id: int, dead_node: str, task_fence: DeadNodeTaskFence,
        now_unix_sec: int | None = None,
    ) -> SceneClaimResult:
        if not dead_node:
            raise ValueError("dead_node must be non-empty")
        return self._claim_scene_teardown(
            zone_id=zone_id,
            scene_id=scene_id,
            expected_owner=dead_node,
            kind="instance",
            reason="node_death",
            count_owner=True,
            require_idle=False,
            task_fence=task_fence,
            now_unix_sec=now_unix_sec,
        )

    async def finish_scene_teardown(self, zone_id: int, scene_id: int, token: str) -> bool:
        td_k = keys.scene_teardown_job_key(zone_id, scene_id)
        pending_k = keys.scene_teardown_pending_key(zone_id)
        keys.assert_control_domain_keys((td_k, pending_k))
        td_hash = self.hashes.get(td_k)
        if not td_hash or _as_text(td_hash.get("token")) != token:
            return False
        self._del(td_k)
        self._zrem(pending_k, str(scene_id))
        return True

    async def list_due_teardown_jobs(self, zone_id: int, max_claimed_at_sec: int, limit: int) -> list[tuple[str, float]]:
        k = keys.scene_teardown_pending_key(zone_id)
        keys.assert_control_domain_keys((k,))
        z = self.zsets.get(k, {})
        due = [(member, score) for member, score in z.items() if score <= float(max_claimed_at_sec)]
        due.sort(key=lambda item: (item[1], item[0]))
        return due[:max(1, int(limit))]

    async def load_scene_teardown_job(self, zone_id: int, scene_id: int) -> SceneTeardownJob | None:
        k = keys.scene_teardown_job_key(zone_id, scene_id)
        keys.assert_control_domain_keys((k,))
        td_hash = self.hashes.get(k)
        if not td_hash or not td_hash.get("token"):
            return None
        token = _as_text(td_hash.get("token"))
        kind = _as_text(td_hash.get("kind"))
        reason = _as_text(td_hash.get("reason"))
        reply = json.loads(_as_text(td_hash.get("reply")))
        claim = _parse_scene_claim_reply(reply, token)
        if not claim.claimed:
            raise ValueError(f"teardown job for scene {scene_id} does not hold a valid claim")
        return SceneTeardownJob(zone_id=zone_id, scene_id=scene_id, kind=kind, reason=reason, claim=claim)

    async def drop_dangling_teardown(self, zone_id: int, scene_id: int) -> bool:
        td_k = keys.scene_teardown_job_key(zone_id, scene_id)
        pending_k = keys.scene_teardown_pending_key(zone_id)
        keys.assert_control_domain_keys((td_k, pending_k))
        if td_k in self.hashes:
            return False
        return self._zrem(pending_k, str(scene_id)) == 1

    async def enqueue_dead_node_reconcile(
        self, zone_id: int, node_id: str, *, queued_at_ms: int, detach: bool = True, node_uuid: str = "",
    ) -> int:
        tasks_k = keys.dead_node_reconcile_tasks_key(zone_id)
        scope_k = keys.node_reconcile_scenes_key(zone_id, node_id)
        scenes_k = keys.node_scenes_key(zone_id, node_id)
        load_k = keys.node_load_key(zone_id)
        inst_k = keys.node_reconcile_instance_key(zone_id, node_id)
        keys.assert_control_domain_keys((tasks_k, scope_k, scenes_k, load_k, inst_k))
        if detach:
            self._zrem(load_k, node_id)
        merged = set(self.sets.get(scope_k, set())) | set(self.sets.get(scenes_k, set()))
        if merged:
            self.sets[scope_k] = merged
        if node_uuid:
            self._set_str(inst_k, node_uuid)
        else:
            self._del(inst_k)
        member = keys.pending_dead_node_member(zone_id, node_id)
        z = self.zsets.setdefault(tasks_k, {})
        if member not in z:
            z[member] = float(queued_at_ms)
        return len(self.sets.get(scope_k, set()))

    async def withdraw_dead_node_for_returning_instance(self, zone_id: int, node_id: str, node_uuid: str) -> int:
        tasks_k = keys.dead_node_reconcile_tasks_key(zone_id)
        scope_k = keys.node_reconcile_scenes_key(zone_id, node_id)
        inst_k = keys.node_reconcile_instance_key(zone_id, node_id)
        death_k = keys.node_death_at_key(zone_id, node_id)
        keys.assert_control_domain_keys((tasks_k, scope_k, inst_k, death_k))
        dead = self._get_str(inst_k)
        if node_uuid and dead and dead == node_uuid:
            self._zrem(tasks_k, keys.pending_dead_node_member(zone_id, node_id))
            self._del(scope_k, inst_k, death_k)
            return 1
        if self._get_str(death_k) is not None:
            return 0
        return -1

    async def list_dead_node_reconcile_tasks(self, zone_id: int) -> tuple[list[DeadNodeReconcileTask], list[str]]:
        k = keys.dead_node_reconcile_tasks_key(zone_id)
        keys.assert_control_domain_keys((k,))
        rows = sorted(self.zsets.get(k, {}).items(), key=lambda item: (item[1], item[0]))
        tasks: list[DeadNodeReconcileTask] = []
        malformed: list[str] = []
        for member, score in rows:
            parsed = keys.parse_dead_node_reconcile_member(member)
            if parsed is None or parsed[0] != zone_id:
                malformed.append(member)
                continue
            tasks.append(DeadNodeReconcileTask(
                member=member, zone_id=parsed[0], node_id=parsed[1], queued_at_ms=int(score),
            ))
        return tasks, malformed

    async def remove_dead_node_reconcile_member(self, zone_id: int, member: str) -> None:
        k = keys.dead_node_reconcile_tasks_key(zone_id)
        keys.assert_control_domain_keys((k,))
        self._zrem(k, member)

    async def get_node_reconcile_scenes(self, zone_id: int, node_id: str) -> list[str]:
        k = keys.node_reconcile_scenes_key(zone_id, node_id)
        keys.assert_control_domain_keys((k,))
        return sorted(self.sets.get(k, set()))

    async def remove_node_reconcile_scenes(self, zone_id: int, node_id: str, scene_ids: Sequence[str]) -> None:
        if not scene_ids:
            return
        k = keys.node_reconcile_scenes_key(zone_id, node_id)
        keys.assert_control_domain_keys((k,))
        for sid in scene_ids:
            self._srem(k, sid)

    async def remove_node_scenes(self, zone_id: int, node_id: str, scene_ids: Sequence[str]) -> None:
        if not scene_ids:
            return
        k = keys.node_scenes_key(zone_id, node_id)
        keys.assert_control_domain_keys((k,))
        for sid in scene_ids:
            self._srem(k, sid)

    async def finish_dead_node_reconcile(
        self, zone_id: int, node_id: str, member: str, queued_at_ms: int, *, abandon: bool = False,
    ) -> bool:
        scope_k = keys.node_reconcile_scenes_key(zone_id, node_id)
        cnt_k = keys.node_scene_count_key(zone_id, node_id)
        mem_k = keys.node_members_key(zone_id, node_id)
        tasks_k = keys.dead_node_reconcile_tasks_key(zone_id)
        inst_k = keys.node_reconcile_instance_key(zone_id, node_id)
        keys.assert_control_domain_keys((scope_k, cnt_k, mem_k, tasks_k, inst_k))
        queued = self.zsets.get(tasks_k, {}).get(member)
        if queued is None or int(queued) != int(queued_at_ms):
            return True
        if not abandon and len(self.sets.get(scope_k, set())) > 0:
            return False
        self._del(scope_k, cnt_k, mem_k, inst_k)
        self._zrem(tasks_k, member)
        return True

    async def get_gate_marks(self, zone_id: int, gate_node_ids: Sequence[int]) -> tuple[set[int], set[int]]:
        draining: set[int] = set()
        stale: set[int] = set()
        for gid in gate_node_ids:
            if self._get_str(keys.gate_draining_key(zone_id, gid)):
                draining.add(gid)
            if self._get_str(keys.gate_stale_key(zone_id, gid)):
                stale.add(gid)
        return draining, stale


class MemoryPlayerPlacementStore:
    """与 RedisPlayerPlacementStore（全套玩家域 Lua）逐条等价的内存孪生。"""

    def __init__(self, *, clock_ms: Callable[[], int] | None = None) -> None:
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.strings: dict[str, bytes] = {}
        self.expires_ms: dict[str, int] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.on_before_mint: Callable[[int], None] | None = None

    def _purge_if_expired(self, key: str) -> None:
        exp = self.expires_ms.get(key)
        if exp is not None and self._clock_ms() >= exp:
            self.expires_ms.pop(key, None)
            self.strings.pop(key, None)
            self.hashes.pop(key, None)

    def _get(self, key: str) -> bytes | None:
        self._purge_if_expired(key)
        return self.strings.get(key)

    def _get_str(self, key: str) -> str | None:
        raw = self._get(key)
        return None if raw is None else raw.decode("utf-8", errors="replace")

    def _set(self, key: str, value: bytes, *, ex: int | None = None) -> None:
        self.strings[key] = value
        if ex is not None:
            self.expires_ms[key] = self._clock_ms() + int(ex) * 1000
        else:
            self.expires_ms.pop(key, None)

    def _del(self, *del_keys: str) -> None:
        for key in del_keys:
            self.strings.pop(key, None)
            self.hashes.pop(key, None)
            self.expires_ms.pop(key, None)

    # ── 测试辅助写入口 ────────────────────────────────────────────────────────
    def set_handoff_marker(self, player_id: int, marker: str, ttl_sec: int = config.HANDOFF_TTL_SEC) -> None:
        self._set(keys.handoff_key(player_id), marker.encode("utf-8"), ex=ttl_sec)

    def set_handoff_fence(self, player_id: int, fence_raw: str) -> None:
        self.hashes.setdefault(keys.handoff_fence_key(player_id), {})[keys.HANDOFF_FENCE_FIELD] = fence_raw.encode("utf-8")

    def get_handoff_ttl_remaining_sec(self, player_id: int) -> int | None:
        k = keys.handoff_key(player_id)
        self._purge_if_expired(k)
        exp = self.expires_ms.get(k)
        if exp is None:
            return None
        return max(0, (exp - self._clock_ms()) // 1000)

    # ── Protocol 方法实现 ─────────────────────────────────────────────────────
    async def observe_location_revision(self, player_id: int, revision: int) -> int:
        k = keys.location_revision_floor_key(player_id)
        keys.assert_player_domain_keys((k,))
        rev_txt = keys.revision_text(revision)
        floor = self._get_str(k)
        if floor is not None and (len(floor) != 20 or not floor.isdigit()):
            return -1
        if floor is not None and floor > rev_txt:
            return 0
        self._set(k, rev_txt.encode("ascii"))
        return 1

    async def read_location_snapshot(self, player_id: int) -> tuple[bytes, str]:
        loc_k, rev_k = keys.location_snapshot_keys(player_id)
        keys.assert_player_domain_keys((loc_k, rev_k))
        return self._get(loc_k) or b"", self._get_str(rev_k) or ""

    async def get_owner_epoch(self, player_id: int) -> int:
        k = keys.owner_epoch_key(player_id)
        keys.assert_player_domain_keys((k,))
        return keys.parse_epoch(self._get(k))

    async def setnx_owner_epoch(self, player_id: int, epoch: int) -> bool:
        k = keys.owner_epoch_key(player_id)
        keys.assert_player_domain_keys((k,))
        if self._get(k) is not None:
            return False
        self._set(k, str(epoch).encode("ascii"))
        return True

    async def get_handoff_marker(self, player_id: int) -> str:
        k = keys.handoff_key(player_id)
        keys.assert_player_domain_keys((k,))
        return self._get_str(k) or ""

    async def get_holder_lease(self, player_id: int) -> str:
        k = keys.holder_lease_key(player_id)
        keys.assert_player_domain_keys((k,))
        return self._get_str(k) or ""

    def _check_revision_guard(
        self, *, loc_k: str, rev_k: str, floor_k: str,
        owner_revision: int, location_bytes: bytes, new_tag: str, observed_tag: str,
    ) -> bool:
        req_rev = keys.optional_revision_text(owner_revision)
        if not req_rev:
            return True
        if self._get_str(floor_k) != req_rev:
            return False
        cur_tag = self._get_str(rev_k) or ""
        if self._get(loc_k) == location_bytes and cur_tag == new_tag:
            return True
        return cur_tag == observed_tag

    def _check_location_expectation(self, loc_k: str, expect_mode: str, expect_raw: bytes) -> bool:
        if expect_mode == "missing":
            return self._get(loc_k) is None
        if expect_mode == "present":
            return self._get(loc_k) == expect_raw
        return expect_mode in ("", "any")

    async def mint_epoch_and_set_location(
        self, *, player_id: int, observed_epoch: int, location_bytes: bytes,
        required_marker: str, expect_mode: str, expect_raw: bytes,
        owner_revision: int, new_tag: str, observed_tag: str, target_epoch: int,
    ) -> int:
        if self.on_before_mint is not None:
            self.on_before_mint(player_id)
        epoch_k, loc_k, handoff_k, rev_k, floor_k = keys.placement_keys(player_id)
        keys.assert_player_domain_keys((epoch_k, loc_k, handoff_k, rev_k, floor_k))

        if observed_epoch < 0 or target_epoch <= observed_epoch or target_epoch > keys.MAX_MINTED_OWNER_EPOCH:
            raise ValueError("owner epoch mint target invalid")

        if not self._check_revision_guard(
            loc_k=loc_k, rev_k=rev_k, floor_k=floor_k,
            owner_revision=owner_revision, location_bytes=location_bytes,
            new_tag=new_tag, observed_tag=observed_tag,
        ):
            return 0

        cur = self._get_str(epoch_k)
        cur_str = "0" if cur is None else cur
        if cur_str != str(observed_epoch):
            if (
                required_marker
                and cur_str == str(target_epoch)
                and self._get(loc_k) == location_bytes
                and self._get_str(handoff_k) == required_marker
            ):
                self._set(epoch_k, cur_str.encode("ascii"))
                return -int(cur_str)
            return 0

        if not self._check_location_expectation(loc_k, expect_mode, expect_raw):
            return 0
        if required_marker and self._get_str(handoff_k) != required_marker:
            return -1

        self._set(epoch_k, str(target_epoch).encode("ascii"))
        self._set(loc_k, location_bytes)
        if owner_revision and new_tag:
            self._set(rev_k, new_tag.encode("ascii"))
        else:
            self._del(rev_k)
        return target_epoch

    async def set_location_if_epoch(
        self, *, player_id: int, observed_epoch: int, location_bytes: bytes,
        expect_mode: str, expect_raw: bytes, owner_revision: int, new_tag: str, observed_tag: str,
    ) -> int:
        epoch_k, loc_k, handoff_k, rev_k, floor_k = keys.placement_keys(player_id)
        keys.assert_player_domain_keys((epoch_k, loc_k, handoff_k, rev_k, floor_k))

        if not self._check_revision_guard(
            loc_k=loc_k, rev_k=rev_k, floor_k=floor_k,
            owner_revision=owner_revision, location_bytes=location_bytes,
            new_tag=new_tag, observed_tag=observed_tag,
        ):
            return 0

        cur = self._get_str(epoch_k)
        cur_str = "0" if cur is None else cur
        if cur_str != str(observed_epoch):
            return 0
        if self._get(loc_k) == location_bytes and (not owner_revision or self._get_str(rev_k) == new_tag):
            return 1
        if not self._check_location_expectation(loc_k, expect_mode, expect_raw):
            return 0

        self._set(loc_k, location_bytes)
        if owner_revision and new_tag:
            self._set(rev_k, new_tag.encode("ascii"))
        else:
            self._del(rev_k)
        return 1

    async def rollback_player_placement(
        self, *, player_id: int, placed_raw: bytes, restored_raw: bytes, placed_epoch: int,
        mode: str, required_marker: str, forward_marker: str, handoff_ttl_sec: int,
        owner_revision: int, placed_tag: str, restored_tag: str,
        void_epoch: int | None = None, observed_raw: bytes = b"",
        observed_tag: str = "", void_epoch_after: int | None = None,
    ) -> int:
        loc_k, epoch_k, handoff_k, rev_k, floor_k, fence_k = keys.rollback_placement_keys(player_id)
        keys.assert_player_domain_keys((loc_k, epoch_k, handoff_k, rev_k, floor_k, fence_k))

        void_requested = void_epoch is not None and void_epoch_after is not None
        tag_is_placement = True
        tag_is_observed = True
        req_rev = keys.optional_revision_text(owner_revision)
        if req_rev:
            if self._get_str(floor_k) != req_rev:
                return 0
            cur_tag = self._get_str(rev_k) or ""
            tag_is_placement = cur_tag in (placed_tag, restored_tag)
            tag_is_observed = void_requested and cur_tag == observed_tag
            if not tag_is_placement and not tag_is_observed:
                return 0

        loc = self._get(loc_k)
        cur = self._get_str(epoch_k)
        cur_str = "0" if cur is None else cur

        def restore_location() -> None:
            if not restored_raw:
                self._del(loc_k)
            else:
                self._set(loc_k, restored_raw)
            if restored_tag:
                self._set(rev_k, restored_tag.encode("ascii"))
            else:
                self._del(rev_k)

        if tag_is_placement and loc == placed_raw and cur_str == str(placed_epoch):
            if required_marker and self._get_str(handoff_k) != required_marker:
                return 3
            if mode == "bump" and not required_marker:
                fence_hash = self.hashes.get(fence_k, {})
                raw_fence = fence_hash.get(keys.HANDOFF_FENCE_FIELD)
                if raw_fence is not None:
                    parsed = keys.parse_handoff_fence(raw_fence)
                    if parsed is None:
                        return 5
                    if parsed.owner_epoch == placed_epoch:
                        return 4
                    if parsed.owner_epoch > placed_epoch:
                        return 5
            after = 0
            if mode == "bump":
                after = int(cur_str) + 1
                self._set(epoch_k, str(after).encode("ascii"))
            restore_location()
            if forward_marker:
                self._set(handoff_k, forward_marker.encode("utf-8"), ex=handoff_ttl_sec)
            return -after if mode == "bump" else 1

        if tag_is_placement and mode == "bump" and cur_str.isdigit() and int(cur_str) == placed_epoch + 1:
            if (not restored_raw and loc is None) or (restored_raw and loc == restored_raw):
                return 2

        if void_requested and tag_is_observed and mode == "bump" and cur_str == str(void_epoch) and (loc or b"") == observed_raw:
            if required_marker:
                if self._get_str(handoff_k) == required_marker:
                    self._del(handoff_k)
                return 7
            self._set(epoch_k, str(void_epoch_after).encode("ascii"))
            restore_location()
            return 6

        return 0

    async def delete_location_if_unchanged(
        self, *, player_id: int, expected_raw: bytes, mode: str, epoch: int,
        expected_tag: str, owner_revision: int,
    ) -> int:
        loc_k, epoch_k, handoff_k, wait_k, rev_k, floor_k = keys.leave_delete_keys(player_id)
        keys.assert_player_domain_keys((loc_k, epoch_k, handoff_k, wait_k, rev_k, floor_k))

        cur = self._get(loc_k)
        if cur is None:
            return 0
        if (self._get_str(rev_k) or "") != expected_tag:
            return -1
        req_rev = keys.optional_revision_text(owner_revision)
        if req_rev and self._get_str(floor_k) != req_rev:
            return -1
        if cur != expected_raw:
            return -1
        if mode == "released":
            owner = self._get_str(epoch_k)
            if owner is not None and owner != str(epoch):
                return -2
            mark = self._get_str(handoff_k)
            if not keys.marker_releases_epoch(mark, epoch):
                return -2
        elif mode != "any":
            raise ValueError(f"LEAVE_SCENE unknown delete mode: {mode!r}")

        self._del(loc_k, wait_k, rev_k)
        return 1

    async def note_leave_release_wait(self, player_id: int, scope: str, ttl_sec: int) -> int:
        k = keys.leave_release_wait_key(player_id)
        keys.assert_player_domain_keys((k,))
        now_ms = self._clock_ms()
        cur = self._get_str(k)
        if cur is not None and ":" in cur:
            stored_scope, first_str = cur.rsplit(":", 1)
            if stored_scope == scope and first_str.isdigit():
                return max(0, now_ms - int(first_str))
        self._set(k, f"{scope}:{now_ms}".encode("utf-8"), ex=ttl_sec)
        return 0

    async def has_channel_switch_cooldown(self, player_id: int) -> bool:
        k = keys.channel_switch_cooldown_key(player_id)
        keys.assert_player_domain_keys((k,))
        return self._get(k) is not None

    async def set_channel_switch_cooldown(self, player_id: int, ttl_sec: int) -> None:
        k = keys.channel_switch_cooldown_key(player_id)
        keys.assert_player_domain_keys((k,))
        self._set(k, b"1", ex=ttl_sec)

    async def setnx_enter_dedupe(self, player_id: int, request_id: str, value: str, ttl_sec: int) -> bool:
        k = keys.enter_scene_dedup_key(player_id, request_id)
        keys.assert_player_domain_keys((k,))
        if self._get(k) is not None:
            return False
        self._set(k, value.encode("utf-8"), ex=ttl_sec)
        return True

    async def get_enter_dedupe(self, player_id: int, request_id: str) -> str:
        k = keys.enter_scene_dedup_key(player_id, request_id)
        keys.assert_player_domain_keys((k,))
        return self._get_str(k) or ""

    async def delete_enter_dedupe_if_value(self, player_id: int, request_id: str, expected: str) -> bool:
        k = keys.enter_scene_dedup_key(player_id, request_id)
        keys.assert_player_domain_keys((k,))
        if self._get_str(k) == expected:
            self._del(k)
            return True
        return False

    async def complete_enter_dedupe(
        self, *, player_id: int, request_id: str, expected_owner: str,
        cached_value: str, ttl_sec: int, owner_revision: int,
    ) -> bool:
        dedupe_k, floor_k = keys.enter_scene_dedupe_complete_keys(player_id, request_id)
        keys.assert_player_domain_keys((dedupe_k, floor_k))
        if self._get_str(floor_k) != keys.revision_text(owner_revision):
            return False
        if self._get_str(dedupe_k) == expected_owner:
            self._set(dedupe_k, cached_value.encode("utf-8"), ex=ttl_sec)
            return True
        return False
