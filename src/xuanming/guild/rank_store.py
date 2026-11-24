"""guild 按区分榜存储（Redis ZSET 版本条件写与确定性内存双胞胎）。

键名与 Lua 脚本与 Go ``internal/data/guild_repo.go`` 逐字对齐：
- ``guild_rank:zone:{gr:<final_zone>}``      ZSET(member=guild_id, score=score)
- ``guild_rank:version:{gr:<final_zone>}``   HASH(field=guild_id, value=score_version)
- ``guild_rank:tombstone:{gr:<final_zone>}`` ZSET(member=guild_id, score=expire_ms)
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Protocol as TypingProtocol, runtime_checkable

from .model import (
    MAX_RANK_VERSION,
    MergedZones,
    RANK_TOMBSTONE_TTL_MS,
    RankEntry,
)

logger = logging.getLogger(__name__)

RANK_WRITE_APPLIED = 1
RANK_WRITE_STALE = 0
RANK_WRITE_DISBANDED = -1

CLEANUP_WATERMARK_KEY = "guild:asset_op:cleanup_watermark_ms"


def guild_rank_tag(final_zone: int) -> str:
    return f"{{gr:{int(final_zone)}}}"


def zone_rank_key(final_zone: int) -> str:
    return f"guild_rank:zone:{guild_rank_tag(final_zone)}"


def zone_rank_version_key(final_zone: int) -> str:
    return f"guild_rank:version:{guild_rank_tag(final_zone)}"


def zone_rank_tombstone_key(final_zone: int) -> str:
    return f"guild_rank:tombstone:{guild_rank_tag(final_zone)}"


# 与 Go guild_repo.go 逐字一致的 5 段 Lua 脚本
RANK_UPSERT_LUA = """
local buried = redis.call("ZSCORE", KEYS[3], ARGV[1])
if buried then
  local now = redis.call("TIME")
  if tonumber(buried) > tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) then return -1 end
end
local recorded = redis.call("HGET", KEYS[2], ARGV[1])
if recorded and tonumber(recorded) > tonumber(ARGV[3]) then return 0 end
redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])
redis.call("HSET", KEYS[2], ARGV[1], ARGV[3])
return 1
"""

RANK_BURY_LUA = """
local now = redis.call("TIME")
local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", nowMs)
redis.call("ZADD", KEYS[3], nowMs + tonumber(ARGV[2]), ARGV[1])
redis.call("PEXPIRE", KEYS[3], ARGV[2])
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1])
return 1
"""

RANK_REMOVE_LUA = """
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[2], ARGV[1])
return 1
"""

GUILD_RANK_PAGE_LUA = """
local total = redis.call("ZCARD", KEYS[1])
local size = tonumber(ARGV[2])
if size == 0 then return {total} end
local start = tonumber(ARGV[1])
if start >= total then return {total} end
local stop = math.min(start + size - 1, total - 1)
local rows = redis.call("ZREVRANGE", KEYS[1], start, stop, "WITHSCORES")
table.insert(rows, 1, total)
return rows
"""

GUILD_RANK_ENTRY_LUA = """
local rank = redis.call("ZREVRANK", KEYS[1], ARGV[1])
if not rank then return {} end
return {rank, redis.call("ZSCORE", KEYS[1], ARGV[1])}
"""


@runtime_checkable
class GuildRankStore(TypingProtocol):
    """按区帮会排行榜接口（RedisGuildRankStore 与 MemoryGuildRankStore 共同实现）。"""

    def set_merged_zones(self, zones: MergedZones) -> None: ...

    async def upsert_score(self, guild_id: int, zone_id: int, score: int, version: int) -> int: ...

    async def bury_guild(self, guild_id: int, zone_id: int, known_zones: Iterable[int] = ()) -> None: ...

    async def remove_guild(self, guild_id: int, known_zones: Iterable[int] = ()) -> None: ...

    async def get_page(self, zone_id: int, page: int, page_size: int) -> tuple[list[RankEntry], int]: ...

    async def get_rank(self, guild_id: int, zone_id: int) -> RankEntry: ...

    async def rebuild_ranks(self, rows: Sequence[tuple[int, int, int, int]]) -> None: ...

    async def terminal_cleanup_watermark_ms(self) -> int: ...


class MemoryGuildRankStore:
    """确定性内存帮会榜实现：完整复现版本条件写入与 24h 解散墓碑语义。"""

    def __init__(
        self,
        *,
        merged_zones: MergedZones | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._zones = merged_zones or MergedZones()
        self._clock_ms = clock_ms
        self._scores: dict[int, dict[int, int]] = {}
        self._versions: dict[int, dict[int, int]] = {}
        self._tombstones: dict[int, dict[int, int]] = {}
        self.cleanup_watermark_ms: int = 0
        self.watermark_error: Exception | None = None

    def set_merged_zones(self, zones: MergedZones) -> None:
        self._zones = zones

    async def upsert_score(self, guild_id: int, zone_id: int, score: int, version: int) -> int:
        if zone_id <= 0:
            return RANK_WRITE_STALE
        if version > MAX_RANK_VERSION:
            raise ValueError(f"score version {version} exceeds max {MAX_RANK_VERSION}")
        final_zone = self._zones.final_zone(zone_id)
        now = self._clock_ms()
        buried = self._tombstones.get(final_zone, {}).get(guild_id)
        if buried is not None and buried > now:
            return RANK_WRITE_DISBANDED
        recorded = self._versions.get(final_zone, {}).get(guild_id)
        if recorded is not None and recorded > version:
            return RANK_WRITE_STALE
        self._scores.setdefault(final_zone, {})[guild_id] = int(score)
        self._versions.setdefault(final_zone, {})[guild_id] = int(version)
        return RANK_WRITE_APPLIED

    async def bury_guild(self, guild_id: int, zone_id: int, known_zones: Iterable[int] = ()) -> None:
        now = self._clock_ms()
        group = set(self._zones.zone_members(zone_id)) if zone_id > 0 else set()
        all_zones = set(group) | set(self._scores.keys()) | set(known_zones)
        for z in sorted(z for z in all_zones if z > 0):
            if z in group:
                tombs = self._tombstones.setdefault(z, {})
                for gid in [k for k, exp in tombs.items() if exp <= now]:
                    del tombs[gid]
                tombs[guild_id] = now + RANK_TOMBSTONE_TTL_MS
            self._scores.get(z, {}).pop(guild_id, None)
            self._versions.get(z, {}).pop(guild_id, None)

    async def remove_guild(self, guild_id: int, known_zones: Iterable[int] = ()) -> None:
        all_zones = set(self._scores.keys()) | set(known_zones)
        for z in sorted(z for z in all_zones if z > 0):
            self._scores.get(z, {}).pop(guild_id, None)
            self._versions.get(z, {}).pop(guild_id, None)

    def _sorted_entries(self, final_zone: int) -> list[tuple[int, int]]:
        board = self._scores.get(final_zone, {})
        # Redis ZREVRANGE 同分按 member 字典序降序
        return sorted(board.items(), key=lambda kv: (kv[1], str(kv[0])), reverse=True)

    async def get_page(self, zone_id: int, page: int, page_size: int) -> tuple[list[RankEntry], int]:
        if zone_id <= 0:
            raise ValueError("guild rank requires a non-zero zone_id")
        final_zone = self._zones.final_zone(zone_id)
        ordered = self._sorted_entries(final_zone)
        total = len(ordered)
        if page <= 0 or page_size <= 0:
            return [], total
        start = (page - 1) * page_size
        if start >= total:
            return [], total
        slice_rows = ordered[start : start + page_size]
        entries = [
            RankEntry(guild_id=gid, score=sc, rank=start + idx + 1)
            for idx, (gid, sc) in enumerate(slice_rows)
        ]
        return entries, total

    async def get_rank(self, guild_id: int, zone_id: int) -> RankEntry:
        if zone_id <= 0:
            raise ValueError("guild rank requires a non-zero zone_id")
        final_zone = self._zones.final_zone(zone_id)
        ordered = self._sorted_entries(final_zone)
        for idx, (gid, sc) in enumerate(ordered):
            if gid == guild_id:
                return RankEntry(guild_id=gid, score=sc, rank=idx + 1)
        return RankEntry(guild_id=guild_id, score=0, rank=0)

    async def rebuild_ranks(self, rows: Sequence[tuple[int, int, int, int]]) -> None:
        for guild_id, zone_id, score, version in rows:
            if zone_id > 0:
                await self.upsert_score(guild_id, zone_id, score, version)

    async def terminal_cleanup_watermark_ms(self) -> int:
        if self.watermark_error is not None:
            raise self.watermark_error
        return self.cleanup_watermark_ms


class RedisGuildRankStore:
    """生产 Redis 分区榜实现（同 tag 三键原子脚本，与 Go guild_repo.go 同构）。"""

    def __init__(self, redis_client, *, merged_zones: MergedZones | None = None) -> None:
        self._rdb = redis_client
        self._zones = merged_zones or MergedZones()
        self._upsert_script = redis_client.register_script(RANK_UPSERT_LUA)
        self._bury_script = redis_client.register_script(RANK_BURY_LUA)
        self._remove_script = redis_client.register_script(RANK_REMOVE_LUA)
        self._page_script = redis_client.register_script(GUILD_RANK_PAGE_LUA)
        self._entry_script = redis_client.register_script(GUILD_RANK_ENTRY_LUA)

    def set_merged_zones(self, zones: MergedZones) -> None:
        self._zones = zones

    async def upsert_score(self, guild_id: int, zone_id: int, score: int, version: int) -> int:
        if zone_id <= 0:
            return RANK_WRITE_STALE
        if version > MAX_RANK_VERSION:
            raise ValueError(f"score version {version} exceeds max {MAX_RANK_VERSION}")
        final_zone = self._zones.final_zone(zone_id)
        keys = [
            zone_rank_key(final_zone),
            zone_rank_version_key(final_zone),
            zone_rank_tombstone_key(final_zone),
        ]
        res = await self._upsert_script(keys=keys, args=[str(guild_id), str(int(score)), str(int(version))])
        return int(res)

    async def bury_guild(self, guild_id: int, zone_id: int, known_zones: Iterable[int] = ()) -> None:
        group = set(self._zones.zone_members(zone_id)) if zone_id > 0 else set()
        zones = set(group)
        for z in known_zones:
            if z > 0:
                zones.add(z)
                zones.add(self._zones.final_zone(z))
        for src, dst in self._zones.merges.items():
            if src > 0:
                zones.add(src)
                zones.add(self._zones.final_zone(src))
            if dst > 0:
                zones.add(dst)
        for z in sorted(zones):
            if z in group:
                keys = [zone_rank_key(z), zone_rank_version_key(z), zone_rank_tombstone_key(z)]
                await self._bury_script(keys=keys, args=[str(guild_id), str(RANK_TOMBSTONE_TTL_MS)])
            else:
                keys = [zone_rank_key(z), zone_rank_version_key(z)]
                await self._remove_script(keys=keys, args=[str(guild_id)])

    async def remove_guild(self, guild_id: int, known_zones: Iterable[int] = ()) -> None:
        zones = {z for z in known_zones if z > 0}
        for z in sorted(zones):
            keys = [zone_rank_key(z), zone_rank_version_key(z)]
            await self._remove_script(keys=keys, args=[str(guild_id)])

    async def get_page(self, zone_id: int, page: int, page_size: int) -> tuple[list[RankEntry], int]:
        if zone_id <= 0:
            raise ValueError("guild rank requires a non-zero zone_id")
        final_zone = self._zones.final_zone(zone_id)
        start = 0 if page <= 0 or page_size <= 0 else (page - 1) * page_size
        size = 0 if page <= 0 or page_size <= 0 or start > ((1 << 63) - 1) else page_size
        reply = await self._page_script(keys=[zone_rank_key(final_zone)], args=[str(start), str(size)])
        if not reply:
            return [], 0
        total = int(reply[0])
        entries: list[RankEntry] = []
        for idx in range(1, len(reply), 2):
            raw_gid = reply[idx]
            raw_score = reply[idx + 1]
            gid = int(raw_gid.decode("utf-8") if isinstance(raw_gid, bytes) else raw_gid)
            score = int(float(raw_score.decode("utf-8") if isinstance(raw_score, bytes) else raw_score))
            entries.append(RankEntry(guild_id=gid, score=score, rank=start + ((idx - 1) // 2) + 1))
        return entries, total

    async def get_rank(self, guild_id: int, zone_id: int) -> RankEntry:
        if zone_id <= 0:
            raise ValueError("guild rank requires a non-zero zone_id")
        final_zone = self._zones.final_zone(zone_id)
        reply = await self._entry_script(keys=[zone_rank_key(final_zone)], args=[str(guild_id)])
        if not reply:
            return RankEntry(guild_id=guild_id, score=0, rank=0)
        rank_zero_based = int(reply[0])
        raw_score = reply[1]
        score = int(float(raw_score.decode("utf-8") if isinstance(raw_score, bytes) else raw_score))
        return RankEntry(guild_id=guild_id, score=score, rank=rank_zero_based + 1)

    async def rebuild_ranks(self, rows: Sequence[tuple[int, int, int, int]]) -> None:
        for guild_id, zone_id, score, version in rows:
            if zone_id > 0:
                await self.upsert_score(guild_id, zone_id, score, version)

    async def terminal_cleanup_watermark_ms(self) -> int:
        raw = await self._rdb.get(CLEANUP_WATERMARK_KEY)
        if raw is None:
            return 0
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return int(text)
