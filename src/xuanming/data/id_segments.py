"""独占号段分配与 C++ MixGuid 打散。"""

import asyncio
import sqlite3
from contextlib import closing

MASK = (1 << 63) - 1


def mix_guid(value: int) -> int:
    value &= MASK
    value ^= value >> 30
    value = value * 0xBF58476D1CE4E5B9 & MASK
    value ^= value >> 27
    value = value * 0x94D049BB133111EB & MASK
    return value ^ (value >> 31)


class IdSegments:
    def __init__(self, reserve, *, size=256) -> None:
        self.reserve = reserve
        self.size = size
        self._next = self._end = 0
        self._lock = asyncio.Lock()

    async def __call__(self) -> int:
        async with self._lock:
            if self._next == self._end:
                first, end = await self.reserve(self.size)
                if not 0 < first < end <= MASK + 1:
                    raise RuntimeError("发号服务返回无效号段")
                self._next, self._end = first, end
            value = mix_guid(self._next)
            self._next += 1
            return value

    @classmethod
    def remote(cls, protocol, call, tag: str):
        async def reserve(size):
            request = protocol.message("data_service.AllocateIdSegmentRequest", biz_tag=tag, step=size)
            raw = await call("/data_service.DataService/AllocateIdSegment", request.SerializeToString())
            response = protocol.parse("data_service.AllocateIdSegmentResponse", raw)
            if response.error_code:
                raise RuntimeError(f"发号服务拒绝 {tag} 号段")
            return response.lo, response.hi

        return cls(reserve)

    @classmethod
    def local(cls, database, tag: str):
        async def reserve(size):
            return await asyncio.to_thread(_reserve_local, database, tag, size)

        return cls(reserve)


def _reserve_local(database, tag: str, size: int):
    with closing(sqlite3.connect(database, timeout=10, isolation_level=None)) as connection, connection:
        connection.execute("CREATE TABLE IF NOT EXISTS local_id_segment (tag TEXT PRIMARY KEY, next TEXT NOT NULL)")
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT next FROM local_id_segment WHERE tag = ?", (tag,)).fetchone()
        start = int(row[0]) if row else 1
        end = start + size
        if end > MASK + 1:
            raise RuntimeError("本地号段已耗尽")
        connection.execute(
            "INSERT INTO local_id_segment VALUES (?, ?) ON CONFLICT(tag) DO UPDATE SET next=excluded.next",
            (tag, str(end)),
        )
        return start, end
