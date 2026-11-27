"""合服映射的 etcd 副本与版本/世代一致性判定（对齐 Go ``shared/mergedzonecopy`` 与 ``merged_zone_copy.go``）。

核心契约：
- 真源仍是目录第 0 片的 ``zone_merged_into`` + ``zone_merge_history``（行数即单调递增版本号）+ ``route_meta.generation``；
- 每次刷新先读副本再读真源：副本只含已提交版本，因此「真源落后于副本」只可能是回档或删表；
- 真源、副本与本进程已回答版本三者间一旦出现世代不符、版本回退或同版本内容不一致，一律 fail-closed 拒绝；
- 启动期仅真源连接不可达时允许从副本起服；运行期真源不可读时可沿用副本更高版本（同版本仍报错触发刷新告警）。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol as TypingProtocol

from .model import MergedZones

logger = logging.getLogger(__name__)

KEY_PREFIX = "/mmorpg/merged_zone_copy/"
PUBLISH_ATTEMPTS = 3


def merged_zone_copy_key(shard_zero_db: str) -> str:
    """返回目录第 0 片合服映射副本的 etcd 键。"""
    return f"{KEY_PREFIX}{shard_zero_db}"


class ShardZeroUnreachableError(RuntimeError):
    """目录第 0 片连接不可达或无写资格（与权限/缺表等确定部署错误区分）。"""


@dataclass(frozen=True, slots=True)
class MergedZoneMapping:
    """目录第 0 片合服映射的一次一致读（世代、版本与 src->dst 映射）。"""

    generation: str
    version: int
    merges: dict[int, int]


def _validate_mapping(mapping: MergedZoneMapping) -> None:
    if not mapping.generation:
        raise ValueError("the merged zone mapping has no directory generation")
    if mapping.version < 0:
        raise ValueError(f"invalid merged zone mapping version: {mapping.version}")
    for src, dst in mapping.merges.items():
        if src <= 0 or dst <= 0:
            raise ValueError(f"the merged zone mapping maps zone {src} to zone {dst}: zone 0 is not a zone")
    # 校验无环与合法合服链（与 MergedZones 同一规则）。
    MergedZones(mapping.merges)


def encode_merged_zone_copy(mapping: MergedZoneMapping) -> bytes:
    """校验并编码合服映射副本为确定性 JSON 字节（按 src 升序）。"""
    _validate_mapping(mapping)
    payload = {
        "generation": mapping.generation,
        "version": int(mapping.version),
        "merges": [{"src": int(src), "dst": int(mapping.merges[src])} for src in sorted(mapping.merges)],
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_merged_zone_copy(raw: bytes) -> MergedZoneMapping:
    """解码并校验合服映射副本 JSON；忽略未知扩展字段，形状非法或重复 src 直接报错。"""
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"the merged zone copy is not valid JSON: {exc}") from exc
    if not isinstance(doc, Mapping):
        raise ValueError("the merged zone copy is not a JSON object")
    generation = doc.get("generation")
    version = doc.get("version")
    raw_merges = doc.get("merges")
    if not isinstance(generation, str) or not generation:
        raise ValueError("the merged zone mapping has no directory generation")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError("the merged zone copy has an invalid version")
    if raw_merges is None:
        raw_merges = []
    if not isinstance(raw_merges, list):
        raise ValueError("the merged zone copy merges must be a list")
    merges: dict[int, int] = {}
    for item in raw_merges:
        if not isinstance(item, Mapping):
            raise ValueError("the merged zone copy merge item must be an object")
        src = item.get("src")
        dst = item.get("dst")
        if isinstance(src, bool) or isinstance(dst, bool) or not isinstance(src, int) or not isinstance(dst, int):
            raise ValueError("the merged zone copy src/dst must be integers")
        if src <= 0 or dst <= 0:
            raise ValueError(f"the merged zone mapping maps zone {src} to zone {dst}: zone 0 is not a zone")
        if src in merges:
            raise ValueError(f"the merged zone mapping lists source zone {src} twice")
        merges[src] = dst
    mapping = MergedZoneMapping(generation=generation, version=version, merges=merges)
    _validate_mapping(mapping)
    return mapping


def decide_merged_zone_publish(
    key: str,
    published: MergedZoneMapping | None,
    truth: MergedZoneMapping,
) -> bool:
    """判定刚读到的真源 ``truth`` 是否需要发布到 etcd 副本（对齐 Go ``mergedzonecopy.Decide``）。

    调用方必须先读副本、后读真源。返回 ``True`` 表示副本缺席或落后于真源，应按原 ModRevision CAS 写入；
    返回 ``False`` 表示两边完全一致无需发布；世代/版本/内容冲突则抛出 ``ValueError``。
    """
    _validate_mapping(truth)
    if published is None:
        return True
    if published.generation != truth.generation:
        raise ValueError(
            f"the etcd copy {key} belongs to directory generation {published.generation}, "
            f"but directory shard 0 is generation {truth.generation}"
        )
    if published.version < truth.version:
        return True
    if published.version > truth.version:
        raise ValueError(
            f"directory shard 0 is at version {truth.version} of the merged zone mapping, "
            f"behind version {published.version} in the etcd copy {key}"
        )
    if published.merges != truth.merges:
        raise ValueError(
            f"directory shard 0 and the etcd copy {key} both hold version {truth.version} "
            f"of the merged zone mapping with different contents"
        )
    return False


class MergedZoneTruth(TypingProtocol):
    """真源接口（目录第 0 片）。"""

    async def read_merged_zone_mapping(self) -> MergedZoneMapping: ...


class MergedZoneCopies(TypingProtocol):
    """etcd 副本 CAS 读写接口。"""

    def key(self) -> str: ...
    async def get(self) -> tuple[MergedZoneMapping | None, int]: ...
    async def put(self, revision: int, mapping: MergedZoneMapping) -> bool: ...


class MemoryMergedZoneCopies:
    """内存 CAS 合服映射副本替身（供单测与无 etcd 本地模式使用）。"""

    def __init__(self, shard_zero_db: str) -> None:
        self._key = merged_zone_copy_key(shard_zero_db)
        self._raw: bytes | None = None
        self._revision: int = 0

    def key(self) -> str:
        return self._key

    async def get(self) -> tuple[MergedZoneMapping | None, int]:
        if self._raw is None:
            return None, 0
        return decode_merged_zone_copy(self._raw), self._revision

    async def put(self, revision: int, mapping: MergedZoneMapping) -> bool:
        raw = encode_merged_zone_copy(mapping)
        if self._revision != revision:
            return False
        self._revision += 1
        self._raw = raw
        return True


class MergedZoneSource:
    """合服映射缓存读源：先读副本、再读真源、必要时 CAS 发布、真源不可达时受控降级到副本。"""

    def __init__(self, truth: MergedZoneTruth, copies: MergedZoneCopies) -> None:
        self._truth = truth
        self._copies = copies
        self._lock = asyncio.Lock()
        self._answered: MergedZoneMapping | None = None
        self._from_copy: bool = False

    @property
    def answered(self) -> MergedZoneMapping | None:
        return self._answered

    async def read(self) -> dict[int, int]:
        async with self._lock:
            key = self._copies.key()
            for attempt in range(1, PUBLISH_ATTEMPTS + 1):
                published, revision = await self._copies.get()
                try:
                    truth = await self._truth.read_merged_zone_mapping()
                except Exception as truth_err:
                    return self._answer_from_copy(published, truth_err)
                self._check_against_answered(truth, "directory shard 0")
                need_publish = decide_merged_zone_publish(key, published, truth)
                if need_publish:
                    written = await self._copies.put(revision, truth)
                    if not written:
                        if attempt < PUBLISH_ATTEMPTS:
                            continue
                        raise RuntimeError(
                            f"the etcd copy {key} of the merged zone mapping changed under each of {attempt} publish attempts"
                        )
                if self._from_copy:
                    logger.info(
                        "[MergedZoneCache] directory shard 0 is readable again at version %d",
                        truth.version,
                    )
                return self._record_answer(truth, from_copy=False)
            raise RuntimeError(f"merged zone publish exhausted attempts for {key}")

    def _answer_from_copy(self, published: MergedZoneMapping | None, truth_err: Exception) -> dict[int, int]:
        key = self._copies.key()
        if self._answered is None and not isinstance(truth_err, ShardZeroUnreachableError):
            raise RuntimeError(
                f"directory shard 0 failed with a non-connectivity error; refusing to start from etcd copy {key}: {truth_err}"
            ) from truth_err
        if published is None:
            raise RuntimeError(
                f"directory shard 0 is unreadable and etcd holds no copy {key} yet: {truth_err}"
            ) from truth_err
        _validate_mapping(published)
        if self._answered is None:
            logger.error(
                "[MergedZoneCache] directory shard 0 is unreadable at startup (%s); starting from version %d in %s",
                truth_err, published.version, key,
            )
            return self._record_answer(published, from_copy=True)
        self._check_against_answered(published, f"the etcd copy {key}")
        if published.version == self._answered.version:
            raise RuntimeError(
                f"directory shard 0 is unreadable; keeping version {self._answered.version} which matches {key}: {truth_err}"
            ) from truth_err
        logger.error(
            "[MergedZoneCache] directory shard 0 is unreadable (%s); adopting version %d from %s",
            truth_err, published.version, key,
        )
        return self._record_answer(published, from_copy=True)

    def _check_against_answered(self, nxt: MergedZoneMapping, source: str) -> None:
        prev = self._answered
        if prev is None:
            return
        if nxt.generation != prev.generation:
            raise ValueError(
                f"{source} is directory generation {nxt.generation}, but this process answers from generation {prev.generation}"
            )
        if nxt.version < prev.version:
            raise ValueError(
                f"{source} is at version {nxt.version}, behind version {prev.version} already answered by this process"
            )
        if nxt.version == prev.version and nxt.merges != prev.merges:
            raise ValueError(
                f"{source} holds version {nxt.version} with contents different from what this process answers with"
            )

    def _record_answer(self, mapping: MergedZoneMapping, *, from_copy: bool) -> dict[int, int]:
        cloned = dict(mapping.merges)
        self._answered = MergedZoneMapping(
            generation=mapping.generation,
            version=mapping.version,
            merges=cloned,
        )
        self._from_copy = from_copy
        return dict(cloned)
