"""目标节点发现、轨道过滤、Aperture 子集选择与下游连接池。"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from typing import Callable

import grpc
from google.protobuf import json_format

from ..grpc_runtime import GrpcClient
from ..protocol import Protocol
from .breaker import EndpointCircuitBreaker
from .config import parse_release_track

logger = logging.getLogger(__name__)
_FULL_RESYNC_SEC = 300.0
_RETRY_BACKOFF_SEC = 3.0


@dataclass(frozen=True, slots=True)
class NodeEntry:
    node_id: int
    node_type: int
    node_uuid: str
    zone_id: int
    endpoint: str
    release_track: str


def format_endpoint(ip: str, port: int) -> str | None:
    """严格要求 grpc_endpoint 有效，绝不回退到 Raw TCP endpoint。"""
    if not ip or not 0 < port <= 65535:
        return None
    if ":" in ip and not ip.startswith("["):
        return f"[{ip}]:{port}"
    return f"{ip}:{port}"


def parse_node_entry(protocol: Protocol, raw: bytes) -> NodeEntry | None:
    try:
        info = json_format.Parse(raw.decode("utf-8"), protocol.message("NodeInfo"), ignore_unknown_fields=True)
    except (UnicodeDecodeError, json_format.ParseError):
        return None
    endpoint = format_endpoint(info.grpc_endpoint.ip, info.grpc_endpoint.port)
    if endpoint is None:
        return None
    track = parse_release_track(info.release_track)
    if track is None:
        return None
    return NodeEntry(
        node_id=info.node_id,
        node_type=info.node_type,
        node_uuid=info.node_uuid,
        zone_id=info.zone_id,
        endpoint=endpoint,
        release_track=track,
    )


def peer_rank(peers: list[NodeEntry], self_uuid: str, self_track: str) -> tuple[int, int]:
    """计算同轨道 Router 集群中本节点的字典序排名与总实例数。"""
    uuids = {entry.node_uuid for entry in peers if entry.release_track == self_track and entry.node_uuid}
    if self_uuid:
        uuids.add(self_uuid)
    ordered = sorted(uuids)
    if not ordered:
        return 0, 1
    return (ordered.index(self_uuid) if self_uuid in ordered else 0), len(ordered)


def aperture_subset_indices(
    target_count: int,
    *,
    rank: int,
    peer_count: int,
    subset_size: int,
    rng: random.Random,
) -> list[int]:
    """与 Go internal/discovery/subset.go 一致的确定性 Aperture 子集抽取顺序。"""
    if target_count <= 0:
        return []
    if subset_size <= 0 or peer_count <= 1 or target_count <= subset_size:
        indices = list(range(target_count))
        rng.shuffle(indices)
        return indices
    width_units = max(1, (2 * subset_size * peer_count + target_count) // (2 * target_count))
    if width_units >= peer_count:
        indices = list(range(target_count))
        rng.shuffle(indices)
        return indices
    cells = peer_count * target_count
    start = rank * target_count
    width = width_units * target_count
    primary = ((start + rng.randrange(width)) % cells) // peer_count
    arc: list[int] = []
    seen: set[int] = set()
    for offset in range(width):
        idx = ((start + offset) % cells) // peer_count
        if idx not in seen:
            seen.add(idx)
            arc.append(idx)
    others = [idx for idx in arc if idx != primary]
    rng.shuffle(others)
    return [primary, *others]


class TargetDirectory:
    """单一目标节点类型的内存快照（支持测试注入与 etcd 监听驱动）。"""

    def __init__(
        self,
        node_type_name: str,
        *,
        on_evict: Callable[[str], None] | None = None,
    ) -> None:
        self.node_type_name = node_type_name
        self._on_evict = on_evict
        self._entries: dict[bytes, NodeEntry] = {}

    def entries(self) -> list[NodeEntry]:
        return list(self._entries.values())

    def replace_all(self, mapping: dict[bytes, NodeEntry]) -> None:
        old_endpoints = {entry.endpoint for entry in self._entries.values()}
        self._entries = dict(mapping)
        new_endpoints = {entry.endpoint for entry in self._entries.values()}
        if self._on_evict is not None:
            for removed in old_endpoints - new_endpoints:
                self._on_evict(removed)

    def put(self, key: bytes, entry: NodeEntry | None) -> None:
        previous = self._entries.get(key)
        if entry is None:
            self.delete(key)
            return
        self._entries[key] = entry
        if (
            self._on_evict is not None
            and previous is not None
            and previous.endpoint != entry.endpoint
            and not any(item.endpoint == previous.endpoint for item in self._entries.values())
        ):
            self._on_evict(previous.endpoint)

    def delete(self, key: bytes) -> None:
        removed = self._entries.pop(key, None)
        if (
            self._on_evict is not None
            and removed is not None
            and not any(item.endpoint == removed.endpoint for item in self._entries.values())
        ):
            self._on_evict(removed.endpoint)


class EtcdTargetWatcher:
    """监听 `<NodeTypeName>.rpc/` 前缀，定期全量对账并在节点下线时触发清理。"""

    def __init__(
        self,
        protocol: Protocol,
        etcd_endpoint: str,
        directory: TargetDirectory,
        *,
        resync_sec: float = _FULL_RESYNC_SEC,
    ) -> None:
        self.protocol = protocol
        self.directory = directory
        self._client = GrpcClient(etcd_endpoint, timeout=4)
        self._prefix = f"{directory.node_type_name}.rpc/".encode("utf-8")
        self._range_end = self._prefix[:-1] + bytes((self._prefix[-1] + 1,))
        self._resync_sec = resync_sec
        self._task: asyncio.Task | None = None
        self._closed = False

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"router-watch-{self.directory.node_type_name}")

    async def _full_sync(self) -> int:
        req = self.protocol.message("etcdserverpb.RangeRequest", key=self._prefix, range_end=self._range_end)
        raw = await self._client.call("/etcdserverpb.KV/Range", req.SerializeToString())
        resp = self.protocol.parse("etcdserverpb.RangeResponse", raw)
        snapshot: dict[bytes, NodeEntry] = {}
        for kv in resp.kvs:
            if b"/allocated/" in kv.key:
                continue
            entry = parse_node_entry(self.protocol, kv.value)
            if entry is not None:
                snapshot[kv.key] = entry
        self.directory.replace_all(snapshot)
        return int(resp.header.revision)

    async def _run(self) -> None:
        while not self._closed:
            call = None
            try:
                revision = await self._full_sync()
                call = self._client.channel.stream_stream("/etcdserverpb.Watch/Watch")()
                watch_req = self.protocol.message(
                    "etcdserverpb.WatchRequest",
                    create_request={
                        "key": self._prefix,
                        "range_end": self._range_end,
                        "start_revision": revision + 1,
                    },
                )
                await call.write(watch_req.SerializeToString())
                deadline = time.monotonic() + self._resync_sec
                while not self._closed:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(call.read(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    if raw is grpc.aio.EOF:
                        break
                    resp = self.protocol.parse("etcdserverpb.WatchResponse", raw)
                    if resp.canceled:
                        break
                    for event in resp.events:
                        if b"/allocated/" in event.kv.key:
                            continue
                        if event.type == 0:
                            self.directory.put(event.kv.key, parse_node_entry(self.protocol, event.kv.value))
                        else:
                            self.directory.delete(event.kv.key)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("router etcd watch failed type=%s", self.directory.node_type_name, exc_info=True)
                await asyncio.sleep(_RETRY_BACKOFF_SEC)
            finally:
                if call is not None:
                    call.cancel()

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._client.close()


class ChannelPool:
    """按目标端点缓存 grpc.aio 通道，定时回收空闲连接。"""

    def __init__(
        self,
        *,
        forward_timeout_ms: int = 5000,
        idle_timeout_sec: float = 600.0,
        clock=time.monotonic,
        client_factory=None,
    ) -> None:
        self._timeout_sec = forward_timeout_ms / 1000.0
        self._idle_timeout_sec = idle_timeout_sec
        self._clock = clock
        self._factory = client_factory or (lambda ep: GrpcClient(ep, timeout=self._timeout_sec, enable_retries=False))
        self._clients: dict[str, tuple[GrpcClient, float]] = {}

    def acquire(self, endpoint: str) -> GrpcClient:
        now = self._clock()
        existing = self._clients.get(endpoint)
        if existing is not None:
            client, _ = existing
            self._clients[endpoint] = (client, now)
            return client
        client = self._factory(endpoint)
        self._clients[endpoint] = (client, now)
        return client

    def evict(self, endpoint: str) -> None:
        entry = self._clients.pop(endpoint, None)
        if entry is not None:
            asyncio.create_task(entry[0].close())

    async def sweep_idle(self) -> int:
        cutoff = self._clock() - self._idle_timeout_sec
        stale = [ep for ep, (_, last_used) in self._clients.items() if last_used <= cutoff]
        for ep in stale:
            entry = self._clients.pop(ep, None)
            if entry is not None:
                await entry[0].close()
        return len(stale)

    async def close(self) -> None:
        clients = [client for client, _ in self._clients.values()]
        self._clients.clear()
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)


def pick_target(
    directory: TargetDirectory,
    breaker: EndpointCircuitBreaker,
    *,
    zone_scoped: bool,
    zone_id: int,
    session_track: str,
    subset_size: int,
    peers: list[NodeEntry],
    self_uuid: str,
    self_track: str,
    rng: random.Random,
) -> tuple[str | None, str]:
    """返回 (endpoint, reason)；reason 为 'ok' | 'no_target' | 'circuit_open'。"""
    all_entries = directory.entries()
    if zone_scoped:
        candidates = [entry for entry in all_entries if entry.zone_id == zone_id]
        if not candidates:
            return None, "no_target"
        start = rng.randrange(len(candidates))
        for offset in range(len(candidates)):
            ep = candidates[(start + offset) % len(candidates)].endpoint
            if breaker.allow(ep):
                return ep, "ok"
        return None, "circuit_open"

    if session_track == "canary":
        candidates = [entry for entry in all_entries if entry.release_track == "canary"]
        if not candidates:
            candidates = [entry for entry in all_entries if entry.release_track == "stable"]
    else:
        candidates = [entry for entry in all_entries if entry.release_track == "stable"]
    if not candidates:
        return None, "no_target"

    candidates.sort(key=lambda item: (item.node_uuid, item.endpoint))
    rank, total_peers = peer_rank(peers, self_uuid, self_track)
    order = aperture_subset_indices(
        len(candidates), rank=rank, peer_count=total_peers, subset_size=subset_size, rng=rng,
    )
    for idx in order:
        ep = candidates[idx].endpoint
        if breaker.allow(ep):
            return ep, "ok"
    return None, "circuit_open"
