"""client_rpc_router 配置解析与启动期严格校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass

from ..protocol import Protocol
from ..settings import Settings

ROUTER_NODE_TYPE = 29  # common.base.eNodeType.ClientRpcRouterNodeService
VALID_RELEASE_TRACKS = frozenset({"", "stable", "canary"})
DEFAULT_IDLE_CONN_TIMEOUT_SEC = 600.0
DEFAULT_IDLE_CONN_SWEEP_SEC = 60.0


def parse_release_track(raw: str) -> str | None:
    """严格解析发布轨道：仅允许空串/stable/canary，不做去空白或大小写折叠。"""
    if raw in ("", "stable"):
        return "stable"
    if raw == "canary":
        return "canary"
    return None


@dataclass(frozen=True, slots=True)
class RouterConfig:
    node_id: int = 106
    host: str = "0.0.0.0"
    grpc_port: int = 50600
    advertise_host: str = "127.0.0.1"
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    lease_ttl_sec: int = 60
    forward_timeout_ms: int = 5000
    server_timeout_ms: int = 6000
    idle_conn_timeout_sec: float = DEFAULT_IDLE_CONN_TIMEOUT_SEC
    idle_conn_sweep_sec: float = DEFAULT_IDLE_CONN_SWEEP_SEC
    zone_scoped_node_types: tuple[str, ...] = ("LoginNodeService",)
    global_target_subset_size: int = 4
    breaker_failure_threshold: int = 5
    breaker_open_duration_ms: int = 10000

    def validate(self, protocol: Protocol) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"router node_id 无效: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"router grpc_port 无效: {self.grpc_port}")
        if self.zone_id < 1:
            raise ValueError(f"router zone_id 必须 >= 1: {self.zone_id}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("router lease_ttl_sec 必须 > 0")
        if self.forward_timeout_ms <= 0 or self.forward_timeout_ms >= self.idle_conn_timeout_sec * 1000:
            raise ValueError("router forward_timeout_ms 必须在 (0, idle_conn_timeout_ms) 内")
        if self.server_timeout_ms != 0 and self.server_timeout_ms <= self.forward_timeout_ms:
            raise ValueError("router server_timeout_ms 必须为 0 或严格大于 forward_timeout_ms")
        if self.global_target_subset_size < 0 or self.global_target_subset_size == 1:
            raise ValueError("router global_target_subset_size 必须为 0 或 >= 2")
        if self.breaker_failure_threshold < 0 or self.breaker_open_duration_ms < 0:
            raise ValueError("router 熔断器参数不得为负")
        if parse_release_track(self.release_track) is None:
            raise ValueError(f"router release_track 无效: {self.release_track!r}")
        enum_names = protocol.pool.FindEnumTypeByName("common.base.eNodeType").values_by_name
        for name in self.zone_scoped_node_types:
            if name not in enum_names:
                raise ValueError(f"未知的 zone_scoped_node_types 节点类型: {name}")

    @classmethod
    def from_settings(cls, settings: Settings, protocol: Protocol) -> "RouterConfig":
        raw = dict(getattr(settings, "router_node", {}) or {})
        env_track = os.environ.get("MMORPG_RELEASE_TRACK")
        track_raw = env_track if env_track is not None else raw.get("release_track", settings.release_track)
        if parse_release_track(track_raw) is None:
            raise ValueError(f"router release_track 无效: {track_raw!r}")
        advertise = os.environ.get("POD_IP") or raw.get("advertise_host") or raw.get("host") or "127.0.0.1"
        if advertise in ("0.0.0.0", "::"):
            advertise = "127.0.0.1"
        zone_types = tuple(raw.get("zone_scoped_node_types", ("LoginNodeService",)))
        cfg = cls(
            node_id=int(raw.get("node_id", 106)),
            host=str(raw.get("host", "0.0.0.0")),
            grpc_port=int(raw.get("grpc_port", 50600)),
            advertise_host=str(advertise),
            zone_id=int(raw.get("zone_id", settings.zone_id)),
            physical_region=int(raw.get("physical_region", settings.physical_region)),
            release_track=parse_release_track(track_raw) or "stable",
            lease_ttl_sec=int(raw.get("lease_ttl_sec", 60)),
            forward_timeout_ms=int(raw.get("forward_timeout_ms", 5000)),
            server_timeout_ms=int(raw.get("server_timeout_ms", 6000)),
            idle_conn_timeout_sec=float(raw.get("idle_conn_timeout_sec", DEFAULT_IDLE_CONN_TIMEOUT_SEC)),
            idle_conn_sweep_sec=float(raw.get("idle_conn_sweep_sec", DEFAULT_IDLE_CONN_SWEEP_SEC)),
            zone_scoped_node_types=zone_types,
            global_target_subset_size=int(raw.get("global_target_subset_size", 4)),
            breaker_failure_threshold=int(raw.get("breaker_failure_threshold", 5)),
            breaker_open_duration_ms=int(raw.get("breaker_open_duration_ms", 10000)),
        )
        cfg.validate(protocol)
        return cfg
