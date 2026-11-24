"""player_locator 服务配置与启动期参数校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass

from ..settings import Settings

PLAYER_LOCATOR_NODE_TYPE = 24  # common.base.eNodeType.PlayerLocatorNodeService
DEFAULT_GRPC_PORT = 50300
DEFAULT_DISCONNECT_LEASE_SEC = 30
DEFAULT_POLL_INTERVAL_SEC = 1.0
DEFAULT_BATCH_SIZE = 100
DEFAULT_SCAN_ROUND_SEC = 16.0
DEFAULT_CLAIM_TTL_SEC = 30.0
DEFAULT_CLAIM_HEARTBEAT_SEC = 10.0
DEFAULT_SCENE_MANAGER_TIMEOUT_SEC = 5.0
HOLDER_RELEASE_FIRST_RETRY_DELAY_SEC = 2.0
AFK_PASS_LEASE_TTL_SEC = 300
AFK_PASS_PROBE_RETRY_SEC = 10
DEFAULT_RECONCILE_INTERVAL_SEC = 60.0
DEFAULT_DEAD_GATE_MEMBERS_PER_ROUND = 200
DEFAULT_GATE_SCAN_COUNT = 100
DEFAULT_LIVE_VIEW_HOLD_SEC = 1800.0


def parse_release_track(raw: str) -> str | None:
    """严格解析发布轨道：仅接受空串/stable/canary，不做去空白或大小写折叠。"""
    if raw in ("", "stable"):
        return "stable"
    if raw == "canary":
        return "canary"
    return None


@dataclass(frozen=True, slots=True)
class PlayerLocatorConfig:
    node_id: int = 103
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = "127.0.0.1"
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    etcd_lease_ttl_sec: int = 60
    default_disconnect_lease_sec: int = DEFAULT_DISCONNECT_LEASE_SEC
    poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC
    batch_size: int = DEFAULT_BATCH_SIZE
    scan_round_sec: float = DEFAULT_SCAN_ROUND_SEC
    claim_ttl_sec: float = DEFAULT_CLAIM_TTL_SEC
    claim_heartbeat_sec: float = DEFAULT_CLAIM_HEARTBEAT_SEC
    scene_manager_timeout_sec: float = DEFAULT_SCENE_MANAGER_TIMEOUT_SEC
    reconcile_interval_sec: float = DEFAULT_RECONCILE_INTERVAL_SEC
    dead_gate_members_per_round: int = DEFAULT_DEAD_GATE_MEMBERS_PER_ROUND
    gate_scan_count: int = DEFAULT_GATE_SCAN_COUNT
    live_view_hold_sec: float = DEFAULT_LIVE_VIEW_HOLD_SEC

    def validate(self) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"player_locator node_id 无效: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"player_locator grpc_port 无效: {self.grpc_port}")
        if self.zone_id < 1:
            raise ValueError(f"player_locator zone_id 必须 >= 1: {self.zone_id}")
        if parse_release_track(self.release_track) is None:
            raise ValueError(f"player_locator release_track 无效: {self.release_track!r}")
        if self.etcd_lease_ttl_sec <= 0:
            raise ValueError("player_locator etcd_lease_ttl_sec 必须 > 0")
        if self.default_disconnect_lease_sec <= 0:
            raise ValueError("player_locator default_disconnect_lease_sec 必须 > 0")
        if self.poll_interval_sec <= 0:
            raise ValueError("player_locator poll_interval_sec 必须 > 0")
        if self.batch_size <= 0:
            raise ValueError("player_locator batch_size 必须 > 0")
        if self.scan_round_sec <= 0:
            raise ValueError("player_locator scan_round_sec 必须 > 0")
        if self.claim_ttl_sec <= 0 or self.claim_heartbeat_sec <= 0:
            raise ValueError("player_locator claim TTL 与心跳间隔必须 > 0")
        if self.scene_manager_timeout_sec <= 0:
            raise ValueError("player_locator scene_manager_timeout_sec 必须 > 0")
        if self.dead_gate_members_per_round <= 0:
            raise ValueError("player_locator dead_gate_members_per_round 必须 > 0")
        if self.gate_scan_count <= 0:
            raise ValueError("player_locator gate_scan_count 必须 > 0")
        if self.live_view_hold_sec < 0:
            raise ValueError("player_locator live_view_hold_sec 必须 >= 0")

    @classmethod
    def from_settings(cls, settings: Settings) -> "PlayerLocatorConfig":
        raw = dict(getattr(settings, "player_locator_node", {}) or {})
        env_track = os.environ.get("MMORPG_RELEASE_TRACK")
        track_raw = env_track if env_track is not None else raw.get("release_track", settings.release_track)
        parsed_track = parse_release_track(track_raw)
        if parsed_track is None:
            raise ValueError(f"player_locator release_track 无效: {track_raw!r}")
        advertise = os.environ.get("POD_IP") or raw.get("advertise_host") or raw.get("host") or "127.0.0.1"
        if advertise in ("0.0.0.0", "::"):
            advertise = "127.0.0.1"
        cfg = cls(
            node_id=int(raw.get("node_id", 103)),
            host=str(raw.get("host", "0.0.0.0")),
            grpc_port=int(raw.get("grpc_port", DEFAULT_GRPC_PORT)),
            advertise_host=str(advertise),
            zone_id=int(raw.get("zone_id", settings.zone_id)),
            physical_region=int(raw.get("physical_region", settings.physical_region)),
            release_track=parsed_track,
            etcd_lease_ttl_sec=int(raw.get("etcd_lease_ttl_sec", raw.get("lease_ttl_sec", 60))),
            default_disconnect_lease_sec=int(raw.get("default_disconnect_lease_sec", DEFAULT_DISCONNECT_LEASE_SEC)),
            poll_interval_sec=float(raw.get("poll_interval_sec", DEFAULT_POLL_INTERVAL_SEC)),
            batch_size=int(raw.get("batch_size", DEFAULT_BATCH_SIZE)),
            scan_round_sec=float(raw.get("scan_round_sec", DEFAULT_SCAN_ROUND_SEC)),
            claim_ttl_sec=float(raw.get("claim_ttl_sec", DEFAULT_CLAIM_TTL_SEC)),
            claim_heartbeat_sec=float(raw.get("claim_heartbeat_sec", DEFAULT_CLAIM_HEARTBEAT_SEC)),
            scene_manager_timeout_sec=float(raw.get("scene_manager_timeout_sec", DEFAULT_SCENE_MANAGER_TIMEOUT_SEC)),
            reconcile_interval_sec=float(raw.get("reconcile_interval_sec", DEFAULT_RECONCILE_INTERVAL_SEC)),
            dead_gate_members_per_round=int(raw.get("dead_gate_members_per_round", DEFAULT_DEAD_GATE_MEMBERS_PER_ROUND)),
            gate_scan_count=int(raw.get("gate_scan_count", DEFAULT_GATE_SCAN_COUNT)),
            live_view_hold_sec=float(raw.get("live_view_hold_sec", DEFAULT_LIVE_VIEW_HOLD_SEC)),
        )
        cfg.validate()
        return cfg
