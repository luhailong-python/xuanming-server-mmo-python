"""scene_manager 服务配置、World.json 加载与启动期参数校验。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from ..settings import Settings

SCENE_MANAGER_NODE_TYPE = 25   # common.base.eNodeType.SceneManagerNodeService
SCENE_NODE_TYPE = 3            # common.base.eNodeType.SceneNodeService
GATE_NODE_TYPE = 4             # common.base.eNodeType.GateNodeService

DEFAULT_GRPC_PORT = 60000
DEFAULT_ETCD_LEASE_TTL_SEC = 60
DEFAULT_WORLD_CHANNEL_COUNT = 1
DEFAULT_CHANNEL_MAX_PLAYERS = 2000
DEFAULT_CHANNEL_SWITCH_COOLDOWN_SEC = 10
DEFAULT_LEAVE_RELEASE_WAIT_SEC = 600
DEFAULT_REENTRY_BARRIER_SEC = 20
DEFAULT_HOME_ZONE_TIMEOUT_SEC = 1.5
DEFAULT_ROLE_DIRECTORY_TIMEOUT_SEC = 1.5
DEFAULT_SCENE_NODE_RPC_TIMEOUT_SEC = 5.0
DEFAULT_KAFKA_WRITE_TIMEOUT_SEC = 5.0
DEFAULT_NODE_SYNC_INTERVAL_SEC = 5.0

HANDOFF_TTL_SEC = 300
REDIRECT_TOKEN_TTL_SEC = 300
ENTER_SCENE_DEDUPE_TTL_SEC = 60
SCENE_REGISTRATION_RECORD_TTL_SEC = 600
WORLD_INIT_LOCK_TTL_SEC = 60

ROLE_DIRECTORY_SECRET_ENV = "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE_MANAGER"
GATE_TOKEN_SECRET_ENV = "GATE_TOKEN_SECRET"

DEFAULT_WORLD_JSON_PATH = Path(__file__).resolve().parent.parent / "data" / "tables" / "World.json"


def parse_release_track(raw: str) -> str | None:
    """严格解析发布轨道：仅接受空串/stable/canary。"""
    if raw in ("", "stable"):
        return "stable"
    if raw == "canary":
        return "canary"
    return None


def load_world_conf_ids(path: Path | None = None) -> tuple[int, ...]:
    """从 World.json 读取大世界地图 scene_id 列表（保持表序，去重且忽略非正数）。"""
    target = DEFAULT_WORLD_JSON_PATH if path is None else path
    if not target.exists():
        return ()
    with target.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    seen: set[int] = set()
    ordered: list[int] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        conf_id = int(row.get("scene_id") or row.get("id") or 0)
        if conf_id > 0 and conf_id not in seen:
            seen.add(conf_id)
            ordered.append(conf_id)
    return tuple(ordered)


@dataclass(frozen=True, slots=True)
class SceneManagerConfig:
    """scene_manager 进程配置，字段语义与 Go config.Config 对齐。"""

    node_id: int = 104
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = "127.0.0.1"
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    etcd_lease_ttl_sec: int = DEFAULT_ETCD_LEASE_TTL_SEC
    world_channel_count: int = DEFAULT_WORLD_CHANNEL_COUNT
    world_channel_count_by_conf_id: dict[int, int] = field(default_factory=dict)
    strict_node_type_separation: bool = True
    allow_unsafe_cross_node_handoff: bool = False
    allow_gate_zone_as_home_zone: bool = False
    home_zone_lookup_timeout_sec: float = DEFAULT_HOME_ZONE_TIMEOUT_SEC
    role_directory_timeout_sec: float = DEFAULT_ROLE_DIRECTORY_TIMEOUT_SEC
    scene_node_rpc_timeout_sec: float = DEFAULT_SCENE_NODE_RPC_TIMEOUT_SEC
    kafka_write_timeout_sec: float = DEFAULT_KAFKA_WRITE_TIMEOUT_SEC
    node_sync_interval_sec: float = DEFAULT_NODE_SYNC_INTERVAL_SEC
    mirror_source_node_load_cap: int = 0
    mirror_dedup_by_source: bool = False
    gate_token_secret: bytes = b""
    require_client_endpoint: bool = False
    channel_switch_disabled: bool = False
    channel_switch_cooldown_sec: int = DEFAULT_CHANNEL_SWITCH_COOLDOWN_SEC
    channel_max_players: int = DEFAULT_CHANNEL_MAX_PLAYERS
    leave_holder_release_wait_sec: int = DEFAULT_LEAVE_RELEASE_WAIT_SEC
    reentry_barrier_sec: int = DEFAULT_REENTRY_BARRIER_SEC
    world_table_path: Path = DEFAULT_WORLD_JSON_PATH
    world_conf_ids: tuple[int, ...] = ()

    def effective_world_conf_ids(self) -> tuple[int, ...]:
        if self.world_conf_ids:
            return self.world_conf_ids
        return load_world_conf_ids(self.world_table_path)

    def channel_count_for(self, conf_id: int) -> int:
        override = self.world_channel_count_by_conf_id.get(conf_id, 0)
        if override > 0:
            return override
        return max(1, self.world_channel_count)

    def effective_channel_switch_cooldown_sec(self) -> int:
        if self.channel_switch_cooldown_sec < 0:
            return 0
        if self.channel_switch_cooldown_sec == 0:
            return DEFAULT_CHANNEL_SWITCH_COOLDOWN_SEC
        return min(self.channel_switch_cooldown_sec, 86400)

    def effective_channel_max_players(self) -> int:
        if self.channel_max_players <= 0:
            return DEFAULT_CHANNEL_MAX_PLAYERS
        return min(self.channel_max_players, 0xFFFFFFFF)

    def effective_leave_release_wait_sec(self) -> int:
        return self.leave_holder_release_wait_sec if self.leave_holder_release_wait_sec > 0 else DEFAULT_LEAVE_RELEASE_WAIT_SEC

    def validate(self) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"scene_manager node_id 无效: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"scene_manager grpc_port 无效: {self.grpc_port}")
        if self.zone_id < 1:
            raise ValueError(f"scene_manager zone_id 必须 >= 1: {self.zone_id}")
        if parse_release_track(self.release_track) is None:
            raise ValueError(f"scene_manager release_track 无效: {self.release_track!r}")
        if self.etcd_lease_ttl_sec <= 0:
            raise ValueError("scene_manager etcd_lease_ttl_sec 必须 > 0")
        if self.world_channel_count < 1:
            raise ValueError("scene_manager world_channel_count 必须 >= 1")
        if self.home_zone_lookup_timeout_sec <= 0 or self.role_directory_timeout_sec <= 0:
            raise ValueError("scene_manager DataService 超时配置必须 > 0")
        if self.scene_node_rpc_timeout_sec <= 0 or self.kafka_write_timeout_sec <= 0:
            raise ValueError("scene_manager 下游 RPC/Kafka 超时配置必须 > 0")
        if self.reentry_barrier_sec < 0:
            raise ValueError("scene_manager reentry_barrier_sec 不能为负数")

    @classmethod
    def from_settings(cls, settings: Settings) -> "SceneManagerConfig":
        raw = dict(getattr(settings, "scene_manager_node", {}) or {})
        env_track = os.environ.get("MMORPG_RELEASE_TRACK")
        track_raw = env_track if env_track is not None else raw.get("release_track", settings.release_track)
        parsed_track = parse_release_track(track_raw)
        if parsed_track is None:
            raise ValueError(f"scene_manager release_track 无效: {track_raw!r}")
        advertise = os.environ.get("POD_IP") or raw.get("advertise_host") or raw.get("host") or "127.0.0.1"
        if advertise in ("0.0.0.0", "::"):
            advertise = "127.0.0.1"

        raw_overrides = raw.get("world_channel_count_by_conf_id") or {}
        overrides = {int(k): int(v) for k, v in raw_overrides.items()}

        gate_secret_env = str(raw.get("gate_token_secret_env") or GATE_TOKEN_SECRET_ENV)
        gate_secret_raw = os.environ.get(gate_secret_env) or str(raw.get("gate_token_secret") or "")

        world_path = Path(raw["world_table_path"]) if raw.get("world_table_path") else DEFAULT_WORLD_JSON_PATH
        cfg = cls(
            node_id=int(raw.get("node_id", 104)),
            host=str(raw.get("host", "0.0.0.0")),
            grpc_port=int(raw.get("grpc_port", DEFAULT_GRPC_PORT)),
            advertise_host=str(advertise),
            zone_id=int(raw.get("zone_id", settings.zone_id)),
            physical_region=int(raw.get("physical_region", settings.physical_region)),
            release_track=parsed_track,
            etcd_lease_ttl_sec=int(raw.get("etcd_lease_ttl_sec", raw.get("lease_ttl_sec", DEFAULT_ETCD_LEASE_TTL_SEC))),
            world_channel_count=int(raw.get("world_channel_count", DEFAULT_WORLD_CHANNEL_COUNT)),
            world_channel_count_by_conf_id=overrides,
            strict_node_type_separation=bool(raw.get("strict_node_type_separation", True)),
            allow_unsafe_cross_node_handoff=bool(raw.get("allow_unsafe_cross_node_handoff", False)),
            allow_gate_zone_as_home_zone=bool(raw.get("allow_gate_zone_as_home_zone", False)),
            home_zone_lookup_timeout_sec=float(raw.get("home_zone_lookup_timeout_sec", DEFAULT_HOME_ZONE_TIMEOUT_SEC)),
            role_directory_timeout_sec=float(raw.get("role_directory_timeout_sec", DEFAULT_ROLE_DIRECTORY_TIMEOUT_SEC)),
            scene_node_rpc_timeout_sec=float(raw.get("scene_node_rpc_timeout_sec", DEFAULT_SCENE_NODE_RPC_TIMEOUT_SEC)),
            kafka_write_timeout_sec=float(raw.get("kafka_write_timeout_sec", DEFAULT_KAFKA_WRITE_TIMEOUT_SEC)),
            node_sync_interval_sec=float(raw.get("node_sync_interval_sec", DEFAULT_NODE_SYNC_INTERVAL_SEC)),
            mirror_source_node_load_cap=int(raw.get("mirror_source_node_load_cap", 0)),
            mirror_dedup_by_source=bool(raw.get("mirror_dedup_by_source", False)),
            gate_token_secret=gate_secret_raw.encode("utf-8"),
            require_client_endpoint=bool(raw.get("require_client_endpoint", False)),
            channel_switch_disabled=bool(raw.get("channel_switch_disabled", False)),
            channel_switch_cooldown_sec=int(raw.get("channel_switch_cooldown_sec", DEFAULT_CHANNEL_SWITCH_COOLDOWN_SEC)),
            channel_max_players=int(raw.get("channel_max_players", DEFAULT_CHANNEL_MAX_PLAYERS)),
            leave_holder_release_wait_sec=int(raw.get("leave_holder_release_wait_sec", DEFAULT_LEAVE_RELEASE_WAIT_SEC)),
            reentry_barrier_sec=int(raw.get("reentry_barrier_sec", DEFAULT_REENTRY_BARRIER_SEC)),
            world_table_path=world_path,
        )
        cfg.validate()
        return cfg
