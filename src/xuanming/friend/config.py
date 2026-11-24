"""friend 节点配置解析与启动期 fail-closed 校验（与 Go friend/internal/config 对齐）。"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import DATABASE_NAME, FriendRules

COORDINATOR_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
ROLE_FENCE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_FRIEND"
MYSQL_PASSWORD_ENV = "FRIEND_MYSQL_PASSWORD"

DEFAULT_GRPC_PORT = 50400
DEFAULT_LEASE_TTL_SEC = 60
MIN_RPC_TIMEOUT_SEC = 1.0
MAX_RPC_TIMEOUT_SEC = 4.0
IN_BAND_REPLY_RESERVE_SEC = 0.5
MIN_HMAC_SECRET_BYTES = 32

_TOML_FORBIDDEN_FIELDS = frozenset(("mysql_password", "coordinator_secret", "role_fence_secret"))


def load_required_secret(environ, env_name: str) -> bytes:
    """加载必选 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


@dataclass(frozen=True, slots=True)
class FriendConfig:
    """friend 节点配置：由 ``[friend_node]`` 与环境变量组装。"""

    node_id: int = 110
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = ""
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC
    rpc_timeout_sec: float = MAX_RPC_TIMEOUT_SEC
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = DATABASE_NAME
    mysql_password: str = ""
    max_open_conn: int = 20
    min_open_conn: int = 1
    data_service: str = ""
    control_redis: str = ""
    player_redis: str = ""
    kafka: str = ""
    kafka_generation: int = 1
    kafka_partitions: int = 256
    bootstrap_schema: bool = False
    coordinator_secret: bytes = b""
    role_fence_secret: bytes = b""

    # 好友业务规则参数（默认值与 Go config.FriendConf 一致）
    max_friends: int = 200
    max_pending_requests: int = 50
    max_incoming_requests: int = 200
    max_blocks: int = 200
    recommend_default_limit: int = 10
    recommend_max_limit: int = 20
    recommend_max_exclude: int = 64
    request_quota_per_minute: int = 10
    list_read_hard_limit: int = 1000
    cache_ttl_sec: int = 1800

    @property
    def request_budget_sec(self) -> float:
        """单次请求 I/O 业务预算 = rpc_timeout_sec - IN_BAND_REPLY_RESERVE_SEC。"""
        return max(0.0, self.rpc_timeout_sec - IN_BAND_REPLY_RESERVE_SEC)

    @property
    def effective_player_redis(self) -> str:
        """未单独指定 player_redis 时回落到 control_redis（与 Go EffectivePlayerRedis 一致）。"""
        return self.player_redis or self.control_redis

    def to_rules(self) -> FriendRules:
        return FriendRules(
            max_friends=self.max_friends,
            max_pending_requests=self.max_pending_requests,
            max_incoming_requests=self.max_incoming_requests,
            max_blocks=self.max_blocks,
            recommend_default_limit=self.recommend_default_limit,
            recommend_max_limit=self.recommend_max_limit,
            recommend_max_exclude=self.recommend_max_exclude,
            request_quota_per_minute=self.request_quota_per_minute,
            list_read_hard_limit=self.list_read_hard_limit,
            cache_ttl_sec=self.cache_ttl_sec,
        )

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "FriendConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "friend_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[friend_node] missing or has invalid fields: {sorted(unknown)}")
        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        raw.setdefault("player_redis", settings.redis)
        raw.setdefault("control_redis", settings.redis)
        raw.setdefault("kafka", settings.kafka)
        raw.setdefault("kafka_generation", settings.kafka_generation)
        raw.setdefault("kafka_partitions", settings.kafka_partitions)
        if not raw.get("advertise_host"):
            pod_ip = environ.get("POD_IP", "").strip()
            if pod_ip:
                raw["advertise_host"] = pod_ip
        config = cls(
            **raw,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            coordinator_secret=load_required_secret(environ, COORDINATOR_SECRET_ENV),
            role_fence_secret=load_required_secret(environ, ROLE_FENCE_SECRET_ENV),
        )
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"friend node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"friend grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("friend_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"friend zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"friend release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("friend lease_ttl_sec must be > 0")
        if not MIN_RPC_TIMEOUT_SEC <= self.rpc_timeout_sec <= MAX_RPC_TIMEOUT_SEC:
            raise ValueError(
                f"friend rpc_timeout_sec ({self.rpc_timeout_sec}) must be in [{MIN_RPC_TIMEOUT_SEC}, {MAX_RPC_TIMEOUT_SEC}]"
            )
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"friend mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 1 <= self.max_open_conn <= 256 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("friend MySQL pool bounds invalid")
        if not self.data_service:
            raise ValueError("friend requires data_service gRPC endpoint")
        if self.kafka_generation < 1 or self.kafka_partitions < 1:
            raise ValueError("friend kafka generation/partitions must be >= 1")
        if len(self.coordinator_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{COORDINATOR_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.role_fence_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ROLE_FENCE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        self.to_rules().validate()
        if settings is not None and not settings.etcd:
            raise ValueError("friend requires etcd endpoint")
