"""guild 节点配置解析与启动期 fail-closed 校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import DATABASE_NAME

# 密钥与口令环境变量名（与 Go guild/etc/guild.yaml 及 rolefence 对齐）
COORDINATOR_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
ROLE_FENCE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_GUILD"
ASSET_OP_SECRET_ENV = "MMORPG_ASSET_OP_SECRET_GUILD"
MYSQL_PASSWORD_ENV = "GUILD_MYSQL_PASSWORD"

# 显式区别于 player_locator 的 50300，避免本地同机端口冲突
DEFAULT_GRPC_PORT = 50310
DEFAULT_LEASE_TTL_SEC = 60
MIN_HMAC_SECRET_BYTES = 32
DEFAULT_TERMINAL_RETENTION_DAYS = 30
MIN_TERMINAL_RETENTION_DAYS = 7
MAX_TERMINAL_RETENTION_DAYS = 365
MAX_APPLICATION_QUOTA_PER_MINUTE = 600

_TOML_FORBIDDEN_FIELDS = frozenset((
    "asset_op_secret",
    "coordinator_secret",
    "role_fence_secret",
    "mysql_password",
))


def load_required_secret(environ, env_name: str) -> bytes:
    """加载必选 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


def load_optional_secret(environ, env_name: str, *, required: bool) -> bytes:
    """加载按开关要求的密钥：开启时必选且至少 32 字节，关闭时允许留空。"""
    if required:
        return load_required_secret(environ, env_name)
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if raw and len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


@dataclass(frozen=True, slots=True)
class GuildConfig:
    """guild 节点配置：由 ``[guild_node]`` 与环境变量组装。"""

    node_id: int = 109
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = ""
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = DATABASE_NAME
    mysql_password: str = ""
    max_open_conn: int = 16
    min_open_conn: int = 1
    sql_timeout_sec: float = 10.0
    request_budget_sec: float = 3.5
    data_service: str = ""
    content_guard: str = ""
    content_guard_optional: bool = False
    redis: str = ""
    tables_dir: str = ""
    bootstrap_schema: bool = False
    application_quota_per_minute: int = 10
    asset_op_enabled: bool = False
    asset_op_lease_ms: int = 10_000
    terminal_retention_days: int = DEFAULT_TERMINAL_RETENTION_DAYS
    trials_enabled: bool = False
    asset_op_secret: bytes = b""
    coordinator_secret: bytes = b""
    role_fence_secret: bytes = b""

    @property
    def runs_background_work(self) -> bool:
        """canary 副本仅承接客户端 RPC，停用全局后台任务。"""
        return self.release_track == "stable"

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "GuildConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "guild_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[guild_node] missing or has invalid fields: {sorted(unknown)}")
        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        raw.setdefault("redis", settings.redis)
        if not raw.get("advertise_host"):
            pod_ip = environ.get("POD_IP", "").strip()
            if pod_ip:
                raw["advertise_host"] = pod_ip

        asset_op_enabled = bool(raw.get("asset_op_enabled", False))
        config = cls(
            **raw,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            asset_op_secret=load_optional_secret(environ, ASSET_OP_SECRET_ENV, required=asset_op_enabled),
            coordinator_secret=load_required_secret(environ, COORDINATOR_SECRET_ENV),
            role_fence_secret=load_required_secret(environ, ROLE_FENCE_SECRET_ENV),
        )
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"guild node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"guild grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("guild_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"guild zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"guild release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("guild lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"guild mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 1 <= self.max_open_conn <= 128 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("guild MySQL pool bounds invalid")
        if self.sql_timeout_sec <= 0 or self.request_budget_sec <= 0:
            raise ValueError("guild timeout/budget must be > 0")
        if not 1 <= self.application_quota_per_minute <= MAX_APPLICATION_QUOTA_PER_MINUTE:
            raise ValueError(
                f"guild application_quota_per_minute must be within [1, {MAX_APPLICATION_QUOTA_PER_MINUTE}]"
            )
        if not MIN_TERMINAL_RETENTION_DAYS <= self.terminal_retention_days <= MAX_TERMINAL_RETENTION_DAYS:
            raise ValueError(
                f"guild terminal_retention_days must be within [{MIN_TERMINAL_RETENTION_DAYS}, {MAX_TERMINAL_RETENTION_DAYS}]"
            )
        if self.asset_op_enabled and len(self.asset_op_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ASSET_OP_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.coordinator_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{COORDINATOR_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.role_fence_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ROLE_FENCE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if not self.data_service:
            raise ValueError("guild requires data_service gRPC endpoint")
        if not self.content_guard and not self.content_guard_optional:
            raise ValueError("guild requires content_guard endpoint unless content_guard_optional=true")
        if settings is not None and not settings.etcd:
            raise ValueError("guild requires etcd endpoint")
