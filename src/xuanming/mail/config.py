"""mail 节点配置解析与启动期 fail-closed 校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import (
    DATABASE_NAME,
    MailLimits,
    SENDER_KINDS,
    SWEEP_MODE_DELETE,
    SWEEP_MODE_REPORT_ONLY,
)
from .notify import DEFAULT_NOTIFY_TIMEOUT_SEC
from .sweep import (
    DEFAULT_SWEEP_BATCH_LIMIT,
    DEFAULT_SWEEP_INTERVAL_SEC,
    DEFAULT_SWEEP_RETENTION_DAYS,
)

# 密钥与数据库口令环境变量名（与 Go mail/etc/mail.yaml 完全对齐）
COORDINATOR_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
ROLE_FENCE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_MAIL"
ASSET_OP_SECRET_ENV = "MMORPG_ASSET_OP_SECRET_MAIL"
MYSQL_PASSWORD_ENV = "MAIL_MYSQL_PASSWORD"

PRODUCER_SECRET_ENVS: dict[str, str] = {
    "admin": "MAIL_PRODUCER_SECRET_ADMIN",
    "guild": "MAIL_PRODUCER_SECRET_GUILD",
    "rank": "MAIL_PRODUCER_SECRET_RANK",
    "trade": "MAIL_PRODUCER_SECRET_TRADE",
    "battle": "MAIL_PRODUCER_SECRET_BATTLE",
}

DEFAULT_GRPC_PORT = 50900
DEFAULT_LEASE_TTL_SEC = 60
MIN_HMAC_SECRET_BYTES = 32

_TOML_FORBIDDEN_FIELDS = frozenset((
    "producer_secrets",
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


def load_producer_secrets(environ) -> tuple[tuple[str, bytes], ...]:
    """加载所有已配置的发件方 HMAC 密钥；若某密钥非空但不足 32 字节则立即拒启。"""
    loaded: list[tuple[str, bytes]] = []
    for caller, env_name in sorted(PRODUCER_SECRET_ENVS.items()):
        raw_str = environ.get(env_name, "").strip()
        if not raw_str:
            continue
        raw = raw_str.encode("utf-8")
        if len(raw) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        loaded.append((caller, raw))
    if not loaded:
        raise ValueError("at least one MAIL_PRODUCER_SECRET_* must be configured")
    return tuple(loaded)


@dataclass(frozen=True, slots=True)
class MailConfig:
    """mail 节点配置：由 ``[mail_node]`` 与环境变量组装。"""

    node_id: int = 108
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
    guild_service: str = ""
    redis: str = ""
    player_redis: str = ""
    kafka: str = ""
    kafka_generation: int = 1
    kafka_partitions: int = 256
    notify_timeout_sec: float = DEFAULT_NOTIFY_TIMEOUT_SEC
    tables_dir: str = ""
    bootstrap_schema: bool = False
    limits: MailLimits = MailLimits()
    sweep_mode: str = SWEEP_MODE_REPORT_ONLY
    sweep_interval_sec: float = DEFAULT_SWEEP_INTERVAL_SEC
    sweep_retention_days: int = DEFAULT_SWEEP_RETENTION_DAYS
    sweep_batch_limit: int = DEFAULT_SWEEP_BATCH_LIMIT
    asset_loop_interval_sec: float = 2.0
    asset_loop_batch: int = 100
    asset_loop_workers: int = 8
    producer_secrets: tuple[tuple[str, bytes], ...] = ()
    asset_op_secret: bytes = b""
    coordinator_secret: bytes = b""
    role_fence_secret: bytes = b""

    def producer_secret_map(self) -> dict[str, bytes]:
        return dict(self.producer_secrets)

    @property
    def effective_player_redis(self) -> str:
        """未单独指定 player_redis 时回落到 redis（与 Go mail.go playerRedisConfig 对齐）。"""
        return self.player_redis or self.redis

    @property
    def runs_background_work(self) -> bool:
        """canary 副本仅承接客户端 RPC，停用附件重投循环与过期清理。"""
        return self.release_track == "stable"

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "MailConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "mail_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[mail_node] missing or has invalid fields: {sorted(unknown)}")
        if "limits" in raw and isinstance(raw["limits"], dict):
            raw["limits"] = MailLimits(**raw["limits"])
        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        raw.setdefault("redis", settings.redis)
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
            producer_secrets=load_producer_secrets(environ),
            asset_op_secret=load_required_secret(environ, ASSET_OP_SECRET_ENV),
            coordinator_secret=load_required_secret(environ, COORDINATOR_SECRET_ENV),
            role_fence_secret=load_required_secret(environ, ROLE_FENCE_SECRET_ENV),
        )
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"mail node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"mail grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("mail_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"mail zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"mail release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("mail lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"mail mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 1 <= self.max_open_conn <= 256 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("mail MySQL pool bounds invalid")
        if self.sql_timeout_sec <= 0 or self.request_budget_sec <= 0:
            raise ValueError("mail timeout/budget must be > 0")
        if self.kafka_generation < 1 or self.kafka_partitions < 1:
            raise ValueError("mail kafka generation/partitions must be >= 1")
        if self.notify_timeout_sec <= 0:
            raise ValueError("mail notify_timeout_sec must be > 0")
        self.limits.validate()
        if self.sweep_mode not in (SWEEP_MODE_REPORT_ONLY, SWEEP_MODE_DELETE):
            raise ValueError(f"mail sweep_mode invalid: {self.sweep_mode!r}")
        if not 1.0 <= self.sweep_interval_sec <= 86400.0:
            raise ValueError("mail sweep_interval_sec must be within [1, 86400]")
        if not 1 <= self.sweep_retention_days <= 36500:
            raise ValueError("mail sweep_retention_days must be within [1, 36500]")
        if not 1 <= self.sweep_batch_limit <= 10000:
            raise ValueError("mail sweep_batch_limit must be within [1, 10000]")
        if self.asset_loop_interval_sec <= 0 or not 1 <= self.asset_loop_workers <= 64:
            raise ValueError("mail asset_loop interval/workers invalid")
        if self.asset_loop_batch < self.asset_loop_workers:
            raise ValueError("mail asset_loop_batch must be >= asset_loop_workers")
        for caller, secret in self.producer_secrets:
            if caller not in SENDER_KINDS or len(secret) < MIN_HMAC_SECRET_BYTES:
                raise ValueError(f"mail producer secret invalid for caller={caller!r}")
        if not self.producer_secrets:
            raise ValueError("mail requires at least one producer secret")
        if len(self.asset_op_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ASSET_OP_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.coordinator_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{COORDINATOR_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.role_fence_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ROLE_FENCE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if not self.data_service:
            raise ValueError("mail requires data_service gRPC endpoint")
        if settings is not None and not settings.etcd:
            raise ValueError("mail requires etcd endpoint")
