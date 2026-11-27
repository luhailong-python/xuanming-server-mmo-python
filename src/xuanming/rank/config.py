"""rank 节点配置解析与启动期 fail-closed 校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import (
    DATABASE_NAME,
    reward_mail_window_ms,
)

DEFAULT_GRPC_PORT = 51100
DEFAULT_LEASE_TTL_SEC = 60
MIN_HMAC_SECRET_BYTES = 32
DEFAULT_MAIL_MAX_PERSONAL_TTL_DAYS = 30

# 默认环境变量名（与 Go go/rank/etc/rank.yaml 完全对齐）
DEFAULT_PRODUCER_SECRET_ENVS: tuple[tuple[str, str], ...] = (
    ("match", "RANK_PRODUCER_SECRET_MATCH"),
    ("admin", "RANK_PRODUCER_SECRET_ADMIN"),
)
DEFAULT_MAIL_SECRET_ENV = "MAIL_PRODUCER_SECRET_RANK"
MYSQL_PASSWORD_ENV = "RANK_MYSQL_PASSWORD"

_TOML_FORBIDDEN_FIELDS = frozenset((
    "producer_secrets",
    "mail_secret",
    "mysql_password",
))


def load_required_secret(environ, env_name: str) -> bytes:
    """加载必选 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    name = env_name.strip()
    if not name:
        raise ValueError("HMAC secret env var name cannot be empty")
    raw = environ.get(name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


@dataclass(frozen=True, slots=True)
class RankConfig:
    """rank 节点配置：由 ``[rank_node]`` 与环境变量组装。"""

    node_id: int = 110
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
    request_budget_sec: float = 3.0
    data_service: str = ""
    mail_service: str = ""
    bootstrap_schema: bool = False
    mail_secret_env: str = DEFAULT_MAIL_SECRET_ENV
    mail_max_personal_mail_ttl_days: int = DEFAULT_MAIL_MAX_PERSONAL_TTL_DAYS
    admin_allow_unapproved: bool = False
    producers: tuple[tuple[str, str], ...] = DEFAULT_PRODUCER_SECRET_ENVS
    producer_secrets: tuple[tuple[str, bytes], ...] = ()
    mail_secret: bytes = b""

    def producer_secrets_dict(self) -> dict[str, bytes]:
        return dict(self.producer_secrets)

    def reward_mail_window_ms(self) -> int:
        return reward_mail_window_ms(self.mail_max_personal_mail_ttl_days)

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "RankConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "rank_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[rank_node] missing or has invalid fields: {sorted(unknown)}")

        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        if not raw.get("advertise_host"):
            pod_ip = environ.get("POD_IP", "").strip()
            if pod_ip:
                raw["advertise_host"] = pod_ip

        producers_raw = raw.pop("producers", None)
        if producers_raw is None:
            producers = DEFAULT_PRODUCER_SECRET_ENVS
        elif isinstance(producers_raw, dict):
            producers = tuple(sorted((str(k), str(v)) for k, v in producers_raw.items()))
        else:
            raise ValueError("[rank_node].producers must be a mapping of caller -> env var name")

        mail_secret_env = str(raw.pop("mail_secret_env", DEFAULT_MAIL_SECRET_ENV) or "").strip()
        producer_secrets = tuple(
            (caller, load_required_secret(environ, env_name)) for caller, env_name in producers
        )
        mail_secret = load_required_secret(environ, mail_secret_env)

        config = cls(
            **raw,
            producers=producers,
            mail_secret_env=mail_secret_env,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            producer_secrets=producer_secrets,
            mail_secret=mail_secret,
        )
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"rank node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"rank grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host or self.advertise_host in ("0.0.0.0", "::"):
            raise ValueError("rank_node requires a concrete advertise_host or POD_IP")
        if self.zone_id < 1:
            raise ValueError(f"rank zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"rank release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("rank lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"rank mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 2 <= self.max_open_conn <= 128 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("rank MySQL pool bounds invalid")
        if self.request_budget_sec <= 0:
            raise ValueError("rank request_budget_sec must be > 0")
        if not self.producers or not self.producer_secrets:
            raise ValueError("rank requires at least one producer secret")
        for caller, secret in self.producer_secrets:
            if not caller or len(secret) < MIN_HMAC_SECRET_BYTES:
                raise ValueError(f"rank producer secret invalid for caller={caller!r}")
        if len(self.mail_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{self.mail_secret_env} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        reward_mail_window_ms(self.mail_max_personal_mail_ttl_days)
        if not self.data_service:
            raise ValueError("rank requires data_service gRPC endpoint")
        if not self.mail_service:
            raise ValueError("rank requires mail_service gRPC endpoint")
        if settings is not None and not settings.etcd:
            raise ValueError("rank requires etcd endpoint")
