"""chat 节点配置解析与启动期 fail-closed 校验（与 Go go/chat/internal/config 对齐）。"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import (
    ChatRules,
    DATABASE_NAME,
    MAX_ACTOR_VERIFY_TTL_SECONDS,
    MAX_HISTORY_PULL_LIMIT,
    MAX_HISTORY_TTL_SECONDS,
    MAX_WORLD_CHANNEL_COOLDOWN_MS,
    MAX_WORLD_HISTORY_CACHE_MS,
    ORIGINAL_RETENTION_DAYS,
)

ROLE_FENCE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_CHAT"
MYSQL_PASSWORD_ENV = "CHAT_MYSQL_PASSWORD"
ADMIN_TOKEN_ENV = "MMORPG_CHAT_ADMIN_TOKEN"
ADMIN_TOKEN_METADATA_KEY = "x-admin-token"

DEFAULT_GRPC_PORT = 50700
DEFAULT_LEASE_TTL_SEC = 60
MAX_RPC_TIMEOUT_SEC = 4.0  # 与 Go MaxRpcTimeoutMs=4000ms 对齐
MIN_HMAC_SECRET_BYTES = 32
MIN_ADMIN_TOKEN_BYTES = 16

_TOML_FORBIDDEN_FIELDS = frozenset(("mysql_password", "role_fence_secret", "admin_token"))


def resolve_admin_token(environ) -> str:
    """解析运维撤回口令（未配置返回空串停用运维面；非空但不足 16 字节则拒启）。"""
    token = environ.get(ADMIN_TOKEN_ENV, "")
    if token and len(token.encode("utf-8")) < MIN_ADMIN_TOKEN_BYTES:
        raise ValueError(f"{ADMIN_TOKEN_ENV} is shorter than {MIN_ADMIN_TOKEN_BYTES} bytes")
    return token


def load_required_secret(environ, env_name: str = ROLE_FENCE_SECRET_ENV) -> bytes:
    """加载必选 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


@dataclass(frozen=True, slots=True)
class ChatConfig:
    """chat 节点配置：由 ``[chat_node]`` 与环境变量组装（无 Redis 依赖）。"""

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
    request_budget_sec: float = MAX_RPC_TIMEOUT_SEC
    data_service: str = ""
    content_guard: str = ""
    content_guard_optional: bool = False
    content_guard_timeout_ms: int = 300
    kafka: str = ""
    kafka_generation: int = 1
    kafka_partitions: int = 256
    bootstrap_schema: bool = False
    role_fence_secret: bytes = b""
    admin_token: str = ""

    # 聊天业务规则参数（默认值与 Go config.go 一致）
    max_content_bytes: int = 512
    rate_limit_per_second: int = 5
    world_channel_cooldown_ms: int = 5000
    world_min_level: int = 0
    duplicate_spam_window_ms: int = 0
    duplicate_spam_max_senders: int = 0
    duplicate_spam_max_per_sender: int = 0
    new_account_age_hours: int = 0
    new_account_daily_private_limit: int = 0
    history_max_entries: int = 200
    history_ttl_seconds: int = 604800
    history_default_limit: int = 20
    history_max_limit: int = 50
    actor_verify_ttl_seconds: int = 30
    world_history_cache_ms: int = 500

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "ChatConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "chat_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[chat_node] missing or has invalid fields: {sorted(unknown)}")
        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
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
            role_fence_secret=load_required_secret(environ, ROLE_FENCE_SECRET_ENV),
            admin_token=resolve_admin_token(environ),
        )
        config.validate(settings=settings)
        return config

    def to_rules(self) -> ChatRules:
        return ChatRules(
            max_content_bytes=self.max_content_bytes,
            rate_limit_per_second=self.rate_limit_per_second,
            history_max_entries=self.history_max_entries,
            history_ttl_seconds=self.history_ttl_seconds,
            history_default_limit=self.history_default_limit,
            history_max_limit=self.history_max_limit,
            actor_verify_ttl_seconds=self.actor_verify_ttl_seconds,
            world_history_cache_ms=self.world_history_cache_ms,
            world_channel_cooldown_ms=self.world_channel_cooldown_ms,
            world_min_level=self.world_min_level,
            duplicate_spam_window_ms=self.duplicate_spam_window_ms,
            duplicate_spam_max_senders=self.duplicate_spam_max_senders,
            duplicate_spam_max_per_sender=self.duplicate_spam_max_per_sender,
            new_account_age_hours=self.new_account_age_hours,
            new_account_daily_private_limit=self.new_account_daily_private_limit,
        )

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"chat node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"chat grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("chat_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"chat zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"chat release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("chat lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"chat mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 2 <= self.max_open_conn <= 128 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("chat MySQL pool bounds invalid (max_open_conn must be in [2, 128])")
        if not 0 < self.request_budget_sec <= MAX_RPC_TIMEOUT_SEC:
            raise ValueError(f"chat request_budget_sec must be in (0, {MAX_RPC_TIMEOUT_SEC}]")
        if not self.data_service:
            raise ValueError("chat requires data_service gRPC endpoint")
        if len(self.role_fence_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ROLE_FENCE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if not self.content_guard and not self.content_guard_optional:
            raise ValueError(
                "chat requires content_guard endpoint unless content_guard_optional=True (local dev only)"
            )
        if self.content_guard and not 20 <= self.content_guard_timeout_ms <= 5000:
            raise ValueError("chat content_guard_timeout_ms must be in [20, 5000]")
        if self.kafka_generation < 1 or self.kafka_partitions < 1:
            raise ValueError("chat kafka generation/partitions must be >= 1")
        if not 0 <= self.world_min_level <= 1000:
            raise ValueError("chat world_min_level must be in [0, 1000]")
        if not 0 <= self.duplicate_spam_window_ms <= 600_000:
            raise ValueError("chat duplicate_spam_window_ms must be in [0, 600000]")
        if not 0 <= self.duplicate_spam_max_senders <= 100 or not 0 <= self.duplicate_spam_max_per_sender <= 100:
            raise ValueError("chat duplicate_spam_max_senders/max_per_sender must be in [0, 100]")
        if not 0 <= self.new_account_age_hours <= 720 or not 0 <= self.new_account_daily_private_limit <= 1000:
            raise ValueError("chat new_account_age_hours/daily_private_limit out of bounds")
        if self.history_ttl_seconds > MAX_HISTORY_TTL_SECONDS:
            raise ValueError(
                f"chat history_ttl_seconds ({self.history_ttl_seconds}) exceeds {MAX_HISTORY_TTL_SECONDS} "
                f"(retention={ORIGINAL_RETENTION_DAYS}d)"
            )
        if not 0 <= self.actor_verify_ttl_seconds <= MAX_ACTOR_VERIFY_TTL_SECONDS:
            raise ValueError(f"chat actor_verify_ttl_seconds must be in [0, {MAX_ACTOR_VERIFY_TTL_SECONDS}]")
        if not 0 <= self.world_history_cache_ms <= MAX_WORLD_HISTORY_CACHE_MS:
            raise ValueError(f"chat world_history_cache_ms must be in [0, {MAX_WORLD_HISTORY_CACHE_MS}]")
        if not 0 <= self.world_channel_cooldown_ms <= MAX_WORLD_CHANNEL_COOLDOWN_MS:
            raise ValueError(f"chat world_channel_cooldown_ms must be in [0, {MAX_WORLD_CHANNEL_COOLDOWN_MS}]")
        if self.history_max_limit > MAX_HISTORY_PULL_LIMIT:
            raise ValueError(f"chat history_max_limit must be <= {MAX_HISTORY_PULL_LIMIT}")
        self.to_rules().validate()
        if settings is not None and not settings.etcd:
            raise ValueError("chat requires etcd endpoint")
