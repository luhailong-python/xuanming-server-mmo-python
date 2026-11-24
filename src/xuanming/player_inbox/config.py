"""player_inbox 节点配置解析与启动期 fail-closed 校验。"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, fields

from ..protocol import Protocol
from ..settings import Settings
from .model import DATABASE_NAME
from .registry import InboxRegistry

logger = logging.getLogger(__name__)

# 5 组凭据及数据库口令环境变量名（与 Go 侧完全对齐）
SCENE_SECRET_ENV = "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE"
MATCH_SECRET_ENV = "MMORPG_PLAYER_INBOX_SECRET_MATCH"
COORDINATOR_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
ROLE_FENCE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_PLAYER_INBOX"
DEFAULT_ADMIN_TOKEN_ENV = "PLAYER_INBOX_ADMIN_TOKEN"
MYSQL_PASSWORD_ENV = "PLAYER_INBOX_MYSQL_PASSWORD"

DEFAULT_GRPC_PORT = 50950
DEFAULT_LEASE_TTL_SEC = 60
MIN_HMAC_SECRET_BYTES = 32
MIN_ADMIN_TOKEN_BYTES = 16

_ADMIN_ENV_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
_TOML_FORBIDDEN_FIELDS = frozenset((
    "scene_secret",
    "match_secret",
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


def load_optional_match_secret(environ, env_name: str = MATCH_SECRET_ENV) -> bytes:
    """加载可选 Match 结算生产密钥：缺失或不足 32 字节返回空字节串（仅停用 Battle 生产面）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        return b""
    return raw


def resolve_admin_token(literal: str, env_name: str, environ) -> str:
    """解析运维口令：空串表示停用运维面；非空时至少 16 字节，禁止同时指定字面值与环境变量名。"""
    if literal and env_name:
        raise ValueError("admin_token and admin_token_env are mutually exclusive")
    if literal:
        if len(literal.encode("utf-8")) < MIN_ADMIN_TOKEN_BYTES:
            raise ValueError(f"admin_token must be at least {MIN_ADMIN_TOKEN_BYTES} bytes when non-empty")
        return literal
    if env_name:
        if not _ADMIN_ENV_PATTERN.fullmatch(env_name):
            raise ValueError(f"admin_token_env must be an uppercase env var name (len={len(env_name)})")
        token = environ.get(env_name, "").strip()
    else:
        token = environ.get(DEFAULT_ADMIN_TOKEN_ENV, "").strip()
    if not token:
        return ""
    if len(token.encode("utf-8")) < MIN_ADMIN_TOKEN_BYTES:
        raise ValueError(
            f"admin token from env must be at least {MIN_ADMIN_TOKEN_BYTES} bytes (got {len(token.encode('utf-8'))})"
        )
    return token


@dataclass(frozen=True, slots=True)
class PlayerInboxConfig:
    """player_inbox 节点配置：由 ``[player_inbox_node]`` 与环境变量组装。"""

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
    max_open_conn: int = 20
    min_open_conn: int = 1
    sql_timeout_sec: float = 10.0
    request_budget_sec: float = 4.5
    data_service: str = ""
    redis: str = ""
    kafka: str = ""
    kafka_generation: int = 1
    kafka_partitions: int = 256
    bootstrap_schema: bool = False
    enqueue_types: tuple[str, ...] = ()
    admin_token_env: str = ""
    admin_token: str = ""
    scene_secret: bytes = b""
    match_secret: bytes = b""
    coordinator_secret: bytes = b""
    role_fence_secret: bytes = b""

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        protocol: Protocol | None = None,
        environ=None,
    ) -> "PlayerInboxConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "player_inbox_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[player_inbox_node] missing or has invalid fields: {sorted(unknown)}")
        if "enqueue_types" in raw:
            raw["enqueue_types"] = tuple(str(item) for item in raw["enqueue_types"])
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

        literal_admin = str(raw.pop("admin_token", "") or "")
        admin_env = str(raw.pop("admin_token_env", "") or "")
        resolved_admin = resolve_admin_token(literal_admin, admin_env, environ)

        config = cls(
            **raw,
            admin_token_env=admin_env,
            admin_token=resolved_admin,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            scene_secret=load_required_secret(environ, SCENE_SECRET_ENV),
            match_secret=load_optional_match_secret(environ, MATCH_SECRET_ENV),
            coordinator_secret=load_required_secret(environ, COORDINATOR_SECRET_ENV),
            role_fence_secret=load_required_secret(environ, ROLE_FENCE_SECRET_ENV),
        )
        config.validate(settings=settings, protocol=protocol)
        return config

    def validate(self, *, settings: Settings | None = None, protocol: Protocol | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"player_inbox node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"player_inbox grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("player_inbox_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"player_inbox zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"player_inbox release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("player_inbox lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"player_inbox mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 1 <= self.max_open_conn <= 256 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("player_inbox MySQL pool bounds invalid")
        if self.sql_timeout_sec <= 0 or self.request_budget_sec <= 0:
            raise ValueError("player_inbox timeout/budget must be > 0")
        if self.kafka_generation < 1 or self.kafka_partitions < 1:
            raise ValueError("player_inbox kafka generation/partitions must be >= 1")
        if self.admin_token and len(self.admin_token.encode("utf-8")) < MIN_ADMIN_TOKEN_BYTES:
            raise ValueError(f"player_inbox admin_token must be empty or >= {MIN_ADMIN_TOKEN_BYTES} bytes")
        if len(self.scene_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{SCENE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if self.match_secret and len(self.match_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{MATCH_SECRET_ENV} must be empty or at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.coordinator_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{COORDINATOR_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.role_fence_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ROLE_FENCE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if not self.data_service:
            raise ValueError("player_inbox requires data_service gRPC endpoint")
        if settings is not None and not settings.etcd:
            raise ValueError("player_inbox requires etcd endpoint")

        registry = InboxRegistry(protocol or Protocol())
        allowed = registry.parse_enqueue_types(self.enqueue_types)
        if not self.match_secret and 4 in allowed:
            logger.error(
                "player_inbox battle settlement producer disabled: %s missing or shorter than %d bytes",
                MATCH_SECRET_ENV,
                MIN_HMAC_SECRET_BYTES,
            )
