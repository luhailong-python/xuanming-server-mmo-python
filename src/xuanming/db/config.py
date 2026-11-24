"""db（player_store）节点配置解析与启动期 fail-closed 校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import DEFAULT_KNOWN_GEN, FIRST_STORAGE_ID, SaveLimits

SCENE_SECRET_ENV = "MMORPG_PLAYER_STORE_SECRET_SCENE"
ROUTE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_DB"
MYSQL_PASSWORD_ENV = "DB_MYSQL_PASSWORD"

_TOML_FORBIDDEN_FIELDS = frozenset({"scene_secret", "route_secret", "mysql_password"})


def load_secret(environ, env_name: str) -> bytes:
    """从环境变量读取并校验 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < 32:
        raise ValueError(f"{env_name} 至少需要 32 字节")
    return raw


@dataclass(frozen=True, slots=True)
class DbConfig:
    """db 节点配置：由 ``[db_node]`` 与环境变量共同组装。"""

    node_id: int = 107
    host: str = "0.0.0.0"
    grpc_port: int = 6000
    advertise_host: str = ""
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    lease_ttl_sec: int = 60
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_password: str = ""
    storage_ids: tuple[int, ...] = (FIRST_STORAGE_ID,)
    max_open_conn: int = 8
    min_open_conn: int = 1
    sql_timeout_sec: float = 12.0
    lock_wait_timeout_sec: int = 10
    route_timeout_sec: float = 2.0
    known_gen: int = DEFAULT_KNOWN_GEN
    max_row_bytes: int = 1 << 20
    max_request_bytes: int = 3 << 20
    data_service: str = ""
    bootstrap_schema: bool = False
    scene_secret: bytes = b""
    route_secret: bytes = b""

    @property
    def limits(self) -> SaveLimits:
        return SaveLimits(max_row_bytes=self.max_row_bytes, max_request_bytes=self.max_request_bytes)

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "DbConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "db_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[db_node] 缺失或含非法字段: {sorted(unknown)}")
        if "storage_ids" in raw:
            raw["storage_ids"] = tuple(int(item) for item in raw["storage_ids"])
        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        if not raw.get("advertise_host"):
            pod_ip = environ.get("POD_IP", "").strip()
            if pod_ip:
                raw["advertise_host"] = pod_ip
        config = cls(
            **raw,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            scene_secret=load_secret(environ, SCENE_SECRET_ENV),
            route_secret=load_secret(environ, ROUTE_SECRET_ENV),
        )
        config.validate(settings)
        return config

    def validate(self, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"db node_id 无效: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"db grpc_port 无效: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("db_node 必须配置 advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"db zone_id 必须 >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"db release_track 无效: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("db lease_ttl_sec 必须 > 0")
        if not self.storage_ids or any(sid < FIRST_STORAGE_ID for sid in self.storage_ids):
            raise ValueError(f"db storage_ids 必须非空且全部 >= {FIRST_STORAGE_ID}")
        if len(set(self.storage_ids)) != len(self.storage_ids):
            raise ValueError("db storage_ids 不得包含重复项")
        if not 1 <= self.max_open_conn <= 256 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("db 连接池上下限配置无效")
        if self.lock_wait_timeout_sec < 1:
            raise ValueError("db lock_wait_timeout_sec 必须 >= 1")
        if self.sql_timeout_sec <= self.lock_wait_timeout_sec:
            raise ValueError("db sql_timeout_sec 必须严格大于 lock_wait_timeout_sec")
        if self.route_timeout_sec <= 0:
            raise ValueError("db route_timeout_sec 必须 > 0")
        if self.known_gen < 1:
            raise ValueError("db known_gen 必须 >= 1")
        self.limits.validate()
        if len(self.scene_secret) < 32:
            raise ValueError(f"{SCENE_SECRET_ENV} 至少需要 32 字节")
        if len(self.route_secret) < 32:
            raise ValueError(f"{ROUTE_SECRET_ENV} 至少需要 32 字节")
        if not self.data_service:
            raise ValueError("db 节点需要配置 data_service gRPC 地址")
        if settings is not None and not settings.etcd:
            raise ValueError("db 节点需要配置 etcd 地址")
