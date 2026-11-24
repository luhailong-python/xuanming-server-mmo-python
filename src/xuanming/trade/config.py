"""trade 节点配置解析与启动期 fail-closed 校验（与 Go trade/internal/config 对齐）。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields

from ..settings import Settings
from .model import (
    CURRENCY_GOLD,
    DATABASE_NAME,
    MarketLimits,
    TradePolicy,
    is_tradable_currency,
    valid_manual_environment,
)

# 密钥与数据库口令环境变量名（与 Go trade/etc/trade.yaml 完全对齐）
COORDINATOR_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
ROLE_FENCE_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_TRADE"
ASSET_OP_SECRET_ENV = "MMORPG_ASSET_OP_SECRET_TRADE"
MANUAL_RESOLUTION_SECRET_ENV = "MMORPG_TRADE_MANUAL_RESOLUTION_SECRET"
MYSQL_PASSWORD_ENV = "TRADE_MYSQL_PASSWORD"

DEFAULT_GRPC_PORT = 50800
DEFAULT_LEASE_TTL_SEC = 60
MIN_HMAC_SECRET_BYTES = 32

_TOML_FORBIDDEN_FIELDS = frozenset((
    "asset_op_secret",
    "coordinator_secret",
    "role_fence_secret",
    "manual_resolution_secret",
    "mysql_password",
))


def load_required_secret(environ, env_name: str) -> bytes:
    """加载必选 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


def load_optional_secret(environ, env_name: str) -> bytes:
    """加载可选 HMAC 密钥；未配置返回空串，不足 32 字节返回空串（入口保持关闭）。"""
    if not env_name:
        return b""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        return b""
    return raw


@dataclass(frozen=True, slots=True)
class TradeConfig:
    """trade 节点配置：由 ``[trade_node]`` 与环境变量组装。"""

    node_id: int = 109
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = ""
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    mode: str = "pro"  # dev / test / pro（SeedListing 仅在 dev|test 可用）
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = DATABASE_NAME
    mysql_password: str = ""
    max_open_conn: int = 20
    min_open_conn: int = 1
    sql_timeout_sec: float = 10.0
    request_budget_sec: float = 3.5
    data_service: str = ""
    redis: str = ""
    bootstrap_schema: bool = False
    market: MarketLimits = field(default_factory=MarketLimits)
    asset_op_enabled: bool = True
    asset_loop_interval_sec: float = 2.0
    asset_loop_batch: int = 100
    asset_loop_workers: int = 8
    commerce_tick_interval_sec: float = 1.0
    merged_zones_refresh_sec: float = 60.0
    commerce_items: tuple[int, ...] = ()
    commerce_item_titles: dict[int, str] = field(default_factory=dict)
    commerce_pets: tuple[int, ...] = ()
    commerce_currencies: tuple[int, ...] = (CURRENCY_GOLD,)
    commerce_allow_instances: bool = False
    mall_url: str = ""
    rmb_enabled: bool = False
    manual_resolution_secret_env: str = ""
    manual_resolution_environment: str = ""
    asset_op_secret: bytes = b""
    coordinator_secret: bytes = b""
    role_fence_secret: bytes = b""
    manual_resolution_secret: bytes = b""

    @property
    def runs_background_work(self) -> bool:
        """canary 副本仅承接 RPC，停用资产重投、合同推进等后台循环。"""
        return self.release_track == "stable"

    @property
    def is_relaxed_mode(self) -> bool:
        """仅 dev/test 模式开放 SeedListing。"""
        return self.mode in ("dev", "test")

    @property
    def policy(self) -> TradePolicy:
        return TradePolicy(
            items=tuple(self.commerce_items),
            item_titles=dict(self.commerce_item_titles),
            pets=tuple(self.commerce_pets),
            currencies=tuple(self.commerce_currencies),
            allow_instances=bool(self.commerce_allow_instances),
        )

    @property
    def manual_resolution_enabled(self) -> bool:
        return (
            self.asset_op_enabled
            and len(self.manual_resolution_secret) >= MIN_HMAC_SECRET_BYTES
            and valid_manual_environment(self.manual_resolution_environment)
        )

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "TradeConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "trade_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[trade_node] missing or has invalid fields: {sorted(unknown)}")
        if "market" in raw and isinstance(raw["market"], dict):
            raw["market"] = MarketLimits(**raw["market"])
        if "commerce_items" in raw:
            raw["commerce_items"] = tuple(int(x) for x in raw["commerce_items"])
        if "commerce_pets" in raw:
            raw["commerce_pets"] = tuple(int(x) for x in raw["commerce_pets"])
        if "commerce_currencies" in raw:
            raw["commerce_currencies"] = tuple(int(x) for x in raw["commerce_currencies"])
        if "commerce_item_titles" in raw and isinstance(raw["commerce_item_titles"], dict):
            raw["commerce_item_titles"] = {int(k): str(v) for k, v in raw["commerce_item_titles"].items()}
        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        raw.setdefault("redis", settings.redis)
        if not raw.get("advertise_host"):
            pod_ip = environ.get("POD_IP", "").strip()
            if pod_ip:
                raw["advertise_host"] = pod_ip

        asset_enabled = bool(raw.get("asset_op_enabled", True))
        asset_secret = load_required_secret(environ, ASSET_OP_SECRET_ENV) if asset_enabled else b""
        manual_env = str(raw.get("manual_resolution_secret_env", "")).strip()
        manual_secret = load_optional_secret(environ, manual_env)

        config = cls(
            **raw,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            asset_op_secret=asset_secret,
            coordinator_secret=load_required_secret(environ, COORDINATOR_SECRET_ENV),
            role_fence_secret=load_required_secret(environ, ROLE_FENCE_SECRET_ENV),
            manual_resolution_secret=manual_secret,
        )
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"trade node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"trade grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("trade_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"trade zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"trade release_track invalid: {self.release_track!r}")
        if self.mode not in ("dev", "test", "pro"):
            raise ValueError(f"trade mode invalid: {self.mode!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("trade lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"trade mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 1 <= self.max_open_conn <= 256 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("trade MySQL pool bounds invalid")
        if self.sql_timeout_sec <= 0 or self.request_budget_sec <= 0:
            raise ValueError("trade timeout/budget must be > 0")
        self.market.validate()
        if self.asset_op_enabled:
            if self.market.scope != "global":
                raise ValueError("trade commerce write surface requires market.scope='global'")
            if len(self.asset_op_secret) < MIN_HMAC_SECRET_BYTES:
                raise ValueError(f"{ASSET_OP_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
            for cfg_id in self.commerce_items:
                if cfg_id == 0 or not self.commerce_item_titles.get(cfg_id, "").strip():
                    raise ValueError(f"tradable item {cfg_id} requires non-empty title")
            for cur in self.commerce_currencies:
                if not is_tradable_currency(cur):
                    raise ValueError(f"non-tradable currency in commerce_currencies: {cur}")
        if self.rmb_enabled and (not self.asset_op_enabled or not self.mall_url):
            raise ValueError("rmb consignment requires asset_op_enabled and mall_url")
        if self.asset_loop_interval_sec <= 0 or not 1 <= self.asset_loop_workers <= 64:
            raise ValueError("trade asset_loop interval/workers invalid")
        if self.asset_loop_batch < self.asset_loop_workers:
            raise ValueError("trade asset_loop_batch must be >= asset_loop_workers")
        if len(self.coordinator_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{COORDINATOR_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if len(self.role_fence_secret) < MIN_HMAC_SECRET_BYTES:
            raise ValueError(f"{ROLE_FENCE_SECRET_ENV} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
        if not self.data_service:
            raise ValueError("trade requires data_service gRPC endpoint")
        if settings is not None and not settings.etcd:
            raise ValueError("trade requires etcd endpoint")
