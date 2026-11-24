"""payment 节点配置解析与启动期 fail-closed 校验。

所有密钥均从环境变量读取，严禁明文写入 TOML 或日志；校验口径与 Go ``go/payment/internal/config/config.go`` 对齐。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from urllib.parse import urlsplit

from ..settings import Settings
from .model import (
    ALLOWED_PLATFORM_CHANNELS,
    CHANNEL_ALIPAY,
    CHANNEL_APPLE,
    CHANNEL_DEVPAY,
    CHANNEL_DOUYIN_MINIGAME,
    CHANNEL_GOOGLE,
    CHANNEL_ID_BY_NAME,
    CHANNEL_WECHAT_MINIGAME,
    CHANNEL_WECHAT_PAY,
    DATABASE_NAME,
    PLATFORM_ID_BY_NAME,
    ProductMapping,
    TestPolicy,
)

# 默认端口与租约参数
DEFAULT_GRPC_PORT = 51200
DEFAULT_NOTIFY_HOST = "127.0.0.1"
DEFAULT_NOTIFY_PORT = 18130
DEFAULT_LEASE_TTL_SEC = 60
DEFAULT_MALL_SUBJECT = "payment-adapter"
DEFAULT_MALL_TIMEOUT_MS = 2500
MIN_SECRET_BYTES = 32

# 合规闸取值
MINOR_SPENDING_GATE_NONE = "none"
MINOR_SPENDING_GATE_REALNAME = "realname"
MINOR_SPENDING_GATE_OVERSEAS = "overseas"

# 环境变量名常量（与 Go etc/payment.yaml 对齐）
MYSQL_PASSWORD_ENV = "PAYMENT_MYSQL_PASSWORD"
MALL_PAYMENT_SECRET_ENV = "COMMERCE_PAYMENT_SECRET"
DEVPAY_SECRET_ENV = "PAYMENT_DEVPAY_SECRET"

_TOML_FORBIDDEN_FIELDS = frozenset((
    "mysql_password",
    "mall_payment_secret",
    "devpay_secret",
))

_PRODUCTION_APP_CHANNELS = (
    CHANNEL_WECHAT_PAY,
    CHANNEL_ALIPAY,
    CHANNEL_APPLE,
    CHANNEL_GOOGLE,
)


def is_relaxed_mode(mode: str) -> bool:
    """dev/test 模式允许 devpay、http 回调根地址与沙箱配置。"""
    return mode in ("dev", "test")


def load_env_secret(environ, field_name: str, env_name: str, min_bytes: int = MIN_SECRET_BYTES) -> bytes:
    """严格读取单行密钥：非空、无首尾空白、长度不少于 min_bytes，报错只点变量名不回显值。"""
    if not env_name:
        raise ValueError(f"payment config: {field_name} names no environment variable")
    raw = environ.get(env_name, "")
    if not raw:
        raise ValueError(f"payment config: {field_name}={env_name}, but that environment variable is unset or empty")
    if raw != raw.strip():
        raise ValueError(f"payment config: the value of {env_name} has leading or trailing whitespace")
    encoded = raw.encode("utf-8")
    if len(encoded) < min_bytes:
        raise ValueError(f"payment config: the value of {env_name} is shorter than {min_bytes} bytes")
    return encoded


def validate_base_url(field_name: str, raw: str, *, allow_http: bool, allow_path: bool = True) -> None:
    """校验 HTTP/HTTPS 根地址：必须含主机、不含查询串/片段/用户信息/占位符。"""
    if not raw or "${" in raw:
        raise ValueError(f"payment config: {field_name} must be an absolute URL without placeholders")
    parts = urlsplit(raw)
    if (
        not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise ValueError(f"payment config: {field_name} must be an absolute URL without query, fragment or user info")
    if not allow_path and parts.path not in ("", "/"):
        raise ValueError(f"payment config: {field_name} must not contain a path prefix")
    if parts.scheme == "https":
        return
    if parts.scheme == "http":
        if not allow_http:
            raise ValueError(f"payment config: {field_name} must use https outside mode=dev/test")
        return
    raise ValueError(f"payment config: {field_name} must use http or https")


@dataclass(frozen=True, slots=True)
class PaymentConfig:
    """payment 节点配置：由 ``[payment_node]`` 与环境变量组装。"""

    node_id: int = 119
    host: str = "127.0.0.1"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = ""
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    mode: str = "dev"
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC

    # 回调 HTTP 服务监听地址与公网根地址
    notify_host: str = DEFAULT_NOTIFY_HOST
    notify_port: int = DEFAULT_NOTIFY_PORT
    notify_base_url: str = ""

    # 独占库 mmorpg_payment 连接配置
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = DATABASE_NAME
    mysql_password: str = ""
    max_open_conn: int = 16
    min_open_conn: int = 1
    bootstrap_schema: bool = False

    # 商城内部 HTTP 接入
    mall_base_url: str = "http://127.0.0.1:18120"
    mall_payment_subject: str = DEFAULT_MALL_SUBJECT
    mall_payment_secret_env: str = MALL_PAYMENT_SECRET_ENV
    mall_payment_secret: bytes = b""
    mall_timeout_ms: int = DEFAULT_MALL_TIMEOUT_MS

    # 合规闸（none / realname / overseas）
    minor_spending_gate: str = MINOR_SPENDING_GATE_NONE
    data_service: str = ""

    # 渠道开关与平台渠道清单
    devpay_enabled: bool = True
    devpay_secret_env: str = DEVPAY_SECRET_ENV
    devpay_secret: bytes = b""
    wechat_pay_enabled: bool = False
    alipay_enabled: bool = False
    wechat_minigame_enabled: bool = False
    douyin_minigame_enabled: bool = False
    apple_enabled: bool = False
    google_enabled: bool = False

    platforms: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("android_app", (CHANNEL_DEVPAY,)),
        ("ios_app", (CHANNEL_DEVPAY,)),
        ("wechat_minigame", (CHANNEL_DEVPAY,)),
        ("douyin_minigame", (CHANNEL_DEVPAY,)),
        ("desktop", (CHANNEL_DEVPAY,)),
    )
    products: tuple[ProductMapping, ...] = ()
    test_policies: tuple[tuple[str, TestPolicy], ...] = ()
    request_budget_sec: float = 3.5

    @property
    def runs_background_work(self) -> bool:
        """canary 副本仅承接客户端 RPC 与渠道回调，停用后台轮询任务。"""
        return self.release_track == "stable"

    def channel_enabled(self, name: str) -> bool:
        mapping = {
            CHANNEL_DEVPAY: self.devpay_enabled,
            CHANNEL_WECHAT_PAY: self.wechat_pay_enabled,
            CHANNEL_ALIPAY: self.alipay_enabled,
            CHANNEL_WECHAT_MINIGAME: self.wechat_minigame_enabled,
            CHANNEL_DOUYIN_MINIGAME: self.douyin_minigame_enabled,
            CHANNEL_APPLE: self.apple_enabled,
            CHANNEL_GOOGLE: self.google_enabled,
        }
        return mapping.get(name, False)

    def platform_channel_ids(self) -> dict[int, tuple[int, ...]]:
        """将配置的平台渠道名清单转为 proto 枚举映射。"""
        return {
            PLATFORM_ID_BY_NAME[pname]: tuple(CHANNEL_ID_BY_NAME[cname] for cname in cnames)
            for pname, cnames in self.platforms
        }

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "PaymentConfig":
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "payment_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = (set(raw) - valid_fields) | (set(raw) & _TOML_FORBIDDEN_FIELDS)
        if unknown or not raw:
            raise ValueError(f"[payment_node] missing or has invalid fields: {sorted(unknown)}")

        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", settings.data_service)
        if not raw.get("advertise_host"):
            pod_ip = environ.get("POD_IP", "").strip()
            if pod_ip:
                raw["advertise_host"] = pod_ip

        if "platforms" in raw and isinstance(raw["platforms"], dict):
            raw["platforms"] = tuple((k, tuple(v)) for k, v in raw["platforms"].items())
        elif "platforms" in raw and isinstance(raw["platforms"], list):
            parsed_platforms = []
            for entry in raw["platforms"]:
                if isinstance(entry, dict):
                    parsed_platforms.append((str(entry["platform"]), tuple(entry.get("channels", ()))))
                else:
                    parsed_platforms.append((str(entry[0]), tuple(entry[1])))
            raw["platforms"] = tuple(parsed_platforms)

        if "products" in raw and isinstance(raw["products"], list):
            parsed_products = []
            for entry in raw["products"]:
                if isinstance(entry, ProductMapping):
                    parsed_products.append(entry)
                else:
                    by_ch = []
                    for ch_name in (CHANNEL_APPLE, CHANNEL_GOOGLE, CHANNEL_WECHAT_MINIGAME):
                        if entry.get(ch_name):
                            by_ch.append((ch_name, str(entry[ch_name])))
                    parsed_products.append(
                        ProductMapping(
                            sku_id=int(entry.get("sku_id", 0)),
                            title=str(entry.get("title", "")),
                            by_channel=tuple(by_ch),
                        )
                    )
            raw["products"] = tuple(parsed_products)

        mall_secret_env = str(raw.get("mall_payment_secret_env", MALL_PAYMENT_SECRET_ENV))
        devpay_enabled = bool(raw.get("devpay_enabled", True))
        devpay_secret_env = str(raw.get("devpay_secret_env", DEVPAY_SECRET_ENV))

        mall_secret = load_env_secret(environ, "mall_payment_secret_env", mall_secret_env, MIN_SECRET_BYTES)
        devpay_secret = (
            load_env_secret(environ, "devpay_secret_env", devpay_secret_env, MIN_SECRET_BYTES)
            if devpay_enabled
            else b""
        )

        config = cls(
            **raw,
            mysql_password=environ.get(MYSQL_PASSWORD_ENV, ""),
            mall_payment_secret=mall_secret,
            devpay_secret=devpay_secret,
        )
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"payment node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535 or not 0 <= self.notify_port <= 65535:
            raise ValueError("payment grpc_port or notify_port invalid")
        if not self.advertise_host:
            raise ValueError("payment_node requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"payment zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"payment release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("payment lease_ttl_sec must be > 0")
        if self.mysql_database != DATABASE_NAME:
            raise ValueError(f"payment mysql_database must be {DATABASE_NAME!r}, got {self.mysql_database!r}")
        if not 1 <= self.max_open_conn <= 64 or not 0 <= self.min_open_conn <= self.max_open_conn:
            raise ValueError("payment MySQL pool bounds invalid")
        if self.request_budget_sec <= 0:
            raise ValueError("payment request_budget_sec must be > 0")

        relaxed = is_relaxed_mode(self.mode)
        validate_base_url("mall_base_url", self.mall_base_url, allow_http=True, allow_path=False)
        if not self.mall_payment_subject:
            raise ValueError("payment mall_payment_subject is required")
        if len(self.mall_payment_secret) < MIN_SECRET_BYTES:
            raise ValueError(f"payment mall_payment_secret must be at least {MIN_SECRET_BYTES} bytes")
        if self.mall_timeout_ms < 0 or (self.mall_timeout_ms > 0 and not 500 <= self.mall_timeout_ms <= 15000):
            raise ValueError("payment mall_timeout_ms must be 0 or within [500, 15000]")

        if self.wechat_pay_enabled or self.alipay_enabled:
            validate_base_url("notify_base_url", self.notify_base_url, allow_http=relaxed, allow_path=True)
        if self.devpay_enabled:
            if not relaxed:
                raise ValueError("payment devpay is only allowed in mode=dev or mode=test")
            if len(self.devpay_secret) < MIN_SECRET_BYTES:
                raise ValueError(f"payment devpay_secret must be at least {MIN_SECRET_BYTES} bytes")

        if self.minor_spending_gate == MINOR_SPENDING_GATE_REALNAME:
            if not self.data_service:
                raise ValueError("payment minor_spending_gate=realname requires data_service endpoint")
        elif self.minor_spending_gate == MINOR_SPENDING_GATE_OVERSEAS:
            pass
        elif self.minor_spending_gate == MINOR_SPENDING_GATE_NONE:
            if not relaxed:
                for ch_name in _PRODUCTION_APP_CHANNELS:
                    if self.channel_enabled(ch_name):
                        raise ValueError(
                            f"payment {ch_name} cannot be enabled in production with minor_spending_gate=none"
                        )
        else:
            raise ValueError(f"payment minor_spending_gate invalid: {self.minor_spending_gate!r}")

        seen_platforms: set[str] = set()
        for pname, ch_list in self.platforms:
            allowed = ALLOWED_PLATFORM_CHANNELS.get(pname)
            if allowed is None:
                raise ValueError(f"payment unknown platform {pname!r}")
            if pname in seen_platforms:
                raise ValueError(f"payment platform {pname!r} listed twice")
            seen_platforms.add(pname)
            for cname in ch_list:
                if cname not in allowed:
                    raise ValueError(f"payment channel {cname!r} cannot be offered on platform {pname!r}")
                if not self.channel_enabled(cname):
                    raise ValueError(f"payment platform {pname!r} lists disabled channel {cname!r}")

        seen_skus: set[int] = set()
        for prod in self.products:
            if prod.sku_id <= 0 or prod.sku_id in seen_skus:
                raise ValueError(f"payment product has zero or repeated sku_id={prod.sku_id}")
            seen_skus.add(prod.sku_id)

        if settings is not None and not settings.etcd:
            raise ValueError("payment requires etcd endpoint")
