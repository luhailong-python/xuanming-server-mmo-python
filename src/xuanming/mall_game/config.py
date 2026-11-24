"""mall_game 配置解析与启动期 fail-closed 校验。

对齐 Java ``CommerceProperties`` 与 ``RmbConsignmentProperties``：
- 密钥须 >= 32 字节且各角色互不相同；
- ``prod`` 模式强制要求 TiDB（``require_tidb=True``）、禁止开启本地 mock 支付，并禁止在 ``allowed_channels`` 中允许 ``devpay`` 或 ``mock*``；
- 所有外部渠道名称受白名单正则约束，``mock`` 仅通过独立本地原件适配器接入。
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

from .models import CommerceError

MIN_SECRET_BYTES = 32
DEFAULT_PORT = 18120
DEFAULT_PAYMENT_SUBJECT = "payment-adapter"
DEFAULT_ADMIN_SUBJECT = "mall-admin"
DEFAULT_MAX_UNPAID_ORDERS = 5
DEFAULT_ORDER_TTL_SEC = 900
DEFAULT_NONCE_TTL_SEC = 600
DEFAULT_CLOCK_SKEW_SEC = 300

DEFAULT_CONSIGNMENT_NOTICE_MS = 172_800_000
DEFAULT_CONSIGNMENT_SALE_MS = 604_800_000
DEFAULT_CONSIGNMENT_COOLING_MS = 259_200_000
DEFAULT_CONSIGNMENT_FEE_BPS = 500
MAX_CONSIGNMENT_DURATION_MS = 366 * 86_400_000

_RUN_MODES = frozenset(("prod", "dev", "test"))
_SUBJECT_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_CHANNEL_RE = re.compile(r"^[a-z][a-z0-9_-]{1,31}$")


def _parse_bool(raw: object, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(f"invalid boolean value: {raw!r}")


def _parse_int(raw: object, default: int) -> int:
    if raw is None or raw == "":
        return default
    if isinstance(raw, bool):
        raise ValueError(f"invalid integer value: {raw!r}")
    return int(raw)


def _parse_channels(raw: object) -> frozenset[str]:
    if raw is None or raw == "":
        return frozenset()
    if isinstance(raw, str):
        items = [part.strip() for part in raw.split(",") if part.strip()]
        return frozenset(items)
    if isinstance(raw, (list, tuple, set, frozenset)):
        return frozenset(str(part).strip() for part in raw if str(part).strip())
    raise ValueError(f"invalid allowed_channels value: {raw!r}")


def _parse_currencies(raw: object) -> frozenset[int]:
    if raw is None or raw == "":
        return frozenset((0,))
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        return frozenset(int(p) for p in parts)
    if isinstance(raw, (list, tuple, set, frozenset)):
        return frozenset(int(p) for p in raw)
    raise ValueError(f"invalid consignment_currencies value: {raw!r}")


def _strong_secret(value: str) -> bool:
    return bool(value) and len(value.encode("utf-8")) >= MIN_SECRET_BYTES


@dataclass(frozen=True, slots=True)
class MallGameConfig:
    """mall_game 服务配置，覆盖监听、数据库、HMAC 密钥、渠道白名单与人民币寄售参数。"""

    host: str = "127.0.0.1"
    port: int = DEFAULT_PORT
    run_mode: str = "prod"
    require_tidb: bool = True

    # 数据库连接配置（独立库）
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 4000
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "mall_game"
    mysql_min_conn: int = 4
    mysql_max_conn: int = 16
    bootstrap_schema: bool = False

    # 四类调用方独立 HMAC 密钥与主体名
    gateway_secret: str = ""
    payment_secret: str = ""
    delivery_secret: str = ""
    admin_secret: str = ""
    payment_subject: str = DEFAULT_PAYMENT_SUBJECT
    admin_subject: str = DEFAULT_ADMIN_SUBJECT

    # 支付渠道策略与订单/防重放窗口
    allowed_channels: frozenset[str] = frozenset()
    mock_payment_enabled: bool = False
    max_unpaid_orders: int = DEFAULT_MAX_UNPAID_ORDERS
    order_ttl_sec: int = DEFAULT_ORDER_TTL_SEC
    nonce_ttl_sec: int = DEFAULT_NONCE_TTL_SEC
    clock_skew_sec: int = DEFAULT_CLOCK_SKEW_SEC

    # 人民币寄售冻结规则（毫秒 / 万分比）
    consignment_notice_ms: int = DEFAULT_CONSIGNMENT_NOTICE_MS
    consignment_sale_ms: int = DEFAULT_CONSIGNMENT_SALE_MS
    consignment_cooling_ms: int = DEFAULT_CONSIGNMENT_COOLING_MS
    consignment_fee_bps: int = DEFAULT_CONSIGNMENT_FEE_BPS
    consignment_currencies: frozenset[int] = frozenset((0,))

    def validate(self) -> None:
        """启动前严格校验全部安全与业务边界，不合法直接抛 ValueError。"""
        if not 0 <= self.port <= 65535:
            raise ValueError(f"invalid mall_game port: {self.port}")
        if self.run_mode not in _RUN_MODES:
            raise ValueError(f"invalid run_mode: {self.run_mode!r}")
        if self.run_mode == "prod" and not self.require_tidb:
            raise ValueError("prod mode requires TiDB authoritative storage (require_tidb=True)")

        if not _strong_secret(self.gateway_secret) or not _strong_secret(self.payment_secret):
            raise ValueError("gateway_secret and payment_secret must each be at least 32 UTF-8 bytes")
        if self.gateway_secret == self.payment_secret:
            raise ValueError("gateway_secret and payment_secret must be distinct")

        if self.delivery_secret:
            if not _strong_secret(self.delivery_secret) or self.delivery_secret in (
                self.gateway_secret,
                self.payment_secret,
            ):
                raise ValueError("delivery_secret must be at least 32 bytes and distinct from gateway/payment secrets")

        if self.admin_secret:
            if not _strong_secret(self.admin_secret) or self.admin_secret in (
                self.gateway_secret,
                self.payment_secret,
                self.delivery_secret,
            ):
                raise ValueError("admin_secret must be at least 32 bytes and distinct from all other secrets")

        if not _SUBJECT_RE.fullmatch(self.payment_subject):
            raise ValueError(f"invalid payment_subject: {self.payment_subject!r}")
        if not _SUBJECT_RE.fullmatch(self.admin_subject):
            raise ValueError(f"invalid admin_subject: {self.admin_subject!r}")

        if not 1 <= self.max_unpaid_orders <= 100:
            raise ValueError("max_unpaid_orders must be within 1..100")
        if self.order_ttl_sec < 1:
            raise ValueError("order_ttl_sec must be >= 1")
        if self.nonce_ttl_sec < 1:
            raise ValueError("nonce_ttl_sec must be >= 1")
        if self.clock_skew_sec < 1:
            raise ValueError("clock_skew_sec must be >= 1")

        if self.mock_payment_enabled and self.run_mode == "prod":
            raise ValueError("prod mode forbids mock_payment_enabled=True")

        if len(self.allowed_channels) > 32 or any(
            not _CHANNEL_RE.fullmatch(ch) or ch == "mock" for ch in self.allowed_channels
        ):
            raise ValueError("invalid allowed_channels; 'mock' must not be listed directly")
        if self.run_mode == "prod" and any(
            ch == "devpay" or ch.startswith("mock") for ch in self.allowed_channels
        ):
            raise ValueError("prod mode forbids local devpay/mock* payment channels")

        if (
            self.consignment_notice_ms < 0
            or self.consignment_notice_ms > MAX_CONSIGNMENT_DURATION_MS
            or self.consignment_sale_ms < 1
            or self.consignment_sale_ms > MAX_CONSIGNMENT_DURATION_MS
            or self.consignment_cooling_ms < 0
            or self.consignment_cooling_ms > MAX_CONSIGNMENT_DURATION_MS
            or not 0 <= self.consignment_fee_bps <= 10_000
            or any(cur not in (0, 1) for cur in self.consignment_currencies)
        ):
            raise ValueError("invalid RMB consignment rules; bound currencies cannot be traded")

    def require_mock_payment(self) -> None:
        """校验本地 mock 支付/出款是否被允许（仅 dev/test 且显式启用）。"""
        if not self.mock_payment_enabled or self.run_mode not in ("dev", "test"):
            raise CommerceError(503, "MOCK_DISABLED", "本地mock支付未启用或运行模式不允许")

    def require_payment_channel(self, channel: str) -> None:
        """校验支付渠道是否允许接入（mock 走独立原件守卫，其余走白名单）。"""
        if channel == "mock":
            self.require_mock_payment()
            return
        if channel not in self.allowed_channels:
            raise CommerceError(403, "CHANNEL_NOT_ALLOWED", "支付渠道未配置允许")

    @property
    def order_ttl_ms(self) -> int:
        return self.order_ttl_sec * 1000

    @property
    def clock_skew_ms(self) -> int:
        return self.clock_skew_sec * 1000

    @property
    def nonce_ttl_ms(self) -> int:
        return self.nonce_ttl_sec * 1000

    @classmethod
    def from_settings(
        cls,
        settings: object | None = None,
        environ: Mapping[str, str] | None = None,
        *,
        validate: bool = True,
    ) -> MallGameConfig:
        """从 ``settings.mall_game_node`` 与 ``COMMERCE_*`` / ``MMORPG_SQL_*`` 环境变量加载配置。"""
        env = os.environ if environ is None else environ
        raw = dict(getattr(settings, "mall_game_node", {}) or {}) if settings is not None else {}

        def _get(key: str, env_key: str, default: object = None) -> object:
            if env_key in env and env[env_key] != "":
                return env[env_key]
            if key in raw and raw[key] is not None:
                return raw[key]
            return default

        run_mode = str(_get("run_mode", "COMMERCE_RUN_MODE", "prod")).strip()
        require_tidb_default = run_mode == "prod"
        require_tidb = _parse_bool(_get("require_tidb", "COMMERCE_REQUIRE_TIDB", require_tidb_default), require_tidb_default)

        channels_val = _get("allowed_channels", "COMMERCE_ALLOWED_PAYMENT_CHANNELS", None)
        if channels_val is None and "allowed_payment_channels" in raw:
            channels_val = raw["allowed_payment_channels"]

        cfg = cls(
            host=str(_get("host", "COMMERCE_BIND_ADDRESS", "127.0.0.1")).strip(),
            port=_parse_int(_get("port", "COMMERCE_PORT", DEFAULT_PORT), DEFAULT_PORT),
            run_mode=run_mode,
            require_tidb=require_tidb,
            mysql_host=str(_get("mysql_host", "COMMERCE_MYSQL_HOST", "127.0.0.1")).strip(),
            mysql_port=_parse_int(_get("mysql_port", "COMMERCE_MYSQL_PORT", 4000), 4000),
            mysql_user=str(_get("mysql_user", "MMORPG_SQL_USERNAME", "root")).strip(),
            mysql_password=str(_get("mysql_password", "MMORPG_SQL_PASSWORD", "")),
            mysql_database=str(_get("mysql_database", "COMMERCE_MYSQL_DATABASE", "mall_game")).strip(),
            mysql_min_conn=_parse_int(raw.get("mysql_min_conn"), 4),
            mysql_max_conn=_parse_int(raw.get("mysql_max_conn"), 16),
            bootstrap_schema=_parse_bool(_get("bootstrap_schema", "COMMERCE_BOOTSTRAP_SCHEMA", False), False),
            gateway_secret=str(_get("gateway_secret", "COMMERCE_GATEWAY_SECRET", "")),
            payment_secret=str(_get("payment_secret", "COMMERCE_PAYMENT_SECRET", "")),
            delivery_secret=str(_get("delivery_secret", "COMMERCE_DELIVERY_SECRET", "")),
            admin_secret=str(_get("admin_secret", "COMMERCE_ADMIN_SECRET", "")),
            payment_subject=str(_get("payment_subject", "COMMERCE_PAYMENT_SUBJECT", DEFAULT_PAYMENT_SUBJECT)).strip(),
            admin_subject=str(_get("admin_subject", "COMMERCE_ADMIN_SUBJECT", DEFAULT_ADMIN_SUBJECT)).strip(),
            allowed_channels=_parse_channels(channels_val),
            mock_payment_enabled=_parse_bool(_get("mock_payment_enabled", "COMMERCE_MOCK_PAYMENT_ENABLED", False), False),
            max_unpaid_orders=_parse_int(_get("max_unpaid_orders", "COMMERCE_MAX_UNPAID_ORDERS", DEFAULT_MAX_UNPAID_ORDERS), DEFAULT_MAX_UNPAID_ORDERS),
            order_ttl_sec=_parse_int(_get("order_ttl_sec", "COMMERCE_ORDER_TTL_SEC", DEFAULT_ORDER_TTL_SEC), DEFAULT_ORDER_TTL_SEC),
            nonce_ttl_sec=_parse_int(_get("nonce_ttl_sec", "COMMERCE_NONCE_TTL_SEC", DEFAULT_NONCE_TTL_SEC), DEFAULT_NONCE_TTL_SEC),
            clock_skew_sec=_parse_int(_get("clock_skew_sec", "COMMERCE_CLOCK_SKEW_SEC", DEFAULT_CLOCK_SKEW_SEC), DEFAULT_CLOCK_SKEW_SEC),
            consignment_notice_ms=_parse_int(
                _get("consignment_notice_ms", "COMMERCE_CONSIGNMENT_NOTICE_MS", DEFAULT_CONSIGNMENT_NOTICE_MS),
                DEFAULT_CONSIGNMENT_NOTICE_MS,
            ),
            consignment_sale_ms=_parse_int(
                _get("consignment_sale_ms", "COMMERCE_CONSIGNMENT_SALE_MS", DEFAULT_CONSIGNMENT_SALE_MS),
                DEFAULT_CONSIGNMENT_SALE_MS,
            ),
            consignment_cooling_ms=_parse_int(
                _get("consignment_cooling_ms", "COMMERCE_CONSIGNMENT_COOLING_MS", DEFAULT_CONSIGNMENT_COOLING_MS),
                DEFAULT_CONSIGNMENT_COOLING_MS,
            ),
            consignment_fee_bps=_parse_int(
                _get("consignment_fee_bps", "COMMERCE_CONSIGNMENT_FEE_BPS", DEFAULT_CONSIGNMENT_FEE_BPS),
                DEFAULT_CONSIGNMENT_FEE_BPS,
            ),
            consignment_currencies=_parse_currencies(
                _get("consignment_currencies", "COMMERCE_CONSIGNMENT_CURRENCIES", None)
            ),
        )
        if validate:
            cfg.validate()
        return cfg
