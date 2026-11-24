"""content_guard 节点配置解析与启动期参数校验。"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
import os
from urllib.parse import urlsplit

from ..settings import Settings
from .lexstore import DEFAULT_PREFIX, DEFAULT_RESYNC_SEC, DEFAULT_RETRY_BACKOFF_SEC

CONTENT_GUARD_NODE_TYPE = 32  # common.base.eNodeType.ContentGuardNodeService
DEFAULT_NODE_ID = 110
DEFAULT_GRPC_PORT = 50250
DEFAULT_LEASE_TTL_SEC = 60
DEFAULT_MAX_TEXT_BYTES = 4096
MIN_TEXT_BYTES = 64
MAX_TEXT_BYTES_LIMIT = 64 * 1024

# 外部平台审核与渠道身份查询默认超时（毫秒）及合法上限（与 Go config.go 对齐）
DEFAULT_SYNC_TIMEOUT_MS = 1500
DEFAULT_WORLD_TIMEOUT_MS = 250
DEFAULT_ASYNC_TIMEOUT_MS = 2500
DEFAULT_LOOKUP_TIMEOUT_MS = 150
MAX_SYNC_TIMEOUT_MS = 5000
MAX_WORLD_TIMEOUT_MS = 2000
MAX_ASYNC_TIMEOUT_MS = 10000
MAX_LOOKUP_TIMEOUT_MS = 2000

_TOML_FORBIDDEN_FIELDS = frozenset((
    "platform_wechat_app_secret",
    "platform_douyin_app_secret",
))


@dataclass(frozen=True, slots=True)
class ContentGuardConfig:
    """content_guard 无状态节点配置；词库、节点发现与热关停均走 etcd，外部平台密钥仅从环境变量加载。"""

    node_id: int = DEFAULT_NODE_ID
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = "127.0.0.1"
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC
    lexicon_prefix: str = DEFAULT_PREFIX
    require_lexicon: bool = False
    max_text_bytes: int = DEFAULT_MAX_TEXT_BYTES
    resync_sec: float = DEFAULT_RESYNC_SEC
    retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC
    data_service: str = ""

    # 可选外部平台审核配置（密钥字段仅由环境变量注入，严禁写入 TOML 或 repr）
    platform_wechat_app_id: str = ""
    platform_wechat_app_secret_env: str = ""
    platform_wechat_base_url: str = ""
    platform_wechat_app_secret: str = field(default="", repr=False)

    platform_douyin_app_id: str = ""
    platform_douyin_app_secret_env: str = ""
    platform_douyin_base_url: str = ""
    platform_douyin_app_secret: str = field(default="", repr=False)

    platform_sync_timeout_ms: int = DEFAULT_SYNC_TIMEOUT_MS
    platform_world_timeout_ms: int = DEFAULT_WORLD_TIMEOUT_MS
    platform_async_timeout_ms: int = DEFAULT_ASYNC_TIMEOUT_MS
    platform_lookup_timeout_ms: int = DEFAULT_LOOKUP_TIMEOUT_MS

    @property
    def effective_lexicon_prefix(self) -> str:
        """返回生效的 etcd 词库键前缀（留空时回退缺省值）。"""
        return self.lexicon_prefix or DEFAULT_PREFIX

    @property
    def has_platform(self) -> bool:
        """是否启用了任一外部平台审核适配器。"""
        return bool(self.platform_wechat_app_id or self.platform_douyin_app_id)

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "ContentGuardConfig":
        """从 ``Settings.content_guard_node`` 与环境变量组装并校验配置。"""
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "content_guard_node", {}) or {})
        forbidden = set(raw) & _TOML_FORBIDDEN_FIELDS
        if forbidden:
            raise ValueError(
                "[content_guard_node] platform secrets must come from environment variables, "
                f"not config: {sorted(forbidden)}"
            )
        valid_fields = {item.name for item in fields(cls)}
        unknown = set(raw) - valid_fields
        if unknown:
            raise ValueError(f"[content_guard_node] has invalid fields: {sorted(unknown)}")

        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)
        raw.setdefault("data_service", str(getattr(settings, "data_service", "") or "").strip())

        advertise = str(raw.get("advertise_host", "") or "").strip()
        if not advertise:
            pod_ip = environ.get("POD_IP", "").strip()
            host = str(raw.get("host", "0.0.0.0") or "").strip()
            if pod_ip:
                advertise = pod_ip
            elif host and host not in ("0.0.0.0", "::"):
                advertise = host
            else:
                advertise = "127.0.0.1"
        raw["advertise_host"] = advertise

        if "lexicon_prefix" in raw and not raw["lexicon_prefix"]:
            raw["lexicon_prefix"] = DEFAULT_PREFIX

        for prefix in ("platform_wechat", "platform_douyin"):
            app_id = str(raw.get(f"{prefix}_app_id", "") or "").strip()
            secret_env = str(raw.get(f"{prefix}_app_secret_env", "") or "").strip()
            base_url = str(raw.get(f"{prefix}_base_url", "") or "").strip()
            if base_url:
                raw[f"{prefix}_base_url"] = base_url
            if not app_id and not secret_env:
                continue
            if not app_id or "${" in app_id:
                raise ValueError(
                    f"content_guard {prefix}_app_id must be set and not contain unexpanded placeholders"
                )
            if not secret_env or "${" in secret_env:
                raise ValueError(
                    f"content_guard {prefix}_app_secret_env must be an environment variable name"
                )
            secret_val = str(environ.get(secret_env, "") or "").strip()
            if not secret_val:
                raise ValueError(f"content_guard missing environment variable {secret_env} for {prefix}")
            raw[f"{prefix}_app_id"] = app_id
            raw[f"{prefix}_app_secret_env"] = secret_env
            raw[f"{prefix}_app_secret"] = secret_val

        config = cls(**raw)
        config.validate(settings=settings)
        return config

    def validate(self, *, settings: Settings | None = None) -> None:
        """启动期整体校验，不通过则阻止起服。"""
        if not 0 < self.node_id < (1 << 17):
            raise ValueError(f"content_guard node_id invalid: {self.node_id}")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"content_guard grpc_port invalid: {self.grpc_port}")
        if not self.advertise_host:
            raise ValueError("content_guard requires advertise_host")
        if self.zone_id < 1:
            raise ValueError(f"content_guard zone_id must be >= 1: {self.zone_id}")
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"content_guard release_track invalid: {self.release_track!r}")
        if self.lease_ttl_sec <= 0:
            raise ValueError("content_guard lease_ttl_sec must be > 0")
        if not MIN_TEXT_BYTES <= self.max_text_bytes <= MAX_TEXT_BYTES_LIMIT:
            raise ValueError(
                f"content_guard max_text_bytes ({self.max_text_bytes}) "
                f"must be in [{MIN_TEXT_BYTES}, {MAX_TEXT_BYTES_LIMIT}]"
            )
        if self.lexicon_prefix and not self.lexicon_prefix.endswith("/"):
            raise ValueError(
                f"content_guard lexicon_prefix ({self.lexicon_prefix!r}) must end with '/'"
            )
        if self.resync_sec <= 0 or self.retry_backoff_sec <= 0:
            raise ValueError("content_guard resync_sec and retry_backoff_sec must be > 0")
        self._validate_platform()
        if settings is not None and not settings.etcd:
            raise ValueError("content_guard requires etcd endpoint")

    def _validate_platform(self) -> None:
        for prefix, app_id, secret_env, secret_val, base_url in (
            (
                "platform_wechat",
                self.platform_wechat_app_id,
                self.platform_wechat_app_secret_env,
                self.platform_wechat_app_secret,
                self.platform_wechat_base_url,
            ),
            (
                "platform_douyin",
                self.platform_douyin_app_id,
                self.platform_douyin_app_secret_env,
                self.platform_douyin_app_secret,
                self.platform_douyin_base_url,
            ),
        ):
            if not app_id and not secret_env and not secret_val:
                continue
            if not app_id or "${" in app_id:
                raise ValueError(
                    f"content_guard {prefix}_app_id must be set and not contain unexpanded placeholders"
                )
            if not secret_env or "${" in secret_env:
                raise ValueError(
                    f"content_guard {prefix}_app_secret_env must be an environment variable name"
                )
            if not secret_val:
                raise ValueError(f"content_guard missing environment variable {secret_env} for {prefix}")
            if base_url:
                parts = urlsplit(base_url)
                if "${" in base_url or parts.scheme not in ("http", "https") or not parts.hostname:
                    raise ValueError(f"content_guard {prefix}_base_url must be a valid http/https URL")

        if not 0 <= self.platform_sync_timeout_ms <= MAX_SYNC_TIMEOUT_MS:
            raise ValueError(
                f"content_guard platform_sync_timeout_ms ({self.platform_sync_timeout_ms}) "
                f"must be within [0, {MAX_SYNC_TIMEOUT_MS}]"
            )
        if not 0 <= self.platform_world_timeout_ms <= MAX_WORLD_TIMEOUT_MS:
            raise ValueError(
                f"content_guard platform_world_timeout_ms ({self.platform_world_timeout_ms}) "
                f"must be within [0, {MAX_WORLD_TIMEOUT_MS}]"
            )
        if not 0 <= self.platform_async_timeout_ms <= MAX_ASYNC_TIMEOUT_MS:
            raise ValueError(
                f"content_guard platform_async_timeout_ms ({self.platform_async_timeout_ms}) "
                f"must be within [0, {MAX_ASYNC_TIMEOUT_MS}]"
            )
        if not 0 <= self.platform_lookup_timeout_ms <= MAX_LOOKUP_TIMEOUT_MS:
            raise ValueError(
                f"content_guard platform_lookup_timeout_ms ({self.platform_lookup_timeout_ms}) "
                f"must be within [0, {MAX_LOOKUP_TIMEOUT_MS}]"
            )
