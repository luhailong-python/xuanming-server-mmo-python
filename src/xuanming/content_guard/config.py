"""content_guard 节点配置解析与启动期参数校验。"""

from __future__ import annotations

from dataclasses import dataclass, fields
import os

from ..settings import Settings
from .lexstore import DEFAULT_PREFIX, DEFAULT_RESYNC_SEC, DEFAULT_RETRY_BACKOFF_SEC

CONTENT_GUARD_NODE_TYPE = 32  # common.base.eNodeType.ContentGuardNodeService
DEFAULT_NODE_ID = 110
DEFAULT_GRPC_PORT = 50250
DEFAULT_LEASE_TTL_SEC = 60
DEFAULT_MAX_TEXT_BYTES = 4096
MIN_TEXT_BYTES = 64
MAX_TEXT_BYTES_LIMIT = 64 * 1024


@dataclass(frozen=True, slots=True)
class ContentGuardConfig:
    """content_guard 无状态节点配置；词库、节点发现与热关停均走 etcd。"""

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

    @property
    def effective_lexicon_prefix(self) -> str:
        """返回生效的 etcd 词库键前缀（留空时回退缺省值）。"""
        return self.lexicon_prefix or DEFAULT_PREFIX

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "ContentGuardConfig":
        """从 ``Settings.content_guard_node`` 与环境变量组装并校验配置。"""
        environ = os.environ if environ is None else environ
        raw = dict(getattr(settings, "content_guard_node", {}) or {})
        valid_fields = {item.name for item in fields(cls)}
        unknown = set(raw) - valid_fields
        if unknown:
            raise ValueError(f"[content_guard_node] has invalid fields: {sorted(unknown)}")

        raw.setdefault("zone_id", settings.zone_id)
        raw.setdefault("physical_region", settings.physical_region)
        raw.setdefault("release_track", settings.release_track)

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
        if settings is not None and not settings.etcd:
            raise ValueError("content_guard requires etcd endpoint")
