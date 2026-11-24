"""match（含 team）节点配置解析与启动期 fail-closed 凭据校验。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields

from ..settings import Settings

# 环境变量名（与 Go match / team / player_inbox / battle 严格一致）
GATE_PUBLIC_KEYS_ENV = "MMORPG_INTERNAL_AUTH_PUBLIC_KEYS"
BATTLE_OUTCOME_SECRET_ENV = "MMORPG_MATCH_OUTCOME_SECRET_BATTLE"
SCENE_SECRET_ENV = "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE"
COORDINATOR_SECRET_ENV = "MMORPG_ROLE_FENCE_SECRET_COORDINATOR"
PLAYER_INBOX_SECRET_ENV = "MMORPG_PLAYER_INBOX_SECRET_MATCH"
GUILD_INTERNAL_SECRET_ENV = "MMORPG_MATCH_INTERNAL_SECRET_GUILD"
OPS_SECRET_ENV = "MMORPG_MATCH_OPS_SECRET"
MANUAL_RESOLUTION_SECRET_ENV = "MMORPG_MATCH_MANUAL_RESOLUTION_SECRET"
PROBE_SECRET_ENV = "MATCH_PROBE_SECRET"
SPECTATE_TOKEN_SECRET_ENV = "MMORPG_SPECTATE_TOKEN_SECRET"
MYSQL_PASSWORD_ENV = "MATCH_MYSQL_PASSWORD"

DEFAULT_GRPC_PORT = 50500
DEFAULT_LEASE_TTL_SEC = 60
DATABASE_NAME = "mmorpg_match"
MIN_HMAC_SECRET_BYTES = 32

_TOML_FORBIDDEN_FIELDS = frozenset((
    "battle_outcome_secret",
    "scene_secret",
    "coordinator_secret",
    "player_inbox_secret",
    "guild_secret",
    "ops_secret",
    "manual_resolution_secret",
    "probe_secret",
    "spectate_secret",
    "mysql_password",
))


def load_required_secret(environ, env_name: str) -> bytes:
    """加载必选 HMAC 密钥（去首尾空白后至少 32 字节，fail-closed）。"""
    raw = environ.get(env_name, "").strip().encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes")
    return raw


def load_optional_secret(environ, env_name: str) -> bytes:
    """加载可选 HMAC 密钥：未配置返回空字节；配置但不足 32 字节直接拒绝启动（fail-closed）。"""
    text = environ.get(env_name, "").strip()
    if not text:
        return b""
    raw = text.encode("utf-8")
    if len(raw) < MIN_HMAC_SECRET_BYTES:
        raise ValueError(f"{env_name} must be at least {MIN_HMAC_SECRET_BYTES} bytes when configured")
    return raw


@dataclass(frozen=True, slots=True)
class MatchConfig:
    """match（含 team）节点运行配置。"""

    node_id: int = 101
    team_node_id: int = 102
    host: str = "0.0.0.0"
    grpc_port: int = DEFAULT_GRPC_PORT
    advertise_host: str = ""
    zone_id: int = 1
    physical_region: int = 1
    release_track: str = "stable"
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC

    # MySQL / TiDB 权威库配置
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = DATABASE_NAME
    mysql_password: str = ""
    max_open_conn: int = 20
    min_open_conn: int = 1
    bootstrap_schema: bool = False

    # 外部集群依赖
    etcd: str = ""
    redis: str = ""
    kafka: str = ""
    kafka_generation: int = 1
    kafka_partitions: int = 256
    result_topic: str = "match-results"
    player_inbox: str = ""

    # 业务规则参数
    allow_cross_zone: bool = True
    require_network_probe: bool = False
    probe_ttl_sec: int = 300
    hard_latency_ms: int = 300
    queue_ttl_sec: int = 600
    challenge_ttl_sec: int = 60
    battle_deadline_sec: int = 900
    prepare_deadline_sec: int = 15
    spectate_ticket_ttl_sec: int = 60
    request_budget_sec: float = 4.5
    matcher_interval_sec: float = 0.25
    outbox_interval_sec: float = 0.5
    table_fingerprint_mode: str = "warn"
    pve_team_sizes: dict[int, int] = field(default_factory=lambda: {1: 5, 2: 5, 3: 5})
    manual_resolution_env: str = ""

    # 密钥材料（仅从环境变量注入，禁止落盘配置）
    battle_outcome_secret: bytes = b""
    scene_secret: bytes = b""
    coordinator_secret: bytes = b""
    player_inbox_secret: bytes = b""
    guild_secret: bytes = b""
    ops_secret: bytes = b""
    manual_resolution_secret: bytes = b""
    probe_secret: bytes = b""
    spectate_secret: bytes = b""

    @property
    def runs_global_background(self) -> bool:
        """仅 stable 轨道运行全局后台任务（撮合、发件箱等）；canary 仅处理客户端 RPC。"""
        return self.release_track == "stable"

    def validate(self) -> None:
        if self.release_track not in ("stable", "canary"):
            raise ValueError(f"invalid release_track={self.release_track!r}")
        if not 0 < self.node_id < (1 << 17) or not 0 < self.team_node_id < (1 << 17):
            raise ValueError("match/team node_id must be in (0, 131072)")
        if not 0 <= self.grpc_port <= 65535:
            raise ValueError(f"invalid grpc_port={self.grpc_port}")
        if self.lease_ttl_sec < 5:
            raise ValueError("lease_ttl_sec must be >= 5")
        if self.table_fingerprint_mode not in ("off", "warn", "enforce"):
            raise ValueError(f"invalid table_fingerprint_mode={self.table_fingerprint_mode!r}")

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "MatchConfig":
        """从 Settings 与环境变量构建配置；所有密钥强制 fail-closed 校验。"""
        env = os.environ if environ is None else environ
        raw = dict(getattr(settings, "match_node", None) or {})
        forbidden = _TOML_FORBIDDEN_FIELDS.intersection(raw)
        if forbidden:
            raise ValueError(f"secrets must not be set in config file: {sorted(forbidden)}")

        allowed = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in raw.items() if k in allowed}
        kwargs.setdefault("zone_id", settings.zone_id)
        kwargs.setdefault("physical_region", settings.physical_region)
        kwargs.setdefault("release_track", settings.release_track)
        kwargs.setdefault("etcd", settings.etcd)
        kwargs.setdefault("redis", settings.redis)
        kwargs.setdefault("kafka", settings.kafka)
        kwargs.setdefault("kafka_generation", settings.kafka_generation)
        kwargs.setdefault("kafka_partitions", settings.kafka_partitions)
        kwargs.setdefault("player_inbox", settings.player_inbox)

        if not kwargs.get("advertise_host"):
            kwargs["advertise_host"] = env.get("POD_IP", "").strip() or "127.0.0.1"

        kwargs["mysql_password"] = env.get(MYSQL_PASSWORD_ENV, "")
        kwargs["battle_outcome_secret"] = load_required_secret(env, BATTLE_OUTCOME_SECRET_ENV)
        kwargs["scene_secret"] = load_required_secret(env, SCENE_SECRET_ENV)
        kwargs["coordinator_secret"] = load_required_secret(env, COORDINATOR_SECRET_ENV)
        kwargs["player_inbox_secret"] = load_optional_secret(env, PLAYER_INBOX_SECRET_ENV)
        kwargs["guild_secret"] = load_optional_secret(env, GUILD_INTERNAL_SECRET_ENV)
        kwargs["ops_secret"] = load_optional_secret(env, OPS_SECRET_ENV)
        kwargs["manual_resolution_secret"] = load_optional_secret(env, MANUAL_RESOLUTION_SECRET_ENV)
        kwargs["probe_secret"] = load_optional_secret(env, PROBE_SECRET_ENV)
        kwargs["spectate_secret"] = load_optional_secret(env, SPECTATE_TOKEN_SECRET_ENV)

        cfg = cls(**kwargs)
        cfg.validate()
        return cfg
