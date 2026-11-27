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
OPS_CALLER_SECRET_ENV = "MMORPG_MATCH_OPS_CALLER_SECRET"
OPS_SECRET_ENV = "MMORPG_MATCH_OPS_SECRET"
RANK_PRODUCER_SECRET_ENV = "RANK_PRODUCER_SECRET_MATCH"
MANUAL_RESOLUTION_SECRET_ENV = "MMORPG_MATCH_MANUAL_RESOLUTION_SECRET"
PROBE_SECRET_ENV = "MATCH_PROBE_SECRET"
SPECTATE_TOKEN_SECRET_ENV = "MMORPG_SPECTATE_TOKEN_SECRET"
MYSQL_PASSWORD_ENV = "MATCH_MYSQL_PASSWORD"

DEFAULT_GRPC_PORT = 50500
DEFAULT_LEASE_TTL_SEC = 60
DATABASE_NAME = "mmorpg_match"
MIN_HMAC_SECRET_BYTES = 32

DEFAULT_SPECTATE_CHAT_MAX_CONTENT_BYTES = 180
DEFAULT_SPECTATE_CHAT_PARTICIPANT_COOLDOWN_MS = 2000
DEFAULT_SPECTATE_CHAT_SPECTATOR_COOLDOWN_MS = 4000
DEFAULT_SPECTATE_CHAT_ROOM_RATE_LIMIT_PER_SEC = 8


@dataclass(frozen=True, slots=True)
class SpectateChatConfig:
    """局外人观战交流模式全局策略与限流配置（与 Go SpectateChatConf 零值默认严格对齐）。"""

    disabled: bool = False
    default_forbid_when_unset: bool = False
    team_pvp_captain_only: bool = False
    tournament_disabled: bool = False
    max_content_bytes: int = DEFAULT_SPECTATE_CHAT_MAX_CONTENT_BYTES
    participant_cooldown_ms: int = DEFAULT_SPECTATE_CHAT_PARTICIPANT_COOLDOWN_MS
    spectator_cooldown_ms: int = DEFAULT_SPECTATE_CHAT_SPECTATOR_COOLDOWN_MS
    room_rate_limit_per_sec: int = DEFAULT_SPECTATE_CHAT_ROOM_RATE_LIMIT_PER_SEC

    @property
    def is_enabled(self) -> bool:
        return not self.disabled

    @property
    def default_allows_when_unset(self) -> bool:
        return not self.default_forbid_when_unset

    @property
    def is_tournament_enabled(self) -> bool:
        return not self.tournament_disabled

    @property
    def effective_max_content_bytes(self) -> int:
        return self.max_content_bytes if self.max_content_bytes > 0 else DEFAULT_SPECTATE_CHAT_MAX_CONTENT_BYTES

    @property
    def effective_participant_cooldown_ms(self) -> int:
        return (
            self.participant_cooldown_ms
            if self.participant_cooldown_ms > 0
            else DEFAULT_SPECTATE_CHAT_PARTICIPANT_COOLDOWN_MS
        )

    @property
    def effective_spectator_cooldown_ms(self) -> int:
        return (
            self.spectator_cooldown_ms
            if self.spectator_cooldown_ms > 0
            else DEFAULT_SPECTATE_CHAT_SPECTATOR_COOLDOWN_MS
        )

    @property
    def effective_room_rate_limit_per_sec(self) -> int:
        return (
            self.room_rate_limit_per_sec
            if self.room_rate_limit_per_sec > 0
            else DEFAULT_SPECTATE_CHAT_ROOM_RATE_LIMIT_PER_SEC
        )

_TOML_FORBIDDEN_FIELDS = frozenset((
    "battle_outcome_secret",
    "scene_secret",
    "coordinator_secret",
    "player_inbox_secret",
    "guild_secret",
    "ops_secret",
    "rank_secret",
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
    rank_endpoint: str = ""

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
    rank_publish_interval_sec: float = 2.0
    table_fingerprint_mode: str = "warn"
    pve_team_sizes: dict[int, int] = field(default_factory=lambda: {1: 5, 2: 5, 3: 5})
    manual_resolution_env: str = ""
    spectate_chat: SpectateChatConfig = field(default_factory=SpectateChatConfig)

    # 密钥材料（仅从环境变量注入，禁止落盘配置）
    battle_outcome_secret: bytes = b""
    scene_secret: bytes = b""
    coordinator_secret: bytes = b""
    player_inbox_secret: bytes = b""
    guild_secret: bytes = b""
    ops_secret: bytes = b""
    rank_secret: bytes = b""
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
        raw_sc = kwargs.get("spectate_chat")
        if isinstance(raw_sc, dict):
            sc_allowed = {f.name for f in fields(SpectateChatConfig)}
            kwargs["spectate_chat"] = SpectateChatConfig(**{k: v for k, v in raw_sc.items() if k in sc_allowed})
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
        ops_secret = load_optional_secret(env, OPS_CALLER_SECRET_ENV)
        if not ops_secret:
            ops_secret = load_optional_secret(env, OPS_SECRET_ENV)
        kwargs["ops_secret"] = ops_secret
        kwargs["rank_secret"] = load_optional_secret(env, RANK_PRODUCER_SECRET_ENV)
        kwargs["manual_resolution_secret"] = load_optional_secret(env, MANUAL_RESOLUTION_SECRET_ENV)
        kwargs["probe_secret"] = load_optional_secret(env, PROBE_SECRET_ENV)
        kwargs["spectate_secret"] = load_optional_secret(env, SPECTATE_TOKEN_SECRET_ENV)

        cfg = cls(**kwargs)
        cfg.validate()
        return cfg
