"""gm_admin 节点配置解析与启动期安全校验。"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields
from urllib.parse import urlsplit

from ..settings import Settings

DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8082
DEFAULT_DATABASE_NAME = "gm_admin"
DEFAULT_REQUEST_BUDGET_SEC = 5.0
DEFAULT_DISCOVERY_TTL_SEC = 5.0
DEFAULT_MAX_CAMPAIGN_TARGETS = 10_000
MAX_SCENE_CHANNELS = 1024
MIN_HMAC_SECRET_BYTES = 32

_UINT32_MAX = 0xFFFF_FFFF
_POSITIVE_NODE_ID_RE = re.compile(r"^[1-9][0-9]{0,9}$")

# 密钥与数据库口令严禁写入 TOML 配置文件，只允许通过环境变量注入。
_TOML_FORBIDDEN_FIELDS = frozenset((
    "admin_token",
    "gateway_admin_key",
    "mail_admin_secret",
    "scene_admin_secret",
    "caller_secret",
    "mysql_password",
))

_LOOPBACK_HOSTS = frozenset(("127.0.0.1", "localhost", "::1"))


def _is_loopback_host(host: str) -> bool:
    normalized = host.strip().lower().removeprefix("[").removesuffix("]")
    return normalized in _LOOPBACK_HOSTS


def validate_http_url(raw: str, *, label: str) -> str:
    """校验 HTTP 管理依赖地址：仅允许 HTTPS 或本机回环 HTTP，且禁止携带凭据、查询串与片段。"""
    cleaned = raw.strip()
    if not cleaned:
        return ""
    parts = urlsplit(cleaned)
    host = parts.hostname
    if (
        not host
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or any(ord(ch) <= 32 or ord(ch) == 127 for ch in cleaned)
    ):
        raise ValueError(f"{label} URL is invalid")
    if parts.scheme == "https":
        return cleaned.rstrip("/")
    if parts.scheme == "http" and _is_loopback_host(host):
        return cleaned.rstrip("/")
    raise ValueError(f"{label} requires HTTPS or loopback HTTP")


def validate_grpc_target(target: str, *, plaintext: bool, label: str = "gRPC target") -> str:
    """校验 gRPC host:port 目标地址；明文连接仅允许显式配置的本机回环地址。"""
    cleaned = target.strip()
    if not cleaned:
        return ""
    parts = urlsplit(f"tcp://{cleaned}")
    host = parts.hostname
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"{label} must be host:port") from exc
    if (
        not host
        or port is None
        or not 1 <= port <= 65535
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or parts.path
    ):
        raise ValueError(f"{label} must be host:port")
    if plaintext and not _is_loopback_host(host):
        raise ValueError(f"{label} plaintext connection is only allowed for loopback addresses")
    return cleaned


def validate_node_id(value: str) -> int:
    """校验场景节点 ID 或区 ID（非零 uint32 十进制文本）。"""
    if not isinstance(value, str) or not _POSITIVE_NODE_ID_RE.fullmatch(value):
        raise ValueError(f"invalid scene node id: {value!r}")
    parsed = int(value)
    if parsed > _UINT32_MAX:
        raise ValueError(f"scene node id out of uint32 range: {value!r}")
    return parsed


def validate_static_scene_key(key: str) -> None:
    """校验静态场景路由键：支持 ``<nodeId>`` 或 ``<zoneId>/<nodeId>``。"""
    if not isinstance(key, str) or not key:
        raise ValueError("scene target key must be a non-empty string")
    if "/" not in key:
        validate_node_id(key)
        return
    parts = key.split("/")
    if len(parts) != 2:
        raise ValueError(f"invalid scene target key: {key!r}")
    validate_node_id(parts[0])
    validate_node_id(parts[1])


def _strict_json_object(raw: str, *, label: str) -> dict[str, object]:
    """解析严格 JSON 对象：拒绝重复键、非法常量及非对象顶层结构。"""
    def _no_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate key: {key}")
            result[key] = value
        return result

    def _no_bad_constant(token: str) -> object:
        raise ValueError(f"{label} contains invalid JSON constant: {token}")

    try:
        parsed = json.loads(
            raw,
            object_pairs_hook=_no_duplicate_pairs,
            parse_constant=_no_bad_constant,
        )
    except ValueError as exc:
        raise ValueError(f"{label} could not be parsed as JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be a JSON object")
    return parsed


def parse_scene_targets(
    raw: Mapping[str, str] | str | None,
    *,
    plaintext: bool,
) -> tuple[tuple[str, str], ...]:
    """解析并校验静态场景映射表（支持字典或 JSON 字符串）。"""
    if raw is None:
        return ()
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return ()
        mapping = _strict_json_object(stripped, label="scene_targets")
    elif isinstance(raw, Mapping):
        mapping = dict(raw)
    else:
        raise ValueError("scene_targets must be a mapping or JSON string")
    if len(mapping) > MAX_SCENE_CHANNELS:
        raise ValueError(f"scene_targets exceeds limit of {MAX_SCENE_CHANNELS}")
    entries: list[tuple[str, str]] = []
    for key, value in sorted(mapping.items()):
        validate_static_scene_key(str(key))
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"scene_targets[{key!r}] must be a non-empty host:port")
        entries.append((str(key), validate_grpc_target(value, plaintext=plaintext, label=f"scene_targets[{key}]")))
    return tuple(entries)


def _parse_bool(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "on"):
            return True
        if lowered in ("0", "false", "no", "off", ""):
            return False
    return default


def _first_env(environ: Mapping[str, str], *names: str) -> str:
    for name in names:
        value = environ.get(name, "").strip()
        if value:
            return value
    return ""


@dataclass(frozen=True, slots=True)
class GmAdminConfig:
    """gm_admin HTTP 管理后台与下游游戏服务连接配置。"""

    host: str = DEFAULT_HTTP_HOST
    port: int = DEFAULT_HTTP_PORT
    admin_token: str = ""
    gateway_url: str = ""
    gateway_admin_key: str = ""
    data_service: str = ""
    player_locator: str = ""
    mail_service: str = ""
    mail_admin_secret: bytes = b""
    login_service: str = ""
    scene_targets: tuple[tuple[str, str], ...] = ()
    scene_etcd_url: str = ""
    scene_admin_secret: bytes = b""
    caller_secret: bytes = b""
    plaintext: bool = True
    request_budget_sec: float = DEFAULT_REQUEST_BUDGET_SEC
    discovery_ttl_sec: float = DEFAULT_DISCOVERY_TTL_SEC
    max_campaign_targets: int = DEFAULT_MAX_CAMPAIGN_TARGETS
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = DEFAULT_DATABASE_NAME
    mysql_password: str = ""
    max_open_conn: int = 10
    min_open_conn: int = 1
    sql_timeout_sec: float = 10.0
    bootstrap_schema: bool = False
    use_memory_journal: bool = False

    def scene_target_map(self) -> dict[str, str]:
        return dict(self.scene_targets)

    @property
    def mail_enabled(self) -> bool:
        return bool(self.mail_service) and len(self.mail_admin_secret) >= MIN_HMAC_SECRET_BYTES

    @property
    def player_commands_enabled(self) -> bool:
        return (
            bool(self.player_locator)
            and (bool(self.scene_targets) or bool(self.scene_etcd_url))
            and len(self.scene_admin_secret) >= MIN_HMAC_SECRET_BYTES
        )

    def validate(self) -> None:
        """启动期参数与安全边界校验。"""
        if not self.host or not self.host.strip():
            raise ValueError("gm_admin host must not be blank")
        if not 0 <= self.port <= 65535:
            raise ValueError("gm_admin port must be in 0..65535")
        if not 0 < self.request_budget_sec <= 5.0:
            raise ValueError("gm_admin request_budget_sec must be in (0, 5.0]")
        if self.discovery_ttl_sec < 0:
            raise ValueError("gm_admin discovery_ttl_sec must be >= 0")
        if not 1 <= self.max_campaign_targets <= 100_000:
            raise ValueError("gm_admin max_campaign_targets must be in 1..100000")
        if self.gateway_url:
            validate_http_url(self.gateway_url, label="gateway_url")
        if self.scene_etcd_url:
            validate_http_url(self.scene_etcd_url, label="scene_etcd_url")
        for label, target in (
            ("data_service", self.data_service),
            ("player_locator", self.player_locator),
            ("mail_service", self.mail_service),
            ("login_service", self.login_service),
        ):
            if target:
                validate_grpc_target(target, plaintext=self.plaintext, label=label)
        for key, target in self.scene_targets:
            validate_static_scene_key(key)
            validate_grpc_target(target, plaintext=self.plaintext, label=f"scene_targets[{key}]")
        if not self.use_memory_journal:
            if not self.mysql_host or not self.mysql_user or not self.mysql_database:
                raise ValueError("gm_admin mysql_host, mysql_user and mysql_database are required")
            if not 1 <= self.mysql_port <= 65535:
                raise ValueError("gm_admin mysql_port must be in 1..65535")
            if self.max_open_conn < 1 or self.min_open_conn < 0 or self.min_open_conn > self.max_open_conn:
                raise ValueError("gm_admin MySQL pool bounds are invalid")
            if self.sql_timeout_sec <= 0:
                raise ValueError("gm_admin sql_timeout_sec must be positive")

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        environ: Mapping[str, str] | None = None,
    ) -> GmAdminConfig:
        """从全局 ``Settings``（含可选 ``gm_admin_node`` 字典）与环境变量构造配置。"""
        env = os.environ if environ is None else environ
        raw_section = getattr(settings, "gm_admin_node", None) or {}
        if not isinstance(raw_section, Mapping):
            raise ValueError("gm_admin_node section must be a table/dict")
        raw = dict(raw_section)
        forbidden = _TOML_FORBIDDEN_FIELDS.intersection(raw)
        if forbidden:
            raise ValueError(
                f"gm_admin secrets must be provided via environment variables, not TOML: {sorted(forbidden)}"
            )

        valid_names = {f.name for f in fields(cls)} | {"scene_targets_json", "mail"}
        unknown = set(raw) - valid_names
        if unknown:
            raise ValueError(f"unknown gm_admin_node config keys: {sorted(unknown)}")

        plaintext = _parse_bool(
            env.get("GM_GAME_PLAINTEXT", raw.get("plaintext", True)),
            default=True,
        )
        host = _first_env(env, "GM_HTTP_ADDRESS") or str(raw.get("host", DEFAULT_HTTP_HOST))
        raw_port = _first_env(env, "GM_HTTP_PORT")
        port = int(raw_port) if raw_port else int(raw.get("port", DEFAULT_HTTP_PORT))

        gateway_raw = _first_env(env, "GM_GATEWAY_URL") or str(raw.get("gateway_url", ""))
        gateway_url = validate_http_url(gateway_raw, label="gateway_url") if gateway_raw else ""

        etcd_raw = (
            _first_env(env, "GM_SCENE_ETCD_URL")
            or str(raw.get("scene_etcd_url", ""))
            or str(getattr(settings, "etcd", ""))
        )
        scene_etcd_url = validate_http_url(etcd_raw, label="scene_etcd_url") if etcd_raw else ""

        data_service = (
            _first_env(env, "GM_DATA_TARGET")
            or str(raw.get("data_service", ""))
            or str(getattr(settings, "data_service", ""))
        )
        player_locator = (
            _first_env(env, "GM_LOCATOR_TARGET")
            or str(raw.get("player_locator", ""))
            or str(getattr(settings, "player_locator", ""))
        )
        mail_service = (
            _first_env(env, "GM_MAIL_TARGET")
            or str(raw.get("mail_service", ""))
            or str(raw.get("mail", ""))
        )
        login_service = _first_env(env, "GM_LOGIN_TARGET") or str(raw.get("login_service", ""))

        scene_targets_source = _first_env(env, "GM_SCENE_TARGETS_JSON")
        if scene_targets_source:
            scene_targets = parse_scene_targets(scene_targets_source, plaintext=plaintext)
        elif "scene_targets_json" in raw:
            scene_targets = parse_scene_targets(raw["scene_targets_json"], plaintext=plaintext)
        elif "scene_targets" in raw:
            scene_targets = parse_scene_targets(raw["scene_targets"], plaintext=plaintext)
        else:
            scene_targets = ()

        admin_token = _first_env(env, "GM_ADMIN_TOKEN", "XUANMING_GM_ADMIN_TOKEN")
        gateway_admin_key = _first_env(env, "GM_GATEWAY_ADMIN_KEY", "XUANMING_PORTAL_ADMIN_API_KEY")
        mail_admin_secret = _first_env(env, "GM_MAIL_ADMIN_SECRET", "MAIL_PRODUCER_SECRET_ADMIN").encode("utf-8")
        scene_admin_secret = _first_env(env, "GM_SCENE_ADMIN_SECRET").encode("utf-8")
        caller_secret = _first_env(env, "GM_CALLER_SECRET").encode("utf-8")
        mysql_password = _first_env(env, "GM_DB_PASSWORD", "GM_MYSQL_PASSWORD")

        config = cls(
            host=host,
            port=port,
            admin_token=admin_token,
            gateway_url=gateway_url,
            gateway_admin_key=gateway_admin_key,
            data_service=data_service.strip(),
            player_locator=player_locator.strip(),
            mail_service=mail_service.strip(),
            mail_admin_secret=mail_admin_secret,
            login_service=login_service.strip(),
            scene_targets=scene_targets,
            scene_etcd_url=scene_etcd_url,
            scene_admin_secret=scene_admin_secret,
            caller_secret=caller_secret,
            plaintext=plaintext,
            request_budget_sec=float(raw.get("request_budget_sec", DEFAULT_REQUEST_BUDGET_SEC)),
            discovery_ttl_sec=float(raw.get("discovery_ttl_sec", DEFAULT_DISCOVERY_TTL_SEC)),
            max_campaign_targets=int(raw.get("max_campaign_targets", DEFAULT_MAX_CAMPAIGN_TARGETS)),
            mysql_host=_first_env(env, "GM_DB_HOST") or str(raw.get("mysql_host", "127.0.0.1")),
            mysql_port=int(_first_env(env, "GM_DB_PORT") or raw.get("mysql_port", 3306)),
            mysql_user=_first_env(env, "GM_DB_USER") or str(raw.get("mysql_user", "appuser")),
            mysql_database=_first_env(env, "GM_DB_DATABASE") or str(raw.get("mysql_database", DEFAULT_DATABASE_NAME)),
            mysql_password=mysql_password,
            max_open_conn=int(raw.get("max_open_conn", 10)),
            min_open_conn=int(raw.get("min_open_conn", 1)),
            sql_timeout_sec=float(raw.get("sql_timeout_sec", 10.0)),
            bootstrap_schema=_parse_bool(raw.get("bootstrap_schema", False)),
            use_memory_journal=_parse_bool(raw.get("use_memory_journal", False)),
        )
        config.validate()
        return config
