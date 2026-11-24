"""Sa-Token 认证服务配置解析与 fail-closed profile 安全门禁。

对应 Java 侧 ``JustAuthProperties`` 与 ``DevLoginController`` 的 ``@Profile("dev")`` 隔离策略：
- 默认运行在 ``prod`` profile，且 ``dev_login_enabled=False``；
- 只要 ``profile == "prod"``，无论调用方是否误传 ``dev_login_enabled=True``，``allow_dev_login``
  恒返回 ``False``，保证生产环境绝不暴露免密码 ``/auth/dev/login`` 入口。
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any

from ..settings import Settings

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8085
DEFAULT_PROFILE = "prod"
DEFAULT_TOKEN_NAME = "satoken"
DEFAULT_TOKEN_TTL_SEC = 86400
DEFAULT_DATABASE = "satoken_auth"

VALID_PROFILES = frozenset(("prod", "dev", "test"))
_TOKEN_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,63}$")
_TOML_FORBIDDEN_FIELDS = frozenset(("mysql_password",))

# 各标准第三方 OAuth 提供商的默认授权页地址（对齐 JustAuth / AuthController.java）。
DEFAULT_OAUTH_AUTHORIZE_URLS: dict[str, str] = {
    "github": "https://github.com/login/oauth/authorize",
    "wechat": "https://open.weixin.qq.com/connect/qrconnect",
    "wechat_open": "https://open.weixin.qq.com/connect/qrconnect",
    "qq": "https://graph.qq.com/oauth2.0/authorize",
}


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
class OAuthProviderConfig:
    """单个第三方 OAuth 提供商配置（对齐 ``JustAuthProperties.ProviderConfig``）。"""

    provider: str
    client_id: str = ""
    client_secret: str = field(default="", repr=False)
    redirect_uri: str = ""
    authorize_url: str = ""
    token_url: str = ""
    userinfo_url: str = ""
    scope: str = ""
    union_id: bool = True

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_id.strip() and self.client_secret and self.client_secret.strip())

    @property
    def effective_authorize_url(self) -> str:
        if self.authorize_url:
            return self.authorize_url
        return DEFAULT_OAUTH_AUTHORIZE_URLS.get(
            self.provider.lower(),
            f"https://oauth.example.invalid/{self.provider}/authorize",
        )


@dataclass(frozen=True, slots=True)
class SaTokenAuthConfig:
    """Sa-Token 认证节点完整配置（fail-closed：``prod`` 下强制关闭 dev login）。"""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    profile: str = DEFAULT_PROFILE
    dev_login_enabled: bool = False
    token_name: str = DEFAULT_TOKEN_NAME
    token_ttl_sec: int = DEFAULT_TOKEN_TTL_SEC
    redis_url: str = ""
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_password: str = field(default="", repr=False)
    mysql_database: str = DEFAULT_DATABASE
    max_open_conn: int = 10
    min_open_conn: int = 1
    sql_timeout_sec: float = 10.0
    use_memory_store: bool = False
    oauth_providers: dict[str, OAuthProviderConfig] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_profile = (self.profile or DEFAULT_PROFILE).strip().lower()
        object.__setattr__(self, "profile", normalized_profile)
        if normalized_profile not in VALID_PROFILES:
            raise ValueError(f"invalid satoken_auth profile {normalized_profile!r}: expected one of {sorted(VALID_PROFILES)}")
        # fail-closed：prod 环境下强制拒绝开启 dev_login，并记录告警。
        if normalized_profile == "prod" and self.dev_login_enabled:
            logger.warning("satoken_auth dev_login_enabled forced off in prod profile")
            object.__setattr__(self, "dev_login_enabled", False)
        if not self.host or not self.host.strip():
            raise ValueError("satoken_auth host must not be blank")
        if not 0 <= int(self.port) <= 65535:
            raise ValueError("satoken_auth port must be in 0..65535")
        if not _TOKEN_NAME_RE.fullmatch(self.token_name):
            raise ValueError(f"invalid satoken_auth token_name: {self.token_name!r}")
        if self.token_ttl_sec < 1:
            raise ValueError("satoken_auth token_ttl_sec must be >= 1")
        if not 1 <= int(self.mysql_port) <= 65535:
            raise ValueError("satoken_auth mysql_port must be in 1..65535")
        if self.max_open_conn < 1 or self.min_open_conn < 0 or self.min_open_conn > self.max_open_conn:
            raise ValueError("satoken_auth MySQL pool bounds are invalid")
        if self.sql_timeout_sec <= 0:
            raise ValueError("satoken_auth sql_timeout_sec must be > 0")

    @property
    def allow_dev_login(self) -> bool:
        """是否允许 ``POST /auth/dev/login``（仅 ``dev`` / ``test`` 且显式开启时为 ``True``）。"""
        return self.profile in ("dev", "test") and bool(self.dev_login_enabled)

    def get_oauth_provider(self, provider: str) -> OAuthProviderConfig | None:
        key = provider.strip().lower()
        if key in self.oauth_providers:
            return self.oauth_providers[key]
        if key == "wechat_open" and "wechat" in self.oauth_providers:
            return self.oauth_providers["wechat"]
        return None

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        environ: Mapping[str, str] | None = None,
    ) -> SaTokenAuthConfig:
        """从全局 ``Settings``（含可选 ``satoken_auth_node`` 字典）与环境变量构造配置。"""
        env = os.environ if environ is None else environ
        raw_section = getattr(settings, "satoken_auth_node", None) or {}
        if not isinstance(raw_section, Mapping):
            raise ValueError("satoken_auth_node section must be a table/dict")
        raw: dict[str, Any] = dict(raw_section)

        forbidden = _TOML_FORBIDDEN_FIELDS.intersection(raw)
        if forbidden:
            raise ValueError(
                f"satoken_auth secrets must be provided via environment variables, not TOML: {sorted(forbidden)}"
            )

        valid_names = {f.name for f in fields(cls)}
        unknown = set(raw) - valid_names
        if unknown:
            raise ValueError(f"unknown satoken_auth_node config keys: {sorted(unknown)}")

        profile = (
            _first_env(env, "SATOKEN_AUTH_PROFILE", "MMORPG_RUN_MODE")
            or str(raw.get("profile", DEFAULT_PROFILE))
        ).strip().lower()
        raw_dev_login = env.get("SATOKEN_DEV_LOGIN_ENABLED")
        dev_login_requested = (
            _parse_bool(raw_dev_login, default=False)
            if raw_dev_login is not None
            else _parse_bool(raw.get("dev_login_enabled", False), default=False)
        )
        # prod 下无条件关闭 dev_login（与 __post_init__ 双重防线）。
        dev_login_enabled = False if profile == "prod" else dev_login_requested

        host = _first_env(env, "SATOKEN_AUTH_HOST") or str(raw.get("host", DEFAULT_HOST))
        raw_port = _first_env(env, "SATOKEN_AUTH_PORT")
        port = int(raw_port) if raw_port else int(raw.get("port", DEFAULT_PORT))
        token_name = _first_env(env, "SATOKEN_TOKEN_NAME") or str(raw.get("token_name", DEFAULT_TOKEN_NAME))
        raw_ttl = _first_env(env, "SATOKEN_TOKEN_TTL_SEC")
        token_ttl_sec = int(raw_ttl) if raw_ttl else int(raw.get("token_ttl_sec", DEFAULT_TOKEN_TTL_SEC))

        redis_url = (
            _first_env(env, "SATOKEN_REDIS_URL")
            or str(raw.get("redis_url", ""))
            or str(getattr(settings, "login_redis", ""))
            or str(getattr(settings, "redis", ""))
        )

        oauth_providers: dict[str, OAuthProviderConfig] = {}
        raw_providers = raw.get("oauth_providers")
        if isinstance(raw_providers, Mapping):
            for name, cfg in raw_providers.items():
                key = str(name).strip().lower()
                if isinstance(cfg, OAuthProviderConfig):
                    oauth_providers[key] = cfg
                elif isinstance(cfg, Mapping):
                    oauth_providers[key] = OAuthProviderConfig(
                        provider=key,
                        client_id=str(cfg.get("client_id", "")),
                        client_secret=str(cfg.get("client_secret", "")),
                        redirect_uri=str(cfg.get("redirect_uri", "")),
                        authorize_url=str(cfg.get("authorize_url", "")),
                        token_url=str(cfg.get("token_url", "")),
                        userinfo_url=str(cfg.get("userinfo_url", "")),
                        scope=str(cfg.get("scope", "")),
                        union_id=_parse_bool(cfg.get("union_id", True), default=True),
                    )

        # 从 JustAuth 兼容环境变量补充 / 覆盖第三方 OAuth 配置。
        for provider_key in ("github", "wechat", "qq"):
            prefix = f"JUSTAUTH_{provider_key.upper()}"
            cid = _first_env(env, f"{prefix}_CLIENT_ID")
            csec = _first_env(env, f"{prefix}_CLIENT_SECRET")
            ruri = _first_env(env, f"{prefix}_REDIRECT_URI")
            if cid or csec or ruri:
                existing = oauth_providers.get(provider_key)
                oauth_providers[provider_key] = OAuthProviderConfig(
                    provider=provider_key,
                    client_id=cid or (existing.client_id if existing else ""),
                    client_secret=csec or (existing.client_secret if existing else ""),
                    redirect_uri=ruri or (existing.redirect_uri if existing else ""),
                    authorize_url=existing.authorize_url if existing else "",
                    token_url=existing.token_url if existing else "",
                    userinfo_url=existing.userinfo_url if existing else "",
                    scope=existing.scope if existing else "",
                    union_id=_parse_bool(
                        env.get(f"{prefix}_UNION_ID", existing.union_id if existing else True),
                        default=True,
                    ),
                )

        return cls(
            host=host,
            port=port,
            profile=profile,
            dev_login_enabled=dev_login_enabled,
            token_name=token_name,
            token_ttl_sec=token_ttl_sec,
            redis_url=redis_url.strip(),
            mysql_host=_first_env(env, "SATOKEN_MYSQL_HOST", "SATOKEN_DB_HOST") or str(raw.get("mysql_host", "127.0.0.1")),
            mysql_port=int(_first_env(env, "SATOKEN_MYSQL_PORT", "SATOKEN_DB_PORT") or raw.get("mysql_port", 3306)),
            mysql_user=_first_env(env, "SATOKEN_MYSQL_USER", "SATOKEN_DB_USER") or str(raw.get("mysql_user", "appuser")),
            mysql_password=_first_env(env, "SATOKEN_MYSQL_PASSWORD", "SATOKEN_DB_PASSWORD", "MMORPG_SQL_PASSWORD"),
            mysql_database=_first_env(env, "SATOKEN_MYSQL_DATABASE", "SATOKEN_DB_DATABASE")
            or str(raw.get("mysql_database", DEFAULT_DATABASE)),
            max_open_conn=int(raw.get("max_open_conn", 10)),
            min_open_conn=int(raw.get("min_open_conn", 1)),
            sql_timeout_sec=float(raw.get("sql_timeout_sec", 10.0)),
            use_memory_store=_parse_bool(
                env.get("SATOKEN_USE_MEMORY_STORE", raw.get("use_memory_store", False)),
                default=False,
            ),
            oauth_providers=oauth_providers,
        )


__all__ = [
    "DEFAULT_DATABASE",
    "DEFAULT_HOST",
    "DEFAULT_OAUTH_AUTHORIZE_URLS",
    "DEFAULT_PORT",
    "DEFAULT_PROFILE",
    "DEFAULT_TOKEN_NAME",
    "DEFAULT_TOKEN_TTL_SEC",
    "OAuthProviderConfig",
    "SaTokenAuthConfig",
    "VALID_PROFILES",
]
