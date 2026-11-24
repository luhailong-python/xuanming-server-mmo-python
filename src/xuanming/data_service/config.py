"""data_service node settings: ``[data_service_node]`` in the cluster TOML plus secrets/passwords from the environment."""

import os
from dataclasses import dataclass, fields

from ..settings import Settings
from .model import DEFAULT_ID_TAGS

SECRET_ENVS = {
    "login": "MMORPG_ACCOUNT_DIRECTORY_SECRET_LOGIN",
    "scene": "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE",
    "scene_manager": "MMORPG_ACCOUNT_DIRECTORY_SECRET_SCENE_MANAGER",
    "trade": "MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE",
    "db": "MMORPG_ROLE_FENCE_SECRET_DB",
    "player_inbox": "MMORPG_ROLE_FENCE_SECRET_PLAYER_INBOX",
    "match": "MMORPG_ROLE_FENCE_SECRET_MATCH",
    "mail": "MMORPG_ROLE_FENCE_SECRET_MAIL",
    "guild": "MMORPG_ROLE_FENCE_SECRET_GUILD",
    "friend": "MMORPG_ROLE_FENCE_SECRET_FRIEND",
}
REQUIRED_CALLERS = ("login", "scene")  # Go also requires scene_manager and trade; see docs in the package README


@dataclass(frozen=True, slots=True)
class DataServiceConfig:
    node_id: int
    host: str = "0.0.0.0"
    grpc_port: int = 9000
    advertise_host: str = ""
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "appuser"
    mysql_database: str = "mmorpg_global"
    mysql_password: str = ""
    logical_shards: int = 1
    max_players_per_account: int = 5
    max_open_conn: int = 8
    storage_ids: tuple[int, ...] = (1_000_000,)
    merged_zone_refresh_sec: int = 5
    id_tags: tuple[str, ...] = DEFAULT_ID_TAGS
    secrets: dict = None

    @classmethod
    def from_settings(cls, settings: Settings, environ=None) -> "DataServiceConfig":
        environ = os.environ if environ is None else environ
        section = dict(settings.data_service_node)
        unknown = set(section) - {field.name for field in fields(cls)} | (set(section) & {"secrets", "mysql_password"})
        if unknown or not section:
            raise ValueError(f"[data_service_node] 缺失或含未知字段: {sorted(unknown)}")
        for key in ("storage_ids", "id_tags"):
            if key in section:
                section[key] = tuple(section[key])
        config = cls(**section, mysql_password=environ.get("DATA_SERVICE_MYSQL_PASSWORD", ""),
                     secrets=caller_secrets(environ))
        config.validate(settings)
        return config

    def validate(self, settings: Settings) -> None:
        if self.logical_shards != 1:
            raise ValueError("Python data_service 只支持 logical_shards = 1")
        if (not 0 < self.node_id < 1 << 17 or not 0 <= self.grpc_port <= 65535 or not self.advertise_host
                or not 1 <= self.max_players_per_account <= 100 or not 1 <= self.max_open_conn <= 128
                or not 1 <= self.merged_zone_refresh_sec <= 60 or not self.storage_ids
                or any(storage < 1_000_000 for storage in self.storage_ids)):
            raise ValueError("data_service_node 配置无效")
        if not settings.etcd:
            raise ValueError("data_service 需要 etcd 地址")


def caller_secrets(environ) -> dict[str, bytes]:
    """Required callers must be valid (>= 32 bytes); every configured secret must be distinct (global_caller_secrets.go)."""
    found = {}
    for caller, name in SECRET_ENVS.items():
        if caller in REQUIRED_CALLERS or environ.get(name):
            found[caller] = _checked(environ, name)
    if len(set(found.values())) != len(found):
        raise ValueError("账号目录调用方密钥必须互不相同")
    return found


def _checked(environ, name: str) -> bytes:
    value = environ.get(name, "").encode()
    if len(value) < 32:
        raise ValueError(f"{name} 至少需要 32 字节")
    return value
