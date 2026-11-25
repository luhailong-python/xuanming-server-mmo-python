"""玩家存储与版本化配置表。"""

from .config_tables import ConfigTable, ConfigTableError, ConfigTableRegistry, ConfigTables
from .player_rows import PlayerRowCodec, PlayerRowKind
from .player_store import (
    DataServicePlayerStore,
    JsonPlayerStore,
    PlayerSnapshot,
    SaveInDoubt,
    SqlitePlayerStore,
    StoreRejected,
    StoreUnavailable,
)
from .storage_factory import create_player_store

__all__ = [
    "ConfigTable",
    "ConfigTableError",
    "ConfigTableRegistry",
    "ConfigTables",
    "DataServicePlayerStore",
    "JsonPlayerStore",
    "PlayerRowCodec",
    "PlayerRowKind",
    "PlayerSnapshot",
    "SaveInDoubt",
    "SqlitePlayerStore",
    "StoreRejected",
    "StoreUnavailable",
    "create_player_store",
]
