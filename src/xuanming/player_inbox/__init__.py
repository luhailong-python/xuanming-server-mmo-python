"""player_inbox 服务包：玩家持久收件箱（消费、Battle 结算、GM 处置运维与角色护栏参与方）。"""

from .config import PlayerInboxConfig
from .node import open_service, run
from .notify import InboxNotifier, KafkaInboxNotifier, NotifyResult, NullInboxNotifier
from .registry import InboxRegistry
from .service import PlayerInboxService
from .sql_store import SqlInboxStore, open_pool
from .store import DataServiceDirectory, GrpcDataServiceDirectory, InboxStore, MemoryInboxStore

__all__ = [
    "DataServiceDirectory",
    "GrpcDataServiceDirectory",
    "InboxNotifier",
    "InboxRegistry",
    "InboxStore",
    "KafkaInboxNotifier",
    "MemoryInboxStore",
    "NotifyResult",
    "NullInboxNotifier",
    "PlayerInboxConfig",
    "PlayerInboxService",
    "SqlInboxStore",
    "open_pool",
    "open_service",
    "run",
]
