"""match（含 team）微服务包对外导出。"""

from .config import MatchConfig
from .gather import (
    EtcdBattleNodeResolver,
    GatherPipeline,
    GrpcEndpointPool,
    Matchmaker,
    MemoryPresenceProvider,
    StaticBattleNodeResolver,
)
from .match_service import MatchGrpcService, MatchServer
from .model import MATCH_NODE_TYPE, TEAM_NODE_TYPE, BattleNodeEntry, CallerIdentity, PlayerPresence
from .node import open_service, run
from .outbox import OutcomeOutboxWorker, OutboxRoundStats
from .spectate import SpectateManager
from .sql_store import SqlMatchStore, open_pool
from .store import MatchStore, MemoryMatchStore
from .team_service import KafkaTeamNotifier, NullTeamNotifier, TeamGrpcService

__all__ = [
    "BattleNodeEntry",
    "CallerIdentity",
    "EtcdBattleNodeResolver",
    "GatherPipeline",
    "GrpcEndpointPool",
    "KafkaTeamNotifier",
    "MATCH_NODE_TYPE",
    "MatchConfig",
    "MatchGrpcService",
    "MatchServer",
    "MatchStore",
    "Matchmaker",
    "MemoryMatchStore",
    "MemoryPresenceProvider",
    "NullTeamNotifier",
    "OutcomeOutboxWorker",
    "OutboxRoundStats",
    "PlayerPresence",
    "SpectateManager",
    "SqlMatchStore",
    "StaticBattleNodeResolver",
    "TEAM_NODE_TYPE",
    "TeamGrpcService",
    "open_pool",
    "open_service",
    "run",
]
