"""match（含 team）微服务包对外导出。"""

from .battle_scripts import BattleScriptCatalog, BattleScriptManager, ConfigTableScriptLookup
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
from .ranked import (
    OFFICIAL_RANKED_PRESETS,
    RankedManager,
    RankedPresetSpec,
    RankedRuleConfig,
    RankedSeasonRow,
    tier_for_rating,
)
from .rating import (
    DEFAULT_RATING_CENTI,
    RANK_BOARD_TYPES,
    RankOutboxPublisher,
    RankPublishStats,
    RatingApplyResult,
    apply_battle_result,
    compute_elo_delta,
)
from .spectate import SpectateManager
from .sql_store import SqlMatchStore, open_pool
from .store import MatchStore, MemoryMatchStore
from .team_service import KafkaTeamNotifier, NullTeamNotifier, TeamGrpcService

__all__ = [
    "BattleNodeEntry",
    "BattleScriptCatalog",
    "BattleScriptManager",
    "CallerIdentity",
    "ConfigTableScriptLookup",
    "DEFAULT_RATING_CENTI",
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
    "OFFICIAL_RANKED_PRESETS",
    "OutcomeOutboxWorker",
    "OutboxRoundStats",
    "PlayerPresence",
    "RANK_BOARD_TYPES",
    "RankOutboxPublisher",
    "RankPublishStats",
    "RankedManager",
    "RankedPresetSpec",
    "RankedRuleConfig",
    "RankedSeasonRow",
    "RatingApplyResult",
    "SpectateManager",
    "SqlMatchStore",
    "StaticBattleNodeResolver",
    "TEAM_NODE_TYPE",
    "TeamGrpcService",
    "apply_battle_result",
    "compute_elo_delta",
    "open_pool",
    "open_service",
    "run",
    "tier_for_rating",
]
