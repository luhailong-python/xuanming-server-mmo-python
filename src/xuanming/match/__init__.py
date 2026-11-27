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
from .jianghu import JianghuBattleGrudgeSummary, JianghuManager, JianghuRuleConfig, load_jianghu_rule
from .match_service import MatchGrpcService, MatchServer
from .model import MATCH_NODE_TYPE, TEAM_NODE_TYPE, BattleNodeEntry, CallerIdentity, PlayerPresence
from .node import open_service, run
from .outbox import OutcomeOutboxWorker, OutboxRoundStats
from .ranked import (
    OFFICIAL_RANKED_PRESETS,
    RankedFeaturedBuildRow,
    RankedManager,
    RankedPresetSpec,
    RankedRivalryRow,
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
from .tactical_puzzle import (
    TacticalPuzzleManager,
    derive_honor,
    deterministic_puzzle_seed,
    normalize_and_validate_text,
    validate_tactical_puzzle_spec,
    validate_tag_mask,
)
from .team_service import KafkaTeamNotifier, NullTeamNotifier, TeamGrpcService
from .tournament import (
    MatchMvpResult,
    SpectateCheerHub,
    TournamentCatalog,
    TournamentManager,
    TournamentRewardSpec,
    TournamentRuleSpec,
)
from .trial_cards import TrialChallengeCardManager, build_trial_battle_context, supports_trial_vows

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
    "JianghuBattleGrudgeSummary",
    "JianghuManager",
    "JianghuRuleConfig",
    "KafkaTeamNotifier",
    "MATCH_NODE_TYPE",
    "MatchConfig",
    "MatchGrpcService",
    "MatchMvpResult",
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
    "RankedFeaturedBuildRow",
    "RankedManager",
    "RankedPresetSpec",
    "RankedRivalryRow",
    "RankedRuleConfig",
    "RankedSeasonRow",
    "RatingApplyResult",
    "SpectateCheerHub",
    "SpectateManager",
    "SqlMatchStore",
    "StaticBattleNodeResolver",
    "TEAM_NODE_TYPE",
    "TacticalPuzzleManager",
    "TeamGrpcService",
    "TournamentCatalog",
    "TournamentManager",
    "TournamentRewardSpec",
    "TournamentRuleSpec",
    "TrialChallengeCardManager",
    "apply_battle_result",
    "build_trial_battle_context",
    "compute_elo_delta",
    "load_jianghu_rule",
    "open_pool",
    "open_service",
    "run",
    "supports_trial_vows",
    "tier_for_rating",
]
