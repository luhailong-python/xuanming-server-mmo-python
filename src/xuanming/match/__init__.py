"""match（含 team）微服务包对外导出。"""

from .battle_scripts import BattleScriptCatalog, BattleScriptManager, ConfigTableScriptLookup
from .config import MatchConfig, SpectateChatConfig
from .gather import (
    EtcdBattleNodeResolver,
    GatherPipeline,
    GrpcEndpointPool,
    Matchmaker,
    MemoryPresenceProvider,
    StaticBattleNodeResolver,
)
from .jianghu import (
    JianghuBattleGrudgeSummary,
    JianghuManager,
    JianghuRuleConfig,
    evaluate_scene_eligibility_response,
    load_jianghu_rule,
)
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
    featured_build_key_hash,
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
from .spectate_chat import (
    SpectateChatGuardResult,
    SpectateChatGuardVerdict,
    SpectateChatHub,
    SpectateChatManager,
    SpectateChatModeRule,
    SpectateChatRoomInfo,
    adapt_content_guard_engine,
    resolve_player_spectate_chat_allow,
    sanitize_sender_name,
    spectate_chat_enabled_for_battle,
    spectate_chat_rule_of_mode,
)
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
    "SpectateChatConfig",
    "SpectateChatGuardResult",
    "SpectateChatGuardVerdict",
    "SpectateChatHub",
    "SpectateChatManager",
    "SpectateChatModeRule",
    "SpectateChatRoomInfo",
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
    "adapt_content_guard_engine",
    "apply_battle_result",
    "build_trial_battle_context",
    "compute_elo_delta",
    "evaluate_scene_eligibility_response",
    "featured_build_key_hash",
    "load_jianghu_rule",
    "open_pool",
    "open_service",
    "resolve_player_spectate_chat_allow",
    "run",
    "sanitize_sender_name",
    "spectate_chat_enabled_for_battle",
    "spectate_chat_rule_of_mode",
    "supports_trial_vows",
    "tier_for_rating",
]
