"""Python 版 player_locator 服务：玩家会话定位、CAS 栅栏与断线租约清理。"""

from .config import (
    DEFAULT_GRPC_PORT,
    PLAYER_LOCATOR_NODE_TYPE,
    SCENE_MANAGER_NODE_TYPE,
    PlayerLocatorConfig,
    parse_scene_manager_endpoints,
)
from .gate_sweep import (
    DirectoryLiveGateView,
    GateSweeper,
    IndexedMemberAction,
    LiveViewGate,
    LiveViewVerdict,
    SweepRoundReport,
    classify_indexed_member,
)
from .hosted_resume import (
    HostedResumeFacts,
    HostedResumeVerdict,
    HostedReturnRecord,
    decide_hosted_resume,
    parse_hosted_return_record,
    resume_hosted_if_lost,
    return_record_ttl_alignment,
)
from .lease_monitor import (
    GrpcSceneManagerClient,
    HolderReleasePendingError,
    KafkaGatePublisher,
    LeaseMonitor,
    RoutingSceneManagerClient,
)
from .node import run
from .service import PlayerLocatorService
from .store import (
    AfkPassState,
    CommitLeaseResult,
    LeaseClaim,
    MemorySessionStore,
    RearmLeaseResult,
    RedisSessionStore,
    ReplaceSessionResult,
    open_player_redis,
)

__all__ = [
    "AfkPassState",
    "CommitLeaseResult",
    "DEFAULT_GRPC_PORT",
    "DirectoryLiveGateView",
    "GateSweeper",
    "GrpcSceneManagerClient",
    "HolderReleasePendingError",
    "HostedResumeFacts",
    "HostedResumeVerdict",
    "HostedReturnRecord",
    "IndexedMemberAction",
    "KafkaGatePublisher",
    "LeaseClaim",
    "LeaseMonitor",
    "LiveViewGate",
    "LiveViewVerdict",
    "MemorySessionStore",
    "PLAYER_LOCATOR_NODE_TYPE",
    "PlayerLocatorConfig",
    "PlayerLocatorService",
    "RearmLeaseResult",
    "RedisSessionStore",
    "ReplaceSessionResult",
    "RoutingSceneManagerClient",
    "SCENE_MANAGER_NODE_TYPE",
    "SweepRoundReport",
    "classify_indexed_member",
    "decide_hosted_resume",
    "open_player_redis",
    "parse_hosted_return_record",
    "parse_scene_manager_endpoints",
    "resume_hosted_if_lost",
    "return_record_ttl_alignment",
    "run",
]
