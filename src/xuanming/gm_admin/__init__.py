"""独立纯 Python GM 管理后台服务（完整移植自 Java ``gm_admin``）。"""

from .app import ALL_GM_PERMISSIONS, READY_STATE_KEY, create_gm_app
from .clients import (
    MAX_ITEM_GRANT_COUNT,
    MAX_PLAYER_LEVEL,
    PLAYER_OPERATIONS,
    PLAYER_OPERATIONS_BY_CODE,
    GamePlayerAdmin,
    GameServicesClient,
    PlayerOperationSpec,
    sign_player_command,
)
from .config import GmAdminConfig
from .journal import (
    PLAYER_DRAFT_TTL_MS,
    BaseGmJournal,
    CallFailure,
    CampaignPlan,
    GmHttpError,
    MailCampaignManifest,
    MemoryGmJournal,
    SqlGmJournal,
    open_pool,
)
from .node import run

__all__ = [
    "ALL_GM_PERMISSIONS",
    "BaseGmJournal",
    "CallFailure",
    "CampaignPlan",
    "GamePlayerAdmin",
    "GameServicesClient",
    "GmAdminConfig",
    "GmHttpError",
    "MAX_ITEM_GRANT_COUNT",
    "MAX_PLAYER_LEVEL",
    "MailCampaignManifest",
    "MemoryGmJournal",
    "PLAYER_DRAFT_TTL_MS",
    "PLAYER_OPERATIONS",
    "PLAYER_OPERATIONS_BY_CODE",
    "PlayerOperationSpec",
    "READY_STATE_KEY",
    "SqlGmJournal",
    "create_gm_app",
    "open_pool",
    "run",
    "sign_player_command",
]
