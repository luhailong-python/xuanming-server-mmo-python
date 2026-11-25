"""match 与 team 服务业务错误码（与 Tip.xlsx 导表编号严格对齐）与异常定义。"""

from __future__ import annotations

from google.protobuf.message import Message

# -- 通用错误码 -------------------------------------------------------------------------------------

ERR_OK = 0
ERR_UNAVAILABLE = 1003
ERR_INVALID_PARAMETER = 1005

# -- 组队业务错误码（team_error 段，base = 4000）------------------------------------------------------

ERR_TEAM_NOT_IN_APPLICANTS = 4000
ERR_TEAM_PLAYER_ID = 4001
ERR_TEAM_MEMBERS_FULL = 4002
ERR_TEAM_MEMBER_IN_TEAM = 4003
ERR_TEAM_MEMBER_NOT_IN_TEAM = 4004
ERR_TEAM_KICK_SELF = 4005
ERR_TEAM_KICK_NOT_LEADER = 4006
ERR_TEAM_APPOINT_SELF = 4007
ERR_TEAM_APPOINT_NOT_LEADER = 4008
ERR_TEAM_FULL = 4009
ERR_TEAM_IN_APPLICANT_LIST = 4010
ERR_TEAM_APPLICATION_NOT_FOUND = 4011
ERR_TEAM_LIST_MAX_SIZE = 4012
ERR_TEAM_NO_TEAM = 4013
ERR_TEAM_DISBAND_NOT_LEADER = 4014
ERR_TEAM_MEMBER_LIST_FULL = 4015
ERR_TEAM_CREATE_MAX_SIZE = 4016
ERR_TEAM_TARGET_OFFLINE = 4017
ERR_TEAM_NOT_LEADER = 4018
ERR_TEAM_HOME_ZONE_UNKNOWN = 4019
ERR_TEAM_CROSS_ZONE_DENIED = 4020
ERR_TEAM_INVITE_NOT_FOUND = 4021
ERR_TEAM_INVITE_LIMIT = 4022
ERR_TEAM_IN_MATCH = 4023
ERR_TEAM_MEMBER_OFFLINE = 4024
ERR_TEAM_MEMBER_IN_BATTLE = 4025
ERR_TEAM_MEMBER_NOT_READY = 4026
ERR_TEAM_DUNGEON_NOT_OPEN = 4027
ERR_TEAM_SIZE_EXCEEDED = 4028
ERR_TEAM_STATE_CHANGED = 4029
ERR_TEAM_INTERNAL = 4030
ERR_TEAM_PVP_PARTY_SIZE = 4031
ERR_TEAM_RATING_GAP_TOO_LARGE = 4032
ERR_TEAM_RANKED_PRESET_UNCONFIRMED = 4033
ERR_TEAM_RANKED_PRESET_INVALID = 4034

# -- 匹配业务错误码（match_error 段，base = 16000）----------------------------------------------------

ERR_MATCH_IN_BATTLE = 16000
ERR_MATCH_ALREADY_QUEUED = 16001
ERR_MATCH_MODE_NOT_OPEN = 16002
ERR_MATCH_TEAM_SIZE_NOT_CONFIGURED = 16003
ERR_MATCH_INTERNAL = 16004
ERR_MATCH_TICKET_MISMATCH = 16005
ERR_MATCH_CANCEL_TOO_LATE = 16006
ERR_MATCH_CHALLENGE_SELF = 16007
ERR_MATCH_CHALLENGE_TARGET_OFFLINE = 16008
ERR_MATCH_CHALLENGE_TARGET_BUSY = 16009
ERR_MATCH_CHALLENGE_SELF_BUSY = 16010
ERR_MATCH_CHALLENGE_PENDING = 16011
ERR_MATCH_CHALLENGE_EXPIRED = 16012
ERR_MATCH_CHALLENGE_NOT_TARGET = 16013
ERR_MATCH_SPECTATE_WHILE_QUEUED = 16014
ERR_MATCH_SPECTATE_WHILE_IN_BATTLE = 16015
ERR_MATCH_ALREADY_WATCHING = 16016
ERR_MATCH_NO_WATCHABLE_BATTLE = 16017
ERR_MATCH_BATTLE_NOT_WATCHABLE = 16018
ERR_MATCH_SPECTATE_OFFLINE = 16019
ERR_MATCH_NOT_IN_SCENE = 16020
ERR_MATCH_SPECTATE_FULL = 16021
ERR_MATCH_PROBE_REQUIRED = 16022
ERR_MATCH_NO_REGION_WITHIN_LATENCY = 16023
ERR_MATCH_SPECTATE_TICKET_INVALID = 16024
ERR_MATCH_BATTLE_REPORT_NOT_FOUND = 16025
ERR_MATCH_BATTLE_SCRIPT_INVALID = 16026
ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND = 16027
ERR_MATCH_BATTLE_SCRIPT_LIMIT_REACHED = 16028
ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT = 16029
ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED = 16030
ERR_MATCH_BATTLE_SCRIPT_TOO_LARGE = 16031
ERR_MATCH_BATTLE_SCRIPT_LOADOUT_INVALID = 16032
ERR_MATCH_BATTLE_SCRIPT_LOADOUT_FULL = 16033
ERR_MATCH_RANKED_SEASON_SETTLING = 16034
ERR_MATCH_RANKED_PRESET_INVALID = 16035
ERR_MATCH_RANKED_RULE_VERSION_MISMATCH = 16036
ERR_MATCH_TOURNAMENT_NOT_OPEN = 16037
ERR_MATCH_TOURNAMENT_STAGE_CLOSED = 16038
ERR_MATCH_TOURNAMENT_SQUAD_NAME_INVALID = 16039
ERR_MATCH_TOURNAMENT_ALREADY_IN_SQUAD = 16040
ERR_MATCH_TOURNAMENT_SQUAD_NOT_FOUND = 16041
ERR_MATCH_TOURNAMENT_NOT_CAPTAIN = 16042
ERR_MATCH_TOURNAMENT_SQUAD_FULL = 16043
ERR_MATCH_TOURNAMENT_SQUAD_LOCKED = 16044
ERR_MATCH_TOURNAMENT_INVITE_NOT_FOUND = 16045
ERR_MATCH_TOURNAMENT_MEMBER_NOT_ENOUGH = 16046
ERR_MATCH_TOURNAMENT_LINEUP_INVALID = 16047
ERR_MATCH_TOURNAMENT_QUALIFIER_LIMIT = 16048
ERR_MATCH_TOURNAMENT_MATCH_NOT_READY = 16049
ERR_MATCH_TOURNAMENT_ALREADY_CHEERED = 16050
ERR_MATCH_TOURNAMENT_REWARD_UNAVAILABLE = 16051
ERR_MATCH_BATTLE_SURRENDER_NOT_ALLOWED = 16052
ERR_MATCH_BATTLE_SURRENDER_TOO_EARLY = 16053
ERR_MATCH_BATTLE_SURRENDER_COOLDOWN = 16054
ERR_MATCH_BATTLE_SURRENDER_ALREADY_VOTED = 16055


def set_tip(resp: Message, code: int, *params: object) -> Message:
    """在带 ``error_message`` 字段的响应上写入业务提示码与可选参数。"""
    if code:
        resp.error_message.id = int(code)
        for param in params:
            resp.error_message.parameters.append(str(param))
    return resp


# -- 领域异常 ----------------------------------------------------------------------------------------


class MatchError(Exception):
    """match / team 服务领域异常基类。"""


class StoreUnavailableError(MatchError):
    """权威存储暂不可用或发生瞬态冲突。"""


class OriginalRoleError(MatchError):
    """角色已冻结、已删除或原会话归属（account / owner_revision）不匹配。"""


class InvalidRoleFenceStateError(MatchError):
    """RoleFenceState 字段不合法。"""


class BattleOutcomeOriginalError(MatchError):
    """终局原件内容不合法或与已提交原件冲突（永久性拒绝，不可重试）。"""


class UnterminableOriginalError(MatchError):
    """CreateBattleRequest 原件落库后无法被终局裁决，拒绝持久。"""


class BattleNotAllocatableError(MatchError):
    """Battle 节点返回 battle_not_allocatable，确认房间未建。"""


class ActivityOutsideWindowError(MatchError):
    """瑞兽迎春等限时活动开局不在权威开放时间窗 [start, end) 内。"""


class ActivityIntentConflictError(MatchError):
    """同一活动开局请求 UUID 携带不一致的请求正文。"""


class PrepareRejectedError(MatchError):
    """Scene.PrepareBattle 明确以业务码拒绝该玩家（用于判定肇事者）。"""

    def __init__(self, player_id: int, code: int, detail: str = "") -> None:
        super().__init__(detail or f"prepare rejected player={player_id} code={code}")
        self.player_id = player_id
        self.code = code


class TeamBusinessError(MatchError):
    """组队业务规则拒绝，携带客户端提示码与可选参数。"""

    def __init__(self, code: int, *params: object) -> None:
        super().__init__(f"team error code={code}")
        self.code = code
        self.params = params


class MatchBusinessError(MatchError):
    """匹配业务规则拒绝，携带客户端提示码与可选参数。"""

    def __init__(self, code: int, *params: object) -> None:
        super().__init__(f"match error code={code}")
        self.code = code
        self.params = params


class ActivityOutsideWindowError(MatchError):
    """活动开局不在权威时钟左闭右开时间窗 [start, end) 内。"""


class ActivityIntentConflictError(MatchError):
    """同幂等键的活动开局请求载荷与已登记记录不一致。"""
