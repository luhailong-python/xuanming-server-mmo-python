"""match 与 team 权威存储抽象契约及确定性内存双胞胎（MemoryMatchStore）。

MemoryMatchStore 使用深拷贝快照事务语义（提交替换、异常整笔回滚），
与 SqlMatchStore 保持逐项一致的并发与状态机不变量，供单测和本地调试使用。
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Protocol as TypingProtocol, runtime_checkable

from google.protobuf.message import Message

from ..protocol import Protocol
from .battle_scripts import (
    REVIEW_REJECT_INVALID,
    REVIEW_REJECT_STALE,
    REVIEW_STATE_APPROVED,
    REVIEW_STATE_PENDING,
    REVIEW_STATE_REJECTED,
    SCRIPT_ORIGIN_PLAYER,
    SCRIPT_ORIGIN_UNSPECIFIED,
    BattleScriptReviewError,
)
from .errors import (
    ActivityIntentConflictError,
    ActivityOutsideWindowError,
    BattleOutcomeOriginalError,
    ERR_INVALID_PARAMETER,
    ERR_RATE_LIMIT_EXCEEDED,
    GuildWarMemberOccupiedError,
    ERR_MATCH_ALREADY_QUEUED,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_BATTLE_SCRIPT_LIMIT_REACHED,
    ERR_MATCH_BATTLE_SCRIPT_LOADOUT_FULL,
    ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED,
    ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND,
    ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT,
    ERR_MATCH_CANCEL_TOO_LATE,
    ERR_MATCH_CHALLENGE_EXPIRED,
    ERR_MATCH_CHALLENGE_NOT_TARGET,
    ERR_MATCH_CHALLENGE_PENDING,
    ERR_MATCH_CHALLENGE_SELF,
    ERR_MATCH_CHALLENGE_SELF_BUSY,
    ERR_MATCH_CHALLENGE_TARGET_BUSY,
    ERR_MATCH_IN_BATTLE,
    ERR_MATCH_MODE_NOT_OPEN,
    ERR_MATCH_SPECTATE_WHILE_QUEUED,
    ERR_MATCH_TICKET_MISMATCH,
    ERR_TEAM_APPLICATION_NOT_FOUND,
    ERR_TEAM_APPOINT_NOT_LEADER,
    ERR_TEAM_APPOINT_SELF,
    ERR_TEAM_CROSS_ZONE_DENIED,
    ERR_TEAM_DISBAND_NOT_LEADER,
    ERR_TEAM_IN_MATCH,
    ERR_TEAM_INVITE_LIMIT,
    ERR_TEAM_INVITE_NOT_FOUND,
    ERR_TEAM_KICK_NOT_LEADER,
    ERR_TEAM_KICK_SELF,
    ERR_TEAM_MEMBERS_FULL,
    ERR_TEAM_MEMBER_IN_TEAM,
    ERR_TEAM_MEMBER_NOT_IN_TEAM,
    ERR_TEAM_MEMBER_NOT_READY,
    ERR_TEAM_NOT_LEADER,
    ERR_TEAM_NO_TEAM,
    ERR_TEAM_PLAYER_ID,
    ERR_TEAM_SIZE_EXCEEDED,
    InvalidRoleFenceStateError,
    MatchBusinessError,
    OriginalRoleError,
    StoreUnavailableError,
    TeamBusinessError,
    UnterminableOriginalError,
)
from .model import (
    APPLICATION_TTL_MS,
    CHALLENGE_STATE_CLOSED,
    CHALLENGE_STATE_DECLINED,
    CHALLENGE_STATE_FORMED,
    CHALLENGE_STATE_PENDING,
    COMPLETION_SOURCE_BATTLE_RPC,
    COMPLETION_SOURCE_MANUAL_ABORT,
    DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID,
    DEFAULT_CUSTOM_DUEL_PET_PRESET_ID,
    FORMATION_STATE_ABORTED,
    FORMATION_STATE_CREATED,
    FORMATION_STATE_PERSISTED,
    FORMATION_STATE_PREPARING,
    FORMATION_STATE_UNKNOWN,
    INVITE_TTL_MS,
    MAX_APPLICATIONS_PER_TEAM,
    MAX_BATTLE_REPORTS,
    MAX_INVITES_PER_PLAYER,
    MAX_TEAM_SIZE,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    MODE_PVP_CHALLENGE,
    MODE_PVP_TEAM,
    OCCUPANCY_KIND_GUILD_TRIAL,
    OUTCOME_DECISION_ABORT,
    OUTCOME_DECISION_COMPLETE,
    OUTCOME_DECISION_UNKNOWN,
    RECALL_COOLDOWN_MS,
    ROLE_BLOCKER_OCCUPANCY,
    ROLE_BLOCKER_SETTLEMENT,
    ROLE_BLOCKER_SPECTATOR,
    ROLE_BLOCKER_TEAM,
    ROLE_BLOCKER_TEAM_LAUNCH,
    ROLE_BLOCKER_TICKET,
    SETTLEMENT_DISPATCH_APPLIED,
    SETTLEMENT_DISPATCH_PENDING,
    SPECTATOR_STATE_ACTIVE,
    SPECTATOR_STATE_CLOSED,
    SPECTATOR_STATE_PENDING_ADD,
    TEAM_IDLE_TTL_MS,
    TEAM_LAUNCH_CLOSED,
    TEAM_LAUNCH_CREATED,
    TEAM_LAUNCH_FORMING,
    TEAM_LAUNCH_QUEUED,
    TEAM_STATE_ACTIVE,
    TEAM_STATE_DISBANDED,
    TERMINAL_OUTCOME_GRACE_MS,
    TERMINAL_OUTCOME_NO_RETRY_MS,
    TICKET_STATE_DONE,
    TICKET_STATE_FORMING,
    TICKET_STATE_QUEUED,
    TICKET_STATE_READY,
    VALID_TEAM_FOLLOW_FORMATIONS,
    CallerIdentity,
    RoleFenceRow,
    build_battle_report_summary,
    classify_stranded_formation,
    completable_original,
    effective_deadline_ms,
    formation_digest,
    formation_outcome_decided,
    is_valid_uuid,
    pair_to_uuid,
    preferred_region,
    uuid_to_pair,
    valid_role_fence_state,
    validate_terminal_against_formation,
    validate_terminal_payload,
)
from .ranked import (
    RANKED_SEASON_ACTIVE,
    RankedFeaturedBuildRow,
    RankedPlayerPrefsRow,
    RankedPlayerSeasonRow,
    RankedRewardRow,
    RankedRivalryRow,
    RankedSeasonRow,
    is_rivalry_revenge_target,
)
from .rating import (
    DEFAULT_RATING_CENTI,
    RANK_SOURCE_SHARDS,
    RANK_SUBMIT_SET,
    RATING_OUTCOME_APPLIED,
    RATING_OUTCOME_DUPLICATE,
    RankSourceRow,
    RatingApplyResult,
    RatingChange,
    compute_elo_delta,
    is_rated_mode,
    rank_source_shard,
)


# -- 存储层结构化返回值（不可变）--------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TeamMutationOutcome:
    """队伍写操作的权威结果，供服务层构造响应并触发异步推送与场景刷新。"""

    team_id: int
    version: int
    record: Message | None
    epochs: dict[int, int]
    reason: int
    actor_id: int
    now_ms: int
    joined: tuple[int, ...] = ()
    kept: tuple[int, ...] = ()
    left: tuple[int, ...] = ()
    leader_changed: bool = False
    formation_changed: bool = False
    invited_player: int = 0
    rejected_applicant: int = 0
    revoked_invitees: tuple[int, ...] = ()
    recall_recipients: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class TeamSnapshotResult:
    """玩家视角的队伍快照读结果。"""

    team_id: int
    membership_epoch: int
    version: int
    record: Message | None
    now_ms: int
    launch_state: int = 0
    launch_battle_id: int = 0
    launch_mode: int = 0
    launch_config_id: int = 0
    launch_target_size: int = 0
    launch_enqueued_ms: int = 0


@dataclass(frozen=True, slots=True)
class IncomingInviteInfo:
    team_id: int
    inviter_id: int
    leader_id: int
    member_count: int
    zone_id: int
    expire_at_ms: int


@dataclass(frozen=True, slots=True)
class CompletionOutboxRow:
    battle_id: int
    raw_payload: bytes
    payload_digest: bytes
    next_retry_ms: int


@dataclass(frozen=True, slots=True)
class SettlementOutboxRow:
    battle_id: int
    player_id: int
    settlement_proto: bytes
    payload_digest: bytes
    owner_account: str
    owner_revision: int
    issued_at_ms: int
    inbox_seq: int
    inbox_state: int
    dispatch_state: int
    next_retry_ms: int


@dataclass(frozen=True, slots=True)
class QueueStatusInfo:
    ticket: Message | None
    request_cancelled: bool
    now_ms: int


@dataclass(frozen=True, slots=True)
class StrandedFormationEntry:
    formation: Message
    formation_class: int
    failed_check: str
    create_req: Message | None
    effective_deadline_ms: int
    has_terminal_row: bool
    terminal_decision: int
    terminal_source_kind: int
    digest: str
    refusal: str
    tickets: tuple[Message, ...] = ()


# -- 抽象存储协议 ------------------------------------------------------------------------------------


@runtime_checkable
class MatchStore(TypingProtocol):
    """match 与 team 服务的统一持久化端口，由 MemoryMatchStore 与 SqlMatchStore 共同实现。"""

    async def now_ms(self) -> int: ...
    async def next_id(self) -> int: ...

    # RoleFence
    async def get_role_fence(self, player_id: int) -> RoleFenceRow | None: ...
    async def verify_original_owner(self, player_id: int, account: str, owner_revision: int) -> None: ...
    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message: ...
    async def apply_role_fence(self, state: Message) -> Message: ...
    async def purge_removed_role(self, state: Message) -> Message: ...

    # Team CRUD & TeamAuthority
    async def create_team(self, caller: CallerIdentity, zone_id: int, request_id: str) -> TeamMutationOutcome: ...
    async def get_my_team(
        self, player_id: int, *, match_request_id: str = "", caller: CallerIdentity | None = None
    ) -> TeamSnapshotResult: ...
    async def read_membership(
        self, player_id: int, *, owner_revision: int = 0
    ) -> tuple[int, int, int, Message | None]: ...
    async def apply_join_team(
        self, caller: CallerIdentity, target_player_id: int, zone_id: int, request_id: str, *, allow_cross_zone: bool
    ) -> TeamMutationOutcome: ...
    async def handle_application(
        self, caller: CallerIdentity, applicant_id: int, approve: bool, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome: ...
    async def invite_to_team(
        self,
        caller: CallerIdentity,
        target_player_id: int,
        target_zone_id: int,
        expected_team_id: int,
        request_id: str,
        *,
        allow_cross_zone: bool,
    ) -> TeamMutationOutcome: ...
    async def respond_invite(
        self, caller: CallerIdentity, team_id: int, accept: bool, zone_id: int, request_id: str
    ) -> TeamMutationOutcome: ...
    async def list_my_invites(self, player_id: int) -> tuple[list[IncomingInviteInfo], int]: ...
    async def leave_team(self, caller: CallerIdentity, expected_team_id: int, request_id: str) -> TeamMutationOutcome: ...
    async def kick_member(
        self, caller: CallerIdentity, target_player_id: int, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome: ...
    async def transfer_leader(
        self, caller: CallerIdentity, target_player_id: int, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome: ...
    async def disband_team(
        self, caller: CallerIdentity, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome: ...
    async def confirm_ranked_preset(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        char_preset: int,
        pet_preset: int,
        rule_version: int,
        request_id: str,
    ) -> TeamMutationOutcome: ...
    async def set_team_formation(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        formation: int,
        request_id: str,
    ) -> TeamMutationOutcome: ...
    async def recall_team_members(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        request_id: str,
    ) -> TeamMutationOutcome: ...
    async def start_team_match(
        self,
        caller: CallerIdentity,
        *,
        expected_team_id: int,
        battle_config_id: int,
        mode: int,
        target_team_size: int,
        request_id: str,
        member_zones: dict[int, int],
        immediate_battle_node: tuple[int, str, str, int, str] | None,
        battle_deadline_ms: int,
        queue_ttl_ms: int,
    ) -> tuple[TeamMutationOutcome, Message | None, Message]: ...

    # Network probe
    async def save_network_plan(self, player_id: int, profile: Message, expires_ms: int) -> None: ...
    async def commit_network_profile(
        self, player_id: int, plan_id: str, binding: str, receipts: list[Message], expires_ms: int
    ) -> None: ...
    async def get_network_profile(self, player_id: int) -> tuple[Message | None, int]: ...

    # Queue & Challenge & Formation
    async def join_queue(
        self,
        caller: CallerIdentity,
        *,
        mode: int,
        config_id: int,
        team_size: int,
        zone_id: int,
        request_id: str,
        request_payload: bytes,
        network_profile: bytes,
        pool_region: int,
        queue_ttl_ms: int,
        is_wuxing_tower: bool = False,
    ) -> Message: ...
    async def cancel_queue(self, caller: CallerIdentity, queue_ticket: str, request_id: str) -> None: ...
    async def get_queue_status(self, caller: CallerIdentity, request_id: str = "") -> QueueStatusInfo: ...
    async def list_queued_tickets(self, *, limit: int = 200) -> list[Message]: ...
    async def list_queued_team_launches(self, *, limit: int = 100) -> list[Message]: ...

    async def create_challenge(
        self,
        challenger: CallerIdentity,
        target_player_id: int,
        config_id: int,
        ttl_ms: int,
        *,
        custom_rules_payload: bytes = b"",
    ) -> Message: ...
    async def respond_challenge(
        self,
        responder: CallerIdentity,
        challenge_id: int,
        accept: bool,
        *,
        battle_node: tuple[int, str, str, int, str] | None,
        battle_deadline_ms: int,
        responder_character_preset_id: int = 0,
        responder_pet_preset_id: int = 0,
    ) -> tuple[Message, Message | None]: ...

    async def claim_formation(
        self,
        *,
        mode: int,
        config_id: int,
        members: list[tuple[int, str, int, bool]],
        physical_region: int,
        target_node_id: int,
        target_instance_id: str,
        target_endpoint: str,
        release_track: str,
        battle_deadline_ms: int,
        requeue_on_failure: bool,
        activity_context: bytes = b"",
        team_launch_keys: list[tuple[int, int, int]] | None = None,
        tournament_context: bytes = b"",
        custom_duel_rules: bytes = b"",
        is_wuxing_tower: bool = False,
        ranked_context: bytes = b"",
        ranked_season_id: int = 0,
    ) -> Message: ...
    async def mark_formation_prepared(self, battle_id: int, player_id: int, endpoint: str) -> None: ...
    async def save_formation_intent(
        self, battle_id: int, create_request_bytes: bytes, preflight_instance_id: str
    ) -> Message: ...
    async def mark_formation_created(self, battle_id: int, adopted_version: int) -> Message: ...
    async def mark_formation_unknown(self, battle_id: int) -> None: ...
    async def abort_formation_pre_persist(self, battle_id: int, culprit_player_id: int = 0) -> None: ...
    async def get_formation(self, battle_id: int) -> Message | None: ...
    async def list_active_formations(
        self, *, release_track: str = "stable", limit: int = 50
    ) -> list[tuple[Message, bool]]: ...
    async def set_formation_featured(self, battle_id: int, featured: bool) -> bool: ...

    # Trial Occupancy & Activity Battles
    async def acquire_trial_occupancy(
        self, owner_id: int, roster: list[tuple[int, str, int]], ttl_ms: int
    ) -> tuple[int, int]: ...
    async def release_trial_occupancy(self, owner_id: int, player_ids: list[int]) -> None: ...
    async def start_activity_battle(
        self,
        *,
        request_id: int,
        battle_config_id: int,
        member_player_ids: list[int],
        activity_context_bytes: bytes,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]: ...
    async def start_festival_beast_battle(
        self,
        *,
        owner_id: int,
        request_high: int,
        request_low: int,
        payload_bytes: bytes,
        battle_config_id: int,
        activity_context_bytes: bytes,
        effective_start_at_ms: int,
        effective_end_at_ms: int,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]: ...
    async def start_guild_war_battle(
        self,
        *,
        request_id: int,
        battle_config_id: int,
        match_mode: int,
        attacker_player_ids: list[int],
        defender_player_ids: list[int],
        activity_context_bytes: bytes,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]: ...
    async def active_battle_id_of(self, player_id: int) -> int: ...
    async def cancel_activity_battle(self, request_id: int) -> tuple[int, bool, int, int]: ...

    # Battle Outcome, Terminal Decision & Outbox
    async def commit_battle_outcome(self, req: Message) -> int: ...
    async def commit_battle_abort(
        self, battle_id: int, source_kind: int, *, manual_audit: dict | None = None
    ) -> int: ...
    async def get_battle_completion(self, battle_id: int) -> tuple[int, bytes]: ...
    async def get_battle_terminal_state(self, battle_id: int) -> tuple[int, bool]: ...

    async def due_outcome_results(self, stamp_ms: int, limit: int) -> list[CompletionOutboxRow]: ...
    async def claim_outcome_result(self, battle_id: int, prev_retry_ms: int, lease_ms: int) -> bool: ...
    async def mark_outcome_result_published(self, battle_id: int, lease_ms: int) -> None: ...

    async def due_settlement_sources(
        self, stamp_ms: int, first_limit: int, waiting_limit: int
    ) -> list[SettlementOutboxRow]: ...
    async def claim_settlement_source(
        self, battle_id: int, player_id: int, prev_retry_ms: int, lease_ms: int
    ) -> bool: ...
    async def update_settlement_source(
        self,
        battle_id: int,
        player_id: int,
        lease_ms: int,
        *,
        inbox_seq: int,
        inbox_state: int,
        dispatch_state: int,
        next_retry_ms: int,
    ) -> None: ...
    async def release_settled_tickets(self, battle_id: int, player_id: int) -> bool: ...

    # Battle Reports
    async def list_battle_reports(self, player_id: int) -> list[Message]: ...
    async def get_battle_report(self, player_id: int, battle_id: int, *, allow_public: bool = False) -> Message: ...

    # Spectator
    async def get_spectator(self, player_id: int) -> Message | None: ...
    async def prepare_spectator(
        self,
        caller: CallerIdentity,
        battle_id: int,
        operation_id: str,
        target_endpoint: str,
        target_instance_id: str,
        request_payload: bytes,
        add_request: bytes,
        request_id: str,
    ) -> Message: ...
    async def commit_spectator(self, player_id: int, operation_id: str) -> None: ...
    async def close_spectator(self, player_id: int, operation_id: str) -> None: ...

    # Stranded Formations
    async def list_stranded_formations(self, after_battle_id: int, limit: int) -> tuple[list[StrandedFormationEntry], int, int]: ...
    async def inspect_stranded_formation(self, battle_id: int) -> tuple[StrandedFormationEntry | None, int]: ...

    # Battle Scripts
    async def list_battle_scripts(self, player_id: int) -> list[Message]: ...
    async def list_battle_script_loadout(self, player_id: int) -> list[Message]: ...
    async def get_approved_battle_script(self, script_id: int, owner_player_id: int) -> tuple[int, bytes]: ...
    async def submit_battle_script(
        self,
        caller: CallerIdentity,
        *,
        script_id: int,
        expected_revision: int,
        content: bytes,
        max_scripts: int,
    ) -> Message: ...
    async def delete_battle_script(self, caller: CallerIdentity, script_id: int, expected_revision: int) -> None: ...
    async def set_battle_script_loadout(
        self,
        caller: CallerIdentity,
        *,
        match_mode: int,
        battle_config_id: int,
        origin: int,
        official_script_id: int,
        player_script_id: int,
        max_entries: int,
    ) -> list[Message]: ...
    async def list_pending_battle_scripts(self, limit: int) -> list[Message]: ...
    async def review_battle_script(
        self,
        *,
        script_id: int,
        revision: int,
        approve: bool,
        reviewer: str,
        reason: str,
        validate: Callable[[bytes], None] | None = None,
    ) -> None: ...

    # Elo Rating & Rank Outbox
    async def get_rating(self, player_id: int, mode: int) -> tuple[int, int, int]: ...
    async def apply_rating(
        self,
        *,
        battle_id: int,
        payload_digest: bytes,
        mode: int,
        team_a: list[int],
        team_b: list[int],
        score_a: float,
        source_epoch: int = 1,
        generation: int = 1,
        eligible_for_rank: set[int] | None = None,
    ) -> RatingApplyResult: ...
    async def stop_rank_source(self, source_epoch: int, mode: int) -> int: ...
    async def list_rank_sources(self, *, source_epoch: int | None = None) -> list[RankSourceRow]: ...
    async def bind_rank_board(self, source_epoch: int, mode: int, board_id: int) -> None: ...
    async def get_rank_outbox_entry(self, source_epoch: int, mode: int, shard: int, source_cursor: int) -> bytes | None: ...
    async def advance_rank_committed_cursor(self, source_epoch: int, mode: int, shard: int, committed_cursor: int) -> None: ...
    async def list_rank_removals(self, *, limit: int = 16) -> list[tuple[int, int]]: ...
    async def delete_rank_removal(self, mode: int, player_id: int) -> None: ...

    # Ranked Play V1
    async def get_active_ranked_season(self) -> RankedSeasonRow | None: ...
    async def upsert_ranked_season(self, season: RankedSeasonRow) -> RankedSeasonRow: ...
    async def list_ranked_seasons(self) -> list[RankedSeasonRow]: ...
    async def get_ranked_player_prefs(self, player_id: int) -> RankedPlayerPrefsRow | None: ...
    async def save_ranked_player_prefs(
        self,
        caller: CallerIdentity,
        *,
        char_preset_id: int,
        pet_preset_id: int,
        rule_version: int,
        cosmetic_loadout: bytes,
    ) -> RankedPlayerPrefsRow: ...
    async def get_ranked_player_season(self, season_id: int, player_id: int) -> RankedPlayerSeasonRow | None: ...
    async def list_ranked_player_seasons(self, player_id: int) -> list[RankedPlayerSeasonRow]: ...
    async def list_ranked_rewards(self, player_id: int) -> list[RankedRewardRow]: ...
    async def save_ranked_battle_settlement(
        self,
        *,
        season_id: int,
        battle_id: int,
        player_id: int,
        summary_template: Message,
        rating_centi: int,
        tier_after: int,
        outcome_kind: int,
        participated: bool,
        settled_at_ms: int,
        milestones: tuple[tuple[int, int, int, str], ...],
        win_milestone: tuple[int, int, int, str] | None = None,
    ) -> tuple[RankedPlayerSeasonRow, list[RankedRewardRow], bytes]: ...
    async def get_ranked_battle_result(self, player_id: int, battle_id: int = 0) -> bytes | None: ...
    async def get_ranked_rivalry(
        self, season_id: int, player_id: int, opponent_id: int,
    ) -> RankedRivalryRow | None: ...
    async def upsert_ranked_rivalry(self, row: RankedRivalryRow) -> RankedRivalryRow: ...
    async def list_ranked_top_rivals(
        self, season_id: int, player_id: int, *, limit: int = 8,
    ) -> list[RankedRivalryRow]: ...
    async def load_rivalry_encounters(
        self, season_id: int, members: list[tuple[int, int]],
    ) -> list[Message]: ...
    async def upsert_ranked_featured_build(
        self,
        *,
        season_id: int,
        build_key: str,
        season_seal_id: int,
        starter_pet_table_id: int,
        core_skill_ids_csv: str,
        tier_qingfeng_centi: int,
        battle_id: int,
        player_id: int,
        player_name: str,
        after_rating_centi: int,
        team_size: int,
        total_rounds: int,
        player_won: bool,
        settled_at_ms: int,
    ) -> RankedFeaturedBuildRow: ...
    async def list_ranked_featured_builds(
        self, season_id: int, *, limit: int = 5,
    ) -> list[RankedFeaturedBuildRow]: ...
    async def is_featured_ranked_replay_allowed(self, battle_id: int) -> bool: ...


# -- 内存快照状态容器 --------------------------------------------------------------------------------


@dataclass
class _MemTeamRow:
    team_id: int
    version: int
    data: Message  # teampb.TeamAuthorityData
    state: int
    idle_expires_ms: int


@dataclass
class _MemMembershipRow:
    player_id: int
    team_id: int
    epoch: int


@dataclass
class _MemCompletionRow:
    battle_id: int
    raw_payload: bytes
    payload_digest: bytes
    format_version: int
    archived_ms: int
    decision: int
    source_kind: int
    placement_operation_id: str
    target_instance_id: str
    outcome_protocol_version: int
    result_published: bool
    next_retry_ms: int


@dataclass
class _MemSettlementRow:
    battle_id: int
    player_id: int
    settlement_proto: bytes
    payload_digest: bytes
    archived_ms: int
    owner_account: str
    owner_revision: int
    issued_at_ms: int
    inbox_seq: int
    inbox_state: int
    dispatch_state: int
    next_retry_ms: int


@dataclass
class _MemOccupancyRow:
    player_id: int
    owner_kind: int
    owner_id: int
    account: str
    owner_revision: int
    expires_ms: int


@dataclass
class _MemJoinIntentRow:
    player_id: int
    request_high: int
    request_low: int
    request_id: str
    ticket_record: bytes
    cancelled: bool
    owner_account: str
    owner_revision: int
    request_payload: bytes
    state: int


@dataclass
class _MemReportRow:
    battle_id: int
    created_ms: int
    match_mode: int
    battle_config_id: int
    envelope: bytes
    envelope_digest: bytes
    ref_count: int


@dataclass
class _MemState:
    role_fences: dict[int, RoleFenceRow] = field(default_factory=dict)
    teams: dict[int, _MemTeamRow] = field(default_factory=dict)
    memberships: dict[int, _MemMembershipRow] = field(default_factory=dict)
    invite_index: dict[tuple[int, int], int] = field(default_factory=dict)  # (invitee, team_id) -> expires_ms
    app_index: dict[tuple[int, int], int] = field(default_factory=dict)     # (applicant, team_id) -> expires_ms
    action_receipts: dict[tuple[int, int, int], tuple[int, bytes, str, int, TeamMutationOutcome]] = field(
        default_factory=dict
    )
    team_launches: dict[tuple[int, int, int], Message] = field(default_factory=dict)
    team_launch_roles: dict[tuple[int, int, int, int], int] = field(default_factory=dict)

    network_profiles: dict[int, tuple[bytes, int]] = field(default_factory=dict)
    tickets: dict[int, Message] = field(default_factory=dict)
    join_intents: dict[tuple[int, int, int], _MemJoinIntentRow] = field(default_factory=dict)
    occupancies: dict[int, _MemOccupancyRow] = field(default_factory=dict)
    challenges: dict[int, Message] = field(default_factory=dict)
    formations: dict[int, Message] = field(default_factory=dict)
    featured_battles: set[int] = field(default_factory=set)
    activity_intents: dict[int, tuple[int, bool]] = field(default_factory=dict)  # request_id -> (battle_id, cancelled)
    direct_intents: dict[tuple[int, int, int, int], tuple[int, bytes, bool]] = field(
        default_factory=dict
    )  # (kind, owner_id, req_high, req_low) -> (battle_id, payload, cancelled)

    completions: dict[int, _MemCompletionRow] = field(default_factory=dict)
    settlements: dict[tuple[int, int], _MemSettlementRow] = field(default_factory=dict)
    spectators: dict[int, Message] = field(default_factory=dict)
    reports: dict[int, _MemReportRow] = field(default_factory=dict)
    player_reports: dict[int, list[tuple[int, int, bytes]]] = field(default_factory=dict)  # pid -> [(created, bid, summary)]
    manual_resolutions: dict[int, dict] = field(default_factory=dict)

    # Battle Scripts
    battle_scripts: dict[int, Message] = field(default_factory=dict)  # script_id -> MatchBattleScriptRecord
    battle_script_reviews: dict[tuple[int, int], Message] = field(default_factory=dict)  # (script_id, rev) -> review
    battle_script_loadouts: dict[tuple[int, int, int], Message] = field(default_factory=dict)  # (pid, mode, cfg) -> loadout

    # Elo Rating & Rank Outbox
    ratings: dict[tuple[int, int], tuple[int, int, int, int]] = field(default_factory=dict)  # (pid, mode) -> (rating_centi, games, updated_ms, score_changed_ms)
    rating_applied: dict[int, tuple[bytes, int]] = field(default_factory=dict)  # battle_id -> (digest, applied_ms)
    rank_sources: dict[tuple[int, int, int], RankSourceRow] = field(default_factory=dict)  # (epoch, mode, shard)
    rank_outbox: dict[tuple[int, int, int, int], tuple[int, bytes]] = field(default_factory=dict)  # (epoch, mode, shard, cursor) -> (req_id, entries)
    rank_removals: dict[tuple[int, int], int] = field(default_factory=dict)  # (mode, pid) -> created_ms

    # Ranked Play V1
    ranked_seasons: dict[int, RankedSeasonRow] = field(default_factory=dict)  # season_id -> row
    ranked_prefs: dict[int, RankedPlayerPrefsRow] = field(default_factory=dict)  # pid -> row
    ranked_player_seasons: dict[tuple[int, int], RankedPlayerSeasonRow] = field(default_factory=dict)  # (season_id, pid)
    ranked_rewards: dict[tuple[int, int, int], RankedRewardRow] = field(default_factory=dict)  # (pid, season_id, reward_kind)
    ranked_battles: dict[tuple[int, int], tuple[int, int, bytes]] = field(default_factory=dict)  # (bid, pid) -> (season_id, settled_ms, summary)
    ranked_rivalries: dict[tuple[int, int, int], RankedRivalryRow] = field(default_factory=dict)  # (season_id, pid, opp_id)
    ranked_featured_builds: dict[tuple[int, str], RankedFeaturedBuildRow] = field(default_factory=dict)  # (season_id, build_key)

    # Guild War V2
    guild_war_intents: dict[int, int] = field(default_factory=dict)  # request_id -> battle_id

    # Wuxing Tower V1 (记录单人排队票是否来自五行幻境爬塔)
    tower_tickets: set[int] = field(default_factory=set)


class MemoryMatchStore:
    """确定性内存实现，与 SqlMatchStore 保持语义一致。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        clock_ms: Callable[[], int] | None = None,
        initial_id: int = 1000,
    ) -> None:
        self.protocol = protocol
        self._clock = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._next_id = initial_id
        self._lock = asyncio.Lock()
        self._state = _MemState()

    async def now_ms(self) -> int:
        return int(self._clock())

    async def next_id(self) -> int:
        async with self._lock:
            self._next_id += 1
            return self._next_id

    def _alloc_id_locked(self) -> int:
        self._next_id += 1
        return self._next_id

    async def _run_tx(self, fn):
        async with self._lock:
            snap = copy.deepcopy(self._state)
            try:
                result = fn(snap)
            except BaseException:
                raise
            self._state = snap
            return result

    # -- RoleFence ------------------------------------------------------------------------------------

    def _check_fence_locked(self, s: _MemState, player_id: int, account: str, owner_revision: int) -> None:
        fence = s.role_fences.get(player_id)
        if fence is None:
            return
        if fence.account == "" or fence.freeze_token != 0 or fence.freeze_final:
            raise OriginalRoleError(f"role frozen or deleted player={player_id}")
        if account and fence.account != account:
            raise OriginalRoleError(f"role account mismatch player={player_id}")
        if owner_revision and fence.owner_revision != owner_revision:
            raise OriginalRoleError(f"role owner_revision mismatch player={player_id}")

    async def get_role_fence(self, player_id: int) -> RoleFenceRow | None:
        async with self._lock:
            return self._state.role_fences.get(player_id)

    async def verify_original_owner(self, player_id: int, account: str, owner_revision: int) -> None:
        async with self._lock:
            self._check_fence_locked(self._state, player_id, account, owner_revision)

    def _pending_blocker_locked(self, s: _MemState, player_id: int, now: int) -> str:
        ticket = s.tickets.get(player_id)
        if ticket is not None and int(ticket.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
            return ROLE_BLOCKER_TICKET
        mem = s.memberships.get(player_id)
        if mem is not None and mem.team_id != 0:
            return ROLE_BLOCKER_TEAM
        for (a, h, l, pid), st in s.team_launch_roles.items():
            if pid == player_id and st != TEAM_LAUNCH_CLOSED:
                return ROLE_BLOCKER_TEAM_LAUNCH
        spec = s.spectators.get(player_id)
        if spec is not None and int(spec.state) != SPECTATOR_STATE_CLOSED:
            return ROLE_BLOCKER_SPECTATOR
        occ = s.occupancies.get(player_id)
        if occ is not None and occ.expires_ms > now:
            return ROLE_BLOCKER_OCCUPANCY
        for (_, pid), row in s.settlements.items():
            if pid == player_id and row.dispatch_state == SETTLEMENT_DISPATCH_PENDING:
                return ROLE_BLOCKER_SETTLEMENT
        return ""

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message:
        if not valid_role_fence_state(state) or int(state.freeze_token) == 0 or bool(state.freeze_final):
            raise InvalidRoleFenceStateError("invalid RoleFenceState for PrepareRoleFreeze")
        now = await self.now_ms()
        pid = int(state.player_id)

        async with self._lock:
            if dry_run:
                blocker = self._pending_blocker_locked(self._state, pid, now)
                return self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=(blocker == ""), blocker=blocker)
            snap = copy.deepcopy(self._state)
            existing = snap.role_fences.get(pid)
            if existing is None or int(state.version) > existing.version:
                snap.role_fences[pid] = RoleFenceRow(
                    player_id=pid,
                    account=state.account,
                    owner_revision=int(state.owner_revision),
                    freeze_token=int(state.freeze_token),
                    freeze_final=bool(state.freeze_final),
                    version=int(state.version),
                    home_zone=int(state.home_zone),
                    updated_ms=now,
                )
            fence = snap.role_fences[pid]
            if fence.version != int(state.version) or fence.freeze_token != int(state.freeze_token):
                return self.protocol.message("rolefence.PrepareRoleFreezeResponse", stale=True)
            blocker = self._pending_blocker_locked(snap, pid, now)
            if blocker:
                return self.protocol.message("rolefence.PrepareRoleFreezeResponse", blocker=blocker)
            self._state = snap
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)

    async def apply_role_fence(self, state: Message) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid RoleFenceState for ApplyRoleFence")
        now = await self.now_ms()
        pid = int(state.player_id)

        def work(s: _MemState):
            existing = s.role_fences.get(pid)
            if existing is None or int(state.version) > existing.version:
                s.role_fences[pid] = RoleFenceRow(
                    player_id=pid,
                    account=state.account,
                    owner_revision=int(state.owner_revision),
                    freeze_token=int(state.freeze_token),
                    freeze_final=bool(state.freeze_final),
                    version=int(state.version),
                    home_zone=int(state.home_zone),
                    updated_ms=now,
                )
            return self.protocol.message("rolefence.ApplyRoleFenceResponse")

        return await self._run_tx(work)

    async def purge_removed_role(self, state: Message) -> Message:
        if not valid_role_fence_state(state) or state.account != "" or int(state.freeze_token) != 0:
            raise InvalidRoleFenceStateError("PurgeRemovedRole requires tombstone state")
        now = await self.now_ms()
        pid = int(state.player_id)

        def work(s: _MemState):
            existing = s.role_fences.get(pid)
            if existing is None or int(state.version) > existing.version:
                s.role_fences[pid] = RoleFenceRow(
                    player_id=pid,
                    account="",
                    owner_revision=int(state.owner_revision),
                    freeze_token=0,
                    freeze_final=bool(state.freeze_final),
                    version=int(state.version),
                    home_zone=int(state.home_zone),
                    updated_ms=now,
                )
            fence = s.role_fences[pid]
            if fence.account != "":
                raise StoreUnavailableError("role_fence is not tombstone")
            s.network_profiles.pop(pid, None)
            s.tickets.pop(pid, None)
            s.occupancies.pop(pid, None)
            s.spectators.pop(pid, None)
            s.player_reports.pop(pid, None)
            for k in [k for k in s.invite_index if k[0] == pid]:
                s.invite_index.pop(k, None)
            for k in [k for k in s.app_index if k[0] == pid]:
                s.app_index.pop(k, None)
            for sid in [sid for sid, rec in s.battle_scripts.items() if int(rec.owner_player_id) == pid]:
                s.battle_scripts.pop(sid, None)
            for k in [k for k in s.battle_script_loadouts if k[0] == pid]:
                s.battle_script_loadouts.pop(k, None)
            for k in [k for k in s.ratings if k[0] == pid]:
                s.ratings.pop(k, None)
                s.rank_removals[(k[1], pid)] = now
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse")

        return await self._run_tx(work)

    # -- Team Helpers & CRUD --------------------------------------------------------------------------

    def _bump_membership(self, s: _MemState, player_id: int, team_id: int) -> int:
        prev = s.memberships.get(player_id)
        epoch = (prev.epoch if prev is not None else 0) + 1
        s.memberships[player_id] = _MemMembershipRow(player_id=player_id, team_id=team_id, epoch=epoch)
        return epoch

    def _prune_team_pending(self, s: _MemState, team_row: _MemTeamRow, now: int) -> None:
        rec = team_row.data.record
        keep_apps = [a for a in rec.applications if int(a.expire_at_ms) > now]
        if len(keep_apps) != len(rec.applications):
            expired_ids = {int(a.player_id) for a in rec.applications if int(a.expire_at_ms) <= now}
            del rec.applications[:]
            rec.applications.extend(keep_apps)
            for pid in expired_ids:
                s.app_index.pop((pid, team_row.team_id), None)
        keep_invs = [inv for inv in rec.invites if int(inv.expire_at_ms) > now]
        if len(keep_invs) != len(rec.invites):
            expired_ids = {int(inv.invitee_id) for inv in rec.invites if int(inv.expire_at_ms) <= now}
            del rec.invites[:]
            rec.invites.extend(keep_invs)
            for pid in expired_ids:
                s.invite_index.pop((pid, team_row.team_id), None)

    def _epochs_for(self, s: _MemState, player_ids: list[int]) -> dict[int, int]:
        out: dict[int, int] = {}
        for pid in player_ids:
            m = s.memberships.get(pid)
            out[pid] = m.epoch if m is not None else 0
        return out

    def _team_locked(self, rec: Message, now: int) -> bool:
        if not rec.match_lock_token:
            return False
        exp = int(rec.match_lock_expire_at_ms)
        return exp == 0 or exp > now

    def _clone_msg(self, msg: Message | None) -> Message | None:
        if msg is None:
            return None
        cloned = type(msg)()
        cloned.CopyFrom(msg)
        return cloned

    def _check_receipt(
        self, s: _MemState, actor_id: int, request_id: str, method: int, payload: bytes, caller: CallerIdentity
    ) -> TeamMutationOutcome | None:
        if not is_valid_uuid(request_id):
            return None
        h, l = uuid_to_pair(request_id)
        row = s.action_receipts.get((actor_id, h, l))
        if row is None:
            return None
        r_method, r_payload, r_acct, r_rev, r_outcome = row
        if r_method != method or r_payload != payload or r_acct != caller.account or r_rev != caller.owner_revision:
            raise OriginalRoleError("team action receipt mismatch")
        return r_outcome

    def _save_receipt(
        self, s: _MemState, actor_id: int, request_id: str, method: int, payload: bytes, caller: CallerIdentity,
        outcome: TeamMutationOutcome,
    ) -> None:
        if not is_valid_uuid(request_id):
            return
        h, l = uuid_to_pair(request_id)
        s.action_receipts[(actor_id, h, l)] = (method, payload, caller.account, caller.owner_revision, outcome)

    def _cancel_queued_team_launch(self, s: _MemState, team_id: int) -> None:
        for key, launch in s.team_launches.items():
            if int(launch.team_id) == team_id and int(launch.state) == TEAM_LAUNCH_QUEUED:
                launch.state = TEAM_LAUNCH_CLOSED
                for (a, h, l, pid), _ in list(s.team_launch_roles.items()):
                    if (a, h, l) == key:
                        s.team_launch_roles[(a, h, l, pid)] = TEAM_LAUNCH_CLOSED
                        tk = s.tickets.get(pid)
                        if tk is not None and int(tk.state) == TICKET_STATE_QUEUED and int(tk.team_id) == team_id:
                            tk.state = TICKET_STATE_DONE

    async def create_team(self, caller: CallerIdentity, zone_id: int, request_id: str) -> TeamMutationOutcome:
        now = await self.now_ms()
        payload = f"create:{zone_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 1, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is not None and mem.team_id != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            team_id = self._alloc_id_locked()
            data = self.protocol.message("teampb.TeamAuthorityData")
            rec = data.record
            rec.team_id = team_id
            rec.leader_id = caller.player_id
            rec.zone_id = zone_id
            rec.created_at_ms = now
            rec.next_join_seq = 2
            rec.members.add(player_id=caller.player_id, zone_id=zone_id, joined_at_ms=now, join_seq=1)
            data.members.add(
                player_id=caller.player_id, owner_account=caller.account, owner_revision=caller.owner_revision
            )
            s.teams[team_id] = _MemTeamRow(
                team_id=team_id, version=1, data=data, state=TEAM_STATE_ACTIVE, idle_expires_ms=now + TEAM_IDLE_TTL_MS
            )
            epoch = self._bump_membership(s, caller.player_id, team_id)
            outcome = TeamMutationOutcome(
                team_id=team_id,
                version=1,
                record=self._clone_msg(rec),
                epochs={caller.player_id: epoch},
                reason=1,  # CREATED
                actor_id=caller.player_id,
                now_ms=now,
                joined=(caller.player_id,),
            )
            self._save_receipt(s, caller.player_id, request_id, 1, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def get_my_team(
        self, player_id: int, *, match_request_id: str = "", caller: CallerIdentity | None = None
    ) -> TeamSnapshotResult:
        now = await self.now_ms()
        async with self._lock:
            s = self._state
            if caller is not None:
                self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            mem = s.memberships.get(player_id)
            team_id = mem.team_id if mem is not None else 0
            epoch = mem.epoch if mem is not None else 0
            version = 0
            record = None
            if team_id:
                row = s.teams.get(team_id)
                if row is not None and row.state == TEAM_STATE_ACTIVE:
                    version = row.version
                    record = self._clone_msg(row.data.record)
                else:
                    team_id = 0
            launch_state = launch_battle_id = launch_mode = launch_config_id = launch_target_size = launch_enqueued_ms = 0
            if match_request_id and is_valid_uuid(match_request_id):
                h, l = uuid_to_pair(match_request_id)
                launch = s.team_launches.get((player_id, h, l))
                if launch is not None:
                    if caller is not None and (
                        launch.owner_account != caller.account or int(launch.owner_revision) != caller.owner_revision
                    ):
                        launch_state = 6
                    else:
                        launch_battle_id = int(launch.battle_id)
                        if launch_battle_id:
                            f = s.formations.get(launch_battle_id)
                            launch_state = int(f.state) if f is not None else int(launch.state)
                        else:
                            launch_state = int(launch.state)
                        launch_mode = int(launch.mode)
                        launch_config_id = int(launch.config_id)
                        launch_target_size = int(launch.target_size)
                        launch_enqueued_ms = int(launch.enqueued_ms)
            elif team_id:
                for launch in s.team_launches.values():
                    if int(launch.team_id) == team_id and int(launch.state) == TEAM_LAUNCH_QUEUED:
                        launch_state = TEAM_LAUNCH_QUEUED
                        launch_mode = int(launch.mode)
                        launch_config_id = int(launch.config_id)
                        launch_target_size = int(launch.target_size)
                        launch_enqueued_ms = int(launch.enqueued_ms)
                        break
            return TeamSnapshotResult(
                team_id=team_id,
                membership_epoch=epoch,
                version=version,
                record=record,
                now_ms=now,
                launch_state=launch_state,
                launch_battle_id=launch_battle_id,
                launch_mode=launch_mode,
                launch_config_id=launch_config_id,
                launch_target_size=launch_target_size,
                launch_enqueued_ms=launch_enqueued_ms,
            )

    async def read_membership(
        self, player_id: int, *, owner_revision: int = 0
    ) -> tuple[int, int, int, Message | None]:
        async with self._lock:
            s = self._state
            self._check_fence_locked(s, player_id, "", owner_revision)
            mem = s.memberships.get(player_id)
            if mem is None or mem.team_id == 0:
                return 0, mem.epoch if mem is not None else 0, 0, None
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                return 0, mem.epoch, 0, None
            return mem.team_id, mem.epoch, row.version, self._clone_msg(row.data.record)

    async def apply_join_team(
        self, caller: CallerIdentity, target_player_id: int, zone_id: int, request_id: str, *, allow_cross_zone: bool
    ) -> TeamMutationOutcome:
        if target_player_id == 0 or target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        now = await self.now_ms()
        payload = f"apply:{target_player_id}:{zone_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 2, payload, caller)
            if cached is not None:
                return cached
            my_mem = s.memberships.get(caller.player_id)
            if my_mem is not None and my_mem.team_id != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            target_mem = s.memberships.get(target_player_id)
            if target_mem is None or target_mem.team_id == 0:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(target_mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            self._prune_team_pending(s, row, now)
            rec = row.data.record
            if not allow_cross_zone and rec.zone_id and zone_id and int(rec.zone_id) != zone_id:
                raise TeamBusinessError(ERR_TEAM_CROSS_ZONE_DENIED)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            if len(rec.applications) >= MAX_APPLICATIONS_PER_TEAM and not any(
                int(a.player_id) == caller.player_id for a in rec.applications
            ):
                raise TeamBusinessError(ERR_TEAM_INVITE_LIMIT)
            expire_at = now + APPLICATION_TTL_MS
            updated = False
            for app in rec.applications:
                if int(app.player_id) == caller.player_id:
                    app.applied_at_ms = now
                    app.expire_at_ms = expire_at
                    updated = True
                    break
            if not updated:
                rec.applications.add(
                    player_id=caller.player_id, zone_id=zone_id, applied_at_ms=now, expire_at_ms=expire_at
                )
            s.app_index[(caller.player_id, row.team_id)] = expire_at
            row.version += 1
            row.idle_expires_ms = now + TEAM_IDLE_TTL_MS
            member_ids = [int(m.player_id) for m in rec.members]
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids + [caller.player_id]),
                reason=8,  # APPLICATION_CHANGED
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(member_ids),
            )
            self._save_receipt(s, caller.player_id, request_id, 2, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def handle_application(
        self, caller: CallerIdentity, applicant_id: int, approve: bool, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if applicant_id == 0:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        now = await self.now_ms()
        payload = f"handle_app:{applicant_id}:{int(approve)}:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 3, payload, caller)
            if cached is not None:
                return cached
            my_mem = s.memberships.get(caller.player_id)
            if my_mem is None or my_mem.team_id == 0 or (expected_team_id and my_mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(my_mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            self._prune_team_pending(s, row, now)
            app = next((a for a in rec.applications if int(a.player_id) == applicant_id), None)
            if app is None:
                raise TeamBusinessError(ERR_TEAM_APPLICATION_NOT_FOUND)
            if not approve:
                keep = [a for a in rec.applications if int(a.player_id) != applicant_id]
                del rec.applications[:]
                rec.applications.extend(keep)
                s.app_index.pop((applicant_id, row.team_id), None)
                row.version += 1
                member_ids = [int(m.player_id) for m in rec.members]
                outcome = TeamMutationOutcome(
                    team_id=row.team_id,
                    version=row.version,
                    record=self._clone_msg(rec),
                    epochs=self._epochs_for(s, member_ids),
                    reason=8,
                    actor_id=caller.player_id,
                    now_ms=now,
                    kept=tuple(member_ids),
                    rejected_applicant=applicant_id,
                )
                self._save_receipt(s, caller.player_id, request_id, 3, payload, caller, outcome)
                return outcome
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            app_mem = s.memberships.get(applicant_id)
            if app_mem is not None and app_mem.team_id != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            self._cancel_queued_team_launch(s, row.team_id)
            for m in rec.members:
                m.ranked_preset_confirmed = False
            kept_ids = [int(m.player_id) for m in rec.members]
            seq = int(rec.next_join_seq) or (len(rec.members) + 1)
            rec.next_join_seq = seq + 1
            rec.members.add(player_id=applicant_id, zone_id=int(app.zone_id), joined_at_ms=now, join_seq=seq)
            keep_apps = [a for a in rec.applications if int(a.player_id) != applicant_id]
            del rec.applications[:]
            rec.applications.extend(keep_apps)
            s.app_index.pop((applicant_id, row.team_id), None)
            revoked: list[int] = []
            if len(rec.members) >= MAX_TEAM_SIZE:
                revoked = [int(inv.invitee_id) for inv in rec.invites]
                del rec.invites[:]
                for inv_pid in revoked:
                    s.invite_index.pop((inv_pid, row.team_id), None)
            self._bump_membership(s, applicant_id, row.team_id)
            row.version += 1
            row.idle_expires_ms = now + TEAM_IDLE_TTL_MS
            all_ids = kept_ids + [applicant_id]
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, all_ids),
                reason=2,  # MEMBER_JOINED
                actor_id=applicant_id,
                now_ms=now,
                joined=(applicant_id,),
                kept=tuple(kept_ids),
                revoked_invitees=tuple(revoked),
            )
            self._save_receipt(s, caller.player_id, request_id, 3, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def invite_to_team(
        self,
        caller: CallerIdentity,
        target_player_id: int,
        target_zone_id: int,
        expected_team_id: int,
        request_id: str,
        *,
        allow_cross_zone: bool,
    ) -> TeamMutationOutcome:
        if target_player_id == 0 or target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        now = await self.now_ms()
        payload = f"invite:{target_player_id}:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 4, payload, caller)
            if cached is not None:
                return cached
            my_mem = s.memberships.get(caller.player_id)
            if my_mem is None or my_mem.team_id == 0 or (expected_team_id and my_mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(my_mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            self._prune_team_pending(s, row, now)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            target_mem = s.memberships.get(target_player_id)
            if target_mem is not None and target_mem.team_id != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            if not allow_cross_zone and rec.zone_id and target_zone_id and int(rec.zone_id) != target_zone_id:
                raise TeamBusinessError(ERR_TEAM_CROSS_ZONE_DENIED)
            active_invites = [
                tid for (pid, tid), exp in s.invite_index.items() if pid == target_player_id and exp > now
            ]
            if len(active_invites) >= MAX_INVITES_PER_PLAYER and row.team_id not in active_invites:
                raise TeamBusinessError(ERR_TEAM_INVITE_LIMIT)
            expire_at = now + INVITE_TTL_MS
            updated = False
            for inv in rec.invites:
                if int(inv.invitee_id) == target_player_id:
                    inv.inviter_id = caller.player_id
                    inv.invited_at_ms = now
                    inv.expire_at_ms = expire_at
                    updated = True
                    break
            if not updated:
                rec.invites.add(
                    invitee_id=target_player_id,
                    inviter_id=caller.player_id,
                    zone_id=target_zone_id,
                    invited_at_ms=now,
                    expire_at_ms=expire_at,
                )
            s.invite_index[(target_player_id, row.team_id)] = expire_at
            row.version += 1
            row.idle_expires_ms = now + TEAM_IDLE_TTL_MS
            member_ids = [int(m.player_id) for m in rec.members]
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids),
                reason=9,  # INVITE_CHANGED
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(member_ids),
                invited_player=target_player_id,
            )
            self._save_receipt(s, caller.player_id, request_id, 4, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def respond_invite(
        self, caller: CallerIdentity, team_id: int, accept: bool, zone_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if team_id == 0:
            raise TeamBusinessError(ERR_TEAM_INVITE_NOT_FOUND)
        now = await self.now_ms()
        payload = f"respond_inv:{team_id}:{int(accept)}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 5, payload, caller)
            if cached is not None:
                return cached
            row = s.teams.get(team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                s.invite_index.pop((caller.player_id, team_id), None)
                raise TeamBusinessError(ERR_TEAM_INVITE_NOT_FOUND)
            self._prune_team_pending(s, row, now)
            rec = row.data.record
            inv = next((i for i in rec.invites if int(i.invitee_id) == caller.player_id), None)
            if inv is None:
                s.invite_index.pop((caller.player_id, team_id), None)
                raise TeamBusinessError(ERR_TEAM_INVITE_NOT_FOUND)
            if not accept:
                keep = [i for i in rec.invites if int(i.invitee_id) != caller.player_id]
                del rec.invites[:]
                rec.invites.extend(keep)
                s.invite_index.pop((caller.player_id, team_id), None)
                row.version += 1
                member_ids = [int(m.player_id) for m in rec.members]
                outcome = TeamMutationOutcome(
                    team_id=team_id,
                    version=row.version,
                    record=self._clone_msg(rec),
                    epochs=self._epochs_for(s, member_ids + [caller.player_id]),
                    reason=9,
                    actor_id=caller.player_id,
                    now_ms=now,
                    kept=tuple(member_ids),
                )
                self._save_receipt(s, caller.player_id, request_id, 5, payload, caller, outcome)
                return outcome
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            my_mem = s.memberships.get(caller.player_id)
            if my_mem is not None and my_mem.team_id != 0:
                raise TeamBusinessError(ERR_TEAM_MEMBER_IN_TEAM)
            if len(rec.members) >= MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_MEMBERS_FULL)
            self._cancel_queued_team_launch(s, team_id)
            for m in rec.members:
                m.ranked_preset_confirmed = False
            kept_ids = [int(m.player_id) for m in rec.members]
            seq = int(rec.next_join_seq) or (len(rec.members) + 1)
            rec.next_join_seq = seq + 1
            rec.members.add(player_id=caller.player_id, zone_id=zone_id, joined_at_ms=now, join_seq=seq)
            row.data.members.add(
                player_id=caller.player_id, owner_account=caller.account, owner_revision=caller.owner_revision
            )
            keep_inv = [i for i in rec.invites if int(i.invitee_id) != caller.player_id]
            del rec.invites[:]
            rec.invites.extend(keep_inv)
            s.invite_index.pop((caller.player_id, team_id), None)
            revoked: list[int] = []
            if len(rec.members) >= MAX_TEAM_SIZE:
                revoked = [int(i.invitee_id) for i in rec.invites]
                del rec.invites[:]
                for inv_pid in revoked:
                    s.invite_index.pop((inv_pid, team_id), None)
            self._bump_membership(s, caller.player_id, team_id)
            row.version += 1
            row.idle_expires_ms = now + TEAM_IDLE_TTL_MS
            all_ids = kept_ids + [caller.player_id]
            outcome = TeamMutationOutcome(
                team_id=team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, all_ids),
                reason=2,
                actor_id=caller.player_id,
                now_ms=now,
                joined=(caller.player_id,),
                kept=tuple(kept_ids),
                revoked_invitees=tuple(revoked),
            )
            self._save_receipt(s, caller.player_id, request_id, 5, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def list_my_invites(self, player_id: int) -> tuple[list[IncomingInviteInfo], int]:
        now = await self.now_ms()
        async with self._lock:
            s = self._state
            out: list[IncomingInviteInfo] = []
            for (pid, tid), exp in list(s.invite_index.items()):
                if pid != player_id or exp <= now:
                    continue
                row = s.teams.get(tid)
                if row is None or row.state != TEAM_STATE_ACTIVE:
                    continue
                rec = row.data.record
                inv = next((i for i in rec.invites if int(i.invitee_id) == player_id and int(i.expire_at_ms) > now), None)
                if inv is None:
                    continue
                out.append(
                    IncomingInviteInfo(
                        team_id=tid,
                        inviter_id=int(inv.inviter_id),
                        leader_id=int(rec.leader_id),
                        member_count=len(rec.members),
                        zone_id=int(rec.zone_id),
                        expire_at_ms=int(inv.expire_at_ms),
                    )
                )
            out.sort(key=lambda item: item.expire_at_ms)
            return out, now

    async def leave_team(self, caller: CallerIdentity, expected_team_id: int, request_id: str) -> TeamMutationOutcome:
        now = await self.now_ms()
        payload = f"leave:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 6, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0:
                return TeamMutationOutcome(
                    team_id=0, version=0, record=None, epochs={caller.player_id: mem.epoch if mem else 0},
                    reason=3, actor_id=caller.player_id, now_ms=now,
                )
            if expected_team_id and mem.team_id != expected_team_id:
                row = s.teams.get(mem.team_id)
                return TeamMutationOutcome(
                    team_id=mem.team_id,
                    version=row.version if row else 0,
                    record=self._clone_msg(row.data.record) if row else None,
                    epochs={caller.player_id: mem.epoch},
                    reason=0,
                    actor_id=caller.player_id,
                    now_ms=now,
                )
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                epoch = self._bump_membership(s, caller.player_id, 0)
                return TeamMutationOutcome(
                    team_id=0, version=0, record=None, epochs={caller.player_id: epoch},
                    reason=3, actor_id=caller.player_id, now_ms=now, left=(caller.player_id,),
                )
            rec = row.data.record
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            self._cancel_queued_team_launch(s, row.team_id)
            remaining = [m for m in rec.members if int(m.player_id) != caller.player_id]
            self._bump_membership(s, caller.player_id, 0)
            row.version += 1
            if not remaining:
                revoked = [int(i.invitee_id) for i in rec.invites]
                for pid in revoked:
                    s.invite_index.pop((pid, row.team_id), None)
                for a in rec.applications:
                    s.app_index.pop((int(a.player_id), row.team_id), None)
                del rec.members[:]
                del rec.invites[:]
                del rec.applications[:]
                row.state = TEAM_STATE_DISBANDED
                outcome = TeamMutationOutcome(
                    team_id=row.team_id,
                    version=row.version,
                    record=None,
                    epochs=self._epochs_for(s, [caller.player_id]),
                    reason=7,  # DISBANDED
                    actor_id=caller.player_id,
                    now_ms=now,
                    left=(caller.player_id,),
                    revoked_invitees=tuple(revoked),
                )
            else:
                remaining.sort(key=lambda m: (int(m.join_seq), int(m.player_id)))
                for m in remaining:
                    m.ranked_preset_confirmed = False
                del rec.members[:]
                rec.members.extend(remaining)
                leader_changed = False
                if int(rec.leader_id) == caller.player_id:
                    rec.leader_id = int(remaining[0].player_id)
                    leader_changed = True
                kept_ids = [int(m.player_id) for m in rec.members]
                outcome = TeamMutationOutcome(
                    team_id=row.team_id,
                    version=row.version,
                    record=self._clone_msg(rec),
                    epochs=self._epochs_for(s, kept_ids + [caller.player_id]),
                    reason=3,  # MEMBER_LEFT
                    actor_id=caller.player_id,
                    now_ms=now,
                    kept=tuple(kept_ids),
                    left=(caller.player_id,),
                    leader_changed=leader_changed,
                )
            self._save_receipt(s, caller.player_id, request_id, 6, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def kick_member(
        self, caller: CallerIdentity, target_player_id: int, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if target_player_id == 0:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        if target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_KICK_SELF)
        now = await self.now_ms()
        payload = f"kick:{target_player_id}:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 7, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_KICK_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            if not any(int(m.player_id) == target_player_id for m in rec.members):
                raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_IN_TEAM)
            self._cancel_queued_team_launch(s, row.team_id)
            remaining = [m for m in rec.members if int(m.player_id) != target_player_id]
            for m in remaining:
                m.ranked_preset_confirmed = False
            del rec.members[:]
            rec.members.extend(remaining)
            self._bump_membership(s, target_player_id, 0)
            row.version += 1
            kept_ids = [int(m.player_id) for m in rec.members]
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, kept_ids + [target_player_id]),
                reason=4,  # MEMBER_KICKED
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(kept_ids),
                left=(target_player_id,),
            )
            self._save_receipt(s, caller.player_id, request_id, 7, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def transfer_leader(
        self, caller: CallerIdentity, target_player_id: int, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        if target_player_id == 0:
            raise TeamBusinessError(ERR_TEAM_PLAYER_ID)
        if target_player_id == caller.player_id:
            raise TeamBusinessError(ERR_TEAM_APPOINT_SELF)
        now = await self.now_ms()
        payload = f"transfer:{target_player_id}:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 8, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_APPOINT_NOT_LEADER)
            if not any(int(m.player_id) == target_player_id for m in rec.members):
                raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_IN_TEAM)
            rec.leader_id = target_player_id
            row.version += 1
            member_ids = [int(m.player_id) for m in rec.members]
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids),
                reason=5,  # LEADER_TRANSFERRED
                actor_id=target_player_id,
                now_ms=now,
                kept=tuple(member_ids),
                leader_changed=True,
            )
            self._save_receipt(s, caller.player_id, request_id, 8, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def disband_team(
        self, caller: CallerIdentity, expected_team_id: int, request_id: str
    ) -> TeamMutationOutcome:
        now = await self.now_ms()
        payload = f"disband:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 9, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_DISBAND_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            self._cancel_queued_team_launch(s, row.team_id)
            member_ids = [int(m.player_id) for m in rec.members]
            revoked = [int(i.invitee_id) for i in rec.invites]
            for pid in member_ids:
                self._bump_membership(s, pid, 0)
            for pid in revoked:
                s.invite_index.pop((pid, row.team_id), None)
            for a in rec.applications:
                s.app_index.pop((int(a.player_id), row.team_id), None)
            del rec.members[:]
            del rec.invites[:]
            del rec.applications[:]
            row.state = TEAM_STATE_DISBANDED
            row.version += 1
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=None,
                epochs=self._epochs_for(s, member_ids),
                reason=7,  # DISBANDED
                actor_id=caller.player_id,
                now_ms=now,
                left=tuple(member_ids),
                revoked_invitees=tuple(revoked),
            )
            self._save_receipt(s, caller.player_id, request_id, 9, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def confirm_ranked_preset(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        char_preset: int,
        pet_preset: int,
        rule_version: int,
        request_id: str,
    ) -> TeamMutationOutcome:
        now = await self.now_ms()
        payload = f"preset:{expected_team_id}:{char_preset}:{pet_preset}:{rule_version}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 10, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            found = next((m for m in rec.members if int(m.player_id) == caller.player_id), None)
            if found is None:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            found.ranked_character_preset_id = char_preset
            found.ranked_pet_preset_id = pet_preset
            found.ranked_rule_version = rule_version
            found.ranked_preset_confirmed = True
            row.version += 1
            member_ids = [int(m.player_id) for m in rec.members]
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids),
                reason=17,  # RANKED_PRESET_CONFIRMED
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(member_ids),
            )
            self._save_receipt(s, caller.player_id, request_id, 10, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def set_team_formation(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        formation: int,
        request_id: str,
    ) -> TeamMutationOutcome:
        if formation not in VALID_TEAM_FOLLOW_FORMATIONS:
            raise TeamBusinessError(ERR_INVALID_PARAMETER)
        now = await self.now_ms()
        payload = f"formation:{expected_team_id}:{formation}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 11, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            member_ids = [int(m.player_id) for m in rec.members]
            changed = int(rec.follow_formation) != formation
            if changed:
                rec.follow_formation = formation
                row.version += 1
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids),
                reason=18 if changed else 0,  # TEAM_CHANGE_REASON_FORMATION_CHANGED
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(member_ids),
                formation_changed=changed,
            )
            self._save_receipt(s, caller.player_id, request_id, 11, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def recall_team_members(
        self,
        caller: CallerIdentity,
        expected_team_id: int,
        request_id: str,
    ) -> TeamMutationOutcome:
        now = await self.now_ms()
        payload = f"recall:{expected_team_id}".encode()

        def work(s: _MemState) -> TeamMutationOutcome:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cached = self._check_receipt(s, caller.player_id, request_id, 12, payload, caller)
            if cached is not None:
                return cached
            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            last_recall = int(rec.last_recall_at_ms)
            if last_recall > 0 and now < last_recall + RECALL_COOLDOWN_MS:
                raise TeamBusinessError(ERR_RATE_LIMIT_EXCEEDED)
            rec.last_recall_at_ms = now
            row.version += 1
            member_ids = [int(m.player_id) for m in rec.members]
            recipients = tuple(pid for pid in member_ids if pid != caller.player_id)
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids),
                reason=0,
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(member_ids),
                recall_recipients=recipients,
            )
            self._save_receipt(s, caller.player_id, request_id, 12, payload, caller, outcome)
            return outcome

        return await self._run_tx(work)

    async def start_team_match(
        self,
        caller: CallerIdentity,
        *,
        expected_team_id: int,
        battle_config_id: int,
        mode: int,
        target_team_size: int,
        request_id: str,
        member_zones: dict[int, int],
        immediate_battle_node: tuple[int, str, str, int, str] | None,
        battle_deadline_ms: int,
        queue_ttl_ms: int,
    ) -> tuple[TeamMutationOutcome, Message | None, Message]:
        now = await self.now_ms()
        req_uuid = request_id if is_valid_uuid(request_id) else str(uuid.uuid4())
        h, l = uuid_to_pair(req_uuid)

        def work(s: _MemState):
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            existing_launch = s.team_launches.get((caller.player_id, h, l))
            if existing_launch is not None:
                if (
                    existing_launch.owner_account != caller.account
                    or int(existing_launch.owner_revision) != caller.owner_revision
                ):
                    raise OriginalRoleError("team launch original owner mismatch")
                row = s.teams.get(int(existing_launch.team_id))
                rec = self._clone_msg(row.data.record) if row is not None else None
                mids = [int(m.player_id) for m in rec.members] if rec is not None else [caller.player_id]
                out = TeamMutationOutcome(
                    team_id=int(existing_launch.team_id),
                    version=row.version if row else 0,
                    record=rec,
                    epochs=self._epochs_for(s, mids),
                    reason=11 if int(existing_launch.battle_id) else 15,
                    actor_id=caller.player_id,
                    now_ms=now,
                    kept=tuple(mids),
                )
                form = s.formations.get(int(existing_launch.battle_id)) if int(existing_launch.battle_id) else None
                return out, self._clone_msg(form), self._clone_msg(existing_launch)

            mem = s.memberships.get(caller.player_id)
            if mem is None or mem.team_id == 0 or (expected_team_id and mem.team_id != expected_team_id):
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            row = s.teams.get(mem.team_id)
            if row is None or row.state != TEAM_STATE_ACTIVE:
                raise TeamBusinessError(ERR_TEAM_NO_TEAM)
            rec = row.data.record
            if int(rec.leader_id) != caller.player_id:
                raise TeamBusinessError(ERR_TEAM_NOT_LEADER)
            if self._team_locked(rec, now):
                raise TeamBusinessError(ERR_TEAM_IN_MATCH)
            member_ids = [int(m.player_id) for m in sorted(rec.members, key=lambda x: (int(x.join_seq), int(x.player_id)))]
            if target_team_size < len(member_ids) or target_team_size > MAX_TEAM_SIZE:
                raise TeamBusinessError(ERR_TEAM_SIZE_EXCEEDED)
            for pid in member_ids:
                tk = s.tickets.get(pid)
                if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                    raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_READY, pid)
                occ = s.occupancies.get(pid)
                if occ is not None and occ.expires_ms > now:
                    raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_READY, pid)

            lock_token = str(uuid.uuid4())
            is_immediate = mode == MODE_PVE_TEAM and target_team_size == len(member_ids)
            launch = self.protocol.message(
                "teampb.TeamLaunchRecord",
                actor_id=caller.player_id,
                request_high=h,
                request_low=l,
                team_id=row.team_id,
                team_version=row.version + 1,
                lock_token=lock_token,
                config_id=battle_config_id,
                target_size=target_team_size,
                member_count=len(member_ids),
                state=TEAM_LAUNCH_FORMING if is_immediate else TEAM_LAUNCH_QUEUED,
                enqueued_ms=now,
                mode=mode,
                owner_account=caller.account,
                owner_revision=caller.owner_revision,
            )
            formation = None
            if is_immediate:
                if immediate_battle_node is None:
                    raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_READY, caller.player_id)
                b_node_id, b_uuid, b_endpoint, b_region, b_track = immediate_battle_node
                battle_id = self._alloc_id_locked()
                op_id = str(uuid.uuid4())
                launch.battle_id = battle_id
                rec.match_lock_token = lock_token
                rec.match_lock_expire_at_ms = 0
                del rec.match_lock_roster[:]
                rec.match_lock_roster.extend(member_ids)
                formation = self.protocol.message(
                    "match.MatchFormationRecord",
                    battle_id=battle_id,
                    operation_id=op_id,
                    state=FORMATION_STATE_PREPARING,
                    physical_region=b_region,
                    target_node_id=b_node_id,
                    target_instance_id=b_uuid,
                    target_endpoint=b_endpoint,
                    mode=mode,
                    config_id=battle_config_id,
                    created_ms=now,
                    deadline_ms=now + battle_deadline_ms,
                    retry_at_ms=now + 60_000,
                    requeue_on_failure=False,
                    release_track=b_track,
                )
                for idx, pid in enumerate(member_ids):
                    tk_id = f"tk-{battle_id}-{pid}"
                    formation.members.add(player_id=pid, ticket=tk_id, team_index=0, captain=(pid == caller.player_id))
                    owner_acct, owner_rev = self._member_owner(s, row, pid, caller)
                    s.tickets[pid] = self.protocol.message(
                        "match.MatchAuthorityTicketRecord",
                        player_id=pid,
                        ticket=tk_id,
                        request_id=req_uuid,
                        mode=mode,
                        config_id=battle_config_id,
                        team_size=target_team_size,
                        state=TICKET_STATE_FORMING,
                        enqueued_ms=now,
                        zone_id=member_zones.get(pid, int(rec.zone_id)),
                        formation_id=battle_id,
                        team_id=row.team_id,
                        expires_ms=now + queue_ttl_ms,
                        selected_region=b_region,
                        owner_account=owner_acct,
                        owner_revision=owner_rev,
                    )
                s.formations[battle_id] = formation
            else:
                for pid in member_ids:
                    tk_id = f"tk-{req_uuid}-{pid}"
                    owner_acct, owner_rev = self._member_owner(s, row, pid, caller)
                    s.tickets[pid] = self.protocol.message(
                        "match.MatchAuthorityTicketRecord",
                        player_id=pid,
                        ticket=tk_id,
                        request_id=req_uuid,
                        mode=mode,
                        config_id=battle_config_id,
                        team_size=target_team_size,
                        state=TICKET_STATE_QUEUED,
                        enqueued_ms=now,
                        zone_id=member_zones.get(pid, int(rec.zone_id)),
                        team_id=row.team_id,
                        expires_ms=now + queue_ttl_ms,
                        owner_account=owner_acct,
                        owner_revision=owner_rev,
                    )

            s.team_launches[(caller.player_id, h, l)] = launch
            for pid in member_ids:
                s.team_launch_roles[(caller.player_id, h, l, pid)] = int(launch.state)
            row.version += 1
            outcome = TeamMutationOutcome(
                team_id=row.team_id,
                version=row.version,
                record=self._clone_msg(rec),
                epochs=self._epochs_for(s, member_ids),
                reason=11 if is_immediate else 15,  # MATCH_STARTED / MATCH_QUEUED
                actor_id=caller.player_id,
                now_ms=now,
                kept=tuple(member_ids),
            )
            return outcome, self._clone_msg(formation), self._clone_msg(launch)

        return await self._run_tx(work)

    def _member_owner(
        self, s: _MemState, row: _MemTeamRow, pid: int, fallback: CallerIdentity
    ) -> tuple[str, int]:
        fence = s.role_fences.get(pid)
        if fence is not None and fence.account:
            return fence.account, fence.owner_revision
        orig = next((m for m in row.data.members if int(m.player_id) == pid), None)
        if orig is not None and orig.owner_account:
            return orig.owner_account, int(orig.owner_revision)
        if pid == fallback.player_id:
            return fallback.account, fallback.owner_revision
        return f"acct:{pid}", 1

    # -- Network Profile ------------------------------------------------------------------------------

    async def save_network_plan(self, player_id: int, profile: Message, expires_ms: int) -> None:
        raw = profile.SerializeToString(deterministic=True)

        def work(s: _MemState):
            s.network_profiles[player_id] = (raw, expires_ms)

        await self._run_tx(work)

    async def commit_network_profile(
        self, player_id: int, plan_id: str, binding: str, receipts: list[Message], expires_ms: int
    ) -> None:
        now = await self.now_ms()

        def work(s: _MemState):
            stored = s.network_profiles.get(player_id)
            if stored is None:
                raise OriginalRoleError("network probe plan missing")
            raw, _ = stored
            profile = self.protocol.parse("match.MatchNetworkProfile", raw)
            if profile.plan_id != plan_id or profile.session_binding != binding:
                raise OriginalRoleError("network probe plan or binding mismatch")
            del profile.receipts[:]
            for r in receipts:
                profile.receipts.add().CopyFrom(r)
            encoded = profile.SerializeToString(deterministic=True)
            s.network_profiles[player_id] = (encoded, expires_ms)
            tk = s.tickets.get(player_id)
            if tk is not None and int(tk.state) == TICKET_STATE_QUEUED:
                tk.network_profile = encoded
                tk.pool_region = preferred_region(profile, now)

        await self._run_tx(work)

    async def get_network_profile(self, player_id: int) -> tuple[Message | None, int]:
        async with self._lock:
            stored = self._state.network_profiles.get(player_id)
            if stored is None:
                return None, 0
            raw, expires_ms = stored
            return self.protocol.parse("match.MatchNetworkProfile", raw), expires_ms

    # -- Queue, Challenge, Formation ------------------------------------------------------------------

    async def join_queue(
        self,
        caller: CallerIdentity,
        *,
        mode: int,
        config_id: int,
        team_size: int,
        zone_id: int,
        request_id: str,
        request_payload: bytes,
        network_profile: bytes,
        pool_region: int,
        queue_ttl_ms: int,
        is_wuxing_tower: bool = False,
    ) -> Message:
        now = await self.now_ms()
        req_uuid = request_id if is_valid_uuid(request_id) else str(uuid.uuid4())
        h, l = uuid_to_pair(req_uuid)

        def work(s: _MemState) -> Message:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            intent = s.join_intents.get((caller.player_id, h, l))
            if intent is not None:
                if intent.owner_account != caller.account or intent.owner_revision != caller.owner_revision:
                    raise OriginalRoleError("join intent belongs to previous owner")
                if intent.cancelled:
                    raise MatchBusinessError(ERR_MATCH_CANCEL_TOO_LATE)
                if intent.ticket_record:
                    return self.protocol.parse("match.MatchAuthorityTicketRecord", intent.ticket_record)

            existing = s.tickets.get(caller.player_id)
            if existing is not None:
                st = int(existing.state)
                if st == TICKET_STATE_QUEUED:
                    if int(existing.expires_ms) > now:
                        raise MatchBusinessError(ERR_MATCH_ALREADY_QUEUED)
                elif st in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                    raise MatchBusinessError(ERR_MATCH_IN_BATTLE)

            occ = s.occupancies.get(caller.player_id)
            if occ is not None and occ.expires_ms > now:
                raise MatchBusinessError(ERR_MATCH_IN_BATTLE)

            ticket_str = f"q-{self._alloc_id_locked()}-{caller.player_id}"
            ticket = self.protocol.message(
                "match.MatchAuthorityTicketRecord",
                player_id=caller.player_id,
                ticket=ticket_str,
                request_id=req_uuid,
                mode=mode,
                config_id=config_id,
                team_size=team_size,
                state=TICKET_STATE_QUEUED,
                enqueued_ms=now,
                zone_id=zone_id,
                network_profile=network_profile,
                expires_ms=now + queue_ttl_ms,
                owner_account=caller.account,
                owner_revision=caller.owner_revision,
                pool_region=pool_region,
            )
            s.tickets[caller.player_id] = ticket
            if is_wuxing_tower and mode == MODE_PVE_SOLO:
                s.tower_tickets.add(caller.player_id)
            else:
                s.tower_tickets.discard(caller.player_id)
            s.join_intents[(caller.player_id, h, l)] = _MemJoinIntentRow(
                player_id=caller.player_id,
                request_high=h,
                request_low=l,
                request_id=req_uuid,
                ticket_record=ticket.SerializeToString(deterministic=True),
                cancelled=False,
                owner_account=caller.account,
                owner_revision=caller.owner_revision,
                request_payload=request_payload,
                state=2,
            )
            return self._clone_msg(ticket)

        return await self._run_tx(work)

    async def cancel_queue(self, caller: CallerIdentity, queue_ticket: str, request_id: str) -> None:
        def work(s: _MemState) -> None:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            if is_valid_uuid(request_id):
                h, l = uuid_to_pair(request_id)
                intent = s.join_intents.get((caller.player_id, h, l))
                if intent is None:
                    s.join_intents[(caller.player_id, h, l)] = _MemJoinIntentRow(
                        player_id=caller.player_id,
                        request_high=h,
                        request_low=l,
                        request_id=request_id,
                        ticket_record=b"",
                        cancelled=True,
                        owner_account=caller.account,
                        owner_revision=caller.owner_revision,
                        request_payload=b"",
                        state=3,
                    )
                else:
                    if intent.owner_account == caller.account and intent.owner_revision == caller.owner_revision:
                        intent.cancelled = True
                        intent.state = 3

            tk = s.tickets.get(caller.player_id)
            if tk is None or int(tk.state) == TICKET_STATE_DONE:
                return
            if queue_ticket and tk.ticket != queue_ticket:
                raise MatchBusinessError(ERR_MATCH_TICKET_MISMATCH)
            if int(tk.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                raise MatchBusinessError(ERR_MATCH_CANCEL_TOO_LATE)
            if int(tk.team_id) != 0:
                self._cancel_queued_team_launch(s, int(tk.team_id))
            tk.state = TICKET_STATE_DONE

        await self._run_tx(work)

    async def get_queue_status(self, caller: CallerIdentity, request_id: str = "") -> QueueStatusInfo:
        now = await self.now_ms()
        async with self._lock:
            s = self._state
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            cancelled = False
            if is_valid_uuid(request_id):
                h, l = uuid_to_pair(request_id)
                intent = s.join_intents.get((caller.player_id, h, l))
                if intent is not None:
                    cancelled = bool(
                        intent.cancelled
                        or intent.owner_account != caller.account
                        or intent.owner_revision != caller.owner_revision
                    )
            tk = s.tickets.get(caller.player_id)
            if tk is not None and int(tk.state) == TICKET_STATE_QUEUED and int(tk.expires_ms) <= now:
                tk = None
            return QueueStatusInfo(ticket=self._clone_msg(tk), request_cancelled=cancelled, now_ms=now)

    async def list_queued_tickets(self, *, limit: int = 200) -> list[Message]:
        now = await self.now_ms()
        async with self._lock:
            rows = [
                self._clone_msg(tk)
                for tk in self._state.tickets.values()
                if int(tk.state) == TICKET_STATE_QUEUED and int(tk.team_id) == 0 and int(tk.expires_ms) > now
            ]
            rows.sort(key=lambda t: (int(t.enqueued_ms), int(t.player_id)))
            return rows[:limit]

    async def list_queued_team_launches(self, *, limit: int = 100) -> list[Message]:
        async with self._lock:
            rows = [
                self._clone_msg(l)
                for l in self._state.team_launches.values()
                if int(l.state) == TEAM_LAUNCH_QUEUED
            ]
            rows.sort(key=lambda l: (int(l.enqueued_ms), int(l.actor_id)))
            return rows[:limit]

    async def create_challenge(
        self,
        challenger: CallerIdentity,
        target_player_id: int,
        config_id: int,
        ttl_ms: int,
        *,
        custom_rules_payload: bytes = b"",
    ) -> Message:
        if target_player_id == 0 or target_player_id == challenger.player_id:
            raise MatchBusinessError(ERR_MATCH_CHALLENGE_SELF)
        now = await self.now_ms()

        def work(s: _MemState) -> Message:
            self._check_fence_locked(s, challenger.player_id, challenger.account, challenger.owner_revision)
            tk_self = s.tickets.get(challenger.player_id)
            if tk_self is not None and int(tk_self.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_SELF_BUSY)
            tk_target = s.tickets.get(target_player_id)
            if tk_target is not None and int(tk_target.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_TARGET_BUSY)
            for inv in s.challenges.values():
                if (
                    int(inv.target_player_id) == target_player_id
                    and int(inv.state) == CHALLENGE_STATE_PENDING
                    and int(inv.expires_ms) > now
                ):
                    raise MatchBusinessError(ERR_MATCH_CHALLENGE_PENDING)
            target_fence = s.role_fences.get(target_player_id)
            target_acct = target_fence.account if target_fence is not None else ""
            target_rev = target_fence.owner_revision if target_fence is not None else 0
            cid = self._alloc_id_locked()
            inv = self.protocol.message(
                "match.MatchChallengeInvitationRecord",
                challenge_id=cid,
                challenger_player_id=challenger.player_id,
                target_player_id=target_player_id,
                config_id=config_id,
                challenger_account=challenger.account,
                challenger_revision=challenger.owner_revision,
                target_account=target_acct,
                target_revision=target_rev,
                created_ms=now,
                expires_ms=now + ttl_ms,
                state=CHALLENGE_STATE_PENDING,
                custom_rules_payload=custom_rules_payload,
            )
            s.challenges[cid] = inv
            return self._clone_msg(inv)

        return await self._run_tx(work)

    async def respond_challenge(
        self,
        responder: CallerIdentity,
        challenge_id: int,
        accept: bool,
        *,
        battle_node: tuple[int, str, str, int, str] | None,
        battle_deadline_ms: int,
        responder_character_preset_id: int = 0,
        responder_pet_preset_id: int = 0,
    ) -> tuple[Message, Message | None]:
        now = await self.now_ms()

        def work(s: _MemState):
            self._check_fence_locked(s, responder.player_id, responder.account, responder.owner_revision)
            inv = s.challenges.get(challenge_id)
            if inv is None or int(inv.state) != CHALLENGE_STATE_PENDING or int(inv.expires_ms) <= now:
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_EXPIRED)
            if int(inv.target_player_id) != responder.player_id:
                raise MatchBusinessError(ERR_MATCH_CHALLENGE_NOT_TARGET)
            if not accept:
                inv.state = CHALLENGE_STATE_DECLINED
                return self._clone_msg(inv), None

            for pid, err_code in (
                (responder.player_id, ERR_MATCH_CHALLENGE_SELF_BUSY),
                (int(inv.challenger_player_id), ERR_MATCH_CHALLENGE_TARGET_BUSY),
            ):
                tk = s.tickets.get(pid)
                if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                    raise MatchBusinessError(err_code)
            if battle_node is None:
                raise MatchBusinessError(ERR_MATCH_MODE_NOT_OPEN)

            b_node_id, b_uuid, b_endpoint, b_region, b_track = battle_node
            battle_id = self._alloc_id_locked()
            op_id = str(uuid.uuid4())
            challenger_id = int(inv.challenger_player_id)
            custom_payload = bytes(getattr(inv, "custom_rules_payload", b""))
            ranked_ctx_bytes = b""
            if custom_payload:
                rules = self.protocol.parse("CustomDuelRules", custom_payload)
                if bool(rules.use_free_preset):
                    # 公平模板切磋：season_id=0 保证绝不计排位分、不进 Elo 榜。
                    rctx = self.protocol.message(
                        "RankedMatchContext",
                        season_id=0,
                        rule_version=0,
                        use_free_preset=True,
                    )
                    m_challenger = rctx.members.add(player_id=challenger_id)
                    m_challenger.presets.character_preset_id = (
                        int(rules.challenger_character_preset_id) or DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID
                    )
                    m_challenger.presets.pet_preset_id = (
                        int(rules.challenger_pet_preset_id) or DEFAULT_CUSTOM_DUEL_PET_PRESET_ID
                    )
                    m_responder = rctx.members.add(player_id=responder.player_id)
                    m_responder.presets.character_preset_id = (
                        int(responder_character_preset_id) or DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID
                    )
                    m_responder.presets.pet_preset_id = (
                        int(responder_pet_preset_id) or DEFAULT_CUSTOM_DUEL_PET_PRESET_ID
                    )
                    ranked_ctx_bytes = rctx.SerializeToString(deterministic=True)

            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id,
                operation_id=op_id,
                state=FORMATION_STATE_PREPARING,
                physical_region=b_region,
                target_node_id=b_node_id,
                target_instance_id=b_uuid,
                target_endpoint=b_endpoint,
                mode=MODE_PVP_CHALLENGE,
                config_id=int(inv.config_id),
                created_ms=now,
                deadline_ms=now + battle_deadline_ms,
                retry_at_ms=now + 60_000,
                requeue_on_failure=False,
                release_track=b_track,
                ranked_context=ranked_ctx_bytes,
                ranked_season_id=0,
                custom_duel_rules=custom_payload,
            )
            for pid, side, acct, rev in (
                (challenger_id, 0, inv.challenger_account, int(inv.challenger_revision)),
                (responder.player_id, 1, responder.account, responder.owner_revision),
            ):
                tk_id = f"ch-{battle_id}-{pid}"
                formation.members.add(player_id=pid, ticket=tk_id, team_index=side)
                s.tickets[pid] = self.protocol.message(
                    "match.MatchAuthorityTicketRecord",
                    player_id=pid,
                    ticket=tk_id,
                    request_id=op_id,
                    mode=MODE_PVP_CHALLENGE,
                    config_id=int(inv.config_id),
                    team_size=1,
                    state=TICKET_STATE_FORMING,
                    enqueued_ms=now,
                    formation_id=battle_id,
                    expires_ms=now + battle_deadline_ms,
                    selected_region=b_region,
                    owner_account=acct,
                    owner_revision=rev,
                )
            inv.state = CHALLENGE_STATE_FORMED
            inv.battle_id = battle_id
            s.formations[battle_id] = formation
            return self._clone_msg(inv), self._clone_msg(formation)

        return await self._run_tx(work)

    async def claim_formation(
        self,
        *,
        mode: int,
        config_id: int,
        members: list[tuple[int, str, int, bool]],
        physical_region: int,
        target_node_id: int,
        target_instance_id: str,
        target_endpoint: str,
        release_track: str,
        battle_deadline_ms: int,
        requeue_on_failure: bool,
        activity_context: bytes = b"",
        team_launch_keys: list[tuple[int, int, int]] | None = None,
        tournament_context: bytes = b"",
        custom_duel_rules: bytes = b"",
        is_wuxing_tower: bool = False,
        ranked_context: bytes = b"",
        ranked_season_id: int = 0,
    ) -> Message:
        now = await self.now_ms()

        def work(s: _MemState) -> Message:
            tower_flag = bool(is_wuxing_tower)
            for pid, ticket_str, _, _ in members:
                tk = s.tickets.get(pid)
                if tk is None or tk.ticket != ticket_str or int(tk.state) != TICKET_STATE_QUEUED:
                    raise StoreUnavailableError(f"ticket not claimable player={pid}")
                if mode == MODE_PVE_SOLO and pid in s.tower_tickets:
                    tower_flag = True
            battle_id = self._alloc_id_locked()
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id,
                operation_id=op_id,
                state=FORMATION_STATE_PREPARING,
                physical_region=physical_region,
                target_node_id=target_node_id,
                target_instance_id=target_instance_id,
                target_endpoint=target_endpoint,
                mode=mode,
                config_id=config_id,
                created_ms=now,
                deadline_ms=now + battle_deadline_ms,
                retry_at_ms=now + 60_000,
                activity_context=activity_context,
                requeue_on_failure=requeue_on_failure,
                release_track=release_track,
                ranked_context=ranked_context,
                ranked_season_id=ranked_season_id,
                tournament_context=tournament_context,
                custom_duel_rules=custom_duel_rules,
                is_wuxing_tower=tower_flag,
            )
            for pid, ticket_str, side, captain in members:
                formation.members.add(player_id=pid, ticket=ticket_str, team_index=side, captain=captain)
                tk = s.tickets[pid]
                tk.state = TICKET_STATE_FORMING
                tk.formation_id = battle_id
                tk.selected_region = physical_region
                s.tower_tickets.discard(pid)
            for key in team_launch_keys or ():
                launch = s.team_launches.get(key)
                if launch is not None:
                    launch.state = TEAM_LAUNCH_FORMING
                    launch.battle_id = battle_id
                    for (a, h, l, pid), _ in list(s.team_launch_roles.items()):
                        if (a, h, l) == key:
                            s.team_launch_roles[(a, h, l, pid)] = TEAM_LAUNCH_FORMING
            s.formations[battle_id] = formation
            return self._clone_msg(formation)

        return await self._run_tx(work)

    async def mark_formation_prepared(self, battle_id: int, player_id: int, endpoint: str) -> None:
        def work(s: _MemState):
            f = s.formations.get(battle_id)
            if f is None:
                return
            for p in f.prepared:
                if int(p.player_id) == player_id:
                    p.endpoint = endpoint
                    return
            f.prepared.add(player_id=player_id, endpoint=endpoint)

        await self._run_tx(work)

    async def save_formation_intent(
        self, battle_id: int, create_request_bytes: bytes, preflight_instance_id: str
    ) -> Message:
        def work(s: _MemState) -> Message:
            f = s.formations.get(battle_id)
            if f is None or int(f.state) != FORMATION_STATE_PREPARING:
                raise StoreUnavailableError(f"formation not in preparing state battle={battle_id}")
            candidate = self._clone_msg(f)
            candidate.create_request = create_request_bytes
            candidate.outcome_protocol_version = 1
            candidate.preflight_supported_version = 1
            candidate.preflight_instance_id = preflight_instance_id
            try:
                completable_original(self.protocol, candidate)
            except ValueError as exc:
                raise UnterminableOriginalError(str(exc)) from exc
            f.CopyFrom(candidate)
            f.state = FORMATION_STATE_PERSISTED
            return self._clone_msg(f)

        return await self._run_tx(work)

    async def mark_formation_created(self, battle_id: int, adopted_version: int) -> Message:
        def work(s: _MemState) -> Message:
            f = s.formations.get(battle_id)
            if f is None:
                raise StoreUnavailableError(f"formation missing battle={battle_id}")
            if int(f.state) != FORMATION_STATE_ABORTED:
                f.state = FORMATION_STATE_CREATED
                f.adopted_outcome_protocol_version = adopted_version
                f.adopted_outcome_observed = True
                for m in f.members:
                    tk = s.tickets.get(int(m.player_id))
                    if tk is not None and int(tk.formation_id) == battle_id and int(tk.state) == TICKET_STATE_FORMING:
                        tk.state = TICKET_STATE_READY
                self._sync_team_launch_state(s, battle_id, TEAM_LAUNCH_CREATED)
            return self._clone_msg(f)

        return await self._run_tx(work)

    async def mark_formation_unknown(self, battle_id: int) -> None:
        def work(s: _MemState):
            f = s.formations.get(battle_id)
            if f is not None and int(f.state) in (FORMATION_STATE_PREPARING, FORMATION_STATE_PERSISTED):
                f.state = FORMATION_STATE_UNKNOWN

        await self._run_tx(work)

    async def abort_formation_pre_persist(self, battle_id: int, culprit_player_id: int = 0) -> None:
        def work(s: _MemState):
            f = s.formations.get(battle_id)
            if f is None:
                return
            f.state = FORMATION_STATE_ABORTED
            for m in f.members:
                pid = int(m.player_id)
                tk = s.tickets.get(pid)
                if tk is None or int(tk.formation_id) != battle_id:
                    continue
                if f.requeue_on_failure and pid != culprit_player_id:
                    tk.state = TICKET_STATE_QUEUED
                    tk.formation_id = 0
                else:
                    tk.state = TICKET_STATE_DONE
            self._close_team_launch_by_battle(s, battle_id)

        await self._run_tx(work)

    def _sync_team_launch_state(self, s: _MemState, battle_id: int, new_state: int) -> None:
        for key, launch in s.team_launches.items():
            if int(launch.battle_id) == battle_id and int(launch.state) != TEAM_LAUNCH_CLOSED:
                launch.state = new_state
                for (a, h, l, pid), _ in list(s.team_launch_roles.items()):
                    if (a, h, l) == key:
                        s.team_launch_roles[(a, h, l, pid)] = new_state

    def _close_team_launch_by_battle(self, s: _MemState, battle_id: int) -> None:
        for key, launch in s.team_launches.items():
            if int(launch.battle_id) == battle_id:
                launch.state = TEAM_LAUNCH_CLOSED
                for (a, h, l, pid), _ in list(s.team_launch_roles.items()):
                    if (a, h, l) == key:
                        s.team_launch_roles[(a, h, l, pid)] = TEAM_LAUNCH_CLOSED
                row = s.teams.get(int(launch.team_id))
                if row is not None and row.data.record.match_lock_token == launch.lock_token:
                    row.data.record.match_lock_token = ""
                    row.data.record.match_lock_expire_at_ms = 0
                    del row.data.record.match_lock_roster[:]

    async def get_formation(self, battle_id: int) -> Message | None:
        async with self._lock:
            return self._clone_msg(self._state.formations.get(battle_id))

    async def list_active_formations(
        self, *, release_track: str = "stable", limit: int = 50
    ) -> list[tuple[Message, bool]]:
        now = await self.now_ms()
        async with self._lock:
            items: list[tuple[Message, bool]] = []
            for bid, f in self._state.formations.items():
                if int(f.state) != FORMATION_STATE_CREATED or formation_outcome_decided(f):
                    continue
                if int(f.deadline_ms) <= now:
                    continue
                track = f.release_track or "stable"
                if release_track == "stable" and track != "stable":
                    continue
                items.append((self._clone_msg(f), bid in self._state.featured_battles))
            items.sort(key=lambda pair: (not pair[1], -int(pair[0].created_ms), int(pair[0].battle_id)))
            return items[:limit]

    async def set_formation_featured(self, battle_id: int, featured: bool) -> bool:
        def work(s: _MemState) -> bool:
            f = s.formations.get(battle_id)
            if f is None or int(f.state) != FORMATION_STATE_CREATED or formation_outcome_decided(f):
                return False
            if featured:
                s.featured_battles.add(battle_id)
            else:
                s.featured_battles.discard(battle_id)
            return True

        return await self._run_tx(work)

    # -- Trial Occupancy & Activity Battles -----------------------------------------------------------

    async def acquire_trial_occupancy(
        self, owner_id: int, roster: list[tuple[int, str, int]], ttl_ms: int
    ) -> tuple[int, int]:
        now = await self.now_ms()

        def work(s: _MemState) -> tuple[int, int]:
            for pid, acct, rev in roster:
                try:
                    self._check_fence_locked(s, pid, acct, rev)
                except OriginalRoleError:
                    return 2, pid  # TRIAL_OCCUPANCY_REJECT_ROLE
                tk = s.tickets.get(pid)
                if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                    return 1, pid  # OCCUPIED
                occ = s.occupancies.get(pid)
                if occ is not None and occ.expires_ms > now and (
                    occ.owner_kind != OCCUPANCY_KIND_GUILD_TRIAL or occ.owner_id != owner_id
                ):
                    return 1, pid
            exp = now + ttl_ms
            for pid, acct, rev in roster:
                prev = s.occupancies.get(pid)
                final_exp = max(prev.expires_ms, exp) if (prev is not None and prev.owner_id == owner_id) else exp
                fence = s.role_fences.get(pid)
                use_acct = acct or (fence.account if fence is not None else f"acct:{pid}")
                use_rev = rev or (fence.owner_revision if fence is not None else 1)
                s.occupancies[pid] = _MemOccupancyRow(
                    player_id=pid,
                    owner_kind=OCCUPANCY_KIND_GUILD_TRIAL,
                    owner_id=owner_id,
                    account=use_acct,
                    owner_revision=use_rev,
                    expires_ms=final_exp,
                )
            return 0, 0

        return await self._run_tx(work)

    async def release_trial_occupancy(self, owner_id: int, player_ids: list[int]) -> None:
        def work(s: _MemState):
            for pid in player_ids:
                occ = s.occupancies.get(pid)
                if occ is not None and occ.owner_kind == OCCUPANCY_KIND_GUILD_TRIAL and occ.owner_id == owner_id:
                    s.occupancies.pop(pid, None)

        await self._run_tx(work)

    async def start_activity_battle(
        self,
        *,
        request_id: int,
        battle_config_id: int,
        member_player_ids: list[int],
        activity_context_bytes: bytes,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]:
        now = await self.now_ms()

        def work(s: _MemState):
            prev = s.activity_intents.get(request_id)
            if prev is not None:
                bid, cancelled = prev
                if cancelled:
                    raise MatchBusinessError(ERR_MATCH_CANCEL_TOO_LATE)
                return self._clone_msg(s.formations[bid]), False
            for pid in member_player_ids:
                tk = s.tickets.get(pid)
                if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                    raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_READY, pid)
            b_node_id, b_uuid, b_endpoint, b_region, b_track = battle_node
            battle_id = self._alloc_id_locked()
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id,
                operation_id=op_id,
                state=FORMATION_STATE_PREPARING,
                physical_region=b_region,
                target_node_id=b_node_id,
                target_instance_id=b_uuid,
                target_endpoint=b_endpoint,
                mode=MODE_PVE_TEAM,
                config_id=battle_config_id,
                created_ms=now,
                deadline_ms=now + battle_deadline_ms,
                retry_at_ms=now + 60_000,
                activity_context=activity_context_bytes,
                requeue_on_failure=False,
                release_track=b_track,
            )
            for idx, pid in enumerate(member_player_ids):
                tk_id = f"act-{battle_id}-{pid}"
                formation.members.add(player_id=pid, ticket=tk_id, team_index=0, captain=(idx == 0))
                occ = s.occupancies.get(pid)
                fence = s.role_fences.get(pid)
                acct = occ.account if occ else (fence.account if fence else f"acct:{pid}")
                rev = occ.owner_revision if occ else (fence.owner_revision if fence else 1)
                s.tickets[pid] = self.protocol.message(
                    "match.MatchAuthorityTicketRecord",
                    player_id=pid,
                    ticket=tk_id,
                    request_id=op_id,
                    mode=MODE_PVE_TEAM,
                    config_id=battle_config_id,
                    team_size=len(member_player_ids),
                    state=TICKET_STATE_FORMING,
                    enqueued_ms=now,
                    formation_id=battle_id,
                    expires_ms=now + battle_deadline_ms,
                    selected_region=b_region,
                    owner_account=acct,
                    owner_revision=rev,
                )
            s.formations[battle_id] = formation
            s.activity_intents[request_id] = (battle_id, False)
            return self._clone_msg(formation), True

        return await self._run_tx(work)

    async def active_battle_id_of(self, player_id: int) -> int:
        async with self._lock:
            tk = self._state.tickets.get(int(player_id))
            if tk is not None and int(tk.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY):
                return int(tk.formation_id)
            return 0

    async def start_guild_war_battle(
        self,
        *,
        request_id: int,
        battle_config_id: int,
        match_mode: int,
        attacker_player_ids: list[int],
        defender_player_ids: list[int],
        activity_context_bytes: bytes,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]:
        """帮会全天帮战 V2 无匹配池直打：按 request_id 幂等，对全部真人参与者原子抢占票据。"""
        now = await self.now_ms()
        all_humans = list(attacker_player_ids) + list(defender_player_ids)

        def work(s: _MemState):
            prev_bid = s.guild_war_intents.get(request_id)
            if prev_bid is not None:
                return self._clone_msg(s.formations[prev_bid]), False
            for pid in all_humans:
                tk = s.tickets.get(pid)
                if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                    in_battle = int(tk.state) in (TICKET_STATE_FORMING, TICKET_STATE_READY) and int(tk.formation_id) > 0
                    raise GuildWarMemberOccupiedError(
                        pid,
                        int(tk.formation_id) if in_battle else 0,
                        in_battle=in_battle,
                    )
            b_node_id, b_uuid, b_endpoint, b_region, b_track = battle_node
            battle_id = self._alloc_id_locked()
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id,
                operation_id=op_id,
                state=FORMATION_STATE_PREPARING,
                physical_region=b_region,
                target_node_id=b_node_id,
                target_instance_id=b_uuid,
                target_endpoint=b_endpoint,
                mode=match_mode,
                config_id=battle_config_id,
                created_ms=now,
                deadline_ms=now + battle_deadline_ms,
                retry_at_ms=now + 60_000,
                activity_context=activity_context_bytes,
                requeue_on_failure=False,
                release_track=b_track,
            )
            for team_idx, side in ((0, attacker_player_ids), (1, defender_player_ids)):
                for idx, pid in enumerate(side):
                    tk_id = f"gwar-{battle_id}-{pid}"
                    formation.members.add(player_id=pid, ticket=tk_id, team_index=team_idx, captain=(idx == 0))
                    occ = s.occupancies.get(pid)
                    fence = s.role_fences.get(pid)
                    acct = occ.account if occ else (fence.account if fence else f"acct:{pid}")
                    rev = occ.owner_revision if occ else (fence.owner_revision if fence else 1)
                    s.tickets[pid] = self.protocol.message(
                        "match.MatchAuthorityTicketRecord",
                        player_id=pid,
                        ticket=tk_id,
                        request_id=op_id,
                        mode=match_mode,
                        config_id=battle_config_id,
                        team_size=len(side),
                        state=TICKET_STATE_FORMING,
                        enqueued_ms=now,
                        formation_id=battle_id,
                        expires_ms=now + battle_deadline_ms,
                        selected_region=b_region,
                        owner_account=acct,
                        owner_revision=rev,
                    )
            s.formations[battle_id] = formation
            s.guild_war_intents[request_id] = battle_id
            return self._clone_msg(formation), True

        return await self._run_tx(work)

    async def start_festival_beast_battle(
        self,
        *,
        owner_id: int,
        request_high: int,
        request_low: int,
        payload_bytes: bytes,
        battle_config_id: int,
        activity_context_bytes: bytes,
        effective_start_at_ms: int,
        effective_end_at_ms: int,
        battle_node: tuple[int, str, str, int, str],
        battle_deadline_ms: int,
    ) -> tuple[Message, bool]:
        """瑞兽迎春 Kind=5 持久直入准入：事务提交点复核 [start, end) 窗口与 UUID 正文幂等。"""
        now = await self.now_ms()
        key = (5, int(owner_id), int(request_high), int(request_low))

        def work(s: _MemState):
            prev = s.direct_intents.get(key)
            if prev is not None:
                bid, orig_payload, cancelled = prev
                if orig_payload != payload_bytes:
                    raise ActivityIntentConflictError("festival beast intent payload mismatch")
                if cancelled:
                    raise MatchBusinessError(ERR_MATCH_CANCEL_TOO_LATE)
                return self._clone_msg(s.formations[bid]), False
            if (
                effective_start_at_ms == 0
                or effective_end_at_ms <= effective_start_at_ms
                or now < effective_start_at_ms
                or now >= effective_end_at_ms
            ):
                raise ActivityOutsideWindowError("outside festival beast open window")
            tk = s.tickets.get(owner_id)
            if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                raise TeamBusinessError(ERR_TEAM_MEMBER_NOT_READY, owner_id)
            b_node_id, b_uuid, b_endpoint, b_region, b_track = battle_node
            battle_id = self._alloc_id_locked()
            op_id = str(uuid.uuid4())
            formation = self.protocol.message(
                "match.MatchFormationRecord",
                battle_id=battle_id,
                operation_id=op_id,
                state=FORMATION_STATE_PREPARING,
                physical_region=b_region,
                target_node_id=b_node_id,
                target_instance_id=b_uuid,
                target_endpoint=b_endpoint,
                mode=MODE_PVE_SOLO,
                config_id=battle_config_id,
                created_ms=now,
                deadline_ms=now + battle_deadline_ms,
                retry_at_ms=now + 60_000,
                activity_context=activity_context_bytes,
                requeue_on_failure=False,
                release_track=b_track,
            )
            tk_id = f"fb-{battle_id}-{owner_id}"
            formation.members.add(player_id=owner_id, ticket=tk_id, team_index=0, captain=True)
            occ = s.occupancies.get(owner_id)
            fence = s.role_fences.get(owner_id)
            acct = occ.account if occ else (fence.account if fence else f"acct:{owner_id}")
            rev = occ.owner_revision if occ else (fence.owner_revision if fence else 1)
            s.tickets[owner_id] = self.protocol.message(
                "match.MatchAuthorityTicketRecord",
                player_id=owner_id,
                ticket=tk_id,
                request_id=op_id,
                mode=MODE_PVE_SOLO,
                config_id=battle_config_id,
                team_size=1,
                state=TICKET_STATE_FORMING,
                enqueued_ms=now,
                formation_id=battle_id,
                expires_ms=now + battle_deadline_ms,
                selected_region=b_region,
                owner_account=acct,
                owner_revision=rev,
            )
            s.formations[battle_id] = formation
            s.direct_intents[key] = (battle_id, payload_bytes, False)
            return self._clone_msg(formation), True

        return await self._run_tx(work)

    async def cancel_activity_battle(self, request_id: int) -> tuple[int, bool, int, int]:
        def work(s: _MemState):
            prev = s.activity_intents.get(request_id)
            if prev is None:
                s.activity_intents[request_id] = (0, True)
                return 0, True, 0, OUTCOME_DECISION_UNKNOWN
            bid, cancelled = prev
            if cancelled and bid == 0:
                return 0, True, 0, OUTCOME_DECISION_UNKNOWN
            f = s.formations.get(bid)
            comp = s.completions.get(bid)
            decision = comp.decision if comp is not None else OUTCOME_DECISION_UNKNOWN
            return bid, False, int(f.state) if f is not None else 0, decision

        return await self._run_tx(work)

    # -- Battle Outcome, Terminal Decision & Outbox ---------------------------------------------------

    async def commit_battle_outcome(self, req: Message) -> int:
        battle_id = int(req.battle_id)
        if not battle_id or not req.placement_operation_id or not req.target_instance_id:
            raise BattleOutcomeOriginalError("incomplete CommitBattleOutcomeRequest")
        raw_payload = bytes(req.raw_result_payload)
        try:
            event = validate_terminal_payload(self.protocol, raw_payload)
            orig_err = None
        except ValueError as exc:
            event = None
            orig_err = exc
        now = await self.now_ms()
        digest = hashlib.sha256(raw_payload).digest()

        def work(s: _MemState) -> int:
            old = s.completions.get(battle_id)
            if old is not None and old.decision != OUTCOME_DECISION_UNKNOWN:
                return self._replay_terminal(old, req, raw_payload, digest)
            if orig_err is not None or event is None:
                raise BattleOutcomeOriginalError(str(orig_err))
            f = s.formations.get(battle_id)
            if (
                f is None
                or f.operation_id != req.placement_operation_id
                or f.target_instance_id != req.target_instance_id
                or int(f.state) == FORMATION_STATE_ABORTED
            ):
                raise BattleOutcomeOriginalError("formation identity or state mismatch")
            try:
                create_req = validate_terminal_against_formation(self.protocol, f, event)
            except ValueError as exc:
                raise BattleOutcomeOriginalError(str(exc)) from exc

            owners: dict[int, tuple[str, int, str]] = {}
            for m in f.members:
                pid = int(m.player_id)
                tk = s.tickets.get(pid)
                if (
                    tk is None
                    or tk.ticket != m.ticket
                    or int(tk.formation_id) != battle_id
                    or int(tk.state) not in (TICKET_STATE_FORMING, TICKET_STATE_READY)
                    or not tk.owner_account
                    or int(tk.owner_revision) == 0
                ):
                    raise BattleOutcomeOriginalError(f"original ticket invalid player={pid}")
                self._check_fence_locked(s, pid, tk.owner_account, int(tk.owner_revision))
                owners[pid] = (tk.owner_account, int(tk.owner_revision), tk.ticket)

            s.completions[battle_id] = _MemCompletionRow(
                battle_id=battle_id,
                raw_payload=raw_payload,
                payload_digest=digest,
                format_version=1,
                archived_ms=now,
                decision=OUTCOME_DECISION_COMPLETE,
                source_kind=COMPLETION_SOURCE_BATTLE_RPC,
                placement_operation_id=f.operation_id,
                target_instance_id=f.target_instance_id,
                outcome_protocol_version=1,
                result_published=False,
                next_retry_ms=now,
            )
            for settlement in event.settlements:
                pid = int(settlement.player_id)
                body = settlement.SerializeToString(deterministic=True)
                p_digest = hashlib.sha256(body).digest()
                acct, rev, _ = owners[pid]
                s.settlements[(battle_id, pid)] = _MemSettlementRow(
                    battle_id=battle_id,
                    player_id=pid,
                    settlement_proto=body,
                    payload_digest=p_digest,
                    archived_ms=now,
                    owner_account=acct,
                    owner_revision=rev,
                    issued_at_ms=now,
                    inbox_seq=0,
                    inbox_state=0,
                    dispatch_state=SETTLEMENT_DISPATCH_PENDING,
                    next_retry_ms=now,
                )

            # 写入战报回放（如果携带 replay 且为 PVP）
            self._write_battle_report_locked(s, f, create_req, event, req, now)

            f.state = FORMATION_STATE_CREATED
            f.retry_at_ms = TERMINAL_OUTCOME_NO_RETRY_MS
            for pid, (_, _, tk_id) in owners.items():
                tk = s.tickets[pid]
                if tk.ticket == tk_id and int(tk.state) == TICKET_STATE_FORMING:
                    tk.state = TICKET_STATE_READY
            return OUTCOME_DECISION_COMPLETE

        return await self._run_tx(work)

    def _replay_terminal(
        self, old: _MemCompletionRow, req: Message, raw_payload: bytes, digest: bytes
    ) -> int:
        if old.decision == OUTCOME_DECISION_ABORT:
            return OUTCOME_DECISION_ABORT
        if (
            old.decision == OUTCOME_DECISION_COMPLETE
            and old.placement_operation_id == req.placement_operation_id
            and old.target_instance_id == req.target_instance_id
            and old.raw_payload == raw_payload
            and old.payload_digest == digest
        ):
            return OUTCOME_DECISION_COMPLETE
        raise BattleOutcomeOriginalError("replayed outcome differs from committed COMPLETE original")

    def _write_battle_report_locked(
        self, s: _MemState, f: Message, create_req: Message, event: Message, req: Message, now: int
    ) -> None:
        has_replay = req.HasField("replay") and bool(req.replay.payload)
        member_ids = [int(p.player_id) for p in create_req.players]
        if has_replay:
            env_bytes = req.replay.SerializeToString(deterministic=True)
            s.reports[int(f.battle_id)] = _MemReportRow(
                battle_id=int(f.battle_id),
                created_ms=now,
                match_mode=int(f.mode),
                battle_config_id=int(f.config_id),
                envelope=env_bytes,
                envelope_digest=hashlib.sha256(env_bytes).digest(),
                ref_count=len(member_ids),
            )
        summary_bytes = build_battle_report_summary(
            self.protocol, create_req, event, has_replay=has_replay
        ).SerializeToString(deterministic=True)
        for pid in member_ids:
            entries = s.player_reports.setdefault(pid, [])
            entries.append((now, int(f.battle_id), summary_bytes))
            entries.sort(key=lambda item: (-item[0], -item[1]))
            while len(entries) > MAX_BATTLE_REPORTS:
                _, evicted_bid, _ = entries.pop()
                rep = s.reports.get(evicted_bid)
                if rep is not None:
                    rep.ref_count -= 1
                    if rep.ref_count <= 0:
                        s.reports.pop(evicted_bid, None)

    async def commit_battle_abort(
        self, battle_id: int, source_kind: int, *, manual_audit: dict | None = None
    ) -> int:
        now = await self.now_ms()

        def work(s: _MemState) -> int:
            old = s.completions.get(battle_id)
            if old is not None and old.decision != OUTCOME_DECISION_UNKNOWN:
                return old.decision
            f = s.formations.get(battle_id)
            if f is None:
                raise StoreUnavailableError(f"formation missing battle={battle_id}")
            s.completions[battle_id] = _MemCompletionRow(
                battle_id=battle_id,
                raw_payload=b"",
                payload_digest=b"",
                format_version=0,
                archived_ms=now,
                decision=OUTCOME_DECISION_ABORT,
                source_kind=source_kind,
                placement_operation_id=f.operation_id,
                target_instance_id=f.target_instance_id,
                outcome_protocol_version=int(f.outcome_protocol_version),
                result_published=True,
                next_retry_ms=0,
            )
            f.state = FORMATION_STATE_ABORTED
            f.retry_at_ms = TERMINAL_OUTCOME_NO_RETRY_MS
            for m in f.members:
                tk = s.tickets.get(int(m.player_id))
                if tk is not None and int(tk.formation_id) == battle_id:
                    tk.state = TICKET_STATE_DONE
            self._close_team_launch_by_battle(s, battle_id)
            if manual_audit is not None:
                s.manual_resolutions[battle_id] = dict(manual_audit, resolved_ms=now)
            return OUTCOME_DECISION_ABORT

        return await self._run_tx(work)

    async def get_battle_completion(self, battle_id: int) -> tuple[int, bytes]:
        async with self._lock:
            row = self._state.completions.get(battle_id)
            if row is None or row.decision == OUTCOME_DECISION_UNKNOWN:
                return OUTCOME_DECISION_UNKNOWN, b""
            return row.decision, row.raw_payload if row.decision == OUTCOME_DECISION_COMPLETE else b""

    async def get_battle_terminal_state(self, battle_id: int) -> tuple[int, bool]:
        async with self._lock:
            row = self._state.completions.get(battle_id)
            if row is not None and row.decision != OUTCOME_DECISION_UNKNOWN:
                return row.decision, False
            f = self._state.formations.get(battle_id)
            if f is None or int(f.state) not in (FORMATION_STATE_PERSISTED, FORMATION_STATE_UNKNOWN, FORMATION_STATE_CREATED):
                return OUTCOME_DECISION_UNKNOWN, False
            try:
                completable_original(self.protocol, f)
                return OUTCOME_DECISION_UNKNOWN, True
            except ValueError:
                return OUTCOME_DECISION_UNKNOWN, False

    async def due_outcome_results(self, stamp_ms: int, limit: int) -> list[CompletionOutboxRow]:
        async with self._lock:
            rows = [
                CompletionOutboxRow(r.battle_id, r.raw_payload, r.payload_digest, r.next_retry_ms)
                for r in self._state.completions.values()
                if r.decision == OUTCOME_DECISION_COMPLETE and not r.result_published and r.next_retry_ms <= stamp_ms
            ]
            rows.sort(key=lambda r: (r.next_retry_ms, r.battle_id))
            return rows[:limit]

    async def claim_outcome_result(self, battle_id: int, prev_retry_ms: int, lease_ms: int) -> bool:
        def work(s: _MemState) -> bool:
            r = s.completions.get(battle_id)
            if r is None or r.decision != OUTCOME_DECISION_COMPLETE or r.result_published or r.next_retry_ms != prev_retry_ms:
                return False
            r.next_retry_ms = lease_ms
            return True

        return await self._run_tx(work)

    async def mark_outcome_result_published(self, battle_id: int, lease_ms: int) -> None:
        def work(s: _MemState):
            r = s.completions.get(battle_id)
            if r is not None and r.decision == OUTCOME_DECISION_COMPLETE and not r.result_published and r.next_retry_ms == lease_ms:
                r.result_published = True

        await self._run_tx(work)

    async def due_settlement_sources(
        self, stamp_ms: int, first_limit: int, waiting_limit: int
    ) -> list[SettlementOutboxRow]:
        async with self._lock:
            first = [
                SettlementOutboxRow(
                    r.battle_id, r.player_id, r.settlement_proto, r.payload_digest, r.owner_account,
                    r.owner_revision, r.issued_at_ms, r.inbox_seq, r.inbox_state, r.dispatch_state, r.next_retry_ms,
                )
                for r in self._state.settlements.values()
                if r.dispatch_state == SETTLEMENT_DISPATCH_PENDING and r.inbox_seq == 0 and r.next_retry_ms <= stamp_ms
            ]
            first.sort(key=lambda r: (r.next_retry_ms, r.battle_id, r.player_id))
            waiting = [
                SettlementOutboxRow(
                    r.battle_id, r.player_id, r.settlement_proto, r.payload_digest, r.owner_account,
                    r.owner_revision, r.issued_at_ms, r.inbox_seq, r.inbox_state, r.dispatch_state, r.next_retry_ms,
                )
                for r in self._state.settlements.values()
                if r.dispatch_state == SETTLEMENT_DISPATCH_PENDING and r.inbox_seq > 0 and r.next_retry_ms <= stamp_ms
            ]
            waiting.sort(key=lambda r: (r.next_retry_ms, r.battle_id, r.player_id))
            return first[:first_limit] + waiting[:waiting_limit]

    async def claim_settlement_source(
        self, battle_id: int, player_id: int, prev_retry_ms: int, lease_ms: int
    ) -> bool:
        def work(s: _MemState) -> bool:
            r = s.settlements.get((battle_id, player_id))
            if r is None or r.dispatch_state != SETTLEMENT_DISPATCH_PENDING or r.next_retry_ms != prev_retry_ms:
                return False
            r.next_retry_ms = lease_ms
            return True

        return await self._run_tx(work)

    async def update_settlement_source(
        self,
        battle_id: int,
        player_id: int,
        lease_ms: int,
        *,
        inbox_seq: int,
        inbox_state: int,
        dispatch_state: int,
        next_retry_ms: int,
    ) -> None:
        def work(s: _MemState):
            r = s.settlements.get((battle_id, player_id))
            if (
                r is None
                or r.dispatch_state != SETTLEMENT_DISPATCH_PENDING
                or r.next_retry_ms != lease_ms
                or (r.inbox_seq != 0 and r.inbox_seq != inbox_seq)
            ):
                return
            r.inbox_seq = inbox_seq
            r.inbox_state = inbox_state
            r.dispatch_state = dispatch_state
            r.next_retry_ms = next_retry_ms

        await self._run_tx(work)

    async def release_settled_tickets(self, battle_id: int, player_id: int) -> bool:
        def work(s: _MemState) -> bool:
            row = s.settlements.get((battle_id, player_id))
            if row is None or row.dispatch_state != SETTLEMENT_DISPATCH_APPLIED:
                return False
            tk = s.tickets.get(player_id)
            if (
                tk is None
                or int(tk.formation_id) != battle_id
                or int(tk.state) != TICKET_STATE_READY
                or tk.owner_account != row.owner_account
                or int(tk.owner_revision) != row.owner_revision
            ):
                return False
            tk.state = TICKET_STATE_DONE
            f = s.formations.get(battle_id)
            if f is not None:
                all_done = all(
                    (t := s.tickets.get(int(m.player_id))) is None
                    or int(t.formation_id) != battle_id
                    or int(t.state) == TICKET_STATE_DONE
                    for m in f.members
                )
                if all_done:
                    self._close_team_launch_by_battle(s, battle_id)
            return True

        return await self._run_tx(work)

    # -- Battle Reports -------------------------------------------------------------------------------

    async def list_battle_reports(self, player_id: int) -> list[Message]:
        async with self._lock:
            entries = self._state.player_reports.get(player_id, ())
            return [self.protocol.parse("match.BattleReportSummary", raw) for _, _, raw in entries[:MAX_BATTLE_REPORTS]]

    async def get_battle_report(self, player_id: int, battle_id: int, *, allow_public: bool = False) -> Message:
        async with self._lock:
            if not allow_public:
                entries = self._state.player_reports.get(player_id, ())
                if not any(bid == battle_id for _, bid, _ in entries):
                    raise MatchBusinessError(ERR_MATCH_BATTLE_REPORT_NOT_FOUND)
            rep = self._state.reports.get(battle_id)
            if rep is None or not rep.envelope:
                raise MatchBusinessError(ERR_MATCH_BATTLE_REPORT_NOT_FOUND)
            return self.protocol.parse("BattleReplayEnvelope", rep.envelope)

    # -- Spectator ------------------------------------------------------------------------------------

    async def get_spectator(self, player_id: int) -> Message | None:
        async with self._lock:
            return self._clone_msg(self._state.spectators.get(player_id))

    async def prepare_spectator(
        self,
        caller: CallerIdentity,
        battle_id: int,
        operation_id: str,
        target_endpoint: str,
        target_instance_id: str,
        request_payload: bytes,
        add_request: bytes,
        request_id: str,
    ) -> Message:
        now = await self.now_ms()
        h, l = uuid_to_pair(request_id) if is_valid_uuid(request_id) else (0, 0)

        def work(s: _MemState) -> Message:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            tk = s.tickets.get(caller.player_id)
            if tk is not None and int(tk.state) in (TICKET_STATE_QUEUED, TICKET_STATE_FORMING, TICKET_STATE_READY):
                raise MatchBusinessError(ERR_MATCH_SPECTATE_WHILE_QUEUED)
            rec = self.protocol.message(
                "match.MatchSpectatorRecord",
                player_id=caller.player_id,
                operation_id=operation_id,
                battle_id=battle_id,
                target_endpoint=target_endpoint,
                target_instance_id=target_instance_id,
                state=SPECTATOR_STATE_PENDING_ADD,
                request_payload=request_payload,
                owner_account=caller.account,
                owner_revision=caller.owner_revision,
                add_request=add_request,
                updated_ms=now,
                request_high=h,
                request_low=l,
            )
            s.spectators[caller.player_id] = rec
            return self._clone_msg(rec)

        return await self._run_tx(work)

    async def commit_spectator(self, player_id: int, operation_id: str) -> None:
        now = await self.now_ms()

        def work(s: _MemState):
            rec = s.spectators.get(player_id)
            if rec is not None and rec.operation_id == operation_id:
                rec.state = SPECTATOR_STATE_ACTIVE
                rec.updated_ms = now

        await self._run_tx(work)

    async def close_spectator(self, player_id: int, operation_id: str) -> None:
        now = await self.now_ms()

        def work(s: _MemState):
            rec = s.spectators.get(player_id)
            if rec is not None and (not operation_id or rec.operation_id == operation_id):
                rec.state = SPECTATOR_STATE_CLOSED
                rec.updated_ms = now

        await self._run_tx(work)

    # -- Stranded Formations --------------------------------------------------------------------------

    def _build_stranded_locked(self, s: _MemState, f: Message, now: int, *, include_tickets: bool) -> StrandedFormationEntry:
        f_class, failed_check, create_req = classify_stranded_formation(self.protocol, f)
        eff_dl = effective_deadline_ms(f, create_req)
        comp = s.completions.get(int(f.battle_id))
        has_term = comp is not None
        term_dec = comp.decision if comp is not None else OUTCOME_DECISION_UNKNOWN
        term_src = comp.source_kind if comp is not None else 0
        refusal = ""
        if has_term and term_dec != OUTCOME_DECISION_UNKNOWN:
            refusal = "already_resolved"
        elif f_class == 1:
            refusal = "auto_recoverable"
        elif now < eff_dl + TERMINAL_OUTCOME_GRACE_MS:
            refusal = "deadline_not_elapsed"
        tickets: list[Message] = []
        if include_tickets:
            for m in f.members:
                tk = s.tickets.get(int(m.player_id))
                if tk is not None:
                    tickets.append(self._clone_msg(tk))
        return StrandedFormationEntry(
            formation=self._clone_msg(f),
            formation_class=f_class,
            failed_check=failed_check,
            create_req=create_req,
            effective_deadline_ms=eff_dl,
            has_terminal_row=has_term,
            terminal_decision=term_dec,
            terminal_source_kind=term_src,
            digest=formation_digest(f),
            refusal=refusal,
            tickets=tuple(tickets),
        )

    async def list_stranded_formations(
        self, after_battle_id: int, limit: int
    ) -> tuple[list[StrandedFormationEntry], int, int]:
        now = await self.now_ms()
        async with self._lock:
            s = self._state
            candidates = sorted(
                (
                    f
                    for bid, f in s.formations.items()
                    if bid > after_battle_id
                    and int(f.state) in (FORMATION_STATE_PERSISTED, FORMATION_STATE_UNKNOWN, FORMATION_STATE_CREATED)
                    and f.create_request
                    and not formation_outcome_decided(f)
                ),
                key=lambda x: int(x.battle_id),
            )
            entries: list[StrandedFormationEntry] = []
            for f in candidates:
                entry = self._build_stranded_locked(s, f, now, include_tickets=False)
                if now >= entry.effective_deadline_ms + TERMINAL_OUTCOME_GRACE_MS:
                    entries.append(entry)
                    if len(entries) >= limit + 1:
                        break
            has_more = len(entries) > limit
            page = entries[:limit]
            next_bid = int(page[-1].formation.battle_id) if (has_more and page) else 0
            return page, next_bid, now

    async def inspect_stranded_formation(self, battle_id: int) -> tuple[StrandedFormationEntry | None, int]:
        now = await self.now_ms()
        async with self._lock:
            f = self._state.formations.get(battle_id)
            if f is None:
                return None, now
            return self._build_stranded_locked(self._state, f, now, include_tickets=True), now

    # -- Battle Scripts -------------------------------------------------------------------------------

    async def list_battle_scripts(self, player_id: int) -> list[Message]:
        async with self._lock:
            items = [
                self._clone_msg(rec)
                for rec in self._state.battle_scripts.values()
                if int(rec.owner_player_id) == player_id
            ]
            items.sort(key=lambda r: (-int(r.submitted_ms), int(r.script_id)))
            return items

    async def list_battle_script_loadout(self, player_id: int) -> list[Message]:
        async with self._lock:
            items = [
                self._clone_msg(rec)
                for (pid, _, _), rec in self._state.battle_script_loadouts.items()
                if pid == player_id
            ]
            items.sort(key=lambda r: (int(r.match_mode), int(r.battle_config_id)))
            return items

    async def get_approved_battle_script(self, script_id: int, owner_player_id: int) -> tuple[int, bytes]:
        async with self._lock:
            rec = self._state.battle_scripts.get(script_id)
            if rec is None or int(rec.owner_player_id) != owner_player_id:
                raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND)
            if int(rec.approved_revision) == 0:
                raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED)
            return int(rec.approved_revision), bytes(rec.approved_content)

    async def submit_battle_script(
        self,
        caller: CallerIdentity,
        *,
        script_id: int,
        expected_revision: int,
        content: bytes,
        max_scripts: int,
    ) -> Message:
        now = await self.now_ms()
        new_id = await self.next_id() if script_id == 0 else 0

        def work(s: _MemState) -> Message:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            if script_id == 0:
                owned = sum(1 for r in s.battle_scripts.values() if int(r.owner_player_id) == caller.player_id)
                if owned >= max_scripts:
                    raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_LIMIT_REACHED)
                rec = self.protocol.message(
                    "match.MatchBattleScriptRecord",
                    script_id=new_id,
                    owner_player_id=caller.player_id,
                    revision=1,
                    content=content,
                    review_state=REVIEW_STATE_PENDING,
                    approved_revision=0,
                    approved_content=b"",
                    reject_reason="",
                    submitted_ms=now,
                    reviewed_ms=0,
                )
                s.battle_scripts[new_id] = rec
                return self._clone_msg(rec)

            rec = s.battle_scripts.get(script_id)
            if rec is None or int(rec.owner_player_id) != caller.player_id:
                raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND)
            if int(rec.revision) != expected_revision:
                raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT)
            rec.revision = int(rec.revision) + 1
            rec.content = content
            rec.review_state = REVIEW_STATE_PENDING
            rec.reject_reason = ""
            rec.submitted_ms = now
            return self._clone_msg(rec)

        return await self._run_tx(work)

    async def delete_battle_script(self, caller: CallerIdentity, script_id: int, expected_revision: int) -> None:
        def work(s: _MemState) -> None:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            rec = s.battle_scripts.get(script_id)
            if rec is None or int(rec.owner_player_id) != caller.player_id:
                raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND)
            if int(rec.revision) != expected_revision:
                raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_REVISION_CONFLICT)
            s.battle_scripts.pop(script_id, None)
            for key in [
                k
                for k, lr in s.battle_script_loadouts.items()
                if k[0] == caller.player_id and int(lr.player_script_id) == script_id
            ]:
                s.battle_script_loadouts.pop(key, None)

        await self._run_tx(work)

    async def set_battle_script_loadout(
        self,
        caller: CallerIdentity,
        *,
        match_mode: int,
        battle_config_id: int,
        origin: int,
        official_script_id: int,
        player_script_id: int,
        max_entries: int,
    ) -> list[Message]:
        now = await self.now_ms()

        def work(s: _MemState) -> list[Message]:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            key = (caller.player_id, match_mode, battle_config_id)
            if origin == SCRIPT_ORIGIN_UNSPECIFIED:
                s.battle_script_loadouts.pop(key, None)
            else:
                if origin == SCRIPT_ORIGIN_PLAYER:
                    rec = s.battle_scripts.get(player_script_id)
                    if rec is None or int(rec.owner_player_id) != caller.player_id:
                        raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_NOT_FOUND)
                    if int(rec.approved_revision) == 0:
                        raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_NOT_APPROVED)
                others = sum(
                    1
                    for k in s.battle_script_loadouts
                    if k[0] == caller.player_id and k != key
                )
                if others >= max_entries:
                    raise MatchBusinessError(ERR_MATCH_BATTLE_SCRIPT_LOADOUT_FULL)
                s.battle_script_loadouts[key] = self.protocol.message(
                    "match.MatchBattleScriptLoadoutRecord",
                    player_id=caller.player_id,
                    match_mode=match_mode,
                    battle_config_id=battle_config_id,
                    origin=origin,
                    official_script_id=official_script_id,
                    player_script_id=player_script_id,
                    updated_ms=now,
                )
            items = [
                self._clone_msg(lr)
                for (pid, _, _), lr in s.battle_script_loadouts.items()
                if pid == caller.player_id
            ]
            items.sort(key=lambda r: (int(r.match_mode), int(r.battle_config_id)))
            return items

        return await self._run_tx(work)

    async def list_pending_battle_scripts(self, limit: int) -> list[Message]:
        async with self._lock:
            items = [
                self._clone_msg(rec)
                for rec in self._state.battle_scripts.values()
                if int(rec.review_state) == REVIEW_STATE_PENDING
            ]
            items.sort(key=lambda r: (int(r.submitted_ms), int(r.script_id)))
            return items[:limit]

    async def review_battle_script(
        self,
        *,
        script_id: int,
        revision: int,
        approve: bool,
        reviewer: str,
        reason: str,
        validate: Callable[[bytes], None] | None = None,
    ) -> None:
        now = await self.now_ms()

        def work(s: _MemState) -> None:
            rec = s.battle_scripts.get(script_id)
            if rec is None or int(rec.revision) != revision or int(rec.review_state) != REVIEW_STATE_PENDING:
                raise BattleScriptReviewError(REVIEW_REJECT_STALE, "script revision is stale or not pending")
            if approve and validate is not None:
                try:
                    validate(bytes(rec.content))
                except Exception as exc:
                    raise BattleScriptReviewError(REVIEW_REJECT_INVALID, str(exc)) from exc
            if approve:
                rec.review_state = REVIEW_STATE_APPROVED
                rec.approved_revision = int(rec.revision)
                rec.approved_content = bytes(rec.content)
                rec.reject_reason = ""
            else:
                rec.review_state = REVIEW_STATE_REJECTED
                rec.reject_reason = reason
            rec.reviewed_ms = now
            s.battle_script_reviews[(script_id, revision)] = self.protocol.message(
                "match.MatchBattleScriptReviewRecord",
                script_id=script_id,
                revision=revision,
                owner_player_id=int(rec.owner_player_id),
                approved=approve,
                reviewer=reviewer,
                reason=reason,
                reviewed_ms=now,
                content=bytes(rec.content),
            )

        await self._run_tx(work)

    # -- Elo Rating & Rank Outbox ---------------------------------------------------------------------

    async def get_rating(self, player_id: int, mode: int) -> tuple[int, int, int]:
        if not is_rated_mode(mode):
            raise ValueError(f"mode {mode} is not rated")
        async with self._lock:
            row = self._state.ratings.get((player_id, mode))
            if row is None:
                return DEFAULT_RATING_CENTI, 0, 0
            return row[0], row[1], row[3]

    async def apply_rating(
        self,
        *,
        battle_id: int,
        payload_digest: bytes,
        mode: int,
        team_a: list[int],
        team_b: list[int],
        score_a: float,
        source_epoch: int = 1,
        generation: int = 1,
        eligible_for_rank: set[int] | None = None,
    ) -> RatingApplyResult:
        if battle_id == 0 or not team_a or not team_b:
            raise ValueError("battle_id and both teams must be non-empty")
        if not is_rated_mode(mode):
            raise ValueError(f"mode {mode} is not rated")
        now = await self.now_ms()

        def work(s: _MemState) -> RatingApplyResult:
            existing_applied = s.rating_applied.get(battle_id)
            if existing_applied is not None:
                if existing_applied[0] != payload_digest:
                    raise BattleOutcomeOriginalError(f"rating replay digest mismatch battle_id={battle_id}")
                return RatingApplyResult(outcome=RATING_OUTCOME_DUPLICATE, battle_id=battle_id, mode=mode)
            s.rating_applied[battle_id] = (payload_digest, now)

            shard = rank_source_shard(battle_id)
            src_key = (source_epoch, mode, shard)
            src = s.rank_sources.get(src_key)
            if src is None:
                src = RankSourceRow(
                    source_epoch=source_epoch,
                    mode=mode,
                    shard=shard,
                    produced_cursor=0,
                    committed_cursor=0,
                    board_id=0,
                    stopped=False,
                )
                s.rank_sources[src_key] = src

            members = sorted(list(team_a) + list(team_b))
            for i, pid in enumerate(members):
                if pid == 0 or (i > 0 and members[i - 1] == pid):
                    raise ValueError("rating member list has zero or duplicate player_id")

            ratings_before: dict[int, int] = {}
            games_before: dict[int, int] = {}
            changed_before: dict[int, int] = {}
            for pid in members:
                row = s.ratings.get((pid, mode))
                if row is None:
                    ratings_before[pid] = DEFAULT_RATING_CENTI
                    games_before[pid] = 0
                    changed_before[pid] = now
                else:
                    ratings_before[pid] = row[0]
                    games_before[pid] = row[1]
                    changed_before[pid] = row[3]

            delta = compute_elo_delta(
                [ratings_before[p] for p in team_a],
                [ratings_before[p] for p in team_b],
                score_a,
            )
            side_a = set(team_a)
            next_cursor = src.produced_cursor + 1
            batch = self.protocol.message(
                "rankpb.SubmitRankScoresRequest",
                generation=generation,
                source_epoch=source_epoch,
                source_shard=shard,
                source_cursor=next_cursor,
                request_id=battle_id,
                mode=RANK_SUBMIT_SET,
            )
            changes: list[RatingChange] = []
            for pid in members:
                change = delta if pid in side_a else -delta
                after = max(0, ratings_before[pid] + change)
                achieved = now if after != ratings_before[pid] else changed_before[pid]
                games_after = games_before[pid] + 1
                s.ratings[(pid, mode)] = (after, games_after, now, achieved)
                changes.append(
                    RatingChange(
                        player_id=pid,
                        rating_before_centi=ratings_before[pid],
                        rating_after_centi=after,
                        delta_centi=after - ratings_before[pid],
                        games_after=games_after,
                        achieved_ms=achieved,
                    )
                )
                if eligible_for_rank is None or pid in eligible_for_rank:
                    batch.entries.add(
                        entity_id=pid,
                        score=after,
                        achieved_ms=achieved,
                        score_version=games_after,
                    )

            unpublished = src.stopped or len(batch.entries) == 0
            written_cursor = 0
            if not unpublished:
                body = batch.SerializeToString(deterministic=True)
                s.rank_outbox[(source_epoch, mode, shard, next_cursor)] = (battle_id, body)
                s.rank_sources[src_key] = RankSourceRow(
                    source_epoch=src.source_epoch,
                    mode=src.mode,
                    shard=src.shard,
                    produced_cursor=next_cursor,
                    committed_cursor=src.committed_cursor,
                    board_id=src.board_id,
                    stopped=src.stopped,
                )
                written_cursor = next_cursor

            return RatingApplyResult(
                outcome=RATING_OUTCOME_APPLIED,
                battle_id=battle_id,
                mode=mode,
                shard=shard,
                source_cursor=written_cursor,
                unpublished=unpublished,
                changes=tuple(changes),
            )

        return await self._run_tx(work)

    async def stop_rank_source(self, source_epoch: int, mode: int) -> int:
        if not is_rated_mode(mode):
            raise ValueError(f"mode {mode} is not rated")

        def work(s: _MemState) -> int:
            total = 0
            for shard in range(RANK_SOURCE_SHARDS):
                key = (source_epoch, mode, shard)
                src = s.rank_sources.get(key)
                if src is None:
                    src = RankSourceRow(
                        source_epoch=source_epoch,
                        mode=mode,
                        shard=shard,
                        produced_cursor=0,
                        committed_cursor=0,
                        board_id=0,
                        stopped=True,
                    )
                else:
                    src = RankSourceRow(
                        source_epoch=src.source_epoch,
                        mode=src.mode,
                        shard=src.shard,
                        produced_cursor=src.produced_cursor,
                        committed_cursor=src.committed_cursor,
                        board_id=src.board_id,
                        stopped=True,
                    )
                s.rank_sources[key] = src
                total += src.produced_cursor
            return total

        return await self._run_tx(work)

    async def list_rank_sources(self, *, source_epoch: int | None = None) -> list[RankSourceRow]:
        async with self._lock:
            rows = [
                row
                for row in self._state.rank_sources.values()
                if source_epoch is None or row.source_epoch == source_epoch
            ]
            rows.sort(key=lambda r: (r.source_epoch, r.mode, r.shard))
            return rows

    async def bind_rank_board(self, source_epoch: int, mode: int, board_id: int) -> None:
        def work(s: _MemState) -> None:
            for key, row in list(s.rank_sources.items()):
                if row.source_epoch == source_epoch and row.mode == mode and row.board_id == 0:
                    s.rank_sources[key] = RankSourceRow(
                        source_epoch=row.source_epoch,
                        mode=row.mode,
                        shard=row.shard,
                        produced_cursor=row.produced_cursor,
                        committed_cursor=row.committed_cursor,
                        board_id=board_id,
                        stopped=row.stopped,
                    )

        await self._run_tx(work)

    async def get_rank_outbox_entry(
        self, source_epoch: int, mode: int, shard: int, source_cursor: int
    ) -> bytes | None:
        async with self._lock:
            row = self._state.rank_outbox.get((source_epoch, mode, shard, source_cursor))
            return row[1] if row is not None else None

    async def advance_rank_committed_cursor(
        self, source_epoch: int, mode: int, shard: int, committed_cursor: int
    ) -> None:
        def work(s: _MemState) -> None:
            key = (source_epoch, mode, shard)
            row = s.rank_sources.get(key)
            if row is not None and committed_cursor > row.committed_cursor:
                s.rank_sources[key] = RankSourceRow(
                    source_epoch=row.source_epoch,
                    mode=row.mode,
                    shard=row.shard,
                    produced_cursor=row.produced_cursor,
                    committed_cursor=committed_cursor,
                    board_id=row.board_id,
                    stopped=row.stopped,
                )

        await self._run_tx(work)

    async def list_rank_removals(self, *, limit: int = 16) -> list[tuple[int, int]]:
        async with self._lock:
            return sorted(self._state.rank_removals.keys())[:limit]

    async def delete_rank_removal(self, mode: int, player_id: int) -> None:
        def work(s: _MemState) -> None:
            s.rank_removals.pop((mode, player_id), None)

        await self._run_tx(work)

    # -- Ranked Play V1 -------------------------------------------------------------------------------

    async def get_active_ranked_season(self) -> RankedSeasonRow | None:
        async with self._lock:
            actives = [s for s in self._state.ranked_seasons.values() if s.state == RANKED_SEASON_ACTIVE]
            if not actives:
                return None
            actives.sort(key=lambda x: x.season_id, reverse=True)
            return actives[0]

    async def upsert_ranked_season(self, season: RankedSeasonRow) -> RankedSeasonRow:
        def work(s: _MemState) -> RankedSeasonRow:
            s.ranked_seasons[season.season_id] = season
            return season

        return await self._run_tx(work)

    async def list_ranked_seasons(self) -> list[RankedSeasonRow]:
        async with self._lock:
            return sorted(self._state.ranked_seasons.values(), key=lambda x: x.season_id, reverse=True)

    async def get_ranked_player_prefs(self, player_id: int) -> RankedPlayerPrefsRow | None:
        async with self._lock:
            return self._state.ranked_prefs.get(player_id)

    async def save_ranked_player_prefs(
        self,
        caller: CallerIdentity,
        *,
        char_preset_id: int,
        pet_preset_id: int,
        rule_version: int,
        cosmetic_loadout: bytes,
    ) -> RankedPlayerPrefsRow:
        now = await self.now_ms()

        def work(s: _MemState) -> RankedPlayerPrefsRow:
            self._check_fence_locked(s, caller.player_id, caller.account, caller.owner_revision)
            row = RankedPlayerPrefsRow(
                player_id=caller.player_id,
                character_preset_id=char_preset_id,
                pet_preset_id=pet_preset_id,
                rule_version=rule_version,
                cosmetic_loadout=cosmetic_loadout,
                updated_ms=now,
            )
            s.ranked_prefs[caller.player_id] = row
            return row

        return await self._run_tx(work)

    async def get_ranked_player_season(self, season_id: int, player_id: int) -> RankedPlayerSeasonRow | None:
        async with self._lock:
            return self._state.ranked_player_seasons.get((season_id, player_id))

    async def list_ranked_player_seasons(self, player_id: int) -> list[RankedPlayerSeasonRow]:
        async with self._lock:
            rows = [r for r in self._state.ranked_player_seasons.values() if r.player_id == player_id]
            rows.sort(key=lambda r: r.season_id, reverse=True)
            return rows

    async def list_ranked_rewards(self, player_id: int) -> list[RankedRewardRow]:
        async with self._lock:
            rows = [r for r in self._state.ranked_rewards.values() if r.player_id == player_id]
            rows.sort(key=lambda r: (r.season_id, r.reward_kind))
            return rows

    async def save_ranked_battle_settlement(
        self,
        *,
        season_id: int,
        battle_id: int,
        player_id: int,
        summary_template: Message,
        rating_centi: int,
        tier_after: int,
        outcome_kind: int,
        participated: bool,
        settled_at_ms: int,
        milestones: tuple[tuple[int, int, int, str], ...],
        win_milestone: tuple[int, int, int, str] | None = None,
    ) -> tuple[RankedPlayerSeasonRow, list[RankedRewardRow], bytes]:
        def work(s: _MemState) -> tuple[RankedPlayerSeasonRow, list[RankedRewardRow], bytes]:
            existing_battle = s.ranked_battles.get((battle_id, player_id))
            if existing_battle is not None:
                prog = s.ranked_player_seasons[(season_id, player_id)]
                return prog, [], existing_battle[2]

            prev = s.ranked_player_seasons.get((season_id, player_id))
            qualified = bool(prev.qualified) if prev else False
            qualified_at_ms = prev.qualified_at_ms if prev else 0
            peak_rating = prev.peak_rating_centi if prev else 0
            peak_tier = prev.peak_tier if prev else 0
            valid_matches = prev.valid_matches if prev else 0
            wins = prev.wins if prev else 0
            losses = prev.losses if prev else 0
            draws = prev.draws if prev else 0
            milestone_bits = prev.milestone_bits if prev else 0
            honor_bits = prev.honor_bits if prev else 0
            final_rank = prev.final_rank if prev else 0
            cur_streak = prev.current_win_streak if prev else 0
            best_streak = prev.best_win_streak if prev else 0

            if participated:
                valid_matches += 1
                if not qualified:
                    qualified = True
                    qualified_at_ms = settled_at_ms
                if outcome_kind == 1:
                    wins += 1
                    cur_streak += 1
                    if cur_streak > best_streak:
                        best_streak = cur_streak
                elif outcome_kind == 2:
                    losses += 1
                    cur_streak = 0
                elif outcome_kind == 3:
                    draws += 1
                    cur_streak = 0

            if qualified:
                if peak_rating == 0 or rating_centi > peak_rating:
                    peak_rating = rating_centi
                if tier_after > peak_tier:
                    peak_tier = tier_after

            unlocked: list[RankedRewardRow] = []
            for threshold, bit_mask, reward_kind, item_code in milestones:
                if valid_matches >= threshold and not (milestone_bits & bit_mask):
                    milestone_bits |= bit_mask
                    rkey = (player_id, season_id, reward_kind)
                    if rkey not in s.ranked_rewards:
                        rew = RankedRewardRow(
                            player_id=player_id,
                            season_id=season_id,
                            reward_kind=reward_kind,
                            tier_value=threshold,
                            item_code=item_code,
                            granted_at_ms=settled_at_ms,
                        )
                        s.ranked_rewards[rkey] = rew
                        unlocked.append(rew)
            if win_milestone is not None:
                w_target, w_bit, w_kind, w_code = win_milestone
                if w_target > 0 and wins >= w_target and not (milestone_bits & w_bit):
                    milestone_bits |= w_bit
                    rkey = (player_id, season_id, w_kind)
                    if rkey not in s.ranked_rewards:
                        rew = RankedRewardRow(
                            player_id=player_id,
                            season_id=season_id,
                            reward_kind=w_kind,
                            tier_value=w_target,
                            item_code=w_code,
                            granted_at_ms=settled_at_ms,
                        )
                        s.ranked_rewards[rkey] = rew
                        unlocked.append(rew)

            prog = RankedPlayerSeasonRow(
                season_id=season_id,
                player_id=player_id,
                qualified=qualified,
                qualified_at_ms=qualified_at_ms,
                rating_centi=rating_centi,
                peak_rating_centi=peak_rating,
                peak_tier=peak_tier,
                valid_matches=valid_matches,
                wins=wins,
                losses=losses,
                draws=draws,
                milestone_bits=milestone_bits,
                honor_bits=honor_bits,
                final_rank=final_rank,
                updated_ms=settled_at_ms,
                current_win_streak=cur_streak,
                best_win_streak=best_streak,
            )
            s.ranked_player_seasons[(season_id, player_id)] = prog

            final_summary = self._clone_msg(summary_template)
            final_summary.season_valid_matches_after = valid_matches
            final_summary.win_streak_after = cur_streak
            for rew in unlocked:
                final_summary.newly_unlocked_rewards.add(
                    season_id=rew.season_id,
                    kind=rew.reward_kind,
                    tier_value=rew.tier_value,
                    item_code=rew.item_code,
                    granted_at_ms=rew.granted_at_ms,
                )
            raw_bytes = final_summary.SerializeToString(deterministic=True)
            s.ranked_battles[(battle_id, player_id)] = (season_id, settled_at_ms, raw_bytes)
            return prog, unlocked, raw_bytes

        return await self._run_tx(work)

    async def get_ranked_battle_result(self, player_id: int, battle_id: int = 0) -> bytes | None:
        async with self._lock:
            if battle_id != 0:
                row = self._state.ranked_battles.get((battle_id, player_id))
                return row[2] if row is not None else None
            matches = [
                (settled_ms, bid, raw)
                for (bid, pid), (_, settled_ms, raw) in self._state.ranked_battles.items()
                if pid == player_id
            ]
            if not matches:
                return None
            matches.sort(key=lambda x: (x[0], x[1]), reverse=True)
            return matches[0][2]

    async def get_ranked_rivalry(
        self, season_id: int, player_id: int, opponent_id: int,
    ) -> RankedRivalryRow | None:
        async with self._lock:
            return self._state.ranked_rivalries.get((season_id, player_id, opponent_id))

    async def upsert_ranked_rivalry(self, row: RankedRivalryRow) -> RankedRivalryRow:
        def work(s: _MemState) -> RankedRivalryRow:
            key = (row.season_id, row.player_id, row.opponent_id)
            prev = s.ranked_rivalries.get(key)
            name = row.opponent_name or (prev.opponent_name if prev else "")
            stored = RankedRivalryRow(
                season_id=row.season_id,
                player_id=row.player_id,
                opponent_id=row.opponent_id,
                opponent_name=name,
                wins=row.wins,
                losses=row.losses,
                draws=row.draws,
                last_outcome=row.last_outcome,
                last_battle_id=row.last_battle_id,
                updated_ms=row.updated_ms,
            )
            s.ranked_rivalries[key] = stored
            return stored

        return await self._run_tx(work)

    async def list_ranked_top_rivals(
        self, season_id: int, player_id: int, *, limit: int = 8,
    ) -> list[RankedRivalryRow]:
        async with self._lock:
            rows = [
                r for r in self._state.ranked_rivalries.values()
                if r.season_id == season_id and r.player_id == player_id and (r.wins + r.losses + r.draws) > 0
            ]
            rows.sort(key=lambda r: (-(r.wins + r.losses), -r.updated_ms, r.opponent_id))
            return rows[:limit]

    async def load_rivalry_encounters(
        self, season_id: int, members: list[tuple[int, int]],
    ) -> list[Message]:
        if season_id == 0 or len(members) < 2:
            return []
        async with self._lock:
            out: list[Message] = []
            for pid_a, team_a in members:
                for pid_b, team_b in members:
                    if not pid_a or not pid_b or team_a == team_b:
                        continue
                    rec = self._state.ranked_rivalries.get((season_id, pid_a, pid_b))
                    if rec is None or (rec.wins + rec.losses) == 0:
                        continue
                    out.append(
                        self.protocol.message(
                            "RankedRivalryEncounter",
                            player_id=rec.player_id,
                            opponent_id=rec.opponent_id,
                            opponent_name=rec.opponent_name,
                            wins=rec.wins,
                            losses=rec.losses,
                            revenge_target=is_rivalry_revenge_target(rec),
                        )
                    )
            out.sort(key=lambda e: (int(e.player_id), int(e.opponent_id)))
            return out

    async def upsert_ranked_featured_build(
        self,
        *,
        season_id: int,
        build_key: str,
        season_seal_id: int,
        starter_pet_table_id: int,
        core_skill_ids_csv: str,
        tier_qingfeng_centi: int,
        battle_id: int,
        player_id: int,
        player_name: str,
        after_rating_centi: int,
        team_size: int,
        total_rounds: int,
        player_won: bool,
        settled_at_ms: int,
    ) -> RankedFeaturedBuildRow:
        def work(s: _MemState) -> RankedFeaturedBuildRow:
            key = (season_id, build_key)
            prev = s.ranked_featured_builds.get(key)
            high_tier_min = tier_qingfeng_centi if tier_qingfeng_centi > 0 else 152_000
            appearances = (prev.appearances if prev else 0) + 1
            wins = (prev.wins if prev else 0) + (1 if player_won else 0)
            high_tier_wins = (prev.high_tier_wins if prev else 0) + (
                1 if (player_won and after_rating_centi >= high_tier_min) else 0
            )
            f_bid = prev.featured_battle_id if prev else 0
            f_pid = prev.featured_player_id if prev else 0
            f_pname = prev.featured_player_name if prev else ""
            f_rating = prev.featured_rating_centi if prev else 0
            f_tsize = prev.featured_team_size if prev else 0
            f_rounds = prev.featured_total_rounds if prev else 0
            if player_won and (f_bid == 0 or after_rating_centi >= f_rating):
                f_bid = battle_id
                f_pid = player_id
                f_pname = player_name
                f_rating = after_rating_centi
                f_tsize = team_size
                f_rounds = total_rounds
            row = RankedFeaturedBuildRow(
                season_id=season_id,
                build_key=build_key,
                season_seal_id=season_seal_id,
                starter_pet_table_id=starter_pet_table_id,
                core_skill_ids=core_skill_ids_csv,
                appearances=appearances,
                wins=wins,
                high_tier_wins=high_tier_wins,
                featured_battle_id=f_bid,
                featured_player_id=f_pid,
                featured_player_name=f_pname,
                featured_rating_centi=f_rating,
                featured_team_size=f_tsize,
                featured_total_rounds=f_rounds,
                updated_ms=settled_at_ms,
            )
            s.ranked_featured_builds[key] = row
            return row

        return await self._run_tx(work)

    async def list_ranked_featured_builds(
        self, season_id: int, *, limit: int = 5,
    ) -> list[RankedFeaturedBuildRow]:
        async with self._lock:
            rows = [
                r for r in self._state.ranked_featured_builds.values()
                if r.season_id == season_id and r.wins > 0
            ]
            rows.sort(key=lambda r: (-r.high_tier_wins, -r.wins, -r.featured_rating_centi, -r.updated_ms))
            return rows[:limit]

    async def is_featured_ranked_replay_allowed(self, battle_id: int) -> bool:
        if battle_id <= 0:
            return False
        async with self._lock:
            return any(r.featured_battle_id == battle_id for r in self._state.ranked_featured_builds.values())
