"""match gRPC 服务层：match.MatchService、match.MatchInternal 与 rolefence.RoleFenceParticipant。

并与 TeamGrpcService 聚合为单一 gRPC 服务表（MatchServer），挂载全部 5 组服务到同一端口。
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
import uuid
from collections.abc import Callable

import grpc
from google.protobuf.message import Message

from ..login.caller_auth import AuthError, CallerVerifier
from ..protocol import Protocol
from .battle_scripts import REVIEW_REJECT_DENIED, BattleScriptManager
from .config import MatchConfig
from .errors import (
    ActivityIntentConflictError,
    ActivityOutsideWindowError,
    BattleOutcomeOriginalError,
    ERR_INVALID_PARAMETER,
    ERR_MATCH_BATTLE_REPORT_NOT_FOUND,
    ERR_MATCH_CHALLENGE_SELF,
    ERR_MATCH_CHALLENGE_TARGET_OFFLINE,
    ERR_MATCH_IN_BATTLE,
    ERR_MATCH_INTERNAL,
    ERR_MATCH_MODE_NOT_OPEN,
    ERR_MATCH_NOT_IN_SCENE,
    ERR_MATCH_NO_REGION_WITHIN_LATENCY,
    ERR_MATCH_PROBE_REQUIRED,
    ERR_MATCH_RANKED_PRESET_INVALID,
    ERR_MATCH_TEAM_SIZE_NOT_CONFIGURED,
    GuildWarMemberOccupiedError,
    InvalidRoleFenceStateError,
    MatchBusinessError,
    OriginalRoleError,
    TeamBusinessError,
    set_tip,
)
from .gather import BattleNodeResolver, GatherPipeline, Matchmaker, PresenceProvider, select_battle_node
from .model import (
    BATTLE_ACTIVITY_KIND_GUILD_WAR,
    COMPLETION_SOURCE_MANUAL_ABORT,
    DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID,
    DEFAULT_CUSTOM_DUEL_PET_PRESET_ID,
    MAX_OPERATOR_BYTES,
    MAX_PROBE_REGIONS,
    MAX_REASON_BYTES,
    MAX_TEAM_SIZE,
    MODE_1V1,
    MODE_5V5,
    MODE_GUILD_WAR_PVP,
    MODE_GUILD_WAR_SIEGE,
    MODE_JIANGHU_DUEL,
    MODE_PVE_SOLO,
    MODE_PVE_TEAM,
    MODE_PVP_TEAM,
    OUTCOME_DECISION_ABORT,
    OUTCOME_DECISION_COMPLETE,
    OUTCOME_DECISION_UNKNOWN,
    PROBE_GRANT_DOMAIN,
    PROBE_RECEIPT_DOMAIN,
    QUEUE_STATE_ENTERING,
    QUEUE_STATE_MATCHED,
    QUEUE_STATE_NOT_QUEUED,
    QUEUE_STATE_QUEUED,
    QUEUE_STATE_READY,
    TICKET_STATE_FORMING,
    TICKET_STATE_QUEUED,
    TICKET_STATE_READY,
    CallerIdentity,
    authenticate_gate_caller,
    has_custom_duel_rules,
    has_session_metadata,
    preferred_region,
    probe_latencies,
    session_binding,
    sign_internal_reply,
    sign_probe_message,
    uuid_to_pair,
    valid_operator_text,
    valid_trial_vow_mask,
    validate_custom_duel_rules,
    verify_internal_hmac,
    verify_probe_message,
)
from .jianghu import JianghuManager
from .outbox import OutcomeOutboxWorker
from .ranked import RankedManager, _default_config_tables
from .rating import RankOutboxPublisher
from .spectate import SpectateManager
from .spectate_chat import SpectateChatManager
from .store import MatchStore, StrandedFormationEntry
from .tactical_puzzle import WORKSHOP_DUNGEON_ID, TacticalPuzzleManager
from .team_service import TeamGrpcService
from .tournament import TournamentManager
from .trial_cards import TrialChallengeCardManager, supports_trial_vows

logger = logging.getLogger(__name__)

SERVICE_MATCH = "match.MatchService"
SERVICE_MATCH_INTERNAL = "match.MatchInternal"
SERVICE_ROLE_FENCE = "rolefence.RoleFenceParticipant"
CODE = grpc.StatusCode


class MatchGrpcService:
    """承载 MatchService、MatchInternal 与 RoleFenceParticipant 三组 gRPC 服务。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        config: MatchConfig,
        *,
        gate_verifier: CallerVerifier | None,
        presence: PresenceProvider,
        battle_nodes: BattleNodeResolver,
        gather: GatherPipeline,
        spectate: SpectateManager,
        battle_scripts: BattleScriptManager | None = None,
        ranked: RankedManager | None = None,
        tournament: TournamentManager | None = None,
        jianghu: JianghuManager | None = None,
        trial_cards: TrialChallengeCardManager | None = None,
        tactical_puzzles: TacticalPuzzleManager | None = None,
        spectate_chat: SpectateChatManager | None = None,
        inline_gather: bool = False,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config
        self._gate_verifier = gate_verifier
        self.presence = presence
        self.battle_nodes = battle_nodes
        self.gather = gather
        self.spectate = spectate
        self._inline_gather = inline_gather
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.battle_scripts = battle_scripts if battle_scripts is not None else BattleScriptManager(protocol, store)
        self.ranked = ranked if ranked is not None else RankedManager(protocol, store, clock_ms=self._clock_ms)
        self.tournament = tournament if tournament is not None else TournamentManager(protocol, clock_ms=self._clock_ms)
        self.jianghu = (
            jianghu
            if jianghu is not None
            else JianghuManager(protocol, store, presence, clock_ms=self._clock_ms)
        )
        if self.jianghu.battle_launcher is None:
            self.jianghu.battle_launcher = self._launch_jianghu_duel_formation
        self.trial_cards = (
            trial_cards
            if trial_cards is not None
            else TrialChallengeCardManager(protocol, store, clock_ms=self._clock_ms)
        )
        self.tactical_puzzles = (
            tactical_puzzles
            if tactical_puzzles is not None
            else TacticalPuzzleManager(
                protocol,
                store,
                author_name_lookup=self._lookup_player_display_name,
                clock_ms=self._clock_ms,
            )
        )
        if self.tactical_puzzles.battle_launcher is None:
            self.tactical_puzzles.battle_launcher = self._launch_tactical_puzzle_formation
        self.spectate_chat = (
            spectate_chat
            if spectate_chat is not None
            else SpectateChatManager(
                protocol,
                store,
                config.spectate_chat,
                display_name_lookup=self._lookup_player_display_name,
                clock_ms=self._clock_ms,
            )
        )
        if getattr(self.spectate, "on_battle_forgotten", None) is None:
            self.spectate.on_battle_forgotten = self.spectate_chat.mark_battle_ended
        if self.gather.on_tournament_profiles is None:
            self.gather.on_tournament_profiles = self.tournament.sync_member_profiles
        if getattr(self.gather, "on_tournament_captains", None) is None:
            self.gather.on_tournament_captains = self.tournament.load_squad_captains
        if getattr(self.gather, "on_jianghu_duel_started", None) is None:
            self.gather.on_jianghu_duel_started = self.jianghu.publish_duel_start_notice
        self._bg_tasks: set[asyncio.Task] = set()

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_MATCH: {
                "GetNetworkProbePlan": self.get_network_probe_plan,
                "CommitNetworkProbe": self.commit_network_probe,
                "JoinQueue": self.join_queue,
                "CancelQueue": self.cancel_queue,
                "GetQueueStatus": self.get_queue_status,
                "ChallengePlayer": self.challenge_player,
                "RespondChallenge": self.respond_challenge,
                "NotifyChallengeInvite": self.push_noop,
                "NotifyChallengeResult": self.push_noop,
                "WatchBattle": self.watch_battle,
                "ListWatchableBattles": self.list_watchable_battles,
                "RequestBattleTicket": self.request_battle_ticket,
                "ListBattleReports": self.list_battle_reports,
                "GetBattleReport": self.get_battle_report,
                "ListBattleScripts": self.list_battle_scripts,
                "SubmitBattleScript": self.submit_battle_script,
                "DeleteBattleScript": self.delete_battle_script,
                "SetBattleScriptLoadout": self.set_battle_script_loadout,
                "GetRankedProfile": self.get_ranked_profile,
                "SaveRankedPreset": self.save_ranked_preset,
                "UpdateRankedCosmetic": self.update_ranked_cosmetic,
                "GetRankedBattleResult": self.get_ranked_battle_result,
                "GetTournamentOverview": self.get_tournament_overview,
                "CreateTournamentSquad": self.create_tournament_squad,
                "InviteTournamentMember": self.invite_tournament_member,
                "RespondTournamentInvite": self.respond_tournament_invite,
                "LeaveTournamentSquad": self.leave_tournament_squad,
                "UpdateTournamentLineup": self.update_tournament_lineup,
                "RegisterTournamentSquad": self.register_tournament_squad,
                "ListTournamentStandings": self.list_tournament_standings,
                "StartTournamentMatch": self.start_tournament_match,
                "CheerTournamentSquad": self.cheer_tournament_squad,
                "ClaimTournamentReward": self.claim_tournament_reward,
                "SendTournamentSpectateCheer": self.send_tournament_spectate_cheer,
                "GetTournamentSpectateCheer": self.get_tournament_spectate_cheer,
                "UpdateTournamentSquadEmblem": self.update_tournament_squad_emblem,
                "StartTournamentWarmup": self.start_tournament_warmup,
                "PredictTournamentMatch": self.predict_tournament_match,
                "GetTournamentPredictionBoard": self.get_tournament_prediction_board,
                "BanTournamentLeyline": self.ban_tournament_leyline,
                "GetTournamentHighlights": self.get_tournament_highlights,
                "CreateJianghuDuelInvite": self.create_jianghu_duel_invite,
                "ConfirmJianghuDuelMember": self.confirm_jianghu_duel_member,
                "RespondJianghuDuelInvite": self.respond_jianghu_duel_invite,
                "ListJianghuGrudges": self.list_jianghu_grudges,
                "DismissJianghuGrudge": self.dismiss_jianghu_grudge,
                "NotifyJianghuDuelInvite": self.push_noop,
                "NotifyJianghuDuelStatus": self.push_noop,
                "CreateTrialChallengeCard": self.create_trial_challenge_card,
                "GetTrialChallengeCard": self.get_trial_challenge_card,
                "GetSharedTrialReplay": self.get_shared_trial_replay,
                "ListTacticalPuzzles": self.list_tactical_puzzles,
                "GetTacticalPuzzle": self.get_tactical_puzzle,
                "SaveTacticalPuzzleDraft": self.save_tactical_puzzle_draft,
                "DeleteTacticalPuzzle": self.delete_tactical_puzzle,
                "StartTacticalPuzzleBattle": self.start_tactical_puzzle_battle,
                "LikeTacticalPuzzle": self.like_tactical_puzzle,
                "GetTacticalPuzzleSolutionReplay": self.get_tactical_puzzle_solution_replay,
                "SendBattleSpectateChat": self.send_battle_spectate_chat,
                "PullBattleSpectateChat": self.pull_battle_spectate_chat,
            },
            SERVICE_MATCH_INTERNAL: {
                "CommitBattleOutcome": self.commit_battle_outcome,
                "GetBattleTerminalDecision": self.get_battle_terminal_decision,
                "StartActivityBattle": self.start_activity_battle,
                "CancelActivityBattle": self.cancel_activity_battle,
                "SetSpectateFeatured": self.set_spectate_featured,
                "AcquireTrialOccupancy": self.acquire_trial_occupancy,
                "ReleaseTrialOccupancy": self.release_trial_occupancy,
                "GetBattleCompletion": self.get_battle_completion,
                "ListStrandedFormations": self.list_stranded_formations,
                "ResolveStrandedFormation": self.resolve_stranded_formation,
                "ListPendingBattleScripts": self.list_pending_battle_scripts,
                "ReviewBattleScript": self.review_battle_script,
                "StartGuildWarBattle": self.start_guild_war_battle,
            },
            SERVICE_ROLE_FENCE: {
                "PrepareRoleFreeze": self.prepare_role_freeze,
                "ApplyRoleFence": self.apply_role_fence,
                "PurgeRemovedRole": self.purge_removed_role,
            },
        }

    async def close(self) -> None:
        tasks = tuple(self._bg_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _spawn(self, coro, *, name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    # -- 鉴权门禁（fail-closed）-----------------------------------------------------------------------

    async def _admit_client(self, context, path: str) -> CallerIdentity:
        try:
            caller = authenticate_gate_caller(self.protocol, self._gate_verifier, context, path)
        except AuthError as exc:
            await context.abort(CODE.PERMISSION_DENIED, f"gate identity rejected: {exc}")
        try:
            await self.store.verify_original_owner(caller.player_id, caller.account, caller.owner_revision)
        except OriginalRoleError as exc:
            await context.abort(CODE.PERMISSION_DENIED, f"role ownership rejected: {exc}")
        return caller

    async def _reject_client_on_internal(self, context, path: str) -> None:
        if has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, f"{path} allows internal service calls only")

    async def _admit_internal(self, context, request: Message, path: str, caller: str, secret: bytes) -> None:
        await self._reject_client_on_internal(context, path)
        if not secret:
            await context.abort(CODE.PERMISSION_DENIED, f"{path} internal caller secret is not configured")
        if not verify_internal_hmac(path, request, caller, secret, int(self._clock_ms())):
            await context.abort(CODE.PERMISSION_DENIED, f"invalid {caller} service call signature")

    # -- MatchService RPCs ----------------------------------------------------------------------------

    async def push_noop(self, request: Message, context, path: str) -> Message:
        return self.protocol.message("Empty")

    async def unimplemented_rpc(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        await context.abort(CODE.UNIMPLEMENTED, f"{path} is not enabled on this server")

    async def get_network_probe_plan(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        if len(self.config.probe_secret) < 32:
            await context.abort(CODE.UNAVAILABLE, "network probe secret not configured")
        binding = session_binding(caller)
        if not binding:
            await context.abort(CODE.PERMISSION_DENIED, "incomplete session binding for network probe")
        now_ms = await self.store.now_ms()
        expires_ms = now_ms + self.config.probe_ttl_sec * 1000
        plan_id = str(uuid.uuid4())
        resp = self.protocol.message(
            "match.GetNetworkProbePlanResponse",
            plan_id=plan_id,
            expires_at_ms=expires_ms,
            required=self.config.require_network_probe,
        )
        nodes = await self.battle_nodes.list_nodes(release_track=caller.release_track)
        # 每个机房挑一台可探测节点，最多 MAX_PROBE_REGIONS 个机房
        by_region: dict[int, list] = {}
        for n in sorted(nodes, key=lambda item: item.node_id):
            if n.physical_region <= 0 or not n.client_host or not 0 < n.client_port <= 65535 or not n.node_uuid:
                continue
            if n.physical_region not in by_region and len(by_region) >= MAX_PROBE_REGIONS:
                continue
            by_region.setdefault(n.physical_region, []).append(n)
        for region, candidates in sorted(by_region.items()):
            node = candidates[caller.player_id % len(candidates)]
            grant = self.protocol.message(
                "match.NetworkProbeGrant",
                plan_id=plan_id,
                player_id=caller.player_id,
                session_binding=binding,
                physical_region=region,
                target_node_id=node.node_id,
                target_instance_id=node.node_uuid,
                expires_at_ms=expires_ms,
            )
            payload, sig = sign_probe_message(self.config.probe_secret, PROBE_GRANT_DOMAIN, grant)
            target = resp.targets.add(
                physical_region=region,
                host=node.client_host,
                port=node.client_port,
                ws_url=node.client_ws_url,
            )
            target.grant.payload = payload
            target.grant.signature = sig

        if not resp.targets:
            return set_tip(resp, ERR_MATCH_NO_REGION_WITHIN_LATENCY)

        profile = self.protocol.message(
            "match.MatchNetworkProfile",
            plan_id=plan_id,
            session_binding=binding,
            owner_account=caller.account,
            owner_revision=caller.owner_revision,
            release_track=caller.release_track,
        )
        await self.store.save_network_plan(caller.player_id, profile, expires_ms)
        return resp

    async def commit_network_probe(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        if len(self.config.probe_secret) < 32:
            await context.abort(CODE.UNAVAILABLE, "network probe secret not configured")
        binding = session_binding(caller)
        if not binding or not request.plan_id or not 1 <= len(request.receipts) <= MAX_PROBE_REGIONS:
            await context.abort(CODE.INVALID_ARGUMENT, "invalid network probe receipt arguments")
        now_ms = await self.store.now_ms()
        seen_regions: set[int] = set()
        verified: list[Message] = []
        min_expires = (1 << 64) - 1
        for signed in request.receipts:
            try:
                receipt = verify_probe_message(
                    self.protocol, self.config.probe_secret, PROBE_RECEIPT_DOMAIN, signed, "match.NetworkProbeReceipt"
                )
            except ValueError as exc:
                await context.abort(CODE.INVALID_ARGUMENT, str(exc))
            grant = receipt.grant
            region = int(grant.physical_region)
            if (
                grant.plan_id != request.plan_id
                or int(grant.player_id) != caller.player_id
                or grant.session_binding != binding
                or region == 0
                or region in seen_regions
                or int(receipt.sample_count) != 3
                or int(receipt.rtt_ms) == 0
                or int(receipt.measured_at_ms) > now_ms
                or int(receipt.expires_at_ms) <= now_ms
                or int(receipt.expires_at_ms) > int(grant.expires_at_ms)
            ):
                await context.abort(CODE.INVALID_ARGUMENT, "probe receipt identity or freshness invalid")
            seen_regions.add(region)
            verified.append(receipt)
            min_expires = min(min_expires, int(receipt.expires_at_ms))

        try:
            await self.store.commit_network_profile(caller.player_id, request.plan_id, binding, verified, min_expires)
        except OriginalRoleError as exc:
            await context.abort(CODE.FAILED_PRECONDITION, str(exc))
        return self.protocol.message("match.CommitNetworkProbeResponse", expires_at_ms=min_expires)

    async def join_queue(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        resp = self.protocol.message("match.JoinQueueResponse")

        def fail(code: int) -> Message:
            resp.error_code = code
            return set_tip(resp, code)

        mode = int(request.mode)
        if mode not in (MODE_PVE_SOLO, MODE_PVE_TEAM, MODE_1V1, MODE_5V5, MODE_PVP_TEAM):
            return fail(ERR_MATCH_MODE_NOT_OPEN)
        config_id = int(request.battle_config_id)
        if config_id != 0 and self._dungeon_boss_trial_kind(config_id) == 7:
            return fail(ERR_MATCH_MODE_NOT_OPEN)
        team_size = int(request.team_size)
        if mode == MODE_PVE_TEAM:
            max_sz = self.config.pve_team_sizes.get(config_id, 0)
            if max_sz <= 0:
                return fail(ERR_MATCH_TEAM_SIZE_NOT_CONFIGURED)
            if team_size == 0:
                team_size = min(max_sz, MAX_TEAM_SIZE)
            elif team_size > min(max_sz, MAX_TEAM_SIZE):
                return fail(ERR_MATCH_MODE_NOT_OPEN)
        elif mode == MODE_PVP_TEAM:
            if team_size == 0:
                team_size = 1
            elif team_size > MAX_TEAM_SIZE:
                return fail(ERR_MATCH_MODE_NOT_OPEN)
        elif mode == MODE_1V1:
            team_size = 1
        elif mode == MODE_5V5:
            team_size = MAX_TEAM_SIZE
        else:
            team_size = 1

        is_wuxing_tower = bool(getattr(request, "is_wuxing_tower", False))
        trial_vow_mask = int(getattr(request, "trial_vow_mask", 0))
        if trial_vow_mask != 0:
            if not valid_trial_vow_mask(trial_vow_mask):
                return fail(ERR_MATCH_MODE_NOT_OPEN)
            is_solo_direct = mode == MODE_PVE_SOLO or (mode == MODE_PVE_TEAM and team_size == 1)
            if is_wuxing_tower or not is_solo_direct or not supports_trial_vows(config_id):
                return fail(ERR_MATCH_MODE_NOT_OPEN)

        pres = await self.presence.get_presence(caller.player_id)
        if pres.in_battle:
            return fail(ERR_MATCH_IN_BATTLE)
        if not pres.scene_grpc_endpoint:
            return fail(ERR_MATCH_NOT_IN_SCENE)

        now_ms = await self.store.now_ms()
        profile_bytes = b""
        pool_reg = 0
        profile, exp = await self.store.get_network_profile(caller.player_id)
        if self.config.require_network_probe:
            if profile is None or exp <= now_ms or (request.network_plan_id and profile.plan_id != request.network_plan_id):
                return fail(ERR_MATCH_PROBE_REQUIRED)
            lats = probe_latencies(profile, now_ms, self.config.hard_latency_ms)
            if not lats:
                return fail(ERR_MATCH_NO_REGION_WITHIN_LATENCY)
            pool_reg = preferred_region(profile, now_ms, self.config.hard_latency_ms)
            profile_bytes = profile.SerializeToString(deterministic=True)
        elif profile is not None and exp > now_ms:
            pool_reg = preferred_region(profile, now_ms, self.config.hard_latency_ms)
            profile_bytes = profile.SerializeToString(deterministic=True)

        try:
            ticket = await self.store.join_queue(
                caller,
                mode=mode,
                config_id=config_id,
                team_size=team_size,
                zone_id=int(request.zone_id) or pres.zone_id or self.config.zone_id,
                request_id=request.request_id,
                request_payload=request.SerializeToString(deterministic=True),
                network_profile=profile_bytes,
                pool_region=pool_reg,
                queue_ttl_ms=self.config.queue_ttl_sec * 1000,
                is_wuxing_tower=is_wuxing_tower,
                trial_vow_mask=trial_vow_mask,
            )
        except MatchBusinessError as exc:
            return fail(exc.code)
        except OriginalRoleError as exc:
            await context.abort(CODE.PERMISSION_DENIED, str(exc))

        resp.queue_ticket = ticket.ticket
        return resp

    async def cancel_queue(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        try:
            await self.store.cancel_queue(caller, request.queue_ticket, request.request_id)
        except MatchBusinessError as exc:
            await context.abort(CODE.FAILED_PRECONDITION, f"cancel rejected code={exc.code}")
        except OriginalRoleError as exc:
            await context.abort(CODE.PERMISSION_DENIED, str(exc))
        return self.protocol.message("Empty")

    async def get_queue_status(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        info = await self.store.get_queue_status(caller, request.request_id)
        resp = self.protocol.message(
            "match.GetQueueStatusResponse",
            state=QUEUE_STATE_NOT_QUEUED,
            request_cancelled=info.request_cancelled,
        )
        tk = info.ticket
        if tk is None:
            return resp
        st = int(tk.state)
        if st == TICKET_STATE_QUEUED:
            resp.state = QUEUE_STATE_QUEUED
            resp.queued_seconds = max(0, (info.now_ms - int(tk.enqueued_ms)) // 1000)
            resp.queue_ticket = tk.ticket
            resp.team_size = int(tk.team_size)
        elif st == TICKET_STATE_FORMING:
            resp.state = QUEUE_STATE_MATCHED
            resp.queue_ticket = tk.ticket
            resp.battle_id = int(tk.formation_id)
            resp.selected_region = int(tk.selected_region)
        elif st == TICKET_STATE_READY:
            resp.state = QUEUE_STATE_READY
            resp.queue_ticket = tk.ticket
            resp.battle_id = int(tk.formation_id)
            resp.selected_region = int(tk.selected_region)
        return resp

    async def challenge_player(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        resp = self.protocol.message("match.ChallengePlayerResponse")
        target_id = int(request.target_player_id)
        if target_id == 0 or target_id == caller.player_id:
            return set_tip(resp, ERR_MATCH_CHALLENGE_SELF)
        custom_rules = request.custom_rules if request.HasField("custom_rules") else None
        if not validate_custom_duel_rules(custom_rules):
            return set_tip(resp, ERR_INVALID_PARAMETER)
        if custom_rules is not None and bool(custom_rules.use_free_preset):
            char_pid = int(custom_rules.challenger_character_preset_id) or DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID
            pet_pid = int(custom_rules.challenger_pet_preset_id) or DEFAULT_CUSTOM_DUEL_PET_PRESET_ID
            if not self.ranked.is_valid_preset_pair(char_pid, pet_pid):
                return set_tip(resp, ERR_MATCH_RANKED_PRESET_INVALID)
        custom_payload = (
            custom_rules.SerializeToString(deterministic=True) if has_custom_duel_rules(custom_rules) else b""
        )
        target_pres = await self.presence.get_presence(target_id)
        if not target_pres.online:
            return set_tip(resp, ERR_MATCH_CHALLENGE_TARGET_OFFLINE)
        try:
            inv = await self.store.create_challenge(
                caller,
                target_id,
                int(request.battle_config_id) or 1,
                self.config.challenge_ttl_sec * 1000,
                custom_rules_payload=custom_payload,
            )
            resp.challenge_id = int(inv.challenge_id)
            return resp
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)

    async def respond_challenge(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        resp = self.protocol.message("match.RespondChallengeResponse")
        resp_char_pid = int(getattr(request, "responder_character_preset_id", 0))
        resp_pet_pid = int(getattr(request, "responder_pet_preset_id", 0))
        if request.accept and (resp_char_pid or resp_pet_pid):
            c_id = resp_char_pid or DEFAULT_CUSTOM_DUEL_CHARACTER_PRESET_ID
            p_id = resp_pet_pid or DEFAULT_CUSTOM_DUEL_PET_PRESET_ID
            if not self.ranked.is_valid_preset_pair(c_id, p_id):
                return set_tip(resp, ERR_MATCH_RANKED_PRESET_INVALID)
        battle_node = None
        if request.accept:
            nodes = await self.battle_nodes.list_nodes(release_track=caller.release_track)
            node = select_battle_node(nodes, release_track=caller.release_track)
            if node is None:
                return set_tip(resp, ERR_MATCH_INTERNAL)
            battle_node = (node.node_id, node.node_uuid, node.grpc_endpoint, node.physical_region, node.release_track)
        try:
            _, formation = await self.store.respond_challenge(
                caller,
                int(request.challenge_id),
                bool(request.accept),
                battle_node=battle_node,
                battle_deadline_ms=self.config.battle_deadline_sec * 1000,
                responder_character_preset_id=resp_char_pid,
                responder_pet_preset_id=resp_pet_pid,
            )
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)
        if formation is not None:
            if self._inline_gather:
                await self.gather.run_formation(formation)
            else:
                self._spawn(self.gather.run_formation(formation), name=f"challenge-gather-{formation.battle_id}")
        return resp

    async def watch_battle(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.spectate.watch_battle(caller, request)

    async def list_watchable_battles(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.spectate.list_watchable_battles(
            caller,
            int(request.limit),
            ranked_only=bool(getattr(request, "ranked_only", False)),
        )

    async def request_battle_ticket(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.spectate.request_battle_ticket(caller, int(request.battle_id))

    async def list_battle_reports(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        resp = self.protocol.message("match.ListBattleReportsResponse")
        for summary in await self.store.list_battle_reports(caller.player_id):
            resp.reports.add().CopyFrom(summary)
        return resp

    async def get_battle_report(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        resp = self.protocol.message("match.GetBattleReportResponse")
        battle_id = int(request.battle_id)
        try:
            allow_public = self.tournament.is_public_knockout_replay_allowed(battle_id)
            if not allow_public:
                allow_public = await self.store.is_featured_ranked_replay_allowed(battle_id)
            env = await self.store.get_battle_report(
                caller.player_id,
                battle_id,
                allow_public=allow_public,
            )
            resp.replay.CopyFrom(env)
            return resp
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)

    # -- BattleScripts 客户端 RPCs -------------------------------------------------------------------

    async def list_battle_scripts(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.battle_scripts.list_battle_scripts(caller)

    async def submit_battle_script(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.battle_scripts.submit_battle_script(caller, request)

    async def delete_battle_script(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.battle_scripts.delete_battle_script(caller, request)

    async def set_battle_script_loadout(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.battle_scripts.set_battle_script_loadout(caller, request)

    # -- Ranked Play V1 客户端 RPCs ------------------------------------------------------------------

    async def get_ranked_profile(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.ranked.get_ranked_profile(caller, request)

    async def save_ranked_preset(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.ranked.save_ranked_preset(caller, request)

    async def update_ranked_cosmetic(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.ranked.update_ranked_cosmetic(caller, request)

    async def get_ranked_battle_result(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.ranked.get_ranked_battle_result(caller, request)

    # -- Season Squad Tournament V1 客户端 RPCs ------------------------------------------------------

    async def get_tournament_overview(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.get_tournament_overview(caller, request)

    async def create_tournament_squad(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.create_tournament_squad(caller, request)

    async def invite_tournament_member(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.invite_tournament_member(caller, request)

    async def respond_tournament_invite(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.respond_tournament_invite(caller, request)

    async def leave_tournament_squad(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.leave_tournament_squad(caller, request)

    async def update_tournament_lineup(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.update_tournament_lineup(caller, request)

    async def register_tournament_squad(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.register_tournament_squad(caller, request)

    async def list_tournament_standings(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.list_tournament_standings(caller, request)

    async def start_tournament_match(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.start_tournament_match(caller, request)

    async def cheer_tournament_squad(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.cheer_tournament_squad(caller, request)

    async def claim_tournament_reward(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.claim_tournament_reward(caller, request)

    # -- Season Squad Tournament V2 / V3 客户端 RPCs（507..508, 511..514；506 已停用）----------------

    async def send_tournament_spectate_cheer(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.send_tournament_spectate_cheer(caller, request)

    async def get_tournament_spectate_cheer(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.get_tournament_spectate_cheer(caller, request)

    async def update_tournament_squad_emblem(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.update_tournament_squad_emblem(caller, request)

    async def start_tournament_warmup(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.start_tournament_warmup(caller, request)

    async def predict_tournament_match(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.predict_tournament_match(caller, request)

    async def get_tournament_prediction_board(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tournament.get_tournament_prediction_board(caller, request)

    async def ban_tournament_leyline(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return self.tournament.ban_tournament_leyline(caller, request)

    async def get_tournament_highlights(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return self.tournament.get_tournament_highlights(caller, request)

    # -- 江湖恩怨与天墉生死状一期客户端 RPCs（548..554）----------------------------------------------

    async def _launch_jianghu_duel_formation(
        self,
        invite_view: Message,
        side_a: list[int],
        side_b: list[int],
        duel_ctx: Message,
    ) -> int:
        """将已抢占 MATCHED 的天墉生死状写入成局记录并驱动 GatherPipeline 开局。"""
        nodes = await self.battle_nodes.list_nodes(release_track="stable")
        node = select_battle_node(nodes, release_track="stable")
        if node is None:
            raise RuntimeError("no battle node available for jianghu duel")
        invite_id = int(invite_view.duel_invite_id)
        members: list[tuple[int, str, int, bool]] = []
        for pid in side_a:
            members.append((pid, f"jh-{invite_id}-{pid}", 0, pid == int(duel_ctx.challenger_captain_id)))
        for pid in side_b:
            members.append((pid, f"jh-{invite_id}-{pid}", 1, pid == int(duel_ctx.target_captain_id)))
        formation = await self.store.claim_formation(
            mode=MODE_JIANGHU_DUEL,
            config_id=0,
            members=members,
            physical_region=node.physical_region,
            target_node_id=node.node_id,
            target_instance_id=node.node_uuid,
            target_endpoint=node.grpc_endpoint,
            release_track=node.release_track or "stable",
            battle_deadline_ms=self.config.battle_deadline_sec * 1000,
            requeue_on_failure=False,
            jianghu_duel_context=duel_ctx.SerializeToString(deterministic=True),
            skip_ticket_check=True,
        )
        if self._inline_gather:
            await self.gather.run_formation(formation)
        else:
            self._spawn(self.gather.run_formation(formation), name=f"jianghu-gather-{formation.battle_id}")
        return int(formation.battle_id)

    async def create_jianghu_duel_invite(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.jianghu.create_invite(caller, request)

    async def confirm_jianghu_duel_member(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.jianghu.confirm_member(caller, request)

    async def respond_jianghu_duel_invite(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.jianghu.respond_invite(caller, request)

    async def list_jianghu_grudges(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.jianghu.list_grudges(caller)

    async def dismiss_jianghu_grudge(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.jianghu.dismiss_grudge(caller, request)

    # -- 五行试炼志 V3 好友约战帖与受控 PVE 录像客户端 RPCs（570..572）------------------------------

    async def create_trial_challenge_card(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.trial_cards.create_card(caller, request)

    async def get_trial_challenge_card(self, request: Message, context, path: str) -> Message:
        await self._admit_client(context, path)
        return await self.trial_cards.get_card(request)

    async def get_shared_trial_replay(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.trial_cards.get_shared_replay(caller, request)

    # -- 万象残局坊 V1 客户端 RPCs（575..581）--------------------------------------------------------

    async def _lookup_player_display_name(self, player_id: int) -> str:
        pres = await self.presence.get_presence(player_id)
        return str(pres.name or "")

    async def _launch_tactical_puzzle_formation(
        self,
        caller: CallerIdentity,
        pctx: Message,
        request_id: str,
    ) -> int:
        """校验玩家在线/不在战后以 Kind=7 幂等建房并驱动 GatherPipeline 开战（副本 314）。"""
        pres = await self.presence.get_presence(caller.player_id)
        if not pres.online or not pres.scene_grpc_endpoint:
            raise MatchBusinessError(ERR_MATCH_NOT_IN_SCENE)
        if pres.in_battle:
            raise MatchBusinessError(ERR_MATCH_IN_BATTLE)

        nodes = await self.battle_nodes.list_nodes(release_track=caller.release_track)
        node = select_battle_node(nodes, release_track=caller.release_track)
        if node is None:
            raise MatchBusinessError(ERR_MATCH_INTERNAL)

        req_high, req_low = uuid_to_pair(request_id)
        payload_bytes = pctx.SerializeToString(deterministic=True)
        ranked_ctx = self.protocol.message(
            "RankedMatchContext",
            season_id=0,
            rule_version=int(self.ranked.rule.rule_version),
            team_size=1,
            use_free_preset=True,
        )
        member_loadout = ranked_ctx.members.add(player_id=caller.player_id)
        member_loadout.presets.character_preset_id = int(pctx.spec.character_preset_id)
        member_loadout.presets.pet_preset_id = int(pctx.spec.pet_preset_id)

        formation, created = await self.store.start_tactical_puzzle_battle(
            caller=caller,
            request_high=req_high,
            request_low=req_low,
            payload_bytes=payload_bytes,
            battle_config_id=WORKSHOP_DUNGEON_ID,
            ranked_context_bytes=ranked_ctx.SerializeToString(deterministic=True),
            tactical_puzzle_context_bytes=payload_bytes,
            battle_node=(
                node.node_id,
                node.node_uuid,
                node.grpc_endpoint,
                node.physical_region,
                node.release_track or caller.release_track,
            ),
            battle_deadline_ms=self.config.battle_deadline_sec * 1000,
        )
        if created and int(formation.state) < 4:
            if self._inline_gather:
                await self.gather.run_formation(formation)
            else:
                self._spawn(self.gather.run_formation(formation), name=f"puzzle-gather-{formation.battle_id}")
        return int(formation.battle_id)

    async def list_tactical_puzzles(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.list_puzzles(caller, request)

    async def get_tactical_puzzle(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.get_puzzle(caller, request)

    async def save_tactical_puzzle_draft(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.save_draft(caller, request)

    async def delete_tactical_puzzle(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.delete_puzzle(caller, request)

    async def start_tactical_puzzle_battle(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.start_battle(caller, request)

    async def like_tactical_puzzle(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.like_puzzle(caller, request)

    async def get_tactical_puzzle_solution_replay(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.tactical_puzzles.get_solution_replay(caller, request)

    # -- 局外人观战交流模式 V1 客户端 RPCs（587..588）------------------------------------------------

    async def send_battle_spectate_chat(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.spectate_chat.send_chat(caller, request)

    async def pull_battle_spectate_chat(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        return await self.spectate_chat.pull_chat(caller, request)

    async def _on_tactical_puzzle_outcome_committed(self, request: Message) -> None:
        battle_id = int(request.battle_id)
        formation = await self.store.get_formation(battle_id)
        if formation is None or not getattr(formation, "tactical_puzzle_context", b""):
            return
        try:
            event = self.protocol.parse("contracts.kafka.BattleResultEvent", bytes(request.raw_result_payload))
        except Exception:
            return
        has_replay = request.HasField("replay") and bool(request.replay.payload)
        await self.tactical_puzzles.on_battle_completed(
            battle_id,
            formation,
            event,
            has_replay=has_replay,
            now_ms=int(self._clock_ms()),
        )

    # -- MatchInternal RPCs ---------------------------------------------------------------------------

    async def commit_battle_outcome(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(context, request, path, "battle", self.config.battle_outcome_secret)
        try:
            decision = await self.store.commit_battle_outcome(request)
        except BattleOutcomeOriginalError as exc:
            logger.error("match CommitBattleOutcome original rejected battle_id=%d err=%s", int(request.battle_id), exc)
            await context.abort(CODE.FAILED_PRECONDITION, str(exc))
        except Exception:
            logger.error("match CommitBattleOutcome failed battle_id=%d", int(request.battle_id), exc_info=True)
            await context.abort(CODE.UNAVAILABLE, "battle outcome commit unavailable")

        if decision == OUTCOME_DECISION_COMPLETE:
            await self._on_tactical_puzzle_outcome_committed(request)
        if decision in (OUTCOME_DECISION_COMPLETE, OUTCOME_DECISION_ABORT):
            self.spectate.forget_battle(int(request.battle_id))
            self.spectate_chat.mark_battle_ended(int(request.battle_id))

        reply = self.protocol.message("match.CommitBattleOutcomeResponse", decision=decision)
        sign_internal_reply(path, request, reply, "match", self.config.battle_outcome_secret, int(self._clock_ms()))
        return reply

    async def get_battle_terminal_decision(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(context, request, path, "scene", self.config.scene_secret)
        battle_id = int(request.battle_id)
        if battle_id == 0:
            await context.abort(CODE.INVALID_ARGUMENT, "battle_id is required")

        reply = self.protocol.message("match.GetBattleTerminalDecisionResponse")
        try:
            decision, terminal_expected = await self.store.get_battle_terminal_state(battle_id)
            if decision not in (OUTCOME_DECISION_UNKNOWN, OUTCOME_DECISION_COMPLETE, OUTCOME_DECISION_ABORT):
                reply.retry = True
            else:
                reply.decision = decision
                reply.terminal_expected = terminal_expected
        except Exception:
            logger.error("match GetBattleTerminalDecision failed battle_id=%d", battle_id, exc_info=True)
            reply.retry = True

        sign_internal_reply(path, request, reply, "match", self.config.scene_secret, int(self._clock_ms()))
        return reply

    @staticmethod
    def _dungeon_boss_trial_kind(battle_config_id: int) -> int:
        tables = _default_config_tables()
        if tables is None or "Dungeon" not in tables.tables:
            return 0
        row = tables["Dungeon"].get(battle_config_id)
        return int(getattr(row, "boss_trial_kind", 0)) if row is not None else 0

    def _unsigned_activity_payload(self, request: Message) -> bytes:
        cloned = self.protocol.message("match.StartActivityBattleRequest")
        cloned.CopyFrom(request)
        cloned.ClearField("auth")
        return cloned.SerializeToString(deterministic=True)

    async def _start_festival_beast_activity(self, request: Message, context, path: str) -> Message:
        """瑞兽迎春单人活动开局：只接受 scene 签名、校验 Kind=5 幂等与数据库时钟时间窗。"""
        resp = self.protocol.message("match.StartActivityBattleResponse")
        if (
            has_session_metadata(context)
            or not self.config.scene_secret
            or not verify_internal_hmac(path, request, "scene", self.config.scene_secret, int(self._clock_ms()))
        ):
            resp.reject = 5  # INTERNAL
            return resp
        mids = [int(x) for x in request.member_player_ids]
        act = request.activity_context
        fb = act.festival_beast if act.HasField("festival_beast") else None
        battle_config_id = int(request.battle_config_id)
        if (
            battle_config_id == 0
            or len(mids) != 1
            or mids[0] == 0
            or int(act.initiator_player_id) != mids[0]
            or (int(request.request_high) == 0 and int(request.request_low) == 0)
            or fb is None
            or int(fb.activity_instance_id) == 0
            or not fb.reward_identity
            or int(fb.required_wins) == 0
            or int(fb.locked_pet_id) == 0
            or int(fb.tier_id) == 0
            or int(fb.boss_health) == 0
            or int(fb.round_limit) == 0
            or int(fb.cycle_rounds) == 0
            or int(fb.first_heavy_round) == 0
            or int(fb.first_heavy_round) > int(fb.cycle_rounds)
            or int(fb.telegraph_lead_rounds) == 0
            or int(fb.telegraph_lead_rounds) >= int(fb.first_heavy_round)
            or int(fb.heavy_damage_percent) == 0
            or int(fb.effective_start_at_ms) == 0
            or int(fb.effective_end_at_ms) <= int(fb.effective_start_at_ms)
            or not fb.client_request_id
        ):
            resp.reject = 1  # INVALID_ARGUMENT
            return resp
        tables = _default_config_tables()
        if tables is not None and "Dungeon" in tables.tables and self._dungeon_boss_trial_kind(battle_config_id) != 7:
            resp.reject = 1  # INVALID_ARGUMENT
            return resp

        pid = mids[0]
        pres = await self.presence.get_presence(pid)
        if not pres.online:
            resp.reject = 2  # MEMBER_OFFLINE
            resp.offender_player_id = pid
            return resp
        if pres.in_battle:
            resp.reject = 3  # MEMBER_IN_BATTLE
            resp.offender_player_id = pid
            return resp
        if not pres.scene_grpc_endpoint:
            resp.reject = 4  # MEMBER_NOT_READY
            resp.offender_player_id = pid
            return resp

        nodes = await self.battle_nodes.list_nodes(release_track="stable")
        node = select_battle_node(nodes, release_track="stable")
        if node is None:
            resp.reject = 5  # INTERNAL
            return resp

        payload_bytes = self._unsigned_activity_payload(request)
        try:
            formation, created = await self.store.start_festival_beast_battle(
                owner_id=pid,
                request_high=int(request.request_high),
                request_low=int(request.request_low),
                payload_bytes=payload_bytes,
                battle_config_id=battle_config_id,
                activity_context_bytes=act.SerializeToString(deterministic=True),
                effective_start_at_ms=int(fb.effective_start_at_ms),
                effective_end_at_ms=int(fb.effective_end_at_ms),
                battle_node=(node.node_id, node.node_uuid, node.grpc_endpoint, node.physical_region, "stable"),
                battle_deadline_ms=self.config.battle_deadline_sec * 1000,
            )
        except ActivityOutsideWindowError:
            resp.reject = 6  # ACTIVITY_BATTLE_REJECT_OUTSIDE_WINDOW
            return resp
        except ActivityIntentConflictError:
            resp.reject = 1  # INVALID_ARGUMENT
            return resp
        except TeamBusinessError as exc:
            resp.reject = 4  # MEMBER_NOT_READY
            resp.offender_player_id = int(exc.params[0]) if exc.params else 0
            return resp
        except MatchBusinessError:
            resp.reject = 4
            return resp
        except Exception:
            logger.error("match StartActivityBattle(festival_beast) failed player_id=%d", pid, exc_info=True)
            resp.reject = 5
            return resp

        if created:
            if self._inline_gather:
                await self.gather.run_formation(formation)
            else:
                self._spawn(self.gather.run_formation(formation), name=f"festival-beast-gather-{formation.battle_id}")
        resp.battle_id = int(formation.battle_id)
        resp.formation_state = int(formation.state)
        resp.admitted_at_ms = int(formation.created_ms)
        return resp

    async def start_activity_battle(self, request: Message, context, path: str) -> Message:
        if int(request.activity_context.kind) == 2:
            return await self._start_festival_beast_activity(request, context, path)
        await self._admit_internal(context, request, path, "guild", self.config.guild_secret)
        resp = self.protocol.message("match.StartActivityBattleResponse")
        mids = [int(x) for x in request.member_player_ids]
        act = request.activity_context
        if (
            int(request.battle_config_id) == 0
            or not 1 <= len(mids) <= MAX_TEAM_SIZE
            or len(set(mids)) != len(mids)
            or any(pid == 0 for pid in mids)
            or int(request.request_id) == 0
            or int(act.kind) != 1
            or int(act.guild_id) == 0
            or int(act.activity_id) == 0
            or int(act.initiator_player_id) != mids[0]
        ):
            resp.reject = 1  # INVALID_ARGUMENT
            return resp
        for pid in mids:
            pres = await self.presence.get_presence(pid)
            if not pres.online:
                resp.reject = 2  # MEMBER_OFFLINE
                resp.offender_player_id = pid
                return resp
            if pres.in_battle:
                resp.reject = 3  # MEMBER_IN_BATTLE
                resp.offender_player_id = pid
                return resp
            if not pres.scene_grpc_endpoint:
                resp.reject = 4  # MEMBER_NOT_READY
                resp.offender_player_id = pid
                return resp

        nodes = await self.battle_nodes.list_nodes(release_track="stable")
        node = select_battle_node(nodes, release_track="stable")
        if node is None:
            resp.reject = 5  # INTERNAL
            return resp
        try:
            formation, created = await self.store.start_activity_battle(
                request_id=int(request.request_id),
                battle_config_id=int(request.battle_config_id),
                member_player_ids=mids,
                activity_context_bytes=act.SerializeToString(deterministic=True),
                battle_node=(node.node_id, node.node_uuid, node.grpc_endpoint, node.physical_region, "stable"),
                battle_deadline_ms=self.config.battle_deadline_sec * 1000,
            )
        except TeamBusinessError as exc:
            resp.reject = 4  # MEMBER_NOT_READY
            resp.offender_player_id = int(exc.params[0]) if exc.params else 0
            return resp
        except MatchBusinessError:
            resp.reject = 4
            return resp
        except Exception:
            logger.error("match StartActivityBattle failed request_id=%d", int(request.request_id), exc_info=True)
            resp.reject = 5
            return resp

        if created:
            if self._inline_gather:
                await self.gather.run_formation(formation)
            else:
                self._spawn(self.gather.run_formation(formation), name=f"activity-gather-{formation.battle_id}")
        resp.battle_id = int(formation.battle_id)
        resp.formation_state = int(formation.state)
        resp.admitted_at_ms = int(formation.created_ms)
        return resp

    async def start_guild_war_battle(self, request: Message, context, path: str) -> Message:
        """帮会全天帮战 V2 无匹配池直打入口：支持真人双侧直打（mode=9）与守旗 AI 快照踢馆（mode=10）。"""
        await self._admit_internal(context, request, path, "guild", self.config.guild_secret)
        resp = self.protocol.message("match.StartGuildWarBattleResponse")
        mode = int(request.match_mode)
        battle_config_id = int(request.battle_config_id)
        request_id = int(request.request_id)
        attackers = [int(x) for x in request.attacker_player_ids]
        defenders = [int(x) for x in request.defender_player_ids]
        act = request.activity_context
        gw = act.guild_war if act.HasField("guild_war") else None

        if (
            battle_config_id == 0
            or request_id == 0
            or mode not in (MODE_GUILD_WAR_PVP, MODE_GUILD_WAR_SIEGE)
            or not 1 <= len(attackers) <= MAX_TEAM_SIZE
            or len(set(attackers)) != len(attackers)
            or any(pid == 0 for pid in attackers)
            or int(act.kind) != BATTLE_ACTIVITY_KIND_GUILD_WAR
            or gw is None
            or int(gw.attacker_guild_id) == 0
        ):
            resp.reject = 1  # ACTIVITY_BATTLE_REJECT_INVALID_ARGUMENT
            return resp
        if mode == MODE_GUILD_WAR_PVP:
            if (
                not 1 <= len(defenders) <= MAX_TEAM_SIZE
                or len(set(defenders)) != len(defenders)
                or any(pid == 0 for pid in defenders)
                or set(attackers) & set(defenders)
            ):
                resp.reject = 1
                return resp
        else:
            if defenders or len(request.ai_defender_snapshots) > MAX_TEAM_SIZE:
                resp.reject = 1
                return resp

        ai_snapshots: list[Message] = []
        if mode == MODE_GUILD_WAR_SIEGE:
            for snap in request.ai_defender_snapshots:
                cloned = self.protocol.message("BattlePlayerSnapshot")
                cloned.CopyFrom(snap)
                cloned.team_index = 1
                cloned.ai_controlled = True
                ai_snapshots.append(cloned)

        for pid in attackers + defenders:
            pres = await self.presence.get_presence(pid)
            if not pres.online:
                resp.reject = 2  # MEMBER_OFFLINE
                resp.offender_player_id = pid
                return resp
            if pres.in_battle:
                resp.reject = 3  # MEMBER_IN_BATTLE
                resp.offender_player_id = pid
                resp.offender_battle_id = await self.store.active_battle_id_of(pid)
                return resp
            if not pres.scene_grpc_endpoint:
                resp.reject = 4  # MEMBER_NOT_READY
                resp.offender_player_id = pid
                return resp

        nodes = await self.battle_nodes.list_nodes(release_track="stable")
        node = select_battle_node(nodes, release_track="stable")
        if node is None:
            resp.reject = 5  # INTERNAL
            return resp

        try:
            formation, created = await self.store.start_guild_war_battle(
                request_id=request_id,
                battle_config_id=battle_config_id,
                match_mode=mode,
                attacker_player_ids=attackers,
                defender_player_ids=defenders,
                activity_context_bytes=act.SerializeToString(deterministic=True),
                battle_node=(node.node_id, node.node_uuid, node.grpc_endpoint, node.physical_region, "stable"),
                battle_deadline_ms=self.config.battle_deadline_sec * 1000,
            )
        except GuildWarMemberOccupiedError as exc:
            resp.reject = 3 if exc.in_battle else 4
            resp.offender_player_id = exc.player_id
            resp.offender_battle_id = exc.battle_id
            return resp
        except MatchBusinessError:
            resp.reject = 4
            return resp
        except Exception:
            logger.error("match StartGuildWarBattle failed request_id=%d", request_id, exc_info=True)
            resp.reject = 5
            return resp

        if created:
            ai_tuple = tuple(ai_snapshots)
            if self._inline_gather:
                await self.gather.run_formation(formation, ai_defender_snapshots=ai_tuple)
            else:
                self._spawn(
                    self.gather.run_formation(formation, ai_defender_snapshots=ai_tuple),
                    name=f"guild-war-gather-{formation.battle_id}",
                )
        resp.battle_id = int(formation.battle_id)
        resp.formation_state = int(formation.state)
        return resp

    async def cancel_activity_battle(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(context, request, path, "guild", self.config.guild_secret)
        req_id = int(request.original.request_id)
        if req_id == 0:
            await context.abort(CODE.INVALID_ARGUMENT, "original.request_id is required")
        battle_id, cancelled, form_state, decision = await self.store.cancel_activity_battle(req_id)
        reply = self.protocol.message(
            "match.CancelActivityBattleResponse",
            battle_id=battle_id,
            request_cancelled=cancelled,
            formation_state=form_state,
            terminal_decision=decision,
        )
        sign_internal_reply(path, request, reply, "match", self.config.guild_secret, int(self._clock_ms()))
        return reply

    async def set_spectate_featured(self, request: Message, context, path: str) -> Message:
        await self._reject_client_on_internal(context, path)
        resp = self.protocol.message("match.SetSpectateFeaturedResponse")
        if not self.config.ops_secret or not verify_internal_hmac(
            path, request, "ops", self.config.ops_secret, int(self._clock_ms())
        ):
            resp.reject = 4  # SPECTATE_FEATURED_REJECT_DENIED
            return resp
        reject, tier = await self.spectate.set_spectate_featured(int(request.battle_id), bool(request.featured))
        if reject == 0:
            self.spectate.forget_battle(int(request.battle_id))
        resp.reject = reject
        resp.tier = tier
        return resp

    async def acquire_trial_occupancy(self, request: Message, context, path: str) -> Message:
        await self._reject_client_on_internal(context, path)
        resp = self.protocol.message("match.AcquireTrialOccupancyResponse")
        if not self.config.guild_secret or not verify_internal_hmac(
            path, request, "guild", self.config.guild_secret, int(self._clock_ms())
        ):
            resp.reject = 3  # DENIED
            return resp
        owner_id = int(request.owner_id)
        ttl_ms = int(request.ttl_ms)
        roster = [(int(r.player_id), r.account, int(r.owner_revision)) for r in request.roster]
        if owner_id == 0 or ttl_ms == 0 or not roster or any(pid == 0 for pid, _, _ in roster):
            resp.reject = 3
            return resp
        try:
            reject, offender = await self.store.acquire_trial_occupancy(owner_id, roster, ttl_ms)
            resp.reject = reject
            resp.offender_player_id = offender
        except Exception:
            logger.error("match AcquireTrialOccupancy failed owner_id=%d", owner_id, exc_info=True)
            resp.reject = 4  # RETRY
        return resp

    async def release_trial_occupancy(self, request: Message, context, path: str) -> Message:
        await self._reject_client_on_internal(context, path)
        resp = self.protocol.message("match.ReleaseTrialOccupancyResponse")
        if not self.config.guild_secret or not verify_internal_hmac(
            path, request, "guild", self.config.guild_secret, int(self._clock_ms())
        ):
            resp.reject = 3
            return resp
        try:
            await self.store.release_trial_occupancy(int(request.owner_id), [int(x) for x in request.player_ids])
        except Exception:
            logger.error("match ReleaseTrialOccupancy failed owner_id=%d", int(request.owner_id), exc_info=True)
            resp.reject = 4
        return resp

    async def get_battle_completion(self, request: Message, context, path: str) -> Message:
        await self._reject_client_on_internal(context, path)
        resp = self.protocol.message("match.GetBattleCompletionResponse")
        if not self.config.guild_secret or not verify_internal_hmac(
            path, request, "guild", self.config.guild_secret, int(self._clock_ms())
        ):
            resp.reject = 3
            return resp
        try:
            decision, raw = await self.store.get_battle_completion(int(request.battle_id))
            resp.decision = decision
            resp.raw_payload = raw
        except Exception:
            logger.error("match GetBattleCompletion failed battle_id=%d", int(request.battle_id), exc_info=True)
            resp.reject = 4
        return resp

    def _fill_stranded_proto(self, entry: StrandedFormationEntry) -> Message:
        f = entry.formation
        out = self.protocol.message(
            "match.StrandedFormation",
            battle_id=int(f.battle_id),
            state=int(f.state),
            operation_id=f.operation_id,
            target_instance_id=f.target_instance_id,
            preflight_instance_id=f.preflight_instance_id,
            match_mode=int(f.mode),
            config_id=int(f.config_id),
            member_count=len(f.members),
            activity=bool(f.activity_context),
            created_ms=int(f.created_ms),
            deadline_ms=int(f.deadline_ms),
            retry_at_ms=int(f.retry_at_ms),
            outcome_protocol_version=int(f.outcome_protocol_version),
            preflight_supported_version=int(f.preflight_supported_version),
            adopted_outcome_protocol_version=int(f.adopted_outcome_protocol_version),
            adopted_outcome_observed=bool(f.adopted_outcome_observed),
            create_request_bytes=len(f.create_request),
            create_decodable=entry.create_req is not None,
            formation_class=entry.formation_class,
            failed_check=entry.failed_check,
            effective_deadline_ms=entry.effective_deadline_ms,
            has_terminal_row=entry.has_terminal_row,
            terminal_decision=entry.terminal_decision,
            terminal_source_kind=entry.terminal_source_kind,
            digest=entry.digest,
            refusal=entry.refusal,
        )
        if entry.create_req is not None:
            out.create_created_at_ms = int(entry.create_req.created_at_ms)
            out.create_deadline_ms = int(entry.create_req.deadline_ms)
            out.create_admit_before_ms = int(entry.create_req.admit_before_ms)
            out.create_player_count = len(entry.create_req.players)
        for tk in entry.tickets:
            out.tickets.add(
                player_id=int(tk.player_id),
                state=int(tk.state),
                formation_id=int(tk.formation_id),
                owner_revision=int(tk.owner_revision),
            )
        return out

    async def list_stranded_formations(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(
            context, request, path, "ops-formation", self.config.manual_resolution_secret
        )
        if not self.config.manual_resolution_env:
            await context.abort(CODE.PERMISSION_DENIED, "manual resolution environment not configured")
        limit = int(request.limit)
        if not 1 <= limit <= 100:
            await context.abort(CODE.INVALID_ARGUMENT, "limit must be in 1..100")
        resp = self.protocol.message("match.ListStrandedFormationsResponse", target=self.config.manual_resolution_env)
        try:
            entries, next_bid, now_ms = await self.store.list_stranded_formations(int(request.after_battle_id), limit)
            resp.next_battle_id = next_bid
            resp.now_ms = now_ms
            for entry in entries:
                resp.formations.add().CopyFrom(self._fill_stranded_proto(entry))
        except Exception:
            logger.error("match ListStrandedFormations failed", exc_info=True)
            resp.retry = True
        return resp

    async def resolve_stranded_formation(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(
            context, request, path, "ops-formation", self.config.manual_resolution_secret
        )
        if not self.config.manual_resolution_env:
            await context.abort(CODE.PERMISSION_DENIED, "manual resolution environment not configured")
        battle_id = int(request.battle_id)
        mode = int(request.mode)
        if battle_id == 0 or mode not in (1, 2, 3):  # SHOW / DRY_RUN / ABORT
            await context.abort(CODE.INVALID_ARGUMENT, "invalid battle_id or mode")
        replica = socket.gethostname()
        resp = self.protocol.message(
            "match.ResolveStrandedFormationResponse",
            target=self.config.manual_resolution_env,
            replica=replica,
        )
        if mode in (2, 3):
            if request.expect_target != self.config.manual_resolution_env:
                resp.verdict = 2  # REFUSED
                resp.refusal = "target_mismatch"
                return resp
            if (
                not request.expect_digest
                or not valid_operator_text(request.operator_name, MAX_OPERATOR_BYTES, required=True)
                or not valid_operator_text(request.reason, MAX_REASON_BYTES, required=True)
            ):
                await context.abort(CODE.INVALID_ARGUMENT, "missing or invalid audit parameters")

        entry, now_ms = await self.store.inspect_stranded_formation(battle_id)
        resp.now_ms = now_ms
        if entry is None:
            resp.verdict = 5  # NOT_FOUND
            return resp
        resp.formation.CopyFrom(self._fill_stranded_proto(entry))
        if entry.has_terminal_row and entry.terminal_decision != OUTCOME_DECISION_UNKNOWN:
            resp.verdict = 4  # RESOLVED
            return resp
        if entry.refusal:
            resp.verdict = 2  # REFUSED
            resp.refusal = entry.refusal
            return resp
        if mode == 1:  # SHOW
            resp.verdict = 1  # ELIGIBLE
            return resp
        if entry.digest != request.expect_digest:
            resp.verdict = 3  # CHANGED
            return resp
        audit_line = (
            f"[FormationManual] mode={mode} target={self.config.manual_resolution_env} "
            f"battle_id={battle_id} operator={request.operator_name} reason={request.reason}"
        )
        resp.audit_line = audit_line
        if mode == 2:  # DRY_RUN
            resp.verdict = 1
            return resp
        await self.store.commit_battle_abort(
            battle_id,
            COMPLETION_SOURCE_MANUAL_ABORT,
            manual_audit={
                "action": 3,
                "formation_class": entry.formation_class,
                "failed_check": entry.failed_check,
                "operator_name": request.operator_name,
                "reason": request.reason,
                "formation_digest_before": entry.digest,
                "formation_before": entry.formation.SerializeToString(deterministic=True),
                "effective_deadline_ms": entry.effective_deadline_ms,
                "replica_instance": replica,
            },
        )
        self.spectate.forget_battle(battle_id)
        self.spectate_chat.mark_battle_ended(battle_id)
        logger.warning("%s", audit_line)
        resp.verdict = 1  # ELIGIBLE (committed)
        return resp

    async def list_pending_battle_scripts(self, request: Message, context, path: str) -> Message:
        await self._reject_client_on_internal(context, path)
        if not self.config.ops_secret or not verify_internal_hmac(
            path, request, "ops", self.config.ops_secret, int(self._clock_ms())
        ):
            return self.protocol.message("match.ListPendingBattleScriptsResponse", reject=REVIEW_REJECT_DENIED)
        return await self.battle_scripts.list_pending_battle_scripts(request)

    async def review_battle_script(self, request: Message, context, path: str) -> Message:
        await self._reject_client_on_internal(context, path)
        if not self.config.ops_secret or not verify_internal_hmac(
            path, request, "ops", self.config.ops_secret, int(self._clock_ms())
        ):
            return self.protocol.message("match.ReviewBattleScriptResponse", reject=REVIEW_REJECT_DENIED)
        return await self.battle_scripts.review_battle_script(request)

    # -- RoleFenceParticipant RPCs --------------------------------------------------------------------

    async def prepare_role_freeze(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(context, request, path, "coordinator", self.config.coordinator_secret)
        try:
            return await self.store.prepare_role_freeze(request.state, dry_run=bool(request.dry_run))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error("match PrepareRoleFreeze failed player_id=%d", int(request.state.player_id), exc_info=True)
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", retry=True)

    async def apply_role_fence(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(context, request, path, "coordinator", self.config.coordinator_secret)
        try:
            return await self.store.apply_role_fence(request.state)
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error("match ApplyRoleFence failed player_id=%d", int(request.state.player_id), exc_info=True)
            return self.protocol.message("rolefence.ApplyRoleFenceResponse", retry=True)

    async def purge_removed_role(self, request: Message, context, path: str) -> Message:
        await self._admit_internal(context, request, path, "coordinator", self.config.coordinator_secret)
        try:
            resp = await self.store.purge_removed_role(request.state)
            await self.tactical_puzzles.purge_player(int(request.state.player_id))
            return resp
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error("match PurgeRemovedRole failed player_id=%d", int(request.state.player_id), exc_info=True)
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)


class MatchServer:
    """将全部 5 组 gRPC 服务、撮合循环与发件箱装配为一个统一托管对象。"""

    def __init__(
        self,
        *,
        protocol: Protocol,
        store: MatchStore,
        config: MatchConfig,
        match_service: MatchGrpcService,
        team_service: TeamGrpcService,
        gather: GatherPipeline,
        matchmaker: Matchmaker,
        outbox: OutcomeOutboxWorker,
        spectate: SpectateManager,
        rank_publisher: RankOutboxPublisher | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config
        self.match_service = match_service
        self.team_service = team_service
        self.gather = gather
        self.matchmaker = matchmaker
        self.outbox = outbox
        self.spectate = spectate
        self.rank_publisher = rank_publisher

    def services(self) -> dict[str, dict[str, Callable]]:
        merged = dict(self.match_service.services())
        merged.update(self.team_service.services())
        return merged

    async def close(self) -> None:
        await self.match_service.close()
        await self.team_service.close()
