"""组队 gRPC 服务层：teampb.ClientPlayerTeam 与 teampb.TeamAuthority。

提供 12 个活跃组队 RPC、ConfirmRankedPreset、3 个 S2C 推送占位方法、
场景权威读接口 ReadMembership，以及 Kafka 异步推送与 Scene 刷新钩子。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Protocol as TypingProtocol

import grpc
from google.protobuf.message import Message

from ..login.caller_auth import AuthError, CallerVerifier
from ..protocol import Protocol
from .config import MatchConfig
from .errors import (
    ERR_TEAM_DUNGEON_NOT_OPEN,
    ERR_TEAM_INTERNAL,
    ERR_TEAM_MEMBER_IN_BATTLE,
    ERR_TEAM_MEMBER_OFFLINE,
    ERR_TEAM_SIZE_EXCEEDED,
    OriginalRoleError,
    TeamBusinessError,
    set_tip,
)
from .gather import BattleNodeResolver, GatherPipeline, PresenceProvider, select_battle_node
from .model import (
    MAX_TEAM_SIZE,
    MODE_PVE_TEAM,
    MODE_PVP_TEAM,
    MODE_UNSPECIFIED,
    TEAM_LAUNCH_QUEUED,
    CallerIdentity,
    PlayerPresence,
    authenticate_gate_caller,
    has_session_metadata,
    sign_internal_reply,
    unique_ints,
    valid_trial_vow_mask,
    verify_internal_hmac,
)
from .ranked import RankedManager
from .store import MatchStore, TeamMutationOutcome, TeamSnapshotResult
from .trial_cards import supports_trial_vows

logger = logging.getLogger(__name__)

SERVICE_CLIENT_TEAM = "teampb.ClientPlayerTeam"
SERVICE_TEAM_AUTHORITY = "teampb.TeamAuthority"
CODE = grpc.StatusCode

MSG_NOTIFY_TEAM_SNAPSHOT = 213
MSG_NOTIFY_TEAM_INVITE = 215
MSG_NOTIFY_TEAM_EVENT = 203
EVENT_PUSH_TO_PLAYER = 39
EVENT_PLAYER_TEAM_REFRESH = 48


class TeamNotifier(TypingProtocol):
    """组队提交后的异步通知端口（推送 GateCommand 与 SceneCommand）。"""

    async def push_to_player(self, player_id: int, message_id: int, payload_msg: Message) -> None: ...
    async def refresh_scene(self, player_id: int) -> None: ...


class NullTeamNotifier:
    """默认空实现（未配置 Kafka 时安全忽略）。"""

    async def push_to_player(self, player_id: int, message_id: int, payload_msg: Message) -> None:
        return None

    async def refresh_scene(self, player_id: int) -> None:
        return None


class KafkaTeamNotifier:
    """基于 KafkaCommands 的生产通知实现，构造 PushToPlayerEvent/GateCommand 与 PlayerTeamRefreshEvent/SceneCommand。"""

    def __init__(
        self,
        protocol: Protocol,
        presence: PresenceProvider,
        publish: Callable[[str, int, int, Message], Awaitable[None]],
    ) -> None:
        self.protocol = protocol
        self.presence = presence
        self._publish = publish

    async def push_to_player(self, player_id: int, message_id: int, payload_msg: Message) -> None:
        pres = await self.presence.get_presence(player_id)
        if not pres.online or not pres.gate_node_id or not pres.gate_instance_id:
            return
        event = self.protocol.message(
            "contracts.kafka.PushToPlayerEvent",
            session_id=pres.session_id,
            player_id=player_id,
            message_content=self.protocol.content(message_id, body=payload_msg.SerializeToString()),
        )
        cmd = self.protocol.message(
            "contracts.kafka.GateCommand",
            event_id=EVENT_PUSH_TO_PLAYER,
            player_id=player_id,
            target_gate_id=pres.gate_node_id,
            target_instance_id=pres.gate_instance_id,
            payload=event.SerializeToString(),
        )
        await self._publish("gate", pres.gate_node_id, player_id, cmd)

    async def refresh_scene(self, player_id: int) -> None:
        pres = await self.presence.get_presence(player_id)
        if not pres.scene_node_id or not pres.scene_instance_id:
            return
        event = self.protocol.message("PlayerTeamRefreshEvent", player_id=player_id)
        cmd = self.protocol.message(
            "contracts.kafka.SceneCommand",
            player_id=player_id,
            target_scene_id=pres.scene_node_id,
            target_instance_id=pres.scene_instance_id,
            event_id=EVENT_PLAYER_TEAM_REFRESH,
            payload=event.SerializeToString(),
        )
        await self._publish("scene", pres.scene_node_id, player_id, cmd)


class TeamGrpcService:
    """集中承载 ClientPlayerTeam 与 TeamAuthority 两组 gRPC 服务。"""

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
        notifier: TeamNotifier | None = None,
        ranked: RankedManager | None = None,
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
        self.notifier = notifier or NullTeamNotifier()
        self._inline_gather = inline_gather
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.ranked = ranked if ranked is not None else RankedManager(protocol, store, clock_ms=self._clock_ms)
        self._bg_tasks: set[asyncio.Task] = set()

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_CLIENT_TEAM: {
                "CreateTeam": self.create_team,
                "GetMyTeam": self.get_my_team,
                "ApplyJoinTeam": self.apply_join_team,
                "HandleApplication": self.handle_application,
                "InviteToTeam": self.invite_to_team,
                "RespondInvite": self.respond_invite,
                "ListMyInvites": self.list_my_invites,
                "LeaveTeam": self.leave_team,
                "KickMember": self.kick_member,
                "TransferLeader": self.transfer_leader,
                "DisbandTeam": self.disband_team,
                "StartTeamMatch": self.start_team_match,
                "NotifyTeamSnapshot": self.push_noop,
                "NotifyTeamInvite": self.push_noop,
                "NotifyTeamEvent": self.push_noop,
                "ConfirmRankedPreset": self.confirm_ranked_preset,
                "SetTeamFormation": self.set_team_formation,
                "RecallTeamMembers": self.recall_team_members,
            },
            SERVICE_TEAM_AUTHORITY: {
                "ReadMembership": self.read_membership,
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

    # -- 鉴权门禁 ------------------------------------------------------------------------------------

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

    # -- 视图组装与副作用发布 -------------------------------------------------------------------------

    async def _member_view(self, pid: int, zone_id: int, is_leader: bool, join_seq: int, rec_member=None) -> Message:
        pres = await self.presence.get_presence(pid)
        view = self.protocol.message(
            "teampb.TeamMemberView",
            player_id=pid,
            name=pres.name,
            level=pres.level,
            class_id=pres.class_id,
            gender=pres.gender,
            is_leader=is_leader,
            is_online=pres.online,
            in_battle=pres.in_battle,
            zone_id=zone_id or pres.zone_id,
            join_seq=join_seq,
            appearance_id=pres.appearance_id,
            is_hosted=bool(pres.hosted),
        )
        if rec_member is not None:
            view.ranked_character_preset_id = int(rec_member.ranked_character_preset_id)
            view.ranked_pet_preset_id = int(rec_member.ranked_pet_preset_id)
            view.ranked_rule_version = int(rec_member.ranked_rule_version)
            view.ranked_preset_confirmed = bool(rec_member.ranked_preset_confirmed)
        return view

    async def _build_team_view(
        self,
        viewer_id: int,
        *,
        team_id: int,
        version: int,
        epoch: int,
        record: Message | None,
        now_ms: int,
        snap: TeamSnapshotResult | None = None,
    ) -> Message:
        view = self.protocol.message(
            "teampb.TeamView",
            team_id=team_id if record is not None else 0,
            capacity=MAX_TEAM_SIZE,
            version=version if record is not None else 0,
            membership_epoch=epoch,
            server_time_ms=now_ms,
        )
        if record is None or team_id == 0:
            return view
        leader_id = int(record.leader_id)
        view.leader_id = leader_id
        view.zone_id = int(record.zone_id)
        view.application_count = len(record.applications)
        view.match_fill_supported = True
        view.pvp_match_supported = True
        view.follow_formation = int(record.follow_formation)
        if record.match_lock_token:
            view.match_state = 1  # STARTING
        elif snap is not None and snap.launch_state == TEAM_LAUNCH_QUEUED:
            view.match_state = 2  # QUEUED
            view.match_target_size = snap.launch_target_size
            view.match_queued_at_ms = snap.launch_enqueued_ms
            view.match_battle_config_id = snap.launch_config_id
            view.match_mode = snap.launch_mode

        active_season = await self.store.get_active_ranked_season()
        active_rule_ver = active_season.rule_version if active_season is not None else self.ranked.rule.rule_version
        view.ranked_season_id = active_season.season_id if active_season is not None else 1
        view.ranked_rule_version = active_rule_ver

        ordered = sorted(record.members, key=lambda m: (int(m.join_seq), int(m.player_id)))
        all_confirmed = bool(ordered)
        for m in ordered:
            pid = int(m.player_id)
            if not m.ranked_preset_confirmed or int(m.ranked_rule_version) != active_rule_ver:
                all_confirmed = False
            mv = await self._member_view(pid, int(m.zone_id), pid == leader_id, int(m.join_seq), m)
            view.members.add().CopyFrom(mv)
        view.ranked_presets_all_confirmed = all_confirmed

        if viewer_id == leader_id:
            for app in record.applications:
                av = view.applications.add(applied_at_ms=int(app.applied_at_ms), expire_at_ms=int(app.expire_at_ms))
                av.player.CopyFrom(await self._member_view(int(app.player_id), int(app.zone_id), False, 0))
            for inv in record.invites:
                iv = view.pending_invites.add(expire_at_ms=int(inv.expire_at_ms))
                iv.invitee.CopyFrom(await self._member_view(int(inv.invitee_id), int(inv.zone_id), False, 0))
        return view

    async def _publish_outcome(self, caller_id: int, outcome: TeamMutationOutcome) -> None:
        """发布提交后的异步推送与 Scene 组队刷新信号。"""
        try:
            recipients: list[int] = []
            if outcome.reason in (8, 9) and outcome.record is not None:
                recipients = [int(outcome.record.leader_id)]
            elif outcome.reason not in (0, 1):
                recipients = unique_ints((*outcome.joined, *outcome.kept, *outcome.left))

            for pid in recipients:
                if pid == caller_id:
                    continue
                is_left = pid in outcome.left
                view = await self._build_team_view(
                    pid,
                    team_id=0 if is_left else outcome.team_id,
                    version=0 if is_left else outcome.version,
                    epoch=outcome.epochs.get(pid, 0),
                    record=None if is_left else outcome.record,
                    now_ms=outcome.now_ms,
                )
                snap_msg = self.protocol.message(
                    "teampb.TeamSnapshotS2C", team=view, reason=outcome.reason, actor_id=outcome.actor_id
                )
                await self.notifier.push_to_player(pid, MSG_NOTIFY_TEAM_SNAPSHOT, snap_msg)

            if outcome.invited_player and outcome.record is not None:
                inv = next(
                    (i for i in outcome.record.invites if int(i.invitee_id) == outcome.invited_player),
                    None,
                )
                if inv is not None:
                    inv_view = self.protocol.message(
                        "teampb.TeamIncomingInviteView",
                        team_id=outcome.team_id,
                        leader_id=int(outcome.record.leader_id),
                        member_count=len(outcome.record.members),
                        zone_id=int(outcome.record.zone_id),
                        expire_at_ms=int(inv.expire_at_ms),
                    )
                    inv_view.inviter.CopyFrom(
                        await self._member_view(int(inv.inviter_id), int(outcome.record.zone_id), True, 1)
                    )
                    await self.notifier.push_to_player(
                        outcome.invited_player,
                        MSG_NOTIFY_TEAM_INVITE,
                        self.protocol.message(
                            "teampb.TeamInviteS2C", invite=inv_view, server_time_ms=outcome.now_ms
                        ),
                    )

            if outcome.rejected_applicant and outcome.rejected_applicant != caller_id:
                await self.notifier.push_to_player(
                    outcome.rejected_applicant,
                    MSG_NOTIFY_TEAM_EVENT,
                    self.protocol.message(
                        "teampb.TeamEventS2C",
                        type=1,  # APPLICATION_REJECTED
                        team_id=outcome.team_id,
                        actor_id=caller_id,
                    ),
                )

            for inv_pid in outcome.revoked_invitees:
                if inv_pid != caller_id:
                    await self.notifier.push_to_player(
                        inv_pid,
                        MSG_NOTIFY_TEAM_EVENT,
                        self.protocol.message(
                            "teampb.TeamEventS2C",
                            type=2,  # INVITE_REVOKED
                            team_id=outcome.team_id,
                            actor_id=outcome.actor_id,
                        ),
                    )

            for rec_pid in outcome.recall_recipients:
                if rec_pid != caller_id:
                    await self.notifier.push_to_player(
                        rec_pid,
                        MSG_NOTIFY_TEAM_EVENT,
                        self.protocol.message(
                            "teampb.TeamEventS2C",
                            type=3,  # TEAM_EVENT_TYPE_LEADER_RECALL
                            team_id=outcome.team_id,
                            actor_id=outcome.actor_id,
                        ),
                    )

            refresh_ids = list(outcome.joined) + list(outcome.left)
            if outcome.leader_changed or outcome.formation_changed:
                refresh_ids.extend(outcome.kept)
            for pid in unique_ints(refresh_ids):
                await self.notifier.refresh_scene(pid)
        except Exception:
            logger.warning("team post-commit notification failed team_id=%d", outcome.team_id, exc_info=True)

    async def _outcome_response(self, caller: CallerIdentity, outcome: TeamMutationOutcome) -> Message:
        is_left = caller.player_id in outcome.left
        view = await self._build_team_view(
            caller.player_id,
            team_id=0 if is_left else outcome.team_id,
            version=0 if is_left else outcome.version,
            epoch=outcome.epochs.get(caller.player_id, 0),
            record=None if is_left else outcome.record,
            now_ms=outcome.now_ms,
        )
        await self._publish_outcome(caller.player_id, outcome)
        return self.protocol.message("teampb.TeamResponse", team=view)

    async def _error_team_response(self, caller: CallerIdentity, code: int, *params: object) -> Message:
        resp = self.protocol.message("teampb.TeamResponse")
        set_tip(resp, code, *params)
        try:
            snap = await self.store.get_my_team(caller.player_id)
            resp.team.CopyFrom(
                await self._build_team_view(
                    caller.player_id,
                    team_id=snap.team_id,
                    version=snap.version,
                    epoch=snap.membership_epoch,
                    record=snap.record,
                    now_ms=snap.now_ms,
                    snap=snap,
                )
            )
        except Exception:
            pass
        return resp

    # -- ClientPlayerTeam RPC 实现 --------------------------------------------------------------------

    async def push_noop(self, request: Message, context, path: str) -> Message:
        return self.protocol.message("Empty")

    async def create_team(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        pres = await self.presence.get_presence(caller.player_id)
        try:
            outcome = await self.store.create_team(caller, pres.zone_id or self.config.zone_id, request.request_id)
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)
        except OriginalRoleError as exc:
            await context.abort(CODE.PERMISSION_DENIED, str(exc))

    async def get_my_team(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        snap = await self.store.get_my_team(
            caller.player_id, match_request_id=request.match_request_id, caller=caller
        )
        view = await self._build_team_view(
            caller.player_id,
            team_id=snap.team_id,
            version=snap.version,
            epoch=snap.membership_epoch,
            record=snap.record,
            now_ms=snap.now_ms,
            snap=snap,
        )
        resp = self.protocol.message("teampb.TeamResponse", team=view)
        if request.match_request_id and (snap.launch_state or snap.launch_battle_id):
            resp.match_intent.request_id = request.match_request_id
            resp.match_intent.battle_id = snap.launch_battle_id
            resp.match_intent.state = snap.launch_state
        if request.regroup and view.team_id and view.leader_id and view.leader_id != caller.player_id:
            await self.notifier.refresh_scene(caller.player_id)
        if request.notify_online and snap.record is not None:
            for m in snap.record.members:
                pid = int(m.player_id)
                if pid != caller.player_id:
                    await self.notifier.push_to_player(
                        pid,
                        MSG_NOTIFY_TEAM_SNAPSHOT,
                        self.protocol.message(
                            "teampb.TeamSnapshotS2C", team=view, reason=10, actor_id=caller.player_id
                        ),
                    )
        return resp

    async def apply_join_team(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        pres = await self.presence.get_presence(caller.player_id)
        try:
            outcome = await self.store.apply_join_team(
                caller,
                int(request.target_player_id),
                pres.zone_id or self.config.zone_id,
                request.request_id,
                allow_cross_zone=self.config.allow_cross_zone,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def handle_application(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.handle_application(
                caller,
                int(request.applicant_id),
                bool(request.approve),
                int(request.expected_team_id),
                request.request_id,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def invite_to_team(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        target_id = int(request.target_player_id)
        target_pres = await self.presence.get_presence(target_id)
        try:
            outcome = await self.store.invite_to_team(
                caller,
                target_id,
                target_pres.zone_id or self.config.zone_id,
                int(request.expected_team_id),
                request.request_id,
                allow_cross_zone=self.config.allow_cross_zone,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def respond_invite(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        pres = await self.presence.get_presence(caller.player_id)
        try:
            outcome = await self.store.respond_invite(
                caller,
                int(request.team_id),
                bool(request.accept),
                pres.zone_id or self.config.zone_id,
                request.request_id,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def list_my_invites(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        invites, now_ms = await self.store.list_my_invites(caller.player_id)
        resp = self.protocol.message("teampb.ListMyInvitesResponse", server_time_ms=now_ms)
        for item in invites:
            entry = resp.invites.add(
                team_id=item.team_id,
                leader_id=item.leader_id,
                member_count=item.member_count,
                zone_id=item.zone_id,
                expire_at_ms=item.expire_at_ms,
            )
            entry.inviter.CopyFrom(await self._member_view(item.inviter_id, item.zone_id, True, 1))
        return resp

    async def leave_team(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.leave_team(caller, int(request.expected_team_id), request.request_id)
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def kick_member(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.kick_member(
                caller, int(request.target_player_id), int(request.expected_team_id), request.request_id
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def transfer_leader(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.transfer_leader(
                caller, int(request.target_player_id), int(request.expected_team_id), request.request_id
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def disband_team(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.disband_team(caller, int(request.expected_team_id), request.request_id)
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def confirm_ranked_preset(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        char_preset = int(request.character_preset_id)
        pet_preset = int(request.pet_preset_id)
        rule_ver = int(request.rule_version)
        try:
            await self.ranked.validate_team_preset_confirm(char_preset, pet_preset, rule_ver)
            outcome = await self.store.confirm_ranked_preset(
                caller,
                int(request.expected_team_id),
                char_preset,
                pet_preset,
                rule_ver,
                request.request_id,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def set_team_formation(self, request: Message, context, path: str) -> Message:
        """524：队长切换跑图跟随队形（0=长蛇 / 1=雁行 / 2=五行），变更时广播并刷新全员场景队形。"""
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.set_team_formation(
                caller,
                int(request.expected_team_id),
                int(request.formation),
                request.request_id,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def recall_team_members(self, request: Message, context, path: str) -> Message:
        """525：队长一键召集队员归队（15 秒团队冷却，向非队长队员推送 LEADER_RECALL 事件）。"""
        caller = await self._admit_client(context, path)
        try:
            outcome = await self.store.recall_team_members(
                caller,
                int(request.expected_team_id),
                request.request_id,
            )
            return await self._outcome_response(caller, outcome)
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

    async def start_team_match(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_client(context, path)
        mode = int(request.match_mode) or MODE_PVE_TEAM
        if mode not in (MODE_PVE_TEAM, MODE_PVP_TEAM):
            return await self._error_team_response(caller, ERR_TEAM_DUNGEON_NOT_OPEN)
        config_id = int(request.battle_config_id)
        if mode == MODE_PVE_TEAM:
            max_size = self.config.pve_team_sizes.get(config_id, 0)
            if max_size <= 0:
                return await self._error_team_response(caller, ERR_TEAM_DUNGEON_NOT_OPEN)
        else:
            max_size = MAX_TEAM_SIZE

        snap = await self.store.get_my_team(caller.player_id)
        if snap.record is None or (int(request.expected_team_id) and snap.team_id != int(request.expected_team_id)):
            return await self._error_team_response(caller, 4013)  # ERR_TEAM_NO_TEAM
        member_ids = [int(m.player_id) for m in snap.record.members]
        target_size = int(request.target_team_size) or len(member_ids)
        if target_size < len(member_ids) or target_size > min(max_size, MAX_TEAM_SIZE):
            return await self._error_team_response(caller, ERR_TEAM_SIZE_EXCEEDED)

        trial_vow_mask = int(getattr(request, "trial_vow_mask", 0))
        if trial_vow_mask != 0:
            if (
                not valid_trial_vow_mask(trial_vow_mask)
                or mode != MODE_PVE_TEAM
                or target_size != len(member_ids)
                or not supports_trial_vows(config_id)
            ):
                return await self._error_team_response(caller, ERR_TEAM_DUNGEON_NOT_OPEN)

        member_zones: dict[int, int] = {}
        for pid in member_ids:
            pres = await self.presence.get_presence(pid)
            if not pres.online:
                return await self._error_team_response(caller, ERR_TEAM_MEMBER_OFFLINE, pid)
            if pres.in_battle:
                return await self._error_team_response(caller, ERR_TEAM_MEMBER_IN_BATTLE, pid)
            member_zones[pid] = pres.zone_id or self.config.zone_id

        immediate_node = None
        if mode == MODE_PVE_TEAM and target_size == len(member_ids):
            nodes = await self.battle_nodes.list_nodes(release_track=caller.release_track)
            node = select_battle_node(nodes, release_track=caller.release_track)
            if node is None:
                return await self._error_team_response(caller, ERR_TEAM_INTERNAL)
            immediate_node = (node.node_id, node.node_uuid, node.grpc_endpoint, node.physical_region, node.release_track)

        try:
            outcome, formation, launch = await self.store.start_team_match(
                caller,
                expected_team_id=int(request.expected_team_id),
                battle_config_id=config_id,
                mode=mode,
                target_team_size=target_size,
                request_id=request.request_id,
                member_zones=member_zones,
                immediate_battle_node=immediate_node,
                battle_deadline_ms=self.config.battle_deadline_sec * 1000,
                queue_ttl_ms=self.config.queue_ttl_sec * 1000,
                trial_vow_mask=trial_vow_mask,
            )
        except TeamBusinessError as exc:
            return await self._error_team_response(caller, exc.code, *exc.params)

        if formation is not None:
            if self._inline_gather:
                await self.gather.run_formation(formation)
            else:
                self._spawn(self.gather.run_formation(formation), name=f"team-gather-{formation.battle_id}")

        resp = await self._outcome_response(caller, outcome)
        resp.match_intent.request_id = request.request_id
        resp.match_intent.battle_id = int(launch.battle_id)
        resp.match_intent.state = int(launch.state)
        return resp

    # -- TeamAuthority.ReadMembership（场景权威读，签名 caller="scene" -> 回包 caller="team"）-----------

    async def read_membership(self, request: Message, context, path: str) -> Message:
        if has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "TeamAuthority.ReadMembership allows internal scene calls only")
        now_ms = int(self._clock_ms())
        if not verify_internal_hmac(path, request, "scene", self.config.scene_secret, now_ms):
            await context.abort(CODE.PERMISSION_DENIED, "invalid scene service call signature")

        admission = request.admission
        reply = self.protocol.message("teampb.ReadTeamMembershipResponse")
        if int(admission.player_id) == 0 or int(admission.owner_revision) == 0:
            reply.retry = True
        else:
            try:
                team_id, epoch, version, record = await self.store.read_membership(
                    int(admission.player_id), owner_revision=int(admission.owner_revision)
                )
                reply.team_id = team_id
                reply.membership_epoch = epoch
                reply.version = version
                if record is not None:
                    reply.record.CopyFrom(record)
            except OriginalRoleError:
                reply.retry = True
            except Exception:
                logger.error(
                    "team ReadMembership failed player_id=%d", int(admission.player_id), exc_info=True
                )
                reply.retry = True

        sign_internal_reply(path, request, reply, "team", self.config.scene_secret, int(self._clock_ms()))
        return reply
