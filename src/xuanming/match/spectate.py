"""观战匹配、分层扇出凭证签发（SpectateTicket）与丢票补签（RequestBattleTicket）。"""

from __future__ import annotations

import logging
import random
import uuid

from google.protobuf.message import DecodeError, Message

from ..protocol import Protocol
from .config import MatchConfig
from .errors import (
    ERR_MATCH_BATTLE_NOT_WATCHABLE,
    ERR_MATCH_INTERNAL,
    ERR_MATCH_NO_WATCHABLE_BATTLE,
    ERR_MATCH_SPECTATE_FULL,
    ERR_MATCH_SPECTATE_OFFLINE,
    ERR_MATCH_SPECTATE_WHILE_IN_BATTLE,
    ERR_MATCH_SPECTATE_WHILE_QUEUED,
    MatchBusinessError,
    set_tip,
)
from .gather import EndpointCaller, PresenceProvider
from .model import (
    FORMATION_STATE_CREATED,
    SPECTATE_DELIVERY_DIRECT,
    SPECTATE_DELIVERY_LOBBY,
    SPECTATE_TIER_BROADCAST,
    SPECTATE_TIER_DIRECT,
    SPECTATE_VISIBILITY_PUBLIC,
    SPECTATOR_STATE_ACTIVE,
    CallerIdentity,
    formation_outcome_decided,
    sign_spectate_ticket,
)
from .store import MatchStore

logger = logging.getLogger(__name__)

ADD_OBSERVER_PATH = "/BattleNode/AddObserver"
REMOVE_OBSERVER_PATH = "/BattleNode/RemoveObserver"
ISSUE_BATTLE_TICKET_PATH = "/BattleNode/IssueBattleTicket"
ATTACH_RELAY_GATE_PATH = "/BattleNode/AttachRelayGate"
PROMOTE_SPECTATE_BROADCAST_PATH = "/BattleNode/PromoteSpectateBroadcast"


def _decode_create_request(protocol: Protocol, formation: Message) -> Message | None:
    if not formation.create_request:
        return None
    try:
        return protocol.parse("CreateBattleRequest", bytes(formation.create_request))
    except DecodeError:
        return None


def _is_public_watchable(protocol: Protocol, formation: Message) -> tuple[bool, Message | None]:
    create_req = _decode_create_request(protocol, formation)
    if create_req is None:
        return False, None
    return int(create_req.spectate_visibility) == SPECTATE_VISIBILITY_PUBLIC, create_req


class SpectateManager:
    """管理活跃公开对局列表、直连/大厅观战准入、赛事广播与战斗票据补签。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        config: MatchConfig,
        rpc: EndpointCaller,
        presence: PresenceProvider,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config
        self.rpc = rpc
        self.presence = presence

    async def list_watchable_battles(self, caller: CallerIdentity, limit: int) -> Message:
        capped = min(max(limit or 20, 1), 100)
        active = await self.store.list_active_formations(release_track=caller.release_track, limit=capped)
        resp = self.protocol.message("match.ListWatchableBattlesResponse")
        for formation, featured in active:
            public, create_req = _is_public_watchable(self.protocol, formation)
            if not public or create_req is None:
                continue
            summary = resp.battles.add(
                battle_id=int(formation.battle_id),
                mode=int(formation.mode),
                battle_config_id=int(formation.config_id),
                created_at_ms=int(formation.created_ms),
                featured=featured,
            )
            for p in create_req.players:
                summary.player_names.append(p.player_name)
        return resp

    async def watch_battle(self, caller: CallerIdentity, req: Message, *, force_lobby: bool = False) -> Message:
        resp = self.protocol.message("match.WatchBattleResponse")
        pres = await self.presence.get_presence(caller.player_id)
        if pres.in_battle:
            return set_tip(resp, ERR_MATCH_SPECTATE_WHILE_IN_BATTLE)
        if not caller.gate_instance_id:
            return set_tip(resp, ERR_MATCH_SPECTATE_OFFLINE)

        target_battle_id = int(req.battle_id)
        formation: Message | None = None
        featured = False
        if target_battle_id == 0:
            active = await self.store.list_active_formations(release_track=caller.release_track, limit=50)
            public_list = [(f, feat) for f, feat in active if _is_public_watchable(self.protocol, f)[0]]
            if not public_list:
                return set_tip(resp, ERR_MATCH_NO_WATCHABLE_BATTLE)
            formation, featured = random.choice(public_list)
            target_battle_id = int(formation.battle_id)
        else:
            formation = await self.store.get_formation(target_battle_id)
            if (
                formation is None
                or int(formation.state) != FORMATION_STATE_CREATED
                or formation_outcome_decided(formation)
            ):
                return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)
            track = formation.release_track or "stable"
            if caller.release_track == "stable" and track != "stable":
                return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)
            public, _ = _is_public_watchable(self.protocol, formation)
            if not public:
                return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)

        # 不能观战自己参与的对局
        if any(int(m.player_id) == caller.player_id for m in formation.members):
            return set_tip(resp, ERR_MATCH_SPECTATE_WHILE_IN_BATTLE)

        # 检查是否持有未结束的匹配票
        status_info = await self.store.get_queue_status(caller)
        if status_info.ticket is not None and int(status_info.ticket.state) in (1, 2, 3):
            return set_tip(resp, ERR_MATCH_SPECTATE_WHILE_QUEUED)

        # 若明确要求大厅模式（或该局已是赛事局），优先走大厅中继并签发 SpectateTicket
        if (force_lobby or featured) and len(self.config.spectate_secret) >= 32:
            lobby_resp = await self._admit_lobby(caller, formation)
            if lobby_resp is not None:
                return lobby_resp

        # 默认直连观战（L0）：向 BattleNode.AddObserver 登记观众
        now_ms = await self.store.now_ms()
        op_id = str(uuid.uuid4())
        add_req = self.protocol.message(
            "AddObserverRequest",
            battle_id=target_battle_id,
            observer_player_id=caller.player_id,
            observer_name=pres.name,
            spectator_operation_id=op_id,
            admit_before_ms=now_ms + 20_000,
            target_instance_id=formation.target_instance_id,
        )
        add_req.routing.session_id = caller.session_id
        add_req.routing.gate_node_id = caller.gate_node_id
        add_req.routing.gate_instance_id = caller.gate_instance_id
        add_req.routing.zone_id = pres.zone_id

        add_bytes = add_req.SerializeToString(deterministic=True)
        try:
            await self.store.prepare_spectator(
                caller,
                target_battle_id,
                op_id,
                formation.target_endpoint,
                formation.target_instance_id,
                req.SerializeToString(deterministic=True),
                add_bytes,
                req.request_id,
            )
        except MatchBusinessError as exc:
            return set_tip(resp, exc.code, *exc.params)

        try:
            raw_add = await self.rpc.call(formation.target_endpoint, ADD_OBSERVER_PATH, add_bytes, timeout=3.0)
            add_resp = self.protocol.parse("AddObserverResponse", raw_add)
        except Exception:
            logger.warning("match spectate AddObserver rpc failed battle_id=%d player_id=%d", target_battle_id, caller.player_id, exc_info=True)
            await self.store.close_spectator(caller.player_id, op_id)
            return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)

        err_id = int(add_resp.error_message.id)
        if err_id != 0:
            await self.store.close_spectator(caller.player_id, op_id)
            # 直连席满时若有观战密钥则无缝升级到大厅中继（L1）
            if err_id == ERR_MATCH_SPECTATE_FULL and len(self.config.spectate_secret) >= 32:
                lobby_resp = await self._admit_lobby(caller, formation)
                if lobby_resp is not None:
                    return lobby_resp
            mapped = ERR_MATCH_SPECTATE_FULL if err_id == ERR_MATCH_SPECTATE_FULL else ERR_MATCH_BATTLE_NOT_WATCHABLE
            return set_tip(resp, mapped)

        await self.store.commit_spectator(caller.player_id, op_id)
        resp.battle_id = target_battle_id
        resp.spectator_operation_id = op_id
        resp.delivery = SPECTATE_DELIVERY_DIRECT
        return resp

    async def _admit_lobby(self, caller: CallerIdentity, formation: Message) -> Message | None:
        """L1/L2 大厅观战准入：挂接 Gate 中继并签发 HMAC-SHA256 SpectateTicket。"""
        battle_id = int(formation.battle_id)
        attach_req = self.protocol.message(
            "AttachRelayGateRequest",
            battle_id=battle_id,
            gate_node_id=caller.gate_node_id,
            gate_instance_id=caller.gate_instance_id,
            target_instance_id=formation.target_instance_id,
        )
        try:
            raw = await self.rpc.call(
                formation.target_endpoint,
                ATTACH_RELAY_GATE_PATH,
                attach_req.SerializeToString(deterministic=True),
                timeout=3.0,
            )
            attach_resp = self.protocol.parse("AttachRelayGateResponse", raw)
        except Exception:
            logger.warning("match spectate AttachRelayGate failed battle_id=%d", battle_id, exc_info=True)
            return None
        if int(attach_resp.error_message.id) != 0:
            return None
        instance_id = attach_resp.battle_instance_id or formation.target_instance_id
        deadline_ms = int(attach_resp.deadline_ms) or int(formation.deadline_ms)
        now_ms = await self.store.now_ms()
        ticket_bytes = sign_spectate_ticket(
            self.protocol,
            self.config.spectate_secret,
            battle_id=battle_id,
            player_id=caller.player_id,
            battle_instance_id=instance_id,
            deadline_ms=deadline_ms,
            expire_at_ms=now_ms + self.config.spectate_ticket_ttl_sec * 1000,
        )
        return self.protocol.message(
            "match.WatchBattleResponse",
            battle_id=battle_id,
            delivery=SPECTATE_DELIVERY_LOBBY,
            spectate_ticket=ticket_bytes,
        )

    async def request_battle_ticket(self, caller: CallerIdentity, battle_id: int) -> Message:
        """丢票补签：按 SQL 成局定位 Battle 节点，透传 BattleNode.IssueBattleTicket。"""
        resp = self.protocol.message("RequestBattleTicketResponse")
        if battle_id == 0:
            return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)
        formation = await self.store.get_formation(battle_id)
        if formation is None or int(formation.state) != FORMATION_STATE_CREATED or formation_outcome_decided(formation):
            return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)

        is_participant = any(int(m.player_id) == caller.player_id for m in formation.members)
        spec_op_id = ""
        if not is_participant:
            spec = await self.store.get_spectator(caller.player_id)
            if (
                spec is None
                or int(spec.battle_id) != battle_id
                or int(spec.state) != SPECTATOR_STATE_ACTIVE
                or not spec.operation_id
            ):
                return set_tip(resp, ERR_MATCH_BATTLE_NOT_WATCHABLE)
            spec_op_id = spec.operation_id

        issue_req = self.protocol.message(
            "IssueBattleTicketRequest",
            battle_id=battle_id,
            player_id=caller.player_id,
            spectator_operation_id=spec_op_id,
            target_instance_id=formation.target_instance_id,
        )
        try:
            raw = await self.rpc.call(
                formation.target_endpoint,
                ISSUE_BATTLE_TICKET_PATH,
                issue_req.SerializeToString(deterministic=True),
                timeout=3.0,
            )
            issue_resp = self.protocol.parse("IssueBattleTicketResponse", raw)
        except Exception:
            logger.warning("match RequestBattleTicket rpc failed battle_id=%d player_id=%d", battle_id, caller.player_id, exc_info=True)
            return set_tip(resp, ERR_MATCH_INTERNAL)
        if int(issue_resp.error_message.id) != 0:
            resp.error_message.CopyFrom(issue_resp.error_message)
            return resp
        resp.assignment.CopyFrom(issue_resp.assignment)
        return resp

    async def set_spectate_featured(self, battle_id: int, featured: bool) -> tuple[int, int]:
        """标记/取消赛事局并尝试升级全服广播，返回 (SpectateFeaturedReject, eSpectateTier)。"""
        formation = await self.store.get_formation(battle_id)
        if formation is None or int(formation.state) != FORMATION_STATE_CREATED or formation_outcome_decided(formation):
            return 1, 0  # NOT_FOUND
        public, _ = _is_public_watchable(self.protocol, formation)
        if not public:
            return 2, 0  # PRIVATE
        if not await self.store.set_formation_featured(battle_id, featured):
            return 1, 0
        if not featured:
            return 0, SPECTATE_TIER_DIRECT
        promo_req = self.protocol.message(
            "PromoteSpectateBroadcastRequest",
            battle_id=battle_id,
            target_instance_id=formation.target_instance_id,
        )
        try:
            raw = await self.rpc.call(
                formation.target_endpoint,
                PROMOTE_SPECTATE_BROADCAST_PATH,
                promo_req.SerializeToString(deterministic=True),
                timeout=3.0,
            )
            promo_resp = self.protocol.parse("PromoteSpectateBroadcastResponse", raw)
            if int(promo_resp.error_message.id) == 0:
                return 0, SPECTATE_TIER_BROADCAST
        except Exception:
            logger.warning("match PromoteSpectateBroadcast failed battle_id=%d", battle_id, exc_info=True)
        return 0, SPECTATE_TIER_DIRECT
