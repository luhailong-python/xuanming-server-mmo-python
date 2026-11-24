"""player_inbox 三组 gRPC 服务适配层与 5 凭据 fail-closed 鉴权。

对外暴露：
- ``player_inbox.PlayerInbox``: FetchItems / AckItems / EnqueueBattleSettlement / LookupBattleSettlement
- ``player_inbox.PlayerInboxAdmin``: GmSanctionPlayer / GetSummary / ListItems / LookupByRequest /
  VoidItem / ReconcilePlayer / GetItemGainBlocks
- ``rolefence.RoleFenceParticipant``: PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import time
from collections.abc import Callable

import grpc
from google.protobuf.message import Message

from ..grpc_runtime import service_digest, sign_service_call, verify_service_call
from ..protocol import Protocol
from .config import MIN_ADMIN_TOKEN_BYTES
from .errors import (
    BattleSettlementOriginalError,
    ERR_INVALID_PARAMETER,
    ERR_PAYLOAD_INVALID,
    ERR_STALE_OWNER,
    ERR_STORE_UNAVAILABLE,
    InvalidRoleFenceStateError,
    OriginalRoleError,
    RoleFenceUnavailableError,
)
from .model import (
    AUDIT_SANCTION,
    AuditInput,
    MAX_OPERATOR_BYTES,
    MAX_REASON_BYTES,
    MSG_GM_ATTACH_CURRENCY_DEBT,
    MSG_GM_BLOCK_CURRENCY_GAIN,
    MSG_GM_BLOCK_ITEM_GAIN,
    MSG_GM_UNBLOCK_CURRENCY_GAIN,
    MSG_GM_UNBLOCK_ITEM_GAIN,
    PRODUCER_GM,
    EnqueueParams,
    VoidParams,
    validate_text,
)
from .store import InboxStore

logger = logging.getLogger(__name__)

SERVICE_INBOX = "player_inbox.PlayerInbox"
SERVICE_ADMIN = "player_inbox.PlayerInboxAdmin"
SERVICE_ROLE_FENCE = "rolefence.RoleFenceParticipant"

ADMIN_TOKEN_METADATA_KEY = "x-admin-token"
SESSION_DETAIL_METADATA_KEY = "x-session-detail-bin"

CODE = grpc.StatusCode


def _metadata_dict(context) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    if context is None:
        return result
    md = context.invocation_metadata() or ()
    for key, value in md:
        k = key.lower()
        v = value.decode("utf-8", errors="ignore") if isinstance(value, (bytes, bytearray)) else str(value)
        result.setdefault(k, []).append(v)
    return result


def _peer_addr(context) -> str:
    if context is None:
        return "unknown"
    try:
        peer = context.peer()
        return peer if peer else "unknown"
    except Exception:
        return "unknown"


def _has_session_metadata(context) -> bool:
    return SESSION_DETAIL_METADATA_KEY in _metadata_dict(context)


def _sanction_payload(protocol: Protocol, req: Message) -> tuple[int, Message | None]:
    """将 GmSanctionPlayerRequest 的 action oneof 映射为 (msg_type, 确定性序列化载荷消息)。"""
    which = req.WhichOneof("action")
    if which == "block_gain":
        return MSG_GM_BLOCK_CURRENCY_GAIN, req.block_gain
    if which == "unblock_gain":
        return MSG_GM_UNBLOCK_CURRENCY_GAIN, req.unblock_gain
    if which == "block_item_gain":
        return MSG_GM_BLOCK_ITEM_GAIN, req.block_item_gain
    if which == "unblock_item_gain":
        return MSG_GM_UNBLOCK_ITEM_GAIN, req.unblock_item_gain
    if which == "attach_debt":
        debt = protocol.message("player_inbox.GmCurrencyDebtPayload")
        debt.CopyFrom(req.attach_debt)
        debt.gm_operator = req.operator
        return MSG_GM_ATTACH_CURRENCY_DEBT, debt
    return 0, None


class PlayerInboxService:
    """集中挂载 PlayerInbox / PlayerInboxAdmin / RoleFenceParticipant 三个 gRPC 服务。"""

    def __init__(
        self,
        protocol: Protocol,
        store: InboxStore,
        *,
        scene_secret: bytes,
        coordinator_secret: bytes,
        match_secret: bytes = b"",
        admin_token: str = "",
        request_budget_sec: float = 4.5,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._scene_secret = scene_secret
        self._match_secret = match_secret
        self._coordinator_secret = coordinator_secret
        self._admin_token = admin_token
        self._budget_sec = request_budget_sec
        self._clock_ms = clock_ms

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_INBOX: {
                "FetchItems": self.fetch_items,
                "AckItems": self.ack_items,
                "EnqueueBattleSettlement": self.enqueue_battle_settlement,
                "LookupBattleSettlement": self.lookup_battle_settlement,
            },
            SERVICE_ADMIN: {
                "GmSanctionPlayer": self.gm_sanction_player,
                "GetSummary": self.get_summary,
                "ListItems": self.list_items,
                "LookupByRequest": self.lookup_by_request,
                "VoidItem": self.void_item,
                "ReconcilePlayer": self.reconcile_player,
                "GetItemGainBlocks": self.get_item_gain_blocks,
            },
            SERVICE_ROLE_FENCE: {
                "PrepareRoleFreeze": self.prepare_role_freeze,
                "ApplyRoleFence": self.apply_role_fence,
                "PurgeRemovedRole": self.purge_removed_role,
            },
        }

    # -- 鉴权门禁（fail-closed） ----------------------------------------------------------------------

    async def _admit_scene(self, context, request: Message, path: str) -> None:
        if _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "inbox consumer allows internal scene calls only")
        if not self._scene_secret:
            await context.abort(CODE.UNAVAILABLE, "inbox scene secret not configured")
        if not verify_service_call(path, request, "scene", self._clock_ms(), self._scene_secret):
            await context.abort(CODE.PERMISSION_DENIED, "invalid scene service call signature")

    async def _admit_match(self, context, request: Message, path: str) -> None:
        if _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "battle settlement allows internal match calls only")
        if not self._match_secret:
            await context.abort(CODE.UNAVAILABLE, "inbox match secret not configured")
        if not verify_service_call(path, request, "match", self._clock_ms(), self._match_secret):
            await context.abort(CODE.PERMISSION_DENIED, "invalid match service call signature")

    async def _admit_admin(self, context, path: str) -> None:
        token = self._admin_token
        if not token or len(token.encode("utf-8")) < MIN_ADMIN_TOKEN_BYTES:
            logger.error("player_inbox admin call refused: admin surface disabled path=%s peer=%s", path, _peer_addr(context))
            await context.abort(CODE.PERMISSION_DENIED, f"{path} is disabled because no admin token is configured")
        vals = _metadata_dict(context).get(ADMIN_TOKEN_METADATA_KEY, ())
        got = vals[0] if vals else ""
        if not hmac.compare_digest(got.encode("utf-8"), token.encode("utf-8")):
            logger.error(
                "player_inbox admin call refused: bad or missing x-admin-token present=%s path=%s peer=%s",
                bool(got),
                path,
                _peer_addr(context),
            )
            await context.abort(CODE.PERMISSION_DENIED, f"{path} requires a valid {ADMIN_TOKEN_METADATA_KEY}")

    async def _admit_coordinator(self, context, request: Message, path: str) -> None:
        if not self._coordinator_secret:
            await context.abort(CODE.UNAVAILABLE, "role fence coordinator secret not configured")
        if not verify_service_call(path, request, "coordinator", self._clock_ms(), self._coordinator_secret):
            await context.abort(CODE.PERMISSION_DENIED, "invalid role fence coordinator signature")

    async def _with_budget(self, coro):
        if self._budget_sec > 0:
            async with asyncio.timeout(self._budget_sec):
                return await coro
        return await coro

    # -- PlayerInbox ----------------------------------------------------------------------------------

    async def fetch_items(self, request: Message, context, path: str) -> Message:
        await self._admit_scene(context, request, path)
        try:
            return await self._with_budget(self.store.fetch(request, peer_addr=_peer_addr(context)))
        except (OriginalRoleError, RoleFenceUnavailableError) as exc:
            logger.info(
                "player_inbox FetchItems role not verified player=%d domain=%d epoch=%d reason=%s",
                request.player_id,
                request.domain,
                request.owner_epoch,
                exc,
            )
            resp = self.protocol.message(
                "player_inbox.FetchPlayerInboxResponse",
                player_id=request.player_id,
                domain=request.domain,
                correlation_id=request.correlation_id,
            )
            resp.error_message.id = ERR_STALE_OWNER
            return resp
        except Exception:
            logger.error(
                "player_inbox FetchItems failed player=%d domain=%d epoch=%d",
                request.player_id,
                request.domain,
                request.owner_epoch,
                exc_info=True,
            )
            resp = self.protocol.message(
                "player_inbox.FetchPlayerInboxResponse",
                player_id=request.player_id,
                domain=request.domain,
                correlation_id=request.correlation_id,
            )
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def ack_items(self, request: Message, context, path: str) -> Message:
        await self._admit_scene(context, request, path)
        try:
            return await self._with_budget(self.store.ack(request, peer_addr=_peer_addr(context)))
        except (OriginalRoleError, RoleFenceUnavailableError) as exc:
            logger.info(
                "player_inbox AckItems role not verified player=%d domain=%d epoch=%d reason=%s",
                request.player_id,
                request.domain,
                request.owner_epoch,
                exc,
            )
            resp = self.protocol.message(
                "player_inbox.AckPlayerInboxResponse",
                player_id=request.player_id,
                domain=request.domain,
                correlation_id=request.correlation_id,
            )
            resp.error_message.id = ERR_STALE_OWNER
            return resp
        except Exception:
            logger.error(
                "player_inbox AckItems failed player=%d domain=%d epoch=%d",
                request.player_id,
                request.domain,
                request.owner_epoch,
                exc_info=True,
            )
            resp = self.protocol.message(
                "player_inbox.AckPlayerInboxResponse",
                player_id=request.player_id,
                domain=request.domain,
                correlation_id=request.correlation_id,
            )
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def enqueue_battle_settlement(self, request: Message, context, path: str) -> Message:
        return await self._battle_settlement(request, context, path, enqueue=True)

    async def lookup_battle_settlement(self, request: Message, context, path: str) -> Message:
        return await self._battle_settlement(request, context, path, enqueue=False)

    async def _battle_settlement(self, request: Message, context, path: str, *, enqueue: bool) -> Message:
        await self._admit_match(context, request, path)
        req_digest = service_digest(request)
        try:
            coro = (
                self.store.enqueue_battle_settlement(request)
                if enqueue
                else self.store.lookup_battle_settlement(request)
            )
            resp = await self._with_budget(coro)
        except BattleSettlementOriginalError as exc:
            logger.error("player_inbox battle settlement original rejected path=%s err=%s", path, exc)
            await context.abort(CODE.FAILED_PRECONDITION, "battle settlement original conflict")
        except Exception:
            logger.error("player_inbox battle settlement receipt unconfirmed path=%s", path, exc_info=True)
            await context.abort(CODE.UNAVAILABLE, "battle settlement receipt unconfirmed")

        resp.request_digest = req_digest
        sign_service_call(path + "/reply", resp, "inbox", self._clock_ms(), self._match_secret)
        return resp

    # -- PlayerInboxAdmin -----------------------------------------------------------------------------

    async def gm_sanction_player(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)

        def reject(code: int) -> Message:
            out = self.protocol.message("player_inbox.GmSanctionPlayerResponse")
            out.error_message.id = code
            return out

        if request.player_id == 0 or request.request_id == 0 or request.issued_at_ms == 0:
            return reject(ERR_INVALID_PARAMETER)
        code = validate_text(request.operator, MAX_OPERATOR_BYTES, required=True)
        if code != 0:
            return reject(code)
        code = validate_text(request.reason, MAX_REASON_BYTES, required=True)
        if code != 0:
            return reject(code)
        msg_type, payload_msg = _sanction_payload(self.protocol, request)
        if msg_type == 0 or payload_msg is None:
            return reject(ERR_INVALID_PARAMETER)
        try:
            payload_bytes = payload_msg.SerializeToString(deterministic=True)
        except Exception:
            return reject(ERR_PAYLOAD_INVALID)

        peer = _peer_addr(context)
        params = EnqueueParams(
            player_id=int(request.player_id),
            msg_type=msg_type,
            producer_kind=PRODUCER_GM,
            request_id=int(request.request_id),
            issued_at_ms=int(request.issued_at_ms),
            payload=payload_bytes,
            operator=request.operator,
            note=request.reason,
        )
        audit = AuditInput(
            action=AUDIT_SANCTION,
            operator=request.operator,
            peer_addr=peer,
            reason=request.reason,
        )
        try:
            res = await self._with_budget(self.store.enqueue(params, audit=audit))
        except (OriginalRoleError, RoleFenceUnavailableError) as exc:
            logger.info(
                "player_inbox GmSanctionPlayer role not verified player=%d request_id=%d peer=%s reason=%s",
                request.player_id,
                request.request_id,
                peer,
                exc,
            )
            return reject(ERR_STALE_OWNER)
        except Exception:
            logger.error(
                "player_inbox GmSanctionPlayer failed player=%d request_id=%d peer=%s",
                request.player_id,
                request.request_id,
                peer,
                exc_info=True,
            )
            return reject(ERR_STORE_UNAVAILABLE)

        if res.code != 0:
            return reject(res.code)
        return self.protocol.message(
            "player_inbox.GmSanctionPlayerResponse",
            seq=res.seq,
            created=res.created,
            state=res.state,
        )

    async def get_summary(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)
        try:
            return await self._with_budget(self.store.summary(int(request.player_id)))
        except Exception:
            logger.error("player_inbox GetSummary failed player=%d peer=%s", request.player_id, _peer_addr(context), exc_info=True)
            resp = self.protocol.message("player_inbox.GetPlayerInboxSummaryResponse", player_id=request.player_id)
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def list_items(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)
        try:
            return await self._with_budget(self.store.list_items(request))
        except Exception:
            logger.error(
                "player_inbox ListItems failed player=%d domain=%d peer=%s",
                request.player_id,
                request.domain,
                _peer_addr(context),
                exc_info=True,
            )
            resp = self.protocol.message("player_inbox.ListPlayerInboxItemsResponse")
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def lookup_by_request(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)
        try:
            return await self._with_budget(self.store.lookup_by_request(request))
        except Exception:
            logger.error(
                "player_inbox LookupByRequest failed player=%d request_id=%d peer=%s",
                request.player_id,
                request.request_id,
                _peer_addr(context),
                exc_info=True,
            )
            resp = self.protocol.message("player_inbox.LookupPlayerInboxResponse")
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def void_item(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)
        params = VoidParams(
            player_id=int(request.player_id),
            domain=int(request.domain),
            seq=int(request.seq),
            reason=int(request.reason),
            operator=request.operator,
            note=request.note,
            peer_addr=_peer_addr(context),
            mode=int(request.mode),
        )
        try:
            return await self._with_budget(self.store.void_item(params))
        except (OriginalRoleError, RoleFenceUnavailableError) as exc:
            logger.info(
                "player_inbox VoidItem role not verified player=%d domain=%d seq=%d reason=%s",
                request.player_id,
                request.domain,
                request.seq,
                exc,
            )
            resp = self.protocol.message("player_inbox.VoidPlayerInboxItemResponse")
            resp.error_message.id = ERR_STALE_OWNER
            return resp
        except Exception:
            logger.error(
                "player_inbox VoidItem failed player=%d domain=%d seq=%d",
                request.player_id,
                request.domain,
                request.seq,
                exc_info=True,
            )
            resp = self.protocol.message("player_inbox.VoidPlayerInboxItemResponse")
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def reconcile_player(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)
        try:
            return await self._with_budget(self.store.reconcile_player(request))
        except Exception:
            logger.error(
                "player_inbox ReconcilePlayer failed player=%d domain=%d peer=%s",
                request.player_id,
                request.domain,
                _peer_addr(context),
                exc_info=True,
            )
            resp = self.protocol.message("player_inbox.ReconcilePlayerInboxResponse")
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    async def get_item_gain_blocks(self, request: Message, context, path: str) -> Message:
        await self._admit_admin(context, path)
        try:
            return await self._with_budget(self.store.item_gain_blocks(int(request.player_id)))
        except Exception:
            logger.error(
                "player_inbox GetItemGainBlocks failed player=%d peer=%s",
                request.player_id,
                _peer_addr(context),
                exc_info=True,
            )
            resp = self.protocol.message("player_inbox.GetPlayerItemGainBlocksResponse", player_id=request.player_id)
            resp.error_message.id = ERR_STORE_UNAVAILABLE
            return resp

    # -- RoleFenceParticipant -------------------------------------------------------------------------

    async def prepare_role_freeze(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(
                self.store.prepare_role_freeze(request.state, dry_run=bool(request.dry_run))
            )
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "player_inbox PrepareRoleFreeze failed player=%d version=%d dry_run=%s",
                request.state.player_id,
                request.state.version,
                request.dry_run,
                exc_info=True,
            )
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", retry=True)

    async def apply_role_fence(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(self.store.apply_role_fence(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "player_inbox ApplyRoleFence failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self.protocol.message("rolefence.ApplyRoleFenceResponse", retry=True)

    async def purge_removed_role(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(self.store.purge_removed_role(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "player_inbox PurgeRemovedRole failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)
