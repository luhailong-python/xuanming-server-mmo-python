"""mail 三组 gRPC 服务适配层与 fail-closed 鉴权。

对外暴露：
- ``mailpb.ClientPlayerMail``: ListMails / ReadMail / ClaimMailAttachments / DeleteMails / ClearReadMails / NotifyMailChanged(拒绝)
- ``mailpb.MailAdmin``: SendPersonalMail / SendSystemMail / SendGuildMail / ImportMailRecipients / SealMailCampaign
- ``rolefence.RoleFenceParticipant``: PrepareRoleFreeze / ApplyRoleFence / PurgeRemovedRole
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

import grpc
from google.protobuf.message import Message

from ..dualcontrol import DualControlError, extract_approval, validate_admin_approval
from ..grpc_runtime import verify_service_call
from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    InvalidRoleFenceStateError,
    rejection_tip_id,
)
from .model import (
    MAIL_KIND_GUILD,
    MAIL_KIND_PERSONAL,
    MAIL_KIND_SYSTEM,
    MAX_CLAIM_BATCH,
    RoleOriginal,
)
from .store import MailNotifier, MailStoreCore

logger = logging.getLogger(__name__)

SERVICE_CLIENT = "mailpb.ClientPlayerMail"
SERVICE_ADMIN = "mailpb.MailAdmin"
SERVICE_ROLE_FENCE = "rolefence.RoleFenceParticipant"

CODE = grpc.StatusCode


def _has_session_metadata(context) -> bool:
    if context is None:
        return False
    for key, _ in context.invocation_metadata() or ():
        if key.lower() == SESSION_DETAIL:
            return True
    return False


class MailService:
    """集中挂载 ClientPlayerMail / MailAdmin / RoleFenceParticipant 三个 gRPC 服务。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MailStoreCore,
        *,
        caller_verifier: CallerVerifier | None,
        producer_secrets: dict[str, bytes],
        coordinator_secret: bytes,
        notifier: MailNotifier | None = None,
        request_budget_sec: float = 3.5,
        approval_required_callers: tuple[str, ...] = ("admin",),
        allow_unapproved: bool = True,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._caller_verifier = caller_verifier
        self._producer_secrets = dict(producer_secrets)
        self._coordinator_secret = coordinator_secret
        self._notifier = notifier
        self._budget_sec = request_budget_sec
        self._approval_required = frozenset(("admin", *(str(c) for c in approval_required_callers if str(c))))
        self._allow_unapproved = bool(allow_unapproved)
        self._clock_ms = clock_ms

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_CLIENT: {
                "ListMails": self.list_mails,
                "ReadMail": self.read_mail,
                "ClaimMailAttachments": self.claim_mail_attachments,
                "DeleteMails": self.delete_mails,
                "ClearReadMails": self.clear_read_mails,
                "NotifyMailChanged": self.notify_mail_changed,
            },
            SERVICE_ADMIN: {
                "SendPersonalMail": self.send_personal_mail,
                "SendSystemMail": self.send_system_mail,
                "SendGuildMail": self.send_guild_mail,
                "ImportMailRecipients": self.import_mail_recipients,
                "SealMailCampaign": self.seal_mail_campaign,
            },
            SERVICE_ROLE_FENCE: {
                "PrepareRoleFreeze": self.prepare_role_freeze,
                "ApplyRoleFence": self.apply_role_fence,
                "PurgeRemovedRole": self.purge_removed_role,
            },
        }

    # -- 鉴权门禁（fail-closed） ----------------------------------------------------------------------

    async def _admit_client(self, context, path: str) -> RoleOriginal:
        if not _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "client mail RPC requires verified gate session")
        if self._caller_verifier is None:
            await context.abort(CODE.UNAVAILABLE, "gate caller verifier not configured")
        try:
            raw = self._caller_verifier.authenticate(context.invocation_metadata(), path)
            if raw is None:
                await context.abort(CODE.PERMISSION_DENIED, "missing gate session identity")
            details = parse_details(self.protocol, raw)
        except AuthError as exc:
            await context.abort(CODE.UNAUTHENTICATED, f"invalid gate session signature: {exc}")
        if not details.player_id or not details.owner_revision or not details.authenticated_account:
            await context.abort(CODE.PERMISSION_DENIED, "incomplete online role identity in gate session")
        return RoleOriginal(
            player_id=int(details.player_id),
            account=str(details.authenticated_account),
            owner_revision=int(details.owner_revision),
        )

    async def _admit_producer(self, context, request: Message, path: str) -> str:
        if _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "client session cannot invoke MailAdmin")
        caller = str(request.auth.caller)
        secret = self._producer_secrets.get(caller, b"")
        if not caller or len(secret) < 32:
            await context.abort(CODE.UNAUTHENTICATED, "mail producer auth missing or unknown caller")
        if not verify_service_call(path, request, caller, self._clock_ms(), secret):
            await context.abort(CODE.UNAUTHENTICATED, "invalid mail producer signature")
        return caller

    async def _admit_coordinator(self, context, request: Message, path: str) -> None:
        if _has_session_metadata(context):
            await context.abort(CODE.PERMISSION_DENIED, "client session cannot invoke RoleFenceParticipant")
        if len(self._coordinator_secret) < 32:
            await context.abort(CODE.UNAVAILABLE, "role fence coordinator secret not configured")
        if not verify_service_call(path, request, "coordinator", self._clock_ms(), self._coordinator_secret):
            await context.abort(CODE.PERMISSION_DENIED, "invalid role fence coordinator signature")

    async def _with_budget(self, coro):
        if self._budget_sec > 0:
            async with asyncio.timeout(self._budget_sec):
                return await coro
        return await coro

    def _tip(self, error: BaseException | None):
        code = rejection_tip_id(error)
        return self.protocol.message("TipInfoMessage", id=code) if code != 0 else None

    # -- ClientPlayerMail ----------------------------------------------------------------------------

    async def notify_mail_changed(self, request: Message, context, path: str) -> Message:
        await context.abort(CODE.PERMISSION_DENIED, "NotifyMailChanged is S2C push only")

    async def list_mails(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        try:
            return await self._with_budget(self.store.list_mails(original, request))
        except Exception as exc:
            resp = self.protocol.message("mailpb.ListMailsResponse")
            resp.error_message.id = rejection_tip_id(exc)
            return resp

    async def read_mail(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        try:
            view = await self._with_budget(self.store.read_mail(original, int(request.mail_id)))
            return self.protocol.message("mailpb.ReadMailResponse", mail=view)
        except Exception as exc:
            resp = self.protocol.message("mailpb.ReadMailResponse")
            resp.error_message.id = rejection_tip_id(exc)
            return resp

    async def claim_mail_attachments(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        if not request.mail_ids or len(request.mail_ids) > MAX_CLAIM_BATCH:
            resp = self.protocol.message("mailpb.ClaimMailAttachmentsResponse")
            resp.error_message.id = ERR_INVALID_PARAMETER
            return resp
        resp = self.protocol.message("mailpb.ClaimMailAttachmentsResponse")
        seen: set[int] = set()
        for raw_id in request.mail_ids:
            mail_id = int(raw_id)
            if mail_id in seen:
                continue
            seen.add(mail_id)
            try:
                action = await self._with_budget(self.store.claim_mail(original, mail_id))
            except Exception as exc:
                action = self.protocol.message("mailpb.MailActionResult", mail_id=mail_id)
                action.error_message.id = rejection_tip_id(exc)
            resp.results.add().CopyFrom(action)
        return resp

    async def delete_mails(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        try:
            deleted, protected = await self._with_budget(
                self.store.delete_mails(original, list(request.mail_ids), clear_read=False)
            )
            return self.protocol.message(
                "mailpb.DeleteMailsResponse",
                deleted_mail_ids=deleted,
                protected_mail_ids=protected,
            )
        except Exception as exc:
            resp = self.protocol.message("mailpb.DeleteMailsResponse")
            resp.error_message.id = rejection_tip_id(exc)
            return resp

    async def clear_read_mails(self, request: Message, context, path: str) -> Message:
        original = await self._admit_client(context, path)
        try:
            deleted, protected = await self._with_budget(
                self.store.delete_mails(original, (), clear_read=True)
            )
            return self.protocol.message(
                "mailpb.ClearReadMailsResponse",
                deleted_mail_ids=deleted,
                protected_mail_ids=protected,
            )
        except Exception as exc:
            resp = self.protocol.message("mailpb.ClearReadMailsResponse")
            resp.error_message.id = rejection_tip_id(exc)
            return resp

    # -- MailAdmin -----------------------------------------------------------------------------------

    async def _admit_approval(self, context, caller: str, path: str, request: Message) -> None:
        """核对 MailAdmin 发件的双人复核批准记录（先验调用证明，再验双人复核）。"""
        approval = extract_approval(request)
        if approval is None and caller not in self._approval_required:
            return
        try:
            approved = validate_admin_approval(
                approval,
                operator="",
                request_id=int(request.request_id),
                allow_unapproved=self._allow_unapproved,
            )
        except DualControlError as exc:
            logger.error(
                "[mail] Rejecting %s from caller %s request_id=%d: %s",
                path,
                caller,
                int(request.request_id),
                exc,
            )
            await context.abort(CODE.PERMISSION_DENIED, str(exc))
        if not approved:
            logger.error(
                "[mail] %s accepted without a dual-control approval record caller=%s request_id=%d (AdminAllowUnapproved is on)",
                path,
                caller,
                int(request.request_id),
            )

    async def _send(self, request: Message, context, path: str, kind: int) -> Message:
        caller = await self._admit_producer(context, request, path)
        await self._admit_approval(context, caller, path, request)
        try:
            resp = await self._with_budget(self.store.send(caller, kind, request))
        except Exception as exc:
            out = self.protocol.message("mailpb.SendMailResponse")
            out.error_message.id = rejection_tip_id(exc)
            return out
        if kind == MAIL_KIND_PERSONAL and not resp.duplicated and self._notifier is not None:
            try:
                await self._notifier.personal(int(request.recipient_player_id), int(resp.mail_id))
            except Exception:
                logger.error(
                    "mail post-commit personal notification failed player=%d mail_id=%d",
                    request.recipient_player_id,
                    resp.mail_id,
                    exc_info=True,
                )
        return resp

    async def send_personal_mail(self, request: Message, context, path: str) -> Message:
        return await self._send(request, context, path, MAIL_KIND_PERSONAL)

    async def send_system_mail(self, request: Message, context, path: str) -> Message:
        return await self._send(request, context, path, MAIL_KIND_SYSTEM)

    async def send_guild_mail(self, request: Message, context, path: str) -> Message:
        return await self._send(request, context, path, MAIL_KIND_GUILD)

    async def import_mail_recipients(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_producer(context, request, path)
        try:
            return await self._with_budget(self.store.import_recipients(caller, request))
        except Exception as exc:
            resp = self.protocol.message("mailpb.ImportMailRecipientsResponse")
            resp.error_message.id = rejection_tip_id(exc)
            return resp

    async def seal_mail_campaign(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_producer(context, request, path)
        try:
            return await self._with_budget(self.store.seal_campaign(caller, request))
        except Exception as exc:
            resp = self.protocol.message("mailpb.SendMailResponse")
            resp.error_message.id = rejection_tip_id(exc)
            return resp

    # -- RoleFenceParticipant ------------------------------------------------------------------------

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
                "mail PrepareRoleFreeze failed player=%d version=%d dry_run=%s",
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
                "mail ApplyRoleFence failed player=%d version=%d",
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
                "mail PurgeRemovedRole failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)
