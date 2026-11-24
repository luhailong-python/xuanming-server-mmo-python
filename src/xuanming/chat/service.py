"""chatpb.ClientPlayerChat gRPC 服务处理器与入口验签门禁（与 Go chatserver/chat_logic 对齐）。"""

from __future__ import annotations

import asyncio
import base64
import logging

import grpc
from google.protobuf.message import Message

from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_MESSAGE_TOO_LONG,
    ERR_STORAGE,
    InvalidChatError,
    OriginalRoleError,
    RequestConflictError,
    StoreUnavailableError,
)
from .model import (
    ActorIdentity,
    CHANNEL_WORLD,
    MAX_REPORT_DETAIL_BYTES,
    MAX_REPORT_REASON,
    MAX_REQUEST_ID_BYTES,
    MAX_RETRACT_REASON_BYTES,
    REVIEW_REJECT,
    REVIEW_UNAVAILABLE,
    TEXT_SCENE_CHAT_WORLD,
    channel_accepted,
    clamp_history_limit,
    valid_account,
)
from .store import ChatStoreCore, ContentReviewer, WorldPusher

logger = logging.getLogger(__name__)

CHAT_SERVICE = "chatpb.ClientPlayerChat"
_ASYNC_RECHECK_TIMEOUT_SEC = 4.0
_PLATFORM_REJECT_REASON = "platform_async_reject"


class ChatService:
    """chatpb.ClientPlayerChat gRPC 服务实现（不含 Push 方法，Push 保持 UNIMPLEMENTED）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: ChatStoreCore,
        verifier: CallerVerifier,
        *,
        world_pusher: WorldPusher | None = None,
        platform_reviewer: ContentReviewer | None = None,
    ) -> None:
        self._protocol = protocol
        self._store = store
        self._verifier = verifier
        self._world_pusher = world_pusher
        self._platform_reviewer = platform_reviewer if platform_reviewer is not None else store.reviewer
        self._bg_tasks: set[asyncio.Task] = set()

    def services(self) -> dict[str, dict]:
        """仅暴露 4 个业务 RPC；NotifyWorldChat / NotifyWorldChatRetracted 保持未实现。"""
        return {
            CHAT_SERVICE: {
                "SendChat": self.send_chat,
                "PullChatHistory": self.pull_chat_history,
                "ReportChat": self.report_chat,
                "RetractWorldChat": self.retract_world_chat,
            }
        }

    async def close(self) -> None:
        """等待或取消在途异步复检任务，供节点停机与单测清理。"""
        tasks = list(self._bg_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._bg_tasks.clear()

    def _tip(self, code: int):
        return self._protocol.message("TipInfoMessage", id=code)

    async def _authenticate_client(self, context, path: str) -> ActorIdentity:
        """校验 Gate Ed25519 签名与完整在线角色身份（fail-closed）。"""
        metadata = context.invocation_metadata()
        try:
            raw = self._verifier.authenticate(metadata, path)
        except AuthError as exc:
            logger.error("chat gate auth failed method=%s reason=%s", path, exc)
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, f"gate identity verification failed ({exc})")
        if raw is None:
            logger.error("chat rejected request without gate session method=%s", path)
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"{path} accepts only client requests forwarded by gate",
            )
        try:
            details = parse_details(self._protocol, raw)
        except AuthError as exc:
            logger.error("chat session payload invalid method=%s reason=%s", path, exc)
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, f"invalid session payload ({exc})")

        if (
            int(details.player_id) == 0
            or int(details.owner_revision) == 0
            or not valid_account(details.authenticated_account)
        ):
            logger.warning("chat rejected incomplete online identity method=%s", path)
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "online request has no original role identity")

        await context.send_initial_metadata(((SESSION_DETAIL, base64.b64encode(raw)),))
        return ActorIdentity(
            player_id=int(details.player_id),
            account=details.authenticated_account,
            owner_revision=int(details.owner_revision),
        )

    async def _reject_client_session(self, context, path: str) -> None:
        """服务端内部方法禁止携带客户端会话头 x-session-detail-bin。"""
        for key, _ in context.invocation_metadata() or ():
            if key == SESSION_DETAIL:
                logger.error("chat rejected client session on server-only method=%s", path)
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, f"{path} is not callable by clients")

    async def _publish_world(self, message: Message | None) -> None:
        if self._world_pusher is None or message is None or int(message.channel) != CHANNEL_WORLD:
            return
        try:
            await self._world_pusher.push_world(message)
        except Exception as exc:
            logger.error(
                "world channel broadcast push failed; message remains committed sender=%d error=%s",
                int(message.sender_player_id),
                exc,
            )

    async def _publish_world_retract(self, notify: Message | None) -> None:
        if (
            self._world_pusher is None
            or notify is None
            or int(notify.sender_player_id) == 0
            or int(notify.send_time_ms) <= 0
        ):
            return
        try:
            await self._world_pusher.push_world_retract(notify)
        except Exception as exc:
            logger.error(
                "world channel retract push failed sender=%d send_time_ms=%d error=%s",
                int(notify.sender_player_id),
                int(notify.send_time_ms),
                exc,
            )

    def _schedule_async_world_platform_recheck(self, message: Message, account: str, raw_content: str) -> None:
        reviewer = self._platform_reviewer
        if reviewer is None or not raw_content:
            return
        sender_id = int(message.sender_player_id)
        send_time_ms = int(message.send_time_ms)

        async def _recheck() -> None:
            try:
                async with asyncio.timeout(_ASYNC_RECHECK_TIMEOUT_SEC):
                    res = await reviewer.check_platform_text(
                        TEXT_SCENE_CHAT_WORLD, sender_id, raw_content, account=account
                    )
                    if res.verdict != REVIEW_REJECT:
                        if res.verdict == REVIEW_UNAVAILABLE:
                            logger.error(
                                "async platform moderation unavailable sender=%d send_time_ms=%d",
                                sender_id,
                                send_time_ms,
                            )
                        return
                    retracted, _ = await self._store.retract_world(sender_id, send_time_ms, _PLATFORM_REJECT_REASON)
                    if retracted:
                        notify = self._protocol.message(
                            "chatpb.ChatRetractedNotify",
                            channel=CHANNEL_WORLD,
                            sender_player_id=sender_id,
                            send_time_ms=send_time_ms,
                            reason=_PLATFORM_REJECT_REASON,
                        )
                        await self._publish_world_retract(notify)
            except Exception as exc:
                logger.error(
                    "async world chat platform recheck failed sender=%d send_time_ms=%d error=%s",
                    sender_id,
                    send_time_ms,
                    exc,
                )

        task = asyncio.create_task(_recheck(), name="chat-world-platform-recheck")
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def send_chat(self, request, context, path: str):
        """发送世界或私聊消息，所有业务与存储错误均通过 in-band TipInfoMessage 返回。"""
        identity = await self._authenticate_client(context, path)

        def fail(tip_code: int):
            return self._protocol.message("chatpb.SendChatResponse", error_message=self._tip(tip_code))

        if request is None or not request.HasField("message"):
            return fail(ERR_INVALID_PARAMETER)
        msg = request.message
        channel_tip = channel_accepted(int(msg.channel), identity.player_id, int(msg.target_player_id))
        if channel_tip != 0:
            return fail(channel_tip)
        max_bytes = self._store.rules.max_content_bytes
        if len(msg.content.encode("utf-8")) > max_bytes or not msg.content.strip():
            return fail(ERR_MESSAGE_TOO_LONG)
        if len(request.request_id.encode("utf-8")) > MAX_REQUEST_ID_BYTES or request.ByteSize() > max_bytes + 8192:
            return fail(ERR_INVALID_PARAMETER)

        cloned = type(request)()
        cloned.CopyFrom(request)
        try:
            result = await self._store.send(identity, cloned)
        except (OriginalRoleError, InvalidChatError, RequestConflictError):
            return fail(ERR_INVALID_PARAMETER)
        except StoreUnavailableError as exc:
            logger.error("chat send SQL unconfirmed; keep request_id for replay error=%s", exc)
            return fail(ERR_STORAGE)
        except Exception:
            logger.exception("chat send failed unexpectedly")
            return fail(ERR_STORAGE)

        if result.tip != 0:
            return fail(result.tip)

        await self._publish_world(result.message)
        if result.pending_platform_review and result.message is not None:
            self._schedule_async_world_platform_recheck(
                result.message, identity.account, result.platform_review_content
            )
        return self._protocol.message("chatpb.SendChatResponse")

    async def pull_chat_history(self, request, context, path: str):
        """拉取世界或私聊近期可见历史（快照式、新在前）。"""
        identity = await self._authenticate_client(context, path)

        def fail(tip_code: int):
            return self._protocol.message("chatpb.PullChatHistoryResponse", error_message=self._tip(tip_code))

        if request is None:
            return fail(ERR_INVALID_PARAMETER)
        channel_tip = channel_accepted(int(request.channel), identity.player_id, int(request.peer_player_id))
        if channel_tip != 0:
            return fail(channel_tip)

        rules = self._store.rules
        limit = clamp_history_limit(int(request.limit), rules.history_default_limit, rules.history_max_limit)
        try:
            messages = await self._store.pull(identity, request, limit)
        except (OriginalRoleError, InvalidChatError):
            return fail(ERR_INVALID_PARAMETER)
        except StoreUnavailableError as exc:
            logger.error("chat pull SQL read unconfirmed error=%s", exc)
            return fail(ERR_STORAGE)
        except Exception:
            logger.exception("chat pull failed unexpectedly")
            return fail(ERR_STORAGE)

        return self._protocol.message("chatpb.PullChatHistoryResponse", messages=messages)

    async def report_chat(self, request, context, path: str):
        """玩家举报已提交聊天消息并记录至举报审计表。"""
        identity = await self._authenticate_client(context, path)

        def fail(tip_code: int):
            return self._protocol.message("chatpb.ReportChatResponse", error_message=self._tip(tip_code))

        if (
            request is None
            or int(request.target_sender_player_id) == 0
            or int(request.send_time_ms) <= 0
            or not 1 <= int(request.reason) <= MAX_REPORT_REASON
            or len(request.detail.encode("utf-8")) > MAX_REPORT_DETAIL_BYTES
        ):
            return fail(ERR_INVALID_PARAMETER)
        if channel_accepted(int(request.channel), identity.player_id, int(request.target_sender_player_id)) != 0:
            return fail(ERR_INVALID_PARAMETER)

        try:
            tip = await self._store.report_message(identity, request)
        except (OriginalRoleError, InvalidChatError):
            return fail(ERR_INVALID_PARAMETER)
        except StoreUnavailableError as exc:
            logger.error("chat report SQL write failed error=%s", exc)
            return fail(ERR_STORAGE)
        except Exception:
            logger.exception("chat report failed unexpectedly")
            return fail(ERR_STORAGE)

        if tip != 0:
            return fail(tip)
        return self._protocol.message("chatpb.ReportChatResponse")

    async def retract_world_chat(self, request, context, path: str):
        """内部撤回世界频道消息并向全服在线玩家广播撤回通知（拒绝客户端会话调用）。"""
        await self._reject_client_session(context, path)

        def fail(tip_code: int):
            return self._protocol.message("chatpb.RetractWorldChatResponse", error_message=self._tip(tip_code))

        if (
            request is None
            or int(request.sender_player_id) == 0
            or int(request.send_time_ms) <= 0
            or len(request.reason.encode("utf-8")) > MAX_RETRACT_REASON_BYTES
        ):
            return fail(ERR_INVALID_PARAMETER)

        reason = request.reason.strip()
        try:
            retracted, tip = await self._store.retract_world(
                int(request.sender_player_id), int(request.send_time_ms), reason
            )
        except InvalidChatError:
            return fail(ERR_INVALID_PARAMETER)
        except StoreUnavailableError as exc:
            logger.error("chat retract SQL write failed error=%s", exc)
            return fail(ERR_STORAGE)
        except Exception:
            logger.exception("chat retract failed unexpectedly")
            return fail(ERR_STORAGE)

        if tip != 0:
            return fail(tip)
        if retracted:
            notify = self._protocol.message(
                "chatpb.ChatRetractedNotify",
                channel=CHANNEL_WORLD,
                sender_player_id=int(request.sender_player_id),
                send_time_ms=int(request.send_time_ms),
                reason=reason,
            )
            await self._publish_world_retract(notify)
        return self._protocol.message("chatpb.RetractWorldChatResponse", retracted=retracted)
