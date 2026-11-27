"""chatpb.ClientPlayerChat gRPC 服务处理器与入口验签门禁（与 Go chatserver/chat_logic 对齐）。"""

from __future__ import annotations

import asyncio
import base64
import hmac
import logging

import grpc
from google.protobuf.message import Message

from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .config import ADMIN_TOKEN_ENV, ADMIN_TOKEN_METADATA_KEY, MIN_ADMIN_TOKEN_BYTES
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
    render_system_world_notice,
    valid_account,
)
from .store import (
    ChatStoreCore,
    ContentReviewer,
    PlatformRecheckTask,
    RECHECK_GAVE_UP,
    RECHECK_PASSED,
    RECHECK_RETRACTED,
    WorldPusher,
)

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
        admin_token: str | None = None,
    ) -> None:
        if admin_token and len(admin_token.encode("utf-8")) < MIN_ADMIN_TOKEN_BYTES:
            raise ValueError(f"{ADMIN_TOKEN_ENV} is shorter than {MIN_ADMIN_TOKEN_BYTES} bytes")
        self._protocol = protocol
        self._store = store
        self._verifier = verifier
        self._world_pusher = world_pusher
        self._platform_reviewer = platform_reviewer if platform_reviewer is not None else store.reviewer
        self._admin_token = admin_token
        self._bg_tasks: set[asyncio.Task] = set()

    def services(self) -> dict[str, dict]:
        """暴露 5 个业务 RPC（含 545 系统播报）；NotifyWorldChat / NotifyWorldChatRetracted 保持未实现。"""
        return {
            CHAT_SERVICE: {
                "SendChat": self.send_chat,
                "PullChatHistory": self.pull_chat_history,
                "ReportChat": self.report_chat,
                "RetractWorldChat": self.retract_world_chat,
                "PublishSystemWorldNotice": self.publish_system_world_notice,
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

    async def _authorize_ops(self, context, path: str) -> None:
        """核对运维口令（常数时间比对，口令值绝不进日志；显式空串表示停用运维撤回面）。"""
        if self._admin_token is None:
            return
        if not self._admin_token:
            logger.error("[chat] %s refused: %s is not set, the ops surface is disabled", path, ADMIN_TOKEN_ENV)
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"{path} is disabled because {ADMIN_TOKEN_ENV} is not set",
            )
        got = ""
        for key, value in context.invocation_metadata() or ():
            if str(key).lower() == ADMIN_TOKEN_METADATA_KEY:
                got = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
                break
        if not got or not hmac.compare_digest(got.encode("utf-8"), self._admin_token.encode("utf-8")):
            logger.error("[chat] %s refused: bad or missing %s (present=%s)", path, ADMIN_TOKEN_METADATA_KEY, bool(got))
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"{path} requires a valid {ADMIN_TOKEN_METADATA_KEY}",
            )

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

    async def _retract_platform_rejected(self, sender_id: int, send_time_ms: int) -> int:
        """撤回平台判违规的世界消息并广播撤回通知；返回结局码（0 表示需下一轮重试）。"""
        try:
            retracted, tip = await self._store.retract_world(sender_id, send_time_ms, _PLATFORM_REJECT_REASON)
        except Exception as exc:
            logger.error(
                "[chat] world platform reject not retracted sender=%d send_time_ms=%d err=%s",
                sender_id,
                send_time_ms,
                exc,
            )
            return 0
        if tip != 0:
            logger.error(
                "[chat] world platform reject could not be retracted sender=%d send_time_ms=%d tip=%d",
                sender_id,
                send_time_ms,
                tip,
            )
            return RECHECK_GAVE_UP
        if retracted:
            notify = self._protocol.message(
                "chatpb.ChatRetractedNotify",
                channel=CHANNEL_WORLD,
                sender_player_id=sender_id,
                send_time_ms=send_time_ms,
                reason=_PLATFORM_REJECT_REASON,
            )
            await self._publish_world_retract(notify)
        return RECHECK_RETRACTED

    async def _decide_platform_recheck(self, task: PlatformRecheckTask) -> int:
        if task.exhausted or task.missing:
            logger.error(
                "[chat] world platform recheck gave_up sender=%d send_time_ms=%d attempts=%d missing=%s",
                task.sender_player_id,
                task.accepted_ms,
                task.attempt,
                task.missing,
            )
            return RECHECK_GAVE_UP
        if task.already_retracted:
            return RECHECK_RETRACTED
        reviewer = self._platform_reviewer
        if reviewer is None:
            return 0
        try:
            async with asyncio.timeout(_ASYNC_RECHECK_TIMEOUT_SEC):
                res = await reviewer.check_platform_text(
                    TEXT_SCENE_CHAT_WORLD, task.sender_player_id, task.content
                )
        except Exception as exc:
            logger.error(
                "[chat] world platform recheck call failed sender=%d send_time_ms=%d err=%s",
                task.sender_player_id,
                task.accepted_ms,
                exc,
            )
            return 0
        if res.verdict == REVIEW_REJECT:
            return await self._retract_platform_rejected(task.sender_player_id, task.accepted_ms)
        if res.verdict == REVIEW_UNAVAILABLE:
            logger.error(
                "[chat] world platform recheck unavailable sender=%d send_time_ms=%d attempt=%d",
                task.sender_player_id,
                task.accepted_ms,
                task.attempt + 1,
            )
            return 0
        return RECHECK_PASSED

    async def run_platform_recheck_once(self, limit: int = 16) -> list[tuple[int, int, int]]:
        """认领并执行一批持久化的世界频道平台补审待办（对应 Go ``svc/platform_recheck.go``）。"""
        tasks = await self._store.claim_platform_rechecks(limit)
        results: list[tuple[int, int, int]] = []
        for task in tasks:
            outcome = await self._decide_platform_recheck(task)
            if outcome != 0:
                await self._store.finish_platform_recheck(task.sender_player_id, task.accepted_ms, outcome)
            results.append((task.sender_player_id, task.accepted_ms, outcome))
        return results

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
                    if res.verdict == REVIEW_UNAVAILABLE:
                        logger.error(
                            "async platform moderation unavailable sender=%d send_time_ms=%d",
                            sender_id,
                            send_time_ms,
                        )
                        return
                    if res.verdict != REVIEW_REJECT:
                        await self._store.finish_platform_recheck(sender_id, send_time_ms, RECHECK_PASSED)
                        return
                    outcome = await self._retract_platform_rejected(sender_id, send_time_ms)
                    if outcome != 0:
                        await self._store.finish_platform_recheck(sender_id, send_time_ms, outcome)
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
        """内部撤回世界频道消息并向全服在线玩家广播撤回通知（拒绝客户端会话调用并核对运维口令）。"""
        await self._reject_client_session(context, path)
        await self._authorize_ops(context, path)

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

    async def publish_system_world_notice(self, request, context, path: str):
        """545 内部白名单模板全服系统播报（CLIENT_RPC_SERVER_ONLY，拒绝客户端会话头）。"""
        await self._reject_client_session(context, path)

        def fail(tip_code: int):
            return self._protocol.message("chatpb.PublishSystemWorldNoticeResponse", error_message=self._tip(tip_code))

        _, ok = render_system_world_notice(request)
        if not ok:
            return fail(ERR_INVALID_PARAMETER)

        cloned = type(request)()
        cloned.CopyFrom(request)
        try:
            result = await self._store.publish_system_world_notice(cloned)
        except InvalidChatError:
            return fail(ERR_INVALID_PARAMETER)
        except StoreUnavailableError as exc:
            logger.error("chat system world notice SQL write failed error=%s", exc)
            return fail(ERR_STORAGE)
        except Exception:
            logger.exception("chat system world notice failed unexpectedly")
            return fail(ERR_STORAGE)

        if not result.duplicate and result.message is not None:
            await self._publish_world(result.message)
        return self._protocol.message("chatpb.PublishSystemWorldNoticeResponse", deduplicated=result.duplicate)
