"""friendpb.ClientPlayerFriend 与 rolefence.RoleFenceParticipant gRPC 服务适配层。"""

from __future__ import annotations

import asyncio
import base64
import logging
import time
from collections.abc import Callable

import grpc
from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import verify_service_call
from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .errors import (
    AcceptorFriendsFullError,
    AlreadyFriendsError,
    BlockListFullError,
    BlockedError,
    ERR_ALREADY_FRIENDS,
    ERR_BLOCKED,
    ERR_BLOCK_LIST_FULL,
    ERR_CANNOT_ADD_SELF,
    ERR_FRIEND_LIST_FULL,
    ERR_INVALID_PARAMETER,
    ERR_NO_PENDING_REQUEST,
    ERR_RATE_LIMITED,
    ERR_REQUEST_ALREADY_SENT,
    ERR_ROLE_NOT_VERIFIED,
    ERR_STORAGE,
    ERR_TARGET_FRIEND_LIST_FULL,
    ERR_TARGET_INBOX_FULL,
    ERR_TOO_MANY_PENDING,
    InvalidDirectoryInputError,
    InvalidRoleFenceStateError,
    OriginalRoleError,
    RequestAlreadySentError,
    RequestNotFoundError,
    RoleFenceUnavailableError,
    SenderFriendsFullError,
    StoreUnavailableError,
    TargetInboxFullError,
    TooManyPendingError,
)
from .model import (
    ActorIdentity,
    AddFriendLimits,
    DIRECTORY_DEFAULT_PAGE,
    DIRECTORY_MAX_BATCH,
    DIRECTORY_MAX_PAGE,
    DIRECTORY_QUOTA_PER_MINUTE,
    DIRECTORY_SCAN_COUNT,
    DIRECTORY_SCAN_ROUNDS,
    DirectoryCursor,
    FRIEND_EVENT_REASON_REQUEST_ACCEPTED,
    FRIEND_EVENT_REASON_REQUEST_RECEIVED,
    SESSION_STATE_ONLINE,
    SYNC_DOMAIN_FRIEND_BLOCK,
    SYNC_DOMAIN_FRIEND_LIST,
    SYNC_DOMAIN_FRIEND_REQUEST,
    encode_directory_cursor,
    parse_session_key,
    recommend_limit,
    valid_account,
    validate_directory_input,
)
from .notify import FriendPusher, NullFriendPusher
from .redis_client import QuotaLimiter, SessionStore, SyncDigestBumper
from .store import FriendStoreCore

logger = logging.getLogger(__name__)

FRIEND_SERVICE = "friendpb.ClientPlayerFriend"
ROLE_FENCE_SERVICE = "rolefence.RoleFenceParticipant"
_COORDINATOR_CALLER = "coordinator"
CODE = grpc.StatusCode


def _has_session_metadata(context) -> bool:
    if context is None:
        return False
    for key, _ in context.invocation_metadata() or ():
        if key.lower() == SESSION_DETAIL:
            return True
    return False


class FriendService:
    """挂载 ClientPlayerFriend（10 C2S + 拒绝 NotifyFriendEvent）与 RoleFenceParticipant（3 RPC）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: FriendStoreCore,
        verifier: CallerVerifier,
        *,
        coordinator_secret: bytes,
        sessions: SessionStore | None = None,
        quota: QuotaLimiter | None = None,
        sync_digest: SyncDigestBumper | None = None,
        pusher: FriendPusher | None = None,
        request_budget_sec: float = 3.5,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._store = store
        self._verifier = verifier
        self._coordinator_secret = coordinator_secret
        self._sessions = sessions
        self._quota = quota
        self._sync_digest = sync_digest
        self._pusher = pusher if pusher is not None else NullFriendPusher()
        self._budget_sec = request_budget_sec
        self._clock_ms = clock_ms

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            FRIEND_SERVICE: {
                "AddFriend": self.add_friend,
                "AcceptFriend": self.accept_friend,
                "RejectFriend": self.reject_friend,
                "RemoveFriend": self.remove_friend,
                "GetFriendList": self.get_friend_list,
                "GetPendingRequests": self.get_pending_requests,
                "Block": self.block,
                "Unblock": self.unblock,
                "ListBlocks": self.list_blocks,
                "RecommendFriends": self.recommend_friends,
                "NotifyFriendEvent": self.notify_friend_event,
                "BatchCheckSocialRelations": self.batch_check_social_relations,
            },
            ROLE_FENCE_SERVICE: {
                "PrepareRoleFreeze": self.prepare_role_freeze,
                "ApplyRoleFence": self.apply_role_fence,
                "PurgeRemovedRole": self.purge_removed_role,
            },
        }

    # -- 公共工具与入口鉴权门禁 ----------------------------------------------------------------------

    def _tip(self, code: int, message: str = ""):
        if message:
            return self._protocol.message("TipInfoMessage", id=code, parameters=[message])
        return self._protocol.message("TipInfoMessage", id=code)

    async def _with_budget(self, coro):
        if self._budget_sec > 0:
            async with asyncio.timeout(self._budget_sec):
                return await coro
        return await coro

    async def _authenticate_client(self, context, path: str) -> ActorIdentity:
        """校验 Gate Ed25519 签名与完整在线原角色身份（fail-closed）。"""
        metadata = context.invocation_metadata()
        try:
            raw = self._verifier.authenticate(metadata, path)
        except AuthError as exc:
            logger.error("friend gate auth failed method=%s reason=%s", path, exc)
            await context.abort(CODE.UNAUTHENTICATED, f"gate identity verification failed ({exc})")
        if raw is None:
            logger.error("friend rejected request without gate session method=%s", path)
            await context.abort(
                CODE.PERMISSION_DENIED,
                f"{path} accepts only client requests forwarded by gate",
            )
        try:
            details = parse_details(self._protocol, raw)
        except AuthError as exc:
            logger.error("friend session payload invalid method=%s reason=%s", path, exc)
            await context.abort(CODE.UNAUTHENTICATED, f"invalid session payload ({exc})")

        if (
            int(details.player_id) == 0
            or int(details.owner_revision) == 0
            or not valid_account(details.authenticated_account)
        ):
            logger.warning("friend rejected incomplete online identity method=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, "online request has no original role identity")

        await context.send_initial_metadata(((SESSION_DETAIL, base64.b64encode(raw)),))
        return ActorIdentity(
            player_id=int(details.player_id),
            account=details.authenticated_account,
            owner_revision=int(details.owner_revision),
        )

    async def _reject_client_session(self, context, path: str) -> None:
        """非 C2S 方法（NotifyFriendEvent / RoleFenceParticipant）禁止携带客户端会话头。"""
        if _has_session_metadata(context):
            logger.error("friend rejected client session on non-c2s method=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, f"{path} is not callable by clients")

    async def _admit_coordinator(self, context, request: Message, path: str) -> None:
        await self._reject_client_session(context, path)
        if not self._coordinator_secret:
            logger.error("friend role fence coordinator secret not configured method=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, "role fence coordinator secret not configured")
        if not verify_service_call(path, request, _COORDINATOR_CALLER, self._clock_ms(), self._coordinator_secret):
            logger.error("friend role fence coordinator signature invalid method=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, "invalid role fence coordinator signature")

    def _storage_failure_tip(self, method: str, exc: Exception):
        if isinstance(exc, (OriginalRoleError, RoleFenceUnavailableError)):
            logger.info("friend %s role not verified reason=%s", method, exc)
            return self._tip(ERR_ROLE_NOT_VERIFIED, "role not verified, retry later")
        logger.error("friend %s dependency failure error=%s", method, exc)
        return self._tip(ERR_STORAGE, "storage unavailable")

    async def _bump_digest(self, player_ids: list[int], *domains: int) -> None:
        if self._sync_digest is None or not player_ids or not domains:
            return
        try:
            await self._sync_digest.bump_all(player_ids, *domains)
        except Exception as exc:
            logger.warning("friend sync digest bump failed players=%s domains=%s error=%s", player_ids, domains, exc)

    # -- ClientPlayerFriend (10 C2S + 1 S2C reject) -------------------------------------------------

    async def notify_friend_event(self, request: Message, context, path: str) -> Message:
        """S2C 推送方向方法：带客户端会话拒为 PERMISSION_DENIED，其余返回 UNIMPLEMENTED。"""
        await self._reject_client_session(context, path)
        await context.abort(CODE.UNIMPLEMENTED, "NotifyFriendEvent is a server-to-client push method")

    async def batch_check_social_relations(self, request: Message, context, path: str) -> Message:
        """546 内部只读批量关系检查（CLIENT_RPC_SERVER_ONLY，拒绝客户端会话头）。"""
        await self._reject_client_session(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.BatchCheckSocialRelationsResponse", error_message=tip)

        side_a = [int(pid) for pid in request.side_a_player_ids] if request is not None else []
        side_b = [int(pid) for pid in request.side_b_player_ids] if request is not None else []
        if not side_a or not side_b or len(side_a) > 5 or len(side_b) > 5:
            return fail(self._tip(ERR_INVALID_PARAMETER, "invalid side player list"))
        set_a: set[int] = set()
        for pid in side_a:
            if pid <= 0:
                return fail(self._tip(ERR_INVALID_PARAMETER, "zero player id"))
            set_a.add(pid)
        for pid in side_b:
            if pid <= 0:
                return fail(self._tip(ERR_INVALID_PARAMETER, "zero player id"))
            if pid in set_a:
                return fail(self._tip(ERR_INVALID_PARAMETER, "overlapping player id across sides"))

        try:
            has_any_block, all_are_friends = await self._with_budget(
                self._store.batch_check_social_relations(side_a, side_b)
            )
        except Exception as exc:
            return fail(self._storage_failure_tip("BatchCheckSocialRelations", exc))

        return self._protocol.message(
            "friendpb.BatchCheckSocialRelationsResponse",
            has_any_block=has_any_block,
            all_are_friends=all_are_friends,
        )

    async def add_friend(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.AddFriendResponse", error_message=tip)

        target = int(request.target_player_id)
        if target == 0:
            return fail(self._tip(ERR_INVALID_PARAMETER, "target_player_id required"))
        if target == actor.player_id:
            return fail(self._tip(ERR_CANNOT_ADD_SELF, "cannot add yourself"))

        rules = self._store.rules
        if self._quota is not None and not await self._quota.allow_friend_request(
            actor.player_id, rules.request_quota_per_minute
        ):
            return fail(self._tip(ERR_RATE_LIMITED, "too many friend requests, retry later"))

        limits = AddFriendLimits(
            max_friends=rules.max_friends,
            max_pending_requests=rules.max_pending_requests,
            max_incoming_requests=rules.max_incoming_requests,
        )
        try:
            change = await self._with_budget(self._store.create_request(actor, target, limits))
        except BlockedError:
            return fail(self._tip(ERR_BLOCKED, "cannot send friend request"))
        except AlreadyFriendsError:
            return fail(self._tip(ERR_ALREADY_FRIENDS, "already friends"))
        except RequestAlreadySentError:
            return fail(self._tip(ERR_REQUEST_ALREADY_SENT, "request already sent"))
        except TooManyPendingError:
            return fail(self._tip(ERR_TOO_MANY_PENDING, "too many pending friend requests"))
        except TargetInboxFullError:
            return fail(self._tip(ERR_TARGET_INBOX_FULL, "target inbox full"))
        # AddFriend 中调用者是 Sender、目标是 Acceptor
        except SenderFriendsFullError:
            return fail(self._tip(ERR_FRIEND_LIST_FULL, "your friend list full"))
        except AcceptorFriendsFullError:
            return fail(self._tip(ERR_TARGET_FRIEND_LIST_FULL, "target's friend list full"))
        except Exception as exc:
            return fail(self._storage_failure_tip("AddFriend", exc))

        if change.changed:
            await self._bump_digest([target], SYNC_DOMAIN_FRIEND_REQUEST)
            await self._pusher.push_friend_event(target, FRIEND_EVENT_REASON_REQUEST_RECEIVED, actor.player_id)
        logger.info("friend request sent from=%d to=%d request_id=%d changed=%s", actor.player_id, target, change.request_id, change.changed)
        return self._protocol.message("friendpb.AddFriendResponse", request_id=change.request_id)

    async def accept_friend(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.AcceptFriendResponse", error_message=tip)

        request_id = int(request.request_id)
        if request_id == 0:
            return fail(self._tip(ERR_INVALID_PARAMETER, "request_id required"))

        try:
            change = await self._with_budget(
                self._store.accept_request(actor, request_id, self._store.rules.max_friends)
            )
        except BlockedError:
            return fail(self._tip(ERR_BLOCKED, "cannot accept friend request"))
        except RequestNotFoundError:
            return fail(self._tip(ERR_NO_PENDING_REQUEST, "no pending friend request"))
        # AcceptFriend 中调用者是 Acceptor、原申请人是 Sender（与 AddFriend 映射正好相反）
        except SenderFriendsFullError:
            return fail(self._tip(ERR_TARGET_FRIEND_LIST_FULL, "sender's friend list full"))
        except AcceptorFriendsFullError:
            return fail(self._tip(ERR_FRIEND_LIST_FULL, "your friend list full"))
        except Exception as exc:
            return fail(self._storage_failure_tip("AcceptFriend", exc))

        if change.changed:
            from_id = change.from_player_id
            await self._bump_digest(
                [actor.player_id, from_id], SYNC_DOMAIN_FRIEND_LIST, SYNC_DOMAIN_FRIEND_REQUEST
            )
            await self._pusher.push_friend_event(from_id, FRIEND_EVENT_REASON_REQUEST_ACCEPTED, actor.player_id)
            logger.info("friend request accepted accepter=%d from=%d request_id=%d", actor.player_id, from_id, request_id)
        return self._protocol.message("friendpb.AcceptFriendResponse")

    async def reject_friend(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.RejectFriendResponse", error_message=tip)

        request_id = int(request.request_id)
        if request_id == 0:
            return fail(self._tip(ERR_INVALID_PARAMETER, "request_id required"))

        try:
            change = await self._with_budget(self._store.reject_request(actor, request_id))
        except RequestNotFoundError:
            return fail(self._tip(ERR_NO_PENDING_REQUEST, "no pending friend request"))
        except Exception as exc:
            return fail(self._storage_failure_tip("RejectFriend", exc))

        if change.changed:
            await self._bump_digest([actor.player_id], SYNC_DOMAIN_FRIEND_REQUEST)
            logger.info("friend request rejected accepter=%d from=%d request_id=%d", actor.player_id, change.from_player_id, request_id)
        return self._protocol.message("friendpb.RejectFriendResponse")

    async def remove_friend(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.RemoveFriendResponse", error_message=tip)

        target = int(request.target_player_id)
        if target == 0:
            return fail(self._tip(ERR_INVALID_PARAMETER, "target_player_id required"))

        try:
            removed = await self._with_budget(self._store.remove_friend(actor, target))
        except Exception as exc:
            return fail(self._storage_failure_tip("RemoveFriend", exc))

        if removed:
            await self._bump_digest([actor.player_id, target], SYNC_DOMAIN_FRIEND_LIST)
        logger.info("friend removed player=%d target=%d removed=%s", actor.player_id, target, removed)
        return self._protocol.message("friendpb.RemoveFriendResponse")

    async def get_friend_list(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.GetFriendListResponse", error_message=tip)

        try:
            friends = await self._with_budget(self._store.get_friend_list(actor))
        except Exception as exc:
            return fail(self._storage_failure_tip("GetFriendList", exc))

        friend_ids = [f.friend_player_id for f in friends]
        statuses = {}
        if friend_ids:
            # 在线状态控制组队邀请，PlayerRedis 未配置或读取/解码异常必须 fail-closed 回 1003。
            if self._sessions is None:
                return fail(self._storage_failure_tip("GetFriendList", StoreUnavailableError("session reader not configured")))
            try:
                statuses = await self._with_budget(self._sessions.batch_online_status(friend_ids))
            except Exception as exc:
                return fail(self._storage_failure_tip("GetFriendList", exc))

        pb_friends = []
        for f in friends:
            st = statuses.get(f.friend_player_id)
            pb_friends.append(
                self._protocol.message(
                    "friendpb.FriendEntry",
                    friend_player_id=f.friend_player_id,
                    since_ms=f.since_ms,
                    is_online=bool(st.online) if st is not None else False,
                    last_active_ms=st.last_active_ms if st is not None else 0,
                )
            )
        # 展示资料补全为尽力而为：失败记录日志但绝不隐藏真实好友关系与在线状态。
        await self._fill_friend_profiles(pb_friends)
        return self._protocol.message("friendpb.GetFriendListResponse", friends=pb_friends)

    async def _fill_friend_profiles(self, pb_friends: list[Message]) -> None:
        if not pb_friends:
            return
        ds = self._store.data_service
        try:
            for start in range(0, len(pb_friends), DIRECTORY_SCAN_COUNT):
                batch = pb_friends[start : start + DIRECTORY_SCAN_COUNT]
                ids = [int(entry.friend_player_id) for entry in batch]
                briefs = await ds.batch_get_player_display(ids)
                missing_names: list[int] = []
                for entry in batch:
                    pid = int(entry.friend_player_id)
                    brief = briefs.get(pid)
                    if brief is not None:
                        entry.name = brief.name
                        entry.level = brief.level
                        entry.class_id = brief.class_id
                        entry.gender = brief.gender
                        entry.appearance_id = brief.appearance_id
                    if not entry.name:
                        missing_names.append(pid)
                if missing_names:
                    names = await ds.batch_get_player_name(missing_names)
                    for entry in batch:
                        if not entry.name:
                            entry.name = names.get(int(entry.friend_player_id), "")
                zones = await ds.batch_get_player_home_zone(ids)
                for entry in batch:
                    entry.zone_id = zones.get(int(entry.friend_player_id), 0)
        except Exception as exc:
            logger.error("friend display profile enrichment failed error=%s", exc)

    async def get_pending_requests(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.GetPendingRequestsResponse", error_message=tip)

        try:
            requests = await self._with_budget(self._store.get_pending_requests(actor))
        except Exception as exc:
            return fail(self._storage_failure_tip("GetPendingRequests", exc))

        pb_requests = [
            self._protocol.message(
                "friendpb.FriendRequest",
                request_id=r.request_id,
                from_player_id=r.from_player_id,
                to_player_id=r.to_player_id,
                request_time_ms=r.request_time_ms,
                status=r.status,
            )
            for r in requests
        ]
        return self._protocol.message("friendpb.GetPendingRequestsResponse", requests=pb_requests)

    async def block(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.BlockResponse", error_message=tip)

        target = int(request.target_player_id)
        if target == 0 or target == actor.player_id:
            return fail(self._tip(ERR_INVALID_PARAMETER, "target_player_id invalid"))

        try:
            effect = await self._with_budget(self._store.block(actor, target, self._store.rules.max_blocks))
        except BlockListFullError:
            return fail(self._tip(ERR_BLOCK_LIST_FULL, "block list full"))
        except Exception as exc:
            return fail(self._storage_failure_tip("Block", exc))

        await self._bump_digest(
            [actor.player_id],
            SYNC_DOMAIN_FRIEND_BLOCK,
            SYNC_DOMAIN_FRIEND_LIST,
            SYNC_DOMAIN_FRIEND_REQUEST,
        )
        peer_domains: list[int] = []
        if effect.edge_removed:
            peer_domains.append(SYNC_DOMAIN_FRIEND_LIST)
        if effect.outbound_pending_rejected:
            peer_domains.append(SYNC_DOMAIN_FRIEND_REQUEST)
        if peer_domains:
            await self._bump_digest([target], *peer_domains)
        logger.info("friend blocked player=%d target=%d edge_removed=%s outbound_rejected=%s", actor.player_id, target, effect.edge_removed, effect.outbound_pending_rejected)
        return self._protocol.message("friendpb.BlockResponse")

    async def unblock(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.UnblockResponse", error_message=tip)

        target = int(request.target_player_id)
        if target == 0 or target == actor.player_id:
            return fail(self._tip(ERR_INVALID_PARAMETER, "target_player_id invalid"))

        try:
            await self._with_budget(self._store.unblock(actor, target))
        except Exception as exc:
            return fail(self._storage_failure_tip("Unblock", exc))

        await self._bump_digest([actor.player_id], SYNC_DOMAIN_FRIEND_BLOCK)
        logger.info("friend unblocked player=%d target=%d", actor.player_id, target)
        return self._protocol.message("friendpb.UnblockResponse")

    async def list_blocks(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)

        def fail(tip):
            return self._protocol.message("friendpb.ListBlocksResponse", error_message=tip)

        try:
            blocks = await self._with_budget(self._store.list_blocks(actor))
        except Exception as exc:
            return fail(self._storage_failure_tip("ListBlocks", exc))

        pb_blocks = [
            self._protocol.message("friendpb.BlockEntry", blocked_player_id=b.blocked_player_id, since_ms=b.since_ms)
            for b in blocks
        ]
        return self._protocol.message("friendpb.ListBlocksResponse", blocks=pb_blocks)

    async def recommend_friends(self, request: Message, context, path: str) -> Message:
        actor = await self._authenticate_client(context, path)
        rules = self._store.rules

        def fail(tip, *, online_directory: bool = False):
            return self._protocol.message(
                "friendpb.RecommendFriendsResponse",
                error_message=tip,
                online_directory=online_directory,
            )

        if len(request.exclude_player_ids) > rules.recommend_max_exclude:
            return fail(self._tip(ERR_INVALID_PARAMETER, "too many exclude_player_ids"))

        limit = recommend_limit(request.limit, rules.recommend_default_limit, rules.recommend_max_limit)
        if request.online_only:
            return await self._list_online_players(actor, request, limit)

        if limit <= 0:
            return self._protocol.message("friendpb.RecommendFriendsResponse")

        exclude = [int(pid) for pid in request.exclude_player_ids]
        exclude.append(actor.player_id)
        candidates = []
        for name, fetcher in (
            ("mutual", self._store.recommend_by_mutual),
            ("random", self._store.recommend_random),
        ):
            if len(candidates) >= limit:
                break
            try:
                picked = await self._with_budget(fetcher(actor, exclude, limit - len(candidates)))
            except Exception as exc:
                logger.error("friend recommend strategy=%s failed player=%d error=%s", name, actor.player_id, exc)
                return fail(self._storage_failure_tip("RecommendFriends", exc))
            for cand in picked:
                candidates.append(cand)
                exclude.append(cand.candidate_player_id)

        statuses = {}
        if candidates and self._sessions is not None:
            statuses = await self._sessions.fill_online_status([c.candidate_player_id for c in candidates])

        entries = []
        for cand in candidates:
            st = statuses.get(cand.candidate_player_id)
            entries.append(
                self._protocol.message(
                    "friendpb.RecommendEntry",
                    candidate_player_id=cand.candidate_player_id,
                    mutual_friends=cand.mutual_friends,
                    is_online=bool(st.online) if st is not None else False,
                    last_active_ms=st.last_active_ms if st is not None else 0,
                )
            )
        return self._protocol.message("friendpb.RecommendFriendsResponse", candidates=entries)

    async def _list_online_players(self, actor: ActorIdentity, request: Message, limit: int) -> Message:
        """在线组队目录：直读 PlayerRedis 会话 + data_service 同区过滤 + ControlRedis 分页限流。"""

        def fail(code: int, msg: str):
            return self._protocol.message(
                "friendpb.RecommendFriendsResponse",
                online_directory=True,
                error_message=self._tip(code, msg),
            )

        try:
            pos = validate_directory_input(request.cursor, request.query)
        except InvalidDirectoryInputError:
            return fail(ERR_INVALID_PARAMETER, "invalid online directory cursor or query")

        if self._quota is None:
            return fail(ERR_STORAGE, "online directory limiter unavailable")
        try:
            allowed = await self._with_budget(
                self._quota.allow_directory_page(actor.player_id, DIRECTORY_QUOTA_PER_MINUTE)
            )
        except Exception as exc:
            logger.error("friend online directory quota error player=%d error=%s", actor.player_id, exc)
            return fail(ERR_STORAGE, "online directory limiter unavailable")
        if not allowed:
            return fail(ERR_RATE_LIMITED, "online directory rate limited")

        if self._sessions is None:
            return fail(ERR_STORAGE, "online directory session reader unavailable")

        page_limit = limit if limit > 0 else DIRECTORY_DEFAULT_PAGE
        if page_limit > DIRECTORY_MAX_PAGE:
            page_limit = DIRECTORY_MAX_PAGE

        try:
            return await self._with_budget(
                self._scan_online_directory(actor.player_id, pos, page_limit, request.exclude_player_ids, request.query)
            )
        except InvalidDirectoryInputError:
            return fail(ERR_INVALID_PARAMETER, "invalid online directory parameters")
        except Exception as exc:
            logger.error("friend online directory scan failed player=%d error=%s", actor.player_id, exc)
            return fail(ERR_STORAGE, "online directory unavailable")

    async def _scan_online_directory(
        self,
        caller: int,
        pos: DirectoryCursor,
        limit: int,
        exclude_ids,
        raw_query: str,
    ) -> Message:
        assert self._sessions is not None
        ds = self._store.data_service
        caller_zones = await ds.batch_get_player_home_zone([caller])
        zone = caller_zones.get(caller, 0)
        if zone == 0:
            raise StoreUnavailableError(f"online directory caller home_zone unknown player={caller}")

        skip: set[int] = {caller, *(int(x) for x in exclude_ids)}
        query = raw_query.strip().lower()
        entries: list[Message] = []

        for _ in range(DIRECTORY_SCAN_ROUNDS):
            page = await self._sessions.scan_session_page(pos.scan, count=DIRECTORY_SCAN_COUNT)
            if len(page.keys) > DIRECTORY_MAX_BATCH:
                raise StoreUnavailableError(f"online directory scan batch exceeded limit count={len(page.keys)}")
            keys = sorted(page.keys)
            start = 0 if page.restarted else min(pos.offset, len(keys))
            batch_entries = await self._read_directory_entries(keys[start:], skip, query, zone)
            for idx in range(start, len(keys)):
                entry = batch_entries.get(keys[idx])
                if entry is None:
                    continue
                cid = int(entry.candidate_player_id)
                if cid in skip:
                    continue
                skip.add(cid)
                entries.append(entry)
                if len(entries) == limit:
                    next_cursor = (
                        encode_directory_cursor(page.current, idx + 1)
                        if idx + 1 < len(keys)
                        else encode_directory_cursor(page.next, 0)
                    )
                    return self._protocol.message(
                        "friendpb.RecommendFriendsResponse",
                        online_directory=True,
                        candidates=entries,
                        next_cursor=next_cursor,
                    )
            pos = DirectoryCursor(scan=page.next, offset=0)
            if page.next.is_start:
                return self._protocol.message(
                    "friendpb.RecommendFriendsResponse",
                    online_directory=True,
                    candidates=entries,
                )

        return self._protocol.message(
            "friendpb.RecommendFriendsResponse",
            online_directory=True,
            candidates=entries,
            next_cursor=encode_directory_cursor(pos.scan, 0),
        )

    async def _read_directory_entries(
        self, keys: list[str], skip: set[int], query: str, zone: int
    ) -> dict[str, Message]:
        assert self._sessions is not None
        if not keys:
            return {}
        key_ids: list[tuple[str, int]] = []
        for key in keys:
            pid = parse_session_key(key)
            if pid is None or pid in skip:
                continue
            key_ids.append((key, pid))
        if not key_ids:
            return {}

        read_keys = [k for k, _ in key_ids]
        live_ids = [pid for _, pid in key_ids]
        values = await self._sessions.mget_sessions_raw(read_keys)
        if len(values) != len(read_keys):
            raise StoreUnavailableError("online directory mget length mismatch")
        displays = await self._store.data_service.batch_get_player_display(live_ids)

        out: dict[str, Message] = {}
        candidate_ids: list[int] = []
        for (key, pid), raw in zip(key_ids, values):
            brief = displays.get(pid)
            if brief is None or not raw:
                continue
            try:
                sess = self._protocol.parse("playerlocator.PlayerSession", raw)
            except DecodeError:
                continue
            if int(sess.player_id) != pid or int(sess.state) != SESSION_STATE_ONLINE:
                continue
            name = brief.name.strip()
            # 允许新角色首次存盘前 level=0，但昵称与职业必须已确立
            if not name or brief.class_id == 0:
                continue
            if query and query not in name.lower() and query not in str(pid):
                continue
            out[key] = self._protocol.message(
                "friendpb.RecommendEntry",
                candidate_player_id=pid,
                is_online=True,
                last_active_ms=int(sess.last_active_ts),
                name=name,
                level=brief.level,
                class_id=brief.class_id,
                gender=brief.gender,
                appearance_id=brief.appearance_id,
                zone_id=zone,
            )
            candidate_ids.append(pid)

        if not candidate_ids:
            return out
        zones = await self._store.data_service.batch_get_player_home_zone(candidate_ids)
        for key in list(out):
            if zones.get(int(out[key].candidate_player_id), 0) != zone:
                del out[key]
        return out

    # -- RoleFenceParticipant -----------------------------------------------------------------------

    async def prepare_role_freeze(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(
                self._store.prepare_role_freeze(request.state, dry_run=bool(request.dry_run))
            )
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "friend PrepareRoleFreeze failed player=%d version=%d dry_run=%s",
                request.state.player_id,
                request.state.version,
                request.dry_run,
                exc_info=True,
            )
            return self._protocol.message("rolefence.PrepareRoleFreezeResponse", retry=True)

    async def apply_role_fence(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(self._store.apply_role_fence(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "friend ApplyRoleFence failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self._protocol.message("rolefence.ApplyRoleFenceResponse", retry=True)

    async def purge_removed_role(self, request: Message, context, path: str) -> Message:
        await self._admit_coordinator(context, request, path)
        try:
            return await self._with_budget(self._store.purge_removed_role(request.state))
        except InvalidRoleFenceStateError as exc:
            await context.abort(CODE.INVALID_ARGUMENT, str(exc))
        except Exception:
            logger.error(
                "friend PurgeRemovedRole failed player=%d version=%d",
                request.state.player_id,
                request.state.version,
                exc_info=True,
            )
            return self._protocol.message("rolefence.PurgeRemovedRoleResponse", retry=True)
