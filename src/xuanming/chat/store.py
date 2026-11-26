"""chat 核心存储引擎、外部依赖协议（身份校验/档案/内容安全/广播）与确定性内存双胞胎（MemoryChatStore）。"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from typing import Protocol as TypingProtocol, TypeVar

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import sign_service_call
from ..protocol import Protocol
from .errors import (
    ERR_CONTACT_REJECTED,
    ERR_CONTENT_CHECK_UNAVAILABLE,
    ERR_CONTENT_REJECTED,
    ERR_DUPLICATE_SPAM,
    ERR_LEVEL_TOO_LOW,
    ERR_MESSAGE_NOT_FOUND,
    ERR_PRIVATE_TARGET_LIMIT,
    ERR_RATE_LIMITED,
    InvalidChatError,
    OriginalRoleError,
    PolicyMismatchError,
    RequestConflictError,
    StoreUnavailableError,
)
from .model import (
    ACCEPTED_STATE,
    ActorIdentity,
    CHANNEL_PRIVATE,
    CHANNEL_WORLD,
    ChatRules,
    HistoryRow,
    INT64_MAX,
    MAX_REPEAT_SCAN_ROWS,
    MAX_REPORT_DETAIL_BYTES,
    MAX_REPORT_REASON,
    MAX_REQUEST_ID_BYTES,
    MAX_RETRACT_REASON_BYTES,
    PlayerProfile,
    REVIEW_MASKED,
    REVIEW_PASS,
    REVIEW_REJECT,
    REVIEW_UNAVAILABLE,
    ReceiptRow,
    ReviewResult,
    SendResult,
    TEXT_CATEGORY_AD,
    TEXT_SCENE_CHAT_PRIVATE,
    TEXT_SCENE_CHAT_WORLD,
    TEXT_VERDICT_MASKED,
    TEXT_VERDICT_PASS,
    WORLD_BROADCAST_MSG_ID,
    WORLD_RETRACT_MSG_ID,
    WORLD_STREAM_KEYS,
    accepted_at_utc,
    filter_retracted_rows,
    newest_rows,
    normalize_spam_hash,
    policy_hash,
    render_system_world_notice,
    stream_key,
    utc_day_ymd,
    valid_account,
    validate_receipt,
    visible_history,
    world_stream_key,
)
from .world_window import WorldSnapshot, WorldWindow

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_CONTENT_GUARD_PREFIX = "/content_guard.ContentGuard/"
_ROLE_FENCE_CALLER = "chat"
_ROLE_FENCE_TIMEOUT_SEC = 2.0
_PROFILE_TIMEOUT_SEC = 2.0
_WORLD_PUSH_TIMEOUT_SEC = 0.5
_PLATFORM_CHECK_MIN_TIMEOUT_SEC = 3.0
_ACTOR_VERIFY_MAX_ENTRIES = 1 << 16
_PROFILE_CACHE_MAX_ENTRIES = 1 << 16

T = TypeVar("T")


class _TxRollbackReturn(Exception):
    """在事务内显式回滚并返回业务结果的内部控制流信号。"""

    def __init__(self, value) -> None:
        super().__init__("rollback_return")
        self.value = value


# ---------------------------------------------------------------------------
# 可注入外部依赖协议（Protocols）
# ---------------------------------------------------------------------------


class ActorVerifier(TypingProtocol):
    """角色归属与冻结状态校验协议（生产调用 DataService.GetRoleFenceState）。"""

    async def verify_actor(self, identity: ActorIdentity) -> None: ...


class ProfileReader(TypingProtocol):
    """玩家基础档案读取协议（用于世界发言等级门槛与新号私聊限额）。"""

    async def get_player_profile(self, player_id: int) -> PlayerProfile: ...


class ContentReviewer(TypingProtocol):
    """正文内容安全送检协议（生产调用 ContentGuard.CheckText / CheckPlatformText）。"""

    async def check_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
        async_platform: bool = False,
    ) -> ReviewResult: ...

    async def check_platform_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
    ) -> ReviewResult: ...


class WorldPusher(TypingProtocol):
    """世界频道广播与撤回通知推送协议（生产写入 Kafka chat-world-bcast_g<N>）。"""

    async def push_world(self, message: Message) -> None: ...

    async def push_world_retract(self, notify: Message) -> None: ...


# ---------------------------------------------------------------------------
# 生产适配器实现
# ---------------------------------------------------------------------------


class GrpcActorVerifier:
    """基于 DataService.GetRoleFenceState（以 'chat' HMAC 签名）的角色身份校验器与 TTL 缓存。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        role_fence_secret: bytes,
        *,
        ttl_sec: float = 30.0,
        max_entries: int = _ACTOR_VERIFY_MAX_ENTRIES,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._secret = role_fence_secret
        self._ttl_sec = ttl_sec
        self._max_entries = max_entries
        self._clock_ms = clock_ms
        self._now = monotonic_clock
        self._expires: dict[ActorIdentity, float] = {}

    async def verify_actor(self, identity: ActorIdentity) -> None:
        if identity.player_id <= 0 or identity.owner_revision <= 0 or not valid_account(identity.account):
            raise OriginalRoleError("invalid actor identity")
        if self._ttl_sec <= 0:
            await self._read_remote(identity)
            return
        started = self._now()
        expiry = self._expires.get(identity)
        if expiry is not None:
            if started < expiry:
                return
            self._expires.pop(identity, None)
        await self._read_remote(identity)
        if identity not in self._expires and len(self._expires) >= self._max_entries:
            self._expires.clear()
        self._expires[identity] = started + self._ttl_sec

    async def _read_remote(self, identity: ActorIdentity) -> None:
        path = _DATA_SERVICE_PREFIX + "GetRoleFenceState"
        req = self._protocol.message("data_service.GetRoleFenceStateRequest", player_id=identity.player_id)
        sign_service_call(path, req, _ROLE_FENCE_CALLER, self._clock_ms(), self._secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=_ROLE_FENCE_TIMEOUT_SEC)
        except Exception as exc:
            raise StoreUnavailableError(f"GetRoleFenceState failed player={identity.player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetRoleFenceStateResponse", raw)
        if resp.retry:
            raise StoreUnavailableError(f"GetRoleFenceState requested retry player={identity.player_id}")
        if not resp.found:
            raise OriginalRoleError(f"role not found player={identity.player_id}")
        state = resp.state
        if int(state.player_id) != identity.player_id:
            raise StoreUnavailableError(f"GetRoleFenceState player_id mismatch player={identity.player_id}")
        if (
            not valid_account(state.account)
            or int(state.owner_revision) == 0
            or state.account != identity.account
            or int(state.owner_revision) != identity.owner_revision
            or bool(state.freeze_final)
        ):
            raise OriginalRoleError(f"role ownership changed or frozen player={identity.player_id}")


class GrpcProfileReader:
    """基于 DataService.GetPlayerChannelIdentity 的玩家基础档案读取器（带 TTL 缓存）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        *,
        ttl_sec: float = 30.0,
        max_entries: int = _PROFILE_CACHE_MAX_ENTRIES,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._ttl_sec = max(30.0, float(ttl_sec))
        self._max_entries = max_entries
        self._now = monotonic_clock
        self._cache: dict[int, tuple[PlayerProfile, float]] = {}

    async def get_player_profile(self, player_id: int) -> PlayerProfile:
        started = self._now()
        cached = self._cache.get(player_id)
        if cached is not None and started < cached[1]:
            return cached[0]
        path = _DATA_SERVICE_PREFIX + "GetPlayerChannelIdentity"
        req = self._protocol.message("data_service.GetPlayerChannelIdentityRequest", player_id=player_id)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=_PROFILE_TIMEOUT_SEC)
        except Exception as exc:
            raise StoreUnavailableError(f"GetPlayerChannelIdentity failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetPlayerChannelIdentityResponse", raw)
        if resp.retry:
            raise StoreUnavailableError(f"GetPlayerChannelIdentity requested retry player={player_id}")
        profile = (
            PlayerProfile(level=int(resp.player_level), account_created_ms=int(resp.account_created_ms))
            if resp.found
            else PlayerProfile()
        )
        if player_id not in self._cache and len(self._cache) >= self._max_entries:
            self._cache.clear()
        self._cache[player_id] = (profile, started + self._ttl_sec)
        return profile


def parse_content_guard_response(resp: Message) -> ReviewResult:
    """将 ContentGuard.CheckTextResponse 折算为 ReviewResult：仅认 PASS 与非空 MASKED。"""
    verdict = int(resp.verdict)
    category = int(resp.category)
    pending = bool(resp.pending_platform_review)
    if verdict == TEXT_VERDICT_PASS:
        return ReviewResult(verdict=REVIEW_PASS, pending_platform_review=pending)
    if verdict == TEXT_VERDICT_MASKED and resp.masked_text:
        return ReviewResult(
            verdict=REVIEW_MASKED,
            masked=resp.masked_text,
            category=category,
            pending_platform_review=pending,
        )
    return ReviewResult(verdict=REVIEW_REJECT, category=category)


class PassthroughContentReviewer:
    """本地开发直通模式（仅 content_guard_optional=True 且未配置目标时使用）。"""

    async def check_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
        async_platform: bool = False,
    ) -> ReviewResult:
        return ReviewResult(verdict=REVIEW_PASS)

    async def check_platform_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
    ) -> ReviewResult:
        return ReviewResult(verdict=REVIEW_PASS)


class GrpcContentReviewer:
    """ContentGuard gRPC 客户端（与 Go shared/contentguard.Client 口径一致）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        *,
        timeout_sec: float = 0.3,
        optional: bool = False,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._timeout_sec = timeout_sec
        self._optional = optional
        self._last_log_sec: int = 0

    def _log_failure(self, scene: int, exc: BaseException) -> None:
        now = int(time.monotonic())
        if now == self._last_log_sec:
            return
        self._last_log_sec = now
        if self._optional:
            logger.error(
                "content_guard check failed scene=%d passing text UNCHECKED (optional=true): %s",
                scene,
                exc,
            )
        else:
            logger.error("content_guard check failed scene=%d treating text as unavailable: %s", scene, exc)

    async def check_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
        async_platform: bool = False,
    ) -> ReviewResult:
        if not text:
            return ReviewResult(verdict=REVIEW_PASS)
        req = self._protocol.message(
            "content_guard.CheckTextRequest",
            scene=scene,
            text=text,
            player_id=player_id,
            account=account,
            async_platform=async_platform,
        )
        try:
            raw = await self._call(
                _CONTENT_GUARD_PREFIX + "CheckText",
                req.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
        except Exception as exc:
            self._log_failure(scene, exc)
            return ReviewResult(verdict=REVIEW_PASS if self._optional else REVIEW_UNAVAILABLE)
        return parse_content_guard_response(self._protocol.parse("content_guard.CheckTextResponse", raw))

    async def check_platform_text(
        self,
        scene: int,
        player_id: int,
        text: str,
        *,
        account: str = "",
    ) -> ReviewResult:
        if not text:
            return ReviewResult(verdict=REVIEW_PASS)
        req = self._protocol.message(
            "content_guard.CheckTextRequest",
            scene=scene,
            text=text,
            player_id=player_id,
            account=account,
        )
        budget = max(self._timeout_sec, _PLATFORM_CHECK_MIN_TIMEOUT_SEC)
        try:
            raw = await self._call(
                _CONTENT_GUARD_PREFIX + "CheckPlatformText",
                req.SerializeToString(deterministic=True),
                timeout=budget,
            )
        except Exception as exc:
            self._log_failure(scene, exc)
            return ReviewResult(verdict=REVIEW_UNAVAILABLE)
        return parse_content_guard_response(self._protocol.parse("content_guard.CheckTextResponse", raw))


class KafkaWorldPusher:
    """将世界频道消息与撤回通知包装为 BroadcastToAllEvent 写入 chat-world-bcast_g<N>。"""

    def __init__(self, protocol: Protocol, producer, *, generation: int = 1) -> None:
        self._protocol = protocol
        self._producer = producer
        self.topic = f"chat-world-bcast_g{generation}"
        self._broadcast_msg_id = int(protocol.message_ids.get("ClientPlayerChatNotifyWorldChat", WORLD_BROADCAST_MSG_ID))
        self._retract_msg_id = int(
            protocol.message_ids.get("ClientPlayerChatNotifyWorldChatRetracted", WORLD_RETRACT_MSG_ID)
        )

    async def _write(self, message_id: int, sender_player_id: int, payload_msg: Message) -> None:
        body = payload_msg.SerializeToString()
        event = self._protocol.message(
            "contracts.kafka.BroadcastToAllEvent",
            message_content=self._protocol.message(
                "MessageContent",
                message_id=message_id,
                serialized_message=body,
            ),
        )
        async with asyncio.timeout(_WORLD_PUSH_TIMEOUT_SEC):
            await self._producer.send_and_wait(
                self.topic,
                event.SerializeToString(),
                key=str(sender_player_id).encode("ascii"),
            )

    async def push_world(self, message: Message) -> None:
        await self._write(self._broadcast_msg_id, int(message.sender_player_id), message)

    async def push_world_retract(self, notify: Message) -> None:
        await self._write(self._retract_msg_id, int(notify.sender_player_id), notify)


# ---------------------------------------------------------------------------
# 底层事务驱动协议与核心业务存储（SqlChatStore 与 MemoryChatStore 共用）
# ---------------------------------------------------------------------------


class ChatTx(TypingProtocol):
    """单事务内聊天权威表原语协议。"""

    async def now_ms(self) -> int: ...

    async def read_receipt(self, player_id: int, request_id: bytes) -> ReceiptRow | None: ...

    async def count_rate_window(self, player_id: int, start_ms: int, end_ms: int) -> int: ...

    async def has_world_cooldown(self, player_id: int, since_ms: int, stream_key_bytes: bytes) -> bool: ...

    async def list_repeat_senders(self, content_hash: bytes, since_ms: int, limit: int) -> list[int]: ...

    async def insert_repeat_event(
        self, content_hash: bytes, accepted_ms: int, sender_player_id: int, accepted_at: str
    ) -> None: ...

    async def has_private_daily_target(self, sender_player_id: int, day_ymd: int, target_player_id: int) -> bool: ...

    async def count_private_daily_targets(self, sender_player_id: int, day_ymd: int) -> int: ...

    async def insert_private_daily_target(
        self, sender_player_id: int, day_ymd: int, target_player_id: int, accepted_at: str
    ) -> None: ...

    async def insert_send_authority(
        self,
        player_id: int,
        request_id: bytes,
        owner_account: bytes,
        owner_revision: int,
        request_digest: bytes,
        request_payload: bytes,
        message_payload: bytes,
        stream_key_bytes: bytes,
        accepted_ms: int,
        accepted_at: str,
    ) -> bool: ...

    async def read_history_rows(self, stream_key_bytes: bytes, limit: int) -> list[HistoryRow]: ...

    async def load_retracted_world_keys(self, min_ms: int) -> set[tuple[int, int]]: ...

    async def message_exists(self, player_id: int, accepted_ms: int, stream_key_bytes: bytes) -> bool: ...

    async def insert_world_retraction(
        self, sender_player_id: int, accepted_ms: int, retracted_ms: int, reason: bytes, accepted_at: str
    ) -> bool: ...

    async def insert_report(
        self,
        reporter_player_id: int,
        channel: int,
        target_sender_player_id: int,
        send_time_ms: int,
        reason: int,
        detail: bytes,
        reported_ms: int,
        accepted_at: str,
    ) -> None: ...


class ChatDriver(TypingProtocol):
    """聊天存储驱动协议（抽象事务生命周期与事务外只读回查）。"""

    async def run_tx(self, work: Callable[[ChatTx], Awaitable[T]]) -> T: ...

    async def read_receipt_direct(self, player_id: int, request_id: bytes) -> ReceiptRow | None: ...


class ChatStoreCore:
    """聊天权威存储核心：编排身份校验、内容审查、事务幂等提交、8 分段世界合并与近窗缓存。"""

    def __init__(
        self,
        protocol: Protocol,
        driver: ChatDriver,
        actor_verifier: ActorVerifier,
        rules: ChatRules,
        *,
        reviewer: ContentReviewer | None = None,
        profile_reader: ProfileReader | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        rules.validate()
        self._protocol = protocol
        self._driver = driver
        self._actor_verifier = actor_verifier
        self._rules = rules
        self._reviewer = reviewer
        self._profile_reader = profile_reader
        self._world_window: WorldWindow | None = (
            WorldWindow(
                rules.world_history_cache_ms / 1000.0,
                self._load_world_snapshot,
                monotonic_clock=monotonic_clock,
            )
            if rules.world_history_cache_ms > 0
            else None
        )

    @property
    def rules(self) -> ChatRules:
        return self._rules

    @property
    def reviewer(self) -> ContentReviewer | None:
        return self._reviewer

    @property
    def world_window(self) -> WorldWindow | None:
        return self._world_window

    async def close(self) -> None:
        if self._world_window is not None:
            await self._world_window.close()

    def _validate_send_request(self, request: Message) -> None:
        if request is None or not request.HasField("message"):
            raise InvalidChatError("missing chat message")
        msg = request.message
        if (
            len(request.request_id.encode("utf-8")) > MAX_REQUEST_ID_BYTES
            or request.ByteSize() > self._rules.max_content_bytes + 8192
            or len(msg.content.encode("utf-8")) > self._rules.max_content_bytes
            or not msg.content.strip()
        ):
            raise InvalidChatError("invalid send chat request payload")
        if int(msg.channel) not in (CHANNEL_WORLD, CHANNEL_PRIVATE):
            raise InvalidChatError("unsupported chat channel")

    async def _review_content(
        self, channel: int, actor: int, account: str, content: str
    ) -> tuple[str, int, bool]:
        """在事务外调用内容安全服务，仅认 PASS 与合法 MASKED。"""
        if self._reviewer is None:
            return content, 0, False
        if channel == CHANNEL_WORLD:
            result = await self._reviewer.check_text(
                TEXT_SCENE_CHAT_WORLD, actor, content, account=account, async_platform=True
            )
        else:
            result = await self._reviewer.check_text(
                TEXT_SCENE_CHAT_PRIVATE, actor, content, account=account, async_platform=False
            )
        if result.verdict == REVIEW_PASS:
            return content, 0, result.pending_platform_review
        if result.verdict == REVIEW_MASKED:
            if result.masked.strip() and len(result.masked.encode("utf-8")) <= self._rules.max_content_bytes:
                return result.masked, 0, result.pending_platform_review
        if result.verdict == REVIEW_UNAVAILABLE:
            return "", ERR_CONTENT_CHECK_UNAVAILABLE, False
        if result.category == TEXT_CATEGORY_AD:
            return "", ERR_CONTACT_REJECTED, False
        return "", ERR_CONTENT_REJECTED, False

    def _replay_receipt(
        self,
        receipt: ReceiptRow,
        identity: ActorIdentity,
        digest: bytes,
        raw_request: bytes,
        channel: int,
        peer: int,
        expected_stream: bytes,
    ) -> SendResult:
        """核对已提交收据的归属、摘要、请求原件与显示原件（重放不刷新时间、不二次送检）。"""
        if receipt.account != identity.account.encode("utf-8") or receipt.revision != identity.owner_revision:
            raise OriginalRoleError("chat receipt belongs to another role owner")
        if receipt.digest != digest or receipt.request != raw_request:
            raise RequestConflictError("request_id already used for a different chat request")
        if receipt.stream != expected_stream:
            raise StoreUnavailableError("chat receipt stream does not match request")
        try:
            stored_msg = self._protocol.parse("chatpb.ChatMessage", receipt.message)
        except DecodeError as exc:
            raise StoreUnavailableError("corrupt stored chat display message") from exc
        if (
            int(stored_msg.sender_player_id) != identity.player_id
            or int(stored_msg.target_player_id) != peer
            or int(stored_msg.channel) != channel
            or int(stored_msg.send_time_ms) != receipt.accepted_ms
            or not stored_msg.content.strip()
        ):
            raise StoreUnavailableError("chat display message does not match accepted decision")
        return SendResult(tip=receipt.tip, duplicate=True)

    async def _load_profile_if_needed(self, actor: int, peer: int) -> tuple[PlayerProfile, bool]:
        need_level = peer == 0 and self._rules.world_min_level > 0
        need_age = (
            peer != 0
            and self._rules.new_account_age_hours > 0
            and self._rules.new_account_daily_private_limit > 0
        )
        if (not need_level and not need_age) or self._profile_reader is None:
            return PlayerProfile(), False
        profile = await self._profile_reader.get_player_profile(actor)
        return profile, True

    async def _check_and_record_private_target(
        self,
        tx: ChatTx,
        actor: int,
        peer: int,
        now_ms: int,
        profile: PlayerProfile,
        has_profile: bool,
    ) -> int:
        if (
            peer == 0
            or self._rules.new_account_age_hours <= 0
            or self._rules.new_account_daily_private_limit <= 0
            or not has_profile
        ):
            return 0
        age_limit_ms = self._rules.new_account_age_hours * 3600 * 1000
        if profile.account_created_ms > 0 and now_ms > profile.account_created_ms:
            if (now_ms - profile.account_created_ms) >= age_limit_ms:
                return 0
        day = utc_day_ymd(now_ms)
        if await tx.has_private_daily_target(actor, day, peer):
            return 0
        distinct_count = await tx.count_private_daily_targets(actor, day)
        if distinct_count >= self._rules.new_account_daily_private_limit:
            return ERR_PRIVATE_TARGET_LIMIT
        await tx.insert_private_daily_target(actor, day, peer, accepted_at_utc(now_ms))
        return 0

    async def _check_and_record_repeat_spam(self, tx: ChatTx, actor: int, now_ms: int, raw_trimmed: str) -> int:
        if self._rules.duplicate_spam_window_ms <= 0 or (
            self._rules.duplicate_spam_max_senders <= 0 and self._rules.duplicate_spam_max_per_sender <= 0
        ):
            return 0
        content_hash = normalize_spam_hash(raw_trimmed)
        if content_hash is None:
            return 0
        since = max(0, now_ms - self._rules.duplicate_spam_window_ms)
        senders = await tx.list_repeat_senders(content_hash, since, MAX_REPEAT_SCAN_ROWS)
        others: set[int] = set()
        self_count = 0
        for sender in senders:
            if sender == actor:
                self_count += 1
            else:
                others.add(sender)
        if self._rules.duplicate_spam_max_senders > 0 and len(others) >= self._rules.duplicate_spam_max_senders:
            return ERR_DUPLICATE_SPAM
        if self._rules.duplicate_spam_max_per_sender > 0 and self_count >= self._rules.duplicate_spam_max_per_sender:
            return ERR_DUPLICATE_SPAM
        await tx.insert_repeat_event(content_hash, now_ms, actor, accepted_at_utc(now_ms))
        return 0

    async def send(self, identity: ActorIdentity, request: Message) -> SendResult:
        """发送世界或私聊消息（事务外验身份/审正文，事务内先判重再限流与落库）。"""
        self._validate_send_request(request)
        raw_request = request.SerializeToString(deterministic=True)
        digest = hashlib.sha256(raw_request).digest()
        request_id = request.request_id or f"server:{secrets.token_hex(16)}"
        request_id_bytes = request_id.encode("utf-8")
        channel = int(request.message.channel)
        peer = int(request.message.target_player_id) if channel == CHANNEL_PRIVATE else 0
        if channel == CHANNEL_PRIVATE and peer == 0:
            raise InvalidChatError("private chat requires target_player_id")

        await self._actor_verifier.verify_actor(identity)
        actor = identity.player_id
        if peer == actor:
            raise InvalidChatError("cannot send private chat to self")

        expected_stream = stream_key(actor, peer).encode("utf-8")

        def replay(receipt: ReceiptRow) -> SendResult:
            return self._replay_receipt(receipt, identity, digest, raw_request, channel, peer, expected_stream)

        async def reject_or_replay(tip: int) -> SendResult:
            # 拒绝前先查权威收据，确保已提交的 request_id 重放不受后续等级/词库变化影响。
            winner = await self._driver.read_receipt_direct(actor, request_id_bytes)
            if winner is not None:
                return replay(winner)
            return SendResult(tip=tip)

        profile, has_profile = await self._load_profile_if_needed(actor, peer)
        if peer == 0 and self._rules.world_min_level > 0 and has_profile and profile.level < self._rules.world_min_level:
            return await reject_or_replay(ERR_LEVEL_TOO_LOW)

        raw_trimmed = request.message.content.strip()
        content, reject_tip, pending_platform = await self._review_content(
            channel, actor, identity.account, raw_trimmed
        )
        if reject_tip != 0:
            return await reject_or_replay(reject_tip)

        async def tx_work(tx: ChatTx) -> SendResult:
            existing = await tx.read_receipt(actor, request_id_bytes)
            if existing is not None:
                raise _TxRollbackReturn(replay(existing))

            now_ms = await tx.now_ms()
            if now_ms <= 0 or now_ms > INT64_MAX:
                raise StoreUnavailableError(f"invalid SQL timestamp: {now_ms}")

            async def tx_reject_or_replay(tip: int, *, cooldown: bool = False) -> SendResult:
                winner = await tx.read_receipt(actor, request_id_bytes)
                if winner is not None:
                    raise _TxRollbackReturn(replay(winner))
                raise _TxRollbackReturn(SendResult(tip=tip, cooldown=cooldown))

            start_ms = now_ms - (now_ms % 1000)
            count = await tx.count_rate_window(actor, start_ms, start_ms + 1000)
            if count >= self._rules.rate_limit_per_second:
                return await tx_reject_or_replay(ERR_RATE_LIMITED)

            if peer == 0 and self._rules.world_channel_cooldown_ms > 0:
                since = max(0, now_ms - self._rules.world_channel_cooldown_ms)
                if await tx.has_world_cooldown(actor, since, expected_stream):
                    return await tx_reject_or_replay(ERR_RATE_LIMITED, cooldown=True)

            priv_tip = await self._check_and_record_private_target(tx, actor, peer, now_ms, profile, has_profile)
            if priv_tip != 0:
                return await tx_reject_or_replay(priv_tip)

            spam_tip = await self._check_and_record_repeat_spam(tx, actor, now_ms, raw_trimmed)
            if spam_tip != 0:
                return await tx_reject_or_replay(spam_tip)

            # 强制以已验签的会话 actor 覆盖 sender_player_id，杜绝客户端伪造发言人。
            canonical_msg = self._protocol.message(
                "chatpb.ChatMessage",
                sender_player_id=actor,
                target_player_id=peer,
                channel=channel,
                content=content,
                send_time_ms=now_ms,
            )
            payload = canonical_msg.SerializeToString(deterministic=True)
            inserted = await tx.insert_send_authority(
                actor,
                request_id_bytes,
                identity.account.encode("utf-8"),
                identity.owner_revision,
                digest,
                raw_request,
                payload,
                expected_stream,
                now_ms,
                accepted_at_utc(now_ms),
            )
            if not inserted:
                winner = await tx.read_receipt(actor, request_id_bytes)
                if winner is None:
                    raise StoreUnavailableError("duplicate key without visible winning receipt")
                raise _TxRollbackReturn(replay(winner))

            return SendResult(
                message=canonical_msg,
                pending_platform_review=(peer == 0 and pending_platform),
                platform_review_content=raw_trimmed if (peer == 0 and pending_platform) else "",
            )

        result = await self._driver.run_tx(tx_work)
        if peer == 0 and result.message is not None and self._world_window is not None:
            self._world_window.invalidate()
        return result

    async def _read_world_rows(self, tx: ChatTx, limit: int) -> list[HistoryRow]:
        """从 8 个世界分段各取最新 limit 行，过滤撤回记录后再按新到旧归并截取前 limit 行。"""
        merged: list[HistoryRow] = []
        for shard_key in WORLD_STREAM_KEYS:
            merged.extend(await tx.read_history_rows(shard_key.encode("utf-8"), limit))
        if merged:
            min_ms = min(row.accepted_ms for row in merged)
            retracted = await tx.load_retracted_world_keys(min_ms)
            merged = filter_retracted_rows(merged, retracted)
        return newest_rows(merged, limit)

    async def _load_world_snapshot(self) -> WorldSnapshot:
        async def tx_work(tx: ChatTx) -> WorldSnapshot:
            now_ms = await tx.now_ms()
            if now_ms <= 0 or now_ms > INT64_MAX:
                raise StoreUnavailableError(f"invalid SQL timestamp: {now_ms}")
            rows = await self._read_world_rows(tx, self._rules.max_pull_rows())
            return WorldSnapshot(rows=tuple(rows), db_now_ms=now_ms)

        return await self._driver.run_tx(tx_work)

    async def pull(self, identity: ActorIdentity, request: Message, limit: int) -> list[Message]:
        """拉取世界或私聊最近 limit 条可见历史（新在前，含闲置 TTL 截断）。"""
        if request is None or limit <= 0:
            raise InvalidChatError("invalid pull chat history request")
        channel = int(request.channel)
        if channel not in (CHANNEL_WORLD, CHANNEL_PRIVATE):
            raise InvalidChatError("unsupported channel for pull")
        peer = int(request.peer_player_id) if channel == CHANNEL_PRIVATE else 0
        if channel == CHANNEL_PRIVATE and peer == 0:
            raise InvalidChatError("private pull requires peer_player_id")

        await self._actor_verifier.verify_actor(identity)
        actor = identity.player_id
        if actor == peer:
            raise InvalidChatError("cannot pull private history with self")

        effective_limit = min(limit, self._rules.max_pull_rows())
        window_ms = self._rules.history_ttl_seconds * 1000

        if peer == 0 and self._world_window is not None:
            snap = await self._world_window.get()
            rows = list(snap.rows[:effective_limit])
            return visible_history(
                self._protocol,
                rows,
                snap.now_ms(self._world_window.now()),
                window_ms,
                channel,
                actor,
                peer,
            )

        async def tx_work(tx: ChatTx) -> tuple[list[HistoryRow], int]:
            now_ms = await tx.now_ms()
            if now_ms <= 0 or now_ms > INT64_MAX:
                raise StoreUnavailableError(f"invalid SQL timestamp: {now_ms}")
            if peer == 0:
                rows = await self._read_world_rows(tx, effective_limit)
            else:
                rows = await tx.read_history_rows(stream_key(actor, peer).encode("utf-8"), effective_limit)
            return rows, now_ms

        rows, now_ms = await self._driver.run_tx(tx_work)
        return visible_history(self._protocol, rows, now_ms, window_ms, channel, actor, peer)

    async def retract_world(self, sender_player_id: int, accepted_ms: int, reason: str) -> tuple[bool, int]:
        """追加写世界频道撤回记录并作废本地近窗快照（不修改原件表）。"""
        if sender_player_id <= 0 or accepted_ms <= 0 or len(reason.encode("utf-8")) > MAX_RETRACT_REASON_BYTES:
            raise InvalidChatError("invalid retract world chat request")
        stream = world_stream_key(sender_player_id).encode("utf-8")
        reason_bytes = reason.encode("utf-8")

        async def tx_work(tx: ChatTx) -> tuple[bool, int]:
            if not await tx.message_exists(sender_player_id, accepted_ms, stream):
                raise _TxRollbackReturn((False, ERR_MESSAGE_NOT_FOUND))
            now_ms = await tx.now_ms()
            if now_ms <= 0 or now_ms > INT64_MAX:
                raise StoreUnavailableError(f"invalid SQL timestamp: {now_ms}")
            inserted = await tx.insert_world_retraction(
                sender_player_id, accepted_ms, now_ms, reason_bytes, accepted_at_utc(accepted_ms)
            )
            return inserted, 0

        retracted, tip = await self._driver.run_tx(tx_work)
        if tip == 0 and self._world_window is not None:
            self._world_window.invalidate()
        return retracted, tip

    async def publish_system_world_notice(self, request: Message) -> SendResult:
        """幂等写入系统全服模板播报至世界频道权威历史（player_id=0, request_id=notice_id）。"""
        content, ok = render_system_world_notice(request)
        if not ok:
            raise InvalidChatError("invalid system world notice request")
        notice_id_bytes = request.notice_id.strip().encode("utf-8")
        raw_request = request.SerializeToString(deterministic=True)
        digest = hashlib.sha256(raw_request).digest()
        stream_bytes = world_stream_key(0).encode("utf-8")

        async def tx_work(tx: ChatTx) -> SendResult:
            existing = await tx.read_receipt(0, notice_id_bytes)
            if existing is not None:
                raise _TxRollbackReturn(SendResult(duplicate=True))
            now_ms = await tx.now_ms()
            if now_ms <= 0 or now_ms > INT64_MAX:
                raise StoreUnavailableError(f"invalid SQL timestamp: {now_ms}")
            message = self._protocol.message(
                "chatpb.ChatMessage",
                sender_player_id=0,
                target_player_id=0,
                channel=CHANNEL_WORLD,
                content=content,
                send_time_ms=now_ms,
            )
            payload = message.SerializeToString(deterministic=True)
            inserted = await tx.insert_send_authority(
                0,
                notice_id_bytes,
                b"system",
                0,
                digest,
                raw_request,
                payload,
                stream_bytes,
                now_ms,
                accepted_at_utc(now_ms),
            )
            if not inserted:
                raise _TxRollbackReturn(SendResult(duplicate=True))
            return SendResult(message=message)

        result = await self._driver.run_tx(tx_work)
        if not result.duplicate and result.message is not None and self._world_window is not None:
            self._world_window.invalidate()
        return result

    async def report_message(self, identity: ActorIdentity, request: Message) -> int:
        """核验目标消息存在且对当前举报人可见后，幂等写入举报审计表 chat_report。"""
        if (
            request is None
            or int(request.target_sender_player_id) == 0
            or int(request.send_time_ms) <= 0
            or not 1 <= int(request.reason) <= MAX_REPORT_REASON
            or len(request.detail.encode("utf-8")) > MAX_REPORT_DETAIL_BYTES
        ):
            raise InvalidChatError("invalid report chat request")
        channel = int(request.channel)
        if channel not in (CHANNEL_WORLD, CHANNEL_PRIVATE):
            raise InvalidChatError("unsupported channel for report")

        await self._actor_verifier.verify_actor(identity)
        reporter = identity.player_id
        target = int(request.target_sender_player_id)
        send_time_ms = int(request.send_time_ms)
        if channel == CHANNEL_PRIVATE and reporter == target:
            raise InvalidChatError("cannot report self in private channel")

        expected_stream = (
            world_stream_key(target) if channel == CHANNEL_WORLD else stream_key(reporter, target)
        ).encode("utf-8")
        detail_bytes = request.detail.strip().encode("utf-8")

        async def tx_work(tx: ChatTx) -> int:
            if not await tx.message_exists(target, send_time_ms, expected_stream):
                raise _TxRollbackReturn(ERR_MESSAGE_NOT_FOUND)
            now_ms = await tx.now_ms()
            if now_ms <= 0 or now_ms > INT64_MAX:
                raise StoreUnavailableError(f"invalid SQL timestamp: {now_ms}")
            await tx.insert_report(
                reporter,
                channel,
                target,
                send_time_ms,
                int(request.reason),
                detail_bytes,
                now_ms,
                accepted_at_utc(now_ms),
            )
            return 0

        return await self._driver.run_tx(tx_work)


# ---------------------------------------------------------------------------
# 确定性内存双胞胎（MemoryChatStore）
# ---------------------------------------------------------------------------


class _MemoryTx:
    """单事务内存视图，配合 _MemoryDriver 快照回滚保证原子性。"""

    def __init__(self, driver: "_MemoryDriver") -> None:
        self._d = driver

    async def now_ms(self) -> int:
        return int(self._d.clock_ms())

    async def read_receipt(self, player_id: int, request_id: bytes) -> ReceiptRow | None:
        row = self._d.send_rows.get((player_id, request_id))
        return validate_receipt(row) if row is not None else None

    async def count_rate_window(self, player_id: int, start_ms: int, end_ms: int) -> int:
        return sum(
            1
            for (pid, _), row in self._d.send_rows.items()
            if pid == player_id and row.state == ACCEPTED_STATE and start_ms <= row.accepted_ms < end_ms
        )

    async def has_world_cooldown(self, player_id: int, since_ms: int, stream_key_bytes: bytes) -> bool:
        return any(
            pid == player_id
            and row.state == ACCEPTED_STATE
            and row.accepted_ms > since_ms
            and row.stream == stream_key_bytes
            for (pid, _), row in self._d.send_rows.items()
        )

    async def list_repeat_senders(self, content_hash: bytes, since_ms: int, limit: int) -> list[int]:
        matched = [
            (accepted_ms, sender)
            for (h, accepted_ms, sender) in self._d.repeat_events
            if h == content_hash and accepted_ms >= since_ms
        ]
        matched.sort()
        return [sender for _, sender in matched[:limit]]

    async def insert_repeat_event(
        self, content_hash: bytes, accepted_ms: int, sender_player_id: int, accepted_at: str
    ) -> None:
        self._d.repeat_events.add((content_hash, accepted_ms, sender_player_id))

    async def has_private_daily_target(self, sender_player_id: int, day_ymd: int, target_player_id: int) -> bool:
        return (sender_player_id, day_ymd, target_player_id) in self._d.private_targets

    async def count_private_daily_targets(self, sender_player_id: int, day_ymd: int) -> int:
        return sum(1 for (s, d, _) in self._d.private_targets if s == sender_player_id and d == day_ymd)

    async def insert_private_daily_target(
        self, sender_player_id: int, day_ymd: int, target_player_id: int, accepted_at: str
    ) -> None:
        self._d.private_targets.add((sender_player_id, day_ymd, target_player_id))

    async def insert_send_authority(
        self,
        player_id: int,
        request_id: bytes,
        owner_account: bytes,
        owner_revision: int,
        request_digest: bytes,
        request_payload: bytes,
        message_payload: bytes,
        stream_key_bytes: bytes,
        accepted_ms: int,
        accepted_at: str,
    ) -> bool:
        key = (player_id, request_id)
        if key in self._d.send_rows:
            return False
        self._d.send_rows[key] = ReceiptRow(
            account=owner_account,
            revision=owner_revision,
            digest=request_digest,
            request=request_payload,
            message=message_payload,
            stream=stream_key_bytes,
            state=ACCEPTED_STATE,
            accepted_ms=accepted_ms,
            tip=0,
        )
        return True

    async def read_history_rows(self, stream_key_bytes: bytes, limit: int) -> list[HistoryRow]:
        rows = [
            HistoryRow(sender=pid, request_id=req_id, payload=row.message, accepted_ms=row.accepted_ms)
            for (pid, req_id), row in self._d.send_rows.items()
            if row.stream == stream_key_bytes and row.state == ACCEPTED_STATE
        ]
        return newest_rows(rows, limit)

    async def load_retracted_world_keys(self, min_ms: int) -> set[tuple[int, int]]:
        return {(sender, ms) for (sender, ms) in self._d.retracted if ms >= min_ms}

    async def message_exists(self, player_id: int, accepted_ms: int, stream_key_bytes: bytes) -> bool:
        return any(
            pid == player_id
            and row.state == ACCEPTED_STATE
            and row.accepted_ms == accepted_ms
            and row.stream == stream_key_bytes
            for (pid, _), row in self._d.send_rows.items()
        )

    async def insert_world_retraction(
        self, sender_player_id: int, accepted_ms: int, retracted_ms: int, reason: bytes, accepted_at: str
    ) -> bool:
        key = (sender_player_id, accepted_ms)
        if key in self._d.retracted:
            return False
        self._d.retracted[key] = (retracted_ms, reason)
        return True

    async def insert_report(
        self,
        reporter_player_id: int,
        channel: int,
        target_sender_player_id: int,
        send_time_ms: int,
        reason: int,
        detail: bytes,
        reported_ms: int,
        accepted_at: str,
    ) -> None:
        key = (reporter_player_id, channel, target_sender_player_id, send_time_ms)
        if key not in self._d.reports:
            self._d.reports[key] = (reason, detail, reported_ms)


class _MemoryDriver:
    """确定性内存事务驱动：通过快照/回滚与 SqlChatStore 保持一致的原子语义。"""

    def __init__(self, clock_ms: Callable[[], int]) -> None:
        self.clock_ms = clock_ms
        self._lock = asyncio.Lock()
        self.send_rows: dict[tuple[int, bytes], ReceiptRow] = {}
        self.retracted: dict[tuple[int, int], tuple[int, bytes]] = {}
        self.reports: dict[tuple[int, int, int, int], tuple[int, bytes, int]] = {}
        self.repeat_events: set[tuple[bytes, int, int]] = set()
        self.private_targets: set[tuple[int, int, int]] = set()
        self.policy_digest: bytes | None = None

    async def run_tx(self, work: Callable[[ChatTx], Awaitable[T]]) -> T:
        async with self._lock:
            snap_send = self.send_rows.copy()
            snap_retracted = self.retracted.copy()
            snap_reports = self.reports.copy()
            snap_repeat = self.repeat_events.copy()
            snap_private = self.private_targets.copy()
            try:
                return await work(_MemoryTx(self))
            except _TxRollbackReturn as ret:
                self.send_rows = snap_send
                self.retracted = snap_retracted
                self.reports = snap_reports
                self.repeat_events = snap_repeat
                self.private_targets = snap_private
                return ret.value
            except BaseException:
                self.send_rows = snap_send
                self.retracted = snap_retracted
                self.reports = snap_reports
                self.repeat_events = snap_repeat
                self.private_targets = snap_private
                raise

    async def read_receipt_direct(self, player_id: int, request_id: bytes) -> ReceiptRow | None:
        row = self.send_rows.get((player_id, request_id))
        return validate_receipt(row) if row is not None else None


class MemoryChatStore(ChatStoreCore):
    """用于单测的确定性内存存储双胞胎，与 SqlChatStore 共享全部核心判定。"""

    def __init__(
        self,
        protocol: Protocol,
        actor_verifier: ActorVerifier,
        rules: ChatRules | None = None,
        *,
        reviewer: ContentReviewer | None = None,
        profile_reader: ProfileReader | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._mem = _MemoryDriver(clock_ms)
        super().__init__(
            protocol,
            self._mem,
            actor_verifier,
            rules or ChatRules(),
            reviewer=reviewer,
            profile_reader=profile_reader,
            monotonic_clock=monotonic_clock,
        )

    @property
    def reports(self) -> dict[tuple[int, int, int, int], tuple[int, bytes, int]]:
        return self._mem.reports

    @property
    def retracted(self) -> dict[tuple[int, int], tuple[int, bytes]]:
        return self._mem.retracted

    @property
    def send_rows(self) -> dict[tuple[int, bytes], ReceiptRow]:
        return self._mem.send_rows

    async def ensure_policy(self) -> None:
        digest = policy_hash(self._rules)
        if self._mem.policy_digest is None:
            self._mem.policy_digest = digest
            return
        if self._mem.policy_digest != digest:
            raise PolicyMismatchError("chat policy hash mismatch")
