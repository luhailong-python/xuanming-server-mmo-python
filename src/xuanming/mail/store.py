"""mail 核心存储引擎（MailStoreCore）、外部依赖适配与确定性内存双胞胎（MemoryMailStore）。"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import replace
from typing import Protocol as TypingProtocol, TypeVar

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import service_digest, sign_service_call
from ..protocol import Protocol
from .errors import (
    AssetOpLeaseLostError,
    AssetOpPoisonRowError,
    ERR_GUILD_NOT_IN_GUILD,
    ERR_MAIL_CLAIM_MANUAL,
    ERR_MAIL_CLAIM_PENDING,
    InvalidRoleFenceStateError,
    MailAttachmentError,
    MailBoxFullError,
    MailConflictError,
    MailDeniedError,
    MailError,
    MailExpiredError,
    MailInvalidError,
    MailNotFoundError,
    MailProtectedError,
    MailRecipientDeletedError,
    MailTooManyPendingError,
    RoleFenceStaleError,
    RoleFenceUnavailableError,
    RoleNotFoundError,
    StoreUnavailableError,
)
from .model import (
    ASSET_OP_MAX_PENDING,
    ASSET_OP_MAX_SPAN,
    ASSET_STREAM_SYSTEM_CREDIT,
    CAMPAIGN_IMPORTING,
    CAMPAIGN_SEALED,
    CLAIM_CLAIMED,
    CLAIM_MANUAL,
    CLAIM_NONE,
    CLAIM_PENDING,
    CLAIM_UNCLAIMED,
    FRESH_ATTEMPT_LIMIT,
    LIST_DEFAULT_LIMIT,
    LIST_MAX_LIMIT,
    MAIL_KIND_GUILD,
    MAIL_KIND_PERSONAL,
    MAIL_KIND_SYSTEM,
    MAIL_KIND_UNSPECIFIED,
    MAX_CLEAR_READ_SCAN,
    MAX_DELETE_BATCH,
    MS_PER_DAY,
    MailAssetOpRow,
    MailCampaignRow,
    MailCampaignShardRow,
    MailLimits,
    MailRow,
    MailStateRow,
    OP_ABANDONED,
    OP_APPLIED,
    OP_MANUAL,
    OP_PENDING,
    OP_REJECTED,
    OP_TERMINAL_STATUSES,
    PendingAssetOp,
    ROLE_BLOCKER_ASSET_OP,
    ROLE_FENCE_CALLER,
    RoleFenceRow,
    RoleOriginal,
    SweepReport,
    SweepStats,
    TX_MAIL_ATTACHMENT,
    UINT64_MAX,
    build_asset_bundle,
    build_mail_view,
    check_role_fence,
    effective_claim_state,
    has_attachments,
    is_protected_claim_state,
    manifest_hash,
    sender_kind_for,
    shard_hash,
    valid_manifest,
    valid_role_fence_state,
    validate_send_content,
)

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_GUILD_SERVICE_PREFIX = "/guildpb.GuildService/"
_GUILD_LOOKUP_TIMEOUT_SEC = 0.3
_ROLE_FENCE_TIMEOUT_SEC = 2.0
_ID_ALLOC_TIMEOUT_SEC = 2.0

# AssetOp 终结语义枚举（对应 Go assetop.Status）
FINAL_APPLIED = "applied"
FINAL_REJECTED = "rejected"
FINAL_ABORTED = "aborted"
FINAL_APPLIED_PARTIAL = "applied_partial"

T = TypeVar("T")


class _TxRollbackReturn(Exception):
    """在事务块内显式回滚并返回业务结果的内部控制流信号。"""

    def __init__(self, value) -> None:
        super().__init__("rollback_return")
        self.value = value


class _DuplicateKeyError(Exception):
    """唯一键或主键冲突（对应 MySQL 1062）。"""


class GuildDirectory(TypingProtocol):
    """查询玩家当前所在帮会的协议（0 表示未加入帮会）。"""

    async def guild_of(self, player_id: int) -> int: ...


class RoleFenceDirectory(TypingProtocol):
    """查询 data_service 权威角色护栏状态的协议。"""

    async def get_role_fence_state(self, player_id: int) -> Message | None: ...


class MailNotifier(TypingProtocol):
    """个人邮件提交后的可丢失提醒协议。"""

    async def personal(self, player_id: int, mail_id: int) -> None: ...


class NullGuildDirectory:
    """无帮会目录实现：所有玩家均视作无帮会。"""

    async def guild_of(self, player_id: int) -> int:
        return 0


class GrpcGuildDirectory:
    """经 guildpb.GuildService/GetPlayerGuild 查询玩家所在帮会（不带客户端会话，按内部只读调用）。"""

    def __init__(self, protocol: Protocol, call: Callable[..., Awaitable[bytes]]) -> None:
        self._protocol = protocol
        self._call = call

    async def guild_of(self, player_id: int) -> int:
        req = self._protocol.message("guildpb.GetPlayerGuildRequest", player_id=player_id)
        raw = await self._call(
            _GUILD_SERVICE_PREFIX + "GetPlayerGuild",
            req.SerializeToString(deterministic=True),
            timeout=_GUILD_LOOKUP_TIMEOUT_SEC,
        )
        resp = self._protocol.parse("guildpb.GetPlayerGuildResponse", raw)
        tip = int(resp.error_message.id)
        if tip == 0:
            return int(resp.guild.guild_id)
        if tip == ERR_GUILD_NOT_IN_GUILD:
            return 0
        raise StoreUnavailableError(f"guild.GetPlayerGuild rejected tip={tip}")


class GrpcRoleFenceDirectory:
    """经 data_service.DataService/GetRoleFenceState 读取权威护栏状态（caller="mail"）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        role_fence_secret: bytes,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._secret = role_fence_secret
        self._clock_ms = clock_ms

    async def get_role_fence_state(self, player_id: int) -> Message | None:
        path = _DATA_SERVICE_PREFIX + "GetRoleFenceState"
        req = self._protocol.message("data_service.GetRoleFenceStateRequest", player_id=player_id)
        sign_service_call(path, req, ROLE_FENCE_CALLER, self._clock_ms(), self._secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=_ROLE_FENCE_TIMEOUT_SEC)
        except Exception as exc:
            raise RoleFenceUnavailableError(f"GetRoleFenceState failed player={player_id}: {exc}") from exc
        resp = self._protocol.parse("data_service.GetRoleFenceStateResponse", raw)
        if resp.retry:
            raise RoleFenceUnavailableError(f"GetRoleFenceState requested retry player={player_id}")
        if not resp.found:
            return None
        state = resp.state
        if int(state.player_id) != player_id or not valid_role_fence_state(state):
            raise RoleFenceUnavailableError(f"GetRoleFenceState returned invalid state player={player_id}")
        return state


class IdSegmentAllocator:
    """经 data_service.DataService/AllocateIdSegment 本地缓段发号（mail / mail_asset_op）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        biz_tag: str,
        *,
        step: int = 1000,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._biz_tag = biz_tag
        self._step = step
        self._lock = asyncio.Lock()
        self._next = 0
        self._hi = 0

    async def __call__(self) -> int:
        async with self._lock:
            if self._next < self._hi:
                value = self._next
                self._next += 1
                return value
            req = self._protocol.message("data_service.AllocateIdSegmentRequest", biz_tag=self._biz_tag, step=self._step)
            try:
                raw = await self._call(
                    _DATA_SERVICE_PREFIX + "AllocateIdSegment",
                    req.SerializeToString(deterministic=True),
                    timeout=_ID_ALLOC_TIMEOUT_SEC,
                )
            except Exception as exc:
                raise StoreUnavailableError(f"AllocateIdSegment failed tag={self._biz_tag}: {exc}") from exc
            resp = self._protocol.parse("data_service.AllocateIdSegmentResponse", raw)
            if resp.error_code != 0 or not 0 < resp.lo < resp.hi:
                raise StoreUnavailableError(
                    f"AllocateIdSegment rejected tag={self._biz_tag} code={resp.error_code} lo={resp.lo} hi={resp.hi}"
                )
            self._next = int(resp.lo) + 1
            self._hi = int(resp.hi)
            return int(resp.lo)


class MailTx(TypingProtocol):
    """单事务内底层存储原语协议（SqlDriver 与 MemoryDriver 共享同一接口）。"""

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None: ...
    async def apply_role_fence(self, state: Message, now_ms: int) -> bool: ...
    async def has_pending_asset_op(self, player_id: int) -> bool: ...
    async def purge_player_rows(self, player_id: int) -> None: ...

    async def ensure_mail_box(self, player_id: int, now_ms: int) -> None: ...
    async def lock_mail_box(self, player_id: int) -> None: ...

    async def get_message_by_request(self, sender_kind: int, request_id: int) -> MailRow | None: ...
    async def get_message_by_id(self, mail_id: int) -> MailRow | None: ...
    async def get_visible_message(self, mail_id: int, player_id: int, guild_id: int) -> MailRow | None: ...
    async def count_personal_active_mails(self, player_id: int, now_ms: int) -> int: ...
    async def count_shared_active_mails(self, guild_id: int, now_ms: int) -> int: ...
    async def insert_message(self, row: MailRow) -> None: ...
    async def publish_campaign_message(self, mail_id: int, kind: int, sent_ms: int) -> None: ...

    async def get_campaign_by_request(self, sender_kind: int, request_id: int) -> MailCampaignRow | None: ...
    async def lock_campaign(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> MailCampaignRow | None: ...
    async def insert_campaign(self, row: MailCampaignRow) -> None: ...
    async def mark_campaign_sealed(
        self, sender_kind: int, campaign_id: int, manifest_generation: int, sealed_ms: int
    ) -> None: ...

    async def get_campaign_shard(
        self, sender_kind: int, campaign_id: int, manifest_generation: int, shard_index: int
    ) -> MailCampaignShardRow | None: ...
    async def sum_campaign_shard_recipients(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> int: ...
    async def list_campaign_shards(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> list[MailCampaignShardRow]: ...
    async def insert_campaign_recipients(
        self,
        sender_kind: int,
        campaign_id: int,
        manifest_generation: int,
        shard_index: int,
        player_ids: Sequence[int],
    ) -> None: ...
    async def insert_campaign_shard(self, row: MailCampaignShardRow) -> None: ...
    async def count_campaign_recipients(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> int: ...

    async def lock_mail_state(self, player_id: int, mail_id: int) -> MailStateRow | None: ...
    async def insert_mail_state(self, row: MailStateRow) -> None: ...
    async def mark_mail_read(self, player_id: int, mail_id: int, now_ms: int) -> MailStateRow: ...
    async def mark_mail_deleted(self, player_id: int, mail_id: int, now_ms: int) -> None: ...
    async def list_clearable_read_mail_ids(self, player_id: int, guild_id: int, limit: int) -> list[int]: ...
    async def list_visible_counts(self, player_id: int, guild_id: int, now_ms: int) -> tuple[int, int]: ...
    async def list_visible_page(
        self,
        player_id: int,
        guild_id: int,
        now_ms: int,
        before_sent_ms: int | None,
        before_mail_id: int | None,
        limit: int,
    ) -> list[tuple[MailRow, MailStateRow | None]]: ...

    async def ensure_seq_row(self, player_id: int, stream: int, now_ms: int) -> None: ...
    async def allocate_seq(
        self, player_id: int, stream: int, max_pending: int, max_span: int, now_ms: int
    ) -> tuple[int, int]: ...
    async def insert_asset_op(self, row: MailAssetOpRow) -> None: ...
    async def cas_claim_state_pending(
        self, player_id: int, mail_id: int, op_id: int, new_version: int, old_version: int, now_ms: int
    ) -> bool: ...

    async def get_asset_op_mail_id(self, op_id: int) -> int | None: ...
    async def lock_asset_op(self, op_id: int) -> MailAssetOpRow | None: ...
    async def update_asset_op_reschedule(
        self,
        op_id: int,
        next_attempt_ms: int,
        durable: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
        last_send_ms: int,
    ) -> None: ...
    async def update_asset_op_final(
        self,
        op_id: int,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
        reason_tip_id: int,
    ) -> None: ...
    async def update_mail_state_final(
        self,
        player_id: int,
        mail_id: int,
        op_id: int,
        claim_version: int,
        new_claim_state: int,
        reason_tip_id: int,
        now_ms: int,
    ) -> None: ...

    async def sweep_one_mail(self, mail_id: int, cutoff_ms: int, remaining: int) -> tuple[int, int]: ...


class MailDriver(TypingProtocol):
    """底层存储驱动协议（事务 + 事务外只读/预检操作）。"""

    async def run_tx(self, work: Callable[[MailTx], Awaitable[T]]) -> T: ...
    async def run_rr_read_tx(self, work: Callable[[MailTx], Awaitable[T]]) -> T: ...
    async def read_role_fence(self, player_id: int) -> RoleFenceRow | None: ...
    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool: ...
    async def ensure_mail_box_direct(self, player_id: int, now_ms: int) -> None: ...
    async def get_message_by_request_direct(self, sender_kind: int, request_id: int) -> MailRow | None: ...
    async def get_campaign_direct(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> MailCampaignRow | None: ...
    async def list_due_ops(
        self, now_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> list[int]: ...
    async def claim_op_direct(
        self, op_id: int, now_ms: int, lease_until_ms: int, lease_token: int
    ) -> MailAssetOpRow | None: ...
    async def list_sweep_candidates(
        self, cutoff_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> tuple[list[tuple[int, int]], int]: ...


class MailStoreCore:
    """mail 领域全部业务规则与事务编排（由 SqlMailStore 与 MemoryMailStore 共用）。"""

    def __init__(
        self,
        protocol: Protocol,
        driver: MailDriver,
        *,
        mail_ids: Callable[[], Awaitable[int]],
        op_ids: Callable[[], Awaitable[int]],
        item_exists: Callable[[int], bool],
        guilds: GuildDirectory | None = None,
        role_directory: RoleFenceDirectory | None = None,
        limits: MailLimits = MailLimits(),
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        limits.validate()
        self.protocol = protocol
        self._driver = driver
        self._mail_ids = mail_ids
        self._op_ids = op_ids
        self._item_exists = item_exists
        self._guilds: GuildDirectory = guilds if guilds is not None else NullGuildDirectory()
        self._role_directory = role_directory
        self._limits = limits
        self._clock_ms = clock_ms

    # -- 角色护栏自愈与锁 ----------------------------------------------------------------------------

    async def _heal_role_fence(self, player_id: int) -> None:
        if self._role_directory is None:
            raise RoleFenceUnavailableError("role fence directory not configured")
        state = await self._role_directory.get_role_fence_state(player_id)
        if state is None:
            raise RoleNotFoundError(f"role not found in directory player={player_id}")
        if int(state.player_id) != player_id or not valid_role_fence_state(state):
            raise RoleFenceUnavailableError(f"invalid role fence state from directory player={player_id}")
        await self._driver.apply_role_fence_direct(state, self._clock_ms())

    async def _run_guarded(self, original: RoleOriginal, action: Callable[[], Awaitable[T]]) -> T:
        if self._role_directory is None:
            raise MailDeniedError("role fence guard not configured")
        try:
            return await action()
        except RoleFenceStaleError as stale:
            await self._heal_role_fence(stale.player_id)
        try:
            return await action()
        except RoleFenceStaleError:
            raise MailDeniedError("role fence check denied after heal")

    async def _lock_actor(self, tx: MailTx, original: RoleOriginal) -> None:
        fence = await tx.lock_role_fence(original.player_id)
        verdict = check_role_fence(fence, original)
        if verdict == "stale":
            raise RoleFenceStaleError(original.player_id)
        if verdict != "ok":
            raise MailDeniedError(f"role fence denied player={original.player_id}")

    async def _guild_of(self, player_id: int) -> int:
        try:
            return int(await self._guilds.guild_of(player_id))
        except Exception as exc:
            logger.error("mail guild lookup failed, hiding guild text mails player=%d err=%s", player_id, exc)
            return 0

    # -- 发件与活动导入/封存 --------------------------------------------------------------------------

    async def send(self, caller: str, kind: int, request: Message) -> Message:
        validate_send_content(request)
        expected_sender = sender_kind_for(caller)
        if expected_sender == 0 or expected_sender != int(request.sender_kind):
            raise MailDeniedError(f"sender_kind mismatch caller={caller}")

        digest = service_digest(request)

        # 先判重：曾成功的请求不会因满箱、配置变化或过期而被改判。
        old = await self._driver.get_message_by_request_direct(int(request.sender_kind), int(request.request_id))
        if old is not None:
            actual_kind = old.kind
            if actual_kind == MAIL_KIND_UNSPECIFIED:
                camp = await self._driver.get_campaign_direct(
                    old.sender_kind, old.campaign_id, old.manifest_generation
                )
                if camp is None:
                    raise MailNotFoundError("campaign missing for placeholder mail")
                actual_kind = camp.kind
            if old.request_hash != digest or actual_kind != kind:
                raise MailConflictError("idempotency request hash or kind conflict")
            return self.protocol.message("mailpb.SendMailResponse", mail_id=old.mail_id, duplicated=True)

        attachments_msg = request.attachments if request.HasField("attachments") else None
        payload = build_asset_bundle(self.protocol, attachments_msg, self._item_exists)
        now = self._clock_ms()
        ttl_days = (
            self._limits.max_personal_mail_ttl_days
            if kind == MAIL_KIND_PERSONAL
            else self._limits.max_system_mail_ttl_days
        )
        ttl_ms = ttl_days * MS_PER_DAY
        expire = int(request.expire_ms) or (now + ttl_ms)
        if expire <= now or expire - now > ttl_ms:
            raise MailInvalidError("mail expire_ms out of allowed TTL window")

        if kind in (MAIL_KIND_SYSTEM, MAIL_KIND_GUILD) and has_attachments(payload):
            return await self._create_campaign(caller, kind, request, digest, payload, expire)

        if (
            request.campaign_id != 0
            or request.manifest_generation != 0
            or request.expected_recipient_count != 0
            or request.expected_shard_count != 0
            or request.manifest_hash != ""
        ):
            raise MailInvalidError("non-campaign mail must not set campaign manifest fields")

        if kind == MAIL_KIND_PERSONAL:
            if request.recipient_player_id == 0 or request.guild_id != 0:
                raise MailInvalidError("personal mail requires recipient_player_id and guild_id=0")
        elif kind == MAIL_KIND_SYSTEM:
            if caller != "admin" or request.recipient_player_id != 0 or request.guild_id != 0:
                raise MailDeniedError("system mail requires caller=admin and recipient=0, guild=0")
        elif kind == MAIL_KIND_GUILD:
            if caller != "guild" or request.guild_id == 0 or request.recipient_player_id != 0 or has_attachments(payload):
                raise MailDeniedError("guild text mail requires caller=guild, guild_id!=0, no attachments")
        else:
            raise MailInvalidError(f"unsupported mail kind={kind}")

        mail_id = await self._mail_ids()
        personal = kind == MAIL_KIND_PERSONAL
        recipient_id = int(request.recipient_player_id)
        guild_id = int(request.guild_id)
        if personal:
            fence = await self._driver.read_role_fence(recipient_id)
            if fence is not None and fence.account == "":
                raise MailRecipientDeletedError(f"recipient deleted player={recipient_id}")
            await self._driver.ensure_mail_box_direct(recipient_id, now)

        async def work(tx: MailTx) -> tuple[int, bool]:
            if personal:
                fence = await tx.lock_role_fence(recipient_id)
                if fence is not None and fence.account == "":
                    raise MailRecipientDeletedError(f"recipient deleted player={recipient_id}")
                await tx.lock_mail_box(recipient_id)

            async def check_replay() -> tuple[int, bool] | None:
                prev = await tx.get_message_by_request(int(request.sender_kind), int(request.request_id))
                if prev is None:
                    return None
                if prev.request_hash != digest or prev.kind != kind:
                    raise MailConflictError("idempotency conflict inside send tx")
                return prev.mail_id, True

            replayed = await check_replay()
            if replayed is not None:
                return replayed

            if personal:
                active = await tx.count_personal_active_mails(recipient_id, now)
                if active >= self._limits.max_personal_mails_per_player:
                    raise MailBoxFullError("recipient personal mailbox full")
            else:
                await self._check_shared_capacity(tx, guild_id, now)

            row = MailRow(
                mail_id=mail_id,
                owner_player_id=recipient_id,
                guild_id=guild_id,
                kind=kind,
                sender_kind=int(request.sender_kind),
                request_id=int(request.request_id),
                request_hash=digest,
                title=request.title,
                body=request.body,
                attachments=payload,
                sent_ms=now,
                expire_ms=expire,
            )
            try:
                await tx.insert_message(row)
            except _DuplicateKeyError:
                replayed = await check_replay()
                if replayed is None:
                    raise MailConflictError("duplicate mail key conflict")
                return replayed

            if personal:
                claim_state = CLAIM_UNCLAIMED if has_attachments(payload) else CLAIM_NONE
                await tx.insert_mail_state(
                    MailStateRow(
                        player_id=recipient_id,
                        mail_id=mail_id,
                        claim_state=claim_state,
                        updated_ms=now,
                    )
                )
            return mail_id, False

        out_id, duplicated = await self._driver.run_tx(work)
        return self.protocol.message("mailpb.SendMailResponse", mail_id=out_id, duplicated=duplicated)

    async def _check_shared_capacity(self, tx: MailTx, guild_id: int, now_ms: int) -> None:
        count = await tx.count_shared_active_mails(guild_id, now_ms)
        limit = (
            self._limits.max_active_guild_mails_per_guild
            if guild_id != 0
            else self._limits.max_active_system_mails
        )
        if count >= limit:
            raise MailBoxFullError("shared mail capacity exceeded")

    async def _create_campaign(
        self,
        caller: str,
        kind: int,
        request: Message,
        digest: str,
        payload: bytes,
        expire_ms: int,
    ) -> Message:
        valid_channel = (
            (kind == MAIL_KIND_SYSTEM and caller == "admin" and request.guild_id == 0)
            or (kind == MAIL_KIND_GUILD and caller == "guild" and request.guild_id != 0)
        )
        if not valid_manifest(request) or not valid_channel:
            raise MailInvalidError("invalid campaign manifest or channel")

        mail_id = await self._mail_ids()
        now = self._clock_ms()

        async def work(tx: MailTx) -> tuple[int, bool]:
            async def check_replay() -> tuple[int, bool] | None:
                prev = await tx.get_campaign_by_request(int(request.sender_kind), int(request.request_id))
                if prev is None:
                    return None
                if prev.request_hash != digest or prev.producer != caller:
                    raise MailConflictError("campaign idempotency conflict")
                return prev.mail_id, True

            replayed = await check_replay()
            if replayed is not None:
                return replayed

            await self._check_shared_capacity(tx, int(request.guild_id), now)

            # kind=0 仅保留跨频道发件幂等键；可见查询明确排除它。
            placeholder = MailRow(
                mail_id=mail_id,
                owner_player_id=0,
                guild_id=int(request.guild_id),
                kind=MAIL_KIND_UNSPECIFIED,
                sender_kind=int(request.sender_kind),
                request_id=int(request.request_id),
                request_hash=digest,
                title=request.title,
                body=request.body,
                attachments=payload,
                sent_ms=now,
                expire_ms=expire_ms,
                campaign_id=int(request.campaign_id),
                manifest_generation=int(request.manifest_generation),
            )
            try:
                await tx.insert_message(placeholder)
            except _DuplicateKeyError:
                replayed = await check_replay()
                if replayed is None:
                    raise MailConflictError("duplicate request key held by non-campaign mail")
                return replayed

            camp = MailCampaignRow(
                sender_kind=int(request.sender_kind),
                campaign_id=int(request.campaign_id),
                manifest_generation=int(request.manifest_generation),
                mail_id=mail_id,
                request_id=int(request.request_id),
                producer=caller,
                request_hash=digest,
                title=request.title,
                body=request.body,
                attachments=payload,
                expire_ms=expire_ms,
                expected_recipient_count=int(request.expected_recipient_count),
                expected_shard_count=int(request.expected_shard_count),
                manifest_hash=request.manifest_hash,
                state=CAMPAIGN_IMPORTING,
                created_ms=now,
                sealed_ms=0,
                kind=kind,
                guild_id=int(request.guild_id),
            )
            try:
                await tx.insert_campaign(camp)
            except _DuplicateKeyError:
                raise MailConflictError("campaign key already exists")
            return mail_id, False

        out_id, duplicated = await self._driver.run_tx(work)
        return self.protocol.message("mailpb.SendMailResponse", mail_id=out_id, duplicated=duplicated)

    async def import_recipients(self, caller: str, request: Message) -> Message:
        if (
            request is None
            or int(request.sender_kind) != sender_kind_for(caller)
            or request.campaign_id == 0
            or request.manifest_generation == 0
        ):
            raise MailInvalidError("invalid import mail recipients request")
        digest, ids = shard_hash(list(request.player_ids))

        async def work(tx: MailTx) -> tuple[int, bool]:
            camp = await tx.lock_campaign(
                int(request.sender_kind), int(request.campaign_id), int(request.manifest_generation)
            )
            if camp is None:
                raise MailNotFoundError("campaign not found")
            if camp.producer != caller:
                raise MailDeniedError("campaign producer mismatch")
            if int(request.shard_index) >= camp.expected_shard_count:
                raise MailInvalidError("shard_index out of bounds")

            existing = await tx.get_campaign_shard(
                int(request.sender_kind),
                int(request.campaign_id),
                int(request.manifest_generation),
                int(request.shard_index),
            )
            if existing is not None:
                if existing.shard_hash != digest or existing.recipient_count != len(ids):
                    raise MailConflictError("campaign shard conflict")
                return existing.recipient_count, True

            if camp.state != CAMPAIGN_IMPORTING:
                raise MailConflictError("campaign is not importing")

            imported = await tx.sum_campaign_shard_recipients(
                int(request.sender_kind), int(request.campaign_id), int(request.manifest_generation)
            )
            if imported + len(ids) > camp.expected_recipient_count:
                raise MailInvalidError("imported recipients exceed expected_recipient_count")

            try:
                await tx.insert_campaign_recipients(
                    int(request.sender_kind),
                    int(request.campaign_id),
                    int(request.manifest_generation),
                    int(request.shard_index),
                    ids,
                )
            except _DuplicateKeyError:
                raise MailConflictError("duplicate recipient across campaign shards")

            await tx.insert_campaign_shard(
                MailCampaignShardRow(
                    sender_kind=int(request.sender_kind),
                    campaign_id=int(request.campaign_id),
                    manifest_generation=int(request.manifest_generation),
                    shard_index=int(request.shard_index),
                    shard_hash=digest,
                    recipient_count=len(ids),
                )
            )
            return len(ids), False

        count, duplicated = await self._driver.run_tx(work)
        return self.protocol.message(
            "mailpb.ImportMailRecipientsResponse", imported_count=count, duplicated=duplicated
        )

    async def seal_campaign(self, caller: str, request: Message) -> Message:
        if (
            request is None
            or int(request.sender_kind) != sender_kind_for(caller)
            or request.campaign_id == 0
            or request.manifest_generation == 0
        ):
            raise MailInvalidError("invalid seal campaign request")

        async def work(tx: MailTx) -> tuple[int, bool]:
            camp = await tx.lock_campaign(
                int(request.sender_kind), int(request.campaign_id), int(request.manifest_generation)
            )
            if camp is None:
                raise MailNotFoundError("campaign not found")
            if camp.producer != caller:
                raise MailDeniedError("campaign producer mismatch")
            if camp.state == CAMPAIGN_SEALED:
                return camp.mail_id, True
            now = self._clock_ms()
            if camp.expire_ms <= now:
                raise MailExpiredError("campaign expired before seal")

            shards = await tx.list_campaign_shards(camp.sender_kind, camp.campaign_id, camp.manifest_generation)
            if len(shards) != camp.expected_shard_count:
                raise MailConflictError("campaign shard count mismatch")

            tuples: list[tuple[int, str, int]] = []
            total_recipients = 0
            for expected_idx, shard in enumerate(shards):
                if shard.shard_index != expected_idx:
                    raise MailConflictError("campaign shard index gap")
                tuples.append((shard.shard_index, shard.shard_hash, shard.recipient_count))
                total_recipients += shard.recipient_count

            if total_recipients != camp.expected_recipient_count or manifest_hash(tuples) != camp.manifest_hash:
                raise MailConflictError("campaign manifest hash or recipient count mismatch")

            actual_rows = await tx.count_campaign_recipients(
                camp.sender_kind, camp.campaign_id, camp.manifest_generation
            )
            if actual_rows != total_recipients:
                raise MailConflictError("campaign recipient row count mismatch")

            await tx.publish_campaign_message(camp.mail_id, camp.kind, now)
            await tx.mark_campaign_sealed(camp.sender_kind, camp.campaign_id, camp.manifest_generation, now)
            return camp.mail_id, False

        mail_id, duplicated = await self._driver.run_tx(work)
        return self.protocol.message("mailpb.SendMailResponse", mail_id=mail_id, duplicated=duplicated)

    # -- 玩家邮箱读/删/领取 --------------------------------------------------------------------------

    async def _lock_box(self, tx: MailTx, player_id: int, now_ms: int) -> None:
        if player_id == 0:
            raise MailInvalidError("player_id=0")
        await tx.ensure_mail_box(player_id, now_ms)
        await tx.lock_mail_box(player_id)

    async def _materialize(
        self, tx: MailTx, player_id: int, guild_id: int, mail_id: int, now_ms: int
    ) -> tuple[MailRow, MailStateRow]:
        mail = await tx.get_visible_message(mail_id, player_id, guild_id)
        if mail is None:
            raise MailNotFoundError(f"mail not found id={mail_id}")
        state = await tx.lock_mail_state(player_id, mail_id)
        if state is None:
            if mail.expire_ms <= now_ms:
                raise MailExpiredError(f"mail expired id={mail_id}")
            init_claim = CLAIM_UNCLAIMED if has_attachments(mail.attachments) else CLAIM_NONE
            state = MailStateRow(
                player_id=player_id,
                mail_id=mail_id,
                claim_state=init_claim,
                updated_ms=now_ms,
            )
            await tx.insert_mail_state(state)
        if state.deleted_ms != 0:
            raise MailNotFoundError(f"mail deleted id={mail_id}")
        return mail, state

    async def list_mails(self, original: RoleOriginal, request: Message) -> Message:
        if original.player_id == 0 or request is None:
            raise MailInvalidError("invalid list mails request")
        limit = int(request.limit) or LIST_DEFAULT_LIMIT
        if limit > LIST_MAX_LIMIT:
            limit = LIST_MAX_LIMIT
        before_mail_id = int(request.before_mail_id)
        guild_id = await self._guild_of(original.player_id)

        async def once() -> Message:
            now = self._clock_ms()

            async def work(tx: MailTx) -> Message:
                await self._lock_actor(tx, original)
                unread, claimable = await tx.list_visible_counts(original.player_id, guild_id, now)
                resp = self.protocol.message(
                    "mailpb.ListMailsResponse",
                    unread_count=unread,
                    claimable_count=claimable,
                    server_now_ms=now,
                )
                before_sent_ms: int | None = None
                cursor_mail_id: int | None = None
                if before_mail_id != 0:
                    cursor_row = await tx.get_message_by_id(before_mail_id)
                    if cursor_row is None:
                        return resp
                    before_sent_ms = cursor_row.sent_ms
                    cursor_mail_id = before_mail_id

                rows = await tx.list_visible_page(
                    original.player_id, guild_id, now, before_sent_ms, cursor_mail_id, limit + 1
                )
                has_more = len(rows) > limit
                page = rows[:limit]
                for mail_row, state_row in page:
                    resp.mails.add().CopyFrom(build_mail_view(self.protocol, mail_row, state_row))
                if has_more and page:
                    resp.next_before_mail_id = page[-1][0].mail_id
                return resp

            return await self._driver.run_rr_read_tx(work)

        return await self._run_guarded(original, once)

    async def read_mail(self, original: RoleOriginal, mail_id: int) -> Message:
        if original.player_id == 0 or mail_id == 0:
            raise MailInvalidError("invalid read mail request")
        guild_id = await self._guild_of(original.player_id)

        async def once() -> Message:
            now = self._clock_ms()

            async def work(tx: MailTx) -> Message:
                await self._lock_actor(tx, original)
                await self._lock_box(tx, original.player_id, now)
                mail, state = await self._materialize(tx, original.player_id, guild_id, mail_id, now)
                if mail.expire_ms <= now and not is_protected_claim_state(state.claim_state):
                    raise MailExpiredError(f"mail expired id={mail_id}")
                updated = await tx.mark_mail_read(original.player_id, mail_id, now)
                return build_mail_view(self.protocol, mail, updated)

            return await self._driver.run_tx(work)

        return await self._run_guarded(original, once)

    async def delete_mails(
        self, original: RoleOriginal, mail_ids: Sequence[int], *, clear_read: bool
    ) -> tuple[list[int], list[int]]:
        if original.player_id == 0 or (not clear_read and (not mail_ids or len(mail_ids) > MAX_DELETE_BATCH)):
            raise MailInvalidError("invalid delete mails request")
        guild_id = await self._guild_of(original.player_id)

        async def once() -> tuple[list[int], list[int]]:
            now = self._clock_ms()

            async def work(tx: MailTx) -> tuple[list[int], list[int]]:
                await self._lock_actor(tx, original)
                await self._lock_box(tx, original.player_id, now)
                if clear_read:
                    targets = await tx.list_clearable_read_mail_ids(
                        original.player_id, guild_id, MAX_CLEAR_READ_SCAN
                    )
                else:
                    targets = [int(mid) for mid in mail_ids]

                deleted: list[int] = []
                protected: list[int] = []
                seen: set[int] = set()
                for mid in targets:
                    if mid == 0 or mid in seen:
                        continue
                    seen.add(mid)
                    try:
                        mail, state = await self._materialize(tx, original.player_id, guild_id, mid, now)
                    except (MailNotFoundError, MailExpiredError):
                        continue
                    if is_protected_claim_state(state.claim_state) or (
                        state.claim_state == CLAIM_UNCLAIMED and mail.expire_ms > now
                    ):
                        protected.append(mid)
                        continue
                    await tx.mark_mail_deleted(original.player_id, mid, now)
                    deleted.append(mid)
                return deleted, protected

            return await self._driver.run_tx(work)

        return await self._run_guarded(original, once)

    async def claim_mail(self, original: RoleOriginal, mail_id: int) -> Message:
        if original.player_id == 0 or mail_id == 0:
            raise MailInvalidError("invalid claim mail request")
        op_id = await self._op_ids()
        guild_id = await self._guild_of(original.player_id)

        async def once() -> Message:
            now = self._clock_ms()

            async def work(tx: MailTx) -> Message:
                await self._lock_actor(tx, original)
                await self._lock_box(tx, original.player_id, now)
                mail, state = await self._materialize(tx, original.player_id, guild_id, mail_id, now)
                if state.claim_state != CLAIM_UNCLAIMED:
                    return self.protocol.message(
                        "mailpb.MailActionResult",
                        mail_id=mail_id,
                        claim_state=state.claim_state,
                        asset_op_id=state.current_op_id,
                    )
                if mail.expire_ms <= now:
                    raise MailExpiredError(f"mail expired id={mail_id}")
                if not has_attachments(mail.attachments):
                    raise MailAttachmentError("mail has no attachments")
                if state.claim_version >= UINT64_MAX:
                    raise MailConflictError("claim_version overflow")
                try:
                    self.protocol.parse("AssetBundle", mail.attachments)
                except DecodeError as exc:
                    raise MailAttachmentError("corrupt attachment payload") from exc

                await tx.ensure_seq_row(original.player_id, ASSET_STREAM_SYSTEM_CREDIT, now)
                epoch, seq = await tx.allocate_seq(
                    original.player_id,
                    ASSET_STREAM_SYSTEM_CREDIT,
                    ASSET_OP_MAX_PENDING,
                    ASSET_OP_MAX_SPAN,
                    now,
                )
                version = state.claim_version + 1
                await tx.insert_asset_op(
                    MailAssetOpRow(
                        op_id=op_id,
                        player_id=original.player_id,
                        stream=ASSET_STREAM_SYSTEM_CREDIT,
                        seq=seq,
                        stream_epoch=epoch,
                        mail_id=mail_id,
                        claim_version=version,
                        status=OP_PENDING,
                        payload=mail.attachments,
                        tx_type=TX_MAIL_ATTACHMENT,
                        attempts=0,
                        next_attempt_ms=now,
                        lease_until_ms=0,
                        lease_token=0,
                        last_outcome=0,
                        last_reason=0,
                        durable=0,
                        reason_tip_id=0,
                        created_ms=now,
                        updated_ms=now,
                        last_send_ms=0,
                    )
                )
                ok = await tx.cas_claim_state_pending(
                    original.player_id, mail_id, op_id, version, state.claim_version, now
                )
                if not ok:
                    raise MailConflictError("claim_state CAS lost")

                result = self.protocol.message(
                    "mailpb.MailActionResult",
                    mail_id=mail_id,
                    claim_state=CLAIM_PENDING,
                    asset_op_id=op_id,
                )
                result.error_message.id = ERR_MAIL_CLAIM_PENDING
                return result

            return await self._driver.run_tx(work)

        return await self._run_guarded(original, once)

    # -- 资产重投存储原语 ----------------------------------------------------------------------------

    async def list_due_ops(
        self, now_ms: int, limit: int, *, owned_shards: frozenset[int] | None = None
    ) -> list[int]:
        if not 1 <= limit <= 1000:
            raise MailInvalidError(f"invalid due op limit={limit}")
        return await self._driver.list_due_ops(now_ms, limit, owned_shards)

    async def claim_op(
        self, op_id: int, now_ms: int, lease_until_ms: int, poison_until_ms: int, lease_token: int
    ) -> PendingAssetOp | None:
        row = await self._driver.claim_op_direct(op_id, now_ms, lease_until_ms, lease_token)
        if row is None:
            return None
        try:
            bundle = self.protocol.parse("AssetBundle", row.payload)
        except DecodeError as exc:
            poison_op = PendingAssetOp(
                op_id=row.op_id,
                player_id=row.player_id,
                stream=row.stream,
                seq=row.seq,
                stream_epoch=row.stream_epoch,
                correlation_id=row.op_id,
                tx_type=row.tx_type,
                bundle=self.protocol.message("AssetBundle"),
                attempts=row.attempts,
                lease_token=lease_token,
                last_reason=row.last_reason,
                created_ms=row.created_ms,
                last_send_ms=row.last_send_ms,
            )
            await self.reschedule_op(poison_op, poison_until_ms, 0, 0, False, False, now_ms)
            raise AssetOpPoisonRowError(f"undecodable payload op_id={op_id}") from exc

        return PendingAssetOp(
            op_id=row.op_id,
            player_id=row.player_id,
            stream=row.stream,
            seq=row.seq,
            stream_epoch=row.stream_epoch,
            correlation_id=row.op_id,
            tx_type=row.tx_type,
            bundle=bundle,
            attempts=row.attempts,
            lease_token=lease_token,
            last_reason=row.last_reason,
            created_ms=row.created_ms,
            last_send_ms=row.last_send_ms,
        )

    async def reschedule_op(
        self,
        op: PendingAssetOp,
        next_attempt_ms: int,
        outcome: int,
        reason: int,
        durable: bool,
        local: bool,
        now_ms: int,
    ) -> None:
        async def work(tx: MailTx) -> None:
            row = await tx.lock_asset_op(op.op_id)
            if row is None or row.status != OP_PENDING or row.lease_token != op.lease_token:
                raise AssetOpLeaseLostError(f"lease lost op_id={op.op_id}")
            last_send = 0 if local else now_ms
            await tx.update_asset_op_reschedule(
                op.op_id,
                next_attempt_ms,
                1 if durable else 0,
                outcome,
                reason,
                now_ms,
                last_send,
            )

        await self._driver.run_tx(work)

    async def finalize_op(
        self,
        op: PendingAssetOp,
        final_kind: str,
        outcome: int,
        reason: int,
        *,
        partial: bool,
        sql_committed: bool,
        now_ms: int,
    ) -> bool:
        # 强制门禁：缺 SQL 提交证据绝不终结。
        if not sql_committed:
            raise StoreUnavailableError("mail asset finalization requires SQL commit proof")

        tip_reason = reason
        if final_kind == FINAL_APPLIED:
            if partial:
                raise MailProtectedError("partial credit cannot finalize as APPLIED")
            op_status, claim_state = OP_APPLIED, CLAIM_CLAIMED
        elif final_kind in (FINAL_REJECTED, FINAL_ABORTED):
            op_status, claim_state = OP_REJECTED, CLAIM_UNCLAIMED
        elif final_kind == FINAL_APPLIED_PARTIAL:
            op_status, claim_state = OP_MANUAL, CLAIM_MANUAL
            tip_reason = ERR_MAIL_CLAIM_MANUAL
        else:
            raise MailInvalidError(f"unknown final status={final_kind!r}")

        async def work(tx: MailTx) -> bool:
            await self._lock_box(tx, op.player_id, now_ms)
            mail_id = await tx.get_asset_op_mail_id(op.op_id)
            if mail_id is None:
                raise MailNotFoundError(f"asset op missing op_id={op.op_id}")
            state = await tx.lock_mail_state(op.player_id, mail_id)
            if state is None:
                raise MailNotFoundError(f"mail state missing mail_id={mail_id}")
            row = await tx.lock_asset_op(op.op_id)
            if row is None:
                raise MailNotFoundError(f"asset op missing op_id={op.op_id}")
            if row.status != OP_PENDING:
                return False
            if (
                state.current_op_id != op.op_id
                or state.claim_version != row.claim_version
                or state.claim_state != CLAIM_PENDING
            ):
                raise MailConflictError(f"mail state mismatch on finalize op_id={op.op_id}")

            await tx.update_asset_op_final(op.op_id, op_status, outcome, reason, now_ms, tip_reason)
            await tx.update_mail_state_final(
                op.player_id, mail_id, op.op_id, state.claim_version, claim_state, tip_reason, now_ms
            )
            return True

        return await self._driver.run_tx(work)

    # -- 过期清理（Sweep） ---------------------------------------------------------------------------

    async def sweep(
        self, cutoff_ms: int, limit: int, delete_rows: bool, *, owned_shards: frozenset[int] | None = None
    ) -> SweepReport:
        if cutoff_ms == 0 or not 1 <= limit <= 10000:
            raise MailInvalidError("invalid sweep parameters")
        candidates, invalid_count = await self._driver.list_sweep_candidates(cutoff_ms, limit, owned_shards)
        personal_cand = sum(1 for _, kind in candidates if kind == MAIL_KIND_PERSONAL)
        system_cand = len(candidates) - personal_cand
        personal_del = 0
        system_del = 0
        state_del = 0
        if delete_rows:
            remaining = limit
            for mail_id, kind in candidates:
                if remaining <= 0:
                    break

                async def work(tx: MailTx, mid: int = mail_id, rem: int = remaining) -> tuple[int, int]:
                    return await tx.sweep_one_mail(mid, cutoff_ms, rem)

                s_del, m_del = await self._driver.run_tx(work)
                remaining -= s_del + m_del
                state_del += s_del
                if kind == MAIL_KIND_PERSONAL:
                    personal_del += m_del
                else:
                    system_del += m_del

        return SweepReport(
            personal=SweepStats(candidates=personal_cand, deleted=personal_del, invalid=invalid_count),
            system=SweepStats(candidates=system_cand, deleted=system_del, invalid=0),
            system_state=SweepStats(candidates=0, deleted=state_del, invalid=0),
        )

    # -- RoleFenceParticipant ------------------------------------------------------------------------

    async def prepare_role_freeze(self, state: Message, *, dry_run: bool = False) -> Message:
        if not valid_role_fence_state(state) or state.freeze_token == 0 or state.freeze_final:
            raise InvalidRoleFenceStateError("invalid role fence state for prepare")

        async def work(tx: MailTx) -> Message:
            if dry_run:
                pending = await tx.has_pending_asset_op(int(state.player_id))
                blocker = ROLE_BLOCKER_ASSET_OP if pending else ""
                raise _TxRollbackReturn(
                    self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=(blocker == ""), blocker=blocker)
                )
            await tx.apply_role_fence(state, self._clock_ms())
            fence = await tx.lock_role_fence(int(state.player_id))
            if (
                fence is None
                or fence.version != int(state.version)
                or fence.freeze_token != int(state.freeze_token)
            ):
                raise _TxRollbackReturn(self.protocol.message("rolefence.PrepareRoleFreezeResponse", stale=True))
            if await tx.has_pending_asset_op(int(state.player_id)):
                raise _TxRollbackReturn(
                    self.protocol.message("rolefence.PrepareRoleFreezeResponse", blocker=ROLE_BLOCKER_ASSET_OP)
                )
            return self.protocol.message("rolefence.PrepareRoleFreezeResponse", clean=True)

        return await self._driver.run_tx(work)

    async def apply_role_fence(self, state: Message) -> Message:
        if not valid_role_fence_state(state):
            raise InvalidRoleFenceStateError("invalid role fence state for apply")
        await self._driver.apply_role_fence_direct(state, self._clock_ms())
        return self.protocol.message("rolefence.ApplyRoleFenceResponse")

    async def purge_removed_role(self, state: Message) -> Message:
        if not valid_role_fence_state(state) or state.account != "" or state.freeze_token != 0:
            raise InvalidRoleFenceStateError("purge requires removed tombstone role fence state")

        async def work(tx: MailTx) -> Message:
            await tx.apply_role_fence(state, self._clock_ms())
            fence = await tx.lock_role_fence(int(state.player_id))
            if fence is None or fence.account != "":
                raise StoreUnavailableError("role fence replica is not tombstone")
            await tx.purge_player_rows(int(state.player_id))
            return self.protocol.message("rolefence.PurgeRemovedRoleResponse")

        return await self._driver.run_tx(work)


# ----------------------------------------------------------------------------------------------------
# 确定性内存驱动与 MemoryMailStore 双胞胎
# ----------------------------------------------------------------------------------------------------


class _MemoryState:
    def __init__(self) -> None:
        self.messages: dict[int, MailRow] = {}
        self.messages_by_req: dict[tuple[int, int], int] = {}
        self.campaigns: dict[tuple[int, int, int], MailCampaignRow] = {}
        self.campaigns_by_req: dict[tuple[int, int], tuple[int, int, int]] = {}
        self.shards: dict[tuple[int, int, int, int], MailCampaignShardRow] = {}
        self.recipients: set[tuple[int, int, int, int]] = set()
        self.boxes: dict[int, int] = {}
        self.states: dict[tuple[int, int], MailStateRow] = {}
        self.seqs: dict[tuple[int, int], tuple[int, int, int]] = {}  # (next_seq, epoch, updated_ms)
        self.ops: dict[int, MailAssetOpRow] = {}
        self.ops_by_seq: dict[tuple[int, int, int, int], int] = {}
        self.role_fences: dict[int, RoleFenceRow] = {}


def _is_visible_in_memory(state: _MemoryState, mail: MailRow, player_id: int, guild_id: int) -> bool:
    if mail.kind not in (MAIL_KIND_PERSONAL, MAIL_KIND_SYSTEM, MAIL_KIND_GUILD):
        return False
    if mail.owner_player_id == player_id:
        return True
    if mail.owner_player_id != 0:
        return False
    if mail.campaign_id > 0:
        return (mail.sender_kind, mail.campaign_id, mail.manifest_generation, player_id) in state.recipients
    if mail.kind == MAIL_KIND_SYSTEM:
        return True
    if mail.kind == MAIL_KIND_GUILD:
        return guild_id != 0 and mail.guild_id == guild_id
    return False


class _MemoryTx:
    def __init__(self, state: _MemoryState) -> None:
        self._s = state

    async def lock_role_fence(self, player_id: int) -> RoleFenceRow | None:
        return self._s.role_fences.get(player_id)

    async def apply_role_fence(self, state: Message, now_ms: int) -> bool:
        pid = int(state.player_id)
        cur = self._s.role_fences.get(pid)
        if cur is not None and int(state.version) <= cur.version:
            return False
        self._s.role_fences[pid] = RoleFenceRow(
            player_id=pid,
            account=str(state.account),
            owner_revision=int(state.owner_revision),
            freeze_token=int(state.freeze_token),
            freeze_final=bool(state.freeze_final),
            version=int(state.version),
            home_zone=int(state.home_zone),
            updated_ms=now_ms,
        )
        return True

    async def has_pending_asset_op(self, player_id: int) -> bool:
        return any(
            op.player_id == player_id and op.status not in OP_TERMINAL_STATUSES
            for op in self._s.ops.values()
        )

    async def purge_player_rows(self, player_id: int) -> None:
        # 锁序与 Go RolePurge 对齐：mail_box -> mail_message -> mail_state -> seq -> sealed recipients -> mail_box
        for mid in [m.mail_id for m in self._s.messages.values() if m.owner_player_id == player_id]:
            row = self._s.messages.pop(mid)
            self._s.messages_by_req.pop((row.sender_kind, row.request_id), None)
        for key in [k for k in self._s.states if k[0] == player_id]:
            self._s.states.pop(key, None)
        for key in [k for k in self._s.seqs if k[0] == player_id]:
            self._s.seqs.pop(key, None)
        sealed_keys = {
            (c.sender_kind, c.campaign_id, c.manifest_generation)
            for c in self._s.campaigns.values()
            if c.state == CAMPAIGN_SEALED
        }
        for rkey in [k for k in self._s.recipients if k[3] == player_id and k[:3] in sealed_keys]:
            self._s.recipients.discard(rkey)
        self._s.boxes.pop(player_id, None)

    async def ensure_mail_box(self, player_id: int, now_ms: int) -> None:
        self._s.boxes.setdefault(player_id, now_ms)

    async def lock_mail_box(self, player_id: int) -> None:
        if player_id not in self._s.boxes:
            raise StoreUnavailableError(f"mail_box missing player={player_id}")

    async def get_message_by_request(self, sender_kind: int, request_id: int) -> MailRow | None:
        mid = self._s.messages_by_req.get((sender_kind, request_id))
        return self._s.messages.get(mid) if mid is not None else None

    async def get_message_by_id(self, mail_id: int) -> MailRow | None:
        return self._s.messages.get(mail_id)

    async def get_visible_message(self, mail_id: int, player_id: int, guild_id: int) -> MailRow | None:
        mail = self._s.messages.get(mail_id)
        if mail is None or not _is_visible_in_memory(self._s, mail, player_id, guild_id):
            return None
        return mail

    async def count_personal_active_mails(self, player_id: int, now_ms: int) -> int:
        count = 0
        for mail in self._s.messages.values():
            if mail.owner_player_id != player_id:
                continue
            st = self._s.states.get((player_id, mail.mail_id))
            if st is None or st.deleted_ms != 0:
                continue
            if mail.expire_ms > now_ms or is_protected_claim_state(st.claim_state):
                count += 1
        return count

    async def count_shared_active_mails(self, guild_id: int, now_ms: int) -> int:
        if guild_id == 0:
            return sum(
                1
                for m in self._s.messages.values()
                if m.owner_player_id == 0
                and m.guild_id == 0
                and m.kind in (MAIL_KIND_UNSPECIFIED, MAIL_KIND_SYSTEM)
                and m.expire_ms > now_ms
            )
        return sum(
            1
            for m in self._s.messages.values()
            if m.owner_player_id == 0 and m.guild_id == guild_id and m.expire_ms > now_ms
        )

    async def insert_message(self, row: MailRow) -> None:
        req_key = (row.sender_kind, row.request_id)
        if row.mail_id in self._s.messages or req_key in self._s.messages_by_req:
            raise _DuplicateKeyError("duplicate mail_message")
        self._s.messages[row.mail_id] = row
        self._s.messages_by_req[req_key] = row.mail_id

    async def publish_campaign_message(self, mail_id: int, kind: int, sent_ms: int) -> None:
        row = self._s.messages.get(mail_id)
        if row is not None and row.kind == MAIL_KIND_UNSPECIFIED:
            self._s.messages[mail_id] = replace(row, kind=kind, sent_ms=sent_ms)

    async def get_campaign_by_request(self, sender_kind: int, request_id: int) -> MailCampaignRow | None:
        ckey = self._s.campaigns_by_req.get((sender_kind, request_id))
        return self._s.campaigns.get(ckey) if ckey is not None else None

    async def lock_campaign(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> MailCampaignRow | None:
        return self._s.campaigns.get((sender_kind, campaign_id, manifest_generation))

    async def insert_campaign(self, row: MailCampaignRow) -> None:
        ckey = (row.sender_kind, row.campaign_id, row.manifest_generation)
        rkey = (row.sender_kind, row.request_id)
        if ckey in self._s.campaigns or rkey in self._s.campaigns_by_req:
            raise _DuplicateKeyError("duplicate mail_campaign")
        self._s.campaigns[ckey] = row
        self._s.campaigns_by_req[rkey] = ckey

    async def mark_campaign_sealed(
        self, sender_kind: int, campaign_id: int, manifest_generation: int, sealed_ms: int
    ) -> None:
        ckey = (sender_kind, campaign_id, manifest_generation)
        row = self._s.campaigns[ckey]
        self._s.campaigns[ckey] = replace(row, state=CAMPAIGN_SEALED, sealed_ms=sealed_ms)

    async def get_campaign_shard(
        self, sender_kind: int, campaign_id: int, manifest_generation: int, shard_index: int
    ) -> MailCampaignShardRow | None:
        return self._s.shards.get((sender_kind, campaign_id, manifest_generation, shard_index))

    async def sum_campaign_shard_recipients(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> int:
        return sum(
            sh.recipient_count
            for sh in self._s.shards.values()
            if sh.sender_kind == sender_kind
            and sh.campaign_id == campaign_id
            and sh.manifest_generation == manifest_generation
        )

    async def list_campaign_shards(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> list[MailCampaignShardRow]:
        matched = [
            sh
            for sh in self._s.shards.values()
            if sh.sender_kind == sender_kind
            and sh.campaign_id == campaign_id
            and sh.manifest_generation == manifest_generation
        ]
        matched.sort(key=lambda s: s.shard_index)
        return matched

    async def insert_campaign_recipients(
        self,
        sender_kind: int,
        campaign_id: int,
        manifest_generation: int,
        shard_index: int,
        player_ids: Sequence[int],
    ) -> None:
        pending = [(sender_kind, campaign_id, manifest_generation, int(pid)) for pid in player_ids]
        for key in pending:
            if key in self._s.recipients:
                raise _DuplicateKeyError("duplicate campaign recipient")
        self._s.recipients.update(pending)

    async def insert_campaign_shard(self, row: MailCampaignShardRow) -> None:
        key = (row.sender_kind, row.campaign_id, row.manifest_generation, row.shard_index)
        if key in self._s.shards:
            raise _DuplicateKeyError("duplicate campaign shard")
        self._s.shards[key] = row

    async def count_campaign_recipients(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> int:
        return sum(
            1
            for k in self._s.recipients
            if k[0] == sender_kind and k[1] == campaign_id and k[2] == manifest_generation
        )

    async def lock_mail_state(self, player_id: int, mail_id: int) -> MailStateRow | None:
        return self._s.states.get((player_id, mail_id))

    async def insert_mail_state(self, row: MailStateRow) -> None:
        key = (row.player_id, row.mail_id)
        if key in self._s.states:
            raise _DuplicateKeyError("duplicate mail_state")
        self._s.states[key] = row

    async def mark_mail_read(self, player_id: int, mail_id: int, now_ms: int) -> MailStateRow:
        key = (player_id, mail_id)
        st = self._s.states[key]
        updated = replace(st, read_ms=st.read_ms or now_ms, updated_ms=now_ms)
        self._s.states[key] = updated
        return updated

    async def mark_mail_deleted(self, player_id: int, mail_id: int, now_ms: int) -> None:
        key = (player_id, mail_id)
        st = self._s.states[key]
        self._s.states[key] = replace(st, deleted_ms=now_ms, updated_ms=now_ms)

    async def list_clearable_read_mail_ids(self, player_id: int, guild_id: int, limit: int) -> list[int]:
        ids = []
        for mail in self._s.messages.values():
            st = self._s.states.get((player_id, mail.mail_id))
            if st is None or st.read_ms == 0 or st.deleted_ms != 0:
                continue
            if _is_visible_in_memory(self._s, mail, player_id, guild_id):
                ids.append(mail.mail_id)
        ids.sort()
        return ids[:limit]

    def _visible_entries(self, player_id: int, guild_id: int, now_ms: int) -> list[tuple[MailRow, MailStateRow | None]]:
        out: list[tuple[MailRow, MailStateRow | None]] = []
        for mail in self._s.messages.values():
            if not _is_visible_in_memory(self._s, mail, player_id, guild_id):
                continue
            st = self._s.states.get((player_id, mail.mail_id))
            if st is not None and st.deleted_ms != 0:
                continue
            if mail.expire_ms > now_ms or (st is not None and is_protected_claim_state(st.claim_state)):
                out.append((mail, st))
        return out

    async def list_visible_counts(self, player_id: int, guild_id: int, now_ms: int) -> tuple[int, int]:
        entries = self._visible_entries(player_id, guild_id, now_ms)
        unread = sum(1 for _, st in entries if st is None or st.read_ms == 0)
        claimable = sum(
            1
            for mail, st in entries
            if mail.expire_ms > now_ms and effective_claim_state(mail.attachments, st) == CLAIM_UNCLAIMED
        )
        return unread, claimable

    async def list_visible_page(
        self,
        player_id: int,
        guild_id: int,
        now_ms: int,
        before_sent_ms: int | None,
        before_mail_id: int | None,
        limit: int,
    ) -> list[tuple[MailRow, MailStateRow | None]]:
        entries = self._visible_entries(player_id, guild_id, now_ms)
        if before_sent_ms is not None and before_mail_id is not None:
            entries = [
                (m, s)
                for m, s in entries
                if m.sent_ms < before_sent_ms or (m.sent_ms == before_sent_ms and m.mail_id < before_mail_id)
            ]
        entries.sort(key=lambda pair: (pair[0].sent_ms, pair[0].mail_id), reverse=True)
        return entries[:limit]

    async def ensure_seq_row(self, player_id: int, stream: int, now_ms: int) -> None:
        if now_ms == 0:
            raise StoreUnavailableError("seq epoch must be positive")
        self._s.seqs.setdefault((player_id, stream), (1, now_ms, now_ms))

    async def allocate_seq(
        self, player_id: int, stream: int, max_pending: int, max_span: int, now_ms: int
    ) -> tuple[int, int]:
        row = self._s.seqs.get((player_id, stream))
        if row is None or row[1] == 0:
            raise StoreUnavailableError("seq row missing or corrupt")
        next_seq, epoch, _ = row
        pending_seqs = sorted(
            op.seq
            for op in self._s.ops.values()
            if op.player_id == player_id
            and op.stream == stream
            and op.stream_epoch == epoch
            and op.status == OP_PENDING
        )
        if len(pending_seqs) >= max_pending:
            raise MailTooManyPendingError("too many pending ops")
        if pending_seqs and next_seq - pending_seqs[0] >= max_span:
            raise MailTooManyPendingError("pending op seq span exceeded")
        self._s.seqs[(player_id, stream)] = (next_seq + 1, epoch, now_ms)
        return epoch, next_seq

    async def insert_asset_op(self, row: MailAssetOpRow) -> None:
        seq_key = (row.player_id, row.stream, row.stream_epoch, row.seq)
        if row.op_id in self._s.ops or seq_key in self._s.ops_by_seq:
            raise _DuplicateKeyError("duplicate mail_asset_op")
        self._s.ops[row.op_id] = row
        self._s.ops_by_seq[seq_key] = row.op_id

    async def cas_claim_state_pending(
        self, player_id: int, mail_id: int, op_id: int, new_version: int, old_version: int, now_ms: int
    ) -> bool:
        key = (player_id, mail_id)
        st = self._s.states.get(key)
        if st is None or st.claim_state != CLAIM_UNCLAIMED or st.claim_version != old_version:
            return False
        self._s.states[key] = replace(
            st,
            claim_state=CLAIM_PENDING,
            current_op_id=op_id,
            claim_version=new_version,
            reason_tip_id=0,
            read_ms=st.read_ms or now_ms,
            updated_ms=now_ms,
        )
        return True

    async def get_asset_op_mail_id(self, op_id: int) -> int | None:
        op = self._s.ops.get(op_id)
        return op.mail_id if op is not None else None

    async def lock_asset_op(self, op_id: int) -> MailAssetOpRow | None:
        return self._s.ops.get(op_id)

    async def update_asset_op_reschedule(
        self,
        op_id: int,
        next_attempt_ms: int,
        durable: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
        last_send_ms: int,
    ) -> None:
        op = self._s.ops[op_id]
        self._s.ops[op_id] = replace(
            op,
            attempts=op.attempts + 1,
            next_attempt_ms=next_attempt_ms,
            lease_until_ms=0,
            durable=durable,
            last_outcome=last_outcome,
            last_reason=last_reason,
            updated_ms=now_ms,
            last_send_ms=max(op.last_send_ms, last_send_ms),
        )

    async def update_asset_op_final(
        self,
        op_id: int,
        final_status: int,
        last_outcome: int,
        last_reason: int,
        now_ms: int,
        reason_tip_id: int,
    ) -> None:
        op = self._s.ops[op_id]
        self._s.ops[op_id] = replace(
            op,
            status=final_status,
            durable=1,
            last_outcome=last_outcome,
            last_reason=last_reason,
            next_attempt_ms=now_ms,
            lease_until_ms=0,
            updated_ms=now_ms,
            reason_tip_id=reason_tip_id,
        )

    async def update_mail_state_final(
        self,
        player_id: int,
        mail_id: int,
        op_id: int,
        claim_version: int,
        new_claim_state: int,
        reason_tip_id: int,
        now_ms: int,
    ) -> None:
        key = (player_id, mail_id)
        st = self._s.states[key]
        if st.current_op_id == op_id and st.claim_version == claim_version and st.claim_state == CLAIM_PENDING:
            self._s.states[key] = replace(
                st,
                claim_state=new_claim_state,
                reason_tip_id=reason_tip_id,
                updated_ms=now_ms,
            )

    def _sweep_eligible(self, mail: MailRow, cutoff_ms: int) -> bool:
        if mail.expire_ms == 0 or mail.expire_ms >= cutoff_ms or mail.campaign_id != 0:
            return False
        if mail.kind not in (MAIL_KIND_PERSONAL, MAIL_KIND_SYSTEM, MAIL_KIND_GUILD):
            return False
        if any(
            st.mail_id == mail.mail_id and is_protected_claim_state(st.claim_state)
            for st in self._s.states.values()
        ):
            return False
        if any(
            op.mail_id == mail.mail_id and op.status not in OP_TERMINAL_STATUSES
            for op in self._s.ops.values()
        ):
            return False
        return True

    async def sweep_one_mail(self, mail_id: int, cutoff_ms: int, remaining: int) -> tuple[int, int]:
        mail = self._s.messages.get(mail_id)
        if mail is None or not self._sweep_eligible(mail, cutoff_ms):
            return 0, 0
        deletable_keys = [
            k
            for k, st in self._s.states.items()
            if st.mail_id == mail_id and not is_protected_claim_state(st.claim_state)
        ][:remaining]
        for k in deletable_keys:
            self._s.states.pop(k, None)
        states_deleted = len(deletable_keys)
        msg_deleted = 0
        if states_deleted < remaining:
            has_states = any(st.mail_id == mail_id for st in self._s.states.values())
            has_open_op = any(
                op.mail_id == mail_id and op.status not in OP_TERMINAL_STATUSES for op in self._s.ops.values()
            )
            if not has_states and not has_open_op:
                self._s.messages.pop(mail_id, None)
                self._s.messages_by_req.pop((mail.sender_kind, mail.request_id), None)
                msg_deleted = 1
        return states_deleted, msg_deleted


class _MemoryDriver:
    def __init__(self) -> None:
        self.state = _MemoryState()
        self._lock = asyncio.Lock()

    async def run_tx(self, work: Callable[[MailTx], Awaitable[T]]) -> T:
        async with self._lock:
            snapshot = copy.deepcopy(self.state)
            try:
                return await work(_MemoryTx(self.state))
            except _TxRollbackReturn as ret:
                self.state = snapshot
                return ret.value
            except BaseException:
                self.state = snapshot
                raise

    async def run_rr_read_tx(self, work: Callable[[MailTx], Awaitable[T]]) -> T:
        return await self.run_tx(work)

    async def read_role_fence(self, player_id: int) -> RoleFenceRow | None:
        async with self._lock:
            return self.state.role_fences.get(player_id)

    async def apply_role_fence_direct(self, state: Message, now_ms: int) -> bool:
        async with self._lock:
            return await _MemoryTx(self.state).apply_role_fence(state, now_ms)

    async def ensure_mail_box_direct(self, player_id: int, now_ms: int) -> None:
        async with self._lock:
            self.state.boxes.setdefault(player_id, now_ms)

    async def get_message_by_request_direct(self, sender_kind: int, request_id: int) -> MailRow | None:
        async with self._lock:
            mid = self.state.messages_by_req.get((sender_kind, request_id))
            return self.state.messages.get(mid) if mid is not None else None

    async def get_campaign_direct(
        self, sender_kind: int, campaign_id: int, manifest_generation: int
    ) -> MailCampaignRow | None:
        async with self._lock:
            return self.state.campaigns.get((sender_kind, campaign_id, manifest_generation))

    async def list_due_ops(
        self, now_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> list[int]:
        async with self._lock:
            candidates = [
                op
                for op in self.state.ops.values()
                if op.status == OP_PENDING
                and op.next_attempt_ms <= now_ms
                and op.lease_until_ms < now_ms
                and (owned_shards is None or (op.player_id % 64) in owned_shards)
            ]
            fresh = sorted(
                (op for op in candidates if op.attempts < FRESH_ATTEMPT_LIMIT),
                key=lambda o: (o.next_attempt_ms, o.op_id),
            )
            aged = sorted(
                (op for op in candidates if op.attempts >= FRESH_ATTEMPT_LIMIT),
                key=lambda o: (o.next_attempt_ms, o.op_id),
            )
            out: list[int] = []
            seen: set[int] = set()
            for op in (*fresh, *aged):
                if len(out) >= limit:
                    break
                if op.op_id not in seen:
                    seen.add(op.op_id)
                    out.append(op.op_id)
            return out

    async def claim_op_direct(
        self, op_id: int, now_ms: int, lease_until_ms: int, lease_token: int
    ) -> MailAssetOpRow | None:
        async with self._lock:
            op = self.state.ops.get(op_id)
            if op is None or op.status != OP_PENDING or op.lease_until_ms >= now_ms:
                return None
            updated = replace(
                op,
                last_send_ms=max(op.last_send_ms, op.lease_until_ms),
                lease_until_ms=lease_until_ms,
                lease_token=lease_token,
            )
            self.state.ops[op_id] = updated
            return updated

    async def list_sweep_candidates(
        self, cutoff_ms: int, limit: int, owned_shards: frozenset[int] | None = None
    ) -> tuple[list[tuple[int, int]], int]:
        async with self._lock:
            tx = _MemoryTx(self.state)
            eligible = [
                m
                for m in self.state.messages.values()
                if tx._sweep_eligible(m, cutoff_ms) and (owned_shards is None or (m.mail_id % 64) in owned_shards)
            ]
            eligible.sort(key=lambda m: (m.expire_ms, m.mail_id))
            sample = sorted(
                (
                    m
                    for m in self.state.messages.values()
                    if owned_shards is None or (m.mail_id % 64) in owned_shards
                ),
                key=lambda m: m.mail_id,
            )[:limit]
            invalid = sum(1 for m in sample if m.expire_ms == 0)
            return [(m.mail_id, m.kind) for m in eligible[:limit]], invalid


class MemoryMailStore(MailStoreCore):
    """确定性内存邮箱存储双胞胎，支持单测完整验证全部事务、回滚、护栏与重投语义。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        item_exists: Callable[[int], bool] = lambda _: True,
        guilds: GuildDirectory | None = None,
        role_directory: RoleFenceDirectory | None = None,
        limits: MailLimits = MailLimits(),
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._driver_mem = _MemoryDriver()
        self._next_mail_id = 1000
        self._next_op_id = 5000

        async def next_mail_id() -> int:
            self._next_mail_id += 1
            return self._next_mail_id

        async def next_op_id() -> int:
            self._next_op_id += 1
            return self._next_op_id

        super().__init__(
            protocol,
            self._driver_mem,
            mail_ids=next_mail_id,
            op_ids=next_op_id,
            item_exists=item_exists,
            guilds=guilds,
            role_directory=role_directory,
            limits=limits,
            clock_ms=clock_ms,
        )

    @property
    def state(self) -> _MemoryState:
        return self._driver_mem.state
