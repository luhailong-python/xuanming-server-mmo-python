"""排行榜结算奖励邮件投递策略与后台工作器（与 Go ``go/rank/internal/reward`` 完全对齐）。

工作器只把已封榜且持久的奖励意图交给 ``mailpb.MailAdmin/SendPersonalMail``，不直接更改玩家资产。
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections.abc import Callable
from typing import Protocol as TypingProtocol, runtime_checkable

from google.protobuf.message import DecodeError, Message

from ..grpc_runtime import GrpcClient, sign_service_call
from ..protocol import Protocol
from .errors import ERR_MAIL_RECIPIENT_DELETED, ERR_OK, ERR_SUCCESS
from .model import (
    MIN_REWARD_MAIL_CLAIM_WINDOW_MS,
    REWARD_ABANDONED,
    REWARD_DELIVERED,
    REWARD_PENDING,
)
from .store import RewardStore

logger = logging.getLogger(__name__)

# 投递结果分类
ATTEMPT_DELIVERED = 1
ATTEMPT_UNKNOWN = 2
ATTEMPT_REJECTED = 3
ATTEMPT_RECIPIENT_GONE = 4

# Tip 码定性结果（与 Go shared/serverbase/tipcode.go 一致）
VERDICT_OK = "ok"
VERDICT_BIZ_REJECT = "biz_reject"
VERDICT_FAULT = "fault"
VERDICT_UNKNOWN = "unknown_code"

# 退避与窗口常量（与 Go internal/reward/policy.go 一致）
MAX_ATTEMPTS = 50_000
UNKNOWN_RETRY_BASE_MS = 1_000
UNKNOWN_RETRY_CEILING_MS = 60_000
REJECT_RETRY_BASE_MS = 60_000
REJECT_RETRY_CEILING_MS = 60 * 60_000
MIN_CLAIM_MS = MIN_REWARD_MAIL_CLAIM_WINDOW_MS  # 24 小时
EXPIRED_PROBE_DELAY_MS = 60 * 60_000  # 过期后 1 小时判重探询

# 邮件载荷常量（标题与正文参与摘要计算，不可随意改动）
REWARD_MAIL_TITLE = "排行榜结算奖励"
REWARD_MAIL_BODY = "本期排行榜奖励已结算，请领取附件。"
REWARD_SENDER_KIND = 3
MAIL_CALLER = "rank"
SEND_PERSONAL_MAIL_PATH = "/mailpb.MailAdmin/SendPersonalMail"

DEFAULT_POLL_INTERVAL_SEC = 2.0
LEASE_DURATION_MS = 10_000
DUE_SCAN_LIMIT = 100
SEND_TIMEOUT_SEC = 1.5
PROCESS_TIMEOUT_SEC = 2.5

# 与 Go shared/generated/tip/segments.go 同步的已分配 Tip 码段 (base, width, lo, hi)
_TIP_SEGMENTS: tuple[tuple[int, int, int, int], ...] = (
    (1000, 1000, 1000, 1018),
    (2000, 1000, 2000, 2056),
    (3000, 1000, 3000, 3028),
    (4000, 1000, 4000, 4034),
    (5000, 1000, 5000, 5006),
    (6000, 1000, 6000, 6017),
    (7000, 1000, 7000, 7013),
    (8000, 1000, 8000, 8001),
    (9000, 1000, 9000, 9000),
    (10000, 1000, 10000, 10000),
    (11000, 1000, 11000, 11000),
    (12000, 1000, 12000, 12001),
    (13000, 1000, 13000, 13000),
    (14000, 1000, 14000, 14047),
    (15000, 1000, 15000, 15009),
    (16000, 1000, 16000, 16036),
    (17000, 1000, 17000, 17011),
    (18000, 1000, 18000, 18006),
    (20000, 1000, 20000, 20017),
    (21000, 1000, 21000, 21003),
    (25000, 1000, 25000, 25014),
    (26000, 1000, 26000, 26017),
    (27000, 1000, 27000, 27008),
    (28000, 1000, 28000, 28022),
    (29000, 1000, 29000, 29014),
    (30000, 1000, 30000, 30011),
    (31000, 1000, 31000, 31012),
    (32000, 1000, 32000, 32004),
    (33000, 1000, 33000, 33009),
)

# 与 Go shared/generated/tip/faults.go 同步的服务端内部故障 Tip 码集合
_TIP_FAULT_CODES: frozenset[int] = frozenset((
    1002, 1003, 1004, 1007, 1009, 1011, 1012, 1016, 1018,
    2015, 2018, 2019, 2020, 2021, 2022, 2023, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2035, 2036, 2047, 2050,
    3003, 3010, 3012, 3016, 3018, 3019, 3023, 3028,
    4030, 5003, 6002, 9000,
    14008, 14041, 14045,
    16004, 18001, 20017, 26016,
    27004, 27007, 27008,
    28010, 29014, 30011, 32004, 33006,
))


def tip_verdict(code: int) -> str:
    """按 Tip 码表对错误码定性：ok / biz_reject / fault / unknown_code。"""
    if code in (ERR_OK, ERR_SUCCESS):
        return VERDICT_OK
    in_allocated = False
    for base, width, lo, hi in _TIP_SEGMENTS:
        if base <= code < base + width:
            in_allocated = lo <= code <= hi
            break
    if not in_allocated:
        return VERDICT_UNKNOWN
    if code in _TIP_FAULT_CODES:
        return VERDICT_FAULT
    return VERDICT_BIZ_REJECT


def backoff_ms(attempts: int, base_ms: int, ceiling_ms: int) -> int:
    """按已完成次数翻倍退避，最多翻 6 次并按 ``ceiling_ms`` 封顶。"""
    shift = min(max(attempts, 0), 6)
    return min(base_ms << shift, ceiling_ms)


def send_cutoff_ms(expire_ms: int) -> int:
    """还能正常发新邮件的最后时刻（含）：此刻发出的邮件恰好剩余 24 小时可领。"""
    if expire_ms < MIN_CLAIM_MS:
        return 0
    return expire_ms - MIN_CLAIM_MS


def expired_probe_ms(expire_ms: int) -> int:
    """过期后判重探询时刻（过期时刻 + 1 小时，覆盖时钟偏差）。"""
    return expire_ms + EXPIRED_PROBE_DELAY_MS


def send_deferred_to(now_ms: int, expire_ms: int) -> tuple[int, bool]:
    """判断当前是否落在“过截止点但未到判重探询时刻”的禁止发新件窗口。"""
    probe_ms = expired_probe_ms(expire_ms)
    return probe_ms, send_cutoff_ms(expire_ms) < now_ms < probe_ms


def classify_mail_reply(response: Message | None, call_err: BaseException | None) -> int:
    """将一次 ``SendPersonalMail`` 的调用结果归类。"""
    if call_err is not None or response is None:
        return ATTEMPT_UNKNOWN
    err_id = int(response.error_message.id) if response.HasField("error_message") else 0
    if err_id == ERR_MAIL_RECIPIENT_DELETED:
        return ATTEMPT_RECIPIENT_GONE
    verdict = tip_verdict(err_id)
    if verdict == VERDICT_OK:
        return ATTEMPT_DELIVERED if int(response.mail_id) != 0 else ATTEMPT_UNKNOWN
    if verdict == VERDICT_BIZ_REJECT:
        return ATTEMPT_REJECTED
    return ATTEMPT_UNKNOWN


def next_step(result: int, attempts: int, now_ms: int, expire_ms: int) -> tuple[int, int]:
    """依据本次结果、历史尝试次数与写定的过期时刻，返回 ``(new_status, next_attempt_ms)``。"""
    cutoff_ms = send_cutoff_ms(expire_ms)
    probe_ms = expired_probe_ms(expire_ms)
    if result == ATTEMPT_DELIVERED:
        return REWARD_DELIVERED, now_ms
    if result == ATTEMPT_RECIPIENT_GONE:
        return REWARD_ABANDONED, now_ms
    if result == ATTEMPT_UNKNOWN:
        next_ms = now_ms + backoff_ms(attempts, UNKNOWN_RETRY_BASE_MS, UNKNOWN_RETRY_CEILING_MS)
        if cutoff_ms < next_ms < probe_ms:
            next_ms = probe_ms
        return REWARD_PENDING, next_ms
    # ATTEMPT_REJECTED
    if expire_ms == 0 or now_ms >= probe_ms or attempts + 1 >= MAX_ATTEMPTS:
        return REWARD_ABANDONED, now_ms
    if now_ms >= cutoff_ms:
        return REWARD_PENDING, probe_ms
    return REWARD_PENDING, min(now_ms + backoff_ms(attempts, REJECT_RETRY_BASE_MS, REJECT_RETRY_CEILING_MS), cutoff_ms)


@runtime_checkable
class MailAdminClient(TypingProtocol):
    """奖励工作器调用 ``mailpb.MailAdmin/SendPersonalMail`` 的最小客户端协议。"""

    async def send_personal_mail(self, request: Message, *, timeout: float = SEND_TIMEOUT_SEC) -> Message: ...


class GrpcMailAdminClient:
    """基于 ``GrpcClient`` 的 ``MailAdmin/SendPersonalMail`` 适配器。"""

    def __init__(self, protocol: Protocol, client: GrpcClient) -> None:
        self._protocol = protocol
        self._client = client

    async def send_personal_mail(self, request: Message, *, timeout: float = SEND_TIMEOUT_SEC) -> Message:
        raw = await self._client.call(
            SEND_PERSONAL_MAIL_PATH,
            request.SerializeToString(),
            timeout=timeout,
        )
        return self._protocol.parse("mailpb.SendMailResponse", raw)


class RewardWorker:
    """2 秒轮询租约式排行奖励邮件投递工作器。"""

    def __init__(
        self,
        protocol: Protocol,
        store: RewardStore,
        mail_client: MailAdminClient,
        *,
        mail_secret: bytes,
        poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.mail_client = mail_client
        self.mail_secret = mail_secret
        self.poll_interval_sec = poll_interval_sec
        self._clock_ms = clock_ms

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """循环投递到期奖励，直至被取消或收到 ``stop_event``。"""
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            try:
                if stop_event is not None:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval_sec)
                        return
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(self.poll_interval_sec)
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("rank mail reward delivery tick failed", exc_info=True)

    async def tick(self) -> None:
        """扫描一批到期奖励并逐条领租约投递。"""
        now = self._clock_ms()
        ids = await self.store.due_rewards(now, DUE_SCAN_LIMIT)
        for reward_id in ids:
            await self.process(reward_id)

    async def process(self, reward_id: int) -> None:
        """领取单条奖励租约并按状态机完成推迟、投递、退避或放弃。"""
        token = secrets.randbits(64) or 1
        now = self._clock_ms()
        claimed = await self.store.claim_reward(reward_id, now, now + LEASE_DURATION_MS, token)
        if claimed is None:
            return

        async def _record(result: int, mail_id: int, stamp: int) -> None:
            status, next_ms = next_step(result, claimed.attempts, stamp, claimed.expire_ms)
            if status != REWARD_ABANDONED:
                await self.store.settle_reward(claimed, mail_id, next_ms, stamp)
                return
            await self.store.abandon_reward(claimed, stamp)
            if result == ATTEMPT_RECIPIENT_GONE:
                logger.info(
                    "rank mail reward dropped recipient_deleted=true reward_id=%d settlement_id=%d",
                    claimed.reward_id,
                    claimed.settlement_id,
                )
            else:
                logger.error(
                    "rank mail reward abandoned needs_manual_followup=true reward_id=%d settlement_id=%d "
                    "attempts=%d expire_ms=%d result=%d",
                    claimed.reward_id,
                    claimed.settlement_id,
                    claimed.attempts + 1,
                    claimed.expire_ms,
                    result,
                )

        # 解析附件原件并核查写定的过期时刻
        atts = self.protocol.message("mailpb.MailAttachments")
        corrupt = claimed.expire_ms == 0
        if not corrupt:
            try:
                atts.ParseFromString(claimed.attachments)
            except DecodeError:
                corrupt = True
        if corrupt:
            await _record(ATTEMPT_REJECTED, 0, now)
            return

        # 剩余可领不足 24h 且尚未到过期后 1h 判重探询时刻：不发新邮件，不计尝试次数
        probe_ms, deferred = send_deferred_to(now, claimed.expire_ms)
        if deferred:
            await self.store.defer_reward(claimed, probe_ms, now)
            logger.error(
                "rank mail reward deferred past_cutoff=true reward_id=%d settlement_id=%d "
                "attempts=%d expire_ms=%d check_at_ms=%d",
                claimed.reward_id,
                claimed.settlement_id,
                claimed.attempts,
                claimed.expire_ms,
                probe_ms,
            )
            return

        req = self.protocol.message(
            "mailpb.SendMailRequest",
            sender_kind=REWARD_SENDER_KIND,
            request_id=claimed.reward_id,
            recipient_player_id=claimed.player_id,
            title=REWARD_MAIL_TITLE,
            body=REWARD_MAIL_BODY,
            expire_ms=claimed.expire_ms,
        )
        req.attachments.CopyFrom(atts)
        sign_service_call(SEND_PERSONAL_MAIL_PATH, req, MAIL_CALLER, now, self.mail_secret)

        response: Message | None = None
        call_err: BaseException | None = None
        try:
            response = await self.mail_client.send_personal_mail(req, timeout=SEND_TIMEOUT_SEC)
        except Exception as exc:
            call_err = exc

        result = classify_mail_reply(response, call_err)
        mail_id = int(response.mail_id) if result == ATTEMPT_DELIVERED and response is not None else 0
        await _record(result, mail_id, self._clock_ms())
