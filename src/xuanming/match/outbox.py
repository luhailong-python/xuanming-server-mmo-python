"""终局发件箱投递器（OutcomeOutboxWorker）：

1. 将 match_battle_completion 中已提交 COMPLETE 的 BattleResultEvent 原件发布至 Kafka ``match-results``；
2. 将 match_battle_settlement_source 中的个人结算原件投递至 PlayerInbox：
   - 首投（inbox_seq == 0）调用 ``/player_inbox.PlayerInbox/EnqueueBattleSettlement``；
   - 等待确认（inbox_seq > 0）调用 ``/player_inbox.PlayerInbox/LookupBattleSettlement``；
   - 以调用方 ``"match"`` 签名，严格校验 ``"inbox"`` 回包签名、``request_digest`` 与 ``content_sha256``；
   - 仅当回执确认 ``PLAYER_INBOX_ITEM_APPLIED (2)`` 后，才标记 ``dispatch_state = 2`` 并释放玩家匹配票据。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..grpc_runtime import service_digest, sign_service_call, verify_service_call
from ..protocol import Protocol
from .model import (
    INBOX_ITEM_STATE_APPLIED,
    INBOX_ITEM_STATE_PENDING,
    INBOX_MSG_BATTLE_SETTLEMENT,
    SETTLEMENT_DISPATCH_APPLIED,
    SETTLEMENT_DISPATCH_PENDING,
    inbox_content_sha256,
    pending_settlement_delay_ms,
)
from .store import MatchStore, SettlementOutboxRow

logger = logging.getLogger(__name__)

ENQUEUE_BATTLE_SETTLEMENT_PATH = "/player_inbox.PlayerInbox/EnqueueBattleSettlement"
LOOKUP_BATTLE_SETTLEMENT_PATH = "/player_inbox.PlayerInbox/LookupBattleSettlement"
OUTBOX_LEASE_MS = 30_000


@dataclass(frozen=True, slots=True)
class OutboxRoundStats:
    """单轮发件箱投递统计。"""

    results_published: int = 0
    settlements_pending: int = 0
    settlements_applied: int = 0
    tickets_released: int = 0
    errors: int = 0


class OutcomeOutboxWorker:
    """驱动 match_battle_completion 与 match_battle_settlement_source 的可靠发件箱投递。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MatchStore,
        *,
        inbox_call: Callable[[str, bytes], Awaitable[bytes]] | None,
        inbox_secret: bytes,
        publish_result: Callable[[int, bytes], Awaitable[None]] | None,
        result_batch: int = 8,
        first_batch: int = 8,
        waiting_batch: int = 8,
        interval_sec: float = 0.5,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._inbox_call = inbox_call
        self._inbox_secret = inbox_secret
        self._publish_result = publish_result
        self._result_batch = result_batch
        self._first_batch = first_batch
        self._waiting_batch = waiting_batch
        self._interval_sec = interval_sec
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    async def run_once(self) -> OutboxRoundStats:
        stamp = await self.store.now_ms()
        lease = stamp + OUTBOX_LEASE_MS
        published = pending = applied = released = errors = 0

        # 1. 发布整局 BattleResultEvent 到 match-results
        if self._publish_result is not None:
            for row in await self.store.due_outcome_results(stamp, self._result_batch):
                if not await self.store.claim_outcome_result(row.battle_id, row.next_retry_ms, lease):
                    continue
                if hashlib.sha256(row.raw_payload).digest() != row.payload_digest:
                    logger.error("match outbox result digest corrupt battle_id=%d", row.battle_id)
                    errors += 1
                    continue
                try:
                    await self._publish_result(row.battle_id, row.raw_payload)
                    await self.store.mark_outcome_result_published(row.battle_id, lease)
                    published += 1
                except Exception:
                    logger.warning("match outbox publish battle result failed battle_id=%d", row.battle_id, exc_info=True)
                    errors += 1

        # 2. 投递个人结算到 PlayerInbox，并仅在 APPLIED(2) 后释放匹配票
        if self._inbox_call is not None and len(self._inbox_secret) >= 32:
            rows = await self.store.due_settlement_sources(stamp, self._first_batch, self._waiting_batch)
            for row in rows:
                if not await self.store.claim_settlement_source(row.battle_id, row.player_id, row.next_retry_ms, lease):
                    continue
                if hashlib.sha256(row.settlement_proto).digest() != row.payload_digest:
                    logger.error(
                        "match outbox settlement digest corrupt battle_id=%d player_id=%d",
                        row.battle_id, row.player_id,
                    )
                    errors += 1
                    continue
                try:
                    seq, state = await self._deliver_personal_settlement(row)
                except Exception:
                    logger.warning(
                        "match outbox deliver personal settlement failed battle_id=%d player_id=%d",
                        row.battle_id, row.player_id, exc_info=True,
                    )
                    errors += 1
                    continue

                if state == INBOX_ITEM_STATE_APPLIED:
                    await self.store.update_settlement_source(
                        row.battle_id, row.player_id, lease,
                        inbox_seq=seq, inbox_state=state,
                        dispatch_state=SETTLEMENT_DISPATCH_APPLIED, next_retry_ms=0,
                    )
                    applied += 1
                    if await self.store.release_settled_tickets(row.battle_id, row.player_id):
                        released += 1
                else:
                    next_ms = stamp + pending_settlement_delay_ms(row.issued_at_ms, stamp)
                    await self.store.update_settlement_source(
                        row.battle_id, row.player_id, lease,
                        inbox_seq=seq, inbox_state=state,
                        dispatch_state=SETTLEMENT_DISPATCH_PENDING, next_retry_ms=next_ms,
                    )
                    pending += 1

        return OutboxRoundStats(
            results_published=published,
            settlements_pending=pending,
            settlements_applied=applied,
            tickets_released=released,
            errors=errors,
        )

    async def _deliver_personal_settlement(self, row: SettlementOutboxRow) -> tuple[int, int]:
        """向 PlayerInbox 投递或查询个人结算原件，并严格核验 inbox 签名回执与内容摘要。"""
        req = self.protocol.message(
            "player_inbox.BattleSettlementInboxRequest",
            player_id=row.player_id,
            battle_id=row.battle_id,
            expected_owner_account=row.owner_account,
            expected_owner_revision=row.owner_revision,
            issued_at_ms=row.issued_at_ms,
            settlement_payload=row.settlement_proto,
        )
        req_digest = service_digest(req)
        path = ENQUEUE_BATTLE_SETTLEMENT_PATH if row.inbox_seq == 0 else LOOKUP_BATTLE_SETTLEMENT_PATH
        now = int(self._clock_ms())
        sign_service_call(path, req, "match", now, self._inbox_secret)
        raw_resp = await self._inbox_call(path, req.SerializeToString(deterministic=True))
        resp = self.protocol.parse("player_inbox.BattleSettlementInboxResponse", raw_resp)

        want_content = inbox_content_sha256(INBOX_MSG_BATTLE_SETTLEMENT, row.issued_at_ms, row.settlement_proto)
        if (
            not verify_service_call(path + "/reply", resp, "inbox", int(self._clock_ms()), self._inbox_secret)
            or resp.request_digest != req_digest
            or int(resp.player_id) != row.player_id
            or int(resp.battle_id) != row.battle_id
            or int(resp.error_message.id) != 0
            or int(resp.seq) == 0
            or (row.inbox_seq != 0 and int(resp.seq) != row.inbox_seq)
            or bytes(resp.content_sha256) != want_content
            or int(resp.state) not in (INBOX_ITEM_STATE_PENDING, INBOX_ITEM_STATE_APPLIED)
        ):
            raise ValueError(
                f"invalid inbox receipt battle_id={row.battle_id} player_id={row.player_id} "
                f"seq={resp.seq} state={resp.state} err={resp.error_message.id}"
            )
        return int(resp.seq), int(resp.state)

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("match outcome outbox round failed", exc_info=True)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._interval_sec)
            except TimeoutError:
                pass
