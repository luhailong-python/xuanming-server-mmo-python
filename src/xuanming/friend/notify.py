"""friend 写事务提交后的 S2C 实时推送（Kafka gate-cmd_g<gen>，at-most-once，与 Go push.go 对齐）。"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Protocol as TypingProtocol

from ..protocol import Protocol
from .model import (
    FRIEND_EVENT_REASON_REQUEST_ACCEPTED,
    FRIEND_EVENT_REASON_REQUEST_RECEIVED,
    NOTIFY_FRIEND_EVENT_MSG_ID,
    PUSH_TIMEOUT_SEC,
    PUSH_TO_PLAYER_EVENT_ID,
    SESSION_STATE_ONLINE,
)
from .redis_client import SessionStore

logger = logging.getLogger(__name__)

_ALLOWED_REASONS = frozenset((
    FRIEND_EVENT_REASON_REQUEST_RECEIVED,
    FRIEND_EVENT_REASON_REQUEST_ACCEPTED,
))


class GateCommandPublisher(TypingProtocol):
    """发布 GateCommand 至 ``gate-cmd_g<N>`` 的最小协议（与 ``KafkaCommands.publish`` 签名一致）。"""

    async def publish(self, role: str, node_id: int, message, *, key: bytes | None = None) -> None: ...


class FriendPusher(TypingProtocol):
    """可注入的好友 S2C 推送协议（实现不得向外抛异常）。"""

    async def push_friend_event(self, to_player_id: int, reason: int, by_player_id: int) -> None: ...


class NullFriendPusher:
    """未配置 Kafka 时的空推送器：仅记录告警，不阻断好友业务。"""

    async def push_friend_event(self, to_player_id: int, reason: int, by_player_id: int) -> None:
        logger.warning(
            "friend push channel disabled; dropping event reason=%d to=%d by=%d",
            reason,
            to_player_id,
            by_player_id,
        )


class KafkaFriendPusher:
    """生产推送器：读 PlayerSession -> 校验 ONLINE 与门禁字段 -> 发送 GateCommand(event_id=39, msg_id=235)。"""

    def __init__(
        self,
        protocol: Protocol,
        sessions: SessionStore,
        publisher: GateCommandPublisher,
        *,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        timeout_sec: float = PUSH_TIMEOUT_SEC,
    ) -> None:
        self._protocol = protocol
        self._sessions = sessions
        self._publisher = publisher
        self._clock_ms = clock_ms
        self._timeout_sec = timeout_sec

    async def push_friend_event(self, to_player_id: int, reason: int, by_player_id: int) -> None:
        """只允许在事务提交之后调用；一切失败仅打日志，绝不改变 RPC 结果。"""
        if reason not in _ALLOWED_REASONS:
            logger.error("friend push rejected unknown reason=%d to=%d", reason, to_player_id)
            return
        if to_player_id <= 0:
            logger.error("friend push rejected zero recipient reason=%d", reason)
            return
        try:
            async with asyncio.timeout(self._timeout_sec):
                await self._push_once(to_player_id, reason, by_player_id)
        except Exception as exc:
            logger.error(
                "friend push failed reason=%d to=%d by=%d error=%s",
                reason,
                to_player_id,
                by_player_id,
                exc,
            )

    async def _push_once(self, to_player_id: int, reason: int, by_player_id: int) -> None:
        sess = await self._sessions.load_session(to_player_id)
        if sess is None or int(sess.state) != SESSION_STATE_ONLINE:
            return
        gate_instance_id = sess.gate_instance_id or ""
        if not gate_instance_id:
            logger.error("friend push rejected empty gate_instance_id to=%d reason=%d", to_player_id, reason)
            return
        try:
            gate_id = int(sess.gate_id)
        except (TypeError, ValueError):
            logger.error(
                "friend push rejected non-integer gate_id=%r to=%d reason=%d",
                sess.gate_id,
                to_player_id,
                reason,
            )
            return
        if gate_id <= 0:
            logger.error("friend push rejected non-positive gate_id=%d to=%d reason=%d", gate_id, to_player_id, reason)
            return

        body = self._protocol.message(
            "friendpb.FriendEventS2C",
            reason=reason,
            by_player_id=by_player_id,
            ts_ms=self._clock_ms(),
        ).SerializeToString()
        mc = self._protocol.message(
            "MessageContent",
            message_id=NOTIFY_FRIEND_EVENT_MSG_ID,
            serialized_message=body,
        )
        push_event = self._protocol.message(
            "contracts.kafka.PushToPlayerEvent",
            session_id=int(sess.session_id),
            message_content=mc,
            player_id=to_player_id,
        )
        cmd = self._protocol.message(
            "contracts.kafka.GateCommand",
            event_id=PUSH_TO_PLAYER_EVENT_ID,
            player_id=to_player_id,
            session_id=int(sess.session_id),
            target_gate_id=gate_id,
            target_instance_id=gate_instance_id,
            payload=push_event.SerializeToString(),
        )
        await self._publisher.publish(
            "gate", gate_id, cmd, key=str(to_player_id).encode("ascii")
        )
