"""mail 发件提交后的可丢失在线提醒（Kafka gate-cmd_g<gen>，at-most-once，与 Go notify/notify.go 对齐）。"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol as TypingProtocol

from google.protobuf.message import DecodeError

from ..protocol import Protocol

logger = logging.getLogger(__name__)

# 跨运行时契约常量（与 Go mail/internal/notify/notify.go 对齐）
PUSH_TO_PLAYER_EVENT_ID = 39  # ContractsKafkaPushToPlayerEvent
NOTIFY_MAIL_CHANGED_MSG_ID = 264  # ClientPlayerMailNotifyMailChanged
SESSION_STATE_ONLINE = 1  # playerlocator.PlayerSessionState.SESSION_STATE_ONLINE
DEFAULT_NOTIFY_TIMEOUT_SEC = 0.4  # 400ms 单次推送预算

_UINT32_MAX = (1 << 32) - 1


def player_session_key(player_id: int) -> str:
    """玩家域 Redis 跨运行时会话键：``player:session:{<player_id>}``。"""
    return f"player:session:{{{int(player_id)}}}"


def _to_bytes(raw) -> bytes:
    if raw is None:
        return b""
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, (bytearray, memoryview)):
        return bytes(raw)
    return str(raw).encode("utf-8")


def _parse_gate_id(raw: str) -> int | None:
    """校验并解析十进制无符号 32 位正整数 gate_id（与 Go kafkacmd.ParseNodeID 对齐）。"""
    text = (raw or "").strip()
    if not text or not text.isascii() or not text.isdigit():
        return None
    value = int(text)
    if not 1 <= value <= _UINT32_MAX:
        return None
    return value


class PlayerSessionReader(TypingProtocol):
    """读取 PlayerRedis 原始键值的最小协议（与 ``redis.asyncio.Redis.get`` 兼容）。"""

    async def get(self, key: str): ...


class GateCommandPublisher(TypingProtocol):
    """发布 GateCommand 至 ``gate-cmd_g<gen>`` 指定分区的最小协议（与 ``KafkaCommands.publish`` 兼容）。"""

    async def publish(self, role: str, node_id: int, message, *, key: bytes | None = None) -> None: ...


class KafkaMailNotifier:
    """个人邮件提交后的尽力而为 Gate 提醒器：只发不改判权威发件，失败吞掉并记录告警。"""

    def __init__(
        self,
        protocol: Protocol,
        redis_client: PlayerSessionReader | None,
        publisher: GateCommandPublisher | None,
        *,
        timeout_sec: float = DEFAULT_NOTIFY_TIMEOUT_SEC,
    ) -> None:
        if timeout_sec <= 0:
            raise ValueError(f"mail notify timeout_sec must be > 0, got {timeout_sec}")
        self._protocol = protocol
        self._redis = redis_client
        self._publisher = publisher
        self._timeout_sec = float(timeout_sec)
        self.outcomes: dict[str, int] = {"sent": 0, "offline": 0, "error": 0}

    async def personal(self, player_id: int, mail_id: int) -> None:
        """在 400ms 预算内至多尝试一次定向提醒；永不抛出异常以免改判已提交发件。"""
        try:
            async with asyncio.timeout(self._timeout_sec):
                outcome = await self._publish(int(player_id), int(mail_id))
        except Exception as exc:
            self.outcomes["error"] += 1
            logger.error(
                "mail notification not published; authoritative mail kept player=%d mail_id=%d failure=%s",
                int(player_id),
                int(mail_id),
                type(exc).__name__,
            )
            return
        self.outcomes[outcome] = self.outcomes.get(outcome, 0) + 1

    async def _publish(self, player_id: int, mail_id: int) -> str:
        if self._redis is None or self._publisher is None or player_id <= 0 or mail_id <= 0:
            raise ValueError("mail notifier unwired or invalid input")

        raw = _to_bytes(await self._redis.get(player_session_key(player_id)))
        if not raw:
            return "offline"

        try:
            session = self._protocol.parse("playerlocator.PlayerSession", raw)
        except DecodeError as exc:
            raise ValueError("corrupt PlayerSession payload") from exc

        if int(session.state) != SESSION_STATE_ONLINE:
            return "offline"
        if int(session.player_id) != player_id:
            raise ValueError("PlayerSession player_id mismatch")

        gate_instance_id = session.gate_instance_id or ""
        session_id = int(session.session_id)
        if not gate_instance_id or session_id <= 0:
            raise ValueError("PlayerSession missing gate_instance_id or session_id")

        gate_id = _parse_gate_id(session.gate_id)
        if gate_id is None:
            raise ValueError(f"invalid gate_id {session.gate_id!r}")

        body = self._protocol.message("mailpb.MailChangedEvent", mail_id=mail_id).SerializeToString()
        # player_id 作为 Gate 目标玩家身份围栏：session 复用时若当前归属不符由 Gate 直接丢弃。
        message_content = self._protocol.message(
            "MessageContent",
            message_id=NOTIFY_MAIL_CHANGED_MSG_ID,
            serialized_message=body,
        )
        push_event = self._protocol.message(
            "contracts.kafka.PushToPlayerEvent",
            session_id=session_id,
            player_id=player_id,
            message_content=message_content,
        )
        command = self._protocol.message(
            "contracts.kafka.GateCommand",
            event_id=PUSH_TO_PLAYER_EVENT_ID,
            player_id=player_id,
            session_id=session_id,
            target_gate_id=gate_id,
            target_instance_id=gate_instance_id,
            payload=push_event.SerializeToString(),
        )
        await self._publisher.publish(
            "gate",
            gate_id,
            command,
            key=str(player_id).encode("ascii"),
        )
        return "sent"
