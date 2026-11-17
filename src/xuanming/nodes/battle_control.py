"""Battle control-plane adapters for Match SQL decisions and targeted Kafka commands."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from collections.abc import Awaitable, Callable

import grpc
from google.protobuf.message import DecodeError, Message

from xuanming.grpc_runtime import GrpcClient
from xuanming.nodes.battle import BattleNode
from xuanming.protocol import Protocol

_LOG = logging.getLogger(__name__)
_COMMIT = "/match.MatchInternal/CommitBattleOutcome"
_SIGNATURE_WINDOW_MS = 300_000
_SHUTDOWN_DRAIN_SECONDS = 15
_SHUTDOWN_RETRY_SECONDS = 1


def service_digest(message: Message) -> str:
    canonical = type(message)()
    canonical.CopyFrom(message)
    canonical.ClearField("auth")
    return hashlib.sha256(canonical.SerializeToString(deterministic=True)).hexdigest()


def _signature(path: str, caller: str, timestamp: int, digest: str, secret: bytes) -> str:
    canonical = "\n".join(("v1", path, caller, str(timestamp), digest)).encode("ascii")
    return hmac.new(secret, canonical, "sha256").hexdigest()


def sign_service_call(path: str, message: Message, caller: str, timestamp: int, secret: bytes) -> None:
    signature = _signature(path, caller, timestamp, service_digest(message), secret)
    message.auth.caller = caller
    message.auth.timestamp_ms = timestamp
    message.auth.signature_hex = signature


def verify_service_call(path: str, message: Message, caller: str, timestamp: int, secret: bytes) -> bool:
    auth = message.auth
    sent = auth.timestamp_ms
    if (auth.caller != caller or not 0 < sent < (1 << 63) or abs(timestamp - sent) > _SIGNATURE_WINDOW_MS
            or len(auth.signature_hex) != 64 or not auth.signature_hex.isascii()):
        return False
    expected = _signature(path, caller, sent, service_digest(message), secret)
    return hmac.compare_digest(expected, auth.signature_hex)


class BattleControl:
    def __init__(
        self, protocol: Protocol, *, match_endpoint: str, outcome_secret: bytes | str,
        publish: Callable[[str, int, int, Message], Awaitable[None]] | None = None, match_client=None,
    ):
        secret = outcome_secret.encode() if isinstance(outcome_secret, str) else outcome_secret
        self.secret = secret.strip()
        if not match_endpoint or len(self.secret) < 32:
            raise ValueError("Battle Match endpoint and a 32-byte outcome signing key are required")
        self.protocol = protocol
        self.match = match_client or GrpcClient(match_endpoint)
        self.publish = publish
        self.node: BattleNode | None = None
        self._pending: dict[int, asyncio.Task] = {}
        self._commands: asyncio.Queue[tuple[str, int, int, Message]] = asyncio.Queue(maxsize=4096)
        self._sender: asyncio.Task | None = None
        self._close_lock = asyncio.Lock()
        self._closed = False

    def bind(self, node: BattleNode) -> None:
        self.node = node

    def outcome(self, battle_id: int, outcome: int) -> None:
        if not self._closed and self.node is not None and battle_id not in self._pending:
            self._pending[battle_id] = asyncio.create_task(self._commit(battle_id), name=f"battle-outcome-{battle_id}")

    async def _commit(self, battle_id: int) -> None:
        try:
            node = self.node
            if node is None or (request := node.outcome_request(battle_id)) is None:
                return
            digest = service_digest(request)
            sign_service_call(_COMMIT, request, "battle", time.time_ns() // 1_000_000, self.secret)
            body = await self.match.call(_COMMIT, request.SerializeToString(deterministic=True))
            response = self.protocol.parse("match.CommitBattleOutcomeResponse", body)
            if response.request_digest != digest or not verify_service_call(
                _COMMIT + "/reply", response, "match", time.time_ns() // 1_000_000, self.secret,
            ):
                _LOG.warning("invalid Match outcome receipt for battle %s", battle_id)
            elif response.decision in (1, 2):
                node.decide_outcome(battle_id, committed=response.decision == 1)
        except (grpc.RpcError, DecodeError):
            _LOG.warning("Match outcome unavailable for battle %s", battle_id, exc_info=True)
        finally:
            self._pending.pop(battle_id, None)

    def lobby(self, routing: Message, player_id: int, message_id: int, message: Message) -> None:
        if (not self.publish or not player_id or not routing.gate_node_id or not routing.gate_instance_id
                or message_id not in (143, 177)):
            return
        event = self.protocol.message(
            "contracts.kafka.PushToPlayerEvent", session_id=routing.session_id, player_id=player_id,
            message_content=self.protocol.content(message_id, body=message.SerializeToString()),
        )
        command = self.protocol.message(
            "contracts.kafka.GateCommand", event_id=self.protocol.event_ids["ContractsKafkaPushToPlayerEvent"],
            player_id=player_id, target_gate_id=routing.gate_node_id, target_instance_id=routing.gate_instance_id,
            payload=event.SerializeToString(),
        )
        self._send("gate", routing.gate_node_id, player_id, command)

    def confirm(self, routing: Message, player_id: int, battle_id: int, deadline_ms: int) -> None:
        if not self.publish or not player_id or not routing.scene_node_id or not routing.scene_instance_id:
            return
        event = self.protocol.message("BattleConfirmedEvent", battle_id=battle_id, player_id=player_id,
                                      deadline_ms=deadline_ms)
        command = self.protocol.message(
            "contracts.kafka.SceneCommand", event_id=self.protocol.event_ids["BattleConfirmedEvent"],
            player_id=player_id, target_scene_id=routing.scene_node_id, target_instance_id=routing.scene_instance_id,
            payload=event.SerializeToString(),
        )
        self._send("scene", routing.scene_node_id, player_id, command)

    def spectate(self, channel: str, gate_id: int, battle_id: int, event: Message) -> None:
        if self.publish:
            self._send(channel, gate_id, battle_id, event)

    def _send(self, role: str, node_id: int, player_id: int, message: Message) -> None:
        if self._closed:
            return
        try:
            self._commands.put_nowait((role, node_id, player_id, message))
        except asyncio.QueueFull:
            _LOG.error("battle command queue saturated for %s %s", role, node_id)
            return
        if self._sender is None:
            self._sender = asyncio.create_task(self._send_commands(), name="battle-command-publisher")

    async def _send_commands(self) -> None:
        while True:
            command = await self._commands.get()
            try:
                if self.publish:
                    await self.publish(*command)
            except Exception:
                _LOG.exception("battle command failed for %s %s", command[0], command[1])
            finally:
                self._commands.task_done()

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            node = self.node
            if node is not None:
                node.begin_shutdown()
            try:
                async with asyncio.timeout(_SHUTDOWN_DRAIN_SECONDS):
                    if node is not None:
                        await self._drain_outcomes(node)
                    await self._commands.join()
            except TimeoutError:
                pending = node.pending_outcome_ids() if node is not None else ()
                _LOG.warning("battle control drain deadline reached; unresolved outcomes: %s", pending)
            finally:
                self._closed = True
                if node is not None:
                    node.stop_spectate_keepalive()
                tasks = tuple(self._pending.values())
                if self._sender is not None:
                    tasks += (self._sender,)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self.match.close()

    async def _drain_outcomes(self, node: BattleNode) -> None:
        loop = asyncio.get_running_loop()
        while pending := node.pending_outcome_ids():
            node.pump_spectate_keepalive()
            retry_at = loop.time() + _SHUTDOWN_RETRY_SECONDS
            for battle_id in pending:
                self.outcome(battle_id, 0)
            await asyncio.wait(tuple(self._pending.values()), timeout=_SHUTDOWN_RETRY_SECONDS)
            if node.pending_outcome_ids():
                await asyncio.sleep(max(0, retry_at - loop.time()))
