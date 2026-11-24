"""playerlocator.PlayerLocator gRPC 服务实现（7 个 RPC，严格对齐 Go 栅栏与 CAS 语义）。"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import grpc

from ..protocol import Protocol
from . import keys
from .config import HOLDER_RELEASE_FIRST_RETRY_DELAY_SEC, PlayerLocatorConfig
from .lease_monitor import (
    HolderReleasePendingError,
    SceneManagerClient,
    notify_scene_manager_leave,
)
from .store import ReplaceSessionResult, SessionStore

logger = logging.getLogger(__name__)

SERVICE_NAME = "playerlocator.PlayerLocator"


def login_generation_advances(current: int, incoming: int) -> bool:
    """登录代次提交栅栏：双非 0 要求严格递增，任一方为 0 放行（兼容无代次写入）。"""
    return current == 0 or incoming == 0 or incoming > current


def is_stale_gate_instance_report(record_instance_id: str, request_instance_id: str) -> bool:
    """判断断线/登出上报是否来自旧 gate 实例：双非空且不等时视为陈旧。"""
    return bool(record_instance_id and request_instance_id and record_instance_id != request_instance_id)


async def _abort(context, code: grpc.StatusCode, message: str):
    if context is not None:
        await context.abort(code, message)
    raise RuntimeError(f"{code.name}: {message}")


class PlayerLocatorService:
    """玩家会话定位与生命周期状态机服务，无 RPC 鉴权（与 Go 版一致）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: SessionStore,
        config: PlayerLocatorConfig | None = None,
        *,
        scene_manager: SceneManagerClient | None = None,
        wake_lease_monitor: Callable[[int, float], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self.config = config or PlayerLocatorConfig()
        self.scene_manager = scene_manager
        self.wake_lease_monitor = wake_lease_monitor
        self.clock = clock
        self.stale_gate_instance_counts: dict[str, int] = {
            "set_disconnecting": 0,
            "mark_offline": 0,
        }

    def services(self) -> dict[str, dict]:
        return {
            SERVICE_NAME: {
                "SetLocation": self.set_location,
                "GetLocation": self.get_location,
                "MarkOffline": self.mark_offline,
                "SetSession": self.set_session,
                "GetSession": self.get_session,
                "SetDisconnecting": self.set_disconnecting,
                "Reconnect": self.reconnect,
            }
        }

    def _empty(self):
        return self.protocol.message("Empty")

    async def set_location(self, request, context, _path: str = ""):
        logger.error(
            "SetLocation rejected deprecated endpoint uid=%d server=%s",
            request.uid, request.server_id,
        )
        await _abort(
            context,
            grpc.StatusCode.FAILED_PRECONDITION,
            "SetLocation is deprecated and disabled; use scene_manager EnterScene/LeaveScene",
        )

    async def get_location(self, request, context, _path: str = ""):
        logger.error("GetLocation rejected deprecated endpoint uid=%d", request.uid)
        await _abort(
            context,
            grpc.StatusCode.FAILED_PRECONDITION,
            "GetLocation is deprecated and disabled; authoritative location is owned by scene_manager",
        )

    async def get_session(self, request, context, _path: str = ""):
        player_id = int(request.player_id)
        if not request.include_placement:
            raw = await self.store.get_session_bytes(player_id)
            if raw is None:
                return self.protocol.message("playerlocator.GetSessionResponse", found=False)
            session = self.protocol.parse("playerlocator.PlayerSession", raw)
            return self.protocol.message("playerlocator.GetSessionResponse", session=session, found=True)

        raw_session, raw_location = await self.store.get_session_and_location_bytes(player_id)
        placement = self.protocol.message("playerlocator.PlayerScenePlacement", found=False)
        if raw_location:
            loc = self.protocol.parse("storage.PlayerLocation", raw_location)
            placement = self.protocol.message(
                "playerlocator.PlayerScenePlacement",
                found=True,
                zone_id=loc.zone_id,
                node_id=loc.node_id,
                scene_id=loc.scene_id,
                owner_epoch=loc.owner_epoch,
                awaiting_placement=(loc.node_id == "" and loc.owner_epoch != 0),
                session_id=loc.session_id,
                login_generation=loc.login_generation,
            )
        if raw_session is None:
            return self.protocol.message(
                "playerlocator.GetSessionResponse", found=False, placement=placement,
            )
        session = self.protocol.parse("playerlocator.PlayerSession", raw_session)
        return self.protocol.message(
            "playerlocator.GetSessionResponse", session=session, found=True, placement=placement,
        )

    async def set_session(self, request, context, _path: str = ""):
        if not request.HasField("session"):
            await _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "session is required")
        incoming = request.session
        if incoming.player_id == 0:
            await _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "session player_id is required")

        session = type(incoming)()
        session.CopyFrom(incoming)
        player_id = int(session.player_id)

        expected = await self.store.get_session_bytes(player_id)
        previous = None
        if expected is not None:
            current = self.protocol.parse("playerlocator.PlayerSession", expected)
            previous = current

            # 仅继承 scene_id / scene_node_id；刻意不继承 login_generation / session_incarnation / gate_zone_id
            if session.scene_id == 0:
                session.scene_id = current.scene_id
            if session.scene_node_id == "":
                session.scene_node_id = current.scene_node_id

            # 幂等重放判断必须排在代次栅栏之前，避免丢包重放因代次相等被误拒
            if session == current:
                pass
            elif session.session_version != current.session_version + 1:
                await _abort(
                    context,
                    grpc.StatusCode.ABORTED,
                    f"stale session update for player {player_id}: "
                    f"current version={current.session_version} incoming={session.session_version}",
                )
            elif not login_generation_advances(current.login_generation, session.login_generation):
                await _abort(
                    context,
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"stale session update for player {player_id}: "
                    f"current login_generation={current.login_generation} incoming={session.login_generation}",
                )

        updated = session.SerializeToString()
        result = await self.store.replace_session_and_cancel_lease(
            player_id,
            expected,
            updated,
            0,
            keys.indexed_gate(previous),
            keys.indexed_gate(session),
        )
        if result == ReplaceSessionResult.CLEANUP_PENDING:
            await _abort(
                context,
                grpc.StatusCode.FAILED_PRECONDITION,
                f"previous session cleanup is still pending for player {player_id}",
            )
        if result != ReplaceSessionResult.APPLIED:
            await _abort(
                context,
                grpc.StatusCode.ABORTED,
                f"session changed concurrently for player {player_id}",
            )
        return self._empty()

    async def set_disconnecting(self, request, context, _path: str = ""):
        player_id = int(request.player_id)
        raw = await self.store.get_session_bytes(player_id)
        if raw is None:
            return self._empty()

        session = self.protocol.parse("playerlocator.PlayerSession", raw)
        if session.session_id != request.session_id:
            logger.info(
                "SetDisconnecting ignored session mismatch player=%d current=%d requested=%d",
                player_id, session.session_id, request.session_id,
            )
            return self._empty()

        if is_stale_gate_instance_report(session.gate_instance_id, request.gate_instance_id):
            self.stale_gate_instance_counts["set_disconnecting"] += 1
            logger.info(
                "SetDisconnecting ignored stale gate instance player=%d session=%d record=%s request=%s",
                player_id, request.session_id, session.gate_instance_id, request.gate_instance_id,
            )
            return self._empty()

        if (
            session.session_incarnation != 0
            and request.session_incarnation != 0
            and session.session_incarnation != request.session_incarnation
        ):
            logger.info(
                "SetDisconnecting ignored stale incarnation player=%d session=%d record=%d request=%d",
                player_id, request.session_id, session.session_incarnation, request.session_incarnation,
            )
            return self._empty()

        ttl_sec = int(request.lease_ttl_seconds) or self.config.default_disconnect_lease_sec or 30
        if session.state == keys.SESSION_STATE_DISCONNECTING:
            # 同一断线事件重放不延长首次定下的租约截止时间，也不推进版本号
            return self._empty()
        if session.state != keys.SESSION_STATE_ONLINE:
            logger.info("SetDisconnecting ignored non-online state player=%d state=%d", player_id, session.state)
            return self._empty()

        previous_gate = keys.indexed_gate(session)
        session.state = keys.SESSION_STATE_DISCONNECTING
        session.session_version += 1
        updated = session.SerializeToString()
        deadline_unix_sec = int(self.clock() + ttl_sec)

        swapped = await self.store.transition_to_disconnecting(
            player_id, raw, updated, deadline_unix_sec, previous_gate,
        )
        if not swapped:
            logger.info(
                "SetDisconnecting ignored CAS mismatch player=%d session=%d",
                player_id, request.session_id,
            )
            return self._empty()

        logger.info(
            "SetDisconnecting committed player=%d session=%d version=%d ttl_sec=%d",
            player_id, request.session_id, session.session_version, ttl_sec,
        )
        return self._empty()

    async def reconnect(self, request, context, _path: str = ""):
        player_id = int(request.player_id)
        raw = await self.store.get_session_bytes(player_id)
        if raw is None:
            return self.protocol.message(
                "playerlocator.ReconnectResponse",
                success=False,
                error_message="no session found for player",
            )

        session = self.protocol.parse("playerlocator.PlayerSession", raw)
        if (
            session.state == keys.SESSION_STATE_ONLINE
            and session.session_id == request.new_session_id
            and session.request_id == request.request_id
        ):
            # 同幂等键 (session_id, request_id) 配不同登录代次不是合法重放，拒绝且不改记录
            if session.login_generation != request.login_generation:
                logger.info(
                    "Reconnect rejected replay generation mismatch player=%d session=%d request_id=%s current=%d incoming=%d",
                    player_id, session.session_id, request.request_id,
                    session.login_generation, request.login_generation,
                )
                return self.protocol.message(
                    "playerlocator.ReconnectResponse",
                    success=False,
                    error_message=(
                        f"stale reconnect: replay generation mismatch "
                        f"current={session.login_generation} incoming={request.login_generation}"
                    ),
                )
            return self.protocol.message("playerlocator.ReconnectResponse", success=True, session=session)

        if session.state != keys.SESSION_STATE_DISCONNECTING:
            return self.protocol.message(
                "playerlocator.ReconnectResponse",
                success=False,
                error_message="session is no longer disconnecting",
            )
        if request.account and session.account and request.account != session.account:
            return self.protocol.message(
                "playerlocator.ReconnectResponse",
                success=False,
                error_message="session account mismatch",
            )
        if not login_generation_advances(session.login_generation, request.login_generation):
            return self.protocol.message(
                "playerlocator.ReconnectResponse",
                success=False,
                error_message=(
                    f"stale login generation: current={session.login_generation} "
                    f"incoming={request.login_generation}"
                ),
            )

        previous_gate = keys.indexed_gate(session)
        session.session_id = request.new_session_id
        session.gate_id = request.gate_id
        session.gate_instance_id = request.gate_instance_id
        # 会话代次、gate 区号与登录代次均按请求原样覆盖（含 0）
        session.session_incarnation = request.session_incarnation
        session.gate_zone_id = request.gate_zone_id
        session.token_id = request.token_id
        session.token_expiry_ms = request.token_expiry_ms
        session.request_id = request.request_id
        session.login_generation = request.login_generation
        session.session_version += 1
        session.state = keys.SESSION_STATE_ONLINE
        session.last_active_ts = int(self.clock() * 1000)

        updated = session.SerializeToString()
        result = await self.store.replace_session_and_cancel_lease(
            player_id,
            raw,
            updated,
            0,
            previous_gate,
            keys.indexed_gate(session),
        )
        if result != ReplaceSessionResult.APPLIED:
            return self.protocol.message(
                "playerlocator.ReconnectResponse",
                success=False,
                error_message="session changed concurrently",
            )

        logger.info(
            "Reconnect committed player=%d new_session=%d gate=%s version=%d login_generation=%d",
            player_id, request.new_session_id, request.gate_id, session.session_version, session.login_generation,
        )
        return self.protocol.message("playerlocator.ReconnectResponse", success=True, session=session)

    async def mark_offline(self, request, context, _path: str = ""):
        player_id = int(request.player_id)
        if player_id == 0:
            await _abort(context, grpc.StatusCode.FAILED_PRECONDITION, "player_id is required")
        if request.expected_session_id == 0 or request.expected_session_version == 0:
            await _abort(
                context,
                grpc.StatusCode.FAILED_PRECONDITION,
                "expected_session_id and expected_session_version are required",
            )

        raw = await self.store.get_session_bytes(player_id)
        if raw is None:
            return self._empty()

        current = self.protocol.parse("playerlocator.PlayerSession", raw)
        if int(current.player_id) != player_id:
            await _abort(
                context,
                grpc.StatusCode.FAILED_PRECONDITION,
                f"session player mismatch: key player={player_id} payload player={current.player_id}",
            )
        if (
            current.session_id != request.expected_session_id
            or current.session_version != request.expected_session_version
        ):
            logger.info(
                "MarkOffline ignored stale cleanup player=%d expected=%d/%d current=%d/%d",
                player_id, request.expected_session_id, request.expected_session_version,
                current.session_id, current.session_version,
            )
            return self._empty()

        if is_stale_gate_instance_report(current.gate_instance_id, request.expected_gate_instance_id):
            self.stale_gate_instance_counts["mark_offline"] += 1
            logger.info(
                "MarkOffline ignored stale gate instance player=%d session=%d/%d record=%s request=%s",
                player_id, current.session_id, current.session_version,
                current.gate_instance_id, request.expected_gate_instance_id,
            )
            return self._empty()

        deleted = await self.store.delete_session_if_unchanged(player_id, raw, keys.indexed_gate(current))
        if not deleted:
            logger.info("MarkOffline ignored CAS mismatch player=%d", player_id)
            return self._empty()

        await self._notify_scene_manager_leave_or_enqueue(player_id, current)
        return self._empty()

    async def _notify_scene_manager_leave_or_enqueue(self, player_id: int, session) -> None:
        try:
            await notify_scene_manager_leave(self.protocol, self.store, self.scene_manager, session)
            return
        except HolderReleasePendingError as exc:
            ready_at = self.clock() + HOLDER_RELEASE_FIRST_RETRY_DELAY_SEC
            logger.info(
                "MarkOffline holder release pending player=%d retry_delay_sec=%.1f detail=%s",
                player_id, HOLDER_RELEASE_FIRST_RETRY_DELAY_SEC, exc,
            )
        except Exception as exc:
            ready_at = self.clock()
            logger.error("MarkOffline LeaveScene failed player=%d error=%s", player_id, exc)

        # 正常登出重试快照置为 DISCONNECTING 且清空 gate 信息：玩家连接仍停在选角界面，绝不发 LeaseExpired 踢线
        retry_session = type(session)()
        retry_session.CopyFrom(session)
        retry_session.state = keys.SESSION_STATE_DISCONNECTING
        retry_session.gate_id = ""
        retry_session.gate_instance_id = ""
        retry_bytes = retry_session.SerializeToString()

        try:
            enqueued = await self.store.enqueue_offline_cleanup(player_id, retry_bytes, int(ready_at))
        except Exception as exc:
            logger.error("MarkOffline enqueue cleanup retry failed player=%d error=%s", player_id, exc)
            return

        if not enqueued:
            logger.info("MarkOffline skipped cleanup enqueue because player re-logged in player=%d", player_id)
            return

        if self.wake_lease_monitor is not None:
            self.wake_lease_monitor(keys.player_slot(player_id), ready_at)
