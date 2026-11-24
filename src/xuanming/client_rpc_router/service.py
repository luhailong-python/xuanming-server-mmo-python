"""client_rpc_router 核心转发逻辑（严格对齐 Go forwardlogic.go 执行顺序与错误映射）。"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import Counter

import grpc

from ..protocol import Protocol
from .breaker import EndpointCircuitBreaker
from .config import RouterConfig, parse_release_track
from .discovery import ChannelPool, TargetDirectory, pick_target
from .routes import BATTLE_NODE_TYPE, RouteEntry, build_route_table

logger = logging.getLogger(__name__)

TIP_SERVICE_UNAVAILABLE = 1003
TIP_INVALID_PARAMETER = 1005
_SESSION_DETAIL_KEY = "x-session-detail-bin"
_LOG_SAMPLE_MASK = 1023


class ClientRpcRouterService:
    """将 Gate 的 ForwardRequest 按 message_id 路由至下游业务微服务并透传会话元数据。"""

    def __init__(
        self,
        protocol: Protocol,
        config: RouterConfig,
        directories: dict[int, TargetDirectory],
        *,
        peers: TargetDirectory | None = None,
        pool: ChannelPool | None = None,
        breaker: EndpointCircuitBreaker | None = None,
        kill_switch=None,
        routes=None,
        self_uuid: str = "",
        rng: random.Random | None = None,
    ) -> None:
        self.protocol = protocol
        self.config = config
        self.routes: dict[int, RouteEntry] = dict(build_route_table(protocol) if routes is None else routes)
        self.directories = directories
        self.peers = peers
        self.pool = pool or ChannelPool(
            forward_timeout_ms=config.forward_timeout_ms,
            idle_timeout_sec=config.idle_conn_timeout_sec,
        )
        self.breaker = breaker or EndpointCircuitBreaker(
            failure_threshold=config.breaker_failure_threshold,
            open_duration_ms=config.breaker_open_duration_ms,
        )
        self.kill_switch = kill_switch
        self.self_uuid = self_uuid
        self._zone_scoped_names = frozenset(config.zone_scoped_node_types)
        self._rng = rng or random.Random()
        self._outcome_counts: Counter[str] = Counter()

    def set_identity(self, node_uuid: str) -> None:
        self.self_uuid = node_uuid

    def on_endpoint_evicted(self, endpoint: str) -> None:
        self.pool.evict(endpoint)
        self.breaker.remove(endpoint)

    def services(self) -> dict:
        return {"client_rpc_router.ClientRpcRouter": {"Forward": self.forward}}

    def _record_failure(self, outcome: str, message_id: int, endpoint: str = "") -> None:
        self._outcome_counts[outcome] += 1
        if (self._outcome_counts[outcome] - 1) & _LOG_SAMPLE_MASK == 0:
            logger.warning("router forward failed outcome=%s message_id=%d endpoint=%s", outcome, message_id, endpoint)

    def _error_envelope(self, call_id: int, message_id: int, tip_code: int):
        return self.protocol.content(message_id, call_id, error=tip_code)

    async def forward(self, request, context, _path: str = ""):
        # Step 0: 解析 release_track（非法时延迟到 Step 3a 拒绝，保持与 Go 完全一致的顺序）
        session_track = parse_release_track(request.release_track)

        # Step 1a: 必须携带 x-session-detail-bin，否则返回 gRPC UNAUTHENTICATED 且不回显头
        incoming_md = tuple(context.invocation_metadata() or ())
        session_values = [value for key, value in incoming_md if key == _SESSION_DETAIL_KEY]
        if not session_values:
            self._record_failure("missing_session", request.request.message_id if request.HasField("request") else 0)
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing x-session-detail-bin")

        # Step 1b: 立即排入响应 initial_metadata，使成功与业务错误包均携带原样字节
        await context.send_initial_metadata(tuple((_SESSION_DETAIL_KEY, value) for value in session_values))

        # Step 1c: 校验 request 存在
        if not request.HasField("request"):
            self._record_failure("invalid_request", 0)
            return self._error_envelope(0, 0, TIP_INVALID_PARAMETER)

        client_req = request.request
        msg_id = int(client_req.message_id)
        call_id = int(client_req.id)

        # Step 2a-2c: 查表、校验 client_protocol 与禁止路由 BattleNodeService
        route = self.routes.get(msg_id)
        if route is None:
            self._record_failure("unknown_message", msg_id)
            return self._error_envelope(call_id, msg_id, TIP_INVALID_PARAMETER)
        if not route.client_protocol:
            self._record_failure("not_client_protocol", msg_id)
            return self._error_envelope(call_id, msg_id, TIP_INVALID_PARAMETER)
        if route.node_type == BATTLE_NODE_TYPE:
            self._record_failure("battle_rejected", msg_id)
            return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)

        # Step 2d: 目标方法 KillSwitch 校验（拒绝时返回业务 1003，而非 gRPC 错误）
        if self.kill_switch is not None:
            if self.kill_switch.check(route.service_full_name, route.method) is not None:
                self._record_failure("killswitch", msg_id)
                return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)

        # Step 3a: 校验 release_track
        if session_track is None:
            self._record_failure("invalid_release_track", msg_id)
            return self._error_envelope(call_id, msg_id, TIP_INVALID_PARAMETER)

        # Step 3b-3c: 选择目标节点
        directory = self.directories.get(route.node_type)
        if directory is None:
            self._record_failure("no_target", msg_id)
            return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)

        endpoint, reason = pick_target(
            directory,
            self.breaker,
            zone_scoped=route.node_type_name in self._zone_scoped_names,
            zone_id=int(request.zone_id),
            session_track=session_track,
            subset_size=self.config.global_target_subset_size,
            peers=self.peers.entries() if self.peers is not None else [],
            self_uuid=self.self_uuid,
            self_track=self.config.release_track,
            rng=self._rng,
        )
        if endpoint is None:
            self._record_failure(reason, msg_id)
            return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)

        # 构造下游元数据：仅透传 x-*（剔除上游 x-release-track），追加权威 x-release-track
        downstream_md = tuple(
            (key, value)
            for key, value in incoming_md
            if key.startswith("x-") and key != "x-release-track"
        ) + (("x-release-track", session_track),)

        # Step 4: 原始字节透传调用下游
        try:
            client = self.pool.acquire(endpoint)
        except Exception:
            self.breaker.record_failure(endpoint)
            self._record_failure("dial_error", msg_id, endpoint)
            return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)

        timeout_sec = self.config.forward_timeout_ms / 1000.0
        try:
            response_bytes = await client.call(
                route.full_method,
                bytes(client_req.body),
                metadata=downstream_md,
                timeout=timeout_sec,
            )
        except asyncio.CancelledError:
            self.breaker.release_probe(endpoint)
            raise
        except grpc.aio.AioRpcError as exc:
            if context.cancelled():
                self.breaker.release_probe(endpoint)
            else:
                self.breaker.record_failure(endpoint)
            outcome = "upstream_timeout" if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED else "upstream_error"
            self._record_failure(outcome, msg_id, endpoint)
            return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)
        except Exception:
            if context.cancelled():
                self.breaker.release_probe(endpoint)
            else:
                self.breaker.record_failure(endpoint)
            self._record_failure("upstream_error", msg_id, endpoint)
            return self._error_envelope(call_id, msg_id, TIP_SERVICE_UNAVAILABLE)

        # Step 5: 成功返回原始响应字节（允许 b""）
        self.breaker.record_success(endpoint)
        self._outcome_counts["ok"] += 1
        return self.protocol.content(msg_id, call_id, body=bytes(response_bytes))
