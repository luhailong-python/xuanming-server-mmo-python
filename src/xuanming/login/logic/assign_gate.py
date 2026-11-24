"""AssignGate 与 QueryQueueStatus（对应 Go ``assigngatelogic.go`` + ``queryqueuestatuslogic.go``）。"""

from __future__ import annotations

import logging

from ...cluster import GATE
from .. import gate_drain
from ..queue import (
    STATUS_ADMITTED,
    STATUS_ERROR,
    STATUS_EXPIRED,
    STATUS_QUEUE_FULL,
    STATUS_QUEUEING,
    STATUS_UNAUTHENTICATED,
    AdmitSlot,
    GateCandidate,
    QueueFullError,
    QueueTokenError,
    candidates_from_nodes,
    free_slots,
    gate_verdict,
    pick_gate,
    retry_after_for_rank,
    retry_after_when_full,
    sign_gate_token,
    verify_queue_token,
)
from ..tokens import AccessRejected

logger = logging.getLogger(__name__)

# 兼容旧导出常量名
ADMITTED, ERROR, QUEUE_DISABLED, UNAUTHENTICATED = (
    STATUS_ADMITTED, STATUS_ERROR, STATUS_EXPIRED, STATUS_UNAUTHENTICATED,
)


def pick_gate_from_nodes(gates, draining: dict[int, bool] | None = None) -> GateCandidate | None:
    """将 ``NodeInfo`` 列表去重分档并随机二选一挑出目标 gate。"""
    return pick_gate(candidates_from_nodes(gates, draining=draining))


async def _load_candidates(ctx, zone_id: int) -> list[GateCandidate]:
    nodes = await ctx.directory.candidates(GATE, zone_id)
    control_redis = ctx.control_redis if ctx.control_redis is not None else ctx.login_redis
    node_ids = list(dict.fromkeys(int(n.node_id) for n in nodes))
    draining = await gate_drain.draining_gates(control_redis, zone_id, node_ids)
    return candidates_from_nodes(nodes, zone_id, draining=draining)


def _zone_capacity(ctx, zone_id: int) -> int:
    if ctx.zone_capacity_fn is not None:
        return int(ctx.zone_capacity_fn(zone_id))
    return int(ctx.config.queue_zone_capacity)


def _fill_admit(response, admit) -> None:
    response.status = STATUS_ADMITTED
    response.ip = admit.ip
    response.port = admit.port
    response.ws_url = admit.ws_url
    response.token_payload = admit.token_payload
    response.token_signature = admit.token_signature
    response.token_deadline = admit.token_deadline


def _make_admit_signer(ctx):
    """领票时现签：先复核放行时选定的 gate，若已排空/满载/下线则按快速通道规则重挑。"""
    protocol, config = ctx.protocol, ctx.config

    async def sign(slot: AdmitSlot):
        chosen = GateCandidate(
            node_id=slot.node_id, ip=slot.ip, port=slot.port, zone_id=slot.zone_id, ws_url=slot.ws_url,
        )
        if ctx.directory is not None:
            try:
                candidates = await _load_candidates(ctx, slot.zone_id)
                matched = next((c for c in candidates if c.node_id == slot.node_id), None)
                if matched is not None and gate_verdict(matched) == "":
                    chosen = matched
                elif candidates:
                    repicked = pick_gate(candidates)
                    if repicked is not None:
                        chosen = repicked
            except Exception:
                logger.info("Admit gate recheck failed; signing original slot gate=%d", slot.node_id)
        return sign_gate_token(protocol, chosen, config.gate_token_secret, config.gate_token_ttl)

    return sign


async def _sign_fast_path(ctx, zone_id: int, response):
    try:
        candidates = await _load_candidates(ctx, zone_id)
        gate = pick_gate(candidates)
    except Exception:
        logger.exception("AssignGate read gate candidates failed zone=%d", zone_id)
        gate = None
    if gate is None:
        response.status, response.error = STATUS_ERROR, "no gate available"
        return response
    admit = sign_gate_token(ctx.protocol, gate, ctx.config.gate_token_secret, ctx.config.gate_token_ttl)
    _fill_admit(response, admit)
    return response


async def _reconnecting_player(ctx, account: str, zone_id: int) -> int:
    """判断账号是否已有角色在本区在线（重连/顶号免排队）；读取失败时按不免排队处理。"""
    if not account or ctx.data is None or ctx.locator is None:
        return 0
    try:
        roles = await ctx.data.read_account(account, False)
    except Exception:
        return 0
    zone_gate_ids: set[str] | None = None
    for role in roles:
        player_id = int(role.player_id)
        if not player_id:
            continue
        try:
            session = await ctx.locator.get(player_id)
        except Exception:
            continue
        if session is None or not session.gate_id:
            continue
        gate_zone = int(getattr(session, "gate_zone_id", 0))
        if gate_zone:
            if gate_zone == zone_id:
                return player_id
            continue
        if int(role.zone_id) == zone_id:
            return player_id
        if zone_gate_ids is None:
            try:
                candidates = await _load_candidates(ctx, zone_id)
                zone_gate_ids = {str(c.node_id) for c in candidates}
            except Exception:
                zone_gate_ids = set()
        if session.gate_id in zone_gate_ids:
            return player_id
    return 0


async def _handle_reentry(ctx, queue_token: str, response):
    config, queue = ctx.config, ctx.login_queue
    try:
        _, zone_id = verify_queue_token(config.gate_token_secret, queue_token, now_s=queue.clock_ms() // 1000)
    except QueueTokenError as error:
        response.status, response.error = STATUS_EXPIRED, str(error)
        return response
    try:
        state = await queue.poll(queue_token, zone_id, _make_admit_signer(ctx))
    except Exception:
        logger.exception("AssignGate reentry poll failed zone=%d", zone_id)
        response.status, response.error = STATUS_ERROR, "queue lookup failed"
        return response
    response.status = state.status
    if state.status == STATUS_ADMITTED and state.admit is not None:
        _fill_admit(response, state.admit)
    elif state.status == STATUS_QUEUEING:
        response.queue_token = queue_token
        response.queue_rank = state.rank
        response.queue_total = state.total
        response.retry_after_ms = retry_after_for_rank(
            config.queue_default_retry_after_ms, state.rank, config.queue_admit_ttl_s * 1000,
        )
    return response


async def run(ctx, request, details):
    protocol, config = ctx.protocol, ctx.config
    response = protocol.message("loginpb.AssignGateResponse")
    queue = ctx.login_queue
    queue_enabled = bool(config.queue_enabled and queue is not None)

    # 1. 携带 queue_token 的排队重入
    if queue_enabled and request.queue_token:
        return await _handle_reentry(ctx, request.queue_token, response)

    # 2. 鉴权：账号仅从 access_token 解出
    account = ""
    if request.access_token:
        try:
            data = await ctx.tokens.validate_access(request.access_token)
            account = data.account
        except AccessRejected:
            response.status, response.error = STATUS_UNAUTHENTICATED, "invalid access token"
            return response
        except Exception:
            logger.exception("AssignGate access token validation failed")
            response.status, response.error = STATUS_ERROR, "token lookup failed"
            return response
    elif not config.legacy_gate_login_enabled:
        response.status, response.error = STATUS_UNAUTHENTICATED, "access token required"
        return response

    # 3. 排队关闭时走快速通道
    if not queue_enabled:
        return await _sign_fast_path(ctx, request.zone_id or config.zone_id, response)

    # 4. 排队开启时仅服务本 login 所属区
    if request.zone_id != config.zone_id:
        logger.error("AssignGate rejected: zone=%d is not served by login zone=%d", request.zone_id, config.zone_id)
        response.status, response.error = STATUS_ERROR, "zone is not served by this login"
        return response

    # 5. 重连/顶号免排队
    if account:
        reconnecting_pid = await _reconnecting_player(ctx, account, config.zone_id)
        if reconnecting_pid:
            try:
                await queue.hold_reconnect_slot(config.zone_id, reconnecting_pid)
            except Exception:
                logger.exception("Hold reconnect slot failed zone=%d player_id=%d", config.zone_id, reconnecting_pid)
            return await _sign_fast_path(ctx, config.zone_id, response)

    # 6. 容量核算与空队列快速通道原子占位
    try:
        candidates = await _load_candidates(ctx, config.zone_id)
        free, capacity, online, _admitted = await free_slots(
            candidates, _zone_capacity(ctx, config.zone_id), queue, config.zone_id, config.queue_soft_cap_multiplier,
        )
    except Exception as error:
        logger.exception("AssignGate free_slots failed zone=%d", config.zone_id)
        response.status, response.error = STATUS_ERROR, str(error)
        return response

    try:
        qlen = await queue.queue_len(config.zone_id)
    except Exception:
        logger.exception("AssignGate queue_len failed zone=%d", config.zone_id)
        qlen = -1

    if qlen == 0 and free > 0:
        budget = max(0, capacity - online)
        try:
            reserved = await queue.try_reserve_fast_path_slot(config.zone_id, budget)
        except Exception:
            logger.exception("AssignGate fast-path slot reservation failed zone=%d", config.zone_id)
            reserved = False
        if reserved:
            return await _sign_fast_path(ctx, config.zone_id, response)

    # 7. 入队
    try:
        result = await queue.enqueue(config.zone_id, account, request.device_id)
    except QueueFullError:
        response.status = STATUS_QUEUE_FULL
        response.retry_after_ms = retry_after_when_full()
        return response
    except Exception:
        logger.exception("AssignGate enqueue failed zone=%d", config.zone_id)
        response.status, response.error = STATUS_ERROR, "queue write failed"
        return response

    response.status = STATUS_QUEUEING
    response.queue_token = result.queue_token
    response.queue_rank = result.rank
    response.queue_total = result.total
    response.retry_after_ms = retry_after_for_rank(
        config.queue_default_retry_after_ms, result.rank, config.queue_admit_ttl_s * 1000,
    )
    return response


async def query_queue_status(ctx, request, details):
    protocol, config = ctx.protocol, ctx.config
    queue = ctx.login_queue
    if not config.queue_enabled or queue is None:
        return protocol.message("loginpb.QueryQueueStatusResponse", status=STATUS_EXPIRED, error="queue disabled")
    try:
        _, zone_id = verify_queue_token(config.gate_token_secret, request.queue_token, now_s=queue.clock_ms() // 1000)
    except QueueTokenError as error:
        return protocol.message("loginpb.QueryQueueStatusResponse", status=STATUS_EXPIRED, error=str(error))
    try:
        state = await queue.poll(request.queue_token, zone_id, _make_admit_signer(ctx))
    except Exception:
        logger.exception("QueryQueueStatus poll failed zone=%d", zone_id)
        return protocol.message("loginpb.QueryQueueStatusResponse", status=STATUS_ERROR, error="queue lookup failed")
    response = protocol.message("loginpb.QueryQueueStatusResponse", status=state.status)
    if state.status == STATUS_ADMITTED and state.admit is not None:
        _fill_admit(response, state.admit)
    elif state.status == STATUS_QUEUEING:
        response.queue_rank = state.rank
        response.queue_total = state.total
        response.retry_after_ms = retry_after_for_rank(
            config.queue_default_retry_after_ms, state.rank, config.queue_admit_ttl_s * 1000,
        )
    return response
