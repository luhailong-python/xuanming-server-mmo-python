"""AssignGate fast path with the login queue disabled (Go ``assigngatelogic.go``)."""

import logging
import random

from ...cluster import GATE
from ...nodes.gate import issue_gate_ticket
from ..tokens import AccessRejected

logger = logging.getLogger(__name__)
ADMITTED, ERROR, QUEUE_DISABLED, UNAUTHENTICATED = 0, 2, 3, 4


def _eligible(info) -> bool:
    return not info.max_connections or info.player_count < info.max_connections * 0.9


def pick_gate(gates):
    """Dedupe by address (newest launch wins), then the less loaded of two random eligible gates."""
    by_address = {}
    for info in gates:
        endpoint = info.client_endpoint if info.HasField("client_endpoint") else info.endpoint
        address = (endpoint.ip, endpoint.port)
        if endpoint.ip and endpoint.port and (address not in by_address
                                              or info.launch_time > by_address[address].launch_time):
            by_address[address] = info
    candidates = [info for info in by_address.values() if _eligible(info)] or list(by_address.values())
    if not candidates:
        return None
    # shortcut: one eligibility tier (stale/draining marks need the Redis control domain, not ported).
    return min(random.sample(candidates, min(2, len(candidates))), key=lambda info: info.player_count)


async def run(ctx, request, details):
    protocol, config = ctx.protocol, ctx.config
    response = protocol.message("loginpb.AssignGateResponse")
    if request.access_token:
        try:
            await ctx.tokens.validate_access(request.access_token)
        except AccessRejected:
            response.status, response.error = UNAUTHENTICATED, "invalid access token"
            return response
        except Exception:
            logger.exception("AssignGate 令牌校验失败")
            response.status, response.error = ERROR, "token lookup failed"
            return response
    elif not config.legacy_gate_login_enabled:
        response.status, response.error = UNAUTHENTICATED, "access token required"
        return response
    try:
        gate = pick_gate(await ctx.directory.candidates(GATE, config.zone_id))
    except Exception:
        logger.exception("AssignGate 读取 gate 列表失败")
        gate = None
    if gate is None:
        response.status, response.error = ERROR, "no gate available"
        return response
    ticket = issue_gate_ticket(protocol, gate.node_id, config.zone_id, config.gate_token_secret,
                               ttl=config.gate_token_ttl)
    endpoint = gate.client_endpoint if gate.HasField("client_endpoint") else gate.endpoint
    response.status = ADMITTED
    response.ip, response.port = endpoint.ip, endpoint.port
    response.token_payload, response.token_signature = ticket.payload, ticket.signature
    response.token_deadline = protocol.parse("GateTokenPayload", ticket.payload).expire_timestamp
    return response


async def query_queue_status(ctx, request, details):
    return ctx.protocol.message("loginpb.QueryQueueStatusResponse", status=QUEUE_DISABLED, error="queue disabled")
