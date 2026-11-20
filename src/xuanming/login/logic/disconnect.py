"""Disconnect: close the fence, then lease every settled player (Go ``disconnectlogic.go``)."""

import logging

from ...grpc_runtime import GrpcUnavailable
from .. import login_session, session_fence
from .leave_game import players_to_settle

logger = logging.getLogger(__name__)


async def run(ctx, request, details):
    response = ctx.protocol.message("loginpb.LoginEmptyResponse")
    if details is None:
        return response
    if request.session_id and request.session_id != details.session_id:
        logger.critical("CRITICAL: Disconnect body session_id=%d differs from verified header session_id=%d; "
                        "settling by the header", request.session_id, details.session_id)
    conn = session_fence.connection_of(details)
    if conn is None:
        return response
    try:
        bound = await session_fence.close(ctx.login_redis, conn)
    except Exception as error:
        logger.exception("Disconnect 栅栏关闭失败，gate 重试 session=%s", conn)
        raise GrpcUnavailable("login session fence unavailable, gate retries Disconnect") from error
    await login_session.cleanup(ctx.login_redis, conn)
    failed = False
    for player_id in players_to_settle(details.player_id, bound):
        try:
            await ctx.locator.set_disconnecting(player_id, details.session_id, details.gate_instance_id,
                                                details.session_incarnation)
        except Exception:
            logger.exception("SetDisconnecting 失败，gate 重试 player=%d", player_id)
            failed = True
    if failed:
        raise GrpcUnavailable("player_locator SetDisconnecting failed, gate retries Disconnect")
    return response
