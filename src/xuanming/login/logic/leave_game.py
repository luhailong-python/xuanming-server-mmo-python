"""LeaveGame (Go ``leavegamelogic.go``)."""

import logging

from ...grpc_runtime import GrpcUnavailable
from .. import login_session, session_fence

logger = logging.getLogger(__name__)


def players_to_settle(from_gate: int, bound: int) -> list[int]:
    """The gate's header player plus the fence-bound player, both settled by session-id CAS downstream."""
    return [player for player in dict.fromkeys((from_gate, bound)) if player]


async def run(ctx, request, details):
    response = ctx.protocol.message("loginpb.LoginEmptyResponse")
    conn = session_fence.connection_of(details)
    if conn is None:
        return response
    try:
        bound = await session_fence.leave(ctx.login_redis, conn)
    except Exception as error:
        logger.exception("LeaveGame 栅栏失败，未做任何清理 session=%s", conn)
        raise GrpcUnavailable("login session fence unavailable, retry LeaveGame") from error
    await login_session.cleanup(ctx.login_redis, conn)
    for player_id in players_to_settle(details.player_id, bound):
        if details.session_id:
            try:
                await ctx.locator.delete(player_id, details.session_id, details.gate_instance_id)
            except Exception:
                logger.exception("删除会话失败 player=%d", player_id)
    return response
