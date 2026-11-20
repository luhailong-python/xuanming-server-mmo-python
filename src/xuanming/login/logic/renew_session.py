"""RenewSession, sent by the gate for live connections (Go ``renewsessionlogic.go``)."""

import logging

from ...grpc_runtime import GrpcUnavailable
from .. import login_session, session_fence

logger = logging.getLogger(__name__)


async def run(ctx, request, details):
    response = ctx.protocol.message("loginpb.RenewSessionResponse")
    conn = session_fence.connection_of(details)
    if conn is None:
        return response
    ttl_ms = ctx.config.session_ttl_ms
    try:
        kept = await login_session.renew(ctx.login_redis, conn, ttl_ms)
    except Exception as error:
        logger.exception("RenewSession 失败，gate 稍后重试 session=%s", conn)
        raise GrpcUnavailable("login session renewal unavailable, retry later") from error
    if kept:
        response.renewed = True
        response.renew_after_seconds = max(1, ttl_ms // 3000)
    return response
