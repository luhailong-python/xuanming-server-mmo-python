"""574 LoginPreGate.HandlePassportEvent：处理网关转来的官方通行证回调（对应 Go ``handlepassporteventlogic.go``）。"""

from __future__ import annotations

import logging

import grpc

from ..passport import BadPassportEventError
from . import RpcAbort

logger = logging.getLogger(__name__)


async def run(ctx, request, details):
    """核对回调点名的通行证用户并吊销失效会话、踢出在线角色（幂等）。"""
    del details
    enforcer = getattr(ctx, "passport_sessions", None)
    if enforcer is None:
        raise RpcAbort(grpc.StatusCode.FAILED_PRECONDITION, "passport admin client is not configured")
    try:
        checked = await enforcer.handle_event(bytes(request.payload))
    except BadPassportEventError as exc:
        logger.info("[passport] webhook payload rejected: %s", exc)
        raise RpcAbort(grpc.StatusCode.INVALID_ARGUMENT, str(exc)) from exc
    except Exception as exc:
        logger.error("[passport] webhook handling failed, passport will redeliver: %s", exc)
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "passport webhook handling failed") from exc
    return ctx.protocol.message("loginpb.PassportEventResponse", checked_users=int(checked))
