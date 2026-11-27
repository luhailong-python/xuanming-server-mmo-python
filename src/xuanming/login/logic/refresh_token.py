"""RefreshToken (Go ``refreshtokenlogic.go``): every failure is an in-band 2000."""

import logging

from .. import tips
from ..tokens import RefreshRejected
from . import tip

logger = logging.getLogger(__name__)


async def run(ctx, request, details):
    response = ctx.protocol.message("loginpb.RefreshTokenResponse")
    if not request.refresh_token:
        response.error_message.CopyFrom(tip(ctx.protocol, tips.ACCOUNT_NOT_FOUND))
        return response
    passport_sessions = getattr(ctx, "passport_sessions", None)
    if passport_sessions is not None:
        try:
            data = await ctx.tokens.read_refresh_payload(request.refresh_token)
        except Exception:
            data = None
        if data is not None and await passport_sessions.check_refresh(data):
            response.error_message.CopyFrom(tip(ctx.protocol, tips.ACCOUNT_NOT_FOUND))
            return response
    try:
        pair = await ctx.tokens.refresh(request.refresh_token)
    except RefreshRejected as error:
        logger.info("RefreshToken rejected: %s", error)
        response.error_message.CopyFrom(tip(ctx.protocol, tips.ACCOUNT_NOT_FOUND))
        return response
    except Exception:
        logger.exception("RefreshToken failed")
        response.error_message.CopyFrom(tip(ctx.protocol, tips.ACCOUNT_NOT_FOUND))
        return response
    response.access_token = pair.access_token
    response.refresh_token = pair.refresh_token
    response.access_token_expire = pair.access_expire
    response.refresh_token_expire = pair.refresh_expire
    return response
