"""SubmitRealName（对应 Go ``submitrealnamelogic.go``）。

凭 /api/login 签发的 access_token 鉴权；姓名与身份证号仅在本次核验中透传，不落盘、不进日志。
"""

import logging

import grpc

from .. import tips
from ..tokens import AccessRejected
from . import RpcAbort, tip

logger = logging.getLogger(__name__)


async def run(ctx, request, details):
    protocol = ctx.protocol
    response = protocol.message("loginpb.SubmitRealNameResponse")
    aa = ctx.anti_addiction
    if aa is None:
        return response
    try:
        token_data = await ctx.tokens.validate_access(request.access_token)
    except AccessRejected as error:
        raise RpcAbort(grpc.StatusCode.UNAUTHENTICATED, "access token rejected") from error
    except Exception as error:
        logger.error("SubmitRealName access token lookup failed: %s", error)
        response.error_message.CopyFrom(tip(protocol, tips.AUTH_PROVIDER_UNAVAILABLE))
        return response

    account, channel = token_data.account, token_data.auth_type
    try:
        _, pi, _ = await ctx.data.read_account_with_lifecycle(account, False)
    except Exception as error:
        logger.error("SubmitRealName read account directory failed account=%s err=%s", account, error)
        response.error_message.CopyFrom(tip(protocol, tips.REAL_NAME_SERVICE_UNAVAILABLE))
        return response

    if not pi and aa.self_managed(channel):
        pi, tip_id = await aa.real_name.submit(account, request.name, request.id_number)
        if tip_id:
            response.error_message.CopyFrom(tip(protocol, tip_id))

    verdict = aa.evaluate(channel, pi)
    if verdict.status is not None:
        response.anti_addiction.CopyFrom(verdict.status)
    return response
