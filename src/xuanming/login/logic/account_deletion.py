"""RequestAccountDeletion 与 CancelAccountDeletion（对应 Go ``requestaccountdeletionlogic.go`` / ``cancelaccountdeletionlogic.go``）。"""

import logging

from .. import tips
from ..account_deletion import SOURCE_USER
from ..tokens import AccessRejected
from . import login as login_logic
from . import tip

logger = logging.getLogger(__name__)


def _deletion_tip(protocol, tip_id: int):
    return protocol.message("loginpb.AccountDeletionResponse", error_message=tip(protocol, tip_id))


async def request_deletion(ctx, request, details):
    """用户本人申请注销：要求重新认证，拒收 access_token。"""
    protocol = ctx.protocol
    deletion = ctx.account_deletion_service()
    if deletion is None:
        return _deletion_tip(protocol, tips.DATA_SERIALIZE_FAILED)
    auth_type = request.auth_type or "password"
    if auth_type == "access_token":
        return _deletion_tip(protocol, tips.ACCOUNT_NOT_FOUND)
    login_req = protocol.message(
        "loginpb.LoginRequest",
        account=request.account,
        password=request.password,
        auth_type=request.auth_type,
        auth_token=request.auth_token,
    )
    try:
        account, _channel = await login_logic.resolve_account(ctx, login_req)
    except login_logic.AuthProviderUnavailable as error:
        logger.info("Account deletion re-auth unavailable type=%s err=%s", auth_type, error)
        return _deletion_tip(protocol, tips.AUTH_PROVIDER_UNAVAILABLE)
    except PermissionError as error:
        logger.info("Account deletion re-auth failed type=%s err=%s", auth_type, error)
        return _deletion_tip(protocol, tips.ACCOUNT_NOT_FOUND)
    return await deletion.request(
        account, SOURCE_USER,
        is_apple=(auth_type == "apple"),
        apple_code=request.authorization_code,
    )


async def cancel_deletion(ctx, request, details):
    """凭冷静期内签发的 access_token 撤销账号注销。"""
    protocol = ctx.protocol
    deletion = ctx.account_deletion_service()
    if deletion is None or ctx.tokens is None:
        return _deletion_tip(protocol, tips.DATA_SERIALIZE_FAILED)
    if not request.access_token:
        return _deletion_tip(protocol, tips.ACCOUNT_NOT_FOUND)
    try:
        token_data = await ctx.tokens.validate_access(request.access_token)
    except AccessRejected:
        return _deletion_tip(protocol, tips.ACCOUNT_NOT_FOUND)
    except Exception as error:
        logger.error("CancelAccountDeletion token lookup failed: %s", error)
        return _deletion_tip(protocol, tips.REDIS_ERROR)
    return await deletion.cancel(token_data.account)
