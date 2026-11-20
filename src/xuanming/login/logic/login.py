"""Login: HTTP path (no session) or gate bind via access token (Go ``loginlogic.go``)."""

import hmac
import logging

from .. import keys, login_session, session_fence, tips
from ..directory import DirectoryError, Missing
from ..locker import RedisLock
from ..tokens import AccessRejected
from . import tip

logger = logging.getLogger(__name__)


class _Unavailable(Exception):
    """The credential provider failed; the credential itself may be fine."""


async def _resolve_account(ctx, request) -> tuple[str, str]:
    auth_type = request.auth_type or "password"
    if auth_type == "password":
        config = ctx.config
        account = request.account
        if (config.dev_password_secret and account and account == account.strip()
                and account.startswith(config.dev_password_prefixes)
                and hmac.compare_digest(request.password.encode(), config.dev_password_secret.encode())):
            return account, auth_type
        raise PermissionError("invalid credentials")
    if auth_type == "access_token":
        try:
            data = await ctx.tokens.validate_access(request.auth_token)
        except AccessRejected:
            raise PermissionError("access token invalid") from None
        except Exception as error:
            raise _Unavailable("access token lookup failed") from error
        return data.account, data.auth_type
    raise PermissionError(f"unknown auth type: {auth_type}")


def _directory_tip(error) -> int:
    return tips.ACCOUNT_NOT_FOUND if isinstance(error, Missing) else tips.DATA_SERIALIZE_FAILED


async def run(ctx, request, details):
    protocol, config = ctx.protocol, ctx.config
    response = protocol.message("loginpb.LoginResponse")
    http = details is None or not details.session_id
    if not http and request.auth_type != "access_token" and not config.legacy_gate_login_enabled:
        response.error_message.CopyFrom(tip(protocol, tips.UNKNOWN_ERROR))
        return response
    conn = None
    if not http:
        conn = session_fence.connection_of(details)
        if conn is None:
            response.error_message.CopyFrom(tip(protocol, tips.SESSION_ID_NOT_FOUND))
            return response
    try:
        account, _channel = await _resolve_account(ctx, request)
    except (PermissionError, _Unavailable) as error:
        logger.info("认证失败 type=%s err=%s", request.auth_type, error)
        unavailable = isinstance(error, _Unavailable)
        response.error_message.CopyFrom(tip(protocol, tips.AUTH_PROVIDER_UNAVAILABLE if unavailable
                                            else tips.ACCOUNT_NOT_FOUND))
        return response
    lock = RedisLock(ctx.login_redis, keys.account_login_lock_key(account), config.account_lock_ttl_ms)
    try:
        locked = await lock.acquire()
    except Exception:
        logger.exception("Login 账号锁失败 account=%s", account)
        locked = False
    if not locked:
        response.error_message.CopyFrom(tip(protocol, tips.IN_PROGRESS))
        return response
    try:
        return await _login_locked(ctx, request, response, account, conn)
    finally:
        try:
            await lock.release()
        except Exception:
            logger.exception("Login 账号锁释放失败 account=%s", account)


async def _login_locked(ctx, request, response, account, conn):
    protocol, config = ctx.protocol, ctx.config
    auth_type = request.auth_type or "password"
    # Access tokens are only issued to existing accounts; a missing row means it was deleted.
    try:
        players = await ctx.data.read_account(account, auth_type != "access_token")
    except DirectoryError as error:
        response.error_message.CopyFrom(tip(protocol, _directory_tip(error)))
        return response
    if conn is None:
        if auth_type != "access_token":
            try:
                pair = await ctx.tokens.issue(account, auth_type)
            except Exception:
                logger.exception("签发令牌失败 account=%s", account)
                response.error_message.CopyFrom(tip(protocol, tips.TOKEN_ISSUE_FAILED))
                return response
            _fill_tokens(response, pair)
        response.players.extend(await ctx.data.role_list(players))
        return response

    redis = ctx.login_redis
    await login_session.prune_stale_devices(redis, account)
    try:
        committed = await session_fence.commit_login(redis, conn, account, config.session_ttl_ms)
    except Exception:
        logger.exception("Login 提交失败 account=%s", account)
        committed = None
    if committed is False:
        response.error_message.CopyFrom(tip(protocol, tips.SESSION_NOT_FOUND))
        return response
    try:
        if committed is None:
            raise RuntimeError("commit failed")
        await login_session.register_device(redis, account, conn, config.session_ttl_ms)
        devices = await redis.scard(keys.account_device_set_key(account))
    except Exception:
        logger.exception("Login 设备集写入失败 account=%s", account)
        response.error_message.CopyFrom(tip(protocol, tips.REDIS_SET_FAILED))
        return response
    if devices > config.max_devices_per_account:
        response.error_message.CopyFrom(tip(protocol, tips.TOO_MANY_DEVICES))
        return response
    if auth_type != "access_token":
        try:
            _fill_tokens(response, await ctx.tokens.issue(account, auth_type))
        except Exception:
            # Non-fatal on the gate path: the session is already bound.
            logger.exception("签发令牌失败 account=%s", account)
    response.players.extend(await ctx.data.role_list(players))
    return response


def _fill_tokens(response, pair) -> None:
    response.access_token = pair.access_token
    response.refresh_token = pair.refresh_token
    response.access_token_expire = pair.access_expire
    response.refresh_token_expire = pair.refresh_expire
