"""Login: HTTP 路径（无会话）或经 gate 绑定会话（对应 Go ``loginlogic.go``）。"""

import logging

from .. import keys, login_session, session_fence, tips
from ..directory import DirectoryError, Missing
from ..locker import RedisLock
from ..password_auth import DevPasswordProvider
from ..tokens import AccessRejected
from . import tip

logger = logging.getLogger(__name__)


class AuthProviderUnavailable(Exception):
    """凭据校验基础设施故障（与凭据本身无效区分，便于客户端提示重试）。"""


_Unavailable = AuthProviderUnavailable


async def resolve_account(ctx, request) -> tuple[str, str]:
    """根据 ``auth_type`` 解析已认证账号及原始渠道名，供 Login 与 RequestAccountDeletion 复用。"""
    auth_type = request.auth_type or "password"
    if auth_type == "password":
        provider = ctx.password_provider
        if provider is None:
            provider = DevPasswordProvider(ctx.config.dev_password_secret, ctx.config.dev_password_prefixes)
        account = await provider.validate_password(request.account, request.password)
        return account, auth_type
    if auth_type == "access_token":
        try:
            data = await ctx.tokens.validate_access(request.auth_token)
        except AccessRejected:
            raise PermissionError("access token invalid") from None
        except Exception as error:
            raise AuthProviderUnavailable("access token lookup failed") from error
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
        account, channel = await resolve_account(ctx, request)
    except (PermissionError, AuthProviderUnavailable) as error:
        logger.info("Authentication failed type=%s err=%s", request.auth_type, error)
        unavailable = isinstance(error, AuthProviderUnavailable)
        response.error_message.CopyFrom(tip(protocol, tips.AUTH_PROVIDER_UNAVAILABLE if unavailable
                                            else tips.ACCOUNT_NOT_FOUND))
        return response
    lock = RedisLock(ctx.login_redis, keys.account_login_lock_key(account), config.account_lock_ttl_ms)
    try:
        locked = await lock.acquire()
    except Exception:
        logger.exception("Login account lock failed account=%s", account)
        locked = False
    if not locked:
        response.error_message.CopyFrom(tip(protocol, tips.IN_PROGRESS))
        return response
    try:
        return await _login_locked(ctx, request, response, account, channel, conn)
    finally:
        try:
            await lock.release()
        except Exception:
            logger.exception("Login account lock release failed account=%s", account)


async def _login_locked(ctx, request, response, account, channel, conn):
    protocol, config = ctx.protocol, ctx.config
    auth_type = request.auth_type or "password"
    # access_token 只签发给已有账号，目录缺失说明已注销，绝不建回
    try:
        players, real_name_pi, lifecycle = await ctx.data.read_account_with_lifecycle(
            account, auth_type != "access_token",
        )
    except DirectoryError as error:
        response.error_message.CopyFrom(tip(protocol, _directory_tip(error)))
        return response

    if conn is None:
        response.account_deletion_state = int(lifecycle.deletion_state)
        response.account_deletion_execute_after = int(lifecycle.deletion_execute_after_ms // 1000)
        response.previous_account_deleted_at = int(lifecycle.previous_deleted_ms // 1000)
        if auth_type != "access_token":
            try:
                pair = await ctx.tokens.issue(account, auth_type)
            except Exception:
                logger.exception("Issue token pair failed account=%s", account)
                response.error_message.CopyFrom(tip(protocol, tips.TOKEN_ISSUE_FAILED))
                return response
            _fill_tokens(response, pair)
        if ctx.anti_addiction is not None:
            pi = real_name_pi
            if not pi and ctx.anti_addiction.self_managed(channel):
                pi = await ctx.anti_addiction.real_name.resolve_pending(account)
            verdict = ctx.anti_addiction.evaluate(channel, pi)
            if verdict.status is not None:
                response.anti_addiction.CopyFrom(verdict.status)
        response.players.extend(await ctx.data.role_list(players))
        return response

    # gate 绑定路径：防沉迷拦截必须在写 login_session 之前执行（拒绝时不留登录态）
    if ctx.anti_addiction is not None:
        verdict = ctx.anti_addiction.evaluate(channel, real_name_pi)
        if verdict.status is not None:
            response.anti_addiction.CopyFrom(verdict.status)
        if verdict.reject_tip:
            logger.info("Login rejected by anti-addiction account=%s channel=%s tip=%d",
                        account, channel, verdict.reject_tip)
            response.error_message.CopyFrom(tip(protocol, verdict.reject_tip))
            return response

    redis = ctx.login_redis
    await login_session.prune_stale_devices(redis, account)
    try:
        committed = await session_fence.commit_login(redis, conn, account, config.session_ttl_ms)
    except Exception:
        logger.exception("Login commit failed account=%s", account)
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
        logger.exception("Login device set write failed account=%s", account)
        response.error_message.CopyFrom(tip(protocol, tips.REDIS_SET_FAILED))
        return response
    if devices > config.max_devices_per_account:
        response.error_message.CopyFrom(tip(protocol, tips.TOO_MANY_DEVICES))
        return response
    if auth_type != "access_token":
        try:
            _fill_tokens(response, await ctx.tokens.issue(account, auth_type))
        except Exception:
            # gate 路径上会话已绑定，发票失败不回滚绑定
            logger.exception("Issue token pair failed on gate path account=%s", account)
    response.players.extend(await ctx.data.role_list(players))
    return response


def _fill_tokens(response, pair) -> None:
    response.access_token = pair.access_token
    response.refresh_token = pair.refresh_token
    response.access_token_expire = pair.access_expire
    response.refresh_token_expire = pair.refresh_expire
