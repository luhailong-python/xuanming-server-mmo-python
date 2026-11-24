"""Sa-Token HTTP 认证应用（基于 ``aiohttp.web``）。

公开路由：
- ``POST /auth/register``：注册用户名/口令账号（``hashlib.scrypt`` PHC 哈希）
- ``POST /auth/login``：校验口令并签发 Sa-Token（统一返回 ``{"code": 200, "msg": "ok", "data": {...}}``）
- ``POST /auth/logout``：从 ``satoken`` 或 ``Authorization: Bearer ...`` 提取令牌并吊销
- ``GET  /auth/isLogin``：查询当前令牌登录态
- ``GET  /auth/tokenInfo``：返回令牌元数据与剩余有效期
- ``GET  /auth/userInfo``：返回当前脱敏用户信息及已绑定 OAuth 列表
- ``GET  /auth/oauth/{provider}/url``：生成带一次性 CSRF ``state`` 的第三方授权跳转 URL
- ``POST /auth/oauth/{provider}/callback``：处理第三方 OAuth 回调并完成账号查找/自动建号与绑定
- ``POST /auth/dev/login``：受严格 profile 门禁保护的开发免密登录（``prod`` 恒返回 403）
- ``GET  /actuator/health``：健康探针
"""

from __future__ import annotations

import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from aiohttp import ClientSession, ClientTimeout, web

from .config import OAuthProviderConfig, SaTokenAuthConfig
from .store import (
    AuthStore,
    DuplicateUserError,
    TokenSessionStore,
    hash_password,
    verify_password_hash,
)

logger = logging.getLogger(__name__)

# 防止用户枚举时在用户不存在分支执行一次等效 KDF 校验的固定虚拟哈希。
_DUMMY_SCRYPT_HASH = hash_password("xuanming-dummy-password-for-constant-time-check")
_OAUTH_STATE_TTL_SEC = 600.0
_MAX_USERNAME_LEN = 128
_MIN_PASSWORD_LEN = 6
_MAX_PASSWORD_LEN = 1024

READY_STATE_KEY: web.AppKey[dict[str, bool]] = web.AppKey("satoken_ready_state", dict)


@dataclass(frozen=True, slots=True)
class OAuthProfile:
    """第三方 OAuth 回调解析得到的标准化用户身份。"""

    open_id: str
    union_id: str = ""
    nickname: str = ""
    avatar: str = ""


OAuthExchangeFn = Callable[[str, str, OAuthProviderConfig], Awaitable[OAuthProfile]]


def _ok(data: Any = None, *, msg: str = "ok", status: int = 200) -> web.Response:
    return web.json_response({"code": 200, "msg": msg, "data": data}, status=status)


def _err(status: int, msg: str, *, code: int | None = None) -> web.Response:
    return web.json_response({"code": code if code is not None else status, "msg": msg, "data": None}, status=status)


def _extract_token(request: web.Request, token_name: str) -> str:
    """按优先级从请求头 ``<token_name>``、``Authorization: Bearer``、Cookie 与查询串提取令牌。"""
    direct = request.headers.get(token_name, "").strip()
    if direct:
        return direct
    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header:
        scheme, _, value = auth_header.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    cookie = request.cookies.get(token_name, "").strip()
    if cookie:
        return cookie
    return request.query.get(token_name, "").strip()


async def _read_json_object(request: web.Request) -> dict[str, Any]:
    if not request.can_read_body:
        return {}
    try:
        payload = await request.json()
    except Exception as exc:
        raise web.HTTPBadRequest(text="invalid JSON body") from exc
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="JSON body must be an object")
    return payload


async def _default_http_oauth_exchange(
    provider: str,
    code: str,
    cfg: OAuthProviderConfig,
) -> OAuthProfile:
    """当提供商配置了真实 ``token_url`` 与 ``userinfo_url`` 时发起标准 OAuth2 交换。"""
    timeout = ClientTimeout(total=10.0)
    async with ClientSession(timeout=timeout) as session:
        async with session.post(
            cfg.token_url,
            data={
                "client_id": cfg.client_id,
                "client_secret": cfg.client_secret,
                "code": code,
                "redirect_uri": cfg.redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"Accept": "application/json"},
        ) as token_resp:
            if token_resp.status != 200:
                raise RuntimeError(f"oauth token exchange failed with HTTP {token_resp.status}")
            token_data = await token_resp.json(content_type=None)
        access_token = str(token_data.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError("oauth token exchange did not return access_token")

        async with session.get(
            cfg.userinfo_url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        ) as info_resp:
            if info_resp.status != 200:
                raise RuntimeError(f"oauth userinfo fetch failed with HTTP {info_resp.status}")
            info = await info_resp.json(content_type=None)

    open_id = str(info.get("openid") or info.get("open_id") or info.get("id") or info.get("sub") or "").strip()
    union_id = str(info.get("unionid") or info.get("union_id") or token_data.get("unionid") or "").strip()
    nickname = str(info.get("nickname") or info.get("name") or info.get("login") or "").strip()
    avatar = str(info.get("avatar_url") or info.get("headimgurl") or info.get("avatar") or "").strip()
    if not open_id and not union_id:
        raise RuntimeError(f"oauth provider {provider} returned neither open_id nor union_id")
    return OAuthProfile(open_id=open_id or union_id, union_id=union_id, nickname=nickname, avatar=avatar)


def create_auth_app(
    user_store: AuthStore,
    token_store: TokenSessionStore,
    config: SaTokenAuthConfig,
    *,
    oauth_exchange: OAuthExchangeFn | None = None,
    ready: bool = True,
) -> web.Application:
    """构建 Sa-Token HTTP 认证服务 ``aiohttp.web.Application``。"""
    app = web.Application()
    app[READY_STATE_KEY] = {"ready": ready}
    # 一次性 OAuth CSRF state 存储：state -> (provider, expires_monotonic)
    oauth_states: dict[str, tuple[str, float]] = {}

    def _prune_states(now_mono: float) -> None:
        expired = [k for k, (_, exp) in oauth_states.items() if now_mono >= exp]
        for k in expired:
            oauth_states.pop(k, None)

    async def handle_health(_request: web.Request) -> web.Response:
        is_ready = app[READY_STATE_KEY].get("ready", False)
        status_code = 200 if is_ready else 503
        return web.json_response(
            {
                "status": "UP" if is_ready else "DOWN",
                "service": "satoken_auth",
                "profile": config.profile,
                "devLoginEnabled": config.allow_dev_login,
            },
            status=status_code,
        )

    async def handle_register(request: web.Request) -> web.Response:
        try:
            body = await _read_json_object(request)
        except web.HTTPBadRequest as exc:
            return _err(400, exc.text or "invalid JSON body")

        username = str(body.get("username") or "").strip()
        password = body.get("password")
        nickname = str(body.get("nickname") or "").strip()
        avatar = str(body.get("avatar") or "").strip()

        if not username or len(username) > _MAX_USERNAME_LEN or username != str(body.get("username")):
            return _err(400, "invalid username")
        if not isinstance(password, str) or len(password) < _MIN_PASSWORD_LEN:
            return _err(400, f"password must be at least {_MIN_PASSWORD_LEN} characters")
        if len(password.encode("utf-8")) > _MAX_PASSWORD_LEN:
            return _err(400, "password exceeds maximum length")

        pw_hash = hash_password(password)
        try:
            user = await user_store.create_user(
                username=username,
                password_hash=pw_hash,
                nickname=nickname or username,
                avatar=avatar,
            )
        except DuplicateUserError:
            return _err(409, "username already exists")

        logger.info("satoken_auth user registered user_id=%d username=%s", user.id, user.username)
        return _ok(user.to_public_dict())

    async def handle_login(request: web.Request) -> web.Response:
        try:
            body = await _read_json_object(request)
        except web.HTTPBadRequest as exc:
            return _err(400, exc.text or "invalid JSON body")

        username = str(body.get("username") or body.get("account") or "").strip()
        password = body.get("password")
        if not username or not isinstance(password, str) or not password:
            return _err(400, "username and password are required")

        user = await user_store.get_user_by_username(username)
        if user is None or not user.password_hash:
            # 常数时间占位校验，避免通过时延区分账号是否存在。
            try:
                verify_password_hash(password, _DUMMY_SCRYPT_HASH)
            except ValueError:
                pass
            return _err(401, "invalid username or password")

        if user.status != 1:
            return _err(403, "account is disabled")

        try:
            matched = verify_password_hash(password, user.password_hash)
        except ValueError:
            matched = False
        if not matched:
            return _err(401, "invalid username or password")

        await user_store.touch_last_login(user.id)
        session = await token_store.create_session(
            str(user.id),
            token_name=config.token_name,
            ttl_sec=config.token_ttl_sec,
            extra={"username": user.username, "authType": "password"},
        )
        logger.info("satoken_auth user logged in user_id=%d username=%s", user.id, user.username)
        return _ok(
            {
                "tokenName": session.token_name,
                "tokenValue": session.token_value,
                "loginId": session.login_id,
                "loginType": session.login_type,
                "tokenTimeout": session.ttl_sec,
                "redisKey": session.redis_token_key,
            }
        )

    async def handle_logout(request: web.Request) -> web.Response:
        token = _extract_token(request, config.token_name)
        if not token:
            try:
                body = await _read_json_object(request)
                token = str(body.get("token") or body.get("tokenValue") or "").strip()
            except web.HTTPBadRequest:
                token = ""
        if not token:
            return _err(401, "missing token")
        removed = await token_store.revoke_token(token, token_name=config.token_name)
        logger.info("satoken_auth logout removed=%s", removed)
        return _ok({"loggedOut": True, "removed": removed})

    async def handle_is_login(request: web.Request) -> web.Response:
        token = _extract_token(request, config.token_name)
        if not token:
            return _ok({"isLogin": False, "loginId": None})
        session = await token_store.get_session_by_token(token, token_name=config.token_name)
        if session is None:
            return _ok({"isLogin": False, "loginId": None})
        return _ok({"isLogin": True, "loginId": session.login_id})

    async def handle_token_info(request: web.Request) -> web.Response:
        token = _extract_token(request, config.token_name)
        if not token:
            return _err(401, "not logged in")
        session = await token_store.get_session_by_token(token, token_name=config.token_name)
        if session is None:
            return _err(401, "invalid or expired token")
        return _ok(
            {
                "tokenName": session.token_name,
                "tokenValue": session.token_value,
                "isLogin": True,
                "loginId": session.login_id,
                "loginType": session.login_type,
                "tokenTimeout": session.remaining_sec(),
                "redisKey": session.redis_token_key,
                "sessionKey": session.redis_session_key,
            }
        )

    async def handle_user_info(request: web.Request) -> web.Response:
        token = _extract_token(request, config.token_name)
        if not token:
            return _err(401, "not logged in")
        session = await token_store.get_session_by_token(token, token_name=config.token_name)
        if session is None:
            return _err(401, "invalid or expired token")

        user = None
        if session.login_id.isdigit():
            user = await user_store.get_user_by_id(int(session.login_id))
        if user is None:
            user = await user_store.get_user_by_username(session.login_id)

        if user is not None:
            bindings = await user_store.list_user_oauths(user.id)
            data = user.to_public_dict()
            data["loginId"] = session.login_id
            data["oauthBindings"] = [b.to_dict() for b in bindings]
            return _ok(data)

        # dev-login 直接以任意账号字符串登录且未建表记录时，返回会话身份摘要。
        return _ok(
            {
                "loginId": session.login_id,
                "username": session.login_id,
                "nickname": session.login_id,
                "oauthBindings": [],
                "extra": dict(session.extra),
            }
        )

    async def handle_oauth_url(request: web.Request) -> web.Response:
        provider = request.match_info["provider"].strip().lower()
        provider_cfg = config.get_oauth_provider(provider)
        if provider_cfg is None or not provider_cfg.is_configured:
            return _err(404, f"oauth provider not configured: {provider}")

        now_mono = time.monotonic()
        _prune_states(now_mono)
        state = secrets.token_urlsafe(24)
        oauth_states[state] = (provider, now_mono + _OAUTH_STATE_TTL_SEC)

        params = {
            "client_id": provider_cfg.client_id,
            "redirect_uri": provider_cfg.redirect_uri,
            "response_type": "code",
            "state": state,
        }
        if provider_cfg.scope:
            params["scope"] = provider_cfg.scope
        auth_url = f"{provider_cfg.effective_authorize_url}?{urlencode(params)}"
        return _ok({"provider": provider, "state": state, "url": auth_url})

    async def handle_oauth_callback(request: web.Request) -> web.Response:
        provider = request.match_info["provider"].strip().lower()
        provider_cfg = config.get_oauth_provider(provider)
        if provider_cfg is None or not provider_cfg.is_configured:
            return _err(404, f"oauth provider not configured: {provider}")

        try:
            body = await _read_json_object(request)
        except web.HTTPBadRequest as exc:
            return _err(400, exc.text or "invalid JSON body")

        code = str(body.get("code") or request.query.get("code") or "").strip()
        state = str(body.get("state") or request.query.get("state") or "").strip()
        if not code:
            return _err(400, "oauth code is required")

        # 一次性消费并校验 state，防止 CSRF 与回调重放。
        now_mono = time.monotonic()
        _prune_states(now_mono)
        if not state:
            return _err(400, "oauth state is required")
        state_entry = oauth_states.pop(state, None)
        if state_entry is None or state_entry[0] != provider or now_mono >= state_entry[1]:
            return _err(400, "invalid or expired oauth state")

        try:
            if oauth_exchange is not None:
                profile = await oauth_exchange(provider, code, provider_cfg)
            elif provider_cfg.token_url and provider_cfg.userinfo_url:
                profile = await _default_http_oauth_exchange(provider, code, provider_cfg)
            else:
                # 无外部 OAuth 网关配置（如本地联调/集成测试）时，使用已校验 state 后的回调身份字段。
                raw_open_id = str(body.get("openId") or body.get("open_id") or "").strip()
                raw_union_id = str(body.get("unionId") or body.get("union_id") or "").strip()
                if not raw_open_id and not raw_union_id:
                    return _err(400, "oauth callback requires openId or configured token_url")
                profile = OAuthProfile(
                    open_id=raw_open_id or raw_union_id,
                    union_id=raw_union_id,
                    nickname=str(body.get("nickname") or "").strip(),
                    avatar=str(body.get("avatar") or "").strip(),
                )
        except Exception as exc:
            logger.warning("satoken_auth oauth exchange failed provider=%s err=%s", provider, exc)
            return _err(502, f"oauth exchange failed: {exc}")

        # 对齐 AuthController.java：开启 union_id 时优先以 union_id 作为跨子应用稳定第三方标识。
        third_party_id = (
            profile.union_id
            if (provider_cfg.union_id and profile.union_id)
            else profile.open_id
        )
        if not third_party_id:
            return _err(400, "oauth identity is empty")

        user, created = await user_store.find_or_create_oauth_user(
            provider,
            third_party_id,
            nickname=profile.nickname,
            avatar=profile.avatar,
            union_id=profile.union_id,
        )
        session = await token_store.create_session(
            str(user.id),
            token_name=config.token_name,
            ttl_sec=config.token_ttl_sec,
            extra={"provider": provider, "authType": "oauth"},
        )
        logger.info(
            "satoken_auth oauth login provider=%s user_id=%d created=%s",
            provider,
            user.id,
            created,
        )
        return _ok(
            {
                "tokenName": session.token_name,
                "tokenValue": session.token_value,
                "loginId": session.login_id,
                "loginType": session.login_type,
                "tokenTimeout": session.ttl_sec,
                "redisKey": session.redis_token_key,
                "provider": provider,
                "isNewUser": created,
                "user": user.to_public_dict(),
            }
        )

    async def handle_dev_login(request: web.Request) -> web.Response:
        # 双重 fail-closed 防线：prod 或未显式开启 dev_login_enabled 时一律拒绝。
        if config.profile == "prod" or not config.allow_dev_login:
            logger.warning(
                "satoken_auth blocked dev login attempt profile=%s dev_login_enabled=%s",
                config.profile,
                config.dev_login_enabled,
            )
            return _err(403, "dev login is disabled in current profile")

        try:
            body = await _read_json_object(request)
        except web.HTTPBadRequest as exc:
            return _err(400, exc.text or "invalid JSON body")

        account = str(
            body.get("account")
            or body.get("username")
            or request.query.get("account")
            or ""
        ).strip()
        if not account or len(account) > _MAX_USERNAME_LEN:
            return _err(400, "account is required")

        session = await token_store.create_session(
            account,
            token_name=config.token_name,
            ttl_sec=config.token_ttl_sec,
            extra={"account": account, "authType": "dev"},
        )
        logger.info("satoken_auth dev login issued account=%s profile=%s", account, config.profile)
        return _ok(
            {
                "tokenName": session.token_name,
                "tokenValue": session.token_value,
                "loginId": session.login_id,
                "account": account,
                "loginType": session.login_type,
                "tokenTimeout": session.ttl_sec,
                "redisKey": session.redis_token_key,
            }
        )

    app.router.add_get("/actuator/health", handle_health)
    app.router.add_post("/auth/register", handle_register)
    app.router.add_post("/auth/login", handle_login)
    app.router.add_post("/auth/logout", handle_logout)
    app.router.add_get("/auth/isLogin", handle_is_login)
    app.router.add_get("/auth/tokenInfo", handle_token_info)
    app.router.add_get("/auth/userInfo", handle_user_info)
    app.router.add_get("/auth/oauth/{provider}/url", handle_oauth_url)
    app.router.add_post("/auth/oauth/{provider}/callback", handle_oauth_callback)
    app.router.add_post("/auth/dev/login", handle_dev_login)

    return app


__all__ = [
    "OAuthExchangeFn",
    "OAuthProfile",
    "READY_STATE_KEY",
    "create_auth_app",
]
