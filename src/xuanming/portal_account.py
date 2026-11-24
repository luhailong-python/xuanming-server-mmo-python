"""账号注销申请与撤销 HTTP 入口，对标 Java gateway_node AccountDeletionController。"""

import logging

import grpc
from aiohttp import web

from .portal_login import (
    client_ip,
    cooldown_key,
    is_fault_tip,
    rpc_error_code_and_message,
)

_LOG = logging.getLogger(__name__)

# 字段长度上限，与 login 侧及 Java AccountDeletionController 保持一致。
MAX_ACCOUNT_CODE_POINTS = 191
MAX_PASSWORD_CHARS = 1024
MAX_TOKEN_CHARS = 16 << 10
_MAX_INT32 = (1 << 31) - 1

# 与 /api/login 共用冷却窗口，避免交替调用获得两份口令尝试窗口。
_COOLDOWN_SCOPE = "login"


def _error(code: int, message: str) -> dict:
    """构造网关侧失败应答（无 login 结论，不携带 state / execute_after / blockers）。"""
    return {"code": code, "message": message, "tip": 0}


def _text(body: dict, field: str) -> str:
    value = body.get(field, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"invalid string field: {field}")
    return value


def _raw_zone(body: dict) -> int:
    value = body.get("zone_id", 0)
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("invalid zone_id")
    return value


def _routable_zone(zone_id: int) -> bool:
    return 0 < zone_id <= _MAX_INT32


def validate_delete_request(
    zone_id: int,
    account: str,
    password: str,
    auth_type: str,
    auth_token: str,
    authorization_code: str,
) -> str | None:
    """校验注销申请参数，合法返回 None，否则返回稳定原因串。"""
    if not _routable_zone(zone_id):
        return "invalid_zone_id"
    if (
        len(account) > MAX_ACCOUNT_CODE_POINTS
        or len(password) > MAX_PASSWORD_CHARS
        or len(auth_type) > MAX_TOKEN_CHARS
        or len(auth_token) > MAX_TOKEN_CHARS
        or len(authorization_code) > MAX_TOKEN_CHARS
    ):
        return "field_too_long"
    return None


def validate_cancel_request(zone_id: int, access_token: str) -> str | None:
    """校验撤销注销参数，合法返回 None，否则返回稳定原因串。"""
    if not _routable_zone(zone_id):
        return "invalid_zone_id"
    if not access_token or not access_token.strip():
        return "empty_access_token"
    if len(access_token) > MAX_TOKEN_CHARS:
        return "field_too_long"
    return None


def _map_response(op: str, response) -> dict:
    """将 login 返回的 AccountDeletionResponse 映射为客户端 JSON 契约。"""
    tip = response.error_message
    tip_id = tip.id
    param_msg = tip.parameters[-1] if (tip.parameters and tip.parameters[-1]) else ""
    has_error = bool(tip_id or param_msg)
    if has_error:
        code = 500 if is_fault_tip(tip_id) else 401
        message = param_msg or f"upstream_err={tip_id}"
        if code == 500:
            _LOG.warning("%s.rpc in-band fault tip=%d", op, tip_id)
    else:
        code = 0
        message = None
    out = {
        "code": code,
        "tip": tip_id,
        "state": response.state,
        "execute_after": response.execute_after,
    }
    if message is not None:
        out["message"] = message
    if response.blockers:
        out["blockers"] = [
            {"player_id": blocker.player_id, "reason": blocker.reason}
            for blocker in response.blockers
        ]
    return out


def register_routes(app: web.Application, deps) -> None:
    """注册账号注销申请与撤销路由（POST /api/account/delete、/api/account/delete/cancel）。"""
    protocol, rpc, limiter = deps.protocol, deps.rpc, deps.limiter

    async def _rate_limited(zone_id: int, request: web.Request, key: str) -> dict | None:
        decision = await limiter.check(
            zone_id,
            client_ip(request, limiter.networks),
            key,
            _COOLDOWN_SCOPE,
        )
        if not decision:
            return None
        kind, value = decision
        # 排队/波次判定同样回 429 + QUEUEING：注销没有排队语义。
        return _error(429, value if kind == "deny" else "QUEUEING")

    async def _call(op: str, zone_id: int, method: str, req) -> web.Response:
        try:
            response = await rpc.call(
                zone_id, "LoginPreGate", method, req, "AccountDeletionResponse"
            )
            return web.json_response(_map_response(op, response))
        except grpc.aio.AioRpcError as exc:
            _LOG.warning("%s.rpc failed: %s", op, exc.code().name)
            code, message = rpc_error_code_and_message(exc)
            return web.json_response(_error(code, message))
        except Exception as exc:
            _LOG.error("%s.rpc unexpected error: %s", op, type(exc).__name__)
            return web.json_response(_error(500, "internal_error"))

    async def request_deletion(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise ValueError("invalid body")
            zone_id = _raw_zone(body)
            account = _text(body, "account")
            password = _text(body, "password")
            auth_type = _text(body, "auth_type")
            auth_token = _text(body, "auth_token")
            authorization_code = _text(body, "authorization_code")
        except (ValueError, TypeError):
            return web.json_response(_error(500, "internal_error"))

        invalid = validate_delete_request(
            zone_id, account, password, auth_type, auth_token, authorization_code
        )
        if invalid is not None:
            return web.json_response(_error(401, invalid))

        key = cooldown_key(account) if account.strip() else cooldown_key(auth_token)
        limited = await _rate_limited(zone_id, request, key)
        if limited is not None:
            return web.json_response(limited)

        req = protocol.message(
            "loginpb.AccountDeletionRequest",
            account=account,
            password=password,
            auth_type=auth_type,
            auth_token=auth_token,
            authorization_code=authorization_code,
        )
        return await _call("request_account_deletion", zone_id, "RequestAccountDeletion", req)

    async def cancel_deletion(request: web.Request) -> web.Response:
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise ValueError("invalid body")
            zone_id = _raw_zone(body)
            access_token = _text(body, "access_token")
        except (ValueError, TypeError):
            return web.json_response(_error(500, "internal_error"))

        invalid = validate_cancel_request(zone_id, access_token)
        if invalid is not None:
            return web.json_response(_error(401, invalid))

        limited = await _rate_limited(zone_id, request, cooldown_key(access_token))
        if limited is not None:
            return web.json_response(limited)

        req = protocol.message(
            "loginpb.CancelAccountDeletionRequest",
            access_token=access_token,
        )
        return await _call("cancel_account_deletion", zone_id, "CancelAccountDeletion", req)

    app.router.add_post("/api/account/delete", request_deletion)
    app.router.add_post("/api/account/delete/cancel", cancel_deletion)
