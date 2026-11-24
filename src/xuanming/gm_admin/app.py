"""gm_admin HTTP 接口层（对齐 Java GmController + GmPlayerCommandController 全部 23 条 GM 路由与健康探针）。

设计要点：
- Fail-closed 管理员鉴权：未配置 ``admin_token`` 一律拒绝；校验 ``X-Admin-Key`` 或 ``X-Gm-Token``
  （SHA-256 常量时间比对），从 ``X-Gm-Operator`` 读取并严格校验操作人身份；
- 支持可选 ``X-Gm-Permissions`` 请求头做最小权限交集约束，与 Java Shiro 14 项细粒度权限一一对应；
- 统一响应信封 ``{"success": bool, "code": int, "message": str, "result": ...}`` 与严格 JSON 解析
  （拒收重复键、NaN/Infinity、未知字段、超限请求体）。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from aiohttp import web

from .clients import (
    MAX_ITEM_GRANT_COUNT,
    MAX_PLAYER_LEVEL,
    PLAYER_OPERATIONS,
    PLAYER_OPERATIONS_BY_CODE,
    GameServicesClient,
    has_iso_control,
)
from .config import GmAdminConfig
from .journal import (
    INT64_MAX,
    PLAYER_DRAFT_TTL_MS,
    BaseGmJournal,
    CallFailure,
    GmHttpError,
    MailCampaignManifest,
)

logger = logging.getLogger(__name__)

# -- 权限与请求字段白名单（与 Java GmController / GmPlayerCommandController 逐项对齐） ----------------

ALL_GM_PERMISSIONS: frozenset[str] = frozenset((
    "gm:zone:read",
    "gm:player:read",
    "gm:mail:prepare",
    "gm:mail:send",
    "gm:system-mail:prepare",
    "gm:system-mail:send",
    "gm:audit:read",
    "gm:currency:add",
    "gm:currency:deduct",
    "gm:currency:block",
    "gm:currency:unblock",
    "gm:player:level",
    "gm:pet:grant",
    "gm:item:grant",
))

_DRAFT_FIELDS = frozenset((
    "clientRequestId",
    "playerId",
    "title",
    "body",
    "reason",
    "currencyType",
    "currencyAmount",
    "itemConfigId",
    "itemCount",
    "currencies",
    "items",
    "validDays",
))
_SYSTEM_MAIL_FIELDS = frozenset(("clientRequestId", "title", "body", "reason", "validDays"))
_CAMPAIGN_FIELDS = frozenset((
    "clientRequestId",
    "title",
    "body",
    "reason",
    "currencies",
    "items",
    "recipientPlayerIds",
    "validDays",
))
_PLAYER_DRAFT_FIELDS = frozenset(("clientRequestId", "playerId", "operation", "arguments", "reason"))
_CURRENCY_ENTRY_FIELDS = frozenset(("currencyType", "amount"))
_ITEM_ENTRY_FIELDS = frozenset(("configId", "count"))

_DEFAULT_MAX_BODY_BYTES = 16_384
_CAMPAIGN_MAX_BODY_BYTES = 512 * 1024
_MAX_CURRENCIES = 4
_MAX_ITEMS = 16
_MAX_TOTAL_ITEM_COUNT = 4096
_INT32_MAX = (1 << 31) - 1
_POSITIVE_INT64_RE = re.compile(r"^[1-9][0-9]{0,18}$")

_PLAYER_OP_DESCRIPTION = "仅开发或测试服，目标玩家须在线且处于可操作状态；草稿5分钟内有效，执行结果未知时禁止重发。"
READY_STATE_KEY: web.AppKey[dict[str, bool]] = web.AppKey("gm_ready_state", dict)


@dataclass(frozen=True, slots=True)
class _Principal:
    """已认证 GM 管理员身份与权限集。"""

    operator: str
    permissions: frozenset[str]

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _ok(result: Any) -> web.Response:
    return web.json_response(
        {"success": True, "code": 200, "message": "", "result": result},
        status=200,
    )


def _error(status: int, message: str) -> web.Response:
    return web.json_response(
        {"success": False, "code": status, "message": message},
        status=status,
    )


def _bad(message: str) -> None:
    raise GmHttpError(400, message)


def _decimal_int64(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _POSITIVE_INT64_RE.fullmatch(value) or int(value) > INT64_MAX:
        _bad(f"{label} 必须是正整数文本")
    return value


def _text(node: dict[str, Any], field_name: str, min_len: int, max_len: int) -> str:
    value = node.get(field_name)
    if not isinstance(value, str):
        _bad(f"{field_name} 必须是文本")
    length = len(value)
    if length < min_len or length > max_len or (min_len > 0 and not value.strip()):
        _bad(f"{field_name} 长度无效")
    return value


def _integer(node: dict[str, Any], field_name: str, min_val: int, max_val: int) -> int:
    value = node.get(field_name)
    if not _is_int(value) or value < min_val or value > max_val:
        _bad(f"{field_name} 超出允许范围")
    return value


def _uuid_field(node: dict[str, Any]) -> str:
    client_id = _text(node, "clientRequestId", 1, 36)
    try:
        parsed = uuid.UUID(client_id)
    except ValueError:
        _bad("请求号无效")
    if str(parsed) != client_id:
        _bad("请求号无效")
    return client_id


def _check_fields(node: dict[str, Any], allowed: frozenset[str]) -> None:
    for key in node:
        if key not in allowed:
            _bad("存在未知字段")


def _parse_int_query(request: web.Request, name: str, default: int) -> int:
    raw = request.query.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise GmHttpError(400, "请求参数格式无效") from None


async def _parse_json_object(request: web.Request, max_bytes: int) -> dict[str, Any]:
    """有界读取并严格解析 JSON 对象（拒收重复键与非标准常量）。"""
    if request.content_length is not None and request.content_length > max_bytes:
        _bad("请求体过大")
    buf = bytearray()
    async for chunk in request.content.iter_chunked(8192):
        buf.extend(chunk)
        if len(buf) > max_bytes:
            _bad("请求体过大")
    if not buf:
        _bad("请求 JSON 无效")
    try:
        raw_text = bytes(buf).decode("utf-8")
    except UnicodeDecodeError:
        _bad("请求 JSON 无效")

    def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("duplicate key")
            out[key] = value
        return out

    def _reject_constant(_token: str) -> Any:
        raise ValueError("invalid constant")

    try:
        parsed = json.loads(raw_text, object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        _bad("请求 JSON 无效")
    if not isinstance(parsed, dict):
        _bad("请求 JSON 无效")
    return parsed


def _authenticate(request: web.Request, config: GmAdminConfig) -> _Principal:
    """Fail-closed 管理员鉴权与操作人身份提取。"""
    configured = config.admin_token.strip() if isinstance(config.admin_token, str) else ""
    if not configured:
        raise GmHttpError(401, "请先登录管理员账号")

    admin_keys = request.headers.getall("X-Admin-Key", [])
    gm_tokens = request.headers.getall("X-Gm-Token", [])
    supplied = [*admin_keys, *gm_tokens]
    if len(supplied) != 1:
        raise GmHttpError(401, "请先登录管理员账号")

    expected_digest = hashlib.sha256(configured.encode("utf-8")).digest()
    actual_digest = hashlib.sha256(supplied[0].encode("utf-8")).digest()
    if not hmac.compare_digest(expected_digest, actual_digest):
        raise GmHttpError(401, "请先登录管理员账号")

    op_headers = request.headers.getall("X-Gm-Operator", [])
    if len(op_headers) > 1:
        raise GmHttpError(401, "管理员身份无效或名称过长")
    operator = op_headers[0] if op_headers else "admin"
    if (
        not operator.strip()
        or len(operator.encode("utf-8")) > 64
        or "|" in operator
        or has_iso_control(operator)
    ):
        raise GmHttpError(401, "管理员身份无效或名称过长")

    perm_headers = request.headers.getall("X-Gm-Permissions", [])
    if perm_headers:
        requested = {item.strip() for header in perm_headers for item in header.split(",") if item.strip()}
        permissions = ALL_GM_PERMISSIONS.intersection(requested)
    else:
        permissions = ALL_GM_PERMISSIONS
    return _Principal(operator=operator, permissions=permissions)


def _require_permission(request: web.Request, config: GmAdminConfig, permission: str) -> str:
    principal = _authenticate(request, config)
    if not principal.has_permission(permission):
        raise GmHttpError(403, "没有此 GM 操作权限")
    return principal.operator


def _build_attachments(
    input_data: dict[str, Any],
    services: GameServicesClient,
    *,
    allow_legacy_flat: bool,
) -> dict[str, Any]:
    attachments: dict[str, Any] = {}
    has_currency_array = input_data.get("currencies") is not None
    has_item_array = input_data.get("items") is not None
    raw_flat_amount = input_data.get("currencyAmount")
    has_flat_currency = (
        allow_legacy_flat
        and raw_flat_amount is not None
        and (not isinstance(raw_flat_amount, str) or bool(raw_flat_amount.strip()))
    )
    has_flat_item = allow_legacy_flat and (
        input_data.get("itemConfigId") is not None or input_data.get("itemCount") is not None
    )

    if has_currency_array and (has_flat_currency or (allow_legacy_flat and input_data.get("currencyType") is not None)):
        _bad("货币附件不能同时混用数组和单字段格式")
    if has_item_array and has_flat_item:
        _bad("物品附件不能同时混用数组和单字段格式")

    currency_count = 0
    if has_currency_array:
        entries = input_data.get("currencies")
        if not isinstance(entries, list) or len(entries) > _MAX_CURRENCIES:
            _bad("货币附件数组格式非法或超过 4 种上限")
        seen_currencies: set[int] = set()
        out_currencies: list[dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                _bad("货币附件项必须是对象")
            _check_fields(entry, _CURRENCY_ENTRY_FIELDS)
            currency_type = _integer(entry, "currencyType", 0, _INT32_MAX)
            if not services.supports_currency(currency_type):
                _bad("未知币种")
            if currency_type in seen_currencies:
                _bad("货币附件不能包含重复币种")
            seen_currencies.add(currency_type)
            amount = _decimal_int64(_text(entry, "amount", 1, 19), "货币数量")
            out_currencies.append({"currencyType": currency_type, "amount": amount})
            currency_count += 1
        if out_currencies:
            attachments["currencies"] = out_currencies
    elif has_flat_currency:
        amount = _decimal_int64(_text(input_data, "currencyAmount", 1, 19), "货币数量")
        currency_type = (
            _integer(input_data, "currencyType", 0, _INT32_MAX)
            if input_data.get("currencyType") is not None
            else 0
        )
        if not services.supports_currency(currency_type):
            _bad("未知币种")
        attachments["currencies"] = [{"currencyType": currency_type, "amount": amount}]
        currency_count = 1

    item_entries = 0
    total_items = 0
    if has_item_array:
        entries = input_data.get("items")
        if not isinstance(entries, list) or len(entries) > _MAX_ITEMS:
            _bad("物品附件数组格式非法或超过 16 种上限")
        seen_items: set[int] = set()
        out_items: list[dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                _bad("物品附件项必须是对象")
            _check_fields(entry, _ITEM_ENTRY_FIELDS)
            config_id = _integer(entry, "configId", 1, _INT32_MAX)
            if config_id in seen_items:
                _bad("物品附件不能包含重复配置 ID")
            seen_items.add(config_id)
            count = _integer(entry, "count", 1, _MAX_TOTAL_ITEM_COUNT)
            total_items += count
            if total_items > _MAX_TOTAL_ITEM_COUNT:
                _bad("单封邮件物品总数不能超过 4096")
            out_items.append({"configId": config_id, "count": count})
            item_entries += 1
        if out_items:
            attachments["items"] = out_items
    elif has_flat_item:
        config_id = _integer(input_data, "itemConfigId", 1, _INT32_MAX)
        count = _integer(input_data, "itemCount", 1, _MAX_TOTAL_ITEM_COUNT)
        attachments["items"] = [{"configId": config_id, "count": count}]
        item_entries = 1

    if currency_count == 0 and item_entries == 0:
        _bad("至少填写一种货币或物品附件")
    return attachments


def _validate_player_command_arguments(
    operation: str,
    args: dict[str, Any],
    services: GameServicesClient,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if operation in ("currency.add", "currency.deduct", "currency.block", "currency.unblock"):
        amount_required = operation in ("currency.add", "currency.deduct")
        _check_fields(args, frozenset(("currencyType", "amount")) if amount_required else frozenset(("currencyType",)))
        currency_type = _integer(args, "currencyType", 0, _INT32_MAX)
        if not services.supports_currency(currency_type):
            _bad("未知币种")
        result["currencyType"] = currency_type
        if amount_required:
            result["amount"] = _decimal_int64(_text(args, "amount", 1, 19), "货币数量")
    elif operation == "player.level":
        _check_fields(args, frozenset(("level",)))
        result["level"] = _integer(args, "level", 1, MAX_PLAYER_LEVEL)
    elif operation == "pet.grant":
        _check_fields(args, frozenset(("petTableId",)))
        result["petTableId"] = _integer(args, "petTableId", 1, _INT32_MAX)
    elif operation == "item.grant":
        _check_fields(args, frozenset(("configId", "count")))
        result["configId"] = _integer(args, "configId", 1, _INT32_MAX)
        result["count"] = _integer(args, "count", 1, MAX_ITEM_GRANT_COUNT)
    else:
        _bad("未知 GM 操作")
    return result


def create_gm_app(
    journal: BaseGmJournal,
    services: GameServicesClient,
    config: GmAdminConfig,
    *,
    clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ready: bool = True,
) -> web.Application:
    """构建 gm_admin ``aiohttp.web.Application``，挂载全部 23 条 GM 路由与健康探针。"""
    state = {"ready": ready}

    @web.middleware
    async def error_middleware(request: web.Request, handler):
        try:
            return await handler(request)
        except GmHttpError as err:
            return _error(err.status, err.message)
        except CallFailure as err:
            return _error(502, f"游戏服务调用未完成：{err.code}")
        except web.HTTPException as err:
            return _error(err.status, err.reason)
        except Exception:
            logger.error("unhandled GM HTTP error path=%s", request.path, exc_info=True)
            return _error(500, "GM 服务内部错误")

    app = web.Application(
        client_max_size=_CAMPAIGN_MAX_BODY_BYTES + 4096,
        middlewares=[error_middleware],
    )
    app[READY_STATE_KEY] = state

    def _require_mail_enabled() -> None:
        if not services.mail_enabled:
            raise GmHttpError(503, "邮件服务未配置，暂不能发放")

    def _require_player_commands_enabled() -> None:
        if not services.player_commands_enabled:
            raise GmHttpError(503, "玩家 GM 管理通道未配置或未完成服务部署")

    # -- 健康探针 -------------------------------------------------------------------------------------

    async def health(_request: web.Request) -> web.Response:
        status_code = 200 if state["ready"] else 503
        return web.json_response({"status": "UP" if state["ready"] else "DOWN"}, status=status_code)

    async def liveness(_request: web.Request) -> web.Response:
        return web.json_response({"status": "UP"}, status=200)

    async def readiness(_request: web.Request) -> web.Response:
        status_code = 200 if state["ready"] else 503
        return web.json_response({"status": "UP" if state["ready"] else "OUT_OF_SERVICE"}, status=status_code)

    # -- 总览与玩家查询 -------------------------------------------------------------------------------

    async def overview(request: web.Request) -> web.Response:
        principal = _authenticate(request, config)
        if not principal.permissions:
            raise GmHttpError(403, "没有此 GM 操作权限")
        result = await services.overview(include_zones=principal.has_permission("gm:zone:read"))
        return _ok(result)

    async def get_player(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:player:read")
        player_id = _decimal_int64(request.match_info["playerId"], "玩家 ID")
        return _ok(await services.player(player_id))

    # -- 个人邮件（5 路由） ---------------------------------------------------------------------------

    async def prepare_mail(request: web.Request) -> web.Response:
        actor = _require_permission(request, config, "gm:mail:prepare")
        _require_mail_enabled()
        input_data = await _parse_json_object(request, _DEFAULT_MAX_BODY_BYTES)
        _check_fields(input_data, _DRAFT_FIELDS)
        client_id = _uuid_field(input_data)
        player_id = _decimal_int64(_text(input_data, "playerId", 1, 19), "玩家 ID")
        frozen_input = {
            "senderKind": 1,
            "title": _text(input_data, "title", 1, 128),
            "body": _text(input_data, "body", 0, 4096),
            "validDays": _integer(input_data, "validDays", 1, 30),
            "attachments": _build_attachments(input_data, services, allow_legacy_flat=True),
        }
        reason = _text(input_data, "reason", 1, 191)
        player_view = await services.player(player_id)
        if not player_view.get("found", False):
            _bad("未找到目标玩家，请核对玩家 ID")
        return _ok(await journal.prepare(actor, client_id, player_id, reason, frozen_input))

    async def send_mail(request: web.Request) -> web.Response:
        actor = _require_permission(request, config, "gm:mail:send")
        _require_mail_enabled()
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.send(command_id, actor, services.send_mail))

    async def list_mails(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        page = _parse_int_query(request, "page", 1)
        page_size = _parse_int_query(request, "pageSize", 50)
        player_id = request.query.get("playerId")
        status = request.query.get("status")
        return _ok(await journal.list_mails(page, page_size, player_id, status))

    async def get_mail(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.get(command_id, "MAIL"))

    async def mail_audit(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.audit_history(command_id, "MAIL"))

    # -- 全服纯文本系统邮件（5 路由） -----------------------------------------------------------------

    async def prepare_system_mail(request: web.Request) -> web.Response:
        actor = _require_permission(request, config, "gm:system-mail:prepare")
        _require_mail_enabled()
        input_data = await _parse_json_object(request, _DEFAULT_MAX_BODY_BYTES)
        _check_fields(input_data, _SYSTEM_MAIL_FIELDS)
        client_id = _uuid_field(input_data)
        frozen_input = {
            "senderKind": 1,
            "title": _text(input_data, "title", 1, 128),
            "body": _text(input_data, "body", 0, 4096),
            "validDays": _integer(input_data, "validDays", 1, 30),
        }
        reason = _text(input_data, "reason", 1, 191)
        return _ok(await journal.prepare_system_mail(actor, client_id, reason, frozen_input))

    async def send_system_mail(request: web.Request) -> web.Response:
        actor = _require_permission(request, config, "gm:system-mail:send")
        _require_mail_enabled()
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.send_system_mail(command_id, actor, services.send_system_mail))

    async def list_system_mails(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        page = _parse_int_query(request, "page", 1)
        page_size = _parse_int_query(request, "pageSize", 50)
        status = request.query.get("status")
        return _ok(await journal.list_system_mails(page, page_size, status))

    async def get_system_mail(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.get(command_id, "SYSTEM_MAIL"))

    async def system_mail_audit(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.audit_history(command_id, "SYSTEM_MAIL"))

    # -- 冻结名单活动邮件（5 路由） -------------------------------------------------------------------

    async def prepare_mail_campaign(request: web.Request) -> web.Response:
        actor = _require_permission(request, config, "gm:system-mail:prepare")
        _require_mail_enabled()
        input_data = await _parse_json_object(request, _CAMPAIGN_MAX_BODY_BYTES)
        _check_fields(input_data, _CAMPAIGN_FIELDS)
        client_id = _uuid_field(input_data)

        max_targets = config.max_campaign_targets
        raw_recipients = input_data.get("recipientPlayerIds")
        if (
            not isinstance(raw_recipients, list)
            or not raw_recipients
            or len(raw_recipients) > max_targets * 2
        ):
            _bad(f"收件人名单必须是非空玩家 ID 数组，且去重后不超过 {max_targets} 人")
        recipient_ids: list[str] = []
        for item in raw_recipients:
            if not isinstance(item, str):
                _bad("收件人玩家 ID 必须是十进制字符串")
            recipient_ids.append(item)
        try:
            plan = MailCampaignManifest.build(recipient_ids, max_recipients=max_targets)
        except ValueError as err:
            _bad(str(err))

        mail_payload = {
            "senderKind": 1,
            "title": _text(input_data, "title", 1, 128),
            "body": _text(input_data, "body", 0, 4096),
            "validDays": _integer(input_data, "validDays", 1, 30),
            "expectedRecipientCount": str(plan.recipient_count),
            "expectedShardCount": len(plan.shards),
            "manifestHash": plan.manifest_hash,
            "attachments": _build_attachments(input_data, services, allow_legacy_flat=False),
        }
        campaign_input = {
            "mail": mail_payload,
            "shards": [list(shard) for shard in plan.shards],
        }
        reason = _text(input_data, "reason", 1, 191)
        return _ok(await journal.prepare_mail_campaign(actor, client_id, reason, campaign_input))

    async def send_mail_campaign(request: web.Request) -> web.Response:
        actor = _require_permission(request, config, "gm:system-mail:send")
        _require_mail_enabled()
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.send_mail_campaign(command_id, actor, services.send_mail_campaign))

    async def list_mail_campaigns(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        page = _parse_int_query(request, "page", 1)
        page_size = _parse_int_query(request, "pageSize", 50)
        status = request.query.get("status")
        return _ok(await journal.list_mail_campaigns(page, page_size, status))

    async def get_mail_campaign(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.get(command_id, "MAIL_CAMPAIGN"))

    async def mail_campaign_audit(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.audit_history(command_id, "MAIL_CAMPAIGN"))

    # -- 在线玩家 GM 命令（6 路由） -------------------------------------------------------------------

    async def player_command_catalog(request: web.Request) -> web.Response:
        principal = _authenticate(request, config)
        if not principal.has_permission("gm:audit:read") and not any(
            principal.has_permission(op.permission) for op in PLAYER_OPERATIONS
        ):
            raise GmHttpError(403, "没有此 GM 操作权限")
        enabled = services.player_commands_enabled
        return _ok(
            {
                "enabled": enabled,
                "limits": {"maxItemCount": MAX_ITEM_GRANT_COUNT, "maxLevel": MAX_PLAYER_LEVEL},
                "currencies": services.currency_types(),
                "operations": [
                    {
                        "code": op.code,
                        "label": op.label,
                        "permission": op.permission,
                        "enabled": enabled and principal.has_permission(op.permission),
                        "description": _PLAYER_OP_DESCRIPTION,
                    }
                    for op in PLAYER_OPERATIONS
                ],
            }
        )

    async def prepare_player_command(request: web.Request) -> web.Response:
        _authenticate(request, config)
        input_data = await _parse_json_object(request, _DEFAULT_MAX_BODY_BYTES)
        _check_fields(input_data, _PLAYER_DRAFT_FIELDS)
        op_code = _text(input_data, "operation", 1, 32)
        spec = PLAYER_OPERATIONS_BY_CODE.get(op_code)
        if spec is None:
            _bad("未知 GM 操作")
        actor = _require_permission(request, config, spec.permission)
        _require_player_commands_enabled()

        client_id = _text(input_data, "clientRequestId", 36, 36)
        try:
            if str(uuid.UUID(client_id)) != client_id:
                _bad("请求号无效")
        except ValueError:
            _bad("请求号无效")

        player_id = _decimal_int64(_text(input_data, "playerId", 1, 19), "玩家 ID")
        reason = _text(input_data, "reason", 1, 191)
        if len(reason.encode("utf-8")) > 768 or has_iso_control(reason):
            _bad("操作原因过长或包含控制字符")
        raw_args = input_data.get("arguments")
        if not isinstance(raw_args, dict):
            _bad("命令参数必须是对象")
        validated_args = _validate_player_command_arguments(spec.code, raw_args, services)

        target = await services.player(player_id)
        session = target.get("session") if isinstance(target.get("session"), dict) else {}
        if (
            not target.get("found", False)
            or not target.get("sessionFound", False)
            or session.get("state") != "SESSION_STATE_ONLINE"
        ):
            _bad("目标玩家尚未在线，请核对玩家 ID 与当前会话")

        frozen_input = {"operation": spec.code, "arguments": validated_args}
        return _ok(await journal.prepare_player_command(actor, client_id, player_id, reason, frozen_input))

    async def send_player_command(request: web.Request) -> web.Response:
        principal = _authenticate(request, config)
        if not any(principal.has_permission(op.permission) for op in PLAYER_OPERATIONS):
            raise GmHttpError(403, "没有此 GM 操作权限")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        command = await journal.get(command_id)
        if command.get("commandType") != "PLAYER":
            _bad("命令类型不符")
        spec = PLAYER_OPERATIONS_BY_CODE.get(str(command.get("operation", "")))
        if spec is None:
            _bad("未知 GM 操作")
        actor = _require_permission(request, config, spec.permission)
        _require_player_commands_enabled()
        if command.get("status") == "DRAFT":
            req_node = command.get("request", {})
            raw_issued = req_node.get("issuedAtMs", 0)
            try:
                issued_at = int(raw_issued)
            except (TypeError, ValueError):
                issued_at = 0
            age = clock_ms() - issued_at
            if issued_at <= 0 or age < 0 or age > PLAYER_DRAFT_TTL_MS:
                raise GmHttpError(409, "草稿已过期，请重新创建并核对")
        return _ok(await journal.send_player_command(command_id, actor, services.send_player_command))

    async def list_player_commands(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        page = _parse_int_query(request, "page", 1)
        page_size = _parse_int_query(request, "pageSize", 50)
        player_id = request.query.get("playerId")
        status = request.query.get("status")
        return _ok(await journal.list_player_commands(page, page_size, player_id, status))

    async def get_player_command(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.get(command_id, "PLAYER"))

    async def player_command_audit(request: web.Request) -> web.Response:
        _require_permission(request, config, "gm:audit:read")
        command_id = _decimal_int64(request.match_info["id"], "命令 ID")
        return _ok(await journal.audit_history(command_id, "PLAYER"))

    # -- 注册全部 23 条 GM 路由 + 健康探针 ------------------------------------------------------------

    app.add_routes(
        [
            web.get("/actuator/health", health),
            web.get("/actuator/health/liveness", liveness),
            web.get("/actuator/health/readiness", readiness),
            # 1-2: 总览与玩家查询
            web.get("/gm/overview", overview),
            web.get("/gm/players/{playerId}", get_player),
            # 3-7: 个人邮件
            web.post("/gm/mail/drafts", prepare_mail),
            web.post("/gm/mail/commands/{id}/send", send_mail),
            web.get("/gm/mail/commands", list_mails),
            web.get("/gm/mail/commands/{id}", get_mail),
            web.get("/gm/mail/commands/{id}/audit", mail_audit),
            # 8-12: 系统邮件
            web.post("/gm/system-mail/drafts", prepare_system_mail),
            web.post("/gm/system-mail/commands/{id}/send", send_system_mail),
            web.get("/gm/system-mail/commands", list_system_mails),
            web.get("/gm/system-mail/commands/{id}", get_system_mail),
            web.get("/gm/system-mail/commands/{id}/audit", system_mail_audit),
            # 13-17: 活动邮件
            web.post("/gm/mail-campaigns/drafts", prepare_mail_campaign),
            web.post("/gm/mail-campaigns/commands/{id}/send", send_mail_campaign),
            web.get("/gm/mail-campaigns/commands", list_mail_campaigns),
            web.get("/gm/mail-campaigns/commands/{id}", get_mail_campaign),
            web.get("/gm/mail-campaigns/commands/{id}/audit", mail_campaign_audit),
            # 18-23: 在线玩家 GM 命令
            web.get("/gm/player-commands/catalog", player_command_catalog),
            web.post("/gm/player-commands/drafts", prepare_player_command),
            web.post("/gm/player-commands/{id}/send", send_player_command),
            web.get("/gm/player-commands", list_player_commands),
            web.get("/gm/player-commands/{id}", get_player_command),
            web.get("/gm/player-commands/{id}/audit", player_command_audit),
        ]
    )
    return app
