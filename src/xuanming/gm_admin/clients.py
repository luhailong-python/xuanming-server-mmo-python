"""下游游戏服务受限 gRPC/HTTP 适配器（对齐 Java GameProto、GameServices 与 GamePlayerAdmin）。

设计要点：
- 仅消费权威 ``Protocol`` 描述符与既有 ``GrpcClient``，不直接访问游戏数据库，也不伪造玩家会话；
- 邮件管理 RPC（``mailpb.MailAdmin/*``）复用 ``grpc_runtime.sign_service_call`` 生成 v1 HMAC-SHA256 证明；
- 场景在线玩家 GM（``scene_node.SceneNodeGrpc/GmExecutePlayerCommand``）生成与 C++ Scene / Java GameProto
  逐字节一致的 ``gm-player-command-v1`` HMAC-SHA256 签名，并支持静态映射 + 可信 etcd v3 动态发现。
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import hmac
import json
import logging
import re
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import aiohttp
import grpc
from google.protobuf import json_format
from google.protobuf.message import Message

from ..grpc_runtime import GrpcClient, sign_service_call
from ..protocol import Protocol
from .config import (
    MAX_SCENE_CHANNELS,
    MIN_HMAC_SECRET_BYTES,
    GmAdminConfig,
    validate_grpc_target,
    validate_http_url,
    validate_node_id,
)
from .journal import (
    INT64_MAX,
    UINT64_MAX,
    CallFailure,
    MailCampaignManifest,
)

logger = logging.getLogger(__name__)

# -- 下游 RPC 方法常量 --------------------------------------------------------------------------------

DISPLAY_METHOD = "/data_service.DataService/BatchGetPlayerDisplay"
HOME_ZONE_METHOD = "/data_service.DataService/GetPlayerHomeZone"
SESSION_METHOD = "/playerlocator.PlayerLocator/GetSession"
SEND_MAIL_METHOD = "/mailpb.MailAdmin/SendPersonalMail"
SEND_SYSTEM_MAIL_METHOD = "/mailpb.MailAdmin/SendSystemMail"
IMPORT_RECIPIENTS_METHOD = "/mailpb.MailAdmin/ImportMailRecipients"
SEAL_CAMPAIGN_METHOD = "/mailpb.MailAdmin/SealMailCampaign"
PLAYER_COMMAND_METHOD = "/scene_node.SceneNodeGrpc/GmExecutePlayerCommand"

MAX_PLAYER_LEVEL = 85
MAX_ITEM_GRANT_COUNT = 99
MISS_REFRESH_COOLDOWN_SEC = 1.0
MAX_ETCD_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_ZONES_RESPONSE_BYTES = 262_144

_SCENE_RPC_PREFIX = "SceneNodeService.rpc/"
_SCENE_RPC_RANGE_END = "SceneNodeService.rpc0"
_ETCD_RANGE_PAYLOAD = json.dumps(
    {
        "key": base64.b64encode(_SCENE_RPC_PREFIX.encode("utf-8")).decode("ascii"),
        "range_end": base64.b64encode(_SCENE_RPC_RANGE_END.encode("utf-8")).decode("ascii"),
    },
    separators=(",", ":"),
)

_POSITIVE_UINT64_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_UINT32_MAX = 0xFFFF_FFFF

_REFUSED_GRPC_CODES = frozenset((
    grpc.StatusCode.UNAUTHENTICATED,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.UNIMPLEMENTED,
))


@dataclass(frozen=True, slots=True)
class PlayerOperationSpec:
    """开放给 GM 后台的在线玩家命令目录项。"""

    code: str
    label: str
    permission: str
    proto_field: str


# 与 Java GmPlayerCommandController.OPERATIONS / GamePlayerAdmin.COMMANDS 逐项对齐。
PLAYER_OPERATIONS: tuple[PlayerOperationSpec, ...] = (
    PlayerOperationSpec("currency.add", "增加货币", "gm:currency:add", "addCurrency"),
    PlayerOperationSpec("currency.deduct", "扣除货币", "gm:currency:deduct", "deductCurrency"),
    PlayerOperationSpec("currency.block", "封禁货币获取", "gm:currency:block", "blockCurrency"),
    PlayerOperationSpec("currency.unblock", "解除货币封禁", "gm:currency:unblock", "unblockCurrency"),
    PlayerOperationSpec("player.level", "设置等级", "gm:player:level", "setLevel"),
    PlayerOperationSpec("pet.grant", "发放宝宝", "gm:pet:grant", "grantPet"),
    PlayerOperationSpec("item.grant", "发放物品", "gm:item:grant", "grantItem"),
)

PLAYER_OPERATIONS_BY_CODE: dict[str, PlayerOperationSpec] = {op.code: op for op in PLAYER_OPERATIONS}

_PLAYER_FROZEN_FIELDS = frozenset((
    "operation",
    "arguments",
    "requestId",
    "playerId",
    "operator",
    "reason",
    "issuedAtMs",
))


def has_iso_control(text: str) -> bool:
    """等价于 Java ``Character.isISOControl``：检测 U+0000..U+001F 与 U+007F..U+009F。"""
    return any(ord(ch) <= 0x1F or 0x7F <= ord(ch) <= 0x9F for ch in text)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _invalid(message: str) -> CallFailure:
    return CallFailure("INVALID_ARGUMENT", message, outcome_unknown=False, tip_id=0)


def _positive_uint64(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _POSITIVE_UINT64_RE.fullmatch(value) or int(value) > UINT64_MAX:
        raise _invalid(f"{label}必须是非零 uint64 十进制字符串")
    return value


def _uint32(value: Any, label: str, *, allow_zero: bool) -> int:
    minimum = 0 if allow_zero else 1
    if not _is_int(value) or value < minimum or value > _UINT32_MAX:
        raise _invalid(f"{label}必须是游戏协议范围内的整数")
    return value


def _require_string_id(payload: dict[str, Any], camel: str, snake: str) -> str:
    if camel in payload and snake in payload:
        raise _invalid("同一字段不能使用两种名称重复提交")
    value = payload.get(camel) if camel in payload else payload.get(snake)
    if not isinstance(value, str):
        raise _invalid(f"{camel}必须是十进制字符串")
    return _positive_uint64(value, camel)


def _format_endpoint(ip: str, port: int) -> str:
    if ":" in ip and not ip.startswith("["):
        return f"[{ip}]:{port}"
    return f"{ip}:{port}"


def _decode_base64_utf8(raw: Any) -> str | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return base64.b64decode(raw, validate=True).decode("utf-8")
    except Exception:
        return None


def sign_player_command(
    method: str,
    request: Message,
    secret: bytes,
    timestamp_seconds: int,
    nonce: str,
) -> None:
    """构造与 Java ``GameProto.signPlayerCommand`` / C++ Scene 逐字节一致的 ``auth`` 签名信封。"""
    full_method = method if method.startswith("/") else f"/{method}"
    actor = str(getattr(request, "operator", ""))
    target_node_id = int(getattr(request, "target_node_id", 0))
    if (
        target_node_id <= 0
        or target_node_id > _UINT32_MAX
        or timestamp_seconds <= 0
        or not actor.strip()
        or len(actor.encode("utf-8")) > 64
        or "|" in actor
        or has_iso_control(actor)
        or not isinstance(nonce, str)
        or not _NONCE_RE.fullmatch(nonce)
        or len(secret) < MIN_HMAC_SECRET_BYTES
    ):
        raise ValueError("场景管理签名参数非法")

    business = type(request)()
    business.CopyFrom(request)
    business.ClearField("auth")
    digest = hashlib.sha256(business.SerializeToString(deterministic=True)).hexdigest()
    reason = f"gm-player-command-v1\n{digest}"
    canonical = f"{full_method}\n{target_node_id}\n{actor}\n{timestamp_seconds}\n{nonce}\n{reason}"
    signature = hmac.digest(secret, canonical.encode("utf-8"), "sha256").hex()
    request.auth = f"{actor}|{timestamp_seconds}|{nonce}|{signature}"


def _proto_method_types(protocol: Protocol, path: str) -> tuple[str, str]:
    normalized = path.lstrip("/")
    service_name, method_name = normalized.split("/", 1)
    service_desc = protocol.pool.FindServiceByName(service_name)
    method_desc = service_desc.methods_by_name[method_name]
    return method_desc.input_type.full_name, method_desc.output_type.full_name


async def _invoke_unary(
    protocol: Protocol,
    client: GrpcClient,
    path: str,
    request_msg: Message,
    *,
    timeout: float,
    writing: bool,
    unknown_message: str,
) -> Message:
    if timeout <= 0:
        raise CallFailure("DEADLINE_EXCEEDED", "游戏服务调用预算已耗尽", outcome_unknown=False)
    _, output_type = _proto_method_types(protocol, path)
    try:
        raw_reply = await client.call(path, request_msg.SerializeToString(), timeout=timeout)
    except grpc.aio.AioRpcError as error:
        code = error.code()
        refused = code in _REFUSED_GRPC_CODES
        unknown = writing and not refused
        msg = unknown_message if unknown else f"游戏服务调用未完成：{code.name}"
        raise CallFailure(code.name, msg, outcome_unknown=unknown) from error
    except asyncio.TimeoutError as error:
        raise CallFailure(
            "DEADLINE_EXCEEDED",
            unknown_message if writing else "游戏服务调用超时",
            outcome_unknown=writing,
        ) from error
    except Exception as error:
        raise CallFailure(
            "UNAVAILABLE",
            unknown_message if writing else "游戏服务连接异常",
            outcome_unknown=writing,
        ) from error
    try:
        return protocol.parse(output_type, raw_reply)
    except Exception as error:
        raise CallFailure("INVALID_RESPONSE", "游戏服务响应无法解析", outcome_unknown=writing) from error


def _message_to_dict(message: Message) -> dict[str, Any]:
    return json_format.MessageToDict(message, preserving_proto_field_name=False)


def _dict_to_message(protocol: Protocol, path: str, payload: dict[str, Any]) -> Message:
    input_type, _ = _proto_method_types(protocol, path)
    msg = protocol.message(input_type)
    json_format.ParseDict(payload, msg)
    return msg


def _currency_catalog(protocol: Protocol) -> list[dict[str, Any]]:
    enum_desc = protocol.pool.FindEnumTypeByName("common.base.CurrencyType")
    return [{"id": v.number, "label": v.name} for v in enum_desc.values]


# -- 在线玩家 GM 调度与场景路由（GamePlayerAdmin） ----------------------------------------------------


@dataclass(slots=True)
class _DynamicSceneChannel:
    endpoint: str
    client: GrpcClient


class GamePlayerAdmin:
    """七项在线玩家 GM 操作适配器：先经 PlayerLocator 核验落点，再签名直连对应 SceneNodeGrpc。"""

    def __init__(
        self,
        protocol: Protocol,
        *,
        locator: GrpcClient | None,
        static_targets: dict[str, str],
        scene_etcd_url: str,
        scene_admin_secret: bytes,
        plaintext: bool = True,
        budget_sec: float = 5.0,
        discovery_ttl_sec: float = 5.0,
        channel_factory: Callable[[str], GrpcClient] | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._locator = locator
        self._plaintext = plaintext
        self._secret = bytes(scene_admin_secret)
        self._budget_sec = budget_sec
        self._discovery_ttl_sec = max(0.0, discovery_ttl_sec)
        self._clock_ms = clock_ms
        self._channel_factory = channel_factory or (
            lambda endpoint: GrpcClient(endpoint, timeout=budget_sec, receive_limit=1 << 20, enable_retries=False)
        )
        self._etcd_range_url = (
            f"{validate_http_url(scene_etcd_url, label='scene_etcd_url')}/v3/kv/range"
            if scene_etcd_url.strip()
            else ""
        )
        self._http_session: aiohttp.ClientSession | None = None
        self._static_scenes: dict[str, GrpcClient] = {}
        try:
            for key, target in static_targets.items():
                validated = validate_grpc_target(target, plaintext=plaintext, label=f"scene_targets[{key}]")
                self._static_scenes[key] = self._channel_factory(validated)
        except Exception:
            # 构造阶段失败时尽力释放已建连接（同步场景由调用方兜底 close）
            raise

        self._discovery_lock = asyncio.Lock()
        self._dynamic_scenes: OrderedDict[str, _DynamicSceneChannel] = OrderedDict()
        self._discovered_by_zone_node: dict[str, str] = {}
        self._ambiguous_zone_nodes: set[str] = set()
        self._discovered_by_node_only: dict[str, str] = {}
        self._ambiguous_node_only: set[str] = set()
        self._last_discovery_monotonic: float = 0.0
        self._supported_currencies = {item["id"] for item in _currency_catalog(protocol)}

    @property
    def enabled(self) -> bool:
        return (
            self._locator is not None
            and (bool(self._static_scenes) or bool(self._etcd_range_url))
            and len(self._secret) >= MIN_HMAC_SECRET_BYTES
        )

    def _validate_arguments(self, operation: str, arguments: dict[str, Any]) -> None:
        allowed_map: dict[str, frozenset[str]] = {
            "currency.add": frozenset(("currencyType", "amount")),
            "currency.deduct": frozenset(("currencyType", "amount")),
            "currency.block": frozenset(("currencyType",)),
            "currency.unblock": frozenset(("currencyType",)),
            "player.level": frozenset(("level",)),
            "pet.grant": frozenset(("petTableId",)),
            "item.grant": frozenset(("configId", "count")),
        }
        allowed = allowed_map.get(operation)
        if allowed is None:
            raise _invalid("玩家操作未开放")
        if set(arguments.keys()) != allowed:
            raise _invalid("操作参数字段不完整或包含未允许参数")
        for field_name in allowed:
            raw_val = arguments.get(field_name)
            if field_name == "amount":
                amount = _positive_uint64(raw_val, "金额")
                if int(amount) > INT64_MAX:
                    raise _invalid("金额超出游戏协议范围")
            else:
                number = _uint32(raw_val, field_name, allow_zero=(field_name == "currencyType"))
                if field_name == "currencyType" and number not in self._supported_currencies:
                    raise _invalid("币种不在游戏协议目录中")
                if field_name == "level" and number > MAX_PLAYER_LEVEL:
                    raise _invalid(f"玩家等级必须在 1 至 {MAX_PLAYER_LEVEL} 之间")
                if field_name in ("configId", "petTableId") and number > (1 << 31) - 1:
                    raise _invalid(f"{field_name}超出已开放的游戏管理范围")
                if field_name == "count" and number > MAX_ITEM_GRANT_COUNT:
                    raise _invalid(f"单次在线 GM 发物不能超过 {MAX_ITEM_GRANT_COUNT} 个")

    async def execute(self, frozen: dict[str, Any]) -> dict[str, Any]:
        """校验冻结玩家命令，核对在线会话落点并签名发送至目标 SceneNodeGrpc。"""
        if not self.enabled or self._locator is None:
            raise CallFailure("NOT_CONFIGURED", "在线玩家管理服务尚未配置", outcome_unknown=False)
        if not isinstance(frozen, dict) or set(frozen.keys()) != _PLAYER_FROZEN_FIELDS:
            raise _invalid("玩家命令字段不完整或包含未允许字段")

        operation = frozen.get("operation")
        if not isinstance(operation, str) or operation not in PLAYER_OPERATIONS_BY_CODE:
            raise _invalid("玩家操作未开放")
        spec = PLAYER_OPERATIONS_BY_CODE[operation]

        request_id = _positive_uint64(frozen.get("requestId"), "请求 ID")
        player_id = _positive_uint64(frozen.get("playerId"), "玩家 ID")
        actor = frozen.get("operator")
        reason = frozen.get("reason")
        if (
            not isinstance(actor, str)
            or not actor.strip()
            or len(actor.encode("utf-8")) > 64
            or "|" in actor
            or has_iso_control(actor)
        ):
            raise _invalid("操作人非法")
        if (
            not isinstance(reason, str)
            or not reason.strip()
            or len(reason.encode("utf-8")) > 768
            or has_iso_control(reason)
        ):
            raise _invalid("处置理由非法")

        issued_raw = frozen.get("issuedAtMs")
        if _is_int(issued_raw):
            issued = int(issued_raw)
        else:
            issued = int(_positive_uint64(issued_raw, "冻结时间"))
        now_ms = self._clock_ms()
        if issued <= 0 or issued < now_ms - 300_000 or issued > now_ms + 300_000:
            raise _invalid("玩家命令已过期，请重新预览")

        arguments = frozen.get("arguments")
        if not isinstance(arguments, dict):
            raise _invalid("操作参数必须是对象")
        self._validate_arguments(operation, arguments)

        deadline = time.monotonic() + self._budget_sec
        session_req = _dict_to_message(
            self._protocol,
            SESSION_METHOD,
            {"playerId": player_id, "includePlacement": True},
        )
        session_reply_msg = await _invoke_unary(
            self._protocol,
            self._locator,
            SESSION_METHOD,
            session_req,
            timeout=deadline - time.monotonic(),
            writing=False,
            unknown_message="玩家定位调用异常",
        )
        session_reply = _message_to_dict(session_reply_msg)
        session = session_reply.get("session")
        if (
            not session_reply.get("found", False)
            or not isinstance(session, dict)
            or session.get("state") != "SESSION_STATE_ONLINE"
        ):
            raise CallFailure("PLAYER_OFFLINE", "玩家当前不在线，不能执行在线命令", outcome_unknown=False)
        if str(session.get("playerId", "")) != player_id:
            raise CallFailure("INVALID_RESPONSE", "玩家定位结果不匹配", outcome_unknown=False)

        session_id = _uint32(session.get("sessionId"), "会话 ID", allow_zero=False)
        generation = _positive_uint64(str(session.get("loginGeneration", "")), "登录代次")

        placement = session_reply.get("placement")
        if isinstance(placement, dict) and session_reply_msg.HasField("placement"):
            if not placement.get("found", False):
                raise CallFailure("PLAYER_NOT_IN_SCENE", "玩家尚未进入场景，不能执行在线命令", outcome_unknown=False)
            if placement.get("awaitingPlacement", False):
                raise CallFailure("PLAYER_HANDOFF", "玩家正在跨区或切图交接中，请稍后重试", outcome_unknown=False)
            placement_session_id = int(placement.get("sessionId", 0) or 0)
            placement_generation = str(placement.get("loginGeneration", "0") or "0")
            if (placement_session_id != 0 and placement_session_id != session_id) or (
                placement_generation not in ("", "0") and placement_generation != generation
            ):
                raise CallFailure("SESSION_MISMATCH", "玩家会话与场景落点尚未同步，请稍后重试", outcome_unknown=False)
            target_node_id = str(placement.get("nodeId", ""))
            target_zone_id = int(placement.get("zoneId", 0) or 0)
        else:
            target_node_id = str(session.get("sceneNodeId", ""))
            target_zone_id = int(session.get("gateZoneId", 0) or 0)

        try:
            target_node_num = validate_node_id(target_node_id)
        except ValueError as exc:
            raise _invalid("场景节点 ID 非法") from exc

        scene_client = await self._resolve_scene(target_zone_id, target_node_id, deadline)
        wire: dict[str, Any] = {
            "commandId": request_id,
            "playerId": player_id,
            "operator": actor,
            "reason": reason,
            "issuedAtMs": str(issued),
            "targetNodeId": target_node_num,
            "expectedSessionId": session_id,
            "expectedLoginGeneration": generation,
            spec.proto_field: copy.deepcopy(arguments),
        }
        try:
            cmd_req = _dict_to_message(self._protocol, PLAYER_COMMAND_METHOD, wire)
            sign_player_command(PLAYER_COMMAND_METHOD, cmd_req, self._secret, issued // 1000, request_id)
        except Exception as exc:
            raise _invalid("玩家命令不符合游戏协议") from exc

        reply_msg = await _invoke_unary(
            self._protocol,
            scene_client,
            PLAYER_COMMAND_METHOD,
            cmd_req,
            timeout=deadline - time.monotonic(),
            writing=True,
            unknown_message="玩家命令结果未知，请保留原工单核对",
        )
        response = _message_to_dict(reply_msg)
        tip = int(reply_msg.error_message.id) if reply_msg.HasField("error_message") else 0
        if tip != 0:
            raise CallFailure("SERVER_TIP", "服务器未确认命令完成，请保留原工单核对", outcome_unknown=True, tip_id=tip)
        if str(reply_msg.command_id) != request_id or not bool(reply_msg.executed):
            raise CallFailure("INVALID_RESPONSE", "玩家命令缺少匹配的完成凭据", outcome_unknown=True)
        return response

    async def _resolve_scene(self, zone_id: int, target_node_id: str, deadline: float) -> GrpcClient:
        if zone_id > 0:
            by_zone = self._static_scenes.get(f"{zone_id}/{target_node_id}")
            if by_zone is not None:
                return by_zone
        configured = self._static_scenes.get(target_node_id)
        if configured is not None:
            return configured
        if not self._etcd_range_url:
            raise CallFailure("SCENE_NOT_CONFIGURED", "玩家所在场景未接入管理服务", outcome_unknown=False)

        lookup_key = f"{zone_id}/{target_node_id}" if zone_id > 0 else target_node_id
        async with self._discovery_lock:
            now = time.monotonic()
            expired = self._last_discovery_monotonic == 0.0 or (
                now - self._last_discovery_monotonic >= self._discovery_ttl_sec
            )
            known = (
                lookup_key in self._discovered_by_zone_node or lookup_key in self._ambiguous_zone_nodes
                if zone_id > 0
                else lookup_key in self._discovered_by_node_only or lookup_key in self._ambiguous_node_only
            )
            cooldown = min(MISS_REFRESH_COOLDOWN_SEC, self._discovery_ttl_sec)
            miss_refreshable = (not known) and (now - self._last_discovery_monotonic >= cooldown)
            if expired or miss_refreshable:
                await self._refresh_from_etcd(deadline)

            ambiguous = (
                lookup_key in self._ambiguous_zone_nodes if zone_id > 0 else lookup_key in self._ambiguous_node_only
            )
            if ambiguous:
                raise CallFailure("SCENE_AMBIGUOUS", "目标场景节点存在重复注册，已拒绝执行", outcome_unknown=False)

            endpoint = (
                self._discovered_by_zone_node.get(lookup_key)
                if zone_id > 0
                else self._discovered_by_node_only.get(lookup_key)
            )
            if not endpoint:
                raise CallFailure("SCENE_NOT_CONFIGURED", "玩家所在场景未接入管理服务", outcome_unknown=False)

            existing = self._dynamic_scenes.get(lookup_key)
            if existing is not None and existing.endpoint == endpoint:
                self._dynamic_scenes.move_to_end(lookup_key)
                return existing.client
            if existing is not None:
                self._dynamic_scenes.pop(lookup_key, None)
                await existing.client.close()
            if len(self._dynamic_scenes) >= MAX_SCENE_CHANNELS:
                _, evicted = self._dynamic_scenes.popitem(last=False)
                await evicted.client.close()

            try:
                validated = validate_grpc_target(endpoint, plaintext=self._plaintext, label="discovered scene")
            except ValueError as exc:
                raise CallFailure("SCENE_TARGET_INVALID", "发现的场景地址不符合连接安全策略", outcome_unknown=False) from exc

            client = self._channel_factory(validated)
            self._dynamic_scenes[lookup_key] = _DynamicSceneChannel(endpoint=endpoint, client=client)
            return client

    async def _refresh_from_etcd(self, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CallFailure("DEADLINE_EXCEEDED", "场景发现前调用预算已耗尽", outcome_unknown=False)
        timeout = max(0.2, min(3.0, remaining))
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        try:
            async with self._http_session.post(
                self._etcd_range_url,
                data=_ETCD_RANGE_PAYLOAD.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                raw = await response.content.read(MAX_ETCD_RESPONSE_BYTES + 1)
                if response.status != 200 or len(raw) > MAX_ETCD_RESPONSE_BYTES:
                    raise CallFailure("SCENE_DISCOVERY_UNAVAILABLE", "场景服务发现返回异常状态", outcome_unknown=False)
                root = json.loads(raw)
        except CallFailure:
            raise
        except Exception as exc:
            raise CallFailure("SCENE_DISCOVERY_UNAVAILABLE", "场景服务发现不可用", outcome_unknown=False) from exc

        by_zone_node: dict[str, str] = {}
        dup_zone_node: set[str] = set()
        by_node_only: dict[str, str] = {}
        dup_node_only: set[str] = set()
        kvs = root.get("kvs") if isinstance(root, dict) else None
        if isinstance(kvs, list):
            for kv in kvs:
                if not isinstance(kv, dict):
                    continue
                key_text = _decode_base64_utf8(kv.get("key"))
                if not key_text or "/allocated/" in key_text:
                    continue
                val_text = _decode_base64_utf8(kv.get("value"))
                if not val_text:
                    continue
                try:
                    reg = json.loads(val_text)
                except ValueError:
                    continue
                if not isinstance(reg, dict):
                    continue
                node_id_val = reg.get("nodeId")
                if not _is_int(node_id_val) or not 1 <= node_id_val <= _UINT32_MAX:
                    continue
                zone_val = reg.get("zoneId")
                reg_zone = zone_val if _is_int(zone_val) and 1 <= zone_val <= _UINT32_MAX else 0
                grpc_ep = reg.get("grpcEndpoint")
                if not isinstance(grpc_ep, dict):
                    continue
                ip = str(grpc_ep.get("ip", "")).strip()
                port = grpc_ep.get("port", 0)
                if not ip or not _is_int(port) or not 1 <= port <= 65535:
                    continue
                node_key = str(node_id_val)
                endpoint = _format_endpoint(ip, port)
                _record_discovery(by_node_only, dup_node_only, node_key, endpoint)
                if reg_zone > 0:
                    _record_discovery(by_zone_node, dup_zone_node, f"{reg_zone}/{node_key}", endpoint)

        self._discovered_by_zone_node = by_zone_node
        self._ambiguous_zone_nodes = dup_zone_node
        self._discovered_by_node_only = by_node_only
        self._ambiguous_node_only = dup_node_only
        self._last_discovery_monotonic = time.monotonic()

        stale_keys: list[str] = []
        for key, entry in self._dynamic_scenes.items():
            latest = by_zone_node.get(key) if "/" in key else by_node_only.get(key)
            if latest != entry.endpoint:
                stale_keys.append(key)
        for key in stale_keys:
            evicted = self._dynamic_scenes.pop(key, None)
            if evicted is not None:
                await evicted.client.close()

    async def close(self) -> None:
        for client in self._static_scenes.values():
            await client.close()
        async with self._discovery_lock:
            for entry in self._dynamic_scenes.values():
                await entry.client.close()
            self._dynamic_scenes.clear()
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()


def _record_discovery(mapping: dict[str, str], ambiguous: set[str], key: str, endpoint: str) -> None:
    if key in ambiguous:
        return
    prior = mapping.get(key)
    if prior is not None and prior != endpoint:
        mapping.pop(key, None)
        ambiguous.add(key)
    else:
        mapping[key] = endpoint


# -- 综合游戏服务客户端（GameServicesClient） ---------------------------------------------------------


class GameServicesClient:
    """独立 GM 受限游戏服务适配器：聚合区服总览、玩家查询、邮件发放与在线玩家 GM 命令。"""

    def __init__(
        self,
        protocol: Protocol,
        config: GmAdminConfig,
        *,
        data_client: GrpcClient | None = None,
        locator_client: GrpcClient | None = None,
        mail_client: GrpcClient | None = None,
        login_client: GrpcClient | None = None,
        player_admin: GamePlayerAdmin | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._protocol = protocol
        self._config = config
        self._clock_ms = clock_ms
        self._budget_sec = config.request_budget_sec
        self._mail_secret = bytes(config.mail_admin_secret)
        self._currencies = _currency_catalog(protocol)
        self._currency_ids = frozenset(item["id"] for item in self._currencies)

        self._owned_clients: list[GrpcClient] = []
        self._data = data_client or self._make_client(config.data_service)
        self._locator = locator_client or self._make_client(config.player_locator)
        self._mail = mail_client or self._make_client(config.mail_service)
        self._login = login_client or self._make_client(config.login_service)
        self._http_session: aiohttp.ClientSession | None = None

        self.player_admin = player_admin or GamePlayerAdmin(
            protocol,
            locator=self._locator,
            static_targets=config.scene_target_map(),
            scene_etcd_url=config.scene_etcd_url,
            scene_admin_secret=config.scene_admin_secret,
            plaintext=config.plaintext,
            budget_sec=config.request_budget_sec,
            discovery_ttl_sec=config.discovery_ttl_sec,
            clock_ms=clock_ms,
        )

    def _make_client(self, target: str) -> GrpcClient | None:
        if not target or not target.strip():
            return None
        validated = validate_grpc_target(target, plaintext=self._config.plaintext, label="game service")
        client = GrpcClient(validated, timeout=self._budget_sec, receive_limit=1 << 20, enable_retries=False)
        self._owned_clients.append(client)
        return client

    def currency_types(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self._currencies]

    def supports_currency(self, currency_type: int) -> bool:
        return currency_type in self._currency_ids

    @property
    def mail_enabled(self) -> bool:
        return self._mail is not None and len(self._mail_secret) >= MIN_HMAC_SECRET_BYTES

    @property
    def player_commands_enabled(self) -> bool:
        return self.player_admin.enabled

    async def overview(self, *, include_zones: bool = True) -> dict[str, Any]:
        """获取集群概览：邮件可用状态、权威币种目录及（按权限）网关区服列表。"""
        result: dict[str, Any] = {
            "mailEnabled": self.mail_enabled,
            "currencies": self.currency_types(),
            "zones": [],
        }
        if include_zones:
            if not self._config.gateway_url:
                result["zonesAvailable"] = False
                return result
            try:
                if self._http_session is None or self._http_session.closed:
                    self._http_session = aiohttp.ClientSession()
                url = f"{self._config.gateway_url.rstrip('/')}/admin/zones"
                async with self._http_session.get(
                    url,
                    headers={"X-Admin-Key": self._config.gateway_admin_key},
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=5.0),
                ) as response:
                    raw = await response.content.read(MAX_ZONES_RESPONSE_BYTES + 1)
                    if response.status != 200 or len(raw) > MAX_ZONES_RESPONSE_BYTES:
                        raise RuntimeError("unexpected gateway zones response")
                    zones = json.loads(raw)
                    if not isinstance(zones, list):
                        raise RuntimeError("gateway zones response is not a JSON array")
                    result["zones"] = zones
                    result["zonesAvailable"] = True
            except Exception:
                result["zonesAvailable"] = False
        return result

    async def player(self, player_id: str) -> dict[str, Any]:
        """按玩家 ID 查询展示信息、归属区与在线会话落点（全链路共享单一预算）。"""
        _positive_uint64(player_id, "玩家 ID")
        if self._data is None:
            raise CallFailure("NOT_CONFIGURED", "玩家资料服务尚未配置", outcome_unknown=False)
        if self._locator is None:
            raise CallFailure("NOT_CONFIGURED", "玩家会话服务尚未配置", outcome_unknown=False)

        deadline = time.monotonic() + self._budget_sec
        display_req = _dict_to_message(self._protocol, DISPLAY_METHOD, {"playerIds": [player_id]})
        display_reply_msg = await _invoke_unary(
            self._protocol,
            self._data,
            DISPLAY_METHOD,
            display_req,
            timeout=deadline - time.monotonic(),
            writing=False,
            unknown_message="玩家资料查询失败",
        )
        displays = _message_to_dict(display_reply_msg)
        players_map = displays.get("players")
        display = players_map.get(player_id) if isinstance(players_map, dict) else None
        found = isinstance(display, dict)
        result: dict[str, Any] = {"playerId": player_id, "found": found}
        if not found:
            return result

        result["display"] = {
            "name": str(display.get("name", "")),
            "level": int(display.get("level", 0) or 0),
            "classId": int(display.get("classId", 0) or 0),
            "gender": int(display.get("gender", 0) or 0),
            "appearanceId": str(display.get("appearanceId", "")),
        }

        home_req = _dict_to_message(self._protocol, HOME_ZONE_METHOD, {"playerId": player_id})
        try:
            home_msg = await _invoke_unary(
                self._protocol,
                self._data,
                HOME_ZONE_METHOD,
                home_req,
                timeout=deadline - time.monotonic(),
                writing=False,
                unknown_message="归属区查询失败",
            )
            result["homeZoneFound"] = True
            result["homeZoneId"] = int(home_msg.home_zone_id)
        except CallFailure as error:
            if error.code != "NOT_FOUND":
                raise
            result["homeZoneFound"] = False

        session_req = _dict_to_message(
            self._protocol,
            SESSION_METHOD,
            {"playerId": player_id, "includePlacement": True},
        )
        session_reply_msg = await _invoke_unary(
            self._protocol,
            self._locator,
            SESSION_METHOD,
            session_req,
            timeout=deadline - time.monotonic(),
            writing=False,
            unknown_message="玩家会话查询失败",
        )
        session_reply = _message_to_dict(session_reply_msg)
        session_found = bool(session_reply.get("found", False))
        result["sessionFound"] = session_found
        if session_found:
            session = session_reply.get("session")
            if not isinstance(session, dict) or str(session.get("playerId", "")) != player_id:
                raise CallFailure("INVALID_RESPONSE", "会话响应与目标玩家不一致", outcome_unknown=False)
            placement = session_reply.get("placement")
            has_placement = isinstance(placement, dict) and bool(placement.get("found", False))
            view: dict[str, Any] = {
                "state": str(session.get("state", "SESSION_STATE_UNKNOWN")),
                "sceneNodeId": str(placement.get("nodeId", "") if has_placement else session.get("sceneNodeId", "")),
                "sceneId": str(placement.get("sceneId", "0") if has_placement else session.get("sceneId", "0")),
                "lastActiveTs": str(session.get("lastActiveTs", "0")),
            }
            if has_placement:
                view["sceneZoneId"] = int(placement.get("zoneId", 0) or 0)
                if placement.get("awaitingPlacement", False):
                    view["awaitingPlacement"] = True
            result["session"] = view
        return result

    def _check_mail_configured(self) -> GrpcClient:
        if self._mail is None:
            raise CallFailure("NOT_CONFIGURED", "邮件服务尚未配置", outcome_unknown=False)
        if len(self._mail_secret) < MIN_HMAC_SECRET_BYTES:
            raise CallFailure("NOT_CONFIGURED", "邮件管理签名密钥未配置", outcome_unknown=False)
        return self._mail

    @staticmethod
    def _has_attachments(req_msg: Message) -> bool:
        if not req_msg.HasField("attachments"):
            return False
        att = req_msg.attachments
        return len(att.currencies) > 0 or len(att.items) > 0

    @staticmethod
    def _verified_mail_reply(reply: Message) -> dict[str, Any]:
        tip = int(reply.error_message.id) if reply.HasField("error_message") else 0
        if tip != 0:
            raise CallFailure("SERVER_TIP", "服务器未确认邮件投递，请保留原请求核对", outcome_unknown=True, tip_id=tip)
        if int(reply.mail_id) == 0:
            raise CallFailure("INVALID_RESPONSE", "邮件响应缺少投递凭据", outcome_unknown=True)
        return _message_to_dict(reply)

    async def send_mail(self, frozen_request: dict[str, Any]) -> dict[str, Any]:
        """发送已冻结的个人邮件（``mailpb.MailAdmin/SendPersonalMail``）。"""
        mail_client = self._check_mail_configured()
        if not isinstance(frozen_request, dict) or "auth" in frozen_request:
            raise _invalid("邮件请求非法，调用证明由服务端生成")
        payload = copy.deepcopy(frozen_request)
        _require_string_id(payload, "requestId", "request_id")
        _require_string_id(payload, "recipientPlayerId", "recipient_player_id")
        _require_string_id(payload, "expireMs", "expire_ms")
        try:
            req = _dict_to_message(self._protocol, SEND_MAIL_METHOD, payload)
        except Exception as exc:
            raise _invalid("邮件请求不符合游戏协议") from exc

        if int(req.sender_kind) != 1:
            raise _invalid("邮件发件角色必须是管理工具")
        for attr in ("guild_id", "campaign_id", "manifest_generation", "expected_recipient_count", "expected_shard_count"):
            if int(getattr(req, attr)) != 0:
                raise _invalid("本入口仅允许给单个玩家发送个人邮件")
        if str(req.manifest_hash):
            raise _invalid("本入口不接受批量发件名单")

        sign_service_call(SEND_MAIL_METHOD, req, "admin", self._clock_ms(), self._mail_secret)
        reply = await _invoke_unary(
            self._protocol,
            mail_client,
            SEND_MAIL_METHOD,
            req,
            timeout=self._budget_sec,
            writing=True,
            unknown_message="邮件投递结果未知，请保留原请求核对",
        )
        return self._verified_mail_reply(reply)

    async def send_system_mail(self, frozen_request: dict[str, Any]) -> dict[str, Any]:
        """发送已冻结的纯文本全服系统邮件（``mailpb.MailAdmin/SendSystemMail``）。"""
        mail_client = self._check_mail_configured()
        if not isinstance(frozen_request, dict) or "auth" in frozen_request:
            raise _invalid("系统邮件请求非法，调用证明由服务端生成")
        payload = copy.deepcopy(frozen_request)
        _require_string_id(payload, "requestId", "request_id")
        _require_string_id(payload, "expireMs", "expire_ms")
        try:
            req = _dict_to_message(self._protocol, SEND_SYSTEM_MAIL_METHOD, payload)
        except Exception as exc:
            raise _invalid("系统邮件请求不符合游戏协议") from exc

        if int(req.sender_kind) != 1:
            raise _invalid("系统邮件发件角色必须是管理工具")
        for attr in (
            "recipient_player_id",
            "guild_id",
            "campaign_id",
            "manifest_generation",
            "expected_recipient_count",
            "expected_shard_count",
        ):
            if int(getattr(req, attr)) != 0:
                raise _invalid("纯文本全服系统邮件不得携带单人收件或活动名单字段")
        if str(req.manifest_hash):
            raise _invalid("纯文本全服系统邮件不得携带名单摘要")
        if self._has_attachments(req):
            raise _invalid("携带附件的系统邮件必须走冻结名单活动流程")

        sign_service_call(SEND_SYSTEM_MAIL_METHOD, req, "admin", self._clock_ms(), self._mail_secret)
        reply = await _invoke_unary(
            self._protocol,
            mail_client,
            SEND_SYSTEM_MAIL_METHOD,
            req,
            timeout=self._budget_sec,
            writing=True,
            unknown_message="邮件投递结果未知，请保留原请求核对",
        )
        return self._verified_mail_reply(reply)

    async def send_mail_campaign(self, frozen_campaign: dict[str, Any]) -> dict[str, Any]:
        """三段式幂等活动发奖：建活动 -> 逐片导入名单 -> 封存发布。"""
        mail_client = self._check_mail_configured()
        if (
            not isinstance(frozen_campaign, dict)
            or not isinstance(frozen_campaign.get("mail"), dict)
            or not isinstance(frozen_campaign.get("shards"), list)
            or not frozen_campaign["shards"]
        ):
            raise _invalid("活动邮件草稿缺少冻结请求或名单分片")
        mail_node = frozen_campaign["mail"]
        if "auth" in mail_node:
            raise _invalid("活动邮件请求非法，调用证明由服务端生成")

        payload = copy.deepcopy(mail_node)
        _require_string_id(payload, "requestId", "request_id")
        _require_string_id(payload, "expireMs", "expire_ms")
        _require_string_id(payload, "campaignId", "campaign_id")
        _require_string_id(payload, "manifestGeneration", "manifest_generation")
        _require_string_id(payload, "expectedRecipientCount", "expected_recipient_count")

        shards: list[list[str]] = []
        total_recipients = 0
        for raw_shard in frozen_campaign["shards"]:
            if not isinstance(raw_shard, list) or not raw_shard or len(raw_shard) > MailCampaignManifest.SHARD_SIZE:
                raise _invalid("活动名单分片格式非法")
            shard_ids: list[str] = []
            for raw_id in raw_shard:
                if not isinstance(raw_id, str):
                    raise _invalid("活动收件人 ID 必须是字符串")
                _positive_uint64(raw_id, "收件人 ID")
                shard_ids.append(raw_id)
            shards.append(shard_ids)
            total_recipients += len(shard_ids)

        try:
            create_req = _dict_to_message(self._protocol, SEND_SYSTEM_MAIL_METHOD, payload)
        except Exception as exc:
            raise _invalid("活动邮件请求不符合游戏协议") from exc

        if int(create_req.sender_kind) != 1:
            raise _invalid("活动邮件发件角色必须是管理工具")
        if int(create_req.recipient_player_id) != 0 or int(create_req.guild_id) != 0:
            raise _invalid("系统活动邮件不得指定单人或帮会目标")
        if not self._has_attachments(create_req):
            raise _invalid("系统活动邮件必须包含至少一项货币或物品附件")

        try:
            computed_hash = MailCampaignManifest.manifest_hash_of(shards)
        except ValueError as exc:
            raise _invalid(str(exc)) from exc
        if (
            int(create_req.expected_shard_count) != len(shards)
            or int(create_req.expected_recipient_count) != total_recipients
            or str(create_req.manifest_hash) != computed_hash
        ):
            raise _invalid("活动名单分片与冻结摘要不一致")

        campaign_id = str(int(create_req.campaign_id))
        generation = str(int(create_req.manifest_generation))

        # 阶段 1：创建或幂等重放活动意图
        sign_service_call(SEND_SYSTEM_MAIL_METHOD, create_req, "admin", self._clock_ms(), self._mail_secret)
        create_reply = await _invoke_unary(
            self._protocol,
            mail_client,
            SEND_SYSTEM_MAIL_METHOD,
            create_req,
            timeout=self._budget_sec,
            writing=True,
            unknown_message="邮件投递结果未知，请保留原请求核对",
        )
        self._verified_mail_reply(create_reply)

        # 阶段 2 & 3：活动意图已在邮件服落盘，后续任一故障均按 outcome_unknown=True 保留原单待重试
        imported_total = 0
        try:
            for shard_index, shard in enumerate(shards):
                import_req = _dict_to_message(
                    self._protocol,
                    IMPORT_RECIPIENTS_METHOD,
                    {
                        "senderKind": 1,
                        "campaignId": campaign_id,
                        "manifestGeneration": generation,
                        "shardIndex": shard_index,
                        "playerIds": shard,
                    },
                )
                sign_service_call(IMPORT_RECIPIENTS_METHOD, import_req, "admin", self._clock_ms(), self._mail_secret)
                import_reply = await _invoke_unary(
                    self._protocol,
                    mail_client,
                    IMPORT_RECIPIENTS_METHOD,
                    import_req,
                    timeout=self._budget_sec,
                    writing=True,
                    unknown_message="导入活动名单分片结果未知，请保留原单重试",
                )
                tip = int(import_reply.error_message.id) if import_reply.HasField("error_message") else 0
                if tip != 0:
                    raise CallFailure("SERVER_TIP", "导入活动名单分片未确认，请保留原单重试", outcome_unknown=True, tip_id=tip)
                imported = int(import_reply.imported_count)
                if imported != len(shard):
                    raise CallFailure("INVALID_RESPONSE", "活动名单分片导入数量不匹配", outcome_unknown=True)
                imported_total += imported

            seal_req = _dict_to_message(
                self._protocol,
                SEAL_CAMPAIGN_METHOD,
                {
                    "senderKind": 1,
                    "campaignId": campaign_id,
                    "manifestGeneration": generation,
                },
            )
            sign_service_call(SEAL_CAMPAIGN_METHOD, seal_req, "admin", self._clock_ms(), self._mail_secret)
            seal_reply = await _invoke_unary(
                self._protocol,
                mail_client,
                SEAL_CAMPAIGN_METHOD,
                seal_req,
                timeout=self._budget_sec,
                writing=True,
                unknown_message="活动邮件封存结果未知，请保留原单重试",
            )
            result = self._verified_mail_reply(seal_reply)
            result.update(
                {
                    "campaignId": campaign_id,
                    "manifestGeneration": generation,
                    "importedRecipients": imported_total,
                    "importedShards": len(shards),
                    "sealed": True,
                }
            )
            return result
        except CallFailure as error:
            if error.outcome_unknown:
                raise
            raise CallFailure(
                error.code,
                f"活动邮件发布中途未完成，请沿用原命令重试：{error.code}",
                outcome_unknown=True,
                tip_id=error.tip_id,
            ) from error

    async def send_player_command(self, frozen_request: dict[str, Any]) -> dict[str, Any]:
        return await self.player_admin.execute(frozen_request)

    async def close(self) -> None:
        await self.player_admin.close()
        for client in self._owned_clients:
            await client.close()
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
