"""GM 冻结命令日志、状态机流转、审计追踪与活动名单分片摘要（对齐 Java GmCommandJournal & MailCampaignManifest）。

设计要点：
- 严格遵循两阶段执行：先在事务内原子占坑（``DRAFT -> SENDING``）并写审计，再在事务外发起下游 RPC，
  最后在独立事务内按 ``attempt`` 乐观锁落盘终态（``DELIVERED`` / ``EXECUTED`` / ``REJECTED`` / ``UNKNOWN``）及审计；
- 个人邮件/系统邮件/活动邮件允许从 ``UNKNOWN`` 或超租约 ``SENDING`` 安全重投；在线玩家 GM 一旦进入 ``SENDING``，
  未知结果禁止重发，防止双倍发放或重复扣款；
- 提供 ``MemoryGmJournal``（单元测试与无库本地模式）和 ``SqlGmJournal``（``aiomysql`` 生产存储）两套同构实现。
"""

from __future__ import annotations

import asyncio
import copy
import datetime as dt
import hashlib
import json
import logging
import re
import secrets
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiomysql
import pymysql

logger = logging.getLogger(__name__)

# -- 公共常量与异常 -----------------------------------------------------------------------------------

UINT64_MAX = (1 << 64) - 1
INT64_MAX = (1 << 63) - 1

SHARD_SIZE = 1000
MAX_WEB_RECIPIENTS = 10_000
MAX_CAMPAIGN_SHARDS = 10_000

PLAYER_DRAFT_TTL_MS = 300_000
MAIL_LEASE_MS = 60_000
CAMPAIGN_LEASE_MS = 120_000
MAX_PAGE = 10_000
MAX_PAGE_SIZE = 100
CAMPAIGN_SAMPLE_LIMIT = 20

MAIL_KINDS = frozenset(("MAIL", "SYSTEM_MAIL", "MAIL_CAMPAIGN"))
ALL_COMMAND_KINDS = frozenset(("MAIL", "SYSTEM_MAIL", "MAIL_CAMPAIGN", "PLAYER"))
MAIL_STATUSES = frozenset(("DRAFT", "SENDING", "DELIVERED", "REJECTED", "UNKNOWN"))
PLAYER_STATUSES = frozenset(("DRAFT", "SENDING", "EXECUTED", "REJECTED", "UNKNOWN"))

FROZEN_MAIL_FIELDS = (
    "requestId",
    "request_id",
    "expireMs",
    "expire_ms",
    "auth",
    "recipient_player_id",
    "campaignId",
    "campaign_id",
    "manifestGeneration",
    "manifest_generation",
)

_CLIENT_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_POSITIVE_UINT64_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_POSITIVE_INT64_RE = re.compile(r"^[1-9][0-9]{0,18}$")

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "config" / "gm_admin_schema.sql"
DUP_ENTRY = 1062


class GmHttpError(Exception):
    """带 HTTP 状态码的 GM 业务/校验错误（对应 Spring ResponseStatusException）。"""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class CallFailure(RuntimeError):
    """下游游戏服务调用失败，区分确定拒绝与结果未知（对应 Java GameServices.CallFailure）。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        outcome_unknown: bool = False,
        tip_id: int = 0,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.outcome_unknown = outcome_unknown
        self.tip_id = tip_id


# -- 活动邮件名单分片与摘要（与 Go mail / Java MailCampaignManifest 逐字节一致） -----------------------


@dataclass(frozen=True, slots=True)
class CampaignPlan:
    """活动邮件收件人去重升序分片计划与 v1 清单摘要。"""

    shards: tuple[tuple[str, ...], ...]
    recipient_count: int
    manifest_hash: str


class MailCampaignManifest:
    """活动邮件名单分片与 SHA-256 摘要计算器（跨语言确定性契约）。"""

    SHARD_SIZE = SHARD_SIZE
    MAX_WEB_RECIPIENTS = MAX_WEB_RECIPIENTS

    @staticmethod
    def parse_positive_uint64(raw: str) -> int:
        if not isinstance(raw, str):
            raise ValueError("收件人玩家 ID 非法")
        text = raw.strip()
        if not _POSITIVE_UINT64_RE.fullmatch(text):
            raise ValueError("收件人玩家 ID 必须是非零 uint64 十进制字符串")
        value = int(text)
        if value > UINT64_MAX:
            raise ValueError("收件人玩家 ID 超出 uint64 范围")
        return value

    @classmethod
    def shard_hash_of(cls, ids: Sequence[str]) -> str:
        if not ids or len(ids) > cls.SHARD_SIZE:
            raise ValueError("收件人分片大小非法")
        parsed = [cls.parse_positive_uint64(raw) for raw in ids]
        ordered = sorted(set(parsed))
        if len(ordered) != len(ids):
            raise ValueError("收件人分片包含重复玩家 ID")
        payload = "".join(f"{pid}\n" for pid in ordered).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @classmethod
    def manifest_hash_of(cls, shards: Sequence[Sequence[str]]) -> str:
        if not shards or len(shards) > MAX_CAMPAIGN_SHARDS:
            raise ValueError("活动名单分片数量非法")
        lines = ["v1\n"]
        for index, shard in enumerate(shards):
            if not shard or len(shard) > cls.SHARD_SIZE:
                raise ValueError("活动名单分片大小非法")
            digest = cls.shard_hash_of(shard)
            lines.append(f"{index}:{digest}:{len(shard)}\n")
        return hashlib.sha256("".join(lines).encode("utf-8")).hexdigest()

    @classmethod
    def build(
        cls,
        raw_ids: Sequence[str],
        *,
        max_recipients: int = MAX_WEB_RECIPIENTS,
    ) -> CampaignPlan:
        if not raw_ids:
            raise ValueError("收件人名单不能为空")
        unique: set[int] = set()
        for raw in raw_ids:
            pid = cls.parse_positive_uint64(raw)
            unique.add(pid)
            if len(unique) > max_recipients:
                raise ValueError(f"单次网页活动邮件收件人不能超过 {max_recipients} 人")
        sorted_ids = [str(pid) for pid in sorted(unique)]
        shards = tuple(
            tuple(sorted_ids[start : start + cls.SHARD_SIZE])
            for start in range(0, len(sorted_ids), cls.SHARD_SIZE)
        )
        return CampaignPlan(
            shards=shards,
            recipient_count=len(sorted_ids),
            manifest_hash=cls.manifest_hash_of(shards),
        )


# -- 公共格式化与校验辅助 -----------------------------------------------------------------------------


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _format_iso_ms(epoch_ms: int) -> str:
    """对齐 Java Instant.ofEpochMilli(ms).toString() 的 UTC ISO-8601 格式。"""
    instant = dt.datetime.fromtimestamp(epoch_ms / 1000.0, tz=dt.timezone.utc)
    if instant.microsecond:
        return instant.strftime("%Y-%m-%dT%H:%M:%S.") + f"{instant.microsecond // 1000:03d}Z"
    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_positive_int64(value: Any) -> int:
    if not isinstance(value, str) or not _POSITIVE_INT64_RE.fullmatch(value):
        return 0
    parsed = int(value)
    return parsed if parsed <= INT64_MAX else 0


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise GmHttpError(400, message)


def _conflict(message: str) -> GmHttpError:
    return GmHttpError(409, message)


def _valid_days(node: dict[str, Any]) -> int:
    value = node.get("validDays")
    _check(_is_int(value) and -(1 << 31) <= value <= (1 << 31) - 1, "有效天数非法")
    _check(1 <= value <= 30, "有效天数须为 1 至 30")
    return value


def _reject_frozen_fields(node: dict[str, Any], *, forbid_recipient_player_id: bool) -> None:
    for field_name in FROZEN_MAIL_FIELDS:
        _check(field_name not in node, "不得覆盖服务端冻结字段")
    if forbid_recipient_player_id:
        _check("recipientPlayerId" not in node, "不得覆盖服务端冻结字段")


def _compact_campaign_request(full_request: dict[str, Any]) -> dict[str, Any]:
    """列表与详情视图裁剪活动大名单，仅保留前 20 个样本及总人数/分片数。"""
    compact: dict[str, Any] = {}
    mail = full_request.get("mail")
    if isinstance(mail, dict):
        compact["mail"] = copy.deepcopy(mail)
    sample: list[str] = []
    total = 0
    shards = full_request.get("shards")
    shard_count = len(shards) if isinstance(shards, list) else 0
    if isinstance(shards, list):
        for shard in shards:
            if not isinstance(shard, list):
                continue
            total += len(shard)
            for pid in shard:
                if len(sample) < CAMPAIGN_SAMPLE_LIMIT:
                    sample.append(str(pid))
                else:
                    break
    compact["recipientSample"] = sample
    compact["totalRecipients"] = total
    compact["shardCount"] = shard_count
    return compact


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _build_view(
    *,
    command_id: str,
    command_type: str,
    status: str,
    operator: str,
    player_id: str,
    reason: str,
    full_request: dict[str, Any],
    result: dict[str, Any] | None,
    created_ms: int,
    updated_ms: int,
) -> dict[str, Any]:
    view: dict[str, Any] = {
        "id": command_id,
        "commandType": command_type,
        "status": status,
        "operator": operator,
        "playerId": player_id,
        "reason": reason,
        "createdAt": _format_iso_ms(created_ms),
        "updatedAt": _format_iso_ms(updated_ms),
        "request": (
            _compact_campaign_request(full_request)
            if command_type == "MAIL_CAMPAIGN"
            else copy.deepcopy(full_request)
        ),
        "result": copy.deepcopy(result) if result is not None else None,
    }
    if command_type == "PLAYER":
        view["operation"] = str(full_request.get("operation", ""))
    return view


def _default_id_generator() -> int:
    return secrets.randbits(63)


def _default_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _validate_prepare_inputs(
    operator: str,
    client_request_id: str,
    player_id: str,
    reason: str,
    input_data: dict[str, Any],
    kind: str,
) -> int:
    _check(isinstance(operator, str) and bool(operator.strip()) and len(operator.encode("utf-8")) <= 512, "操作者非法")
    _check(
        isinstance(client_request_id, str) and bool(_CLIENT_REQUEST_ID_RE.fullmatch(client_request_id)),
        "客户端幂等号非法",
    )
    if kind in ("SYSTEM_MAIL", "MAIL_CAMPAIGN"):
        _check(player_id == "0", "系统邮件不绑定单个玩家")
    else:
        _check(isinstance(player_id, str) and bool(_POSITIVE_UINT64_RE.fullmatch(player_id)), "玩家 ID 非法")
    _check(isinstance(reason, str) and bool(reason.strip()) and len(reason) <= 1000, "操作原因非法")
    _check(isinstance(input_data, dict), "命令内容不能为空")

    days = 0
    if kind == "MAIL_CAMPAIGN":
        _check(
            len(input_data) == 2
            and isinstance(input_data.get("mail"), dict)
            and isinstance(input_data.get("shards"), list),
            "活动邮件草稿格式非法",
        )
        mail_input = input_data["mail"]
        shards_input = input_data["shards"]
        _check(len(shards_input) > 0, "活动名单分片不能为空")
        for shard in shards_input:
            _check(isinstance(shard, list) and len(shard) > 0, "活动名单分片格式非法")
        days = _valid_days(mail_input)
        _reject_frozen_fields(mail_input, forbid_recipient_player_id=True)
    elif kind in ("MAIL", "SYSTEM_MAIL"):
        days = _valid_days(input_data)
        _reject_frozen_fields(input_data, forbid_recipient_player_id=(kind == "SYSTEM_MAIL"))
        if kind == "MAIL" and "recipientPlayerId" in input_data:
            _check(str(input_data["recipientPlayerId"]) == player_id, "玩家不一致")
    else:
        _check(
            isinstance(input_data.get("operation"), str) and isinstance(input_data.get("arguments"), dict),
            "玩家命令格式非法",
        )
        for forbidden in ("operator", "reason", "issuedAtMs", "playerId"):
            _check(forbidden not in input_data, "不得覆盖服务端冻结字段")
        _reject_frozen_fields(input_data, forbid_recipient_player_id=True)
    return days


def _freeze_request(
    *,
    kind: str,
    command_id: str,
    operator: str,
    player_id: str,
    reason: str,
    original: dict[str, Any],
    now_ms: int,
    days: int,
) -> dict[str, Any]:
    request = copy.deepcopy(original)
    if kind == "MAIL":
        request.pop("validDays", None)
        request["requestId"] = command_id
        request["recipientPlayerId"] = player_id
        request["expireMs"] = str(now_ms + days * 86_400_000)
    elif kind == "SYSTEM_MAIL":
        request.pop("validDays", None)
        request["requestId"] = command_id
        request["expireMs"] = str(now_ms + days * 86_400_000)
    elif kind == "MAIL_CAMPAIGN":
        mail_req = request["mail"]
        mail_req.pop("validDays", None)
        mail_req["requestId"] = command_id
        mail_req["campaignId"] = command_id
        mail_req["manifestGeneration"] = "1"
        mail_req["expireMs"] = str(now_ms + days * 86_400_000)
    else:
        request["requestId"] = command_id
        request["playerId"] = player_id
        request["operator"] = operator
        request["reason"] = reason
        request["issuedAtMs"] = str(now_ms)
    return request


def _validate_list_filters(
    kind: str,
    page: int,
    page_size: int,
    player_id: str | None,
    status: str | None,
) -> tuple[str | None, str | None]:
    _check(_is_int(page) and 1 <= page <= MAX_PAGE, f"页码须为 1 至 {MAX_PAGE}")
    _check(_is_int(page_size) and 1 <= page_size <= MAX_PAGE_SIZE, f"每页条数须为 1 至 {MAX_PAGE_SIZE}")
    clean_player: str | None = None
    if player_id is not None and player_id.strip():
        clean_player = player_id.strip()
        _check(bool(_POSITIVE_UINT64_RE.fullmatch(clean_player)), "筛选玩家 ID 非法")
    clean_status: str | None = None
    if status is not None and status.strip():
        clean_status = status.strip()
        allowed = MAIL_STATUSES if kind in MAIL_KINDS else PLAYER_STATUSES
        _check(clean_status in allowed, "筛选状态非法")
    return clean_player, clean_status


SenderFn = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


# -- 抽象基类（模板方法模式：统一校验与发送编排，子类仅负责持久化事务） -----------------------------


class BaseGmJournal:
    """GM 命令日志基类，复用所有校验、幂等重放与两阶段发送状态机。"""

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _default_clock_ms,
        id_generator: Callable[[], int] = _default_id_generator,
    ) -> None:
        self._clock_ms = clock_ms
        self._id_generator = id_generator

    def _next_id(self) -> str:
        for _ in range(8):
            candidate = int(self._id_generator()) & INT64_MAX
            if candidate > 0:
                return str(candidate)
        raise RuntimeError("Command ID generation failed")

    async def prepare(
        self,
        operator: str,
        client_request_id: str,
        player_id: str,
        reason: str,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._prepare_internal(operator, client_request_id, player_id, reason, input_data, "MAIL")

    async def prepare_system_mail(
        self,
        operator: str,
        client_request_id: str,
        reason: str,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._prepare_internal(operator, client_request_id, "0", reason, input_data, "SYSTEM_MAIL")

    async def prepare_mail_campaign(
        self,
        operator: str,
        client_request_id: str,
        reason: str,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._prepare_internal(operator, client_request_id, "0", reason, input_data, "MAIL_CAMPAIGN")

    async def prepare_player_command(
        self,
        operator: str,
        client_request_id: str,
        player_id: str,
        reason: str,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._prepare_internal(operator, client_request_id, player_id, reason, input_data, "PLAYER")

    async def get(self, command_id: str, kind: str | None = None) -> dict[str, Any]:
        if kind is not None:
            _check(isinstance(command_id, str) and bool(_POSITIVE_UINT64_RE.fullmatch(command_id)), "命令 ID 非法")
        view = await self._get_view(command_id)
        if kind is not None and view["commandType"] != kind:
            raise _conflict("命令类型与操作入口不符")
        return view

    async def list_mails(
        self,
        page: int = 1,
        page_size: int = 50,
        player_id: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        return await self._list_kind("MAIL", page, page_size, player_id, status)

    async def list_system_mails(
        self,
        page: int = 1,
        page_size: int = 50,
        status: str | None = None,
    ) -> dict[str, Any]:
        return await self._list_kind("SYSTEM_MAIL", page, page_size, None, status)

    async def list_mail_campaigns(
        self,
        page: int = 1,
        page_size: int = 50,
        status: str | None = None,
    ) -> dict[str, Any]:
        return await self._list_kind("MAIL_CAMPAIGN", page, page_size, None, status)

    async def list_player_commands(
        self,
        page: int = 1,
        page_size: int = 50,
        player_id: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        return await self._list_kind("PLAYER", page, page_size, player_id, status)

    async def audit_history(self, command_id: str, kind: str) -> dict[str, Any]:
        _check(isinstance(command_id, str) and bool(_POSITIVE_UINT64_RE.fullmatch(command_id)), "命令 ID 非法")
        view = await self._get_view(command_id)
        if view["commandType"] != kind:
            raise _conflict("命令类型与操作入口不符")
        records = await self._list_audit_records(command_id)
        return {"commandId": command_id, "records": records}

    async def send(self, command_id: str, operator: str, sender: SenderFn) -> dict[str, Any]:
        return await self._send_internal(command_id, operator, sender, "MAIL")

    async def send_system_mail(self, command_id: str, operator: str, sender: SenderFn) -> dict[str, Any]:
        return await self._send_internal(command_id, operator, sender, "SYSTEM_MAIL")

    async def send_mail_campaign(self, command_id: str, operator: str, sender: SenderFn) -> dict[str, Any]:
        return await self._send_internal(command_id, operator, sender, "MAIL_CAMPAIGN")

    async def send_player_command(self, command_id: str, operator: str, sender: SenderFn) -> dict[str, Any]:
        return await self._send_internal(command_id, operator, sender, "PLAYER")

    async def _send_internal(
        self,
        command_id: str,
        operator: str,
        sender: SenderFn,
        kind: str,
    ) -> dict[str, Any]:
        mail_kind = kind in MAIL_KINDS
        complete = "DELIVERED" if mail_kind else "EXECUTED"
        lease_ms = CAMPAIGN_LEASE_MS if kind == "MAIL_CAMPAIGN" else MAIL_LEASE_MS

        claimed_view, full_request, attempt, already_complete = await self._claim_for_send(
            command_id=command_id,
            operator=operator,
            kind=kind,
            complete=complete,
            lease_ms=lease_ms,
        )
        if already_complete:
            return claimed_view

        try:
            outcome = await sender(copy.deepcopy(full_request))
            if not isinstance(outcome, dict):
                raise RuntimeError("sender returned non-dict outcome")
            final_state = complete
        except CallFailure as error:
            outcome = {
                "code": error.code,
                "tipId": error.tip_id,
                "outcomeUnknown": error.outcome_unknown,
            }
            final_state = "UNKNOWN" if error.outcome_unknown else "REJECTED"
        except Exception:
            logger.error(
                "unexpected GM sender failure command_id=%s kind=%s attempt=%d",
                command_id,
                kind,
                attempt,
                exc_info=True,
            )
            outcome = {"code": "UNEXPECTED_FAILURE", "outcomeUnknown": True}
            final_state = "UNKNOWN"

        return await self._finish_send(
            command_id=command_id,
            operator=operator,
            attempt=attempt,
            final_state=final_state,
            outcome=copy.deepcopy(outcome),
        )

    # -- 子类持久化钩子 -------------------------------------------------------------------------------

    async def _prepare_internal(
        self,
        operator: str,
        client_request_id: str,
        player_id: str,
        reason: str,
        input_data: dict[str, Any],
        kind: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def _get_view(self, command_id: str) -> dict[str, Any]:
        raise NotImplementedError

    async def _list_kind(
        self,
        kind: str,
        page: int,
        page_size: int,
        player_id: str | None,
        status: str | None,
    ) -> dict[str, Any]:
        raise NotImplementedError

    async def _list_audit_records(self, command_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def _claim_for_send(
        self,
        *,
        command_id: str,
        operator: str,
        kind: str,
        complete: str,
        lease_ms: int,
    ) -> tuple[dict[str, Any], dict[str, Any], int, bool]:
        raise NotImplementedError

    async def _finish_send(
        self,
        *,
        command_id: str,
        operator: str,
        attempt: int,
        final_state: str,
        outcome: dict[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError


# -- 内存实现（MemoryGmJournal） ----------------------------------------------------------------------


@dataclass
class _MemCommandRow:
    id: str
    command_type: str
    operator: str
    client_request_id: str
    player_id: str
    reason: str
    original: dict[str, Any]
    full_request: dict[str, Any]
    result: dict[str, Any] | None
    status: str
    attempt: int
    created_ms: int
    updated_ms: int

    def to_view(self) -> dict[str, Any]:
        return _build_view(
            command_id=self.id,
            command_type=self.command_type,
            status=self.status,
            operator=self.operator,
            player_id=self.player_id,
            reason=self.reason,
            full_request=self.full_request,
            result=self.result,
            created_ms=self.created_ms,
            updated_ms=self.updated_ms,
        )


@dataclass
class _MemAuditRow:
    id: int
    command_id: str
    operator: str
    event: str
    attempt: int
    result: dict[str, Any] | None
    created_ms: int

    def to_view(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "operator": self.operator,
            "event": self.event,
            "attempt": self.attempt,
            "createdAt": _format_iso_ms(self.created_ms),
            "result": copy.deepcopy(self.result) if self.result is not None else None,
        }


class MemoryGmJournal(BaseGmJournal):
    """纯内存 GM 命令日志，与 SqlGmJournal 保持完全一致的状态机、幂等和审计语义。"""

    def __init__(
        self,
        *,
        clock_ms: Callable[[], int] = _default_clock_ms,
        id_generator: Callable[[], int] = _default_id_generator,
    ) -> None:
        super().__init__(clock_ms=clock_ms, id_generator=id_generator)
        self._lock = asyncio.Lock()
        self._rows: dict[str, _MemCommandRow] = {}
        self._by_key: dict[tuple[str, str], str] = {}
        self._audits: dict[str, list[_MemAuditRow]] = {}
        self._audit_seq = 0

    def _append_audit(
        self,
        command_id: str,
        operator: str,
        event: str,
        attempt: int,
        result: dict[str, Any] | None,
        now_ms: int,
    ) -> None:
        self._audit_seq += 1
        self._audits.setdefault(command_id, []).append(
            _MemAuditRow(
                id=self._audit_seq,
                command_id=command_id,
                operator=operator,
                event=event,
                attempt=attempt,
                result=copy.deepcopy(result) if result is not None else None,
                created_ms=now_ms,
            )
        )

    @staticmethod
    def _replay(
        old: _MemCommandRow,
        player_id: str,
        reason: str,
        original: dict[str, Any],
        kind: str,
    ) -> dict[str, Any]:
        if (
            old.command_type != kind
            or old.player_id != player_id
            or old.reason != reason
            or old.original != original
        ):
            raise _conflict("同一幂等号不能更换玩家、原因或发放内容")
        return old.to_view()

    async def _prepare_internal(
        self,
        operator: str,
        client_request_id: str,
        player_id: str,
        reason: str,
        input_data: dict[str, Any],
        kind: str,
    ) -> dict[str, Any]:
        days = _validate_prepare_inputs(operator, client_request_id, player_id, reason, input_data, kind)
        original = copy.deepcopy(input_data)
        async with self._lock:
            existing_id = self._by_key.get((operator, client_request_id))
            if existing_id is not None:
                return self._replay(self._rows[existing_id], player_id, reason, original, kind)
            for _ in range(3):
                command_id = self._next_id()
                if command_id in self._rows:
                    continue
                now_ms = self._clock_ms()
                full_request = _freeze_request(
                    kind=kind,
                    command_id=command_id,
                    operator=operator,
                    player_id=player_id,
                    reason=reason,
                    original=original,
                    now_ms=now_ms,
                    days=days,
                )
                row = _MemCommandRow(
                    id=command_id,
                    command_type=kind,
                    operator=operator,
                    client_request_id=client_request_id,
                    player_id=player_id,
                    reason=reason,
                    original=original,
                    full_request=full_request,
                    result=None,
                    status="DRAFT",
                    attempt=0,
                    created_ms=now_ms,
                    updated_ms=now_ms,
                )
                self._rows[command_id] = row
                self._by_key[(operator, client_request_id)] = command_id
                self._append_audit(command_id, operator, "DRAFT", 0, None, now_ms)
                return row.to_view()
            raise _conflict("命令号冲突，请重新创建草稿")

    async def _get_view(self, command_id: str) -> dict[str, Any]:
        async with self._lock:
            row = self._rows.get(command_id)
            if row is None:
                raise GmHttpError(404, "命令不存在")
            return row.to_view()

    async def _list_kind(
        self,
        kind: str,
        page: int,
        page_size: int,
        player_id: str | None,
        status: str | None,
    ) -> dict[str, Any]:
        clean_player, clean_status = _validate_list_filters(kind, page, page_size, player_id, status)
        async with self._lock:
            matched = [
                row
                for row in self._rows.values()
                if row.command_type == kind
                and (clean_player is None or row.player_id == clean_player)
                and (clean_status is None or row.status == clean_status)
            ]
            matched.sort(key=lambda r: (r.created_ms, r.id), reverse=True)
            total = len(matched)
            offset = (page - 1) * page_size
            page_rows = matched[offset : offset + page_size]
            return {
                "records": [row.to_view() for row in page_rows],
                "total": total,
                "page": page,
                "pageSize": page_size,
            }

    async def _list_audit_records(self, command_id: str) -> list[dict[str, Any]]:
        async with self._lock:
            return [entry.to_view() for entry in self._audits.get(command_id, ())]

    async def _claim_for_send(
        self,
        *,
        command_id: str,
        operator: str,
        kind: str,
        complete: str,
        lease_ms: int,
    ) -> tuple[dict[str, Any], dict[str, Any], int, bool]:
        mail_kind = kind in MAIL_KINDS
        async with self._lock:
            row = self._rows.get(command_id)
            if row is None:
                raise GmHttpError(404, "命令不存在")
            if row.command_type != kind:
                raise _conflict("命令类型与操作入口不符")
            if row.operator != operator:
                raise GmHttpError(403, "仅草稿创建者可发送")
            if row.status == complete:
                return row.to_view(), copy.deepcopy(row.full_request), row.attempt, True

            now_ms = self._clock_ms()
            if mail_kind and row.status == "DRAFT":
                mail_node = row.full_request.get("mail", {}) if kind == "MAIL_CAMPAIGN" else row.full_request
                expire_ms = _parse_positive_int64(mail_node.get("expireMs", ""))
                if expire_ms <= now_ms:
                    raise _conflict("邮件草稿已过设定到期时间，请重新创建草稿")
            if kind == "PLAYER" and row.status == "DRAFT":
                issued_at_ms = _parse_positive_int64(row.full_request.get("issuedAtMs", ""))
                age = now_ms - issued_at_ms
                if issued_at_ms <= 0 or age < 0 or age > PLAYER_DRAFT_TTL_MS:
                    raise _conflict("草稿已过期，请重新创建并核对")

            if mail_kind:
                claimable = row.status in ("DRAFT", "UNKNOWN") or (
                    row.status == "SENDING" and row.updated_ms < now_ms - lease_ms
                )
            else:
                claimable = row.status == "DRAFT"

            if not claimable:
                raise _conflict("命令正在发送或已拒绝，请核对当前状态")

            row.status = "SENDING"
            row.attempt += 1
            row.updated_ms = now_ms
            self._append_audit(command_id, operator, "SENDING", row.attempt, None, now_ms)
            return row.to_view(), copy.deepcopy(row.full_request), row.attempt, False

    async def _finish_send(
        self,
        *,
        command_id: str,
        operator: str,
        attempt: int,
        final_state: str,
        outcome: dict[str, Any],
    ) -> dict[str, Any]:
        async with self._lock:
            row = self._rows.get(command_id)
            if row is None:
                raise GmHttpError(404, "命令不存在")
            if row.status == "SENDING" and row.attempt == attempt:
                now_ms = self._clock_ms()
                row.status = final_state
                row.result = copy.deepcopy(outcome)
                row.updated_ms = now_ms
                self._append_audit(command_id, operator, final_state, attempt, outcome, now_ms)
            return row.to_view()


# -- MySQL/TiDB 实现（SqlGmJournal） ------------------------------------------------------------------


async def open_pool(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    minsize: int = 1,
    maxsize: int = 10,
    connect_timeout: float = 5.0,
) -> aiomysql.Pool:
    """打开 gm_admin 独占 MySQL/TiDB 连接池（禁用 autocommit，由事务显式提交）。"""
    return await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=database,
        minsize=minsize,
        maxsize=maxsize,
        connect_timeout=connect_timeout,
        autocommit=False,
        charset="utf8mb4",
    )


def _decode_varbinary(raw: Any) -> str:
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8")
    return str(raw)


class SqlGmJournal(BaseGmJournal):
    """基于 aiomysql 的持久 GM 命令日志，表结构对应 ``config/gm_admin_schema.sql``。"""

    def __init__(
        self,
        pool: aiomysql.Pool,
        *,
        clock_ms: Callable[[], int] = _default_clock_ms,
        id_generator: Callable[[], int] = _default_id_generator,
        schema_file: Path = SCHEMA_FILE,
    ) -> None:
        super().__init__(clock_ms=clock_ms, id_generator=id_generator)
        self._pool = pool
        self._schema_file = schema_file

    async def bootstrap(self) -> None:
        """幂等执行 ``config/gm_admin_schema.sql`` 中的全部建表语句。"""
        sql_text = self._schema_file.read_text(encoding="utf-8")
        lines = [line for line in sql_text.splitlines() if not line.lstrip().startswith("--")]
        statements = [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                for stmt in statements:
                    await cur.execute(stmt)
            await conn.commit()

    async def verify_schema(self) -> None:
        """启动期校验 ``gm_command`` 与 ``gm_command_audit`` 表已就绪。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_name IN ('gm_command', 'gm_command_audit')"
                )
                found = {row[0] for row in await cur.fetchall()}
            await conn.commit()
        missing = {"gm_command", "gm_command_audit"} - found
        if missing:
            raise RuntimeError(f"gm_admin required tables missing: {sorted(missing)}")

    @staticmethod
    def _row_to_tuple(row: tuple[Any, ...]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], int]:
        (
            cmd_id,
            cmd_type,
            operator_raw,
            _client_req_raw,
            player_id,
            reason,
            input_json,
            request_json,
            result_json,
            status,
            attempt,
            created_ms,
            updated_ms,
        ) = row
        full_request = json.loads(request_json)
        original = json.loads(input_json)
        result = json.loads(result_json) if result_json is not None else None
        view = _build_view(
            command_id=str(cmd_id),
            command_type=str(cmd_type),
            status=str(status),
            operator=_decode_varbinary(operator_raw),
            player_id=str(player_id),
            reason=str(reason),
            full_request=full_request,
            result=result,
            created_ms=int(created_ms),
            updated_ms=int(updated_ms),
        )
        return view, full_request, original, int(attempt)

    @staticmethod
    def _replay_sql(
        existing_view: dict[str, Any],
        existing_original: dict[str, Any],
        player_id: str,
        reason: str,
        original: dict[str, Any],
        kind: str,
    ) -> dict[str, Any]:
        if (
            existing_view["commandType"] != kind
            or existing_view["playerId"] != player_id
            or existing_view["reason"] != reason
            or existing_original != original
        ):
            raise _conflict("同一幂等号不能更换玩家、原因或发放内容")
        return existing_view

    async def _find_by_key(self, cur, operator: str, client_request_id: str):
        await cur.execute(
            "SELECT id,command_type,operator_name,client_request_id,player_id,reason,input_json,"
            "request_json,result_json,status,attempt,created_ms,updated_ms "
            "FROM gm_command WHERE operator_name=UNHEX(%s) AND client_request_id=UNHEX(%s)",
            (operator.encode("utf-8").hex(), client_request_id.encode("utf-8").hex()),
        )
        row = await cur.fetchone()
        return self._row_to_tuple(row) if row is not None else None

    async def _load_row(self, cur, command_id: str, *, for_update: bool = False):
        suffix = " FOR UPDATE" if for_update else ""
        await cur.execute(
            "SELECT id,command_type,operator_name,client_request_id,player_id,reason,input_json,"
            "request_json,result_json,status,attempt,created_ms,updated_ms "
            f"FROM gm_command WHERE id=%s{suffix}",
            (command_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise GmHttpError(404, "命令不存在")
        return self._row_to_tuple(row)

    @staticmethod
    async def _insert_audit(
        cur,
        command_id: str,
        operator: str,
        event: str,
        attempt: int,
        result: dict[str, Any] | None,
        now_ms: int,
    ) -> None:
        await cur.execute(
            "INSERT INTO gm_command_audit (command_id,operator_name,event,attempt,result_json,created_ms) "
            "VALUES (%s,UNHEX(%s),%s,%s,%s,%s)",
            (
                command_id,
                operator.encode("utf-8").hex(),
                event,
                attempt,
                None if result is None else _canonical_json(result),
                now_ms,
            ),
        )

    async def _prepare_internal(
        self,
        operator: str,
        client_request_id: str,
        player_id: str,
        reason: str,
        input_data: dict[str, Any],
        kind: str,
    ) -> dict[str, Any]:
        days = _validate_prepare_inputs(operator, client_request_id, player_id, reason, input_data, kind)
        original = copy.deepcopy(input_data)
        original_json = _canonical_json(original)

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                previous = await self._find_by_key(cur, operator, client_request_id)
                if previous is not None:
                    await conn.commit()
                    return self._replay_sql(previous[0], previous[2], player_id, reason, original, kind)

                for _ in range(3):
                    command_id = self._next_id()
                    now_ms = self._clock_ms()
                    full_request = _freeze_request(
                        kind=kind,
                        command_id=command_id,
                        operator=operator,
                        player_id=player_id,
                        reason=reason,
                        original=original,
                        now_ms=now_ms,
                        days=days,
                    )
                    try:
                        await cur.execute(
                            "INSERT INTO gm_command "
                            "(id,command_type,operator_name,client_request_id,player_id,reason,input_json,"
                            "request_json,status,attempt,created_ms,updated_ms) "
                            "VALUES (%s,%s,UNHEX(%s),UNHEX(%s),%s,%s,%s,%s,'DRAFT',0,%s,%s)",
                            (
                                command_id,
                                kind,
                                operator.encode("utf-8").hex(),
                                client_request_id.encode("utf-8").hex(),
                                player_id,
                                reason,
                                original_json,
                                _canonical_json(full_request),
                                now_ms,
                                now_ms,
                            ),
                        )
                        await self._insert_audit(cur, command_id, operator, "DRAFT", 0, None, now_ms)
                        view, _, _, _ = await self._load_row(cur, command_id)
                        await conn.commit()
                        return view
                    except pymysql.IntegrityError as exc:
                        await conn.rollback()
                        if exc.args and exc.args[0] == DUP_ENTRY:
                            previous = await self._find_by_key(cur, operator, client_request_id)
                            await conn.commit()
                            if previous is not None:
                                return self._replay_sql(previous[0], previous[2], player_id, reason, original, kind)
                            continue
                        raise
            raise _conflict("命令号冲突，请重新创建草稿")

    async def _get_view(self, command_id: str) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                view, _, _, _ = await self._load_row(cur, command_id)
            await conn.commit()
            return view

    async def _list_kind(
        self,
        kind: str,
        page: int,
        page_size: int,
        player_id: str | None,
        status: str | None,
    ) -> dict[str, Any]:
        clean_player, clean_status = _validate_list_filters(kind, page, page_size, player_id, status)
        where_parts = ["command_type=%s"]
        args: list[Any] = [kind]
        if clean_player is not None:
            where_parts.append("player_id=%s")
            args.append(clean_player)
        if clean_status is not None:
            where_parts.append("status=%s")
            args.append(clean_status)
        where_sql = "WHERE " + " AND ".join(where_parts)
        offset = (page - 1) * page_size

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id,command_type,operator_name,player_id,reason,request_json,status,"
                    f"result_json,created_ms,updated_ms FROM gm_command {where_sql} "
                    "ORDER BY created_ms DESC, id DESC LIMIT %s OFFSET %s",
                    (*args, page_size, offset),
                )
                rows = await cur.fetchall()
                await cur.execute(f"SELECT COUNT(*) FROM gm_command {where_sql}", tuple(args))
                total_row = await cur.fetchone()
            await conn.commit()

        records = [
            _build_view(
                command_id=str(r[0]),
                command_type=str(r[1]),
                operator=_decode_varbinary(r[2]),
                player_id=str(r[3]),
                reason=str(r[4]),
                full_request=json.loads(r[5]),
                status=str(r[6]),
                result=json.loads(r[7]) if r[7] is not None else None,
                created_ms=int(r[8]),
                updated_ms=int(r[9]),
            )
            for r in rows
        ]
        return {
            "records": records,
            "total": int(total_row[0]) if total_row else 0,
            "page": page,
            "pageSize": page_size,
        }

    async def _list_audit_records(self, command_id: str) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id,operator_name,event,attempt,result_json,created_ms "
                    "FROM gm_command_audit WHERE command_id=%s ORDER BY id ASC",
                    (command_id,),
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [
            {
                "id": str(r[0]),
                "operator": _decode_varbinary(r[1]),
                "event": str(r[2]),
                "attempt": int(r[3]),
                "createdAt": _format_iso_ms(int(r[5])),
                "result": json.loads(r[4]) if r[4] is not None else None,
            }
            for r in rows
        ]

    async def _claim_for_send(
        self,
        *,
        command_id: str,
        operator: str,
        kind: str,
        complete: str,
        lease_ms: int,
    ) -> tuple[dict[str, Any], dict[str, Any], int, bool]:
        mail_kind = kind in MAIL_KINDS
        async with self._pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    view, full_request, _, attempt = await self._load_row(cur, command_id, for_update=True)
                    if view["commandType"] != kind:
                        raise _conflict("命令类型与操作入口不符")
                    if view["operator"] != operator:
                        raise GmHttpError(403, "仅草稿创建者可发送")
                    if view["status"] == complete:
                        await conn.commit()
                        return view, full_request, attempt, True

                    now_ms = self._clock_ms()
                    if mail_kind and view["status"] == "DRAFT":
                        mail_node = full_request.get("mail", {}) if kind == "MAIL_CAMPAIGN" else full_request
                        expire_ms = _parse_positive_int64(mail_node.get("expireMs", ""))
                        if expire_ms <= now_ms:
                            raise _conflict("邮件草稿已过设定到期时间，请重新创建草稿")
                    if kind == "PLAYER" and view["status"] == "DRAFT":
                        issued_at_ms = _parse_positive_int64(full_request.get("issuedAtMs", ""))
                        age = now_ms - issued_at_ms
                        if issued_at_ms <= 0 or age < 0 or age > PLAYER_DRAFT_TTL_MS:
                            raise _conflict("草稿已过期，请重新创建并核对")

                    if mail_kind:
                        changed = await cur.execute(
                            "UPDATE gm_command SET status='SENDING',attempt=attempt+1,updated_ms=%s "
                            "WHERE id=%s AND attempt=%s AND "
                            "(status IN ('DRAFT','UNKNOWN') OR (status='SENDING' AND updated_ms<%s))",
                            (now_ms, command_id, attempt, now_ms - lease_ms),
                        )
                    else:
                        changed = await cur.execute(
                            "UPDATE gm_command SET status='SENDING',attempt=attempt+1,updated_ms=%s "
                            "WHERE id=%s AND attempt=%s AND status='DRAFT'",
                            (now_ms, command_id, attempt),
                        )
                    if changed != 1:
                        raise _conflict("命令正在发送或已拒绝，请核对当前状态")

                    new_attempt = attempt + 1
                    await self._insert_audit(cur, command_id, operator, "SENDING", new_attempt, None, now_ms)
                    updated_view, updated_req, _, _ = await self._load_row(cur, command_id)
                await conn.commit()
                return updated_view, updated_req, new_attempt, False
            except Exception:
                await conn.rollback()
                raise

    async def _finish_send(
        self,
        *,
        command_id: str,
        operator: str,
        attempt: int,
        final_state: str,
        outcome: dict[str, Any],
    ) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    now_ms = self._clock_ms()
                    changed = await cur.execute(
                        "UPDATE gm_command SET status=%s,result_json=%s,updated_ms=%s "
                        "WHERE id=%s AND status='SENDING' AND attempt=%s",
                        (final_state, _canonical_json(outcome), now_ms, command_id, attempt),
                    )
                    if changed == 1:
                        await self._insert_audit(cur, command_id, operator, final_state, attempt, outcome, now_ms)
                    view, _, _, _ = await self._load_row(cur, command_id)
                await conn.commit()
                return view
            except Exception:
                await conn.rollback()
                raise
