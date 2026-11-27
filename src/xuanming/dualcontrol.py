"""共享后台双人复核批准记录核对（对应 Go ``go/shared/dualcontrol``）。

高风险管理命令（甲级）在请求体中携带 ``common.base.AdminApproval``，由资源服务一侧做无状态只读复查：
- ``approval_id`` 必须为 1~19 位不含前导零的十进制正整数文本；
- ``proposer`` 与 ``approver`` 必须为合法的身份提供方主体标识（``^[A-Za-z0-9._@-]{1,64}$``）；
- ``approver != proposer``（按字节严格比对，不做大小写折叠）；
- ``approved_at_ms > 0``；
- 提供 ``operator`` 时 ``proposer`` 必须与 ``operator`` 逐字一致；
- 提供 ``request_id`` 时 ``approval_id`` 必须等于 ``str(request_id)``（确保一次批准只执行一次）；
- ``allow_unapproved=True`` 仅在批准记录完全缺失/全空时放行（限本机开发）；一旦请求携带了任意非空批准字段，
  即使 ``allow_unapproved=True`` 也必须严格按 fail-closed 核对。
"""

from __future__ import annotations

import re

_APPROVAL_ID_RE = re.compile(r"^[1-9][0-9]{0,18}$")
_SUBJECT_RE = re.compile(r"^[A-Za-z0-9._@-]{1,64}$")


class DualControlError(ValueError):
    """双人复核批准记录核对失败基类。"""


class ApprovalMissingError(DualControlError):
    """请求缺少双人复核批准记录。"""


class ApprovalInvalidError(DualControlError):
    """请求携带的双人复核批准记录字段不合法、自批或与操作人/请求号不符。"""


def extract_approval(request: object | None, field_name: str = "approval") -> object | None:
    """从 protobuf 请求消息中提取可选 ``AdminApproval`` 子消息；未设置时返回 ``None``。"""
    if request is None:
        return None
    has_field = getattr(request, "HasField", None)
    if callable(has_field):
        try:
            if not has_field(field_name):
                return None
        except ValueError:
            return None
    return getattr(request, field_name, None)


def _is_empty_approval(approval: object | None) -> bool:
    """判断批准对象是否缺失或全部四项均为零值。"""
    if approval is None:
        return True
    return (
        not str(getattr(approval, "approval_id", "") or "")
        and not str(getattr(approval, "proposer", "") or "")
        and not str(getattr(approval, "approver", "") or "")
        and int(getattr(approval, "approved_at_ms", 0) or 0) == 0
    )


def validate_admin_approval(
    approval: object | None,
    *,
    operator: str = "",
    allow_unapproved: bool = False,
    request_id: int | None = None,
    require_operator: bool = False,
) -> bool:
    """核对高风险管理请求的 ``AdminApproval``，返回 ``True`` 表示经有效双人批准、``False`` 表示本机开发免批放行。

    错误文本仅指出哪一项不合格，不回显请求内容，避免日志注入与敏感信息泄露。
    """
    if _is_empty_approval(approval):
        if allow_unapproved:
            return False
        raise ApprovalMissingError("dual-control approval record is missing")

    approval_id = str(getattr(approval, "approval_id", "") or "")
    proposer = str(getattr(approval, "proposer", "") or "")
    approver = str(getattr(approval, "approver", "") or "")
    approved_at_ms = int(getattr(approval, "approved_at_ms", 0) or 0)

    if not _APPROVAL_ID_RE.fullmatch(approval_id):
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: approval_id must be a positive decimal number"
        )
    if not _SUBJECT_RE.fullmatch(proposer):
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: proposer is not a valid operator identifier"
        )
    if not _SUBJECT_RE.fullmatch(approver):
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: approver is not a valid operator identifier"
        )
    if approved_at_ms <= 0:
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: approved_at_ms is missing"
        )
    if proposer == approver:
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: proposer and approver are the same person"
        )
    if (require_operator or operator != "") and proposer != operator:
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: proposer differs from the request operator"
        )
    if request_id is not None and approval_id != str(int(request_id)):
        raise ApprovalInvalidError(
            "dual-control approval record is invalid: request_id must be the approval id"
        )
    return True
