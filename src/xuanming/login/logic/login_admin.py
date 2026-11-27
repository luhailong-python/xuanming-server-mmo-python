"""LoginAdmin 运维 RPC 与 fail-closed 鉴权（对应 Go ``server/loginadmin`` + ``logic/loginadmin`` + ``logic/admin``）。"""

from __future__ import annotations

import hmac
import logging

import grpc

from ...dualcontrol import ApprovalMissingError, DualControlError, extract_approval, validate_admin_approval
from .. import keys, tips
from ..account_deletion import SOURCE_ADMIN, mask_account
from ..directory import Conflict, Invalid, Missing
from . import RpcAbort, tip

logger = logging.getLogger(__name__)

ADMIN_TOKEN_ENV = "MMORPG_LOGIN_ADMIN_TOKEN"
ADMIN_TOKEN_HEADER = "x-admin-token"
MIN_ADMIN_TOKEN_BYTES = 16

MAX_REMOVE_PLAYERS = 100
MAX_OPERATOR_BYTES = 64
MAX_REASON_BYTES = 256

# loginpb.RemovePlayerOutcome 枚举值
OUTCOME_UNSPECIFIED = 0
OUTCOME_REMOVED = 1
OUTCOME_NOT_FOUND = 2
OUTCOME_NOT_ORPHAN = 3
OUTCOME_CONFLICT = 4
OUTCOME_RETRY = 5


def authorize_admin(metadata, expected_token: str, method: str) -> None:
    """LoginAdmin 统一鉴权（fail-closed）：未配置口令即停用整个运维面，口令用常数时间比对且绝不进日志。"""
    if not expected_token:
        logger.error("[LoginAdmin] %s refused: %s is not set, admin surface disabled", method, ADMIN_TOKEN_ENV)
        raise RpcAbort(grpc.StatusCode.PERMISSION_DENIED,
                       f"{method} is disabled because {ADMIN_TOKEN_ENV} is not set")
    got = ""
    for key, value in metadata or ():
        if str(key).lower() == ADMIN_TOKEN_HEADER:
            got = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
            break
    if not got or not hmac.compare_digest(got.encode("utf-8"), expected_token.encode("utf-8")):
        logger.error("[LoginAdmin] %s refused: bad or missing %s present=%s", method, ADMIN_TOKEN_HEADER, bool(got))
        raise RpcAbort(grpc.StatusCode.PERMISSION_DENIED, f"{method} requires a valid {ADMIN_TOKEN_HEADER}")


def admit_admin_approval(request, method: str, *, allow_unapproved: bool) -> None:
    """LoginAdmin 双人复核拦截（对应 Go ``server/loginadmin/admin_approval.go``）。

    - ``RemovePlayersFromAccounts``：``dry_run=True`` 时不强制要求批准记录，但带了仍须合格；
    - ``ScheduleAccountDeletion`` 与 ``ResetAccountRealName``：恒要求合格的批准记录；
    - 仅当 ``allow_unapproved=True``（本机开发）时放行缺失记录并打 ERROR。
    """
    short = method.rsplit("/", 1)[-1]
    if short == "RemovePlayersFromAccounts":
        required = not bool(getattr(request, "dry_run", False))
    elif short in ("ScheduleAccountDeletion", "ResetAccountRealName"):
        required = True
    else:
        logger.error("[LoginAdmin] %s refused: method is not classified for dual control", short)
        raise RpcAbort(grpc.StatusCode.PERMISSION_DENIED, f"{short} is not classified for dual control")

    approval = extract_approval(request)
    if approval is None:
        if not required:
            return
        if not allow_unapproved:
            logger.error("[LoginAdmin] %s refused: dual-control approval record is missing", short)
            raise RpcAbort(grpc.StatusCode.PERMISSION_DENIED, "dual_control_approval_required")
        logger.error(
            "[LoginAdmin] %s accepted without a dual-control approval record (AdminAllowUnapproved is on)",
            short,
        )
        return
    try:
        validate_admin_approval(
            approval,
            operator=str(getattr(request, "operator", "") or ""),
            allow_unapproved=False,
            require_operator=True,
        )
    except ApprovalMissingError:
        raise RpcAbort(grpc.StatusCode.PERMISSION_DENIED, "dual_control_approval_required") from None
    except DualControlError as exc:
        logger.error("[LoginAdmin] %s refused: %s", short, exc)
        raise RpcAbort(grpc.StatusCode.PERMISSION_DENIED, f"dual_control_approval_invalid: {exc}") from exc


def _valid_bounded_text(text: str, max_bytes: int) -> bool:
    if not text:
        return False
    return 0 < len(text.encode("utf-8")) <= max_bytes


async def _remove_one_orphan(ctx, player_id: int, dry_run: bool) -> int:
    try:
        owned = await ctx.data.lookup_role(player_id)
    except Missing:
        return OUTCOME_NOT_FOUND
    except Exception as error:
        logger.error("[LoginAdmin] orphan ownership lookup failed player=%d err=%s", player_id, error)
        return OUTCOME_RETRY

    try:
        has_ledger = await ctx.data.get_player_asset_op_ledger(player_id)
    except Exception as error:
        logger.error("[LoginAdmin] orphan player_store ledger check failed player=%d err=%s", player_id, error)
        return OUTCOME_RETRY
    if has_ledger:
        return OUTCOME_NOT_ORPHAN

    # 路由缺失时账本同样查不到，无法证明没有存档，绝不写不可逆墓碑
    try:
        await ctx.data.get_player_home_zone_strict(player_id)
    except Missing:
        logger.error("[LoginAdmin] role has owner but no route; refusing orphan tombstone player=%d", player_id)
        return OUTCOME_NOT_ORPHAN
    except Exception as error:
        logger.error("[LoginAdmin] orphan route check failed player=%d err=%s", player_id, error)
        return OUTCOME_RETRY

    if getattr(owned, "initial_player_commit", False):
        logger.error("[LoginAdmin] role committed by current create flow; refusing orphan tombstone player=%d",
                     player_id)
        return OUTCOME_NOT_ORPHAN

    if dry_run:
        return OUTCOME_REMOVED

    try:
        await ctx.data.remove_account_roles(owned.account, [(player_id, int(owned.owner_revision))])
        return OUTCOME_REMOVED
    except Missing:
        return OUTCOME_NOT_FOUND
    except Conflict:
        return OUTCOME_CONFLICT
    except Exception as error:
        logger.error("[LoginAdmin] orphan removal failed player=%d err=%s", player_id, error)
        return OUTCOME_RETRY


async def remove_players_from_accounts(ctx, request, details):
    """摘除孤儿角色（核验无存档且路由存在且非初始提交残留后走账号目录删角，支持 dry_run）。"""
    if ctx.data is None:
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "account directory is not ready")
    if (
        len(request.player_ids) > MAX_REMOVE_PLAYERS
        or not _valid_bounded_text(request.operator, MAX_OPERATOR_BYTES)
        or not _valid_bounded_text(request.reason, MAX_REASON_BYTES)
    ):
        raise RpcAbort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "player_ids must be at most 100 entries; operator and reason are required",
        )

    response = ctx.protocol.message("loginpb.RemovePlayersFromAccountsResponse")
    seen: set[int] = set()
    for raw_pid in request.player_ids:
        pid = int(raw_pid)
        if pid in seen:
            continue
        seen.add(pid)
        outcome = OUTCOME_NOT_FOUND if pid == 0 else await _remove_one_orphan(ctx, pid, bool(request.dry_run))
        if outcome == OUTCOME_REMOVED:
            response.removed_count += 1
        elif outcome == OUTCOME_NOT_FOUND:
            response.not_found_count += 1
        elif outcome == OUTCOME_NOT_ORPHAN:
            response.not_orphan_count += 1
        else:
            response.failed_count += 1
        response.results.add(player_id=pid, outcome=outcome)
        logger.info("[LoginAdmin] RemovePlayersFromAccounts player=%d outcome=%d dry_run=%s operator=%s",
                    pid, outcome, bool(request.dry_run), request.operator)
    return response


async def schedule_account_deletion(ctx, request, details):
    """客服代办注销：按角色反查账号，走与用户本人相同的预检与冷静期（source=2）。"""
    if (
        not request.player_id
        or not _valid_bounded_text(request.operator, MAX_OPERATOR_BYTES)
        or not _valid_bounded_text(request.reason, MAX_REASON_BYTES)
    ):
        raise RpcAbort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "player_id, operator (<= 64 bytes) and reason (<= 256 bytes) are required",
        )
    deletion = ctx.account_deletion_service()
    if deletion is None or ctx.data is None:
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "account deletion is not ready")
    try:
        owned = await ctx.data.lookup_role(int(request.player_id))
    except Missing:
        return ctx.protocol.message("loginpb.AccountDeletionResponse",
                                    error_message=tip(ctx.protocol, tips.ACCOUNT_NOT_FOUND))
    except Exception as error:
        logger.error("[LoginAdmin] ScheduleAccountDeletion lookup failed player=%d err=%s", request.player_id, error)
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "account directory lookup failed") from error
    logger.info("[LoginAdmin] ScheduleAccountDeletion player=%d account=%s operator=%s",
                request.player_id, mask_account(owned.account), request.operator)
    return await deletion.request(owned.account, SOURCE_ADMIN)


async def reset_account_real_name(ctx, request, details):
    """客服重置实名：player_id 与 account 二选一，清掉账号 pi 与认证中的 pending ai（幂等）。"""
    has_pid = bool(request.player_id)
    has_account = bool(request.account)
    if (
        has_pid == has_account
        or not _valid_bounded_text(request.operator, MAX_OPERATOR_BYTES)
        or not _valid_bounded_text(request.reason, MAX_REASON_BYTES)
    ):
        raise RpcAbort(
            grpc.StatusCode.INVALID_ARGUMENT,
            "exactly one of player_id / account, plus operator (<= 64 bytes) and reason (<= 256 bytes), are required",
        )
    if ctx.data is None:
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "account directory is not ready")
    not_found = ctx.protocol.message("loginpb.ResetAccountRealNameResponse",
                                     error_message=tip(ctx.protocol, tips.ACCOUNT_NOT_FOUND))
    account = request.account
    if not account:
        try:
            owned = await ctx.data.lookup_role(int(request.player_id))
        except Missing:
            return not_found
        except Exception as error:
            logger.error("[LoginAdmin] ResetAccountRealName lookup failed player=%d err=%s", request.player_id, error)
            raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "account directory lookup failed") from error
        account = owned.account

    try:
        had_real_name = await ctx.data.reset_real_name(account)
    except (Missing, Invalid):
        return not_found
    except Exception as error:
        logger.error("[LoginAdmin] ResetAccountRealName failed account=%s err=%s", mask_account(account), error)
        raise RpcAbort(grpc.StatusCode.UNAVAILABLE, "reset real name failed") from error

    # 尽力作废认证中提交的 pending ai，删不掉时 48 小时自然过期
    if ctx.login_redis is not None:
        try:
            await ctx.login_redis.delete(keys.real_name_pending_key(account))
        except Exception as error:
            logger.error("[LoginAdmin] drop pending real-name failed account=%s err=%s", mask_account(account), error)

    logger.info("[LoginAdmin] ResetAccountRealName account=%s had_real_name=%s operator=%s",
                mask_account(account), had_real_name, request.operator)
    return ctx.protocol.message("loginpb.ResetAccountRealNameResponse", had_real_name=had_real_name)
