"""rank gRPC 服务适配层与 fail-closed 鉴权（ClientPlayerRank + RankAdmin）。

对外暴露：
- ``rankpb.ClientPlayerRank``: ``GetRankPage`` / ``GetRankAround``（Gate Ed25519 会话鉴权 + 全服榜限制）
- ``rankpb.RankAdmin``: ``OpenRankGeneration`` / ``SubmitRankScores`` / ``FreezeRankGeneration`` /
  ``RemoveRankEntry`` / ``DeleteRankGeneration``（拒绝客户端会话 + ServiceCallAuth HMAC 鉴权）
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from typing import Protocol as TypingProtocol, runtime_checkable

import grpc
from google.protobuf.message import Message

from ..dualcontrol import ApprovalMissingError, DualControlError, extract_approval, validate_admin_approval
from ..grpc_runtime import GrpcClient, verify_service_call
from ..login.caller_auth import AuthError, CallerVerifier, SESSION_DETAIL, parse_details
from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_SERVICE_UNAVAILABLE,
    RankError,
    tip_code_for_error,
)
from .model import RANK_SCOPE_GLOBAL, valid_account
from .store import RankStore

logger = logging.getLogger(__name__)

SERVICE_CLIENT_RANK = "rankpb.ClientPlayerRank"
SERVICE_RANK_ADMIN = "rankpb.RankAdmin"
BATCH_GET_PLAYER_NAME_PATH = "/data_service.DataService/BatchGetPlayerName"
NAME_ENRICH_TIMEOUT_SEC = 0.2

CODE = grpc.StatusCode


@runtime_checkable
class NameReader(TypingProtocol):
    """排行榜读路径玩家名批量回填客户端协议（``BatchGetPlayerName``）。"""

    async def batch_get_player_name(
        self,
        player_ids: list[int],
        *,
        timeout: float = NAME_ENRICH_TIMEOUT_SEC,
    ) -> dict[int, str]: ...


class GrpcNameReader:
    """基于 ``GrpcClient`` 的 ``DataService/BatchGetPlayerName`` 适配器。"""

    def __init__(self, protocol: Protocol, client: GrpcClient) -> None:
        self._protocol = protocol
        self._client = client

    async def batch_get_player_name(
        self,
        player_ids: list[int],
        *,
        timeout: float = NAME_ENRICH_TIMEOUT_SEC,
    ) -> dict[int, str]:
        req = self._protocol.message("data_service.BatchGetPlayerNameRequest", player_ids=player_ids)
        raw = await self._client.call(
            BATCH_GET_PLAYER_NAME_PATH,
            req.SerializeToString(),
            timeout=timeout,
        )
        resp = self._protocol.parse("data_service.BatchGetPlayerNameResponse", raw)
        return {int(pid): str(name) for pid, name in resp.names.items()}


def _has_session_metadata(context) -> bool:
    """检查 gRPC 入站 metadata 是否带有 ``x-session-detail-bin`` 头。"""
    if context is None:
        return False
    for key, _value in context.invocation_metadata() or ():
        if key.lower() == SESSION_DETAIL:
            return True
    return False


class RankService:
    """挂载 ``rankpb.ClientPlayerRank`` 与 ``rankpb.RankAdmin`` 两组 gRPC 服务。"""

    def __init__(
        self,
        protocol: Protocol,
        store: RankStore,
        *,
        producer_secrets: Mapping[str, bytes],
        caller_verifier: CallerVerifier | None = None,
        name_reader: NameReader | None = None,
        request_budget_sec: float = 3.0,
        allow_unapproved: bool = True,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.protocol = protocol
        self.store = store
        self._producer_secrets = dict(producer_secrets)
        self._caller_verifier = caller_verifier
        self._name_reader = name_reader
        self._budget_sec = request_budget_sec
        self._allow_unapproved = bool(allow_unapproved)
        self._clock_ms = clock_ms

    def services(self) -> dict[str, dict[str, Callable]]:
        return {
            SERVICE_CLIENT_RANK: {
                "GetRankPage": self.get_rank_page,
                "GetRankAround": self.get_rank_around,
            },
            SERVICE_RANK_ADMIN: {
                "OpenRankGeneration": self.open_rank_generation,
                "SubmitRankScores": self.submit_rank_scores,
                "FreezeRankGeneration": self.freeze_rank_generation,
                "RemoveRankEntry": self.remove_rank_entry,
                "DeleteRankGeneration": self.delete_rank_generation,
            },
        }

    # -- 鉴权门禁（fail-closed） ----------------------------------------------------------------------

    async def _admit_client(self, context, path: str) -> int:
        """校验 Gate Ed25519 签名会话与完整在线角色原身份，返回 ``player_id``。"""
        if self._caller_verifier is None:
            await context.abort(CODE.UNAUTHENTICATED, "gate session verifier is not configured")
        metadata = context.invocation_metadata() if context is not None else ()
        try:
            raw = self._caller_verifier.authenticate(metadata, path)
        except AuthError as exc:
            logger.warning("rank client auth failed path=%s reason=%s", path, exc)
            await context.abort(CODE.UNAUTHENTICATED, f"invalid gate session: {exc}")
        if raw is None:
            logger.warning("rank client call rejected without gate session path=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, f"{path} accepts only client requests forwarded by gate")
        try:
            details = parse_details(self.protocol, raw)
        except AuthError as exc:
            logger.warning("rank client session payload invalid path=%s reason=%s", path, exc)
            await context.abort(CODE.UNAUTHENTICATED, "invalid gate session payload")
        if (
            details.player_id == 0
            or details.owner_revision == 0
            or not valid_account(details.authenticated_account)
        ):
            await context.abort(CODE.PERMISSION_DENIED, "online request has no original role identity")
        return int(details.player_id)

    async def _admit_admin(self, context, request: Message, path: str) -> str:
        """拒绝带客户端会话头的调用，并校验内部 ``ServiceCallAuth`` HMAC 签名。"""
        if _has_session_metadata(context):
            logger.warning("rank admin call rejected with client session metadata path=%s", path)
            await context.abort(CODE.PERMISSION_DENIED, f"{path} is not callable by clients")
        caller = request.auth.caller if request.HasField("auth") else ""
        secret = self._producer_secrets.get(caller, b"")
        if not caller or not secret:
            await context.abort(CODE.UNAUTHENTICATED, "internal caller authentication failed")
        if not verify_service_call(path, request, caller, self._clock_ms(), secret):
            await context.abort(CODE.UNAUTHENTICATED, "internal caller authentication failed")
        return caller

    async def _with_budget(self, coro):
        if self._budget_sec > 0:
            async with asyncio.timeout(self._budget_sec):
                return await coro
        return await coro

    async def _enrich_names(self, resp: Message) -> None:
        """批量回填排行条目的玩家名（200ms 上限，权威名称服务缺失或失败按 fail-closed 报错）。"""
        if self._name_reader is None:
            raise RuntimeError("rank authoritative name service is not configured")
        targets: list[Message] = list(resp.entries)
        if resp.HasField("self"):
            targets.append(resp.self)
        seen: set[int] = set()
        player_ids: list[int] = []
        for entry in targets:
            eid = int(entry.entity_id)
            if eid not in seen:
                seen.add(eid)
                player_ids.append(eid)
        if not player_ids:
            return
        async with asyncio.timeout(NAME_ENRICH_TIMEOUT_SEC):
            names = await self._name_reader.batch_get_player_name(player_ids, timeout=NAME_ENRICH_TIMEOUT_SEC)
        for entry in targets:
            entry.name = names.get(int(entry.entity_id), "")

    def _reject(self, response_type: str, code: int) -> Message:
        resp = self.protocol.message(response_type)
        if code != 0:
            resp.error_message.id = code
        return resp

    # -- ClientPlayerRank -----------------------------------------------------------------------------

    async def get_rank_page(self, request: Message, context, path: str) -> Message:
        player_id = await self._admit_client(context, path)
        if not request.HasField("board") or int(request.board.scope) != RANK_SCOPE_GLOBAL:
            return self._reject("rankpb.GetRankPageResponse", ERR_INVALID_PARAMETER)
        try:
            resp = await self._with_budget(self.store.get_page(player_id, request))
            await self._enrich_names(resp)
            return resp
        except RankError as exc:
            return self._reject("rankpb.GetRankPageResponse", exc.tip_code)
        except Exception:
            logger.error("rank GetRankPage failed player_id=%d", player_id, exc_info=True)
            return self._reject("rankpb.GetRankPageResponse", ERR_SERVICE_UNAVAILABLE)

    async def get_rank_around(self, request: Message, context, path: str) -> Message:
        player_id = await self._admit_client(context, path)
        if not request.HasField("board") or int(request.board.scope) != RANK_SCOPE_GLOBAL:
            return self._reject("rankpb.GetRankAroundResponse", ERR_INVALID_PARAMETER)
        try:
            resp = await self._with_budget(self.store.get_around(player_id, request))
            await self._enrich_names(resp)
            return resp
        except RankError as exc:
            return self._reject("rankpb.GetRankAroundResponse", exc.tip_code)
        except Exception:
            logger.error("rank GetRankAround failed player_id=%d", player_id, exc_info=True)
            return self._reject("rankpb.GetRankAroundResponse", ERR_SERVICE_UNAVAILABLE)

    # -- RankAdmin ------------------------------------------------------------------------------------

    async def open_rank_generation(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_admin(context, request, path)
        try:
            return await self._with_budget(self.store.open_generation(caller, request))
        except RankError as exc:
            return self._reject("rankpb.OpenRankGenerationResponse", exc.tip_code)
        except Exception:
            logger.error("rank OpenRankGeneration failed caller=%s", caller, exc_info=True)
            return self._reject("rankpb.OpenRankGenerationResponse", ERR_SERVICE_UNAVAILABLE)

    async def submit_rank_scores(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_admin(context, request, path)
        try:
            return await self._with_budget(self.store.submit_scores(caller, request))
        except RankError as exc:
            return self._reject("rankpb.SubmitRankScoresResponse", exc.tip_code)
        except Exception:
            logger.error(
                "rank SubmitRankScores failed caller=%s board_id=%d shard=%d cursor=%d",
                caller,
                request.board_id,
                request.source_shard,
                request.source_cursor,
                exc_info=True,
            )
            return self._reject("rankpb.SubmitRankScoresResponse", ERR_SERVICE_UNAVAILABLE)

    async def freeze_rank_generation(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_admin(context, request, path)
        try:
            return await self._with_budget(self.store.freeze_generation(caller, request))
        except RankError as exc:
            return self._reject("rankpb.FreezeRankGenerationResponse", exc.tip_code)
        except Exception:
            logger.error(
                "rank FreezeRankGeneration failed caller=%s board_id=%d",
                caller,
                request.board_id,
                exc_info=True,
            )
            return self._reject("rankpb.FreezeRankGenerationResponse", ERR_SERVICE_UNAVAILABLE)

    async def _admit_remove_approval(self, context, caller: str, path: str, request: Message) -> None:
        """核对 RankAdmin 删除/隐藏条目的双人复核批准记录（仅 caller=='admin' 强制，其余带了照样核）。"""
        approval = extract_approval(request)
        if approval is None and caller != "admin":
            return
        try:
            approved = validate_admin_approval(
                approval,
                operator="",
                request_id=int(request.request_id),
                allow_unapproved=self._allow_unapproved,
            )
        except ApprovalMissingError:
            logger.error(
                "[RankAdmin] %s refused: dual-control approval record is missing caller=%s board_id=%d generation=%d request_id=%d",
                path,
                caller,
                int(request.board_id),
                int(request.generation),
                int(request.request_id),
            )
            await context.abort(CODE.PERMISSION_DENIED, "dual_control_approval_required")
        except DualControlError as exc:
            logger.error(
                "[RankAdmin] %s refused: %s caller=%s board_id=%d generation=%d request_id=%d",
                path,
                exc,
                caller,
                int(request.board_id),
                int(request.generation),
                int(request.request_id),
            )
            await context.abort(CODE.PERMISSION_DENIED, f"dual_control_approval_invalid: {exc}")
        if not approved:
            logger.error(
                "[RankAdmin] %s accepted without a dual-control approval record caller=%s board_id=%d generation=%d request_id=%d (AdminAllowUnapproved is on)",
                path,
                caller,
                int(request.board_id),
                int(request.generation),
                int(request.request_id),
            )

    async def remove_rank_entry(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_admin(context, request, path)
        await self._admit_remove_approval(context, caller, path, request)
        try:
            await self._with_budget(self.store.remove_entry(caller, request, hide=False))
            return self._reject("rankpb.RankAdminResponse", 0)
        except RankError as exc:
            return self._reject("rankpb.RankAdminResponse", tip_code_for_error(exc))
        except Exception:
            logger.error(
                "rank RemoveRankEntry failed caller=%s board_id=%d entity_id=%d",
                caller,
                request.board_id,
                request.entity_id,
                exc_info=True,
            )
            return self._reject("rankpb.RankAdminResponse", ERR_SERVICE_UNAVAILABLE)

    async def delete_rank_generation(self, request: Message, context, path: str) -> Message:
        caller = await self._admit_admin(context, request, path)
        await self._admit_remove_approval(context, caller, path, request)
        try:
            await self._with_budget(self.store.remove_entry(caller, request, hide=True))
            return self._reject("rankpb.RankAdminResponse", 0)
        except RankError as exc:
            return self._reject("rankpb.RankAdminResponse", tip_code_for_error(exc))
        except Exception:
            logger.error(
                "rank DeleteRankGeneration failed caller=%s board_id=%d",
                caller,
                request.board_id,
                exc_info=True,
            )
            return self._reject("rankpb.RankAdminResponse", ERR_SERVICE_UNAVAILABLE)
