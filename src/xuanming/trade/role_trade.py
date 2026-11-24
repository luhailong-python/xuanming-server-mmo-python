"""角色交易目录客户端、Scene 封存器、RMB 身份解析器与内容安全标题送检适配器。

严格对齐 Go 侧 ``shared/accountdirectory/{trade_client.go,admission.go}`` 与 ``trade/internal/svc/role_trade.go``：
- 角色目录 6 个方法均以调用方 ``"trade"`` + ``MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE`` 签发 ``ServiceCallAuth``；
- Scene 封存调用 ``/scene_node.SceneNodeGrpc/SealAccountRoleTrade``，必须核对原持有人进程 ``node_uuid == seal.scene_instance_id``；
- ``ContentGuard`` 对人民币寄售标题按 ``TEXT_SCENE_TRADE_TITLE``(9) 同步送检，异常 fail-closed 拒绝，严禁日志输出标题原文。
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol as TypingProtocol

import grpc
from google.protobuf.message import Message

from ..cluster import SCENE
from ..grpc_runtime import GrpcClient, sign_service_call
from ..protocol import Protocol
from .errors import (
    ERR_INVALID_PARAMETER,
    ERR_TRADE_ORDER_STATE_CONFLICT,
    RoleDirectoryConflictError,
    RoleDirectoryFullError,
    RoleDirectoryInvalidError,
    RoleDirectoryMissingError,
    RoleDirectoryRetryLaterError,
    RoleDirectoryUnavailableError,
    TextCheckUnavailableError,
    TradeRejection,
)
from .model import (
    ACCOUNT_DIRECTORY_CALLER,
    UINT64_MAX,
    RoleOriginal,
    valid_account,
)

logger = logging.getLogger(__name__)

_DATA_SERVICE_PREFIX = "/data_service.DataService/"
_SEAL_METHOD_PATH = "/scene_node.SceneNodeGrpc/SealAccountRoleTrade"
_CONTENT_GUARD_PATH = "/content_guard.ContentGuard/CheckText"

_DIRECTORY_TIMEOUT_SEC = 2.5
_SEAL_TIMEOUT_SEC = 0.35
_CONTENT_GUARD_TIMEOUT_SEC = 1.0

MAX_TRADE_HOLDERS = 256
INITIAL_OWNER_EPOCH = 1
INITIAL_SAVE_SEQ = 1

# AccountRoleTradeState 枚举值（与 proto/data_service/data_service.proto 对齐）
ROLE_TRADE_STATE_FREEZING = 1
ROLE_TRADE_STATE_FROZEN = 2
ROLE_TRADE_STATE_CANCELLED = 3
ROLE_TRADE_STATE_TRANSFERRED = 4
ROLE_TRADE_STATE_CANCEL_PENDING = 5
ROLE_TRADE_STATE_PREPARING = 6

# AccountRoleFreezeEvidenceKind 枚举值
FREEZE_EVIDENCE_NO_ADMISSION = 1
FREEZE_EVIDENCE_SCENE = 2
FREEZE_EVIDENCE_INITIAL_COMMIT = 3

_INSTANCE_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


# -- 角色交易凭据与回执纯校验（与 Go accountdirectory/admission.go & trade_client.go 对齐） --------------------

def valid_scene_instance_id(instance_id: str) -> bool:
    return bool(_INSTANCE_UUID_RE.match(instance_id))


def valid_scene_node_id(node_id: str) -> bool:
    if not node_id or len(node_id.encode("utf-8")) > 96:
        return False
    return not any(ch in "\x00\r\n\t " for ch in node_id)


def valid_trade_seal(seal: Message | None) -> bool:
    if seal is None:
        return False
    return (
        int(seal.listing_id) > 0
        and int(seal.player_id) > 0
        and int(seal.owner_revision) > 0
        and valid_scene_instance_id(str(seal.scene_instance_id))
        and int(seal.through_generation) > 0
        and valid_scene_node_id(str(seal.scene_node_id))
        and int(seal.zone_id) > 0
    )


def valid_freeze_evidence(ev: Message | None) -> bool:
    if ev is None:
        return False
    if (
        int(ev.player_id) == 0
        or int(ev.listing_id) == 0
        or int(ev.owner_revision) == 0
        or len(bytes(ev.persisted_digest)) != 32
    ):
        return False
    kind = int(ev.kind)
    epoch = int(ev.owner_epoch)
    seq = int(ev.save_seq)
    if kind == FREEZE_EVIDENCE_NO_ADMISSION:
        return epoch == 0 and seq == 0
    if kind == FREEZE_EVIDENCE_SCENE:
        return epoch > 0 and seq > 0 and not (epoch == INITIAL_OWNER_EPOCH and seq == INITIAL_SAVE_SEQ)
    if kind == FREEZE_EVIDENCE_INITIAL_COMMIT:
        return epoch == INITIAL_OWNER_EPOCH and seq == INITIAL_SAVE_SEQ
    return False


def no_actor_freeze_evidence(ev: Message | None) -> bool:
    return ev is not None and int(ev.kind) in (FREEZE_EVIDENCE_NO_ADMISSION, FREEZE_EVIDENCE_INITIAL_COMMIT)


def valid_role_trade_receipt(
    receipt: Message | None,
    listing_id: int,
    player_id: int,
    seller_account: str,
    owner_revision: int,
) -> bool:
    if receipt is None:
        return False
    state = int(receipt.state)
    if (
        int(receipt.listing_id) != listing_id
        or int(receipt.player_id) != player_id
        or str(receipt.seller_account) != seller_account
        or int(receipt.seller_owner_revision) != owner_revision
        or not ROLE_TRADE_STATE_FREEZING <= state <= ROLE_TRADE_STATE_CANCEL_PENDING
        or len(receipt.holders) > MAX_TRADE_HOLDERS
    ):
        return False
    ev = receipt.freeze_evidence if receipt.HasField("freeze_evidence") else None
    if ev is not None:
        if (
            not valid_freeze_evidence(ev)
            or int(ev.player_id) != player_id
            or int(ev.listing_id) != listing_id
            or int(ev.owner_revision) != owner_revision
        ):
            return False
    seen: set[str] = set()
    for holder in receipt.holders:
        seal = holder.seal if holder.HasField("seal") else None
        if (
            not valid_trade_seal(seal)
            or int(seal.listing_id) != listing_id
            or int(seal.player_id) != player_id
            or int(seal.owner_revision) != owner_revision
        ):
            return False
        inst_id = str(seal.scene_instance_id)
        if inst_id in seen:
            return False
        seen.add(inst_id)
        if state in (ROLE_TRADE_STATE_FROZEN, ROLE_TRADE_STATE_TRANSFERRED, ROLE_TRADE_STATE_CANCELLED) and not holder.drained:
            return False
    if no_actor_freeze_evidence(ev) and len(receipt.holders) > 0:
        return False
    if state in (ROLE_TRADE_STATE_FROZEN, ROLE_TRADE_STATE_TRANSFERRED) and ev is None:
        return False
    return True


def valid_role_transfer_receipt(
    receipt: Message | None,
    tx_id: int,
    listing_id: int,
    player_id: int,
    seller_account: str,
    buyer_account: str,
    expected_owner_revision: int,
) -> bool:
    if receipt is None or expected_owner_revision >= UINT64_MAX:
        return False
    return (
        int(receipt.tx_id) == tx_id
        and int(receipt.listing_id) == listing_id
        and int(receipt.player_id) == player_id
        and str(receipt.seller_account) == seller_account
        and str(receipt.buyer_account) == buyer_account
        and int(receipt.seller_owner_revision) == expected_owner_revision
        and int(receipt.buyer_owner_revision) == expected_owner_revision + 1
        and receipt.HasField("role")
        and int(receipt.role.player_id) == player_id
        and int(receipt.created_ms) > 0
    )


def _classify_directory_error(exc: Exception) -> Exception:
    code = exc.code() if isinstance(exc, grpc.RpcError) else None
    if code == grpc.StatusCode.NOT_FOUND:
        return RoleDirectoryMissingError(str(exc))
    if code in (grpc.StatusCode.ALREADY_EXISTS, grpc.StatusCode.FAILED_PRECONDITION):
        return RoleDirectoryConflictError(str(exc))
    if code == grpc.StatusCode.ABORTED:
        return RoleDirectoryRetryLaterError(str(exc))
    if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
        return RoleDirectoryFullError(str(exc))
    if code == grpc.StatusCode.INVALID_ARGUMENT:
        return RoleDirectoryInvalidError(str(exc))
    return RoleDirectoryUnavailableError(str(exc))


# -- 角色交易 / 封存 / 身份 / 内容安全协议接口 ----------------------------------------------------------------

class RoleTradeAuthority(TypingProtocol):
    """全局账号角色目录交易客户端协议（对应 Go commerce.RoleTradeAuthority）。"""

    async def lookup(self, player_id: int) -> Message: ...
    async def begin(self, req: Message) -> Message: ...
    async def confirm(self, req: Message) -> Message: ...
    async def cancel(self, req: Message) -> Message: ...
    async def transfer(self, req: Message) -> Message: ...
    async def role_slot_full(self, account: str) -> bool: ...


class RoleSealer(TypingProtocol):
    """Scene 角色交易封存调用器协议。"""

    async def seal(self, seal_msg: Message) -> None: ...


class RmbIdentityResolver(TypingProtocol):
    """RMB 入口身份解析器：校验在线会话原身份与目标角色同账号归属。"""

    async def resolve(self, original: RoleOriginal, target_player_id: int = 0) -> Message: ...


@dataclass(frozen=True, slots=True)
class ContentCheckResult:
    """内容安全单次判定结果（仅含低基数 verdict 与 category，不携带被检原文）。"""

    verdict: int
    category: int = 0


class ContentGuardChecker(TypingProtocol):
    """标题内容安全送检协议。"""

    async def check_text(self, scene: int, player_id: int, text: str) -> ContentCheckResult: ...


# -- 生产 gRPC 适配器实现 ------------------------------------------------------------------------------------

class GrpcRoleTradeDirectory:
    """以 caller='trade' + MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE 调用 data_service 角色交易接口。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        secret: bytes,
        *,
        timeout_sec: float = _DIRECTORY_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if len(secret) < 32:
            raise ValueError("MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE must be at least 32 bytes")
        self._protocol = protocol
        self._call = call
        self._secret = secret
        self._timeout_sec = timeout_sec
        self._clock_ms = clock_ms

    async def _invoke(self, method: str, req: Message, resp_name: str) -> Message:
        path = _DATA_SERVICE_PREFIX + method
        sign_service_call(path, req, ACCOUNT_DIRECTORY_CALLER, int(self._clock_ms()), self._secret)
        try:
            raw = await self._call(path, req.SerializeToString(deterministic=True), timeout=self._timeout_sec)
        except Exception as exc:
            raise _classify_directory_error(exc) from exc
        return self._protocol.parse(resp_name, raw)

    async def lookup(self, player_id: int) -> Message:
        req = self._protocol.message("data_service.LookupAccountRoleRequest", player_id=int(player_id))
        out = await self._invoke("LookupAccountRole", req, "data_service.AccountRoleOwnership")
        if (
            not out.HasField("role")
            or int(out.role.player_id) != int(player_id)
            or not valid_account(str(out.account))
            or int(out.owner_revision) == 0
        ):
            raise RoleDirectoryUnavailableError(f"LookupAccountRole returned invalid ownership player={player_id}")
        return out

    async def begin(self, req: Message) -> Message:
        out = await self._invoke("BeginAccountRoleTrade", req, "data_service.AccountRoleTradeReceipt")
        if not valid_role_trade_receipt(
            out, int(req.listing_id), int(req.player_id), str(req.seller_account), int(req.expected_owner_revision)
        ):
            raise RoleDirectoryUnavailableError("BeginAccountRoleTrade returned invalid receipt")
        return out

    async def confirm(self, req: Message) -> Message:
        out = await self._invoke("ConfirmAccountRoleTradeFreeze", req, "data_service.AccountRoleTradeReceipt")
        if not valid_role_trade_receipt(
            out, int(req.listing_id), int(req.player_id), str(req.seller_account), int(req.expected_owner_revision)
        ):
            raise RoleDirectoryUnavailableError("ConfirmAccountRoleTradeFreeze returned invalid receipt")
        return out

    async def cancel(self, req: Message) -> Message:
        out = await self._invoke("CancelAccountRoleTrade", req, "data_service.AccountRoleTradeReceipt")
        if not valid_role_trade_receipt(
            out, int(req.listing_id), int(req.player_id), str(req.seller_account), int(req.expected_owner_revision)
        ):
            raise RoleDirectoryUnavailableError("CancelAccountRoleTrade returned invalid receipt")
        return out

    async def transfer(self, req: Message) -> Message:
        out = await self._invoke("TransferAccountRole", req, "data_service.AccountRoleTransferReceipt")
        if not valid_role_transfer_receipt(
            out,
            int(req.tx_id),
            int(req.listing_id),
            int(req.player_id),
            str(req.seller_account),
            str(req.buyer_account),
            int(req.expected_owner_revision),
        ):
            raise RoleDirectoryUnavailableError("TransferAccountRole returned invalid receipt")
        return out

    async def role_slot_full(self, account: str) -> bool:
        req = self._protocol.message("data_service.CheckAccountRoleSlotRequest", account=account)
        out = await self._invoke("CheckAccountRoleSlot", req, "data_service.CheckAccountRoleSlotResponse")
        if out.retry:
            raise RoleDirectoryRetryLaterError("CheckAccountRoleSlot requested retry")
        return bool(out.full)


class GrpcRoleSealer:
    """经 etcd 节点目录定位原持有人 Scene 节点并签发 SealAccountRoleTrade（绝不替换成新进程实例）。"""

    def __init__(
        self,
        protocol: Protocol,
        directory,
        secret: bytes,
        *,
        timeout_sec: float = _SEAL_TIMEOUT_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if len(secret) < 32:
            raise ValueError("MMORPG_ACCOUNT_DIRECTORY_SECRET_TRADE must be at least 32 bytes")
        self._protocol = protocol
        self._directory = directory
        self._secret = secret
        self._timeout_sec = timeout_sec
        self._clock_ms = clock_ms
        self._clients: dict[str, GrpcClient] = {}

    async def seal(self, seal_msg: Message) -> None:
        if self._directory is None or not valid_trade_seal(seal_msg):
            raise RoleDirectoryUnavailableError("invalid role trade seal or missing scene directory")
        try:
            node_id = int(str(seal_msg.scene_node_id))
        except ValueError as exc:
            raise RoleDirectoryUnavailableError(f"invalid scene_node_id: {seal_msg.scene_node_id!r}") from exc
        info = await self._directory.lookup(SCENE, int(seal_msg.zone_id), node_id)
        if info is None or not info.grpc_endpoint.ip or not 0 < int(info.grpc_endpoint.port) < 65536:
            raise RoleDirectoryUnavailableError(f"scene holder node not registered zone={seal_msg.zone_id} node={node_id}")
        if str(info.node_uuid) != str(seal_msg.scene_instance_id):
            raise RoleDirectoryUnavailableError("scene holder instance uuid mismatch")
        ip = str(info.grpc_endpoint.ip)
        endpoint = f"[{ip}]:{info.grpc_endpoint.port}" if ":" in ip and not ip.startswith("[") else f"{ip}:{info.grpc_endpoint.port}"
        client = self._clients.get(endpoint)
        if client is None:
            client = self._clients[endpoint] = GrpcClient(endpoint, timeout=self._timeout_sec, enable_retries=False)
        req = self._protocol.message("SealAccountRoleTradeRequest")
        req.seal.CopyFrom(seal_msg)
        sign_service_call(_SEAL_METHOD_PATH, req, ACCOUNT_DIRECTORY_CALLER, int(self._clock_ms()), self._secret)
        await client.call(_SEAL_METHOD_PATH, req.SerializeToString(deterministic=True), timeout=self._timeout_sec)

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        if clients:
            await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


class RmbIdentity:
    """用 Gate 验签原身份核对角色目录当前归属（与 Go svc/role_trade.go RmbIdentity 对齐）。"""

    def __init__(self, directory: RoleTradeAuthority) -> None:
        self._directory = directory

    async def resolve(self, original: RoleOriginal, target_player_id: int = 0) -> Message:
        if original.player_id <= 0 or original.owner_revision <= 0 or not valid_account(original.account):
            raise TradeRejection(ERR_INVALID_PARAMETER, "incomplete original role identity")
        me = await self._directory.lookup(original.player_id)
        if str(me.account) != original.account or int(me.owner_revision) != original.owner_revision:
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "actor role ownership changed")
        if target_player_id == 0 or target_player_id == original.player_id:
            return me
        target = await self._directory.lookup(target_player_id)
        if str(target.account) != original.account:
            raise TradeRejection(ERR_TRADE_ORDER_STATE_CONFLICT, "target role does not belong to actor account")
        return target


class GrpcContentGuardChecker:
    """经 content_guard.ContentGuard/CheckText 送检人民币寄售标题（fail-closed，不记正文日志）。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        *,
        timeout_sec: float = _CONTENT_GUARD_TIMEOUT_SEC,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._timeout_sec = timeout_sec

    async def check_text(self, scene: int, player_id: int, text: str) -> ContentCheckResult:
        req = self._protocol.message(
            "content_guard.CheckTextRequest",
            scene=int(scene),
            text=text,
            player_id=int(player_id),
        )
        try:
            raw = await self._call(
                _CONTENT_GUARD_PATH,
                req.SerializeToString(deterministic=True),
                timeout=self._timeout_sec,
            )
            resp = self._protocol.parse("content_guard.CheckTextResponse", raw)
        except Exception as exc:
            raise TextCheckUnavailableError(f"content guard rpc failed: {exc}") from exc
        return ContentCheckResult(verdict=int(resp.verdict), category=int(resp.category))
