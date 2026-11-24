"""mail 附件资产投递循环（AssetDeliveryLoop）：签名、投递 Scene、SQL 账本强制核验与退避重投。"""

from __future__ import annotations

import asyncio
import hmac
import logging
import random
import secrets
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from enum import IntEnum
from typing import Protocol as TypingProtocol

from google.protobuf.message import Message

from ..cluster import SCENE
from ..data.asset_ledger import (
    Outcome,
    canonical as asset_canonical,
    classify as classify_stream,
    find_receipt,
    find_stream,
    receipt_matches,
)
from ..grpc_runtime import GrpcClient
from ..protocol import Protocol
from .errors import (
    AssetOpLeaseLostError,
    AssetOpOutcomeFlipError,
    AssetOpPoisonRowError,
    ERR_ASSET_PARTIAL_APPLIED,
)
from .model import ASSET_OP_CALLER, ASSET_STREAM_SYSTEM_CREDIT, PendingAssetOp
from .store import (
    FINAL_ABORTED,
    FINAL_APPLIED,
    FINAL_APPLIED_PARTIAL,
    FINAL_REJECTED,
    MailStoreCore,
)

logger = logging.getLogger(__name__)

_ASSET_CREDIT_PATH = "/scene_node.SceneNodeGrpc/AssetCredit"
_LEDGER_PATH = "/data_service.DataService/GetPlayerAssetOpLedger"

DEFAULT_REQUERY_DELAYS_SEC = (0.1, 0.2, 0.4)


class LoopAction(IntEnum):
    FINALIZE = 1
    AWAIT_DURABLE = 2
    RETRY = 3
    ALERT = 4


@dataclass(frozen=True, slots=True)
class AssetCallResult:
    """单次资产 RPC 或 SQL 账本核验结果。"""

    outcome: int = int(Outcome.UNKNOWN)
    reason: int = 0
    durable: bool = False
    sql_committed: bool = False
    partial: bool = False
    local: bool = False
    absent: bool = False

    @property
    def is_scene_terminal(self) -> bool:
        return self.outcome in (int(Outcome.APPLIED), int(Outcome.REJECTED))

    @property
    def is_finalizable(self) -> bool:
        return self.sql_committed and self.durable and self.is_scene_terminal


@dataclass(frozen=True, slots=True)
class SceneTarget:
    """已定位到的 Scene 节点目标。"""

    endpoint: str
    call: Callable[..., Awaitable[bytes]]


class SceneResolver(TypingProtocol):
    """按 player_id 解析目标 Scene 节点（返回 None 表示离线或暂无持有人）。"""

    async def resolve(self, player_id: int) -> tuple[SceneTarget | None, bool]:
        """返回 ``(target, absent)``；``target is None`` 且 ``absent=True`` 表示位置键不存在。"""
        ...


class SqlCommittedLedgerReader(TypingProtocol):
    """强读玩家 SQL 已提交资产账本（data_service.GetPlayerAssetOpLedger）。"""

    async def read_sql_committed_ledger(self, player_id: int) -> Message | None: ...


def sign_asset_op(rpc: str, request: Message, caller: str, secret: bytes, now_ms: int) -> None:
    """按 mmorpg-asset-op/v1 跨语言规范就地写入 AssetOpAuth 签名。"""
    if not caller or len(secret) < 32:
        raise ValueError("asset op signer requires caller and >=32-byte secret")
    request.auth.caller = caller
    request.auth.timestamp_ms = now_ms
    request.auth.signature_hex = hmac.digest(secret, asset_canonical(rpc, request), "sha256").hex()


def build_asset_op_request(protocol: Protocol, op: PendingAssetOp, *, caller: str = ASSET_OP_CALLER) -> Message:
    """将待办资产行组装为 AssetOpRequest（未签名，仅填入 caller 供摘要/签名使用）。"""
    req = protocol.message(
        "AssetOpRequest",
        player_id=op.player_id,
        stream=op.stream,
        seq=op.seq,
        correlation_id=op.correlation_id,
        tx_type=op.tx_type,
        stream_epoch=op.stream_epoch,
    )
    req.bundle.CopyFrom(op.bundle)
    req.auth.caller = caller
    return req


def verify_ledger_receipt(protocol: Protocol, ledger: Message, op: PendingAssetOp) -> tuple[int, bool, int] | None:
    """核对 SQL 已提交账本中的位图与收据身份（debit, timestamp=0）；不满足时返回 None。"""
    streams = [st for st in ledger.streams if int(st.stream) == op.stream]
    if len(streams) != 1 or int(streams[0].stream_epoch) != op.stream_epoch:
        return None
    stream = streams[0]
    outcome = classify_stream(stream, op.stream_epoch, op.seq)
    if outcome not in (Outcome.APPLIED, Outcome.REJECTED):
        return None
    matching = [row for row in stream.receipts if int(row.seq) == op.seq]
    if len(matching) != 1:
        return None
    receipt = matching[0]
    identity_req = build_asset_op_request(protocol, op, caller=ASSET_OP_CALLER)
    if not receipt_matches(receipt, identity_req):
        return None
    if receipt.HasField("snapshot") and (receipt.snapshot.items or receipt.snapshot.HasField("pet")):
        return None
    partial = op.seq in stream.partial_seqs
    if outcome == Outcome.APPLIED:
        reason = ERR_ASSET_PARTIAL_APPLIED if partial else 0
    else:
        reason = next((int(row.reason_tip_id) for row in stream.rejections if int(row.seq) == op.seq), 0)
    return int(outcome), partial, reason


def decide_action(res: AssetCallResult, error: BaseException | None) -> LoopAction:
    """根据单次投递与 SQL 账本核验结果决定下一步动作（与 Go assetop.Decide 一致）。"""
    if error is not None:
        if isinstance(error, AssetOpOutcomeFlipError):
            return LoopAction.ALERT
        return LoopAction.RETRY
    if res.outcome == int(Outcome.UNKNOWN):
        return LoopAction.ALERT
    if res.outcome in (int(Outcome.APPLIED), int(Outcome.REJECTED)):
        if res.durable and res.sql_committed:
            return LoopAction.FINALIZE
        return LoopAction.AWAIT_DURABLE
    if res.outcome in (int(Outcome.RETRY), int(Outcome.NOT_HERE)):
        return LoopAction.RETRY
    return LoopAction.ALERT


def determine_final_status(rpc: str, res: AssetCallResult, op: PendingAssetOp) -> str | None:
    """把终结结局映射为业务终态，综合本次 partial 与行上粘性 27007 证据。"""
    if res.outcome == int(Outcome.APPLIED):
        if (
            res.partial
            or op.last_reason == ERR_ASSET_PARTIAL_APPLIED
            or res.reason == ERR_ASSET_PARTIAL_APPLIED
        ):
            return FINAL_APPLIED_PARTIAL
        return FINAL_APPLIED
    if res.outcome == int(Outcome.REJECTED):
        if rpc == "abort_debit" and res.reason == 0:
            return FINAL_ABORTED
        return FINAL_REJECTED
    return None


def carry_partial_reason(op: PendingAssetOp, res: AssetCallResult) -> AssetCallResult:
    """保持 27007 部分发放证据在重排时的粘性，绝不被后续 0/RETRY 原因覆盖。"""
    if (
        (op.last_reason == ERR_ASSET_PARTIAL_APPLIED or res.partial)
        and res.reason != ERR_ASSET_PARTIAL_APPLIED
    ):
        return replace(res, reason=ERR_ASSET_PARTIAL_APPLIED)
    return res


def next_backoff_ms(
    now_ms: int,
    attempts: int,
    base_ms: int,
    max_ms: int,
    *,
    rnd: Callable[[], float] | None = None,
) -> int:
    """指数退避 + ±20% 抖动（与 Go assetop.NextAttemptMs 一致）。"""
    shift = min(max(attempts, 0), 16)
    delay = min(base_ms << shift, max_ms)
    jitter = 0.5 if rnd is None else min(max(rnd(), 0.0), 0.999999)
    return now_ms + int(delay * (0.8 + 0.4 * jitter))


class GrpcDataServiceLedgerReader:
    """经 data_service.DataService/GetPlayerAssetOpLedger 强读 SQL 已提交账本。"""

    def __init__(
        self,
        protocol: Protocol,
        call: Callable[..., Awaitable[bytes]],
        *,
        timeout_sec: float = 0.3,
    ) -> None:
        self._protocol = protocol
        self._call = call
        self._timeout_sec = timeout_sec

    async def read_sql_committed_ledger(self, player_id: int) -> Message | None:
        req = self._protocol.message("data_service.GetPlayerAssetOpLedgerRequest", player_id=player_id)
        raw = await self._call(
            _LEDGER_PATH,
            req.SerializeToString(deterministic=True),
            timeout=self._timeout_sec,
        )
        resp = self._protocol.parse("data_service.GetPlayerAssetOpLedgerResponse", raw)
        if not resp.found:
            return None
        return resp.ledger


class RedisEtcdSceneResolver:
    """生产场景定位器：读 Redis ``player:{id}:location`` -> 查 etcd Scene 节点 -> 复用 GrpcClient。"""

    def __init__(self, protocol: Protocol, redis_client, directory, *, call_timeout_sec: float = 0.8) -> None:
        self._protocol = protocol
        self._redis = redis_client
        self._directory = directory
        self._call_timeout_sec = call_timeout_sec
        self._clients: dict[str, GrpcClient] = {}

    async def resolve(self, player_id: int) -> tuple[SceneTarget | None, bool]:
        raw = await self._redis.get(f"player:{{{player_id}}}:location")
        if not raw:
            return None, True
        loc = self._protocol.parse("storage.PlayerLocation", bytes(raw))
        if not loc.node_id:
            return None, False
        try:
            node_id = int(loc.node_id)
        except ValueError:
            return None, False
        info = await self._directory.lookup(SCENE, int(loc.zone_id), node_id)
        if info is None or not info.grpc_endpoint.ip or not 0 < info.grpc_endpoint.port < 65536:
            return None, False
        ip = info.grpc_endpoint.ip
        endpoint = f"[{ip}]:{info.grpc_endpoint.port}" if ":" in ip and not ip.startswith("[") else f"{ip}:{info.grpc_endpoint.port}"
        client = self._clients.get(endpoint)
        if client is None:
            client = self._clients[endpoint] = GrpcClient(endpoint, timeout=self._call_timeout_sec, enable_retries=False)
        return SceneTarget(endpoint=endpoint, call=client.call), False

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        if clients:
            await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


class AssetOpCaller:
    """负责签名、调用 SceneNodeGrpc.AssetCredit 及未落盘短重查。"""

    def __init__(
        self,
        protocol: Protocol,
        resolver: SceneResolver,
        secret: bytes,
        *,
        caller: str = ASSET_OP_CALLER,
        call_timeout_sec: float = 0.8,
        requery_delays_sec: Sequence[float] = DEFAULT_REQUERY_DELAYS_SEC,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        if len(secret) < 32:
            raise ValueError("MMORPG_ASSET_OP_SECRET_MAIL must be at least 32 bytes")
        self._protocol = protocol
        self._resolver = resolver
        self._secret = secret
        self._caller = caller
        self._call_timeout_sec = call_timeout_sec
        self._requery_delays = tuple(requery_delays_sec)
        self._clock_ms = clock_ms

    async def do_credit(self, op: PendingAssetOp) -> tuple[AssetCallResult, BaseException | None]:
        try:
            target, absent = await self._resolver.resolve(op.player_id)
        except Exception as exc:
            return AssetCallResult(), exc
        if target is None:
            return AssetCallResult(outcome=int(Outcome.NOT_HERE), local=True, absent=absent), None

        res, err = await self._invoke(target, op)
        if err is not None:
            return AssetCallResult(), err

        if res.outcome == int(Outcome.NOT_HERE):
            try:
                next_target, _ = await self._resolver.resolve(op.player_id)
            except Exception:
                next_target = None
            if next_target is not None and next_target.endpoint != target.endpoint:
                res2, err2 = await self._invoke(next_target, op)
                if err2 is not None:
                    return AssetCallResult(), err2
                target, res = next_target, res2

        if not res.is_scene_terminal or res.durable:
            return res, None
        return await self._requery(target, op, res)

    async def _invoke(self, target: SceneTarget, op: PendingAssetOp) -> tuple[AssetCallResult, BaseException | None]:
        req = build_asset_op_request(self._protocol, op, caller=self._caller)
        sign_asset_op("credit", req, self._caller, self._secret, self._clock_ms())
        try:
            raw = await target.call(_ASSET_CREDIT_PATH, req.SerializeToString(), timeout=self._call_timeout_sec)
        except Exception as exc:
            return AssetCallResult(), exc
        resp = self._protocol.parse("AssetOpResponse", raw)
        reason = int(resp.reason.id)
        if resp.partial and reason == 0:
            reason = ERR_ASSET_PARTIAL_APPLIED
        return (
            AssetCallResult(
                outcome=int(resp.outcome),
                reason=reason,
                durable=bool(resp.durable),
                partial=bool(resp.partial),
            ),
            None,
        )

    async def _requery(
        self, target: SceneTarget, op: PendingAssetOp, last: AssetCallResult
    ) -> tuple[AssetCallResult, BaseException | None]:
        for delay in self._requery_delays:
            if delay > 0:
                await asyncio.sleep(delay)
            res, err = await self._invoke(target, op)
            if err is not None or not res.is_scene_terminal:
                return last, None
            if res.outcome != last.outcome:
                return res, AssetOpOutcomeFlipError(
                    f"scene outcome flipped op_id={op.op_id} seq={op.seq} {last.outcome} -> {res.outcome}"
                )
            last = res
            if res.durable:
                return res, None
        return last, None


class AssetDeliveryLoop:
    """mail 附件重投循环：到期扫描 -> 主键 CAS 领取 -> Scene AssetCredit -> SQL 账本证明 -> 终结/重排。"""

    def __init__(
        self,
        protocol: Protocol,
        store: MailStoreCore,
        caller: AssetOpCaller,
        ledger_reader: SqlCommittedLedgerReader,
        *,
        interval_sec: float = 2.0,
        batch: int = 100,
        workers: int = 8,
        lease_ms: int = 10_000,
        base_backoff_ms: int = 1_000,
        max_backoff_ms: int = 60_000,
        await_durable_delay_ms: int = 500,
        poison_delay_ms: int = 3_600_000,
        ledger_read_min_attempts: int = 3,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        rnd: Callable[[], float] | None = None,
    ) -> None:
        if ledger_reader is None:
            raise ValueError("AssetDeliveryLoop requires a SqlCommittedLedgerReader")
        self._protocol = protocol
        self._store = store
        self._caller = caller
        self._ledger_reader = ledger_reader
        self._interval_sec = interval_sec
        self._batch = batch
        self._workers = workers
        self._lease_ms = lease_ms
        self._base_backoff_ms = base_backoff_ms
        self._max_backoff_ms = max_backoff_ms
        self._await_durable_delay_ms = await_durable_delay_ms
        self._poison_delay_ms = poison_delay_ms
        self._ledger_read_min_attempts = ledger_read_min_attempts
        self._clock_ms = clock_ms
        self._rnd = rnd if rnd is not None else random.random
        self._slots = asyncio.Semaphore(workers)

    async def run(self, *, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("mail asset delivery tick failed", exc_info=True)
            if stop_event is None:
                await asyncio.sleep(self._interval_sec)
            else:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self._interval_sec)
                except asyncio.TimeoutError:
                    pass

    async def tick(self, *, owned_shards: frozenset[int] | None = None) -> int:
        now = self._clock_ms()
        try:
            op_ids = await self._store.list_due_ops(now, self._batch, owned_shards=owned_shards)
        except Exception:
            logger.error("mail asset loop failed to list due ops", exc_info=True)
            return 0
        if not op_ids:
            return 0

        async def handle(oid: int) -> bool:
            async with self._slots:
                return await self.claim_and_process(oid)

        results = await asyncio.gather(*(handle(oid) for oid in op_ids), return_exceptions=False)
        return sum(1 for done in results if done)

    async def claim_and_process(self, op_id: int) -> bool:
        now = self._clock_ms()
        token = (secrets.randbits(64) | 1)
        try:
            op = await self._store.claim_op(
                op_id,
                now,
                now + self._lease_ms,
                now + self._poison_delay_ms,
                token,
            )
        except AssetOpPoisonRowError:
            logger.error("mail asset op poison payload postponed op_id=%d", op_id)
            return False
        except Exception:
            logger.error("mail asset op claim failed op_id=%d", op_id, exc_info=True)
            return False
        if op is None:
            return False
        await self.process_one(op)
        return True

    async def process_one(self, op: PendingAssetOp) -> AssetCallResult:
        now = self._clock_ms()
        if op.stream != ASSET_STREAM_SYSTEM_CREDIT:
            logger.error("mail asset op invalid stream op_id=%d stream=%d", op.op_id, op.stream)
            await self._reschedule(op, now + self._max_backoff_ms, AssetCallResult(), now)
            return AssetCallResult()

        res, err = await self._caller.do_credit(op)

        if err is None and res.local and op.attempts >= self._ledger_read_min_attempts:
            res = await self._try_offline_ledger(op, res)

        # 终结前一律强制核对 SQL 已提交账本，不以 Scene 在线回包直接入账。
        if err is None and not res.sql_committed and res.is_scene_terminal:
            res, err = await self._verify_sql_commit(op, res)

        action = decide_action(res, err)
        if action == LoopAction.FINALIZE:
            final_kind = determine_final_status("credit", res, op)
            if final_kind is None:
                logger.error("mail asset op could not determine final status op_id=%d", op.op_id)
                await self._reschedule(op, now + self._max_backoff_ms, res, now)
                return res
            finalized = await self._store.finalize_op(
                op,
                final_kind,
                res.outcome,
                res.reason,
                partial=(final_kind == FINAL_APPLIED_PARTIAL or res.partial),
                sql_committed=res.sql_committed,
                now_ms=now,
            )
            if finalized and final_kind == FINAL_APPLIED_PARTIAL:
                logger.error(
                    "mail partial credit finalized to MANUAL op_id=%d player=%d seq=%d",
                    op.op_id,
                    op.player_id,
                    op.seq,
                )
            return res

        if action == LoopAction.AWAIT_DURABLE:
            await self._reschedule(op, now + self._await_durable_delay_ms, res, now)
            return res

        if action == LoopAction.RETRY:
            next_ms = next_backoff_ms(
                now, op.attempts, self._base_backoff_ms, self._max_backoff_ms, rnd=self._rnd
            )
            stored = AssetCallResult(outcome=int(Outcome.RETRY)) if err is not None else res
            await self._reschedule(op, next_ms, stored, now)
            return res

        # LoopAction.ALERT（UNKNOWN 或结局翻转）：绝不终结，长退避重排
        logger.error(
            "mail asset op alert, staying pending op_id=%d seq=%d outcome=%d reason=%d err=%s",
            op.op_id,
            op.seq,
            res.outcome,
            res.reason,
            err,
        )
        await self._reschedule(op, now + self._max_backoff_ms, res, now)
        return res

    async def _try_offline_ledger(self, op: PendingAssetOp, res: AssetCallResult) -> AssetCallResult:
        try:
            ledger = await self._ledger_reader.read_sql_committed_ledger(op.player_id)
        except Exception:
            logger.error("mail offline ledger read failed op_id=%d", op.op_id, exc_info=True)
            return res
        if ledger is None:
            return res
        verified = verify_ledger_receipt(self._protocol, ledger, op)
        if verified is None:
            return res
        outcome, partial, reason = verified
        logger.info(
            "mail offline ledger outcome recovered op_id=%d seq=%d outcome=%d partial=%s",
            op.op_id,
            op.seq,
            outcome,
            partial,
        )
        return AssetCallResult(
            outcome=outcome,
            reason=reason,
            durable=True,
            sql_committed=True,
            partial=partial,
        )

    async def _verify_sql_commit(
        self, op: PendingAssetOp, res: AssetCallResult
    ) -> tuple[AssetCallResult, BaseException | None]:
        unproved = replace(res, durable=False, sql_committed=False)
        try:
            ledger = await self._ledger_reader.read_sql_committed_ledger(op.player_id)
        except Exception:
            logger.error("mail SQL commit ledger verification failed op_id=%d", op.op_id, exc_info=True)
            return unproved, None
        if ledger is None:
            return unproved, None
        verified = verify_ledger_receipt(self._protocol, ledger, op)
        if verified is None:
            return unproved, None
        outcome, ledger_partial, ledger_reason = verified
        if outcome != res.outcome:
            return unproved, AssetOpOutcomeFlipError(
                f"SQL ledger outcome differs from scene op_id={op.op_id} scene={res.outcome} ledger={outcome}"
            )
        combined_partial = ledger_partial or res.partial
        combined_reason = ledger_reason or res.reason
        if combined_partial and combined_reason == 0:
            combined_reason = ERR_ASSET_PARTIAL_APPLIED
        return (
            AssetCallResult(
                outcome=outcome,
                reason=combined_reason,
                durable=True,
                sql_committed=True,
                partial=combined_partial,
            ),
            None,
        )

    async def _reschedule(
        self, op: PendingAssetOp, next_attempt_ms: int, res: AssetCallResult, now_ms: int
    ) -> None:
        carried = carry_partial_reason(op, res)
        try:
            await self._store.reschedule_op(
                op,
                next_attempt_ms,
                carried.outcome,
                carried.reason,
                carried.durable,
                carried.local,
                now_ms,
            )
        except AssetOpLeaseLostError:
            logger.info("mail asset op lease taken over op_id=%d seq=%d", op.op_id, op.seq)
