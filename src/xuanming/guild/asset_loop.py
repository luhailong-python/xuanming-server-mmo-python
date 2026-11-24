"""guild 通用资产通道循环（GuildAssetOpLoop）：GUILD_DEBIT / GUILD_CREDIT / AssetAbortDebit 与 SQL 账本强制核验。"""

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
from .model import (
    ASSET_OP_CALLER,
    ASSET_STREAM_GUILD_CREDIT,
    ASSET_STREAM_GUILD_DEBIT,
    OP_STATUS_ABORTED,
    OP_STATUS_APPLIED,
    OP_STATUS_APPLIED_PARTIAL,
    OP_STATUS_REJECTED,
    PendingAssetOp,
)
from .store import GuildStoreCore

logger = logging.getLogger(__name__)

_RPC_PATHS = {
    "debit": "/scene_node.SceneNodeGrpc/AssetDebit",
    "credit": "/scene_node.SceneNodeGrpc/AssetCredit",
    "abort_debit": "/scene_node.SceneNodeGrpc/AssetAbortDebit",
}
_LEDGER_PATH = "/data_service.DataService/GetPlayerAssetOpLedger"

DEFAULT_REQUERY_DELAYS_SEC = (0.1, 0.2, 0.4)
DEFAULT_ABANDON_GRACE_MS = 60_000


class LoopAction(IntEnum):
    FINALIZE = 1
    AWAIT_DURABLE = 2
    RETRY = 3
    ALERT = 4


@dataclass(frozen=True, slots=True)
class AssetCallResult:
    """单次 Scene 资产 RPC 或 SQL 已提交账本核验结果。"""

    outcome: int = int(Outcome.UNKNOWN)
    reason: int = 0
    durable: bool = False
    sql_committed: bool = False
    partial: bool = False
    local: bool = False
    absent: bool = False
    player_released: bool = False

    @property
    def is_scene_terminal(self) -> bool:
        return self.outcome in (int(Outcome.APPLIED), int(Outcome.REJECTED))


@dataclass(frozen=True, slots=True)
class LedgerSnapshot:
    """data_service.GetPlayerAssetOpLedger 读取结果（含 player_released 永久释放标记）。"""

    ledger: Message | None
    player_released: bool = False


@dataclass(frozen=True, slots=True)
class SceneTarget:
    """已定位到的 Scene 节点目标。"""

    endpoint: str
    call: Callable[..., Awaitable[bytes]]


class SceneResolver(TypingProtocol):
    """按 player_id 解析目标 Scene 节点（返回 (None, True) 表示确认离线）。"""

    async def resolve(self, player_id: int) -> tuple[SceneTarget | None, bool]: ...


class SqlCommittedLedgerReader(TypingProtocol):
    """强读玩家 SQL 已提交资产账本（data_service.GetPlayerAssetOpLedger）。"""

    async def read_sql_committed_ledger(self, player_id: int) -> LedgerSnapshot: ...


def sign_asset_op(rpc: str, request: Message, caller: str, secret: bytes, now_ms: int) -> None:
    """按 mmorpg-asset-op/v1 跨语言规范就地写入 AssetOpAuth 签名。"""
    if not caller or len(secret) < 32:
        raise ValueError("guild asset op signer requires caller and >=32-byte secret")
    request.auth.caller = caller
    request.auth.timestamp_ms = now_ms
    request.auth.signature_hex = hmac.digest(secret, asset_canonical(rpc, request), "sha256").hex()


def build_asset_op_request(protocol: Protocol, op: PendingAssetOp, *, caller: str = ASSET_OP_CALLER) -> Message:
    """将待办资产行组装为 AssetOpRequest（未签名，填入 caller 供收据摘要校验）。"""
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


def _snapshot_has_instances(snapshot: Message | None) -> bool:
    if snapshot is None:
        return False
    return len(snapshot.items) > 0 or (snapshot.HasField("pet") and int(snapshot.pet.pet_id) != 0)


def verify_ledger_receipt(
    protocol: Protocol,
    ledger: Message,
    op: PendingAssetOp,
) -> tuple[int, bool, int] | None:
    """核对 SQL 已提交账本中的位图、收据身份与非托管约束；未满足返回 None。"""
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
    receipt_snap = receipt.snapshot if receipt.HasField("snapshot") else None
    if _snapshot_has_instances(receipt_snap):
        # 帮会通道仅扣货币/发可堆叠道具，不应出现实例托管快照
        return None

    partial = op.seq in stream.partial_seqs
    if outcome == Outcome.APPLIED:
        reason = ERR_ASSET_PARTIAL_APPLIED if partial else 0
    else:
        reason = next((int(row.reason_tip_id) for row in stream.rejections if int(row.seq) == op.seq), 0)
    return int(outcome), partial, reason


def decide_action(res: AssetCallResult, error: BaseException | None) -> LoopAction:
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


def determine_final_status(rpc: str, res: AssetCallResult, op: PendingAssetOp) -> int | None:
    """将终结结局映射为 GuildAssetOpStatus（含粘性 27007 与 abort_debit -> ABORTED）。"""
    if res.outcome == int(Outcome.APPLIED):
        if (
            res.partial
            or op.last_reason == ERR_ASSET_PARTIAL_APPLIED
            or res.reason == ERR_ASSET_PARTIAL_APPLIED
        ):
            return OP_STATUS_APPLIED_PARTIAL
        return OP_STATUS_APPLIED
    if res.outcome == int(Outcome.REJECTED):
        if rpc == "abort_debit" and res.reason == 0:
            return OP_STATUS_ABORTED
        return OP_STATUS_REJECTED
    return None


def carry_partial_reason(op: PendingAssetOp, res: AssetCallResult) -> AssetCallResult:
    """保持 27007 部分发放证据在重排时的粘性，绝不被后续 0/RETRY 覆盖。"""
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

    async def read_sql_committed_ledger(self, player_id: int) -> LedgerSnapshot:
        req = self._protocol.message("data_service.GetPlayerAssetOpLedgerRequest", player_id=player_id)
        raw = await self._call(
            _LEDGER_PATH,
            req.SerializeToString(deterministic=True),
            timeout=self._timeout_sec,
        )
        resp = self._protocol.parse("data_service.GetPlayerAssetOpLedgerResponse", raw)
        if not resp.found:
            return LedgerSnapshot(ledger=None, player_released=bool(resp.player_released))
        return LedgerSnapshot(ledger=resp.ledger, player_released=bool(resp.player_released))


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
        endpoint = (
            f"[{ip}]:{info.grpc_endpoint.port}"
            if ":" in ip and not ip.startswith("[")
            else f"{ip}:{info.grpc_endpoint.port}"
        )
        client = self._clients.get(endpoint)
        if client is None:
            client = self._clients[endpoint] = GrpcClient(
                endpoint, timeout=self._call_timeout_sec, enable_retries=False
            )
        return SceneTarget(endpoint=endpoint, call=client.call), False

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        if clients:
            await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)


class GuildAssetOpCaller:
    """负责签名、调用 SceneNodeGrpc.Asset{Debit,Credit,AbortDebit} 及未落盘短重查。"""

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
            raise ValueError("MMORPG_ASSET_OP_SECRET_GUILD must be at least 32 bytes")
        self._protocol = protocol
        self._resolver = resolver
        self._secret = secret
        self._caller = caller
        self._call_timeout_sec = call_timeout_sec
        self._requery_delays = tuple(requery_delays_sec)
        self._clock_ms = clock_ms

    async def do(self, rpc: str, op: PendingAssetOp) -> tuple[AssetCallResult, BaseException | None]:
        if rpc not in _RPC_PATHS:
            return AssetCallResult(), ValueError(f"unsupported guild asset op rpc: {rpc!r}")
        try:
            target, absent = await self._resolver.resolve(op.player_id)
        except Exception as exc:
            return AssetCallResult(), exc
        if target is None:
            return AssetCallResult(outcome=int(Outcome.NOT_HERE), local=True, absent=absent), None

        res, err = await self._invoke(target, rpc, op)
        if err is not None:
            return AssetCallResult(), err

        if res.outcome == int(Outcome.NOT_HERE):
            try:
                next_target, _ = await self._resolver.resolve(op.player_id)
            except Exception:
                next_target = None
            if next_target is not None and next_target.endpoint != target.endpoint:
                res2, err2 = await self._invoke(next_target, rpc, op)
                if err2 is not None:
                    return AssetCallResult(), err2
                target, res = next_target, res2

        if not res.is_scene_terminal or res.durable:
            return res, None
        return await self._requery(target, rpc, op, res)

    async def _invoke(
        self, target: SceneTarget, rpc: str, op: PendingAssetOp
    ) -> tuple[AssetCallResult, BaseException | None]:
        req = build_asset_op_request(self._protocol, op, caller=self._caller)
        sign_asset_op(rpc, req, self._caller, self._secret, self._clock_ms())
        try:
            raw = await target.call(_RPC_PATHS[rpc], req.SerializeToString(), timeout=self._call_timeout_sec)
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
        self, target: SceneTarget, rpc: str, op: PendingAssetOp, last: AssetCallResult
    ) -> tuple[AssetCallResult, BaseException | None]:
        for delay in self._requery_delays:
            if delay > 0:
                await asyncio.sleep(delay)
            res, err = await self._invoke(target, rpc, op)
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


class GuildAssetOpLoop:
    """guild 资产通道重投循环（2s 间隔，两段 ListDue，强制 SQL 已提交账本门禁）。"""

    def __init__(
        self,
        protocol: Protocol,
        store: GuildStoreCore,
        caller: GuildAssetOpCaller,
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
        abandon_grace_ms: int = DEFAULT_ABANDON_GRACE_MS,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        rnd: Callable[[], float] | None = None,
    ) -> None:
        if ledger_reader is None:
            raise ValueError("GuildAssetOpLoop requires a SqlCommittedLedgerReader")
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
        self._abandon_grace_ms = abandon_grace_ms
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
                logger.error("guild asset op loop tick failed", exc_info=True)
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
            op_ids = await self._store.list_due_asset_ops(now, self._batch, owned_shards=owned_shards)
        except Exception:
            logger.error("guild asset loop failed to list due ops", exc_info=True)
            return 0
        if not op_ids:
            return 0

        async def _handle(oid: int) -> bool:
            async with self._slots:
                return await self.claim_and_process(oid)

        results = await asyncio.gather(*(_handle(oid) for oid in op_ids), return_exceptions=False)
        return sum(1 for done in results if done)

    async def deliver_now(self, op_id: int) -> AssetCallResult | None:
        """同步首投快路径：按租约认领单条资产指令并尝试即时投递与终结。"""
        now = self._clock_ms()
        token = secrets.randbits(64) | 1
        try:
            op = await self._store.claim_asset_op(
                op_id,
                now,
                now + self._lease_ms,
                now + self._poison_delay_ms,
                token,
            )
        except AssetOpPoisonRowError:
            logger.error("guild asset op poison payload postponed op_id=%d", op_id)
            return None
        except Exception:
            logger.error("guild asset op sync claim failed op_id=%d", op_id, exc_info=True)
            return None
        if op is None:
            return None
        return await self.process_one(op)

    async def claim_and_process(self, op_id: int) -> bool:
        return (await self.deliver_now(op_id)) is not None

    def _select_rpc(self, op: PendingAssetOp, now_ms: int) -> str | None:
        if op.stream not in (ASSET_STREAM_GUILD_DEBIT, ASSET_STREAM_GUILD_CREDIT):
            return None
        if op.stream == ASSET_STREAM_GUILD_DEBIT and op.deadline_ms > 0 and now_ms >= op.deadline_ms:
            return "abort_debit"
        return "debit" if op.stream == ASSET_STREAM_GUILD_DEBIT else "credit"

    async def process_one(self, op: PendingAssetOp) -> AssetCallResult:
        now = self._clock_ms()
        rpc = self._select_rpc(op, now)
        if rpc is None:
            logger.error("guild asset op invalid stream op_id=%d stream=%d", op.op_id, op.stream)
            await self._reschedule(op, now + self._max_backoff_ms, AssetCallResult(), now)
            return AssetCallResult()

        res, err = await self._caller.do(rpc, op)

        if err is None and res.local and op.attempts >= self._ledger_read_min_attempts:
            res = await self._try_offline_ledger(op, res, now)
            if res.player_released and res.local and res.absent and op.lease_token != 0:
                if op.last_send_ms == 0 or (now - op.last_send_ms) >= self._abandon_grace_ms:
                    try:
                        await self._store.abandon_asset_op(op, now)
                        logger.info(
                            "guild asset op abandoned for released player op_id=%d player=%d",
                            op.op_id,
                            op.player_id,
                        )
                        return res
                    except AssetOpLeaseLostError:
                        logger.info("guild asset op lease taken over during abandon op_id=%d", op.op_id)
                        return res

        # 终结前一律强制核对 SQL 已提交账本，不以 Scene 在线回包直接入账。
        if err is None and not res.sql_committed and res.is_scene_terminal:
            res, err = await self._verify_sql_commit(op, res)

        action = decide_action(res, err)
        if action == LoopAction.FINALIZE:
            final_status = determine_final_status(rpc, res, op)
            if final_status is None:
                logger.error("guild asset op could not determine final status op_id=%d", op.op_id)
                await self._reschedule(op, now + self._max_backoff_ms, res, now)
                return res
            finalized = await self._store.finalize_asset_op(
                op,
                final_status,
                res.outcome,
                res.reason,
                now,
            )
            if finalized and final_status == OP_STATUS_APPLIED_PARTIAL:
                logger.error(
                    "guild partial asset op finalized to APPLIED_PARTIAL op_id=%d player=%d seq=%d",
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

        logger.error(
            "guild asset op alert, staying pending op_id=%d seq=%d outcome=%d reason=%d err=%s",
            op.op_id,
            op.seq,
            res.outcome,
            res.reason,
            err,
        )
        await self._reschedule(op, now + self._max_backoff_ms, res, now)
        return res

    async def _try_offline_ledger(
        self, op: PendingAssetOp, res: AssetCallResult, now_ms: int
    ) -> AssetCallResult:
        try:
            snap = await self._ledger_reader.read_sql_committed_ledger(op.player_id)
        except Exception:
            logger.error("guild offline ledger read failed op_id=%d", op.op_id, exc_info=True)
            return res
        if snap.ledger is not None:
            verified = verify_ledger_receipt(self._protocol, snap.ledger, op)
            if verified is not None:
                outcome, partial, reason = verified
                logger.info(
                    "guild offline ledger outcome recovered op_id=%d seq=%d outcome=%d partial=%s",
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
        if snap.player_released:
            return replace(res, player_released=True)
        return res

    async def _verify_sql_commit(
        self, op: PendingAssetOp, res: AssetCallResult
    ) -> tuple[AssetCallResult, BaseException | None]:
        unproved = replace(res, durable=False, sql_committed=False)
        try:
            snap = await self._ledger_reader.read_sql_committed_ledger(op.player_id)
        except Exception:
            logger.error("guild SQL commit ledger verification failed op_id=%d", op.op_id, exc_info=True)
            return unproved, None
        if snap.ledger is None:
            return unproved, None
        verified = verify_ledger_receipt(self._protocol, snap.ledger, op)
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
            await self._store.reschedule_asset_op(
                op,
                next_attempt_ms,
                carried.outcome,
                carried.reason,
                carried.durable,
                carried.local,
                now_ms,
            )
        except AssetOpLeaseLostError:
            logger.info("guild asset op lease taken over op_id=%d seq=%d", op.op_id, op.seq)
