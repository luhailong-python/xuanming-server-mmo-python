"""与 C++ / Go 共享的资产签名、回执和 1024 位幂等窗口。"""

from __future__ import annotations

import hashlib
import hmac
from enum import IntEnum

WINDOW = 1024
WORDS = 16
UINT64_MAX = (1 << 64) - 1
INT64_MAX = (1 << 63) - 1
INSTANCE_LIMIT = 16 * 1024
SNAPSHOT_LIMIT = 64 * 1024
RECEIPT_LIMIT = 256 * 1024
REJECTION_LIMIT = 64
CALLERS = {1: "guild", 2: "guild", 3: "trade", 4: "trade", 5: "mail", 6: "data_service"}


class Outcome(IntEnum):
    UNKNOWN = 0
    APPLIED = 1
    REJECTED = 2
    RETRY = 3
    NOT_HERE = 4


def canonical(rpc: str, request, *, timestamp: int | None = None) -> bytes:
    bundle = request.bundle
    currency = ",".join(f"{value.currency_type}:{value.amount}" for value in bundle.currencies)
    items = ",".join(f"{item.config_id}:{item.count}" for item in bundle.items)
    guids = ",".join(map(str, bundle.item_uuids))
    payload = f"c={currency};i={items};u={guids};p={bundle.pet_id}"
    if bundle.item_instances or bundle.HasField("pet_instance"):
        instances = "".join(
            f"{item.SerializeToString(deterministic=True).hex()},"
            for item in bundle.item_instances
        )
        pet = (
            bundle.pet_instance.SerializeToString(deterministic=True).hex()
            if bundle.HasField("pet_instance") else ""
        )
        payload += f";v2={instances};{pet}"
    fields = (
        "mmorpg-asset-op/v1", request.auth.caller, rpc, request.player_id, request.stream,
        request.stream_epoch, request.seq, request.correlation_id, request.tx_type,
        payload, request.auth.timestamp_ms if timestamp is None else timestamp,
    )
    return "\n".join(map(str, fields)).encode("utf-8")


def authorized(rpc: str, request, secrets: dict[str, bytes], now_ms: int) -> bool:
    caller = CALLERS.get(request.stream)
    if caller is None or request.auth.caller != caller:
        return False
    secret = secrets.get(caller, b"")
    timestamp = request.auth.timestamp_ms
    if len(secret) < 32 or not 0 <= timestamp <= INT64_MAX or abs(now_ms - timestamp) > 300_000:
        return False
    signature = request.auth.signature_hex.encode("utf-8")
    expected = hmac.digest(secret, canonical(rpc, request), "sha256").hex().encode("ascii")
    return hmac.compare_digest(signature, expected)


def find_stream(ledger, stream: int):
    return next((row for row in ledger.streams if row.stream == stream), None)


def find_receipt(stream, seq: int):
    return next((row for row in stream.receipts if row.seq == seq), None)


def _identity(request) -> bytes:
    return hashlib.sha256(canonical("debit", request, timestamp=0)).digest()


def receipt_matches(receipt, request) -> bool:
    return (
        receipt is not None and receipt.correlation_id == request.correlation_id
        and receipt.request_digest == _identity(request)
    )


def _test(words, bit: int) -> bool:
    return bool(words[bit // 64] & (1 << (bit % 64)))


def classify(stream, epoch: int, seq: int) -> Outcome | None:
    """None 表示可记一条新结局，其余是已知结果或不可恢复的 UNKNOWN。"""
    old_epoch = stream.stream_epoch if stream is not None else 0
    if not epoch or not seq or epoch < old_epoch:
        return Outcome.UNKNOWN
    if epoch > old_epoch:
        return None if seq <= WINDOW else Outcome.UNKNOWN
    low = stream.watermark
    if seq <= low or low > UINT64_MAX - WINDOW or seq > stream.max_seq + WINDOW:
        return Outcome.UNKNOWN
    bit = seq - low - 1
    if bit >= WINDOW or not _test(stream.seen_bits, bit):
        return None
    return Outcome.APPLIED if _test(stream.applied_bits, bit) else Outcome.REJECTED


def valid(ledger) -> bool:
    previous_stream = 0
    total_bytes = 0
    for stream in ledger.streams:
        if (
            stream.stream <= previous_stream or stream.stream not in CALLERS or not stream.stream_epoch
            or len(stream.seen_bits) != WORDS or len(stream.applied_bits) != WORDS
            or stream.watermark > UINT64_MAX - WINDOW or stream.max_seq < stream.watermark
            or len(stream.rejections) > REJECTION_LIMIT
            or any(applied & ~seen for applied, seen in zip(stream.applied_bits, stream.seen_bits))
        ):
            return False
        previous_stream = stream.stream
        low = stream.watermark
        previous = low
        for rejection in stream.rejections:
            seq = rejection.seq
            if not previous < seq <= low + WINDOW:
                return False
            bit = seq - low - 1
            if not _test(stream.seen_bits, bit) or _test(stream.applied_bits, bit):
                return False
            previous = seq
        previous = low
        for seq in stream.partial_seqs:
            if not previous < seq <= low + WINDOW or not _test(stream.applied_bits, seq - low - 1):
                return False
            previous = seq
        previous = low
        for receipt in stream.receipts:
            seq = receipt.seq
            if not previous < seq <= low + WINDOW or len(receipt.request_digest) != 32:
                return False
            bit = seq - low - 1
            if not _test(stream.seen_bits, bit):
                return False
            if receipt.HasField("snapshot"):
                snapshot = receipt.snapshot
                size = snapshot.ByteSize()
                if (
                    not receipt.correlation_id or not _test(stream.applied_bits, bit)
                    or size > SNAPSHOT_LIMIT
                    or any(not 1 <= len(item.instance_proto) <= INSTANCE_LIMIT for item in snapshot.items)
                    or snapshot.HasField("pet") and not 1 <= len(snapshot.pet.instance_proto) <= INSTANCE_LIMIT
                ):
                    return False
                total_bytes += size
            previous = seq
    return total_bytes <= RECEIPT_LIMIT


def record(ledger, request, outcome: Outcome, reason: int, snapshot=None, *, partial=False):
    """返回已写入候选账本的回执。预算或前置失败时原账本保持不变。"""
    candidate = type(ledger)()
    candidate.CopyFrom(ledger)
    stream = find_stream(candidate, request.stream)
    if classify(stream, request.stream_epoch, request.seq) is not None:
        return None
    if stream is None:
        stream = candidate.streams.add(stream=request.stream)
    if stream.stream_epoch != request.stream_epoch:
        stream.Clear()
        stream.stream = request.stream
        stream.stream_epoch = request.stream_epoch
        stream.seen_bits.extend([0] * WORDS)
        stream.applied_bits.extend([0] * WORDS)
    seq = request.seq
    low = stream.watermark
    shift = max(0, seq - (low + WINDOW))
    if shift:
        low += shift
        stream.watermark = low
        for words in (stream.seen_bits, stream.applied_bits):
            value = sum(word << (64 * index) for index, word in enumerate(words)) >> shift
            words[:] = [(value >> (64 * index)) & UINT64_MAX for index in range(WORDS)]
        for rows in (stream.rejections, stream.receipts):
            del rows[:sum(row.seq <= low for row in rows)]
        del stream.partial_seqs[:sum(value <= low for value in stream.partial_seqs)]
    bit = seq - low - 1
    mask = 1 << (bit % 64)
    stream.seen_bits[bit // 64] |= mask
    if outcome == Outcome.APPLIED:
        stream.applied_bits[bit // 64] |= mask
        if partial:
            stream.partial_seqs.append(seq)
            stream.partial_seqs.sort()
    elif reason:
        stream.rejections.add(seq=seq, reason_tip_id=reason)
        stream.rejections.sort(key=lambda row: row.seq)
        del stream.rejections[:max(0, len(stream.rejections) - REJECTION_LIMIT)]
    stream.max_seq = max(stream.max_seq, seq)
    receipt = stream.receipts.add(seq=seq, correlation_id=request.correlation_id, request_digest=_identity(request))
    if snapshot is not None and (snapshot.items or snapshot.HasField("pet") or snapshot.recall_lines):
        receipt.snapshot.CopyFrom(snapshot)
    stream.receipts.sort(key=lambda row: row.seq)
    candidate.streams.sort(key=lambda row: row.stream)
    if not valid(candidate):
        return None
    ledger.CopyFrom(candidate)
    return find_receipt(find_stream(ledger, request.stream), seq)
