"""持久客户端幂等键，编码与 C++ ClientRequestLedgerComp 相同。"""

import hashlib

from .asset_inventory import INVALID, UNAVAILABLE, clone
from .asset_ledger import UINT64_MAX


def check(ledger, request, kind: int):
    """返回 (tip, fingerprint, prior)；prior 非空时直接重放。"""
    key = request.client_request_id
    if not key:
        return 0, b"", None
    next_seq = ledger.next_seq or 1
    keys, sequences = set(), set()
    if len(ledger.records) > 256:
        return UNAVAILABLE, b"", None
    prior = None
    for record in ledger.records:
        if (
            not record.key or record.key in keys or len(record.fingerprint) != 16
            or not 1 <= record.seq < next_seq or record.seq in sequences or not record.kind
        ):
            return UNAVAILABLE, b"", None
        keys.add(record.key)
        sequences.add(record.seq)
        if record.key == key:
            prior = record
    stable = clone(request)
    stable.ClearField("client_request_id")
    fingerprint = hashlib.sha256(stable.SerializeToString(deterministic=True)).digest()[:16]
    if prior is not None and (prior.kind != kind or prior.fingerprint != fingerprint):
        return INVALID, b"", None
    if prior is None and next_seq == UINT64_MAX:
        return UNAVAILABLE, b"", None
    return 0, fingerprint, prior


def remember(ledger, key: int, kind: int, fingerprint: bytes, **result) -> None:
    if not key:
        return
    ledger.records.sort(key=lambda record: record.seq)
    if len(ledger.records) >= 32:
        del ledger.records[:len(ledger.records) - 31]
    sequence = ledger.next_seq or 1
    ledger.records.add(key=key, kind=kind, fingerprint=fingerprint, seq=sequence, **result)
    ledger.next_seq = sequence + 1
