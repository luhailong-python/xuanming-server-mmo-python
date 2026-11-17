import base64
import hashlib

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from xuanming.grpc_runtime import gate_signing_key, session_metadata


SEED = bytes(range(32))
PUBLIC = Ed25519PrivateKey.from_private_bytes(SEED).public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
CONFIG = {
    "MMORPG_INTERNAL_AUTH_SIGNING_KEY": SEED.hex(),
    "MMORPG_INTERNAL_AUTH_PUBLIC_KEYS": PUBLIC.hex(),
}


def test_metadata_binds_subject_and_full_method(monkeypatch):
    monkeypatch.setattr("xuanming.grpc_runtime.time.time_ns", lambda: 1700000000123000000)
    monkeypatch.setattr("xuanming.grpc_runtime.secrets.token_hex", lambda size: "ab" * size)
    subject = b"\x00\xffsession"
    method = "/loginpb.ClientPlayerLogin/Login"
    metadata = dict(session_metadata(subject, method, gate_signing_key(CONFIG)))
    canonical = "\n".join((
        "mmorpg-gate-identity/v2", "gate", method, hashlib.sha256(subject).hexdigest(),
        "1700000000123", "ab" * 16,
    )).encode()
    assert base64.b64decode(metadata["x-session-detail-bin"]) == subject
    assert metadata["x-caller-id"] == "gate"
    assert metadata["x-caller-ts"] == "1700000000123"
    assert metadata["x-caller-nonce"] == "ab" * 16
    signature = metadata["x-caller-sig"]
    assert len(signature) == 128 and signature == signature.lower()
    key = Ed25519PrivateKey.from_private_bytes(SEED).public_key()
    key.verify(bytes.fromhex(signature), canonical)
    with pytest.raises(InvalidSignature):
        key.verify(bytes.fromhex(signature), canonical.replace(b"/Login", b"/Logout"))
    with pytest.raises(ValueError):
        session_metadata(subject, "Login", SEED)


def test_key_configuration_accepts_rotation_and_uppercase():
    other = Ed25519PrivateKey.from_private_bytes(b"x" * 32).public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    config = dict(CONFIG, MMORPG_INTERNAL_AUTH_SIGNING_KEY=SEED.hex().upper(),
                  MMORPG_INTERNAL_AUTH_PUBLIC_KEYS=other.hex() + "," + PUBLIC.hex().upper())
    assert gate_signing_key(config).public_key().public_bytes(Encoding.Raw, PublicFormat.Raw) == PUBLIC


@pytest.mark.parametrize("field,value", [
    ("MMORPG_INTERNAL_AUTH_SIGNING_KEY", ""),
    ("MMORPG_INTERNAL_AUTH_SIGNING_KEY", SEED.hex() + " "),
    ("MMORPG_INTERNAL_AUTH_SIGNING_KEY", "g" * 64),
    ("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", ""),
    ("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", PUBLIC.hex() + ","),
    ("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", " " + PUBLIC.hex()),
    ("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", PUBLIC.hex() + "," + PUBLIC.hex().upper()),
    ("MMORPG_INTERNAL_AUTH_PUBLIC_KEYS", "00" * 32),
])
def test_invalid_key_configuration_rejected_without_disclosing_value(field, value):
    config = dict(CONFIG, **{field: value})
    with pytest.raises(ValueError) as error:
        gate_signing_key(config)
    assert field in str(error.value)
    assert not value or value not in str(error.value)
