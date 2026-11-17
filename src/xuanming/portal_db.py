"""Shared MySQL value conversions."""


def as_bool(value):
    """Convert MySQL BIT(1) as well as integer/boolean SQL values."""
    return bool(int.from_bytes(value, "big")) if isinstance(value, bytes) else bool(value)
