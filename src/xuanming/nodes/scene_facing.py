"""Shared horizontal yaw convention for Scene actors."""

from __future__ import annotations

import math


def yaw_degrees_toward(dx: float, dy: float) -> float | None:
    """Return the server's z-axis yaw, preserving orientation for vertical directions."""
    if dx * dx + dy * dy <= 1e-12:
        return None
    return math.degrees(math.atan2(dy, dx))


def rotation_toward(dx: float, dy: float) -> tuple[float, float, float] | None:
    yaw = yaw_degrees_toward(dx, dy)
    return (0.0, 0.0, yaw) if yaw is not None else None
