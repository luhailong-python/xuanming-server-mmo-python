"""Simulation-step load smoothing for adaptive AOI visibility capacity."""

from __future__ import annotations

import math


SMOOTHING = 0.05
DECISION_INTERVAL_SECONDS = 1.0
LOW_UTILIZATION = 0.5
HIGH_UTILIZATION = 0.9
PRESSURE_LEVELS = 10
HYSTERESIS_LEVELS = 0.75


class AoiLoadMonitor:
    def __init__(self) -> None:
        self.smoothed_utilization = 0.0
        self._seconds_since_decision = 0.0
        self.level = 0

    @property
    def pressure_factor(self) -> float:
        return self.level / PRESSURE_LEVELS

    def observe(self, step_seconds: float, fixed_delta_seconds: float) -> bool:
        """Return whether a decision interval elapsed and update the pressure level."""
        if (not math.isfinite(step_seconds) or not math.isfinite(fixed_delta_seconds)
                or fixed_delta_seconds <= 0):
            return False
        utilization = max(0.0, step_seconds / fixed_delta_seconds)
        self.smoothed_utilization += SMOOTHING * (utilization - self.smoothed_utilization)
        self._seconds_since_decision += fixed_delta_seconds
        if self._seconds_since_decision + 1e-9 < DECISION_INTERVAL_SECONDS:
            return False
        self._seconds_since_decision = 0.0

        raw = min(1.0, max(0.0, (self.smoothed_utilization - LOW_UTILIZATION)
                           / (HIGH_UTILIZATION - LOW_UTILIZATION)))
        scaled = raw * PRESSURE_LEVELS
        if abs(scaled - self.level) > HYSTERESIS_LEVELS:
            self.level = min(PRESSURE_LEVELS, max(0, math.floor(scaled + 0.5)))
        return True
