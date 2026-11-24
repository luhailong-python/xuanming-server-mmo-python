"""每端点三态熔断器（Closed / Open / HalfOpen），对齐 Go shared/circuitbreaker。"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(slots=True)
class _EndpointState:
    consecutive_failures: int = 0
    open_until_sec: float = 0.0
    probe_in_flight: bool = False


class EndpointCircuitBreaker:
    """按目标 gRPC 端点隔离故障；调用方取消仅释放探测槽而不计入下游故障。"""

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        open_duration_ms: int = 10000,
        clock=time.monotonic,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._open_duration_sec = open_duration_ms / 1000.0
        self._clock = clock
        self._states: dict[str, _EndpointState] = {}

    @property
    def enabled(self) -> bool:
        return self._failure_threshold > 0 and self._open_duration_sec > 0

    def allow(self, endpoint: str) -> bool:
        if not self.enabled:
            return True
        state = self._states.get(endpoint)
        if state is None:
            return True
        if state.open_until_sec <= 0.0:
            return True
        now = self._clock()
        if now < state.open_until_sec:
            return False
        if state.probe_in_flight:
            return False
        state.probe_in_flight = True
        return True

    def record_success(self, endpoint: str) -> None:
        if not self.enabled:
            return
        self._states.pop(endpoint, None)

    def record_failure(self, endpoint: str) -> None:
        if not self.enabled:
            return
        state = self._states.get(endpoint)
        if state is None:
            state = _EndpointState()
            self._states[endpoint] = state
        if state.probe_in_flight:
            state.probe_in_flight = False
            state.open_until_sec = self._clock() + self._open_duration_sec
            return
        state.consecutive_failures += 1
        if state.consecutive_failures >= self._failure_threshold:
            state.open_until_sec = self._clock() + self._open_duration_sec

    def release_probe(self, endpoint: str) -> None:
        """上游取消时仅释放 HalfOpen 探测占位，不触发熔断。"""
        state = self._states.get(endpoint)
        if state is not None:
            state.probe_in_flight = False

    def remove(self, endpoint: str) -> None:
        self._states.pop(endpoint, None)
