from __future__ import annotations

import time
import threading
from enum import Enum
from typing import Optional

from lightning.client._types import CircuitBreakerConfig


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig, telemetry=None):
        self._config = config
        self._telemetry = telemetry
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0.0
        self._half_open_permits: int = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                elapsed = time.monotonic() - self._last_failure_time
                if elapsed >= self._config.recovery_timeout:
                    self._transition_to_half_open()
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_permits < self._config.half_open_max_requests:
                    self._half_open_permits += 1
                    return True
                return False

            return False

    def on_success(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._transition_to_closed()
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def on_failure(self) -> None:
        with self._lock:
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to_open()
                return
            if self._state == CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self._config.failure_threshold:
                    self._transition_to_open()

    def _transition_to_open(self) -> None:
        previous = self._state
        self._state = CircuitState.OPEN
        self._half_open_permits = 0
        self._success_count = 0
        if self._telemetry and self._telemetry.on_circuit_breaker:
            self._telemetry.on_circuit_breaker("open", previous.value)

    def _transition_to_half_open(self) -> None:
        previous = self._state
        self._state = CircuitState.HALF_OPEN
        self._half_open_permits = 0
        self._success_count = 0
        if self._telemetry and self._telemetry.on_circuit_breaker:
            self._telemetry.on_circuit_breaker("half_open", previous.value)

    def _transition_to_closed(self) -> None:
        previous = self._state
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_permits = 0
        if self._telemetry and self._telemetry.on_circuit_breaker:
            self._telemetry.on_circuit_breaker("closed", previous.value)
