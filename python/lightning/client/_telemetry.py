from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger("lightning.client")


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, default=str)
    except (TypeError, ValueError):
        return str(obj)


@dataclass
class StructuredTelemetry:
    enabled: bool = True
    log_body: bool = False

    def on_request_start(self, request_id: str, method: str, path: str) -> None:
        if not self.enabled:
            return
        logger.info(
            "lightning.request.start",
            extra={
                "event": "request_start",
                "request_id": request_id,
                "method": method,
                "path": path,
            },
        )

    def on_request_end(
        self, request_id: str, method: str, path: str, status: int, duration: float
    ) -> None:
        if not self.enabled:
            return
        logger.info(
            "lightning.request.end",
            extra={
                "event": "request_end",
                "request_id": request_id,
                "method": method,
                "path": path,
                "status": status,
                "duration_ms": round(duration * 1000, 1),
            },
        )

    def on_error(
        self, request_id: str, method: str, path: str, error: Exception
    ) -> None:
        if not self.enabled:
            return
        logger.error(
            "lightning.request.error",
            extra={
                "event": "request_error",
                "request_id": request_id,
                "method": method,
                "path": path,
                "error": type(error).__name__,
                "error_message": str(error),
            },
        )

    def on_retry(
        self, request_id: str, method: str, path: str, attempt: int, delay: float
    ) -> None:
        if not self.enabled:
            return
        logger.warning(
            "lightning.request.retry",
            extra={
                "event": "request_retry",
                "request_id": request_id,
                "method": method,
                "path": path,
                "attempt": attempt,
                "delay_ms": round(delay * 1000, 1),
            },
        )

    def on_circuit_breaker(self, new_state: str, previous_state: str) -> None:
        if not self.enabled:
            return
        logger.warning(
            "lightning.circuit_breaker.state_change",
            extra={
                "event": "circuit_breaker",
                "new_state": new_state,
                "previous_state": previous_state,
            },
        )


class NoopTelemetry:
    def on_request_start(self, request_id: str, method: str, path: str) -> None:
        pass

    def on_request_end(self, request_id: str, method: str, path: str, status: int, duration: float) -> None:
        pass

    def on_error(self, request_id: str, method: str, path: str, error: Exception) -> None:
        pass

    def on_retry(self, request_id: str, method: str, path: str, attempt: int, delay: float) -> None:
        pass

    def on_circuit_breaker(self, new_state: str, previous_state: str) -> None:
        pass


class OpenTelemetryBridge:
    def __init__(self, tracer=None, meter=None):
        self._tracer = tracer
        self._meter = meter

    def on_request_start(self, request_id: str, method: str, path: str) -> None:
        if self._tracer:
            span = self._tracer.start_span(f"{method} {path}")
            span.set_attribute("http.request_id", request_id)
            span.set_attribute("http.method", method)
            span.set_attribute("http.url", path)

    def on_request_end(self, request_id: str, method: str, path: str, status: int, duration: float) -> None:
        if self._meter:
            hist = self._meter.create_histogram(
                "lightning.client.request.duration",
                unit="ms",
                description="Request duration in milliseconds",
            )
            hist.record(duration * 1000, {"method": method, "path": path, "status": str(status)})

    def on_error(self, request_id: str, method: str, path: str, error: Exception) -> None:
        if self._meter:
            counter = self._meter.create_counter(
                "lightning.client.request.errors",
                description="Count of request errors",
            )
            counter.add(1, {"method": method, "path": path, "error_type": type(error).__name__})

    def on_retry(self, request_id: str, method: str, path: str, attempt: int, delay: float) -> None:
        pass

    def on_circuit_breaker(self, new_state: str, previous_state: str) -> None:
        pass
