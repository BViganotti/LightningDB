from __future__ import annotations

import random
import time
from typing import Optional

from lightning.client._types import RetryConfig, TelemetryHooks


def compute_backoff(
    attempt: int,
    config: RetryConfig,
) -> float:
    delay = min(config.base_delay * (2 ** attempt), config.max_delay)
    jitter = random.uniform(-config.jitter_factor * delay, config.jitter_factor * delay)
    return max(0.0, delay + jitter)


def should_retry(
    status_code: int,
    attempt: int,
    config: RetryConfig,
) -> bool:
    if attempt >= config.max_retries:
        return False
    return status_code in config.retryable_statuses


def is_connection_error(exception: Exception) -> bool:
    import httpx
    if isinstance(exception, (httpx.ConnectError, httpx.ConnectTimeout)):
        return True
    if isinstance(exception, httpx.ReadTimeout) and "connection" in str(exception).lower():
        return True
    if isinstance(exception, httpx.RemoteProtocolError):
        return True
    return False


def retry_with_backoff(
    attempt: int,
    config: RetryConfig,
    telemetry: Optional[TelemetryHooks],
    request_id: str,
    path: str,
    status_code: int,
) -> float:
    delay = compute_backoff(attempt, config)
    if telemetry and telemetry.on_retry:
        telemetry.on_retry(request_id, path, str(status_code), attempt + 1, delay)
    time.sleep(delay)
    return delay
