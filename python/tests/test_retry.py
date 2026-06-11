"""Tests for retry/backoff logic."""
from lightning.client._retry import compute_backoff, should_retry
from lightning.client._types import RetryConfig


def test_backoff_increases_with_attempts() -> None:
    config = RetryConfig(base_delay=0.1, max_delay=10.0, jitter_factor=0.0)
    delays = [compute_backoff(i, config) for i in range(5)]
    for i in range(1, len(delays)):
        assert delays[i] >= delays[i - 1]


def test_backoff_respects_max() -> None:
    config = RetryConfig(base_delay=1.0, max_delay=2.0, jitter_factor=0.0)
    d = compute_backoff(10, config)
    assert d <= 2.0


def test_backoff_never_negative() -> None:
    config = RetryConfig(base_delay=0.1, max_delay=10.0, jitter_factor=0.5)
    for i in range(10):
        assert compute_backoff(i, config) >= 0


def test_should_retry_429() -> None:
    config = RetryConfig(max_retries=3)
    assert should_retry(429, 0, config)
    assert should_retry(429, 2, config)
    assert not should_retry(429, 3, config)


def test_should_retry_503() -> None:
    config = RetryConfig(max_retries=3)
    assert should_retry(503, 0, config)
    assert not should_retry(503, 3, config)


def test_should_not_retry_400() -> None:
    config = RetryConfig(max_retries=3)
    assert not should_retry(400, 0, config)


def test_should_not_retry_401() -> None:
    config = RetryConfig(max_retries=3)
    assert not should_retry(401, 0, config)


def test_should_not_retry_zero_retries() -> None:
    config = RetryConfig(max_retries=0)
    assert not should_retry(429, 0, config)
