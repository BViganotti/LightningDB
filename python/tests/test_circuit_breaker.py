"""Tests for circuit breaker."""
import time
import threading

from lightning.client._circuit_breaker import CircuitBreaker, CircuitState
from lightning.client._types import CircuitBreakerConfig


def _cb(**kw) -> CircuitBreaker:
    return CircuitBreaker(CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30.0, **kw))


def test_initial_state() -> None:
    c = _cb()
    assert c.state == CircuitState.CLOSED
    assert c.allow_request()


def test_opens_after_threshold() -> None:
    c = _cb(failure_threshold=3)
    c.on_failure()
    assert c.state == CircuitState.CLOSED
    c.on_failure()
    assert c.state == CircuitState.CLOSED
    c.on_failure()
    assert c.state == CircuitState.OPEN
    assert not c.allow_request()


def test_recovers_to_half_open() -> None:
    c = _cb(failure_threshold=1, recovery_timeout=0.1)
    c.on_failure()
    assert c.state == CircuitState.OPEN
    time.sleep(0.15)
    assert c.allow_request()


def test_half_open_limits_requests() -> None:
    c = _cb(failure_threshold=1, recovery_timeout=0.1, half_open_max_requests=2)
    c.on_failure()
    time.sleep(0.15)
    assert c.allow_request()
    assert c.allow_request()
    assert not c.allow_request()


def test_half_open_success_closes() -> None:
    c = _cb(failure_threshold=1, recovery_timeout=0.1, success_threshold=2)
    c.on_failure()
    time.sleep(0.15)
    c.allow_request()
    c.on_success()
    c.allow_request()
    c.on_success()
    assert c.state == CircuitState.CLOSED


def test_half_open_failure_reopens() -> None:
    c = _cb(failure_threshold=1, recovery_timeout=60.0, success_threshold=1)
    c.on_failure()
    assert c.state == CircuitState.OPEN
    import time as _t

    c._last_failure_time = _t.monotonic() - 61.0
    assert c.allow_request()
    c.on_failure()
    assert c.state == CircuitState.OPEN


def test_success_resets_counter() -> None:
    c = _cb(failure_threshold=3)
    c.on_failure()
    c.on_failure()
    c.on_success()
    c.on_failure()
    assert c.state == CircuitState.CLOSED
    c.on_failure()
    assert c.state == CircuitState.CLOSED
    c.on_failure()
    assert c.state == CircuitState.OPEN


def test_thread_safety() -> None:
    c = _cb(failure_threshold=100)
    errors = []

    def worker() -> None:
        for _ in range(100):
            c.on_failure()

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert c.state == CircuitState.CLOSED
