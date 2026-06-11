"""Tests for the sync LightningDB HTTP client.

These tests use a mock HTTP server to verify client behavior
(endpoints, retry, circuit breaker, validation, etc.) without
needing a running lightning-server instance.
"""

from __future__ import annotations

import json
import time
from typing import Any
from unittest import mock

import httpx
import pytest
from httpx import Request, Response

from lightning.client import Client, ClientConfig, RetryConfig, CircuitBreakerConfig, TlsConfig


def _build_client(**overrides: Any) -> Client:
    config = ClientConfig(
        base_url="http://localhost:9999",
        retry=RetryConfig(max_retries=1, base_delay=0.01),
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=0.5,
            half_open_max_requests=2,
            success_threshold=1,
        ),
        **overrides,
    )
    return Client(config)


def _mock_json_response(data: Any, status: int = 200) -> Response:
    body = json.dumps({"data": data, "meta": {"requestId": "test", "durationMs": 1}})
    return Response(status, content=body, request=Request("POST", "http://localhost:9999/test"))


def _mock_error_response(
    error: str = "test error",
    code: str = "TEST_ERROR",
    status: int = 400,
) -> Response:
    body = json.dumps({"error": error, "code": code, "requestId": "test"})
    return Response(status, content=body, request=Request("POST", "http://localhost:9999/test"))


# ── Validation Tests ───────────────────────────────────────────────────


def test_store_validates_id() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must not be empty"):
        client.store("", "content")


def test_store_validates_content_too_large() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="exceeds max"):
        client.store("id", "x" * (11 * 1024 * 1024))


def test_store_validates_entity_type() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must not be empty"):
        client.store("id", "content", entity_type="")


def test_store_validates_embedding_type() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must be a list"):
        client.store("id", "content", embedding="not_a_list")  # type: ignore


def test_store_validates_embedding_dimension() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="exceeds max"):
        client.store("id", "content", embedding=[0.0] * 20000)


def test_top_k_validation() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must be >= 1"):
        client.recall("query", top_k=0)
    with pytest.raises(ValueError, match="exceeds max"):
        client.recall("query", top_k=99999)


def test_batch_entities_validation() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must be a list"):
        client.store_batch("not_list")  # type: ignore
    with pytest.raises(ValueError, match="must not be empty"):
        client.store_batch([])


def test_hops_validation() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must be >= 1"):
        client.expand("id", hops=0)
    with pytest.raises(ValueError, match="must not exceed 10"):
        client.expand("id", hops=20)


def test_id_validation() -> None:
    client = _build_client()
    with pytest.raises(ValueError, match="must be a string"):
        client.forget(123)  # type: ignore


# ── Transport / Retry Tests ────────────────────────────────────────────


def test_retry_on_429() -> None:
    """Client should retry on 429 Too Many Requests."""
    transport = mock.MagicMock()
    transport.request.side_effect = [
        httpx.HTTPStatusError(
            "too many requests",
            request=mock.MagicMock(),
            response=Response(429, content=b'{"error":"rate limit"}', request=Request("GET", "http://localhost:9999/test")),
        ),
        {"results": []},
    ]

    client = _build_client(retry=RetryConfig(max_retries=2, base_delay=0.01))
    client._transport = transport
    result = client.recall("test")
    assert result == []


def test_max_retries_exceeded() -> None:
    """Client should raise after max retries on persistent 429."""
    transport = mock.MagicMock()
    transport.request.side_effect = [
        httpx.HTTPStatusError(
            "rate limit",
            request=mock.MagicMock(),
            response=Response(429, content=b'{"error":"rate limit"}', request=Request("GET", "http://localhost:9999/test")),
        )
    ] * 5

    client = _build_client(retry=RetryConfig(max_retries=2, base_delay=0.01))
    client._transport = transport
    with pytest.raises(Exception):
        client.recall("test")


def test_no_retry_on_400() -> None:
    """Client should NOT retry on 400 Bad Request."""
    transport = mock.MagicMock()
    transport.request.side_effect = [
        httpx.HTTPStatusError(
            "bad request",
            request=mock.MagicMock(),
            response=Response(400, content=b'{"error":"bad request","code":"BAD_REQUEST"}', request=Request("GET", "http://localhost:9999/test")),
        )
    ]

    client = _build_client()
    client._transport = transport
    with pytest.raises(Exception):
        client.recall("test")
    assert transport.request.call_count == 1


# ── Circuit Breaker Tests ──────────────────────────────────────────────


def test_circuit_breaker_opens_after_failures() -> None:
    client = _build_client(
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=30.0,
            half_open_max_requests=1,
            success_threshold=1,
        ),
    )
    cb = client._transport._circuit_breaker
    assert cb is not None
    assert cb.allow_request()
    cb.on_failure()
    assert cb.allow_request()
    cb.on_failure()
    assert not cb.allow_request()


def test_circuit_breaker_half_open_recovers() -> None:
    client = _build_client(
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.1,
            half_open_max_requests=2,
            success_threshold=1,
        ),
    )
    cb = client._transport._circuit_breaker
    assert cb is not None
    cb.on_failure()
    assert not cb.allow_request()
    time.sleep(0.15)
    assert cb.allow_request()
    cb.on_success()
    assert cb.allow_request()


def test_circuit_breaker_half_open_fails_reopens() -> None:
    client = _build_client(
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=0.1,
            half_open_max_requests=2,
            success_threshold=1,
        ),
    )
    cb = client._transport._circuit_breaker
    assert cb is not None
    cb.on_failure()
    cb.on_failure()
    assert not cb.allow_request()
    time.sleep(0.15)
    assert cb.allow_request()
    cb.on_failure()
    assert not cb.allow_request()


# ── TLS Config Tests ───────────────────────────────────────────────────


def test_tls_validation_passes() -> None:
    config = TlsConfig(verify=True)
    config.verify_self_consistency()


def test_tls_requires_both_cert_and_key() -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        TlsConfig(cert_path="/tmp/cert.pem").verify_self_consistency()


# ── Config Validation Tests ────────────────────────────────────────────


def test_config_validates_timeout() -> None:
    with pytest.raises(ValueError, match="default_timeout"):
        ClientConfig(base_url="http://localhost:9999", default_timeout=0)


def test_config_validates_max_connections() -> None:
    with pytest.raises(ValueError, match="max_connections"):
        ClientConfig(base_url="http://localhost:9999", max_connections=0)


def test_config_validates_max_content_bytes() -> None:
    with pytest.raises(ValueError, match="max_content_bytes"):
        ClientConfig(base_url="http://localhost:9999", max_content_bytes=0)


def test_config_validates_max_batch_entities() -> None:
    with pytest.raises(ValueError, match="max_batch_entities"):
        ClientConfig(base_url="http://localhost:9999", max_batch_entities=0)


def test_config_validates_max_top_k() -> None:
    with pytest.raises(ValueError, match="max_top_k"):
        ClientConfig(base_url="http://localhost:9999", max_top_k=0)


def test_retry_config_validation() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        RetryConfig(max_retries=-1)
    with pytest.raises(ValueError, match="base_delay"):
        RetryConfig(base_delay=0)
    with pytest.raises(ValueError, match="max_delay"):
        RetryConfig(base_delay=5, max_delay=1)
    with pytest.raises(ValueError, match="jitter_factor"):
        RetryConfig(jitter_factor=1.5)


def test_circuit_breaker_config_validation() -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreakerConfig(failure_threshold=0)
    with pytest.raises(ValueError, match="recovery_timeout"):
        CircuitBreakerConfig(recovery_timeout=0)
    with pytest.raises(ValueError, match="half_open_max_requests"):
        CircuitBreakerConfig(half_open_max_requests=0)
    with pytest.raises(ValueError, match="success_threshold"):
        CircuitBreakerConfig(success_threshold=0)


# ── Endpoint Request Verification Tests ────────────────────────────────


def test_store_sends_correct_body() -> None:
    client = _build_client()
    client._transport.request = mock.MagicMock(return_value=None)
    client.store("test-id", "test content", entity_type="fact", metadata={"key": "val"})
    call_args = client._transport.request.call_args
    assert call_args[0][0] == "POST"
    assert call_args[0][1] == "/v1/memory/store"
    body = call_args[1]["json_body"]
    assert body["id"] == "test-id"
    assert body["content"] == "test content"
    assert body["entityType"] == "fact"
    assert json.loads(body["metadata"]) == {"key": "val"}


def test_recall_sends_correct_body() -> None:
    client = _build_client()
    client._transport.request = mock.MagicMock(return_value={"results": []})
    client.recall("search query", top_k=20)
    call_args = client._transport.request.call_args
    assert call_args[0][0] == "POST"
    assert call_args[0][1] == "/v1/memory/recall"
    body = call_args[1]["json_body"]
    assert body["query"] == "search query"
    assert body["topK"] == 20


def test_associate_sends_correct_body() -> None:
    client = _build_client()
    client._transport.request = mock.MagicMock(return_value=None)
    client.associate("src", "dst", "knows", weight=2.0)
    call_args = client._transport.request.call_args
    body = call_args[1]["json_body"]
    assert body["srcId"] == "src"
    assert body["dstId"] == "dst"
    assert body["relType"] == "knows"
    assert body["weight"] == 2.0


def test_query_sends_correct_body() -> None:
    client = _build_client()
    client._transport.request = mock.MagicMock(return_value={"columns": [], "rows": [], "numRows": 0})
    client.query("MATCH (n) RETURN n", params={"limit": 10}, snapshot_ts=12345, timeout_ms=5000)
    call_args = client._transport.request.call_args
    body = call_args[1]["json_body"]
    assert body["query"] == "MATCH (n) RETURN n"
    assert body["params"] == {"limit": 10}
    assert body["snapshotTs"] == 12345
    assert body["timeoutMs"] == 5000
