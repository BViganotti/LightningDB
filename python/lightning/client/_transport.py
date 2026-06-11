from __future__ import annotations

import json
import os
import ssl
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterator, Optional

import httpx

from lightning.client._circuit_breaker import CircuitBreaker, CircuitState
from lightning.client._retry import compute_backoff, is_connection_error, should_retry
from lightning.client._types import (
    CircuitBreakerConfig,
    ClientConfig,
    TlsConfig,
    TelemetryHooks,
)


class LightningTransportError(Exception):
    def __init__(self, message: str, status_code: int = 0, request_id: str = ""):
        self.status_code = status_code
        self.request_id = request_id
        super().__init__(message)


class CircuitBreakerOpenError(LightningTransportError):
    pass


class MaxRetriesExceededError(LightningTransportError):
    def __init__(self, message: str, attempts: int, request_id: str = ""):
        self.attempts = attempts
        super().__init__(message, request_id=request_id)


class PayloadTooLargeError(LightningTransportError):
    pass


def _build_ssl_context(tls: Optional[TlsConfig]) -> Optional[ssl.SSLContext]:
    if tls is None:
        return None
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=tls.ca_bundle_path,
    )
    if not tls.verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if tls.cert_path and tls.key_path:
        ctx.load_cert_chain(tls.cert_path, tls.key_path)
    return ctx


def _choose_base_url(config: ClientConfig) -> str:
    base = config.base_url.rstrip("/")
    if config.tls and not base.startswith("https://"):
        base = base.replace("http://", "https://")
    return base


def _resolve_auth(config: ClientConfig) -> Optional[str]:
    if config.auth_token_provider:
        return config.auth_token_provider()
    return config.auth_token


def _make_headers(config: ClientConfig, request_id: str, auth_token: Optional[str]) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "User-Agent": config.user_agent,
        "X-Request-Id": request_id,
    }
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    return headers


class SyncTransport:
    def __init__(self, config: ClientConfig):
        self._config = config
        self._circuit_breaker: Optional[CircuitBreaker] = None
        if config.circuit_breaker:
            self._circuit_breaker = CircuitBreaker(
                config.circuit_breaker, telemetry=config.telemetry
            )
        ssl_ctx = _build_ssl_context(config.tls)
        limits = httpx.Limits(
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
            keepalive_expiry=config.keepalive_timeout,
        )
        base_url = _choose_base_url(config)
        self._client = httpx.Client(
            base_url=base_url,
            verify=ssl_ctx if ssl_ctx else True,
            limits=limits,
            timeout=httpx.Timeout(config.default_timeout),
            follow_redirects=config.follow_redirects,
        )

    def _check_circuit_breaker(self, path: str) -> None:
        if self._circuit_breaker is None:
            return
        if not self._circuit_breaker.allow_request():
            state = self._circuit_breaker.state
            telemetry = self._config.telemetry
            if telemetry and telemetry.on_circuit_breaker:
                telemetry.on_circuit_breaker("denied", state.value)
            raise CircuitBreakerOpenError(
                f"circuit breaker is {state.value}, request denied",
                request_id="",
            )

    def _report_success(self) -> None:
        if self._circuit_breaker:
            self._circuit_breaker.on_success()

    def _report_failure(self) -> None:
        if self._circuit_breaker:
            self._circuit_breaker.on_failure()

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        self._check_circuit_breaker(path)
        request_id = str(uuid.uuid4())
        auth_token = _resolve_auth(self._config)
        headers = _make_headers(self._config, request_id, auth_token)
        telemetry = self._config.telemetry
        start = time.monotonic()

        if telemetry and telemetry.on_request_start:
            telemetry.on_request_start(request_id, method, path)

        last_exception: Optional[Exception] = None

        for attempt in range(self._config.retry.max_retries + 1):
            if attempt > 0:
                delay = compute_backoff(attempt - 1, self._config.retry)
                if telemetry and telemetry.on_retry:
                    telemetry.on_retry(
                        request_id, method, path, str(attempt), delay
                    )
                import time as _time
                _time.sleep(delay)

            try:
                resp = self._client.request(
                    method,
                    path,
                    json=json_body,
                    headers=headers,
                    timeout=timeout or self._config.default_timeout,
                )

                if resp.is_error:
                    status = resp.status_code
                    body = self._try_decode_error(resp, request_id)

                    if status == 429:
                        if attempt < self._config.retry.max_retries:
                            continue
                    elif status in (502, 503, 504):
                        if attempt < self._config.retry.max_retries:
                            continue

                    self._report_failure()
                    error_msg = body.get("error", resp.text)
                    error_code = body.get("code")
                    raise LightningTransportError(
                        error_msg,
                        status_code=status,
                        request_id=body.get("requestId", request_id),
                    )

                self._report_success()
                duration = time.monotonic() - start
                if telemetry and telemetry.on_request_end:
                    telemetry.on_request_end(
                        request_id, method, path, resp.status_code, duration
                    )

                content_type = resp.headers.get("content-type", "")
                if "text/plain" in content_type:
                    return resp.text

                try:
                    wrapper = resp.json()
                except json.JSONDecodeError:
                    return resp.text

                return wrapper.get("data", wrapper)

            except httpx.TimeoutException as e:
                last_exception = e
                self._report_failure()
                if telemetry and telemetry.on_error:
                    telemetry.on_error(request_id, method, path, e)
                if attempt >= self._config.retry.max_retries:
                    raise LightningTransportError(
                        f"request timed out after {self._config.default_timeout}s",
                        request_id=request_id,
                    ) from e

            except httpx.TransportError as e:
                last_exception = e
                self._report_failure()
                if telemetry and telemetry.on_error:
                    telemetry.on_error(request_id, method, path, e)
                if is_connection_error(e) and attempt < self._config.retry.max_retries:
                    continue
                raise LightningTransportError(
                    f"transport error: {e}",
                    request_id=request_id,
                ) from e

            except LightningTransportError:
                raise

            except Exception as e:
                last_exception = e
                self._report_failure()
                if telemetry and telemetry.on_error:
                    telemetry.on_error(request_id, method, path, e)
                raise LightningTransportError(
                    f"unexpected error: {e}",
                    request_id=request_id,
                ) from e

        self._report_failure()
        raise MaxRetriesExceededError(
            f"max retries ({self._config.retry.max_retries}) exceeded",
            attempts=self._config.retry.max_retries + 1,
            request_id=request_id,
        )

    def _try_decode_error(self, resp: httpx.Response, request_id: str) -> dict:
        try:
            return resp.json()
        except json.JSONDecodeError:
            return {"error": resp.text, "requestId": request_id}

    def stream(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
    ) -> Iterator[dict]:
        self._check_circuit_breaker(path)
        request_id = str(uuid.uuid4())
        auth_token = _resolve_auth(self._config)
        headers = _make_headers(self._config, request_id, auth_token)

        with self._client.stream(
            method,
            path,
            json=json_body,
            headers=headers,
        ) as resp:
            if resp.is_error:
                raise LightningTransportError(
                    f"stream error: {resp.status_code}",
                    status_code=resp.status_code,
                    request_id=request_id,
                )
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    yield json.loads(line[6:])

    def close(self) -> None:
        self._client.close()


class AsyncTransport:
    def __init__(self, config: ClientConfig):
        self._config = config
        self._circuit_breaker: Optional[CircuitBreaker] = None
        if config.circuit_breaker:
            self._circuit_breaker = CircuitBreaker(
                config.circuit_breaker, telemetry=config.telemetry
            )
        ssl_ctx = _build_ssl_context(config.tls)
        limits = httpx.Limits(
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
            keepalive_expiry=config.keepalive_timeout,
        )
        base_url = _choose_base_url(config)
        self._client = httpx.AsyncClient(
            base_url=base_url,
            verify=ssl_ctx if ssl_ctx else True,
            limits=limits,
            timeout=httpx.Timeout(config.default_timeout),
            follow_redirects=config.follow_redirects,
        )

    def _check_circuit_breaker(self, path: str) -> None:
        if self._circuit_breaker is None:
            return
        if not self._circuit_breaker.allow_request():
            state = self._circuit_breaker.state
            telemetry = self._config.telemetry
            if telemetry and telemetry.on_circuit_breaker:
                telemetry.on_circuit_breaker("denied", state.value)
            raise CircuitBreakerOpenError(
                f"circuit breaker is {state.value}, request denied",
                request_id="",
            )

    def _report_success(self) -> None:
        if self._circuit_breaker:
            self._circuit_breaker.on_success()

    def _report_failure(self) -> None:
        if self._circuit_breaker:
            self._circuit_breaker.on_failure()

    async def request(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        self._check_circuit_breaker(path)
        request_id = str(uuid.uuid4())
        auth_token = _resolve_auth(self._config)
        headers = _make_headers(self._config, request_id, auth_token)
        telemetry = self._config.telemetry
        start = time.monotonic()

        if telemetry and telemetry.on_request_start:
            telemetry.on_request_start(request_id, method, path)

        last_exception: Optional[Exception] = None

        for attempt in range(self._config.retry.max_retries + 1):
            if attempt > 0:
                delay = compute_backoff(attempt - 1, self._config.retry)
                if telemetry and telemetry.on_retry:
                    telemetry.on_retry(
                        request_id, method, path, str(attempt), delay
                    )
                import asyncio
                await asyncio.sleep(delay)

            try:
                resp = await self._client.request(
                    method,
                    path,
                    json=json_body,
                    headers=headers,
                    timeout=timeout or self._config.default_timeout,
                )

                if resp.is_error:
                    status = resp.status_code
                    body = self._try_decode_error(resp, request_id)

                    if status == 429:
                        if attempt < self._config.retry.max_retries:
                            continue
                    elif status in (502, 503, 504):
                        if attempt < self._config.retry.max_retries:
                            continue

                    self._report_failure()
                    error_msg = body.get("error", resp.text)
                    raise LightningTransportError(
                        error_msg,
                        status_code=status,
                        request_id=body.get("requestId", request_id),
                    )

                self._report_success()
                duration = time.monotonic() - start
                if telemetry and telemetry.on_request_end:
                    telemetry.on_request_end(
                        request_id, method, path, resp.status_code, duration
                    )

                content_type = resp.headers.get("content-type", "")
                if "text/plain" in content_type:
                    return resp.text

                try:
                    wrapper = resp.json()
                except json.JSONDecodeError:
                    return resp.text

                return wrapper.get("data", wrapper)

            except httpx.TimeoutException as e:
                last_exception = e
                self._report_failure()
                if telemetry and telemetry.on_error:
                    telemetry.on_error(request_id, method, path, e)
                if attempt >= self._config.retry.max_retries:
                    raise LightningTransportError(
                        f"request timed out after {self._config.default_timeout}s",
                        request_id=request_id,
                    ) from e

            except httpx.TransportError as e:
                last_exception = e
                self._report_failure()
                if telemetry and telemetry.on_error:
                    telemetry.on_error(request_id, method, path, e)
                if is_connection_error(e) and attempt < self._config.retry.max_retries:
                    continue
                raise LightningTransportError(
                    f"transport error: {e}",
                    request_id=request_id,
                ) from e

            except LightningTransportError:
                raise

            except Exception as e:
                last_exception = e
                self._report_failure()
                if telemetry and telemetry.on_error:
                    telemetry.on_error(request_id, method, path, e)
                raise LightningTransportError(
                    f"unexpected error: {e}",
                    request_id=request_id,
                ) from e

        self._report_failure()
        raise MaxRetriesExceededError(
            f"max retries ({self._config.retry.max_retries}) exceeded",
            attempts=self._config.retry.max_retries + 1,
            request_id=request_id,
        )

    def _try_decode_error(self, resp: httpx.Response, request_id: str) -> dict:
        try:
            return resp.json()
        except json.JSONDecodeError:
            return {"error": resp.text, "requestId": request_id}

    async def stream(
        self,
        method: str,
        path: str,
        json_body: Optional[dict] = None,
    ) -> Any:
        self._check_circuit_breaker(path)
        request_id = str(uuid.uuid4())
        auth_token = _resolve_auth(self._config)
        headers = _make_headers(self._config, request_id, auth_token)

        resp = await self._client.request(
            method,
            path,
            json=json_body,
            headers=headers,
        )
        if resp.is_error:
            raise LightningTransportError(
                f"stream error: {resp.status_code}",
                status_code=resp.status_code,
                request_id=request_id,
            )
        return resp

    async def close(self) -> None:
        await self._client.aclose()
