from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class SearchResult:
    id: str
    content: str
    entity_type: str
    score: float
    metadata: str

    @classmethod
    def from_dict(cls, d: dict) -> SearchResult:
        return cls(
            id=d["id"],
            content=d["content"],
            entity_type=d.get("entity_type", d.get("type", "")),
            score=d["score"],
            metadata=d.get("metadata", ""),
        )


@dataclass(frozen=True)
class Entity:
    id: str
    entity_type: str
    content: str
    metadata: str
    created_at: int
    last_accessed: int
    access_count: int
    ttl_seconds: int
    valid_from: int
    valid_until: int

    @classmethod
    def from_dict(cls, d: dict) -> Entity:
        return cls(
            id=d["id"],
            entity_type=d.get("entity_type", d.get("type", "")),
            content=d.get("content", ""),
            metadata=d.get("metadata", ""),
            created_at=d.get("createdAt", 0),
            last_accessed=d.get("lastAccessed", 0),
            access_count=d.get("accessCount", 0),
            ttl_seconds=d.get("ttlSeconds", 0),
            valid_from=d.get("validFrom", 0),
            valid_until=d.get("validUntil", 0),
        )


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    num_rows: int

    @classmethod
    def from_dict(cls, d: dict) -> QueryResult:
        return cls(
            columns=d.get("columns", []),
            rows=d.get("rows", []),
            num_rows=d.get("numRows", d.get("num_rows", 0)),
        )


@dataclass(frozen=True)
class SourceRef:
    id: str
    score: float
    entity_type: str
    excerpt: str

    @classmethod
    def from_dict(cls, d: dict) -> SourceRef:
        return cls(
            id=d["id"],
            score=d["score"],
            entity_type=d.get("entity_type", d.get("type", "")),
            excerpt=d.get("excerpt", ""),
        )


@dataclass(frozen=True)
class RagResult:
    context: str
    sources: list[SourceRef]
    total_sources: int
    warnings: list[str]


@dataclass(frozen=True)
class LinkDetail:
    source_id: str
    target_id: str
    rel_type: str
    score: float
    reason: str


@dataclass(frozen=True)
class ContradictionDetail:
    entity_id: str
    source_id: str
    target_id: str
    fields: list[str]
    cosine_sim: float
    jaccard_sim: float
    reason: str


@dataclass(frozen=True)
class ConsolidationDetail:
    links: list[LinkDetail]
    contradictions: list[ContradictionDetail]


@dataclass(frozen=True)
class ConsolidationReport:
    links_created: int
    contradictions_found: int
    total_entities: int
    warnings: list[str]
    links: Optional[list[LinkDetail]] = None
    contradictions: Optional[list[ContradictionDetail]] = None


@dataclass(frozen=True)
class SnapshotSelector:
    iso: Optional[str] = None
    relative: Optional[str] = None
    label: Optional[str] = None


@dataclass(frozen=True)
class StoreBatchResult:
    stored: int


@dataclass(frozen=True)
class DecayResult:
    expired: int


@dataclass(frozen=True)
class ChangeEvent:
    timestamp: int
    bytes_written: int
    total_wal_bytes: int
    entity_id: Optional[str]
    operation_type: str


@dataclass
class TlsConfig:
    verify: bool = True
    ca_bundle_path: Optional[str] = None
    cert_path: Optional[str] = None
    key_path: Optional[str] = None
    server_name_override: Optional[str] = None

    def verify_self_consistency(self) -> None:
        if (self.cert_path is not None) != (self.key_path is not None):
            raise ValueError("cert_path and key_path must be provided together for mTLS")
        if self.ca_bundle_path is not None and not os.path.isfile(self.ca_bundle_path):
            raise ValueError(f"CA bundle not found: {self.ca_bundle_path}")
        if self.cert_path is not None and not os.path.isfile(self.cert_path):
            raise ValueError(f"Cert not found: {self.cert_path}")
        if self.key_path is not None and not os.path.isfile(self.key_path):
            raise ValueError(f"Key not found: {self.key_path}")


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_requests: int = 3
    success_threshold: int = 2

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")
        if self.half_open_max_requests < 1:
            raise ValueError("half_open_max_requests must be >= 1")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 0.1
    max_delay: float = 10.0
    jitter_factor: float = 0.1
    retryable_statuses: frozenset[int] = field(
        default_factory=lambda: frozenset({429, 502, 503, 504})
    )

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be > 0")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if not 0 <= self.jitter_factor <= 1:
            raise ValueError("jitter_factor must be in [0, 1]")


@dataclass
class TelemetryHooks:
    on_request_start: Optional[Callable[[str, str, str], None]] = None
    on_request_end: Optional[Callable[[str, str, str, int, float], None]] = None
    on_error: Optional[Callable[[str, str, str, Exception], None]] = None
    on_retry: Optional[Callable[[str, str, str, int, float], None]] = None
    on_circuit_breaker: Optional[Callable[[str, str], None]] = None


@dataclass
class ClientConfig:
    base_url: str = "http://127.0.0.1:8080"
    auth_token: Optional[str] = None
    auth_token_provider: Optional[Callable[[], Optional[str]]] = None
    default_timeout: float = 30.0
    tls: Optional[TlsConfig] = None
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: Optional[CircuitBreakerConfig] = None
    telemetry: Optional[TelemetryHooks] = None
    max_connections: int = 10
    max_keepalive_connections: int = 5
    keepalive_timeout: float = 60.0
    follow_redirects: bool = False
    max_content_bytes: int = 10 * 1024 * 1024  # 10MB
    max_batch_entities: int = 1000
    max_top_k: int = 1000
    user_agent: str = f"lightning-client-py/0.1.0"

    def __post_init__(self) -> None:
        if self.default_timeout <= 0:
            raise ValueError("default_timeout must be > 0")
        if self.max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        if self.max_keepalive_connections < 1:
            raise ValueError("max_keepalive_connections must be >= 1")
        if self.max_content_bytes <= 0:
            raise ValueError("max_content_bytes must be > 0")
        if self.max_batch_entities <= 0:
            raise ValueError("max_batch_entities must be > 0")
        if self.max_top_k <= 0:
            raise ValueError("max_top_k must be > 0")
        if self.tls is not None:
            self.tls.verify_self_consistency()
