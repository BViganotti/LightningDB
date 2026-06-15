from __future__ import annotations

import json
from typing import Any, AsyncIterator, Optional, Union

from lightning.client._transport import (
    AsyncTransport,
)
from lightning.client._types import (
    ChangeEvent,
    ClientConfig,
    ConsolidationReport,
    ContradictionDetail,
    DecayResult,
    Entity,
    LinkDetail,
    QueryResult,
    RagResult,
    SearchResult,
    SnapshotSelector,
    SourceRef,
)
from lightning.client._validation import (
    validate_batch_entities,
    validate_embedding,
    validate_entity_type,
    validate_hops,
    validate_id,
    validate_metadata,
    validate_query_string,
    validate_store_params,
    validate_top_k,
)


class AsyncClient:
    def __init__(self, config: ClientConfig):
        self._config = config
        self._transport = AsyncTransport(config)
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None

    @property
    def config(self) -> ClientConfig:
        return self._config

    async def _post(self, path: str, body: dict, timeout: Optional[float] = None) -> Any:
        return await self._transport.request("POST", path, json_body=body, timeout=timeout)

    async def _get(self, path: str, timeout: Optional[float] = None) -> Any:
        return await self._transport.request("GET", path, timeout=timeout)

    # ── Auth ───────────────────────────────────────────────────────────

    async def login(self, username: str, password: str) -> None:
        body = {"username": username, "password": password}
        result = await self._post("/v1/auth/login", body)
        self._access_token = result["accessToken"]
        self._refresh_token = result["refreshToken"]
        self._config.auth_token = self._access_token
        self._config.auth_token_provider = lambda: self._access_token

    async def login_with_api_key(self, api_key: str) -> None:
        self._config.auth_token = api_key

    # ── Memory ─────────────────────────────────────────────────────────

    async def store(
        self,
        id: str,
        content: str,
        entity_type: str = "memory",
        metadata: Any = None,
        embedding: Optional[list[float]] = None,
        ttl_seconds: Optional[int] = None,
        created_at: Optional[int] = None,
        last_accessed: Optional[int] = None,
        access_count: Optional[int] = None,
        valid_from: Optional[int] = None,
        valid_until: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> None:
        validate_store_params(id, content, entity_type, metadata, embedding)
        body: dict[str, Any] = {
            "id": id,
            "content": content,
            "entityType": entity_type,
            "metadata": validate_metadata(metadata) if metadata is not None else "{}",
        }
        if embedding is not None:
            body["embedding"] = embedding
        if ttl_seconds is not None:
            body["ttlSeconds"] = ttl_seconds
        if created_at is not None:
            body["createdAt"] = created_at
        if last_accessed is not None:
            body["lastAccessed"] = last_accessed
        if access_count is not None:
            body["accessCount"] = access_count
        if valid_from is not None:
            body["validFrom"] = valid_from
        if valid_until is not None:
            body["validUntil"] = valid_until
        await self._post("/v1/memory/store", body, timeout=timeout)

    async def store_batch(
        self,
        entities: list[dict],
        timeout: Optional[float] = None,
    ) -> int:
        validate_batch_entities(entities, self._config.max_batch_entities)
        result = await self._post(
            "/v1/memory/store-batch",
            {"entities": entities},
            timeout=timeout,
        )
        return result["stored"]

    async def recall(
        self,
        query: str = "",
        embedding: Optional[list[float]] = None,
        top_k: int = 10,
        timeout: Optional[float] = None,
    ) -> list[SearchResult]:
        validate_top_k(top_k, self._config.max_top_k)
        validate_embedding(embedding)
        body: dict[str, Any] = {"query": query, "topK": top_k}
        if embedding is not None:
            body["embedding"] = embedding
        result = await self._post("/v1/memory/recall", body, timeout=timeout)
        return [SearchResult.from_dict(r) for r in result["results"]]

    async def recall_recent(
        self,
        top_k: int = 10,
        timeout: Optional[float] = None,
    ) -> list[Entity]:
        validate_top_k(top_k, self._config.max_top_k)
        result = await self._post(
            "/v1/memory/recall-recent",
            {"topK": top_k},
            timeout=timeout,
        )
        return [Entity.from_dict(e) for e in result["entities"]]

    async def associate(
        self,
        src_id: str,
        dst_id: str,
        rel_type: str,
        weight: float = 1.0,
        timeout: Optional[float] = None,
    ) -> None:
        validate_id(src_id, "src_id")
        validate_id(dst_id, "dst_id")
        await self._post(
            "/v1/graph/associate",
            {"srcId": src_id, "dstId": dst_id, "relType": rel_type, "weight": weight},
            timeout=timeout,
        )

    async def expand(
        self,
        entity_id: str,
        hops: int = 1,
        edge_types: Optional[list[str]] = None,
        timeout: Optional[float] = None,
    ) -> list[Entity]:
        validate_id(entity_id, "entity_id")
        validate_hops(hops)
        body: dict[str, Any] = {"entityId": entity_id, "hops": hops}
        if edge_types is not None:
            body["edgeTypes"] = edge_types
        result = await self._post("/v1/graph/expand", body, timeout=timeout)
        return [Entity.from_dict(e) for e in result["entities"]]

    # ── RAG ────────────────────────────────────────────────────────────

    async def rag_query(
        self,
        query: str,
        embedding: Optional[list[float]] = None,
        top_k: int = 5,
        expansion_depth: Optional[int] = None,
        search_weight: Optional[float] = None,
        recency_weight: Optional[float] = None,
        degree_weight: Optional[float] = None,
        max_tokens: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> RagResult:
        validate_query_string(query)
        validate_top_k(top_k, self._config.max_top_k)
        validate_embedding(embedding)
        body: dict[str, Any] = {"query": query, "topK": top_k}
        if embedding is not None:
            body["embedding"] = embedding
        if expansion_depth is not None:
            body["expansionDepth"] = expansion_depth
        if search_weight is not None:
            body["searchWeight"] = search_weight
        if recency_weight is not None:
            body["recencyWeight"] = recency_weight
        if degree_weight is not None:
            body["degreeWeight"] = degree_weight
        if max_tokens is not None:
            body["maxTokens"] = max_tokens
        result = await self._post("/v1/rag/query", body, timeout=timeout)
        return RagResult(
            context=result["context"],
            sources=[
                SourceRef(
                    id=s["id"],
                    score=s["score"],
                    entity_type=s.get("entity_type", s.get("type", "")),
                    excerpt=s.get("excerpt", ""),
                )
                for s in result["sources"]
            ],
            total_sources=result["totalSources"],
            warnings=result["warnings"],
        )

    # ── Query ─────────────────────────────────────────────────────────

    async def query(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        snapshot_ts: Optional[Union[int, SnapshotSelector]] = None,
        timeout_ms: int = 30000,
        timeout: Optional[float] = None,
    ) -> QueryResult:
        validate_query_string(query)
        body: dict[str, Any] = {"query": query, "timeoutMs": timeout_ms}
        if params:
            body["params"] = params
        if snapshot_ts is not None:
            if isinstance(snapshot_ts, SnapshotSelector):
                sel: SnapshotSelector = snapshot_ts
                sel_body: dict[str, Any] = {}
                if sel.iso is not None:
                    sel_body["iso"] = sel.iso
                if sel.relative is not None:
                    sel_body["relative"] = sel.relative
                if sel.label is not None:
                    sel_body["label"] = sel.label
                body["snapshot"] = sel_body
            else:
                body["snapshotTs"] = snapshot_ts
        result = await self._post("/v1/query", body, timeout=timeout)
        return QueryResult.from_dict(result)

    async def query_stream(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
    ) -> AsyncIterator[dict]:
        validate_query_string(query)
        body: dict[str, Any] = {"query": query}
        if params:
            body["params"] = params
        resp = await self._transport.stream("POST", "/v1/query/stream", json_body=body)
        async for line in resp.aiter_lines():
            if line.startswith("data: "):
                yield json.loads(line[6:])

    # ── Admin ──────────────────────────────────────────────────────────

    async def checkpoint(self, timeout: Optional[float] = None) -> None:
        await self._post("/v1/admin/checkpoint", {}, timeout=timeout)

    async def vacuum(self, timeout: Optional[float] = None) -> None:
        await self._post("/v1/admin/vacuum", {}, timeout=timeout)

    # ── Health / Metrics ───────────────────────────────────────────────

    async def health(self, timeout: Optional[float] = None) -> dict:
        return await self._get("/health", timeout=timeout)

    async def metrics(self, timeout: Optional[float] = None) -> str:
        return await self._transport.request("GET", "/metrics", timeout=timeout)

    # ── CDC ────────────────────────────────────────────────────────────

    async def subscribe(self) -> AsyncIterator[ChangeEvent]:
        resp = await self._transport.stream("GET", "/v1/subscribe")
        async for line in resp.aiter_lines():
            if line.startswith("data: "):
                event = json.loads(line[6:])
                yield ChangeEvent(
                    timestamp=event.get("timestamp", 0),
                    bytes_written=event.get("bytesWritten", 0),
                    total_wal_bytes=event.get("totalWalBytes", 0),
                    entity_id=event.get("entityId"),
                    operation_type=event.get("operationType", ""),
                )

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._transport.close()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
