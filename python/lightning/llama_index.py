"""LlamaIndex integration for Lightning memory store.

Provides VectorStore and memory implementations compatible
with llama-index-core.
"""

from __future__ import annotations

import uuid
from typing import Any, List, Optional, Sequence

from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.schema import BaseNode, Document, TextNode
from llama_index.core.vector_stores import (
    VectorStore as LlamaVectorStore,
    VectorStoreQuery,
    VectorStoreQueryResult,
)

from . import MemoryStore


class LightningVectorStore(LlamaVectorStore):
    """LlamaIndex VectorStore backed by Lightning.
    
    Stores nodes as Entity nodes with embeddings in Lightning's
    graph+vector database.
    
    Example:
        from lightning.llama_index import LightningVectorStore
        
        vector_store = LightningVectorStore(path="/tmp/memory")
        index = VectorStoreIndex.from_documents(
            documents,
            vector_store=vector_store,
        )
    """

    stores_text: bool = True
    flat_metadata: bool = False

    def __init__(
        self,
        path: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._memory = MemoryStore(path)

    @classmethod
    def class_name(cls) -> str:
        return "LightningVectorStore"

    @property
    def client(self) -> MemoryStore:
        return self._memory

    def add(self, nodes: List[BaseNode]) -> List[str]:
        ids = []
        for node in nodes:
            node_id = node.node_id
            self._memory.store(
                id=node_id,
                content=node.get_content(),
                entity_type="document",
                metadata=str(node.metadata or {}),
                embedding=node.embedding,
            )
            ids.append(node_id)
        return ids

    def delete(self, ref_doc_id: str, **delete_kwargs: Any) -> None:
        self._memory.forget(ref_doc_id)

    def query(self, query: VectorStoreQuery) -> VectorStoreQueryResult:
        query_text = query.query_str or ""
        query_embedding = query.query_embedding or []

        results = self._memory.recall_with_embedding(
            query_text,
            list(query_embedding) if query_embedding is not None else [],
            query.similarity_top_k or 4,
        )

        nodes = []
        similarities = []
        ids = []
        for r in results:
            nodes.append(Document(
                text=r["content"],
                metadata={
                    "id": r["id"],
                    "type": r["type"],
                    "score": r["score"],
                },
            ))
            similarities.append(r["score"])
            ids.append(r["id"])

        return VectorStoreQueryResult(
            nodes=nodes,
            similarities=similarities,
            ids=ids,
        )


__all__ = ["LightningVectorStore"]
