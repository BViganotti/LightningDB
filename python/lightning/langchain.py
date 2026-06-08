"""LangChain integration for Lightning memory store.

Provides VectorStore and BaseMemory implementations compatible
with langchain-core and langchain-memory.
"""

from __future__ import annotations

import uuid
from typing import Any, Iterable, List, Optional

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

from . import MemoryStore


class LightningVectorStore(VectorStore):
    """LangChain VectorStore backed by Lightning.
    
    Stores documents as Entity nodes with embeddings, enabling
    graph+vector hybrid retrieval.
    
    Example:
        from lightning.langchain import LightningVectorStore
        from langchain_openai import OpenAIEmbeddings
        
        vector_store = LightningVectorStore(
            path="/tmp/memory",
            embedding=OpenAIEmbeddings(),
        )
        vector_store.add_texts(["Hello world", "Goodbye world"])
        results = vector_store.similarity_search("hello", k=5)
    """

    def __init__(
        self,
        path: str,
        embedding: Embeddings,
        **kwargs: Any,
    ):
        self._memory = MemoryStore(path)
        self._embedding = embedding

    @property
    def embeddings(self) -> Embeddings:
        return self._embedding

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        texts_list = list(texts)
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts_list]
        if metadatas is None:
            metadatas = [{} for _ in texts_list]

        embeddings = self._embedding.embed_documents(texts_list)
        for i, text in enumerate(texts_list):
            self._memory.store(
                id=ids[i],
                content=text,
                entity_type=kwargs.get("entity_type", "document"),
                metadata=str(metadatas[i] if i < len(metadatas) else {}),
                embedding=embeddings[i],
            )

        return ids

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[Document]:
        query_embedding = self._embedding.embed_query(query)
        results = self._memory.recall_with_embedding(query, query_embedding, k)
        return [
            Document(
                page_content=r["content"],
                metadata={
                    "id": r["id"],
                    "type": r["type"],
                    "score": r["score"],
                },
            )
            for r in results
        ]

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> Optional[bool]:
        if ids is None:
            return False
        for entity_id in ids:
            self._memory.forget(entity_id)
        return True

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding: Embeddings,
        metadatas: Optional[List[dict]] = None,
        path: str = "/tmp/lightning",
        **kwargs: Any,
    ) -> LightningVectorStore:
        store = cls(path=path, embedding=embedding, **kwargs)
        store.add_texts(texts, metadatas, **kwargs)
        return store


class LightningChatMemory:
    """LangChain-compatible chat memory backed by Lightning.
    
    Stores conversation messages as typed entities with temporal ordering
    and graph relationships between related messages.
    
    Example:
        from lightning.langchain import LightningChatMemory
        from langchain.memory import ConversationBufferMemory
        
        memory = LightningChatMemory(path="/tmp/chat_memory", session_id="session-1")
        memory.chat_memory.add_user_message("Hello!")
        memory.chat_memory.add_ai_message("Hi! How can I help?")
    """

    def __init__(
        self,
        path: str,
        session_id: str = "default",
    ):
        self._memory = MemoryStore(path)
        self._session_id = session_id

    @property
    def memory_variables(self) -> List[str]:
        return ["history"]

    def load_memory_variables(self, inputs: dict) -> dict:
        recent = self._memory.recall_by_type(f"chat_{self._session_id}", 50)
        history = []
        for msg in recent:
            history.append({
                "role": msg["type"].split("_")[-1],  # "chat_user", "chat_ai"
                "content": msg["content"],
            })
        return {"history": history}

    def save_context(self, inputs: dict, outputs: dict) -> None:
        if "input" in inputs:
            self._memory.store(
                id=str(uuid.uuid4()),
                content=inputs["input"],
                entity_type=f"chat_{self._session_id}",
            )
        if "output" in outputs:
            self._memory.store(
                id=str(uuid.uuid4()),
                content=outputs["output"],
                entity_type=f"chat_{self._session_id}",
            )

    def clear(self) -> None:
        recent = self._memory.recall_by_type(f"chat_{self._session_id}", 1000)
        for msg in recent:
            self._memory.forget(msg["id"])


__all__ = ["LightningVectorStore", "LightningChatMemory"]
