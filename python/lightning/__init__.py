from ._native import PyMemoryStore, LightningDatabase

class MemoryStore:
    def __init__(self, path: str):
        self._store = PyMemoryStore.open(path)

    def store(
        self,
        id: str,
        content: str,
        entity_type: str = "memory",
        metadata: str = "{}",
        embedding: list[float] | None = None,
    ):
        self._store.store(id, content, entity_type, metadata, embedding)

    def store_batch(self, entities: list[dict]) -> int:
        return self._store.store_batch(entities)

    def recall(self, query: str, top_k: int = 10, embedding: list[float] | None = None):
        return self._store.recall(query, top_k, embedding)

    def recall_with_embedding(self, query: str, embedding: list[float], top_k: int = 10):
        return self._store.recall_with_embedding(query, embedding, top_k)

    def recall_recent(self, top_k: int = 10):
        return self._store.recall_recent(top_k)

    def recall_by_type(self, entity_type: str, top_k: int = 10):
        return self._store.recall_by_type(entity_type, top_k)

    def associate(self, src_id: str, dst_id: str, rel_type: str, weight: float = 1.0):
        self._store.associate(src_id, dst_id, rel_type, weight)

    def expand(self, entity_id: str, hops: int = 1, edge_types: list[str] | None = None):
        return self._store.expand(entity_id, hops, edge_types)

    def forget(self, entity_id: str) -> bool:
        return self._store.forget(entity_id)

    def decay(self) -> int:
        return self._store.decay()

    def rag_query(self, query: str, top_k: int = 10):
        return self._store.rag_query(query, top_k)

    def recall_at_time(self, at_micros: int, top_k: int = 10):
        return self._store.recall_at_time(at_micros, top_k)

    def consolidate(self):
        return self._store.consolidate()

    def entity_history(self, entity_id: str):
        return self._store.entity_history(entity_id)

    def subscribe_changes(self):
        return self._store.subscribe_changes()


__all__ = ["MemoryStore", "PyMemoryStore", "LightningDatabase"]
