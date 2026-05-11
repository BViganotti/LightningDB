from ._native import PyMemoryStore, LightningDatabase

class MemoryStore:
    """High-level memory store for AI agents.
    
    Wraps the native PyMemoryStore with additional convenience.
    """

    def __init__(self, path: str):
        self._store = PyMemoryStore.open(path)

    def store(
        self,
        id: str,
        content: str,
        entity_type: str = "memory",
        metadata: str = "{}",
    ):
        self._store.store(id, content, entity_type, metadata)

    def recall(self, query: str, top_k: int = 10):
        return self._store.recall(query, top_k)

    def recall_with_embedding(self, query: str, embedding: list[float], top_k: int = 10):
        return self._store.recall_with_embedding(query, embedding, top_k)

    def recall_recent(self, top_k: int = 10):
        return self._store.recall_recent(top_k)

    def recall_by_type(self, entity_type: str, top_k: int = 10):
        return self._store.recall_by_type(entity_type, top_k)

    def associate(self, src_id: str, dst_id: str, rel_type: str, weight: float = 1.0):
        self._store.associate(src_id, dst_id, rel_type, weight)

    def expand(self, entity_id: str, hops: int = 1):
        return self._store.expand(entity_id, hops)

    def forget(self, entity_id: str) -> bool:
        return self._store.forget(entity_id)

    def decay(self) -> int:
        return self._store.decay()

    def store_batch(self, entities: list[dict]) -> int:
        return self._store.store_batch(entities)


__all__ = ["MemoryStore", "PyMemoryStore", "LightningDatabase"]
