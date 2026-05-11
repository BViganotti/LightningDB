# Lightning

**Embedded graph+vector+hybrid database for AI agent memory.**

Lightning collapses what currently requires 3-4 separate services (vector DB + graph DB + full-text search + relational store) into a single embeddable binary. Built in Rust.

## Quickstart

### Python

```bash
pip install lightning-memory
```

```python
from lightning import MemoryStore

# Open or create a memory store
memory = MemoryStore("/tmp/my-memory")

# Store a memory
memory.store(
    id="msg-1",
    content="The user prefers Python over Rust",
    entity_type="preference",
)

# Recall by semantic similarity
results = memory.recall("what does the user prefer?", top_k=5)
for r in results:
    print(f"{r['content']} (score: {r['score']:.3f})")

# Recall with custom embedding
embedding = embedding_model.encode("python preference")
results = memory.recall_with_embedding("", embedding, top_k=5)

# Create relationships between memories
memory.associate("msg-1", "msg-2", "references", weight=0.9)

# Graph traversal: expand from a seed memory
context = memory.expand("msg-1", hops=2)

# Time-based recall
recent = memory.recall_recent(10)
by_type = memory.recall_by_type("preference", 10)
```

### LangChain

```python
from lightning.langchain import LightningVectorStore
from langchain_openai import OpenAIEmbeddings

store = LightningVectorStore(
    path="/tmp/memory",
    embedding=OpenAIEmbeddings(),
)
store.add_texts(["Hello world", "Goodbye world"])
results = store.similarity_search("hello", k=5)
```

### LlamaIndex

```python
from lightning.llama_index import LightningVectorStore
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader

vector_store = LightningVectorStore(path="/tmp/memory")
documents = SimpleDirectoryReader("./data").load_data()
index = VectorStoreIndex.from_documents(documents, vector_store=vector_store)
```

### Rust

```toml
[dependencies]
lightning-core = { git = "https://github.com/BViganotti/lightning" }
```

```rust
use lightning_core::{Database, SystemConfig};
use lightning_core::memory::{MemoryStore, MemoryEntity};

let db = Database::new("/tmp/memory", SystemConfig::default())?;
let conn = db.connect();
let memory = MemoryStore::new(conn);

memory.store(MemoryEntity {
    id: "msg-1".into(),
    entity_type: "preference".into(),
    content: "The user prefers Python over Rust".into(),
    created_at: 0,
    last_accessed: 0,
    access_count: 0,
    ttl_seconds: 0,
    metadata: "{}".into(),
})?;

let results = memory.recall("what does the user prefer?", &[], 5)?;
```

## Features

| Feature | Description |
|---------|-------------|
| **Graph model** | Native NODE/REL types with Cypher query support (`MATCH`, `CREATE`, `MERGE`, etc.) |
| **Columnar storage** | Compressed, page-based, with ALP/bitpacking/delta/RLE compression |
| **Vector search** | Exhaustive 768-dim float32 cosine similarity, parallel scan via Rayon |
| **Full-text search** | Tantivy-based BM25 for text fields |
| **Hybrid search** | Reciprocal Rank Fusion across vector + FTS results |
| **MVCC transactions** | Snapshot isolation, WAL durability, undo buffer for rollbacks |
| **CSR adjacency index** | Bidirectional compressed sparse row for O(1) graph traversal |
| **Arrow-native** | Zero-copy Apache Arrow interop via C Data Interface |
| **Memory schema** | Built-in Entity/Relates types for agent memory out of the box |
| **Memorization** | Automatic decay (TTL), importance scoring (PageRank), multi-hop expansion |

## Architecture

```
┌────────────────────────────────────────────┐
│              Agent Application              │
├────────────────────────────────────────────┤
│  Python / LangChain / LlamaIndex / Rust    │
├────────────────────────────────────────────┤
│               MemoryStore API              │
│   store · recall · expand · associate     │
├────────────────────────────────────────────┤
│    Cypher Query Engine (16 optimizer rules)│
├────────────────┬───────┬──────────────────┤
│  Graph (CSR)   │Vector │   FTS (Tantivy)  │
│  Bidirectional │Cosine │   BM25 scoring   │
│  O(1) neighbor │ 768d  │   Multi-field    │
├────────────────┴───────┴──────────────────┤
│         Columnar Storage Engine           │
│  MVCC · WAL · Compression · Buffer Pool   │
└────────────────────────────────────────────┘
```

## Status

Alpha stage. Active development. The core database engine is functional with 219+ passing tests.

Known limitations:
- Vector index is hardcoded to 768 dimensions
- WAL durability is configurable (Normal/Off) but defaults to synchronous
- Python bindings are new (PyO3)

## License

MIT
