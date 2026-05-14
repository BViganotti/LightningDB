# Lightning

**Lightning is an embedded graph+vector+hybrid database for AI agent memory.**
It collapses what currently requires 3-4 separate services (vector DB + graph DB + full-text search + relational store) into a **single embeddable binary** written in Rust.

```python
from lightning import MemoryStore

memory = MemoryStore("/tmp/agent-memory")
memory.store(id="msg-1", content="User prefers Python", entity_type="preference")
results = memory.recall("python", top_k=10)      # Semantic keyword search
context = memory.expand("msg-1", hops=1)          # Graph traversal
report = memory.consolidate()                     # Auto-link related memories
snapshot = memory.recall_at_time(timestamp)        # Time-travel query
```

## Why Lightning?

| You need this | Instead of 3-4 services | Lightning does it |
|---|---|---|
| Store memories with relationships | Neo4j + Postgres | Graph NODE/REL tables |
| Semantic search | Pinecone + Weaviate | 768-dim vector index (SIMD + parallel) |
| Keyword search | Elasticsearch + Meilisearch | Tantivy FTS (BM25) |
| Hybrid search | Custom glue code | RRF fusion built-in |
| Memory consolidation | External pipeline | Auto-link + PageRank + contradiction detection |
| Time-travel queries | Snapshots + restore | `recall_at_time(t)` — native MVCC |
| RAG pipeline | LangChain + Chroma + custom | `rag_query()` — single call |
| Real-time change streaming | Kafka + Debezium | `subscribe_changes()` — WAL CDC |
| User-defined functions | External microservice | WASM inside Cypher queries |
| Streaming results | Batching + pagination | Channel-based `query_stream()` |
| All in one process | Docker Compose nightmare | Single embedded binary |

## Feature Comparison

| Capability | **Lightning** | SQLite | DuckDB | Neo4j | Pinecone | Kuzu | Chroma |
|---|---|---|---|---|---|---|---|
| **Embedded** | ✅ | ✅ | ✅ | ❌ | ❌ | ✅ | ✅ |
| **Graph model** | ✅ Native | ❌ | ❌ | ✅ Native | ❌ | ✅ Native | ❌ |
| **Cypher queries** | ✅ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ |
| **Vector search** | ✅ SIMD | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| **Full-text search** | ✅ Tantivy | ✅ FTS5 | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Hybrid (vector+FTS)** | ✅ RRF | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Columnar storage** | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ |
| **MVCC transactions** | ✅ Snapshot | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| **WAL durability** | ✅ Configurable | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| **Temporal queries** | ✅ MVCC-native | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **RAG pipeline** | ✅ Built-in | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Memory consolidation** | ✅ Auto-link | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **WAL change streaming** | ✅ CDC | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Streaming queries** | ✅ Channel | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **WASM functions** | ✅ User-defined | ❌ | ❌ | ✅ Plugin | ❌ | ❌ | ❌ |
| **SIMD acceleration** | ✅ AVX2/SSE | ❌ | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Learned cache prefetch** | ✅ Markov chain | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Python bindings** | ✅ PyO3 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **LangChain integration** | ✅ VectorStore | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ |
| **Arrow native** | ✅ Zero-copy | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| **Written in Rust** | ✅ | C | C++ | Java | Go | C++ | Python |

## Benchmarks (Release Mode, MacBook Pro M4)

### Insert Throughput

| Operation | Lightning | SQLite | Speedup |
|---|---|---|---|
| 10K rows (bulk) | **270K rows/s** | — | — |
| 100K rows (bulk) | **616K rows/s** | — | — |
| 100K rows, 4 columns | **1.24M rows/s** | — | — |
| 10K large strings | **145K rows/s** | — | — |
| 10K rows (existing test) | **241K rows/s** | 17K rows/s | **14× faster** |
| 20K rows (existing test) | **125K rows/s** | 28K rows/s | **4.5× faster** |

### Query Latency

| Query | Lightning |
|---|---|
| Full scan, 20K rows | **810K rows/s** |
| Filtered scan (WHERE), 20K rows | **320K rows/s** |
| Aggregate (COUNT, AVG, MAX), 20K rows | **38 ops/s**¹ |
| Point lookup by PK, 100K rows | **128 ops/s**¹ |
| Filter by indexed column, 100K rows | **109 ops/s**¹ |
| Graph 1-hop traversal, 1000 nodes | **152 ops/s**¹ |

*¹ Single-shot queries, not throughput benchmarks*

### Disk Usage

| Data | Size | Per Row |
|---|---|---|
| 50K rows (int + string + float) | 4 MB | **97 bytes/row** |

### Stability

| Test | Duration | Result |
|---|---|---|
| 10K mixed ops continuous | ~150s | **0 errors** |
| 100 K bulk insert | 0.16s | **All verified** |
| 500 ops under memory pressure (256-page pool) | 5.6s | **All rows intact** |
| WAL replay (clean + unclean restart) | 3 phases | **All data survives** |
| WAL corruption (bitflip) | 3.5s | **Checkpointed data intact** |
| 20 concurrent schema create/drop cycles | 2.8s | **Base table intact** |
| 10 threads × 100 concurrent writes | 11.6s | **All values match** |
| 4 threads × 25 rollback transactions | 1.9s | **X=Y counts match** |
| 5 threads, 5 types, 5 tables | 3.6s | **0 MVCC conflicts** |

## Quickstart

### Python
```bash
pip install lightning-memory
```

```python
from lightning import MemoryStore

memory = MemoryStore("/tmp/agent-memory")

memory.store(id="msg-1", content="User prefers Python", entity_type="preference")
memory.store(id="msg-2", content="User works at a fintech company", entity_type="fact")

# Graph traversal
memory.associate("msg-1", "msg-2", "related_to", 0.9)
neighbors = memory.expand("msg-1", hops=1, edge_types=["related_to"])

# Hybrid search (FTS + keyword fused via RRF)
results = memory.recall("python preference", top_k=5)

# Temporal query — what did the agent know yesterday?
snapshot = memory.recall_at_time(yesterday_micros, top_k=100)

# RAG pipeline — returns LLM-ready context
rag = memory.rag_query("user background and preferences", top_k=5)
print(rag.context)

# Memory consolidation — auto-link + contradict + PageRank
report = memory.consolidate()
print(f"Linked {report.links_created}, contradictions {report.contradictions_found}")

# Time-based recall
recent = memory.recall_recent(50)
by_type = memory.recall_by_type("preference", 10)

# Change data capture — subscribe to real-time changes
for event in memory.subscribe_changes():
    print(f"Memory changed: +{event.bytes_written} bytes")
```

### LangChain
```python
from lightning.langchain import LightningVectorStore
from langchain_openai import OpenAIEmbeddings

store = LightningVectorStore(path="/tmp/memory", embedding=OpenAIEmbeddings())
store.add_texts(["Hello world", "Goodbye world"])
results = store.similarity_search("hello", k=5)
```

### LlamaIndex
```python
from lightning.llama_index import LightningVectorStore
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader

vector_store = LightningVectorStore(path="/tmp/memory")
index = VectorStoreIndex.from_documents(documents, vector_store=vector_store)
```

### Rust

```toml
[dependencies]
lightning-core = { git = "https://github.com/BViganotti/lightning" }
```

```rust
use lightning_core::{Database, SystemConfig};
use lightning_core::memory::{MemoryEntity, MemoryStore, RagResult};

let db = Database::new("/tmp/memory", SystemConfig::default())?;
let conn = db.connect();
let memory = MemoryStore::new(conn);

memory.store(MemoryEntity {
    id: "msg-1".into(),
    entity_type: "preference".into(),
    content: "User prefers Python".into(),
    created_at: 0, last_accessed: 0,
    access_count: 1, ttl_seconds: 0,
    metadata: "{}".into(),
    valid_from: 0, valid_until: 0,
})?;

// RAG pipeline
let rag: RagResult = memory.rag_query("user background", &[], 5)?;
println!("{}", rag.context);

// Streaming query
let rx = memory.query_stream("MATCH (e:Entity) RETURN e.id, e.content")?;
while let Ok(Ok(chunk)) = rx.recv() {
    println!("Got chunk: {} rows", chunk.batch.num_rows());
}
```

## Unique Features

### Built-in RAG Pipeline
Single `rag_query()` call does: hybrid search → graph expansion → multi-factor reranking → LLM-ready context assembly.

```python
rag = memory.rag_query("what does the user know about Rust?", top_k=5)
# Returns: context string, sources list, total_sources count
```

### Auto-temporal Versioning
Every modification uses MVCC timestamps. Query the database at ANY past moment:

```python
snapshot = memory.recall_at_time(micros_since_epoch, top_k=50)
```

No extra storage — uses Lightning's built-in MVCC commit timestamps.

### Memory Consolidation Pipeline
Automatically links related memories, detects contradictions, and identifies important entities via PageRank:

```python
report = memory.consolidate()
# Links created: 12, contradictions found: 2, entities processed: 100
```

### WAL Change Data Capture
Real-time streaming of every database mutation via channel subscription:

```python
for event in memory.subscribe_changes():
    print(f"WAL grew by {event.bytes_written} bytes")
```

### WebAssembly Functions
Register user-defined functions compiled from any WAT/WASM source, callable from Cypher queries:

```rust
db.register_wasm_function("/path/to/double.wat", "double")?;
// Then: RETURN WASM_double(t.val)
```

### Streaming Queries
Large result sets arrive as channels instead of buffered batches:

```rust
let rx = conn.query_stream("MATCH (e:Entity) RETURN e.id, e.content")?;
while let Ok(Ok(chunk)) = rx.recv() {
    // Process each chunk as it arrives
}
```

### Learned Cache Prefetching
The buffer manager learns page access patterns using a Markov-chain transition matrix and prefetches predicted pages:

```rust
SystemConfig {
    prefetch_enabled: true,      // default: true
    prefetch_depth: 2,           // prefetch top-2 predicted pages
    prefetch_confidence: 0.15,   // minimum probability to prefetch
    ..Default::default()
}
```

### SIMD-accelerated Vector Search
AVX2 FMA (8-wide) and SSE (4-wide) auto-detected dot product. Parallel exhaustive scan via Rayon.

### Row-level MVCC with Merge-on-Commit
Two transactions modifying different rows on the same page can both commit. Their changes are merged at commit time. Only true row-level conflicts (same row, different transactions) are rejected.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                 Agent Application                      │
├──────────────────────────────────────────────────────┤
│     Python / LangChain / LlamaIndex / Rust / C       │
├──────────────────────────────────────────────────────┤
│                   MemoryStore API                     │
│    store · recall · expand · associate · rag_query    │
│    consolidate · recall_at_time · subscribe_changes   │
├──────────────────────────────────────────────────────┤
│              Cypher Query Engine                      │
│    Parser (Pest) → Binder → Planner (16 opt rules)    │
├────────────┬────────────┬─────────────┬─────────────┤
│ Graph CSR  │ Vector SIMD │ FTS Tantivy │ Columnar    │
│ adjacency  │ Cosine 768d │ BM25 search │ Compressed  │
│ bidirectional│ Parallel  │ Multi-field │ MVCC buffer │
├────────────┴────────────┴─────────────┴─────────────┤
│                Storage Engine                         │
│  WAL · Buffer Pool · MVCC · Compression · Checkpoint │
│  Learned Prefetch · Row-level OCC · Free Space Mgmt  │
└──────────────────────────────────────────────────────┘
```

## Test Coverage

**298 tests, 0 failures** (release mode):

| Suite | Tests | Scope |
|---|---|---|
| Core engine | 223 | CRUD, DML, optimizer, planner, FTS, vector, compression |
| Memory store | 36 | Temporal queries, RAG, consolidation, CDC, WASM |
| Benchmarks | 14 | Insert, scan, filter, graph, memory, crash recovery |
| Fuzz | 17 | 200+ random query patterns, edge cases |
| Torture | 18 | WAL replay, memory pressure, concurrent schema, graph scale |
| Streaming | 4 | Stream cancel, empty results, recall stream |
| Bugfix | 3 | BOOL storage, string overflow, MVCC concurrency |

## Run the Agent Memory Demo

```bash
cargo run --example agent_memory --release
```

This exercises: store, hybrid search, graph traversal, temporal queries, RAG pipeline, consolidation, WAL CDC, streaming queries, and WASM functions — all on the same data.

## Status

Pre-alpha. The core engine is functional with 298 passing tests in release mode. Python bindings are new (PyO3). WASM runtime uses `wasmi` interpreter (no native compilation).

### Known Limitations
- Vector index is hardcoded to 768 dimensions
- WAL replay of DELETE operations has a StringArray null-buffer alignment issue
- Columnar compression is analyzed but not yet activated (always uncompressed)
- Python bindings not yet published to PyPI (install from source)

## License

MIT
