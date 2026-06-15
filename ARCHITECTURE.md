# Lightning Architecture

## Overview

Lightning is a **columnar, graph-native database server** with built-in vector search, full-text search, and AI agent memory capabilities. It runs as a standalone HTTP server or Docker container.

## Crate Map

| Crate | Purpose | Language |
|---|---|---|
| `lightning-types` | Common types (`LogicalType`, `Value`) shared across the stack | Rust |
| `lightning-arrow` | Arrow FFI bridge (C Data Interface for zero-copy interop) | Rust |
| `lightning-core` | Core engine: storage, MVCC, Cypher parser/planner/executor, MemoryStore | Rust |
| `lightning` | **Rust driver crate** — ergonomic public API wrapping `lightning-core` | Rust |
| `lightning-server` | HTTP server binary (Axum, 20+ endpoints) — the primary deployment mode | Rust |
| `@lightningDB/client` | Node.js/TypeScript HTTP client SDK | TypeScript |
| `lightning` (Python) | Python HTTP client SDK (sync + async) | Python |

## Storage Engine

### Columnar Layout
Each table column is stored as a separate file on disk:
```
<db_path>/<table>_<column>.lbug        — column data
<db_path>/<table>_<column>_null.lbug   — null bitmap (1 byte/row)
<db_path>/<table>_<column>_overflow.lbug — overflow (strings >63 chars)
```

### Buffer Manager
- Page size: 4096 bytes
- Sharded into 16 independent buffer pools for concurrency
- CLOCK eviction algorithm with referenced-bit second chance
- **Learned Cache Prefetching**: tracks page access patterns via Markov-chain transition matrix. After `min_observations` (3) accesses from a page, predicts the next pages and pre-reads them into the OS page cache via `pread`.

### Compression
The column stats module analyzes data distribution and selects compression. Implemented codecs:
- **ALP** (Adaptive Lossless Floating-Point) — for float64 columns
- **Bitpacking** — for integer columns with narrow range
- **Delta** — for sequential integers
- **Dictionary** — for low-cardinality strings
- **RLE** — for run-length encodable data
- **Constant** — for single-value columns

Compression is activated: `column.rs:optimize()` analyzes column data and applies the optimal codec. The compression metadata (`compression_meta`) is stored in column stats and reused on subsequent reads.

### MVCC (Multi-Version Concurrency Control)
- Snapshot isolation: each transaction sees a consistent snapshot at its `read_ts`
- **Note**: Lightning provides **Snapshot Isolation**, not Serializable. Write skew is possible (e.g., two transactions reading each other's pre-image and writing to disjoint rows). For Serializable isolation, see the roadmap SSI implementation.
- Page-level versioning: each page frame stores an atomic `version` field
- Uncommitted bit (bit 63): marks uncommitted versions
- `commit_ts` = current_ts + 1, stored in page version on commit
- Vacuum thread periodically reclaims versions < min_active_read_ts

### Row-Level OCC with Merge-on-Commit
Instead of page-level optimistic concurrency control (which rejects any concurrent modification to the same page), Lightning uses row-level conflict detection:

1. **Transaction records per-row modifications**: file_id, page_idx, row_id, element_size, raw_value_bytes
2. **On commit**: re-reads the latest committed page, applies only this transaction's row values on top
3. **Result**: two transactions modifying different rows on the same page can both commit

Row-level conflicts are detected by `RowVersion::mark_row` — if two transactions modify the same row, the second `mark_row` call detects the committed version and rejects the second writer.

### WAL (Write-Ahead Log)
- Records each page update with `(tx_id, file_id, page_idx, data_4096_bytes)`
- Commit records: `(tx_id)`
- Recovery: replays committed transactions' page updates after `last_checkpoint_ts`
- Sync modes: `Normal` (fsync on every commit), `Off` (kernel buffer only)
- Truncated after checkpoint (when data files are synced)

### Checkpoint
`Database::checkpoint()` atomically persists: dirty data pages + catalog + free space map + header. Called:
- Explicitly by the user
- On clean shutdown (`Drop`)
- After every DML auto-commit

## Query Engine

### Parser
Pest-based parser for a Cypher-compatible graph query language. Supports:
- `MATCH` (node patterns, relationship patterns, labels, properties)
- `RETURN` (projections, ORDER BY, SKIP, LIMIT, DISTINCT)
- `WHERE` (comparisons, boolean logic, string predicates, `IN` lists)
- `CREATE`, `MERGE`, `SET`, `DELETE`
- `UNWIND`, `UNION`
- `CALL` (procedures)
- Optional `OPTIONAL MATCH`
- Subqueries (`EXISTS`)

### Binder
Resolves table references, variable types, property names → column indices.
Type-checks expressions, resolves function signatures.

### Logical Planner
Produces `LogicalOperator` tree: Scan, Filter, Project, Join (HashJoin/CrossJoin), Aggregate, Sort, Limit, Flatten, Unwind, Create, Set, Delete, CreateRel, etc.

### Optimizer (16 rules)
| Rule | Description |
|---|---|
| Filter PushDown | Move filters closer to scans |
| Projection PushDown | Project only needed columns early |
| Join Reordering | Reorder join tree for selectivity |
| Limit PushDown | Apply LIMIT as early as possible |
| Order By PushDown | Use sort-order from indexes |
| SemiJoin PushDown | Push semijoin filters |
| Subquery Unnesting | Unnest correlated subqueries |
| Factorization Rewriter | Factor common subexpressions |
| TopK Optimizer | Specialized top-k sort+limit |
| Aggregate Key Dependency | Remove redundant aggregates |
| Foreign Join PushDown | Push joins into foreign scans |
| Index PushDown | Use hash index for equality filters |
| Count Rel Table Optimizer | Optimize count queries on rel tables |
| Acc Hash Join Optimizer | Accelerate hash joins |
| Cardinality Estimator | Estimate result sizes |

### Physical Planner
Converts logical operators to physical operators:
- `Scan` → `PhysicalScan` (with pushdown filter, projection, mask)
- `Filter` → `PhysicalFilter`
- `HashJoin` → `PhysicalHashJoin` (parallel build + probe)
- `Aggregate` → `PhysicalAggregate`
- `Sort` → `PhysicalSort`
- `Limit` → `PhysicalLimitSkip`

### Scheduler
Rayon-based work-stealing scheduler. Each operator tree is executed by a thread pool (`num_cpus::get()` workers). Results are pushed into a crossbeam channel — enabling streaming queries.

## Indexes

### CSR (Compressed Sparse Row)
Bidirectional adjacency index for graph edge traversal:
- Forward CSR: `node_id → [neighbor_ids]`
- Backward CSR: `node_id → [predecessor_ids]`
- Stored as two file pairs: offset array (prefix-sum) + adjacency array
- O(1) neighbor lookup: `offset[node_id]..offset[node_id+1]`
- Lazy rebuild: checks if cardinality changed since last build

### Hash Index
B-tree-like index for primary key lookups. Maps key → row_id.

### Hash-based vector search

### Search methods:
- **Vector**: exhaustive parallel scan with SIMD dot product (AVX2 FMA or SSE)
- **FTS**: Tantivy-based BM25 index with field-level scoring
- **Hybrid**: Reciprocal Rank Fusion (RRF) with configurable k
- **Trigram**: n-gram index for substring/fuzzy matching

## AI Agent Memory Features

### MemoryStore API
High-level Rust/Python API for agent memory:
- `store()` / `store_batch()` — persist memories with embeddings
- `recall()` — hybrid FTS+vector search with RRF fusion
- `recall_by_type()` / `recall_recent()` / `recall_by_time()` — filtered recall
- `expand()` — graph traversal via edge types
- `associate()` — create relationships
- `forget()` — soft-delete by setting `valid_until`
- `decay()` — prune expired TTL memories

### Temporal Queries (True MVCC Time-Travel)
- `recall_at_time(micros)` — true MVCC snapshot: uses `execute_at(snapshot_micros)` which creates a read transaction at `micros`. Shows exactly what was committed at that time, powered by the MVCC engine's existing version tracking. No application-level timestamp filtering.
- `entity_history(id)` — full version history of a memory
- Uses MVCC commit timestamps — no extra storage needed

### Built-in RAG Pipeline
`rag_query(text, embedding, k)` does:
1. Hybrid search via `recall()`
2. Graph expansion via `expand()` for context enrichment
3. Multi-factor reranking (search_score × recency)
4. LLM-ready context string assembly

### Memory Consolidation
`consolidate()` does:
1. Compute n-gram Jaccard similarity between all active entities
2. Create `RelatedTo` edges between similar entities (>35% overlap)
3. Detect `Contradicts` edges (low overlap but similar content length)
4. Run PageRank on the inferred graph
5. Mark top-10 important entities with PageRank scores

### WAL Change Data Capture
`subscribe_changes()` polls WAL file size on a background thread, pushes `ChangeEvent` structs into a crossbeam channel.

### Streaming Queries
`query_stream()` returns a crossbeam `Receiver<Result<DataChunk>>`. Results flow as they're produced by the parallel scheduler. Drop the receiver to cancel.

### WebAssembly Functions
`register_wasm_function(path, func_name)` loads a `.wasm` or `.wat` file, compiles it with the `wat` crate, and registers the exported function as a Cypher-callable scalar function. Each function call creates a new `wasmi` Store instance (fully isolated).

Learned Cache Prefetching
Default: enabled. Tracks every `pin_page()` access in a transition matrix.
- `record_access(file_id, page_idx)` — builds the Markov chain
- `predict_next(file_id, page_idx, top_k, min_confidence)` — predicts next pages
- `get_hot_pages(n)` — returns most frequently accessed pages
- Background speculative prefetch: reads predicted pages into OS page cache

## File Format

### Database Files
```
<db_path>/
  database.header    — magic bytes, version, last_checkpoint_ts
  wal.lbug           — write-ahead log
  catalog.lbug       — table schemas, column metadata, cardinalities
  free_space.bin     — free page tracker
  data.lbug          — shared data file (unused currently)
  overflow.lbug      — shared overflow file (unused currently)
  <table>_<col>.lbug — per-column data
  <table>_<col>_null.lbug — per-column null bitmap
  <table>_<col>_overflow.lbug — per-column overflow strings
  <table>_fts/       — Tantivy FTS index directory
  <table>_vector.lbug — vector embeddings
  <table>_fwd_offset.lbug — CSR forward offset array
  <table>_fwd_adj.lbug — CSR forward adjacency array
  <table>_bwd_offset.lbug — CSR backward offset array
  <table>_bwd_adj.lbug — CSR backward adjacency array
  <table>_pk_index.lbug — hash index for primary key
```

### Row Format
Each row is identified by an auto-incrementing `_id` (uint64). Columns are stored at offset `_id × element_size` within each column file.

### String Storage
- ≤63 chars: inline — `[length: 1B][data: length bytes]` in a 64-byte slot
- >63 chars: overflow — `[255: 1B][page_idx: 8B][offset: 8B][length: 4B]` in a 64-byte slot, actual data in overflow file page
