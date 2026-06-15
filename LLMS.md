# LightningDB — LLM Reference

> Auto-generated reference for LLM consumption. Concise, structured, accurate.
> For human docs see: README.md, ARCHITECTURE.md, CYPHER_REFERENCE.md, PERFORMANCE_TUNING.md, ROADMAP.md

## Project Identity

- **Name**: LightningDB (crate: `lightning`)
- **Type**: Graph+vector+hybrid database server
- **Language**: Rust
- **Status**: Pre-alpha (400+ tests passing)
- **License**: MIT
- **Monorepo path**: `/research/lightning/`

## What It Does

Single binary that replaces 4 services:
- **Graph DB** (Neo4j-like) — Cypher query language, NODE/REL tables, CSR adjacency index
- **Vector DB** (Pinecone-like) — SIMD dot product, 768-dim embeddings, parallel scan
- **Full-text search** (Elasticsearch-like) — Tantivy BM25, field-level scoring
- **Agent memory** — RAG pipeline, temporal queries, consolidation, CDC streaming

## Crate Structure

```
crates/
├── lightning-types/     # LogicalType, Value, StructField — shared enums
├── lightning-arrow/     # Arrow C Data Interface bridge (FFI_ArrowArray)
├── lightning-core/      # Core engine (storage, MVCC, Cypher, MemoryStore, Fusion)
│   └── lib.rs           # Database, Connection, SystemConfig, QueryResult
├── lightning/           # ★ RUST DRIVER — ergonomic wrapper, the crate users depend on
│   ├── lib.rs           # Prelude, re-exports
│   ├── database.rs      # Database: open/checkpoint/vacuum/WASM/metrics
│   ├── connection.rs    # Connection: query/execute/typed/JSON/DDL/bulk/transactions
│   ├── memory.rs        # MemoryStore: CRUD/hybrid search/RAG/graph/consolidation/CDC
│   ├── fusion.rs        # Fusion: code graph analysis, PageRank, D3 export
│   └── types.rs         # TypedQueryResult: Arrow → JSON rows
└── lightning-server/    # ★ HTTP SERVER — primary deployment mode (Axum, 20+ endpoints)
packages/
└── lightning-client/    # Node.js/TypeScript HTTP client SDK
python/
└── lightning/           # Python HTTP client SDK (sync + async)
```

## Public API Surface (lightning crate)

### Database
```rust
Database::open(path) -> Result<Database>
Database::open_with_config(path, SystemConfig) -> Result<Database>
Database::open_read_only(path) -> Result<Database>
db.connect() -> Connection
db.checkpoint() -> Result<()>
db.vacuum() -> Result<()>
db.register_wasm_function(path, func_name) -> Result<()>
db.metrics() -> &DatabaseMetrics       // query count, buffer hit rate, etc.
db.repair_cardinalities() -> Result<()>
db.path() -> &Path
db.inner() -> &Arc<CoreDatabase>
```

### Connection
```rust
conn.query(query) -> Result<QueryResult>              // raw Arrow batches
conn.execute(query, params) -> Result<QueryResult>    // with named params
conn.execute_at(query, snapshot_micros, params) -> Result<QueryResult>  // time-travel
conn.query_stream(query) -> Result<Receiver<Result<DataChunk>>>         // streaming
conn.execute_stream(query, params) -> Result<Receiver<Result<DataChunk>>>
conn.execute_typed(query, params) -> Result<TypedQueryResult>          // Arrow→JSON
conn.execute_json(query, params) -> Result<String>                     // serialized JSON
conn.execute_ddl(stmt) -> Result<()>                // DDL/DML no-return
conn.create_node_table(name, &[(col, type)], primary_key) -> Result<()>
conn.create_rel_table(name, from_table, to_table, &[(col, type)]) -> Result<()>
conn.drop_table(name) -> Result<()>
conn.bulk_insert_batch(table, &RecordBatch) -> Result<usize>
conn.fast_insert(table, Vec<Vec<(String, Value)>>) -> Result<usize>
conn.begin() -> Result<()>
conn.commit() -> Result<()>
conn.rollback() -> Result<()>
conn.client_context() -> &ClientContext    // query timeout, memory quota
conn.inner() -> &CoreConnection
```

### MemoryStore
```rust
MemoryStore::new(conn, embedding_dim) -> Self    // dim: 768 (default), 384, 1024
MemoryStore::from_connection(conn) -> Self       // uses DEFAULT_EMBEDDING_DIM

// Storage
store.ensure_schema() -> Result<()>
store.store(entity: MemoryEntity) -> Result<()>
store.store_batch(entities: Vec<MemoryEntity>) -> Result<usize>
store.forget(entity_id) -> Result<bool>          // soft-delete
store.decay() -> Result<usize>                   // prune expired TTL

// Search
store.recall(query_text, embedding, top_k) -> Result<Vec<SearchResult>>     // hybrid FTS+vector
store.recall_stream(query_text, embedding, top_k) -> Result<Receiver<...>>  // streaming
store.recall_by_type(entity_type, top_k) -> Result<Vec<MemoryEntity>>
store.recall_recent(top_k) -> Result<Vec<MemoryEntity>>           // newest first
store.recall_by_time(start_micros, end_micros, top_k) -> Result<Vec<MemoryEntity>>
store.recall_at_time(at_micros, top_k) -> Result<Vec<MemoryEntity>> // temporal snapshot
store.entity_history(entity_id) -> Result<Vec<MemoryEntity>>       // full version history

// RAG
store.rag_query(query, embedding, top_k) -> Result<RagResult>
store.rag_query_with_config(query, embedding, top_k, &RagConfig) -> Result<RagResult>

// Graph
store.expand(entity_id, hops, &[edge_types]) -> Result<Vec<MemoryEntity>>
store.associate(src_id, dst_id, rel_type, weight) -> Result<()>

// Consolidation
store.consolidate() -> Result<ConsolidationReport>   // auto-link + PageRank + contradictions

// Streaming & CDC
store.query_stream(query) -> Result<Receiver<Result<DataChunk>>>
store.execute_at(query, snapshot_micros) -> Result<QueryResult>
store.subscribe_changes() -> Result<mpsc::Receiver<ChangeEvent>>

// Utility
MemoryStore::now_micros() -> i64
store.inner() -> &CoreMemoryStore
```

### Fusion (code graph analysis)
```rust
Fusion::init_schema(&conn) -> Result<()>
Fusion::find_node_by_name(&conn, name) -> Result<Vec<String>>
Fusion::find_paths(&conn, src_id, tgt_id, &[edge_types]) -> Result<Vec<String>>
Fusion::find_connected_nodes(&conn, node_id, &[edge_types], ConnectedDirection) -> Result<Vec<String>>
Fusion::lookup_node_names(&conn, &[String]) -> Result<Vec<(id, name, node_type)>>
Fusion::add_observation(&conn, id, content, parent_id) -> Result<()>
Fusion::get_recent_observations(&conn, limit) -> Result<Vec<String>>
Fusion::compute_architecture_cohesion(&conn) -> Result<Vec<ModuleCohesion>>
Fusion::materialize_pagerank(&conn) -> Result<()>
Fusion::export_to_d3_json(&conn) -> Result<String>    // D3.js visualization JSON
```

### Key Types
```rust
// Config
SystemConfig { buffer_pool_size, max_num_threads, read_only, sync_mode, ... }
SyncMode::{Normal, Off}

// Errors
LightningError::{Internal, Database, Query, Io}
type Result<T> = std::result::Result<T, LightningError>;

// Results
QueryResult { column_names, column_types, batches: Vec<RecordBatch>, error }
TypedQueryResult { columns: Vec<String>, rows: Vec<Row>, num_rows }
// Row = serde_json::Map<String, serde_json::Value>

// Values
Value::{String, Number, Boolean, Null, Node, Relationship, Date, Timestamp, List, Struct, Map}

// Memory types
MemoryEntity { id, entity_type, content, created_at, last_accessed, access_count, ttl_seconds, metadata, valid_from, valid_until }
SearchResult { entity: MemoryEntity, score: f64 }
RagConfig { expansion_depth, search_weight, recency_weight, degree_weight, cross_encoder_wasm }
RagResult { context, sources, total_sources, query }
ConsolidationReport { links_created, contradictions_found, total_entities }
ChangeEvent { timestamp, bytes_written, total_wal_bytes, entity_id, operation_type }
```

## Cypher Query Language

Lightning implements a significant Cypher subset. Supported:

**Read**: `MATCH (n:Label)`, `WHERE`, `RETURN`, `RETURN DISTINCT`, `ORDER BY`, `SKIP`, `LIMIT`, `OPTIONAL MATCH`, variable-length paths `[*1..3]`, aggregations (COUNT, SUM, AVG, MIN, MAX, COLLECT, COLLECT_DISTINCT, MEDIAN, STDDEV, VARIANCE, GROUP_CONCAT), subqueries (EXISTS), `CASE WHEN`

**Write**: `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `DETACH DELETE`, `SET n = {...}`, `SET n += {...}`

**DDL**: `CREATE NODE TABLE`, `CREATE REL TABLE`, `DROP TABLE`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN), `IF NOT EXISTS` / `IF EXISTS`, `CREATE INDEX`

**Expressions**: IS NULL, IS NOT NULL, IN, NOT IN, XOR, CAST, EXTRACT

**Functions**: 100+ scalar (string, math, date, hash, JSON, list, map, bit, conversion), 12+ aggregates (COUNT, SUM, AVG, MIN, MAX, COLLECT, MEDIAN, STDDEV, VARIANCE, GROUP_CONCAT, COLLECT_DISTINCT, COUNT_DISTINCT), IFNULL, NULLIF, IF/IIF

**Procedures**: `CALL show_tables()`, `CALL db.labels()`, `CALL db.schema()`, `CALL db.relationshipTypes()`

**Not yet supported**: Window functions (ROW_NUMBER, RANK, OVER), list indexing/slicing syntax, map/struct literal expressions, variable-length path aggregation (`RETURN p` returns raw IDs, not structured paths)

## Index Types

| Index | Storage | Use |
|---|---|---|
| CSR (Compressed Sparse Row) | File pairs (offset + adjacency) | Bidirectional graph traversal, O(1) neighbor lookup |
| Hash Index | B-tree pages | Primary key lookups |
| Vector Index | Flat pages, SIMD dot product (AVX2/SSE) | Exhaustive parallel vector search |
| FTS (Tantivy) | Tantivy directory | BM25 full-text search |
| Trigram Index | In-memory, rebuilt on startup | Substring/fuzzy matching |

## Storage Engine

- **Columnar**: Each column → separate file (`<table>_<col>.lbug`)
- **Page size**: 4096 bytes
- **Buffer pool**: Sharded (16 shards), CLOCK eviction, Markov-chain learned prefetch
- **MVCC**: Snapshot isolation, page-level versioning, row-level merge-on-commit
- **WAL**: Write-ahead log with CRC32 checksums, configurable fsync
- **Compression codecs**: ALP (float), Bitpacking (int), Delta, Dictionary, RLE, Constant — fully activated with automatic analysis
- **String storage**: ≤63 chars inline (64B slot), >63 chars → overflow file

## File Layout

```
<db_path>/
├── database.header       # magic bytes, version, last_checkpoint_ts
├── wal.lbug              # write-ahead log
├── catalog.lbug          # table schemas and metadata
├── free_space.bin         # free page tracker
├── <table>_<col>.lbug    # column data
├── <table>_<col>_null.lbug       # null bitmap
├── <table>_<col>_overflow.lbug   # overflow strings
├── <table>_fts/          # Tantivy FTS index
├── <table>_vector.lbug   # vector embeddings
├── <table>_fwd_{offset,adj}.lbug  # CSR forward index
├── <table>_bwd_{offset,adj}.lbug  # CSR backward index
└── <table>_pk_index.lbug # hash index
```

## SystemConfig Defaults

```rust
SystemConfig {
    buffer_pool_size: 1GB,
    max_num_threads: 0,          // auto-detect (num_cpus)
    read_only: false,
    sync_mode: SyncMode::Normal,
    vacuum_interval_ms: 1000,
    prefetch_enabled: true,
    prefetch_depth: 2,
    prefetch_confidence: 0.15,
    slow_query_threshold_ms: 100,
}
```

## Build Commands

```bash
# Build all crates
cargo build --release

# Build only the Rust driver
cargo build -p lightning --release

# Run all tests
cargo test --release

# Run lightning crate tests only
cargo test -p lightning --release

# Build HTTP server (primary deployment)
cargo build -p lightning-server --release

# Build Node.js client SDK
cd packages/lightning-client && npm install && npm run build

# Build docs
cargo doc -p lightning --no-deps --open
```

## Key Architectural Patterns

1. **Arc\<Database\> everywhere**: Database is behind `Arc`, connections clone the Arc
2. **Connection per thread**: Create one Connection per thread for multi-threaded access
3. **Auto-commit by default**: Queries without explicit `begin()` auto-commit
4. **MVCC timestamps**: All data versioned by `read_ts` / `commit_ts` — enables time-travel
5. **Plan caching**: Normalized query strings key into `plan_cache` HashMap
6. **Streaming via crossbeam**: `query_stream()` returns `crossbeam::channel::Receiver`
7. **MemoryStore tables**: Uses `Entity` (NODE) and `Relates` (REL) tables internally
8. **Hybrid search via RRF**: Reciprocal Rank Fusion merges FTS + vector results
9. **Soft delete**: `forget()` sets `valid_until` timestamp; `decay()` prunes by TTL
10. **WASM isolation**: Each UDF call creates a new `wasmi` Store instance

## Known Limitations (Pre-Alpha)

- No window functions (ROW_NUMBER, RANK, OVER, LAG/LEAD, etc.)
- No sorted aggregation (GROUP BY uses hash-based, no sort-based fallback)
- List indexing/slicing syntax: `list[0]`, `list[1..3]` not yet exposed in Cypher
- Map/struct literal expressions: `{key: value}` not supported in RETURN clauses
- Variable-length path aggregation: `RETURN p`, `nodes(p)` return raw IDs, not structured path objects
- Python HTTP client SDK not published to PyPI (install from source)
- Node.js HTTP client SDK not published to npm (install from source)
- String overflow file `write_string()` is a no-op (production writes use `append_to_overflow()` in column.rs)
