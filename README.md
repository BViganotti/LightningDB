# LightningDB

An embedded graph+vector database with Cypher queries, MVCC, and full-text search. Written in Rust.

## Status

**Pre-alpha.** The core engine compiles and passes 400+ tests. Python, Node.js, and C bindings exist. HTTP server with 20+ endpoints works. Expect breaking changes.

## What It Is

LightningDB is a single-process embedded database that stores graph nodes and relationships in columnar pages with MVCC. It runs in-process (no server needed) or as a standalone HTTP server.

- **Graph model**: Node and relationship tables with Cypher MATCH/CREATE/SET/DELETE/MERGE
- **Vector search**: Flat cosine similarity (SIMD via auto-detected AVX2/SSE), configurable dimensions
- **Full-text search**: Tantivy-backed BM25 indexing
- **Hybrid search**: Reciprocal Rank Fusion (RRF) combining FTS + vector scores
- **Transactions**: MVCC with snapshot isolation, WAL durability (configurable sync), row-level merge-on-commit
- **Storage**: Columnar pages (4KB), configurable buffer pool, LRU eviction with CLOCK, learned Markov-chain prefetch
- **Compression**: ALP (float), bitpacking, delta, dictionary, RLE — auto-selected by an analyzer pass
- **Indexes**: Hash index (primary key), CSR (graph adjacency), HNSW (ANN), trigram (substring)
- **WASM UDFs**: Register WAT/WASM modules callable from Cypher, fuel-metered execution
- **CDC**: WAL-polling change data capture with subscriber channels
- **Time-travel**: Query the database at any prior MVCC timestamp via `execute_at()`

## What's Partial / Disabled / Missing

**Optimizer passes — 6 exist but are DISABLED** (commented out in the optimizer pipeline):
- ProjectionPushdown, SemijoinPushdown, AccHashJoinOptimizer, AggKeyDependencyOptimizer, CountRelTableOptimizer, IndexPushDown
- All have documented bugs or lifecycle issues. Only 6 passes are active: SubqueryUnnesting, FilterPushDown, JoinReordering, TopKOptimizer, LimitPushDown, OrderByPushDown.

**Parser gaps**: `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SEQUENCE`, `CREATE MACRO` exist as AST nodes but have no PEG grammar rules — they cannot be parsed from Cypher text.

**Compression**: `FixedFrameOfReference` type exists in the enum but has no implementation module. BooleanBitpacking type exists but has no implementation file. IVF index module exists but is not wired into the build.

**No access control, no auth, no TLS in default mode** (TLS is available via `--tls-enabled` flag in the server binary).

## Crates

| Crate | Description |
|---|---|
| `lightning-types` | Shared type definitions (LogicalType, etc.) |
| `lightning-core` | Core engine: parser, planner, optimizer, processor, storage, MVCC, WASM, CDC, C FFI |
| `lightning-arrow` | Arrow integration helpers |
| `lightning` | Top-level Rust driver crate. Re-exports from core. |
| `lightning-python` | Python bindings via PyO3 (20+ MemoryStore methods) |
| `lightning-node` | Node.js bindings via napi-rs (full API) |
| `lightning-server` | Axum HTTP server with 7 route groups |

## Quickstart

### Rust

```toml
[dependencies]
lightning = { git = "https://github.com/BViganotti/lightning" }
```

```rust
use lightning::prelude::*;

let db = Database::open("/tmp/lightning-db").unwrap();
let conn = db.connect();
let store = MemoryStore::new(conn, DEFAULT_EMBEDDING_DIM);

let entity = MemoryEntity {
    id: "msg-1".into(),
    entity_type: "note".into(),
    content: "User prefers Rust".into(),
    ..Default::default()
};
store.store(entity).unwrap();

let results = store.recall("rust", &[], 5).unwrap();
for r in &results {
    println!("{} (score={})", r.entity.content, r.score);
}
```

### Python

```bash
pip install lightning-memory
```

```python
from lightning import MemoryStore

store = MemoryStore("/tmp/lightning-db")
store.store(id="msg-1", content="Hello world", entity_type="note")
results = store.recall("hello", top_k=5)
print(results[0].content, results[0].score)
```

### HTTP Server

```bash
cargo run -p lightning-server -- --db-path /tmp/lightning-db --port 8080
```

```bash
curl -X POST http://localhost:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (e:Entity) RETURN e.id, e.content LIMIT 5"}'
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│   Client: Rust · Python · Node.js · C · HTTP    │
├─────────────────────────────────────────────────┤
│              Cypher Query Engine                 │
│  PEG Parser → Binder → Planner → 6 Optimizers   │
├──────────────────┬──────────────────────────────┤
│  Graph CSR       │  Vector HNSW/Flat + SIMD     │
│  FTS Tantivy     │  Trigram substring           │
│  Hash PK index   │  Compression (6 algos)       │
├──────────────────┴──────────────────────────────┤
│           Storage Engine                        │
│  WAL · 4KB pages · Buffer pool · MVCC · OCC    │
│  Learned prefetch · Free space mgmt · Vacuum    │
└─────────────────────────────────────────────────┘
```

## Test Coverage (32 test files, 400+ tests)

| Suite | Scope |
|---|---|
| Comprehensive (4 files) | End-to-end CRUD, DML, DDL, queries |
| Hash join, intersect, semi-mask, union | Join operator correctness |
| Fuzz, torture, crash recovery | Random queries, WAL replay, concurrency, memory pressure |
| Optimizer | Correctness of 6 active optimizer passes |
| Expression, function, date | Scalar evaluation, type handling |
| Contains, flatten, merge, unwind | Individual operator tests |
| Benchmark suite | Insert/scan/filter throughput, SQLite comparison |
| Lightning vs SQLite | Cross-validation of query results |

## Known Limitations

- 6 optimizer passes are disabled (documented bugs). Projection pushdown, semi-join pushdown, and several others do not run.
- `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SEQUENCE`, `CREATE MACRO` cannot be parsed from Cypher text (no grammar rules).
- Window functions, list indexing (`list[0]`), and map literal expressions are not supported in Cypher.
- Python and Node.js bindings are not published to PyPI/npm — install from source.
- WASM functions must be re-registered after each database restart (no persistence).
- No authentication, authorization, or multi-tenant isolation in the HTTP server.
- No Docker image.
- IVF index module exists but is not wired into the build.

## License

MIT
