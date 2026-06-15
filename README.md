# LightningDB

A graph+vector database server with Cypher queries, MVCC, and full-text search. Written in Rust.

## Status

**Pre-alpha.** The core engine compiles and passes 400+ tests. Python, Node.js, and C bindings exist. HTTP server with 20+ endpoints works. Expect breaking changes.

## What It Is

LightningDB is a standalone HTTP server that stores graph nodes and relationships in columnar pages with MVCC. It runs as a single binary or Docker container — deploy it anywhere, access it from any language via HTTP.

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
- **Auth**: JWT access/refresh tokens, API keys, RBAC (Reader/Writer/Admin), brute-force login protection
- **TLS/mTLS**: Optional TLS with configurable protocol versions (1.2/1.3) and mutual TLS

## What's Partial / Disabled / Missing

**Optimizer passes — 6 exist but are DISABLED** (commented out in the optimizer pipeline):
- ProjectionPushdown, SemijoinPushdown, AccHashJoinOptimizer, AggKeyDependencyOptimizer, CountRelTableOptimizer, IndexPushDown
- All have documented bugs or lifecycle issues. Only 6 passes are active: SubqueryUnnesting, FilterPushDown, JoinReordering, TopKOptimizer, LimitPushDown, OrderByPushDown.

**Parser gaps**: `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SEQUENCE`, `CREATE MACRO` exist as AST nodes but have no PEG grammar rules — they cannot be parsed from Cypher text.

**Compression**: `FixedFrameOfReference` type exists in the enum but has no implementation module. BooleanBitpacking type exists but has no implementation file. IVF index module exists but is not wired into the build.

**Auth is stored in core DB system tables** (`__auth_users`, `__auth_refresh_tokens`, `__auth_api_keys`) with full MVCC, WAL durability, and transactional consistency. Tokens are hard-deleted on revocation (no soft-delete). A bloom filter + revoked HashMap provides fast-path validation without hitting the DB. Expired tokens are purged by a background GC task every 5 minutes.

## Crates

| Crate/Package | Description |
|---|---|---|
| `lightning-types` | Shared type definitions (LogicalType, etc.) |
| `lightning-core` | Core engine: parser, planner, optimizer, processor, storage, MVCC, WASM, CDC, C FFI |
| `lightning-arrow` | Arrow integration helpers |
| `lightning` | Top-level Rust driver crate. Re-exports from core. |
| `lightning-server` | Axum HTTP server with JWT auth, RBAC, TLS/mTLS |
| `@lightningDB/client` | Node.js/TypeScript HTTP client SDK |
| `lightning` (Python) | Python HTTP client SDK (sync + async) |

## Quickstart

### 1. Start the server

```bash
# From source:
cargo run -p lightning-server -- --data-dir /tmp/lightning-data

# Or build the Docker image first:
docker build -t lightningdb . && docker run -p 8080:8080 -v ./data:/data lightningdb
```

Flags: `--port` (default 8080), `--tls-enabled`, `--tls-cert`, `--tls-key`, `--tls-min-version`, `--tls-max-version`, `--auth-mode` (`none`|`token`|`jwt`), `--auth-admin-password`, `--jwt-secret`, `--buffer-pool-size`.

### 2. Run queries

**curl (no auth):**
```bash
curl -X POST http://localhost:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "CREATE (n:Person {name: $name, age: $age}) RETURN n.id"}'
```

```bash
curl -X POST http://localhost:8080/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age ORDER BY n.age"}'
```

**curl (JWT auth):**
```bash
# Start with JWT auth enabled
cargo run -p lightning-server -- --data-dir /tmp/lightning-data \
  --auth-mode jwt --auth-admin-password my-secret-password --jwt-secret @/path/to/jwt-secret.txt

# Login
TOKEN=$(curl -s -X POST http://localhost:8080/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username": "admin", "password": "my-secret-password"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Authenticated requests
curl -X POST http://localhost:8080/v1/query \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n:Person) RETURN n.name"}'
```

**Python:**
```bash
# Install from source:
cd python && pip install .
```

```python
from lightning import Client

client = Client(base_url="http://localhost:8080")

# Graph query
result = client.query("MATCH (n:Person) WHERE n.age > $th RETURN n.name, n.age",
                      params={"th": 25})
for row in result:
    print(row["n.name"], row["n.age"])

# Memory/agent API
client.store(id="msg-1", content="Hello world", entity_type="note")
results = client.recall("hello", top_k=5)
print(results[0].content, results[0].score)
```

**TypeScript:**
```bash
# Install from source:
cd packages/lightning-client && npm install
```

```typescript
import { Client } from '@lightningDB/client';

// No auth
const client = new Client('http://localhost:8080');

// With TLS
const tlsClient = new Client('https://localhost:8443', {
  tls: { caCertPath: '/path/to/ca.pem' }
});

// With JWT auth
const authClient = new Client('http://localhost:8080', {
  auth: { username: 'admin', password: 'secret' }
});
await authClient.login(); // acquire JWT

const result = await authClient.query(
  'MATCH (n:Person) WHERE n.age > $th RETURN n.name, n.age',
  { th: 25 }
);

// Memory/agent API
await authClient.store('msg-1', 'Hello world', 'note');
const results = await authClient.recall('hello', 5);
```

**Rust (library):**
```toml
[dependencies]
lightning = { git = "https://github.com/lightningDB/lightning" }
```

```rust
use lightning::prelude::*;

let db = Database::open("/tmp/lightningdb").unwrap();
let conn = db.connect();

conn.execute_typed("CREATE (n:Person {name: 'Alice', age: 30})", None).unwrap();

let result = conn.execute_typed(
    "MATCH (n:Person) RETURN n.name, n.age", None
).unwrap();
for row in &result.rows {
    println!("{}: {}", row["n.name"], row["n.age"]);
}
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Client: Rust driver · Python SDK · Node.js SDK │
│         C FFI · curl / any HTTP client          │
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

## HTTP API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Server health check |
| `/v1/query` | POST | Run a Cypher query |
| `/v1/query/stream` | POST | Stream query results as NDJSON |
| `/v1/memory/store` | POST | Store a memory entity |
| `/v1/memory/store-batch` | POST | Batch store entities |
| `/v1/memory/recall` | POST | Semantic recall via hybrid search |
| `/v1/memory/recall-recent` | POST | Recall most recent entities |
| `/v1/memory/recall-by-type` | POST | Recall entities by type |
| `/v1/memory/forget` | POST | Delete a memory entity |
| `/v1/memory/decay` | POST | Decay memory scores |
| `/v1/memory/entity-history` | POST | Get entity version history |
| `/v1/memory/consolidate` | POST | Consolidate short-term → long-term |
| `/v1/graph/associate` | POST | Create an association between entities |
| `/v1/graph/expand` | POST | Expand from an entity via associations |
| `/v1/rag/query` | POST | RAG pipeline: retrieve + LLM prompt |
| `/v1/admin/checkpoint` | POST | Force a WAL checkpoint |
| `/v1/admin/vacuum` | POST | Run garbage collection |
| `/v1/auth/login` | POST | Authenticate, get JWT + refresh token |
| `/v1/auth/refresh` | POST | Rotate refresh token |
| `/v1/auth/logout` | POST | Revoke refresh token |
| `/v1/auth/me` | GET | Current user info |
| `/v1/admin/users` | GET | List users (admin) |
| `/v1/admin/users` | POST | Create user (admin) |
| `/v1/admin/users/:id` | PUT | Update user role (admin) |
| `/v1/admin/users/:id` | DELETE | Delete user (admin) |
| `/v1/admin/api-keys` | GET | List API keys (admin) |
| `/v1/admin/api-keys` | POST | Create API key (admin) |
| `/v1/admin/api-keys/:id` | DELETE | Revoke API key (admin) |
| `/metrics` | GET | Prometheus metrics |
| `/v1/subscribe` | GET | WebSocket CDC subscription |

## Test Coverage (32 test files, 400+ tests)

| Suite | Scope |
|---|---|
| Comprehensive (13 files) | End-to-end CRUD, DML, DDL, queries, schema evolution, transactions |
| Join operators | Hash join, intersect, semi-mask, union |
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
- Python and Node.js HTTP client SDKs are not published to PyPI/npm — install from source.
- WASM functions must be re-registered after each database restart (no persistence).
- Auth uses core DB system tables (MVCC, WAL) but login rate limiting is in-memory only (resets on restart).
- No multi-tenant isolation in the HTTP server.
- No officially published Docker image (Dockerfile builds from source).
- IVF index module exists but is not wired into the build.

## Acknowledgments

LightningDB began as a fork of [KuzuDB](https://kuzudb.com/), a graph database. KuzuDB's columnar storage design, MVCC architecture, and Cypher query engine were the direct inspiration for this project. The codebase has since been substantially rewritten and extended with vector search, full-text indexing, WASM UDFs, time-travel queries, and an HTTP server layer, but KuzuDB's foundational design decisions remain visible throughout the storage engine and query planner.

## License

Business Source License 1.1 (BUSL-1.1) — see [LICENSE](LICENSE).

You may use LightningDB for non-production purposes (testing, evaluation, research) and for production use as long as you do not offer it as a Database Service. Upon 2030-06-15, the project automatically converts to MIT.
