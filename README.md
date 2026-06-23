# LightningDB

A graph+vector database server with Cypher queries, MVCC, and full-text search. Written in Rust.

## Status

**Pre-alpha.** The core engine compiles and passes 1000+ tests across 60+ test files, including a 90-test relationship traversal crucible (71 passing). Python, Node.js, and C bindings exist. HTTP server with 20+ endpoints works. Expect breaking changes.

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
- **Auth**: Three modes (`none`/`token`/`jwt`). JWT access + refresh tokens, API key authentication, RBAC (Reader/Writer/Admin), brute-force login lockout, per-IP rate limiting. Auth stored in DB system tables with WAL durability — no config file needed.
- **TLS/mTLS**: Optional TLS with configurable protocol versions (1.2/1.3) and mutual TLS

## What's Partial / Disabled / Missing

**Optimizer**: All 12 optimizer passes are enabled: SubqueryUnnesting, FilterPushDown, IndexPushDown, JoinReordering, TopKOptimizer, LimitPushDown, OrderByPushDown, ProjectionPushDown, AggKeyDependencyOptimizer, CountRelTableOptimizer, SemiJoinPushDown, AccHashJoinOptimizer, FactorizationRewriter, ForeignJoinPushDown. Projection pushdown has a known column-remapping gap for rel tables with more properties than their binder offset — this affects queries that RETURN specific rel properties together with node properties from multiple tables.

**Relationship traversal**: Variable-length path patterns (`*min..max`) work correctly. 15 tests remain failing: 7 need `shortestPath()` implementation, others need deeper optimizer/planner work (column remapping, join semantics for self-loops, OptionalMatch).

**Parser gaps**: `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SEQUENCE`, `CREATE MACRO` exist as AST nodes but have no PEG grammar rules — they cannot be parsed from Cypher text.

**Compression**: `FixedFrameOfReference` type exists in the enum but has no implementation module. BooleanBitpacking type exists but has no implementation file. IVF index module exists but is not wired into the build.

**Auth is stored in core DB system tables** (`__auth_users`, `__auth_refresh_tokens`, `__auth_api_keys`) with full MVCC, WAL durability, and transactional consistency. Tokens are hard-deleted on revocation (no soft-delete). A bloom filter + revoked HashMap provides fast-path validation without hitting the DB. Expired tokens are purged by a background GC task every 5 minutes. Login rate limiting (5 fails / 15 min → 15 min lockout) is in-memory only — resets on restart.

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

Flags: `--port` (default 8080), `--tls-enabled`, `--tls-cert`, `--tls-key`, `--tls-min-version`, `--tls-max-version`, `--auth-mode` (`none`|`token`|`jwt`), `--admin-username` (default `admin`), `--admin-password` (generated if not set), `--jwt-secret` (generated if not set; supports `@file` prefix), `--jwt-access-ttl` (default 900s), `--jwt-refresh-ttl` (default 2592000s / 30 days), `--buffer-pool-size`.

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

### 3. Authentication

Three auth modes, set via `--auth-mode`:

| Mode | Description |
|---|---|
| `none` | All requests are anonymous (admin role). No tokens needed. |
| `token` | API key authentication only. Clients send `Authorization: Bearer <api-key>`. |
| `jwt` | Full auth: JWT access tokens + refresh tokens + API keys. Login required. |

**Start in JWT mode** (password auto-generated if not set):
```bash
cargo run -p lightning-server -- --data-dir /tmp/lightning-data --auth-mode jwt
# Look for the generated admin password in the terminal output.
```

**Start in JWT mode with explicit credentials:**
```bash
cargo run -p lightning-server -- --data-dir /tmp/lightning-data \
  --auth-mode jwt --admin-password my-password --jwt-secret $(openssl rand -base64 32)
```

**Login and query:**
```bash
LOGIN=$(curl -s -X POST http://localhost:8080/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username": "admin", "password": "my-password"}')
TOKEN=$(echo "$LOGIN" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
REFRESH=$(echo "$LOGIN" | python3 -c "import sys,json; print(json.load(sys.stdin)['refresh_token'])")

curl -X POST http://localhost:8080/v1/query \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n:Person) RETURN n.name"}'
```

**Refresh tokens** (when access token expires):
```bash
NEW_TOKEN=$(curl -s -X POST http://localhost:8080/v1/auth/refresh \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\": \"$REFRESH\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
```

**Logout** (revoke the refresh token):
```bash
curl -s -X POST http://localhost:8080/v1/auth/logout \
  -H 'Content-Type: application/json' \
  -d "{\"refresh_token\": \"$REFRESH\"}"
```

**Create and use an API key** (does not expire):
```bash
# Replace <user_id> with the actual user ID (e.g., "u_xxx" from user listing)
APIKEY=$(curl -s -X POST http://localhost:8080/v1/admin/users/<user_id>/keys \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name": "my-key"}' \
  | python3 -c "import sys,json; k = json.load(sys.stdin); print(k['key'])")

curl -X POST http://localhost:8080/v1/query \
  -H "Authorization: Bearer $APIKEY" \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n:Person) RETURN n.name"}'
```

**Manage users** (admin only):
```bash
# List users
curl -s http://localhost:8080/v1/admin/users \
  -H "Authorization: Bearer $TOKEN"

# Create a reader user
curl -s -X POST http://localhost:8080/v1/admin/users \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "alice-pw", "role": "Reader"}'

# Update user role
curl -s -X POST http://localhost:8080/v1/admin/users/u_xxx \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"role": "Writer"}'

# Delete user
curl -s -X DELETE http://localhost:8080/v1/admin/users/u_xxx \
  -H "Authorization: Bearer $TOKEN"

# List API keys for a user (replace user_id)
curl -s http://localhost:8080/v1/admin/users/u_xxx/keys \
  -H "Authorization: Bearer $TOKEN"

# Revoke an API key (replace user_id and key_id)
curl -s -X DELETE http://localhost:8080/v1/admin/users/u_xxx/keys/k_xxx \
  -H "Authorization: Bearer $TOKEN"

# Reset a user's password
curl -s -X POST http://localhost:8080/v1/admin/users/u_xxx/reset-password \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"new_password": "new-secure-password"}'
```

**Python:**
```bash
# Install from source:
cd python && pip install .
```

```python
from lightning import Client, ClientConfig

# No auth
client = Client(ClientConfig(base_url="http://localhost:8080"))

result = client.query(
    "MATCH (n:Person) WHERE n.age > $th RETURN n.name, n.age",
    params={"th": 25},
)
for row in result:
    print(row["n.name"], row["n.age"])

# With JWT or API key auth
auth_client = Client(ClientConfig(
    base_url="http://localhost:8080",
    auth_token="<access_token_or_api_key>",
))
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

// With JWT auth (auto-login, auto-refresh on expiry)
const authClient = new Client('http://localhost:8080', {
  auth: { username: 'admin', password: 'secret' }
});
await authClient.login();

const result = await authClient.query(
  'MATCH (n:Person) WHERE n.age > $th RETURN n.name, n.age',
  { th: 25 }
);

// With API key
const keyClient = new Client('http://localhost:8080', {
  auth: { apiKey: 'ldk_abc123...' }
});
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
| `/v1/admin/users/{id}` | POST | Update user role/enabled (admin) |
| `/v1/admin/users/{id}` | DELETE | Delete user (admin) |
| `/v1/admin/users/{id}/keys` | GET | List API keys for a user (admin) |
| `/v1/admin/users/{id}/keys` | POST | Create API key for a user (admin) |
| `/v1/admin/users/{user_id}/keys/{key_id}` | DELETE | Revoke an API key (admin) |
| `/v1/admin/users/{id}/reset-password` | POST | Reset user password (admin) |
| `/metrics` | GET | Prometheus metrics |
| `/v1/subscribe` | GET | WebSocket CDC subscription |

## Test Coverage (60+ test files, 1000+ tests)

| Suite | Scope |
|---|---|
| Relationship traversal crucible (87 tests) | Single/multi-hop, variable-length, shortest-path, self-loops, cycles, CSR, cross-table, rel properties, WHERE filters, aggregation, persistence, concurrency (71 pass) |
| Comprehensive (13 files) | End-to-end CRUD, DML, DDL, queries, schema evolution, transactions |
| Join operators | Hash join, intersect, semi-mask, union |
| Fuzz, torture, crash recovery | Random queries, WAL replay, concurrency, memory pressure |
| Optimizer | Correctness of all 12 optimizer passes |
| Expression, function, date | Scalar evaluation, type handling |
| Contains, flatten, merge, unwind | Individual operator tests |
| Vector search | Flat + HNSW insert/query accuracy |
| Full-text search | BM25 indexing and search |
| Auth & security | JWT, API keys, RBAC, login rate limiting |
| WASM UDFs | WAT module execution with fuel metering |
| CDC | WAL-polling subscriber channels |
| Benchmark suite | Insert/scan/filter throughput, SQLite comparison |
| Lightning vs SQLite | Cross-validation of query results |

## Known Limitations

- Variable-length path traversal with column remapping (rel tables with more properties than binder offset) has a known gap affecting RETURN of specific rel properties together with node properties from multiple tables.
- `shortestPath()` and `allShortestPaths()` functions are not yet implemented.
- `CREATE VECTOR INDEX`, `CREATE FULLTEXT INDEX`, `CREATE SEQUENCE`, `CREATE MACRO` cannot be parsed from Cypher text (no grammar rules).
- Window functions, list indexing (`list[0]`), and map literal expressions are not supported in Cypher.
- `OptionalMatch` is parsed but not yet planned/executed.
- Python and Node.js HTTP client SDKs are not published to PyPI/npm — install from source.
- WASM functions must be re-registered after each database restart (no persistence).
- Login rate limiting (5 fails / 15 min) is in-memory only — resets on restart.
- No multi-tenant isolation in the HTTP server.
- No officially published Docker image (Dockerfile builds from source).
- IVF index module exists but is not wired into the build.

## Acknowledgments

LightningDB began as a fork of [KuzuDB](https://kuzudb.com/), a graph database. KuzuDB's columnar storage design, MVCC architecture, and Cypher query engine were the direct inspiration for this project. The codebase has since been substantially rewritten and extended with vector search, full-text indexing, WASM UDFs, time-travel queries, and an HTTP server layer, but KuzuDB's foundational design decisions remain visible throughout the storage engine and query planner.

## License

Business Source License 1.1 (BUSL-1.1) — see [LICENSE](LICENSE).

You may use LightningDB for non-production purposes (testing, evaluation, research) and for production use as long as you do not offer it as a Database Service. Upon 2030-06-15, the project automatically converts to MIT.
