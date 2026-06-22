# LightningDB Usability Audit

> Generated: 2026-06-19
> Scope: Rust driver, Python driver, TypeScript driver, Cypher query language, HTTP server, documentation

---

## Executive Summary

**~100 issues** found across 6 areas. The most critical problems:

1. **WHERE clause silently returns 0 rows** on non-PK columns (filter pushdown bug)
2. **Python consolidation crashes** at runtime (camelCase/snake_case mismatch)
3. **README documents fictional APIs** in both CLI flags and TypeScript
4. **No package-specific docs** for Python or TypeScript clients
5. **Errors silently swallowed** in FTS/Vector index operations
6. **MemoryStore embedding dimensions silently mismatch** (768 vs 384)

---

## Table of Contents

1. [Rust Driver API](#1-rust-driver-api)
2. [Python Driver](#2-python-driver)
3. [TypeScript Driver](#3-typescript-driver)
4. [Cypher Query Language](#4-cypher-query-language)
5. [HTTP Server](#5-http-server)
6. [Documentation](#6-documentation)
7. [Remediation Plan](#7-remediation-plan)

---

## 1. Rust Driver API

### 1.1 `query()` vs `execute()` naming is misleading

**Files:** `crates/lightning/src/connection.rs:57-76`

In most DB APIs, `query` = read/SELECT, `execute` = write/INSERT. Here both accept any Cypher and both return `QueryResult`. The only difference is that `execute` takes optional params. This contradicts community convention.

### 1.2 `query_stream()` vs `execute_stream()` — same redundancy

**Files:** `crates/lightning/src/connection.rs:95-109`

`query_stream(query)` is literally `execute_stream(query, None)`. Duplicates the naming confusion.

### 1.3 `execute_ddl()` is misnamed (also runs DML)

**Files:** `crates/lightning/src/connection.rs:148-151`

Doc says "Run a raw DDL or DML statement" but method is named `execute_ddl`.

### 1.4 No convenience `execute()` overload for no-params

**Files:** `crates/lightning/src/connection.rs:74`

Users must always write `execute(query, None)` or use turbofish. `query()` serves this role but has a confusingly different name.

### 1.5 No `query_typed()` / `query_json()` convenience methods

**Files:** `crates/lightning/src/connection.rs:124-141`

`execute_typed()` and `execute_json()` exist but require `None` params. No no-params variants.

### 1.6 Streaming return type is extremely verbose

**Files:** `crates/lightning/src/connection.rs:95-109`

Returns `Result<crossbeam::channel::Receiver<Result<lightning_core::processor::DataChunk>>>` — 3-level nested generic with fully-qualified path. No type alias.

### 1.7 `execute_at()` parameter order and type inconsistency

**Files:** `crates/lightning/src/connection.rs:82-89`

- Timestamp is raw `u64` between query and params
- Parameter named `snapshot_micros` but no doc-comment on the parameter
- `MemoryStore::recall_by_time()` uses `i64` — inconsistent with `execute_at`'s `u64`

### 1.8 `fast_insert()` takes `Vec<Vec<(String, Value)>>` — awkward

**Files:** `crates/lightning/src/connection.rs:238`

Each row is `Vec<(String, Value)>`. A `HashMap<String, Value>` per row or `Vec<HashMap<String, Value>>` would be more natural.

### 1.9 `bulk_insert_batch()` requires `RecordBatch` import

**Files:** `crates/lightning/src/connection.rs:218-219`

Users must depend on `arrow` directly. No convenience for `Vec<HashMap>`.

### 1.10 `MemoryStore::new()` wastes its `Connection` parameter

**Files:** `crates/lightning/src/memory.rs:50-56`

Takes ownership of a `Connection`, immediately extracts `database` from it and discards the original. The connection's transaction state and client context settings are lost.

### 1.11 `MemoryStore::from_connection()` hardcodes 768-dim embedding

**Files:** `crates/lightning/src/memory.rs:59-65`

Hardcodes `DEFAULT_EMBEDDING_DIM` (768). If `SystemConfig` default `embedding_dim` (384) was used, the stores silently mismatch. **CRITICAL.**

### 1.12 `now_micros()` wraps `now_micros_for_test()` — misleading name

**Files:** `crates/lightning/src/memory.rs:237-239`

The core method is named `_for_test`, scaring users away from a perfectly valid utility.

### 1.13 `recall()` uses sentinel values instead of `Option`

**Files:** `crates/lightning/src/memory.rs:110-112`

- `embedding: &[]` for FTS-only (should be `Option<&[f32]>`)
- `query: ""` for vector-only (should be `Option<&str>`)

### 1.14 `ensure_schema()` not called eagerly

**Files:** `crates/lightning/src/memory.rs:75`

Neither `new()` nor `from_connection()` calls `ensure_schema()`. Errors surface late.

### 1.15 `MemoryEntity`, `SearchResult`, `ConsolidationReport`, `RagResult` lack `Serialize`

**Files:** `crates/lightning-core/src/memory.rs:62,104,1698,1708`

Users cannot easily serialize entities to JSON for API responses or logging.

### 1.16 `TypedQueryResult` missing ergonomic methods

**Files:** `crates/lightning/src/types.rs:14-140`

- No `to_json_value()` — only `to_json()` returning `String` (serialize-then-deserialize round trip)
- No `IntoIterator` — cannot `for row in &result`
- No `Display` — cannot `println!("{}", result)`

### 1.17 Arrow type fallback silently returns `null`

**Files:** `crates/lightning/src/types.rs:92-99`

Unrecognized Arrow types (e.g., `Decimal128`, `Duration`) are downcast to `StringArray`. If that fails, silently returns `null`. **CRITICAL — silent data loss.**

### 1.18 `LightningError::code()` uses string matching on error messages

**Files:** `crates/lightning-core/src/lib.rs:210-228`

```rust
if msg.contains("Variable") && msg.contains("not found") { ErrorCode::NotFound }
else if msg.contains("already exists") { ErrorCode::AlreadyExists }
else if msg.contains("syntax") || msg.contains("parse") { ErrorCode::SyntaxError }
```

Fragile — breaks if error messages are reworded. No structured error data.

### 1.19 `QueryResult::error` field creates dual error channel

**Files:** `crates/lightning-core/src/lib.rs:1089`

Errors can arrive via `Result<QueryResult>` wrapper OR `QueryResult { error: Some(...) }`. Users must check both.

### 1.20 `SystemConfig` lacks builder pattern — 12-field struct

**Files:** `crates/lightning-core/src/lib.rs:252-276`

Users who want to customize one field must construct the entire struct. No builder.

### 1.21 `Value::Number(f64)` is lossy for integer columns

**Files:** `crates/lightning-core/src/processor/mod.rs:154`

All numeric parameters go through `f64`. Lossy for integers > 2^53.

### 1.22 `Value::Map` uses `HashMap<Value, Value>` — unusual keys

**Files:** `crates/lightning-core/src/processor/mod.rs:164`

Map keys are `Value` enums instead of `String`. Must construct `Value::String("key".into())` for every key.

### 1.23 `connect_internal()` is public but documented as internal

**Files:** `crates/lightning/src/database.rs:66-69`

### 1.24 `DEFAULT_EMBEDDING_DIM` (768) vs `SystemConfig::default().embedding_dim` (384) silently conflict

**Files:** `crates/lightning/src/lib.rs:9`, `crates/lightning-core/src/lib.rs:292`

### 1.25 `SyncMode::Off` is semantically confusing

**Files:** `crates/lightning-core/src/lib.rs:240`

Sounds like "syncing is off" (dangerous). Should be `Async` or `Lazy`.

### 1.26 `max_num_threads: 0` magic value for auto-detect

**Files:** `crates/lightning-core/src/lib.rs:254,282`

Should be `Option<u32>` where `None` means auto-detect.

---

## 2. Python Driver

### 2.1 Consolidation response parsing crashes (camelCase vs snake_case)

**Files:** `_client.py:227,229`, `_async_client.py:216,218`, `_types.py:99-115`

```python
links = [LinkDetail(**l) for l in result["links"]]  # CRASHES
```

Server returns `sourceId`, `relType` (camelCase). `LinkDetail` expects `source_id`, `rel_type` (snake_case). Python dataclasses reject unexpected kwargs. **CRITICAL — always crashes at runtime.**

### 2.2 No `logout()` or `refresh_token()` methods

**Files:** `_client.py`, `_async_client.py`

Server has `/v1/auth/logout` and `/v1/auth/refresh`. Client supports `login()` but not the inverse operations.

### 2.3 No admin user management methods

**Files:** `_client.py`, `_async_client.py`

Server exposes 8 admin endpoints. Client has zero methods for any of them.

### 2.4 No README, no docstrings, no `py.typed`

**Files:** `python/` directory

Zero Python-specific documentation. No docstrings on any public method.

### 2.5 `recall()` does not validate query string

**Files:** `_client.py:136-143`, `_async_client.py:120-133`

`rag_query()` calls `validate_query_string()` but `recall()` does not.

### 2.6 Confusing `timeout_ms` vs `timeout` in `query()`

**Files:** `_client.py:320-326`, `_async_client.py:307-313`

Two parameters: `timeout_ms` (server-side query timeout) and `timeout` (HTTP timeout). Different types (`int` vs `Optional[float]`). Confusing.

### 2.7 `query_stream()` lacks `timeout` parameter

**Files:** `_client.py:348-357`, `_async_client.py:335-345`

All other methods accept `timeout`, but streaming query does not.

### 2.8 Dead code: `_access_token`/`_refresh_token` instance vars

**Files:** `_client.py:43-44,69-72`

Tokens stored in both instance vars and config. Instance vars are never read after `__init__`.

### 2.9 Dead code: `_validate_and_post()` defined but never called

**Files:** `_client.py:56-62`

### 2.10 Unused dataclasses: `StoreBatchResult`, `DecayResult`, `ConsolidationDetail`

**Files:** `_types.py:118-149`

Defined, exported, imported, but never instantiated.

### 2.11 `__all__` mismatch between package and subpackage

**Files:** `lightning/__init__.py` vs `lightning/client/__init__.py`

`lightning/__init__.py` missing 6 types vs `client/__init__.py` (`ConsolidationDetail`, `LinkDetail`, etc.)

### 2.12 No `__init__.py` in `python/tests/`

### 2.13 Local imports inside method body

**Files:** `_transport.py:150,346`

`import time as _time` (time already at module level); `import asyncio` inside method.

### 2.14 ~200 lines duplicated between `SyncTransport` and `AsyncTransport`

**Files:** `_transport.py:102-236 vs 298-432`

### 2.15 `associate()` does not validate `rel_type`

**Files:** `_client.py:241-255`

### 2.16 `rag_query()` weight params have no bounds validation

**Files:** `_client.py:274-316`

`search_weight`, `recency_weight`, `degree_weight` — no [0,1] bounds checking.

### 2.17 `consolidate()` parameters numerous and unvalidated

**Files:** `_client.py:200-237`

6 optional float/int params with no range checking.

### 2.18 Hardcoded version string in user_agent

**Files:** `_types.py:244`

Will drift from actual package version.

### 2.19 Wrong setuptools backend in pyproject.toml

**Files:** `pyproject.toml:3`

Uses `_legacy` backend instead of standard `setuptools.build_meta`.

### 2.20 No automatic token refresh on 401

**Files:** `_client.py:66-72`

Tokens expire at default 900s TTL. No auto-refresh mechanism.

### 2.21 `rag_query()` accesses `result["warnings"]` without `.get()`

**Files:** `_client.py:315` — will `KeyError` if server omits `warnings`.

### 2.22 `subscribe()` silently zeroes malformed events

**Files:** `_client.py:378-386`

All fields use `.get()` with defaults — no error on malformed events.

### 2.23 `SnapshotSelector` has no `to_dict()` method

**Files:** `_client.py:333-344`

Manual dict building duplicated in sync and async clients.

### 2.24 `Entity.from_dict()` silently defaults missing keys

**Files:** `_types.py:43-55`

All optional fields use `.get()` — no warning on missing critical fields.

---

## 3. TypeScript Driver

### 3.1 README shows fictional `Client(url)` API

**Files:** `README.md:216-230`

README shows `new Client('http://localhost:8080')` and `auth` config option. Actual API is `new LightningClient({baseUrl: 'http://127.0.0.1:8080'})` with separate `login(username, password)`. **CRITICAL — every user following README hits an immediate error.**

### 3.2 No README in the package itself

**Files:** `packages/lightning-client/`

Zero documentation, no JSDoc, no examples in the package.

### 3.3 No `logout()` method

**Files:** `src/client.ts`

Server has `POST /v1/auth/logout`. Client has no way to programmatically log out.

### 3.4 `query()` has two confusing timeout parameters

**Files:** `src/client.ts:532-538`

```typescript
query(query, params?, snapshotTsOrSelector?, timeoutMs = 30000, timeout?)
```

`timeoutMs` (4th, server-side) and `timeout` (5th, HTTP). Naming collision, confusing order, no JSDoc.

### 3.5 `maxConnections` and `maxKeepaliveConnections` are dead config

**Files:** `src/types.ts:10-11`, `src/client.ts:98-123`

Defined in types, accepted by constructor, but never read or used anywhere.

### 3.6 11 missing server API endpoints

**Files:** `src/client.ts`

No client methods for: `logout`, `auth/me`, `query/stream`, all 8 admin endpoints.

### 3.7 Dead variable assignment in `request()`

**Files:** `src/client.ts:238-240`

```typescript
const authToken = this.resolveAuth()  // assigned, NEVER used
const headers = this.headers(requestId)  // calls resolveAuth() again internally
```

Causes double `resolveAuth()` call per request.

### 3.8 `onRetry` telemetry always passes `delayMs = 0`

**Files:** `src/client.ts:308,331`

Computed backoff delay is never passed to the telemetry hook.

### 3.9 `login()` overwrites custom `authTokenProvider`

**Files:** `src/client.ts:148`

If user provides a custom `authTokenProvider` in options, calling `login()` silently overwrites it.

### 3.10 Token refresh can loop indefinitely

**Files:** `src/client.ts:284-305`

`_refreshAttempts` is reset to 0 on success. If refreshed token also returns 401, the guard passes again — potential infinite loop.

### 3.11 `checkCircuitBreaker(path)` accepts unused `path` parameter

**Files:** `src/client.ts:214-221`

`path` parameter is never used in the function body.

### 3.12 No documented minimum Node.js version

**Files:** `package.json`

No `engines` field. Uses `fetch`, `AbortController`, `performance.now()` — Node 16+ but undocumented.

### 3.13 Two disconnected error classes with no guidance

**Files:** `src/client.ts:31`, `src/validation.ts:1`

`LightningError` and `ValidationError` — both extend `Error` but are unrelated. Users must `catch` and `instanceof` twice.

### 3.14 `fs.readFileSync` inside dynamic `await import()`

**Files:** `src/client.ts:173-182`

Odd pattern — sync read after async import. Blocks event loop on first TLS request.

### 3.15 Zero HTTP integration tests

**Files:** `tests/client.test.ts`

Only 19 tests for validation, circuit breaker, and retry math. No tests for query, recall, store, auth, or any HTTP transport.

### 3.16 `subscribe()` has no per-call cancellation

**Files:** `src/client.ts:574-608`

Only way to stop is `client.close()` which cancels ALL in-flight requests.

### 3.17 `followRedirects` defaults to `false`

**Files:** `src/client.ts:109`

Breaks HTTP-to-HTTPS redirects, load balancers, reverse proxies.

### 3.18 `sleep(ms)` is actually `sleep(seconds)`

**Files:** `src/retry.ts:28-29`

```typescript
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms * 1000))  // ×1000!
}
```

Parameter named `ms` but multiplies by 1000. `sleep(500)` = 500 seconds.

### 3.19 Zero JSDoc/TSDoc in entire codebase

Every public method, interface, and parameter has zero documentation comments.

### 3.20 Mangled type name `RetryConfig$1` leaked to public API

**Files:** `dist/index.d.ts:239`

### 3.21 `caBundlePath` vs `caCertPath` — README uses different name

**Files:** `README.md:224` vs `src/types.ts:36`

### 3.22 Build step required after `npm install`

**Files:** `README.md:212`

`npm install` installs deps but doesn't build. User also needs `npm run build`.

---

## 4. Cypher Query Language

### 4.1 WHERE clause returns 0 rows on non-PK columns (FILTER PUSHDOWN BUG)

**Files:** `scan.rs:258-302,370-443,650-694`, `physical_plan.rs:83-86,161-178`

`MATCH (n:Table) WHERE n.non_pk_col = 'val' RETURN n.id` returns 0 rows when matching rows exist.

**Root cause:** Filter pushdown into `PhysicalScan` doesn't remap property lookup indices. The separate `PhysicalFilter` path (via `WITH...WHERE`) does.

**Workaround:** `MATCH (n:Table) WITH n WHERE n.non_pk_col = 'val' RETURN n.id`

**Not documented in any user-facing file.** **CRITICAL.**

### 4.2 Pest parser error messages shown raw to users

**Files:** `parser/mod.rs:12-18`, `lib.rs:1493`

Internal grammar rule names like `Rule::create_node_table`, `Rule::column_def` leak to users. Examples:
- `"Pest error: expected create_node_table"`
- `"Missing expected parser pair: pair"`
- `"empty statement"` (bare expression without MATCH/RETURN/CREATE)
- `"Pattern must have a node"`

No suggestions for correct syntax. No line/column pointers.

### 4.3 FTS/Vector errors silently swallowed

**Files:** `dml.rs:251,256,542-553,692-697`

```rust
tracing::warn!("FTS insert error for CREATE batch: {e}");
tracing::warn!("FTS commit error for CREATE batch: {e}");
```

Users won't know full-text indexes are corrupt. **CRITICAL — silent wrong results.**

### 4.4 `var_len_bounds` parse failure silently continues

**Files:** `parser/mod.rs:928-932`

When variable-length path bounds fail to parse, the error is logged and parsing continues with `None`. Users writing `[:REL *1..10]` with bad ranges get silently wrong matching.

### 4.5 `LightningError` has no `SyntaxError` variant

**Files:** `core/lib.rs:196-228`

Syntax errors classified as `Query` errors, detected by fragile substring matching:
```rust
if msg.contains("syntax") || msg.contains("parse") { ErrorCode::SyntaxError }
```

Not all parse error messages contain these keywords.

### 4.6 IN operator requires `[...]` not `(...)`

**Files:** `cypher.pest:118`

`WHERE n.prop IN [1, 2, 3]` works. `WHERE n.prop IN (1, 2, 3)` fails with confusing Pest error. Non-standard.

### 4.7 CREATE CONSTRAINT uses `REQUIRE` not `ASSERT`

**Files:** `cypher.pest:18`

Standard Cypher uses `ASSERT ... IS UNIQUE`. Lightning uses `REQUIRE ... IS UNIQUE`. `CYPHER_REFERENCE.md` documents `ASSERT` — mismatch.

### 4.8 Map assignment uses `:` not `=` inside `{}`

**Files:** `cypher.pest:83-84`

`SET n = {prop: val}` works (correct). `SET n = {prop = val}` fails with confusing error.

### 4.9 Multiple MATCH requires comma between patterns

**Files:** `cypher.pest:59`

`MATCH (a:A), (b:B)` works. `MATCH (a:A) MATCH (b:B)` fails. Second form is valid in standard Cypher.

### 4.10 Anonymous nodes get confusing generated names

**Files:** `binder.rs:931,967`

Anonymous patterns get names like `_n0`, `_rel1`. Error messages referencing these confuse users.

### 4.11 Error messages say WHAT is wrong but not HOW to fix

**Files:** `binder.rs` (various)

- `"Table {name} not found"` — doesn't list available tables
- `"Variable {var} not found"` — doesn't list available variables
- `"Property {key} not found in table {name}"` — doesn't list available properties
- `"MATCH must have a label"` — doesn't mention that `(n)` without `:Label` is invalid

### 4.12 `MATCH (n)` without label confusingly rejected

**Files:** `binder.rs:934`

Valid in standard Cypher to match all nodes. Here throws `"MATCH must have a label"`.

### 4.13 Error type hierarchy inconsistent

**Files:** `core/lib.rs:196-228`

- `"Table already exists"` → `Database`
- `"Table not found"` → `Query`
- `"No index found"` → `Internal`
- Schema mismatch → `Internal` (should be `Database`)

Users can't distinguish "I typed something wrong" from "the system is broken."

### 4.14 DDL syntax quirks

- **PRIMARY KEY technically optional** but many features don't work without it — no warning
- **`CREATE REL TABLE` has non-standard syntax** — `FROM`/`TO` inside parens looks like column names
- **`COPY` uses parenthesized options** — `COPY ... FROM 'file' (DELIM ',', HEADER true)` non-standard
- **Index creation requires redundant name and repeated variable** — `CREATE INDEX my_idx FOR (n:Person) ON (n.name)`

---

## 5. HTTP Server

### 5.1 README CLI flag names are wrong (DOCUMENTATION BUG)

**Files:** `README.md:57,59,92,98` vs `config.rs:13,25,107,111`

| README says | Actual flag |
|---|---|
| `--data-dir` | `--db-path` |
| `--buffer-pool-size` | `--buffer-pool-mb` |
| `--jwt-access-ttl` | `--jwt-access-ttl-secs` |
| `--jwt-refresh-ttl` | `--jwt-refresh-ttl-secs` |

Plus: README says `--jwt-refresh-ttl` defaults to 2,592,000 (30d), actual is 604,800 (7d).

### 5.2 `--query-timeout-ms` CLI flag is dead code

**Files:** `config.rs:114-116,148`

Defined, parsed, stored — but never read. The actual timeout comes from per-request JSON body.

### 5.3 `/health` response format inconsistent

**Files:** `health.rs:7-20`

All other endpoints return `{"data": ..., "meta": {"requestId": ..., "durationMs": ...}}`. Health returns raw `{"status": "ok", "version": "...", "database": "connected"}`. No `requestId` or `durationMs`.

### 5.4 Error responses have `request_id: None` in JSON body

**Files:** `error.rs:102`

The `x-request-id` header IS set, but the JSON body's `requestId` is always `null`. Cannot correlate error log lines from the JSON body alone.

### 5.5 Streaming query ignores `snapshot_ts`, `timeout_ms`, `request_id`

**Files:** `query.rs:79-119`

`query_stream_handler` only reads `query` and `params` from the request body. All other fields silently ignored.

### 5.6 No global HTTP request timeout

**Files:** `server.rs:289-307`

Middleware stack has tracing, CORS, compression, body limit, rate limiting — but no HTTP request timeout.

### 5.7 RAG query has no timeout protection

**Files:** `rag.rs:8-66`

Unlike `query_handler` which wraps the DB call in `tokio::time::timeout`, RAG has no timeout.

### 5.8 `snapshot_ts` unit undocumented

**Files:** `request.rs:22-25`

Field is `Option<u64>` with no doc comment. Internally treated as microseconds since epoch. Users sending seconds get no results.

### 5.9 `snapshot` selector precedence over `snapshot_ts` undocumented

**Files:** `query.rs:33-37`

When both provided, `snapshot` wins. Not documented.

### 5.10 Token auth uses non-constant-time comparison

**Files:** `middleware.rs:94`

```rust
Some(token) if token == expected => {  // timing attack vector
```

### 5.11 `/metrics` is public/unauthenticated

**Files:** `middleware.rs:19`

Leaks operational data (query counts, buffer hit rates, uptime) to unauthenticated users.

### 5.12 `PUBLIC_PATHS` uses `starts_with` matching — latent security issue

**Files:** `middleware.rs:63`

`path.starts_with(&format!("{p}/"))` means `/health/foo` bypasses auth. Latent risk if admin paths are added to public paths.

### 5.13 Token auth grants full Admin role — no reader-only token

**Files:** `middleware.rs:95-99`

### 5.14 Default CORS origins miss common dev ports

**Files:** `config.rs:164-171`

Covers `3000`, `8080` only. Misses `5173` (Vite), `4200` (Angular), `3001` (Next.js).

### 5.15 No `/ready` vs `/live` distinction for Kubernetes probes

**Files:** `health.rs:7-20`

### 5.16 No Arrow/IPC response format option

**Files:** `response.rs:29-35`

Despite Arrow dependency, all results are JSON only.

### 5.17 Hardcoded limits (not configurable)

| Limit | Value | File |
|---|---|---|
| Rate limiter | 100 req/s | `server.rs:110` |
| Max concurrent queries | 64 | `server.rs:92` |
| Connection pool size | 64 | `server.rs:107` |
| Request body limit | 10 MB | `server.rs:302` |
| Token GC interval | 300s | `main.rs:164` |

### 5.18 OpenAPI/Swagger commented out

**Files:** `Cargo.toml:27-28`

No `/docs`, `/openapi.json`, or `/swagger-ui` endpoint. Users must read source code or README to understand all 20+ endpoints.

### 5.19 `/v1/snapshots` returns hardcoded timestamps, not real snapshots

**Files:** `snapshots.rs:18-43`

Returns four predefined timestamps (now, yesterday, 7d, 30d). Name implies database snapshot metadata but doesn't query actual MVCC state.

### 5.20 413 errors use tower-http default format, not `ErrorResponse` JSON

**Files:** `server.rs:302`

Body limit exceeded returns tower-http's default HTML/text 413, inconsistent with all other error responses.

---

## 6. Documentation

### 6.1 WHERE clause bug undocumented in user-facing docs

**Not mentioned in** `README.md`, `CYPHER_REFERENCE.md`, or `KNOWN_LIMITATIONS.md`. Only documented in `CLAUDE.md` (invisible to non-Claude users). **CRITICAL — first user hitting this will abandon.**

### 6.2 LLMS.md states MIT license, actual is BUSL-1.1

**Files:** `LLMS.md:12`

Project uses Business Source License 1.1. LLMS.md says MIT. Legal misinformation risk for LLM-generated advice.

### 6.3 CYPHER_REFERENCE.md and LLMS.md contradict each other

- **List indexing/slicing:** CYPHER_REFERENCE lines 94-95 lists as supported. LLMS.md line 169 says "not yet exposed."
- **IN subquery:** CYPHER_REFERENCE line 93 shows `IN (subquery)`. LLMS.md line 183 says "static lists work" (only).
- **Aggregate functions:** Completeness implied in LLMS.md, ROADMAP shows they were stubs.

### 6.4 No curl example with working params

**Files:** `README.md:72`

Uses `$name` and `$age` but doesn't include `params` JSON object. Query will fail.

### 6.5 No pre-built Docker image

**Files:** `README.md:343`

Every evaluation requires 5-15 minute Rust compile. Buried in "Known Limitations" instead of Quickstart.

### 6.6 No dedicated examples directory

No `examples/` directory anywhere. Users must reverse-engineer from tests or complex external apps.

### 6.7 Python package has no README

No `python/README.md`, no docstrings, no `py.typed`.

### 6.8 TypeScript package has no README

No `packages/lightning-client/README.md`, no JSDoc, no examples.

### 6.9 Python install instruction lacks virtualenv guidance

**Files:** `README.md:186`

`cd python && pip install .` may hit permission errors. No `--user` or virtualenv suggestion.

### 6.10 `PERFORMANCE_TUNING.md` is Rust-code-snippet-only

No Python/HTTP equivalents. Env var alternatives not mentioned.

### 6.11 `ROADMAP.md` has stale claims

- "ARCHITECTURE.md is a stub" — actually 224 lines of substantive content
- "Zero Python tests exist anywhere" — contradicts actual test files
- Task completion dates missing — 98 `[x]` markers with no dates

### 6.12 `LLMS.md` has no timestamp or version

No `last_updated` or `generated_from_commit`. LLMs can't tell if it's current.

### 6.13 Docker quickstart has no auth guidance

**Files:** `README.md:56-61`

Docker defaults to `--auth-mode none` but the next section dives into 60 lines of JWT auth curl commands. No hint to skip auth section.

### 6.14 Rust crate dependency references GitHub URL that may not exist

**Files:** `README.md:243-263`

```toml
lightning = { git = "https://github.com/lightningDB/lightning" }
```

---

## 7. Remediation Plan

### Phase 1 — Stop the Bleeding (Critical Bugs)

| # | Task | Est. Effort |
|---|---|---|
| 1.1 | Fix WHERE clause filter pushdown in `scan.rs` (remap property indices) | 1-2 days |
| 1.2 | Add `from_server_dict()` mapping in Python `_types.py` for `LinkDetail`/`ContradictionDetail` | 2 hours |
| 1.3 | Fix `MemoryStore::from_connection()` to use correct embedding dim | 1 hour |
| 1.4 | Fix Arrow `_ =>` fallback in `types.rs` — warn/log on unknown types | 1 hour |
| 1.5 | Propagate FTS/Vector errors instead of `tracing::warn!` in `dml.rs` | 2 hours |
| 1.6 | Fix TypeScript `sleep(ms)` → remove the `* 1000` | 5 minutes |
| 1.7 | Add `SyntaxError` variant to `LightningError` | 1 hour |

### Phase 2 — Fix the Onboarding (Docs & API Surface)

| # | Task | Est. Effort |
|---|---|---|
| 2.1 | Fix README CLI flag names and curl examples | 1 hour |
| 2.2 | Write README for Python package | 4 hours |
| 2.3 | Write README for TypeScript package | 4 hours |
| 2.4 | Add `logout()` / `refresh_token()` to Python and TypeScript clients | 1 day |
| 2.5 | Add `query_typed()` / `query_json()` convenience methods | 1 hour |
| 2.6 | Add `SystemConfig` builder pattern | 4 hours |
| 2.7 | Add `examples/` directory with hello-world in all 3 languages | 4 hours |
| 2.8 | Document WHERE clause workaround prominently in README + CYPHER_REFERENCE | 30 minutes |
| 2.9 | Remove dead config (`--query-timeout-ms`, `maxConnections`, `maxKeepaliveConnections`) | 1 hour |

### Phase 3 — Polish (Error Messages & Ergonomics)

| # | Task | Est. Effort |
|---|---|---|
| 3.1 | Replace raw Pest errors with user-friendly syntax hints | 2 days |
| 3.2 | Add `to_json_value()` and `IntoIterator` to `TypedQueryResult` | 2 hours |
| 3.3 | Add `Option<&[f32]>` overloads instead of sentinel values | 2 hours |
| 3.4 | Restore OpenAPI/Swagger docs endpoint | 1 day |
| 3.5 | Standardize error response format across all endpoints (add `requestId`) | 4 hours |
| 3.6 | Add meaningful help text to all binder error messages | 1 day |
| 3.7 | Fix `IN` to accept `(...)` as well as `[...]` | 2 hours |
| 3.8 | Add `impl Display` for `TypedQueryResult` | 30 minutes |
| 3.9 | Rename `SyncMode::Off` → `SyncMode::Async` | 30 minutes |
| 3.10 | Fix `execute_ddl()` naming | 30 minutes |
| 3.11 | Remove dead code (Python `_validate_and_post`, unused dataclasses, `_access_token` vars) | 1 hour |
| 3.12 | Add pre-built Docker image to CI / GHCR | 1 day |

---

*End of audit*
