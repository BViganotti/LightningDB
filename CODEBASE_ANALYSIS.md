# LightningDB Codebase Deep Analysis

## Executive Summary

After thorough analysis of the entire codebase (Rust core, Go/TS/Python clients, server), I've identified **systemic issues** across three categories: **stability**, **complexity**, and **correctness**. The problems are not isolated -- they stem from a few root causes that propagate throughout the stack.

**The core thesis: The codebase has too many abstraction layers that don't add value, error handling that silently swallows failures, and duplicated logic that creates divergence bugs. The product is powerful but harder to use than it needs to be.**

---

## 1. CONNECTION CREATION & MANAGEMENT

### 1.1 Database::new() is a 170-line monolithic constructor (CRITICAL)

**File:** `crates/lightning-core/src/lib.rs:422-591`

The `Database::new()` method performs ~12 sequential initialization steps in a single function:
1. Header load/create
2. WAL creation
3. StorageManager init
4. Catalog load
5. Table restoration loop (duplicates DDL logic)
6. WAL replay
7. FreeSpaceManager init
8. Wire FreeSpaceManager into file handles (move workaround)
9. TransactionManager creation
10. BufferManager creation
11. Weak reference wiring (`set_self_weak`, `set_bm_weak`)
12. Vacuum thread spawn (inline `std::thread::spawn` with a loop)
13. SEARCH function registration

**Problems:**
- If any step fails partway, partially-initialized state leaks (files created but not wired)
- Lines 530-534: `storage_manager` is moved out of scope to wire FreeSpaceManager, then moved back -- a workaround for borrow checker that signals design smell
- Lines 545-546: `set_self_weak()` and `set_bm_weak()` must be called after Arc creation -- temporal coupling not enforced by the type system
- Lines 551-568: Vacuum thread spawned inline with raw `std::thread::spawn` -- mixes concerns inside constructor
- No builder pattern, no staged initialization, no way to test individual init steps

**Impact:** Any change to initialization order risks breaking the entire startup path. The constructor is untestable in isolation.

### 1.2 "ConnectionPool" is not a pool (MISLEADING)

**File:** `crates/lightning-server/src/extract.rs:15-27`

```rust
pub struct ConnectionPool {
    db: Arc<lightning::Database>,
}

impl ConnectionPool {
    pub fn acquire(&self) -> lightning::Connection {
        self.db.connect()  // Creates a NEW connection every time
    }
}
```

`acquire()` calls `self.db.connect()` which creates a brand-new `Connection` every time. There is no pool of pre-created connections, no connection reuse, no connection limits, and no idle connection management. The name "ConnectionPool" is misleading.

**File:** `crates/lightning-core/src/lib.rs:657-659`
```rust
pub fn connect(self: &Arc<Self>) -> Connection {
    Connection::new(Arc::clone(self))
}
```

Every HTTP request creates a new `Connection` with its own `ClientContext`, `plan_caches`, etc. Under high load, this is wasteful.

### 1.3 Connection struct mixes concerns

**File:** `crates/lightning-core/src/lib.rs:1026-1049`

```rust
pub struct Connection {
    pub client_context: Arc<ClientContext>,
    pub transaction: parking_lot::Mutex<Option<Arc<Transaction>>>,
    pub pending_tables: parking_lot::RwLock<Vec<String>>,  // Dead code?
    pub skip_auth_check: bool,  // Security bypass flag
}
```

- `pending_tables` is declared but never visibly used in any method -- dead code
- `skip_auth_check` silently disables security validation -- a boolean flag that should be a separate type
- `Connection::new_internal()` vs `Connection::new()` differ only by this flag -- fragile

### 1.4 Query semaphore doesn't protect connection creation

**File:** `crates/lightning-server/src/server.rs:89,92`

The `query_semaphore` (64 concurrent queries) is acquired in the handler, but connections are created in the `DbConnection` extractor (before the semaphore). This means connection creation is unbounded even when queries are throttled.

---

## 2. SCHEMA CREATION & READING

### 2.1 Catalog persistence uses JSON (FRAGILE)

**File:** `crates/lightning-core/src/catalog/catalog.rs:329-342`

```rust
pub fn save_to_disk(&self, path: &std::path::Path) -> crate::Result<()> {
    let shadow_path = path.with_extension("lbug.shadow");
    let buf = serde_json::to_vec_pretty(self)
        .map_err(|e| crate::LightningError::Database(e.to_string()))?;
    std::fs::write(&shadow_path, buf)?;
    std::fs::rename(shadow_path, path)?;
    // ...
}
```

For a high-performance columnar database, JSON serialization of the catalog is:
- Slow (parsing overhead on every restart)
- Fragile (JSON parsing failures on corrupt data produce opaque errors)
- Space-inefficient (pretty-printed JSON for structured data)

### 2.2 Table restoration duplicates DDL logic (DUPLICATION)

**File:** `crates/lightning-core/src/lib.rs:445-498`

The loop in `Database::new()` that restores tables from the catalog duplicates the same logic found in `crates/lightning-core/src/processor/operators/ddl.rs`:
- Creates columns
- Restores `next_row_id`
- Creates indexes
- Creates FTS/vector indexes

If the DDL logic changes, this restoration code must be updated in lockstep. This is a maintenance burden and a bug vector.

### 2.3 Three catalog save methods with overlapping semantics (COMPLEXITY)

**File:** `crates/lightning-core/src/catalog/lazy_catalog.rs`

- `save_if_needed()` (line 70) -- checks dirty flag + tx count interval
- `force_save()` (line 86) -- saves unconditionally
- `force_save_with_catalog()` (line 96) -- saves with an external catalog reference, includes a raw pointer equality check (`std::ptr::eq`) that is fragile

The `force_save_with_catalog` method uses raw pointer comparison to verify identity -- this is brittle and could be invalidated by any clone or move.

### 2.4 DDL operations are non-atomic (STABILITY)

**File:** `crates/lightning-core/src/processor/operators/ddl.rs:347-376`

DDL operations acquire catalog and storage locks separately:
```rust
let mut catalog = database.catalog.write();
catalog.add_node_table(name.clone(), columns.clone(), Some(primary_key.clone()))?;

let mut storage = database.storage_manager.write();
if let Err(e) = storage.create_table(name.clone(), col_defs, false, None) {
    // Rollback catalog on storage failure
    catalog.node_tables.remove(name);
    return Err(e);
}
```

If the process crashes between catalog update and storage update, the database is inconsistent. The rollback logic is manual and error-prone.

### 2.5 LazyCatalog dirty tracking is not enforced (DESIGN)

**File:** `crates/lightning-core/src/catalog/lazy_catalog.rs:42-50`

```rust
pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, Catalog> {
    self.inner.read()
}

pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, Catalog> {
    self.inner.write()
}
```

Callers can freely mutate the catalog through the `write()` guard without going through `mark_dirty()`. The dirty-tracking invariant is not enforced by the type system -- it's a convention that can be (and is) violated.

---

## 3. ERROR HANDLING

### 3.1 Error status codes determined by string matching (FRAGILE)

**File:** `crates/lightning-server/src/error.rs:59-83`

```rust
AppError::Db(db_err) => match db_err {
    lightning_core::LightningError::Query(msg) => {
        if msg.contains("Variable") && msg.contains("not found") {
            (StatusCode::NOT_FOUND, Some("NOT_FOUND".into()))
        } else if msg.contains("already exists") {
            (StatusCode::CONFLICT, Some("ALREADY_EXISTS".into()))
        } else if msg.contains("syntax") || msg.contains("parse") {
            (StatusCode::BAD_REQUEST, Some("SYNTAX_ERROR".into()))
        } else {
            (StatusCode::BAD_REQUEST, Some("QUERY_ERROR".into()))
        }
    }
    // ...
}
```

The server determines HTTP status codes by parsing error messages with `msg.contains()`. Any change to error message text in the core crate will silently break HTTP status codes. The `LightningError` enum should carry structured error codes, not just strings.

### 3.2 Error messages stripped for users (DEBUGGING)

**File:** `crates/lightning-server/src/error.rs:86-92`

```rust
let user_message = match &self {
    AppError::Internal(_) => "An internal error occurred".to_string(),
    AppError::Db(lightning_core::LightningError::Internal(_)) => "An internal database error occurred".to_string(),
    AppError::Db(lightning_core::LightningError::Database(_)) => "A database error occurred".to_string(),
    AppError::Db(lightning_core::LightningError::Io(_)) => "An I/O error occurred".to_string(),
    _ => self.to_string(),
};
```

Internal errors strip all context, making debugging impossible for operators. The original error message should at least be logged server-side.

### 3.3 Many errors silently swallowed (STABILITY)

Throughout `crates/lightning-core/src/lib.rs`:
- Lines 470-475: FTS index creation failure logged as warning, not propagated
- Lines 473-475: Vector index creation failure logged as warning, not propagated
- Line 417: Checkpoint failure during drop logged as warning
- Lines 785-787: Free space map save failure during checkpoint logged as warning

Silently swallowing errors during index creation means the database can be in a partially-indexed state without the caller knowing. Queries may return incomplete results.

### 3.4 LightningError variants carry only strings (DESIGN)

**File:** `crates/lightning-core/src/lib.rs:178-196`

```rust
pub enum LightningError {
    Internal(String),
    Database(String),
    Query(String),
    Config(String),
    Io(#[from] std::io::Error),
}
```

The `Internal`, `Database`, `Query`, and `Config` variants all carry just a `String`. No backtrace, no error chain, no structured error codes. The `From<ArrowError>` impl converts to `Internal(e.to_string())`, losing the original error type.

---

## 4. COMPLEXITY & OVER-ENGINEERING

### 4.1 Driver crate adds no value (OVER-ENGINEERING)

**File:** `crates/lightning/src/connection.rs`, `crates/lightning/src/database.rs`

Every method in the driver `Connection` and `Database` is a one-line delegation to `self.inner.*`:
```rust
pub fn query(&self, query_str: &str) -> Result<QueryResult> {
    self.inner.query(query_str)
}
```

The only value-add is `execute_typed()` and `execute_json()`. The crate re-exports `lightning_core` types freely, making the boundary blurry. This layer adds complexity without benefit.

### 4.2 Redundant type system (OVER-ENGINEERING)

**File:** `crates/lightning-types/src/lib.rs`

`LogicalTypeID` is an enum of 27 variants that mirrors `LogicalType` exactly, with a 1:1 `id()` method. `LogicalType` already derives `Hash` and `Eq`. The `LogicalTypeID` enum doubles the maintenance burden for any type addition and is used nowhere beyond the type definition.

### 4.3 Many type variants are aspirational, not functional (DESIGN)

Types like `Int128`, `Uint128`, `Interval`, `InternalID`, `Serial`, `Blob`, `Lambda` are defined but the storage layer only handles a subset. `fast_insert` falls through to a `_ =>` catch-all for unhandled types, serializing them as strings. This means many type definitions give false confidence.

### 4.4 Plan cache is over-complex (COMPLEXITY)

**File:** `crates/lightning-core/src/lib.rs:375-388, 581`

```rust
pub plan_caches: Vec<
    Arc<
        parking_lot::Mutex<
            LruCache<
                String,
                Arc<(BoundStatement, HashMap<String, usize>)>,
            >,
        >,
    >,
>,
```

4 shards of `Arc<Mutex<LruCache<...>>>` with hash-based shard selection. The `cache_shard` and `hash_cache_key` functions must use the same hash function -- enforced by comments, not the type system. This adds complexity for marginal concurrency benefit.

### 4.5 Database struct has 13 public fields (ENCAPSULATION)

**File:** `crates/lightning-core/src/lib.rs:359-392`

```rust
pub struct Database {
    pub(crate) _path: PathBuf,
    pub(crate) _config: SystemConfig,
    pub storage_manager: Arc<RwLock<StorageManager>>,
    pub wal: Arc<WAL>,
    pub transaction_manager: Arc<TransactionManager>,
    pub buffer_manager: Arc<BufferManager>,
    pub free_space_manager: Arc<FreeSpaceManager>,
    pub catalog: Arc<LazyCatalog>,
    pub function_registry: Arc<FunctionRegistry>,
    pub header: RwLock<DatabaseHeader>,
    pub plan_caches: Vec<...>,
    pub physical_plan_caches: Vec<...>,
    pub metrics: DatabaseMetrics,
}
```

Any code with a reference to `Database` can directly access and mutate internal state, bypassing any invariants. This makes it impossible to add validation or logging to state access.

### 4.6 build_physical_plan has duplicated transaction creation (DUPLICATION)

**File:** `crates/lightning-core/src/lib.rs:1320-1457`

The method has three levels of caching (physical plan cache, bound statement cache, no-cache). Each path duplicates the transaction creation logic:
```rust
let tx = match (snapshot_ts, explicit_tx) {
    (_, Some(tx)) => tx,
    (Some(ts), None) => Arc::new(self...begin_at(true, ts)?),
    (None, None) => Arc::new(self...begin(false)?),
};
```

This 3-way match appears twice (lines 1353-1367 and 1374-1388). Changes to transaction initialization must be made in two places.

### 4.7 Arrow-to-Value conversion duplicated 3+ times (DUPLICATION)

The same `match on DataType` pattern for converting Arrow types to Values/JSON appears in:
1. `crates/lightning/src/types.rs:57-103` (`from_batches`)
2. `crates/lightning-server/src/streaming.rs:10-63` (`arrow_row_to_json`)
3. `crates/lightning-core/src/processor/mod.rs:370-432` (`Value::from_arrow`)

Each handles different subsets of types. The `from_batches` version doesn't handle Date32 or Timestamp. The `arrow_row_to_json` version doesn't handle List or Struct. This is a bug vector.

### 4.8 Value-to-Arrow builder pattern duplicated (DUPLICATION)

The same per-`LogicalType` match arms for building Arrow arrays from Values appear in:
1. `crates/lightning-core/src/lib.rs:1090-1231` (`fast_insert`)
2. `crates/lightning-core/src/storage/storage_manager.rs:116-233` (`flush_buffer`)

Neither handles all cases consistently. `fast_insert` handles Date/Timestamp but `flush_buffer` handles Rel/unsigned types.

### 4.9 ensure_csr_fresh and rebuild_csr_if_stale are identical (DEAD CODE)

**File:** `crates/lightning-core/src/storage/storage_manager.rs:1026-1070`

These two methods have identical logic. One is dead code.

---

## 5. CLIENT SDK ISSUES

### 5.1 Go Client

#### 5.1.1 Context leak in retry loop (BUG)

**File:** `packages/lightning-go/lightning/client.go:167-170`

```go
if timeout > 0 {
    ctx, cancel := context.WithTimeout(req.Context(), timeout)
    defer cancel()  // Deferred to function return, not loop iteration
    req = req.WithContext(ctx)
}
```

Each retry iteration creates a new `context.WithTimeout` but `defer cancel()` defers to function return. On retry, N contexts are created and all N cancel functions are deferred. Previous attempt's timeout context is still ticking while the new request starts.

#### 5.1.2 MaxContentBytes is dead config (BUG)

**File:** `packages/lightning-go/lightning/types.go:88`

`MaxContentBytes` is defined in `ClientConfig` but never used anywhere. `io.ReadAll(resp.Body)` reads the entire response body with no size limit.

#### 5.1.3 validateStoreParams ignores metadata (BUG)

**File:** `packages/lightning-go/lightning/validation.go:103-117`

```go
func validateStoreParams(id, content, entityType string, metadata interface{}, embedding []float32) error {
    // ... validates id, content, entityType, embedding
    // metadata parameter is accepted but NEVER validated
    return nil
}
```

#### 5.1.4 Silent protocol upgrade (FRAGILE)

**File:** `packages/lightning-go/lightning/client.go:63-65`

```go
if cfg.TLS != nil && !strings.HasPrefix(baseURL, "https://") {
    baseURL = strings.Replace(baseURL, "http://", "https://", 1)
}
```

If user provides `http://` but configures TLS, the URL is silently rewritten to `https://`. If the server doesn't listen on TLS, the user gets an opaque TLS handshake failure with no hint.

#### 5.1.5 ErrValidation is a function, not a sentinel (DESIGN)

**File:** `packages/lightning-go/lightning/errors.go:12`

```go
ErrValidation = func(msg string) error { return &ValidationError{msg: msg} }
```

This looks like a sentinel error but is a factory function. `errors.Is(err, ErrValidation)` won't work.

#### 5.1.6 ValidationError lacks Unwrap() (BUG)

**File:** `packages/lightning-go/lightning/errors.go:28-34`

No `Unwrap()` method means `errors.Is()` and `errors.As()` cannot traverse through it.

### 5.2 TypeScript Client

#### 5.2.1 No close()/dispose() method (RESOURCE LEAK)

**File:** `packages/lightning-client/src/client.ts`

The TypeScript client has absolutely no way to clean up resources. No `AbortController` lifecycle, no way to close the TLS agent. In long-running applications, this leaks connections.

#### 5.2.2 Token refresh can loop infinitely (BUG)

**File:** `packages/lightning-client/src/client.ts:264-281`

On 401, the code attempts token refresh and calls `return attempt(retryCount)` -- note it does NOT increment `retryCount`. If the server returns 401 persistently, this loops forever.

#### 5.2.3 Circuit breaker is not thread-safe (BUG)

**File:** `packages/lightning-client/src/circuit_breaker.ts`

The TypeScript `CircuitBreaker` has no synchronization. Multiple concurrent async operations can all pass `allowRequest()` while in HALF_OPEN state, exceeding `halfOpenMaxRequests`.

#### 5.2.4 metrics() bypasses retry and circuit breaker (INCONSISTENCY)

**File:** `packages/lightning-client/src/client.ts:542-563`

The `metrics()` method duplicates fetch logic instead of using the standard `request()` method, bypassing retry and circuit breaker.

#### 5.2.5 Recursive retry can stack overflow (BUG)

**File:** `packages/lightning-client/src/client.ts:232-323`

The `attempt` function is recursive. With high retry counts and the token refresh path (which resets the counter), this could blow the stack.

### 5.3 Python Client

#### 5.3.1 should_retry() is dead code (BUG)

**File:** `python/lightning/client/_transport.py:14`, `python/lightning/client/_retry.py:19-26`

`should_retry` is imported but never called. The transport hardcodes retryable status checks inline:
```python
if status == 429:
    if attempt < self._config.retry.max_retries:
        continue
elif status in (502, 503, 504):
```

`RetryConfig.retryable_statuses` is effectively ignored.

#### 5.3.2 AsyncClient missing 3 methods (BUG)

**File:** `python/lightning/client/_async_client.py`

The async client is missing methods that the sync client has:
- `recall_by_type` (present in `_client.py:159-172`)
- `entity_history` (present in `_client.py:187-198`)
- `consolidate` (present in `_client.py:200-237`)

Users switching between sync and async will hit `AttributeError`.

#### 5.3.3 SyncTransport vs AsyncTransport duplication (COMPLEXITY)

**File:** `python/lightning/client/_transport.py`

`SyncTransport` (197 lines) and `AsyncTransport` (196 lines) are nearly identical -- line-for-line except for `await` keywords. ~400 lines of near-identical code.

#### 5.3.4 Client vs AsyncClient duplication (COMPLEXITY)

Every method in `Client` is duplicated in `AsyncClient` with `async`/`await` added. ~300 lines of near-identical code.

#### 5.3.5 PayloadTooLargeError defined but never raised (DEAD CODE)

**File:** `python/lightning/client/_transport.py:40-41`

---

## 6. CROSS-CUTTING ISSUES

### 6.1 Mixed error handling strategies

`lightning-core` uses both `anyhow` and `thiserror` as dependencies (not dev-dependencies). This suggests inconsistent error handling strategy.

### 6.2 Two parser technologies

Both `pest` and `antlr4rust` are dependencies for parsing. The comment acknowledges this is transitional, but it increases compile time and binary size.

### 6.3 `tempfile` is a runtime dependency

`tempfile` is listed under `[dependencies]` in `lightning-core/Cargo.toml`. If only used in tests, it should be under `[dev-dependencies]`.

### 6.4 JSON field name inconsistency

Requests use `"entityType"` but responses use `"type"`:
- `StoreRequest.EntityType` has JSON tag `"entityType"` (Go: `client.go:281`)
- `SearchResult.EntityType` has JSON tag `"type"` (Go: `types.go:15`)
- `Entity.EntityType` has JSON tag `"type"` (Go: `types.go:23`)

This forces clients to use defensive fallbacks like `s.get("entity_type", s.get("type", ""))`.

### 6.5 Timeout parameter ambiguity

Both TS and Python clients have `timeout_ms` (server query timeout) and `timeout` (HTTP timeout) in the `query()` method. The names are easily confused.

---

## 7. ROOT CAUSE ANALYSIS

The issues stem from a few root causes:

### 7.1 No abstraction boundary enforcement

The `Database` struct exposes all internal state as `pub` fields. The driver crate (`lightning`) is a thin passthrough. There's no encapsulation boundary that prevents direct access to internals.

### 7.2 Error handling as strings, not structured types

`LightningError` variants carry `String` instead of structured error codes. This forces the server to parse error messages with `msg.contains()` to determine HTTP status codes. Any message text change breaks the HTTP layer.

### 7.3 Copy-paste code evolution

The Arrow-to-Value conversion, Value-to-Arrow builders, catalog save logic, and client SDK implementations all evolved by copying code and modifying it slightly. This creates divergence bugs where each copy handles a different subset of types.

### 7.4 "Make it work" without "Make it right"

Many features are implemented with `tracing::warn` instead of proper error propagation. Index creation failures, checkpoint failures, and FTS errors are all silently swallowed. The database can be in inconsistent states without the caller knowing.

### 7.5 Client SDKs built independently

The Go, TypeScript, and Python clients were built independently with different patterns, different bug fixes, and different feature sets. The Python client's `should_retry()` is dead code. The TS client has no `close()`. The Go client has a context leak. Each has unique bugs that the others don't.

---

## 8. RECOMMENDATIONS

### 8.1 Immediate (Stability)

1. **Replace string-based error matching with structured error codes** in `LightningError`. Add a `ErrorCode` enum and use it in the server's `IntoResponse` impl.
2. **Propagate errors instead of swallowing them** -- FTS/vector index creation failures should fail the operation, not log warnings.
3. **Fix the Go client context leak** -- move `defer cancel()` inside the loop or use a per-iteration scope.
4. **Add `close()` to the TypeScript client** with proper resource cleanup.
5. **Fix the Python AsyncClient** -- add missing `recall_by_type`, `entity_history`, `consolidate` methods.
6. **Fix the Python `should_retry()`** -- actually use it in the transport, or remove the dead config.

### 8.2 Short-term (Complexity)

1. **Extract table restoration into a shared helper** used by both `Database::new()` and the DDL operator.
2. **Deduplicate Arrow-to-Value conversion** into a single module used by all three locations.
3. **Deduplicate Value-to-Arrow builders** between `fast_insert` and `flush_buffer`.
4. **Remove the driver crate passthrough** -- either add real value (retry, connection management) or merge into `lightning-core`.
5. **Remove `LogicalTypeID`** -- it duplicates `LogicalType` and is unused.
6. **Remove dead code**: `pending_tables`, `ensure_csr_fresh`/`rebuild_csr_if_stale` duplicate, `PayloadTooLargeError`, `retry_with_backoff()`.

### 8.3 Medium-term (Architecture)

1. **Make `Database` fields private** -- expose only methods that enforce invariants.
2. **Implement staged initialization** for `Database::new()` -- use a builder pattern or init stages that can be tested independently.
3. **Implement actual connection pooling** in the server -- reuse connections, add limits, add monitoring.
4. **Standardize client SDKs** -- generate from a shared spec, or at least share validation/retry/circuit-breaker logic.
5. **Replace JSON catalog persistence** with a binary format (bincode, msgpack) for speed and robustness.
6. **Make DDL operations atomic** -- use a single lock or a transactional catalog+storage update.

---

## 9. SEVERITY MATRIX

| Category | Critical | High | Medium | Low |
|----------|----------|------|--------|-----|
| Stability | 3 | 5 | 4 | 2 |
| Correctness | 2 | 4 | 3 | 1 |
| Complexity | 1 | 5 | 6 | 3 |
| **Total** | **6** | **14** | **13** | **6** |

### Critical Issues (Fix Immediately)
1. Error status codes via string matching (server will break on any message change)
2. Errors silently swallowed (database can be in inconsistent state)
3. TS client token refresh infinite loop
4. Go client context leak in retry loop
5. Python AsyncClient missing methods (breaks async users)
6. DDL operations non-atomic (crash = inconsistent state)

### High Issues (Fix Soon)
1. Database::new() monolithic constructor
2. ConnectionPool is not a pool
3. Arrow-to-Value conversion 3x duplication
4. Value-to-Arrow builder 2x duplication
5. TS client no close()/dispose()
6. TS circuit breaker not thread-safe
7. Python should_retry() dead code
8. Catalog JSON persistence
9. Table restoration duplicates DDL logic
10. Go validateStoreParams ignores metadata
11. Go MaxContentBytes dead config
12. Go ErrValidation is function not sentinel
13. JSON field name inconsistency (entityType vs type)
14. Timeout parameter ambiguity
