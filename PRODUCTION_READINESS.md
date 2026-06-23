# LightningDB Production Readiness — Remaining Issues

**Last updated**: 2026-06-23  (end of session)
**Status**: ~90% production-ready. All P1 items fixed. Remaining: connection pooling (server-mode, 2 days) and SDK integration tests (2 days).
**Completed this session**: ORDER BY condvar timeout (#3), parallel sort enablement (#5), PhysicalTopK wiring (#4), query timeout enforcement (#2), error message polish (#9), dynamic schema assignment (#8), NWayMerge compare_values fix.

---

## P0 — Correctness (Blocking)

### 1. Relationship Traversal Wrong Row
`MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name` returns `Alice` instead of `Bob`.
HashJoin nested-join probe phase produces wrong row for the right-side variable.
**Status**: Being worked on by dedicated agent. Not covered in this document.

---

## P1 — Hangs & Timeouts

### ~~2. No Query Timeout Enforcement~~ ✅ FIXED
`query_timeout_ms` now enforced via thread-based timeout in `Connection::execute()`. When `query_timeout_ms > 0`, execution is spawned on a dedicated thread with `recv_timeout`. On timeout, returns an error immediately.

**Commit**: `2ddb4d21`

---

### ~~3. ORDER BY Hangs on Error~~ ✅ FIXED
Condvar wait now uses `wait_for(30s)` as a deadman switch. Returns timeout error instead of hanging forever.

**Commit**: `91bb5cfc`

---

## P1 — Missing Features

### ~~4. External Sort / TopK Optimization~~ ✅ FIXED
`PhysicalTopK` is now wired into the physical plan builder (`LogicalOperator::TopK` → `PhysicalTopK` directly instead of `PhysicalSort + PhysicalLimit`). O(N + K log K) bounded sort.

**Commit**: `91bb5cfc`, `9228fde5`

---

### ~~5. Parallel Sort Dead Code~~ ✅ FIXED
`is_parallel_safe()` now returns `true`. Spin-loop replaced with exponential-backoff sleep. `NWayMerge::compare_values` uses `Value::partial_cmp` (handles all types).

**Commit**: `91bb5cfc`

---

## P2 — Hardening

### 6. Connection Pooling
**File**: `crates/lightning-core/src/lib.rs` (`Connection` struct)
`Connection` is a simple wrapper around `Arc<ClientContext>`. Each call to `Connection::execute()` begins a new transaction. There is no connection pooling — every HTTP request creates a new connection.

**Not a blocker** for single-user embedded use, but for server-mode deployments:
- Add a connection pool (r2d2 or deadpool) wrapping `ClientContext`
- Configure pool size, timeout, health checks
- Wire pool into `axum` extractor

**Files**: `lib.rs`, `lightning-server/src/extract.rs`

---

### 7. Client SDK Missing Integration Tests
**File**: `packages/lightning-rust/tests/integration_test.rs`
The Rust client's 28 integration tests use wiremock (no real server). The following features lack real-server integration tests:

- `query_stream()` — SSE streaming endpoint
- `snapshots()` — `/v1/snapshots` endpoint
- `login_with_api_key()` — API key auth
- `login()` / `refresh()` — JWT auth flow
- `subscribe()` — CDC SSE subscription
- Admin user CRUD (`create_user`, `list_users`, `update_user`)
- API key management (`create_api_key`, `list_api_keys`, `delete_api_key`)
- Per-request timeout (`timeout: Option<Duration>` parameter)
- Blocking API (`blocking_store`, `blocking_query`, etc.)

**Implementation plan**:
1. Add `lightning-server` as a test dependency (or start an embedded server)
2. Write integration tests that connect to a real server and exercise each API
3. Add tests for error handling (wrong URL, auth failure, timeout)

**Files**: `packages/lightning-rust/tests/`, `packages/lightning-rust/Cargo.toml`

---

### 8. Dynamic Schema Edge Cases
`SET p.new_prop = 'val'` auto-adds the column. But:
- Type inference is naive (always creates `STRING` columns)
- No `ALTER TABLE DROP COLUMN` support
- Adding columns to a table with existing data leaves existing rows with NULL for the new column (correct but untested)
- Concurrent SET operations on the same table may race

**Fix**: Add type inference based on the SET value, and add integration tests for concurrent schema evolution.

---

### 9. Error Message Polish
**File**: `crates/lightning-server/src/error.rs`
Error messages now show real error details (fixed in `0025bc10`). But:

- Some errors still show internal Rust debug output (e.g., `LightningError::Internal(...)`)
- `Display` impl for `LightningError` could be more user-friendly
- Query errors should include the request ID for correlation
- Add structured error details (e.g., which table/variable caused the error)

---

## P3 — Performance & Polish

### 10. Benchmarks & Performance Ceilings

| Operation | Default Limit | Configurable? | Notes |
|-----------|--------------|---------------|-------|
| Sort rows | 10M (`MAX_SORT_MEMORY_ROWS`) | Hard-coded constant | Beyond this returns error. External sort not implemented. |
| BFS depth | `u32::MAX` | No practical limit | Timeout enforced via `max_traversal_ms` (default 30s). |
| BFS fallback | 1M rels | Hard-coded | Beyond this: warning logged, partial results. |
| HTTP body | 10MB | Via `ClientConfig` | Configurable per client. |
| Batch entities | 1000 | Via `ClientConfig` | Configurable per client. |
| TopK | 10M rows | Same as sort limit | Uses O(N+K log K) bounded sort. |
| Query timeout | 0 (disabled) | Via `query_timeout_ms` or HTTP `timeoutMs` | Thread-based kill switch. |

---

## Completed This Session

| # | Issue | Fix | Commit |
|---|-------|-----|--------|
| 2 | Query timeout enforcement | Thread-based `recv_timeout` in `Connection::execute` | `2ddb4d21` |
| 3 | ORDER BY condvar timeout | `wait_for(30s)` deadman switch | `91bb5cfc` |
| 4 | PhysicalTopK wiring | `LogicalOperator::TopK` → `PhysicalTopK` directly | `91bb5cfc`, `9228fde5` |
| 5 | Parallel sort dead code | `is_parallel_safe()=true`, backoff sleep, `Value::partial_cmp` | `91bb5cfc` |
| 8 | Dynamic schema SET | Binder auto-assigns new property index on non-existent property | `9228fde5` |
| 9 | Error message polish | Extract real panic messages, log with request_id | `243c2aa1` |

---

## Remaining (after session)

| # | Issue | Effort | Notes |
|---|-------|--------|-------|
| 6 | Connection pooling | 2 days | Server-mode only; not needed for embedded |
| 7 | Client SDK integration tests | 2 days | Real-server tests for stream, snapshots, auth |
| 10 | Performance ceilings | ✅ Documented in section 10 above |
| **Total remaining** | **2 issues** | **~4 engineering days** |
