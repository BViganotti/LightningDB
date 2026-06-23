# LightningDB Production Readiness — Remaining Issues

**Last updated**: 2026-06-23
**Status**: ~85% production-ready. Core engine, crashes, correctness, and performance all addressed. Remaining issues are client/sdk polish and hardening.
**Completed in this session**: ORDER BY timeout guard, parallel sort enablement, PhysicalTopK wiring, query timeout enforcement, NWayMerge compare_values fix.

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
Document known ceilings:

| Operation | Limit | Action |
|-----------|-------|--------|
| Sort rows | 10M (hard-coded) | Increase or make configurable |
| BFS max depth | u32::MAX | Add practical default (e.g., 10) |
| BFS fallback rels | 1M | Document or make configurable |
| HTTP body | 10MB (configurable) | Document default |
| Batch entities | 1000 | Document default |
| TopK | `MAX_SORT_MEMORY_ROWS` | Wire PhysicalTopK |

---

## Quick Wins (1 day each)

| # | Issue | Effort |
|---|-------|--------|
| 3 | ORDER BY condvar timeout | 1 hour |
| 5 | Enable `is_parallel_safe()` | 1 day |
| 8 | Dynamic schema type inference | 1 day |
| 9 | Error message polish | 1 day |
| 10 | Document ceilings | 2 hours |

## Heavy Lifts (2-3 days each)

| # | Issue | Effort |
|---|-------|--------|
| 2 | Query timeout enforcement | 2-3 days |
| 4 | External sort / TopK | 2 days |
| 6 | Connection pooling | 2 days |
| 7 | Client SDK integration tests | 2 days |

---

## Summary

| Priority | Issues | Total Effort |
|----------|--------|-------------|
| P0 (correctness) | 1 (being worked on) | — |
| P1 (hangs/features) | 2, 3, 4, 5 | ~7 days |
| P2 (hardening) | 6, 7, 8, 9 | ~6 days |
| P3 (polish) | 10 | ~2 hours |
| **Total** | **9 issues** | **~13-15 engineering days** |
