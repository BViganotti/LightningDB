# LightningDB Production Readiness — Remaining Issues

**Last updated**: 2026-06-23
**Status**: ~75% production-ready. Core engine is well-architected (WAL, MVCC, Arrow, CSR). All crashes fixed. Remaining issues are correctness edge cases, missing features, and hardening.

---

## P0 — Correctness (Blocking)

### 1. Relationship Traversal Wrong Row
`MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name` returns `Alice` instead of `Bob`.
HashJoin nested-join probe phase produces wrong row for the right-side variable.
**Status**: Being worked on by dedicated agent. Not covered in this document.

---

## P1 — Hangs & Timeouts

### 2. No Query Timeout Enforcement
**File**: `crates/lightning-core/src/lib.rs` line 1068 (`query_timeout_ms`)
`query_timeout_ms` is defined in `ClientContext` but set to `0` (disabled) and **never checked** in the execution path. Any long-running query (full table scan, large sort, BFS traversal) hangs the server thread indefinitely.

**Scope**: Thread a `deadline: Instant` through `Processor::execute` → all `PhysicalOperator::get_next` calls.

**Implementation plan**:
1. Add `deadline: Option<Instant>` to `Processor::execute()` and pass it to the `Scheduler`
2. Add `deadline` field to `PhysicalOperator` trait (or pass via `get_next` parameters)
3. Check `Instant::now() > deadline` in:
   - `PhysicalScan::get_next` — large table scans
   - `PhysicalSort::collect_and_sort` — sorting >10M rows
   - `PhysicalRecursiveJoin` — BFS traversal (already has `max_traversal_ms`, unify with this)
   - `HashJoin::build` — building the hash table
4. Wire `query_timeout_ms` from HTTP handler → `Connection::execute` → `Processor::execute`
5. Add a fallback kill switch: wrap `spawn_blocking` in `tokio::time::timeout`

**Files**: `lib.rs`, `processor/mod.rs`, `processor/scheduler.rs`, `processor/physical_plan.rs`, many operator files

---

### 3. ORDER BY Hangs on Error
**File**: `crates/lightning-core/src/processor/operators/sort.rs`
The Condvar wait in `get_next` hangs forever if `collect_and_sort` panics or returns an error without signaling `sort_done`. Partially fixed by `signal_sort_done()` guard, but the condvar wait has no timeout.

**Fix**: Replace bare `condvar.wait()` with `condvar.wait_timeout(Duration::from_secs(30))`. On timeout, return an error instead of hanging forever.

**Files**: `sort.rs` line ~186-198

---

## P1 — Missing Features

### 4. External Sort / TopK Optimization
**File**: `crates/lightning-core/src/processor/operators/topk.rs` (dead code)
`ORDER BY ... LIMIT K` does a full O(N log N) sort via Arrow's `lexsort_to_indices`, then takes the first K rows. `PhysicalTopK` implements the optimal O(N + K log K) bounded sort but is **never instantiated** — `LogicalOperator::TopK` compiles to `PhysicalSort + PhysicalLimit` instead.

**Implementation plan**:
1. Change `LogicalOperator::TopK` compilation in `physical_plan.rs` to create `PhysicalTopK` directly
2. For large sorts (>10M rows), implement spill-to-disk: write sorted runs to temp files, merge with K-way merge
3. Increase `MAX_SORT_MEMORY_ROWS` or make it configurable

**Files**: `physical_plan.rs` (line ~340-351), `operators/topk.rs`, `operators/sort.rs`

---

### 5. Parallel Sort Dead Code
**File**: `crates/lightning-core/src/processor/operators/sort.rs` line 221-223
`PhysicalSort::is_parallel_safe()` returns `false`, so the parallel sort infrastructure (`NWayMerge`, shared sort state, partitioned collections) is dead code. Enabling it would speed up large sorts on multi-core machines.

**Implementation plan**:
1. Change `is_parallel_safe()` to return `true`
2. Fix `collect_and_sort` spin-loop (`while num_collected < num_parts`) to use a Condvar instead of busy-waiting
3. Verify `NWayMerge::compare_values` handles all `Value` types correctly (currently only Number, String, Boolean, Null)
4. Add integration tests for parallel sort correctness

**Files**: `operators/sort.rs`, `operators/nway_merge.rs`

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
