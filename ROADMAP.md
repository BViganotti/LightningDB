# Lightning Roadmap: Pre-Alpha → Alpha → Beta

> **Current state:** Pre-alpha. 300+ tests passing. Core engine works. Critical durability gaps exist.
> **Goal:** A production-grade embedded graph+vector+hybrid database that replaces 4 separate services.

## Progress Tracker

| Phase | Tasks | Done | Target |
|-------|-------|------|--------|
| **0: Critical Fixes** | 15 | 15 | Week 1-2 |
| **0.5: Soundness & Correctness** | 35 | 10 | Week 4-6 |
| **0.6: Operator Completeness** | 28 | 0 | Week 5-7 |
| **0.7: Parser & Language** | 32 | 0 | Week 6-10 |
| **0.8: Index Engineering** | 25 | 0 | Week 6-12 |
| **0.9: Concurrency & Infra** | 20 | 0 | Week 8-14 |
| **1: Alpha Readiness** | 36 | 0 | Month 2-3 |
| **1.6: Language Expansion** | 30 | 0 | Month 3-4 |
| **1.7: Python Bindings & Integrations** | 35 | 0 | Month 3-4 |
| **2: Beta Readiness** | 35 | 0 | Month 3-6 |
| **2.8: Compression Codec Overhaul** | 30 | 0 | Month 4-5 |
| **2.9: Storage Engine Hardening** | 25 | 0 | Month 4-5 |
| **3: Release Readiness** | 58 | 0 | Month 6+ |

**Total tasks: ~404** | **Completed: 64** | **Actual `[ ]` remaining: ~276** | **Target: Production Beta in 6-9 months**

### How To Track Progress

Each task has a unique ID like `0.1.1`, `1.2.3`, etc. When a task is completed:

1. Change `[ ]` to `[x]` in this file
2. Add the date of completion in the commit message
3. The Progress Tracker table above should be updated when a significant batch is done

Example: `- [x] **0.1.1** Checkpoint — done 2024-06-01`

The task IDs are stable — they won't change as new tasks are added (new tasks get new IDs).

---

## Phase 0: Critical Fixes (Pre-Alpha → Alpha) — 2-3 weeks

These are **non-negotiable** — the database cannot be trusted with real data until these are fixed.

### 0.1 Prevent uncommitted data from reaching disk

**Problem:** Checkpoint, clock eviction, and bulk append all write uncommitted page versions to data files without WAL protection. On crash, phantom data survives.

**Files:** `buffer_manager.rs`, `column.rs`, `storage_manager.rs`

**Tasks:**

- [x] **0.1.1** Checkpoint (`buffer_manager.rs:503-528`): Before flushing a dirty page, check `UNCOMMITTED_BIT` on the frame's version. Skip uncommitted frames entirely — they should never be written to data files. Only committed versions (`UNCOMMITTED_BIT == 0`) should be checkpointed.

- [x] **0.1.2** Clock eviction (`buffer_manager.rs:530-556`): Same protection — before `fh.write_page()` during eviction, verify `(version & UNCOMMITTED_BIT) == 0`. If the page is uncommitted AND unpinned, that means the transaction that created it is either still running or was aborted. For still-running: keep it in memory. For aborted: discard the frame without writing.

- [x] **0.1.3** Bulk append WAL logging (`column.rs`, `storage_manager.rs`): The `bulk_append_array_bulk_mode` fast-path (`skip_modified_rows=true`) writes directly to files via `fh.write_page()` and `fh.write_bytes_at()` with zero WAL records. This path must:
  1. Log page updates to WAL before writing to data files (WAL-first)
  2. Or route through the buffer manager which already has WAL integration
  3. The existing `BufferManager::create_new_version` + `log_page_update` flow is the correct pattern — bulk writes should use it.

- [x] **0.1.4** `rollback_versions` disk undo (`buffer_manager.rs:482-501`): After fixing 0.1.1-0.1.3, uncommitted data should never reach disk. But verify via test: start a write tx, cause eviction pressure, roll back, crash, recover. Assert no phantom data.

### 0.2 WAL hardening

**Files:** `wal.rs`

**Tasks:**

- [x] **0.2.1** WAL checksums: Add a CRC32/XXH3 checksum to every WAL record. On replay, verify checksums before applying page updates. Skip corrupt records but report them. This handles torn writes on power loss.

- [x] **0.2.2** WAL header magic: Write a 4-byte magic `LNIW` + version byte at WAL creation time. Validate on replay. Enables future format upgrades.

- [x] **0.2.3** WAL record alignment: Align records to 8-byte boundaries to prevent torn writes on common hardware (4096-byte page data plus 8-byte aligned headers).

- [x] **0.2.4** Replay error handling: Currently, trailing `read_exact` errors are silently `break`'d. Report partial/incomplete WAL properly so callers know recovery may be incomplete.

### 0.3 Correctness gaps

- [x] **0.3.1** `consolidate` batch-size bug (`memory.rs:510`): `batch_size = std::cmp::min(n, 200)` should be a loop over chunks of 200, not a single chunk. Fix: replace `let batch_size = ...; for i in 0..batch_size` with `for chunk in entities.chunks(200)`.

- [x] **0.3.2** Undo for UpdateColumn / DeleteNode (`undo_buffer.rs:35-39`): Currently stubs with comments saying "handled by page-level rollback in BufferManager." This is only true if the page was never evicted. Add before-image capture for evicted pages, or ensure pages can never be evicted while dirty+uncommitted (see 0.1.2).

- [x] **0.3.3** `Drop for Transaction` leak (transaction_manager.rs): When a `Transaction` is dropped without `commit()` or `rollback()`, its `tx_id` stays in `active_tx_ids` and its `read_ts` reference count is never decremented. Implement `Drop for Transaction` that auto-rolls back.

- [x] **0.3.4** `Drop for Database` polish (`lib.rs:129-148`): The polling loop for final flush is good but should log if pages remain dirty after all retries.

---

## Phase 1: Alpha Readiness (1-2 months)

### 1.1 Test coverage

The existing 300 tests are impressive but miss critical dimensions:

- [ ] **1.1.1 Durability torture tests**: Kill the process at every point in the WAL lifecycle (before write, after write, before fsync, after fsync, during checkpoint). Verify no data loss on recovery.
- [ ] **1.1.2 Concurrent stress tests**: N threads inserting/querying/deleting simultaneously. Verify no deadlocks, no lost updates, no phantom reads.
- [ ] **1.1.3 Snapshot isolation tests**: Verify write-skew is documented. Test specific write-skew scenarios to confirm the isolation level is Snapshot Isolation, not Serializable.
- [ ] **1.1.4 Fuzz testing**: The existing `fuzz_test.rs` should be expanded to randomize query patterns, data types, and concurrency levels.
- [ ] **1.1.5 Edge case tests**: Empty tables, single-row, large inlined strings (>63 chars → overflow pages), NULL handling across all operators, extreme timestamps, Unicode identifiers.

### 1.2 Feature completeness for agent workloads

- [x] **1.2.1 Multi-hop `expand`** (`memory.rs:663-780`): Replace the current boolean `hops` with real transitive closure. Use the `recursive_join` operator if it supports variable-length paths, or implement iterative traversal in Rust. Remove the comment "variable-length paths are not implemented."
- [x] **1.2.2 `edge_types` filtering in `expand`** (`memory.rs:663`): The parameter is accepted but fully ignored. Wire it into the Cypher query as a filter on `r.type IN $edge_types`.
- [x] **1.2.3 Record-level CDC** (`memory.rs:601-660`): Replace WAL-file-size polling with actual WAL parsing. Emit `ChangeEvent` structs containing `entity_id`, `operation_type` (INSERT/UPDATE/DELETE), `timestamp`, and optionally the new value. The WAL already contains all page updates — parse them and reconstruct logical events.
- [x] **1.2.4 RAG pipeline enhancement** (`memory.rs:309-402`): Add configurable reranking (cross-encoder via WASM UDF or pluggable scorer). Increase expansion depth beyond top-3 seeds. Make the reranking formula configurable.
- [x] **1.2.5 WASM UDF flexibility** (`wasm_function.rs`): Support multi-argument WASM functions, not just `f64 → f64`. Accept `&[f32]` for vector operations. Add support for returning strings.

### 1.3 Documentation

- [x] **1.3.1 Architecture docs**: Document the storage engine, MVCC design, WAL format, compression codecs, and transaction model. `ARCHITECTURE.md` exists but is a stub — expand it.
- [x] **1.3.2 API reference**: Auto-generated docs for the Python API (`lightning.__init__.py`) and Rust API.
- [x] **1.3.3 Cypher query reference**: Document which Cypher features are supported and which are not (COLLECT, CASE WHEN, variable-length paths, etc.).
- [x] **1.3.4 Migration guide**: How to migrate from SQLite/Postgres/Neo4j. How to migrate Lightning versions.
- [x] **1.3.5 Performance tuning guide**: Buffer pool sizing, thread count, sync mode, compression settings, prefetch configuration.
- [x] **2.2.2 `SyncMode::Normal` verified correctness**: After implementing ARIES WAL, verify that `sync_all()` is called at exactly the right points (WAL before data, commit record fsynced before acknowledging commit).
- [x] **2.2.3 WAL archiving**: Support continuous WAL archiving for point-in-time recovery and replication.
- [x] **2.4.5 LangChain/LlamaIndex integration**: The existing integrations (`langchain.py`, `llama_index.py`) should be tested and documented with real agent examples.
- [x] **3.2.2 ARM64 optimization**: NEON SIMD for vector search on ARM (currently only AVX2/SSE for x86).
- [ ] **3.2.3 WASM target**: Full test suite passes in browser WASM runtime.
- [ ] **3.2.4 musl builds**: Static musl-linked binaries for Alpine Linux Docker images (~10MB).

### 3.3 Scalability

- [ ] **3.3.1 Database size >100GB**: Test and profile with 100GB+ datasets. Profile buffer manager behavior under memory pressure.
- [ ] **3.3.2 Concurrent connections >100**: Stress-test with 100+ concurrent read/write connections.
- [ ] **3.3.3 Multi-instance replication**: Primary-replica setup with WAL shipping for read scaling.
- [ ] **3.3.4 Sharding**: User-defined sharding across multiple database instances (advanced — deferrable).

---

## Appendix: Current State vs Target

| Dimension | Current (Pre-Alpha) | Alpha Target | Beta Target |
|---|---|---|---|
| **Durability** | Uncommitted pages leaked to disk, bulk writes bypass WAL | No uncommitted data reaches disk, all writes WAL-logged | ARIES WAL, SSI isolation, online backup |
| **Tests** | 300 tests, no crash recovery tests | 500+ tests including crash/recovery, concurrent stress | 1000+ tests, 48h fuzz campaign |
| **Agent API** | 22 methods, 3 have gaps | All methods production-complete | Cross-encoder RAG, real-time CDC |
| **Performance** | Basic benchmarks exist | Optimized checkpoint, compression tuning | SIMD scan, parallel checkpoint, adaptive prefetch |
| **Clients** | Python (PyO3), C FFI | Python + Node.js + Go | Python + Node.js + Go + WASM browser + gRPC |
| **Observability** | None | Prometheus metrics, tracing | Slow query log, EXPLAIN ANALYZE |
| **Platform** | macOS x86_64 | macOS + Linux x86_64 + aarch64 | All platforms + WASM + musl static builds |
| **Docs** | README only | Architecture + API + Cypher docs | Full reference + migration + tuning guides |

---

## Priority Matrix

```
                    High Impact                Medium Impact
                ┌─────────────────────┬────────────────────────┐
   Easy         │  0.1 Uncommitted    │  0.2.1 WAL checksums   │
                │  pages fix          │  0.3.1 Consolidate bug │
                │  0.1.3 Bulk WAL     │  1.4.1 Error messages  │
                │  0.3.3 Drop tx      │  1.3 Docs              │
                ├─────────────────────┼────────────────────────┤
   Medium       │  1.1 Test coverage  │  2.4 Client drivers    │
                │  (crash, stress)    │  2.5 Observability     │
                │  1.2 Agent features │  2.6 Maintenance tools │
                │  (expand, CDC)      │                        │
                ├─────────────────────┼────────────────────────┤
   Hard         │  2.1 SSI (serializ-│  3.3 Scalability       │
                │  able isolation)    │  3.2 Platform support  │
                │  2.2 ARIES WAL      │                        │
                │  3.1 Security audit │                        │
                └─────────────────────┴────────────────────────┘
```

---

## Recommended Sprint Plan (First 4 weeks)

| Week | Focus | Deliverables |
|------|-------|------------|
| **1** | Critical durability fixes | 0.1.1-0.1.4, 0.2.1, 0.3.3. All existing tests pass + new crash recovery tests |
| **2** | WAL + correctness | 0.2.2-0.2.4, 0.3.1, 0.3.2. Write-skew documented. consolidate bug fixed |
| **3** | Test coverage | 1.1.1-1.1.5. Crash recovery tests, concurrent stress tests, fuzz expansion. 500+ tests |
| **4** | Agent feature completeness | 1.2.1-1.2.5. Multi-hop expand, edge-type filtering, record-level CDC. Docs update |

After week 4: **Alpha release**. The database is safe for non-financial use. Agent features are complete. Tests cover crash recovery and concurrency.

---

## Phase 0.5: Soundness & Correctness (Weeks 5-8)

These are critical issues that can produce wrong results or undefined behavior — they must be fixed before any production use.

### 0.5.1 Fix soundness hole: `Frame.data` lacks `UnsafeCell`

**Severity:** HIGH — undefined behavior under Rust's aliasing rules.

**Problem:** `Frame.data` is declared as `[u8; PAGE_SIZE]` (line 13 of `buffer_manager.rs`). The struct is always behind `Arc<Frame>` (shared ownership). 11 locations across `column.rs`, `hash_index.rs`, `csr.rs`, `vector_index.rs`, and `transaction_manager.rs` write to `frame.data` through raw pointer casts:

```rust
let src_ptr = frame.data.as_ptr() as *mut u8;  // &[u8] → *const → *mut — UNDEFINED BEHAVIOR
```

The Rust compiler may assume `[u8; PAGE_SIZE]` behind `Arc` is immutable and optimize accordingly (e.g., hoist loads, reorder stores). This is technically UB.

**Fix:** Wrap `data` in `UnsafeCell<[u8; PAGE_SIZE]>`:

```rust
pub struct Frame {
    pub data: UnsafeCell<[u8; PAGE_SIZE]>,
    pub version: AtomicU64,
    pub pin_count: AtomicU64,
}
```

Then update all read sites to use `UnsafeCell::get()` and all writes to use the resulting `*mut u8` pointer. This is safe because all writes are serialized by the per-page `Mutex` or by the shard `RwLock`.

- [x] **0.5.1a** Change `Frame.data` to `UnsafeCell<[u8; PAGE_SIZE]>`
- [x] **0.5.1b** Update all 11 write sites to use `frame.data.get()` instead of `frame.data.as_ptr() as *mut u8`
- [x] **0.5.1c** Add `as_slice()` / `as_mut_slice()` / `as_ptr()` safe accessors to `Frame`
- [ ] **0.5.1d** Verify MIRI-clean (run `cargo miri test`)

### 0.5.2 Add safety comments to all 39 unsafe blocks

**Problem:** Only 1 of 39 `unsafe` blocks has a `// SAFETY:` comment. The remaining 38 lack justification for aliasing, non-overlap, or initialization invariants.

- [ ] **0.5.2a** Audit all `unsafe` blocks across the codebase
- [ ] **0.5.2b** Add `// SAFETY:` comment to each one documenting the invariant
- [ ] **0.5.2c** Add `// SAFETY:` comments to all 4 `Vec::set_len` calls documenting that `read_pages()` immediately fills the buffer

### 0.5.3 HashIndex silent data loss

**Severity:** HIGH — silently drops data on bucket overflow.

**Problem:** `hash_index.rs:131` checks `if (num_entries as usize) < MAX_ENTRIES_PER_PAGE` but returns `Ok(())` even when the check fails. Once 15 entries hash to the same bucket, subsequent inserts silently succeed without writing any data. Reads will find fewer entries than expected — data corruption without error.

- [x] **0.5.3a** Return `Err(LightningError::Database("HashIndex bucket overflow"))` when a bucket is full, or implement overflow page chaining
- [x] **0.5.3b** Add overflow page support: when `MAX_ENTRIES_PER_PAGE` is reached, allocate a new overflow page, link to the original bucket, and continue inserting
- [ ] **0.5.3c** Implement `resize()` or `rehash()` for dynamic bucket growth (optional, see 0.5.3a)
- [ ] **0.5.3d** Add tests: insert >15 entries with collisions, verify all are found

### 0.5.4 VectorIndex insert/search page layout mismatch

**Severity:** HIGH — search reads wrong pages for node IDs > ~4000.

**Problem:** `vector_index.rs:131` uses `page_idx = *node_id` for insert (sparse: one page per node ID). But the `search()` method at line 200 iterates entries assuming dense sequential layout: `page_idx = (entry_idx * entry_bytes) / 4096`. For node IDs > 4000 (entries_per_page * entries_per_page), the search computes page indices that don't match the insert layout.

**Example:** Node ID 5000 is inserted at page 5000. Search tries to read from page ~20 for entry index 5000. Wrong page → wrong data.

- [x] **0.5.4a** Fix insert to use dense sequential page allocation: maintain a `next_entry_page` counter, append pages sequentially
- [ ] **0.5.4b** Or fix search to iterate sparse pages: read the header to know the actual page count, then iterate page IDs rather than entry indices
- [x] **0.5.4c** Fix `get_num_entries()` to return the actual count of entries, not `pages * entries_per_page` (which is always wrong since `entries_per_page = 1` for 768-dim vectors)
- [x] **0.5.4d** Add tests: insert vectors with non-sequential node IDs (1, 100, 5000, 100000), verify search returns correct results

### 0.5.5 HashIndex and VectorIndex delete/update support

- [x] **0.5.5a** Add `delete()` to HashIndex: tombstone or compact away entries
- [x] **0.5.5b** Add `delete()` to VectorIndex: remove vector by node_id
- [x] **0.5.5c** Add `update()` to VectorIndex: replace embedding for existing node_id

### 0.5.6 6 aggregate functions silently produce wrong results

**Severity:** HIGH — queries using `STDDEV`, `VARIANCE`, `GROUP_CONCAT`, `MEDIAN`, `COLLECT_DISTINCT`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` get back `COUNT` results instead.

**Problem:** `operators/aggregate.rs:65` has a catch-all `_ => Box::new(Count::new())` that maps 8 unimplemented aggregate functions to Count.

- [x] **0.5.6a** Implement `StdDevPop` / `StdDevSamp`: two-pass (mean then variance) or Welford's online algorithm
- [x] **0.5.6b** Implement `VarPop` / `VarSamp`: reuse stddev calculation
- [x] **0.5.6c** Implement `GroupConcat`: string aggregation with separator
- [x] **0.5.6d** Implement `Median`: sort values, pick middle
- [x] **0.5.6e** Implement `CollectDistinct`: HashSet-based dedup
- [ ] **0.5.6f** Add tests for each aggregate verifying correct results
- [x] **0.5.6g** Remove the `_ => Count::new()` catch-all — make future missing aggregates return a compile error

### 0.5.7 Register `COUNT_DISTINCT` aggregate

**Problem:** `COUNT_DISTINCT` is implemented in `aggregate_function.rs:103-157` but NEVER registered in the registry HashMap. It's dead code.

- [x] **0.5.7a** Add `COUNT_DISTINCT` to the aggregate function registry
- [ ] **0.5.7b** Add tests verifying `COUNT(DISTINCT col)` returns correct results

### 0.5.8 Remove duplicate function registrations

**Problem:** 11 functions are registered twice in `registry.rs`. The second registration silently replaces the first, meaning the first implementation is dead code.

**Affected:** `INITCAP`, `MD5`, `SHA256`, `LEVENSHTEIN`, `HASH`, `PI`, `E`, `BIT_AND`, `BIT_OR`, `BIT_XOR`, `BIT_NOT`, `STRUCT_EXTRACT`, `STRUCT_PACK`, `GEN_RANDOM_UUID` (11 functions, 22 registrations).

- [x] **0.5.8a** Deduplicate all function registrations — keep the better implementation, remove the duplicate
- [ ] **0.5.8b** Add a CI check or test that detects duplicate registrations

---

## Phase 0.6: Operator Completeness (Weeks 5-8, parallel with 0.5)

### 0.6.1 Fix no-op and stub operators

- [x] **0.6.1a** `PhysicalASP` — `all_shortest_paths.rs:40-45`: The `run_asp()` method is an empty `Ok(())`. Implement actual all-shortest-paths using Yen's algorithm or Eppstein's algorithm. At minimum, implement a DAG-based all-paths variant.
- [x] **0.6.1b** `PhysicalCall` — `call.rs`: Only handles `"show_tables"`. Add a procedure registry system where procedures can be registered (like functions). Support the common Cypher procedures: `db.labels()`, `db.relationshipTypes()`, `db.schema()`, etc.
- [x] **0.6.1c** `PhysicalTransaction` — `transaction.rs:33-37`: The entire operator is a no-op. Transaction BEGIN/COMMIT/ROLLBACK must actually work when used as standalone operators inside queries (not just via the Connection API).
- [x] **0.6.1d** `PhysicalMultiplicityReducer` — `multiplicity_reducer.rs`: Currently a pure pass-through. Either implement actual Cypher multiplicity (which deduplicates rows from pattern matching), or remove it.
- [x] **0.6.1e** `PhysicalPartitioner` — `partitioner.rs`: Currently a sink that never emits rows. Either implement actual partitioning, or remove it.
- [x] **0.6.1f** `PhysicalCopy::COPY TO` — `copy.rs:214`: Returns `Err("COPY TO not yet implemented")`. Implement CSV/JSON export.

### 0.6.2 Aggregate operator improvements

- [ ] **0.6.2a** Sort-based aggregation: The current hash-based aggregation works well for many groups but degrades with memory pressure. Add a sort-based fallback that avoids building a large HashMap.
- [ ] **0.6.2b** Vectorized aggregation: The global aggregation (no GROUP BY) path uses vectorized batch processing. Extend this to grouped aggregation — currently it processes row-by-row which is slow for large groups.
- [x] **0.6.2c** Implement `is_single_row()` for operators that can produce exactly one row (e.g., `Limit(1)`, `TopK(1)`, `Aggregate` without GROUP BY, `IndexScan` with equality on unique key).

### 0.6.3 Reduce unwrap count

**Problem:** 28 `unwrap()` calls across 10 operator files. 6 in `hash_join.rs` hot path alone.

- [x] **0.6.3a** Replace all 28 `unwrap()` calls with proper error propagation (`?`) or context via `expect("meaningful message")`
- [x] **0.6.3b** Remove the `panic!()` in `PhysicalScan::new` (line 44) — unrecoverable anyway, but should be a proper error
- [x] **0.6.3c** Run `cargo clippy -- -D clippy::unwrap_used` as CI gate to prevent new unwraps

### 0.6.4 Operator test coverage

**Problem:** Only 2 operators have dedicated test files (`intersect_test.rs`, `unwind_dedup_test.rs`).

- [ ] **0.6.4a** Add test files for at minimum: `scan`, `filter`, `projection`, `hash_join`, `sort`, `aggregate`, `limit`, `sort`, `topk`, `unwind`, `flatten`, `union`, `dml` (create/delete/set), `ddl`, `pagerank`, `shortest_path`, `recursive_join`
- [ ] **0.6.4b** Each test file should test: basic execution, edge cases (empty input, single row, nulls), and error handling
- [ ] **0.6.4c** Add a concurrent throughput benchmark that measures ops/sec for N readers + M writers under sustained load

### 0.6.5 FreeSpaceManager integration

**Severity:** HIGH — the free space manager is fully instantiated and persisted, but `get_free_page()` and `record_free_page()` are **NEVER called**. Storage grows monotonically and space from deleted rows is never recycled.

- [x] **0.6.5a** Wire `FreeSpaceManager::get_free_page()` into `FileHandle::add_new_page()` — check free list before extending file
- [x] **0.6.5b** Wire `FreeSpaceManager::record_free_page()` into page deallocation paths (row deletion, VACUUM)
- [ ] **0.6.5c** Add tests: insert N rows, delete them, verify new inserts reuse freed pages
- [ ] **0.6.5d** Add tests: verify free space manager survives restart (persist + reload)

### 0.6.6 Expression planning stub

**Problem:** `PhysicalPlanner::plan_expression()` at `physical_plan.rs` is a complete no-op — it clones the expression and returns it unchanged. No constant folding, type analysis, or expression rewriting occurs.

- [x] **0.6.6a** Implement constant folding: evaluate `1 + 2` at plan time → `3`
- [x] **0.6.6b** Implement type analysis: verify expression types match, insert implicit casts where needed
- [x] **0.6.6c** Implement predicate simplification: `NOT (a > b)` → `a <= b`, `x = true` → `x`

---

## Phase 0.7: Parser & Language Completeness (Weeks 6-10)

### 0.7.1 Critical missing expression types

- [x] **0.7.1a** `IS NULL` / `IS NOT NULL` — Add to PEG grammar(`cypher.pest`). Generate `Function("IS_NULL", ...)` or a new `BoundExpression::IsNull` variant. Currently users must write `n.prop = null` which is not NULL-safe (NULL = NULL is unknown, not true).
- [x] **0.7.1b** `IN` operator — The current `preprocess_in_expressions` hack (parsing `IN` by expanding into `OR` chains) is fragile: it doesn't support subqueries, it fails for large lists (blows up the AST), and it can't push down into indexes. Add proper `IN` to the grammar with `BoundExpression::InList` and `BoundExpression::InSubquery` variants.
- [x] **0.7.1c** `NOT IN` — Must be added alongside `IN`
- [x] **0.7.1d** `XOR` — Add to the grammar, map to `BoundExpression::Logical(left, Xor, right)` or desugar to `(A AND NOT B) OR (NOT A AND B)`
- [x] **0.7.1e** `CAST(expr AS type)` — Add grammar and binder support. Currently only `CAST` as a function call works.
- [x] **0.7.1f** `EXTRACT(field FROM source)` — Add grammar support. Currently only `DATE_PART(field, source)` as a function call works.

### 0.7.2 Missing DDL / DML

- [x] **0.7.2a** `DETACH DELETE` — Most graph databases require `DETACH DELETE` to remove a node AND all its relationships. Without it, deleting a node with relationships leaves orphan edges.
- [ ] **0.7.2b** `ALTER TABLE` — Four operations needed:
  - `ALTER TABLE name ADD [COLUMN] name type` (add property)
  - `ALTER TABLE name DROP [COLUMN] name` (drop property)
  - `ALTER TABLE name RENAME TO new_name` (rename table)
  - `ALTER TABLE name RENAME [COLUMN] name TO new_name` (rename property)
- [x] **0.7.2c** `CREATE [TABLE [IF NOT EXISTS]]` — Add `IF NOT EXISTS` / `IF EXISTS` clauses
- [x] **0.7.2d** `DROP TABLE [IF EXISTS]` — Add `IF EXISTS`
- [x] **0.7.2e** `REMOVE n.prop` — Full property removal syntax
- [x] **0.7.2f** `SET n += {prop: val}` / `SET n = {prop: val}` — Map-based property assignment
- [ ] **0.7.2g** `CREATE CONSTRAINT` / `DROP CONSTRAINT` — Unique constraints, existence constraints
- [ ] **0.7.2h** `CREATE INDEX` / `DROP INDEX` — Explicit index management

### 0.7.3 Path and graph pattern features

- [ ] **0.7.3a** Variable-length path aggregation — Currently `MATCH (a)-[*]->(b)` works but `MATCH p = (a)-[*]->(b) RETURN p` does not return the path properly
- [ ] **0.7.3b** `ALL SHORTEST PATHS` grammar — The PEG grammar doesn't parse `ALL SHORTEST` (only `shortestPath`). Add grammar support even though the physical operator needs completion.
- [ ] **0.7.3c** `WSHORTEST`, `TRAIL`, `ACYCLIC` path qualifiers — Advanced path modes for recursive traversal

### 0.7.4 Expression and type features

- [x] **0.7.4a** List indexing: `list[0]`, `list[-1]` — Add to grammar, bind to `BoundExpression::ListIndex`
- [x] **0.7.4b** List slicing: `list[0..3]`, `list[1..]`, `list[..-1]` — Add to grammar, bind to `BoundExpression::ListSlice`
- [ ] **0.7.4c** Map/struct literals: `{key: value, key2: value2}` as an expression (currently only supported inside pattern property matchers)
- [ ] **0.7.4d** `ALL(x IN list WHERE ...)`, `ANY(...)`, `NONE(...)`, `SINGLE(...)` list quantifiers — Already used internally for `LIST_ALL` etc. but not exposed as `ALL()` syntax
- [ ] **0.7.4e** `COUNT { MATCH ... }` subquery — Add alongside the existing `EXISTS { MATCH ... }`
- [ ] **0.7.4f** Parameterized properties — Support `{key: $param}` in MATCH/WITH RETURN patterns
- [x] **0.7.4g** Multi-label handling (`binder.rs`): `MATCH (n:Person:Employee)` should match intersection of labels, not silently ignore labels after the first. Currently 6 call sites use `labels.get(0)` (lines 554, 664, 699, 740, 909, 937). At minimum return an error for multiple labels instead of silent discard.

---

## Phase 0.8: Index Engineering (Weeks 6-12)

### 0.8.1 HashIndex production hardening

- [ ] **0.8.1a** Dynamic resize: When insertion exceeds 75% fill ratio, allocate multiply by factor 2, rehash all entries, update header
- [ ] **0.8.1b** Overflow chaining: Extend to overflow pages when a bucket is full (alternative to resize for write-heavy workloads)
- [x] **0.8.1c** Delete support: Tombstone entries with a `DELETED_BIT` in the hash field; `lookup` skips tombstones; compaction cleans them up
- [x] **0.8.1d** Configurable initial bucket count (not hardcoded 64)
- [ ] **0.8.1e** Thread-safety audit: All operations go through BufferManager pinning, verify concurrent reads/writes are safe
- [x] **0.8.1f** WAL integration: Hash index modifications should be logged to WAL (currently the tx_id parameter is unused)

### 0.8.2 VectorIndex ANN (Approximate Nearest Neighbor)

**Problem:** The current brute-force search is O(n) and doesn't scale beyond ~100K vectors. For 1M+ vectors, latency becomes unacceptable.

- [ ] **0.8.2a** Implement HNSW (Hierarchical Navigable Small World) graph index — the gold standard for vector search
  - Multi-layer navigable graph with logarithmic search complexity
  - SIMD distance computation within HNSW layers
  - Support arbitrary dimensions (not just 768)
- [ ] **0.8.2b** Or implement IVF (Inverted File Index) — simpler, good for high-dimensional data
  - K-means clustering of vectors into N centroids
  - At search time: find nearest centroid, search within its cluster
  - nprobe parameter for accuracy/speed tradeoff
- [ ] **0.8.2c** If both are implemented, expose as configurable index type in CREATE INDEX
- [ ] **0.8.2d** Support multiple distance metrics: cosine (current), L2, inner product
- [ ] **0.8.2e** Add `vector_index_type` column option in CREATE NODE TABLE
- [ ] **0.8.2f** Delete/update support for ANN indexes

### 0.8.3 InvertedIndex (FTS) production hardening

- [x] **0.8.3a** Add document deletion via Tantivy's `delete_term()` — currently stale documents accumulate
- [ ] **0.8.3b** Expose custom analyzer configuration (per-column language, tokenizer, stop words)
- [ ] **0.8.3c** Support multiple indexed text fields per table (currently only indexes `content` column)
- [ ] **0.8.3d** Add `SEARCH()` or `QUERY()` function that exposes Tantivy's query parser syntax
- [ ] **0.8.3e** Add phased commit: commit batches at configurable intervals to balance freshness vs performance

### 0.8.4 CSR index improvements

- [x] **0.8.4a** Add reverse adjacency index (incoming edges). Currently only outgoing edges are stored. Bi-directional graph algorithms like PageRank need both directions.
- [ ] **0.8.4b** Add incremental edge insertion without full rebuild
- [ ] **0.8.4c** Add edge deletion support
- [ ] **0.8.4d** Add property filtering during neighbor iteration (currently only iterates neighbor IDs)

### 0.8.5 TrigramIndex persistence

- [ ] **0.8.5a** The trigram index is entirely in-memory and rebuilt from scratch on startup. For large tables (>1M rows), this can take minutes. Add on-disk serialization using Lightning's own columnar storage, so the index survives restarts.

---

## Phase 0.9: Concurrency & Infrastructure (Weeks 8-14)

### 0.9.1 Remove dead dependencies

- [x] **0.9.1a** Remove `tokio` from Cargo.toml — it's not imported anywhere. Saves ~40 crate dependencies and compile time. The codebase is purely synchronous.
- [ ] **0.9.1b** Audit all workspace dependencies for unused crates. Check which of `roaring`, `uuid`, `sha2`, `md-5`, `levenshtein`, `rusqlite` are actually used.

### 0.9.2 Rayon pool coordination

- [ ] **0.9.2a** The `Scheduler` uses a custom `rayon::ThreadPool`, but vector search, parallel scan, and recursive join use the global rayon pool. This can cause CPU oversubscription. Either:
  - Use the global pool everywhere (simplest), or
  - Use the custom pool everywhere and pass it around
- [ ] **0.9.2b** Move blocking I/O (`bm.pin_page()`) out of rayon parallel tasks, or document the starvation risk. See `vector_index.rs:222` where `pin_page()` with potential disk I/O is inside a `rayon::par_iter().fold()`.

### 0.9.3 Transaction infrastructure

- [ ] **0.9.3a** Deadlock detection for page-level locks (`buffer_manager.rs`: `get_page_lock` creates per-page `Mutex`). Add a lock timeout and deadlock detection (cycle detection via wait-for graph).
- [ ] **0.9.3b** Transaction timeout: Abort transactions that run longer than a configurable timeout (prevent hanging connections).
- [ ] **0.9.3c** Multi-statement transactions in the Python API: Support explicit `BEGIN`/`COMMIT`/`ROLLBACK` via the Connection object.
- [ ] **0.9.3d** Nested savepoints: `SAVEPOINT name` / `ROLLBACK TO SAVEPOINT name` / `RELEASE SAVEPOINT name`

### 0.9.4 Observability (moved up from Phase 2)

- [ ] **0.9.4a** Query metrics: Track per-query execution time, operator-level timing, rows scanned, pages read
- [ ] **0.9.4b** Buffer pool metrics: Hit rate, eviction rate, dirty page count, memory usage
- [ ] **0.9.4c** WAL metrics: Write rate, fsync count, total bytes, checkpoint duration
- [ ] **0.9.4d** Expose all metrics via a `db.metrics()` Python API call

### 0.9.5 Build & dependency cleanup

- [x] **0.9.5a** Move `rusqlite` from `[workspace.dependencies]` to `[dev-dependencies]` in `lightning-core/Cargo.toml` — it's only used by `lightning_vs_sqlite.rs` integration test. Saves minutes per build for users not running benchmarks.
- [x] **0.9.5b** Add `pub use accumulate::PhysicalAccumulate;` to `processor/operators/mod.rs` for API consistency.
- [x] **0.9.6** `profile.rs:44-46` uses `println!()` for output. Replace with proper `tracing::info!()` or structured logging.

---

## Phase 1.6: Cypher Language Expansion (Months 3-4)

### 1.6.1 Type system expansion

- [ ] **1.6.1a** Add `Decimal` type: fixed-precision decimal for financial calculations. 128-bit integer with configurable scale. Avoids floating-point rounding in tax/financial math.
- [ ] **1.6.1b** Add `Time` type: time of day (nanoseconds since midnight). New variant in `LogicalType`.
- [ ] **1.6.1c** Add `TimestampTZ` type: timestamp with timezone. Store as UTC + offset minutes.
- [ ] **1.6.1d** Add `UUID` type: native UUID storage (128-bit) instead of string. New variant in `LogicalType`.
- [ ] **1.6.1e** Add `Interval` type support for arithmetic: `date + interval`, `timestamp - timestamp = interval`
- [ ] **1.6.1f** Add fixed-size list type: `FLOAT[768]` for typed vector embeddings (currently stored as `List(Float)` which is variable-length).

### 1.6.2 Missing scalar functions

- [x] **1.6.2a** `IFNULL` / `ISNULL` — return first non-null argument
- [x] **1.6.2b** `NULLIF` — return null if two arguments are equal
- [x] **1.6.2c** `IF` / `IIF` — inline conditional: `IF(condition, true_val, false_val)`
- [ ] **1.6.2d** `GREATEST` / `LEAST` — varargs min/max
- [ ] **1.6.2e** `COALESCE` exists but verify it handles arbitrary argument count
- [ ] **1.6.2f** `ASCII` / `CHR` — character code conversion
- [ ] **1.6.2g** `FORMAT` / `FORMAT_TIMESTAMP` — printf-style string formatting
- [ ] **1.6.2h** `TRUNC` / `TRUNCATE` — truncate toward zero
- [ ] **1.6.2i** `WEEK` / `WEEKOFYEAR` / `DAYOFYEAR` / `QUARTER` — date part extraction
- [ ] **1.6.2j** `LAST_DAY` — last day of the month
- [ ] **1.6.2k** `STR_TO_DATE` / `TO_DATE` with format string
- [ ] **1.6.2l** `REGEXP_LIKE` — regex match returning boolean

### 1.6.3 Statistical aggregate functions

- [ ] **1.6.3a** `CORR` — Pearson correlation coefficient
- [ ] **1.6.3b** `COVAR_POP` / `COVAR_SAMP` — population/sample covariance
- [ ] **1.6.3c** `REGR_SLOPE` / `REGR_INTERCEPT` — linear regression parameters
- [ ] **1.6.3d** `PERCENTILE_CONT` / `PERCENTILE_DISC` — percentile with continuous/discrete interpolation
- [ ] **1.6.3e** `MODE` — most frequent value
- [ ] **1.6.3f** `BOOLEAN_AND` / `BOOLEAN_OR` — logical aggregation
- [ ] **1.6.3g** `FIRST` / `LAST` — first/last value in group (non-deterministic without ORDER BY)
- [ ] **1.6.3h** `BIT_AND_AGG` / `BIT_OR_AGG` / `BIT_XOR_AGG` — bitwise aggregation

### 1.6.4 Window functions

- [ ] **1.6.4a** `ROW_NUMBER()` — sequential row index within partition
- [ ] **1.6.4b** `RANK()` / `DENSE_RANK()` — rank with gaps / without gaps
- [ ] **1.6.4c** `NTILE(N)` — distribute rows into N buckets
- [ ] **1.6.4d** `LAG(col, offset)` / `LEAD(col, offset)` — access previous/next row value
- [ ] **1.6.4e** `FIRST_VALUE(col)` / `LAST_VALUE(col)` — first/last value in window frame
- [ ] **1.6.4f** `SUM() OVER (PARTITION BY ... ORDER BY ...)` — running total
- [ ] **1.6.4g** `AVG() OVER (PARTITION BY ... ROWS BETWEEN ...)` — moving average

---

## Phase 2.7: Storage Engine v2 (Months 3-5)

### 2.7.1 True VACUUM / compaction

- [ ] **2.7.1a** File-level VACUUM: rewrite all data files, compacting used pages together, truncating unused space at end
- [ ] **2.7.1b** Page defragmentation: merge partially-filled pages to reduce page count
- [ ] **2.7.1c** Index rebuild during VACUUM: rebuild hash/vector/CSR indexes to remove stale entries
- [ ] **2.7.1d** WAL compaction: archive + truncate WAL after checkpoint, keeping only the minimum needed for crash recovery
- [ ] **2.7.1e** Free space map integration: the `FreeSpaceManager` exists but is not wired into new page allocation. Wire it in so deleted page space is reused.

### 2.7.2 Compression improvements

- [ ] **2.7.2a** Fix `dict.rs:decompress_from_page()` — currently a no-op (`Ok(())`). Implement actual dictionary decompression.
- [ ] **2.7.2b** Fix `compression/analyzer.rs:analyze_column()` — currently returns `Uncompressed` for everything. The real analysis is inlined in `column.rs:optimize()`. Either consolidate into the analyzer, or remove it as dead code.
- [ ] **2.7.2c** Fix `compression/analyzer.rs:analyze_float_chunk()` — always returns `Alp`. Add checks for constant → RLE, or low-cardinality → dict.
- [ ] **2.7.2d** Add compression statistics tracking: track compression ratio per column, use it to inform future compression decisions.
- [ ] **2.7.2e** Add Zstd/Zlib compression as a general-purpose fallback for string columns that don't benefit from dictionary encoding.

### 2.7.3 File format versioning

- [ ] **2.7.3a** Add file format magic + version to all `.lbug` files (data files, catalog, WAL, free space map, header)
- [ ] **2.7.3b** Add migration framework for upgrading between file format versions
- [ ] **2.7.3c** Add backward compatibility tests: open databases created by old versions

### 2.7.4 Overflow file completion

- [ ] **2.7.4a** `overflow_file.rs:64-69`: `write_string()` is a no-op returning `(0, 0)`. Strings >63 chars are silently truncated or lost. Implement actual overflow storage with proper page allocation, WAL logging, and read-back.

### 2.7.5 Type system fix: RecursiveRel discrepancy

**Problem:** `lightning-types/src/lib.rs` has `RecursiveRel` in `LogicalTypeID` enum but NOT in `LogicalType` enum (missing variant). This means the type system can name/identify a recursive relationship type but cannot actually store a value of that type.

- [ ] **2.7.5a** Add `RecursiveRel(Vec<StructField>)` variant to `LogicalType` enum
- [ ] **2.7.5b** Add proper serialization/deserialization for the new variant
- [ ] **2.7.5c** Add type coercion and comparison support for `RecursiveRel`
- [ ] **2.7.5d** Add tests: verify recursive join variable types are correctly propagated through the planner and execution pipeline

---

## Phase 3.4: Production Deployability (Months 4-6)

### 3.4.1 Built-in HTTP server

- [ ] **3.4.1a** Embed a lightweight HTTP server (e.g., `tiny_http` or `actix-web` as optional feature) exposing:
  - `POST /query` — execute Cypher query, return JSON results
  - `POST /query/stream` — streaming NDJSON response
  - `GET /health` — liveness check
  - `GET /metrics` — Prometheus metrics
  - `POST /backup` — trigger online backup
- [ ] **3.4.1b** Configurable bind address, port, and optional TLS
- [ ] **3.4.1c** Connection limit and rate limiting per IP

### 3.4.2 CLI tool

- [ ] **3.4.2a** Standalone CLI binary (`lgt`) that can:
  - `lgt query "MATCH ..."` — run a query from command line
  - `lgt shell` — interactive Cypher shell with history
  - `lgt backup /path/to/backup` — online backup
  - `lgt restore /path/to/backup` — restore from backup
  - `lgt vacuum` — run full VACUUM
  - `lgt check` — check database integrity
  - `lgt stats` — show database statistics
  - `lgt serve` — start HTTP server mode

### 3.4.3 Python client improvements

- [ ] **3.4.3a** Add `lightning.Connection.query()` that returns Pandas DataFrames via Arrow zero-copy
- [ ] **3.4.3b** Add `lightning.Connection.query_stream()` returning iterators for large results
- [ ] **3.4.3c** Add `lightning.Database.backup()` / `.restore()` methods
- [ ] **3.4.3d** Add `lightning.Database.vacuum()` method
- [ ] **3.4.3e** Add type annotations for all public APIs (`MemoryStore`, `Database`, `Connection`, `QueryResult`)

### 3.4.4 Load and performance testing

- [ ] **3.4.4a** Benchmark vs SQLite, DuckDB, Neo4j, Pinecone for equivalent workloads
- [ ] **3.4.4b** Write-heavy benchmark: 1M inserts, measure throughput at various batch sizes
- [ ] **3.4.4c** Read-heavy benchmark: 100 concurrent readers, measure P50/P95/P99 latency
- [ ] **3.4.4d** Mixed workload: 80% reads, 20% writes, concurrent
- [ ] **3.4.4e** Vector search benchmark: 1M vectors, measure QPS at various recall targets
- [ ] **3.4.4f** Memory profiling: buffer pool, WAL, and heap usage under load

### 3.4.5 Demo applications

- [ ] **3.4.5a** Personal finance agent (your tax app) — end-to-end demo with:
  - Import transactions from CSV/OFX/QFX
  - Store as graph entities
  - Hybrid search across transaction descriptions and categories
  - RAG pipeline with tax knowledge embedded as vectors
  - Temporal queries for year-over-year comparison
  - Graph traversal for spending pattern analysis
  - WASM UDF for custom tax calculation formulas
- [ ] **3.4.5b** AI agent memory demo: conversation memory, knowledge graph, RAG
- [ ] **3.4.5c** E-commerce demo: product catalog with hybrid search, recommendation graph

---

## Appendix A: Comprehensive Bug Inventory

| ID | Severity | File | Type | Description |
|----|----------|------|------|-------------|
| B01 | CRITICAL | `buffer_manager.rs:508` | Data loss | Checkpoint writes uncommitted dirty pages to disk |
| B02 | CRITICAL | `buffer_manager.rs:553` | Data loss | Clock eviction writes uncommitted dirty pages to disk |
| B03 | CRITICAL | `column.rs` (bulk path) | Data loss | Bulk append bypasses WAL entirely |
| B04 | CRITICAL | `buffer_manager.rs:13` | UB/Soundness | `Frame.data` lacks `UnsafeCell`, raw pointer writes are UB |
| B05 | HIGH | `hash_index.rs:131` | Data corruption | Bucket overflow silently drops entries with no error |
| B06 | HIGH | `vector_index.rs:131` | Wrong results | Insert uses sparse page layout, search assumes dense layout |
| B07 | HIGH | `vector_index.rs:303` | Wrong results | `get_num_entries()` conflates pages with entries |
| B08 | HIGH | `aggregate.rs:65` | Wrong results | 6 aggregate stubs silently produce COUNT instead of error |
| B09 | HIGH | `aggregate_function.rs:103` | Dead code | `COUNT_DISTINCT` implemented but never registered |
| B10 | HIGH | `undo_buffer.rs:35` | Data loss | `UpdateColumn` / `DeleteNode` undo records are stubs |
| B11 | HIGH | `transaction_manager.rs:-` | Durability | No `Drop` impl for Transaction — leaks tx_id on dropped handle |
| B12 | HIGH | `wal.rs:98` | Data loss | WAL replay silently stops at partial records with no error |
| B13 | HIGH | `wal.rs` | Data loss | No WAL checksums — corrupt records applied silently |
| B14 | HIGH | `memory.rs:510` | Bug | `consolidate` only processes first 200 entities |
| B15 | MEDIUM | `all_shortest_paths.rs:40` | Stub | `run_asp()` is empty `Ok(())` — does nothing |
| B16 | MEDIUM | `call.rs` | Stub | Only `show_tables` implemented; other procedures error |
| B17 | MEDIUM | `transaction.rs:33` | Stub | PhysicalTransaction operator is a no-op |
| B18 | MEDIUM | `multiplicity_reducer.rs` | Stub | Pass-through, does nothing |
| B19 | MEDIUM | `partitioner.rs` | Stub | Sink-only, never emits rows |
| B20 | MEDIUM | `copy.rs:214` | Stub | `COPY TO` not implemented |
| B21 | MEDIUM | `overflow_file.rs:64` | Stub | `write_string()` is no-op, large strings lost |
| B22 | MEDIUM | `dict.rs:69` | Stub | `decompress_from_page()` is no-op |
| B23 | MEDIUM | `compression/analyzer.rs:168` | Stub | `analyze_column()` always returns Uncompressed |
| B24 | MEDIUM | `registry.rs` | Dead code | 11 duplicate function registrations (second overwrites first) |
| B25 | LOW | `scan.rs:44` | Crash | `panic!()` on empty table schema |
| B26 | LOW | 10 files | Crash risk | 28 `unwrap()` calls that could panic on unexpected states |
| B27 | LOW | `profile.rs:44` | Polish | Uses `println!()` instead of structured logging |
| B28 | LOW | `Cargo.toml` | Dead weight | `tokio` dependency never imported (~40 extra crates) |
| B29 | LOW | `recursive_join.rs:15` | Dead code | `rel_var` field stored but never read |
| B30 | LOW | `inverted_index.rs:11` | Dead code | `path` field stored but never read |

---

## Appendix B: Full Feature Gap Matrix

| Feature Category | Supported | Missing |
|---|---|---|
| **DDL** | CREATE/DROP NODE TABLE, CREATE/DROP REL TABLE, COPY FROM | ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP CONSTRAINT, IF [NOT] EXISTS |
| **DML** | MATCH, CREATE, MERGE (node), DELETE, SET | DETACH DELETE, REMOVE, MERGE (rel), SET +=, FOREACH |
| **Types** | Bool, Int8-128, UInt8-128, Float, Double, String, Blob, Date, Timestamp, Interval, List, Struct, Map, Union, Node, Rel | Decimal, Time, TimestampTZ, UUID, FixedSizeList, Enum, JSON, Geo |
| **Expressions** | Literals, Variables, Property lookup, Comparison, Arithmetic, Logical, CASE WHEN, EXISTS, NOT, Parameters, Function calls, Lambda, List literals | IS NULL, IN (proper), NOT IN, XOR, ^ (power), =~ (regex), List indexing, List slicing, Map literals, CAST, EXTRACT, ALL/ANY/NONE/SINGLE |
| **Scalar funcs** | 100+ functions (string, math, date, hash, JSON, list, map, bit, conversion) | IFNULL, NULLIF, IIF, GREATEST, LEAST, ASCII, CHR, FORMAT, TRUNC, WEEK, QUARTER, LAST_DAY, STR_TO_DATE, REGEXP_LIKE |
| **Aggregate funcs** | COUNT, COUNT_STAR, SUM, AVG, MIN, MAX, COLLECT (7 real) | COUNT_DISTINCT (orphaned), STDDEV, VARIANCE, GROUP_CONCAT, MEDIAN, CORR, PERCENTILE, MODE, BOOL_AND/OR, FIRST/LAST (8 stubs) |
| **Window funcs** | None | ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG/LEAD, SUM/AVG OVER |
| **Indexes** | Hash (static), Vector (flat brute-force), Tantivy FTS, Trigram, CSR (outgoing) | Hash (dynamic), Vector (HNSW/IVF), CSR (bidirectional), Composite indexes, Partial indexes, Index rebuild |
| **Graph** | Shortest path (BFS), Variable-length paths, PageRank, 1-hop neighbor | AllShortestPaths (stub), Multi-hop expand, Edge type filtering, Yen's algorithm, Community detection, Centrality |
| **Concurrency** | Snapshot Isolation, OCC, MVCC, Merge-on-commit, WAL (basic) | Serializable isolation, Deadlock detection, Transaction timeout, Savepoints, Lock-free reads |
| **Durability** | WAL with fsync, Checkpoint | ARIES WAL, WAL checksums, Before-image logging, Point-in-time recovery, Online backup |
| **Compression** | ALP, Bitpacking, Delta, Dict (partial), RLE, Analyzer (partial) | Dict decompress (stub), Column analyzer (stub), Zstd, Adaptive compression |
| **Network** | None | HTTP server, gRPC, TLS, Connection pooling, Rate limiting |
| **Clients** | Python (PyO3), C FFI | Node.js, Go, WASM browser, gRPC stub |
| **Memory** | Raw pointer writes via UnsafeCell-missing Frame (UB), 39 unsafe blocks (1 documented) | SafeCell wrap, safety docs, MIRI verification |

---

## Appendix C: File Format Versioning Plan

| File | Extension | Current Format | Versioning Needed? |
|------|-----------|---------------|-------------------|
| Data files | `.lbug` | Raw column pages (unnamed) | YES — add magic+version per file |
| Catalog | `catalog.lbug` | Serialized catalog struct | YES — add header |
| WAL | `wal.lbug` | Raw records, no header | YES — add magic+format version+checksum |
| Header | `database.header` | Has version already? | CHECK — might already be versioned |
| Free space | `free_space.bin` | Bincode-serialized | YES — add header |

---

## Appendix D: Estimated Effort Summary

| Phase | Focus Area | Estimated Effort | Dependencies |
|-------|-----------|-----------------|--------------|
| **0.1** | Prevent uncommitted data on disk | 1 week | None |
| **0.2** | WAL hardening | 1 week | 0.1 |
| **0.3** | Correctness gaps | 3-5 days | 0.1, 0.2 |
| **0.5** | Soundness + correctness | 2 weeks | None |
| **0.6** | Operator completeness | 2 weeks | None |
| **0.7** | Parser/language completeness | 3-4 weeks | None |
| **0.8** | Index engineering | 4-6 weeks | None |
| **0.9** | Concurrency/infrastructure | 2 weeks | None |
| **1.1** | Test coverage | Ongoing | All |
| **1.2** | Agent features | 2 weeks | None |
| **1.3** | Documentation | 2-3 weeks | None |
| **1.4** | Polish | 1 week | None |
| **1.5** | CI/CD | 1 week | None |
| **1.6** | Language expansion | 4-6 weeks | 0.7 |
| **2.1** | Serializable isolation | 4-6 weeks | 0.3 |
| **2.2** | ARIES WAL | 4-6 weeks | 0.1, 0.2 |
| **2.7** | Storage engine v2 | 4-6 weeks | 0.8 |
| **3.4** | Production deployability | 6-8 weeks | 2.1, 2.2 |

**Total estimated time to Beta: ~6-9 months** with a focused team of 1-2 engineers.

The fast path to usable alpha (all Phase 0 items + basic test + doc coverage) is **~6 weeks**. After that, the database is safe for personal/small-team use with non-financial data.

The path to production-grade Beta with SSI, ARIES WAL, full ANN, and multi-language support is **~6-9 months**.

---

## Phase 1.7: Python Bindings & Integrations (Months 3-4)

### 1.7.1 Expose missing MemoryStore methods

**Problem:** Only 10 of 21 Rust `MemoryStore` methods are exposed in the Python bindings (`crates/lightning-python/src/lib.rs`). 11 methods are inaccessible:

- [ ] **1.7.1a** Expose `rag_query()` — full RAG pipeline with hybrid search, graph expansion, reranking, and context assembly. Return a `RagResult` Python object with `context` and `sources` fields.
- [ ] **1.7.1b** Expose `consolidate()` — memory consolidation (auto-link, contradiction detection, PageRank). Return a `ConsolidationReport` as a Python dict.
- [ ] **1.7.1c** Expose `recall_stream()` — streaming recall via Python generator (yields results as they arrive).
- [ ] **1.7.1d** Expose `recall_at_time()` — temporal query (entities valid at a specific timestamp).
- [ ] **1.7.1e** Expose `entity_history()` — full version history of an entity.
- [ ] **1.7.1f** Expose `execute_at()` — time-travel query execution.
- [ ] **1.7.1g** Expose `query_stream()` — streaming Cypher query results via Python generator.
- [ ] **1.7.1h** Expose `recall_by_time()` — entities within a timestamp range.
- [ ] **1.7.1i** Expose `with_embedding_dim()` — configurable embedding dimension.
- [ ] **1.7.1j** Expose `subscribe_changes()` — CDC event stream via Python generator.
- [ ] **1.7.1k** Expose `now_micros_for_test()` — test utility.

### 1.7.2 Python integration quality

- [ ] **1.7.2a** Add `close()` method and `__enter__`/`__exit__` context manager to both `MemoryStore` and `LightningDatabase` for deterministic resource cleanup.
- [ ] **1.7.2b** Add return type annotations to ALL Python API methods (currently 7/10 methods in `__init__.py` lack return annotations).
- [ ] **1.7.2c** Map `LightningError` variants to specific Python exceptions: `LightningError.Internal` → `RuntimeError`, `LightningError.Query` → `ValueError`, `LightningError.Database` → `IOError`, `LightningError.Io` → `IOError` with original OS errno.
- [ ] **1.7.2d** Fix `store_batch` to raise `TypeError` instead of panicking when non-dict is passed.
- [ ] **1.7.2e** Add `expand()` parameter for `edge_types` — currently hardcodes `["Relates"]` with no way to override.
- [ ] **1.7.2f** Fix `store_batch` GIL usage: avoid per-entity `with_gil` closure nesting.

### 1.7.3 LangChain integration fix

**Problem:** `langchain.py:63-80` computes embeddings via `embed_documents` but stores them via `MemoryStore.store()` which accepts NO embedding parameter. The entities list (with embeddings) is a dead variable. Vector/hybrid search returns zero results.

- [ ] **1.7.3a** Fix the `MemoryEntity` struct in Rust to accept an optional embedding field, OR add a separate `store_with_embedding()` method that stores entity content AND indexes the vector.
- [ ] **1.7.3b** In `langchain.py::add_texts()`, use `store_batch()` instead of per-text `store()` loop for batch efficiency.
- [ ] **1.7.3c** Add `similarity_search_by_vector()` to LangChain integration.
- [ ] **1.7.3d** Add `similarity_search_with_score()` and `similarity_search_with_relevance_scores()`.
- [ ] **1.7.3e** Add `max_marginal_relevance_search()` (MMR for diversity).
- [ ] **1.7.3f** Add async variants (`asimilarity_search`, `aadd_texts`, `adelete`).

### 1.7.4 LlamaIndex integration fix

- [ ] **1.7.4a** Same embedding fix as LangChain: ensure vectors are stored in the vector index during `add()`.
- [ ] **1.7.4b** Use `store_batch()` instead of per-node `store()` loop.
- [ ] **1.7.4c** Fix tautological `is not None` check in `query()` — change `if query_embedding is not None` to `if query_embedding`.
- [ ] **1.7.4d** Add `persist()` / `load()` for index serialization.
- [ ] **1.7.4e** Add `get_nodes()` for node retrieval.

### 1.7.5 Python test infrastructure

**Problem:** Zero Python tests exist anywhere in the repository.

- [ ] **1.7.5a** Add pytest configuration to `pyproject.toml`
- [ ] **1.7.5b** Create `tests/test_memory_store.py` — test all `MemoryStore` methods with a temporary database
- [ ] **1.7.5c** Create `tests/test_bindings.py` — test `LightningDatabase`, `Connection` methods
- [ ] **1.7.5d** Create `tests/test_langchain.py` — test `add_texts`, `similarity_search`, `delete` with real embeddings
- [ ] **1.7.5e** Create `tests/test_llama_index.py` — test `add`, `query`, `delete`
- [ ] **1.7.5f** Create `tests/test_rag.py` — test end-to-end RAG pipeline from Python
- [ ] **1.7.5g** Add Python CI job to GitHub Actions that runs pytest across Python 3.9–3.12

---

## Phase 2.8: Compression Codec Overhaul (Months 4-5)

### 2.8.1 Fix critical decompression stubs

- [ ] **2.8.1a** `dict.rs:decompress_from_page()` — currently a no-op `Ok(())`. Reading Dict-compressed pages silently produces empty output. Implement: read dict header, rebuild dictionary, unpack bit-packed indices, emit values.
- [ ] **2.8.1b** `alp.rs:compress_next_page()` — currently a raw `memcpy` stub that never calls `Alp::encode_value`. Zero actual compression. Implement proper float analysis, exponent selection, exceptions list.
- [ ] **2.8.1c** `compression/analyzer.rs:analyze_column()` — stub returning `Uncompressed`. The real analysis is inlined in `Column::optimize()`. Consolidate into the analyzer or remove dead code.

### 2.8.2 Fix numeric correctness bugs

- [ ] **2.8.2a** `alp.rs:encode_value()` — NaN and Infinity round-trip wrongly: `NaN as i64` = `i64::MIN` (`-9.22e18`), `Inf as i64` = `i64::MAX` (`9.22e18`). Add exceptions list for non-finite values.
- [ ] **2.8.2b** `alp.rs:encode_value()` — `-0.0` is lost: `(-0.0).round() as i64` = `0`, sign bit discarded. Result: `-0.0` → `+0.0` after round-trip.
- [ ] **2.8.2c** `alp.rs:encode_value()` — large values overflow f64: `1e300 * 1e10` = `Infinity`, then `Infinity as i64` = `i64::MAX`. Add overflow-safe computation.
- [ ] **2.8.2d** `bitpacking.rs:write_bits()` — uses `|=` (OR) instead of `=` (assignment). Requires output buffer to be pre-zeroed. No caller explicitly zeroes. Fix: change to `=` or document and verify pre-zeroing.
- [ ] **2.8.2e** `delta.rs:compress_next_page()` — `src_offset` parameter is ignored for selecting the compressed block; always reads block 0 from `src`. Fix: `BitPacker::unpack_32` should start at the correct byte offset.
- [ ] **2.8.2f** `delta.rs:encode_page()` — negative deltas (when `val < min`) wrap to huge u64 via `val as u64`. Validate `val >= min` at compression time, or use correct metadata.
- [ ] **2.8.2g** `dict.rs:bit_width` calculation — off-by-one: `ceil(log2(n))` computed as `ceil(log2(n+1))`. Fix: if `dict_count <= 1 { 0 } else { 64 - (dict_count - 1).leading_zeros() }`.
- [ ] **2.8.2h** `dict.rs:output_buffer_size` — reserves only 32 bytes for packed indices, but actual requirement is `(32 * bit_width + 7) / 8`. Fix: compute required size dynamically.
- [ ] **2.8.2i** `rle.rs` — decompression uses element-by-element loop with `std::ptr::copy_nonoverlapping` per run of 4B values. Replace with bulk `copy_from_slice`.

### 2.8.3 Codec parameterization

- [ ] **2.8.3a** All codecs hardcode `element_size = 8` (f64/i64). Add parameterized `element_size: u8` field to each codec struct.
- [ ] **2.8.3b** Update `Column::optimize()` to select codec based on logical type: RLE for integers with few distinct values, ALP for floats, Dict for low-cardinality strings, etc.
- [ ] **2.8.3c** Add compression statistics tracking per column: raw bytes, compressed bytes, compression ratio, elapsed time.
- [ ] **2.8.3d** Add Zstd compression as general-purpose fallback for string/blob columns.

### 2.8.4 Codec test coverage

- [ ] **2.8.4a** ALP tests: edge cases (NaN, Inf, -0.0, denormals, very small numbers like `1e-20`, very large numbers like `1e200`), round-trip verification, compression ratio measurement.
- [ ] **2.8.4b** Bitpacking tests: boundary bit widths (0, 1, 63, 64), max values at each width, buffer overflow protection.
- [ ] **2.8.4c** Delta tests: monotonically decreasing sequences, values spanning full i64 range, single-element blocks, multi-block pages.
- [ ] **2.8.4d** RLE tests: single-value runs, max-count runs (u32::MAX), all-unique input (expansion check), page boundary runs.
- [ ] **2.8.4e** Dict tests: single-entry dict, max-entries dict, 0-bit (single value) dict, values not present in dict.

---

## Phase 2.9: Storage Engine Hardening (Months 4-5)

### 2.9.1 Overflow file completion

- [ ] **2.9.1a** Implement `overflow_file.rs::write_string()` — the current no-op stub silently loses strings >63 chars. Implement: allocate overflow pages via `BufferManager::create_new_version`, write string data in linked-list pages (up to 4KB per page minus header), log each page update to WAL.
- [ ] **2.9.1b** Implement `overflow_file.rs::read_string()` — handle linked-list traversal correctly. Add cycle detection (max page limit) to prevent infinite loops from corrupted overflow chains.
- [ ] **2.9.1c** Add overflow page bounds checking: if `current_offset > usable_size` due to corrupted next-page pointer, return error instead of underflow.
- [ ] **2.9.1d** Add overflow WAL integration: overflow page writes must go through WAL for crash recovery.

### 2.9.2 RowVersion committed map memory leak

**Problem:** `row_version.rs:177-193` — entries in `committed` HashMap are never removed. Over time, with millions of rows, this consumes gigabytes of memory.

- [ ] **2.9.2a** Add background GC for `committed` map: scan shards, remove entries where `commit_ts` is older than the minimum active `read_ts` across all transactions.
- [ ] **2.9.2b** Add TTL-based cleanup: entries committed more than N seconds ago are candidates for eviction if no transaction can still see them.
- [ ] **2.9.2c** Add memory pressure trigger: when total `committed` entries exceed a configurable threshold, initiate GC.
- [ ] **2.9.2d** Update `has_modifications()` and `has_committed()` — currently always returns `true` after the first commit because `committed` never shrinks. Change to return `false` when `committed` is empty after GC.

### 2.9.3 Database header atomic writes

- [ ] **2.9.3a** `database_header.rs:save()` — currently truncates the file then writes. On crash between truncation and write, the header is lost. Fix: write to a temporary file, call `sync_all()`, then `rename` atomically over the original.
- [ ] **2.9.3b** Add CRC32 checksum to the header file. On `load()`, verify the checksum before trusting the contents.
- [ ] **2.9.3c** Add magic bytes + version to `database_header` for format identification.

### 2.9.4 File I/O hardening

- [ ] **2.9.4a** `file_handle.rs:truncate()` — missing `sync_all()` after `set_len(0)`. Add `file.sync_all()` call.
- [ ] **2.9.4b** `file_handle.rs:get_file_size()` — silently swallows errors with `unwrap_or(0)`. Replace with proper `io::Error` propagation via `Result<u64>`.
- [ ] **2.9.4c** `file_handle.rs:read_page()` — calls `metadata()` syscall on every read (kernel context switch). The file length should be tracked in the `num_pages` field with a `try_read` fallback.
- [ ] **2.9.4d** `file_handle.rs:read_pages()` — integer overflow risk in `expected_bytes = num_pages * PAGE_SIZE` for large `num_pages`. Add `checked_mul` or saturating arithmetic.
- [ ] **2.9.4e** `file_handle.rs:add_new_page()` — logical page count incremented but physical file NOT extended. Every `read_page()` requires a `metadata()` syscall to discover the page doesn't exist. Fix: extend the file on allocation (write zero page or use `fallocate`).
- [ ] **2.9.4f** `file_handle.rs:write_bytes_at()` — accepts arbitrary offsets and lengths with no page alignment enforcement. Add validation or ensure all callers are page-aligned.

### 2.9.5 RowVersion mark_row_batch conflict detection

- [ ] **2.9.5a** `row_version.rs:mark_row_batch()` — performs ZERO conflict detection (unlike `mark_row()`). Add checks for: existing conflicting transactions in `versions`, existing committed entries with `commit_ts > read_ts`.

### 2.9.6 PageState version counter wrap

- [ ] **2.9.6a** `page_state.rs:95` — 56-bit version counter wraps after `2^56` increments (~2 years at 1 GHz). Add wrap-safe comparison: replace `version > other_version` with `(version.wrapping_sub(other_version) as i64) > 0`.

---

## Appendix E: Full Compression Codec Bug Inventory

| ID | Codec | Severity | Description |
|----|-------|----------|-------------|
| C01 | Dict | CRITICAL | `decompress_from_page` is no-op `Ok(())` stub. Reading Dict pages silently produces empty/zero output — data corruption. |
| C02 | ALP | HIGH | `compress_next_page` is raw memcpy stub — never calls `Alp::encode_value`. Zero compression. |
| C03 | ALP | HIGH | NaN round-trip: `NaN as i64 = i64::MIN` (−9.22e18), not NaN. |
| C04 | ALP | HIGH | Infinity round-trip: `Inf as i64 = i64::MAX` (9.22e18), not Inf. |
| C05 | ALP | HIGH | `-0.0` sign lost: `(-0.0).round() as i64 = 0`, becomes `+0.0`. |
| C06 | ALP | MEDIUM | f64 overflow in `val * EXP_ARR[exp_idx]`: 1e300 * 1e10 = Inf → i64::MAX. |
| C07 | Bitpacking | HIGH | `write_bits` uses `|=` (OR), requires pre-zeroed buffer. No caller zeroes. Latent cross-contamination. |
| C08 | Bitpacking | MEDIUM | `assert!` in production code (`values.len() >= 32`, `output.len() >= 32`). Panics on undersized input. |
| C09 | Bitpacking | LOW | No validation that values fit within declared bit width. Truncation silently. |
| C10 | Delta | HIGH | `src_offset` ignored for block selection — always reads block 0. Multi-block pages produce wrong output. |
| C11 | Delta | MEDIUM | `val < min` arithmetic: `(val - min) as u64` wraps to huge positive. Wrong data when metadata is stale. |
| C12 | Delta | MEDIUM | `min + deltas[i] as i64` can overflow i64 (debug panic, release wrap). |
| C13 | Dict | MEDIUM | Bit-width formula off-by-one: `ceil(log2(n))` = `ceil(log2(n+1))`. Wastes 1 bit per value. |
| C14 | Dict | MEDIUM | Output buffer check reserves 32 bytes; actual need is `(32 * bit_width + 7) / 8`. Buffer overflow for bit_width > 8. |
| C15 | Dict | LOW | Dangling slice references: `dict_map` keys borrow from `src` parameter. Fragile under refactoring. |
| C16 | RLE | LOW | Decompression uses element-by-element loop; `O(run_length)` for 4B-value runs. Use `copy_from_slice` instead. |
| C17 | All | MEDIUM | All codecs hardcode `element_size = 8`. Breaks for i32, i16, etc. |
| C18 | All | LOW | No checksum/integrity verification. Corrupted pages propagate silently. |

## Appendix F: Full Python Binding Gap Inventory

| ID | Severity | File | Description |
|----|----------|------|-------------|
| P01 | CRITICAL | `langchain.py:63` | Embeddings computed by `embed_documents()` but never stored — `MemoryStore.store()` has no embedding parameter. Vector/hybrid search returns zero results. |
| P02 | CRITICAL | `llama_index.py:70-86` | Same embedding gap as P01. `add()` stores nodes without vectors. |
| P03 | HIGH | `__init__.py` | 11 of 21 Rust `MemoryStore` methods not exposed via Python, including `rag_query`, `consolidate`, `recall_stream`, `recall_at_time`, `entity_history`, `execute_at`, `query_stream`, `subscribe_changes`. |
| P04 | HIGH | `*.py` | Zero Python tests — no pytest config, no test files, no CI job. |
| P05 | MEDIUM | `langchain.py:75-80` | `add_texts` stores texts one-by-one via `self._memory.store()` instead of batching with `store_batch()`. O(n) FFI calls. |
| P06 | MEDIUM | `llama_index.py:58-66` | Same O(n) issue as P05. |
| P07 | MEDIUM | `lib.rs` | `expand()` hardcodes `edge_types = vec!["Relates"]` — no Python parameter to override. |
| P08 | MEDIUM | `lib.rs:store_batch` | `unwrap()` on `downcast_bound::<PyDict>` will panic (crash interpreter) on non-dict input instead of raising TypeError. |
| P09 | LOW | `__init__.py` | 7 of 10 methods missing return type annotations (`-> list[dict]`). |
| P10 | LOW | `lib.rs` | No `__enter__`/`__exit__` context manager on `MemoryStore` or `LightningDatabase`. |
| P11 | LOW | `lib.rs` | All `LightningError` variants flattened to generic `PyRuntimeError` — Python callers cannot distinguish error types. |
| P12 | LOW | `langchain.py` | Missing standard LangChain VectorStore methods: `similarity_search_by_vector`, `similarity_search_with_score`, `max_marginal_relevance_search`, async variants. |

---

## Appendix G: Full Storage Engine Bug Inventory (Additions)

| ID | Severity | File | Description |
|----|----------|------|-------------|
| S01 | CRITICAL | `overflow_file.rs:64` | `write_string()` is no-op — strings >63 chars silently lost |
| S02 | CRITICAL | `row_version.rs:177-193` | `committed` HashMap never GC'd — unbounded memory growth |
| S03 | CRITICAL | `database_header.rs:40` | Header save is non-atomic — crash between truncation and write corrupts DB |
| S04 | HIGH | `row_version.rs:61` | `mark_row_batch()` skips all conflict detection |
| S05 | HIGH | `file_handle.rs:136` | `truncate()` missing `sync_all()` — truncated data may reappear on crash |
| S06 | HIGH | `file_handle.rs:126` | `get_file_size()` swallows errors with `unwrap_or(0)` |
| S07 | MEDIUM | `row_version.rs:79` | `commit_row()` of unmarked row: inserts committed entry for non-existent modification |
| S08 | MEDIUM | `file_handle.rs:58` | `read_page()` calls `metadata()` per call — kernel syscall on hot path |
| S09 | MEDIUM | `file_handle.rs:76` | `read_pages()`, `expected_bytes` integer overflow risk with large `num_pages` |
| S10 | MEDIUM | `file_handle.rs:107` | `add_new_page()` doesn't extend physical file — TOCTOU on every read |
| S11 | MEDIUM | `page_state.rs:95` | 56-bit version counter wraps after `2^56` increments |
| S12 | LOW | `file_handle.rs:18` | No `O_EXCL` on file creation — two processes can open same file simultaneously |
| S13 | LOW | `file_handle.rs` | No `O_DIRECT` — double buffering (kernel cache + Frame data)
