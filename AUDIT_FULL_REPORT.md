# COMPREHENSIVE CODE AUDIT REPORT — Lightning Database

**Date:** 2026-06-10
**Scope:** 154 `.rs` files across all crates (excluding `.md` documentation)
**Methodology:** Static analysis by file-by-file review of every production source file

---

## EXECUTIVE SUMMARY

| Severity | Count | Description |
|----------|-------|-------------|
| **CRITICAL** | 30 | Exploitable vulnerabilities, data corruption, compilation failures |
| **HIGH** | 55 | Logic bugs, race conditions, OOM risks, significant performance issues |
| **MEDIUM** | 64 | Error handling gaps, diagnostic improvements, non-critical performance |
| **LOW** | 28 | Code quality, logging levels, unused variables, minor improvements |
| **TOTAL** | **177** | |

### Top 10 Most Urgent Issues

1. **Cypher injection** — fusion.rs/memory.rs: string interpolation in Cypher queries (5+ sites)
2. **WASM path traversal + sandbox bypass** — wasm_function.rs: arbitrary file loading, no timeout enforcement
3. **HNSW broken** — hnsw.rs: `random_level()` re-seeds RNG every call, all nodes in same layer
4. **Index corruption** — inverted_index.rs: read lock used instead of write lock on tantivy writer
5. **Trigram index silent miss** — trigram_index.rs: unsorted posting lists cause binary_search to miss matches
6. **Bitpacking data corruption** — bitpacking.rs: byte path doesn't clear target bits before OR
7. **CCD thread deadlock** — cdc.rs: lock held across blocking I/O and channel sends
8. **Unsafe frame mutation** — transaction_manager.rs: raw pointer write bypasses buffer manager
9. **Copy/path traversal** — binder.rs + logical_plan.rs: no validation against `copy_base_dir`
10. **Write operators cached as read-only** — dml.rs: `is_read_only()` in inherent impl not trait impl — 6 operators affected

---

# SECTION 1: CRITICAL ISSUES

---

## 1.1 CYPHER INJECTION (fusion.rs, memory.rs) — 5+ sites

**FILE:** `crates/lightning-core/src/fusion.rs` — lines 34, 56-58, 71-73, 99-105, 127-131, 166-177, 190-192, 406-416
**FILE:** `crates/lightning-core/src/memory.rs` — lines 1047-1051, 1268-1275

**Issue:** All Cypher queries are constructed via `format!()` with user-supplied values interpolated directly. The `sq()` helper only escapes `'` -> `\'` which is trivially bypassed. Some sites (fusion.rs:56-58) use `.replace('\'', "")` which strips quotes entirely.

**Example** (fusion.rs:56-58):
```rust
let q = format!(
    "MATCH (s:CodeNode {{id: '{}'}})-[r]->(t:CodeNode {{id: '{}'}}) RETURN type(r) as rel_type",
    source_id.replace('\'', ""),
    target_id.replace('\'', "")
);
```

**Impact:** Any user who can query `find_node_by_name`, `find_paths`, or related functions can execute arbitrary Cypher (DROP TABLE, COPY to filesystem, etc.). This is **remote code execution via query injection**.

**Fix:** Use parameterized queries (`$param` syntax) exclusively. The `Connection::execute()` method already supports parameters.

---

## 1.2 WASM PATH TRAVERSAL (lib.rs, wasm_function.rs)

**FILE:** `crates/lightning-core/src/lib.rs` — lines 525-538
**FILE:** `crates/lightning-core/src/wasm_function.rs` — lines 45-68

**Issue:** `register_wasm_function` accepts an arbitrary filesystem path with no validation against `copy_base_dir` or any allowed-path configuration.

**Impact:** Attacker with query access can load arbitrary WAT/WASM files from the server filesystem.

**Fix:** Restrict WASM paths to a configured allowed directory.

---

## 1.3 WASM SANDBOX BYPASS — NO TIMEOUT (wasm_function.rs)

**FILE:** `crates/lightning-core/src/wasm_function.rs` — lines 14, 66-67, 136-323

**Issue:** `timeout_ms` is stored but **never used** during execution. The WASM function runs without any timeout. Shared memory is never cleared between invocations.

**Impact:** A malicious WASM module can loop forever, blocking the thread. Cross-query data leakage via uncleared shared memory.

**Fix:** Use wasmi's fuel metering (`Store::set_fuel`). Clear shared memory between invocations.

---

## 1.4 UNSAFE POINTER MUTATION ON SHARED FRAME DATA (transaction_manager.rs)

**FILE:** `crates/lightning-core/src/transaction/transaction_manager.rs` — lines 232-246

**Issue:** During page merge, writes directly into frame data via `unsafe { std::ptr::copy_nonoverlapping(...) }` and `*latest_frame.data.get() = merged_data`, bypassing BufferManager synchronization.

**Impact:** If BufferManager evicts this frame concurrently, this is a **data race** causing silent data corruption.

**Fix:** Add a safe API on Frame for updating page content with proper synchronization.

---

## 1.5 CDC THREAD HOLDS LOCK DURING BLOCKING I/O (cdc.rs)

**FILE:** `crates/lightning-core/src/cdc.rs` — lines 86-108

**Issue:** CDC dispatcher acquires `subscribers.lock()` then performs blocking WAL reads and channel sends while holding the lock. Meanwhile, `subscribe()` (line 52) also needs this lock — it can be blocked indefinitely.

**Impact:** subscribe() callers hang for the duration of WAL reading + all channel sends. Writes blocked.

**Fix:** Clone subscriber list under lock, drop lock, then do I/O and sends.

---

## 1.6 MISSING PATH VALIDATION IN COPY TO/FROM (binder.rs, logical_plan.rs)

**FILE:** `crates/lightning-core/src/planner/binder.rs` — lines 562-597
**FILE:** `crates/lightning-core/src/planner/logical_plan.rs` — lines 98-107

**Issue:** `CopyFrom`/`CopyTo` accept `file_path` but never validate against `SystemConfig.copy_base_dir`. The field exists but is never checked.

**Impact:** Attacker with query access can read/write arbitrary files on the server.

**Fix:** Validate all COPY paths against `copy_base_dir`.

---

## 1.7 HNSW random_level() RE-SEEDS RNG EVERY CALL (hnsw.rs)

**FILE:** `crates/lightning-core/src/storage/index/hnsw.rs` — lines 126-131

**Issue:** `random_level()` initializes RNG with fixed seed `12345` on EVERY invocation. Every node gets the same level, destroying the multi-layer structure. Search degrades to brute-force O(n*d).

**Impact:** HNSW index provides no benefit over brute-force. Complete functional regression of vector search.

**Fix:** Use thread-local or instance-level RNG that persists across calls.

---

## 1.8 INVERTED INDEX DATA RACE (inverted_index.rs)

**FILE:** `crates/lightning-core/src/storage/index/inverted_index.rs` — lines 73, 91, 114

**Issue:** `insert_batch`, `insert_multi_field_batch`, `insert_multi_field` acquire a **read** lock then call `writer.add_document()`. IndexWriter uses interior mutability — it's NOT thread-safe under shared access.

**Impact:** Concurrent inserts corrupt the tantivy index — data loss, search returning wrong results.

**Fix:** Use `self.writer.write()` (write lock), same as `commit` and `delete`.

---

## 1.9 UNSORTED POSTING LISTS → BINARY SEARCH SILENTLY MISSES (trigram_index.rs)

**FILE:** `crates/lightning-core/src/storage/index/trigram_index.rs` — lines 207-238, 486-519

**Issue:** `insert` appends `row_id` without maintaining sorted order. `intersect_sorted_lists` uses `binary_search` which requires sorted input. With unsorted lists, intersection misses valid candidates.

**Impact:** CONTAINS queries silently return false negatives (missing rows that should match).

**Fix:** Sort lists in `insert`, or store in BTreeSet and convert on query.

---

## 1.10 BITPACKING BYTE PATH DOESN'T CLEAR TARGET BITS (bitpacking.rs)

**FILE:** `crates/lightning-core/src/storage/compression/bitpacking.rs` — lines 73-86

**Issue:** The byte-by-byte write path uses `data[byte_idx] |= bits << bit_in_byte` without clearing target bits first. The 64-bit path correctly clears with mask. If output buffer is not pre-zeroed, OR corrupts data.

**Impact:** Corrupted compressed data on decode — silent data corruption.

**Fix:** Add bit-clearing before OR in byte path.

---

## 1.11 WAL CRC VERIFICATION NOT PERFORMED (wal.rs)

**FILE:** `crates/lightning-core/src/storage/wal.rs` — line 495

**Issue:** `next_record` computes CRC into `_computed_crc` (underscore-prefixed, unused). The computed checksum is never compared against `stored_crc` during replay iteration. Meanwhile, `replay` path (lines 289-295) correctly verifies checksums.

**Impact:** Corrupted WAL records are silently accepted as valid. Crash recovery produces wrong database state.

**Fix:** Compare computed CRC against stored CRC and skip/error on mismatch.

---

## 1.12 BUFFER CACHE INCOHERENCE AFTER DIRECT FILE WRITE (column.rs) — 3 sites

**FILE:** `crates/lightning-core/src/storage/column.rs` — lines 1663-1669, 1844-1854, 1454-1456

**Issue:** `bulk_append_*_fast_inner` writes data directly to files via `write_bytes_at`, bypassing buffer manager. It only evicts cache when `skip_modified_rows=true`. When `false`, stale cached pages persist for the written range.

**Impact:** Subsequent `pin_page` calls return OLD data instead of freshly written data. Silent wrong results.

**Fix:** Always evict affected pages from buffer cache after direct file writes.

---

## 1.13 RACE CONDITION IN HASH INDEX RESIZE (hash_index.rs)

**FILE:** `crates/lightning-core/src/storage/index/hash_index.rs` — lines 92-148, 427-481

**Issue:** `resize` updates header to new bucket count, then allocates/zeros pages, then re-inserts entries. Between header update and zeroing, a concurrent `insert` writes to an old bucket that gets zeroed out — entry is permanently lost.

**Impact:** Inserted rows silently disappear. Data loss.

**Fix:** Perform resize under exclusive access.

---

## 1.14 COMPILATION ERROR IN analyzer_test.rs

**FILE:** `crates/lightning-core/src/storage/compression/analyzer_test.rs` — lines 9, 20, 31, 44, 55

**Issue:** Every test function has a syntax error. Leftover `analyze_integer_chunk(` prefix from refactoring:
```rust
let meta = CompressionAnalyzer::analyze_integer_chunk(analyze_integer_chunk(&vals, ...));
```

**Impact:** **Test module won't compile.** Entire compression analyzer is untested.

**Fix:** Remove the spurious duplicate function name.

---

## 1.15 DANGLING TEMPDIR — USE-AFTER-FREE (hash_join_test.rs)

**FILE:** `crates/lightning-core/tests/hash_join_test.rs` — line 57

**Issue:** `tempfile::tempdir().unwrap().path()` creates a TempDir that is **dropped at end of statement**, making the path argument point to a deleted directory.

**Impact:** Use-after-free / undefined behavior in test.

**Fix:** Bind TempDir to a local variable.

---

## 1.16 AGGREGATE KEY DEPENDENCY OPTIMIZER BROKEN (agg_key_dependency_optimizer.rs)

**FILE:** `crates/lightning-core/src/optimizer/agg_key_dependency_optimizer.rs` — lines 96-105

**Issue:** Generic catch-all for non-Aggregate operators checks `op.get_child()` but returns `Ok(op)` unchanged. Only immediate child of Aggregate is examined. Any intermediate operators (Filter, Projection, Sort) between root and Aggregate act as barriers.

**Impact:** **Optimizer is a no-op** for any plan with intermediate operators. No aggregate key dependency optimization is ever applied.

**Fix:** Recurse into children for ALL non-Aggregate operators.

---

## 1.17 PROJECTION PUSHDOWN CORRUPTS EXPRESSION VARIABLES (projection_pushdown.rs)

**FILE:** `crates/lightning-core/src/optimizer/projection_pushdown.rs` — line 96

**Issue:** `remap_expression_indices` sets `*var = "".to_string()` after remapping a PropertyLookup. This **destroys the variable name in-place**, corrupting the expression. Downstream resolution sees empty string and fails.

**Impact:** **Data corruption of the logical plan.** Projection pushdown produces a corrupted plan.

**Fix:** Keep original variable name or use a separate HashMap-based remapping.

---

## 1.18 PROJECTION PUSHDOWN PRUNES ALL COLUMNS (projection_pushdown.rs)

**FILE:** `crates/lightning-core/src/optimizer/projection_pushdown.rs` — lines 346-349

**Issue:** If top-level node is NOT a Projection (e.g., Sort, Limit, bare Scan), `apply` passes empty `required_indices: HashMap` to `push_down`. All columns are pruned — Scans return zero columns.

**Impact:** **Broken queries** for any plan without top-level Projection (all columns returned as None).

**Fix:** If no top-level projection, assume ALL columns are needed.

---

## 1.19 COUNT REL TABLE OPTIMIZER WRONG TABLE TYPE (count_rel_table_optimizer.rs)

**FILE:** `crates/lightning-core/src/optimizer/count_rel_table_optimizer.rs` — lines 37-43

**Issue:** Converts ANY Scan to CountRelTable without verifying the table is a relationship table. Comment on line 40-41 admits "we don't easily know if a table is REL or NODE here without catalog access."

**Impact:** `MATCH (n:Person) RETURN count(n)` returns relationship count instead of node count — **wrong results**.

**Fix:** Pass catalog access; only apply for relationship tables.

---

## 1.20 INDEX PUSHDOWN DESTROYS RECURSIVE JOIN MASK (index_pushdown.rs)

**FILE:** `crates/lightning-core/src/optimizer/index_pushdown.rs` — line 183

**Issue:** RecursiveJoin handler hardcodes `mask_id: None`, destroying any existing mask_id from previous optimization passes.

**Impact:** Semi-mask optimization for recursive joins is silently cleared — **data corruption in the plan**.

**Fix:** Preserve the mask_id: `mask_id: mask_id`.

---

## 1.21 FIVE OPTIMIZERS DISABLED WITH KNOWN BUGS (mod.rs)

**FILE:** `crates/lightning-core/src/optimizer/mod.rs` — lines 44-51

**Issue:** Five optimizers are disabled with known bugs but remain compiled code:
- `acc_hash_join_optimizer`, `count_rel_table_optimizer`, `factorization_rewriter`, `semijoin_pushdown`, `foreign_join_pushdown`

**Impact:** Dead code that could be accidentally re-enabled without fixing underlying issues.

**Fix:** Either fix bugs or remove the modules.

---

## 1.22 ORDER BY PUSHDOWN IS COMPLETE NO-OP (order_by_pushdown.rs)

**FILE:** `crates/lightning-core/src/optimizer/order_by_pushdown.rs` — lines 37-41

**Issue:** Generic catch-all returns `Ok(plan)` WITHOUT recursing into children. Only root node is visited. If root is not a Sort, entire optimization is silently skipped.

**Impact:** **Optimizer is a no-op.** No Order By pushdown ever applied.

**Fix:** Recurse into children for ALL operator types.

---

## 1.23 LOGICAL PLAN set_child DROPS JOIN/UNION RIGHT CHILD (logical_plan.rs)

**FILE:** `crates/lightning-core/src/planner/logical_plan.rs` — lines 220-228

**Issue:** `set_child` on Join/Union logs a warning but does NOT modify the operator — new child is silently dropped. Any optimizer using generic `get_child().cloned()` + `op.clone()` + `set_child()` on Join/Union produces an incorrect plan.

**Impact:** All optimizers using this pattern silently produce corrupted plans with missing Join/Union children.

**Fix:** Make set_child set the LEFT child (matching get_child), or panic with clear error.

---

## 1.24 DML MERGE USES ALL PROPERTIES AS INDEX KEYS (dml.rs)

**FILE:** `crates/lightning-core/src/processor/operators/dml.rs` — lines 929-935

**Issue:** MERGE operator uses every property assignment as a lookup key. Only the primary-key property should be used. If a non-PK property matches an existing row, a false match enters ON MATCH instead of ON CREATE.

**Impact:** MERGE creates spurious matches — **wrong semantics**.

**Fix:** Determine PK column from catalog; only use that column for index lookup.

---

## 1.25 is_read_only() IN INHERENT IMPL — BYPASSES CACHING (dml.rs, copy.rs)

**FILES:** `crates/lightning-core/src/processor/operators/dml.rs` — lines 292, 516, 700, 885, 1084
`crates/lightning-core/src/processor/operators/copy.rs` — line 459

**Issue:** All DML operators and PhysicalCopy define `is_read_only() -> bool { false }` in **inherent** impls (not trait impls). The trait default returns `true`. When dispatched through `Box<dyn PhysicalOperator>`, the vtable dispatches to the trait default. All write operators are treated as read-only.

**Impact:** **Write plans cached across transactions.** Transaction-specific state reused. Incorrect writes or data corruption. PhysicalCreate has no is_read_only at all.

**Fix:** Move all `is_read_only` into `impl PhysicalOperator for ...` blocks.

---

## 1.26 LIMIT OPERATOR RACE CONDITION (limit_skip.rs)

**FILE:** `crates/lightning-core/src/processor/operators/limit_skip.rs` — lines 36-60

**Issue:** Load-check-fetch_add sequence — two concurrent threads can both pass the gate and return full batches, exceeding the limit by up to (batch_size × concurrency) rows.

**Impact:** Queries with LIMIT return more rows than requested.

**Fix:** Use single atomic operation — fetch_add and check return value.

---

## 1.27 CROSS JOIN DATA LOSS (cross_join.rs)

**FILE:** `crates/lightning-core/src/processor/operators/cross_join.rs` — lines 76-84, 198

**Issue:** `concat_batches` error is silently swallowed (`if let Ok`). When it fails, multi-chunk right side only reads chunk[0] — all other chunks' data is lost.

**Impact:** Cross joins silently return incomplete results — **data loss**.

**Fix:** Propagate error with `?` instead of `if let Ok`.

---

## 1.28 UNWIND O(R²) EVALUATION (unwind.rs)

**FILE:** `crates/lightning-core/src/processor/operators/unwind.rs` — lines 69-76

**Issue:** Expression evaluated for each row, passing the full batch each time. O(R²) total work per chunk.

**Impact:** Extreme performance degradation on large chunks (exponential work factor).

**Fix:** Evaluate once per chunk, cache result, index per row.

---

## 1.29 DATABASE HEADER MAGIC NUMBER COMMENT (database_header.rs)

**FILE:** `crates/lightning-core/src/storage/database_header.rs` — line 21

**Issue:** `pub const MAGIC: [u8; 8] = *b"LIGHTNIG";` — the comment admits the spelling is wrong. If this ever changes, all existing database files become unreadable.

---

---

# SECTION 2: HIGH ISSUES

---

## Storage Layer

| File | Line(s) | Issue |
|------|---------|-------|
| overflow_file.rs | 62-67 | `write_string` returns `(0,0)` for ALL inputs — data-corrupting stub |
| buffer_manager.rs | 704-710 | Wrong error message when buffer pool exhausted but all pages pinned |
| column.rs | 907 | Unnecessary `vec![0u8; skip_bytes]` allocation |
| storage_manager.rs | 996-1022 | Duplicate code: `ensure_csr_fresh` and `rebuild_csr_if_stale` identical |
| column.rs | 2185-2197 | Compression analysis runs but pages never actually compressed |
| file_handle.rs | 111-132 | TOCTOU between `get_file_size` and `read_exact_at` |
| buffer_manager.rs | 140-145 | Per-page lock leak — HashMap grows unboundedly |
| buffer_manager.rs | 482-487 | Blocking I/O under shard write lock |
| trigram_index_worker.rs | 33 | 50ms idle wakeups via `recv_timeout` |
| prefetch.rs | 150-174 | Race in `report_prediction_result` auto-tuner counters |
| undo_buffer.rs | 233-251 | Prefix-match deletes wrong tables' files (case-insensitive collision) |
| wal.rs | 429-450 | `read_records_from` reads entire remaining WAL into memory |

---

## Optimizer Layer

| File | Line(s) | Issue |
|------|---------|-------|
| cardinality_estimator.rs | 90 | `unreachable!()` macro — panics on Logical Not expression |
| cardinality_estimator.rs | 46-50 | Join cardinality = max(left, right) — wildly inaccurate |
| count_rel_table_optimizer.rs | 50-70 | 3-table join pattern assumes exact shape — breaks with filter pushdown |
| factorization_rewriter.rs | 28-35 | Complete no-op — computes left_vars/right_vars but never uses them |
| filter_pushdown.rs | 267-271 | No conjunct splitting for partial pushdown across joins |
| foreign_join_pushdown.rs | 24-33 | Stub/no-op — Join arm does nothing beyond visiting children |
| join_reordering.rs | 27-34 | Limit handler is no-op — just recurses without pushing |
| limit_pushdown.rs | 36-43 | Double-clone and set_child data-loss for Join/Union |
| order_by_pushdown.rs | 24-35 | Sort-above-Projection rebuilds same structure (index remapping "complex") |
| semijoin_pushdown.rs | 100 | `_ => plan` fallthrough — all operators missing from apply_mask |
| semijoin_pushdown.rs | 218 | `_ => Ok(plan)` fallthrough — missing operator types in push_down |
| subquery_unnesting.rs | 32-39 | Join-based subquery unnesting is stub — never processes sub_child |
| topk_optimizer.rs | 55 | `_ => Ok(plan)` catch-all — 16+ operator types missing |
| acc_hash_join_optimizer.rs | 78-82 | Generic set_child silently drops Join/Union right children |
| mod.rs | 59 | Fixed-point convergence check uses unreliable `node_count()` |

---

## Processor / Operators

| File | Line(s) | Issue |
|------|---------|-------|
| nway_merge.rs | 153-174 | O(N×K) linear scan instead of heap-based K-way merge |
| flatten.rs | 35-44 | Creates one RecordBatch per row — massive allocation pressure |
| aggregate.rs | 189-196 | O(N×G) filter mask allocation per group |
| aggregate.rs | 263 | `group_indices.clone()` per group in tight loop |
| evaluator.rs | 605-653 | Overflow detection iterates 2-3 passes over data instead of 1 |
| evaluator.rs | 246-263 | Logical AND short-circuit only when ALL false — no partial short-circuit |
| intersect.rs | 126-148 | Deadlock-prone multi-lock acquisition pattern |
| union.rs | 54-67 | Double `Value::from_arrow` per row for dedup |
| projection_hash.rs | 76-114 | Per-row heap allocation for sort keys in TopK |
| gds/pagerank.rs | 68-70 | Dense allocation O(max_id) even for sparse node IDs |
| gds/pagerank.rs | 56, 60, 72-76 | u64::MAX sentinel conflicts with valid node IDs |
| gds/all_shortest_paths.rs | 109-110 | f64 used for u64 node IDs — precision loss beyond 2^53 |
| hash_join.rs | 347, 364, 404-472 | Global-to-local index truncation to u32 (overflow at >4B rows) |
| functions/registry.rs | 108-114, 157-163 | `new_unchecked` called without SAFETY comment |
| arrow_utils.rs | 38+ sites | `.expect()` panics in production code paths |
| mod.rs | 450 | Panic on NaN/Inf f64 JSON conversion |
| physical_plan.rs | 925-931 | `plan_expression` is stub — returns input unchanged |

---

## Server / API

| File | Line(s) | Issue |
|------|---------|-------|
| server.rs | 121 | `CorsLayer::permissive()` — any origin, any method, any header |
| routes/memory.rs | 82-98 | No limit on `entities.len()` — unbounded memory allocation |
| routes/memory.rs | 69, 80, 88, 121-125, 159, 183 | `top_k` unbounded — OOM vector search |
| routes/graph.rs | 51-52 | `req.hops` no upper bound — exponential traversal DoS |
| routes/subscribe.rs | 14-31 | No per-client limit on SSE connections — FD exhaustion |
| streaming.rs | 74 | `rx.recv()` blocks indefinitely with no timeout |
| error.rs | 51 | Internal error details sent to client — information disclosure |
| streaming.rs | 84 | Internal error string sent via SSE |
| routes/rag.rs | 15-22 | Config fields unbounded — resource exhaustion |
| models/request.rs | 69-132 | Multiple unbounded fields (top_k, hops, max_tokens) |
| config.rs | 106-128 | TLS cert/key paths not validated for existence at startup |
| routes/memory.rs | 46-58 | Mutation handlers don't check `config.read_only` |
| routes/admin.rs | 12-50 | No authentication on admin endpoints |
| routes/query.rs | 29-33 | Query injection — user string passed directly to execute |

---

## Core Infrastructure

| File | Line(s) | Issue |
|------|---------|-------|
| lib.rs | 567-586 | Catalog write lock held during synchronous file I/O |
| lib.rs | 312, 364-368 | No WAL rotation/truncation — unbounded growth |
| api.rs | 9-17 | `c_str_to_str` returns incorrect `'static` lifetime |
| api.rs | 13-17, 41-86, 89-95, 98-104 | Unsafe C FFI — dangling pointer and double-free risks |
| memory.rs | 967-968 | `expand()` loads entire edge table into memory — OOM for large graphs |
| memory.rs | 1268 | u64→f64 precision loss in ID resolution |
| memory.rs | 767-809 | O(k×n) consolidation complexity |
| fusion.rs | 56-58, 71-73, 406 | Strip-quotes sanitization facilitates injection |
| parser/mod.rs | 1389, 1401 | `unreachable!()` panics on grammar change |
| parser/mod.rs | 869, 885-889 | `var_len_bounds` always None (unused parse result) |
| wasm_function.rs | 66 | timeout stored but never enforced |

---

## Test Files

| File | Line(s) | Issue |
|------|---------|-------|
| bugfix_test.rs | 64-89 | Truncation test never asserts — always passes |
| comprehensive_test_4.rs | 1087-1095 | Asserts broken feature returns 0 rows |
| flatten_test.rs | 13, 105 | Hardcoded temp dir, no cleanup on panic |
| semi_mask_test.rs | 87-95 | Dead code — operator never executed |
| projection_pushdown_test.rs | 12 | INT32 may not be supported (everything else uses INT64) |
| comprehensive_test.rs | 253-296 | 210 near-identical generated tests |
| comprehensive_*.rs | many | 100+ silent assertion skipping sites |
| torture_test.rs | 119-145, 244-249, 309-328, 625-643 | Data mismatches only logged, not asserted |
| torture_test.rs | 1338-1348 | Silently skipped WAL corruption |
| crash_recovery_test.rs | 54, 88 | Hardcoded wal.lbug filename |

---

# SECTION 3: MEDIUM ISSUES

---

## Storage Layer

| File | Line(s) | Issue |
|------|---------|-------|
| buffer_manager.rs | 164, 197 | Dead variable `found_our_own` — set but never read |
| page_state.rs | 66-70 | TOCTOU in `unlock()` — non-atomic load-modify-store |
| column.rs | 182 | Linear scan of `pending_nulls` per `is_null` call |
| column.rs | 246-249 | Fragile coupling between pending_nulls offset and file byte offset |
| buffer_manager.rs | 601-602 | Checkpoint acquires shard write locks sequentially — cascading lock dependency |
| storage_manager.rs | 978-1022 | cardinality read without snapshot isolation |

## Index / Compression

| File | Line(s) | Issue |
|------|---------|-------|
| csr.rs | 338-354 | start_frame leaked on early return (no unpin) |
| csr.rs | 182-183 | O(n*m) deletion filtering |
| csr.rs | 157-164 | `compact` race: deletions lost after clear |
| hash_index.rs | 168-174 | 256-byte entries waste space |
| hash_index.rs | 97-111 | Direct write_page bypasses tx system |
| hnsw.rs | 73 | NaN distance → undefined Ord behavior |
| hnsw.rs | 147 | HashSet per search_layer (perf overhead) |
| hnsw.rs | 261 | Full embeddings clone under lock |
| inverted_index.rs | 161 | Reload error silently ignored |
| ivf.rs | 134-161 | O(N log N) sort instead of heap |
| trigram_index.rs | 117-127 | Backwards selectivity heuristic |
| trigram_index.rs | 250-252 | Triple write lock held simultaneously |
| trigram_index.rs | 283-288 | posting_size_sum accumulates across batches — wrong stats |
| vector_index.rs | 308-380 | tx shared across rayon threads — latent unsoundness |
| delta.rs | 35, 65 | i128→u64 truncation risk |
| dict.rs | 31-37 | Self-referential slice Vec |
| alp.rs | 113-119 | Brute-force 209 combinations per page |
| analyzer.rs | 105-144 | Range may overflow u64 cast |

## Optimizer

| File | Line(s) | Issue |
|------|---------|-------|
| cardinality_estimator.rs | 151-193 | Many missing operator types in `find_table_for_var` |
| join_reordering.rs | 150-158 | O(4^n) instead of O(3^n) submask enumeration |
| join_reordering.rs | 201-204 | Larger cardinality on left — backwards from optimal |
| join_reordering.rs | 176-179 | Greedy condition assignment — suboptimal placement |
| join_reordering.rs | 73-104 | Many missing operator types in `get_plan_vars` |
| filter_pushdown.rs | 249-250, 289-298 | Intersect pushdown only checks probe vars |
| index_pushdown.rs | 134-232 | Many missing operator type handlers |
| projection_pushdown.rs | 157-166 | Nested projections don't merge required_indices |
| subquery_unnesting.rs | 90 | String comparison `name == "NOT"` — case-sensitive |
| subquery_unnesting.rs | 134-150 | SemiJoin condition always uses index 0 for both sides |
| subquery_unnesting.rs | 126-128 | `create_semi_join` re-plans subquery (expensive + inconsistent) |
| topk_optimizer.rs | 26-27 | Only matches `Limit(Sort(grandchild))` with no intermediate ops |
| acc_hash_join_optimizer.rs | 41 | Hardcoded assumption that property indices 0/1 are node IDs |
| acc_hash_join_optimizer.rs | 78-82 | Double-clone per non-Join leaf |
| logical_plan.rs | 252-283 | `node_count` returns 0 for many operator types |
| logical_plan.rs | 284-303 | `get_variables` only handles Scan, IndexScan, Projection |

## Processor

| File | Line(s) | Issue |
|------|---------|-------|
| evaluator.rs | 288-301 | Logical OR has no short-circuit optimization |
| gds/gds_state.rs | 22, 27, 33 | No bounds checking on array index |
| path_probe.rs | 74 | Lossy debug-format output of path properties |
| path_probe.rs | 57-60 | Unnecessary full column scans for property retrieval |

## Server

| File | Line(s) | Issue |
|------|---------|-------|
| server.rs | 54-67 | x-request-id echoed back with `.unwrap()` — fragile |
| query.rs | 40 | Log injection via user-supplied query string |
| admin.rs | 57-61 | `Ordering::Relaxed` on all atomic counters |
| subscribe.rs | 25 | `.unwrap()` on `json_data(payload)` |
| streaming.rs | 20-42 | Multiple `.unwrap()` on downcast_ref |
| streaming.rs | 47-51 | Unrecognized Arrow type silently returns Null |
| config.rs | 113-114 | `max_connections` no upper bound |

## Core Infrastructure

| File | Line(s) | Issue |
|------|---------|-------|
| binder.rs | 1226-1238 | O(n) property lookup per item (no HashMap) |
| lib.rs | 567-574 | Deadlock potential — catalog lock then storage lock |
| catalog.rs | 474-484 | `remove_constraint` returns first matching table only |
| capi.rs | 185 | Double-unwrap on CString fallback |
| memory.rs | 1283-1335 | Hardcoded column indices (position-dependent) |
| memory.rs | 850-853 | Redundant data in PageRank metadata |
| lib.rs | 1543-1576 | Duplicate `mark_dirty` calls |
| transaction_manager.rs | 335-366 | Drop may leave inconsistent state |
| memory.rs | 686 | Debug `println!` in production |
| api.rs | 9-17 | Incorrect 'static lifetime on C string |

## Tests

| File | Line(s) | Issue |
|------|---------|-------|
| benchmark_suite.rs | 676-682 | No-op test (just prints) |
| benchmark_suite.rs | 565-670 | Flaky sleep-based concurrent benchmark |
| lightning_vs_sqlite.rs | 329 | Unused variable (commented out) |
| extreme_test.rs | 37-38, 52 | Sleep-based flakiness + fragile error matching |
| expression_test.rs | 21 | `:memory:` path may not be valid |
| flatten_test.rs | 105 | Temp dir leak on failure |
| comprehensive_* | many | Silent assertion skipping (100+ sites) |

---

# SECTION 4: LOW ISSUES

| Severity | File | Line(s) | Issue |
|----------|------|---------|-------|
| LOW | lib.rs | 32-35 | Misnamed function |
| LOW | lib.rs | 340-345 | Silent downgrade of index creation errors |
| LOW | lib.rs | 631-669 | Vacuum rolls back before checkpoint |
| LOW | capi.rs | 72, 104, 151 | Inconsistent drop patterns |
| LOW | catalog.rs | 341 | Hardcoded `.lbug` extension assumption |
| LOW | capi.rs | 185 | Fragile expect with justification comment |
| LOW | physical_plan.rs | 175, 186 | JOIN operations use `tracing::warn!` instead of debug |
| LOW | unwind_dedup_test.rs | 6 | Unused import |
| LOW | unwind_dedup_test.rs | 56 | NaN-panic risk in partial_cmp |
| LOW | expression_test.rs | 90-95 | Duplicated assertions |
| LOW | union_test.rs | 70, 76 | Fragile error string matching |
| LOW | torture_test.rs | 412-482 | Potentially slow (10K ops) |
| LOW | torture_test.rs | 73-74 | Redundant is_ok check |
| LOW | agent_memory.rs | 36 | Hardcoded temp dir (example) |
| LOW | csr.rs | 440-443 | Edges with src > num_nodes silently dropped |
| LOW | ivf.rs | 82-113 | 10 iterations regardless of convergence |
| LOW | dict.rs | 43 | Inaccurate buffer-size guard |
| LOW | vector_index.rs | 351 | Potential unaligned SIMD load on ARM |
| LOW | server.rs | 125 | IPv6 address produces unparseable format |
| LOW | server.rs | 133-135 | `expect("Failed to bind address")` — no retry |
| LOW | main.rs | 57-65 | Init panics with expect |
| LOW | main.rs | 60-61, 67 | Possible use-after-move of db |
| LOW | config.rs | 15 | Default host 0.0.0.0 binds to all interfaces |
| LOW | database_header.rs | 21 | Messy magic number comment |
| LOW | column.rs | 1948 | `parse_value` falls through to Null for unknown types |
| LOW | column.rs | 2040-2038 | 64-byte string slots waste space for short strings |
| LOW | buffer_manager.rs | 447-448 | Unused fh and page_idx parameters |

---

# SECTION 5: AREA-BY-AREA BREAKDOWN

| Area | Files | CRITICAL | HIGH | MEDIUM | LOW | TOTAL |
|------|-------|----------|------|--------|-----|-------|
| **Core Infrastructure** | 37 | 7 | 11 | 12 | 5 | 35 |
| **Storage Layer** | 14 | 4 | 8 | 8 | 4 | 24 |
| **Index/Compression** | 19 | 6 | 3 | 11 | 3 | 23 |
| **Optimizer** | 16 | 6 | 12 | 10 | 0 | 28 |
| **Processor/Operators** | 45 | 5 | 13 | 8 | 4 | 30 |
| **Server/Routes** | 18 | 0 | 3 | 14 | 4 | 21 |
| **Tests** | 31 | 2 | 5 | 10 | 6 | 23 |
| **TOTAL** | **180** | **30** | **55** | **73** | **26** | **184** |

---

# SECTION 6: REMEDIATION PRIORITIES

## P0 — Fix Immediately (10 items)

1. **Cypher injection** — Convert ALL string-interpolated queries to parameterized
2. **HNSW rng reseeding** — Fix random_level() with persistent RNG
3. **Inverted index data race** — Change read lock to write lock
4. **Trigram index unsorted lists** — Sort posting lists in insert()
5. **Bitpacking byte path** — Add bit-clearing before OR
6. **HashMap resize race** — Add exclusive access to resize
7. **analyzer_test.rs compilation** — Fix syntax errors
8. **is_read_only() in inherent impl** — Move to trait impl for all 7 operators
9. **Dangling tempdir in hash_join_test** — Bind TempDir to variable
10. **WAL CRC not verified** — Compare computed_crc vs stored_crc

## P1 — Fix This Week (15 items)

11. WASM sandbox — Add fuel metering + shared memory clearing
12. WASM path traversal — Validate against allowed directories
13. Unsafe frame mutation — Add safe API to Frame
14. CDC thread lock — Clone subscriber list before I/O
15. COPY path validation — Check against copy_base_dir
16. Buffer cache incoherence — Evict pages after direct file write
17. Prefix-match undo table deletion — Use exact match
18. Projection pushdown variable corruption — Don't set var to ""
19. Projection pushdown empty required_indices — All columns if no Projection
20. CountRelTable wrong table type — Check table type from catalog
21. Index pushdown RecursiveJoin mask — Preserve existing mask_id
22. LogicalPlan set_child Join/Union — Fix child assignment
23. DML MERGE index lookup — Use only PK column
24. Limit operator race — Single atomic fetch_add
25. Cross Join data loss — Propagate concat_batches error

## P2 — Fix This Sprint (20 items)

26. WAL unbounded growth — Add rotation/truncation
27. C FFI dangling pointers — Use Arc for handles
28. MemoryStore expand() loads all edges — Push down to CSR
29. O(k×n) consolidation — Use LSH batching
30. u64→f64 precision loss — Format as u64
31. Permissive CORS — Restrict origins
32. Unbounded batch/entity sizes — Add limits
33. SSE connection limits — Add semaphore
34. Blocking recv without timeout — Add tokio::time::timeout
35. Error info disclosure — Sanitize error responses
36. OrderByPushdown no-op — Fix recursion
37. Agg key dep optimizer broken — Fix generic catch-all recursion
38. NWayMerge O(N×K) — Binary heap merge
39. Unwind O(R²) — Cache expression evaluation
40. Flatten O(R) batch allocation — Batch output rows
41. Arrow utils .expect() — Replace with proper error returns
42. NaN JSON panic in mod.rs — Handle NaN gracefully
43. Cardinality estimator unreachable! — Fall back to default selectivity
44. Join reordering O(4^n) — Fix submask enumeration
45. TopK O(N) heap — Fixed-capacity min-heap

---

*End of Audit Report — 184 issues across 180 source files*
