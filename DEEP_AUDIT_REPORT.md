# Lightning Database — Deep Code Audit Report

**Date:** 2026-06-12  
**Scope:** All `.rs` files under `crates/` (562 files total, ~200 production source files audited)  
**Methodology:** File-by-file manual audit + automated pattern scanning  
**Excludes:** `.forge/worktrees/`, `target/`, test files (tested separately)

---

## Executive Summary

| Severity | Count |
|----------|-------|
| **CRITICAL** | 17 |
| **HIGH** | 52 |
| **MEDIUM** | 78 |
| **LOW** | 58 |
| **TOTAL** | **205** |

Top-level pattern counts across production code:

| Pattern | Count |
|---------|-------|
| `unsafe` blocks | 78 |
| `.unwrap()` (non-test) | ~150 |
| `.expect()` | 217 |
| `panic!()` (non-test) | 4 |
| Numeric `as` casts (truncation risk) | 667 |
| `.clone()` | 977 |
| Disabled optimizers | 6 |

---

## CRITICAL Issues (17)

### C1. Rate Limiter Completely Broken — No Rate Limiting Enforced
**File:** `crates/lightning-server/src/server.rs:79-90`

`Clone for AppState` creates a **new, independent `RateLimiter`** on every clone. Since `Router::with_state` clones the state, each request handler sees a different rate limiter with its own empty bucket map. The `Mutex<HashMap>` is never shared. **Zero rate limiting is actually enforced.**

```rust
// Each clone gets a fresh rate limiter — the old one is discarded
fn clone(&self) -> Self {
    Self {
        rate_limiter: RateLimiter::new(self.rate_limiter.max_requests, ...), // NEW
        ...
    }
}
```

**Fix:** Wrap `RateLimiter` in `Arc<Mutex<...>>`.

---

### C2. Connection Pool Not Shared Across Clones
**File:** `crates/lightning-server/src/server.rs:85-86`

Same cloning issue: `Clone for AppState` creates a new `ConnectionPool` per clone, making the pool useless.

---

### C3. Cypher Injection via String Formatting
**File:** `crates/lightning-core/src/memory.rs:716-721`

`entity_history` uses manual string escaping (`replace('\'', "\\'")`) instead of parameterized queries. Trivially bypassable with `\'; DROP TABLE Entity; --`.

```rust
let query = format!(
    "MATCH (e:{ENTITY_TABLE}) WHERE e.id = '{}' ...",
    entity_id.replace('\'', "\\'")  // NOT SAFE
);
```

**Also affected:** `memory.rs:680-686` (`recall_by_type` with `top_k` directly interpolated).

---

### C4. Unsafe `StringArray::new_unchecked` — Potential UB
**File:** `crates/lightning-core/src/processor/functions/registry.rs:163-169`

LOWER function uses `unsafe` `new_unchecked` on `StringArray`. If `to_ascii_lowercase()` produces invalid data, this is undefined behavior.

---

### C5. C FFI: `c_str_to_str` Returns Unsound `'static` Lifetime
**File:** `crates/lightning-core/src/api.rs:9-17`

Returns `&'static str` but the pointer is owned by C and could be freed at any time. **Use-after-free** if C frees the string.

---

### C6. C API: Double-Free Risk with `CString::from_raw`
**File:** `crates/lightning-core/src/api.rs:98-104`, `capi.rs:146-149`

`lightning_free_string` calls `CString::from_raw` on a pointer that may not have been allocated by `CString::into_raw`. UB if misused from C side.

---

### C7. C API: Use-After-Free in Database/Connection Lifetime
**File:** `crates/lightning-core/src/capi.rs:7-9, 52-56, 74`

Double `Box::into_raw` creates two separate heap allocations. If the C code destroys the database before the connection, `unsafe { &*(*database).database }` dereferences a dangling pointer.

---

### C8. Unsafe Frame Aliasing — Data Races in Buffer Manager
**File:** `crates/lightning-core/src/storage/buffer_manager.rs:34-50`

`Frame::as_mut_slice()` returns `&mut [u8]` from `UnsafeCell`. `as_slice()` and `as_mut_slice()` can be called concurrently via `Arc<Frame>` clones, violating Rust's aliasing rules. The `unsafe impl Send/Sync` masks data race UB.

---

### C9. TOCTOU Race in `reclaim_expired_versions` — Stale Data Flushed to Disk
**File:** `crates/lightning-core/src/storage/buffer_manager.rs:459-537`

Phase 1 (read lock) collects dirty data, releases lock. Phase 3 writes to disk. Between phases, another thread could modify the frame data, meaning **stale data is written to disk**.

---

### C10. `unsafe set_len` on Vec Before Fallible I/O — Uninitialized Memory
**File:** `crates/lightning-core/src/storage/column.rs:687-689, 697-699, 1010-1013, 1025-1028, 1074-1077, 1084-1088`

Multiple places call `unsafe { data_buf.set_len(N) }` before `read_pages`. If `read_pages` fails, the Vec contains uninitialized memory that will be dropped.

---

### C11. Bitpacking Shift Overflow — UB/Panic at `bit_width=64`
**File:** `crates/lightning-core/src/storage/compression/bitpacking.rs:66, 105`

`(1u64 << bit_width) - 1` when `bit_width == 64` causes shift overflow (UB in debug, panic in release). Affects both `write_bits` and `read_bits`.

---

### C12. HNSW `Ord` Contract Violation — Corrupts BinaryHeap
**File:** `crates/lightning-core/src/storage/index/hnsw.rs:80`

`unwrap_or(Ordering::Greater)` for NaN distances violates the requirement that `Ord` is total and transitive. This corrupts `BinaryHeap` ordering.

---

### C13. Unaligned Pointer Cast — UB on ARM
**File:** `crates/lightning-core/src/storage/index/vector_index.rs:350-352`

`std::slice::from_raw_parts(emb_bytes.as_ptr() as *const f32, dim)` — `emb_bytes` may not be aligned to 4 bytes. UB on ARM, slow on x86.

---

### C14. Weak Hash Function in Hash Index — DoS Vector
**File:** `crates/lightning-core/src/storage/index/hash_index.rs:246-299`

`compute_hash` uses simple LCG/xor mixing, NOT SipHash despite doc comments. Hash collisions cause O(n) bucket chains, enabling hash-flooding DoS.

---

### C15. Overflow File Infinite Loop on Corrupt Data
**File:** `crates/lightning-core/src/storage/overflow_file.rs:73-86`

`read_string` follows `next_page` pointers. If the pointer is corrupt (cycle to page 0), the function enters an infinite loop with no cycle detection.

---

### C16. Overflow File Pin Count Leak — Pages Never Evicted
**File:** `crates/lightning-core/src/storage/overflow_file.rs:46-87`

`read_string` calls `bm.pin_page` in a loop but NEVER calls `bm.unpin_page`. Each iteration pins a new page without unpinning the previous, leaking pin counts permanently.

---

### C17. Delta Compression Data Corruption
**File:** `crates/lightning-core/src/storage/compression/delta.rs:41, 71`

`(val as i128 - min as i128) as u64` — if `val < min`, the result is negative but cast to `u64`, producing a huge value. **Silently corrupts data.**

---

## HIGH Issues (52)

### Security

| # | File | Line | Issue |
|---|------|------|-------|
| H1 | `server.rs` | 132 | `x-forwarded-for` trusted without validation — rate limit bypass trivially |
| H2 | `error.rs` | 71-72 | Internal error messages leaked to HTTP clients (file paths, SQL syntax) |
| H3 | `request.rs` | 7-8 | Raw SQL query accepted from user input with no enforced parameterization |
| H4 | `admin.rs` | 12-50 | Admin endpoints (checkpoint, vacuum) not guarded by read-only mode |
| H5 | `request.rs` | 68,115 | No upper bounds on `top_k` and `hops` — resource exhaustion DoS vectors |
| H6 | `config.rs` | 65-66 | Auth token stored as plain `String`, cloned into every state copy |
| H7 | `subscribe.rs` | 18-37 | Thread leak on client disconnect — no cancellation for blocking CDC bridge |
| H8 | `query.rs` | 36-46 | No per-client concurrency limit — `spawn_blocking` pool exhaustion DoS |
| H9 | `wasm_function.rs` | 135-152 | WASM engine has no maximum memory limit — unbounded allocation possible |
| H10 | `wasm_function.rs` | 310-327 | WASM sandbox output can read beyond written region (info leak) |

### Memory Safety

| # | File | Line | Issue |
|---|------|------|-------|
| H11 | `buffer_manager.rs` | 770-780 | `thread::sleep` while holding shard write lock — severe lock convoy |
| H12 | `buffer_manager.rs` | 67 | LRU `page_locks` can evict in-use mutex — UB if lock guard references freed mutex |
| H13 | `hash_index.rs` | 106-124 | Unsafe `ptr::copy_to` (should be `copy_nonoverlapping`) |
| H14 | `hash_index.rs` | 170 | Unsafe cast to `[u8; PAGE_SIZE]` on potentially uninitialized memory |
| H15 | `vector_index.rs` | 231-248 | Unsafe writes without bounds check — buffer overflow if offset exceeds page |
| H16 | `column.rs` | 755-767 | `scan_string_direct` reads ENTIRE overflow file into memory (OOM risk) |
| H17 | `column.rs` | 619 | `.expect("internal invariant")` panics on missing null page |

### Data Integrity

| # | File | Line | Issue |
|---|------|------|-------|
| H18 | `wal.rs` | 555-567 | Commit records not CRC-validated during replay — corrupt commits accepted |
| H19 | `undo_buffer.rs` | 255-271 | `DropConstraint`/`DropIndex` rollback not implemented — permanent loss on rollback |
| H20 | `storage_manager.rs` | 1038-1043 | `apply_page` silently ignores unknown file IDs during WAL replay — data loss |
| H21 | `column.rs` | 1988-1999 | `serialize_value_into` silently drops strings without overflow FH |
| H22 | `free_space_manager.rs` | 50 | `load` silently returns empty on ANY I/O error (not just "file not found") |
| H23 | `column.rs` | 1989 | Element size defaults to 8 for many types — misaligned reads |
| H24 | `memory.rs` | 1033,1283 | `u64` IDs cast to `f64` — precision loss for IDs > 2^53 (data corruption at scale) |

### Query Correctness

| # | File | Line | Issue |
|---|------|------|-------|
| H25 | `evaluator.rs` | 808 | Float literal truncated to `i64` for Int64 column comparison (3.7 → 3) |
| H26 | `hash_join.rs` | 333-341 | Cross join only uses first build chunk — silently drops rows from chunk 1+ |
| H27 | `aggregate.rs` | 360 | `.expect()` on RecordBatch creation — panics on schema mismatch |
| H28 | `dml.rs` | 560-573 | Storage read lock held across entire delete loop — potential deadlock |
| H29 | `registry.rs` | 99-173 | LOWER registered twice — byte-level version overwrites Unicode-aware version |
| H30 | `all_shortest_paths.rs` | 91-101 | BFS finds only ONE shortest path, not ALL (misnamed algorithm) |
| H31 | `subquery_unnesting.rs` | 139-161 | Only first correlated variable used in semi-join condition — incorrect results |
| H32 | `logical_plan.rs` | 882-886 | Duplicate `Unwind` match arm — copy-paste error / dead code |
| H33 | `recursive_join.rs` | 73 | `FixedBitSet` capacity based on `next_row_id` — unbounded memory for large graphs |
| H34 | `logical_plan.rs` | 921 | Catch-all `_ => plan` silently drops unknown clauses from query plan |
| H35 | `scan.rs` | 603-607 | Pushdown filter error silently swallowed — may return unfiltered rows |
| H36 | `fusion.rs` | 487 | PageRank writeback error silently discarded |
| H37 | `mod.rs` (processor) | 236 | i32 overflow in list offset computation |
| H38 | `mod.rs` (processor) | 363 | Lossy i64→f64 cast in `from_arrow` |

### Concurrency

| # | File | Line | Issue |
|---|------|------|-------|
| H39 | `buffer_manager.rs` | 353-384 | Prefetch I/O under shard write lock — extends lock hold time |
| H40 | `buffer_manager.rs` | 525-526 | `dirty_count` can underflow — no guard against concurrent decrement |
| H41 | `prefetch.rs` | 140-167 | Multiple write locks acquired simultaneously — potential deadlock |
| H42 | `ddl.rs` | 348-371 | Non-atomic DDL execution — catalog and storage modified under separate locks |
| H43 | `transaction_manager.rs` | 371-411 | `Drop` impl acquires locks — potential deadlock if dropped during `begin()` |

### Compression/Index

| # | File | Line | Issue |
|---|------|------|-------|
| H44 | `compression/mod.rs` | 74-93 | Missing bounds checks in `compress_next_page`/`decompress_from_page` — panics |
| H45 | `alp.rs` | 73 | Float precision loss in encode — values round to i64::MAX/MIN silently |
| H46 | `delta.rs` | 71 | Unsigned to signed overflow in decompression |
| H47 | `dict.rs` | 33,45-46 | Missing bounds check; silent failure returns `(0,0)` |
| H48 | `rle.rs` | 24-25,74 | Missing bounds checks in compress/decompress |
| H49 | `hnsw.rs` | 123 | Division by zero in cosine distance for zero vectors |
| H50 | `hnsw.rs` | 210-217 | Silent node overwrite — stale neighbor references |
| H51 | `trigram_index.rs` | 271,279,289 | O(n²) batch insert — linear search in posting lists |
| H52 | `csr.rs` | 221,247,284 | Out-of-bounds access; OOM from corrupted file size |

### Other

| # | File | Line | Issue |
|---|------|------|-------|
| H53 | `memory.rs` | 446-463 | `recall_stream` spawns untracked thread per call — unbounded threads |
| H54 | `registry.rs` | 239-903 | 40+ `.expect("type mismatch")` calls — panics on unexpected user types |
| H55 | `recursive_join.rs` | 115-128 | Fallback full table scan loads ALL relationships into memory |
| H56 | `logical_plan.rs` | 864-868 | f64→u64 cast for SKIP/LIMIT can truncate negative values |

---

## MEDIUM Issues (78)

### Server/HTTP (10)

| File | Line | Issue |
|------|------|-------|
| `config.rs` | 94 | Buffer pool size can overflow — no upper bound validation |
| `config.rs` | 145-167 | TLS cert/key paths not validated for existence |
| `error.rs` | 44-68 | Error classification via substring matching — fragile |
| `extract.rs` | 24-26 | `ConnectionPool::acquire()` creates new connection — no actual pooling |
| `streaming.rs` | 20-43 | `unwrap()` on `downcast_ref` — panics on unexpected Arrow types |
| `request.rs` | 13-14 | `timeout_ms` has no maximum — client can hold connection forever |
| `memory.rs` | 50 | `req.id` unsanitized — potential path traversal/injection |
| `memory.rs` | 85-101 | Batch handler processes entire batch synchronously — no progress feedback |
| `admin.rs` | 52-93 | Metrics endpoint unauthenticated — leaks operational data |
| `subscribe.rs` | 29 | `json_data().unwrap()` — panics on serialization failure |

### Storage (15)

| File | Line | Issue |
|------|------|-------|
| `buffer_manager.rs` | 255 | `page_to_slots` Vec can accumulate stale entries |
| `storage_manager.rs` | 286-293 | `append_row` silently ignores extra column values |
| `storage_manager.rs` | 267,369 | Double-counting cardinality in `batch_append_rows` vs `flush_buffer` |
| `storage_manager.rs` | 588-737 | `create_table` doesn't rollback partial file creation on error |
| `wal.rs` | 336-340 | WAL replay applies pages before checking all commits |
| `wal.rs` | 131,419 | `archive_seq` uses Relaxed ordering — duplicate sequence numbers |
| `undo_buffer.rs` | 72-85 | `UpdateColumn`/`DeleteNode` undo records do nothing |
| `undo_buffer.rs` | 202-221 | Rollback order issue with `AlterRenameTable` |
| `overflow_file.rs` | 97 | `write_string` doesn't handle string > u32::MAX |
| `file_handle.rs` | 122-143 | `add_new_page` doesn't extend file on disk |
| `database_header.rs` | 56-63 | No atomic write for header save — crash corrupts header |
| `free_space_manager.rs` | 35-46 | Same non-atomic write issue |
| `free_space_manager.rs` | 30-33 | Free pages not zeroed — sensitive data leak |
| `prefetch.rs` | 10-12 | Transition maps grow unbounded — memory leak |
| `trigram_index_worker.rs` | 88-100 | `flush()` is fire-and-forget; `Drop` doesn't wait |

### Compression (10)

| File | Line | Issue |
|------|------|-------|
| `compression/mod.rs` | 127 | `ConstantCompression` hardcodes 8-byte elements |
| `alp.rs` | 62-88 | No validation on `fac_idx`/`exp_idx` — out-of-bounds panic |
| `bitpacking.rs` | 17-19 | Silent truncation on bit_width==64 path |
| `bitpacking.rs` | 7-10 | `assert!` in non-test code — should return error |
| `dict.rs` | 58 | dict_count=0 triggers bit_width=64 overflow |
| `dict.rs` | 87,96-97 | Missing bounds checks in decompress |
| `rle.rs` | 27-33 | Run count can overflow u32 |
| `analyzer.rs` | 27-28 | HLL accuracy with DefaultHasher biased for small inputs |
| `analyzer.rs` | 67-92 | `skip_minmax` path skips `all_same` check — wrong Constant detection |
| `analyzer.rs` | 136-151 | `Number` type cast to i64 truncates large floats |

### Index (8)

| File | Line | Issue |
|------|------|-------|
| `hash_index.rs` | 460 | `allocate_overflow_page` race condition |
| `hash_index.rs` | 480-503 | `write_entry_to_page` no bounds check |
| `hnsw.rs` | 11 | Weak PRNG with fixed seed |
| `hnsw.rs` | 136 | Level calculation produces usize::MAX for u=0 |
| `hnsw.rs` | 157-161 | `visited_pool` Mutex contention bottleneck |
| `inverted_index.rs` | 49 | Hardcoded 50MB writer memory |
| `inverted_index.rs` | 81-83 | Write lock acquired per document in batch |
| `vector_index.rs` | 80 | f64 fallback for dot product — 2x slower, different results |

### Processor (15)

| File | Line | Issue |
|------|------|-------|
| `mod.rs` | 139-142 | `execute_stream` replaces root with placeholder — can't call twice |
| `physical_plan.rs` | 261-262 | Index into empty comparisons vec — panic |
| `evaluator.rs` | 675 | Division by zero returns error instead of NULL (non-standard SQL) |
| `evaluator.rs` | 690-693 | `i64::MIN % -1` undefined behavior |
| `cross_join.rs` | 141 | `left_batch.clone()` on every get_next — unnecessary allocation |
| `aggregate.rs` | 323 | HashMap iteration non-deterministic — GROUP BY order varies |
| `aggregate.rs` | 243-256 | Sort-based aggregation O(groups × keys) — no key caching |
| `aggregate.rs` | 296 | Group-by keys always output as Utf8 — lossy for numbers |
| `projection.rs` | 52-56 | String-based deduplication of expressions — false positives possible |
| `sort.rs` | 98-103 | Hard limit on sort memory — returns error instead of external sort |
| `union.rs` | 69 | O(collision_rows) lookup per row — O(n²) for UNION DISTINCT |
| `limit_skip.rs` | 68-74 | `clone_box` creates new Mutex — breaks shared synchronization |
| `nway_merge.rs` | 147-194 | Materializes all output as `Vec<Value>` — doubles memory usage |
| `index_scan.rs` | 71 | `read_ts` from construction, not from transaction — stale data |
| `dml.rs` | 960-967 | MERGE evaluates assignments with wrong batch context |

### Optimizer (8)

| File | Line | Issue |
|------|------|-------|
| `optimizer/mod.rs` | 39-51 | **6 optimizers disabled** due to known correctness issues |
| `optimizer/mod.rs` | 59-63 | Fixed-point detection via `format!("{:?}")` — O(n) per iteration, fragile |
| `filter_pushdown.rs` | 247-267 | Filter pushed into scan without validating variable ownership |
| `cardinality_estimator.rs` | 52-54 | Clones entire plan trees just to pass to `estimate_selectivity` |
| `subquery_unnesting.rs` | 95 | NOT EXISTS detection broken — `Function("NOT",...)` doesn't match `Not(Exists(...))` |
| `limit_pushdown.rs` | 18-58 | Does not actually push limits down — effectively a no-op |
| `order_by_pushdown.rs` | 18-48 | Does not actually push ORDER BY — no-op |
| `factorization_rewriter.rs` | 19-53 | Doesn't actually factorize — no-op |

### Parser/Binder (5)

| File | Line | Issue |
|------|------|-------|
| `parser/mod.rs` | 886-889 | Variable-length bounds silently ignored |
| `parser/mod.rs` | 1434-1511 | Unknown data types silently become `String` |
| `parser/mod.rs` | 200 | ORDER BY parse failures silently swallowed |
| `binder.rs` | 1267 | `RETURN *` column order non-deterministic (HashMap iteration) |
| `binder.rs` | 1795 | Macro substitution doesn't recurse into Case/List/Map/Lambda/Exists |

### Client/Other (12)

| File | Line | Issue |
|------|------|-------|
| `memory.rs` | 150 | MinHash similarity uses wrong denominator — artificially low scores |
| `memory.rs` | 770-784 | Consolidation O(n²) with sampling — still expensive |
| `memory.rs` | 420,561,585 | `partial_cmp` panics on NaN scores |
| `memory.rs` | 222-224 | `as_micros() as i64` — silent overflow |
| `wasm_function.rs` | 361-404 | WASM string mode data leakage between rows |
| `lazy_catalog.rs` | 99-104 | Pointer comparison for catalog identity — spurious failures |
| `transaction_manager.rs` | 267-272 | Page merge locks accumulate if transaction abandoned |
| `shortest_path.rs` | 213-216 | Path stored as Utf8 instead of List — wrong data type |
| `gds_state.rs` | 12-14 | `Vec<AtomicU32>` pre-allocated per node — expensive for large graphs |
| `registry.rs` | 320-353 | LIST_FILTER/TRANSFORM don't handle non-list inputs |
| `registry.rs` | 1142-1143 | Debug logging of user data in CONTAINS function |
| `column.rs` | 38 | `pending_nulls` Vec grows unbounded |

---

## LOW Issues (58)

### Performance

| # | File | Issue |
|---|------|-------|
| L1 | `server.rs:25-53` | Rate limiter allocates String per request |
| L2 | `server.rs:25-53` | Rate limiter never evicts stale entries — unbounded growth |
| L3 | `query.rs:22` | Query string cloned only for logging |
| L4 | `hnsw.rs:296-301` | O(n²) neighbor pruning |
| L5 | `inverted_index.rs:161-163` | Reader reload failure silently continues with stale data |
| L6 | `column.rs:2043` | `element_size` defaults to 8 for many types |
| L7 | `binder.rs:669-673` | StandaloneCall parameters computed but never used |
| L8 | `binder.rs:306` | `get_type()` returns `Any` for aggregates |
| L9 | `join_reordering.rs:198` | Cloning full plan trees in DP transitions |
| L10 | `mod.rs:302-321` | Weak hash combination for `Value::Map` |
| L11 | `mod.rs:170-173` | String values truncated to 64 bytes silently |
| L12 | `arrow_utils.rs:284` | UInt64→f64 cast lossy for values > 2^53 |
| L13 | `cross_join.rs:12-15` | Mixed atomic/non-atomic in SharedCrossJoinBuild |
| L14 | `intersect.rs:137` | Vec cloned on each hash probe |
| L15 | `nway_merge.rs:32-36` | HeapEntry Ord ignores child_idx tiebreaker |
| L16 | `path_probe.rs:74` | Debug formatting used for JSON output |
| L17 | `memory.rs:969` | `_edge_types` parameter accepted but never used |
| L18 | `memory.rs:224` | `now_micros` returns 0 on clock error |
| L19 | `arrow FFI:36-47` | No schema validation on FFI export |
| L20 | `lib.rs:136` | `max_num_threads: 0` ambiguous default |
| L21 | `mod.rs:326-338` | PartialOrd returns None for List/Struct/Map — ORDER BY silent no-op |
| L22 | `column.rs:1617-1708` | Dead code: `bulk_append_primitive_fast` and `bulk_append_string_fast` |
| L23 | `page_state.rs:73` | `unlock` uses `store` instead of CAS — can lose dirty bits |

### Safety/Correctness

| # | File | Issue |
|---|------|-------|
| L24 | `health.rs:3-8` | Health endpoint doesn't verify database connectivity |
| L25 | `server.rs:96-101` | `x-request-id` from client trusted without length limit |
| L26 | `server.rs:300-303` | TLS server has no graceful shutdown |
| L27 | `main.rs:29` | `parse_lossy(&log_filter)` silently discards invalid filter directives |
| L28 | `main.rs:57-65` | `expect()` panics on startup with no actionable context |
| L29 | `config.rs:117` | `vacuum_interval_ms` silently clamped to max(100) |
| L30 | `error.rs:71-76` | ErrorResponse never populates request_id |
| L31 | `extract.rs:75-79` | RequestId generates UUID if middleware missing — misleading |
| L32 | `streaming.rs:74` | Stream silently terminates if sender drops without error |
| L33 | `streaming.rs:66` | New connection per stream — no pooling |
| L34 | `request.rs:24` | StoreRequest.id has no validation |
| L35 | `rag.rs:16-21` | RagConfig defaults have no upper bounds |
| L36 | `rag.rs:28` | Empty embedding fallback silently degrades |
| L37 | `memory.rs:281-295` | ConsolidateRequest validation is all-or-nothing |
| L38 | `graph.rs:55-69` | Entity expansion clones every field |
| L39 | `parser/mod.rs:729,739` | SKIP/LIMIT parse errors silently become 0 |
| L40 | `parser/mod.rs:107` | `normalize_query` comment stripping operates on bytes, not chars |
| L41 | `parser/mod.rs:46-67` | `preprocess_distinct_functions` O(n²) allocations |
| L42 | `ast.rs:339` | `Literal::Number` uses f64 — precision loss for large integers |
| L43 | `logical_plan.rs:216` | `set_child` misses `Profile` operator |
| L44 | `logical_plan.rs:273-274` | `node_count` misses many operators |
| L45 | `logical_plan.rs:278-297` | `get_variables` incomplete — misses Join right side, Union right side |
| L46 | `expression_visitor.rs:151-156` | Subquery expressions not rewritten |
| L47 | `topk_optimizer.rs:24-30` | TopK only fuses `Limit(Sort(...))` — misses Projection/Filter between |
| L48 | `agg_key_dependency_optimizer.rs:48` | Heuristic: `prop_idx==0` assumed to be PK |
| L49 | `pagerank.rs:120,148` | Errors during neighbor traversal silently ignored |
| L50 | `gds_state.rs:31` | `SeqCst` ordering used where `AcqRel` suffices |
| L51 | `column_stats.rs:83-105` | `update` doesn't track `distinct_count` |
| L52 | `table_stats.rs:6` | `cardinality` never updated |
| L53 | `column_stats.rs:115-117` | Unbounded `page_bounds` growth from corrupted page_idx |
| L54 | `ivf.rs:76-78` | Non-random centroid initialization |
| L55 | `page_state.rs:73` | `unlock` can overwrite concurrent `set_dirty` |
| L56 | `storage_manager.rs:829-844` | `remove_table` doesn't clean up file_handles map |
| L57 | `comprehensive_test_3.rs:69` | **FIXME: Hangs — 10K CREATE statements cause deadlock** |

---

## Disabled Optimizers (Known Correctness Issues)

6 out of ~15 query optimizers are **disabled** due to known bugs:

| Optimizer | Reason |
|-----------|--------|
| `index_pushdown` | (not specified) |
| `projection_pushdown` | Needs cross-operator expression index remapping |
| `semijoin_pushdown` | Physical planner mask lifecycle issues |
| `acc_hash_join_optimizer` | Mask lifecycle issues |
| `agg_key_dependency_optimizer` | Incorrect group-by dependency analysis in edge cases |
| `count_rel_table_optimizer` | Wrong COUNT results for single-relationship tables |

This means the query optimizer is operating at ~40% capability.

---

## Recommendations (Priority Order)

### Immediate (P0) — Data Loss / Security
1. Fix rate limiter sharing (`Arc<Mutex<RateLimiter>>`)
2. Fix connection pool sharing
3. Parameterize all Cypher queries in `memory.rs`
4. Add bounds checks to all compression encode/decode paths
5. Fix `Frame` aliasing safety in buffer manager
6. Fix overflow file infinite loop and pin leak
7. Fix delta compression signed/unsigned cast
8. Validate C FFI lifetimes and add safety documentation

### Short-term (P1) — Correctness
9. Enable and fix disabled optimizers (especially `projection_pushdown`)
10. Fix cross join multi-chunk bug
11. Fix LOWER function duplicate registration
12. Add CRC validation for WAL commit records
13. Implement DropConstraint/DropIndex rollback
14. Fix subquery unnesting multi-variable correlation
15. Fix HNSW Ord contract violation
16. Add error handling for all `expect()` calls in user-facing paths

### Medium-term (P2) — Performance
17. Replace O(n²) algorithms (trigram batch insert, UNION DISTINCT)
18. Add external sort support (currently errors on large datasets)
19. Optimize aggregation group key comparison
20. Fix lock convoy in buffer manager eviction
21. Replace weak hash functions with SipHash/ahash
22. Add bounds to all user-configurable parameters (top_k, hops, timeout)

### Long-term (P3) — Quality
23. Reduce `.unwrap()` count in production code (currently ~150)
24. Replace numeric `as` casts with `try_into()` where truncation is possible
25. Add memory limits to WASM sandbox
26. Implement proper connection pooling
27. Fix all no-op optimizer passes
28. Add comprehensive fuzz testing for compression/decompression
