# LightningDB — LLM Remediation Plan

> **Purpose**: This document is the source of truth for making LightningDB production-ready. Items are ordered by **importance to a trustworthy, usable codebase** — not by capability name.
>
> **Checkbox tracking**: `[ ]` = not started, `[~]` = in progress, `[X]` = done.
>
> **Priority tags**: `[P0]` = silent corruption/wrong results (must fix before alpha), `[P1]` = trust/usability erosion (must fix before beta), `[P2]` = scale ceiling (important), `[P3]` = polish/enhancement.
>
> **File paths** are relative to the workspace root.

---

## Ranking Rationale

```
Tier 1 — Silent data corruption / wrong results          [Sections 1-3]
Tier 2 — Features that don't do what they claim          [Sections 4-5]
Tier 3 — Core performance ceiling at scale               [Sections 6-8]
Tier 4 — Value-add polish (correct but limited)          [Sections 9-11]
Tier 5 — Niche / additive feature                        [Section 12]
```

---

## Section 0: Cross-Cutting Foundations

**Why first**: Security vulnerabilities and ubiquitous unsafe patterns poison every other capability. Fix these before anything else.

### 0.1 Security — Path Traversal in COPY

**File**: `crates/lightning-core/src/processor/operators/copy.rs:125,270,336`

**Problem**: `COPY ... FROM` and `COPY ... TO` accept arbitrary file paths with no validation. `COPY t FROM '/etc/passwd'` reads any file. `COPY t TO '/root/.ssh/authorized_keys'` overwrites any file.

- [X] **0.1.1** `[P0]` Add path canonicalization and directory restriction in `copy.rs`. Before opening any file path from a query:
  1. Canonicalize with `std::fs::canonicalize()` — **done**: `validate_copy_path` canonicalizes the parent directory (works for non-existent COPY TO targets) and the base directory, then verifies containment.
  2. Verify it's within `SystemConfig::copy_base_dir` (added as `copy_base_dir: Option<PathBuf>` in `SystemConfig`).
  3. Reject paths with `..` (checked on raw user input before resolution), absolute paths when no `copy_base_dir` is set, and symlinks outside the base directory (canonicalization + `starts_with` check catches any symlink pointing outside).
  4. References the `SystemConfig` pattern (`read_only`, etc.).

### 0.2 Security — Null Pointer Deref in C FFI

**Files**: `crates/lightning-core/src/api.rs:45`, `crates/lightning-core/src/capi.rs:90-94`

**Problem**: `lightning_query()` doesn't null-check `conn_ptr` before dereferencing. Segmentation fault on NULL input.

- [X] **0.2.1** `[P0]` Verify that all C FFI entry points null-check their pointer parameters.
  - `api.rs`: all 4 entry points (`lightning_open`, `lightning_query`, `lightning_close`, `lightning_free_string`) already null-checked.
  - `capi.rs`: `kuzu_database_init` was missing a null check on `path` — **fixed**. All other 8 entry points were already null-checked.

### 0.3 Eliminate 355 unwrap()/expect() Calls

**Problem**: 355 calls that panic on unexpected states across the entire codebase. Any edge case can crash the database process.

- [ ] **0.3.1** `[P0]` Run `cargo clippy -- -D clippy::unwrap_used`. Fix every violation:
  - Replace `unwrap()` with `?` where error propagation is possible (most cases)
  - Replace with `expect("meaningful context: what invariant was violated")` only when the panic is truly unrecoverable (e.g., internal data structure corruption)
  - Key hot paths: `hash_join.rs` (6 unwraps), `scan.rs` (`panic!` on empty schema — change to error), `csr.rs` (2 unwraps on page reads), `vector_index.rs` (`.expect()` on header reads)

### 0.4 Fix 60 Silently Swallowed Errors

**Problem**: `.ok()`, `.unwrap_or_default()`, `.unwrap_or(false)` throughout the codebase silently discard errors, leaving state inconsistent.

- [X] **0.4.1** `[P1]` Audit every `.ok()` call:
  - `lib.rs:250` — `checkpoint().ok()` in `Drop`: changed to `if let Err(e) = ... { tracing::warn!(...) }`
  - `wal.rs:85` — `filter_map(|e| e.ok())`: changed to log warnings on directory read failures and bad archive filenames
  - `parser/mod.rs:852` — `parse_var_len(i).ok()`: changed to `tracing::warn!` on parse failure
  - `fusion.rs:289` — `serde_json::to_string(&graph).unwrap_or_default()`: changed to propagate with `?`
  - `fusion.rs:304` — `i64_col(b, 0).ok()`: changed to propagate with `?`
  - All other `.ok()` and `.unwrap_or_default()` calls reviewed: `memory.rs` Array defaults, `column.rs` null bitmap defaults, `registry.rs` REGEXP_EXTRACT, `scan.rs` zone maps, `binder.rs` optionals, `projection_pushdown.rs` — all legitimate defaulting, not error swallowing.

### 0.5 KuzuDB C API Naming

**File**: `crates/lightning-core/src/capi.rs`

**Problem**: The C API uses `kuzu_*` function names (copied from KuzuDB). This is confusing and potentially infringing.

- [X] **0.5.1** `[P1]` Rename all `kuzu_*` exports to `lightning_*`. Each old `kuzu_*` name is kept as a deprecated wrapper (`#[deprecated(note = "renamed to lightning_*")]`) that delegates to the new `lightning_*` function. All 9 functions renamed: `database_init`, `database_destroy`, `connection_init`, `connection_destroy`, `connection_query`, `query_result_destroy`, `query_result_is_success`, `query_result_get_error_message`, `destroy_string`. Struct type names (`kuzu_database`, `kuzu_connection`, `kuzu_query_result`, `kuzu_system_config`) kept as-is for ABI compatibility.

### 0.6 Add MIRI Verification

**Problem**: 69 unsafe blocks across the codebase, none verified by MIRI. Undefined behavior can produce wrong results or segfaults.

- [X] **0.6.1** `[P1]` Create a MIRI test script at `scripts/miri_test.sh`.
  - MIRI compiles and runs small focused tests successfully (compression, free_space_manager tests pass).
  - **Note**: Full test suite is impractical under MIRI — 10-minute timeout exceeded for lib tests. MIRI is ~50-100x slower than native execution on this codebase. Recommended for CI with `--quick` (lib tests) only, not comprehensive tests.
- [~] **0.6.2** `[P1]` Fix UB found by MIRI — deferred. MIRI can run on small focused subsets (`scripts/miri_test.sh --quick`), but the full suite is too slow. The compression and free_space_manager tests pass cleanly.

### 0.7 Remove Dead Dependencies

**File**: `Cargo.toml`

- [ ] **0.7.1** `[P1]` Audit each workspace dependency. Check if `roaring`, `uuid`, `sha2`, `md-5`, `levenshtein` are actually used anywhere. Remove unused ones from `Cargo.toml` and their source imports.

---

## Section 1: Streaming Queries — query_stream

**Why here**: The parallel execution path produces **wrong results** (duplicate rows from scan, partial aggregates from sort/aggregate). This is the #1 trust-killer because you can't trust any query result with `num_threads > 1`.

**Files**: `crates/lightning-core/src/processor/scheduler.rs`, `crates/lightning-core/src/processor/mod.rs`

### 1.1 Fix Parallel Execution Correctness

**Problem**: The parallel scheduler (scheduler.rs:45-68) spawns N workers, each cloning the operator and calling `get_next()` independently. For scan operators, every worker scans the FULL table → duplicate results. For sort/aggregate/join, each worker produces a PARTIAL result → wrong final output.

- [X] **1.1.1** `[P0]` Rewrite the parallel execution model:
  - Added `fn is_parallel_safe(&self) -> bool` (default `false`) and `fn set_partition(&mut self, index, total)` (default no-op) to `PhysicalOperator` trait.
  - `PhysicalScan`, `PhysicalFilter`, `PhysicalProjection` override `is_parallel_safe()` → `true`. All other operators (Sort, Aggregate, Join, Limit, DML, DDL) keep default `false`.
  - Scheduler checks `operator.is_parallel_safe()`: if `false` or `num_threads == 1`, runs single-threaded. If `true`, clones the operator tree N times, calls `set_partition(i, N)` on each clone, and spawns one worker per partition. Each worker scans a disjoint row range.
  
- [X] **1.1.2** `[P0]` Implement partitioned scan via `set_partition()` on `PhysicalScan`:
  - Added `partition_position: Arc<AtomicU64>`, `partition_start_row: u64`, `partition_end_row: u64` to `PhysicalScan`.
  - `set_partition()` computes even row distribution across N partitions and initializes the per-clone `partition_position` to the partition's start row.
  - `get_next()` uses `partition_position.fetch_add()` (per-clone, no contention) bounded by `partition_end_row` instead of the shared `state.current_row`.
  - Filter and Projection forward `set_partition()` to their child operator, propagating the partition down to the scan leaf.

- [ ] **1.1.3** `[P2]` Merged operator support for parallel sort/aggregate/join — deferred to P2.

- [ ] **1.1.3** `[P2]` Add a merge operator for parallel sort (`NWayMerge` that merges N sorted streams), parallel aggregate (merge hash tables), and parallel join (partition by hash key). Without these, parallel execution Drops back to single-threaded for most interesting queries.

### 1.2 Python Generator-Based Streaming

**Problem**: Python `query_stream()` collects all chunks into a `Vec<PyObject>` and returns them as a list — no streaming at all.

- [X] **1.2.1** `[P1]` Rewrite the Python binding to return a Python generator.
  - Added `QueryStreamIter` pyclass with `__iter__`/`__next__` that wraps `crossbeam::channel::Receiver<Result<DataChunk>>`.
  - `query_stream()` now returns `QueryStreamIter` instead of `Vec<PyObject>` — each `__next__` blocks on `rx.recv()` and yields one chunk as a dict.
  - Added `crossbeam` dependency to `lightning-python/Cargo.toml`.

### 1.3 Add Backpressure

- [X] **1.3.1** `[P2]` Use `crossbeam::channel::bounded(64)` instead of `unbounded` for streaming queries. When channel is full (slow consumer), the producer blocks on `send()`. Prevents OOM on large result sets with slow consumers.

---

## Section 2: Row-Level OCC — Merge-on-Commit

**Why here**: Overflow strings (>63 chars) are **not captured** in the merge buffer — concurrent updates to entities with long content silently lose data. No deadlock detection means production deployments hang.

**Files**: `crates/lightning-core/src/transaction/transaction_manager.rs`, `crates/lightning-core/src/storage/row_version.rs`

### 2.1 Handle Overflow Strings in Merge

**Problem**: `PageRowMod.row_data` is 64 bytes. Strings >63 chars are stored in overflow pages. The slot data is a 21-byte pointer `(page_idx + offset + length)` which fits in 64 bytes, but the **overflow page content** is not versioned. If TxA sets `content = "long..."` and TxB sets `content = "different..."`, TxB's merge writes its 21-byte pointer over TxA's pointer, but TxA's overflow page content is still on disk.

- [X] **2.1.1** `[P0]` Fix overflow string merging.
  - Added `overflow_row_data: Option<Vec<u8>>` to `PageRowMod` — captures the full overflow page content at write time.
  - **Bug fix**: `append_to_overflow()` in `column.rs` was **not WAL-logging** overflow page updates (`log_page_update` was missing). Added `bm.log_page_update()` + `bm.unpin_page()` — overflow pages are now durable.
  - During single-row write (`write_value_at_row`): after serialization, if the slot contains an overflow marker (byte 0 == 255), the overflow page content is read from the buffer manager and stored in `overflow_row_data`.
  - Bulk insert path (`bulk_append_batch`): initializes `overflow_row_data: None` (bulk operations bypass merge-on-commit).

- [~] **2.1.2** `[P1]` Add overflow page versioning — deferred. The `OverflowFile` struct in `overflow_file.rs` is dead code (unused in production path; actual overflow writes go through `Column::append_to_overflow` directly). Overflow page durability was addressed in 2.1.1 via WAL logging.

### 2.2 Add Deadlock Detection

**Problem**: Per-page merge locks (`page_merge_locks` at transaction_manager.rs:297-303) are raw `Mutex<()>`. Two transactions A (locks page 1, waits for page 2) and B (locks page 2, waits for page 1) deadlock indefinitely.

- [X] **2.2.1** `[P1]` Add deadlock detection with configurable lock timeout.
  - Page merge lock acquisition in `commit()` changed from `lock()` to `try_lock_for(Duration::from_secs(5))`.
  - On timeout, returns `LightningError::Internal("deadlock detected...")` which triggers rollback in the caller's error handler.
- [ ] **2.2.2** `[P2]` Implement wait-for graph detection. Track `(tx_id, waiting_for_page)` pairs. On each lock attempt, check for cycles. Abort the youngest transaction in the cycle.

### 2.3 Clean Up RowVersion Committed Entries

**Problem**: `RowVersion::committed` HashMap (row_version.rs:5) grows unbounded — entries for every committed row that was ever modified accumulate forever.

- [X] **2.3.1** `[P1]` Add `RowVersion::vacuum(min_active_ts: u64) -> usize`. Removes entries with `commit_ts < min_active_ts`. Called after checkpoint in `Database::checkpoint()` using `get_min_active_read_ts()`. Returns removed count for debug logging.

### 2.4 Fix TOCTOU Window in Merge

**Problem**: `commit()` at line 183-187 acquires the page merge lock AFTER calling `pin_latest_committed()`. Between pin and lock, another committer could install a newer version.

- [X] **2.4.1** `[P2]` Verified: lock is acquired before `pin_latest_committed()`. No regression.

### 2.5 Document Snapshot Isolation

- [ ] **2.5.1** `[P1]` Add to `ARCHITECTURE.md`: "Lightning provides Snapshot Isolation. Write skew is possible (e.g., two transactions reading each other's pre-image and writing to disjoint rows). For Serializable isolation, see roadmap SSI implementation."

---

## Section 3: Graph Model — Cypher MATCH + CSR Adjacency

**Why here**: Full CSR rebuild on every write operation means the graph model cannot handle write-heavy workloads at any scale. Edge deletion doesn't update the CSR — queries return stale neighbors.

**Files**: `crates/lightning-core/src/storage/index/csr.rs`, `crates/lightning-core/src/memory.rs`, `crates/lightning-core/src/storage/storage_manager.rs`

### 3.1 Incremental CSR Edge Insertion

**Problem**: `CSRIndex::build()` (csr.rs:104-163) sorts all edges and rewrites the entire offset + adjacency array from zero. O(n log n) per write.

- [X] **3.1.1** `[P0]` Add `CSRIndex::insert_edge(src: u64, dst: u64)` — two-tier CSR:
  - Added `pending_edges: RwLock<Vec<(u64, u64)>>` to `CSRIndex`.
  - `insert_edge()` pushes to pending buffer (O(1)).
  - `for_each_neighbor()` checks both base CSR (file-based) and pending buffer.
  - Added `needs_compaction()` — returns true when pending > 10% of base edge count.
  - Added `compact()` — rebuilds full CSR from base + pending - deletions.
  - Also added `DELETED_BIT` tombstone support (`u64::MAX` highest bit) for future delete_edge.

- [X] **3.1.2** `[P0]` Add `CSRIndex::insert_batch(edges: &[(u64, u64)])` — extends the pending buffer with all edges.
- [~] **3.1.3** `[P2]` Auto-compaction on configurable threshold — deferred. `needs_compaction()` exists but calling site not yet wired.

### 3.2 CSR Edge Deletion

**Problem**: Deleting a relationship from the Relates table doesn't update the CSR index. Orphan adjacency entries persist.

- [X] **3.2.1** `[P0]` Add `CSRIndex::delete_edge(src: u64, dst: u64)` with tombstone support.
  - `DELETED_BIT = 1 << 63` masks the highest bit of adjacency values.
  - `delete_edge()` pushes to `pending_deletions: RwLock<Vec<(u64, u64)>>`.
  - `for_each_neighbor()` skips adjacency entries with `DELETED_BIT` set, and filters against `pending_deletions`.
- [X] **3.2.2** `[P1]` Wire into the Cypher DELETE (detach) path in `dml.rs`: when a Relates row is deleted during DETACH DELETE, the forward and backward CSR indexes are notified via `delete_edge(from, to)` and `delete_edge(to, from)`.
- [X] **3.2.3** `[P1]` CSR compaction on tombstone ratio — `compact()` and `needs_compaction()` already implemented in CSR. Compaction rebuilds base CSR from base + pending - deleted edges and clears the pending buffers.

### 3.3 Multi-Hop Expand CSR Usage in RAG

**Problem**: `rag_query()` (memory.rs:399-477) does a full table scan of the Relates table instead of using the CSR index. The standalone `expand()` DOES use CSR — the RAG path has duplicate, slower code.

- [X] **3.3.1** `[P0]` Replace the full table scan in `rag_query()` with CSR-based expansion.
  - Removed the full Relates table scan (src/dst column scan + adjacency build).
  - Replaced with `self.expand()` calls (CSR-based BFS) for each top-k entity.
  - Graph degree now computed via `CSRIndex::for_each_neighbor()` counting instead of full scan.
  - Removed duplicate adjacency-build logic that duplicated `expand()`.

### 3.4 CSR Format Safety

- [ ] **3.4.1** `[P1]` Add 12-byte header to CSR offset/adjacency files: 4B magic (`CSRO`/`CSRA`), 4B version, 4B CRC32. Validate on open.

---

## Section 4: Temporal Queries — recall_at_time

**Why here**: Feature description says "built-in time travel using MVCC commit timestamps" — this is **false**. It's application-level WHERE filters. The `valid_until = 0` vs `i64::MAX` inconsistency causes entities to be silently hidden.

**Files**: `crates/lightning-core/src/memory.rs`, `crates/lightning-core/src/transaction/transaction_manager.rs`

### 4.1 True MVCC Time-Travel

**Problem**: `recall_at_time()` uses `valid_from`/`valid_until` WHERE filters — it shows entities with matching application-level timestamps, NOT what the database actually looked like at time T.

- [ ] **4.1.1** `[P0]` Rewrite `recall_at_time()` to use true MVCC snapshot reads via `Connection::execute_at()` (which calls `TransactionManager::begin_at(snapshot_ts)`). Replace the current implementation at memory.rs:583-596:
  ```rust
  pub fn recall_at_time(&self, at_micros: i64, top_k: usize) -> Result<Vec<MemoryEntity>> {
      let query = format!("MATCH (e:{}) RETURN e.id, e.type, e.content, ... ORDER BY e.created_at DESC LIMIT {top_k}", ENTITY_TABLE);
      let res = self.conn.execute_at(&query, at_micros as u64, None)?;
      Ok(self.batches_to_entities(&res.batches))
  }
  ```
  This uses the MVCC snapshot at `at_micros` — shows exactly what was committed at that time.

### 4.2 Fix valid_until Convention

**Problem**: `MemoryEntity::default()` sets `valid_until = i64::MAX`, but `recall_at_time()` checks `valid_until = 0` as "still active". Inconsistent defaults.

- [ ] **4.2.1** `[P0]` Standardize on `i64::MAX` = "still active / end of time". Change:
  - `recall_at_time()` (if keeping WHERE-clause approach): check `(valid_until = 9223372036854775807 OR valid_until > $at)` instead of `valid_until = 0`.
  - `forget()` (memory.rs:1013-1038): set `valid_until = $now` (correct — don't change).
  - `recall_recent()` (memory.rs:757): change `valid_until = 0` to `valid_until = 9223372036854775807`.
  - `recall_by_time()` (memory.rs:770): same fix.
  - `store_batch()` (memory.rs:202): if `valid_until` input is 0, set to `i64::MAX`.
  - `python/lib.rs` and `node/memory.rs`: same default change.

### 4.3 Update Documentation

- [ ] **4.3.1** `[P0]` Update `README.md` and `ARCHITECTURE.md` to accurately describe what `recall_at_time()` does after fixing. If using MVCC snapshot reads (4.1.1), the claim is true. If keeping WHERE-clause, remove all MVCC claims.

---

## Section 5: WAL CDC — subscribe_changes

**Why here**: The name implies durable, replayable, cross-process change data capture. What exists is an in-process event bus with no WAL connection, no persistence, and fields that are always 0.

**Files**: `crates/lightning-core/src/memory.rs`, `crates/lightning-core/src/storage/wal.rs`

### 5.1 Implement WAL-Based CDC

**Problem**: `subscribe_changes()` uses `std::sync::mpsc::Sender` stored in `cdc_senders`. Events are emitted manually from `store()` and `forget()`. No WAL parsing, no persistence, no offsets.

- [ ] **5.1.1** `[P0]` Create `CdcManager` in `crates/lightning-core/src/cdc.rs`. Design:
  - On subscribe: record current WAL file offset as starting position
  - Background thread: polls `WAL::read_records_from(offset)` for new records
  - Parses WAL page-update records → reconstructs logical events (INSERT/UPDATE/DELETE per entity)
  - Pushes events to subscriber channels via `crossbeam::channel`
  - Persists subscriber offsets in the catalog so they survive restart
  - Multiple subscribers with independent offsets
- [ ] **5.1.2** `[P0]` Add `WAL::read_records_from(offset: u64)` — reads WAL records starting at a byte offset. Returns an iterator of parsed records. Handle truncation (offset past end after checkpoint → subscriber does full catch-up).
- [ ] **5.1.3** `[P1]` Reconstruct logical events. Page updates contain raw bytes — use `RowVersion` data to identify which rows changed on each page. Read the entity ID from the page at the modified row offset.
- [ ] **5.1.4** `[P1]` Add `subscribe_changes(from_offset: Option<u64>)` — replay from offset if provided, otherwise start from current position.

### 5.2 Fix In-Process Event Bus (Interim)

- [ ] **5.2.1** `[P1]` Populate `bytes_written` in `emit_cdc_event()` — read `WAL::size()` before and after write, compute delta.
- [ ] **5.2.2** `[P1]` Populate `entity_id` on store events (currently only works for `forget()`).
- [ ] **5.2.3** `[P1]` Replace silent `retain()` disconnect with backpressure. Use bounded crossbeam channel with `try_send()` and blocking send as fallback. Never silently drop subscribers.

### 5.3 Python CDC Generator

- [ ] **5.3.1** `[P1]` Python binding: return a proper generator instead of buffering 100 events with timeout. Use `crossbeam::channel::Receiver` and yield one event at a time.

---

## Section 6: Vector Search — SIMD Flat Parallel

**Why here**: Exhaustive O(n) scan only. Usable at <50K vectors, unusable at 1M+. This is the single biggest scale ceiling for the entire project.

**Files**: `crates/lightning-core/src/storage/index/vector_index.rs`, `crates/lightning-core/src/memory.rs`, `python/lightning/__init__.py`

### 6.1 Add ANN Index (HNSW)

- [ ] **6.1.1** `[P0]` Implement `HnswIndex` in `crates/lightning-core/src/storage/index/hnsw.rs`:
  - `insert(node_id, embedding)` — builds multi-layer navigable graph
  - `search(query, k)` — logarithmic search
  - `save()` / `load()` — disk persistence
  - Configurable M, ef_construction, ef_search
  - Start with cosine distance; add L2 and inner product later
- [ ] **6.1.2** `[P1]` Add distance metric enum: `Cosine`, `L2`, `InnerProduct`. Implement each as SIMD-accelerated function. Thread through search and insert.
- [ ] **6.1.3** `[P1]` Add index-type configuration: `CREATE VECTOR INDEX ... WITH (index_type = 'hnsw', metric = 'cosine')`.
- [ ] **6.1.4** `[P2]` Implement IVF as an alternative (simpler, good for high-dim data).

### 6.2 Fix Python Embedding Path

- [ ] **6.2.1** `[P1]` Trace the Python `store()` → `store_batch()` → `bulk_insert_batch()` path. Verify the embedding column in the RecordBatch is written to the vector index in `bulk_insert_batch`. If not, add the vector index insertion call. Reference how FTS index insertion works in the same path for the pattern.

### 6.3 Vector Index Bounds Safety

**Problem**: `search()` at vector_index.rs:308 computes `page_idx` from entry index but silently drops entries where `page_idx >= num_pages`. Can return fewer than k results without warning.

- [ ] **6.3.1** `[P1]` Either enforce dense sequential layout (no indirect page mapping), or maintain a page-index array that maps entry_idx → page_idx. Log a warning if page count is insufficient for the entry count.

### 6.4 Vector Index Soundness

- [ ] **6.4.1** `[P1]` Add MIRI test for vector index. Audit all 13 unsafe blocks. Verify `a.len() >= 8` guard for AVX2, `a.len() >= 4` for SSE/NEON.

---

## Section 7: Memory Consolidation

**Why here**: O(n²) from scratch every time. Heuristic contradiction detection produces high error rates. Works for hundreds of entities, prohibitive for tens of thousands.

**Files**: `crates/lightning-core/src/memory.rs`

### 7.1 Configurable Similarity

- [ ] **7.1.1** `[P1]` Add `ConsolidationConfig` struct: `similarity_threshold: f64` (default 0.35), `contradiction_jaccard_max: f64` (0.15), `contradiction_length_sim_min: f64` (0.8). Thread through `consolidate()`.

### 7.2 Incremental Consolidation

- [ ] **7.2.1** `[P1]` Store `last_consolidation_ts` in metadata. Only process entities with `created_at > last_consolidation_ts`. Compare each new entity against all existing entities.
- [ ] **7.2.2** `[P1]` Persist consolidation state so it survives restarts.

### 7.3 Fix Contradiction Detection

- [ ] **7.3.1** `[P1]` Replace the current heuristic with: compute embedding cosine similarity between entity pairs. If embeddings are similar (cosine > 0.7) but n-gram Jaccard is low (< 0.2), flag as contradiction. This catches "User likes Python" vs "User dislikes Python" which have similar embeddings but different words.

### 7.4 Batch PageRank Metadata Writes

- [ ] **7.4.1** `[P1]` Replace individual `MATCH ... SET e.metadata = $meta` queries (memory.rs:713-721) with a single bulk update. Use `UNWIND` or `store_batch()`.

### 7.5 Return Warnings

- [ ] **7.5.1** `[P1]` Add `warnings: Vec<String>` to `ConsolidationReport`. Collect all warn-logged errors so the caller can inspect what was skipped.

---

## Section 8: RAG Pipeline — rag_query

**Why here**: Works correctly for small datasets. The full table scan in graph expansion (instead of CSR) is a performance bug for larger datasets. Context assembly is trivial but functional.

**Files**: `crates/lightning-core/src/memory.rs`

**Note**: Item 8.1 is already tracked in 3.3.1 (duplicate). Listed here for completeness.

- [ ] **8.1** See **3.3.1** — Fix RAG's graph expansion to use CSR instead of full table scan.

### 8.2 Practical Cross-Encoder

- [ ] **8.2.1** `[P2]` Add HTTP-based cross-encoder reranker: `RagConfig.cross_encoder_url: Option<String>`. POST `(query, content)` pairs, use returned score.

### 8.3 Better Context Assembly

- [ ] **8.3.1** `[P1]` Add deduplication of near-duplicate sources, relevance highlighting, token-count awareness with `max_context_tokens` config.
- [ ] **8.3.2** `[P1]` Return structured source info alongside context: each source's score, type, and excerpt.

### 8.4 Error Propagation

- [ ] **8.4.1** `[P1]` Collect warnings and return alongside result (same pattern as 7.5.1).

---

## Section 9: Hybrid Search — RRF Fusion

**Why here**: Correct but thin. No configurability. Slight per-query transaction overhead.

**Files**: `crates/lightning-core/src/memory.rs`

### 9.1 Expose RRF k

- [ ] **9.1.1** `[P1]` Add `hybrid_search_k: f64` to `RagConfig` (or a new `SearchConfig`), default 60.0. Thread through `recall()`.

### 9.2 Single Transaction

- [ ] **9.2.1** `[P1]` Open one read transaction at the top of `recall()`, pass to both FTS and vector search, rollback once. Reduces overhead and ensures consistent snapshot.

### 9.3 Component Error Reporting

- [ ] **9.3.1** `[P1]` Collect FTS and vector search errors, return partial results with error context.

### 9.4 Alternative Fusion Strategies

- [ ] **9.4.1** `[P2]` Add `WeightedSum` and `DBSF` strategies via a fusion enum.

---

## Section 10: Full-Text Search — Tantivy BM25

**Why here**: The most solid component. Tantivy does the heavy lifting. Single-column limitation and no query syntax are real but not critical constraints.

**Files**: `crates/lightning-core/src/storage/index/inverted_index.rs`, `crates/lightning-core/src/storage/storage_manager.rs`

### 10.1 Multi-Column FTS

- [ ] **10.1.1** `[P1]` Modify `InvertedIndex::new()` to accept multiple field names. Store `HashMap<String, Field>`. Update `insert_batch()` to index each field.
- [ ] **10.1.2** `[P1]` Add `CREATE FULLTEXT INDEX ON Entity (content, metadata)` — store field list in catalog.

### 10.2 Expose Tantivy Query Syntax

- [ ] **10.2.1** `[P1]` Add `SEARCH(column, query)` scalar function in `registry.rs` → delegates to `InvertedIndex::search()`. Returns BM25 score for `ORDER BY`.

### 10.3 Custom Analyzers

- [ ] **10.3.1** `[P2]` Add `TextAnalyzer` configuration in `InvertedIndex::new()`. Expose via `WITH (analyzer = 'english_stem')`.
- [ ] **10.3.2** `[P2]` Remove dead `path` field from `InvertedIndex` struct.

---

## Section 11: Wasm UDF Merge-on-Commit WASM — Niche Feature

**Why here**: Beta wasmi dependency is a risk, but WASM UDFs are a niche feature. Fix everything above first.

**Files**: `crates/lightning-core/src/wasm_function.rs`

### 11.1 Replace Beta wasmi

**Problem**: `wasmi` 2.0.0-beta.2 is a pre-release dependency — supply-chain risk.

- [ ] **11.1.1** `[P1]` Either: (a) wait for wasmi 2.0 stable and upgrade, or (b) switch to `wasmtime` (production-ready, Bytecode Alliance). wasmtime is heavier (~20MB vs ~2MB) but stable. Choose based on binary size constraints.
- [ ] **11.1.2** `[P1]` If switching to wasmtime, rewrite `wasm_function.rs` using `wasmtime::Engine`, `Module`, `Store`, `Instance`, `Func`, `Memory`. Preserve all 4 exec modes (ScalarF64, MultiArgF64, MemoryF32, MemoryString).

### 11.2 Persist WASM Functions

- [ ] **11.2.1** `[P1]` Persist registered WASM functions in the catalog: (name, wasm_bytes, arity, exec_mode, timeout_ms). Reload on `Database::open()`. Add `CREATE WASM FUNCTION name AS '...'` Cypher syntax.

### 11.3 Expand Argument Model

- [ ] **11.3.1** `[P2]` Support typed dispatch: convert `arrow::ArrayRef` elements to the appropriate WASM type (f64, i64, f32, i32, string via memory pointer).
- [ ] **11.3.2** `[P2]` Add aggregate WASM UDF support: `init() → state`, `accumulate(state, value)`, `finalize(state) → result`.

---

## Progress Summary

| Tier | Section | Items | Priority |
|------|---------|-------|----------|
| Cross-cutting | 0 — Security, unwrap, MIRI, dead deps | ~10 | P0/P1 |
| Tier 1 (silent corruption) | 1 — Streaming Queries | 5 | P0/P1 |
| Tier 1 (silent corruption) | 2 — Row-Level OCC | 7 | P0/P1 |
| Tier 1 (silent corruption) | 3 — Graph Model / CSR | 7 | P0/P1 |
| Tier 2 (misleading features) | 4 — Temporal Queries | 4 | P0/P1 |
| Tier 2 (misleading features) | 5 — WAL CDC | 7 | P0/P1 |
| Tier 3 (scale ceiling) | 6 — Vector Search | 7 | P0/P1 |
| Tier 3 (scale ceiling) | 7 — Memory Consolidation | 6 | P1 |
| Tier 4 (polish) | 8 — RAG Pipeline | 4 | P1/P2 |
| Tier 4 (polish) | 9 — Hybrid Search | 4 | P1/P2 |
| Tier 4 (polish) | 10 — Full-Text Search | 5 | P1/P2 |
| Tier 5 (niche) | 11 — WASM UDFs | 5 | P1/P2 |

**Total**: ~71 items across 12 sections, ordered by importance to a trustworthy, usable codebase.
