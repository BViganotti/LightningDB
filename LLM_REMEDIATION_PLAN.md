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

- [ ] **0.2.1** `[P0]` In `api.rs:45`, add before dereferencing:
  ```rust
  if conn_ptr.is_null() { return std::ptr::null_mut(); }
  ```
  The same pattern already exists in `capi.rs:90-94` — verify that all other C FFI entry points (`lightning_open`, `lightning_close`, etc.) also null-check.

### 0.3 Eliminate 355 unwrap()/expect() Calls

**Problem**: 355 calls that panic on unexpected states across the entire codebase. Any edge case can crash the database process.

- [ ] **0.3.1** `[P0]` Run `cargo clippy -- -D clippy::unwrap_used`. Fix every violation:
  - Replace `unwrap()` with `?` where error propagation is possible (most cases)
  - Replace with `expect("meaningful context: what invariant was violated")` only when the panic is truly unrecoverable (e.g., internal data structure corruption)
  - Key hot paths: `hash_join.rs` (6 unwraps), `scan.rs` (`panic!` on empty schema — change to error), `csr.rs` (2 unwraps on page reads), `vector_index.rs` (`.expect()` on header reads)

### 0.4 Fix 60 Silently Swallowed Errors

**Problem**: `.ok()`, `.unwrap_or_default()`, `.unwrap_or(false)` throughout the codebase silently discard errors, leaving state inconsistent.

- [ ] **0.4.1** `[P1]` Audit every `.ok()` call:
  - If the error is meaningful → propagate with `?`
  - If the error should be logged → add `tracing::warn!("context: {e}")` before the `.ok()`
  - Key areas: `memory.rs` (dozens of warn! + continue — most should remain warnings, but some should propagate), `column.rs`, `hash_index.rs`

### 0.5 KuzuDB C API Naming

**File**: `crates/lightning-core/src/capi.rs`

**Problem**: The C API uses `kuzu_*` function names (copied from KuzuDB). This is confusing and potentially infringing.

- [ ] **0.5.1** `[P1]` Rename all `kuzu_*` exports to `lightning_*`. Keep `kuzu_*` as deprecated aliases with a compile-time deprecation warning. Functions: `kuzu_database_init`, `kuzu_connection_init`, `kuzu_connection_query`, `kuzu_database_destroy`, `kuzu_connection_destroy`.

### 0.6 Add MIRI Verification

**Problem**: 69 unsafe blocks across the codebase, none verified by MIRI. Undefined behavior can produce wrong results or segfaults.

- [ ] **0.6.1** `[P1]` Create a MIRI test script:
  ```bash
  MIRIFLAGS="-Zmiri-disable-isolation" cargo +nightly miri test --test comprehensive_test
  ```
- [ ] **0.6.2** `[P1]` Fix all UB found by MIRI. Common patterns to fix:
  - Raw pointer writes through `&[u8]` → must go through `UnsafeCell<[u8; PAGE_SIZE]>` (already done for `Frame.data` in 0.5.1 — verify all 11 write sites use `frame.data.get()`)
  - `Vec::set_len()` calls must be preceded by filling the buffer (check `read_page` patterns)
  - `copy_nonoverlapping` with overlapping source/destination

### 0.7 Remove Dead Dependencies

**File**: `Cargo.toml`

- [ ] **0.7.1** `[P1]` Audit each workspace dependency. Check if `roaring`, `uuid`, `sha2`, `md-5`, `levenshtein` are actually used anywhere. Remove unused ones from `Cargo.toml` and their source imports.

---

## Section 1: Streaming Queries — query_stream

**Why here**: The parallel execution path produces **wrong results** (duplicate rows from scan, partial aggregates from sort/aggregate). This is the #1 trust-killer because you can't trust any query result with `num_threads > 1`.

**Files**: `crates/lightning-core/src/processor/scheduler.rs`, `crates/lightning-core/src/processor/mod.rs`

### 1.1 Fix Parallel Execution Correctness

**Problem**: The parallel scheduler (scheduler.rs:45-68) spawns N workers, each cloning the operator and calling `get_next()` independently. For scan operators, every worker scans the FULL table → duplicate results. For sort/aggregate/join, each worker produces a PARTIAL result → wrong final output.

- [ ] **1.1.1** `[P0]` Rewrite the parallel execution model:
  - Add `fn is_parallel_safe(&self) -> bool` to the `PhysicalOperator` trait. Returns `true` only for stateless operators: Scan, Filter, Projection, Map, Expression evaluation.
  - For parallel-unsafe operators (Sort, Aggregate, Join, Union, Limit, TopK, DML, DDL), force single-threaded execution.
  - For parallel-safe operators: partition the scan's row range into N non-overlapping ranges. Each worker processes one partition. Results are merged downstream by a serial operator.
  - Reference `scheduler.rs:27-72` — the existing channel infrastructure is correct, only the partitioning is wrong.

- [ ] **1.1.2** `[P0]` Implement partitioned scan. Add `PhysicalScan::with_range(start_row: u64, end_row: u64)` that scans only rows in `[start_row, end_row)`. The scheduler creates N of these, each one disjoint.

- [ ] **1.1.3** `[P2]` Add a merge operator for parallel sort (`NWayMerge` that merges N sorted streams), parallel aggregate (merge hash tables), and parallel join (partition by hash key). Without these, parallel execution Drops back to single-threaded for most interesting queries.

### 1.2 Python Generator-Based Streaming

**Problem**: Python `query_stream()` collects all chunks into a `Vec<PyObject>` and returns them as a list — no streaming at all.

- [ ] **1.2.1** `[P1]` Rewrite the Python binding (`crates/lightning-python/src/lib.rs:317-328`) to return a Python generator. Use `crossbeam::channel::Receiver` and yield each `DataChunk` as a Python dict as it arrives. The generator should block on `rx.recv()`.

### 1.3 Add Backpressure

- [ ] **1.3.1** `[P2]` Use `crossbeam::channel::bounded(capacity)` instead of `unbounded` for streaming queries. When channel is full (slow consumer), block the producer. Prevents OOM.

---

## Section 2: Row-Level OCC — Merge-on-Commit

**Why here**: Overflow strings (>63 chars) are **not captured** in the merge buffer — concurrent updates to entities with long content silently lose data. No deadlock detection means production deployments hang.

**Files**: `crates/lightning-core/src/transaction/transaction_manager.rs`, `crates/lightning-core/src/storage/row_version.rs`

### 2.1 Handle Overflow Strings in Merge

**Problem**: `PageRowMod.row_data` is 64 bytes. Strings >63 chars are stored in overflow pages. The slot data is a 21-byte pointer `(page_idx + offset + length)` which fits in 64 bytes, but the **overflow page content** is not versioned. If TxA sets `content = "long..."` and TxB sets `content = "different..."`, TxB's merge writes its 21-byte pointer over TxA's pointer, but TxA's overflow page content is still on disk.

- [ ] **2.1.1** `[P0]` Fix overflow string merging. Options:
  - **Option A (recommended)**: Increase `row_data` to capture overflow page content inline. When a string exceeds inline capacity, read its overflow content into an `OverflowData` extension buffer.
  - **Option B**: Version the overflow pages. Each overflow page gets an MVCC version counter. During merge, re-read the latest overflow page version and apply modifications on top.
  - Implementation: `transaction_manager.rs:19-24` — add `overflow_row_data: Option<Vec<u8>>` to `PageRowMod`. When `element_size > 64` (or the data contains an overflow pointer), capture the full overflow content.

- [ ] **2.1.2** `[P1]` Add overflow page versioning. Each overflow page in `overflow_file.rs` should have an atomic version field. On read, verify version matches the expected commit timestamp. On write, create a new version.

### 2.2 Add Deadlock Detection

**Problem**: Per-page merge locks (`page_merge_locks` at transaction_manager.rs:297-303) are raw `Mutex<()>`. Two transactions A (locks page 1, waits for page 2) and B (locks page 2, waits for page 1) deadlock indefinitely.

- [ ] **2.2.1** `[P1]` Add a configurable lock timeout. Replace `lock()` with `try_lock_for(Duration)` or use `parking_lot::Mutex` which supports `try_lock`. Default timeout: 5 seconds. On timeout, rollback with `LightningError::Internal("deadlock detected")`.
- [ ] **2.2.2** `[P2]` Implement wait-for graph detection. Track `(tx_id, waiting_for_page)` pairs. On each lock attempt, check for cycles. Abort the youngest transaction in the cycle.

### 2.3 Clean Up RowVersion Committed Entries

**Problem**: `RowVersion::committed` HashMap (row_version.rs:5) grows unbounded — entries for every committed row that was ever modified accumulate forever.

- [ ] **2.3.1** `[P1]` Add `RowVersion::vacuum(min_active_ts: u64) -> usize`. Remove entries with `commit_ts < min_active_ts`. Call this after checkpoint. Returns number of removed entries for metrics.

### 2.4 Fix TOCTOU Window in Merge

**Problem**: `commit()` at line 183-187 acquires the page merge lock AFTER calling `pin_latest_committed()`. Between pin and lock, another committer could install a newer version.

- [ ] **2.4.1** `[P2]` Reorder: acquire merge lock FIRST, then pin. Change lines 183-187 in `transaction_manager.rs` from:
  ```rust
  let merge_lock = self.get_page_merge_lock(*file_id, *page_idx);
  let _merge_guard = merge_lock.lock();
  let latest_frame = bm.pin_latest_committed(...);
  ```
  to:
  ```rust
  let merge_lock = self.get_page_merge_lock(*file_id, *page_idx);
  let _merge_guard = merge_lock.lock();
  let latest_frame = bm.pin_latest_committed(...);
  ```
  (Already correct — verify it's not regressed.)

### 2.5 Document Snapshot Isolation

- [ ] **2.5.1** `[P1]` Add to `ARCHITECTURE.md`: "Lightning provides Snapshot Isolation. Write skew is possible (e.g., two transactions reading each other's pre-image and writing to disjoint rows). For Serializable isolation, see roadmap SSI implementation."

---

## Section 3: Graph Model — Cypher MATCH + CSR Adjacency

**Why here**: Full CSR rebuild on every write operation means the graph model cannot handle write-heavy workloads at any scale. Edge deletion doesn't update the CSR — queries return stale neighbors.

**Files**: `crates/lightning-core/src/storage/index/csr.rs`, `crates/lightning-core/src/memory.rs`, `crates/lightning-core/src/storage/storage_manager.rs`

### 3.1 Incremental CSR Edge Insertion

**Problem**: `CSRIndex::build()` (csr.rs:104-163) sorts all edges and rewrites the entire offset + adjacency array from zero. O(n log n) per write.

- [ ] **3.1.1** `[P0]` Add `CSRIndex::insert_edge(src: u64, dst: u64)`. The CSR is a prefix-sum offset array, so inserting an edge for node N requires incrementing all offsets from N+1 onward. Implement a two-tier CSR:
  - **Base CSR**: the compact prefix-sum representation (existing `build()` output)
  - **Pending buffer**: a `Vec<(u64, u64)>` of recently inserted edges
  - `for_each_neighbor()` checks both tiers
  - A background or write-time compaction threshold triggers `build()` when pending buffer exceeds ratio (e.g., pending > 10% of base)
- [ ] **3.1.2** `[P0]` Add `CSRIndex::insert_batch(edges: &[(u64, u64)])` — same two-tier approach.
- [ ] **3.1.3** `[P2]` Add auto-compaction on configurable threshold.

### 3.2 CSR Edge Deletion

**Problem**: Deleting a relationship from the Relates table doesn't update the CSR index. Orphan adjacency entries persist.

- [ ] **3.2.1** `[P0]` Add `CSRIndex::delete_edge(src: u64, dst: u64)`. Implement as tombstone: use a `DELETED_BIT` in the highest bit of the adjacency value (node IDs < 2⁶³). Update `for_each_neighbor()` to skip tombstones.
- [ ] **3.2.2** `[P1]` Wire into the Cypher DELETE path in `storage_manager.rs`: when a Relates row is deleted, call `CSRIndex::delete_edge()`.
- [ ] **3.2.3** `[P1]` Add compaction that rebuilds the CSR when tombstone ratio exceeds a threshold (e.g., 25%).

### 3.3 Multi-Hop Expand CSR Usage in RAG

**Problem**: `rag_query()` (memory.rs:399-477) does a full table scan of the Relates table instead of using the CSR index. The standalone `expand()` DOES use CSR — the RAG path has duplicate, slower code.

- [ ] **3.3.1** `[P0]` Replace the full scan in `rag_query()` (lines 399-477) with calls to `self.expand()` for each top-k entity. Remove the duplicate full-scan + adjacency-build logic. This also fixes edge type filtering in RAG's expansion path.

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
