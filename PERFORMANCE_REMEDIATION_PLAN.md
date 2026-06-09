# LightningDB — Performance Remediation Plan

> **Purpose**: Performance issues discovered across the entire codebase. Organized by **impact on real-world throughput and latency**.
>
> **Checkbox tracking**: `[ ]` = not started, `[~]` = in progress, `[X]` = done.
>
> **Priority tags**: `[P0]` = correctness-level (wrong perf or OOM), `[P1]` = order-of-magnitude improvement available, `[P2]` = significant (2-10x), `[P3]` = incremental improvement.
>
> **Impact notation**: `N/A` = not applicable per this item.

---

## Ranking Rationale

```
Tier 1 — Catastrophic: O(N²+) algorithms, OOM, 1000x waste         [Sections 1-3]
Tier 2 — Order-of-magnitude: 10-100x slowdowns on common ops       [Sections 4-8]
Tier 3 — Significant: 2-10x improvement available                  [Sections 9-15]
Tier 4 — Incremental: <2x improvement, or niche                   [Sections 16-20]
```

---

## Section 1: Catastrophic — O(N²) / OOM / 1000x Waste

### 1.1 Catalog Saves on Every Transaction After First Threshold

**File**: `crates/lightning-core/src/catalog/lazy_catalog.rs:106-107`

**Problem**: After the first 1000 transactions trigger a save, `save_internal` increments `last_saved_tx_count` by **1** instead of setting it to the current transaction count. So `current_tx_count - 1 >= 1000` is always true — every subsequent transaction triggers a full catalog serialization to disk. For a catalog with 1000 tables at 500KB, that's 500GB of writes for 1M transactions instead of 500MB intended.

- [X] **1.1.1** `[P0]` Fix `save_internal` to set `last_saved_tx_count = current_tx_count` (the value passed to `save_if_needed`), not `fetch_add(1)`.

**Impact**: 1000x more disk writes than intended. Production databases with >1000 transactions will spend all their time writing the catalog.

### 1.2 WASM Store Created Per Row (1M Store Creations for 1M Rows)

**File**: `crates/lightning-core/src/wasm_function.rs:150,223,276`

**Problem**: In ScalarF64 mode, a fresh `wasmi::Store` is created INSIDE the per-row loop (line 150). Each Store creation allocates WASM linear memory, initializes globals, sets up the call stack — ~5-50µs per creation. For 1M rows: 5-50 seconds of overhead just recreating the same store.

- [X] **1.2.1** `[P0]` Move Store creation outside the row loop. Create one Store before the loop, reuse it across rows. For stateless WASM functions, this is safe. For stateful ones, reset memory between calls instead of recreating.

**Impact**: 1M-row WASM UDF evaluation goes from seconds to minutes. Also flagged: the `wasm` binary bytes are captured in the closure unnecessarily (line 117), holding hundreds of KB of leaked memory.

### 1.3 Node.js Streaming Formats Entire Array Per Cell

**File**: `crates/lightning-node/src/streaming.rs:40`

**Problem**: `format!("{:?}", col)` calls Debug on the **entire** Arrow array, not on the individual cell. For a column with 1000 rows, every cell gets ALL 1000 values serialized. For a 10-column, 1000-row result, each cell contains a 10KB debug string = 100KB per row = 100MB total output. O(rows × cols × rows) = O(N²) in data size.

- [X] **1.3.1** `[P0]` Replace with per-cell extraction: `string_array.value(row_idx)`, `int_array.value(row_idx)`, etc. Match on `col.data_type()` and downcast to the correct typed array.

**Impact**: Streaming a 10K-row result from Node.js would produce ~10GB of debug-formatted output and take hours.

### 1.4 Cross Join Materializes All Values as Vec<Value> (OOM)

**File**: `crates/lightning-core/src/processor/operators/cross_join.rs:138-168`

**Problem**: Every cell of the cross product is allocated as a heap `Value` via `Value::from_arrow()`. For L=1000, R=100K, C=10: that's 1B heap-allocated Value objects. Each Value for a string column also heap-allocates the string. This single line is the most egregious memory issue in the engine.

- [X] **1.4.1** `[P0]` Rewrite cross join to output using Arrow `take()` with index arrays instead of materializing Value objects. Build index arrays: left indices = `[0...0, 1...1, ...]` (each repeated R times), right indices = `[0,1,...,R-1, 0,1,...]` (repeated L times), then call `take()` per column. This avoids all per-cell Value allocation.

**Impact**: Any cross join between moderately sized tables (L=100, R=100K) will OOM the process.

### 1.5 Aggregate Sort Comparator Allocates Two Strings Per Comparison

**File**: `crates/lightning-core/src/processor/operators/aggregate.rs:219`

**Problem**: `format!("{:?}", va).cmp(&format!("{:?}", vb))` allocates TWO heap Strings per comparison in the sort-based aggregation path. For 200K rows, this is ~200K × log₂(200K) × 2 ≈ 7M String allocations. This is the single worst allocation pathology in the query engine.

- [X] **1.5.1** `[P1]` Implement `Value::cmp` using type-specific numeric/string comparison instead of Debug formatting. For primitive types (Int64, Float64), compare the raw numeric values. For strings, use `str::cmp`.

**Impact**: Sort-based aggregation on 500K rows takes minutes instead of seconds.

### 1.6 Cross-Join Double Conversion: Arrow → Value → Arrow

**File**: `crates/lightning-core/src/processor/operators/cross_join.rs:182,189`

**Problem**: Data is converted from Arrow → Value (lines 149-157), then back from Value → Arrow (lines 182, 189). This round-trip doubles the conversion cost on top of the O(N²) materialization.

- [X] **1.6.1** `[P1]` Remove the round-trip. Use `arrow::compute::take` with index arrays directly from the original Arrow arrays. (Same fix as 1.4.1.)

---

## Section 2: BFS / Graph O(V²) Degradation

### 2.1 BFS Visited Set Uses Vec<u64> — O(V) Contains Check

**File**: `crates/lightning-core/src/processor/operators/gds/all_shortest_paths.rs:92`

**Problem**: `self.bfs_visited.contains(&n)` on a `Vec<u64>` is O(visited_count) per check. For a graph with 500K visited nodes and a node with degree 10K, that's 10K × 500K = 5B comparisons for one node's expansion. Worst case: the BFS is O(V²).

- [X] **2.1.1** `[P1]` Replace `Vec<u64>` with `FixedBitSet` (as done correctly in `recursive_join.rs:70`). For sparse graphs, use `HashSet<u64>`.

**Impact**: BFS on any graph with 100K+ nodes becomes unusable.

### 2.2 DELETE DETACH Scans All Relationship Tables Per Node

**File**: `crates/lightning-core/src/processor/operators/dml.rs:395-433`

**Problem**: For each deleted node, the detach loop scans EVERY row of EVERY relationship table from position 0. For deleting 1000 nodes with 5 rel tables of 100K rows each: 1000 × 5 × 100K = 500M row scans. O(N × M) where N = deleted nodes, M = total edges.

- [ ] **2.2.1** `[P1]` Use the CSR index (`fwd_csr`, `bwd_csr`) to find incident edges in O(degree) per node instead of O(rel_table_size). The CSR already exists — use it for detach lookups.

**Impact**: DELETE with DETACH on any non-trivial graph takes hours.

### 2.3 BFS Distance Storage Uses HashMap Instead of Vec

**File**: `crates/lightning-core/src/processor/operators/gds/all_shortest_paths.rs:22`

**Problem**: Distance storage uses `HashMap<u64, u32>`. Since node IDs are densely allocated, a `Vec<u32>` indexed by node ID is more memory-efficient (8 bytes vs ~32 bytes per entry) and faster (direct index vs hashing).

- [X] **2.3.1** `[P2]` Use `Vec<u32>` indexed by node ID, initialized to `u32::MAX`.

---

## Section 3: Fusion Module — 1M Queries for PageRank

### 3.1 Fusion PageRank Issues Per-Node Queries Per Iteration

**File**: `crates/lightning-core/src/fusion.rs:334-371`

**Problem**: For each of up to 100 iterations and each node (10K+), a separate Cypher query is issued: `MATCH (n:CodeNode {id:'...'})-[r]->(t) RETURN t.id`. For 10K nodes × 100 iterations = 1M queries. Each query goes through the full parse→bind→plan→execute pipeline.

- [X] **3.1.1** `[P1]` Load the entire adjacency graph into memory once (scan all edges), compute PageRank locally in Rust, write back final ranks in bulk. Memory for 1M edges = ~16MB — acceptable.

**Impact**: Fusion PageRank on a 10K-node graph takes 50-200 seconds. Feature is unusable at any real scale.

### 3.2 `lookup_node_names` Issues One Query Per Node ID

**File**: `crates/lightning-core/src/fusion.rs:124-148`

**Problem**: N separate MATCH queries for N node IDs.

- [X] **3.2.1** `[P2]` Use a single `WHERE n.id IN [...]` batch query.

### 3.3 `compute_architecture_cohesion` Issues 6 Full Graph Scans

**File**: `crates/lightning-core/src/fusion.rs:197-228`

**Problem**: Six separate Cypher queries, each scanning all edges.

- [X] **3.3.1** `[P2]` Combine into a single query: `MATCH (n)-[r]->(m) RETURN type(r), n.file_path, m.file_path`.

---

## Section 4: WAL Write Contention

### 4.1 Single WAL Mutex Serializes All Concurrent Writers

**File**: `crates/lightning-core/src/storage/wal.rs:42`

**Problem**: The WAL uses a single `Mutex<File>` for all write operations. Every concurrent writer serializes through this Mutex. Under 8+ concurrent writers, throughput collapses to single-threaded WAL write speed (~50-100 MB/s regardless of core count).

- [ ] **4.1.1** `[P1]` Implement group commit. Batch pending page updates from all threads, write them as a single I/O. Use a leader-follower pattern: one thread collects pending writes, issues a single `writev`, signals completion.
- [ ] **4.1.2** `[P2]` Alternative: segment the WAL into multiple files with round-robin assignment per transaction.

**Impact**: 8× throughput degradation under concurrent writes vs group commit.

### 4.2 CRC32 Computed Per WAL Record (CPU Overhead)

**File**: `crates/lightning-core/src/storage/wal.rs:166-172`

**Problem**: CRC32 on 4KB of data takes ~1-3µs (software CRC32, ~1.5 GB/s). At 10K page updates/sec, CRC computation consumes 10-30 ms/s of CPU.

- [X] **4.2.1** `[P2]` Switch to CRC32C with hardware acceleration (SSE 4.2 on x86, ARMv8 on ARM). CRC32C runs at ~10-20 GB/s, reducing overhead by ~10×. Or use xxHash3 (~20 GB/s) if cryptographic strength isn't needed.

### 4.3 WAL Align Position Allocates Vec for 0-7 Bytes

**File**: `crates/lightning-core/src/storage/wal.rs:151`

**Problem**: `vec![0u8; padding]` heap-allocates for every WAL record. Padding is at most 7 bytes.

- [X] **4.3.1** `[P2]` Use `write_all(&[0u8; 8][..padding])` — stack-allocated array.

---

## Section 5: File I/O Overhead

### 5.1 `file.metadata()` Syscall Per Page Read

**File**: `crates/lightning-core/src/storage/file_handle.rs:63,82`

**Problem**: Every `read_page` and `read_pages` issues `self.file.metadata()?` (an `fstat` syscall). For page-at-a-time reads (typical OLTP), each read_page pays ~100-500ns for the syscall. At 100K page reads/sec: 10-50ms/sec of pure syscall overhead.

- [X] **5.1.1** `[P1]` Cache the file length in an `AtomicU64` on FileHandle. Update on writes, read from cache on reads. Only syscall as a fallback.

**Impact**: 30-50% overhead on page read throughput.

### 5.2 `get_file_size()` Also Issue `fstat` Every Call

**File**: `crates/lightning-core/src/storage/file_handle.rs:141-143`

**Problem**: Same root cause — returns `file.metadata()?.len()` instead of using cached `num_pages × PAGE_SIZE`.

- [X] **5.2.1** `[P1]` Return `self.num_pages.load() * PAGE_SIZE as u64`.

### 5.3 Checkpoint Syncs Files Redundantly Across Shards

**File**: `crates/lightning-core/src/storage/buffer_manager.rs:579-586`

**Problem**: Phase 2 iterates all 16 shards and calls `fh.sync()` for each file found in each shard. A file with pages in 8 shards is sync'd 8 times. `fsync` is one of the most expensive syscalls (~1-10ms).

- [X] **5.3.1** `[P2]` Deduplicate file handles before the sync loop. Use the `synced_fids` set from Phase 1 to sync each file exactly once.

---

## Section 6: Per-Row Value Allocation in Scalar Functions

### 6.1 Every Scalar Function Iterates Row-by-Row (Anti-Vectorized)

**File**: `crates/lightning-core/src/processor/functions/registry.rs` — every function, ~3500 lines

**Problem**: Almost every scalar function follows the pattern:
```rust
for i in 0..num_rows {
    let val = Value::from_arrow(&args[0], i);  // heap allocation per row
    // ... scalar processing ...
    builder.append_value(result);
}
```
This negates ALL Arrow columnar benefits. No use of Arrow compute kernels (which exist for sqrt, sin, cos, abs, ceil, floor, round, cast, comparison, boolean ops, etc.). Each `Value::from_arrow` for a string allocates a heap String.

- [X] **6.1.1** `[P1]` Use `arrow::compute::kernels` for math functions (sqrt, sin, cos, abs, ceil, floor, round) directly on `Float64Array` — these accept whole arrays.
- [X] **6.1.2** `[P1]` Use `arrow::compute::kernels::comparison` for comparison operators — `eq`, `neq`, `lt`, `gt`, `leq`, `geq` work on whole arrays.
- [X] **6.1.3** `[P1]` Use `arrow::compute::kernels::boolean` for AND, OR, NOT — bitwise bitmap operations on whole arrays.
- [ ] **6.1.4** `[P2]` For string functions that can't use Arrow kernels, batch-process with SIMD-accelerated operations where possible.

**Impact**: All scalar function evaluation on 10M+ rows is 10-100x slower than vectorized alternatives. ALL ~60+ functions in registry.rs are affected.

### 6.2 Literal Expansion Creates Full Array Per Filter

**File**: `crates/lightning-core/src/processor/evaluator.rs:29-41`

**Problem**: `Float64Array::from_value(*n, num_rows)` allocates a full array of N identical values for every literal in a filter. For `WHERE age > 25` on 1M rows: a 1M-element array of `25.0` is allocated. With 5 literals: 5 arrays = ~40MB.

- [X] **6.2.1** `[P1]` Use Arrow scalar comparison kernels that accept a literal value directly. `arrow::compute::kernels::cmp::gt(l, Scalar::from(25.0))` avoids the expansion.

**Impact**: Each filter literal causes O(num_rows) memory allocation and copying.

### 6.3 Value::from_arrow for Strings Always Allocates

**File**: `crates/lightning-core/src/processor/arrow_utils.rs:274`

**Problem**: `Value::String(a.value(i).to_string())` clones the string from the Arrow array. For every cell extracted from a string column, a heap-allocated String is created.

- [ ] **6.3.1** `[P2]` Use `Arc<str>` or pass through `&str` where the Value is transient. This is a wider refactor but would dramatically reduce allocation pressure in expression evaluation.

---

## Section 7: Buffer Manager Contention

### 7.1 CLOCK Eviction Is O(capacity) Per Victim

**File**: `crates/lightning-core/src/storage/buffer_manager.rs:598-636`

**Problem**: The CLOCK hand scans from `clock_ptr` through every slot until it finds a victim. Worst case (all unpinned pages are dirty-uncommitted): the loop runs through the entire shard capacity. Each iteration does 2× AtomicU64::load. For a 4K-slot shard × 16 shards = 65K atomic loads per eviction.

- [X] **7.1.1** `[P2]` Maintain a "free candidate" MPSC queue. When a page's pin_count drops to 0 in `unpin_page`, push its slot index. `evict_with_clock` pops from the queue first, falls back to CLOCK scan only when empty. Makes eviction O(1) in the common case.

### 7.2 `update_timestamps` Acquires Write Lock for Read-Only Scan

**File**: `crates/lightning-core/src/storage/buffer_manager.rs:402-419`

**Problem**: Acquires a shard WRITE lock for the entire operation, even though the mutation is just a version `store` on a field that could be updated atomically.

- [X] **7.2.1** `[P2]` Use read lock for the HashMap lookup and slot scan. Use `AtomicU64::compare_exchange` for the version update without the shard lock.

### 7.3 `reclaim_expired_versions` Holds Write Lock Across Full Scan

**File**: `crates/lightning-core/src/storage/buffer_manager.rs:425-457`

**Problem**: WRITE lock acquired on a shard, then EVERY slot is scanned. During this, no other thread can pin/unpin/create-versions in that shard.

- [X] **7.3.1** `[P2]` Phase 1: scan under READ lock to build candidate list. Phase 2: acquire WRITE lock only to mutate candidates.

### 7.4 Prefetch Reads Pages Into Discarded Buffers

**File**: `crates/lightning-core/src/storage/buffer_manager.rs:348-349`

**Problem**: Speculative prefetch reads pages into a local `[u8; PAGE_SIZE]` that is immediately dropped. The data is NOT inserted into the buffer pool. The only hope is the OS page cache, which `pread` doesn't always populate.

- [X] **7.4.1** `[P2]` Either (a) insert prefetched pages into the buffer pool (find/allocate a slot, mark clean), or (b) disable prefetch entirely. Measure hit rate before enabling.

### 7.5 `dirty_page_count` Acquires 16 Shard Locks, Scans All Slots

**File**: `crates/lightning-core/src/storage/buffer_manager.rs:643-654`

**Problem**: O(total_slots) work for a simple counter.

- [X] **7.5.1** `[P3]` Maintain an `AtomicU64 dirty_count`. Increment on dirty set, decrement on dirty clear.

---

## Section 8: Storage Manager Write Contention

### 8.1 Write Buffer Flush Converts Rows Via Per-Cell Builder Appends

**File**: `crates/lightning-core/src/storage/storage_manager.rs:110-272`

**Problem**: `Vec<Vec<Value>>` → Arrow array conversion uses per-cell `builder.append_value()` calls. For 100 columns × 200 rows: 20,000 individual builder method calls with type dispatch, bounds checking, null handling, Vec growth.

- [X] **8.1.1** `[P1]` Store the write buffer in column-oriented format (one `Vec<Value>` per column). Use Arrow's bulk builders (`append_values`, `append_slice`) for primitive columns.

**Impact**: 3-5× slower batch insert than direct column extraction.

### 8.2 `append_plain_value` Acquires Stats Write Lock Per Row

**File**: `crates/lightning-core/src/storage/column.rs:1294-1298`

**Problem**: Every `append_plain_value` call acquires `self.stats.write()`, calls `stats.update(val)` and `stats.update_page_bounds(...)`. Under concurrent writers, this lock serializes appends.

- [X] **8.2.1** `[P2]` Use atomic counters for `num_values` and `null_count`. Defer `min`/`max`/`page_bounds` computation to `optimize()` or checkpoint time.

**Impact**: 3-10× throughput collapse under concurrent writers.

### 8.3 Per-Txn Commit Iterates ALL Tables to Sync Stats

**File**: `crates/lightning-core/src/transaction/transaction_manager.rs:136-150`

**Problem**: Every write transaction commit iterates ALL node_tables and ALL rel_tables to sync num_rows. For 10K tables: 10K HashMap lookups and `next_row_id.load()` per commit.

- [X] **8.3.1** `[P2]` Track which tables were modified in the transaction. Only iterate modified tables.

**Impact**: For 10K tables, each commit spends ~1-5ms just iterating unmodified tables.

---

## Section 9: Query Compilation Pipeline Overhead

### 9.1 Single-Pass Optimizer Misses Interactions

**File**: `crates/lightning-core/src/optimizer/mod.rs:56-59`

**Problem**: Each optimizer rule runs exactly once in order. If filter pushdown enables new projection pushdown opportunities (or vice versa), they're missed. No fixed-point iteration.

- [X] **9.1.1** `[P2]` Run optimizer rules to a fixed point (max 3-5 iterations). Track plan changes by comparing node counts before/after each pass.

**Impact**: Suboptimal query plans for complex queries. Could be 2-10× slower than optimal.

### 9.2 Rule Ordering Suboptimal

**File**: `crates/lightning-core/src/optimizer/mod.rs:36-43`

**Problem**: Limit pushdown runs before OrderBy pushdown. ORDER BY + LIMIT still sorts all rows. Filter pushdown runs before subquery unnesting — unnested subqueries may produce new filter opportunities.

- [X] **9.2.1** `[P2]` Reorder: SubqueryUnnesting → FilterPushDown → IndexPushDown → JoinReordering → TopKOptimizer → OrderByPushDown → LimitPushDown.

### 9.3 Plan Cache Key Is Full Normalized String

**File**: `crates/lightning-core/src/lib.rs:233,981`

**Problem**: `LruCache<String, Arc<BoundStatement>>` uses the full normalized query text. Whitespace differences, comment changes, or alias variations create cache misses. Protected by a `Mutex`, creating serialization on every query lookup.

- [X] **9.3.1** `[P2]` Use a two-level cache: logical plan (cheap) + physical plan (expensive). Use a hash of a structurally-normalized AST as the key. Normalize aliases and strip whitespace more aggressively.
- [ ] **9.3.2** `[P2]` Use `RwLock` or sharded cache to reduce lock contention.

---

## Section 10: Plan Cache and Query Prep

### 10.1 `strip_modifiers` Creates O(n) String Allocations Per Query

**File**: `crates/lightning-core/src/parser/mod.rs:77-168`

**Problem**: For every query: `s.replace('\n', " ")` → new String, `result.to_uppercase()` 3 times → 3 more Strings, then up to 3 `format!` calls. For a 1KB query: ~10KB of temporary allocations.

- [X] **10.1.1** `[P2]` Rewrite using `find`/`split` on `&str` slices. Replace `format!` with `write!` into a pre-allocated buffer.

### 10.2 Regex Normalization on Every Query

**File**: `crates/lightning-core/src/lib.rs:36-38`

**Problem**: `normalize_query()` is called on every query even for cache misses, allocating a new String.

- [X] **10.2.1** `[P3]` Only normalize when inserting into the plan cache. Use a hash-based lookup for the fast path.

### 10.3 Binder Creates Fresh HashMap Lookups Per Property Access

**File**: `crates/lightning-core/src/planner/binder.rs:1688-1707`

**Problem**: `get_table_properties` does double HashMap lookup per call (node_tables, then rel_tables). For 100 property references: 200 HashMap searches with full string key comparisons.

- [X] **10.3.1** `[P3]` Cache variable-to-table resolution. Add a `table_kind: enum { NodeTable, RelTable }` field to `BoundVariable`.

---

## Section 11: Scan and Visibility Overhead

### 11.1 Triple Vec<bool> Allocations Per Scan Batch

**File**: `crates/lightning-core/src/processor/operators/scan.rs:421,474,500`

**Problem**: Three separate `Vec<bool>` / `Vec<u8>` allocations per batch: visibility mask, null-id filter, semi-mask filter. Each is batch-size. Could be merged into a single pass.

- [X] **11.1.1** `[P2]` Combine visibility + null-id + mask checking into a single row-loop producing one filter vector.

### 11.2 `compute_morsel_size` Called Per get_next()

**File**: `crates/lightning-core/src/processor/operators/scan.rs:263`

**Problem**: Recomputes morsel size every batch, but it's a pure function of static table metadata.

- [X] **11.2.1** `[P2]` Compute once in `new()` and store as `morsel_size: u64`.

### 11.3 `has_modifications()` Acquires 32 Read Locks Per Scan Check

**File**: `crates/lightning-core/src/storage/row_version.rs:175-188`

**Problem**: Called from the scan hot path (`scan.rs:515,802`). Acquires `versions.read()` + `committed.read()` on all 16 shards = 32 lock ops.

- [X] **11.3.1** `[P2]` Maintain a global `AtomicBool dirty_flag`. Set on any `mark_row` call. `has_modifications()` returns the atomic. Clear it only when vacuum reclaims all entries.

### 11.4 Pushdown Filter Evaluated Twice When All Rows Pass

**File**: `crates/lightning-core/src/processor/operators/scan.rs:282-337,545-577`

**Problem**: When the early-filter path succeeds (all rows pass), the code falls through to normal scan, then evaluates the filter AGAIN.

- [X] **11.4.1** `[P2]` Track whether the filter was already satisfied in the early path. Skip the second evaluation if so.

---

## Section 12: Join and Sort Memory

### 12.1 Hash Join Build Concatenation Doubles Memory

**File**: `crates/lightning-core/src/processor/operators/hash_join.rs:210-216`

**Problem**: `concat_batches` creates a new mega-batch copying all build-side data. Original chunks + concatenated batch exist simultaneously = 2× memory.

- [X] **12.1.1** `[P2]` Incrementally concatenate as chunks arrive, or use index-based probe across chunks with offsets.

### 12.2 Full Materialization Before Sort (No External Sort)

**File**: `crates/lightning-core/src/processor/operators/sort.rs:53-56`

**Problem**: ALL child rows are materialized in memory before sorting. No threshold to switch to external sort (sorted runs on disk).

- [X] **12.2.1** `[P2]` Add estimated row count check before materialization. If exceeding `max_sort_memory`, build sorted runs in temp files, merge externally.

### 12.3 Sort Sort-Comparator Yield-Loop Busy Waits

**File**: `crates/lightning-core/src/processor/operators/sort.rs:145-147`

**Problem**: Threads spin with `std::thread::yield_now()` waiting for other collectors. 3 of 4 cores spin at 100% CPU.

- [X] **12.3.1** `[P2]` Use a `Condvar` or `Barrier` to block until all collectors finish.

### 12.4 TOP-K Materializes and Sorts ALL Rows

**File**: `crates/lightning-core/src/processor/operators/topk.rs:50-75`

**Problem**: Collects ALL rows, concatenates, full-sorts via `lexsort_to_indices`, then slices to K. For K=10, N=1M: O(N log N) time and O(N) memory for what should be O(N log K) time and O(K) memory.

- [X] **12.4.1** `[P1]` Replace with a bounded binary heap (min-heap for descending, max-heap for ascending). Process rows in streaming fashion. O(N log K) time, O(K) memory.

---

## Section 13: Aggregate Operator

### 13.1 Sort-Based Aggregate Converts All Rows to Vec<Vec<Value>>

**File**: `crates/lightning-core/src/processor/operators/aggregate.rs:150-173`

**Problem**: When sort-based aggregation kicks in (>100K groups), every cell becomes a heap `Value` enum. For 10 columns × 200K rows: 2M Value allocations at ~32-256 bytes each = ~200MB+.

- [X] **13.1.1** `[P2]` Keep data in Arrow format. Use columnar sort-based aggregation operating on original arrays.

### 13.2 take() Kernel Per Group Per Column Per Batch

**File**: `crates/lightning-core/src/processor/operators/aggregate.rs:199-208`

**Problem**: For each group in a batch, a `take()` kernel extracts its rows. For 500 groups × 5 columns: 2500 `take` calls, each allocating a new array.

- [X] **13.2.1** `[P2]` Use `filter()` with a bitmap segment for each group instead of per-group `take()`.

---

## Section 14: Hash Join Probe

### 14.1 O(C²) Build Cost From Redundant Offset Summation

**File**: `crates/lightning-core/src/processor/operators/hash_join.rs:157-161`

**Problem**: `shared.build_chunks.iter().map(|b| b.num_rows()).sum()` recomputes the cumulative row offset from scratch every time a new chunk arrives. For C chunks: O(1+2+...+C) = O(C²).

- [X] **14.1.1** `[P2]` Maintain a running `base_offset` counter incremented by each chunk's row count.

### 14.2 UNION Dedup HashSet Memory Explosion

**File**: `crates/lightning-core/src/processor/operators/union.rs:10`

**Problem**: `HashSet<Vec<Value>>` grows to the full UNION output size. For 1M unique rows with 10 columns each: 1M entries × (Vec: 24 bytes + 8 × 10 Values) × ~1.5× HashMap overhead = ~160MB+. For string columns, each Value heap-allocates the string separately.

- [X] **14.2.1** `[P2]` Use row-hash based dedup: compute a hash per row, store only `(hash, row_id)`. For collisions, fall back to full comparison. Or use a Bloom filter for a probabilistic first pass.

### 14.3 Intersect Stores All Rows as Vec<Value> in Hash Table

**File**: `crates/lightning-core/src/processor/operators/intersect.rs:77-84`

**Problem**: Same pattern as 12.1 — ALL build rows are converted to `Vec<Value>` with per-cell heap allocation.

- [X] **14.3.1** `[P2]` Store only the intersect key columns in the hash table plus row IDs. Use `take()` on original build data during probe.

---

## Section 15: Per-Row Helper Overhead

### 15.1 `set_null()` Creates Full 4KB Page Version to Write 1 Byte

**File**: `crates/lightning-core/src/storage/column.rs:1213-1235`

**Problem**: Setting a single null bit calls `create_new_version()` → allocates new 4KB frame, copies entire page, writes 1 byte, logs 4KB to WAL. For 1M single-row appends with nulls: 4GB of unnecessary memcpy and WAL traffic.

- [X] **15.1.1** `[P2]` Buffer null bit changes in memory for the single-row path. Flush to the page only when the batch is full or on flush.

### 15.2 `is_null()` Pins 4KB Page to Read 1 Byte

**File**: `crates/lightning-core/src/storage/column.rs:215-227`

**Problem**: For sequential scanning with per-row `is_null` checks, the same null page is pinned/unpinned for every row.

- [X] **15.2.1** `[P2]` Cache a reference to the current null frame during sequential scan.

### 15.3 `serialize_value_into` Clones LogicalType Per Call

**File**: `crates/lightning-core/src/storage/column.rs:1836`

**Problem**: `match (val, self.data_type.clone())` clones the entire LogicalType enum (with nested Vec<StructField> for structs) on every serialization.

- [X] **15.3.1** `[P2]` Match on `&self.data_type` instead.

### 15.4 `parse_value` Allocates String for Every Inline String Read

**File**: `crates/lightning-core/src/storage/column.rs:1812-1816`

**Problem**: `String::from_utf8_lossy(&data[...]).to_string()` always allocates a heap String. For scanning 10M strings: 10M heap allocations.

- [X] **15.4.1** `[P3]` Return `Cow<'_, str>` from parse_value. Only allocate when the value escapes the scan iteration.

---

## Section 16: Memory Store Performance

### 16.1 Consolidate() Builds Full HashSet Word Set in Memory

**File**: `crates/lightning-core/src/memory.rs:641-645`

**Problem**: `Vec<HashSet<String>>` for ALL entities. For 100K entities × 500 words each: 50M heap-allocated Strings in 100K HashSets = ~2.8GB.

- [X] **16.1.1** `[P1]` Use word-level MinHash signatures (fixed-size sketches). Reduces memory from O(n × w) to O(n × sketch_size).

### 16.2 recall() Issues Two Separate Transactions

**File**: `crates/lightning-core/src/memory.rs:273-300`

**Problem**: FTS search and vector search each create and rollback their own read transaction.

- [ ] **16.2.1** `[P2]` Share a single read transaction.

### 16.3 lookup_by_internal_id() Executes Full Cypher Query Per Call

**File**: `crates/lightning-core/src/memory.rs:1080-1095`

**Problem**: Called per FTS/vector result (line 276) and per neighbor in rag_query (lines 458-464). Each call traverses the full parse→bind→plan→execute pipeline.

- [X] **16.3.1** `[P2]` Batch lookups in a single `WHERE e._id IN [...]` query. Or maintain a temporary HashMap from `_id` to entity for the duration of the recall.

### 16.4 rag_query Scans Entire Relates Table for Expansion

**File**: `crates/lightning-core/src/memory.rs:408-419`

**Problem**: Full column scan of all Relates edges per rag_query call. For 1M edges: 8MB read per call.

- [ ] **16.4.1** `[P1]` Use the CSR index (`fwd_csr`) instead of the full column scan.

---

## Section 17: Python Binding Overhead

### 17.1 `store_batch` Acquires GIL Per Entity

**File**: `crates/lightning-python/src/lib.rs:358-377`

**Problem**: `Python::with_gil(|py| { ... })` is called inside the per-entity `.map()` closure. For N entities: N GIL acquire/release cycles at ~1-5µs each.

- [X] **17.1.1** `[P1]` Acquire GIL ONCE before the entity loop. Convert all entities while holding GIL, then release.

### 17.2 recall_stream / query_stream Collect All Results Into Vec

**File**: `crates/lightning-python/src/lib.rs:241-250,317-328`

**Problem**: Both streaming methods receive all results from the channel into a `Vec<PyObject>` before returning. Defeats the purpose of streaming. Memory grows linearly with result count.

- [X] **17.2.1** `[P1]` Return a Python generator that lazily pulls from the channel.

### 17.3 Full JSON Serialization for Query Results

**File**: `crates/lightning-python/src/lib.rs:84-135`

**Problem**: Every cell is converted to `serde_json::Value`, collected into `Vec`, then serialized to JSON string. For 1M rows: ~500MB+ of intermediate Values.

- [ ] **17.3.1** `[P2]` Stream results as Arrow RecordBatches to Python via PyArrow. Zero-copy columnar access, no JSON overhead.

---

## Section 18: Node.js Binding Overhead

### 18.1 tokio spawn_blocking Per Single Operation

**File**: `crates/lightning-node/src/memory.rs:61-348` (every method)

**Problem**: EVERY operation (store, recall, associate, expand, forget, decay, store_batch, rag_query, consolidate) spawns a tokio blocking task. For simple ops like `associate` (~100-500µs), the tokio spawn overhead (~50-200µs) is 50-100% of total cost.

- [ ] **18.1.1** `[P2]` For lightweight operations, batch multiple calls into a single `spawn_blocking`. Or use a dedicated thread pool with lower overhead.

### 18.2 Per-Entity GIL + Conversion Overhead in Node.js

**File**: `crates/lightning-node/src/memory.rs:132`

**Problem**: f64→f32 conversion on input (JS→Rust), then f32→f64 on output (Rust→JS). For embedding dim 768: 1536 type conversions per call. Also per-entity spawning (see 18.1).

- [ ] **18.2.1** `[P2]` Accept `Float32Array` from JS natively (napi supports it). Return as `Float32Array` to avoid the f64 round-trip.

### 18.3 CDC Subscriber Spawns Dedicated Bridge Thread

**File**: `crates/lightning-node/src/memory.rs:387-401`

**Problem**: `convert_mpsc_to_crossbeam` spawns a thread that runs for the subscriber's lifetime. Each thread has 8MB stack reservation.

- [ ] **18.3.1** `[P2]` Use a crossbeam channel from the start in the CDC event system. Eliminates the bridge thread entirely.

---

## Section 19: WASM Function Performance

### 19.1 Per-Row WASM Call Overhead (Already flagged in 1.2)

**File**: `crates/lightning-core/src/wasm_function.rs:164-172`

**Problem**: Each row triggers a separate `func.call(&mut store, ...)` with WASM interpreter overhead (opcode dispatch, stack setup, type checking). ~100-500ns per call.

- [ ] **19.1.1** `[P2]` Batch rows into chunks. Use the MemoryF32 mode pattern (already exists) for array-at-a-time processing.

### 19.2 MemoryF32 Mode Allocates NaN Vector Then Overwrites

**File**: `crates/lightning-core/src/wasm_function.rs:239,256`

**Problem**: First allocates `Vec<f32>` initialized to NaN, then allocates another Vec for actual results. First allocation is dropped immediately.

- [X] **19.2.1** `[P3]` Remove the first allocation. Initialize directly into the results vector.

### 19.3 String Mode Uses format! Per Row

**File**: `crates/lightning-core/src/wasm_function.rs:294`

**Problem**: `format!("{}", Value::from_arrow(&args[0], i))` allocates two heap objects per row (the Value and the formatted String).

- [X] **19.3.1** `[P2]` Downcast directly to `StringArray` and use `.value(i)` to get `&str` — avoids the intermediate Value allocation.

---

## Section 20: Compression Codec Performance

### 20.1 ALP Factor Search Always Uses fac_idx=0 (Missing Optimization)

**File**: `crates/lightning-core/src/storage/compression/alp.rs:55,94,106`

**Problem**: ALP has 19 factor options for optimal float encoding, but `encode_value` always receives `fac_idx=0`. The factor search loop is entirely missing. ALP never achieves its advertised compression ratio — it stores 8 bytes per float when it could store 4-6.

- [X] **20.1.1** `[P2]` Implement factor search: for the first block, try all 19 factor indices, compute the max encoded absolute value, pick the one that minimizes encoded range. Store the best `fac_idx` alongside `exp_idx`.

**Impact**: 20-50% worse compression than optimal ALP for float columns.

### 20.2 `analyze_integer_chunk` Rederives Min/Max From Stats

**File**: `crates/lightning-core/src/storage/compression/analyzer.rs:21-41`

**Problem**: Iterates the entire sample to compute min, max, all_same — but the caller (`column.rs:optimize`) already has these from column stats.

- [X] **20.2.1** `[P3]` Accept pre-computed min/max as parameters to the analyzer.

### 20.3 HashSet Allocated Per Analysis Call for Distinct Counting

**File**: `crates/lightning-core/src/storage/compression/analyzer.rs:53,157`

**Problem**: 4K-element HashSet per analysis call. For 100 columns: 100 × 4K = 400K entries of allocation.

- [X] **20.3.1** `[P3]` Use streaming cardinality estimation (HyperLogLog) with a fixed-size register array.

---

## Summary: Performance Issues by Tier

| Tier | Sections | Focus | Count |
|------|----------|-------|-------|
| **Tier 1** (catastrophic) | §1-3 | O(N²+), OOM, 1000x waste, BFS O(V²), 1M queries for fusion | ~12 |
| **Tier 2** (order-of-magnitude) | §4-8 | WAL contention, syscall per read, row-by-row functions, write lock per append, per-txn table iteration | ~16 |
| **Tier 3** (significant, 2-10x) | §9-15 | Optimizer single-pass, TOP-K full sort, join memory, scan per-batch overhead, null bitmap per-row | ~24 |
| **Tier 4** (incremental, <2x) | §16-20 | MemoryStore batch lookups, Python GIL per entity, WASM per-row call, ALP factor search | ~16 |

**Total: ~68 performance issues** across the entire codebase, ranked by impact.
