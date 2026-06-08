# LightningDB — Top 20 Critical & High-Severity Issues

> Generated: 2026-06-08
> Status of fixes applied so far: see commits `bedc984`, `93534dd`, `27823de`, `a4ec796`

---

## FIXED (4 items)

| # | Issue | File(s) | Commit |
|---|-------|---------|--------|
| 1 | Binder property index offsets double-count catalog system columns (`_id`/`_src`/`_dst`) | `binder.rs` | `bedc984` |
| 2 | VectorIndex delete swap-to-same-page data loss (two `create_new_version` calls on same page) | `vector_index.rs` | `bedc984` |
| 3 | HashIndex serialization missing Relationship, Date, Timestamp types | `hash_index.rs` | `27823de` |
| 4 | Python bindings: embeddings computed but never stored (LangChain, LlamaIndex, `__init__`) | `python/` | `a4ec796` |

---

## REMAINING (16 items)

### CRITICAL

#### C1. DELETE operator never processes rows (planner doesn't project `_id`)

**Files:** `physical_plan.rs`, `dml.rs`, `logical_plan.rs`
**Description:** `MATCH (n:Label) WHERE n.pk = X DELETE n` returns 0 rows affected. The `PhysicalDelete::get_next()` expects `Value::Node(id)` in column 0 (the internal `_id`), but the child scan may project only user columns. Every row hits `_ => continue` and is silently skipped. The row is never removed.
**Test:** `comprehensive_test::node_5_delete_node`, `comprehensive_test_2::basic_5_delete_node`
**Root cause:** The physical planner doesn't ensure column 0 is `_id` when the plan contains a DELETE. The optimizer's filter/projection pushdown may strip `_id` from the scan output. The `LogicalOperator::IndexScan` variant (from index_pushdown optimizer) is not handled by the physical planner at all.
**Fix approach:** Ensure the scan always includes `_id` (column 0) when the plan has a DELETE or SET clause. Either in the logical planner (track required columns) or in the physical planner (force include column 0).

#### C2. IndexScan logical operator not handled by physical planner

**Files:** `physical_plan.rs`, `index_pushdown.rs`
**Description:** The `index_pushdown` optimizer rule creates `LogicalOperator::IndexScan` nodes, but the `PhysicalPlanner::plan()` method has no match arm for `IndexScan` — only a catch-all returning `Err("Operator not implemented in PhysicalPlanner")`. When the rule fires (equality filter on primary key), the query fails with an internal error.
**Outcome:** Queries that SHOULD use an index scan fall back to full table scan + filter. This works but is slow and bypasses the primary key index.
**Fix approach:** Add a match arm in `PhysicalPlanner::plan()` for `LogicalOperator::IndexScan` that creates a `PhysicalIndexScan`.

#### C3. 355 `unwrap()`/`expect()` calls in production code

**Files:** `arrow_utils.rs` (50+), `trigram_index.rs` (20+), `parser/mod.rs` (15+), `hash_index.rs` (15+), `vector_index.rs`, etc.
**Description:** Every `.unwrap()` or `.expect()` can panic the query thread (or process) on unexpected input. Worst offenders: Arrow type downcasts (`downcast_ref::<T>().unwrap()`), lock acquisitions (`read().unwrap()`), and parser pair access (`pairs.next().unwrap()`). A single unexpected null or type mismatch crashes the entire database session.
**Severity:** Database crashes on malformed input, schema evolution edge cases, or concurrent access patterns.
**Fix approach:** Phase 1: Replace all `.unwrap()` on lock acquisitions with `?` (switch remaining `std::sync` locks to `parking_lot`). Phase 2: Replace type downcast `.unwrap()` with proper error returns.

#### C4. `LogicalOperator::IndexScan` physical planner not implemented

**Files:** `physical_plan.rs`
**See:** C2 above (same root cause — the IndexScan variant has NO handler in the physical planner).

---

### HIGH

#### H1. No WAL/MVCC integration for vector index

**Files:** `vector_index.rs`
**Description:** `insert_batch()`, `delete()`, and `update()` call `log_page_update()` but the `Transaction` parameter is only used for `pin_page()` snapshot isolation. There is no WAL logging of vector index operations tied to transaction commit/rollback. If the transaction rolls back, vector index changes are not reverted.
**Impact:** Vector index can become inconsistent with table data after a transaction rollback.
**Fix approach:** Use `log_page_update_for_tx()` with the transaction ID. On rollback, revert the vector index pages. Or, at minimum, log the entry count changes so they can be replayed/reverted.

#### H2. No WAL/MVCC integration for FTS (Tantivy) index

**Files:** `inverted_index.rs`
**Description:** The `bm` (BufferManager) and `tx` (Transaction) parameters accepted by `InvertedIndex` methods are completely ignored. Tantivy manages its own storage independently. There is no coordination between Lightning's MVCC transactions and FTS index commits.
**Impact:** FTS index can contain stale data after transaction rollback. FTS commits happen independently of the main WAL.
**Fix approach:** Integrate Tantivy's lifecycle with Lightning's transaction system, or at minimum log FTS operations to the WAL for replay.

#### H3. MemoryStore `rag_query` uses wrong "degree" metric

**Files:** `memory.rs`
**Description:** In `rag_query_with_config`, the "degree" used for reranking is computed as `all_entities.keys().filter(\|k\| *k != id).count()` — a set count of other entities in the *search result set*, NOT the actual graph degree from the `Relates` table. Every entity in the result set gets a degree of `N-1`, making this metric meaningless.
**Impact:** RAG reranking weight for graph degree is broken — it provides no signal.
**Fix approach:** Query the `Relates` table (or CSR index) to get the actual edge count for each entity.

#### H4. MemoryStore `consolidate()` PageRank runs on transient data

**Files:** `memory.rs`
**Description:** The PageRank computation in `consolidate()` builds an in-memory adjacency matrix from Jaccard similarities, runs PageRank on it, and updates the top-10 scoring entities' metadata. This PageRank is on a *transient* structure, not on the persisted `Relates` graph. The scores are lost after `consolidate()` returns.
**Impact:** Consolidation's PageRank scores are not persisted and have no durable effect. The `PhysicalPageRank` operator (in `gds/pagerank.rs`) is a separate implementation.
**Fix approach:** Either persist consolidation PageRank scores to the database, or use the database's own graph (CSR + `PhysicalPageRank`) instead of computing transient PageRank.

#### H5. `std::sync::RwLock` poison risk

**Files:** `trigram_index.rs`, `buffer_manager.rs`, `memory.rs`
**Description:** The codebase uses `parking_lot::RwLock` throughout most of the storage/transaction layers but `std::sync::RwLock` in trigram indexes and a few other places. `std::sync` locks POISON on panic — a single panic in any thread while holding the lock causes ALL subsequent `.read().unwrap()` / `.write().unwrap()` calls to panic forever.
**Impact:** A single transient error permanently disables the trigram index.
**Fix approach:** Replace remaining `std::sync::RwLock` / `std::sync::Mutex` with `parking_lot::RwLock` / `parking_lot::Mutex`.

#### H6. 60 silently swallowed errors (`let _ =`)

**Files:** `memory.rs`, `lib.rs`, `undo_buffer.rs`, `scheduler.rs`, `trigram_index_worker.rs`, `dml.rs`, `copy.rs`, `recursive_join.rs`
**Description:** Errors in 60 locations are discarded with no logging, metrics, or propagation. This includes: RAG pipeline errors silently producing incomplete results, rollback failures leaving inconsistent state, channel send errors losing query results, and FTS insert/commit errors leaving stale indexes.
**Fix approach:** Replace each `let _ = fallible_op()` with `tracing::warn!()` or proper error propagation.

---

### MEDIUM

#### M1. MemoryStore has zero tests

**Files:** `memory.rs` (all 3 copies)
**Description:** The `MemoryStore` struct (core + driver + node) has zero `#[test]` functions. Its methods (`store`, `recall`, `rag_query`, `consolidate`, `expand`, `subscribe_changes`, etc.) are entirely untested. There is only an example file (`examples/agent_memory.rs`).

#### M2. VectorIndex has zero unit tests

**Files:** `vector_index.rs`
**Description:** No test functions exist for `VectorIndex::insert`, `search`, `delete`, `update`, or `insert_batch`. The hash index and trigram index in the same directory have thorough tests.

#### M3. Connection `recall()` hardcodes `&[]` as embedding (FTS-only)

**Files:** `crates/lightning-python/src/lib.rs:201`
**Description:** The Python `recall()` method calls `self.inner.recall(query, &[], k)`, passing an empty embedding slice. This means `recall()` is FTS-only from Python. Users must call `recall_with_embedding()` explicitly for vector search. The Python wrapper `__init__.py`'s `recall()` similarly doesn't compute or pass embeddings.

#### M4. `expand()` hardcodes edge types `["Relates"]` in Python bindings

**Files:** `crates/lightning-python/src/lib.rs:273`
**Description:** The Python `expand()` method always passes `&["Relates"]` as edge types, ignoring any custom edge type filters. Users cannot filter by edge type from Python.

#### M5. StorageManager single-lock serialization

**Files:** `lib.rs:184`
**Description:** `storage_manager: Arc<RwLock<StorageManager>>` — all operations on ALL tables pass through a single `RwLock`. Under write-heavy workloads, this is a serialization bottleneck.
**Fix approach:** Shard at the table level (per-table locks).

---

## Summary

| Severity | Fixed | Remaining |
|----------|-------|-----------|
| CRITICAL | 3 | 4 |
| HIGH | 1 | 6 |
| MEDIUM | 0 | 5 |
| **Total** | **4** | **15** |
