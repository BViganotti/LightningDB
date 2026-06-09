# Lightning Graph Database — Full Code Audit

**Audited:** 150+ `.rs` files across all crates  
**Date:** 2026-06-09  
**Scope:** Security, Performance, Bugs, Missing Features, Code Quality, TODOs

---

## SEVERITY TOTALS

| Severity | Count |
|----------|-------|
| CRITICAL | 13 |
| HIGH | 56 |
| MEDIUM | 104 |
| LOW | 57 |
| **TOTAL** | **230** |

---

# CRITICAL FINDINGS (13)

## C-1: Query Plan Cache Completely Broken — `lib.rs:1090-1108`

The first cache lookup in `build_physical_plan` finds a cached bound statement, but `let cached_stmt = { ... }` at line 1105 **shadows** the first result. Since `cache_key` is empty-string when the first lookup hits, the second shadowing lookup queries `""` (always miss), discarding the cache hit. **EVERY query pays the full parse+bind+plan cost.** Additionally, the two cache lookups may select different shards (`cache_shard(query_str, 4)` vs `cache_shard(&cache_key, 4)`).

## C-2: Cypher Injection Everywhere — `fusion.rs:5-7,34,56-59,71-73,98-105,127-131,165-176,405-406`

The `sq()` escape function only escapes `'` → `\'` but does **NOT** handle backslash escapes. Input `\` followed by `'` produces `\'` which is then interpreted as a closing quote allowing full Cypher injection. This affects: `find_connected_nodes`, `add_observation`, `lookup_node_names`, `materialize_pagerank`, `find_paths`.

Additionally, `edge_types` in `find_connected_nodes` are interpolated directly without **any** sanitization.

## C-3: CSS/CSR `pin_page` Leaks (No `unpin_page`) — `storage/index/csr.rs:222-234,259-274,329-356`

`scan_edges_from_csr` calls `bm.pin_page()` in loops but **never calls `bm.unpin_page()`**. Each loop iteration increments `pin_count` on frames without decrementing. Over time, the buffer pool becomes exhausted — all frames appear pinned and cannot be evicted.

## C-4: Stack Buffer Overflow Risk — `storage/column.rs:321,1320,1545,2000`

`stack_buf = [0u8; 64]` is a fixed-size serialization buffer. `element_size()` returns values up to 64 (for String). If `element_size` extends beyond 64 bytes, `copy_nonoverlapping` writes past the buffer — **stack buffer overflow** with memory safety implications.

## C-5: Strict Aliasing UB — `storage/index/vector_index.rs:352-354`

`std::slice::from_raw_parts(emb_bytes.as_ptr() as *const f32, dim)` casts `&[u8]` to `&[f32]` via raw pointer, violating Rust's strict aliasing rules. The compiler may miscompile code around this access. Also misaligned on ARM.

## C-6: Projection Pushdown Corrupts Expressions — `optimizer/projection_pushdown.rs:96`

`remap_expression_indices` sets `*var = "".to_string()` to mark a property as remapped. This **mutates the expression in-place** and the empty string is never checked downstream. Next optimization pass sees corrupted expressions. (Rule is disabled, but the code compiles.)

## C-7: HashJoin Ignores Join Condition — `physical_plan.rs:189-190`

`HashJoin::new` is called with hard-coded join column indices `0` and `0` for both sides — **ignoring the actual join condition**. The parsed condition is thrown away. All hash joins produce wrong results.

## C-8: MERGE Discards Child Operator — `physical_plan.rs:579`

`let _planned_child = self.plan(*child)?;` — the child is planned but the result is **discarded** (`_planned_child`). The merge operator gets no child input, meaning MERGE always operates on zero rows.

## C-9: Aggregate Data Loss on Hash-to-Sort Switch — `processor/operators/aggregate.rs:157-198`

When `all_batches` row count exceeds `SORT_AGGREGATION_THRESHOLD`, `use_sort_based` is set to `true`, but the hash map already accumulated in `self.shared_state.groups` is **NOT flushed** into `all_batches`. All rows processed via hash-based accumulation before the threshold hit are **silently lost**.

## C-10: `parse/mod.rs` — `strip_modifiers` Panics on ORDER BY + SKIP/LIMIT — `parser/mod.rs:114-204`

After ORDER BY is removed from `result`, the `upper` variable is NEVER updated. When SKIP/LIMIT extraction uses positions derived from `upper` to index into the now-shorter `result`, the slice will be out of bounds causing a **runtime panic**. Example: `MATCH (n) RETURN n.name ORDER BY n.name SKIP 5`.

## C-11: `analyzer_test.rs` — Compile Error — `storage/compression/analyzer_test.rs:9,20,31,44,55`

Every test function has malformed double-nested calls and missing parentheses. **The entire test module will not compile.**

## C-12: Undo Records Pushed Before Successful Writes — `processor/operators/dml.rs:119-123,200,528,756`

In PhysicalCreate, UndoRecord::DeleteNode is pushed BEFORE `batch_append_rows` completes. If the append fails, the undo buffer contains an orphaned delete record that will be applied on rollback, deleting rows never created. Same pattern in PhysicalDelete, PhysicalCreateRel.

## C-13: WAL — CRC Verification Skipped — `storage/wal.rs:495`

The CRC checksum is computed but stored in `_computed_crc` (underscore-prefixed, **never used**). The computed CRC is never compared against `stored_crc`. Corrupted WAL records are silently accepted as valid.

---

# HIGH FINDINGS (56)

## Security (12)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| H-1 | `api.rs` | 9-13 | `'static` lifetime lie on C string references — UB if caller frees memory |
| H-2 | `capi.rs` | 52-53, 69-74 | Double-boxed `Arc<Database>` via raw ptr — dependency on free order |
| H-3 | `connection.rs` | 168-169, 189-190, 195 | Cypher injection in `create_node_table`/`create_rel_table`/`drop_table` — user table names directly interpolated |
| H-4 | `ddl.rs` | 491 | Index path constructed from user-supplied table/index names — path traversal possible via `..` |
| H-5 | `fusion.rs` | 96-106 | Edge types joined with `\|` directly into MATCH pattern — no sanitization |
| H-6 | `fusion.rs` | 165-176 | Content not sanitized for `\r` in `add_observation` |
| H-7 | `fusion.rs` | 127-131 | IN clause concatenation with insufficient escaping |
| H-8 | `fusion.rs` | 405-416 | Bulk pagerank update with unsanitized IDs |
| H-9 | `wasm_function.rs` | 14, 66, 119 | No WASM execution timeout — fuel metering not configured, WASM can run indefinitely |
| H-10 | `registry.rs` | 3446-3450 | `GEN_RANDOM_UUID` uses predictable LCG (seeded with `timestamp_nanos`) with broken UUID format |
| H-11 | `registry.rs` | 2472 | `CURRENT_USER` reads `USER` env var — information disclosure |
| H-12 | `registry.rs` | 2461 | `VERSION` exposes OS type — information disclosure |

## Bugs (20)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| H-13 | `lib.rs` | 484 | SEARCH function uses `unwrap()` on transaction begin — panic/DoS vector |
| H-14 | `lib.rs` | 1106 | Cache shard inconsistency between two lookups — wrong cache shard read |
| H-15 | `memory.rs` | 1241 | u64 → f64 precision loss in `lookup_by_internal_ids` — IDs > 2^53 lose precision |
| H-16 | `transaction/transaction_manager.rs` | 244-246 | Unsafe write to frame data without documented safety invariants — potential data race UB |
| H-17 | `transaction/transaction_manager.rs` | 326-332 | `page_merge_locks` grows unbounded — memory leak over weeks of operation |
| H-18 | `types.rs` (lightning) | 51-91 | `.unwrap()` in `from_batches` panics on Arrow type mismatch |
| H-19 | `hash_index.rs` | 163, 209, 218, 458, 507-508, 589, 623 | Unaligned pointer-to-reference casts — UB on ARM |
| H-20 | `storage_manager.rs` | 341 | `try_read()` silently drops trigram indexing on lock contention |
| H-21 | `registry.rs` | 2688-2693 | RAND function reinitializes LCG every call — all rows get SAME random number |
| H-22 | `registry.rs` | 1787-1797 | DATE_ADD/DATE_SUB MONTH logic completely broken — wrong control flow |
| H-23 | `column.rs` | 2185-2197 | Compression metadata never applied — pages always stored uncompressed (dead code) |
| H-24 | `planner/logical_plan.rs` | 870-874, 888-892 | Duplicate `BoundClause::Unwind` arm — second arm is dead code |
| H-25 | `planner/binder.rs` | 562-597 | CopyFrom/CopyTo file paths not validated for traversal attacks |
| H-26 | `planner/binder.rs` | 1733-1743 | `substitute_macro_body` no-op for PropertyLookup — both branches return `body.clone()` |
| H-27 | `planner/binder.rs` | 647-653 | StandaloneCall computes bound parameters but discards them |
| H-28 | `parser/mod.rs` | 107 | `normalize_query` casts bytes to `char`, breaking multi-byte UTF-8 |
| H-29 | `parser/mod.rs` | 82-97 | `normalize_query` strips comments inside string literals |
| H-30 | `binder.rs` | 562-597 | No path validation for CopyFrom/CopyTo — file traversal |
| H-31 | `cross_join.rs` | 78-82 | Silent data loss when `concat_batches` fails — error swallowed |
| H-32 | `dml.rs` | 90-91 | Row IDs leaked on failed insert — `fetch_add` reserves IDs but never rolls back |

## Performance (12)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| H-33 | `dml.rs` | 131-138, 380-392, 538-539, 985-987 | O(N * M²) column-name lookups in DML — `iter().position()` inside row loop |
| H-34 | `hnsw.rs` | 233, 261 | Full embeddings clone on every insert — O(n²) memory per insert |
| H-35 | `wasm_function.rs` | 119-134, 150-154, 223-227, 275-279 | WASM engine/module/instance re-created every invocation (~1-10ms each) |
| H-36 | `gds/pagerank.rs` | 68-69 | Memory allocation O(max_node_id), not O(active_nodes) — massive waste for sparse graphs |
| H-37 | `gds/all_shortest_paths.rs` | 78-80, 118-123 | Same O(max_node_id) allocation for BFS distances |
| H-38 | `gds/recursive_join.rs` | 70 | Same O(max_node_id) FixedBitSet allocation |
| H-39 | `flatten.rs` | 42-47 | O(N) RecordBatch creations for N rows — each batch has 1 row |
| H-40 | `aggregate.rs` | 157 | O(N²) row count computation — sums all batches every iteration |
| H-41 | `cdc.rs` | 86-108 | Subscribers lock held during all WAL reads — blocks registration |
| H-42 | `lazy_catalog.rs` | 109-131 | `save_internal` holds read lock during synchronous disk I/O — blocks all readers |
| H-43 | `lazy_catalog.rs` | 138-147 | `Clone` for `LazyCatalog` creates NEW atomics — breaks dirty-tracking |
| H-44 | `registry.rs` | 3485 | REGEXP compiles regex per-row — ReDoS vector and O(n) recompilation |

## Code Quality / Missing Feature (12)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| H-45 | `optimizer/mod.rs` | 44-51 | 5 optimizer rules disabled with known bugs — `projection_pushdown`, `semijoin_pushdown`, `acc_hash_join_optimizer`, `agg_key_dependency_optimizer`, `count_rel_table_optimizer` |
| H-46 | `optimizer/mod.rs` | 57 | Hard-coded `max_iters = 5` — silent convergence failure |
| H-47 | `optimizer/foreign_join_pushdown.rs` | entire file | Complete stub — does nothing |
| H-48 | `optimizer/factorization_rewriter.rs` | entire file | Complete stub — does nothing |
| H-49 | `optimizer/order_by_pushdown.rs` | 38-39 | Never recurses into children — effectively stub |
| H-50 | `projection_pushdown.rs` | entire file | Disabled, code compiles but has known-broken remap logic |
| H-51 | `semijoin_pushdown.rs` | entire file | Disabled, mask lifecycle issues |
| H-52 | `acc_hash_join_optimizer.rs` | entire file | Disabled, fragile column assumptions |
| H-53 | `agg_key_dependency_optimizer.rs` | entire file | Disabled, incorrect group-by analysis |
| H-54 | `count_rel_table_optimizer.rs` | entire file | Disabled, wrong COUNT results |
| H-55 | `subquery_unnesting.rs` | 32-39 | Subquery decorrelation is dead code — correlation not handled |
| H-56 | `overflow_file.rs` | 62-67 | `write_string` is stub returning `(0, 0)` — strings > 63 chars silently lost |

---

# MEDIUM FINDINGS (104)

## Storage (25)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-1 | `column.rs` | 239-244 | `is_null` linear scan of `pending_nulls` every call — O(n) per check |
| M-2 | `column.rs` | 347-353, 1331-1334 | `mark_row` errors logged but **swallowed** — lost write-write conflict detection |
| M-3 | `column.rs` | 1983-1994 | String length exactly 64 without overflow file → data silently lost |
| M-4 | `column.rs` | 649 | Invalid UTF-8 silently replaced with empty string via `unwrap_or("")` |
| M-5 | `column.rs` | 2080 | `compute_page_bounds` re-reads all data O(n*pages) |
| M-6 | `buffer_manager.rs` | 361 | Prefetch read errors silently ignored — garbage data cached |
| M-7 | `buffer_manager.rs` | 482-486, 509-545 | Dirty page eviction without WAL logging — data file/WAL inconsistency on crash |
| M-8 | `file_handle.rs` | 138-140 | TOCTOU in `get_file_size` — file size can change between read and I/O |
| M-9 | `storage_manager.rs` | 269, 370, 432 | Unchecked integer overflow in cardinality update |
| M-10 | `storage_manager.rs` | 978-1022 | Duplicate code: `ensure_csr_fresh` and `rebuild_csr_if_stale` identical |
| M-11 | `storage_manager.rs` | 365-368 | `flush()` called on all workers unconditionally — unnecessary |
| M-12 | `compression/delta.rs` | 35 | i128 delta underflow if `val < min` — negative delta wraps |
| M-13 | `compression/alp.rs` | 74 | `f64 → i64` cast panics on NaN/inf during encoding |
| M-14 | `compression/alp.rs` | 113-127 | Brute-force 209 combinations per page — expensive encoding |
| M-15 | `trigram_index.rs` | 214, 259, 268, 278 | Duplicate detection by last-element only — out-of-order rows create duplicates |
| M-16 | `trigram_index.rs` | 283-288 | Statistics only updated for trigrams, not bigrams/unigrams |
| M-17 | `inverted_index.rs` | 161 | Stale search results on `reload()` failure — error ignored |
| M-18 | `free_space_manager.rs` | 35-46 | Save not atomic (no temp+rename) — corrupted state on crash |
| M-19 | `database_header.rs` | 21 | Magic bytes "LIGHTNIG" misspelled |
| M-20 | `page_state.rs` | 5 | Comment says 7 bits, implementation uses 6 |
| M-21 | `undo_buffer.rs` | 204 | DropConstraint rollback not fully implemented — warning only |
| M-22 | `undo_buffer.rs` | 218 | DropIndex rollback not fully implemented — warning only |
| M-23 | `overflow_file.rs` | 62-67 | `write_string` stub returning `(0, 0)` — data loss |
| M-24 | `stats/mod.rs` | 11 | `StorageStats` is an empty struct placeholder |
| M-25 | `column.rs` | 2190 | TODO: compression not wired into write path |

## Processor Operators (15)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-26 | `scan.rs` | 335 | `filter_expr.clone()` per morsel — wasted allocation on empty morsels |
| M-27 | `scan.rs` | 585 | `mask.values().count_set_bits()` may count padding bits as valid rows |
| M-28 | `filter.rs` | 40 | `as_boolean()` via downcast can panic if eval returns non-boolean |
| M-29 | `hash_join.rs` | 175, 187-188, 317-318, 327-328 | `.expect()` on Arrow downcast panics on type mismatch |
| M-30 | `hash_join.rs` | 387 | `left_indices.clone()` wasted clone — could use `take()` |
| M-31 | `hash_join.rs` | 441-446 | O(N log C) binary search per row for chunk resolution |
| M-32 | `topk.rs` | 95, 104 | `partial_cmp` treats incomparable values as equal — non-deterministic sort |
| M-33 | `union.rs` | 60-73 | Dedup collision false positive — first value with given hash not in collision table |
| M-34 | `partitioner.rs` | 100-107 | Hash computed for rows that are later skipped |
| M-35 | `recursive_join.rs` | 113-127 | Linear fallback scan per node when CSR missing — O(batch * rel_cardinality) |
| M-36 | `shortest_path.rs` | 74-131 | Bi-BFS has no dead-end pruning — explores entire graph for unreachable |
| M-37 | `copy.rs` | 193, 393 | Delimiter string indexing `s.as_bytes()[0]` panics on empty string |
| M-38 | `copy.rs` | 367 | JSON export doesn't escape special characters (newlines, tabs, etc.) |
| M-39 | `copy.rs` | 432 | Writer flush error silently ignored — `let _ = writer.into_inner()` |
| M-40 | `gds/gds_state.rs` | 22, 27, 32 | No bounds check on `node_id` — index out of bounds panic |
| M-41 | `gds/gds_state.rs` | 54 | TODO: next frontier never cleared between iterations |
| M-42 | `gds/pagerank.rs` | 94 | No convergence check — runs full `max_iterations` regardless |
| M-43 | `gds/all_shortest_paths.rs` | 120-122, 132-135 | Node IDs stored as f64 lose precision above 2^53 |
| M-44 | `gds/recursive_join.rs` | 125-126 | `src_col_idx` hardcoded to column 0 — ignores planner-specified index |
| M-45 | `dml.rs` | 316, 364-365 | No bounds check on `property_idx` — panics if planner provides OOB index |
| M-46 | `aggregate.rs` | 310, 327 | Group-by columns always cast to Utf8 — losing type info and precision |

## Optimizer (12)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-47 | `filter_pushdown.rs` | 72-117 | `extract_variables` for Exists and CountSubquery are identical copy-paste |
| M-48 | `filter_pushdown.rs` | 207 | Catch-all `_ => {}` silently ignores operators like `CountRelTable`, `OptionalMatch` |
| M-49 | `join_reordering.rs` | 128 | DP uses u32 bitmask — limited to 32 relations |
| M-50 | `join_reordering.rs` | 130, 199 | Entire plan tree cloned N times during DP |
| M-51 | `join_reordering.rs` | 164 | HashSet cloned for every partition pair — O(3^n) |
| M-52 | `join_reordering.rs` | 135-136 | O(2^n * 2^n) iteration — full range scanned per subset size |
| M-53 | `limit_pushdown.rs` | 21-22 | Sort arm can't push down without TopK coordination |
| M-54 | `index_pushdown.rs` | 183 | RecursiveJoin hardcodes `mask_id: None` — breaks mask chain |
| M-55 | `cardinality_estimator.rs` | 90-91 | `unreachable!()` for Logical(Not) — reachable panic |
| M-56 | `cardinality_estimator.rs` | 191 | `_ => None` silently ignores Union, CountRelTable, RecursiveJoin, etc. |
| M-57 | `subquery_unnesting.rs` | 136-146 | `create_semi_join` assumes correlation always on node ID column 0 |
| M-58 | `subquery_unnesting.rs` | 90 | NOT detection by string name `"NOT"` — fragile |

## Processor / Evaluator / Scheduler (8)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-59 | `mod.rs` (processor) | 161-163 | String values truncated at 64 bytes via `std::cmp::min` — data loss |
| M-60 | `mod.rs` | 329-333 | UInt64 always returns `Value::Node` — conflates node IDs with relationship IDs |
| M-61 | `physical_plan.rs` | 403-427 | Vector/FTS index creation ignores `field`/`index_type`/`fields` params |
| M-62 | `physical_plan.rs` | 123, 131 | Storage lock acquired twice for IndexScan — unnecessary contention |
| M-63 | `scheduler.rs` | 69 | `clone_box()` for parallel execution — no guarantee operators implement correctly |
| M-64 | `evaluator.rs` | 855, 929 | `evaluate_list_filter`/`evaluate_list_transform` — incorrect offset if res.len() != values.len() |
| M-65 | `evaluator.rs` | 514-529 | `count_subquery` acquires storage read lock per-row |
| M-66 | `arrow_utils.rs` | 231 | String sentinel byte 0xFF conflicts with valid UTF-8 data |
| M-67 | `arrow_utils.rs` | 249-252 | List-type column data silently discarded during raw appends |
| M-68 | `arrow_utils.rs` | 263-294 | `from_arrow` duplicated with `Value::from_arrow` in mod.rs |

## Registry / Functions (15)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-69 | `registry.rs` | 297-298 | COALESCE type detection logic is bizarre and likely incorrect |
| M-70 | `registry.rs` | 104, 153 | UPPER/LOWER use `to_ascii_*` — fail on non-ASCII characters |
| M-71 | `registry.rs` | 1610 | LIST_SLICE uses 1-based indexing — inconsistent with rest of codebase |
| M-72 | `registry.rs` | 1998-2052 | LPAD/RPAD byte-level padding splits multi-byte UTF-8 characters |
| M-73 | `registry.rs` | 2443-2444 | DATE_DIFF hard-codes 30-day months and 365-day years |
| M-74 | `registry.rs` | 3245 | HASH uses `format!("{v:?}")` — Debug format not stable across versions |
| M-75 | `registry.rs` | 993-994 | LIST_CONCAT returns `List(Null)` regardless of element type |
| M-76 | `registry.rs` | 1681-1687 | LIST_DISTINCT/LIST_SORT/LIST_REVERSE output `DataType::Null` |
| M-77 | `registry.rs` | 2566 | LEVENSHTEIN allocates O(n*m) matrix per row — OOM vector |
| M-78 | `registry.rs` | 51-52 | `resolve_type` returns `LogicalType::Any` for many functions |
| M-79 | `aggregate_function.rs` | 143, 152 | CountDistinct uses `Debug` format — collisions between mixed types |
| M-80 | `aggregate_function.rs` | 57 | `Count.finalize()` returns u64 as f64 — precision loss > 2^53 |
| M-81 | `aggregate_function.rs` | 200-216, 269-287 | Sum/Avg only handle Float64/Int64 — Int32/UInt64 silently ignored |
| M-82 | `aggregate_ext.rs` | 337-339 | CollectDistinct outputs debug strings like "Number(42.0)" instead of actual values |
| M-83 | `registry.rs` | 32-69 | Median, GroupConcat, CollectDistinct, StdDev* not registered |

## Parser / Planner / Catalog (12)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-84 | `parser/mod.rs` | 55 | `result[pos..].to_uppercase()` allocates on every iteration — O(n*m) |
| M-85 | `parser/mod.rs` | 1150-1166 | Dead code `parse_arithmetic` kept in source |
| M-86 | `parser/mod.rs` | 1387 | `parse().unwrap_or(0.0)` silently converts parse failures to 0.0 |
| M-87 | `parser/mod.rs` | 922 | `unwrap_or(1)` silently swallows parse errors |
| M-88 | `parser/mod.rs` | 1402, 1413 | `unreachable!()` in operator parsers — panics on grammar mismatch |
| M-89 | `parser/mod.rs` | 522-528 | CALL clause always parsed as standalone — `Clause::Call` unreachable |
| M-90 | `parser/mod.rs` | 739, 747 | `.expect()` in skip/limit parsing — panics on malformed input |
| M-91 | `parser/mod.rs` | 1430-1475 | LIST/STRUCT type parsing fragile string manipulation |
| M-92 | `ast.rs` | 339 | `Literal::Number(f64)` — integer precision loss above 2^53 |
| M-93 | `binder.rs` | 1219-1238, 1649-1673 | O(n*m) property index lookups via linear scan |
| M-94 | `binder.rs` | 1398-1404 | Lambda binding clones entire `variables` HashMap |
| M-95 | `catalog.rs` | 418 | `next_val` cast of `increment` to u64 underflows for negative values |
| M-96 | `catalog.rs` | 473-496 | `remove_constraint`/`get_constraint` O(n*m) over all tables |
| M-97 | `logical_plan.rs` | 284-303 | `get_variables` misses right-child variables of Union, Intersect |

## Transaction / CDC / WASM (5)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| M-98 | `transaction_manager.rs` | 208-213 | 5-second merge lock timeout doesn't detect deadlocks |
| M-99 | `transaction_manager.rs` | 177 | `commit_ts` timestamps not monotonic — gaps created by `fetch_add(1) + 1` |
| M-100 | `cdc.rs` | 77-84 | `last_positions` grows unbounded with subscriber churn |
| M-101 | `wasm_function.rs` | 293-299 | WASM MemoryString writes each row at offset 0 — corrupting previous data |
| M-102 | `wasm_function.rs` | 242-248 | WASM MemoryF32 writes at hardcoded offset 0 — may corrupt module state |
| M-103 | `memory.rs` | 1200-1227 | `decay()` does one query per expired entity — O(n) queries |
| M-104 | `memory.rs` | 1241-1248 | `lookup_by_internal_ids` builds huge IN clause — may exceed string limits |

---

# LOW FINDINGS (57)

## Security / Code Quality / TODOs

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| L-1 | `scan.rs` | 283 | `Ordering::Relaxed` for partition position — minor |
| L-2 | `sort.rs` | 170 | Hardcoded batch size 1024 — should be configurable |
| L-3 | `dml.rs` | 928 | `fetch_add(0, ...)` no-op in MERGE match — dead code |
| L-4 | `dml.rs` | 747-753 | Inconsistent indentation in PhysicalCreateRel |
| L-5 | `dml.rs` | 845 | Unused `_current_num_rows` parameter |
| L-6 | `cross_join.rs` | 150-152 | `right_total` computed but never used |
| L-7 | `semi_masker.rs` | 44 | Unused `_initial_len` variable |
| L-8 | `mod.rs` (operators) | 26 | PhysicalCreateRel and PhysicalMerge not re-exported |
| L-9 | `hash_join.rs` | 87-88 | Cross join overload of `left_key_idx`/`right_key_idx` misleading |
| L-10 | `aggregate.rs` group columns | 295 | Group-by columns all cast to String — type info lost |
| L-11 | `sort.rs` | 173-176 | SeqCst compare_exchange overkill — Acquire/Release sufficient |
| L-12 | `file_handle.rs` | 40-47 | Hash collision risk for file_id |
| L-13 | `wal.rs` | 393-400 | WAL archive holds Mutex for entire duration — blocks concurrent writes |
| L-14 | `storage_manager.rs` | 365-368 | `flush()` called on all workers unconditionally |
| L-15 | `free_space_manager.rs` | 35-46 | Save not atomic |
| L-16 | `column.rs` | 239-244 | `is_null` linear scan of pending_nulls |
| L-17 | `memory.rs` | 733 | `consolidate()` loads ALL entities into memory |
| L-18 | `memory.rs` | 675 | `println!("query: {query}")` left in production code |
| L-19 | `cdc.rs` | 126-131 | `now_micros()` duplicates identical function in memory.rs |
| L-20 | `streaming.rs` (node) | 13, 63, 89, 126, 153, 174 | `std::result::Result` fully qualified instead of imported |
| L-21 | `mod.rs` (processor) | 422 | `Value::to_json` `expect()` panic if number is NaN/Infinity |
| L-22 | `mod.rs` | 195 | `Value::Path` to `Value::List` clones entire path vector |
| L-23 | `mod.rs` | 208-214 | `Value::List` in `to_arrow` stub — returns NullArray |
| L-24 | `physical_plan.rs` | 620 | Fallback `get_table_num_columns` returns magic number 2 |
| L-25 | `scheduler.rs` | 84 | Error path sends to channel but send itself can fail |
| L-26 | `evaluator.rs` | 618-641 | Division/modulo by zero returns None but no documentation |
| L-27 | `planner/logical_plan.rs` | 927 | Wildcard `_ => plan` in clause loop — fragile for new variants |
| L-28 | `catalog.rs` | 349 | `f.sync_all().ok()` — directory sync error silently discarded |
| L-29 | `catalog.rs` | 376 | `get_table_properties` returns u8 type tag never checked by callers |
| L-30 | `lazy_catalog.rs` | 70-85 | TOCTOU race in `save_if_needed` — state changes between check and save |
| L-31 | `lazy_catalog.rs` | 87-90 | `force_save` causes tx counter drift from actual count |
| L-32 | `connection.rs` | 162-163 | Column names in `create_node_table` not sanitized |
| L-33 | `memory.rs` (lightning) | 51-55 | `MemoryStore::new()` creates redundant second connection |
| L-34 | `types.rs` (lightning) | 42 | `TypedQueryResult` copies column names per row |
| L-35 | `lightning-arrow/src/lib.rs` | 1 | Unused `Array` import |
| L-36 | `lightning-python/src/lib.rs` | 147 | `extract_embedding` silently skips non-f32 items |
| L-37 | `lightning-python/src/lib.rs` | 97-122 | `entity_to_pydict` and `search_result_to_pydict` duplicate field mapping |
| L-38 | `node/memory.rs` | 95-98 | `recall()` NAPI method always uses empty embedding |
| L-39 | `expressions_visitor.rs` | 151-156 | ExpressionRewriter doesn't recurse into Exists/CountSubquery subqueries |
| L-40 | `planner/binder.rs` | 788, 832, 900-903 | Auto-generated variable names collide with user-defined variables |
| L-41 | `parser/mod.rs` | 1263 | `parse_case` fragile expression identification |
| L-42 | `ast.rs` | 153 | `Clause::Call` variant dead code — never produced by parser |
| L-43 | `binder.rs` | 1564-1565 | CASE return type from first THEN only — type mismatch possible |
| L-44 | `binder.rs` | 1187-1188 | CREATE relationship binds no src/dst column indices (always None) |
| L-45 | `binder.rs` | 1580-1615 | Duplicate binding code for Exists and CountSubquery |
| L-46 | `binder.rs` | 706-764 | Duplicate binding code for ON MATCH and ON CREATE in MERGE |
| L-47 | `logical_plan.rs` | 767-775 | Aggregate DISTINCT ORDER BY may reference wrong columns |
| L-48 | `logical_plan.rs` | 279 | `node_count` omits Intersect build_children and Union right child |
| L-49 | `logical_plan.rs` | 313 | `plan_union_query` clones input unnecessarily |
| L-50 | `planner/expression_visitor.rs` | 64 | Only visits Node match elements, not Rel or AllShortestPaths |
| L-51 | `transaction_manager.rs` | 232-244 | Unsafe blocks without `// SAFETY:` comments |
| L-52 | `node/database.rs` | 13-22 | JS Database has no `close()` or `checkpoint()` |
| L-53 | `node/memory.rs` | 23-58 | `open`/`open_with_config` duplicate database creation logic |
| L-54 | `node/types.rs` | 57-71 | `from_core` shadows `e` with local variable |
| L-55 | `stats/mod.rs` | 11 | TODO: "keep it simple" — StorageStats is empty |
| L-56 | `planner/logical_plan.rs` | 870-892 | Duplicate Unwind arm; second arm unreachable |
| L-57 | `catalog.rs` | 349 | `f.sync_all().ok()` error silently ignored |

---

# TEST FILE FINDINGS

## Tests: Critical (4)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| T-C1 | `hash_join_test.rs` | 56 | `TempDir` immediately destroyed — `Database::new()` gets deleted path |
| T-C2 | `comprehensive_test.rs` | 238-296 | 210 auto-generated tests all panic on type mismatch — give false confidence |
| T-C3 | multiple files | ~80+ locations | "Conditional assertion" pattern — test passes even when query returns empty |
| T-C4 | `comprehensive_test_4.rs` | 1073-1095 | Unimplemented ASP operator tested as passing (expects 0 rows) |

## Tests: High (8)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| T-H1 | `torture_test.rs` | 319-328 | BOOL write-buffer corruption known-issue — not asserted/fixed |
| T-H2 | `torture_test.rs` | 625-644 | Null value mismatches printed but never asserted |
| T-H3 | `comprehensive_test_3.rs` | 69-84 | `seq_1_insert_10k` commented out with FIXME (deadlock) |
| T-H4 | `comprehensive_test_3.rs` | 105-118 | PRIMARY KEY uniqueness NOT enforced — duplicate key passes |
| T-H5 | `torture_test.rs` | 28-151 | Invariant checks only printed, never asserted |
| T-H6 | `crash_recovery_test.rs` | 54, 88, 132 | WAL filename hardcoded as `wal.lbug` — if different, tests are silent no-ops |
| T-H7 | `torture_test.rs` | 244-249 | Concurrency data mismatches printed but never fail test |
| T-H8 | `lightning_vs_sqlite.rs` | multiple | Tests run queries but many never assert correctness |

## Tests: Medium (12)

| # | File | Line(s) | Issue |
|---|------|---------|-------|
| T-M1 | `fuzz_test.rs` | 48-73 | Fuzz tests only check no-panic — never validate correctness |
| T-M2 | `extreme_test.rs` | 185, 224-251, 262, 279, 282-289 | Debug `eprintln!` left in committed tests |
| T-M3 | multiple files | multiple | Massive duplication: `comprehensive_test.rs`, `comprehensive_test_2.rs`, `final_comprehensive.rs` nearly identical |
| T-M4 | `torture_test.rs` | 339-405 | `torture_file_deletion_recovery` counts graceful error as "passed" |
| T-M5 | `torture_test.rs` | 1294-1297 | `assert_eq!(err_count, 0)` — flaky in concurrent execution |
| T-M6 | `contains_edge_test.rs` | 156-170 | Query result never checked — passes regardless |
| T-M7 | `comprehensive_test_2.rs` | 606-616, `comprehensive_test_4.rs` | Assertion count only, no value verification |
| T-M8 | `agent_memory.rs` | 36 | Predictable temp directory — symlink attack risk |
| T-M9 | `projection_pushdown_test.rs` | 12 | INT32 used instead of INT64 — only test using this type |
| T-M10 | `flatten_test.rs` | 13 | OS temp dir used instead of `tempfile::tempdir()` |
| T-M11 | `merge_test.rs` | 35 | `is_success()` not verified — only count checked |
| T-M12 | `fuzz_test.rs` | 55 | Warmup `RETURN 1` only, subsequent errors silently counted |

---

# RAW TOTALS BY FILE

| File | C | H | M | L |
|------|---|---|---|---|
| `src/lib.rs` | 1 | 2 | 1 | 1 |
| `src/api.rs` | - | 1 | - | - |
| `src/capi.rs` | - | 1 | - | - |
| `src/fusion.rs` | 1 | 4 | - | - |
| `src/memory.rs` | - | 1 | 3 | 2 |
| `src/cdc.rs` | - | 1 | 1 | 1 |
| `src/wasm_function.rs` | - | 2 | 2 | 1 |
| `src/transaction/transaction_manager.rs` | - | 2 | 2 | 1 |
| `src/parser/mod.rs` | 1 | 3 | 8 | 2 |
| `src/parser/ast.rs` | - | - | 1 | 1 |
| `src/planner/binder.rs` | - | 3 | 3 | 4 |
| `src/planner/logical_plan.rs` | - | 1 | 2 | 2 |
| `src/planner/expression_visitor.rs` | - | - | 1 | 1 |
| `src/catalog/catalog.rs` | - | - | 2 | 2 |
| `src/catalog/lazy_catalog.rs` | - | 2 | 1 | 2 |
| `src/processor/mod.rs` | - | 1 | 2 | 3 |
| `src/processor/physical_plan.rs` | 2 | - | 2 | 1 |
| `src/processor/scheduler.rs` | - | - | 1 | 1 |
| `src/processor/evaluator.rs` | - | - | 2 | 1 |
| `src/processor/arrow_utils.rs` | - | - | 3 | - |
| `src/processor/aggregate.rs` | - | - | 1 | - |
| `src/processor/functions/registry.rs` | - | 4 | 6 | 4 |
| `src/processor/functions/scalar_function.rs` | - | - | - | 1 |
| `src/processor/functions/aggregate_function.rs` | - | - | 3 | 1 |
| `src/processor/functions/aggregate_ext.rs` | - | - | 1 | 1 |
| `src/processor/operators/aggregate.rs` | 1 | 1 | 1 | - |
| `src/processor/operators/hash_join.rs` | - | - | 3 | 1 |
| `src/processor/operators/filter.rs` | - | 1 | - | - |
| `src/processor/operators/scan.rs` | - | 1 | 2 | 1 |
| `src/processor/operators/sort.rs` | - | - | - | 2 |
| `src/processor/operators/topk.rs` | - | - | 1 | - |
| `src/processor/operators/dml.rs` | 1 | 1 | 2 | 3 |
| `src/processor/operators/ddl.rs` | - | 1 | - | - |
| `src/processor/operators/union.rs` | - | - | 1 | - |
| `src/processor/operators/cross_join.rs` | - | 1 | - | 1 |
| `src/processor/operators/partitioner.rs` | - | - | 1 | - |
| `src/processor/operators/flatten.rs` | - | 1 | - | - |
| `src/processor/operators/copy.rs` | - | - | 3 | - |
| `src/processor/operators/recursive_join.rs` | - | - | 2 | - |
| `src/processor/operators/shortest_path.rs` | - | - | 1 | - |
| `src/processor/operators/semi_masker.rs` | - | - | - | 1 |
| `src/processor/operators/gds/gds_state.rs` | - | - | 2 | - |
| `src/processor/operators/gds/pagerank.rs` | - | 1 | 1 | - |
| `src/processor/operators/gds/all_shortest_paths.rs` | - | 1 | 1 | 1 |
| `src/processor/operators/gds/recursive_join.rs` | - | 1 | 1 | - |
| `src/processor/operators/mod.rs` | - | - | - | 1 |
| `src/storage/column.rs` | 1 | 1 | 3 | 2 |
| `src/storage/wal.rs` | 1 | - | - | 1 |
| `src/storage/buffer_manager.rs` | - | - | 2 | - |
| `src/storage/file_handle.rs` | - | - | 1 | 1 |
| `src/storage/free_space_manager.rs` | - | - | 1 | - |
| `src/storage/storage_manager.rs` | - | 1 | 1 | 1 |
| `src/storage/database_header.rs` | - | - | - | 1 |
| `src/storage/page_state.rs` | - | - | - | 1 |
| `src/storage/undo_buffer.rs` | - | - | 2 | - |
| `src/storage/overflow_file.rs` | - | - | 1 | - |
| `src/storage/stats/mod.rs` | - | - | - | 1 |
| `src/storage/index/csr.rs` | 1 | - | - | - |
| `src/storage/index/vector_index.rs` | 1 | - | - | - |
| `src/storage/index/hash_index.rs` | - | 1 | - | - |
| `src/storage/index/hnsw.rs` | - | 1 | - | - |
| `src/storage/index/inverted_index.rs` | - | - | 1 | - |
| `src/storage/compression/analyzer_test.rs` | 1 | - | - | - |
| `src/storage/compression/delta.rs` | - | - | 1 | - |
| `src/storage/compression/alp.rs` | - | - | 2 | - |
| `src/storage/compression/analyzer.rs` | - | - | - | - |
| `src/optimizer/mod.rs` | - | - | 2 | - |
| `src/optimizer/filter_pushdown.rs` | - | 1 | 1 | - |
| `src/optimizer/projection_pushdown.rs` | 1 | 2 | 2 | - |
| `src/optimizer/join_reordering.rs` | - | 1 | 3 | 1 |
| `src/optimizer/limit_pushdown.rs` | - | - | 2 | - |
| `src/optimizer/order_by_pushdown.rs` | - | 1 | - | 1 |
| `src/optimizer/index_pushdown.rs` | - | - | 3 | 2 |
| `src/optimizer/semijoin_pushdown.rs` | - | 1 | 2 | 1 |
| `src/optimizer/foreign_join_pushdown.rs` | - | 1 | 1 | 1 |
| `src/optimizer/subquery_unnesting.rs` | - | 1 | 3 | - |
| `src/optimizer/factorization_rewriter.rs` | - | 1 | 1 | 1 |
| `src/optimizer/cardinality_estimator.rs` | - | - | 2 | 2 |
| `src/optimizer/topk_optimizer.rs` | - | - | 1 | 1 |
| `src/optimizer/acc_hash_join_optimizer.rs` | - | 1 | 2 | - |
| `src/optimizer/agg_key_dependency_optimizer.rs` | - | 1 | 2 | 1 |
| `src/optimizer/count_rel_table_optimizer.rs` | - | 1 | 1 | 1 |
| `lightning/src/connection.rs` | - | 2 | - | - |
| `lightning/src/memory.rs` | - | - | - | 1 |
| `lightning/src/types.rs` | - | 1 | - | 1 |
| `lightning-node/src/memory.rs` | - | 1 | - | - |
| `lightning-node/src/database.rs` | - | - | - | 1 |
| `lightning-node/src/streaming.rs` | - | - | - | 1 |
| `lightning-node/src/types.rs` | - | - | - | 1 |
| `lightning-python/src/lib.rs` | - | - | - | 2 |
| `lightning-arrow/src/lib.rs` | - | - | - | 1 |
| Test files | 4 | 8 | 12 | 8 |

---

## TOP 10 MOST URGENT ISSUES

1. **Plan cache completely broken** (`lib.rs:1090-1108`) — every query re-parses. Fix: remove shadowing `let`.
2. **Cypher injection everywhere** (`fusion.rs:5-7`, sq() function) — backslash+quote breaks escaping. Fix: use parameterized queries.
3. **HashJoin ignores join condition** (`physical_plan.rs:189-190`) — all joins join on column 0. Fix: pass actual equality columns.
4. **MERGE discards child operator** (`physical_plan.rs:579`) — merge always sees 0 rows. Fix: use `_planned_child`.
5. **Aggregate data loss on hash-to-sort switch** (`aggregate.rs:157-198`) — accumulated groups silently lost. Fix: flush hash map to all_batches before switching.
6. **strip_modifiers panics** (`parser/mod.rs:114-204`) — ORDER BY + SKIP/LIMIT crashes. Fix: update position variables after result modification.
7. **CSR pin_page leaks** (`storage/index/csr.rs:222-356`) — buffer pool exhaustion. Fix: add `unpin_page()` in each loop.
8. **WAL CRC not verified** (`storage/wal.rs:495`) — corrupted WAL silently accepted. Fix: compare `_computed_crc`.
9. **Undo-before-write in DML** (`processor/operators/dml.rs:119-123,200,528,756`) — orphaned undo records on write failure. Fix: push undo records AFTER successful write.
10. **Stack buffer overflow risk** (`storage/column.rs:321,etc`) — 64-byte stack buffer. Fix: use heap allocation or assert size.
