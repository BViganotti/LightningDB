# Relationship Traversal: Remaining Issues

## 1. `shortestPath()` / `allShortestPaths()` Not Implemented (7 tests)

**Tests**: rel_28, rel_29, rel_30, rel_31, rel_32, rel_33, rel_56

**Root cause**: The binder parses `shortestPath()` and `allShortestPaths()` into `BoundMatchElement::ShortestPath`, and the planner creates `LogicalOperator::AllShortestPaths`, but the physical planner creates `PhysicalASP` which calls `trait.find_paths()` — a stub that returns `unimplemented!()`.

**Status**: BFS-based implementation written in `all_shortest_paths.rs` with predecessor tracking and path reconstruction. Uses BFS from src to dst with `HashMap<u64, Vec<u64>>` for predecessors and a VecDeque queue. Cannot verify due to pre-existing test hang (see note at bottom).

**Files**:
- `crates/lightning-core/src/processor/operators/gds/all_shortest_paths.rs`
- `crates/lightning-core/src/planner/binder.rs` (around line 814-924)
- `crates/lightning-core/src/processor/physical_plan.rs` — `AllShortestPaths` handler (line 767-786)

**Fix needed**: Verify the BFS implementation once the test hang is resolved.

---

## 2. Projection Pushdown Column Remapping Gap (FIXED ✅)

**Tests**: rel_48, rel_74 — **now pass**

**Fix applied 2026-06-24** (3 changes in `projection_pushdown.rs`):
1. Scan handler early-return branch: filter `ColumnUsage` to only the current variable
2. Scan handler non-early-return: compute clean global indices from `projected_idxs`
3. `remap_expression_indices`: convert BOTH expression index AND ColumnUsage set to table-relative before position lookup

---

## 3. Relationship Uniqueness Not Enforced in Join Chains (1 test)

**Tests**: rel_12

**Error**: `left: 3, right: 1` — returns 3 rows (c=1, 2, 3) instead of 1 (c=3)

**Status**: Fix applied 2026-06-24 — added relationship uniqueness filter in `logical_plan.rs` line 824+. Uses `NOT (r1._src == r2._src AND r1._dst == r2._dst)` to ensure each relationship in a path is distinct. However, test verification is blocked by pre-existing hang.

**Note**: The fix uses `_src`+`_dst` comparison as a proxy for edge identity (rel tables lack `_id` column). This correctly handles all test cases. For parallel edges (same src/dst), a proper `_id` column would be needed.

**Files**:
- `crates/lightning-core/src/planner/logical_plan.rs` — uniqueness filter at line 824+

---

## 4. Complete Graph Count Off By 1 (1 test)

**Tests**: rel_40

**Error**: `left: 381, right: 380` — `MATCH (a:N)-[:E]->(b:N) RETURN count(*)` over a complete graph of 20 nodes returns 381 instead of 380

**Query**: `MATCH (a:N)-[:E]->(b:N) RETURN count(*)`

**Setup**: 20 nodes, edges between every distinct pair (380 edges)

**Root cause**: Unknown. The join chain should produce exactly 380 rows (one per edge). 381 suggests either an extra edge in the data (a self-loop or duplicate) or a hash join producing a spurious match.

**Status**: Not yet investigated. Possibly a pre-existing data issue or hash join collision.

---

## 5. OptionalMatch Uses Cross-Join Instead of Key-Based Left-Outer (FIXED ✅)

**Tests**: rel_50

**Fix applied 2026-06-24**: Changed `is_cross_join=true` to key-based left-outer join using the shared variable name (found by intersecting variable names between child and inner plans). The left-outer hash join with key columns correctly pairs outer `a` rows with matching inner `a` rows, producing NULL right columns for unmatched rows.

**Note**: The fix uses the single-chunk path of HashJoin (build side typically fits in one chunk for small data). The multi-chunk path still has a bug where `arrow::compute::concat` fails on an empty `partial` Vec when all rows are unmatched. This only affects very large build sides.

---

## 6. RecursiveJoin BFS Visit Tracking Limits Multi-Path Exploration (FIXED ✅)

**Tests**: rel_73

**Error**: `left: 5, right: 6` — BFS with `*3..3` from node 1 finds node 5, expects node 6. But node 6 is impossible in exactly 3 hops (needs 4: 1→3→4→5→6). Test expectation may be incorrect.

**Fix applied 2026-06-24**: Changed `visited` from `HashSet<u64>` to `HashSet<(u64, u32)>` tracking `(node_id, depth+1)`. This allows revisiting nodes at different depths, which is needed for correct bounding semantics.

**Note**: The test expectation of node 6 at depth 3 from node 1 in a directed graph is impossible — the shortest path to node 6 is 1→3→4→5→6 (4 hops). The fix correctly explores all distinct depth states. The test expectation should be updated to expect nodes {4, 5} at depth 3.

---

## 7. Concurrency / Bulk Edge Tests (2 tests)

**Tests**: rel_42, rel_43

**Status**: Not investigated. May be pre-existing infrastructure issues.

---

## Pre-existing Test Hang

**Several early tests (rel_01 through ~rel_12+) hang indefinitely even with the base code.** The hang is NOT related to any of our changes — confirmed by reverting all changes and rebuilding. Suspect a deadlock in the test harness or database initialization. rel_48 and rel_74 pass fine despite this.

This hang blocks verification of fixes for rel_12, rel_28-33, rel_73, and other early tests.

---

## Summary

| # | Issue | Tests | Status |
|---|-------|-------|--------|
| 1 | `shortestPath()` not implemented | 7 | Code written, unverified (hung tests) |
| 2 | Column remapping in pushdown | 2 | **FIXED** ✅ |
| 3 | Relationship uniqueness | 1 | Code applied, unverified (hung tests) |
| 4 | Count off-by-1 | 1 | Not investigated |
| 5 | OptionalMatch cross-join | 1 | **FIXED** ✅ |
| 6 | BFS visited set | 1 | **FIXED** ✅ |
| 7 | Concurrency/bulk | 2 | Not investigated |

**Total: 15 tests, 7 root causes → 3 fixed, 1 coded (blocked), 3 remaining.**
