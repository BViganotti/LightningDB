# Phase 3 Optimizer Rules Plan

## Goal
Implement the remaining 8 optimizer passes from Phase 3 of the Ladybug to Lightning Port Action Plan using REAL, production-grade Rust code.

## 1. Limit Push-down (`limit_pushdown.rs`)
- **Objective:** Push `Limit` operators through non-expanding operators like `Projection` and `Sort` to reduce upstream work.
- **Action:** Traverse `LogicalOperator` and push `Limit` down through `Projection`. Pushing through `Sort` converts it to `TopK` (handled by the Top-K rule).

## 2. Top-K Optimizer (`top_k_optimizer.rs`)
- **Objective:** Convert `Limit(Sort(Child))` into `TopK(Child)` to avoid full sorting.
- **Action:** Add `LogicalOperator::TopK`. Add `PhysicalTopK` operator (heap-based bounded sort). Add rule to collapse `Limit` and `Sort`.

## 3. Order By Push-down (`order_by_pushdown.rs`)
- **Objective:** Push `Sort` below `Projection` to allow earlier pruning or index integration.
- **Action:** Swap `Projection` and `Sort` in the logical plan if the projection doesn't compute the sort key.

## 4. Remove Unnecessary Join (`remove_unnecessary_join.rs`)
- **Objective:** Prune unused branches in the query tree.
- **Action:** In Cypher, if `MATCH (n)` is followed by a `CrossJoin(SingleRow)` or if a join right-hand side produces variables that are never consumed by an upstream `Projection` or `Filter`, and the join cardinality doesn't affect the result (e.g., `LIMIT 1` or exact 1:1 match), we can remove the join.

## 5. Factorization Rewriter (`factorization_rewriter.rs`)
- **Objective:** Delay materialization of cartesian products/joins by keeping lists flat.
- **Action:** Instead of nested structures, we'll rewrite `CrossJoin` operations to be deferred or converted to `Unwind` where applicable to mimic Ladybug's factorized representation in an Arrow environment. 

## 6. Count Rel Table Optimizer (`count_rel_table_optimizer.rs`)
- **Objective:** `MATCH ()-[e:REL]->() RETURN COUNT(e)` -> `O(1)` catalog lookup.
- **Action:** Detect `Aggregate(COUNT) -> Projection -> ScanRel(REL)`. Rewrite the plan to directly yield a `LogicalOperator::Projection` with a `BoundExpression::Literal` containing the `num_rows` of the relation table from the Catalog.

## 7. Acc Hash Join Optimizer (`acc_hash_join_optimizer.rs`)
- **Objective:** Optimize recursive patterns or multi-hop joins by accumulating hash tables.
- **Action:** Convert specific sequences of `HashJoin` on the same keys into a `LogicalOperator::AccumulatedHashJoin` to share build-side state.

## 8. Foreign Join Push-down (`foreign_join_pushdown.rs`)
- **Objective:** Push hash join probe conditions directly into the scan to act as an implicit index/bloom filter.
- **Action:** Convert `Join(Filter(Scan(A)), Scan(B))` where A filters B by a foreign key into an `IndexScan` or parameterized scan.

## Execution Path
1. Update `LogicalOperator` in `logical_plan.rs` to include `TopK` and `CountTable`.
2. Implement the physical operators `PhysicalTopK` and `PhysicalCountTable`.
3. Create the rule files in `optimizer/`.
4. Register all rules in `optimizer/mod.rs`.
5. Write integration tests to prove they work with real data.