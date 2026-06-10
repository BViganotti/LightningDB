---
active: true
iteration: 0
maxIterations: 500
---

Fix ALL audit issues (177 total) in the lightning codebase at `/Users/bviga/Developement/new_research/research/lightning-fixes` on branch `audit-fixes-v2`, following strict priority order from P0 (critical) → P1 (high) → P2 (medium) → P3 (low). Push to `origin/audit-fixes-v2` after EACH fix.

## CRITICAL RULES
- NEVER SIMPLIFY CODE under any circumstances. Every fix must be production-grade.
- Read the actual file before editing. Do NOT hallucinate or guess code.
- After every fix: `cargo build` from worktree root, then `git add -A && git commit -m "fix(severity): description" && git push origin audit-fixes-v2`
- One issue per commit. Small focused commits only.
- Only output `<promise>DONE</promise>` when ALL 177 issues are fixed, built, committed, and pushed.

## ALREADY FIXED (skip these)
1. Cypher injection in fusion.rs — parameterized all queries
2. HNSW random_level() — thread-local persistent RNG
3. AllShortestPaths dst_var discarded — logical_plan.rs + physical_plan.rs
4. RecursiveJoin variable positions — physical_plan.rs collect_variable_positions
5. Evaluator.rs MutableArrayData → arrow::compute::interleave for Arrow 58
6. Hash join broken while loop from stash — restored else/probe branches
7. target/ removed from git tracking + .gitignore

## P0 REMAINING (fix first — in this order)
1. `crates/lightning-core/src/storage/index/inverted_index.rs:73,91,114` — read lock → write lock on tantivy writer
2. `crates/lightning-core/src/storage/index/trigram_index.rs:207-238` — unsorted posting lists cause binary_search misses
3. `crates/lightning-core/src/storage/compression/bitpacking.rs:73-86` — byte path doesn't clear target bits before OR
4. `crates/lightning-core/src/storage/compression/analyzer_test.rs:9,20,31,44,55` — syntax errors (spurious `analyze_integer_chunk(` prefix)
5. `crates/lightning-core/src/storage/index/hash_index.rs:92-148` — resize race (header updated before zeroing)
6. ~~`crates/lightning-core/src/storage/wal.rs:495` — WAL CRC computed but never compared~~ (FIXED)
7. ~~`crates/lightning-core/tests/hash_join_test.rs:57` — dangling tempdir use-after-free~~ (FIXED)
8. ~~`crates/lightning-core/src/memory.rs:1047-1051,1268-1275` — remaining cypher injection sites~~ (FIXED)
9. ~~`crates/lightning-core/src/planner/binder.rs:562-597` — COPY TO/FROM path validation against copy_base_dir~~ (FIXED)
10. ~~`crates/lightning-core/src/cdc.rs:86-108` — CDC thread holds lock during blocking I/O~~ (FIXED)
11. ~~`crates/lightning-core/src/transaction/transaction_manager.rs:232-246` — unsafe pointer mutation bypasses buffer manager~~ (FIXED)
12. ~~`crates/lightning-core/src/storage/column.rs:1663-1669,1844-1854,1454-1456` — buffer cache incoherence after direct file write~~ (FIXED)
13. `crates/lightning-core/src/optimizer/projection_pushdown.rs:96` — variable corruption (sets var to "")
14. `crates/lightning-core/src/optimizer/projection_pushdown.rs:346-349` — empty required_indices prunes all columns
15. `crates/lightning-core/src/optimizer/agg_key_dependency_optimizer.rs:96-105` — generic catch-all doesn't recurse
16. `crates/lightning-core/src/optimizer/order_by_pushdown.rs:37-41` — generic catch-all doesn't recurse
17. `crates/lightning-core/src/optimizer/count_rel_table_optimizer.rs:37-43` — wrong table type
18. `crates/lightning-core/src/optimizer/index_pushdown.rs:183` — RecursiveJoin mask_id destroyed
19. `crates/lightning-core/src/planner/logical_plan.rs:220-228` — set_child drops Join/Union right child
20. `crates/lightning-core/src/processor/operators/dml.rs:929-935` — MERGE uses all properties as index keys
21. `crates/lightning-core/src/processor/operators/limit_skip.rs:36-60` — limit race condition
22. `crates/lightning-core/src/processor/operators/cross_join.rs:76-84,198` — cross join data loss
23. `crates/lightning-core/src/processor/operators/unwind.rs:69-76` — O(R²) evaluation
24. `crates/lightning-core/src/storage/database_header.rs:21` — MAGIC number comment

## P1-P3 Issues
Fix P0 first, then continue through HIGH (section 2), MEDIUM (section 3), and LOW (section 4) issues in order.

## Workflow Per Fix
1. Read the file at the specified line range
2. Understand the bug and design the fix
3. Apply the fix using the Edit tool
4. Build with `cargo build` 
5. Commit + push with descriptive message
6. Output `<promise>DONE</promise>` only at the very end when ALL issues fixed
