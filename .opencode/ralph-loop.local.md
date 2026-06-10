---
active: true
iteration: 0
maxIterations: 500
---

YOU MUST FIX ALL 184 AUDIT ISSUES found in AUDIT_FULL_REPORT.md in the worktree at /Users/bviga/Developement/new_research/research/lightning-fixes

## CRITICAL RULES
1. NEVER SIMPLIFY CODE — every fix must be production-grade. No half-measures, no stubs, no "TODO remain".
2. Read the ACTUAL file before editing. Always use Read tool first, then Edit.
3. After EVERY fix, verify by running: `cargo build 2>&1 | head -50` from /Users/bviga/Developement/new_research/research/lightning-fixes
4. After build succeeds, commit with a descriptive message and push: `git add -A && git commit -m "fix(area): description" && git push origin audit-fixes-v2`
5. One issue per commit. Small focused commits.

## FIX ORDER (strictly follow this priority):
### P0 — CRITICAL (fix these first, one at a time, commit+push each)
1. Cypher injection in fusion.rs/memory.rs — convert ALL format!() queries to parameterized $param syntax
2. HNSW random_level() — fix RNG seeding, use persistent thread-local RNG
3. Inverted index data race — change read lock to write lock on tantivy writer
4. Trigram index unsorted posting lists — sort after insert or use BTreeSet
5. Bitpacking byte path — add bit-clearing before OR
6. Hash index resize race — add exclusive access to resize
7. analyzer_test.rs compilation — fix syntax errors in all test functions
8. is_read_only() in inherent impl — move ALL 7 operators to trait impl
9. Dangling tempdir in hash_join_test.rs — bind TempDir to variable
10. WAL CRC not verified — compare computed_crc vs stored_crc
11. Unsafe frame mutation in transaction_manager.rs — add safe Frame API
12. CDC thread lock blocking I/O — clone subscriber list before I/O
13. WASM sandbox — add fuel metering timeout
14. WASM path traversal — validate against allowed directories
15. Copy path validation — check copy_base_dir
16. Buffer cache incoherence after direct file write — evict pages
17. Prefix-match undo table deletion — use exact match
18. Projection pushdown variable corruption — don't set var to ""
19. Projection pushdown empty required_indices — all columns if no Projection
20. CountRelTable wrong table type — check table type from catalog
21. Index pushdown RecursiveJoin mask — preserve existing mask_id
22. LogicalPlan set_child Join/Union — fix child assignment
23. DML MERGE index lookup — use only PK column
24. Limit operator race — single atomic fetch_add
25. Cross Join data loss — propagate concat_batches error
26. Unwind O(R²) evaluation — cache expression evaluation
27. ALP brute force 209 combinations — optimize search
28. WAL unbounded growth — add rotation/truncation
29. C FFI dangling pointers — use Arc for handles
30. MemoryStore expand() loads all edges — push down to CSR
31. O(k×n) consolidation — use LSH batching
32. Permissive CORS — restrict origins
33. Unbounded batch/entity sizes — add limits
34. SSE connection limits — add semaphore
35. Blocking recv without timeout — add tokio::time::timeout
36. Error info disclosure — sanitize error responses

## COMPLETION SIGNAL
Only output `<promise>DONE</promise>` when ALL 184 issues in AUDIT_FULL_REPORT.md are fixed, verified with cargo build, committed, and pushed.
