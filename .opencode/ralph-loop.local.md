---
active: true
iteration: 0
maxIterations: 200
---

# Ralph Loop: Fix ALL Findings from Code Audit

## CRITICAL RULES
- **NEVER SIMPLIFY CODE** — never remove functionality, never reduce safety margins, never delete code paths, never replace complex implementations with simpler ones
- Every fix must be **production-grade**: proper error handling, no new unwrap()/expect(), no silently swallowed errors
- After each individual fix: **git add, git commit, git push**
- Once ALL fixes are done: **switch to main, merge branch, push**

## Complete Fix List

### CRITICAL (6 items)

1. **C-01 PageRank batch update** — `crates/lightning-core/src/fusion.rs:426-432`  
   Fix the UNWIND batch update to use paired iteration so each node gets its own rank. Replace `SET n.page_rank = $ranks[0]` with proper per-row assignment using paired UNWIND or a STRUCT list.

2. **C-02 CORS Permissive** — `crates/lightning-server/src/server.rs:121`  
   Replace `CorsLayer::permissive()` with a configured `CorsLayer` that allows specific origins from config, or at minimum restricts to localhost origins when no config is provided.

3. **C-03 TLS Not Wired** — `crates/lightning-server/src/config.rs` + `server.rs`  
   Wire up TLS in the server when `tls_enabled` is true. Use `axum::serve` with a TLS acceptor (via `tokio_rustls` or `axum_server::bind_rustls`). Do NOT simplify by removing the config fields.

4. **C-04 WASM Sandbox Escape** — `crates/lightning-core/src/wasm_function.rs:241-312`  
   Add memory sandboxing: restrict WASM memory to only the exact region written, zero out memory after use, validate output offsets, limit total accessible memory. Use `wasmi`'s memory API to enforce bounds.

5. **C-05 Page Merge Lock Leak** — `crates/lightning-core/src/transaction/transaction_manager.rs:335-341`  
   Add cleanup mechanism for `page_merge_locks`. Either use a bloom filter + periodic GC, or use a bounded LRU cache, or remove entries after commit transaction cleanup.

6. **C-06 Commit Flushes ALL Dirty Frames** — `crates/lightning-core/src/transaction/transaction_manager.rs:281`  
   Change `bm.flush_all()` in commit to only flush pages modified by this transaction (available in `tx.modified_pages`). Keep `flush_all()` only for the full `Database::checkpoint()` path.

### HIGH (9 items)

7. **H-01 Cache Shard Mismatch** — `crates/lightning-core/src/lib.rs`  
   Ensure `query_hash` shard selection and the plan cache `cache_shard` use the SAME hash function consistently. Extract a shared hash function.

8. **H-02 println! in Production** — `crates/lightning-core/src/memory.rs:686`  
   Replace `println!("query: {query}")` with `tracing::info!("query: {query}")`.

9. **H-03 expand() Loads ALL Edges** — `crates/lightning-core/src/memory.rs:965-968`  
   Optimize `expand()` to use the CSR index directly (via `storage.fwd_csr.get(RELATES_TABLE)`) instead of loading all relationships via Cypher MATCH. The CSR already exists.

10. **H-04 consolidate() O(n²)** — `crates/lightning-core/src/memory.rs:767-809`  
    Add an index/limit: batch entities, add a max_comparisons_per_entity config, or use locality-sensitive hashing to bucket candidates before pairwise comparison. Do NOT simplify the consolidation logic itself.

11. **H-05 Read-Only Transaction read_ts Leak** — `crates/lightning-core/src/transaction/transaction_manager.rs:344-350`  
    Fix `Drop for Transaction` to call `remove_read_ts()` even for read-only transactions after the `is_read_only` early return. Either call it before the early return, or add a drop path for read-only.

12. **H-06 Checkpoint dirty_count Ordering** — `crates/lightning-core/src/storage/buffer_manager.rs:635-641`  
    Ensure `dirty` field synchronization is correct. Add proper atomic ordering or use a write lock around the dirty flag check/clear sequence.

13. **H-07 WAL read_records_from Holds Mutex During I/O** — `crates/lightning-core/src/storage/wal.rs:435-453`  
    Restructure `read_records_from` to snapshot the file position and length under the lock, then release the lock before doing the actual read I/O.

14. **H-08 TOCTOU WASM Path Validation** — `crates/lightning-core/src/lib.rs:568-619`  
    Pass the validation result (canonical path) into `WasmFunction::load()` so it uses the pre-validated path instead of re-resolving. Pass the resolved path as a parameter.

15. **H-09 log_page_update Wrong tx_id** — `crates/lightning-core/src/storage/buffer_manager.rs:570-588`  
    Instead of using `slot_indices.first()`, iterate all slots and use the caller's explicit tx_id parameter. The caller knows which tx_id is being committed.

### MEDIUM (10 items)

16. **M-01 Buffer Pool Exhaustion Unrecoverable** — `crates/lightning-core/src/storage/buffer_manager.rs:729-739`  
    Add a retry loop with backoff when eviction finds no evictable page. Wait for in-flight operations to complete and release pins.

17. **M-02 CDC Blocking Send** — `crates/lightning-core/src/memory.rs:910-918`  
    Remove the fallback blocking `tx.send()`. Use only `try_send` and drop events for slow consumers, or use an unbounded channel for CDC.

18. **M-03 CLOCK O(capacity)** — `crates/lightning-core/src/storage/buffer_manager.rs:684-741`  
    Optimize CLOCK eviction to use a skip-list of free candidates, or bound the number of slots scanned per eviction attempt.

19. **M-04 _id Column Assumption** — `crates/lightning-core/src/processor/operators/scan.rs`  
    Add a runtime check that column 0 is `_id` at scan construction time. Return a clear error if the schema is malformed.

20. **M-05 SemiMask XOR Bug** — `crates/lightning-core/src/processor/physical_plan.rs:1355-1387`  
    Fix the XOR case in trigram candidate extraction: compute `(A ∪ B) - (A ∩ B)` instead of `A ∪ B`.

21. **M-06 Naive String Replace in Cohesion** — `crates/lightning-core/src/fusion.rs:226`  
    Replace the naive `replace(nf, '.rs', '')` with a proper path-to-module extraction that only strips `.rs` at the end and handles paths correctly.

22. **M-07 sync_all_data_files Ordering** — `crates/lightning-core/src/storage/storage_manager.rs:954`  
    Document the ordering invariant. Add a comment explaining that this sync happens before WAL commit and what the crash recovery guarantees are.

23. **M-08 Read Lock for UnsafeCell** — `crates/lightning-core/src/storage/buffer_manager.rs:270-300`  
    Upgrade to a write lock in `create_new_version` when reading `source_data` via `UnsafeCell::get()`, or add stronger documentation proving why the read lock is sufficient.

24. **M-09 Wrong Column Name entity_type vs type** — `crates/lightning-core/src/memory.rs:1121`  
    Fix the `get()` method to use the correct column name `type` instead of `entity_type` in the Cypher RETURN clause. The schema defines the column as `type`.

25. **M-10 compute_architecture_cohesion Duplicate** — Fix the duplicate medium item numbering.

### LOW (11 items)

26. **L-01** — Replace `println!` in `memory.rs:686` with `tracing::info!`  
27. **L-02** — Either implement `init_fusion_schema()` properly or remove the dead code  
28. **L-03** — Remove `kuzu_*` deprecated aliases from `capi.rs` (they're behind `#[deprecated]` and clutter the API)  
29. **L-04** — Fix duplicate `SAFETY: SAFETY:` comments in `buffer_manager.rs:38,44`  
30. **L-05** — Remove the double `if visited.is_empty()` check in `memory.rs:1046`  
31. **L-06** — Use `_edge_types` in `fusion.rs:52` or prefix with `_` properly  
32. **L-07** — Remove unused variables `_fts_exists` and `_vec_exists` in `memory.rs:435-436`  
33. **L-08** — Use `_is_rel` parameter in `lib.rs:664` or remove it  
34. **L-09** — Remove unnecessary `RequestIdExtension` import in `server.rs:15`  
35. **L-10** — Prefix unused `_state` parameter in `query.rs:15` with `_`  
36. **L-11** — Remove unused `_config` field prefix in applicable locations  

### DEPENDENCY (6 items)

37. **D-01** — Lock wasmi to compatible semver and add upgrade path comment  
38. **D-02** — Add note about antlr4rust experimental status  
39. **D-03** — Update tantivy to latest compatible version  
40. **D-04** — Add cargo-deny or cargo-audit configuration  
41. **D-05** — Move rusqlite to optional or document dev-only status  
42. **D-06** — Scope tokio features to only what's actually used

## Workflow
- Fix ONE item at a time
- After fixing: `git add -A && git commit -m "fix(C-XX): description" && git push`
- Continue to next item
- After ALL items: `git checkout main && git merge audit-fix-all-findings && git push`
- Output `<promise>DONE</promise>` only after ALL fixes are committed AND merged into main AND pushed
