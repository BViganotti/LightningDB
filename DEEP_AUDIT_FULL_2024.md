# LightningDB — Comprehensive Codebase Audit Report

> Generated: 2026-06-17
> Source: 5 deep-agent audits + direct source verification + compiler diagnostics

---

## Summary

| Severity | Count |
|----------|-------|
| **🔴 CRITICAL** | **13** |
| **🟠 HIGH** | **20** |
| **🟡 MEDIUM** | **22** |
| **🟢 LOW** | **12** |
| **Total** | **67** |

---

## 🔴 CRITICAL (13 issues)

### 1. Subquery `CountSubquery` is a dummy — returns total table count, ignores WHERE
**`evaluator.rs:569-587`** — `CountSubquery` counts all rows in a node table via `relaxed` load, ignoring the subquery's WHERE, JOIN, or match patterns. `RETURN EXISTS { MATCH (n:Person WHERE n.age > 30) }` returns total Person count regardless of age. Completely wrong results.
**Fix:** Implement actual subquery evaluation.

### 2. Auth tables exposed to arbitrary Cypher queries — privilege escalation
**`store.rs:19-21`, `query.rs:43-48`** — System tables `auth_users`, `auth_refresh_tokens`, `auth_api_keys` are normal node tables. Any `Reader` can `MATCH (u:auth_users) RETURN u.password_hash`. Any `Writer` can `MATCH (u:auth_users {username: "admin"}) SET u.role = "admin"`. **Zero query-level ACL.**
**Fix:** Block direct MATCH/SET against auth table names in the query handler, or wrap all auth ops behind the store API.

### 3. Auth bypass via public-path prefix hijacking
**`middleware.rs:63`** — `path.starts_with(p)` means `/healthz` → matches `/health` → **auth bypass**. Same for `/metricsv2`, `/v1/auth/login_backdoor`.
**Fix:** Use `path == *p || path.starts_with(&format!("{p}/"))`.

### 4. Empty `auth_token` grants unrestricted Admin
**`middleware.rs:114-123`** — When `expected` is empty (`LIGHTNING_AUTH_TOKEN=""`), every request (with or without any token) gets `Role::Admin`.
**Fix:** Reject empty auth_token at config validation time.

### 5. `PhysicalMerge` MATCH path `total_affected.fetch_add(0, ...)` is no-op
**`dml.rs:1116-1118`** — Matched rows never counted. MERGE MATCH path also never updates catalog cardinality.
**Fix:** Change `fetch_add(0)` to `fetch_add(chunk_rows_count)`.

### 6. Undo pushed BEFORE write succeeds in Create/CreateRel
**`dml.rs:207-209`, `dml.rs:873-874`** — Undo records are pushed, THEN `batch_append_rows` is called. If the write fails, undo will try to delete a node that was never created.
**Fix:** Push undo records only after successful write.

### 7. `PhysicalDelete` detach phase modifies rel tables with NO undo records
**`dml.rs:706-710`** — On `DETACH DELETE`, relationship rows are nullified without any `UndoRecord`. Data loss on rollback.
**Fix:** Add UndoRecord pushes for every detach modification.

### 8. WASM path validation: symlink-in-filename TOCTOU
**`lib.rs:610-637`** — Final filename component is never resolved. If the file is a symlink to `/etc/passwd`, validation passes but `std::fs::read()` follows the symlink.
**Fix:** Use `O_NOFOLLOW` on open, or canonicalize the full path.

### 9. WASM re-read after validation (TOCTOU)
**`lib.rs:567-575`** — Comment claims TOCTOU is fixed but validates path, then calls `std::fs::read()` — re-opening the path.
**Fix:** Open the fd inside the validation scope.

### 10. `PhysicalSet` multi-assignment to same column captures wrong undo value
**`dml.rs:428-445`** — `SET n.x = 1, n.x = 2` — second assignment reads the already-modified value, not the original. Undo restores to `1` instead of original.
**Fix:** Snapshot original values before any writes.

### 11. Subquery unnesting correlation prefix is never renamed
**`subquery_unnesting.rs:160-172`** — Semi-join correlation builds `PropertyLookup("__sub_n", 0, Any)` but subquery plan uses original variable `"n"`. Join references nonexistent columns.
**Fix:** Rename variables in the subquery plan to `__sub_` prefix during unnesting.

### 12. CALL clause parsing drops all arguments and YIELD items
**`parser/mod.rs:530-536`** — `parse_statement` returns `StandaloneCall` with `Vec::new()` for args. Also, CALL short-circuits so subsequent RETURN is never parsed.
**Fix:** Parse CALL arguments and YIELD items properly.

### 13. COPY FROM/TO statement not handled in parser
**`parser/mod.rs`** — `copy_statement` is defined in pest grammar but `parse_statement` has no match arm — falls through to `_ => {}` returning `Err("empty statement")`. Entire COPY feature is dead code.
**Fix:** Implement `Rule::copy_statement` handler in `parse_statement`.

---

## 🟠 HIGH (20 issues)

### 14. `PhysicalMerge` MATCH output uses pattern property literals, not stored values
**`dml.rs:1091-1115`** — Output row constructed from `prop_arrays` (search values), not stored data. Non-pattern, non-assigned columns silently become Null.
**Fix:** Read actual stored values for output after MERGE MATCH.

### 15. Negative float literal cast to u64 wraps silently
**`evaluator.rs:804-820`** — `-3.7` → `-4 as u64` → `18446744073709551612` — wildly wrong UInt64 comparisons.
**Fix:** Check for negative values before u64 cast.

### 16. Vector index not updated on SET (hardcoded skip)
**`dml.rs:548-552`** — Comment says "skip vector index update for SET." After `SET n.embedding = [...]`, vector search returns stale results.
**Fix:** Implement proper vector index update on SET.

### 17. CDC: race window — events between subscribe() and first poll are lost
**`cdc.rs:53-57`** — `subscribe()` captures offset but polling thread only begins reading from it later. No replay mechanism.
**Fix:** Read records synchronously in `subscribe()` before returning.

### 18. CDC: `try_send` silently drops events on full channel
**`cdc.rs:101`** — Bounded channel (capacity 64). Slow subscriber = guaranteed data loss. No warning logged.
**Fix:** Use blocking `send()` with timeout or log warning on drop.

### 19. CDC: Unsynchronized WAL reads risk torn/partial records
**`cdc.rs:91` / `wal.rs:465-491`** — CDC thread reads WAL while concurrent writers write. No read-write locking — a partial write can produce corrupt records.
**Fix:** Add read-lock to WAL or use atomic write sizes.

### 20. WASM `with_arity()` is a no-op; `MultiArgF64` arity ignored
**`wasm_function.rs:37-43`** — `MultiArgF64(arg_count)` reconstructs variant with call-time arg count, discarding configured arity. `ScalarF64` and `MultiArgF64` behave identically.
**Fix:** Actually enforce configured arity at function call time.

### 21. WASM MemoryF32 output offset validation blocks correct usage
**`wasm_function.rs:337-357`** — Forces output to overwrite input region. Writing output at `input_byte_len` (first byte after input) is rejected.
**Fix:** Validate output offset against total WASM memory, not just written input.

### 22. ORDER BY remap bug in aggregate queries
**`logical_plan.rs:366-374`** — `agg_offset` computation uses current item's var/idx to check all previous items. Produces wrong column index mappings for ORDER BY on aggregate queries with multiple non-group-by columns.
**Fix:** Use `ret_items[prev]`'s variable/idx in the inner loop.

### 23. `inject_modifiers` only handles `Statement::Match`
**`parser/mod.rs:205-261`** — ORDER BY/SKIP/LIMIT re-injected only into Match statements. `CALL proc() YIELD x RETURN x ORDER BY x` loses ORDER BY.
**Fix:** Handle `StandaloneCall`, `Create`, `Merge`, etc.

### 24. Auth token in URL query string leaks to logs/proxy/browser history
**`middleware.rs:92-101`, `133-146`** — Both Token and JWT modes accept `?access_token=` URL parameter.
**Fix:** Add config option to disable query-string auth.

### 25. Custom `percent_decode` — broken and unvalidated
**`middleware.rs:148-163`** — Hand-rolled decoder: invalid hex → 0, multi-byte UTF-8 broken. `%ZZ` → `\x00`. Should use `url` crate.
**Fix:** Replace with `percent_encoding` or `url` crate.

### 26. Rate limiter memory DoS (100k+ entries before eviction)
**`server.rs:50-76`** — Stale entries pruned only when map > 100,000 entries. Attacker with IP spoofing or IPv6 subnets can exhaust memory.
**Fix:** Set a lower max-size cap and evict aggressively.

### 27. No per-IP login throttle (per-username only)
**`store.rs:268-316`** — Login lockout keyed by `username` only. Attacker can lock out legitimate users or brute-force many usernames.
**Fix:** Add per-IP rate limiting to the login endpoint.

### 28. `get_effective_interventions()` returns empty list (dead return)
**`lightning-psych-app/db/queries.py:360-362`** — `return []` as first executable line. Therapist agent always sees "No highly effective interventions identified yet."
**Fix:** Remove the dead `return []`.

### 29. External app `ensureAuthenticated()` calls `/v1/auth/me` on every request
**`lightning-external-app/src/services/lightning.service.ts:152-173`** — HTTP round-trip before every API call. N+1 for `getFullGraph()` with N nodes = N*2 API calls. Adds 50ms-500ms per check.
**Fix:** Decode JWT client-side to check expiration.

### 30. Health check uses wrong endpoint (POST `/v1/query` instead of GET `/health`)
**`lightning-feature-test/src/client.ts:12, runner.ts:18`** — Masks server health issues; would report unhealthy server as healthy if query engine works.
**Fix:** Use actual `/health` endpoint.

### 31. TS client TLS file reads silently disable TLS on error
**`packages/lightning-client/src/client.ts:160-165`** — `fs.readFileSync` without try/catch in `buildTlsAgent`. Unreadable cert/key files silently return `undefined`, disabling TLS.
**Fix:** Wrap in try/catch and throw on TLS config failure.

### 32. Python `AsyncTransport.stream()` buffers entire response
**`python/lightning/client/_transport.py:462-474`** — Uses `self._client.request()` (buffered) instead of `.stream()`. Defeats streaming for CDC subscribe and query stream.
**Fix:** Use `self._client.stream()` for stream endpoints.

### 33. 355 `unwrap()`/`expect()` calls in production code
Worst offenders: `arrow_utils.rs` (50+), `trigram_index.rs` (20+), `hash_index.rs` (45+). Any malformed input or edge case crashes the process.

---

## 🟡 MEDIUM (22 issues)

### 34. `PhysicalDelete` output batch shows all nulls (returns post-deletion state)
**`dml.rs:744-745`** — `DELETE n RETURN n` returns nulls. Most databases return pre-deletion values.
**Fix:** Capture original values before deleting.

### 35. `PhysicalMerge` duplicate `affected_ids` across chunks
**`dml.rs:1062-1063`** — Same node matching in multiple chunks creates duplicate rows in MERGE output.
**Fix:** Use a `HashSet` or dedup after collection.

### 36. `PhysicalMerge` consumes entire child result into memory before processing
**`dml.rs:1007-1015`** — OOM risk for large result sets.
**Fix:** Process chunks incrementally.

### 37. `PhysicalMerge` re-evaluates `on_match_assignments` expressions twice per row
**`dml.rs:1065-1113`** — 2×N evaluations per assignment expression.
**Fix:** Cache evaluated results.

### 38. Debug `eprintln!` left in production `rows_to_batch`
**`dml.rs:37-42`** — Spams stderr on every DML RETURN, leaks data.
**Fix:** Remove or gate behind `#[cfg(debug_assertions)]`.

### 39. `evaluator.rs` `evaluate_list_predicate` ignores NULL predicate results
**`evaluator.rs:1162-1167`** — `bool_arr.value(k)` on null entry reads raw data bit. `LIST_ANY`/`LIST_ALL`/`LIST_SINGLE`/`LIST_NONE` wrong results with nulls.
**Fix:** Check `is_null(k)` first.

### 40. Literal number rounding changes comparison semantics
**`evaluator.rs:805-806`** — `3.7` rounds to `4` for int column comparison. `WHERE int_col = 3.7` matches rows where `int_col = 4`.
**Fix:** Cast int to float and compare as float.

### 41. Logical AND short-circuit incorrectly propagates NULLs
**`evaluator.rs:249-251`** — `FALSE AND NULL = NULL` instead of SQL's `FALSE AND NULL = FALSE`.
**Fix:** Nullify short-circuit path.

### 42. 60 silently swallowed errors (`let _ = fallible_op()`)
In `memory.rs` (21), `undo_buffer.rs` (4), `cdc.rs` (2), `lib.rs` (1), `dml.rs` (1), etc.
**Fix:** Replace each with `tracing::warn!()` or proper propagation.

### 43. `std::sync::RwLock` poison risk in trigram_index, buffer_manager, memory
Triagram index panic poisons the lock permanently.
**Fix:** Replace remaining `std::sync::RwLock` with `parking_lot::RwLock`.

### 44. AuthMode::None grants full Admin to all unauthenticated requests
**`middleware.rs:68-77`** — No way to run read-only unauthenticated server.
**Fix:** Add a config option for default role in None mode.

### 45. Login timing side-channel for username enumeration
**`store.rs:268-316`** — Invalid username returns before Argon2 hash; valid username runs hash. Measurable timing difference.
**Fix:** Always run Argon2 even for unknown usernames.

### 46. JWT has no `aud`/`iss` validation
**`jwt.rs:38-53`** — `Validation::default()` skips `aud`/`iss`. Tokens from one deployment work in all others sharing the same secret.
**Fix:** Add `aud`/`iss` validation.

### 47. No WAL/MVCC integration for vector index
**`vector_index.rs`** — Rollback doesn't revert vector index changes.
**Fix:** Log vector index operations to WAL by transaction ID.

### 48. No WAL/MVCC integration for FTS (Tantivy) index
**`inverted_index.rs`** — Tantivy commits independently of WAL.
**Fix:** Coordinate FTS lifecycle with MVCC transactions.

### 49. MemoryStore consolidate() PageRank runs on transient data — scores lost
**`memory.rs`** — Scores from MemoryStore consolidation PageRank are never persisted.
**Fix:** Persist PageRank scores to database or use DB's own graph.

### 50. `StorageManager` single `Arc<RwLock<StorageManager>>` serialization bottleneck
**`lib.rs:253`** — All operations across ALL tables go through one RwLock.
**Fix:** Per-table locks.

### 51. Massive test coverage gap (~70% of SDK public API untested)
Zero tests for: Memory operations, vector search, RAG, CDC, JWT, admin operations, TLS, circuit breaker, retry logic.
**Fix:** Add coverage for all public API surfaces.

### 52. Transaction suite doesn't test explicit transactions
**`08-transactions.ts`** — No `BEGIN`/`COMMIT`/`ROLLBACK`. Only autocommit behavior tested.
**Fix:** Add explicit transaction tests.

### 53. Concurrency suite is not concurrent
**`09-concurrency.ts`** — Purely sequential read-after-write operations.
**Fix:** Add actual concurrent/parallel operations.

### 54. QueryResult type mismatch between test and production SDK
**`types.ts (test)` vs `types.ts (SDK)`** — Test defines `{data: {columns, rows, numRows}}`, SDK defines flat `{columns, rows, numRows}`.
**Fix:** Align types.

### 55. CDC `Relaxed` ordering on shutdown flag — may hang on ARM
**`cdc.rs:63,69,122`** — `AtomicBool` with `Relaxed` — CDC thread may never observe shutdown on ARM/PowerPC.
**Fix:** Use `Acquire`/`Release` or `SeqCst`.

---

## 🟢 LOW / CODE QUALITY (12 issues)

### 56. 72 compiler warnings (unused imports, unused vars, non-snake-case, suspicious clone)
Worst: `lightning-core` has 72 warnings. Includes unused `LruCache`, `HashSet`, `Crc::Digest`, unused variables in `memory.rs`, HNSW non-snake-case fields.
**Fix:** Clean up warnings.

### 57. Dead duplicate `Float64Array` downcast block in `compare_column_literal`
**`evaluator.rs:889-907`** — Two identical blocks, second is dead code.

### 58. Dead code: `factorization_rewriter.rs`, `foreign_join_pushdown.rs` — no-op optimizers
These optimizers visit the plan tree but never modify it.

### 59. ~60% of optimizer rules disabled due to known bugs
Six rules disabled: IndexPushDown, ProjectionPushDown, SemiJoinPushDown, AccHashJoinOptimizer, AggKeyDependencyOptimizer, CountRelTableOptimizer.

### 60. `LogicalOperator::IndexScan` variant unhandled in physical planner
**`physical_plan.rs`** — Creates `IndexScan` nodes but physical planner has no handler for them.

### 61. All numeric literals typed as `Double` in binder
**`binder.rs:289`** — `5` from `n.age + 5` becomes `Double`. Precision loss for integer columns.

### 62. UNION column name check is overly strict
**`binder.rs:428-440`** — Cypher/SQL allows column names from first query only.

### 63. `MemoryF32` WASM per-call instantiation (redundant)
**`wasm_function.rs:136-137`** — Re-instantiates module for every batch of rows.

### 64. CDC `subscribe()` has fictitious `Result` return — never returns `Err`
**`cdc.rs:52`** — `wal.size()` failure silently falls back to offset 0.

### 65. `start()` called twice leaks thread handle
**`cdc.rs:62-119`** — Second call overwrites `handle` without joining first.

### 66. C API const-mut mismatch in `lightning_query`
**`api.rs:53`** — Returns `*const c_char` from `CString::into_raw()` (which is `*mut`).

### 67. External app: `getFullGraph()` is O(n²) API calls for N nodes
**`graph.service.ts:133-190`** — Calls `expand()` for every single node.

---

## Top 5 actions with highest ROI

1. **Fix auth table exposure (C2)** — Single config change blocks `MATCH (u:auth_users ...)`. Closing the highest-severity security hole.
2. **Fix public-path prefix bypass (C3)** — Change one line from `starts_with(p)` to `== p || starts_with(p + "/")`.
3. **Fix `CountSubquery` dummy impl (C1)** — Subqueries (`EXISTS { ... }`) return completely wrong results. Makes the feature work at all.
4. **Fix `PhysicalMerge` no-op `fetch_add(0)` (C5)** — One-character fix restores MERGE MATCH affected-row counting.
5. **Fix `get_effective_interventions()` dead return (H28)** — Remove one line to restore the psych app's therapist functionality.
