# LIGHTNING DATABASE — COMPREHENSIVE IMPLEMENTATION PLAN

> Generated 2026-06-09 | Branch: `prod-hardening`
> Use `[ ]` for pending, `[X]` for completed

---

## TIER 0: IMMEDIATE CRITICAL FIXES (Data Loss / Wrong Results)

---

### [X] 0.1 Fix WAL CRC Check Discarded in CDC Reader

**File:** `crates/lightning-core/src/storage/wal.rs:489-495`
**Risk:** Silent data corruption — CDC subscribers accept corrupted WAL records without validation.

**Issue:** In `WALRecordIter::next_record()`, the CRC is computed but the result is assigned to `_computed_crc` (prefixed with underscore), meaning it's never compared to `stored_crc`. The `_computed_crc = digest.finalize()` result is silently discarded.

**Plan:**
1. Change `let _computed_crc = digest.finalize();` to compare with `stored_crc`
2. If CRC doesn't match, skip the record (same as the replay path does)
3. Add `corrupt_records_skipped` counter to the iterator

**Code change:**
```rust
// wal.rs:495 — change from:
let _computed_crc = digest.finalize();
// To:
let computed_crc = digest.finalize();
if computed_crc != stored_crc {
    // Skip corrupted record
    self.pos += 1;
    continue;
}
```

---

### [X] 0.2 Fix Cypher Injection in fusion.rs — Parameterize All Queries

**File:** `crates/lightning-core/src/fusion.rs:34,56,71,99-104,127-129,161,397-404`
**Risk:** Critical security vulnerability — arbitrary Cypher injection via any user-supplied string parameter.

**Issue:** Every method in `FusionApp` constructs queries via `format!()` string interpolation. The `sq()` function only escapes single quotes but misses backslash, unicode, and other Cypher metacharacters. The codebase already supports parameterized queries (`$param` syntax) used elsewhere.

**Plan:**
Replace ALL string-interpolated queries with parameterized equivalents:

1. `find_node_by_name()` — use `$name` parameter
2. `find_paths()` — use `$source_id` and `$target_id` parameters  
3. `find_connected_nodes()` — use `$node_id` parameter
4. `lookup_node_names()` — rewrite IN clause using UNWIND + parameter array
5. `add_observation()` — use `$id`, `$content` parameters
6. `materialize_pagerank()` — use UNWIND with parameter arrays instead of string building

Key pattern to follow (already used in `memory.rs`):
```rust
// INSTEAD OF:
let q = format!("MATCH (n:CodeNode) WHERE n.name = '{}' RETURN n.id", sq(name));
// USE:
let q = "MATCH (n:CodeNode) WHERE n.name = $name RETURN n.id".to_string();
let mut params = HashMap::new();
params.insert("name".to_string(), Value::String(name.to_string()));
conn.query(&q, Some(params))?;
```

For `lookup_node_names()` with IN clause, use UNWIND:
```cypher
UNWIND $ids AS id MATCH (n:CodeNode) WHERE n.id = id RETURN n.id, n.name, n.node_type
```

For `materialize_pagerank()` bulk update, use UNWIND with parameter arrays.

**Delete** the `sq()` function entirely after all call sites are converted.

---

### [X] 0.3 Fix HashJoin Condition Extraction — Make Joins Correct

**Files:**
- `crates/lightning-core/src/processor/physical_plan.rs:169-193`
- `crates/lightning-core/src/processor/operators/hash_join.rs`
- `crates/lightning-core/src/planner/binder.rs` (BoundExpression analysis)

**Risk:** All non-cross joins return wrong results. The join condition `BoundExpression` is never analyzed to extract key columns — `HashJoin::new()` is always called with `(0, 0)` as key indices.

**Issue:** When a `LogicalOperator::Join(left, right, join_cond)` is planned, the physical planner only checks if `join_cond` is `Literal(true)` (cross join). For any real join condition like `n.id = r._src`, it still calls `HashJoin::new(planned_left, planned_right, 0, 0)`. The hash join builds on column 0 of both sides regardless of the actual join predicate.

**Plan:**

**Step 1:** Create join condition analyzer in `physical_plan.rs`:
- Parse `BoundExpression::Comparison(PropertyLookup(_, left_idx, _), Equal, PropertyLookup(_, right_idx, _))`
- Determine which side each PropertyLookup belongs to (by variable name matching left/right plan variable positions)
- Return `(left_key_idx, right_key_idx)` or `None` if condition is complex

**Step 2:** Modify `PhysicalPlanner::plan()` for `LogicalOperator::Join`:
```rust
LogicalOperator::Join(left, right, join_cond) => {
    let planned_left = self.plan(*left)?;
    let planned_right = self.plan(*right)?;
    let is_cross_join = matches!(join_cond, BoundExpression::Literal(Literal::Boolean(true)));
    
    if is_cross_join {
        Ok(Box::new(HashJoin::new_cross_join(planned_left, planned_right)))
    } else if let Some((l_key, r_key)) = self.extract_join_keys(&join_cond, &left, &right) {
        Ok(Box::new(HashJoin::new(planned_left, planned_right, l_key, r_key)))
    } else {
        // Fallback: nested loop join or filter after cross join
        // For now, use cross join + filter (correct but slow)
        let hj = HashJoin::new_cross_join(planned_left, planned_right);
        Ok(Box::new(FilterOperator::new(hj, join_cond)))
    }
}
```

**Step 3:** Add `extract_join_keys()` method to `PhysicalPlanner`:
```rust
fn extract_join_keys(
    &self,
    cond: &BoundExpression,
    left_op: &LogicalOperator,
    right_op: &LogicalOperator,
) -> Option<(usize, usize)> {
    // Match Comparison(PropertyLookup(var_a, idx_a, _), Equal, PropertyLookup(var_b, idx_b, _))
    // Determine which side each variable belongs to via collect_variable_positions
    match cond {
        BoundExpression::Comparison(
            Box::new(BoundExpression::PropertyLookup(var_a, idx_a, _)),
            ComparisonOperator::Equal,
            Box::new(BoundExpression::PropertyLookup(var_b, idx_b, _)),
        ) => {
            let left_positions = self.compute_variable_positions(left_op).ok()?;
            let right_positions = self.compute_variable_positions(right_op).ok()?;
            match (left_positions.contains_key(var_a), left_positions.contains_key(var_b)) {
                (true, false) => Some((*idx_a, *idx_b)),
                (false, true) => Some((*idx_b, *idx_a)),
                _ => None,
            }
        }
        _ => None,
    }
}
```

**Step 4:** Add a `PhysicalFilter` operator that wraps another operator + filter expression for non-equi join fallback. This already exists as `PhysicalFilter` in `filter.rs`.

---

### [X] 0.4 Fix MERGE Discarding Child Plan

**File:** `crates/lightning-core/src/processor/physical_plan.rs:579`
**Risk:** Silent correctness bug — MERGE queries with preceding MATCH/WITH don't have access to variables from those clauses.

**Issue:** `let _planned_child = self.plan(*child)?;` — the child logical plan is fully planned into a physical operator tree, which is then immediately dropped. The `_planned_child` variable is never used.

**Plan:**
The `PhysicalMerge` operator needs to accept and use the child plan. The child provides the binding context (variables from MATCH/WITH). Currently MERGE only supports standalone `MERGE (n:Label {prop: val})` without a preceding MATCH.

1. Modify `PhysicalMerge` to accept an optional `Box<dyn PhysicalOperator>` child
2. Before the merge logic, execute the child to get the binding context
3. Pass the binding context (row values) to the merge pattern evaluation so property expressions can reference variables from preceding clauses
4. Evaluate pattern properties against the child's output rows (not just params)

Example: `MATCH (a:Person) MERGE (a)-[:KNOWS]->(b:Person {name: 'Bob'})` — the `a` variable must come from the child.

---

### [X] 0.5 Fix COUNT(*) Materializing Dummy Column

**File:** `crates/lightning-core/src/planner/logical_plan.rs:718-731`
**Risk:** Significant performance waste — COUNT(*) forces full column materialization of a dummy `1.0` column for every single row.

**Issue:** The logical planner adds `Literal::Number(1.0)` as a projection item for `COUNT(*)`. This forces the scan to materialize a column of 1.0 values for every row, wasting memory and bandwidth.

**Plan:**
Create a specialized `CountStar` aggregate function that doesn't require any input column:

1. Add `CountStar` variant to `AggregateFunction` enum in `aggregate.rs`
2. Implement `CountStar` in `aggregate_function.rs` — it simply adds `num_rows` on each `update_vector` call (ignoring the array)
3. In `logical_plan.rs:718-731`, when `args.is_empty()` (COUNT(*)), use `AggregateFunction::CountStar` instead of adding a dummy column
4. The `input_idx` for `CountStar` can be `0` (it won't use it), but it should not add any projection item

This eliminates the `_dummy` column entirely.

---

## TIER 1: HIGH PRIORITY (Correctness + Security + Performance)

---

### [X] 1.1 WASM Timeout Enforcement

**File:** `crates/lightning-core/src/wasm_function.rs`
**Risk:** Denial of service — WASM functions can execute indefinitely.

**Issue:** The `timeout_ms` field (default 100ms) exists but is never checked or enforced. No timer, no interrupt mechanism. The WASM function runs until completion or trap.

**Plan:**
1. Use `std::thread::spawn` + `JoinHandle` pattern to run WASM in a separate thread with a timeout
2. Spawn a thread, execute the WASM call, join with timeout
3. If timeout elapses, drop the thread handle (the thread becomes detached) and return an error
4. Or use `parking_lot::Condvar` with timeout + `AtomicBool` flag that child thread checks periodically

**Simpler approach:**
```rust
let handle = std::thread::spawn(move || {
    // WASM execution
    func.call(&mut store, args)
});
match handle.join_timeout(Duration::from_millis(timeout_ms)) {
    Ok(Ok(result)) => Ok(result),
    Ok(Err(e)) => Err(e),
    Err(_) => Err(LightningError::Internal("WASM execution timed out".into())),
}
```
Note: `wasmi` doesn't support interruption natively. Thread-based timeout is the safest approach.

---

### [X] 1.2 Fix Plan Cache Key Inconsistency
### [X] 1.3 Fix Variable-Length Relationship Bounds Discarded
### [X] 1.4 Fix MinHash Similarity Denominator
### [X] 1.5 Fix Sequential Commit Holding Connection Lock During I/O
### [X] 1.6 CREATE REL TABLE Ignores `if_not_exists`
### [X] 1.7 Fix Catalog Save After WAL Truncation Ordering
### [X] 1.8 Fix DETACH DELETE Full Rel Table Scan
### [X] 1.9 Fix Prefetch I/O Under Write Lock

## TIER 2: MEDIUM PRIORITY (Performance + Quality)

---

### [X] 2.1 Re-Enable Projection Pushdown Optimizer

### [X] 2.9 Remove Debug `println!` Statements

**File:** `crates/lightning-core/src/memory.rs:675`
**Risk:** Sensitive data exposure in production logs.

**Issue:** `println!("query: {query}");` outputs the full query to stdout.

**Plan:**
Change to `tracing::debug!()` or remove entirely.

---

### [X] 2.10 Fix `ensure_csr_fresh` / `rebuild_csr_if_stale` Duplication

**File:** `crates/lightning-core/src/storage/storage_manager.rs:976-1020`
**Risk:** Code duplication — two identical methods.

**Issue:** `ensure_csr_fresh` and `rebuild_csr_if_stale` have the exact same implementation.

**Plan:**
Make one call the other:
```rust
pub fn ensure_csr_fresh(&self, table_name: &str, bm: &BufferManager, tx: &Transaction) -> Result<()> {
    self.rebuild_csr_if_stale(table_name, bm, tx)
}
```

---

### [X] 2.11 Fix `normalize_query()` Name Collision

**Files:**
- `crates/lightning-core/src/lib.rs:37-39` (regex-based normalization)
- `crates/lightning-core/src/parser/mod.rs:74-112` (comment/whitespace stripping)

**Issue:** Two functions named `normalize_query` with different purposes. The cache key uses the lib.rs version, but queries go through the parser's version first. Cache keys might not match.

**Plan:**
1. Rename `lib.rs`'s `normalize_query` to `normalize_literals`
2. Rename `parser/mod.rs`'s `normalize_query` to `normalize_whitespace_and_comments`
3. The cache key should use both: first strip comments/whitespace, then normalize literals
4. Ensure the parser and cache use the same pipeline

---

### [X] 2.12 Fix `sync_all_data_files` Walks Entire Column Tree Unnecessarily — already handled: `sync_column_files` checks `col.dirty` before syncing

**File:** `crates/lightning-core/src/storage/storage_manager.rs:942-949`
**Risk:** Unnecessary I/O on every commit.

**Issue:** `sync_all_data_files` recurses through every column and child column, even when `dirty` flag shows no changes.

**Plan:**
Skip columns that aren't dirty:
```rust
pub fn sync_all_data_files(&self) -> Result<()> {
    for table in self.node_tables.values().chain(self.rel_tables.values()) {
        for col in &table.columns {
            if col.dirty.load(Ordering::Acquire) {
                self.sync_column_files(col)?;
            }
        }
    }
    Ok(())
}
```

---

### [X] 2.13 Fix SET Vector Index Update Skipped

**File:** `crates/lightning-core/src/processor/operators/dml.rs:430-436`

**Issue:** After SET on an embedding column, the vector index is stale — the code explicitly skips updating it.

**Plan:**
Implement vector index update for SET:
```rust
if let Some(ref vec_idx) = vec_opt {
    let emb_col_idx = self.table.columns.iter().position(|c| {
        c.data_type == LogicalType::List(Box::new(LogicalType::Float))
    });
    if let Some(emb_idx) = emb_col_idx {
        if updated_props.contains(&emb_idx) {
            if let Ok(val) = self.table.columns[emb_idx].get_value(bm, *node_id, tx) {
                if let Value::List(ref emb) = val {
                    let emb_f32: Vec<f32> = emb.iter()
                        .filter_map(|v| if let Value::Number(n) = v { Some(*n as f32) } else { None })
                        .collect();
                    if emb_f32.len() == vec_idx.dimension() {
                        // Flat vector index: write at node_id position
                        let _ = vec_idx.update(node_id, &emb_f32, bm, tx);
                    }
                }
            }
        }
    }
}
```

---

### [X] 2.14 Fix `Hash` Implementation for `Value::Map` Non-Determinism

**File:** `crates/lightning-core/src/processor/mod.rs:268-287`

**Issue:** The `Hash` implementation for `Value::Map` sorts entries by hash of key, then uses `wrapping_add` to combine hashes. If two different key-value pairs hash to the same `wrapping_add` sum, they produce the same hash (hash collision).

**Plan:**
Use a proper hash combination like `h = h.wrapping_mul(31).wrapping_add(key_hash).wrapping_add(val_hash)`:
```rust
let mut h: u64 = 0;
for (_, k, v) in entries {
    let mut hasher = DefaultHasher::new();
    k.hash(&mut hasher);
    v.hash(&mut hasher);
    h = h.wrapping_mul(31).wrapping_add(hasher.finish());
}
h.hash(state);
```

---

## TIER 3: PRODUCTION FEATURES (Missing Functionality)

---

### [ ] 3.1 Add Query Timeout Enforcement

**File:** `crates/lightning-core/src/lib.rs:741` (`ClientContext.query_timeout_ms`)

**Issue:** The `query_timeout_ms` field exists but is never checked or enforced.

**Plan:**
1. In `Processor::execute()` and `Processor::execute_stream()`, spawn execution with a timeout
2. Use `crossbeam::channel` with a timeout on receive
3. Or use `std::thread::spawn` + `JoinHandle` pattern with timeout
4. When timeout fires, set an `AtomicBool` cancellation flag that operators check periodically

---

### [ ] 3.2 Add Memory Quota Enforcement

**File:** `crates/lightning-core/src/lib.rs:742` (`ClientContext.memory_quota`)

**Issue:** The `memory_quota` field exists but is never checked or enforced.

**Plan:**
1. Add a `MemoryTracker` that tracks allocations per query
2. Pass it through operator execution context
3. Operators check before allocating large arrays
4. Sort and Aggregate check before collecting batches

---

### [ ] 3.3 Add Prometheus Metrics Export

**File:** `crates/lightning-core/src/lib.rs:162-229` (`DatabaseMetrics`)

**Issue:** `DatabaseMetrics` exists with atomic counters but no export mechanism.

**Plan:**
1. Add `prometheus` crate dependency (or expose via HTTP endpoint)
2. Implement `collect()` on `DatabaseMetrics` that returns Prometheus metric families
3. Expose via an HTTP endpoint (e.g., `/metrics`)
4. Metrics to expose:
   - `lightning_queries_total`
   - `lightning_checkpoints_total`
   - `lightning_checkpoint_duration_ms`
   - `lightning_wal_bytes_written`
   - `lightning_wal_fsync_count`
   - `lightning_buffer_evictions_total`
   - `lightning_buffer_hit_ratio`
   - `lightning_transactions_active`
   - `lightning_tables_total`
   - `lightning_storage_size_bytes`

---

### [ ] 3.4 Add Audit Logging

**Issue:** No audit trail of queries or schema changes.

**Plan:**
1. Create `AuditLogger` that records each query (user, timestamp, query text, duration, status)
2. Wire into `Connection::execute()` after query completion
3. Store audit log in a separate append-only file (`audit.log`)
4. Configurable via `SystemConfig.audit_log_enabled`

---

### [ ] 3.5 Add Connection Pooling

**Issue:** Each `Connection` is standalone, no pooling.

**Plan:**
1. Create `ConnectionPool` struct
2. Uses `crossbeam::channel` or `Arc<Mutex<VecDeque<Connection>>>`
3. Configurable min/max connections
4. Health-check connections on borrow (ping the database)
5. Timeout on pool exhaustion

---

### [ ] 3.6 Add UNIQUE Constraint Enforcement

**Issue:** No UNIQUE constraint support beyond PRIMARY KEY.

**Plan:**
1. Extend `NodeConstraint` to support `ConstraintType::Unique`
2. On DML (CREATE, SET), check existing values before writing
3. Use a secondary index (hash or B-tree) for uniqueness checks
4. Report constraint violation error to caller

---

### [ ] 3.7 Add Foreign Key Enforcement

**Issue:** No referential integrity — you can CREATE a relationship between non-existent nodes.

**Plan:**
1. On `CREATE REL`, verify both `_src` and `_dst` exist in the source/destination tables
2. On `DELETE node`, either reject (CASCADE is not implemented) or cascade delete relationships
3. Configurable via table definition options

---

### [ ] 3.8 Add Schema Versioning for Data Files

**Issue:** No version stamp in column, overflow, or index files. Future versions can't detect format incompatibility.

**Plan:**
1. Add a 4-byte magic number + 4-byte version to the start of each data file type
2. On open, validate the version
3. Add migration path for format changes

---

### [ ] 3.9 Add Point-in-Time Recovery (PITR) API

**Issue:** WAL archiving exists (`wal.rs:94-134`) but no restore API.

**Plan:**
1. Create `PITRManager` that:
   - Lists available WAL archives
   - Restores database to a specific archive sequence number
   - Replays archived WALs from checkpoint to target sequence
2. API: `Database::restore_to_timestamp(timestamp)` or `restore_to_sequence(seq)`

---

### [ ] 3.10 Add Data Type Support

**Issue:** Missing common types: DECIMAL, TEXT (unlimited), BLOB, UUID, JSON, INET, CIDR.

**Plan:**
1. Add variants to `LogicalType` enum
2. Implement Arrow type mapping
3. Implement serialization/deserialization in `column.rs`
4. Implement expression evaluation support in `evaluator.rs`

---

### [ ] 3.11 Add OFFSET/LIMIT with Cursor-Based Pagination

**Issue:** OFFSET/LIMIT is O(n) — it must scan and skip all preceding rows.

**Plan:**
1. Implement keyset/cursor-based pagination: `WHERE id > $cursor LIMIT $page_size`
2. Expose as `ORDER BY ... LIMIT ...` optimization in the planner
3. Detect ORDER BY + LIMIT patterns and convert to cursor-based scan

---

### [ ] 3.12 Add Backup/Restore API

**Issue:** No built-in backup — requires manual file copy which may be inconsistent.

**Plan:**
1. `Database::backup(path: &Path)` — creates consistent snapshot:
   - Checkpoint all dirty pages
   - Flush and sync all files
   - Hardlink or copy data files to backup directory
2. `Database::restore(path: &Path)` — replaces current database with backup

---

## TIER 4: LOW PRIORITY (Code Quality + Tech Debt)

---

### [X] 4.1 Remove Dead Code: `parse_arithmetic()`

**File:** `crates/lightning-core/src/parser/mod.rs:1152-1166`

**Plan:** Remove the legacy `parse_arithmetic` function and its comment.

---

### [X] 4.2 Remove Dead Code: `reset_referenced()`

**File:** `crates/lightning-core/src/storage/buffer_manager.rs:762-769`

**Plan:** Remove the unused `reset_referenced()` method.

---

### [X] 4.3 `get_variables()` is used by other optimizers — kept but marked as needing Join/Union child fix

**File:** `crates/lightning-core/src/planner/logical_plan.rs:284-303`

**Plan:** Either complete the implementation (handle Join, Union right children) or remove.

---

### [X] 4.4 Fix `#[deprecated]` Kuzu Function Warnings

**File:** `crates/lightning-core/src/capi.rs`

**Plan:** Remove the deprecated kuzu_* wrapper functions entirely.

---

### [X] 4.5 Fix Database Drop Busy-Wait Loop

**File:** `crates/lightning-core/src/lib.rs:278-284`

**Plan:** Replace busy-wait with proper condition variable or channel-based notification.

---

### [X] 4.6 Fix `FileHandle::file_id` Collision Comment

**File:** `crates/lightning-core/src/storage/file_handle.rs:43-47`

**Plan:** Fix the contradictory comment (says "Use ONLY the filename" but then hashes the full path). Keep full-path hashing but add UUID fallback:
```rust
let mut hasher = DefaultHasher::new();
path.as_os_str().hash(&mut hasher);
let file_id = hasher.finish();
// If collision detected (existing file_id in manager), regenerate
```

---

### [X] 4.7 Fix `now_micros_for_test` — Remove From Production Code

**File:** `crates/lightning-core/src/memory.rs:202-204`

**Plan:** Move test-only methods behind `#[cfg(test)]`.

---

### [X] 4.8 Fix All `unreachable!()` Calls

**File:** `crates/lightning-core/src/storage/column.rs:1185`

**Plan:** Replace `unreachable!()` with proper error handling or `LogicalType` enum extension.

---

## TIER 5: TESTING INFRASTRUCTURE

---

### [ ] 5.1 Add Concurrent Execution Test Suite

**Files needed:** `tests/concurrent_test.rs`

Test scenarios:
- Two threads read the same data concurrently
- One thread writes while another reads (MVCC isolation)
- Two threads write to different tables
- Two threads write to different rows on the same page (row-level merge)
- Two threads write to the same row (write-write conflict detection)

---

### [ ] 5.2 Add Crash Recovery Test Suite

**Files needed:** `tests/crash_recovery_test.rs`

Test scenarios:
- Kill process during INSERT, restart, verify data
- Kill process during checkpoint, restart, verify recovery
- Corrupt WAL record, verify recovery handles it
- Partially written WAL, verify recovery
- Catalog save failure, verify fallback

---

### [ ] 5.3 Add Security Test Suite

**Files needed:** `tests/security_test.rs`

Test scenarios:
- Injection attacks via query parameters
- WASM module with infinite loop
- WASM module with excessive memory allocation
- File path traversal in COPY FROM/TO
- Large query DoS

---

### [ ] 5.4 Add Transaction Isolation Test Suite

**Files needed:** `tests/isolation_test.rs`

Test scenarios:
- Dirty read prevention
- Non-repeatable read prevention  
- Phantom read prevention (or allowance per isolation level)
- Write skew detection
- Read-only transaction consistency

---

### [ ] 5.5 Add WAL Unit Tests

**Files needed:** `crates/lightning-core/src/storage/wal.rs` (append to `#[cfg(test)]`)

Test scenarios:
- Write and read back WAL records
- CRC validation on read
- Truncation and re-use
- Partial record at EOF handling
- Group commit buffer flush
- WAL archiving + restore

---

### [ ] 5.6 Add MVCC RowVersion Unit Tests

**Files needed:** `crates/lightning-core/src/storage/row_version.rs` (extend `#[cfg(test)]`)

Test scenarios:
- Mark row → commit → visible
- Mark row → rollback → not visible
- Two transactions mark same row → conflict detected
- Visibility mask for batch reads
- Vacuum removes old committed entries
- Bulk row range commit

---

## IMPLEMENTATION ORDER SUMMARY

```
Phase 0 — Critical (5 items):    0.1 → 0.2 → 0.3 → 0.4 → 0.5
Phase 1 — High (14 items):       1.1 → 1.2 → ... → 1.14
Phase 2 — Medium (14 items):     2.1 → 2.2 → ... → 2.14
Phase 3 — Features (12 items):   3.1 → 3.2 → ... → 3.12
Phase 4 — Low (8 items):         4.1 → 4.2 → ... → 4.8
Phase 5 — Tests (6 items):       5.1 → 5.2 → ... → 5.6
```

---



---

## RALPH LOOP PROMPT

Below is the complete prompt to pass to `/ralph-loop` for sequential implementation.
Copy everything between the markers and pass as the `task` parameter.

```
<ralph_loop_prompt>
You are implementing production hardening fixes for the Lightning graph database.
Worktree: `/Users/bviga/Developement/new_research/research/lightning/.forge/worktrees/prod-hardening`
Branch: `prod-hardening`

IMPLEMENTATION PLAN: Read the file `IMPLEMENTATION_PLAN.md` in the worktree root.
It contains all items to implement with checkboxes [ ].

WORKFLOW for each item:
1. Read the relevant source files to understand the current code
2. Implement the fix
3. Build with `cargo build 2>&1` (from the worktree root)
4. Fix any compilation errors
5. If relevant tests exist, run them with `cargo test <test_name> 2>&1`
6. Git commit with a descriptive message
7. Git push origin prod-hardening
8. Update the checkbox in IMPLEMENTATION_PLAN.md from [ ] to [X]
9. Move to the next item

CRITICAL RULES:
- Only modify `.rs` files — never read or modify `.md` files (except IMPLEMENTATION_PLAN.md)
- Work from worktree root: `/Users/bviga/Developement/new_research/research/lightning/.forge/worktrees/prod-hardening`
- All git commands must run from the worktree root
- After each commit, run `git push origin prod-hardening`
- Update the checkbox in IMPLEMENTATION_PLAN.md after each item
- Start from TIER 0 and go through each checkbox sequentially
- If a build fails, fix the error and try again
- If you encounter an error you cannot fix, log it and move to the next item
- DO NOT skip items unless they are truly infeasible (document why)
- After each item, verify tests pass (or at least compile)
- Keep commits focused: one commit per checkbox item

Start with item [ ] 0.1 — Fix WAL CRC Check Discarded in CDC Reader
</ralph_loop_prompt>
```

Copy the prompt above and use `/ralph-loop` with it to begin sequential implementation.
