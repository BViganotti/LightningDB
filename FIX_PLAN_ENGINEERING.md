# LightningDB Bug Fix Plan

Discovered by `lightning-feature-test/` — 17 remaining failures after parallel-safety and ORDER BY fixes.

---

## Priority 0 (P0): DML → RETURN Column Values Broken

**Bug 1: `SET ... RETURN n.col` → 500**
**Bug 4: `MERGE ... RETURN n.col` → undefined values**
**Bug 8: `CREATE ... RETURN n.col` → undefined values**

### Root Cause (All Three)

All DML operators (`PhysicalSet`, `PhysicalMerge`, `PhysicalCreate`) produce a **synthetic single-column count batch** (`{count: N}`) instead of passing through the created/modified node's data:

```rust
// dml.rs:223-242 (CREATE), dml.rs:462-480 (SET), dml.rs:1070-1088 (MERGE)
if self.shared_state.results_returned.fetch_add(1, Ordering::SeqCst) == 0 {
    let total = self.shared_state.total_affected.load(Ordering::SeqCst);
    return Ok(Some(DataChunk {
        batch: RecordBatch::try_new(
            Arc::new(Schema::new([Field::new("count", DataType::Float64, true)])),
            vec![Arc::new(Float64Array::from(vec![total as f64]))],
        ).expect("..."),
    }));
}
```

The downstream `Projection` (from `RETURN n.id, n.name`) evaluates `PropertyLookup(n, idx)` against this 1-column batch → **index out of bounds** → `LightningError::Internal` → HTTP 500.

### Fix Approach

**Option A (Recommended — minimal diff):** Extend the DML result batch to include the node's properties alongside the count. The DML operators already have access to `self.table` and the created node's internal ID. After creating/updating the node, read back the property values and add them as additional columns in the output batch.

For `PhysicalMerge` and `PhysicalSet`, the operator already holds `self.assignments` and `self.table`. After the DML operation:

1. Read row data from storage for the affected internal IDs
2. Build additional columns in the output batch matching the RETURN projection's expected schema
3. The Projection's PropertyLookup indices will now resolve correctly

**Option B (Architectural — larger scope):** Remove the synthetic batch entirely. The DML operator's `get_next` should yield the original node data as if it were a `MATCH ... RETURN`. The `RETURN count(*)` case (which relies on the synthetic batch) would need to be handled by an Aggregate placed after the DML that counts the yielded rows.

**Recommendation:** Option A — minimal, targeted, production-safe.

### Files to modify
- `crates/lightning-core/src/processor/operators/dml.rs`:
  - `PhysicalCreate::get_next()` (line ~264)
  - `PhysicalSet::get_next()` (line ~494)
  - `PhysicalMerge::get_next()` (line ~1090)

---

## Priority 1 (P1): Aggregate / GROUP BY

**Bug 5: `GROUP BY` + `ORDER BY` → 500**
**Bug 6: `ORDER BY alias` after GROUP BY → 404**

### Root Cause (Bug 5)

The planner places `Sort` between `Aggregate` and `Projection`, but Sort's ORDER BY expression uses **binder-relative PropertyLookup indices** that reference the original scan schema, not the Aggregate's output schema:

```rust
// logical_plan.rs ~line 888
if let Some(order_by) = &ret.order_by {
    current_plan = LogicalOperator::Sort(
        Box::new(current_plan),  // wraps Aggregate
        order_by.clone(),        // indices are scan-relative!
    );
}
```

For `RETURN n.dept, count(*) AS cnt ORDER BY n.dept`:
- Aggregate output: 2 columns (`group0`=String, `agg0`=Int64)
- Sort evaluates `PropertyLookup("n", 2)` against 2-column batch → OOB → 500

### Root Cause (Bug 6)

RETURN aliases are not registered as variables for ORDER BY resolution:

```rust
// binder.rs ~line 1308
// Aliases like "total" are NOT added to self.variables
// so ORDER BY total fails with "Variable total not found"
```

### Fix Approach (Bug 5)

In the aggregate return path of `logical_plan.rs`, remap the Sort's ORDER BY PropertyLookup indices from binder-relative to aggregate-output-relative indices:

- GROUP BY columns map to aggregate output index 0..N-1
- Aggregate columns map to aggregate output index N..N+M-1

The `final_items` at line 819-849 already does this remapping for the *projection*. The Sort's `order_by` items need the same treatment.

### Fix Approach (Bug 6)

In `binder.rs`, before binding ORDER BY expressions, register RETURN aliases as variables:

```rust
// Before order_by binding in bind_return_clause
for item in &ret_items {
    if !item.alias.is_empty() && item.variable_name.is_none() {
        self.variables.insert(item.alias.clone(), BoundVariable {
            table_name: String::new(),
            type_: item.expression.get_type(),
        });
    }
}
```

### Files to modify
- `crates/lightning-core/src/planner/logical_plan.rs` (lines ~887-892)
- `crates/lightning-core/src/planner/binder.rs` (lines ~1308-1319)

---

## Priority 2 (P2): BOOL WHERE Filter Matches All Rows

### Root Cause

`compare_column_literal` in `evaluator.rs` has a fast path for `Literal::Number` and `Literal::String` but **not for `Literal::Boolean`**:

```rust
// evaluator.rs ~line 795-855
if let Literal::Number(n) = lit { /* handles number */ }
if let Literal::String(s) = lit { /* handles string */ }
// Boolean literal falls through → None → fallback path
```

The fallback path evaluates both sides and calls `eq(BooleanArray, Float64Array)` or similar type-mismatched comparison, which produces incorrect results (all-true mask).

### Fix Approach

Add Boolean literal handling to `compare_column_literal`:

```rust
if let Literal::Boolean(b) = lit {
    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
        let scalar = BooleanArray::new_scalar(*b);
        let res = match op {
            Equal => eq(arr, &scalar),
            NotEqual => neq(arr, &scalar),
            _ => return None,
        };
        return Some(res.map(|a| Arc::new(a) as ArrayRef)
            .map_err(|e| LightningError::Internal(e.to_string())));
    }
    // Also handle Int64 storage (0/1) for boolean columns
}
```

### File to modify
- `crates/lightning-core/src/processor/evaluator.rs` (function `compare_column_literal`, ~line 795)

---

## Priority 3 (P3): DML Edge Cases

**Bug 3: `DELETE non-existent node RETURN count(*)` → returns 1 instead of 0**

### Root Cause

The `Delete` operator produces a synthetic count batch (`{count: 0}` with 1 row). The downstream `Aggregate` (for `count(*)`) counts this synthetic row as 1, not 0. The aggregate sees 1 input row with `_dummy = 1.0` → `count = 1`.

### Fix Approach

When the DML operator has no child rows (nothing to delete), it should return `Ok(None)` instead of producing a `{count: 0}` batch. The `RETURN count(*)` should then receive 0 input rows → `count = 0`.

Alternatively, implement the Option A fix from Priority 0 (pass through node data) which naturally fixes this: the Aggregate sees 0 matching rows and returns 0.

### File to modify
- `crates/lightning-core/src/processor/operators/dml.rs` (PhysicalDelete, PhysicalMerge — early-exit when no child rows)

---

## Priority 4 (P4): Concurrency

**Bug 7: `SET n.c = n.c + 1` → 500 under concurrent execution**

### Root Cause

The read-modify-write cycle `counter = counter + 1` spans multiple operator calls:
1. Scan reads `counter = 5` at `read_ts = T1`
2. Set evaluates `5 + 1 = 6`
3. Set writes `counter = 6`

Under MVCC, if two transactions run concurrently, both read `5`, compute `6`, and attempt to write. The second write detects a version conflict and fails with 500.

### Fix Approach

**Option A (Recommended):** Retry the SET on version conflict. When the storage layer rejects a write due to MVCC conflict, abort and retry the entire SET operation with a fresh snapshot.

**Option B:** Implement atomic increment at the storage level (`SET n.counter += 1` as a delta operation that bypasses the read-modify-write cycle).

### Files to modify
- `crates/lightning-core/src/processor/operators/dml.rs`
- `crates/lightning-core/src/storage/storage_manager.rs` (retry logic)

---

## Priority 5 (P5): Misc

**Bug 9: `ORDER BY + LIMIT` column values undefined**

Investigation shows the Sort operator yields batches via `compare_exchange` on `results_returned`. Under single-threaded execution this should be correct. May be a race condition in the Limit operator's interaction with Sort's batched output.

Likely fix: Ensure `PhysicalLimit` and `PhysicalSort` interact correctly when Limit wraps Sort (not through TopK optimization).

**Non-existent table returns 404 instead of 400**

Minor: the error handler in `routes/query.rs` maps `LightningError::Query(...)` to HTTP 404 when it should be 400 for table-not-found errors.

---

## Execution Order

```
Week 1: P0 (DML RETURN) — unlocks MERGE, CREATE, SET with RETURN
Week 2: P1 (GROUP BY) — unlocks analytics queries
Week 3: P2 (BOOL filter) — one-function fix
Week 4: P3 (DELETE count) — often fixed by P0
Week 5: P4 (Concurrency) — complex, needs MVCC understanding
Week 6: P5 (Misc) — low-hanging fruit
```

## Verification

After each fix, run:
```bash
cd lightning-feature-test && npx tsc && LIGHTNING_URL=http://127.0.0.1:9199 node dist/runner.js
```

Expected progression:
- P0 fixes: 47 → ~55 passing (MERGE, CREATE, SET, DELETE tests)
- P1 fixes: 55 → ~60 passing (GROUP BY tests)
- P2 fixes: 60 → ~62 passing (BOOL filter tests)
- P3 fixes: 62 → ~64 passing (DELETE count)
- P4 fixes: 64 → ~66 passing (concurrent SET)
- P5 fixes: 66 → ~68 passing (LIMIT column, error codes)

Target: **68/68 tests passing** (100% of feature test suite).
