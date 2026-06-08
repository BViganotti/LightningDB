# LightningDB — LLM Remediation Plan v2: Deep Scan Findings

> **Purpose**: Companion to `LLM_REMEDIATION_PLAN.md`. Covers ~299 newly discovered issues from deep scans of parser, optimizer, operators, compression, storage internals, catalog, connections, bindings, and tests. Organized by **importance to a trustworthy, usable codebase**.
>
> **Checkbox tracking**: `[ ]` = not started, `[~]` = in progress, `[X]` = done.
>
> **Priority tags**: `[P0]` = silent corruption/wrong results, `[P1]` = trust erosion, `[P2]` = scale ceiling, `[P3]` = polish.
>
> **File paths** are relative to the workspace root.
>
> **Note**: Items from v1 are NOT duplicated here. This is additive. Cross-references like `(v1 §1.2)` point to items in `LLM_REMEDIATION_PLAN.md`.

---

## Ranking Rationale

```
Tier 1 — Silent data corruption / wrong query results      [Sections 1-6]
Tier 2 — Trust erosion (features broken or misleading)     [Sections 7-10]
Tier 3 — Scale ceiling and correctness gaps                [Sections 11-14]
Tier 4 — Hardening, tests, polish                          [Sections 15-18]
```

---

## Section 1: Arithmetic & Expression Evaluation (Wrong Results)

**Why first**: Wrong arithmetic results poison every numeric query. Case expressions silently return nothing. These are the #1 correctness bugs in the entire codebase.

### 1.1 No Operator Precedence in Arithmetic

**File**: `crates/lightning-core/src/parser/cypher.pest:114`

**Problem**: `arithmetic_expr = { atom ~ ( arithmetic_operator ~ atom )* }` gives `+`, `-`, `*`, `/`, `%` the same precedence. `1 + 2 * 3` parses as `(1+2)*3 = 9` instead of `1+(2*3) = 7`.

- [ ] **1.1.1** `[P0]` Split into `term` and `factor` with correct precedence:
  ```pest
  term = { factor ~ (("+" | "-") ~ factor)* }
  factor = { atom ~ (("*" | "/" | "%") ~ atom)* }
  ```

### 1.2 CASE Expression AST Always Empty

**File**: `crates/lightning-core/src/parser/mod.rs:1186-1190`

**Problem**: `CASE WHEN ... THEN ... END` produces an empty AST (`when_then: Vec::new()`). Every CASE expression evaluates to nothing — silent data loss for conditional logic.

- [ ] **1.2.1** `[P0]` Implement `parse_case_expression()`. Iterate `when_then_clause` and `else_clause` tokens from the Pest parse tree. Build the full `Expression::Case { expression, when_then, else_expression }` AST node.

### 1.3 COUNT(*) with GROUP BY Counts Wrong Column

**File**: `crates/lightning-core/src/planner/logical_plan.rs:649-674`

**Problem**: When `COUNT(*)` appears with GROUP BY, a dummy constant is NOT added to the aggregate args. The input_idx is always `0`, which points to the **first group-by column**. `COUNT(*)` is executed as `COUNT(group_by_col_0)`, which (a) excludes NULLs (COUNT(col) ≠ COUNT(*)), and (b) has no relation to actual row count.

- [ ] **1.3.1** `[P0]` Always add a dummy literal column for `COUNT(*)` regardless of GROUP BY presence. The literal should be a constant non-null value so it counts all rows.

### 1.4 Integer Division by Zero = Process Crash

**File**: `crates/lightning-core/src/processor/evaluator.rs:506-511`

**Problem**: `arrow::compute::kernels::numeric::div` on `Int64Array` panics on division by zero (or returns Err depending on Arrow version). Either way, the query fails. No explicit check before calling Arrow's div.

- [ ] **1.4.1** `[P0]` Add division-by-zero check before calling Arrow's integer div kernel. Return NULL for division by zero (SQL standard behavior).

### 1.5 Integer Arithmetic Overflow Wraps Silently

**File**: `crates/lightning-core/src/processor/evaluator.rs:507-509`

**Problem**: Arrow's `add`, `sub`, `mul` on Int64Array use two's complement wrapping. `i64::MAX + 1 = i64::MIN` silently.

- [ ] **1.5.1** `[P0]` Use checked arithmetic. When overflow is detected, either promote to Float64 or return an error. SQL standard behavior depends on dialect — choose explicit error for safety.

### 1.6 UInt64 Arithmetic Loses Precision via Float64 Cast

**File**: `crates/lightning-core/src/processor/evaluator.rs:184-193`

**Problem**: UInt64 values > 2^53 lose precision when cast to Float64 for arithmetic. `u64::MAX` becomes `18446744073709552000.0` (off by 15).

- [ ] **1.6.1** `[P0]` Add a dedicated UInt64 arithmetic path (same structure as the Int64 path at lines 497-533).

### 1.7 ExpressionVisitor / ExpressionRewriter Miss Subqueries, Map, Not

**Files**: `crates/lightning-core/src/planner/expression_visitor.rs:53,119`

**Problem**: `BoundExpression::Exists`, `CountSubquery`, `Map`, and `Not` branches are NOT recursively visited or rewritten by their respective traits. Any optimizer or analysis pass using these traits silently ignores subquery conditions, map values, and negated expressions.

- [ ] **1.7.1** `[P0]` Add explicit handler arms for `Exists`, `CountSubquery`, `Map`, and `Not` in both `ExpressionVisitor::visit()` and `ExpressionRewriter::rewrite()`.

---

## Section 2: Query Planner Wrong Results

### 2.1 `set_child` Loses Join/Union Right Side

**File**: `crates/lightning-core/src/planner/logical_plan.rs:209-211`

**Problem**: `LogicalOperator::set_child()` replaces only the left child of `Join` and `Union`. Optimizer rules using the fallback `set_child` approach (like `projection_pushdown.rs` catch-all at line 332) silently discard the right side of joins and unions.

- [ ] **2.1.1** `[P0]` `set_child` should either (a) return an error for multi-child operators, or (b) accept a child index. The current atomic-swap API is fundamentally unsafe for Join/Union. Audit all optimizer rules that use the catch-all `set_child` pattern.

### 2.2 DISTINCT Detection via String Contains

**File**: `crates/lightning-core/src/parser/mod.rs:659-660`

**Problem**: `let distinct = return_str.contains("DISTINCT")` — checks if the entire RETURN string contains the substring "DISTINCT". `RETURN 'DISTINCT' AS x` incorrectly sets `distinct = true`. Any property named like `n.distinct_city` also triggers this.

- [ ] **2.2.1** `[P0]` Use the Pest parse tree to check for the `DISTINCT` token directly (`Rule::DISTINCT` or `^"DISTINCT"`).

### 2.3 Macro Expansion Always Fails with Parameters

**File**: `crates/lightning-core/src/planner/binder.rs:1402-1435`

**Problem**: `bound_args` is always empty when the macro check runs (the generic argument binding happens AFTER). Any macro with >0 parameters triggers "Macro X expects N arguments, but 0 were provided." All parameterized macros are dead code.

- [ ] **2.3.1** `[P0]` Move the macro check after the generic argument binding loop (line 1438).

### 2.4 NEXTVAL Function Is Dead Code

**File**: `crates/lightning-core/src/planner/binder.rs:1394-1400`

**Problem**: Same root cause as 2.3 — `bound_args` is empty when the NEXTVAL check runs. `NEXTVAL('seq_name')` can never match.

- [ ] **2.4.1** `[P0]` Move the NEXTVAL check after the argument binding loop.

### 2.5 Duplicate Lambda Binding Code Blocks

**File**: `crates/lightning-core/src/planner/binder.rs:1343-1391,1465-1513`

**Problem**: Identical lambda binding logic for LIST_FILTER/LIST_TRANSFORM/LIST_ANY/etc. appears TWICE. The first block (1343-1391) checks `bound_args` (always empty) and never matches. Dead code.

- [ ] **2.5.1** `[P0]` Remove lines 1343-1391 entirely.

### 2.6 SET/REMOVE Property Assignments Use Wrong Column Index

**File**: `crates/lightning-core/src/planner/binder.rs:1688-1707`

**Problem**: `get_table_properties()` always returns `offset = 0`. For multi-table queries (`MATCH (a:Person)-[:KNOWS]->(b:Person) SET a.name = 'x'`), the property index is computed as `i + offset` where offset is always 0 instead of the variable's `column_offset`. The SET writes to the wrong column.

- [ ] **2.6.1** `[P0]` Look up `self.column_offsets.get(variable).copied().unwrap_or(0)` and use that as the offset. Reference how `PropertyLookup` binding does it at line 1282-1291.

### 2.7 Anonymous Nodes in MATCH Rejected

**File**: `crates/lightning-core/src/planner/binder.rs:917-919`

**Problem**: `MATCH (a)-[:REL]->()` is rejected with "MATCH destination node must have a variable". Anonymous destination nodes are essential for count queries and simple relationship navigation.

- [ ] **2.7.1** `[P1]` Generate an internal variable name (e.g., `_anon_n0`) when the destination node has no user-provided variable.

### 2.8 CREATE Relationship Uses Label Instead of Table Name

**File**: `crates/lightning-core/src/planner/binder.rs:1144`

**Problem**: `BoundRelPattern.table_name` is set to `rel_label` (e.g., `"REL"`) instead of `rel_table.name` (the catalog table name). Compare with MATCH bindings at line 962 which correctly use `rel_table.name.clone()`.

- [ ] **2.8.1** `[P1]` Change to `rel_table.name.clone()`.

### 2.9 Case Expression Type Inference Uses Only First WHEN

**File**: `crates/lightning-core/src/planner/binder.rs:1577-1579`

**Problem**: `CASE WHEN x > 1 THEN 1 WHEN x > 2 THEN 'hello' END` infers type from the first branch only (Int64). The second branch producing a String causes runtime type mismatch.

- [ ] **2.9.1** `[P1]` Compute the least-upper-bound type across all WHEN branches and ELSE.

---

## Section 3: DML Does Not Update Indexes (Wrong Query Results)

**Why here**: CREATE, SET, and DELETE modify data but leave FTS, vector, and PK hash indexes stale. Every subsequent search returns wrong results.

### 3.1 CREATE Skips FTS and Vector Index Updates

**Files**: `crates/lightning-core/src/processor/operators/dml.rs:122-123,151-171`

**Problem**: When `PhysicalCreate` has a child operator (subquery-based CREATE), `batch_append_rows` is called but NO FTS/vector/PK-hash index update is performed. The no-child path only updates the PK hash index — no FTS, no vector.

- [ ] **3.1.1** `[P0]` After `batch_append_rows`, add index update calls for FTS and vector indexes (same pattern as `PhysicalMerge` at lines 825-840).

### 3.2 SET Does Not Update Any Index

**File**: `crates/lightning-core/src/processor/operators/dml.rs:285-302`

**Problem**: `SET n.name = 'new'` updates the column value but does NOT update the PK hash index, FTS index, or vector index. If the PK changes, the hash index still points to the old value. If a string property changes, FTS still contains the old text.

- [ ] **3.2.1** `[P0]` After each property update in SET, check if the column has an associated index. If so, update it: remove old entry, insert new entry. For FTS: delete old document, index new document. For vector: update embedding. For PK hash: update key.

### 3.3 DELETE Does Not Clean Up Any Index

**File**: `crates/lightning-core/src/processor/operators/dml.rs:435-437`

**Problem**: DELETE sets all column values to Null but does NOT remove entries from PK hash index, FTS index, or vector index. Deleted entities continue to appear in search results.

- [ ] **3.3.1** `[P0]` After deletion, remove the row ID from all associated indexes: PK hash (`lookup`+deletion), FTS (`delete_term`), vector (`delete`), CSR (`delete_edge` from v1 §3.2.1).

### 3.4 COPY FROM Is Not Transactional

**File**: `crates/lightning-core/src/processor/operators/copy.rs:239-265`

**Problem**: `execute_copy_from` calls `table.bulk_append_batch()` but never writes to the undo buffer. A ROLLBACK after COPY FROM cannot revert the import. A CSV parse error mid-file leaves partially imported rows with no rollback.

- [ ] **3.4.1** `[P0]` Wrap COPY FROM in an explicit transaction. On parse error, rollback the transaction. Write undo buffer records for each batch.

---

## Section 4: Compression Codec Data Corruption

### 4.1 Dictionary Compress/Decompress Use Different Bit Widths (Wrong Results for ALL Dict Data)

**File**: `crates/lightning-core/src/storage/compression/dict.rs:58,99`

**Problem**: Compress uses `(dict_count as u64).leading_zeros()` (64-bit), decompress uses `(dict_count as u32).leading_zeros()` (32-bit). For `dict_count = 1`: compress bit_width = 1, decompress bit_width = max(64 - 31, 1) = 33. Every dictionary with ≥2 entries produces decompressed garbage.

- [ ] **4.1.1** `[P0]` Fix the bit_width calculation in `decompress_from_page` to match compress: use `(dict_count as u64).leading_zeros()` consistently.

### 4.2 ALP Encodes NaN as 0 and Infinity as i64::MAX/MIN

**File**: `crates/lightning-core/src/storage/compression/alp.rs:55-58`

**Problem**: `NaN.round() as i64 = 0` (Rust defined but corrupts NaN to 0.0). `Inf.round() as i64 = i64::MAX`, `NegInf = i64::MIN`. These large values overflow on decode, producing garbage.

- [ ] **4.2.1** `[P0]` Check for NaN and Infinity before encoding. Store NaN as a special sentinel, and encode Inf as ±f64::MAX explicitly with a flag. On decode, check for the sentinel.

### 4.3 BitPacker Panics on bit_width = 64

**File**: `crates/lightning-core/src/storage/compression/bitpacking.rs:48,83`

**Problem**: `1u64 << bit_width` panics when `bit_width == 64` (Rust's shift is checked in debug, UB in release). `calculate_bit_width()` in analyzer.rs returns 64 for `range = u64::MAX`, so this is reachable.

- [ ] **4.3.1** `[P0]` Handle `bit_width == 64` as a special case: either store raw values (no packing) or use `(1u64 << 63).wrapping_mul(2)` to construct the mask.

### 4.4 ALP Factor Index Always 0 (Compression Ratio Always Suboptimal)

**File**: `crates/lightning-core/src/storage/compression/alp.rs:55,94,106`

**Problem**: `encode_value` always called with `fac_idx=0`. The `FACTOR_ARR` / `FRAC_ARR` arrays (with 19 factor options) are dead code. ALP never achieves its claimed compression ratio.

- [ ] **4.4.1** `[P2]` Implement factor optimization: try all 19 factor combinations for the first page, pick the one that minimizes encoded range, store the best `fac_idx` alongside `exp_idx` in the compressed format.

### 4.5 BitPacker bit_width = 0 Silently Drops Data

**File**: `crates/lightning-core/src/storage/compression/bitpacking.rs:6`

**Problem**: When `bit_width = 0`, `pack_32` returns without writing anything. On unpack, all output values are 0. Any non-zero original values are silently corrupted.

- [ ] **4.5.1** `[P1]` When `bit_width = 0`, all values must be 0 (zero range). Add an assertion that all source values are 0, or skip the write and let the caller handle zero-filled pages.

### 4.6 FixedFrameOfReference and BooleanBitpacking Are Dead Code

**Files**: `crates/lightning-core/src/processor/physical_plan.rs:1763-1773`, `compression/mod.rs:138-151`

**Problem**: `FixedFrameOfReferenceAlg` is implemented (delta.rs) and the analyzer recommends it, but `column.rs:get_alg()` has no match arm for `CompressionType::FixedFrameOfReference`. The `_` wildcard silently converts it to `Uncompressed`. Similarly `BooleanBitpacking` has no implementation anywhere.

- [ ] **4.6.1** `[P1]` Either: (a) wire up the match arms in `column.rs:get_alg()`, or (b) remove the dead enum variants and the `delta.rs` implementation if it's never used.

### 4.7 Compression `optimize()` Sets metadata but Never Re-encodes

**File**: `crates/lightning-core/src/storage/column.rs:2037-2061`

**Problem**: When `optimize()` selects a compression algorithm and sets `compression_meta`, the existing page data is NOT re-encoded. It remains as raw uncompressed bytes. Reads after `optimize()` use the compression codec to interpret raw data as compressed → garbage.

- [ ] **4.7.1** `[P0]` After setting `compression_meta`, re-encode all existing pages using the selected compression algorithm. Or, defer compression to only apply to newly written pages (and document that existing data stays uncompressed).

---

## Section 5: Storage Engine Data Loss

### 5.1 Deadlock: List/Struct Batch Append

**File**: `crates/lightning-core/src/storage/column.rs:277,281-288,1294`

**Problem**: `batch_append_values` acquires `self.stats.write()`, then calls `append_value` which calls `append_plain_value` which tries to acquire `self.stats.write()` again. `parking_lot::RwLock` is not reentrant. The thread blocks forever. ANY batch insert to a List or Struct column hangs the database permanently.

- [ ] **5.1.1** `[P0]` Restructure to avoid reentrant lock acquisition. Options: (a) pass the stats lock guard through the call chain, (b) use a separate lock for the child-column stats, (c) refactor `append_value` for List/Struct to not re-acquire the parent's stats lock.

### 5.2 Bulk Write Paths Bypass WAL Entirely

**Files**: `crates/lightning-core/src/storage/column.rs:1367,1560-1563,1730-1732,1900`

**Problem**: The `skip_modified_rows` (bulk) paths write directly to files via `write_page()` and `write_bytes_at()` with NO `bm.log_page_update()`. These writes are NOT in the WAL. Crash recovery loses all bulk-inserted data.

- [ ] **5.2.1** `[P0]` Add WAL logging to all bulk write paths. Before each `write_page()` or `write_bytes_at()` call, call `bm.log_page_update()`. The existing `bm.create_new_version` + `log_page_update` pattern is the correct one to follow.

### 5.3 Overflow Strings Merges Lose Data in Row-Level OCC

**File**: `crates/lightning-core/src/transaction/transaction_manager.rs:19-24` (see also v1 §2.1)

**Problem**: `PageRowMod.row_data` is 64 bytes. Long strings >63 chars are stored as a 21-byte overflow pointer in the 64-byte column slot. This pointer fits, but the **overflow page content** is not versioned. Two concurrent transactions modifying different long-content entities on the same page: TxA writes a pointer to overflow page 5, TxB writes a pointer to overflow page 6. The merge applies TxB's pointer over TxA's pointer. TxA's overflow page 5 content is orphaned. On read, TxA's entity points to page 6 content.

- [ ] **5.3.1** `[P0]` Extend `PageRowMod` with overflow string content capture. When a row modification involves an overflow string, read the entire overflow content into an `overflow_data: Option<Vec<u8>>` buffer, and write it back during merge. This ensures concurrent overflow string modifications don't interfere.

### 5.4 `apply_page()` for Unknown file_id Silently No-Ops

**File**: `crates/lightning-core/src/storage/storage_manager.rs:1042-1047`

**Problem**: During WAL replay, if a page targets an unknown `file_id` (e.g., index file handles not tracked in `self.file_handles` — see 5.5), the function returns `Ok(())` without applying any data. WAL replay silently skips updates to index files.

- [ ] **5.4.1** `[P0]` Log a warning when `apply_page` encounters an unknown file_id. Register all index file handles in `self.file_handles` so WAL replay can find them (see 5.5).

### 5.5 Vector/FTS/CSR Index File Handles Not Tracked in `self.file_handles`

**File**: `crates/lightning-core/src/storage/storage_manager.rs:581-616`

**Problem**: `create_vector_index`, `create_fts_index`, and CSR creation methods create index file handles but never insert them into `self.file_handles`. This means: (a) WAL replay can't find them, (b) `sync_all_data_files()` never syncs them (data loss on crash), (c) free space tracking doesn't apply to them.

- [ ] **5.5.1** `[P0]` After creating each index file handle, insert it into `self.file_handles`:
  ```rust
  self.file_handles.insert(fh.file_id, Arc::clone(&fh));
  ```

### 5.6 `remove_table()` Leaks Index Metadata

**File**: `crates/lightning-core/src/storage/storage_manager.rs:857`

**Problem**: Removes from `self.indexes` but NOT from `self.fts_indexes`, `self.vector_indexes`, `self.fwd_csr`, `self.bwd_csr`. After dropping a table, orphaned index handles remain in memory. Re-creating the table with the same name finds stale entries.

- [ ] **5.6.1** `[P1]` Also remove from all four index collections in `remove_table()`.

---

## Section 6: Locking, Concurrency, Undefined Behavior

### 6.1 Lock Ordering Deadlock: checkpoint vs bulk_insert

**File**: `crates/lightning-core/src/lib.rs:482-498,1352-1376`

**Problem**: `checkpoint()` acquires `storage.read()` → `catalog.write()`. `bulk_insert_batch()` acquires `catalog.write()` → `storage.read()`. Two threads executing these simultaneously deadlock permanently.

- [ ] **6.1.1** `[P0]` Pick a consistent lock ordering. Either: (a) always acquire catalog lock FIRST, then storage lock, or (b) always acquire storage lock FIRST, then catalog lock. Fix the function that violates the chosen order.

### 6.2 TOCTOU Race on Explicit Transaction in execute()

**File**: `crates/lightning-core/src/lib.rs:1090-1142`

**Problem**: `execute()` clones the `Arc<Transaction>` from the mutex, drops the mutex, then uses the Arc. Between dropping the mutex and using the Arc, another thread can `commit()` or `rollback()`, rendering the transaction object stale. Use-after-commit on the same Transaction object.

- [ ] **6.2.1** `[P0]` Keep the transaction lock held during the entire query execution path, or use a state flag on Transaction to prevent double-use.

### 6.3 UB: *const → *mut Cast on Arc Data

**File**: `crates/lightning-core/src/lib.rs:448-454`

**Problem**: `Arc::as_ptr(&self.function_registry)` returns `*const`, cast to `*mut`, then `(*reg_mut).register_scalar(scalar)`. The `FunctionRegistry` uses a raw `HashMap` with no synchronization. `register_wasm_function` is `pub` — concurrent calls produce a data race = UB.

- [ ] **6.3.1** `[P0]` Wrap the `FunctionRegistry`'s inner HashMap in a `Mutex` or `RwLock`. Or use `Arc<RwLock<FunctionRegistry>>` and pass the lock through `register_scalar`.

### 6.4 Commit/Rollback Race With Concurrent begin()

**File**: `crates/lightning-core/src/lib.rs:723-743,945-957`

**Problem**: `commit()` takes `guard.take()` (sets `self.transaction` to None), drops the lock, then performs flush+commit without the lock. Another thread can call `begin()` (sees None), acquire a new transaction, and execute concurrently on the same Connection.

- [ ] **6.4.1** `[P1]` Hold the transaction lock during the entire commit/rollback operation. Only release after the commit record is written to the WAL.

### 6.5 Connection::execute_at() Time-Travel Bypasses Explicit Transaction

**File**: `crates/lightning-core/src/lib.rs:1107-1116`

**Problem**: When `snapshot_ts` is Some, a NEW transaction is created and used instead of the explicit one. Time-travel queries inside explicit transactions bypass the user's transaction.

- [ ] **6.5.1** `[P1]` Either error when execute_at is called inside an explicit transaction, or create a sub-transaction scoped to the snapshot timestamp.

---

## Section 7: Plan Cache and Query Pipeline Trust Issues

### 7.1 Plan Cache Has Very Low Hit Rate

**File**: `crates/lightning-core/src/lib.rs:36-38`

**Problem**: `normalize_query()` only replaces `'...'` with `'?'`. Whitespace differences, comments, and double-quoted identifiers produce different cache keys. The plan cache is mostly ineffective for real workloads.

- [ ] **7.1.1** `[P1]` Add whitespace normalization (collapse contiguous whitespace to single space). Add comment stripping. Normalize double-quoted identifiers the same way as single-quoted strings.

### 7.2 Checkpoint Silently Skips Catalog Save on Error

**File**: `crates/lightning-core/src/lib.rs:469-499`

**Problem**: After the buffer manager checkpoint succeeds, if the catalog save fails (e.g., disk full), the error is logged but `checkpoint()` returns `Ok`. On restart, the catalog metadata is stale — row counts don't match the data files.

- [ ] **7.2.1** `[P0]` Return the catalog save error from `checkpoint()`. The caller must decide whether to retry or abort. The current behavior silently creates an inconsistent on-disk state.

### 7.3 Bulk Insert Returns Success Even if Catalog Update Fails

**File**: `crates/lightning-core/src/lib.rs:1348-1378`

**Problem**: Transaction is committed at line 1348, then catalog is updated at lines 1352-1376. If the catalog update fails, the function returns `Ok(num_rows)` but the catalog doesn't reflect the inserted rows.

- [ ] **7.3.1** `[P1]` Return an error if the catalog update fails. The data is committed but the catalog is stale — the caller needs to know so they can retry the catalog save.

### 7.4 Read-Only Mode Is Not Enforced

**Files**: `crates/lightning-core/src/lib.rs:224`, `crates/lightning/src/database.rs:50-56`

**Problem**: `Database::open_read_only()` sets `read_only: true` in the config, but this flag is never checked. Every write operation succeeds on a "read-only" database.

- [ ] **7.4.1** `[P1]` Add `if self._config.read_only { return Err(...) }` guards at the top of `execute()`, `fast_insert()`, `bulk_insert_batch()`, `register_wasm_function()`, and `checkpoint()`.

### 7.5 Vacuum Thread Never Joined on Drop

**File**: `crates/lightning-core/src/lib.rs:245-279,398-409`

**Problem**: The vacuum thread handle is stored in `vacuum_handle: Option<JoinHandle<()>>` but never joined in `Drop`. On process exit, the vacuum thread may be mid-flush when the process terminates.

- [ ] **7.5.1** `[P1]` Join the vacuum thread in `Drop`. Use a shutdown flag + channel to signal the thread to exit, then join it with a timeout.

### 7.6 catalog.save_to_disk() No fsync After rename()

**File**: `crates/lightning-core/src/catalog/catalog.rs:334-341`

**Problem**: The shadow-write + rename pattern for catalog persistence needs `fsync` on the parent directory after `rename()`. Without it, a crash between rename and directory metadata flush loses the catalog.

- [ ] **7.6.1** `[P1]` After `rename()`, open the parent directory and call `sync_all()`. Same fix needed for header save in `lib.rs:504-509`.

### 7.7 LazyCatalog Clone Creates Independent Dirty Flag

**File**: `crates/lightning-core/src/catalog/lazy_catalog.rs:121-129`

**Problem**: Cloning a `LazyCatalog` copies the `dirty` atomic but shares the same `Arc<RwLock<Catalog>>`. If clone A writes and saves (clearing its dirty flag), clone B still has its own dirty flag set. This can cause double-saves or missed saves.

- [ ] **7.7.1** `[P1]` Either (a) prohibit cloning, or (b) make the dirty flag shared via `Arc<AtomicBool>` so all clones share the same dirty state.

### 7.8 `last_saved_tx_count` Increment Bug Causes Continuous Saves

**File**: `crates/lightning-core/src/catalog/lazy_catalog.rs:106-107`

**Problem**: `save_internal()` increments `last_saved_tx_count` by 1 from its own previous value, instead of setting it to the current transaction count. After ~1000 transactions, `save_if_needed()` triggers on EVERY write operation because `current_tx_count - 1 >= 1000` is always true.

- [ ] **7.8.1** `[P1]` Fix: `self.last_saved_tx_count.store(current_tx_count, Ordering::Release)` instead of `self.last_saved_tx_count.fetch_add(1, Ordering::Release)`.

### 7.9 Sequence Negative Increment Corrupts to u64::MAX

**File**: `crates/lightning-core/src/catalog/catalog.rs:381-389`

**Problem**: `seq.next_val += seq.increment as u64` — if `increment` is negative (e.g., -1), the `as u64` cast wraps to `u64::MAX`. The sequence value jumps to `next_val + u64::MAX`, instantly corrupting it.

- [ ] **7.9.1** `[P1]` Validate that increment is positive, or handle negative increments correctly using checked signed arithmetic.

---

## Section 8: Fusion Module and Extensions

### 8.1 Cypher Injection Throughout Fusion API

**Files**: `crates/lightning-core/src/fusion.rs:34,56-58,70-72,99-106,127-129,158-166,346-348,389-392`

**Problem**: Multiple functions build Cypher queries via string interpolation without proper escaping. `find_paths` uses `source_id.replace('\'', "")` (removes quotes — different from escaping), while `find_node_by_name` uses `sq()` (escapes). Inconsistent and exploitable.

- [ ] **8.1.1** `[P1]` Replace ALL string interpolation in Cypher query building with parameterized queries (`$param`). Every dynamically inserted value must go through the `params` HashMap.

### 8.2 PageRank Issues 1M+ Queries for 10K Nodes

**File**: `crates/lightning-core/src/fusion.rs:294-397`

**Problem**: `materialize_pagerank()` issues one MATCH query per node per iteration. For 10K nodes × 100 iterations = 1M queries. Execution takes hours. Line 393 uses `let _ = conn.execute(...)` — silently ignoring all errors.

- [ ] **8.2.1** `[P1]` Rewrite PageRank to: (a) load all node IDs in a single query, (b) compute scores in Rust (already done in `memory.rs:consolidate()`), (c) write results in bulk with a single `UNWIND` batch update.

### 8.3 Fusion add_observation() Never Stores parent_id

**File**: `crates/lightning-core/src/fusion.rs:157`

**Problem**: The `_parent_id: Option<&str>` parameter is accepted but never used in any query. Observations are created without parent relationships.

- [ ] **8.3.1** `[P2]` Either implement parent_id storage or remove the parameter.

### 8.4 Module Cohesion Detection Only Uses First Path Segment

**File**: `crates/lightning-core/src/fusion.rs:202-203`

**Problem**: Module detection uses `np[0]` — only the first path segment. `src/storage/index/csr.rs` becomes just `src`. Nested modules are flattened into top-level directories.

- [ ] **8.4.1** `[P2]` Use all path segments up to a configurable depth. Or use the full path as the module identifier.

---

## Section 9: Node.js Bindings

### 9.1 Streaming Corrupts All String Data

**File**: `crates/lightning-node/src/streaming.rs:40`

**Problem**: `format!("{:?}", col)` uses Rust's Debug trait on Arrow arrays. A `StringArray` with value `hello` produces `"hello"` (with literal quote characters in the output). A newline becomes `"hello\nworld"`. ALL string data in streaming query results is corrupted.

- [ ] **9.1.1** `[P1]` Replace Debug formatting with proper Arrow value extraction:
  ```rust
  match col.data_type() {
      DataType::Utf8 => col.as_any().downcast_ref::<StringArray>()
          .map(|a| a.value(row_idx).to_string()),
      DataType::Int64 => ...,
      // etc.
  }
  ```

### 9.2 Mutex Held Across Blocking Channel Receive

**File**: `crates/lightning-node/src/streaming.rs:23-25,72-74,97-99`

**Problem**: `Arc<Mutex<Receiver>>` — the Mutex guard is held while `rx.recv()` blocks. Two concurrent calls to `stream.next()` deadlock (second caller waits on Mutex while first is blocked on recv).

- [ ] **9.2.1** `[P1]` Restructure: clone the `Receiver` out of the Mutex, drop the guard, then call `recv()`. Or use `tokio::sync::Mutex` for async-friendly locking.

### 9.3 Silent u64 → i64 Truncation

**File**: `crates/lightning-node/src/types.rs:78-79,87-89`

**Problem**: `bytes_written: e.bytes_written as i64` — when `u64 > i64::MAX`, wraps to negative. Links_created, contradictions_found similarly truncated.

- [ ] **9.3.1** `[P1]` Change the JS-side types to accept `u64` (napi-rs supports `u64` via `#[napi]`). Or clamp to `i64::MAX` with a warning.

### 9.4 SystemConfig Not Configurable in Node.js

**File**: `crates/lightning-node/src/database.rs:15-18`

**Problem**: The Node.js binding hardcodes `SyncMode::Normal` and all defaults. Users cannot configure `buffer_pool_size`, `max_num_threads`, or `copy_base_dir`.

- [ ] **9.4.1** `[P1]` Add an `open_with_config(path, config)` factory that accepts a JsConfig object with all SystemConfig fields.

---

## Section 10: Python Bindings and Integrations

### 10.1 LangChain Integration Missing super().__init__()

**File**: `python/lightning/langchain.py:43`

**Problem**: `__init__` sets `self._memory` and `self._embedding` but never calls `super().__init__(**kwargs)`. LangChain's `VectorStore.__init__` stores kwargs and may perform other initialization. Uninitialized parent state breaks downstream LangChain pipelines.

- [ ] **10.1.1** `[P1]` Add `super().__init__(**kwargs)` as the first call in `__init__`.

### 10.2 LangChain entity_type=None Returns Default Wrong

**File**: `python/lightning/langchain.py:53`

**Problem**: `kwargs.get("entity_type", "document")` — if user passes `entity_type=None`, `.get()` returns `None` instead of `"document"`. The default only fires when the key is absent, not when the value is explicitly None.

- [ ] **10.2.1** `[P1]` Use a sentinel: `entity_type = kwargs.pop("entity_type", None) or "document"`.

### 10.3 LangChain delete() Fails on Single String ID

**File**: `python/lightning/langchain.py:95`

**Problem**: `def delete(self, ids: Optional[List[str]] = None)` — LangChain convention allows both `List[str]` and `str`. A single string ID would be iterated as characters.

- [ ] **10.3.1** `[P1]` Normalize to list: `ids = [ids] if isinstance(ids, str) else ids`.

### 10.4 LlamaIndex Returns Document Instead of TextNode

**File**: `python/lightning/llama_index.py:89`

**Problem**: Query results are wrapped in `Document` (ingestion type) instead of `TextNode` (retrieval type). This causes type errors in downstream postprocessors and synthesizers.

- [ ] **10.4.1** `[P1]` Change to `TextNode(text=..., metadata=...)`.

### 10.5 LlamaIndex Redundant None Handling

**File**: `python/lightning/llama_index.py:75-83`

**Problem**: `query_embedding = query.query_embedding or []` then later `list(query_embedding) if query_embedding is not None else []` — the second check is dead code since the first already converted None to [].

- [ ] **10.5.1** `[P2]` Clean up to use `query_embedding` directly after the first normalization.

---

## Section 11: Test Trustworthiness

**Why here**: Tests that don't assert anything, accept wrong results, or document known bugs create a false sense of security and waste CI time.

### 11.1 Torture Tests Use println! Instead of assert_eq! for All Invariant Checks

**Files**: `crates/lightning-core/tests/torture_test.rs:110-111,134-142,308-328,625-644` (and many more)

**Problem**: Throughout `torture_test.rs`, critical invariant checks are done via `println!` instead of `assert_eq!`. The test admits bugs exist ("BOOL columns have a known issue...") but does not fail when wrong results are detected. ~20 locations.

- [ ] **11.1.1** `[P1]` Replace every `println!` mismatch check with `assert_eq!` or `assert!`. If a known bug prevents strict validation, add a `#[ignore]` test for the bug and a passing test that validates what IS correct. Do NOT use print-and-continue.

### 11.2 Bugfix Tests Don't Assert

**File**: `crates/lightning-core/tests/bugfix_test.rs:84-87`

**Problem**: The string truncation bugfix test only prints warnings but never asserts. It's designed to detect a known bug but won't fail CI when the bug is present.

- [ ] **11.2.1** `[P1]` Add `assert_eq!(actual.len(), len)` to the test. If the bug is not fixed, `#[ignore]` the test.

### 11.3 Fuzz Tests Never Validate Result Correctness

**File**: `crates/lightning-core/tests/fuzz_test.rs:60-69,294-302`

**Problem**: All fuzz tests check only that queries don't crash (`match execute { Ok(_) => {}, Err(_) => { errors += 1 } }`). No query result is ever validated. An operator returning `2 + 2 = 5` would pass.

- [ ] **11.3.1** `[P1]` Add round-trip validation to fuzz tests. After each mutation, insert a known value, query it back, and assert the value is correct. Without this, the fuzzer is just a crash tester.

### 11.4 Expression Test Creates `:memory:` Directory in CWD

**File**: `crates/lightning-core/tests/expression_test.rs:21`

**Problem**: `Database::new(":memory:", config)` — the string `:memory:` is treated as a file system path. This creates a directory named `:memory:` in the CWD when tests run, leaving artifacts in the developer's working tree.

- [ ] **11.4.1** `[P1]` Use `tempfile::tempdir()` and resolve the path, like all other test files do.

### 11.5 ALL SHORTEST PATHS Test Asserts 0 Rows (Known Broken)

**File**: `crates/lightning-core/tests/comprehensive_test_4.rs:1074-1095`

**Problem**: `assert_eq!(total, 0, "ALL SHORTEST PATHS returns 0 rows (physical operator needs completion)")` — the test asserts that a feature is broken. Should be `#[ignore]` with a tracking issue.

- [ ] **11.5.1** `[P1]` Either implement allShortestPaths or mark the test as `#[ignore]` with a reference to the roadmap item.

### 11.6 Torture Concurrency Test Accepts Data Loss

**File**: `crates/lightning-core/tests/torture_test.rs:175-251`

**Problem**: The concurrent read-verify test prints "PASS" even with mismatches: `if mismatches > 0 { println!("WARN...") } else { println!("PASS") }`. A test that tolerates data loss is not a test.

- [ ] **11.6.1** `[P1]` Change to `assert_eq!(mismatches, 0)` and `assert_eq!(verified, total_expected)`.

### 11.7 Benchmark Suite Has No Performance Assertions

**Files**: `crates/lightning-core/tests/benchmark_suite.rs`, `perf_benchmark.rs`, `lightning_vs_sqlite.rs`

**Problem**: Benchmarks only print results. If a regression makes Lightning 100x slower, all benchmarks still pass. The SQLite comparison tests bulk_insert_batch (optimized) against SQLite row-by-row inserts (unoptimized) — not an apples-to-apples comparison.

- [ ] **11.7.1** `[P2]` Add performance regression assertions with a threshold (e.g., "assert! duration < 2 * baseline"). Use `env!("CARGO_MANIFEST_DIR")` to store baseline files. On the SQLite comparison: use SQLite batch inserts for a fair comparison.

### 11.8 210 Near-Identical Generated Tests

**File**: `crates/lightning-core/tests/comprehensive_test.rs:238-296`

**Problem**: `gen_tests!` creates 210 tests, each testing `RETURN {N}` with `assert_val!`. These are 99% identical, test only literal evaluation (simplest possible operation), and add ~2100 lines of expansion.

- [ ] **11.8.1** `[P2]` Replace with a single parameterized test or a loop. Use the freed test lines for meaningful coverage.

---

## Section 12: Storage Manager Gaps

### 12.1 Stats Double-Counted for Single-Row Appends

**File**: `crates/lightning-core/src/storage/storage_manager.rs:306,344`

**Problem**: `append_row` increments stats at line 344 per row, then `flush_buffer` increments by the full batch size again at line 306. Cardinality is always 2x for single-row appends going through the write buffer.

- [ ] **12.1.1** `[P1]` Fix: only increment stats in one place. Either (a) remove the line 344 increment and let flush_buffer handle all stats, or (b) don't increment in flush_buffer for buffered rows.

### 12.2 WAL Replay for Index Untracked w/o file_handles

**File**: `crates/lightning-core/src/storage/storage_manager.rs:1042-1047`

**Problem**: `apply_page()` silently returns `Ok(())` for unknown `file_id` during WAL replay. Since index handles aren't in `self.file_handles` (see 5.5), ALL WAL replay of index pages is silently skipped.

- [ ] **12.2.1** `[P0]` See 5.5.1 — must be fixed first for WAL replay to work.

### 12.3 remove_table() Leaves Orphan Trigram Worker Threads

**File**: `crates/lightning-core/src/storage/storage_manager.rs:854-858`

**Problem**: `trigram_workers` are per-column (created at line 749). `remove_table` does not drain/shutdown the workers. Orphaned threads continue running with references to potentially freed/overwritten data.

- [ ] **12.3.1** `[P1]` Send shutdown signal to all trigram workers before removing the table. Join or detach them.

### 12.4 FileHandle Collisions via Filename-Only Hashing

**File**: `crates/lightning-core/src/storage/storage_manager.rs:40-50`

**Problem**: `FileHandle::open` computes `file_id` by hashing only the filename (not the full path). Two columns with the same name in different tables (e.g., `table1_name.lbug` and `table2_name.lbug`) get the same `file_id`. The second insertion silently overwrites the first.

- [ ] **12.4.1** `[P1]` Include the full path in the file_id hash. Or use a monotonically incrementing counter instead of a hash-based ID.

---

## Section 13: Catalog and Connection

### 13.1 Catalog Properties Overwritten on Schema Re-create

**File**: `crates/lightning-core/src/catalog/catalog.rs:251-327`

**Problem**: When `CREATE NODE TABLE` is called on an existing table name, existing `num_rows` and `stats` are preserved but **properties are completely replaced**. No validation that existing data is compatible with the new schema.

- [ ] **13.1.1** `[P1]` Return an error if the table already exists. Only allow schema changes through explicit `ALTER TABLE`.

### 13.2 vacuum() Silently Ignores Rollback Failure

**File**: `crates/lightning-core/src/lib.rs:557-559`

**Problem**: If `rollback()` fails during VACUUM, the error is logged but the function returns `Ok(())`. The checkpoint at line 562 proceeds even though the transaction state is unresolved.

- [ ] **13.2.1** `[P1]` Return the rollback error. Do NOT proceed to checkpoint if rollback failed.

### 13.3 checkpoint() Drops Catalog Lock Before force_save

**File**: `crates/lightning-core/src/lib.rs:496-499`

**Problem**: `drop(cat)` releases the catalog write lock, then `self.catalog.force_save()` acquires it again. Between these, another thread can modify the catalog. The `force_save()` saves a potentially different state than what was synced from storage.

- [ ] **13.3.1** `[P2]` Keep the catalog write lock held through `force_save()`.

---

## Section 14: Compression Analysis Gaps

### 14.1 analyzer.analyze_column() Always Returns Uncompressed (Dead Code)

**File**: `crates/lightning-core/src/storage/compression/analyzer.rs:168-178`

**Problem**: `analyze_column()` always returns `CompressionType::Uncompressed`. It has ZERO callers in the entire codebase (confirmed by search). The actual compression selection is done inline in `column.rs:optimize()`.

- [ ] **14.1.1** `[P2]` Either implement `analyze_column()` properly and wire it up, or delete it to avoid confusion.

### 14.2 No Integration Tests for Compression Pipeline

**Files**: All compression test files (`alp_test.rs`, `bitpacking_test.rs`, `analyzer_test.rs`)

**Problem**: Every codec is tested in isolation or not at all. There is ZERO test coverage for the full pipeline: write column → analyze → compress → flush to page → read page → decompress → read value. Integration tests exist for uncompressed storage but compression is never exercised end-to-end.

- [ ] **14.2.1** `[P2]` Add integration tests that create tables with compressible data, run `optimize()`, and verify round-trip read correctness for each codec.

### 14.3 No Tests for NaN, Infinity, Edge Values

**Files**: `alp_test.rs`, `analyzer_test.rs`, `bitpacking_test.rs`

**Problem**: Zero coverage for NaN, ±Inf, ±0.0, subnormals, f64::MIN/MAX, bit_width=0, bit_width=64, empty input, single-element input.

- [ ] **14.3.1** `[P1]` Add edge case tests for every codec. Minimum: NaN round-trip, Inf round-trip, all-same-value, alternating values, empty input.

---

## Section 15: Operator Implementation Gaps

### 15.1 PhysicalScan: Filter on Non-Projected Column Crashes

**File**: `crates/lightning-core/src/processor/operators/scan.rs:275-276,545-577`

**Problem**: When `projected_idxs` is set AND a pushdown filter references a column NOT in the projected set, the filter evaluation fails with index-out-of-bounds on the projected batch. If `only_scan_filter_cols` is false (edge case), it falls through to a crash.

- [ ] **15.1.1** `[P1]` When a pushdown filter references columns not in the projected set, include those columns in the projection, or evaluate the filter before projection.

### 15.2 Parallel Sort Race Condition

**File**: `crates/lightning-core/src/processor/operators/sort.rs:48-64`

**Problem**: `num_active_collectors` is incremented under a READ lock. The write lock is acquired later. Between the two, another thread can finish, decrement to 0, and trigger the sort while this thread's data isn't yet added. Sorted result is missing that thread's data.

- [ ] **15.2.1** `[P1]` Increment `num_active_collectors` under the WRITE lock. Use a two-phase approach: (a) count active collectors under write lock, (b) release lock for collection, (c) re-acquire write lock for sorting only when count reaches 0.

### 15.3 UNION Recursive get_next Stack Overflow

**File**: `crates/lightning-core/src/processor/operators/union.rs:93-97,106-110`

**Problem**: When `deduplicate()` returns `None` (all rows in chunk are already seen), the code recursively calls `self.get_next()`. If all chunks are duplicates, this recurses infinitely and overflows the stack.

- [ ] **15.3.1** `[P1]` Replace recursion with a loop: `loop { match deduplicate() { None => continue, Some(chunk) => ... } }`.

### 15.4 Sort-Based Aggregate Corrupts Non-Numeric Types

**File**: `crates/lightning-core/src/processor/operators/aggregate.rs:233-239`

**Problem**: All aggregate values in the sort-based aggregation path are hardcoded to `DataType::Float64`. For string aggregates (COLLECT, GROUP_CONCAT), every value becomes `0.0` or NULL.

- [ ] **15.4.1** `[P1]` Use the aggregate function's own type instead of hardcoding Float64. The `AggregateFunction::finalize()` method returns the correct type — use it.

### 15.5 IndexScan Bypasses MVCC Visibility Check

**File**: `crates/lightning-core/src/processor/operators/index_scan.rs:71-101`

**Problem**: `PhysicalIndexScan` does NOT check row visibility against the transaction's snapshot timestamp. Deleted rows (by another committed transaction) are still visible. Compare with `scan.rs:420-466` which does extensive visibility filtering.

- [ ] **15.5.1** `[P1]` Add `version_info.get_visibility_mask()` check in index_scan, same pattern as scan.rs.

### 15.6 CountDistinct Uses Debug Format for Equality

**File**: `crates/lightning-core/src/processor/functions/aggregate_function.rs:143,152`

**Problem**: `CountDistinct.update()` uses `format!("{val:?}")` for deduplication. Two different values with the same Debug output are incorrectly counted as duplicates. This breaks COUNT(DISTINCT) for any type where Debug isn't injective.

- [ ] **15.6.1** `[P1]` Use actual value comparison via `Value`'s `PartialEq` trait. Store distinct values in a `HashSet<Value>` if `Value` implements `Hash`, or use `Vec` with `contains()`.

### 15.7 Partitioner Loses Multi-Batch Partitions

**File**: `crates/lightning-core/src/processor/operators/partitioner.rs:100-106`

**Problem**: `get_next()` uses `guard.pop()` which returns the LAST batch. If a partition accumulated multiple batches, only the last batch is ever returned. Subsequent calls return None. ALL data except the last batch per partition is lost.

- [ ] **15.7.1** `[P1]` Use `pop_front()` / `VecDeque` instead of `pop()`. Or drain all batches at once.

### 15.8 Evaluator Parameters Cannot Be List, Map, Node, Date

**File**: `crates/lightning-core/src/processor/evaluator.rs:461-463`

**Problem**: `BoundExpression::Parameter` only supports `Value::Number`, `Value::String`, `Value::Boolean`. Passing Node, List, Map, Date, Timestamp, or Null as a parameter produces an Internal error.

- [ ] **15.8.1** `[P1]` Add support for all Value variants in parameter handling.

### 15.9 from_arrow Silently Returns Null for Date/Timestamp/List/Struct

**File**: `crates/lightning-core/src/processor/arrow_utils.rs:292-293`

**Problem**: The catch-all at line 292 returns `Value::Null` for any unhandled Arrow type. Reading columns of type Date, Timestamp, List, Struct, Float32 silently returns NULL.

- [ ] **15.9.1** `[P1]` Add explicit match arms for Date32, Date64, Timestamp, List, Struct, Float32, and other common types.

---

## Section 16: Missing fsync and Durability Gaps

### 16.1 checkpoint() Header Save Missing fsync

**File**: `crates/lightning-core/src/lib.rs:504-509`

**Problem**: After `header.save(&header_path)?`, there's no fsync of the parent directory. (Already noted in 7.6 for catalog — same issue for header.)

- [ ] **16.1.1** `[P1]` Same fix as 7.6.1: after rename, open parent directory and `sync_all()`.

### 16.2 Bulk Append Writes Bypass WAL

**File**: `crates/lightning-core/src/storage/column.rs:1367,1560-1563,1730-1732` (same as 5.2)

- [ ] **16.2.1** `[P0]` See 5.2.1.

---

## Section 17: Thread/Resource Leaks

### 17.1 Trigram Worker Infinite Busy Loop on Channel Disconnect

**File**: `crates/lightning-core/src/storage/trigram_index_worker.rs:33-67`

**Problem**: On `RecvTimeoutError::Disconnected`, the worker flushes pending entries (correct) then loops back to `recv_timeout`, which immediately returns `Disconnected` again — infinite loop. The thread never terminates. Leaks a rayon thread permanently.

- [ ] **17.1.1** `[P1]` Handle `Disconnected` by breaking out of the loop: `Err(RecvTimeoutError::Disconnected) => break`.

### 17.2 Trigram Worker 1ms Poll Burns CPU

**File**: `crates/lightning-core/src/storage/trigram_index_worker.rs:33`

**Problem**: `recv_timeout(Duration::from_millis(1))` wakes up 1000 times/second when idle. For 10 string columns, 10 threads wake 10,000 times/second.

- [ ] **17.2.1** `[P1]` Increase to 50ms or use a blocking `recv()` with a mechanism to wake the thread when new work arrives.

### 17.3 TrigramIndex Shared Arc Accessed Without Synchronization

**Files**: `crates/lightning-core/src/storage/trigram_index_worker.rs:22`, `storage_manager.rs:746`

**Problem**: The same `Arc<TrigramIndex>` is passed to the worker thread AND stored in `table.trigram_indexes`. The `TrigramIndex` is not internally synchronized — the worker's `index.insert()` calls race with any direct reads from `table.trigram_indexes`.

- [ ] **17.3.1** `[P1]` Wrap the `TrigramIndex` in a `Mutex` or use atomics for internal state.

### 17.4 Arc<Mutex<Receiver>> Is Not the Right Abstraction

**File**: `crates/lightning-node/src/streaming.rs:13`

**Problem**: `crossbeam::Receiver` is `Send` but not `Sync`. Wrapping in `Arc<Mutex<>>` makes it `Sync` at the cost of the deadlock issue (see 9.2). The channel should be consumed once, not shared.

- [ ] **17.4.1** `[P2]` Use a single-owner design: the stream consumer owns the channel and hands it off via a oneshot or async task.

---

## Section 18: Cross-Cutting Design Issues

### 18.1 PhysicalOperator Trait Lacks is_parallel_safe()

**Files**: All operator files (v1 §1.1.2 flags this but it warrants its own entry)

**Problem**: The trait has no method to indicate whether an operator can be safely cloned and executed in parallel on partitioned data. Without this, the scheduler cannot know which operators are safe to parallelize.

- [ ] **18.1.1** `[P1]` Add `fn is_parallel_safe(&self) -> bool` to the `PhysicalOperator` trait with a default implementation returning `false`.

### 18.2 PhysicalOperator Trait Lacks output_schema()

**Files**: All operator files

**Problem**: No operator exposes its output schema via a trait method. Schema is reconstructed internally but never exposed to the planner or optimizer after plan creation.

- [ ] **18.2.1** `[P2]` Add `fn output_schema(&self) -> Option<SchemaRef>` to the trait.

### 18.3 Undo Buffer: DDL Rollback Doesn't Clean Up Files

**File**: `crates/lightning-core/src/storage/undo_buffer.rs:84-91`

**Problem**: On rollback of CREATE TABLE, `StorageManager::remove_table()` only removes from in-memory HashMaps — it does NOT delete any data files. On retry, `FileHandle::open()` with `create(true)` (no truncate) reopens the stale files. The new table inherits corrupted/partial data from the aborted transaction.

- [ ] **18.3.1** `[P1]` On DDL rollback, either (a) delete all data files for the table, or (b) keep a before-image of the table metadata and restore it.

### 18.4 Undo Buffer: DropConstraint/DropIndex Rollback Is a No-Op

**File**: `crates/lightning-core/src/storage/undo_buffer.rs:198-201,212-214`

**Problem**: `DropConstraint` and `DropIndex` rollback log a warning and do nothing. Dropped constraints and indexes are permanently lost even after rollback.

- [ ] **18.4.1** `[P1]` Implement rollback for both: restore the constraint/index metadata from the saved before-image in the UndoRecord.

### 18.5 Multiple Operators Use .unwrap() on downcast_ref

**Files**: `hash_join.rs:167,179,308,317`, `scan.rs:327,507`, `semi_masker.rs:42`, and many more

**Problem**: Multiple operators use `.unwrap()` after `downcast_ref` for type coercion. If the runtime type doesn't match (planner/optimizer bug), the database crashes with a panic instead of returning a clean error.

- [ ] **18.5.1** `[P1]` Replace all `.unwrap()` calls on `downcast_ref` with proper error handling: `.ok_or_else(|| LightningError::Internal("expected TypeX, got TypeY"))?`.

### 18.6 cross_join.rs Materializes All Rows (OOM Risk)

**File**: `crates/lightning-core/src/processor/operators/cross_join.rs:138-140`

**Problem**: Unlike `hash_join.rs` which uses index-based `take()` (O(n) memory), `PhysicalCrossJoin` materializes all cross-product rows into `Vec<Vec<Value>>`. A 10K × 10K cross join creates 100M Value instances — easy OOM.

- [ ] **18.6.1** `[P2]` Use index-based approach: build the left-side index, iterate the right side, and take rows by index without copying values.

---

## Summary: New Items by Tier

| Tier | Section | Focus | Count |
|------|---------|-------|-------|
| **Tier 1** (silent corruption) | §1-6 | Arithmetic, planner, DML indexes, compression, storage data loss, concurrency | ~50 |
| **Tier 2** (trust erosion) | §7-10 | Plan cache, fusion, Node.js, Python, checkpoint gaps | ~25 |
| **Tier 3** (correctness gaps) | §11-14 | Test trustworthiness, storage gaps, catalog, compression analysis | ~30 |
| **Tier 4** (hardening) | §15-18 | Operator implementation, durability, resource leaks, design | ~30 |

**Total new items: ~135** (top-priority actionable items, consolidated from ~299 raw findings).
