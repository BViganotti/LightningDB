# Production-Grade Engineering Plan

## Item 1: Bug-Sweep the Cypher Parser

### Current State

- 155-line PEG grammar (`cypher.pest`) covering 19 statement types, 11 expression types, 4 literals
- 1564-line parser implementation (`mod.rs`) with pre-processing (`strip_modifiers`/`inject_modifiers`)
- 40 test functions, ~25 untested feature areas
- Key known issues: dead AST variants, f64 number precision, single ORDER BY item, number-only SKIP/LIMIT, no escaped identifiers

### Phase 1: Fix Existing Bugs (CRITICAL — 2 weeks)

| # | Task | File | Effort | Impact |
|---|------|------|--------|--------|
| 1.1 | **Fix IF NOT EXISTS for REL TABLE** — hardcoded to false on line 367 of mod.rs. Needs to read `if_not_exists` from grammar | `parser/mod.rs:367` | 1 day | DDL correctness |
| 1.2 | **Fix inject_modifiers for standalone RETURN** — currently only mutates `Statement::Match`. If a query is only `RETURN expr ORDER BY x` (no MATCH), ORDER BY is silently dropped. Need to also handle `StandaloneReturn` or the `With+Return` path | `parser/mod.rs:202-237` | 2 days | Query correctness |
| 1.3 | **Fix composite primary keys** — grammar only allows single column. Change `primary_key_def` to `"(" ~ variable ~ ("," ~ variable)* ")"`, update binder and planner | `cypher.pest:39`, `parser/mod.rs:324-361`, `binder.rs`, `logical_plan.rs`, `scan.rs` | 3 days | Schema flexibility |
| 1.4 | **Fix CREATE TABLE with escaped identifiers** — `table_name`, `label_name`, `variable` don't allow backtick-quoted names. Add: `escaped_name = @{ "`" ~ (!"`" ~ ANY)* ~ "`" }` and include as alternative in identifier rules. Update all references | `cypher.pest:40,78,155` | 2 days | Real-world DDL |
| 1.5 | **Fix strip_modifiers multi-item ORDER BY** — currently only captures one expression. `ORDER BY a, b, c` silently drops `b, c`. Change to extract all comma-separated items up to SKIP/LIMIT | `parser/mod.rs:119-155` | 2 days | Sort correctness |
| 1.6 | **Fix number precision** — all numbers parsed as f64, lossy beyond 2^53. Change `number_literal` to try i64 first, fall back to f64. Update `ast.rs:338-339`, `parser/mod.rs:1399` | `parser/mod.rs:1399`, `ast.rs:338-339` | 1 day | Numeric correctness |
| 1.7 | **Fix escape sequences in string literals** — `string_literal` doesn't handle `\n`, `\t`, `\\`, `\"`, `\'`. Add `EscapedChar` grammar rule | `cypher.pest:149`, `parser/mod.rs:1385-1400` | 1 day | String correctness |
| 1.8 | **Fix SKIP/LIMIT parameter support** — only accept `number_literal`, not `$param`. Change grammar to accept `parameter`, update `strip_modifiers` to pass param names through, update binder/planner/processor | `cypher.pest:94-95`, `parser/mod.rs:149-186`, `binder.rs:227-233`, `logical_plan.rs:883-911`, `limit_skip.rs` | 3 days | Query flexibility |

### Phase 2: Wire Dead AST to Grammar (MEDIUM — 1 week)

| # | Task | File | Effort | Impact |
|---|------|------|--------|--------|
| 2.1 | **CREATE VECTOR INDEX parser rule** — `CreateVectorIndex` exists in AST but no PEG rule. Grammar: `create_vector_index = { ^"CREATE" ~ ^"VECTOR" ~ ^"INDEX" ~ variable ~ ^"ON" ~ table_name ~ "(" ~ variable ~ ")" ~ ^"TYPE" ~ vector_index_type ~ ^"DIMENSION" ~ number_literal }`. Wire to binder/planner | `cypher.pest`, `parser/mod.rs`, `binder.rs`, `logical_plan.rs` | 2 days | Vector usability |
| 2.2 | **CREATE FULLTEXT INDEX parser rule** — `CreateFtsIndex` exists in AST but no PEG rule. Grammar: `create_fts_index = { ^"CREATE" ~ ^"FULLTEXT" ~ ^"INDEX" ~ variable ~ ^"ON" ~ table_name ~ "(" ~ variable ~ ("," ~ variable)* ~ ")" }`. Wire to binder/planner | `cypher.pest`, `parser/mod.rs`, `binder.rs`, `logical_plan.rs` | 2 days | FTS usability |
| 2.3 | **CREATE SEQUENCE parser rule** — `CreateSequence` exists in AST but no PEG rule. Grammar: `create_sequence = { ^"CREATE" ~ ^"SEQUENCE" ~ variable ~ (^"START" ~ ^"WITH" ~ number_literal)? ~ (^"INCREMENT" ~ ^"BY" ~ number_literal)? }`. Wire to binder/planner | `cypher.pest`, `parser/mod.rs`, `binder.rs`, `logical_plan.rs` | 2 days | Feature completeness |
| 2.4 | **CREATE MACRO parser rule** — `CreateMacro` exists in AST but no PEG rule. Grammar: `create_macro = { ^"CREATE" ~ ^"MACRO" ~ variable ~ "(" ~ variable ~ ("," ~ variable)* ~ ")" ~ ^"AS" ~ ^"$$" ~ (!^"$$" ~ ANY)* ~ ^"$$" }`. Wire to binder/planner | `cypher.pest`, `parser/mod.rs`, `binder.rs`, `logical_plan.rs` | 2 days | Feature completeness |
| 2.5 | **Subquery clause parser rule** — `Clause::Subquery(Box<Query>)` exists in AST but no PEG rule. Add `subquery_clause = { ^"CALL" ~ "{" ~ query ~ "}" }` as a clause alternative | `cypher.pest`, `parser/mod.rs` `parse_clause` | 1 day | Nested query support |

### Phase 3: Add Missing Expression Features (MEDIUM — 1 week)

| # | Task | File | Effort | Impact |
|---|------|------|--------|--------|
| 3.1 | **Regex `=~` operator** — Grammar: `regex_match_op = { "=~" }`. Wire to `Expression::Function("REGEX_MATCH", ...)` similar to CONTAINS | `cypher.pest:116-122`, `parser/mod.rs:1050-1064` | 1 day | Cypher completeness |
| 3.2 | **Bitwise operators** `\|`, `&`, `<<`, `>>` — Add to expression precedence chain | `cypher.pest`, `parser/mod.rs` | 2 days | Cypher completeness |
| 3.3 | **Power operator `^`** — Add to factor rule | `cypher.pest`, `parser/mod.rs` | 1 day | Cypher completeness |
| 3.4 | **Path patterns as expressions** — `shortestPath()`, `allShortestPaths()` exist in ANTLR grammar but not PEG. Determine if needed | `cypher.pest` | 2 days | GDS support |
| 3.5 | **Schema-qualified names** — `schema.table.column` notation. Grammar: `schema_qualified_name = { variable ~ "." ~ variable ~ ("." ~ variable)? }` | `cypher.pest`, `parser/mod.rs` | 1 day | Multi-schema |

### Phase 4: Add Comprehensive Tests (HIGH — 1 week)

| # | Task | Coverage Area | Tests |
|---|------|---------------|-------|
| 4.1 | EXPLAIN / PROFILE parsing | `EXPLAIN MATCH ...`, `PROFILE MATCH ...` | 4 |
| 4.2 | UNION / UNION ALL | `... UNION ...`, `... UNION ALL ...` | 4 |
| 4.3 | CREATE INDEX / DROP INDEX / CREATE CONSTRAINT / DROP CONSTRAINT | All DDL parsing | 8 |
| 4.4 | ALTER TABLE (all 4 operations) | ADD COLUMN, DROP COLUMN, RENAME TO, RENAME COLUMN | 4 |
| 4.5 | COPY FROM / COPY TO | Basic bulk operations | 4 |
| 4.6 | CALL procedure | `CALL proc()`, `CALL proc() YIELD a, b` | 4 |
| 4.7 | CASE expression | Simple CASE, searched CASE, with ELSE, without ELSE | 6 |
| 4.8 | CAST / EXTRACT | `CAST(x AS type)`, `EXTRACT(field FROM x)` | 4 |
| 4.9 | EXISTS / COUNT subquery | `EXISTS { MATCH ... }`, `COUNT { MATCH ... }` | 4 |
| 4.10 | List subscript / slice | `x[0]`, `x[-1]`, `x[0..3]`, `x[0..]` | 6 |
| 4.11 | List quantifiers | `ALL(x IN list WHERE ...)`, `ANY(...)`, `NONE(...)`, `SINGLE(...)` | 4 |
| 4.12 | Map literal | `{k: v}`, nested maps | 3 |
| 4.13 | IS NULL / IS NOT NULL | `WHERE x IS NULL`, `WHERE x IS NOT NULL` | 2 |
| 4.14 | Variable-length rel bounds | `-[*]-`, `-[*1..3]->`, `-[*..5]->` | 6 |
| 4.15 | IF NOT EXISTS / IF EXISTS | All DDL with existence checks | 6 |
| 4.16 | Error handling / parse failures | Invalid queries, syntax errors, edge cases | 20 |
| 4.17 | REMOVE clause | `REMOVE n.prop` | 2 |
| 4.18 | SET map assignment | `SET n += {k: v}`, `SET n = {k: v}` | 3 |
| 4.19 | Composite primary keys | Multi-column PK in CREATE NODE TABLE | 3 |
| 4.20 | Empty patterns, edge cases | `()`, `-->`, `()-[]-()`, multiple consecutive MATCH | 8 |

**Total new tests: ~105**

### Phase 5: Fuzz Testing (ONGOING — 2 weeks)

| # | Task | Effort |
|---|------|--------|
| 5.1 | Build PEG-aware grammar fuzzer that generates valid random Cypher queries | 3 days |
| 5.2 | Run against parser, assert no panics or infinite loops | 1 day |
| 5.3 | Run against full pipeline (parse → bind → plan), assert no crashes | 2 days |
| 5.4 | Add to CI with nightly runs | 1 day |
| 5.5 | Integrate cargo-fuzz (libfuzzer) for structured fuzzing of the parser | 3 days |

---

## Item 2: Productionize Sort & Aggregate Operators

### Critical Bug Fixes (WEEK 1)

| # | Task | File | Description |
|---|------|------|-------------|
| 2.1 | **Fix COUNT(*) returning 0** — `aggregate_function.rs` `Count::update_vector` counts `len() - null_count()`. When called with a `NullArray` (used as dummy for `count(*)`), `null_count() == len()` so result is always 0. Replace with `CountStar` which counts rows without inspecting values. The `CountStar` struct already exists in `aggregate_function.rs` but is never instantiated by the aggregate operator. In `aggregate.rs` lines 108-131, when a `NullArray` is detected (0 columns), use `CountStar` instead of `Count` | `aggregate_function.rs:9-18`, `aggregate.rs:116-130` | 1 day |
| 2.2 | **Wire PhysicalTopK to the planner** — `PhysicalTopK` exists in `topk.rs` (159 lines) but is never used. The physical planner decomposes `LogicalOperator::TopK` into `PhysicalSort + PhysicalLimit` (physical_plan.rs:340-351). Change to instantiate `PhysicalTopK` directly. This optimizes `ORDER BY ... LIMIT K` from O(N log N) to O(N + K log K) for K << N | `physical_plan.rs:340-351` | 1 day |
| 2.3 | **Fix sort-based aggregation overwriting hash results** — In `aggregate.rs:272`, when sort-based mode triggers mid-execution, groups from the hash phase are overwritten by `groups.insert()`. Change to `groups.entry(key).or_insert_with(|| ...)` so hash results are preserved | `aggregate.rs:270-275` | 1 day |
| 2.4 | **Fix final group sort by debug string** — `aggregate.rs:326-330` sorts groups by `format!("{:?}", key)` which is lexicographic on debug representation (e.g., `Number(100)` before `Number(50)` since "1" < "5"). Change to use actual `Value::partial_cmp` at line 328: `.sort_by(|(k1, _), (k2, _)| k1.partial_cmp(k2).unwrap_or(std::cmp::Ordering::Equal))` | `aggregate.rs:326-330` | 1 day |
| 2.5 | **Fix PhysicalSkip fetch_add race** — `limit_skip.rs:104-106` uses `fetch_add` to track skipped rows but the `fetch_add` is not atomic with the batch consumption. Two concurrent calls could both skip the same batch. Change to use a `Mutex<()>` for mutual exclusion (like `PhysicalLimit` does) | `limit_skip.rs:90-125` | 1 day |

### Performance Optimization (WEEK 2)

| # | Task | File | Description |
|---|------|------|-------------|
| 2.6 | **Sort: limit memory via streaming top-K** — Current `PhysicalSort` concatenates ALL input into one batch before sorting. For `ORDER BY ... LIMIT K`, use a bounded heap: track top-K rows as they arrive, emit sorted result at end. This reduces memory from O(N) to O(K). Implement as a new `StreamingTopK` operator or modify `PhysicalTopK` to work in streaming mode | `sort.rs`, `topk.rs` | 3 days |
| 2.7 | **Aggregate: replace per-group filter with sort** — Current hash-based `aggregate.rs` builds boolean mask and calls `arrow::compute::filter` per group, O(num_groups × num_rows). Replace with: sort group-by columns, walk sorted indices to find contiguous group ranges, use `arrow::compute::take` per range. This is O(N log N + N) instead of O(G × N) | `aggregate.rs:180-210` | 3 days |
| 2.8 | **Aggregate: use CountStar instead of NullArray** — Change `aggregate.rs:116-130` to instantiate `CountStar` when the aggregate function is Count and there's no argument expression. The `CountStar` struct already exists and correctly counts all rows | `aggregate.rs:108-131`, `aggregate_function.rs:9-18` | 1 day |
| 2.9 | **Sort: remove unnecessary Condvar infrastructure** — `sort.rs` uses a `Condvar` + `Mutex` + `AtomicBool` for single-threaded sort synchronization. Since `is_parallel_safe()` returns `false`, this complexity is wasted. Simplify: drain child, sort, store result, return in next call. Remove `sort_started`, `sort_done`, `Condvar` | `sort.rs` | 1 day |
| 2.10 | **Limit: remove unnecessary Mutex** — `limit_skip.rs:27` uses `Mutex<()>` with `AtomicUsize`. The `AtomicUsize` CAS is sufficient for correct concurrent behavior; the Mutex adds contention. Remove it | `limit_skip.rs:27,59-70` | 1 day |

### Dead Code Cleanup (WEEK 3)

| # | Task | File | Description |
|---|------|------|-------------|
| 2.11 | **Remove or implement NWayMerge** — `nway_merge.rs` is 220 lines of unreachable code. Either: (a) **Remove** the file and clean up `sort.rs:217-233` `try_parallelize()`, or (b) **Implement** proper parallel sort by setting `is_parallel_safe()` to `true` and fixing the `compare_values` function to respect `SortOptions` (descending, nulls_first) | `nway_merge.rs`, `sort.rs:217-233` | 2 days |
| 2.12 | **Clean up clone_box concurrency bug** — `sort.rs:235-243` `clone_box()` shares the same `Arc<RwLock<SharedSort>>` between clones, causing state corruption. Fix: create fresh `SharedSort` on clone (like `try_parallelize` does) | `sort.rs:235-243` | 1 day |
| 2.13 | **Reduce warnings: remove dead code** — 72 warnings in `lightning-core`. Many are unused variables, unused imports, dead code in `nway_merge.rs`, `topk.rs`, and other files. Run `cargo fix --lib -p lightning-core` as a start, then manually review remaining warnings | All files | 2 days |

### Comprehensive Tests (WEEK 3-4)

| # | Task | Tests |
|---|------|-------|
| 2.14 | **Sort tests** — ORDER BY ASC/DESC with all types (int, float, string, date, timestamp, null), multi-column sort, SKIP+LIMIT+ORDER BY, LIMIT 1, empty table, null-heavy data, duplicate values | ~25 |
| 2.15 | **TopK tests** — Same as sort but specifically testing the PhysicalTopK path (small LIMIT, large LIMIT, edge cases) | ~10 |
| 2.16 | **Aggregate tests** — COUNT(*) (critical), COUNT(col), COUNT(DISTINCT), GROUP BY all types, GROUP BY multiple columns, GROUP BY with NULL keys, sort-based aggregation path (100K+ groups), hash-based with early switch, aggregate + ORDER BY, HAVING | ~20 |
| 2.17 | **Limit/Skip tests** — LIMIT 0, LIMIT with parallel execution, SKIP past end, SKIP+LIMIT across multiple batches | ~10 |
| 2.18 | **Parallel sort/aggregate tests** — Multi-threaded execution with assertion that results match single-threaded | ~10 |

---

## Item 4: Add EXPLAIN Query Plan Introspection

### Current State

- `EXPLAIN` and `PROFILE` are parsed and carried through the pipeline (`query.is_explain`, `query.is_profile`)
- `LogicalOperator::Explain(Box<child>)` and `LogicalOperator::Profile(Box<child>)` exist
- `PhysicalPlanner` maps `Explain` → `PhysicalProfile::with_explain_analyze()` which returns a single row: `"EXPLAIN ANALYZE: total rows: N, elapsed: D"`
- The logical plan has 40+ operator variants with rich metadata; physical operators have `output_schema()`
- Plan caching exists (`physical_plan_cache`) — plans must still be displayed for EXPLAIN even if cached

### Phase 1: Plan Rendering Infrastructure (WEEK 1)

| # | Task | File | Description |
|---|------|------|-------------|
| 4.1 | **Create ExplainPlan struct** — A structured representation of the plan tree for rendering. Each node has: type name, parameters (filter expr, sort keys, limit, join cond, etc.), children, schema, estimated cardinality | New file: `planner/explain.rs` | 1 day |
| 4.2 | **Implement LogicalPlan explain** — Recursively walk `LogicalOperator` tree, rendering each node with its parameters. Format as indented text tree. For each operator: | `explain.rs` | 2 days |
| | • `Scan(name, var, ...)` → `Scan | table: Person, alias: p, projected: [id, name], pushdown: [age > 25]` | |
| | • `Filter(child, expr)` → `Filter | expr: p.age > 25` | |
| | • `Sort(child, items)` → `Sort | keys: [p.name DESC, p.age ASC]` | |
| | • `Aggregate { group_by, aggregates }` → `Aggregate | group_by: [p.name], agg: [count(*), avg(p.age)]` | |
| | • `Limit/Join/Projection` → similar parameter display | |
| 4.3 | **Implement PhysicalPlan explain** — Walk `PhysicalOperator` after physical planning. Includes column index mappings, schema info from `output_schema()`, physical operator type names | `explain.rs` | 2 days |
| 4.4 | **Add `output_schema()` to all operators** — Currently returns `None` for most operators. Implement properly so EXPLAIN can show intermediate schemas | All operator files | 1 day |
| 4.5 | **Render plan to string** — Format as a readable tree: | `explain.rs` | 1 day |
| | ``` | |
| | Projection (schema: id, name, cnt) | |
| | ├── Limit (count: 10) | |
| | │   └── Skip (count: 5) | |
| | │       └── Sort (keys: [2 DESC]) | |
| | │           └── Aggregate (group_by: [0], agg: [Count(1)]) | |
| | │               └── Scan (table: Person, alias: p) | |
| | ``` | |

### Phase 2: PhysicalExplain Operator (WEEK 2)

| # | Task | File | Description |
|---|------|------|-------------|
| 4.6 | **Create PhysicalExplain operator** — New physical operator that captures the logical plan text and physical plan text at construction time. `get_next()` returns a single `DataChunk` with the plan text. No child execution needed | `processor/operators/explain.rs` | 1 day |
| 4.7 | **Create PhysicalExplainAnalyze operator** — Same as PhysicalExplain but EXECUTES the child plan first, collecting per-operator metrics (row count, time, batches). Returns plan + metrics as a single `DataChunk` | `processor/operators/explain.rs` | 2 days |
| 4.8 | **Update PhysicalPlanner** — Map `LogicalOperator::Explain(child)` → `PhysicalExplain` (not `PhysicalProfile`). Pass the logical plan text (serialized during LogicalPlanner) and the physical plan text (serialized during PhysicalPlanner) | `physical_plan.rs:353-359` | 1 day |
| 4.9 | **Wire EXPLAIN into HTTP** — The `/v1/query` endpoint already returns `QueryResult { columns, rows }`. EXPLAIN should return a single row with `{ plan: "..." }`. The client SDK needs no changes since it just gets a QueryResult | `routes/query.rs` | 1 day |

### Phase 3: Metrics Collection (WEEK 3)

| # | Task | File | Description |
|---|------|------|-------------|
| 4.10 | **Add timing wrapper operator** — A lightweight wrapper that wraps any physical operator and measures: wall-clock time, row count, batches processed, memory allocated. Compose around root operator for EXPLAIN ANALYZE | `processor/operators/metrics.rs` | 2 days |
| 4.11 | **Add cardinality estimation** — The optimizer's `JoinReordering` rule already has a cardinality estimator. Expose `estimated_cardinality()` on each `LogicalOperator` variant. Start with simple heuristics (scan = table row count, filter *= 0.1, join *= 0.3, limit = value) | `planner/logical_plan.rs`, `planner/optimizer/cardinality.rs` | 2 days |
| 4.12 | **Add "analyze" table statistics** — For accurate cardinality, store per-table row counts and column statistics. `ANALYZE TABLE Person` command updates stats. `StorageManager` tracks `table_row_count`. Column histograms for distribution estimation | `storage/storage_manager.rs`, `parser/` | 3 days |

### Phase 4: Integration Tests (WEEK 3-4)

| # | Task | Tests |
|---|------|-------|
| 4.13 | EXPLAIN returns valid tree with correct operator names | 5 |
| 4.14 | EXPLAIN shows exact filter expressions | 3 |
| 4.15 | EXPLAIN shows sort keys with direction | 3 |
| 4.16 | EXPLAIN shows aggregate functions and group-by cols | 3 |
| 4.17 | EXPLAIN shows join conditions | 2 |
| 4.18 | EXPLAIN shows limit/skip values | 2 |
| 4.19 | EXPLAIN ANALYZE shows metrics (time, rows, batches) | 3 |
| 4.20 | EXPLAIN with UNION, WITH, subqueries | 3 |
| 4.21 | EXPLAIN with DDL (CREATE TABLE, etc.) | 2 |
| 4.22 | EXPLAIN error handling (invalid queries, schema errors) | 2 |

### Output Example

```
$ MATCH (p:Person) WHERE p.age > 25
  RETURN p.name, count(*) AS cnt
  ORDER BY cnt DESC LIMIT 10

Projection (rows: 10, cols: [name, cnt])
├── Limit (count: 10)
│   └── Sort (keys: [1 DESC])
│       └── Aggregate (group_by: [0], agg: [Count(*)])
│           └── Projection (cols: [name])
│               └── Filter (expr: p.age > 25)
│                   └── Scan (table: Person, alias: p, rows: 1000)
```

### Total Effort Summary

| Item | Phase | Duration |
|------|-------|----------|
| 1: Parser | Fix bugs | 2 weeks |
| | Wire dead AST | 1 week |
| | Expression features | 1 week |
| | Tests | 1 week |
| | Fuzzing | 2 weeks |
| **Subtotal** | | **~7 weeks** |
| 2: Sort/Agg | Critical fixes | 1 week |
| | Performance | 1 week |
| | Dead code | 1 week |
| | Tests | 1 week |
| **Subtotal** | | **~4 weeks** |
| 4: EXPLAIN | Plan rendering | 2 weeks |
| | Physical op | 1 week |
| | Metrics | 1 week |
| | Tests | 1 week |
| **Subtotal** | | **~5 weeks** |
| **Total** | | **~16 weeks** (4 months) |
