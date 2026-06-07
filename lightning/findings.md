# LightningDB Production Readiness Audit

> **Date**: 2026-05-27
> **Scope**: Full codebase audit (37.5K lines src, 11.7K lines tests, 6 crates)
> **Status**: Pre-alpha (v0.1.0)
> **Verdict**: NOT production-ready. Critical security, correctness, and completeness gaps exist.

---

## Executive Summary

LightningDB is an ambitious embedded graph+vector+hybrid database with impressive scope (Cypher engine, SIMD vector search, Tantivy FTS, RAG pipeline, MVCC, WASM UDFs, Python/Node.js/Rust/C bindings). The core engine passes 400+ tests and benchmarks show competitive performance. However, **27 critical or high-severity issues must be resolved before production use**, spanning:

- **2 critical security vulnerabilities** (path traversal, unsound FFI)
- **1 critical supply-chain risk** (beta wasm interpreter)
- **355 `unwrap()`/`expect()` calls** that can crash the database on edge cases
- **60 silently swallowed errors** that can leave state inconsistent
- **Lock poison risk** from mixing `std::sync::RwLock` with `parking_lot`
- **C API naming is copied from KuzuDB** (`kuzu_*` everywhere)
- **Python embeddings computed but never stored** (vector search returns zero results)
- **No containerization, no health checks, no graceful shutdown, no MIRI verification of 69 unsafe blocks**

---

## Severity Key

| Label | Meaning |
|-------|---------|
| **CRITICAL** | Will crash, corrupt data, expose security vulnerability, or silently produce wrong results |
| **HIGH** | Likely to cause issues in production under real-world workloads |
| **MEDIUM** | Production concern, should be addressed before launch |
| **LOW** | Best practice gap, non-blocking |

---

## 1. SECURITY

### CRITICAL — Path Traversal via COPY Statement

**Location**: `crates/lightning-core/src/processor/operators/copy.rs:125,270,336`

The `COPY ... FROM 'path'` and `COPY ... TO 'path'` statements accept arbitrary file paths from user queries with no validation or sandboxing.

```rust
// line 125
let file = File::open(&self.file_path)?; // self.file_path from user query
```

**Impact**: `COPY t FROM '/etc/passwd'` reads arbitrary files. `COPY t TO '/root/.ssh/authorized_keys'` overwrites arbitrary files.

**Fix**: Canonicalize all paths and restrict to a configurable data directory. Reject paths containing `..`, absolute paths, or paths outside the base directory.

---

### CRITICAL — Unsafe FFI Null Pointer Dereference

**Location**: `crates/lightning-core/src/api.rs:45`

```rust
pub extern "C" fn lightning_query(conn_ptr: *mut LightningConnection, ...) {
    let conn_wrapper = unsafe { &*conn_ptr }; // NO NULL CHECK — segfault
```

While `lightning_close` (line 87) and the `capi.rs` functions correctly null-check, `lightning_query` does not. Passing NULL crashes the process.

**Contrast with**: `capi.rs:90-94` — `kuzu_connection_query` does check `connection.is_null()`.

**Fix**: Add `if conn_ptr.is_null() { return std::ptr::null_mut(); }` before dereferencing.

---

### HIGH — 69 Unsafe Blocks in Production Code, No MIRI Verification

**Locations**: Primarily storage engine (`hash_index.rs`, `column.rs`, `vector_index.rs`, `buffer_manager.rs`, `csr.rs`), C FFI (`capi.rs`, `api.rs`), WASM (`lib.rs`).

Most belong to the page-level I/O layer (necessary for a database), but the volume is high:

| File | Count | Risk Context |
|------|-------|-------------|
| `storage/index/hash_index.rs` | ~20 | Raw pointer arithmetic with computed offsets; `copy_nonoverlapping` on pages |
| `storage/column.rs` | ~15 | Frame pointer writes; bounds depend on correct `PAGE_SIZE / element_size` math |
| `storage/index/vector_index.rs` | ~13 | SIMD intrinsics with manual bounds guards |
| `api.rs` | 4 | FFI dereference without null check (see above) |
| `storage/buffer_manager.rs` | 6 | `unsafe impl Send/Sync for Frame` — correctness on lock discipline |
| `capi.rs` | ~13 | FFI pointer dereferencing |
| `storage/index/csr.rs` | 2 | Page writes in CSR rebuild |

**Fix**: Run `cargo miri test` on the storage engine test suite. Add `UnsafeCell` to `Frame.data`. Document invariants for every unsafe block.

---

### MEDIUM — Predictable Hash Collisions (DoS Vector)

**Location**: `crates/lightning-core/src/storage/index/hash_index.rs:248`

Uses `DefaultHasher` for hash table bucket distribution. Not cryptographically random — a malicious user could craft keys that all hash to the same bucket, degrading index performance to O(N).

**Fix**: Switch to `ahash` (already available in ecosystem) or `SipHasher` for DoS resistance.

---

### HIGH — No Security Policy or Vulnerability Reporting

No `SECURITY.md`, no responsible disclosure process, no security contact. A database with user-controlled Cypher queries, WASM UDF execution, CSV file I/O, and C FFI should have a public security policy.

---

## 2. DEPENDENCIES & SUPPLY CHAIN

### CRITICAL — Beta WASM Interpreter in Production Path

**Location**: `crates/lightning-core/Cargo.toml` — `wasmi = "2.0.0-beta.2"`

This is a beta release of the wasm interpreter used for user-defined functions. Beta wasm runtimes may have undiscovered sandbox escapes, memory corruption, or security vulnerabilities.

**Fix**: Downgrade to `wasmi` v1.x (stable) or switch to `wasmtime` (well-audited, production-hardened).

---

### HIGH — Stale Arrow Dependency

**Location**: `Cargo.toml` — `arrow = "58.0.0"`

Arrow 58 is ~6 major versions behind current (64+). Each release contains performance improvements and bug fixes. The codebase uses Arrow extensively (columnar storage, scan operators, evaluator).

**Fix**: Upgrade to latest Arrow (`>= 64`). Test all scan/filter/evaluator paths.

---

### MEDIUM — Stale Binding Dependencies

- `napi 3` → napi 4 is available
- `napi-derive 3` → 4
- `napi-build 1` → 2
- `bincode 1.3` → 2.x (breaking changes, needs migration)

---



## 3. ERROR HANDLING & PANIC RISK

### CRITICAL — 355 Unwrap/Expect Calls in Production Source (Non-Test Code)

These will panic and crash the database thread (or process) on unexpected values:

**Worst Offenders**:

| File | Count | Risk |
|------|-------|------|
| `processor/arrow_utils.rs` | 50+ | Schema mismatch panics query thread — `.downcast_ref::<Int64Array>().unwrap()` |
| `storage/index/trigram_index.rs` | 20+ | `.read().unwrap()` / `.write().unwrap()` on `std::sync::RwLock` — poison = permanent panic |
| `parser/mod.rs` | 15+ | `pairs.next().unwrap()` — malformed input panics |
| `storage/index/hash_index.rs` | 15+ | `.try_into().expect(...)` — technically infallible but noisy |
| `processor/operators/scan.rs` | 4 | `.expect("filter expression must evaluate to BooleanArray")` — type error panics |
| `storage/buffer_manager.rs` | 2 | `synced_fids.lock().unwrap()` on `std::sync::Mutex` |

**Fix**: Phase 1 — Replace all `.unwrap()` on lock acquisitions with `?` (switching to `parking_lot` makes locks infallible). Phase 2 — Replace type downcast `.unwrap()` with proper error returns.

---

### HIGH — 60 Silently Swallowed Errors (`let _ =`)

Errors are discarded with no logging, no metric, and no propagation in 60 locations:

| File | Lines | Impact |
|------|-------|--------|
| `memory.rs` | 154, 364-365, 648, 796-801, 899, 904, 936 | RAG pipeline errors lost — query results silently incomplete |
| `lib.rs` | 278, 420, 475, 1021, 1062 | Vector index creation, FSM save, rollback errors silently dropped |
| `storage/undo_buffer.rs` | 127, 129, 133, 135, 159, 198 | Rollback failures silently ignored — database can end up inconsistent |
| `processor/scheduler.rs` | 44, 65 | Channel send errors lost — query errors may never reach caller |
| `storage/trigram_index_worker.rs` | 74, 78, 82, 88 | Index update tasks silently dropped |
| `processor/operators/dml.rs` | 833-834 | FTS insert/commit errors lost |
| `processor/operators/copy.rs` | 352 | CSV finalization errors lost |
| `processor/operators/gds/recursive_join.rs` | 92 | CSR traversal errors lost |

**Fix**: At minimum, add `tracing::warn!()` or `tracing::error!()` for every swallowed error. Preferably, propagate or accumulate errors into a structured error channel.

---

### HIGH — Lock Poison Risk from std::sync Locks + Unwrap

**Locations**: `trigram_index.rs:60-64` (3 `std::sync::RwLock`s), `buffer_manager.rs:545` (1 `std::sync::Mutex`), `memory.rs:93` (1 `std::sync::Mutex`)

The codebase uses `parking_lot::RwLock` throughout the storage/transaction layers but `std::sync::RwLock` in trigram indexes and a few other places. `std::sync` locks **poison on panic** — if a thread panics while holding one of these locks, every subsequent `.read().unwrap()` / `.write().unwrap()` in ANY thread will also panic, permanently disabling that index.

**Fix**: Replace all remaining `std::sync::RwLock` / `std::sync::Mutex` with `parking_lot::RwLock` / `parking_lot::Mutex` (already a workspace dependency).

---

## 4. C API ISSUES

### CRITICAL — All Types and Functions Named `kuzu_*` Instead of `lightning_*`

**Location**: `crates/lightning-core/src/capi.rs` (entire file, 157 lines)

Every type, function, and struct uses the `kuzu_` prefix from KuzuDB:
- `kuzu_database`, `kuzu_connection`, `kuzu_query_result`, `kuzu_system_config`
- `kuzu_database_init`, `kuzu_connection_query`, `kuzu_query_result_get_error_message`

This is clearly copy-pasted from KuzuDB's C API and never renamed.

---

### CRITICAL — Unsound Double-Indirection FFI Layout

**Location**: `crates/lightning-core/src/capi.rs:7-9`

```rust
pub struct kuzu_database {
    pub database: *mut Arc<Database>, // Double indirection through Arc
}
```

`kuzu_database_init` (line 47-50) allocates `Box::into_raw(Box::new(db))`, then wraps that in another `Box` with `kuzu_database { database: db_ptr }`. This double-boxing with `Arc` behind a raw pointer is unsound for C interop — the `Arc` refcount is managed on the Rust heap but the C caller owns the raw pointer. A second `kuzu_connection_init` call (line 71) clones the Arc through the pointer indirection: `Arc::clone(&*(*database).database)`.

This works only by accident if the code is single-threaded and the `Arc` hasn't been dropped. It will UAF if the C caller passes a freed pointer.

**Fix**: Either use `*mut Database` directly (raw pointer, caller manages lifetime) or `Box<Database>` (full ownership transfer).

---

### HIGH — No Result Extraction Functions

The C API has `kuzu_query_result_is_success` and `kuzu_query_result_get_error_message`, but no functions to:
- Get column count
- Get column names
- Get row count
- Get individual cell values
- Iterate results

C consumers cannot extract data from query results.

---

### HIGH — No C Header File

No `.h` file is generated or published. C consumers must hand-write bindings.

---

### MEDIUM — Inconsistent Null Checks Across API Surface

`lightning_query` (api.rs:45) is the only function that dereferences a raw pointer without a null check. All other `api.rs` and `capi.rs` functions check `is_null()` first.

---

### MEDIUM — Error Information Swallowed

`capi.rs:145`:
```rust
CString::new(msg).unwrap_or_else(|_| CString::new("error").unwrap())
```
If the error message contains a null byte (possible for corrupted data errors), the original message is replaced with a generic "error" string.

---

## 5. PYTHON & NODE BINDINGS

### CRITICAL — Embeddings Computed But Never Stored (Python)

**Location**: `python/lightning/__init__.py` + `python/lightning/langchain.py`

The Python `MemoryEntity` class has no `embedding` field (line in __init__.py). `langchain.py` computes embeddings via `embed_documents()` but they're discarded because `store()` cannot accept them. **Vector and hybrid search returns zero results in Python**. Same issue in `llama_index.py`.

---

### HIGH — Python `store_batch` Panics on Non-Dict Input

**Location**: `crates/lightning-python/src/lib.rs` — `downcast_bound::<PyDict>` with `unwrap()`

Passing a non-dict argument to `store_batch()` crashes the CPython interpreter. Python is a duck-typed language — this should return a `TypeError` exception, not an abort.

---

### HIGH — 11 of 21 MemoryStore Methods Missing from Python

Missing Python bindings: `rag_query`, `consolidate`, `recall_stream`, `recall_at_time`, `entity_history`, `execute_at`, `query_stream`, `recall_by_time`, `with_embedding_dim`, `subscribe_changes`, `now_micros_for_test`.

The RAG pipeline, streaming, and CDC features advertised in the README are **completely unusable from Python**.

---

### HIGH — No Python Tests

Zero `.py` test files. Zero pytest configuration. No CI for Python.

---

### HIGH — No Node.js Tests

`crates/lightning-node/package.json` references `vitest` but no `.test.ts` files exist.

---

### MEDIUM — Python Error Type Erasure

All `LightningError` variants (Query, Database, Internal, Serialization, etc.) are flattened to generic `PyRuntimeError`. Python callers cannot distinguish between a query syntax error and a database corruption error.

---

### MEDIUM — Python Missing Type Annotations

7 of 10 Python wrapper methods have no return type annotations.

---

## 6. CONCURRENCY & CORRECTNESS

### MEDIUM — Single StorageManager Lock Serializes All Table Access

**Location**: `crates/lightning-core/src/lib.rs:184`

```rust
pub storage_manager: Arc<RwLock<StorageManager>>
```

All operations on all tables pass through a single `RwLock`. Under write-heavy workloads, this is a serialization bottleneck. Consider sharding at the table level.

---

### MEDIUM — TransactionManager Write Lock on Every Begin/Commit/Rollback

**Location**: `crates/lightning-core/src/transaction/transaction_manager.rs:48-49`

```rust
active_tx_ids: RwLock<HashSet<u64>>,
active_read_ts: RwLock<BTreeMap<u64, usize>>,
```

Every `begin()` acquires BOTH write locks. On high-throughput workloads, this serializes transaction creation.

---

### MEDIUM — Drop Implementation Blocks While Holding Read Lock

**Location**: `crates/lightning-core/src/lib.rs:204-231`

`Drop for Database` acquires `storage_manager.read()`, then uses a polling loop (10×50ms) while holding it, then calls `flush_all_with_handles()`. If another thread holds a write lock and needs buffer pool access, this can deadlock or block indefinitely.

---

### MEDIUM — CDC Sender Leak (Memory Leak)

**Location**: `crates/lightning-core/src/memory.rs:93`

```rust
cdc_senders: std::sync::Mutex<Vec<std::sync::mpsc::Sender<ChangeEvent>>>
```

Senders are pushed into the Vec when clients subscribe. When a client disconnects (drops the receiver), the dead sender stays in the Vec forever. No cleanup mechanism.

---

## 7. RESOURCE MANAGEMENT

### MEDIUM — Unbounded Buffer Allocations

| Location | Issue |
|----------|-------|
| `storage/column.rs:664` | Reads ALL overflow pages into a single Vec. Millions of large strings could exhaust memory. |
| `storage/index/hash_index.rs:80,100` | Doubles bucket count without limit. Forced resizing can exhaust memory. |
| `processor/operators/gds/pagerank.rs:82-111` | Stores per-node state in Vecs sized to `max_node_id`. Sparse graphs with large IDs waste memory. |

---

### LOW — WAL Archives Accumulate Without Purging

**Location**: `crates/lightning-core/src/storage/wal.rs:318-323`

WAL archives are rotated but never automatically deleted. They accumulate indefinitely.

---

### LOW — No Config File Validation

`Database::new()` accepts `SystemConfig` with no validation. `buffer_pool_size: 0` creates zero-capacity pools that immediately fail. `max_num_threads: 0` behaves unpredictably with Rayon.

---

## 8. PRODUCTION INFRASTRUCTURE

### HIGH — No Graceful Shutdown

- No SIGTERM/SIGINT handler
- `Database::drop` does checkpoint + shutdown, but only if the `Arc<Database>` is cleanly dropped
- Process kill loses unflushed data
- No connection draining — active queries are aborted
- Background threads (`trigram_index_worker.rs`) may lose state on kill

---

### HIGH — No Health Checks

- No `is_healthy()`, `is_ready()`, or equivalent
- No liveness probe (buffer pool accepting reads/writes?)
- No readiness probe (recovery complete? WAL replay done?)
- Cannot integrate with Kubernetes or load balancers

---

### HIGH — No Observability

- No Prometheus metrics endpoint
- No OpenTelemetry tracing
- No structured log export (uses `tracing::info!()` but no subscriber configuration)
- `DatabaseMetrics` struct exists but has no export mechanism
- `slow_query_threshold_ms` is set but no slow query log emission exists
- No buffer pool hit/miss ratio exposed
- No WAL throughput or queue depth metrics

---



### MEDIUM — No Containerization

- No `Dockerfile`
- No `docker-compose.yml`
- No build/push pipeline for container images
- `musl` static builds (ROADMAP 3.2.4) not done

---

### MEDIUM — No Configuration Mechanism

`SystemConfig` is programmatic-only. There is no:
- Config file parsing (TOML/JSON/YAML)
- Environment variable overrides
- `--config` CLI option
- Default config file location search

---

### MEDIUM — No Task Runner

- No `Makefile`, `justfile`, or equivalent
- No single command to: build all crates, run all tests, format, lint, build Python wheels, build Node package

---

## 9. TESTING

### CRITICAL — No Codec Unit Tests

**Reference**: ROADMAP items 2.8.4a-e

The compression codecs (ALP, Bitpacking, Delta, RLE, Dict) are used in the storage engine but have **zero dedicated unit tests**. The ROADMAP documents multiple known critical dict/ALP/delta bugs with empty test coverage. Compression bugs can cause silent data corruption.

---

### HIGH — No MIRI Verification of Unsafe Code

69 unsafe blocks with no MIRI (Rust's undefined behavior detector) verification. Given that some blocks touch memory layout and pointer arithmetic in the storage layer, this is a significant correctness risk.

---

### HIGH — Zero Python Tests

No `.py` test files. No pytest configuration. No CI.

---

### HIGH — Zero Node.js Tests

Vitest is configured but no test files exist.

---

### MEDIUM — No Continuous/Mutation Fuzzing

`cargo-fuzz` / `afl.rs` not integrated. Current fuzz tests are deterministic combinatorial, not mutation-based. A database needs coverage-guided fuzzing.

---

### MEDIUM — Missing Test Categories

| Gap | Impact |
|-----|--------|
| No chaos/failure injection framework | Process-level kill testing not automated |
| No snapshot isolation violation tests (write-skew) | MVCC correctness not proven |
| No FreeSpaceManager reuse tests | Silent storage leaks possible |
| No load/throughput stress tests | Performance at scale unknown |
| No operator-level unit tests for scan/filter/projection/sort/aggregate/limit/topk/dml/ddl | Operators tested only via integration |
| No property-based testing with `proptest`/`quickcheck` | Test coverage is single-seed, not randomized |

---

## 10. DOCUMENTATION & COMPLIANCE

### MEDIUM — No LICENSE File on Disk

`Cargo.toml` declares `license = "MIT"` but no `LICENSE` or `LICENSE.md` file exists. GitHub's license detection will mark the repo as having no license.

---

### MEDIUM — No Contributing Guide

No `CONTRIBUTING.md` — no development setup instructions, code style guidelines, PR process, or CLA.

---

### MEDIUM — No Changelog

No `CHANGELOG.md`. The `AUDIT.md` partially serves this role but is not user-facing.

---

### MEDIUM — Repository URL Mismatch

Workspace `Cargo.toml`: `repository = "https://github.com/lightning-db/lightning"`
`pyproject.toml`: `repository = "https://github.com/BViganotti/lightning"`
These are different URLs.

---

### MEDIUM — Thin Operational Documentation

- `PERFORMANCE_TUNING.md` — 91 lines, no profiling methodology, no EXPLAIN output guide
- `MIGRATION_GUIDE.md` — 63 lines, no step-by-step procedures for version upgrades or index format migrations

---

### LOW — Thin Documentation Sections

- `ARCHITECTURE.md` missing: lock/latch hierarchy, error taxonomy, recovery state machine diagram
- `CYPHER_REFERENCE.md` missing: versioned grammar reference (what syntax works in which version)
- No `SECURITY.md`, `CODE_OF_CONDUCT.md`

---

## 11. CONFIGURATION & VERSIONING

### MEDIUM — No API Versioning Strategy

Workspace version `0.1.0`. No SemVer enforcement, no `cargo-semver-checks`, no `#[deprecated]` annotations, no deprecation macros. Backward compatibility has no guardrails.

---

### LOW — Gitignore Missing IDE/Editor Files

Missing patterns: `.DS_Store`, `*.swp`, `*.swo`, `.vscode/`, `.idea/`.

---

## 12. COSMETIC / NIT

### LOW — Crash Recovery Claim Slightly Misleading

README claims "Crash Recovery: WAL + Checkpoint. Automatic on restart." but there's no automated restart — the database must be reopened, and WAL replay happens in `Database::new()`. This is documented correctly in ARCHITECTURE.md.

### LOW — Pre-built Python Binary in Repo

`python/lightning/_native.cpython-313-darwin.so` is committed to the repository. This should be built by CI, not checked in as a binary.

---

## Summary: What Must Be Fixed Before V1.0 Production

### Phase 1 — Blockers (must fix before any production use)

| # | Issue | Severity |
|---|-------|----------|
| 1 | Path traversal in COPY statement | CRITICAL |
| 2 | Compile-time regex with no null check in `lightning_query` | CRITICAL |
| 3 | Replace beta `wasmi 2.0.0-beta.2` with stable wasmi or wasmtime | CRITICAL |
| 4 | Rename all `kuzu_*` types/functions to `lightning_*` | CRITICAL |
| 5 | Fix Python embeddings-not-stored bug (vector search broken) | CRITICAL |
| 6 | Add codec unit tests (compression corruption) | CRITICAL |
| 7 | Replace 355 `unwrap()`/`expect()` calls in production code | CRITICAL |
| 8 | Fix 60 silently swallowed errors | HIGH |
| 9 | Replace `std::sync::RwLock` with `parking_lot::RwLock` (lock poison) | HIGH |
| 10 | Fix double-boxed FFI layout in C API | CRITICAL |
| 11 | Run `cargo miri test` on all unsafe blocks | HIGH |
| 12 | Add C result extraction functions + header file | HIGH |

### Phase 2 — Must Have Before Launch

| # | Issue |
|---|-------|
| 13 | Graceful shutdown (SIGTERM handler + connection draining) |
| 14 | Health check endpoints |
| 15 | Python tests |
| 16 | Node.js tests |
| 17 | Expose missing 11 Python MemoryStore methods |
| 18 | Fix Python error type erasure |
| 19 | Config file parsing + env var overrides |
| 20 | Prometheus metrics + OpenTelemetry tracing |
| 21 | LICENSE file, CHANGELOG.md, CONTRIBUTING.md, SECURITY.md |

### Phase 3 — Should Have

| # | Issue |
|---|-------|
| 22 | Mutation/coverage fuzzing (cargo-fuzz) |
| 23 | Property-based testing (proptest) |
| 24 | Load/stress throughput benchmarks |
| 25 | CDC sender cleanup |
| 26 | StorageManager lock sharding |
| 27 | Vector index - replace modulo/division with bitwise ops |
| 28 | Flood-resistant hash (DoS) |
| 29 | Full Python type annotations |
| 30 | SemVer strategy + cargo-semver-checks |
| 31 | Expand PERFORMANCE_TUNING.md and MIGRATION_GUIDE.md |

---

## Methodology

This audit was performed by:
1. Full codebase structure analysis (6 crates, 35 test files)
2. Targeted grep for security patterns (unsafe, unwrap, expect, `let _ =`, file I/O, FFI)
3. Manual review of all C API, Python bindings, Node bindings, and core engine entry points
4. Cross-reference against ROADMAP.md (400+ tracked tasks), AUDIT.md (25 audited items), and crate documentation
