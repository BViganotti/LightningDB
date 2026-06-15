# Changelog

All notable changes to the LightningDB project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Status: Pre-alpha

This is the initial pre-alpha release of Lightning. The API is unstable and
subject to breaking changes without notice. Not recommended for production use.

### Added

- Core graph engine with native property graph model
- Cypher-compatible query language frontend
- Vector indexing and hybrid search (graph + vector)
- MVCC (Multi-Version Concurrency Control) storage engine
- Event-driven architecture with `EventBus`
- Writer-level snapshot isolation
- Node, relationship, and property CRUD operations
- Rust driver crate (`lightning`)
- Python bindings via PyO3 and maturin
- Arrow-based columnar integration via FFI
- WASM UDF support for embedded user-defined functions
- In-memory mode for lightweight deployments
- 57 comprehensive tests for buffer manager, optimizer, compression, joins,
  transactions, edge cases, and functions
- Full-text search support via tantivy integration

### Changed

- License changed from MIT to **BSL 1.1** (Business Source License) with
  Change Date of 2030-06-15, MIT as Change License — prevents competitors
  from offering LightningDB as a service without licensing
- README rewritten with real API examples (Python, Rust, cURL) and HTTP
  endpoint reference table
- Repository cleanup: removed stale files, audit reports, agent artifacts,
  ANTLR JARs, .forge worktrees, and build artifacts
- Upgraded `lru` dependency from 0.12.0 to 0.16.3 (fixes RUSTSEC-2026-0002)
- `deny.toml` updated to cargo-deny 0.19.x format
- Updated `pyproject.toml`, `SECURITY.md`, `CYPHER_REFERENCE.md` for
  open-source readiness

### Fixed

- Remove unnecessary Vec clone in intersect hash probe
- Use f32 for vector index dot product fallback (was f64)
- Acquire inverted index write lock once per batch (was per document)
- Use `add_new_page` for atomic page allocation in hash_index
- Use HashMap for UNION DISTINCT collision lookup (was O(n²))
- Detect NOT EXISTS via `BoundExpression::Not` in subquery unnesting
- Validate variable ownership before pushing filter into scan
- Implement `order_by_pushdown` and `limit_pushdown` optimizers (were no-op)
- Use `node_count` for optimizer fixed-point detection
- Document `execute_stream` root consumption limitation
- TrigramIndexWorker Drop now waits for worker thread
- Extract inverted index writer memory to named constant
- Assign parsed variable-length bounds to pattern
- Correct MinHash similarity denominator to Jaccard formula
- Sort aggregate groups for deterministic GROUP BY output
- Zero WASM memory before each string mode invocation
- Use transaction read_ts in index_scan MVCC check
- Clean up page_merge_locks on transaction rollback
- Warn when pending_nulls grows too large
- Atomic write for database header and free space map
- Rollback catalog on storage failure in DDL
- Move prefetch I/O outside shard write lock
- Rate limiter: use IpAddr key (avoid String allocation) and evict stale
  entries to prevent unbounded growth
- Prefix auto-generated request IDs with 'auto-'
- Return specific types for aggregate expressions
- Log warning on SystemTime error in now_micros
- Cap page_bounds growth to prevent unbounded memory
- Warn on embedding dimension mismatch in recall
- Add upper bounds for RagRequest optional parameters
- Handle channel close in query stream
- Use proper JSON serialization in path_probe instead of Debug
- Use CAS loop in unlock to avoid losing dirty bits
- Use AcqRel instead of SeqCst in GDS frontier visit
- Remove unused `_parameters` computation in StandaloneCall binding
- Replace unwrap with safe downcast in streaming.rs
- Clean up file_handles when removing a table
- Remove redundant vacuum_interval_ms clamp
- Add missing operators to `node_count`
- Warn when edge_types parameter is used but not implemented
- Limit x-request-id header length to 256 characters
- Health endpoint now verifies database connectivity
- Bounds checks in dict decompress
- Remove debug logging of user data in CONTAINS function
- Bounds check ALP fac_idx/exp_idx against array sizes
- Saturating cast for f64→i64 in compression analyzer
- Cap RLE run count at u32::MAX to prevent overflow
- Check all_same even when skip_minmax is true
- Remove assert in bitpacking pack_32 for bit_width=0
- Prevent usize::MAX level in HNSW random_level
- Use per-thread random seed for HNSW PRNG
- Bounds check in hash_index write_entry_to_page
- Validate string length in overflow write_string
- Use AcqRel ordering for WAL archive_seq
- Division by zero returns NULL instead of error (SQL standard)
- Replace O(n²) contains() with HashSet for trigram batch insert

### Security

- Upgraded `lru` to 0.16.3 (RUSTSEC-2026-0002)
- Removed debug logging of user data in CONTAINS function
- Input validation across compression, indexing, and storage layers
- Bounds checks in dictionary decompression, hash index, and string overflow
- Rate limiter entry eviction prevents unbounded memory growth

[0.1.0]: https://github.com/lightning-db/lightning/releases/tag/v0.1.0
