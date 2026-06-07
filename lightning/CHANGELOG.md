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

[0.1.0]: https://github.com/lightning-db/lightning/releases/tag/v0.1.0
