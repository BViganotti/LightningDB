# Lightning

Lightning is an embedded graph+vector+hybrid database engine designed for **AI agent memory**.

It collapses what currently requires 3-4 separate services (vector DB + graph DB + full-text search + relational store) into a single embeddable binary.

## Features

- **Graph model** — native NODE/REL types with Cypher-compatible query language
- **Columnar storage** — compressed, page-based, with custom ALP/bitpacking/delta/RLE compression
- **Vector search** — 768-dim float32 embedding search, no external index needed
- **Full-text search** — Tantivy-based FTS for text fields
- **Hybrid search** — RRF fusion across vector and FTS results
- **MVCC transactions** — snapshot isolation, WAL durability, undo buffer for rollbacks
- **CSR adjacency index** — bidirectional compressed sparse row for O(1) graph traversal
- **Arrow-native** — zero-copy Apache Arrow interop via C Data Interface

## Status

Early-stage. Active development.
