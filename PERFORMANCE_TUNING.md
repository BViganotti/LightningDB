# Performance Tuning Guide

## Buffer Pool

The buffer pool caches database pages in memory. Default: 1GB.

```
SystemConfig { buffer_pool_size: 1024 * 1024 * 1024 }
```

- **Small datasets (<1GB)**: Set to total dataset size × 1.5
- **Large datasets (>10GB)**: Set to 10-20% of dataset size
- **Read-heavy workloads**: Larger buffer pool (30-50% of dataset)
- **Write-heavy workloads**: Smaller buffer pool (5-10% of dataset), rely on OS page cache

## Prefetch

The learned prefetch system tracks page access patterns and predicts future accesses.

```
SystemConfig {
    prefetch_enabled: true,
    prefetch_depth: 2,
    prefetch_confidence: 0.15,
}
```

- **Sequential scans**: Higher `prefetch_depth` (4-8) and lower `prefetch_confidence` (0.05)
- **Random access**: Lower `prefetch_depth` (1) and higher `prefetch_confidence` (0.3)
- **Disable**: Set `prefetch_enabled: false` for predictable workloads

## Threading

```
SystemConfig { max_num_threads: 0 }
```

- `0` = auto-detect (uses `num_cpus::get()`)
- Set to 2-4 for in-process agent workloads (leave CPU for LLM)
- Set to 8-16 for standalone database workloads

## Sync Mode

```rust
pub enum SyncMode { Normal, Lazy }
```

- **Normal** (default): `fsync` on every commit — maximum durability, ~100-1000µs per commit
- **Lazy**: No `fsync` — up to 10x faster writes, risk of losing last ~1 second of committed data on crash

## Compression

Columns are analyzed at `optimize()` time and compressed automatically:

| Codec | Best for |
|-------|----------|
| Constant | Single-value columns (e.g., deleted rows) |
| RLE | Columns with long runs of the same value |
| Dict | Low-cardinality strings (<25% unique) |
| Integer Bitpacking | Integers with narrow value ranges |
| Fixed-Frame-Of-Reference | Sequential integers with positive offset |
| ALP | Float64 columns with moderate precision |

Call `column.optimize(bm, tx)` after bulk inserts to trigger compression analysis.

## Checkpoint

Checkpoints flush dirty pages to disk. They happen:
- After every DML auto-commit
- On explicit `CHECKPOINT` call
- On clean shutdown

The checkpoint is parallelized across buffer pool shards. For very large databases (>100GB) with millions of dirty pages, checkpoint may take several seconds.

## Vector Search

Vector search is an exhaustive parallel scan with SIMD dot product:
- Uses AVX2 FMA when available (x86) — ~1M vectors/second
- Falls back to SSE or scalar on other platforms

For larger datasets, consider sharding vectors across multiple tables.

## Monitoring

```rust
db.metrics.buffer_hit_rate()        // Target: >0.95
db.metrics.avg_checkpoint_duration_ms()  // Should be <1000ms
```

Slow queries (>100ms by default) are logged via `tracing::warn!`.
Adjust threshold: `SystemConfig { slow_query_threshold_ms: 500 }`
