# Benchmarks

This document describes how to run the benchmarks and interpret the results.

## Prerequisites

- Rust 1.70+
- Criterion 0.5 (included in dev-dependencies)
- Sufficient RAM (16GB+ recommended for large-scale benchmarks)

## Benchmark Files

| File | Description |
|------|-------------|
| `paper_benchmarks.rs` | Core paper claims: merge latency, insert throughput, multi-peer sync (2-16), conflict resolution, DAG depth |
| `paper_extended.rs` | Extended paper figures: 1M scale, 50-peer, memory, sensitivity, backend comparison, throughput/latency, tiebreaker, break-even |
| `fair_comparison.rs` | DAG-CRR vs CR-SQLite (both SQLite-backed for fair comparison) |
| `gc_demo.rs` | Coordination-free GC demonstration (key differentiator) |
| `alternatives.rs` | Comparison with Automerge, HLC-LWW, and LWW+Audit |
| `common/mod.rs` | Shared utilities for data generation and CR-SQLite setup |

## Running Benchmarks

### Run all benchmarks

```bash
cargo bench
```

### Run a specific benchmark file

```bash
cargo bench --bench paper_benchmarks
cargo bench --bench paper_extended
cargo bench --bench fair_comparison
cargo bench --bench gc_demo
cargo bench --bench alternatives
```

### Run with filtering

```bash
# Run only benchmarks matching a pattern
cargo bench -- "Insert"
cargo bench -- "Merge"
cargo bench -- "GC"
cargo bench -- "Scalability"
```

## Benchmark Groups

### paper_benchmarks.rs (Core)

- `MergeLatency` - Merge performance at 100, 1K, 5K, 10K changeset sizes
- `InsertThroughput` - Insert performance at 1K, 10K, 100K rows
- `MultiPeerSync` - Sync with 2, 4, 8, 16 peers
- `ConflictResolution` - Conflict handling at 10%, 25%, 50%, 75%, 100% rates
- `DAGDepth` - DAG depth impact at 5, 10, 25, 50, 100 versions

### paper_extended.rs (Extended Figures)

**Large Scale (fig2b, fig3):**
- `InsertLargeScale` - Insert at 100K, 500K, 1M rows
- `MergeLargeScale` - Merge at 1M base with 1K, 10K, 100K changesets

**Scalability (fig_scalability):**
- `ScalabilityFullMesh` - Full mesh sync with 5, 10, 20, 30, 50 peers
- `ScalabilityPairwise` - Pairwise sync chain with 5-50 peers

**Sensitivity (fig_sensitivity_*):**
- `SensitivityColumns` - Insert/merge with 2, 6, 12, 24, 48 columns
- `SensitivityValueSize` - Insert/merge with 10B, 100B, 1KB, 10KB values
- `SensitivityConflictRate` - Merge at 0%, 10%, 25%, 50%, 75%, 100% conflicts

**Tiebreaker (fig5):**
- `TiebreakerOverhead` - LexMin vs PreferExisting vs PreferIncoming at 30% conflicts

**Throughput/Latency (Table 9):**
- `ThroughputInsert` - Insert ops/sec at 1K, 10K, 100K rows
- `ThroughputUpdate` - Update ops/sec at 1K, 10K, 100K rows
- `ThroughputMerge` - Merge ops/sec at 1K, 10K, 100K rows
- `LatencySingleOp` - p95 latency for single insert, update, merge (500 rows)

**Memory (fig6, fig_memory):**
- `MemoryUsage` - Creation time as proxy for memory at 1K, 10K, 100K, 1M rows

**Backend Comparison (Table 7):**
- `BackendComparison` - DAG-CRR SQLite vs Plain SQLite at 1K, 10K rows

**Break-even (fig_breakeven_*):**
- `BreakevenHistoryQuery` - DAG write overhead vs history query cost

### fair_comparison.rs

- `Insert` - DAG-CRR vs CR-SQLite insert performance
- `Merge` - DAG-CRR vs CR-SQLite merge/update performance
- `Read` - DAG-CRR vs CR-SQLite read performance

### gc_demo.rs

- `GC` - GC performance at different retention depths (5, 10, 25, 50 versions)
- `SyncAfterGC` - Sync between peers with asymmetric GC (peer A: 5, peer B: 50)
- `GCThenMerge` - Merge performance after GC

### alternatives.rs

- `Automerge_Insert` - Insert comparison with Automerge
- `Automerge_Merge` - Merge comparison with Automerge
- `Automerge_DocSize` - Document size comparison
- `HLC_Insert` - Insert comparison with HLC-based LWW
- `HLC_Merge` - Merge comparison with HLC-based LWW
- `LWW_Audit_Insert` - Insert comparison with LWW+Audit table
- `LWW_Audit_History` - History query comparison

## Paper Figure Mapping

| Paper Figure/Table | Benchmark |
|---|---|
| fig1_merge_latency | `paper_benchmarks::MergeLatency` |
| fig2a_insert_small | `paper_benchmarks::InsertThroughput` |
| fig2b_insert_large | `paper_extended::InsertLargeScale` |
| fig3_merge_at_scale | `paper_extended::MergeLargeScale` |
| fig4_multi_peer | `paper_benchmarks::MultiPeerSync` |
| fig5_tiebreaker | `paper_extended::TiebreakerOverhead` |
| fig6_memory | `paper_extended::MemoryUsage` |
| fig_scalability | `paper_extended::ScalabilityFullMesh/Pairwise` |
| fig_sensitivity_* | `paper_extended::Sensitivity*` |
| fig8_hlc | `alternatives::HLC_*` |
| fig9_automerge | `alternatives::Automerge_*` |
| Table 3 (GC sync) | `gc_demo::SyncAfterGC` |
| Table 7 (backend) | `paper_extended::BackendComparison` |
| Table 9 (throughput) | `paper_extended::Throughput*` |

## CR-SQLite Setup (for fair_comparison)

```bash
./scripts/setup_crsqlite.sh
```

This downloads the CR-SQLite extension to `vendor/crsqlite.dylib`.

## Output Location

Criterion generates HTML reports in:

```
target/criterion/
```

## Reading Results

### Console Output

```
MergeLatency/1000
                        time:   [1.2345 ms 1.2456 ms 1.2567 ms]
                        thrpt:  [795.73 Kelem/s 802.79 Kelem/s 809.97 Kelem/s]
```

- `time`: [lower bound, estimate, upper bound] of execution time
- `thrpt`: Throughput (elements per second)

### HTML Reports

Open `target/criterion/report/index.html` in a browser for:

- Interactive plots
- Detailed statistics
- Historical comparisons

### Extracting Data for Figures

```python
import json
import glob

for path in glob.glob('target/criterion/**/estimates.json', recursive=True):
    with open(path) as f:
        data = json.load(f)
        mean = data['mean']['point_estimate'] / 1e6  # ns to ms
        ci_lo = data['mean']['confidence_interval']['lower_bound'] / 1e6
        ci_hi = data['mean']['confidence_interval']['upper_bound'] / 1e6
        error = (ci_hi - ci_lo) / 2
        print(f"{path}: {mean:.3f} +/- {error:.3f} ms")
```

## Troubleshooting

### Out of Memory

If benchmarks fail with OOM:

1. Run fewer benchmarks at once
2. Reduce scale (edit benchmark file temporarily)
3. Use a machine with more RAM

### Large Scale Benchmarks

1M row benchmarks require significant time and memory:

```bash
# Run only large scale benchmarks
cargo bench --bench paper_extended -- "LargeScale"
```

### Noisy Results

If results vary significantly:

1. Close background applications
2. Disable CPU frequency scaling
3. Run on a dedicated machine
