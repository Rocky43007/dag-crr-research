# Reproducing Paper Results

This document provides step-by-step instructions to reproduce the experimental results in the DAG-CRR paper.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/rocky43007/dag-crr-research.git
cd dag-crr-research

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Run all benchmarks (takes ~30 minutes)
cargo bench
```

## Prerequisites

- **Rust**: 1.70+ (install via rustup)
- **RAM**: 16GB+ recommended for large-scale benchmarks
- **OS**: Linux or macOS

## Pre-computed Results

If you want to inspect results without running benchmarks:

- `results/network.json` - GCP RTT measurements and coordinated GC latency
- `results/criterion.json` - Consolidated benchmark results

## Reproducing Specific Paper Claims

### Claim 1: 238x Speedup (Table 8, Section 6.2)

The 238x speedup compares coordinated GC (798ms) vs local GC (3.4ms).

**Pre-computed data** (from GCP deployment):
```bash
cat results/network.json | python3 -c "
import json, sys
data = json.load(sys.stdin)
for peer in data:
    print(f\"RTT to {peer['peer']}: {peer['rtt_mean_us']/1000:.1f}ms\")
print(f\"Coordinated GC: {data[0]['coordinated_gc_us']/1000:.1f}ms\")
print(f\"Local GC: {data[0]['local_gc_us']/1000:.1f}ms\")
print(f\"Speedup: {data[0]['speedup']:.1f}x\")
"
```

To reproduce locally (measures local GC only, not network):
```bash
cargo bench --bench gc_demo -- "GC/"
```

### Claim 2: 34x DAG Overhead (Table 7, Section 6.4)

```bash
cargo bench --bench paper_extended -- "BackendComparison"
```

Expected output:
```
DAG-CRR_SQLite/1000   ~100ms
Plain_SQLite/1000     ~3ms
```

### Claim 3: Linear Scalability to 50 Peers (Figure 4)

```bash
cargo bench --bench paper_extended -- "Scalability"
```

### Claim 4: Sync After Independent GC (Table 3)

Demonstrates that peers can GC at different depths and still sync correctly:

```bash
cargo bench --bench gc_demo -- "SyncAfterGC"
```

### Claim 5: Per-Column Edit Preservation (Table 2)

Row-level systems lose 50% of concurrent edits; per-column preserves all:

```bash
cargo bench --bench alternatives -- "ConcurrentEdits"
```

## Generating Figures

```bash
# Install dependencies
pip install matplotlib numpy

# Generate all figures
python3 scripts/generate_figures.py

# Figures are saved to results/figures/
ls results/figures/
```

## Full Benchmark Suite

See [BENCHMARKS.md](BENCHMARKS.md) for detailed documentation on:
- All benchmark files and their purposes
- Mapping between benchmarks and paper figures/tables
- Interpreting Criterion output
- Troubleshooting

## Hardware Used in Paper

- **Local benchmarks**: Apple M2 Max, 32GB RAM, macOS 15.5
- **Network benchmarks**: Google Cloud Platform VMs
  - Client: us-central1-a (e2-standard-2)
  - Servers: us-west1-a, us-east1-b, europe-west1-b

## Expected Runtime

| Benchmark Set | Time |
|--------------|------|
| Core benchmarks | ~5 min |
| Extended benchmarks | ~15 min |
| Large-scale (1M rows) | ~30 min |
| Full suite | ~45 min |

## Verifying Results Match Paper

After running benchmarks, compare against paper claims:

```bash
# Extract key metrics
python3 scripts/extract_results.py --filter "Insert"
python3 scripts/extract_results.py --filter "Merge"
python3 scripts/extract_results.py --filter "GC"
```

Results should be within 10% of paper values (hardware-dependent).

## Questions or Issues

Open an issue at: https://github.com/rocky43007/dag-crr-research/issues
