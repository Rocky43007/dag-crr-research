# DAG-CRR: Per-Column Conflict Resolution with Coordination-Free Garbage Collection

A synchronization engine for replicated databases combining per-column versioning with DAG-based history tracking. Designed for local-first applications requiring offline operation, audit trails, and coordination-free garbage collection.

## Overview

DAG-CRR provides:

- **Per-column versioning**: Fine-grained conflict resolution at column level
- **DAG history tracking**: Complete audit trails with per-column rollback
- **Coordination-free GC**: Each peer garbage collects independently without affecting sync
- **Deterministic conflict resolution**: Symmetric tiebreakers guarantee Strong Eventual Consistency
- **Delta sync**: O(changed columns) merge complexity

## Quick Start

```bash
# Clone the repository
git clone https://github.com/rocky43007/dag-crr-research
cd dag-crr-research

# Run the interactive demo
cargo run --release

# Run benchmarks
cargo bench --bench paper_benchmarks
```

## Usage

```rust
use sync_engine::{SyncEngine, crr::TieBreakPolicy};
use std::collections::HashMap;

// Create two peers
let mut peer_a = SyncEngine::new();
let mut peer_b = SyncEngine::new();

// Insert data on peer A
let mut cols = HashMap::new();
let mut vers = HashMap::new();
cols.insert("name".to_string(), "Alice".to_string());
cols.insert("email".to_string(), "alice@example.com".to_string());
vers.insert("name".to_string(), 1);
vers.insert("email".to_string(), 1);
peer_a.crr_table.insert_or_update("user_1", cols, vers);

// Sync A -> B using CRR merge
let changeset = peer_a.crr_table.changeset();
let report = peer_b.crr_table.crr_merge(&changeset, TieBreakPolicy::LexicographicMin);

// Report shows: inserted=2, updated=0, conflicts=0
println!("Inserted: {}, Conflicts: {}",
    report.inserted.len(),
    report.conflicts_equal_version.len());
```

## Tiebreaker Policies

When two peers assign the same version to different values:

| Policy | Behavior | SEC Guarantee |
|--------|----------|---------------|
| `LexicographicMin` | Lexicographically smaller value wins | Yes (symmetric) |
| `PreferExisting` | Keep local value | No (requires coordination) |
| `PreferIncoming` | Accept remote value | No (requires coordination) |

Use `LexicographicMin` for peer-to-peer sync without coordination.

## Architecture

```
sync-engine/        # Core library (~2,000 lines)
  src/lib.rs        # CRR, DAG, schema, transactions, network
src/                # Demo application (~2,000 lines)
  main.rs           # Entry point
  ui/
    demo.rs         # Interactive GPUI visualization
    theme.rs        # UI theming
benches/            # Benchmarks (~8,000 lines)
  paper_benchmarks.rs
  gc_coordination_demo.rs
  ...
```

## Reproducing Paper Results

The benchmarks reproduce results from the accompanying paper.

```bash
# Core benchmarks (Table 7, 8)
cargo bench --bench paper_benchmarks

# GC coordination comparison (Table 4)
cargo bench --bench gc_coordination_demo

# Automerge comparison (Figure 9)
cargo bench --bench automerge_comparison

# All benchmarks
cargo bench
```

Results are written to `target/criterion/` with HTML reports.

**Hardware used in paper**: Apple M2 Pro (10-core), 16GB RAM, macOS, Rust 1.90.0

## Paper

This implementation accompanies the research paper:

> "Per-Column Conflict Resolution with Coordination-Free Garbage Collection for Replicated Databases"
>
> Arnab Chakraborty, Stony Brook University

## Citation

If you use this software in your research, please cite:

```bibtex
@software{dagcrr2025,
  author = {Chakraborty, Arnab},
  title = {DAG-CRR: Per-Column Conflict Resolution with Coordination-Free Garbage Collection},
  year = {2025},
  url = {https://github.com/rocky43007/dag-crr-research}
}
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by challenges building [Spacedrive](https://spacedrive.com)
- Built with [GPUI](https://github.com/zed-industries/zed) for visualization
- Research conducted at Stony Brook University
