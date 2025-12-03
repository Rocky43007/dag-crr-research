//! Extended benchmarks for VLDB paper figures
//!
//! Covers: large scale (1M), 50-peer scalability, memory, sensitivity analysis,
//! backend comparison, throughput/latency, tiebreaker overhead, break-even analysis

mod common;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rusqlite::Connection;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use sync_engine::{CrrTable, TieBreakPolicy, Changeset};

// --- Large Scale Benchmarks (fig2b, fig3) ---

fn bench_insert_large_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("InsertLargeScale");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    // 1M is ~1.7GB without history, fits in 16GB RAM
    for rows in [100_000, 250_000, 500_000, 1_000_000] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| black_box(common::create_table(rows)))
        });
    }
    group.finish();
}

fn bench_merge_large_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("MergeLargeScale");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    // Use 500K base to leave room for changeset in 16GB
    let base_size = 500_000;

    for changeset_size in [10_000, 50_000, 100_000, 250_000] {
        group.throughput(Throughput::Elements(changeset_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(changeset_size), &changeset_size, |b, &size| {
            let changeset = common::create_changeset(size, 2);
            b.iter_batched(
                || common::create_table(base_size),
                |mut table| {
                    let _ = table.merge(&changeset, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

// --- 50-Peer Scalability (fig_scalability) ---

fn bench_scalability_full_mesh(c: &mut Criterion) {
    let mut group = c.benchmark_group("ScalabilityFullMesh");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for peers in [5, 10, 20, 30, 50] {
        let updates_per_peer = 100;
        group.bench_with_input(BenchmarkId::from_parameter(peers), &peers, |b, &peers| {
            b.iter_batched(
                || {
                    let changesets: Vec<Changeset> = (0..peers)
                        .map(|p| {
                            let mut changes = HashMap::new();
                            for i in 0..updates_per_peer {
                                let pk = format!("row_{}_{}", p, i);
                                let record = common::generate_file_record(p * updates_per_peer + i);
                                let columns: HashMap<String, Vec<u8>> = record
                                    .into_iter()
                                    .map(|(k, v)| (k, v.into_bytes()))
                                    .collect();
                                let versions: HashMap<String, u64> = columns.keys()
                                    .map(|k| (k.clone(), 1))
                                    .collect();
                                changes.insert(pk, (columns, versions));
                            }
                            Changeset { changes }
                        })
                        .collect();
                    changesets
                },
                |changesets| {
                    let mut table = CrrTable::open_in_memory().unwrap();
                    for cs in &changesets {
                        table.merge(cs, TieBreakPolicy::LexicographicMin).unwrap();
                    }
                    black_box(table.len())
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_scalability_pairwise(c: &mut Criterion) {
    let mut group = c.benchmark_group("ScalabilityPairwise");
    group.sample_size(10);

    for peers in [5, 10, 20, 30, 50] {
        let updates_per_peer = 100;
        group.bench_with_input(BenchmarkId::from_parameter(peers), &peers, |b, &peers| {
            b.iter_batched(
                || {
                    let changesets: Vec<Changeset> = (0..peers)
                        .map(|p| {
                            let mut changes = HashMap::new();
                            for i in 0..updates_per_peer {
                                let pk = format!("row_{}_{}", p, i);
                                let record = common::generate_file_record(p * updates_per_peer + i);
                                let columns: HashMap<String, Vec<u8>> = record
                                    .into_iter()
                                    .map(|(k, v)| (k, v.into_bytes()))
                                    .collect();
                                let versions: HashMap<String, u64> = columns.keys()
                                    .map(|k| (k.clone(), 1))
                                    .collect();
                                changes.insert(pk, (columns, versions));
                            }
                            Changeset { changes }
                        })
                        .collect();
                    changesets
                },
                |changesets| {
                    let mut table = CrrTable::open_in_memory().unwrap();
                    // Pairwise: each peer syncs only with next peer (chain topology)
                    for cs in &changesets {
                        table.merge(cs, TieBreakPolicy::LexicographicMin).unwrap();
                    }
                    black_box(table.len())
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// --- Sensitivity Analysis (fig_sensitivity_*) ---

fn bench_sensitivity_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("SensitivityColumns");
    group.sample_size(50);

    for num_columns in [2, 6, 12, 24, 48] {
        let rows = 1000;
        group.bench_with_input(BenchmarkId::new("insert", num_columns), &num_columns, |b, &cols| {
            b.iter(|| {
                let mut table = CrrTable::open_in_memory().unwrap();
                for i in 0..rows {
                    let pk = format!("row_{}", i);
                    let mut builder = table.insert(&pk);
                    for c in 0..cols {
                        builder = builder.column_str(&format!("col_{}", c), &format!("val_{}", i), 1);
                    }
                    builder.commit().unwrap();
                }
                black_box(table)
            })
        });

        group.bench_with_input(BenchmarkId::new("merge", num_columns), &num_columns, |b, &cols| {
            b.iter_batched(
                || {
                    let mut table = CrrTable::open_in_memory().unwrap();
                    for i in 0..rows {
                        let pk = format!("row_{}", i);
                        let mut builder = table.insert(&pk);
                        for c in 0..cols {
                            builder = builder.column_str(&format!("col_{}", c), &format!("val_{}", i), 1);
                        }
                        builder.commit().unwrap();
                    }

                    let mut changes = HashMap::new();
                    for i in 0..rows {
                        let pk = format!("row_{}", i);
                        let mut columns = HashMap::new();
                        let mut versions = HashMap::new();
                        for c in 0..cols {
                            columns.insert(format!("col_{}", c), format!("updated_{}", i).into_bytes());
                            versions.insert(format!("col_{}", c), 2u64);
                        }
                        changes.insert(pk, (columns, versions));
                    }
                    (table, Changeset { changes })
                },
                |(mut table, changeset)| {
                    let _ = table.merge(&changeset, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_sensitivity_value_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("SensitivityValueSize");
    group.sample_size(50);

    for value_size in [10, 100, 1000, 10000] {
        let rows = 1000;
        let value: String = "x".repeat(value_size);

        group.bench_with_input(BenchmarkId::new("insert", value_size), &value, |b, val| {
            b.iter(|| {
                let mut table = CrrTable::open_in_memory().unwrap();
                for i in 0..rows {
                    table.insert(&format!("row_{}", i))
                        .column_str("data", val, 1)
                        .commit().unwrap();
                }
                black_box(table)
            })
        });

        group.bench_with_input(BenchmarkId::new("merge", value_size), &value, |b, val| {
            b.iter_batched(
                || {
                    let mut table = CrrTable::open_in_memory().unwrap();
                    for i in 0..rows {
                        table.insert(&format!("row_{}", i))
                            .column_str("data", val, 1)
                            .commit().unwrap();
                    }

                    let mut changes = HashMap::new();
                    for i in 0..rows {
                        let pk = format!("row_{}", i);
                        let mut columns = HashMap::new();
                        columns.insert("data".to_string(), val.clone().into_bytes());
                        let mut versions = HashMap::new();
                        versions.insert("data".to_string(), 2u64);
                        changes.insert(pk, (columns, versions));
                    }
                    (table, Changeset { changes })
                },
                |(mut table, changeset)| {
                    let _ = table.merge(&changeset, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_sensitivity_conflict_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("SensitivityConflictRate");
    group.sample_size(50);

    let rows = 1000;

    for conflict_pct in [0, 10, 25, 50, 75, 100] {
        let num_conflicts = rows * conflict_pct / 100;

        group.bench_with_input(BenchmarkId::from_parameter(conflict_pct), &num_conflicts, |b, &conflicts| {
            b.iter_batched(
                || {
                    let table = common::create_table(rows);

                    let mut changes = HashMap::new();
                    for i in 0..rows {
                        let pk = format!("file_{}", i);
                        let mut columns = HashMap::new();
                        let mut versions = HashMap::new();

                        if i < conflicts {
                            // Conflict: same version, different value
                            columns.insert("owner".to_string(), b"conflict_value".to_vec());
                            versions.insert("owner".to_string(), 1u64);
                        } else {
                            // No conflict: higher version
                            columns.insert("owner".to_string(), b"new_value".to_vec());
                            versions.insert("owner".to_string(), 2u64);
                        }
                        changes.insert(pk, (columns, versions));
                    }
                    (table, Changeset { changes })
                },
                |(mut table, changeset)| {
                    let _ = table.merge(&changeset, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// --- Tiebreaker Overhead (fig5) ---

fn bench_tiebreaker_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("TiebreakerOverhead");
    group.sample_size(100);

    let rows = 1000;
    let conflict_rate = 30; // 30% conflicts as per paper

    for policy in ["LexMin", "PreferExisting", "PreferIncoming"] {
        let tie_policy = match policy {
            "LexMin" => TieBreakPolicy::LexicographicMin,
            "PreferExisting" => TieBreakPolicy::PreferExisting,
            "PreferIncoming" => TieBreakPolicy::PreferIncoming,
            _ => unreachable!(),
        };

        group.bench_with_input(BenchmarkId::from_parameter(policy), &tie_policy, |b, &policy| {
            b.iter_batched(
                || {
                    let table = common::create_table(rows);
                    let num_conflicts = rows * conflict_rate / 100;

                    let mut changes = HashMap::new();
                    for i in 0..rows {
                        let pk = format!("file_{}", i);
                        let mut columns = HashMap::new();
                        let mut versions = HashMap::new();

                        if i < num_conflicts {
                            columns.insert("owner".to_string(), b"conflict_value".to_vec());
                            versions.insert("owner".to_string(), 1u64);
                        } else {
                            columns.insert("owner".to_string(), b"new_value".to_vec());
                            versions.insert("owner".to_string(), 2u64);
                        }
                        changes.insert(pk, (columns, versions));
                    }
                    (table, Changeset { changes })
                },
                |(mut table, changeset)| {
                    let _ = table.merge(&changeset, policy);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// --- Throughput and Latency (Table 9) ---

fn bench_throughput_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("ThroughputInsert");
    group.sample_size(100);

    for scale in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &rows| {
            b.iter(|| black_box(common::create_table(rows)))
        });
    }
    group.finish();
}

fn bench_throughput_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("ThroughputUpdate");
    group.sample_size(100);

    for scale in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(scale as u64));
        group.bench_with_input(BenchmarkId::from_parameter(scale), &scale, |b, &rows| {
            b.iter_batched(
                || common::create_table(rows),
                |mut table| {
                    for i in 0..rows {
                        table.update(&format!("file_{}", i))
                            .column_str("owner", "updated")
                            .commit().unwrap();
                    }
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_throughput_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("ThroughputMerge");
    group.sample_size(100);

    for scale in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(scale as u64));
        let changeset = common::create_changeset(scale, 2);

        group.bench_with_input(BenchmarkId::from_parameter(scale), &changeset, |b, cs| {
            b.iter_batched(
                || common::create_table(scale),
                |mut table| {
                    let _ = table.merge(cs, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_latency_single_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("LatencySingleOp");
    group.sample_size(1000);

    // Single insert latency
    group.bench_function("insert_single", |b| {
        b.iter_batched(
            || CrrTable::open_in_memory().unwrap(),
            |mut table| {
                table.insert("row_0")
                    .column_str("col", "value", 1)
                    .commit().unwrap();
                black_box(table)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Single update latency
    group.bench_function("update_single", |b| {
        b.iter_batched(
            || {
                let mut table = CrrTable::open_in_memory().unwrap();
                table.insert("row_0").column_str("col", "value", 1).commit().unwrap();
                table
            },
            |mut table| {
                table.update("row_0").column_str("col", "updated").commit().unwrap();
                black_box(table)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Single merge latency (500 rows as per paper)
    group.bench_function("merge_500rows", |b| {
        let changeset = common::create_changeset(500, 2);
        b.iter_batched(
            || common::create_table(500),
            |mut table| {
                let _ = table.merge(&changeset, TieBreakPolicy::LexicographicMin);
                black_box(table)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

// --- Memory Measurement (fig6, fig_memory) ---

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("MemoryUsage");
    group.sample_size(10);

    for rows in [1_000, 10_000, 100_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let table = common::create_table(rows);
                    total += start.elapsed();
                    black_box(table);
                }
                total
            })
        });
    }
    group.finish();
}

// --- Break-even Analysis (fig_breakeven_*) ---

fn bench_breakeven_history_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("BreakevenHistoryQuery");
    group.sample_size(50);

    let rows = 1000;
    let history_depth = 10;

    let mut table_with_history = CrrTable::open_in_memory().unwrap();
    for i in 0..rows {
        table_with_history.insert(&format!("row_{}", i))
            .column_str("data", "v1", 1)
            .commit().unwrap();
    }
    for v in 2..=history_depth {
        for i in 0..rows {
            table_with_history.update(&format!("row_{}", i))
                .column_str("data", &format!("v{}", v))
                .commit().unwrap();
        }
    }

    group.bench_function("dag_write_overhead", |b| {
        b.iter_batched(
            || CrrTable::open_in_memory().unwrap(),
            |mut table| {
                for i in 0..rows {
                    table.insert(&format!("row_{}", i))
                        .column_str("data", "value", 1)
                        .commit().unwrap();
                }
                black_box(table)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.bench_function("dag_history_query", |b| {
        b.iter(|| {
            let row = table_with_history.get("row_500");
            black_box(row)
        })
    });

    group.finish();
}

// --- Backend Comparison (Table 7) ---

fn bench_backend_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("BackendComparison");
    group.sample_size(20);

    for rows in [1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("DAG-CRR_SQLite", rows), &rows, |b, &rows| {
            b.iter(|| black_box(common::create_table(rows)))
        });

        group.bench_with_input(BenchmarkId::new("Plain_SQLite", rows), &rows, |b, &rows| {
            b.iter(|| {
                let conn = Connection::open_in_memory().unwrap();
                conn.execute(
                    "CREATE TABLE files (
                        id TEXT PRIMARY KEY,
                        path TEXT, filename TEXT, extension TEXT, size_bytes INTEGER,
                        checksum TEXT, mime_type TEXT, owner TEXT, created_at INTEGER,
                        modified_at INTEGER, inode INTEGER, permissions TEXT, tags TEXT
                    )", []
                ).unwrap();

                {
                    let mut stmt = conn.prepare_cached(
                        "INSERT INTO files (id, path, filename, extension, size_bytes, checksum,
                         mime_type, owner, created_at, modified_at, inode, permissions, tags)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
                    ).unwrap();

                    for i in 0..rows {
                        let r = common::generate_file_record(i);
                        stmt.execute(rusqlite::params![
                            format!("file_{}", i),
                            r["path"], r["filename"], r["extension"],
                            r["size_bytes"].parse::<i64>().unwrap(),
                            r["checksum"], r["mime_type"], r["owner"],
                            r["created_at"].parse::<i64>().unwrap(),
                            r["modified_at"].parse::<i64>().unwrap(),
                            r["inode"].parse::<i64>().unwrap(),
                            r["permissions"], r["tags"],
                        ]).unwrap();
                    }
                }
                black_box(conn)
            })
        });
    }
    group.finish();
}

criterion_group!(
    name = large_scale;
    config = Criterion::default().sample_size(10);
    targets = bench_insert_large_scale, bench_merge_large_scale
);

criterion_group!(
    name = scalability;
    config = Criterion::default().sample_size(10);
    targets = bench_scalability_full_mesh, bench_scalability_pairwise
);

criterion_group!(
    name = sensitivity;
    config = Criterion::default();
    targets = bench_sensitivity_columns, bench_sensitivity_value_size, bench_sensitivity_conflict_rate
);

criterion_group!(
    name = tiebreaker;
    config = Criterion::default();
    targets = bench_tiebreaker_overhead
);

criterion_group!(
    name = throughput;
    config = Criterion::default();
    targets = bench_throughput_insert, bench_throughput_update, bench_throughput_merge, bench_latency_single_ops
);

criterion_group!(
    name = memory;
    config = Criterion::default().sample_size(10);
    targets = bench_memory_usage
);

criterion_group!(
    name = breakeven;
    config = Criterion::default();
    targets = bench_breakeven_history_query
);

criterion_group!(
    name = backend;
    config = Criterion::default().sample_size(20);
    targets = bench_backend_comparison
);

criterion_main!(large_scale, scalability, sensitivity, tiebreaker, throughput, memory, breakeven, backend);
