//! Core benchmarks for VLDB paper (Tables 3-7)

mod common;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::time::Duration;
use sync_engine::{CrrTable, TieBreakPolicy, Changeset};

fn bench_merge_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("MergeLatency");
    group.measurement_time(Duration::from_secs(10));

    for size in [100, 1_000, 5_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let table = common::create_table(10_000);
                    let changeset = common::create_changeset(size, 2);
                    (table, changeset)
                },
                |(mut table, changeset)| {
                    black_box(table.merge(&changeset, TieBreakPolicy::LexicographicMin).unwrap())
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("InsertThroughput");
    group.measurement_time(Duration::from_secs(10));

    for rows in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| black_box(common::create_table(rows)))
        });
    }
    group.finish();
}

fn bench_multi_peer_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("MultiPeerSync");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for peers in [2, 4, 8, 16] {
        let rows_per_peer = 1000;
        group.throughput(Throughput::Elements((peers * rows_per_peer) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(peers), &peers, |b, &peers| {
            b.iter_batched(
                || {
                    let table = CrrTable::open_in_memory().unwrap();
                    let changesets: Vec<_> = (0..peers)
                        .map(|p| {
                            let mut changes = HashMap::new();
                            for i in 0..rows_per_peer {
                                let idx = p * rows_per_peer + i;
                                let pk = format!("file_{}", idx);
                                let record = common::generate_file_record(idx);
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
                    (table, changesets)
                },
                |(mut table, changesets)| {
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

fn bench_conflict_resolution(c: &mut Criterion) {
    let mut group = c.benchmark_group("ConflictResolution");
    group.measurement_time(Duration::from_secs(10));

    for conflict_pct in [10, 25, 50, 75, 100] {
        let base_rows = 1000;
        let conflicts = base_rows * conflict_pct / 100;
        group.throughput(Throughput::Elements(conflicts as u64));
        group.bench_with_input(BenchmarkId::from_parameter(conflict_pct), &conflict_pct, |b, &pct| {
            let num_conflicts = base_rows * pct / 100;
            b.iter_batched(
                || {
                    let table = common::create_table(base_rows);
                    let mut changes = HashMap::new();
                    for i in 0..num_conflicts {
                        let pk = format!("file_{}", i);
                        let mut columns = HashMap::new();
                        columns.insert("modified_at".to_string(), format!("{}", 1800000000 + i).into_bytes());
                        let mut versions = HashMap::new();
                        versions.insert("modified_at".to_string(), 1u64);
                        changes.insert(pk, (columns, versions));
                    }
                    (table, Changeset { changes })
                },
                |(mut table, changeset)| {
                    black_box(table.merge(&changeset, TieBreakPolicy::LexicographicMin))
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_dag_depth(c: &mut Criterion) {
    let mut group = c.benchmark_group("DAGDepth");
    group.sample_size(50);

    for depth in [5, 10, 25, 50, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(depth), &depth, |b, &depth| {
            b.iter_batched(
                || CrrTable::open_in_memory().unwrap(),
                |mut table| {
                    table.insert("row").column_str("val", "v0", 1).commit().unwrap();
                    for v in 2..=depth {
                        table.update("row").column_str("val", &format!("v{}", v)).commit().unwrap();
                    }
                    black_box(table.get("row"))
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_merge_latency,
    bench_insert_throughput,
    bench_multi_peer_sync,
    bench_conflict_resolution,
    bench_dag_depth,
);
criterion_main!(benches);
