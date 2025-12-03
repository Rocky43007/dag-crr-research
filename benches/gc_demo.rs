//! Coordination-free GC demonstration
//!
//! Shows DAG-CRR's key advantage: peers can GC independently without coordination.

mod common;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use sync_engine::{CrrTable, TieBreakPolicy, run_gc, GcPolicy};

fn bench_gc_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("GC");

    for keep in [5, 10, 25, 50] {
        group.bench_with_input(BenchmarkId::new("keep_versions", keep), &keep, |b, &keep| {
            b.iter_batched(
                || {
                    let mut table = CrrTable::open_in_memory().unwrap();
                    for i in 0..100 {
                        table.insert(&format!("row_{}", i))
                            .column_str("val", "v1", 1)
                            .commit().unwrap();
                    }
                    for v in 2..=100 {
                        for i in 0..100 {
                            table.update(&format!("row_{}", i))
                                .column_str("val", &format!("v{}", v))
                                .commit().unwrap();
                        }
                    }
                    table
                },
                |mut table| {
                    black_box(run_gc(&mut table, GcPolicy::KeepLast(keep)))
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_sync_after_asymmetric_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("SyncAfterGC");
    group.sample_size(50);

    group.bench_function("peer_a_gc5_peer_b_gc50", |b| {
        b.iter_batched(
            || {
                let mut peer_a = CrrTable::open_in_memory().unwrap();
                let mut peer_b = CrrTable::open_in_memory().unwrap();

                for i in 0..100 {
                    let pk = format!("row_{}", i);
                    peer_a.insert(&pk).column_str("val", "v1", 1).commit().unwrap();
                    peer_b.insert(&pk).column_str("val", "v1", 1).commit().unwrap();
                }

                for v in 2..=50 {
                    for i in 0..100 {
                        let pk = format!("row_{}", i);
                        peer_a.update(&pk).column_str("val", &format!("v{}", v)).commit().unwrap();
                        peer_b.update(&pk).column_str("val", &format!("v{}", v)).commit().unwrap();
                    }
                }

                run_gc(&mut peer_a, GcPolicy::KeepLast(5)).unwrap();
                run_gc(&mut peer_b, GcPolicy::KeepLast(50)).unwrap();

                for i in 0..50 {
                    peer_a.update(&format!("row_{}", i))
                        .column_str("val", "peer_a_update")
                        .commit().unwrap();
                }
                for i in 50..100 {
                    peer_b.update(&format!("row_{}", i))
                        .column_str("val", "peer_b_update")
                        .commit().unwrap();
                }

                let cs_a = peer_a.changeset().unwrap();
                let cs_b = peer_b.changeset().unwrap();
                (peer_a, peer_b, cs_a, cs_b)
            },
            |(mut peer_a, mut peer_b, cs_a, cs_b)| {
                peer_a.merge(&cs_b, TieBreakPolicy::LexicographicMin).unwrap();
                peer_b.merge(&cs_a, TieBreakPolicy::LexicographicMin).unwrap();
                black_box((peer_a.len(), peer_b.len()))
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_gc_then_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("GCThenMerge");

    for gc_depth in [5, 10, 25] {
        group.bench_with_input(BenchmarkId::from_parameter(gc_depth), &gc_depth, |b, &gc_depth| {
            b.iter_batched(
                || {
                    let mut table = CrrTable::open_in_memory().unwrap();
                    for i in 0..1000 {
                        table.insert(&format!("row_{}", i))
                            .column_str("val", "v1", 1)
                            .commit().unwrap();
                    }
                    for v in 2..=50 {
                        for i in 0..1000 {
                            table.update(&format!("row_{}", i))
                                .column_str("val", &format!("v{}", v))
                                .commit().unwrap();
                        }
                    }
                    run_gc(&mut table, GcPolicy::KeepLast(gc_depth)).unwrap();

                    let changeset = common::create_changeset(500, 51);
                    (table, changeset)
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

criterion_group!(benches, bench_gc_performance, bench_sync_after_asymmetric_gc, bench_gc_then_merge);
criterion_main!(benches);
