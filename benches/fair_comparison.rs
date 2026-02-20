//! Fair comparison: DAG-CRR vs CR-SQLite (both SQLite-backed)

mod common;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rusqlite::Connection;
use std::time::Duration;
use sync_engine::TieBreakPolicy;

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("Insert");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    let conn = Connection::open_in_memory().unwrap();
    let has_crsqlite = common::load_crsqlite(&conn);

    for rows in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_with_input(BenchmarkId::new("DAG-CRR", rows), &rows, |b, &rows| {
            b.iter(|| black_box(common::create_table(rows)))
        });

        group.bench_with_input(BenchmarkId::new("DAG-CRR_SQLite", rows), &rows, |b, &rows| {
            b.iter(|| black_box(common::create_table_sqlite(rows)))
        });

        if has_crsqlite {
            group.bench_with_input(BenchmarkId::new("CR-SQLite", rows), &rows, |b, &rows| {
                b.iter_batched(
                    || {
                        let conn = Connection::open_in_memory().unwrap();
                        common::load_crsqlite(&conn);
                        common::setup_crsqlite_table(&conn);
                        conn
                    },
                    |conn| {
                        common::crsqlite_insert(&conn, rows);
                        black_box(conn)
                    },
                    criterion::BatchSize::SmallInput,
                )
            });
        }
    }

    if !has_crsqlite {
        eprintln!("CR-SQLite not found. Run: ./scripts/setup_crsqlite.sh");
    }
    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("Merge");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    let conn = Connection::open_in_memory().unwrap();
    let has_crsqlite = common::load_crsqlite(&conn);

    for size in [100, 1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("DAG-CRR", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let table = common::create_table(10_000);
                    let changeset = common::create_changeset(size, 2);
                    (table, changeset)
                },
                |(mut table, changeset)| {
                    black_box(table.merge(&changeset, TieBreakPolicy::LexicographicMin))
                },
                criterion::BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("DAG-CRR_SQLite", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let table = common::create_table_sqlite(10_000);
                    let changeset = common::create_changeset_for_sqlite(size, 2);
                    (table, changeset)
                },
                |(mut table, changeset)| {
                    black_box(table.merge(&changeset, TieBreakPolicy::LexicographicMin))
                },
                criterion::BatchSize::SmallInput,
            )
        });

        if has_crsqlite {
            group.bench_with_input(BenchmarkId::new("CR-SQLite", size), &size, |b, &size| {
                b.iter_batched(
                    || {
                        let conn = Connection::open_in_memory().unwrap();
                        common::load_crsqlite(&conn);
                        common::setup_crsqlite_table(&conn);
                        common::crsqlite_insert(&conn, 10_000);
                        conn
                    },
                    |conn| {
                        {
                            let mut stmt = conn.prepare_cached(
                                "UPDATE files SET modified_at = ?1, tags = ?2 WHERE id = ?3"
                            ).unwrap();
                            for i in 0..size {
                                stmt.execute(rusqlite::params![
                                    1704067200i64 + i as i64 + 86400,
                                    "updated,synced",
                                    format!("file_{}", i),
                                ]).unwrap();
                            }
                        }
                        black_box(conn)
                    },
                    criterion::BatchSize::SmallInput,
                )
            });
        }
    }
    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read");
    group.sample_size(100);

    let conn = Connection::open_in_memory().unwrap();
    let has_crsqlite = common::load_crsqlite(&conn);

    for rows in [1_000, 10_000] {
        group.bench_with_input(BenchmarkId::new("DAG-CRR", rows), &rows, |b, &rows| {
            let table = common::create_table(rows);
            b.iter(|| {
                for i in 0..100 {
                    let _ = black_box(table.get(&format!("file_{}", i % rows)));
                }
            })
        });

        group.bench_with_input(BenchmarkId::new("DAG-CRR_SQLite", rows), &rows, |b, &rows| {
            let table = common::create_table_sqlite(rows);
            b.iter(|| {
                for i in 0..100 {
                    let _ = black_box(table.get(&format!("file_{}", i % rows)));
                }
            })
        });

        if has_crsqlite {
            group.bench_with_input(BenchmarkId::new("CR-SQLite", rows), &rows, |b, &rows| {
                let conn = Connection::open_in_memory().unwrap();
                common::load_crsqlite(&conn);
                common::setup_crsqlite_table(&conn);
                common::crsqlite_insert(&conn, rows);

                b.iter(|| {
                    let mut stmt = conn.prepare_cached("SELECT * FROM files WHERE id = ?1").unwrap();
                    for i in 0..100 {
                        let _: Option<String> = stmt
                            .query_row([format!("file_{}", i % rows)], |r| r.get(1))
                            .ok();
                    }
                    black_box(())
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_insert, bench_merge, bench_read);
criterion_main!(benches);
