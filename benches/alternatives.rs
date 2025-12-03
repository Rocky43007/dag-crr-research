//! Alternative sync mechanisms comparison: Automerge, HLC, LWW+Audit

mod common;

use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjType, ReadDoc, ROOT};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::time::Duration;
use sync_engine::{CrrTable, TieBreakPolicy};

// --- HLC Implementation ---

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HLC {
    pub wall_time: u64,
    pub logical: u32,
    pub node_id: u16,
}

impl HLC {
    pub fn new(node_id: u16) -> Self {
        Self { wall_time: Self::now_millis(), logical: 0, node_id }
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub fn tick(&mut self) -> HLC {
        let now = Self::now_millis();
        if now > self.wall_time {
            self.wall_time = now;
            self.logical = 0;
        } else {
            self.logical += 1;
        }
        *self
    }

    pub fn receive(&mut self, remote: HLC) -> HLC {
        let now = Self::now_millis();
        if now > self.wall_time && now > remote.wall_time {
            self.wall_time = now;
            self.logical = 0;
        } else if self.wall_time > remote.wall_time {
            self.logical += 1;
        } else if remote.wall_time > self.wall_time {
            self.wall_time = remote.wall_time;
            self.logical = remote.logical + 1;
        } else {
            self.logical = self.logical.max(remote.logical) + 1;
        }
        *self
    }
}

#[derive(Clone, Debug)]
pub struct HlcRow {
    pub columns: HashMap<String, String>,
    pub hlc: HLC,
}

#[derive(Clone, Debug, Default)]
pub struct HlcTable {
    pub rows: HashMap<String, HlcRow>,
    pub clock: Option<HLC>,
}

impl HlcTable {
    pub fn new(node_id: u16) -> Self {
        Self { rows: HashMap::new(), clock: Some(HLC::new(node_id)) }
    }

    pub fn insert(&mut self, pk: &str, columns: HashMap<String, String>) {
        let hlc = self.clock.as_mut().map(|c| c.tick()).unwrap_or(HLC {
            wall_time: 0, logical: 0, node_id: 0,
        });
        self.rows.insert(pk.to_string(), HlcRow { columns, hlc });
    }

    pub fn merge(&mut self, incoming: &HashMap<String, (HashMap<String, String>, HLC)>) {
        for (pk, (cols, remote_hlc)) in incoming {
            if let Some(local_row) = self.rows.get_mut(pk) {
                if remote_hlc > &local_row.hlc {
                    local_row.columns = cols.clone();
                    local_row.hlc = *remote_hlc;
                }
                if let Some(ref mut clock) = self.clock {
                    clock.receive(*remote_hlc);
                }
            } else {
                self.rows.insert(pk.clone(), HlcRow { columns: cols.clone(), hlc: *remote_hlc });
            }
        }
    }
}

// --- LWW + Audit Implementation ---

#[derive(Clone)]
struct LwwWithAudit {
    rows: HashMap<String, LwwRow>,
    audit_log: Vec<AuditEntry>,
    timestamp: u64,
}

#[derive(Clone)]
struct LwwRow {
    data: HashMap<String, String>,
    last_modified: u64,
}

#[derive(Clone)]
#[allow(dead_code)]
struct AuditEntry {
    timestamp: u64,
    row_id: String,
    old_values: Option<HashMap<String, String>>,
    new_values: HashMap<String, String>,
}

impl LwwWithAudit {
    fn new() -> Self {
        Self { rows: HashMap::new(), audit_log: Vec::new(), timestamp: 0 }
    }

    fn insert(&mut self, pk: &str, data: HashMap<String, String>) {
        self.timestamp += 1;
        self.audit_log.push(AuditEntry {
            timestamp: self.timestamp,
            row_id: pk.to_string(),
            old_values: None,
            new_values: data.clone(),
        });
        self.rows.insert(pk.to_string(), LwwRow { data, last_modified: self.timestamp });
    }

    fn update(&mut self, pk: &str, data: HashMap<String, String>) {
        self.timestamp += 1;
        let old_values = self.rows.get(pk).map(|r| r.data.clone());
        self.audit_log.push(AuditEntry {
            timestamp: self.timestamp,
            row_id: pk.to_string(),
            old_values,
            new_values: data.clone(),
        });
        if let Some(row) = self.rows.get_mut(pk) {
            row.data = data;
            row.last_modified = self.timestamp;
        }
    }

    #[allow(dead_code)]
    fn merge_lww(&mut self, remote: &LwwWithAudit) {
        for (pk, remote_row) in &remote.rows {
            match self.rows.get(pk) {
                Some(local_row) => {
                    if remote_row.last_modified > local_row.last_modified {
                        self.timestamp += 1;
                        self.audit_log.push(AuditEntry {
                            timestamp: self.timestamp,
                            row_id: pk.clone(),
                            old_values: Some(local_row.data.clone()),
                            new_values: remote_row.data.clone(),
                        });
                        self.rows.insert(pk.clone(), remote_row.clone());
                    }
                }
                None => {
                    self.timestamp += 1;
                    self.audit_log.push(AuditEntry {
                        timestamp: self.timestamp,
                        row_id: pk.clone(),
                        old_values: None,
                        new_values: remote_row.data.clone(),
                    });
                    self.rows.insert(pk.clone(), remote_row.clone());
                }
            }
        }
    }

    fn get_column_history(&self, pk: &str, column: &str) -> Vec<(u64, String)> {
        let mut history = Vec::new();
        for entry in &self.audit_log {
            if entry.row_id == pk {
                if let Some(val) = entry.new_values.get(column) {
                    history.push((entry.timestamp, val.clone()));
                }
            }
        }
        history
    }
}

// --- Automerge Benchmarks ---

fn bench_automerge_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("Automerge_Insert");
    group.sample_size(50);

    for size in [100, 1_000, 5_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("DAG-CRR", size), &size, |b, &size| {
            b.iter(|| black_box(common::create_table(size)))
        });

        group.bench_with_input(BenchmarkId::new("Automerge", size), &size, |b, &size| {
            b.iter(|| {
                let mut doc = AutoCommit::new();
                let files = doc.put_object(ROOT, "files", ObjType::Map).unwrap();

                for i in 0..size {
                    let record = common::generate_file_record(i);
                    let pk = format!("file_{}", i);
                    let file_obj = doc.put_object(&files, &pk, ObjType::Map).unwrap();
                    for (key, value) in record {
                        doc.put(&file_obj, &key, value).unwrap();
                    }
                }
                black_box(doc)
            })
        });
    }
    group.finish();
}

fn bench_automerge_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("Automerge_Merge");
    group.sample_size(50);

    let base_size = 1_000;

    for changeset_size in [50, 100, 500] {
        group.throughput(Throughput::Elements(changeset_size as u64));

        let dag_changeset = common::create_changeset(changeset_size, 2);

        let mut am_base = AutoCommit::new();
        let files = am_base.put_object(ROOT, "files", ObjType::Map).unwrap();
        for i in 0..base_size {
            let record = common::generate_file_record(i);
            let pk = format!("file_{}", i);
            let file_obj = am_base.put_object(&files, &pk, ObjType::Map).unwrap();
            for (key, value) in record {
                am_base.put(&file_obj, &key, value).unwrap();
            }
        }

        let mut am_remote = am_base.fork();
        let remote_files = am_remote.get(ROOT, "files").unwrap().unwrap().1;
        for i in 0..changeset_size {
            let pk = format!("file_{}", i);
            if let Some((_, file_obj)) = am_remote.get(&remote_files, &pk).unwrap() {
                am_remote.put(&file_obj, "owner", "remote_user").unwrap();
            }
        }
        let am_changes = am_remote.save_incremental();

        group.bench_with_input(BenchmarkId::new("DAG-CRR", changeset_size), &dag_changeset, |b, cs| {
            b.iter_batched(
                || common::create_table(base_size),
                |mut table| {
                    let _ = table.merge(cs, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("Automerge", changeset_size), &am_changes, |b, changes| {
            b.iter_batched(
                || am_base.clone(),
                |mut local| {
                    local.load_incremental(changes).unwrap();
                    black_box(local)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_automerge_doc_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("Automerge_DocSize");
    group.sample_size(10);

    println!("\n=== Document Size Comparison ===\n");

    for size in [100, 1_000, 5_000, 10_000] {
        let mut am_doc = AutoCommit::new();
        let files = am_doc.put_object(ROOT, "files", ObjType::Map).unwrap();
        for i in 0..size {
            let record = common::generate_file_record(i);
            let pk = format!("file_{}", i);
            let file_obj = am_doc.put_object(&files, &pk, ObjType::Map).unwrap();
            for (key, value) in record {
                am_doc.put(&file_obj, &key, value).unwrap();
            }
        }

        let am_bytes = am_doc.save();
        println!("{} rows: Automerge {} KB", size, am_bytes.len() / 1024);

        group.bench_with_input(BenchmarkId::new("DAG-CRR_create", size), &size, |b, &size| {
            b.iter(|| black_box(common::create_table(size)))
        });

        group.bench_with_input(BenchmarkId::new("Automerge_save", size), &am_doc, |b, doc| {
            b.iter(|| {
                let mut doc_clone = doc.clone();
                black_box(doc_clone.save())
            })
        });
    }
    group.finish();
}

// --- HLC Benchmarks ---

fn hlc_insert_rows(count: usize) -> HlcTable {
    let mut table = HlcTable::new(1);
    for i in 0..count {
        table.insert(&format!("file_{}", i), common::generate_file_record(i));
    }
    table
}

fn hlc_create_changeset(count: usize, node_id: u16) -> HashMap<String, (HashMap<String, String>, HLC)> {
    let mut clock = HLC::new(node_id);
    (0..count)
        .map(|i| {
            let mut record = common::generate_file_record(i);
            record.insert("modified_at".into(), (1704067200 + i as u64 + 86400).to_string());
            (format!("file_{}", i), (record, clock.tick()))
        })
        .collect()
}

fn bench_hlc_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("HLC_Insert");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for row_count in [10_000, 50_000, 100_000] {
        group.throughput(Throughput::Elements(row_count as u64));

        group.bench_with_input(BenchmarkId::new("DAG-CRR", row_count), &row_count, |b, &count| {
            b.iter(|| black_box(common::create_table(count)))
        });

        group.bench_with_input(BenchmarkId::new("HLC-LWW", row_count), &row_count, |b, &count| {
            b.iter(|| black_box(hlc_insert_rows(count)))
        });
    }
    group.finish();
}

fn bench_hlc_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("HLC_Merge");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(30);

    let base_size = 100_000;
    let hlc_base = hlc_insert_rows(base_size);

    for changeset_size in [1_000, 10_000, 50_000] {
        group.throughput(Throughput::Elements(changeset_size as u64));

        let dagcrr_changeset = common::create_changeset(changeset_size, 2);
        let hlc_changeset = hlc_create_changeset(changeset_size, 2);

        group.bench_with_input(BenchmarkId::new("DAG-CRR", changeset_size), &dagcrr_changeset, |b, cs| {
            b.iter_batched(
                || common::create_table(base_size),
                |mut table| {
                    let _ = table.merge(cs, TieBreakPolicy::LexicographicMin);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("HLC-LWW", changeset_size), &hlc_changeset, |b, cs| {
            b.iter_batched(
                || hlc_base.clone(),
                |mut table| {
                    table.merge(cs);
                    black_box(table)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// --- LWW+Audit Benchmarks ---

fn lww_insert_rows(count: usize) -> LwwWithAudit {
    let mut table = LwwWithAudit::new();
    for i in 0..count {
        table.insert(&format!("file_{}", i), common::generate_file_record(i));
    }
    table
}

fn bench_lww_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("LWW_Audit_Insert");
    group.sample_size(50);

    for size in [10_000, 50_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("DAG-CRR", size), &size, |b, &size| {
            b.iter(|| black_box(common::create_table(size)))
        });

        group.bench_with_input(BenchmarkId::new("LWW+Audit", size), &size, |b, &size| {
            b.iter(|| black_box(lww_insert_rows(size)))
        });
    }
    group.finish();
}

fn bench_lww_history_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("LWW_Audit_History");
    group.sample_size(50);

    let base_size = 10_000;
    let updates_per_row = 10;

    let mut dag_table = CrrTable::open_in_memory().unwrap();
    for i in 0..base_size {
        let record = common::generate_file_record(i);
        let pk = format!("file_{}", i);
        let mut builder = dag_table.insert(&pk);
        for (col, value) in &record {
            builder = builder.column_str(col, value, 1);
        }
        builder.commit().unwrap();
    }

    for v in 2..=updates_per_row {
        for i in 0..base_size {
            dag_table.update(&format!("file_{}", i))
                .column_str("owner", &format!("user_v{}", v))
                .commit().unwrap();
        }
    }

    let mut lww_table = LwwWithAudit::new();
    for i in 0..base_size {
        lww_table.insert(&format!("file_{}", i), common::generate_file_record(i));
    }

    for v in 2..=updates_per_row {
        for i in 0..base_size {
            let mut record = common::generate_file_record(i);
            record.insert("owner".into(), format!("user_v{}", v));
            lww_table.update(&format!("file_{}", i), record);
        }
    }

    group.bench_function("DAG-CRR_column_history", |b| {
        b.iter(|| {
            let row = dag_table.get("file_5000");
            black_box(row)
        })
    });

    group.bench_function("LWW+Audit_column_history", |b| {
        b.iter(|| {
            let history = lww_table.get_column_history("file_5000", "owner");
            black_box(history)
        })
    });

    group.finish();
}

fn bench_concurrent_edit_preservation(c: &mut Criterion) {
    let mut group = c.benchmark_group("ConcurrentEditPreservation");
    group.sample_size(20);

    let rows = 10_000;

    group.bench_function("DAG-CRR", |b| {
        b.iter(|| {
            let mut peer_a = CrrTable::open_in_memory().unwrap();
            let mut peer_b = CrrTable::open_in_memory().unwrap();

            for i in 0..rows {
                let pk = format!("file_{}", i);
                peer_a.insert(&pk)
                    .column_str("owner", "alice", 1)
                    .column_str("tags", "work", 1)
                    .commit().unwrap();
                peer_b.insert(&pk)
                    .column_str("owner", "alice", 1)
                    .column_str("tags", "work", 1)
                    .commit().unwrap();
            }

            for i in 0..rows {
                peer_a.update(&format!("file_{}", i))
                    .column_str("owner", "bob")
                    .commit().unwrap();
                peer_b.update(&format!("file_{}", i))
                    .column_str("tags", "urgent")
                    .commit().unwrap();
            }

            let cs_a = peer_a.changeset().unwrap();
            let cs_b = peer_b.changeset().unwrap();
            peer_a.merge(&cs_b, TieBreakPolicy::LexicographicMin).unwrap();
            peer_b.merge(&cs_a, TieBreakPolicy::LexicographicMin).unwrap();

            let mut preserved = 0;
            for i in 0..rows {
                let row = peer_a.get(&format!("file_{}", i)).unwrap().unwrap();
                if row.get_string("owner").as_deref() == Some("bob") { preserved += 1; }
                if row.get_string("tags").as_deref() == Some("urgent") { preserved += 1; }
            }
            black_box(preserved)
        })
    });

    group.bench_function("HLC-LWW", |b| {
        b.iter(|| {
            let mut peer_a = HlcTable::new(1);
            let mut peer_b = HlcTable::new(2);

            for i in 0..rows {
                let pk = format!("file_{}", i);
                let mut cols = HashMap::new();
                cols.insert("owner".into(), "alice".into());
                cols.insert("tags".into(), "work".into());
                peer_a.insert(&pk, cols.clone());
                peer_b.insert(&pk, cols);
            }

            for i in 0..rows {
                let pk = format!("file_{}", i);
                let mut cols = peer_a.rows.get(&pk).unwrap().columns.clone();
                cols.insert("owner".into(), "bob".into());
                peer_a.insert(&pk, cols);
            }

            for i in 0..rows {
                let pk = format!("file_{}", i);
                let mut cols = peer_b.rows.get(&pk).unwrap().columns.clone();
                cols.insert("tags".into(), "urgent".into());
                peer_b.insert(&pk, cols);
            }

            let remote: HashMap<_, _> = peer_b.rows.iter()
                .map(|(k, v)| (k.clone(), (v.columns.clone(), v.hlc)))
                .collect();
            peer_a.merge(&remote);

            let mut preserved = 0;
            for i in 0..rows {
                let row = &peer_a.rows[&format!("file_{}", i)];
                if row.columns.get("owner") == Some(&"bob".into()) { preserved += 1; }
                if row.columns.get("tags") == Some(&"urgent".into()) { preserved += 1; }
            }
            black_box(preserved)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_automerge_insert,
    bench_automerge_merge,
    bench_automerge_doc_size,
    bench_hlc_insert,
    bench_hlc_merge,
    bench_lww_insert,
    bench_lww_history_query,
    bench_concurrent_edit_preservation,
);

criterion_main!(benches);
