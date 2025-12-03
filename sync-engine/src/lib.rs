mod error;
mod gc;
mod merge;
mod row;
mod storage;
mod sync;
mod table;

pub use error::{Error, Result};
pub use gc::{run_gc, GcPolicy};
pub use merge::{MergeReport, TieBreakPolicy};
pub use row::{InsertBuilder, RowView, UpdateBuilder};
pub use storage::{Cell, DagNode, MemoryStorage, Row, SqliteStorage, Storage};
pub use sync::{Changeset, HeadExchange, MeshSync, SyncResult, SyncSession};
pub use table::CrrTable;

// Re-export legacy types for backward compatibility with existing benchmarks and demo
pub mod crr {
    pub use crate::merge::{MergeReport, TieBreakPolicy};
    pub use crate::table::CrrTable;

    use std::collections::HashMap;

    // Legacy MergeReport with Vec fields for compatibility
    #[derive(Clone, Debug, Default)]
    pub struct LegacyMergeReport {
        pub inserted: Vec<(String, String)>,
        pub updated: Vec<(String, String, u64)>,
        pub skipped_older: Vec<(String, String, u64)>,
        pub conflicts_equal_version: Vec<(String, String, u64, String, String)>,
    }

    // Legacy CrrRow for UI compatibility
    #[derive(Clone, Debug)]
    pub struct CrrRow {
        pub pk: String,
        pub columns: HashMap<String, String>,
        pub versions: HashMap<String, u64>,
        pub dags: HashMap<String, crate::dag::VersionDag>,
    }

    // Legacy CrrTable that wraps the new implementation
    #[derive(Clone, Debug, Default)]
    pub struct LegacyCrrTable {
        pub rows: HashMap<String, CrrRow>,
    }

    impl LegacyCrrTable {
        pub fn new() -> Self {
            Self { rows: HashMap::new() }
        }

        pub fn insert_or_update(
            &mut self,
            pk: &str,
            columns: HashMap<String, String>,
            versions: HashMap<String, u64>,
        ) {
            let row = self.rows.entry(pk.to_string()).or_insert_with(|| CrrRow {
                pk: pk.to_string(),
                columns: HashMap::new(),
                versions: HashMap::new(),
                dags: HashMap::new(),
            });

            for (col, val) in &columns {
                let version = versions.get(col).copied().unwrap_or(1);
                let prev_version = row.versions.get(col).copied();

                let dag = row.dags.entry(col.clone()).or_insert_with(crate::dag::VersionDag::new);
                let parents = prev_version.map(|v| vec![v]).unwrap_or_default();
                dag.add_node(version, val.clone(), parents);

                row.columns.insert(col.clone(), val.clone());
                row.versions.insert(col.clone(), version);
            }
        }

        pub fn changeset(&self) -> HashMap<String, (HashMap<String, String>, HashMap<String, u64>)> {
            self.rows.iter()
                .map(|(pk, row)| (pk.clone(), (row.columns.clone(), row.versions.clone())))
                .collect()
        }

        pub fn crr_merge(
            &mut self,
            changeset: &HashMap<String, (HashMap<String, String>, HashMap<String, u64>)>,
            policy: TieBreakPolicy,
        ) -> LegacyMergeReport {
            let mut report = LegacyMergeReport::default();

            for (pk, (cols, vers)) in changeset {
                let row = self.rows.entry(pk.clone()).or_insert_with(|| CrrRow {
                    pk: pk.clone(),
                    columns: HashMap::new(),
                    versions: HashMap::new(),
                    dags: HashMap::new(),
                });

                for (col, val) in cols {
                    let v_r = vers.get(col).copied().unwrap_or(0);
                    let v_l = row.versions.get(col).copied().unwrap_or(0);
                    let current_value = row.columns.get(col).cloned();

                    if v_l == 0 {
                        row.columns.insert(col.clone(), val.clone());
                        row.versions.insert(col.clone(), v_r);
                        let dag = row.dags.entry(col.clone()).or_insert_with(crate::dag::VersionDag::new);
                        dag.add_node(v_r, val.clone(), vec![]);
                        report.inserted.push((pk.clone(), col.clone()));
                    } else if v_r > v_l {
                        row.columns.insert(col.clone(), val.clone());
                        row.versions.insert(col.clone(), v_r);
                        let dag = row.dags.entry(col.clone()).or_insert_with(crate::dag::VersionDag::new);
                        dag.add_node(v_r, val.clone(), vec![v_l]);
                        report.updated.push((pk.clone(), col.clone(), v_r));
                    } else if v_r == v_l {
                        if let Some(cv) = current_value {
                            if cv != *val {
                                report.conflicts_equal_version.push((
                                    pk.clone(),
                                    col.clone(),
                                    v_r,
                                    cv.clone(),
                                    val.clone(),
                                ));
                                match policy {
                                    TieBreakPolicy::PreferExisting => {}
                                    TieBreakPolicy::PreferIncoming => {
                                        let v_new = v_r + 1;
                                        row.columns.insert(col.clone(), val.clone());
                                        row.versions.insert(col.clone(), v_new);
                                        let dag = row.dags.entry(col.clone()).or_insert_with(crate::dag::VersionDag::new);
                                        dag.add_node(v_new, val.clone(), vec![v_l, v_r]);
                                    }
                                    TieBreakPolicy::LexicographicMin => {
                                        if &cv > val {
                                            let v_new = v_r + 1;
                                            row.columns.insert(col.clone(), val.clone());
                                            row.versions.insert(col.clone(), v_new);
                                            let dag = row.dags.entry(col.clone()).or_insert_with(crate::dag::VersionDag::new);
                                            dag.add_node(v_new, val.clone(), vec![v_l, v_r]);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        report.skipped_older.push((pk.clone(), col.clone(), v_r));
                    }
                }
            }
            report
        }
    }
}

// Legacy DAG module for UI compatibility
pub mod dag {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct DagNode {
        pub version: u64,
        pub parent_versions: Vec<u64>,
        pub value: String,
        pub timestamp: u64,
        pub is_tombstone: bool,
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct VersionDag {
        pub nodes: HashMap<u64, DagNode>,
        pub head: Option<u64>,
    }

    impl VersionDag {
        pub fn new() -> Self {
            Self { nodes: HashMap::new(), head: None }
        }

        pub fn add_node(&mut self, version: u64, value: String, parent_versions: Vec<u64>) {
            self.add_node_with_tombstone(version, value, parent_versions, false);
        }

        pub fn add_node_with_tombstone(
            &mut self,
            version: u64,
            value: String,
            parent_versions: Vec<u64>,
            is_tombstone: bool,
        ) {
            let node = DagNode {
                version,
                parent_versions,
                value,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                is_tombstone,
            };
            self.nodes.insert(version, node);
            if self.head.map_or(true, |h| version > h) {
                self.head = Some(version);
            }
        }

        pub fn find_missing_versions(&self) -> Vec<u64> {
            let mut missing = Vec::new();
            for node in self.nodes.values() {
                for &parent in &node.parent_versions {
                    if !self.nodes.contains_key(&parent) {
                        missing.push(parent);
                    }
                }
            }
            missing.sort();
            missing.dedup();
            missing
        }

        pub fn reconstruct_missing_version(&self, missing_version: u64) -> Option<String> {
            let children: Vec<&DagNode> = self.nodes.values()
                .filter(|node| node.parent_versions.contains(&missing_version))
                .collect();

            if children.is_empty() {
                return None;
            }

            let parent = self.nodes.iter()
                .filter(|(&v, _)| v < missing_version)
                .max_by_key(|(&v, _)| v)
                .map(|(_, node)| node);

            match (parent, children.first()) {
                (Some(before), Some(after)) => Some(format!(
                    "[Reconstructed v{}] between '{}' and '{}'",
                    missing_version, before.value, after.value
                )),
                (None, Some(after)) => Some(format!(
                    "[Reconstructed v{}] Pre-cursor to '{}'",
                    missing_version, after.value
                )),
                _ => None,
            }
        }

        pub fn get_reconstructed_timeline(&self) -> Vec<(u64, String, bool)> {
            let mut all_versions: Vec<u64> = self.nodes.keys().copied().collect();
            let missing = self.find_missing_versions();
            all_versions.extend(&missing);
            all_versions.sort();
            all_versions.dedup();

            all_versions.into_iter()
                .filter_map(|v| {
                    if let Some(node) = self.nodes.get(&v) {
                        Some((v, node.value.clone(), false))
                    } else {
                        self.reconstruct_missing_version(v).map(|r| (v, r, true))
                    }
                })
                .collect()
        }

        pub fn gc_depth_based(&mut self, depth: usize) -> usize {
            if depth == 0 || self.head.is_none() {
                return 0;
            }

            let head = self.head.unwrap();
            let mut reachable = std::collections::HashSet::new();
            let mut to_visit = vec![(head, 0)];

            while let Some((version, d)) = to_visit.pop() {
                if reachable.contains(&version) || d >= depth {
                    continue;
                }
                reachable.insert(version);
                if let Some(node) = self.nodes.get(&version) {
                    for &parent in &node.parent_versions {
                        if d + 1 < depth {
                            to_visit.push((parent, d + 1));
                        }
                    }
                }
            }

            let to_remove: Vec<u64> = self.nodes.keys()
                .filter(|v| !reachable.contains(v))
                .copied()
                .collect();

            let removed = to_remove.len();
            for v in to_remove {
                self.nodes.remove(&v);
            }
            removed
        }
    }
}

// Legacy schema module for UI compatibility
pub mod schema {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct SchemaVersion {
        pub version: u64,
        pub columns: HashMap<String, ColumnDef>,
        pub timestamp: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ColumnDef {
        pub name: String,
        pub col_type: ColumnType,
        pub nullable: bool,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    pub enum ColumnType {
        Text,
        Integer,
        Real,
        Blob,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum SchemaMigration {
        AddColumn { name: String, col_type: ColumnType, nullable: bool },
        RenameColumn { old_name: String, new_name: String },
        ChangeColumnType { name: String, new_type: ColumnType },
        DropColumn { name: String },
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct SchemaManager {
        pub versions: HashMap<u64, SchemaVersion>,
        pub current_version: u64,
        pub migrations: Vec<(u64, SchemaMigration)>,
    }

    impl SchemaManager {
        pub fn new() -> Self {
            Self { versions: HashMap::new(), current_version: 0, migrations: Vec::new() }
        }

        pub fn apply_migration(&mut self, migration: SchemaMigration) -> u64 {
            let new_version = self.current_version + 1;
            let mut new_columns = self.versions.get(&self.current_version)
                .map(|s| s.columns.clone())
                .unwrap_or_default();

            match &migration {
                SchemaMigration::AddColumn { name, col_type, nullable } => {
                    new_columns.insert(name.clone(), ColumnDef {
                        name: name.clone(),
                        col_type: col_type.clone(),
                        nullable: *nullable,
                    });
                }
                SchemaMigration::RenameColumn { old_name, new_name } => {
                    if let Some(mut col) = new_columns.remove(old_name) {
                        col.name = new_name.clone();
                        new_columns.insert(new_name.clone(), col);
                    }
                }
                SchemaMigration::ChangeColumnType { name, new_type } => {
                    if let Some(col) = new_columns.get_mut(name) {
                        col.col_type = new_type.clone();
                    }
                }
                SchemaMigration::DropColumn { name } => {
                    new_columns.remove(name);
                }
            }

            self.versions.insert(new_version, SchemaVersion {
                version: new_version,
                columns: new_columns,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            });
            self.migrations.push((new_version, migration));
            self.current_version = new_version;
            new_version
        }
    }
}

// Legacy foreign_keys module
pub mod foreign_keys {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ForeignKey {
        pub name: String,
        pub from_table: String,
        pub from_column: String,
        pub to_table: String,
        pub to_column: String,
        pub on_delete: OnDeleteAction,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum OnDeleteAction {
        Cascade,
        SetNull,
        Restrict,
        NoAction,
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct ForeignKeyManager {
        pub constraints: HashMap<String, ForeignKey>,
    }

    impl ForeignKeyManager {
        pub fn new() -> Self {
            Self { constraints: HashMap::new() }
        }

        pub fn add_constraint(&mut self, fk: ForeignKey) {
            self.constraints.insert(fk.name.clone(), fk);
        }
    }
}

// Legacy transactions module
pub mod transactions {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Transaction {
        pub id: String,
        pub operations: Vec<TransactionOp>,
        pub version: u64,
        pub timestamp: u64,
        pub committed: bool,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum TransactionOp {
        Insert { table: String, pk: String, columns: HashMap<String, String> },
        Update { table: String, pk: String, columns: HashMap<String, String> },
        Delete { table: String, pk: String },
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct TransactionManager {
        pub transactions: HashMap<String, Transaction>,
        pub pending: Vec<String>,
    }

    impl TransactionManager {
        pub fn new() -> Self {
            Self { transactions: HashMap::new(), pending: Vec::new() }
        }

        pub fn begin(&mut self) -> String {
            let tx_id = format!("tx_{}", self.transactions.len() + 1);
            let tx = Transaction {
                id: tx_id.clone(),
                operations: Vec::new(),
                version: 0,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                committed: false,
            };
            self.transactions.insert(tx_id.clone(), tx);
            self.pending.push(tx_id.clone());
            tx_id
        }

        pub fn add_operation(&mut self, tx_id: &str, op: TransactionOp) -> Result<(), String> {
            if let Some(tx) = self.transactions.get_mut(tx_id) {
                if tx.committed {
                    return Err("Transaction already committed".to_string());
                }
                tx.operations.push(op);
                Ok(())
            } else {
                Err("Transaction not found".to_string())
            }
        }

        pub fn commit(
            &mut self,
            tx_id: &str,
            tables: &mut HashMap<String, super::crr::LegacyCrrTable>,
        ) -> Result<(), String> {
            let tx = self.transactions.get(tx_id).ok_or("Transaction not found")?;
            if tx.committed {
                return Err("Transaction already committed".to_string());
            }

            let operations = tx.operations.clone();
            let tx_version = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            for op in &operations {
                match op {
                    TransactionOp::Insert { table, pk, columns } => {
                        let crr_table = tables.entry(table.clone()).or_insert_with(super::crr::LegacyCrrTable::new);
                        let versions: HashMap<String, u64> = columns.keys().map(|c| (c.clone(), tx_version)).collect();
                        crr_table.insert_or_update(pk, columns.clone(), versions);
                    }
                    TransactionOp::Update { table, pk, columns } => {
                        let crr_table = tables.entry(table.clone()).or_insert_with(super::crr::LegacyCrrTable::new);
                        let mut final_columns = crr_table.rows.get(pk)
                            .map(|r| r.columns.clone())
                            .unwrap_or_default();
                        let mut versions = HashMap::new();
                        for (col, val) in columns {
                            final_columns.insert(col.clone(), val.clone());
                            versions.insert(col.clone(), tx_version);
                        }
                        crr_table.insert_or_update(pk, final_columns, versions);
                    }
                    TransactionOp::Delete { table, pk } => {
                        if let Some(crr_table) = tables.get_mut(table) {
                            crr_table.rows.remove(pk);
                        }
                    }
                }
            }

            if let Some(tx) = self.transactions.get_mut(tx_id) {
                tx.committed = true;
                tx.version = tx_version;
            }
            self.pending.retain(|id| id != tx_id);
            Ok(())
        }

        pub fn rollback(&mut self, tx_id: &str) -> Result<(), String> {
            if let Some(tx) = self.transactions.get_mut(tx_id) {
                if tx.committed {
                    return Err("Cannot rollback committed transaction".to_string());
                }
                tx.operations.clear();
                self.pending.retain(|id| id != tx_id);
                Ok(())
            } else {
                Err("Transaction not found".to_string())
            }
        }
    }
}

// Legacy delta_sync module
pub mod delta_sync {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
    pub struct VectorClock {
        pub clocks: HashMap<String, u64>,
    }

    impl VectorClock {
        pub fn new() -> Self {
            Self { clocks: HashMap::new() }
        }

        pub fn update(&mut self, peer_id: &str, version: u64) {
            let current = self.clocks.entry(peer_id.to_string()).or_insert(0);
            if version > *current {
                *current = version;
            }
        }

        pub fn get(&self, peer_id: &str) -> u64 {
            *self.clocks.get(peer_id).unwrap_or(&0)
        }
    }

    #[derive(Clone)]
    pub struct DeltaTracker {
        pub changelog: HashMap<u64, (String, String, String)>,
        pub next_seq: u64,
        pub vector_clock: VectorClock,
        pub peer_id: String,
    }

    impl DeltaTracker {
        pub fn new(peer_id: String) -> Self {
            Self {
                changelog: HashMap::new(),
                next_seq: 1,
                vector_clock: VectorClock::new(),
                peer_id,
            }
        }

        pub fn record_change(&mut self, pk: &str, column: &str, value: &str) -> u64 {
            let seq = self.next_seq;
            self.changelog.insert(seq, (pk.to_string(), column.to_string(), value.to_string()));
            self.next_seq += 1;
            seq
        }
    }
}

// Legacy SyncEngine for UI compatibility
#[derive(Clone)]
pub struct SyncEngine {
    pub crr_table: crr::LegacyCrrTable,
    pub tables: std::collections::HashMap<String, crr::LegacyCrrTable>,
    pub schema_manager: schema::SchemaManager,
    pub fk_manager: foreign_keys::ForeignKeyManager,
    pub tx_manager: transactions::TransactionManager,
    pub delta_tracker: delta_sync::DeltaTracker,
}

impl Default for SyncEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncEngine {
    pub fn new() -> Self {
        Self::new_with_peer_id("default_peer".to_string())
    }

    pub fn new_with_peer_id(peer_id: String) -> Self {
        Self {
            crr_table: crr::LegacyCrrTable::new(),
            tables: std::collections::HashMap::new(),
            schema_manager: schema::SchemaManager::new(),
            fk_manager: foreign_keys::ForeignKeyManager::new(),
            tx_manager: transactions::TransactionManager::new(),
            delta_tracker: delta_sync::DeltaTracker::new(peer_id),
        }
    }

    pub fn get_table(&mut self, name: &str) -> &mut crr::LegacyCrrTable {
        self.tables.entry(name.to_string()).or_insert_with(crr::LegacyCrrTable::new)
    }
}

// Legacy sync_protocol module
pub mod sync_protocol {
    use super::crr::{LegacyCrrTable, LegacyMergeReport, TieBreakPolicy};
    use std::collections::HashMap;

    #[derive(Clone, Debug)]
    pub struct SyncPeer {
        pub peer_id: String,
        pub table: LegacyCrrTable,
    }

    impl SyncPeer {
        pub fn new(peer_id: &str) -> Self {
            Self { peer_id: peer_id.to_string(), table: LegacyCrrTable::new() }
        }

        pub fn with_table(peer_id: &str, table: LegacyCrrTable) -> Self {
            Self { peer_id: peer_id.to_string(), table }
        }
    }

    #[derive(Clone, Debug)]
    pub struct HeadExchange {
        pub peer_id: String,
        pub heads: HashMap<String, HashMap<String, u64>>,
    }

    impl HeadExchange {
        pub fn from_table(peer_id: &str, table: &LegacyCrrTable) -> Self {
            let mut heads = HashMap::new();
            for (pk, row) in &table.rows {
                heads.insert(pk.clone(), row.versions.clone());
            }
            Self { peer_id: peer_id.to_string(), heads }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Changeset {
        pub from_peer: String,
        pub to_peer: String,
        pub changes: HashMap<String, (HashMap<String, String>, HashMap<String, u64>)>,
        pub compressed_bytes: Option<usize>,
    }

    impl Changeset {
        pub fn compute(sender: &SyncPeer, receiver_heads: &HeadExchange) -> Self {
            let mut changes = HashMap::new();

            for (pk, row) in &sender.table.rows {
                let receiver_versions = receiver_heads.heads.get(pk);
                let mut needed_cols = HashMap::new();
                let mut needed_vers = HashMap::new();

                for (col, val) in &row.columns {
                    let sender_ver = row.versions.get(col).copied().unwrap_or(0);
                    let receiver_ver = receiver_versions.and_then(|rv| rv.get(col).copied()).unwrap_or(0);

                    if sender_ver > receiver_ver {
                        needed_cols.insert(col.clone(), val.clone());
                        needed_vers.insert(col.clone(), sender_ver);
                    }
                }

                if !needed_cols.is_empty() {
                    changes.insert(pk.clone(), (needed_cols, needed_vers));
                }
            }

            Self {
                from_peer: sender.peer_id.clone(),
                to_peer: receiver_heads.peer_id.clone(),
                changes,
                compressed_bytes: None,
            }
        }

        pub fn column_count(&self) -> usize {
            self.changes.values().map(|(cols, _)| cols.len()).sum()
        }

        pub fn estimate_size(&self) -> usize {
            self.changes.iter()
                .map(|(pk, (cols, vers))| pk.len() + cols.iter().map(|(c, v)| c.len() + v.len()).sum::<usize>() + vers.len() * 8)
                .sum()
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct SyncResult {
        pub merge_report: LegacyMergeReport,
        pub rows_affected: usize,
        pub columns_updated: usize,
        pub conflicts_resolved: usize,
    }

    #[derive(Clone, Debug)]
    pub struct SyncSession {
        pub peer_a_id: String,
        pub peer_b_id: String,
        pub policy: TieBreakPolicy,
    }

    impl SyncSession {
        pub fn new(peer_a_id: &str, peer_b_id: &str, policy: TieBreakPolicy) -> Self {
            Self {
                peer_a_id: peer_a_id.to_string(),
                peer_b_id: peer_b_id.to_string(),
                policy,
            }
        }

        pub fn sync(&self, peer_a: &mut SyncPeer, peer_b: &mut SyncPeer) -> (SyncResult, SyncResult, SyncStats) {
            let heads_a = HeadExchange::from_table(&peer_a.peer_id, &peer_a.table);
            let heads_b = HeadExchange::from_table(&peer_b.peer_id, &peer_b.table);

            let changeset_a_to_b = Changeset::compute(peer_a, &heads_b);
            let changeset_b_to_a = Changeset::compute(peer_b, &heads_a);

            let report_a = peer_a.table.crr_merge(&changeset_b_to_a.changes, self.policy);
            let report_b = peer_b.table.crr_merge(&changeset_a_to_b.changes, self.policy);

            let result_a = SyncResult {
                rows_affected: changeset_b_to_a.changes.len(),
                columns_updated: report_a.updated.len() + report_a.inserted.len(),
                conflicts_resolved: report_a.conflicts_equal_version.len(),
                merge_report: report_a,
            };

            let result_b = SyncResult {
                rows_affected: changeset_a_to_b.changes.len(),
                columns_updated: report_b.updated.len() + report_b.inserted.len(),
                conflicts_resolved: report_b.conflicts_equal_version.len(),
                merge_report: report_b,
            };

            let stats = SyncStats {
                heads_exchanged: heads_a.heads.len() + heads_b.heads.len(),
                changeset_a_to_b_columns: changeset_a_to_b.column_count(),
                changeset_b_to_a_columns: changeset_b_to_a.column_count(),
                changeset_a_to_b_bytes: changeset_a_to_b.estimate_size(),
                changeset_b_to_a_bytes: changeset_b_to_a.estimate_size(),
            };

            (result_a, result_b, stats)
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct SyncStats {
        pub heads_exchanged: usize,
        pub changeset_a_to_b_columns: usize,
        pub changeset_b_to_a_columns: usize,
        pub changeset_a_to_b_bytes: usize,
        pub changeset_b_to_a_bytes: usize,
    }

    impl SyncStats {
        pub fn total_columns_transferred(&self) -> usize {
            self.changeset_a_to_b_columns + self.changeset_b_to_a_columns
        }

        pub fn total_bytes_transferred(&self) -> usize {
            self.changeset_a_to_b_bytes + self.changeset_b_to_a_bytes
        }
    }

    pub struct MeshSync {
        pub peers: HashMap<String, SyncPeer>,
        pub policy: TieBreakPolicy,
    }

    impl MeshSync {
        pub fn new(policy: TieBreakPolicy) -> Self {
            Self { peers: HashMap::new(), policy }
        }

        pub fn add_peer(&mut self, peer: SyncPeer) {
            self.peers.insert(peer.peer_id.clone(), peer);
        }

        pub fn sync_all(&mut self) -> usize {
            let peer_ids: Vec<String> = self.peers.keys().cloned().collect();
            let mut rounds = 0;
            let mut changed = true;

            while changed {
                changed = false;
                rounds += 1;

                for i in 0..peer_ids.len() {
                    for j in (i + 1)..peer_ids.len() {
                        let id_a = &peer_ids[i];
                        let id_b = &peer_ids[j];

                        let mut peer_a = self.peers.remove(id_a).unwrap();
                        let mut peer_b = self.peers.remove(id_b).unwrap();

                        let session = SyncSession::new(id_a, id_b, self.policy);
                        let (result_a, result_b, _) = session.sync(&mut peer_a, &mut peer_b);

                        if result_a.columns_updated > 0 || result_b.columns_updated > 0 {
                            changed = true;
                        }

                        self.peers.insert(id_a.clone(), peer_a);
                        self.peers.insert(id_b.clone(), peer_b);
                    }
                }

                if rounds > 100 {
                    break;
                }
            }

            rounds
        }

        pub fn is_converged(&self) -> bool {
            let peer_ids: Vec<&String> = self.peers.keys().collect();
            if peer_ids.len() < 2 {
                return true;
            }

            let first = self.peers.get(peer_ids[0]).unwrap();
            for id in peer_ids.iter().skip(1) {
                let peer = self.peers.get(*id).unwrap();
                if first.table.rows.len() != peer.table.rows.len() {
                    return false;
                }
                for (pk, row_a) in &first.table.rows {
                    match peer.table.rows.get(pk) {
                        None => return false,
                        Some(row_b) => {
                            if row_a.columns != row_b.columns || row_a.versions != row_b.versions {
                                return false;
                            }
                        }
                    }
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_api_basic() {
        let mut table = CrrTable::open_in_memory().unwrap();

        table.insert("user_1")
            .column_str("name", "Alice", 1)
            .commit()
            .unwrap();

        let pks = table.pks().unwrap();
        assert_eq!(pks.len(), 1);
    }

    #[test]
    fn test_legacy_api_compatible() {
        let mut table = crr::LegacyCrrTable::new();

        let mut cols = std::collections::HashMap::new();
        let mut vers = std::collections::HashMap::new();
        cols.insert("name".to_string(), "Alice".to_string());
        vers.insert("name".to_string(), 1);

        table.insert_or_update("user_1", cols, vers);

        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows.get("user_1").unwrap().columns.get("name").unwrap(), "Alice");
    }
}
