use std::collections::HashMap;

use crate::error::Result;
use crate::merge::{MergeReport, TieBreakPolicy, resolve_versions, resolve_conflict, MergeDecision};
use crate::row::{InsertBuilder, RowView, UpdateBuilder};
use crate::storage::{Cell, DagNode, SqliteStorage, Storage, now_millis};
use crate::sync::Changeset;

pub struct CrrTable<S: Storage = SqliteStorage> {
    storage: S,
}

impl CrrTable<SqliteStorage> {
    pub fn open(path: &str) -> Result<Self> {
        let storage = SqliteStorage::open(path)?;
        Ok(Self { storage })
    }

    pub fn open_in_memory() -> Result<Self> {
        let storage = SqliteStorage::open_in_memory()?;
        Ok(Self { storage })
    }
}

impl<S: Storage> CrrTable<S> {
    pub fn with_storage(storage: S) -> Self {
        Self { storage }
    }

    pub fn insert(&mut self, pk: &str) -> InsertBuilder<'_, S> {
        InsertBuilder::new(&mut self.storage, pk.to_string())
    }

    pub fn update(&mut self, pk: &str) -> UpdateBuilder<'_, S> {
        UpdateBuilder::new(&mut self.storage, pk.to_string())
    }

    pub fn get(&self, pk: &str) -> Result<Option<RowView>> {
        let row = self.storage.get_row(pk)?;
        match row {
            None => Ok(None),
            Some(row) => {
                let mut dag_history = HashMap::new();
                for col in row.cells.keys() {
                    if let Ok(history) = self.storage.get_dag_history(pk, col) {
                        dag_history.insert(col.clone(), history);
                    }
                }
                Ok(Some(RowView {
                    pk: pk.to_string(),
                    cells: row.cells,
                    dag_history,
                }))
            }
        }
    }

    pub fn delete(&mut self, pk: &str) -> Result<()> {
        self.storage.delete_row(pk)
    }

    pub fn len(&self) -> Result<usize> {
        self.storage.row_count()
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.storage.row_count()? == 0)
    }

    pub fn pks(&self) -> Result<Vec<String>> {
        self.storage.all_pks()
    }

    pub fn changeset(&self) -> Result<Changeset> {
        let mut changes = HashMap::new();
        let pks = self.storage.all_pks()?;

        for pk in pks {
            if let Some(row) = self.storage.get_row(&pk)? {
                let mut columns = HashMap::new();
                let mut versions = HashMap::new();

                for (col, cell) in row.cells {
                    columns.insert(col.clone(), cell.value);
                    versions.insert(col, cell.version);
                }

                changes.insert(pk, (columns, versions));
            }
        }

        Ok(Changeset { changes })
    }

    pub fn merge(&mut self, changeset: &Changeset, policy: TieBreakPolicy) -> Result<MergeReport> {
        let mut report = MergeReport::default();

        self.storage.begin_transaction()?;

        for (pk, (remote_columns, remote_versions)) in &changeset.changes {
            for (col, remote_value) in remote_columns {
                let remote_version = *remote_versions.get(col).unwrap_or(&1);
                let local = self.storage.get_cell(pk, col)?;

                let (local_value, local_version) = match &local {
                    Some(cell) => (Some(cell.value.clone()), cell.version),
                    None => (None, 0),
                };

                match resolve_versions(local_version, remote_version) {
                    MergeDecision::Accept => {
                        let cell = Cell { value: remote_value.clone(), version: remote_version };
                        self.storage.set_cell(pk, col, cell)?;

                        let node = DagNode {
                            version: remote_version,
                            value: remote_value.clone(),
                            parent_version: if local_version > 0 { Some(local_version) } else { None },
                            parent2_version: None,
                            timestamp: now_millis(),
                            is_tombstone: false,
                        };
                        self.storage.append_dag_node(pk, col, node)?;

                        if local_version == 0 {
                            report.inserted += 1;
                        } else {
                            report.updated += 1;
                        }
                    }
                    MergeDecision::Reject => {
                        report.skipped += 1;
                    }
                    MergeDecision::Conflict => {
                        let local_val = local_value.as_ref().unwrap();
                        if local_val == remote_value {
                            report.skipped += 1;
                        } else {
                            report.conflicts += 1;
                            let accept_remote = resolve_conflict(local_val, remote_value, policy);

                            if accept_remote {
                                let new_version = remote_version + 1;
                                let cell = Cell { value: remote_value.clone(), version: new_version };
                                self.storage.set_cell(pk, col, cell)?;

                                let node = DagNode {
                                    version: new_version,
                                    value: remote_value.clone(),
                                    parent_version: Some(local_version),
                                    parent2_version: Some(remote_version),
                                    timestamp: now_millis(),
                                    is_tombstone: false,
                                };
                                self.storage.append_dag_node(pk, col, node)?;
                                report.updated += 1;
                            }
                        }
                    }
                }
            }
        }

        self.storage.commit_transaction()?;
        Ok(report)
    }

    pub fn gc(&mut self, keep_versions: usize) -> Result<usize> {
        let mut total_removed = 0;
        let pks = self.storage.all_pks()?;

        for pk in pks {
            if let Some(row) = self.storage.get_row(&pk)? {
                for col in row.cells.keys() {
                    total_removed += self.storage.gc_dag(&pk, col, keep_versions)?;
                }
            }
        }

        Ok(total_removed)
    }

    #[deprecated(note = "Use insert() builder instead")]
    pub fn insert_or_update(
        &mut self,
        pk: &str,
        columns: HashMap<String, String>,
        versions: HashMap<String, u64>,
    ) -> Result<()> {
        for (col, value) in columns {
            let version = versions.get(&col).copied().unwrap_or(1);
            let current = self.storage.get_cell(pk, &col)?;
            let parent_version = current.as_ref().map(|c| c.version);

            let cell = Cell { value: value.as_bytes().to_vec(), version };
            self.storage.set_cell(pk, &col, cell)?;

            let node = DagNode {
                version,
                value: value.into_bytes(),
                parent_version,
                parent2_version: None,
                timestamp: now_millis(),
                is_tombstone: false,
            };
            self.storage.append_dag_node(pk, &col, node)?;
        }
        Ok(())
    }

    #[deprecated(note = "Use merge() instead")]
    pub fn crr_merge(
        &mut self,
        changeset: &HashMap<String, (HashMap<String, String>, HashMap<String, u64>)>,
        policy: TieBreakPolicy,
    ) -> Result<MergeReport> {
        let converted = Changeset {
            changes: changeset.iter()
                .map(|(pk, (cols, vers))| {
                    let columns = cols.iter()
                        .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                        .collect();
                    (pk.clone(), (columns, vers.clone()))
                })
                .collect(),
        };
        self.merge(&converted, policy)
    }
}
