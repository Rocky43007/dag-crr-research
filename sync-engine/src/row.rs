use std::collections::HashMap;

use crate::storage::{Cell, DagNode, Storage, now_millis};
use crate::error::Result;

pub struct RowView {
    pub(crate) pk: String,
    pub(crate) cells: HashMap<String, Cell>,
    pub(crate) dag_history: HashMap<String, Vec<DagNode>>,
}

impl RowView {
    pub fn pk(&self) -> &str {
        &self.pk
    }

    pub fn get(&self, col: &str) -> Option<&[u8]> {
        self.cells.get(col).map(|c| c.value.as_slice())
    }

    pub fn get_string(&self, col: &str) -> Option<String> {
        self.cells.get(col)
            .and_then(|c| String::from_utf8(c.value.clone()).ok())
    }

    pub fn version(&self, col: &str) -> Option<u64> {
        self.cells.get(col).map(|c| c.version)
    }

    pub fn columns(&self) -> impl Iterator<Item = (&str, &[u8], u64)> {
        self.cells.iter().map(|(k, v)| (k.as_str(), v.value.as_slice(), v.version))
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.cells.keys().map(|s| s.as_str())
    }

    pub fn dag_history(&self, col: &str) -> Option<&[DagNode]> {
        self.dag_history.get(col).map(|v| v.as_slice())
    }
}

pub struct InsertBuilder<'a, S: Storage> {
    storage: &'a mut S,
    pk: String,
    columns: Vec<(String, Vec<u8>, u64)>,
}

impl<'a, S: Storage> InsertBuilder<'a, S> {
    pub(crate) fn new(storage: &'a mut S, pk: String) -> Self {
        Self { storage, pk, columns: Vec::new() }
    }

    pub fn column(mut self, name: &str, value: impl AsRef<[u8]>, version: u64) -> Self {
        self.columns.push((name.to_string(), value.as_ref().to_vec(), version));
        self
    }

    pub fn column_str(mut self, name: &str, value: &str, version: u64) -> Self {
        self.columns.push((name.to_string(), value.as_bytes().to_vec(), version));
        self
    }

    pub fn commit(self) -> Result<()> {
        for (col, value, version) in self.columns {
            let current = self.storage.get_cell(&self.pk, &col)?;
            let current_version = current.as_ref().map(|c| c.version).unwrap_or(0);

            let cell = Cell { value: value.clone(), version };
            self.storage.set_cell(&self.pk, &col, cell)?;

            let node = DagNode {
                version,
                value,
                parent_version: if current_version > 0 { Some(current_version) } else { None },
                parent2_version: None,
                timestamp: now_millis(),
                is_tombstone: false,
            };
            self.storage.append_dag_node(&self.pk, &col, node)?;
        }
        Ok(())
    }
}

pub struct UpdateBuilder<'a, S: Storage> {
    storage: &'a mut S,
    pk: String,
    columns: Vec<(String, Vec<u8>)>,
}

impl<'a, S: Storage> UpdateBuilder<'a, S> {
    pub(crate) fn new(storage: &'a mut S, pk: String) -> Self {
        Self { storage, pk, columns: Vec::new() }
    }

    pub fn column(mut self, name: &str, value: impl AsRef<[u8]>) -> Self {
        self.columns.push((name.to_string(), value.as_ref().to_vec()));
        self
    }

    pub fn column_str(mut self, name: &str, value: &str) -> Self {
        self.columns.push((name.to_string(), value.as_bytes().to_vec()));
        self
    }

    pub fn commit(self) -> Result<()> {
        for (col, value) in self.columns {
            let current = self.storage.get_cell(&self.pk, &col)?;
            let new_version = current.as_ref().map(|c| c.version + 1).unwrap_or(1);
            let parent_version = current.as_ref().map(|c| c.version);

            let cell = Cell { value: value.clone(), version: new_version };
            self.storage.set_cell(&self.pk, &col, cell)?;

            let node = DagNode {
                version: new_version,
                value,
                parent_version,
                parent2_version: None,
                timestamp: now_millis(),
                is_tombstone: false,
            };
            self.storage.append_dag_node(&self.pk, &col, node)?;
        }
        Ok(())
    }
}
