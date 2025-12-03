use std::collections::HashMap;

use super::{Cell, DagNode, Row, Storage};
use crate::error::Result;

pub struct MemoryStorage {
    cells: HashMap<(String, String), Cell>,
    dag: HashMap<(String, String), Vec<DagNode>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            cells: HashMap::new(),
            dag: HashMap::new(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn get_cell(&self, pk: &str, col: &str) -> Result<Option<Cell>> {
        Ok(self.cells.get(&(pk.to_string(), col.to_string())).cloned())
    }

    fn set_cell(&mut self, pk: &str, col: &str, cell: Cell) -> Result<()> {
        self.cells.insert((pk.to_string(), col.to_string()), cell);
        Ok(())
    }

    fn get_row(&self, pk: &str) -> Result<Option<Row>> {
        let cells: HashMap<String, Cell> = self.cells.iter()
            .filter(|((p, _), _)| p == pk)
            .map(|((_, col), cell)| (col.clone(), cell.clone()))
            .collect();

        if cells.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Row { pk: pk.to_string(), cells }))
        }
    }

    fn delete_row(&mut self, pk: &str) -> Result<()> {
        self.cells.retain(|(p, _), _| p != pk);
        self.dag.retain(|(p, _), _| p != pk);
        Ok(())
    }

    fn row_count(&self) -> Result<usize> {
        let pks: std::collections::HashSet<_> = self.cells.keys().map(|(pk, _)| pk).collect();
        Ok(pks.len())
    }

    fn all_pks(&self) -> Result<Vec<String>> {
        let mut pks: Vec<_> = self.cells.keys()
            .map(|(pk, _)| pk.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        pks.sort();
        Ok(pks)
    }

    fn append_dag_node(&mut self, pk: &str, col: &str, node: DagNode) -> Result<()> {
        self.dag.entry((pk.to_string(), col.to_string()))
            .or_default()
            .push(node);
        Ok(())
    }

    fn get_dag_history(&self, pk: &str, col: &str) -> Result<Vec<DagNode>> {
        Ok(self.dag.get(&(pk.to_string(), col.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    fn gc_dag(&mut self, pk: &str, col: &str, keep_versions: usize) -> Result<usize> {
        let key = (pk.to_string(), col.to_string());
        if let Some(history) = self.dag.get_mut(&key) {
            if history.len() <= keep_versions {
                return Ok(0);
            }
            let removed = history.len() - keep_versions;
            history.drain(0..removed);
            Ok(removed)
        } else {
            Ok(0)
        }
    }

    fn begin_transaction(&mut self) -> Result<()> {
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        Ok(())
    }
}

unsafe impl Send for MemoryStorage {}
unsafe impl Sync for MemoryStorage {}
