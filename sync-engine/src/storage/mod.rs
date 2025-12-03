mod sqlite;
mod memory;

pub use sqlite::SqliteStorage;
pub use memory::MemoryStorage;

use crate::error::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Cell {
    pub value: Vec<u8>,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub pk: String,
    pub cells: HashMap<String, Cell>,
}

#[derive(Debug, Clone)]
pub struct DagNode {
    pub version: u64,
    pub value: Vec<u8>,
    pub parent_version: Option<u64>,
    pub parent2_version: Option<u64>,
    pub timestamp: u64,
    pub is_tombstone: bool,
}

pub trait Storage {
    fn get_cell(&self, pk: &str, col: &str) -> Result<Option<Cell>>;
    fn set_cell(&mut self, pk: &str, col: &str, cell: Cell) -> Result<()>;
    fn get_row(&self, pk: &str) -> Result<Option<Row>>;
    fn delete_row(&mut self, pk: &str) -> Result<()>;
    fn row_count(&self) -> Result<usize>;
    fn all_pks(&self) -> Result<Vec<String>>;

    fn append_dag_node(&mut self, pk: &str, col: &str, node: DagNode) -> Result<()>;
    fn get_dag_history(&self, pk: &str, col: &str) -> Result<Vec<DagNode>>;
    fn gc_dag(&mut self, pk: &str, col: &str, keep_versions: usize) -> Result<usize>;

    fn begin_transaction(&mut self) -> Result<()>;
    fn commit_transaction(&mut self) -> Result<()>;
    fn rollback_transaction(&mut self) -> Result<()>;
}

pub fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
