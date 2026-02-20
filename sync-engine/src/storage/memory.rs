use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use super::{Cell, DagNode, Row, Storage};
use crate::error::Result;

struct ColumnInterner {
    to_id: HashMap<String, u16>,
    to_name: Vec<String>,
}

impl ColumnInterner {
    fn new() -> Self {
        Self {
            to_id: HashMap::new(),
            to_name: Vec::new(),
        }
    }

    fn intern(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.to_id.get(name) {
            return id;
        }
        let id = self.to_name.len() as u16;
        self.to_name.push(name.to_string());
        self.to_id.insert(name.to_string(), id);
        id
    }

    fn resolve(&self, id: u16) -> &str {
        &self.to_name[id as usize]
    }

    fn lookup(&self, name: &str) -> Option<u16> {
        self.to_id.get(name).copied()
    }
}

struct ValuePool {
    pool: HashMap<u64, Rc<[u8]>>,
}

impl ValuePool {
    fn new() -> Self {
        Self {
            pool: HashMap::new(),
        }
    }

    fn hash_value(value: &[u8]) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn intern(&mut self, value: &[u8]) -> Rc<[u8]> {
        let hash = Self::hash_value(value);

        if let Some(existing) = self.pool.get(&hash) {
            if existing.as_ref() == value {
                return Rc::clone(existing);
            }
            // Hash collision with different content; skip pooling
            return Rc::from(value);
        }

        let rc: Rc<[u8]> = Rc::from(value);
        self.pool.insert(hash, Rc::clone(&rc));
        rc
    }
}

struct InternalCell {
    value: Rc<[u8]>,
    version: u64,
}

struct InternalDagNode {
    version: u64,
    value: Rc<[u8]>,
    parent_version: Option<u64>,
    parent2_version: Option<u64>,
    timestamp: u64,
    is_tombstone: bool,
}

struct RowData {
    cells: HashMap<u16, InternalCell>,
    dag: HashMap<u16, Vec<InternalDagNode>>,
}

impl RowData {
    fn new() -> Self {
        Self {
            cells: HashMap::new(),
            dag: HashMap::new(),
        }
    }
}

pub struct MemoryStorage {
    rows: HashMap<String, RowData>,
    columns: ColumnInterner,
    values: ValuePool,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            rows: HashMap::new(),
            columns: ColumnInterner::new(),
            values: ValuePool::new(),
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
        let col_id = match self.columns.lookup(col) {
            Some(id) => id,
            None => return Ok(None),
        };

        let row = match self.rows.get(pk) {
            Some(r) => r,
            None => return Ok(None),
        };

        match row.cells.get(&col_id) {
            Some(cell) => Ok(Some(Cell {
                value: cell.value.to_vec(),
                version: cell.version,
            })),
            None => Ok(None),
        }
    }

    fn set_cell(&mut self, pk: &str, col: &str, cell: Cell) -> Result<()> {
        let col_id = self.columns.intern(col);
        let pooled_value = self.values.intern(&cell.value);

        let row = self.rows.entry(pk.to_string()).or_insert_with(RowData::new);
        row.cells.insert(
            col_id,
            InternalCell {
                value: pooled_value,
                version: cell.version,
            },
        );
        Ok(())
    }

    fn get_row(&self, pk: &str) -> Result<Option<Row>> {
        let row = match self.rows.get(pk) {
            Some(r) => r,
            None => return Ok(None),
        };

        if row.cells.is_empty() {
            return Ok(None);
        }

        let cells: HashMap<String, Cell> = row
            .cells
            .iter()
            .map(|(&col_id, cell)| {
                let col_name = self.columns.resolve(col_id).to_string();
                let cell = Cell {
                    value: cell.value.to_vec(),
                    version: cell.version,
                };
                (col_name, cell)
            })
            .collect();

        Ok(Some(Row {
            pk: pk.to_string(),
            cells,
        }))
    }

    fn delete_row(&mut self, pk: &str) -> Result<()> {
        self.rows.remove(pk);
        Ok(())
    }

    fn row_count(&self) -> Result<usize> {
        Ok(self.rows.len())
    }

    fn all_pks(&self) -> Result<Vec<String>> {
        let mut pks: Vec<String> = self.rows.keys().cloned().collect();
        pks.sort();
        Ok(pks)
    }

    fn append_dag_node(&mut self, pk: &str, col: &str, node: DagNode) -> Result<()> {
        let col_id = self.columns.intern(col);
        let pooled_value = self.values.intern(&node.value);

        let row = self.rows.entry(pk.to_string()).or_insert_with(RowData::new);
        row.dag.entry(col_id).or_default().push(InternalDagNode {
            version: node.version,
            value: pooled_value,
            parent_version: node.parent_version,
            parent2_version: node.parent2_version,
            timestamp: node.timestamp,
            is_tombstone: node.is_tombstone,
        });
        Ok(())
    }

    fn get_dag_history(&self, pk: &str, col: &str) -> Result<Vec<DagNode>> {
        let col_id = match self.columns.lookup(col) {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };

        let row = match self.rows.get(pk) {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };

        match row.dag.get(&col_id) {
            Some(nodes) => Ok(nodes
                .iter()
                .map(|n| DagNode {
                    version: n.version,
                    value: n.value.to_vec(),
                    parent_version: n.parent_version,
                    parent2_version: n.parent2_version,
                    timestamp: n.timestamp,
                    is_tombstone: n.is_tombstone,
                })
                .collect()),
            None => Ok(Vec::new()),
        }
    }

    fn gc_dag(&mut self, pk: &str, col: &str, keep_versions: usize) -> Result<usize> {
        let col_id = match self.columns.lookup(col) {
            Some(id) => id,
            None => return Ok(0),
        };

        let row = match self.rows.get_mut(pk) {
            Some(r) => r,
            None => return Ok(0),
        };

        if let Some(history) = row.dag.get_mut(&col_id) {
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

// Rc<[u8]> is !Send+!Sync but storage is only accessed from a single thread.
unsafe impl Send for MemoryStorage {}
unsafe impl Sync for MemoryStorage {}
