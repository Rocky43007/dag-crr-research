use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;

use super::{Cell, DagNode, Row, Storage};
use crate::error::Result;

const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS crr_cells (
    pk TEXT NOT NULL,
    col TEXT NOT NULL,
    value BLOB NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (pk, col)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS crr_dag (
    pk TEXT NOT NULL,
    col TEXT NOT NULL,
    version INTEGER NOT NULL,
    value BLOB NOT NULL,
    parent_version INTEGER,
    parent2_version INTEGER,
    timestamp INTEGER NOT NULL,
    is_tombstone INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pk, col, version)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_dag_pk_col ON crr_dag(pk, col);

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
"#;

pub struct SqliteStorage {
    conn: Connection,
    in_transaction: bool,
}

impl SqliteStorage {
    pub fn open(path: &str) -> Result<Self> {
        let conn = if path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };
        conn.execute_batch(INIT_SQL)?;
        Ok(Self { conn, in_transaction: false })
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }
}

impl Storage for SqliteStorage {
    fn get_cell(&self, pk: &str, col: &str) -> Result<Option<Cell>> {
        let result = self.conn.query_row(
            "SELECT value, version FROM crr_cells WHERE pk = ?1 AND col = ?2",
            params![pk, col],
            |row| Ok(Cell { value: row.get(0)?, version: row.get(1)? }),
        ).optional()?;
        Ok(result)
    }

    fn set_cell(&mut self, pk: &str, col: &str, cell: Cell) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO crr_cells (pk, col, value, version) VALUES (?1, ?2, ?3, ?4)",
            params![pk, col, cell.value, cell.version],
        )?;
        Ok(())
    }

    fn get_row(&self, pk: &str) -> Result<Option<Row>> {
        let mut stmt = self.conn.prepare(
            "SELECT col, value, version FROM crr_cells WHERE pk = ?1"
        )?;
        let mut rows = stmt.query(params![pk])?;

        let mut cells = HashMap::new();
        while let Some(row) = rows.next()? {
            let col: String = row.get(0)?;
            let value: Vec<u8> = row.get(1)?;
            let version: u64 = row.get(2)?;
            cells.insert(col, Cell { value, version });
        }

        if cells.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Row { pk: pk.to_string(), cells }))
        }
    }

    fn delete_row(&mut self, pk: &str) -> Result<()> {
        self.conn.execute("DELETE FROM crr_cells WHERE pk = ?1", params![pk])?;
        self.conn.execute("DELETE FROM crr_dag WHERE pk = ?1", params![pk])?;
        Ok(())
    }

    fn row_count(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT pk) FROM crr_cells",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    fn all_pks(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT DISTINCT pk FROM crr_cells ORDER BY pk")?;
        let pks = stmt.query_map([], |row| row.get(0))?
            .collect::<std::result::Result<Vec<String>, _>>()?;
        Ok(pks)
    }

    fn append_dag_node(&mut self, pk: &str, col: &str, node: DagNode) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO crr_dag (pk, col, version, value, parent_version, parent2_version, timestamp, is_tombstone)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                pk,
                col,
                node.version,
                node.value,
                node.parent_version,
                node.parent2_version,
                node.timestamp,
                node.is_tombstone as i32,
            ],
        )?;
        Ok(())
    }

    fn get_dag_history(&self, pk: &str, col: &str) -> Result<Vec<DagNode>> {
        let mut stmt = self.conn.prepare(
            "SELECT version, value, parent_version, parent2_version, timestamp, is_tombstone
             FROM crr_dag WHERE pk = ?1 AND col = ?2 ORDER BY version"
        )?;
        let nodes = stmt.query_map(params![pk, col], |row| {
            Ok(DagNode {
                version: row.get(0)?,
                value: row.get(1)?,
                parent_version: row.get(2)?,
                parent2_version: row.get(3)?,
                timestamp: row.get(4)?,
                is_tombstone: row.get::<_, i32>(5)? != 0,
            })
        })?.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(nodes)
    }

    fn gc_dag(&mut self, pk: &str, col: &str, keep_versions: usize) -> Result<usize> {
        let history = self.get_dag_history(pk, col)?;
        if history.len() <= keep_versions {
            return Ok(0);
        }

        let cutoff_idx = history.len() - keep_versions;
        let cutoff_version = history[cutoff_idx].version;

        let deleted = self.conn.execute(
            "DELETE FROM crr_dag WHERE pk = ?1 AND col = ?2 AND version < ?3",
            params![pk, col, cutoff_version],
        )?;
        Ok(deleted)
    }

    fn begin_transaction(&mut self) -> Result<()> {
        if !self.in_transaction {
            self.conn.execute("BEGIN", [])?;
            self.in_transaction = true;
        }
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            self.conn.execute("COMMIT", [])?;
            self.in_transaction = false;
        }
        Ok(())
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        if self.in_transaction {
            self.conn.execute("ROLLBACK", [])?;
            self.in_transaction = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::now_millis;

    #[test]
    fn test_basic_operations() {
        let mut storage = SqliteStorage::open_in_memory().unwrap();

        storage.set_cell("row1", "name", Cell { value: b"Alice".to_vec(), version: 1 }).unwrap();
        storage.set_cell("row1", "email", Cell { value: b"alice@example.com".to_vec(), version: 1 }).unwrap();

        let cell = storage.get_cell("row1", "name").unwrap().unwrap();
        assert_eq!(cell.value, b"Alice");
        assert_eq!(cell.version, 1);

        let row = storage.get_row("row1").unwrap().unwrap();
        assert_eq!(row.cells.len(), 2);
    }

    #[test]
    fn test_dag_history() {
        let mut storage = SqliteStorage::open_in_memory().unwrap();

        for v in 1..=5 {
            let node = DagNode {
                version: v,
                value: format!("value_{}", v).into_bytes(),
                parent_version: if v > 1 { Some(v - 1) } else { None },
                parent2_version: None,
                timestamp: now_millis(),
                is_tombstone: false,
            };
            storage.append_dag_node("row1", "col1", node).unwrap();
        }

        let history = storage.get_dag_history("row1", "col1").unwrap();
        assert_eq!(history.len(), 5);

        let removed = storage.gc_dag("row1", "col1", 2).unwrap();
        assert_eq!(removed, 3);

        let history = storage.get_dag_history("row1", "col1").unwrap();
        assert_eq!(history.len(), 2);
    }
}
