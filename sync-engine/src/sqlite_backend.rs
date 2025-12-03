//! SQLite-backed DAG-CRR implementation for fair comparison benchmarks.
//!
//! This module provides a SQLite-backed implementation of the DAG-CRR storage,
//! enabling apples-to-apples comparison with CR-SQLite which also uses SQLite.

#[cfg(feature = "sqlite")]
pub mod sqlite {
    use rusqlite::{params, Connection, Result};
    use std::collections::HashMap;

    /// SQLite-backed DAG-CRR table storage
    pub struct SqliteDagCrr {
        conn: Connection,
    }

    /// Tiebreak policy for conflict resolution
    #[derive(Debug, Clone, Copy)]
    pub enum TieBreakPolicy {
        PreferExisting,
        PreferIncoming,
        LexicographicMin,
    }

    /// Result of a merge operation
    #[derive(Debug, Default)]
    pub struct MergeReport {
        pub inserted: usize,
        pub updated: usize,
        pub conflicts: usize,
        pub skipped: usize,
    }

    /// A changeset entry for sync
    #[derive(Debug, Clone)]
    pub struct ChangesetEntry {
        pub pk: String,
        pub columns: HashMap<String, Vec<u8>>,
        pub versions: HashMap<String, u64>,
    }

    impl SqliteDagCrr {
        /// Create a new SQLite-backed DAG-CRR instance
        pub fn new(path: &str) -> Result<Self> {
            let conn = if path == ":memory:" {
                Connection::open_in_memory()?
            } else {
                Connection::open(path)?
            };

            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS crr_data (
                    pk TEXT NOT NULL,
                    col TEXT NOT NULL,
                    value BLOB,
                    version INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (pk, col)
                );

                CREATE TABLE IF NOT EXISTS crr_dag (
                    pk TEXT NOT NULL,
                    col TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    value BLOB,
                    parent1 INTEGER,
                    parent2 INTEGER,
                    PRIMARY KEY (pk, col, version)
                );

                CREATE INDEX IF NOT EXISTS idx_crr_data_pk ON crr_data(pk);
                CREATE INDEX IF NOT EXISTS idx_crr_dag_pk_col ON crr_dag(pk, col);

                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                ",
            )?;

            Ok(Self { conn })
        }

        pub fn insert(&mut self, pk: &str, columns: HashMap<String, Vec<u8>>) -> Result<()> {
            let tx = self.conn.transaction()?;

            for (col, value) in columns {
                let existing: Option<u64> = tx
                    .query_row(
                        "SELECT version FROM crr_data WHERE pk = ? AND col = ?",
                        params![pk, &col],
                        |row| row.get(0),
                    )
                    .ok();

                let new_version = existing.map(|v| v + 1).unwrap_or(1);

                tx.execute(
                    "INSERT OR REPLACE INTO crr_data (pk, col, value, version) VALUES (?, ?, ?, ?)",
                    params![pk, &col, &value, new_version],
                )?;

                let parent = existing;
                tx.execute(
                    "INSERT INTO crr_dag (pk, col, version, value, parent1, parent2) VALUES (?, ?, ?, ?, ?, NULL)",
                    params![pk, &col, new_version, &value, parent],
                )?;
            }

            tx.commit()
        }

        pub fn merge(
            &mut self,
            changeset: &[ChangesetEntry],
            policy: TieBreakPolicy,
        ) -> Result<MergeReport> {
            let mut report = MergeReport::default();
            let tx = self.conn.transaction()?;

            for entry in changeset {
                for (col, val) in &entry.columns {
                    let v_r = entry.versions.get(col).copied().unwrap_or(1);

                    let local: Option<(Vec<u8>, u64)> = tx
                        .query_row(
                            "SELECT value, version FROM crr_data WHERE pk = ? AND col = ?",
                            params![&entry.pk, col],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .ok();

                    match local {
                        None => {
                            tx.execute(
                                "INSERT INTO crr_data (pk, col, value, version) VALUES (?, ?, ?, ?)",
                                params![&entry.pk, col, val, v_r],
                            )?;
                            tx.execute(
                                "INSERT INTO crr_dag (pk, col, version, value, parent1, parent2) VALUES (?, ?, ?, ?, NULL, NULL)",
                                params![&entry.pk, col, v_r, val],
                            )?;
                            report.inserted += 1;
                        }
                        Some((local_val, v_l)) => {
                            if v_r > v_l {
                                tx.execute(
                                    "UPDATE crr_data SET value = ?, version = ? WHERE pk = ? AND col = ?",
                                    params![val, v_r, &entry.pk, col],
                                )?;
                                tx.execute(
                                    "INSERT OR IGNORE INTO crr_dag (pk, col, version, value, parent1, parent2) VALUES (?, ?, ?, ?, ?, ?)",
                                    params![&entry.pk, col, v_r, val, v_l, v_r],
                                )?;
                                report.updated += 1;
                            } else if v_r == v_l && local_val != *val {
                                // Conflict: same version, different values
                                report.conflicts += 1;

                                let winner = match policy {
                                    TieBreakPolicy::PreferExisting => local_val.clone(),
                                    TieBreakPolicy::PreferIncoming => val.clone(),
                                    TieBreakPolicy::LexicographicMin => {
                                        if &local_val < val {
                                            local_val.clone()
                                        } else {
                                            val.clone()
                                        }
                                    }
                                };

                                if winner != local_val {
                                    let v_new = v_r + 1;
                                    tx.execute(
                                        "UPDATE crr_data SET value = ?, version = ? WHERE pk = ? AND col = ?",
                                        params![&winner, v_new, &entry.pk, col],
                                    )?;
                                    tx.execute(
                                        "INSERT OR IGNORE INTO crr_dag (pk, col, version, value, parent1, parent2) VALUES (?, ?, ?, ?, ?, ?)",
                                        params![&entry.pk, col, v_new, &winner, v_l, v_r],
                                    )?;
                                }
                            } else {
                                report.skipped += 1;
                            }
                        }
                    }
                }
            }

            tx.commit()?;
            Ok(report)
        }

        /// Get a row by primary key
        pub fn get_row(&self, pk: &str) -> Result<Option<HashMap<String, (Vec<u8>, u64)>>> {
            let mut stmt = self
                .conn
                .prepare("SELECT col, value, version FROM crr_data WHERE pk = ?")?;

            let mut rows = stmt.query(params![pk])?;
            let mut result: HashMap<String, (Vec<u8>, u64)> = HashMap::new();

            while let Some(row) = rows.next()? {
                let col: String = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                let version: u64 = row.get(2)?;
                result.insert(col, (value, version));
            }

            if result.is_empty() {
                Ok(None)
            } else {
                Ok(Some(result))
            }
        }

        /// Get the DAG history for a column
        pub fn get_history(&self, pk: &str, col: &str) -> Result<Vec<(u64, Vec<u8>)>> {
            let mut stmt = self.conn.prepare(
                "SELECT version, value FROM crr_dag WHERE pk = ? AND col = ? ORDER BY version",
            )?;

            let mut rows = stmt.query(params![pk, col])?;
            let mut history = Vec::new();

            while let Some(row) = rows.next()? {
                let version: u64 = row.get(0)?;
                let value: Vec<u8> = row.get(1)?;
                history.push((version, value));
            }

            Ok(history)
        }

        /// Garbage collect history older than the given version
        pub fn gc_history(&mut self, min_version: u64) -> Result<usize> {
            let deleted = self.conn.execute(
                "DELETE FROM crr_dag WHERE version < ?",
                params![min_version],
            )?;
            Ok(deleted)
        }

        /// Get row count
        pub fn row_count(&self) -> Result<usize> {
            self.conn.query_row(
                "SELECT COUNT(DISTINCT pk) FROM crr_data",
                [],
                |row| row.get(0),
            )
        }

        /// Generate changeset for sync (all rows with version > min_version)
        pub fn generate_changeset(&self, min_version: u64) -> Result<Vec<ChangesetEntry>> {
            let mut stmt = self.conn.prepare(
                "SELECT pk, col, value, version FROM crr_data WHERE version > ? ORDER BY pk",
            )?;

            let mut rows = stmt.query(params![min_version])?;
            let mut entries: HashMap<String, ChangesetEntry> = HashMap::new();

            while let Some(row) = rows.next()? {
                let pk: String = row.get(0)?;
                let col: String = row.get(1)?;
                let value: Vec<u8> = row.get(2)?;
                let version: u64 = row.get(3)?;

                let entry = entries.entry(pk.clone()).or_insert_with(|| ChangesetEntry {
                    pk,
                    columns: HashMap::new(),
                    versions: HashMap::new(),
                });

                entry.columns.insert(col.clone(), value);
                entry.versions.insert(col, version);
            }

            Ok(entries.into_values().collect())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_insert_and_get() {
            let mut db = SqliteDagCrr::new(":memory:").unwrap();

            let mut cols = HashMap::new();
            cols.insert("name".to_string(), b"Alice".to_vec());
            cols.insert("email".to_string(), b"alice@example.com".to_vec());

            db.insert("user1", cols).unwrap();

            let row = db.get_row("user1").unwrap().unwrap();
            assert_eq!(row.get("name").unwrap().0, b"Alice");
            assert_eq!(row.get("email").unwrap().0, b"alice@example.com");
        }

        #[test]
        fn test_merge_conflict_lexmin() {
            let mut db = SqliteDagCrr::new(":memory:").unwrap();

            // Insert initial value
            let mut cols = HashMap::new();
            cols.insert("value".to_string(), b"beta".to_vec());
            db.insert("key1", cols).unwrap();

            // Merge with conflict (same version, different value)
            let changeset = vec![ChangesetEntry {
                pk: "key1".to_string(),
                columns: [("value".to_string(), b"alpha".to_vec())]
                    .into_iter()
                    .collect(),
                versions: [("value".to_string(), 1)].into_iter().collect(),
            }];

            let report = db.merge(&changeset, TieBreakPolicy::LexicographicMin).unwrap();
            assert_eq!(report.conflicts, 1);

            // "alpha" < "beta", so alpha should win
            let row = db.get_row("key1").unwrap().unwrap();
            assert_eq!(row.get("value").unwrap().0, b"alpha");
        }

        #[test]
        fn test_history() {
            let mut db = SqliteDagCrr::new(":memory:").unwrap();

            // Multiple inserts to same column
            for i in 1..=5 {
                let mut cols = HashMap::new();
                cols.insert("value".to_string(), format!("v{}", i).into_bytes());
                db.insert("key1", cols).unwrap();
            }

            let history = db.get_history("key1", "value").unwrap();
            assert_eq!(history.len(), 5);
            assert_eq!(history[0].1, b"v1");
            assert_eq!(history[4].1, b"v5");
        }
    }
}
