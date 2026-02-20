use std::collections::HashMap;

use crate::error::Result;
use crate::merge::{MergeReport, TieBreakPolicy};
use crate::storage::Storage;
use crate::table::CrrTable;

#[derive(Debug, Clone)]
pub struct Changeset {
    pub changes: HashMap<String, (HashMap<String, Vec<u8>>, HashMap<String, u64>)>,
}

impl Changeset {
    pub fn new() -> Self {
        Self { changes: HashMap::new() }
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn column_count(&self) -> usize {
        self.changes.values().map(|(cols, _)| cols.len()).sum()
    }

    pub fn estimate_bytes(&self) -> usize {
        self.changes.iter()
            .map(|(pk, (cols, _))| {
                pk.len() + cols.iter().map(|(k, v)| k.len() + v.len() + 8).sum::<usize>()
            })
            .sum()
    }
}

impl Default for Changeset {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct HeadExchange {
    pub peer_id: String,
    pub heads: HashMap<String, HashMap<String, u64>>,
}

impl HeadExchange {
    pub fn from_table<S: Storage>(peer_id: &str, table: &CrrTable<S>) -> Result<Self> {
        let mut heads = HashMap::new();
        let pks = table.pks()?;

        for pk in pks {
            if let Some(row) = table.get(&pk)? {
                let mut col_versions = HashMap::new();
                for (col, _, version) in row.columns() {
                    col_versions.insert(col.to_string(), version);
                }
                heads.insert(pk, col_versions);
            }
        }

        Ok(Self { peer_id: peer_id.to_string(), heads })
    }
}

pub struct SyncSession {
    pub policy: TieBreakPolicy,
}

impl SyncSession {
    pub fn new(policy: TieBreakPolicy) -> Self {
        Self { policy }
    }

    pub fn sync<S: Storage>(
        &self,
        peer_a: &mut CrrTable<S>,
        peer_b: &mut CrrTable<S>,
    ) -> Result<SyncResult> {
        let changeset_a = peer_a.changeset()?;
        let changeset_b = peer_b.changeset()?;

        let report_a = peer_a.merge(&changeset_b, self.policy)?;
        let report_b = peer_b.merge(&changeset_a, self.policy)?;

        Ok(SyncResult {
            a_to_b: report_b,
            b_to_a: report_a,
            bytes_transferred: changeset_a.estimate_bytes() + changeset_b.estimate_bytes(),
        })
    }
}

#[derive(Debug)]
pub struct SyncResult {
    pub a_to_b: MergeReport,
    pub b_to_a: MergeReport,
    pub bytes_transferred: usize,
}

impl SyncResult {
    pub fn total_changes(&self) -> usize {
        self.a_to_b.total_changes() + self.b_to_a.total_changes()
    }

    pub fn total_conflicts(&self) -> usize {
        self.a_to_b.conflicts + self.b_to_a.conflicts
    }
}

pub struct MeshSync<S: Storage> {
    pub peers: Vec<CrrTable<S>>,
    pub policy: TieBreakPolicy,
}

impl<S: Storage> MeshSync<S> {
    pub fn new(policy: TieBreakPolicy) -> Self {
        Self { peers: Vec::new(), policy }
    }

    pub fn add_peer(&mut self, peer: CrrTable<S>) {
        self.peers.push(peer);
    }

    pub fn sync_all(&mut self) -> Result<usize> {
        let mut total_changes = 0;
        let session = SyncSession::new(self.policy);

        for _round in 0..100 {
            let mut round_changes = 0;

            for i in 0..self.peers.len() {
                for j in (i + 1)..self.peers.len() {
                    let (left, right) = self.peers.split_at_mut(j);
                    let peer_a = &mut left[i];
                    let peer_b = &mut right[0];

                    let result = session.sync(peer_a, peer_b)?;
                    round_changes += result.total_changes();
                }
            }

            total_changes += round_changes;
            if round_changes == 0 {
                break;
            }
        }

        Ok(total_changes)
    }
}

// Hard-delete (`CrrTable::delete`) erases the version, so it is NOT
// zombie-safe after GC. Use soft-deletes (tombstone cells) instead.
#[cfg(test)]
mod zombie_data_tests {
    use crate::gc::{run_gc, GcPolicy};
    use crate::merge::TieBreakPolicy;
    use crate::storage::SqliteStorage;
    use crate::sync::SyncSession;
    use crate::table::CrrTable;

    const TOMBSTONE: &[u8] = b"__TOMBSTONE__";

    fn new_peer() -> CrrTable<SqliteStorage> {
        CrrTable::open_in_memory().unwrap()
    }

    fn soft_delete(table: &mut CrrTable<SqliteStorage>, pk: &str, columns: &[&str]) {
        for col in columns {
            let current_version = table
                .get(pk)
                .unwrap()
                .and_then(|row| row.version(col))
                .unwrap_or(0);
            table
                .insert(pk)
                .column(col, TOMBSTONE, current_version + 1)
                .commit()
                .unwrap();
        }
    }

    fn is_tombstone(table: &CrrTable<SqliteStorage>, pk: &str, col: &str) -> bool {
        match table.get(pk).unwrap() {
            Some(row) => row.get(col) == Some(TOMBSTONE),
            None => false,
        }
    }

    fn assert_peers_converged(
        a: &CrrTable<SqliteStorage>,
        b: &CrrTable<SqliteStorage>,
    ) {
        let pks_a = a.pks().unwrap();
        let pks_b = b.pks().unwrap();
        assert_eq!(pks_a, pks_b, "peer PK sets diverge");

        for pk in &pks_a {
            let row_a = a.get(pk).unwrap();
            let row_b = b.get(pk).unwrap();
            match (&row_a, &row_b) {
                (Some(ra), Some(rb)) => {
                    let mut cols_a: Vec<_> = ra.columns().map(|(c, v, ver)| (c.to_string(), v.to_vec(), ver)).collect();
                    let mut cols_b: Vec<_> = rb.columns().map(|(c, v, ver)| (c.to_string(), v.to_vec(), ver)).collect();
                    cols_a.sort_by(|x, y| x.0.cmp(&y.0));
                    cols_b.sort_by(|x, y| x.0.cmp(&y.0));
                    assert_eq!(cols_a, cols_b, "rows diverge for pk={}", pk);
                }
                (None, None) => {}
                _ => panic!("row presence diverges for pk={}", pk),
            }
        }
    }

    // A soft-deletes (v=2), B stays offline (v=1), A runs GC, B reconnects.
    // Delete must win: v=2 > v=1.
    #[test]
    fn test_zombie_data_basic() {
        let session = SyncSession::new(TieBreakPolicy::LexicographicMin);

        let mut peer_a = new_peer();
        let mut peer_b = new_peer();

        peer_a
            .insert("r1")
            .column_str("name", "Alice", 1)
            .column_str("email", "alice@example.com", 1)
            .commit()
            .unwrap();

        peer_b
            .insert("r1")
            .column_str("name", "Alice", 1)
            .column_str("email", "alice@example.com", 1)
            .commit()
            .unwrap();

        soft_delete(&mut peer_a, "r1", &["name", "email"]);

        assert!(is_tombstone(&peer_a, "r1", "name"));
        assert!(is_tombstone(&peer_a, "r1", "email"));

        let gc_removed = run_gc(&mut peer_a, GcPolicy::KeepLast(1)).unwrap();
        assert!(gc_removed > 0, "GC should have pruned some history");

        assert!(is_tombstone(&peer_a, "r1", "name"));
        assert!(is_tombstone(&peer_a, "r1", "email"));

        let result = session.sync(&mut peer_a, &mut peer_b).unwrap();

        assert!(is_tombstone(&peer_a, "r1", "name"), "zombie: deleted data reappeared on A");
        assert!(is_tombstone(&peer_b, "r1", "name"), "B should adopt tombstone");
        assert!(is_tombstone(&peer_a, "r1", "email"), "zombie: deleted email reappeared on A");
        assert!(is_tombstone(&peer_b, "r1", "email"), "B should adopt tombstone email");
        assert!(result.a_to_b.total_changes() > 0, "A's tombstones should propagate to B");
        assert_peers_converged(&peer_a, &peer_b);
    }

    #[test]
    fn test_gc_preserves_current_versions() {
        let mut table = new_peer();

        table.insert("r1").column_str("name", "v1", 1).commit().unwrap();
        table.update("r1").column_str("name", "v2").commit().unwrap();
        table.update("r1").column_str("name", "v3").commit().unwrap();
        table.update("r1").column_str("name", "v4").commit().unwrap();
        table.update("r1").column_str("name", "v5").commit().unwrap();

        let version_before = table.get("r1").unwrap().unwrap().version("name").unwrap();
        let value_before = table.get("r1").unwrap().unwrap().get_string("name").unwrap();

        let history_before = table.get("r1").unwrap().unwrap()
            .dag_history("name").unwrap().len();
        assert_eq!(history_before, 5, "should have 5 history nodes before GC");

        let removed = run_gc(&mut table, GcPolicy::KeepLast(2)).unwrap();
        assert_eq!(removed, 3, "should prune 3 of 5 history nodes");

        let version_after = table.get("r1").unwrap().unwrap().version("name").unwrap();
        let value_after = table.get("r1").unwrap().unwrap().get_string("name").unwrap();

        assert_eq!(version_before, version_after, "GC must not change current version");
        assert_eq!(value_before, value_after, "GC must not change current value");

        let history_after = table.get("r1").unwrap().unwrap()
            .dag_history("name").unwrap().len();
        assert_eq!(history_after, 2, "should have 2 history nodes after GC");
    }

    // Peers with different GC depths must still converge on current state.
    #[test]
    fn test_asymmetric_gc_sync_correctness() {
        let session = SyncSession::new(TieBreakPolicy::LexicographicMin);

        let mut peer_a = new_peer();
        let mut peer_b = new_peer();

        for pk_idx in 0..5 {
            let pk = format!("row_{}", pk_idx);
            peer_a
                .insert(&pk)
                .column_str("col", &format!("init_{}", pk_idx), 1)
                .commit()
                .unwrap();
            peer_b
                .insert(&pk)
                .column_str("col", &format!("init_{}", pk_idx), 1)
                .commit()
                .unwrap();
        }

        for round in 0..4 {
            for pk_idx in 0..5 {
                let pk = format!("row_{}", pk_idx);
                peer_a
                    .update(&pk)
                    .column_str("col", &format!("a_r{}_{}", round, pk_idx))
                    .commit()
                    .unwrap();
            }
        }
        let gc_removed = run_gc(&mut peer_a, GcPolicy::KeepLast(2)).unwrap();
        assert!(gc_removed > 0, "Peer A should have pruned history");

        for pk_idx in 0..5 {
            let pk = format!("row_{}", pk_idx);
            peer_b
                .update(&pk)
                .column_str("col", &format!("b_only_{}", pk_idx))
                .commit()
                .unwrap();
        }

        // A is at v5 (1 insert + 4 updates), B at v2 (1 insert + 1 update).
        session.sync(&mut peer_a, &mut peer_b).unwrap();

        assert_peers_converged(&peer_a, &peer_b);

        for pk_idx in 0..5 {
            let pk = format!("row_{}", pk_idx);
            let val_a = peer_a.get(&pk).unwrap().unwrap().get_string("col").unwrap();
            let val_b = peer_b.get(&pk).unwrap().unwrap().get_string("col").unwrap();
            assert_eq!(val_a, val_b);
            assert_eq!(val_a, format!("a_r3_{}", pk_idx), "Peer A's latest value should win");
        }

        let result2 = session.sync(&mut peer_a, &mut peer_b).unwrap();
        assert_eq!(result2.total_changes(), 0, "second sync should be a no-op");
    }

    #[test]
    fn test_offline_peer_reconnect_after_gc() {
        let session = SyncSession::new(TieBreakPolicy::LexicographicMin);

        let mut peer_a = new_peer();
        let mut peer_b = new_peer();

        for i in 0..100 {
            let pk = format!("row_{:03}", i);
            peer_a
                .insert(&pk)
                .column_str("name", &format!("name_{}", i), 1)
                .column_str("status", "active", 1)
                .commit()
                .unwrap();
            peer_b
                .insert(&pk)
                .column_str("name", &format!("name_{}", i), 1)
                .column_str("status", "active", 1)
                .commit()
                .unwrap();
        }

        assert_eq!(peer_a.len().unwrap(), 100);
        assert_eq!(peer_b.len().unwrap(), 100);

        for round in 0..3 {
            for i in (0..100).step_by(2) {
                let pk = format!("row_{:03}", i);
                peer_a
                    .update(&pk)
                    .column_str("status", &format!("a_round_{}", round))
                    .commit()
                    .unwrap();
            }
        }

        for i in [0, 2, 4] {
            let pk = format!("row_{:03}", i);
            soft_delete(&mut peer_a, &pk, &["name", "status"]);
        }

        for i in (1..100).step_by(2) {
            let pk = format!("row_{:03}", i);
            peer_b
                .update(&pk)
                .column_str("name", &format!("b_updated_{}", i))
                .commit()
                .unwrap();
        }

        let gc_removed = run_gc(&mut peer_a, GcPolicy::KeepLast(2)).unwrap();
        assert!(gc_removed > 0, "should prune A's accumulated history");

        for i in [0, 2, 4] {
            let pk = format!("row_{:03}", i);
            assert!(
                is_tombstone(&peer_a, &pk, "name"),
                "tombstone for {} should survive GC",
                pk
            );
            assert!(
                is_tombstone(&peer_a, &pk, "status"),
                "tombstone status for {} should survive GC",
                pk
            );
        }

        session.sync(&mut peer_a, &mut peer_b).unwrap();
        assert_peers_converged(&peer_a, &peer_b);

        for i in [0, 2, 4] {
            let pk = format!("row_{:03}", i);
            assert!(
                is_tombstone(&peer_a, &pk, "name"),
                "zombie: row {} name reappeared on A after sync",
                pk
            );
            assert!(
                is_tombstone(&peer_b, &pk, "name"),
                "zombie: row {} name not tombstoned on B after sync",
                pk
            );
        }

        for i in (1..100).step_by(2) {
            let pk = format!("row_{:03}", i);
            let val = peer_a.get(&pk).unwrap().unwrap().get_string("name").unwrap();
            assert_eq!(
                val,
                format!("b_updated_{}", i),
                "B's update for {} should appear on A",
                pk
            );
        }

        // Rows 0,2,4 are tombstoned; remaining even rows should have A's status
        for i in (6..100).step_by(2) {
            let pk = format!("row_{:03}", i);
            let val = peer_b.get(&pk).unwrap().unwrap().get_string("status").unwrap();
            assert_eq!(
                val, "a_round_2",
                "A's latest status for {} should appear on B",
                pk
            );
        }

        let result2 = session.sync(&mut peer_a, &mut peer_b).unwrap();
        assert_eq!(result2.total_changes(), 0, "converged peers should produce no further changes");
    }
}
