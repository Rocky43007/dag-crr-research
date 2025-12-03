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
