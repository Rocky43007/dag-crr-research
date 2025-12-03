use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TieBreakPolicy {
    PreferExisting,
    PreferIncoming,
    LexicographicMin,
}

#[derive(Debug, Clone, Default)]
pub struct MergeReport {
    pub inserted: usize,
    pub updated: usize,
    pub skipped: usize,
    pub conflicts: usize,
}

impl MergeReport {
    pub fn total_changes(&self) -> usize {
        self.inserted + self.updated
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeDecision {
    Accept,
    Reject,
    Conflict,
}

pub(crate) fn resolve_versions(local_version: u64, remote_version: u64) -> MergeDecision {
    match local_version.cmp(&remote_version) {
        std::cmp::Ordering::Less => MergeDecision::Accept,
        std::cmp::Ordering::Greater => MergeDecision::Reject,
        std::cmp::Ordering::Equal => MergeDecision::Conflict,
    }
}

pub(crate) fn resolve_conflict(
    local_value: &[u8],
    remote_value: &[u8],
    policy: TieBreakPolicy,
) -> bool {
    match policy {
        TieBreakPolicy::PreferExisting => false,
        TieBreakPolicy::PreferIncoming => true,
        TieBreakPolicy::LexicographicMin => remote_value < local_value,
    }
}
