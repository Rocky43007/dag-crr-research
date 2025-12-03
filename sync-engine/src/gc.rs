use crate::error::Result;
use crate::storage::Storage;
use crate::table::CrrTable;

#[derive(Debug, Clone, Copy)]
pub enum GcPolicy {
    KeepLast(usize),
    KeepAll,
}

impl Default for GcPolicy {
    fn default() -> Self {
        GcPolicy::KeepLast(10)
    }
}

pub fn run_gc<S: Storage>(table: &mut CrrTable<S>, policy: GcPolicy) -> Result<usize> {
    match policy {
        GcPolicy::KeepAll => Ok(0),
        GcPolicy::KeepLast(n) => table.gc(n),
    }
}
