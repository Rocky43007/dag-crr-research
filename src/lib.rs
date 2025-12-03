//! DAG+CRR: Distributed Database Synchronization with Per-Column Versioning
//!
//! # Example
//! ```rust
//! use sync_engine::{SyncEngine, crr::TieBreakPolicy};
//!
//! let mut peer_a = SyncEngine::new();
//! let mut peer_b = SyncEngine::new();
//!
//! // Insert data on peer A
//! let mut cols = std::collections::HashMap::new();
//! let mut vers = std::collections::HashMap::new();
//! cols.insert("name".to_string(), "Alice".to_string());
//! vers.insert("name".to_string(), 1);
//! peer_a.crr_table.insert_or_update("user_1", cols, vers);
//!
//! // Sync A -> B
//! let changeset = peer_a.crr_table.changeset();
//! peer_b.crr_table.crr_merge(&changeset, TieBreakPolicy::LexicographicMin);
//!
//! // Both peers now have the same data
//! ```

pub mod ui;
