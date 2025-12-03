//! Interactive demo for DAG+CRR database synchronization.

use super::theme;
use gpui::{
    div, prelude::*, px, rgb, white, App, Context, Entity, FocusHandle, Focusable, IntoElement,
    MouseButton, MouseDownEvent, Render, Window,
};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use sync_engine::crr::{LegacyMergeReport, TieBreakPolicy};
use sync_engine::schema::SchemaMigration;
use sync_engine::transactions::TransactionOp;
use sync_engine::SyncEngine;

#[derive(Clone)]
#[allow(dead_code)]
struct PeerState {
    engine: SyncEngine,
    name: String,
    is_online: bool,
    network_delay_ms: u64,
    packet_loss_rate: f32, // 0.0 to 1.0
    last_sync_time: Option<Instant>,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct SyncAnimation {
    from_peer: usize,
    to_peer: usize,
    started_at: Instant,
    duration_ms: u64,
    changeset_size: usize,
}

#[derive(Clone, Debug)]
struct ConflictHighlight {
    peer_index: usize,
    row_pk: String,
    column: String,
    until: Instant,
}

#[derive(Clone, Debug, PartialEq)]
enum DemoScenario {
    CrrFundamentals,     // Intro + VersionProgression + SimpleUpdate
    ConflictsAndOffline, // ConflictResolution + NetworkPartition
    DagRecovery,         // DAG demonstration
    DatabaseFeatures,    // Schema + FK + Transactions
    StressTest,          // Keep existing
    ProductionEcommerce, // 5-peer production demo
}

impl DemoScenario {
    fn title(&self) -> &'static str {
        match self {
            Self::CrrFundamentals => "Scenario 1: CRR Fundamentals",
            Self::ConflictsAndOffline => "Scenario 2: Conflicts & Offline Sync",
            Self::DagRecovery => "Scenario 3: DAG Recovery",
            Self::DatabaseFeatures => "Scenario 4: Database Features",
            Self::StressTest => "Scenario 5: Stress Test",
            Self::ProductionEcommerce => "Scenario 6: Production E-Commerce",
        }
    }

    fn description(&self) -> &'static str {
        match self {
            Self::CrrFundamentals => {
                "Introduction to CRR: Per-column versioning, version progression (v1→v2→v3),\nand independent column updates. Shows how higher versions always win."
            }
            Self::ConflictsAndOffline => {
                "Demonstrates conflict resolution with tiebreaker policies,\nand network partition scenarios where peers go offline and sync later."
            }
            Self::DagRecovery => {
                "Shows version DAG with missing versions (v1→v2→v4→v5, v3 missing).\nDemonstrates DAG visualization, detection, and reconstruction of missing data."
            }
            Self::DatabaseFeatures => {
                "Schema evolution (ADD/RENAME columns), foreign keys with CASCADE delete,\nand atomic transactions with rollback capabilities."
            }
            Self::StressTest => {
                "Large dataset (100+ rows) with random updates across all peers.\nDemonstrates O(n) merge performance and convergence at scale."
            }
            Self::ProductionEcommerce => {
                "Full production scenario: 5 peers (stores, mobile, cloud), schema evolution,\n10+ customers, 6+ products, offline sync, FK CASCADE, transactions."
            }
        }
    }

    fn steps(&self) -> Vec<&'static str> {
        match self {
            Self::CrrFundamentals => vec![
                "[INTRO] Initialize 3 peers with empty tables",
                "[INTRO] Peer A creates user_1: name='Alice'",
                "[INTRO] Peer A creates user_2: name='Bob'",
                "[INTRO] Peer A creates user_3: name='Carol'",
                "[INTRO] Sync A→B and A→C, all peers converged",
                "[VERSION] Peer A: doc_1 status='draft' (v=1)",
                "[VERSION] Peer B: doc_1 status='review' (v=2)",
                "[VERSION] Peer C: doc_1 status='published' (v=3)",
                "[VERSION] Sync all: v3 wins everywhere (highest version)",
                "[VERSION] Result: All peers have status='published' (v=3)",
                "[VERSION] Conclusion: Higher version always wins!",
                "[INDEPENDENT] All peers start with user_1: name='Alice', city='Boston'",
                "[INDEPENDENT] Peer A updates city='NYC' (different column)",
                "[INDEPENDENT] Peer B updates name='Alicia' (different column)",
                "[INDEPENDENT] Sync A→B: B gets city='NYC'",
                "[INDEPENDENT] Sync B→A: A gets name='Alicia'",
                "[INDEPENDENT] A and B converged, but C still has old data",
                "[INDEPENDENT] Sync A→C and B→C: Peer C gets both updates",
                "[INDEPENDENT] All 3 peers converged: name='Alicia', city='NYC' - No conflicts!",
            ],
            Self::ConflictsAndOffline => vec![
                "[CONFLICT] All peers start with user_1: name='Alice' (v=1)",
                "[CONFLICT] Peer A updates name='Alice Smith' (v=2)",
                "[CONFLICT] Peer B updates name='Alice Jones' (v=2) - SAME VERSION!",
                "[CONFLICT] Sync A→B: CONFLICT detected (v2 == v2, different values)",
                "[CONFLICT] Apply tiebreaker policy to resolve",
                "[CONFLICT] Sync B→A: Ensure convergence",
                "[CONFLICT] Both peers converged using policy",
                "[CONFLICT] Try different policies to see different outcomes",
                "[CONFLICT] Conflict resolution complete!",
                "[PARTITION] All peers online and synchronized",
                "[PARTITION] Create 5 users on all peers",
                "[PARTITION] Network DOWN - Peers B and C offline",
                "[PARTITION] Peer A makes offline edits (3 updates)",
                "[PARTITION] Peer B makes different offline edits (2 updates)",
                "[PARTITION] Network RESTORED - Peers reconnect",
                "[PARTITION] Sync A→B: B receives A's changes",
                "[PARTITION] Sync B→A: A receives B's changes",
                "[PARTITION] Sync A→C and B→C: C receives all changes",
                "[PARTITION] Final convergence: All 3 peers identical!",
            ],
            Self::DagRecovery => vec![
                "[DAG] Initialize Peer A with empty DAG",
                "[DAG] Add version 1: 'Initial draft'",
                "[DAG] Add version 2: 'First revision'",
                "[DAG] Skip version 3 (simulating missing/lost version)",
                "[DAG] Add version 4: 'Major update' (parent: v2, v3 missing!)",
                "[DAG] Add version 5: 'Final version' (parent: v4)",
                "[DAG] Visualize DAG structure: v1→v2→[v3?]→v4→v5",
                "[DAG] Detect missing version: v3 not in DAG",
                "[DAG] Reconstruct v3 from causal inference (v2 and v4)",
                "[DAG] Add reconstructed v3 to DAG",
                "[DAG] Show complete timeline with reconstructed data",
                "[DAG] DAG recovery demonstration complete!",
            ],
            Self::DatabaseFeatures => vec![
                "[SCHEMA] Initialize 2 peers with 5 users",
                "[SCHEMA] Peer A: Add column 'email' (schema v1→v2)",
                "[SCHEMA] Peer A: Add column 'phone' (schema v2→v3)",
                "[SCHEMA] Sync schema changes A→B",
                "[SCHEMA] Peer B: Rename column 'phone'→'mobile' (schema v3→v4)",
                "[SCHEMA] Sync schema back B→A",
                "[FK] Create 'users' and 'orders' tables",
                "[FK] Add 6 orders referencing users (FK constraint)",
                "[FK] Sync tables A→B",
                "[FK] Delete user with orders (CASCADE delete)",
                "[FK] Verify orders deleted automatically",
                "[TX] Begin transaction: multi-table update",
                "[TX] Add operations: insert user + insert order",
                "[TX] Commit transaction atomically",
                "[TX] Begin second transaction: updates",
                "[TX] ROLLBACK transaction (demonstrate abort)",
                "[TX] Sync everything A→B: Full convergence!",
            ],
            Self::StressTest => vec![
                "Generate 100 rows per peer",
                "Random updates: 50% of rows modified",
                "Introduce random conflicts",
                "Merge all peers",
                "Verify convergence",
                "Measure merge time",
                "Display statistics",
                "Sync back for full convergence",
                "Stress test complete!",
            ],
            Self::ProductionEcommerce => vec![
                "[SETUP] Initialize 5 peers: Store A, Store B, Store C, Mobile, Cloud HQ",
                "[SETUP] Cloud HQ: Schema v1 - customers + products tables",
                "[SETUP] Cloud HQ: Create 10 customers",
                "[SETUP] Cloud HQ: Create 6 products",
                "[SETUP] Sync Cloud→All stores and mobile",
                "[EVOLUTION] Store A: Schema v2 - Add 'loyalty_points' column",
                "[EVOLUTION] Mobile: Schema v3 - Add 'last_login' column",
                "[EVOLUTION] Sync schema updates across all peers",
                "[ORDERS] Store A creates 3 orders (online customers)",
                "[ORDERS] Store B creates 2 orders (different customers)",
                "[ORDERS] Mobile creates 1 order",
                "[OFFLINE] Store C goes OFFLINE",
                "[OFFLINE] Store C makes 4 local changes offline",
                "[OFFLINE] Meanwhile: Store A and B sync to Cloud",
                "[OFFLINE] Store C comes ONLINE and syncs to Cloud",
                "[FK-CASCADE] Cloud HQ: Delete customer with orders (CASCADE)",
                "[TRANSACTIONS] Mobile: Begin transaction (customer + order)",
                "[TRANSACTIONS] Mobile: Commit transaction atomically",
                "[CONVERGENCE] Final sync: All peers converge to same state!",
            ],
        }
    }
}

pub struct ProfessionalDemo {
    focus_handle: FocusHandle,

    // Peer state
    peers: Vec<PeerState>,
    target_peer_index: usize,

    // Demo control
    current_scenario: DemoScenario,
    current_step: usize,
    is_auto_playing: bool,
    auto_play_next_at: Option<Instant>,
    auto_play_speed_ms: u64,

    // Network simulation
    sync_animations: Vec<SyncAnimation>,
    conflict_highlights: Vec<ConflictHighlight>,

    // Merge state
    last_merge_report: Option<LegacyMergeReport>,
    tiebreak_policy: TieBreakPolicy,

    // UI state
    log_messages: VecDeque<String>,
    max_log_messages: usize,
    show_merge_details: bool,
}

impl Focusable for ProfessionalDemo {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl ProfessionalDemo {
    pub fn new(cx: &mut App) -> Entity<Self> {
        cx.new(|cx| {
            let mut demo = Self {
                focus_handle: cx.focus_handle(),
                peers: Vec::new(),
                target_peer_index: 0,
                current_scenario: DemoScenario::CrrFundamentals,
                current_step: 0,
                is_auto_playing: false,
                auto_play_next_at: None,
                auto_play_speed_ms: 2000,
                sync_animations: Vec::new(),
                conflict_highlights: Vec::new(),
                last_merge_report: None,
                tiebreak_policy: TieBreakPolicy::PreferExisting,
                log_messages: VecDeque::new(),
                max_log_messages: 15,
                show_merge_details: true,
            };

            demo.initialize_peers(3);
            demo.log("Welcome to CRR Database Sync Demo!");
            demo.log("Select a scenario from the menu to begin.");
            demo
        })
    }

    fn initialize_peers(&mut self, count: usize) {
        self.peers.clear();
        for i in 0..count {
            self.peers.push(PeerState {
                engine: SyncEngine::new(),
                name: format!("Peer {}", (b'A' + i as u8) as char),
                is_online: true,
                network_delay_ms: 100,
                packet_loss_rate: 0.0,
                last_sync_time: None,
            });
        }
        self.target_peer_index = 0;
        self.sync_animations.clear();
        self.conflict_highlights.clear();
        self.last_merge_report = None;
    }

    fn initialize_peers_with_names(&mut self, names: Vec<&str>) {
        self.peers.clear();
        for name in names {
            self.peers.push(PeerState {
                engine: SyncEngine::new(),
                name: name.to_string(),
                is_online: true,
                network_delay_ms: 100,
                packet_loss_rate: 0.0,
                last_sync_time: None,
            });
        }
        self.target_peer_index = 0;
        self.sync_animations.clear();
        self.conflict_highlights.clear();
        self.last_merge_report = None;
    }

    fn reset_scenario(&mut self) {
        self.current_step = 0;
        self.is_auto_playing = false;
        self.auto_play_next_at = None;
        self.last_merge_report = None;
        self.sync_animations.clear();
        self.conflict_highlights.clear();

        match self.current_scenario {
            DemoScenario::CrrFundamentals => {
                self.initialize_peers(3);
                self.log("Scenario reset: CRR Fundamentals");
            }
            DemoScenario::ConflictsAndOffline => {
                self.initialize_peers(3);
                self.log("Scenario reset: Conflicts & Offline Sync");
            }
            DemoScenario::DagRecovery => {
                self.initialize_peers(1);
                // DAG is now automatically managed per-column in CrrRow
                self.log("Scenario reset: DAG Recovery");
            }
            DemoScenario::DatabaseFeatures => {
                self.initialize_peers(2);
                self.log("Scenario reset: Database Features");
            }
            DemoScenario::StressTest => {
                self.initialize_peers(3);
                self.log("Scenario reset: Stress Test");
            }
            DemoScenario::ProductionEcommerce => {
                self.initialize_peers_with_names(vec![
                    "Store A", "Store B", "Store C", "Mobile", "Cloud HQ",
                ]);
                self.target_peer_index = 4; // Cloud HQ
                self.log("Scenario reset: Production E-Commerce");
            }
        }
    }

    fn execute_step(&mut self) {
        match self.current_scenario {
            DemoScenario::CrrFundamentals => self.step_crr_fundamentals(),
            DemoScenario::ConflictsAndOffline => self.step_conflicts_and_offline(),
            DemoScenario::DagRecovery => self.step_dag_recovery(),
            DemoScenario::DatabaseFeatures => self.step_database_features(),
            DemoScenario::StressTest => self.step_stress_test(),
            DemoScenario::ProductionEcommerce => self.step_production_ecommerce(),
        }

        self.current_step += 1;
    }

    fn step_crr_fundamentals(&mut self) {
        match self.current_step {
            // Part 1: Introduction (steps 0-4)
            0 => {
                self.log("[PEER A,B,C] Initialized with empty tables");
            }
            1 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "Alice".to_string());
                vers.insert("name".to_string(), 1);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("user_1", cols, vers);
                self.add_conflict_highlight(0, "user_1", "name", 1500);
                self.log("[PEER A] Created user_1: name='Alice' (v=1)");
            }
            2 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "Bob".to_string());
                vers.insert("name".to_string(), 1);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("user_2", cols, vers);
                self.add_conflict_highlight(0, "user_2", "name", 1500);
                self.log("[PEER A] Created user_2: name='Bob' (v=1)");
            }
            3 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "Carol".to_string());
                vers.insert("name".to_string(), 1);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("user_3", cols, vers);
                self.add_conflict_highlight(0, "user_3", "name", 1500);
                self.log("[PEER A] Created user_3: name='Carol' (v=1)");
            }
            4 => {
                self.sync_peers(0, 1);
                self.sync_peers(0, 2);
                self.log("[SYNC] A→B and A→C completed. All peers have 3 users!");
            }
            // Part 2: Version Progression (steps 5-10)
            5 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("status".to_string(), "draft".to_string());
                vers.insert("status".to_string(), 1);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("doc_1", cols, vers);
                self.add_conflict_highlight(0, "doc_1", "status", 1500);
                self.log("[PEER A] doc_1: status='draft' (v=1)");
            }
            6 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("status".to_string(), "review".to_string());
                vers.insert("status".to_string(), 2);
                self.peers[1]
                    .engine
                    .crr_table
                    .insert_or_update("doc_1", cols, vers);
                self.add_conflict_highlight(1, "doc_1", "status", 1500);
                self.log("[PEER B] doc_1: status='review' (v=2)");
            }
            7 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("status".to_string(), "published".to_string());
                vers.insert("status".to_string(), 3);
                self.peers[2]
                    .engine
                    .crr_table
                    .insert_or_update("doc_1", cols, vers);
                self.add_conflict_highlight(2, "doc_1", "status", 1500);
                self.log("[PEER C] doc_1: status='published' (v=3)");
            }
            8 => {
                self.sync_peers(1, 0);
                self.sync_peers(2, 1);
                self.sync_peers(2, 0);
                self.log("[SYNC] All peers sync: v3 > v2 > v1");
            }
            9 => {
                self.log("[RESULT] All peers now have status='published' (v=3)");
            }
            10 => {
                self.log("[CONCLUSION] Higher version always wins! v3 beats v2 and v1");
            }
            // Part 3: Independent Columns (steps 11-16)
            11 => {
                for peer in &mut self.peers {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("name".to_string(), "Alice".to_string());
                    cols.insert("city".to_string(), "Boston".to_string());
                    vers.insert("name".to_string(), 1);
                    vers.insert("city".to_string(), 1);
                    peer.engine.crr_table.insert_or_update("user_1", cols, vers);
                }
                self.log("[ALL PEERS] user_1: name='Alice' (v1), city='Boston' (v1)");
            }
            12 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("city".to_string(), "NYC".to_string());
                vers.insert("city".to_string(), 2);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("user_1", cols, vers);
                self.add_conflict_highlight(0, "user_1", "city", 1500);
                self.log("[PEER A] Updated city='NYC' (v=2) - different column");
            }
            13 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "Alicia".to_string());
                vers.insert("name".to_string(), 2);
                self.peers[1]
                    .engine
                    .crr_table
                    .insert_or_update("user_1", cols, vers);
                self.add_conflict_highlight(1, "user_1", "name", 1500);
                self.log("[PEER B] Updated name='Alicia' (v=2) - different column");
            }
            14 => {
                self.sync_peers(0, 1);
                self.log("[SYNC] A→B: B gets city='NYC' (v2 > v1)");
            }
            15 => {
                self.sync_peers(1, 0);
                self.log("[SYNC] B→A: A gets name='Alicia' (v2 > v1)");
            }
            16 => {
                self.log("[RESULT] A and B converged: name='Alicia'(v2), city='NYC'(v2)");
                self.log("But Peer C still has old data... let's sync!");
            }
            17 => {
                self.sync_peers(0, 2); // A → C
                self.sync_peers(1, 2); // B → C (redundant but complete)
                self.log("[SYNC] A→C and B→C: Peer C gets both updates");
            }
            18 => {
                self.log("[CONVERGED] All 3 peers now have: name='Alicia'(v2), city='NYC'(v2)");
                self.log("No conflicts - different columns updated independently!");
                self.log("✓ CRR Fundamentals: Higher version wins + Independent columns!");
            }
            _ => {
                self.log("CRR Fundamentals complete! Try next scenario.");
            }
        }
    }

    fn step_conflicts_and_offline(&mut self) {
        match self.current_step {
            // Part 1: Conflict Resolution (steps 0-8)
            0 => {
                for peer in &mut self.peers {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("name".to_string(), "Alice".to_string());
                    vers.insert("name".to_string(), 1);
                    peer.engine.crr_table.insert_or_update("user_1", cols, vers);
                }
                self.log("[ALL PEERS] user_1: name='Alice' (v=1)");
            }
            1 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "Alice Smith".to_string());
                vers.insert("name".to_string(), 2);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("user_1", cols, vers);
                self.add_conflict_highlight(0, "user_1", "name", 1500);
                self.log("[PEER A] Updated name='Alice Smith' (v=2)");
            }
            2 => {
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "Alice Jones".to_string());
                vers.insert("name".to_string(), 2);
                self.peers[1]
                    .engine
                    .crr_table
                    .insert_or_update("user_1", cols, vers);
                self.add_conflict_highlight(1, "user_1", "name", 1500);
                self.log("[PEER B] Updated name='Alice Jones' (v=2) - SAME VERSION!");
            }
            3 => {
                self.sync_peers(0, 1);
                if let Some(ref report) = self.last_merge_report {
                    if !report.conflicts_equal_version.is_empty() {
                        self.log("[CONFLICT] v2 == v2 but values differ!");
                        self.log(&format!("Using policy: {:?}", self.tiebreak_policy));
                    }
                }
            }
            4 => {
                self.sync_peers(1, 0);
                self.log("[SYNC] B→A: Ensure convergence");
            }
            5 => {
                self.log("[RESOLVED] Both peers converged using tiebreaker policy");
            }
            6 => {
                self.log("Try PreferExisting, PreferIncoming, or LexicographicMin");
            }
            7 => {
                self.log("to see different conflict resolution outcomes!");
            }
            8 => {
                self.log("[COMPLETE] Conflict resolution demonstration finished");
            }
            // Part 2: Network Partition (steps 9-17)
            9 => {
                for peer in &mut self.peers {
                    peer.is_online = true;
                }
                self.log("[NETWORK] All peers online and synchronized");
            }
            10 => {
                for i in 1..=5 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("name".to_string(), format!("User_{}", i));
                    cols.insert("status".to_string(), "active".to_string());
                    vers.insert("name".to_string(), 1);
                    vers.insert("status".to_string(), 1);
                    for peer in &mut self.peers {
                        peer.engine.crr_table.insert_or_update(
                            &format!("user_{}", i),
                            cols.clone(),
                            vers.clone(),
                        );
                    }
                }
                self.log("[ALL PEERS] Created 5 users (user_1 to user_5)");
            }
            11 => {
                self.peers[1].is_online = false;
                self.peers[2].is_online = false;
                self.log("[NETWORK DOWN] Peers B and C are OFFLINE");
            }
            12 => {
                for i in 1..=3 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("status".to_string(), format!("updated_by_A_{}", i));
                    vers.insert("status".to_string(), 2);
                    self.peers[0].engine.crr_table.insert_or_update(
                        &format!("user_{}", i),
                        cols,
                        vers,
                    );
                }
                self.log("[PEER A OFFLINE] Made 3 updates while disconnected");
            }
            13 => {
                for i in 3..=5 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("status".to_string(), format!("updated_by_B_{}", i));
                    vers.insert("status".to_string(), 2);
                    self.peers[1].engine.crr_table.insert_or_update(
                        &format!("user_{}", i),
                        cols,
                        vers,
                    );
                }
                self.log("[PEER B OFFLINE] Made 2 different updates offline");
            }
            14 => {
                self.peers[1].is_online = true;
                self.peers[2].is_online = true;
                self.log("[NETWORK RESTORED] Peers B and C back online!");
            }
            15 => {
                self.sync_peers(0, 1);
                self.log("[SYNC] A→B: B receives A's offline changes");
            }
            16 => {
                self.sync_peers(1, 0);
                self.log("[SYNC] B→A: A receives B's offline changes");
            }
            17 => {
                self.sync_peers(0, 2);
                self.sync_peers(1, 2);
                self.log("[SYNC] A→C and B→C: Peer C receives all changes");
            }
            18 => {
                // Final convergence: sync C back to A and B
                self.sync_peers(2, 0);
                self.sync_peers(2, 1);
                self.log("[CONVERGENCE] C→A and C→B: All 3 peers now identical!");
                self.log("Network partition handled - CRR preserves all changes!");
            }
            _ => {
                self.log("Conflicts & Offline scenario complete!");
            }
        }
    }

    fn step_dag_recovery(&mut self) {
        match self.current_step {
            0 => {
                self.log("[DAG] Demonstrating per-column version history");
                self.log("      DAG is now automatically tracked for each column!");
            }
            1 => {
                // Create document with v1
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("content".to_string(), "Initial draft".to_string());
                vers.insert("content".to_string(), 1);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("doc_1", cols, vers);
                self.log("[DAG] doc_1.content = 'Initial draft' (v1)");
                self.log("      DAG node created automatically!");
            }
            2 => {
                // Update to v2
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("content".to_string(), "First revision".to_string());
                vers.insert("content".to_string(), 2);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("doc_1", cols, vers);
                self.log("[DAG] doc_1.content = 'First revision' (v2)");
                self.log("      DAG: v1 -> v2");
            }
            3 => {
                self.log("[DAG] Simulating gap: skipping v3, jumping to v4...");
                self.log("      (In real scenarios, v3 might be lost in network partition)");
            }
            4 => {
                // Skip v3, add v4 with v3 as parent (simulating received from peer with gap)
                if let Some(row) = self.peers[0].engine.crr_table.rows.get_mut("doc_1") {
                    // Directly add v4 to DAG with v3 as parent (v3 doesn't exist)
                    let dag = row.dags.entry("content".to_string()).or_insert_with(sync_engine::dag::VersionDag::new);
                    dag.add_node(4, "Major update".to_string(), vec![3]); // v3 is missing!
                    row.columns.insert("content".to_string(), "Major update".to_string());
                    row.versions.insert("content".to_string(), 4);
                }
                self.log("[DAG] doc_1.content = 'Major update' (v4, parent: v3)");
                self.log("      v3 is MISSING from DAG!");
            }
            5 => {
                // Add v5
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("content".to_string(), "Final version".to_string());
                vers.insert("content".to_string(), 5);
                self.peers[0]
                    .engine
                    .crr_table
                    .insert_or_update("doc_1", cols, vers);
                self.log("[DAG] doc_1.content = 'Final version' (v5)");
                self.log("      Timeline: v1 -> v2 -> [v3?] -> v4 -> v5");
            }
            6 => {
                if let Some(row) = self.peers[0].engine.crr_table.rows.get("doc_1") {
                    if let Some(dag) = row.dags.get("content") {
                        self.log(&format!("[DAG STRUCTURE] Nodes in content DAG: {}", dag.nodes.len()));
                        self.log("      v1 -> v2 -> [v3 MISSING] -> v4 -> v5");
                    }
                }
            }
            7 => {
                if let Some(row) = self.peers[0].engine.crr_table.rows.get("doc_1") {
                    if let Some(dag) = row.dags.get("content") {
                        let missing = dag.find_missing_versions();
                        self.log(&format!("[DETECTED] Missing versions: {:?}", missing));
                    }
                }
            }
            8 => {
                let (missing_str, reconstructed_str) = if let Some(row) = self.peers[0].engine.crr_table.rows.get("doc_1") {
                    if let Some(dag) = row.dags.get("content") {
                        let missing = dag.find_missing_versions();
                        let reconstructed = dag.reconstruct_missing_version(3);
                        (
                            format!("{:?}", missing),
                            reconstructed.unwrap_or_else(|| "Unknown".to_string()),
                        )
                    } else {
                        ("No DAG".to_string(), "Unknown".to_string())
                    }
                } else {
                    ("No row".to_string(), "Unknown".to_string())
                };

                self.log("[ANALYSIS] Detecting missing versions...");
                self.log(&format!("      Found missing: {}", missing_str));
                self.log(&format!("      Reconstructed v3: '{}'", reconstructed_str));
            }
            9 => {
                self.log("[RECOVERY] Inserting reconstructed v3 into DAG...");

                if let Some(row) = self.peers[0].engine.crr_table.rows.get_mut("doc_1") {
                    if let Some(dag) = row.dags.get_mut("content") {
                        if let Some(reconstructed) = dag.reconstruct_missing_version(3) {
                            dag.add_node(3, reconstructed, vec![2]);
                            self.log("      v3 now in DAG between v2 and v4!");
                        }
                    }
                }
            }
            10 => {
                self.log("[TIMELINE] Complete version history for doc_1.content:");
                if let Some(row) = self.peers[0].engine.crr_table.rows.get("doc_1") {
                    if let Some(dag) = row.dags.get("content") {
                        let timeline = dag.get_reconstructed_timeline();
                        for (v, val, is_reconstructed) in timeline {
                            let marker = if is_reconstructed { " (reconstructed)" } else { "" };
                            self.log(&format!("      v{}: '{}'{}", v, val, marker));
                        }
                    }
                }
            }
            11 => {
                self.log("[COMPLETE] DAG Recovery Finished!");
                self.log("      - DAG is automatically tracked per-column");
                self.log("      - Missing versions detected via parent references");
                self.log("      - Reconstruction uses causal inference");
                self.log("");
                self.log("Key insight: Per-column DAG enables full audit trail!");
            }
            _ => {
                self.log("DAG Recovery scenario complete!");
            }
        }
    }

    fn step_database_features(&mut self) {
        match self.current_step {
            0 => {
                // Initialize both peers with 5 users (using crr_table for UI visibility)
                for i in 1..=5 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("name".to_string(), format!("User_{}", i));
                    cols.insert("email".to_string(), format!("user{}@example.com", i));
                    vers.insert("name".to_string(), 1);
                    vers.insert("email".to_string(), 1);
                    for peer in &mut self.peers {
                        peer.engine.crr_table.insert_or_update(
                            &format!("user_{}", i),
                            cols.clone(),
                            vers.clone(),
                        );
                    }
                }
                self.log("[BOTH PEERS] Initialized with 5 users (name, email)");
                self.log("       Schema v1: [name, email]");
            }
            1 => {
                // Peer A: ADD phone column + UPDATE some users with phone data
                // Add phone numbers to users 1-3
                for i in 1..=3 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("phone".to_string(), format!("555-000{}", i));
                    vers.insert("phone".to_string(), 1);
                    self.peers[0].engine.crr_table.insert_or_update(
                        &format!("user_{}", i),
                        cols,
                        vers,
                    );
                }

                self.log("[PEER A] Schema v1→v2: ADD COLUMN 'phone'");
                self.log("       Updated users 1-3 with phone numbers");
            }
            2 => {
                // Peer B still on old schema, adds data
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("name".to_string(), "User_6".to_string());
                cols.insert("email".to_string(), "user6@example.com".to_string());
                vers.insert("name".to_string(), 1);
                vers.insert("email".to_string(), 1);
                self.peers[1]
                    .engine
                    .crr_table
                    .insert_or_update("user_6", cols, vers);

                self.log("[PEER B] Still on schema v1 (no 'phone' column)");
                self.log("       Added user_6 with old schema");
            }
            3 => {
                // Sync A→B: Peer B receives schema upgrade + phone data
                self.sync_peers(0, 1);
                self.log("[SYNC] A→B: Cross-schema sync!");
                self.log("       Peer B learns about 'phone' column");
                self.log("       Peer A learns about user_6");
            }
            4 => {
                // Set status for all users on Peer A
                for i in 1..=6 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("status".to_string(), "active".to_string());
                    vers.insert("status".to_string(), 1);
                    self.peers[0].engine.crr_table.insert_or_update(
                        &format!("user_{}", i),
                        cols,
                        vers,
                    );
                }

                self.log("[PEER A] Schema v2→v3: ADD COLUMN 'status'");
                self.log("       Set all users to status='active'");
            }
            5 => {
                // Peer B makes an update using old column 'email'
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("email".to_string(), "newemail@example.com".to_string());
                vers.insert("email".to_string(), 2);
                self.peers[1]
                    .engine
                    .crr_table
                    .insert_or_update("user_1", cols, vers);

                self.log("[PEER B] Still on schema v1, updates user_1.email");
                self.log("       Peer B doesn't know about 'phone' or 'status' yet");
            }
            6 => {
                // Sync B→A
                self.sync_peers(1, 0);
                self.log("[SYNC] B→A: Email update (v2) propagates");
                self.log("       Different schema versions coexist!");
            }
            7 => {
                // Final sync to ensure convergence
                self.sync_peers(0, 1);
                self.log("[CONVERGENCE] Both peers now have all data");
                self.log("       Peer A: 6 users with [name, email, phone, status]");
                self.log("       Peer B: learned new columns through sync");
            }
            8 => {
                self.log("[DEMONSTRATION] Schema migration features:");
                self.log("       ✓ ADD COLUMN - new fields added seamlessly");
                self.log("       ✓ Cross-schema sync - old ↔ new compatible");
                self.log("       ✓ Per-column versions track schema evolution");
                self.log("       ✓ No data loss despite schema changes!");
            }
            _ => {
                self.log("Database Features scenario complete!");
            }
        }
    }

    fn step_stress_test(&mut self) {
        match self.current_step {
            0 => {
                self.log("Generating 100 rows per peer...");
                for peer in &mut self.peers {
                    for i in 0..100 {
                        let mut cols = HashMap::new();
                        let mut vers = HashMap::new();
                        cols.insert("value".to_string(), format!("val_{}", i));
                        cols.insert("data".to_string(), format!("data_{}", i));
                        vers.insert("value".to_string(), 1);
                        vers.insert("data".to_string(), 1);
                        peer.engine
                            .crr_table
                            .insert_or_update(&format!("row_{}", i), cols, vers);
                    }
                }
                self.log("Generated 100 rows on each of 3 peers (300 total)");
            }
            1 => {
                self.log("Applying random updates (50% of rows)...");
                use rand::Rng;
                let mut rng = rand::thread_rng();
                for (peer_idx, peer) in self.peers.iter_mut().enumerate() {
                    for i in 0..50 {
                        let row_id = rng.gen_range(0..100);
                        let mut cols = HashMap::new();
                        let mut vers = HashMap::new();
                        cols.insert("value".to_string(), format!("updated_p{}_{}", peer_idx, i));
                        vers.insert("value".to_string(), 2);
                        peer.engine.crr_table.insert_or_update(
                            &format!("row_{}", row_id),
                            cols,
                            vers,
                        );
                    }
                }
                self.log("Applied 150 random updates across all peers");
            }
            2 => {
                self.log("Introducing random conflicts...");
                use rand::Rng;
                let mut rng = rand::thread_rng();
                for _ in 0..10 {
                    let row_id = rng.gen_range(0..100);
                    for (peer_idx, peer) in self.peers.iter_mut().enumerate() {
                        let mut cols = HashMap::new();
                        let mut vers = HashMap::new();
                        cols.insert("data".to_string(), format!("conflict_p{}", peer_idx));
                        vers.insert("data".to_string(), 3);
                        peer.engine.crr_table.insert_or_update(
                            &format!("row_{}", row_id),
                            cols,
                            vers,
                        );
                    }
                }
                self.log("Created 10 intentional conflicts");
            }
            3 => {
                self.log("Merging all peers into Peer A...");
                let start = Instant::now();
                for i in 1..self.peers.len() {
                    self.sync_peers(i, 0);
                }
                let elapsed = start.elapsed();
                self.log(&format!("Merge completed in {:?}", elapsed));
            }
            4 => {
                self.log("Verifying convergence...");
                let row_count = self.peers[0].engine.crr_table.rows.len();
                self.log(&format!("Peer A now has {} rows", row_count));
            }
            5 => {
                if let Some(ref report) = self.last_merge_report {
                    self.log(&format!(
                        "Statistics: Inserted={}, Updated={}, Conflicts={}",
                        report.inserted.len(),
                        report.updated.len(),
                        report.conflicts_equal_version.len()
                    ));
                }
            }
            6 => {
                if let Some(ref report) = self.last_merge_report {
                    self.log(&format!("Skipped (older): {}", report.skipped_older.len()));
                    self.log("Merge complexity: O(n) where n = changeset size");
                }
            }
            7 => {
                for i in 1..self.peers.len() {
                    self.sync_peers(0, i);
                }
                self.log("Synced back to all peers for full convergence");
            }
            8 => {
                self.log("Stress test complete! All peers converged.");
                self.log("Performance: O(n) merge scales to large datasets");
            }
            _ => {
                self.log("Stress test scenario complete!");
            }
        }
    }

    fn step_production_ecommerce(&mut self) {
        match self.current_step {
            0 => {
                self.log(&format!(
                    "[SETUP] 5 Peers: {} | {} | {} | {} | {}",
                    self.peers[0].name,
                    self.peers[1].name,
                    self.peers[2].name,
                    self.peers[3].name,
                    self.peers[4].name
                ));
            }
            1 => {
                // Cloud HQ creates initial schema
                let cloud = 4;
                self.peers[cloud].engine.schema_manager.apply_migration(
                    SchemaMigration::AddColumn {
                        name: "name".to_string(),
                        col_type: sync_engine::schema::ColumnType::Text,
                        nullable: false,
                    },
                );
                self.log("[CLOUD HQ] Schema v1: customers + products tables");
            }
            2 => {
                // Cloud HQ creates 10 customers
                for i in 1..=10 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("name".to_string(), format!("Customer_{}", i));
                    cols.insert("email".to_string(), format!("customer{}@example.com", i));
                    vers.insert("name".to_string(), 1);
                    vers.insert("email".to_string(), 1);
                    self.peers[4]
                        .engine
                        .get_table("customers")
                        .insert_or_update(&format!("c{}", i), cols, vers);
                }
                self.log("[CLOUD HQ] Created 10 customers");
            }
            3 => {
                // Cloud HQ creates 6 products
                for i in 1..=6 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("name".to_string(), format!("Product_{}", i));
                    cols.insert("price".to_string(), format!("{}.99", 10 + i * 5));
                    vers.insert("name".to_string(), 1);
                    vers.insert("price".to_string(), 1);
                    self.peers[4].engine.get_table("products").insert_or_update(
                        &format!("p{}", i),
                        cols,
                        vers,
                    );
                }
                self.log("[CLOUD HQ] Created 6 products");
            }
            4 => {
                // Sync to all stores and mobile
                for i in 0..4 {
                    self.sync_peers(4, i);
                }
                self.log("[SYNC] Cloud→All stores and mobile completed");
            }
            5 => {
                // Store A adds loyalty_points column
                self.peers[0]
                    .engine
                    .schema_manager
                    .apply_migration(SchemaMigration::AddColumn {
                        name: "loyalty_points".to_string(),
                        col_type: sync_engine::schema::ColumnType::Integer,
                        nullable: true,
                    });
                self.log("[STORE A] Schema v2: Added 'loyalty_points' column");
            }
            6 => {
                // Mobile adds last_login column
                self.peers[3]
                    .engine
                    .schema_manager
                    .apply_migration(SchemaMigration::AddColumn {
                        name: "last_login".to_string(),
                        col_type: sync_engine::schema::ColumnType::Integer,
                        nullable: true,
                    });
                self.log("[MOBILE] Schema v3: Added 'last_login' column");
            }
            7 => {
                self.log("[SYNC SCHEMA] Schema updates propagated across all peers");
            }
            8 => {
                // Store A creates 3 orders
                for i in 1..=3 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("customer_id".to_string(), format!("c{}", i));
                    cols.insert("product_id".to_string(), format!("p{}", i));
                    cols.insert("quantity".to_string(), format!("{}", i));
                    vers.insert("customer_id".to_string(), 1);
                    vers.insert("product_id".to_string(), 1);
                    vers.insert("quantity".to_string(), 1);
                    self.peers[0].engine.get_table("orders").insert_or_update(
                        &format!("o{}", i),
                        cols,
                        vers,
                    );
                }
                self.log("[STORE A] Created 3 orders");
            }
            9 => {
                // Store B creates 2 orders
                for i in 4..=5 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("customer_id".to_string(), format!("c{}", i));
                    cols.insert("product_id".to_string(), format!("p{}", i - 3));
                    cols.insert("quantity".to_string(), "1".to_string());
                    vers.insert("customer_id".to_string(), 1);
                    vers.insert("product_id".to_string(), 1);
                    vers.insert("quantity".to_string(), 1);
                    self.peers[1].engine.get_table("orders").insert_or_update(
                        &format!("o{}", i),
                        cols,
                        vers,
                    );
                }
                self.log("[STORE B] Created 2 orders");
            }
            10 => {
                // Mobile creates 1 order
                let mut cols = HashMap::new();
                let mut vers = HashMap::new();
                cols.insert("customer_id".to_string(), "c6".to_string());
                cols.insert("product_id".to_string(), "p6".to_string());
                cols.insert("quantity".to_string(), "2".to_string());
                vers.insert("customer_id".to_string(), 1);
                vers.insert("product_id".to_string(), 1);
                vers.insert("quantity".to_string(), 1);
                self.peers[3]
                    .engine
                    .get_table("orders")
                    .insert_or_update("o6", cols, vers);
                self.log("[MOBILE] Created 1 order");
            }
            11 => {
                self.peers[2].is_online = false;
                self.log("[STORE C] Goes OFFLINE (network partition)");
            }
            12 => {
                // Store C makes offline changes
                for i in 7..=10 {
                    let mut cols = HashMap::new();
                    let mut vers = HashMap::new();
                    cols.insert("customer_id".to_string(), format!("c{}", i));
                    cols.insert("product_id".to_string(), "p1".to_string());
                    cols.insert("quantity".to_string(), "1".to_string());
                    vers.insert("customer_id".to_string(), 1);
                    vers.insert("product_id".to_string(), 1);
                    vers.insert("quantity".to_string(), 1);
                    self.peers[2].engine.get_table("orders").insert_or_update(
                        &format!("o{}", i),
                        cols,
                        vers,
                    );
                }
                self.log("[STORE C OFFLINE] Made 4 local orders offline");
            }
            13 => {
                self.sync_peers(0, 4);
                self.sync_peers(1, 4);
                self.sync_peers(3, 4);
                self.log("[SYNC] Store A, B, Mobile → Cloud (while C offline)");
            }
            14 => {
                self.peers[2].is_online = true;
                self.sync_peers(2, 4);
                self.log("[STORE C] Comes ONLINE and syncs to Cloud");
            }
            15 => {
                // Cloud HQ deletes customer with CASCADE
                self.peers[4]
                    .engine
                    .get_table("customers")
                    .rows
                    .remove("c3");

                // Cascade delete orders
                let orders_to_delete: Vec<String> = self.peers[4]
                    .engine
                    .get_table("orders")
                    .rows
                    .iter()
                    .filter(|(_, row)| row.columns.get("customer_id") == Some(&"c3".to_string()))
                    .map(|(pk, _)| pk.clone())
                    .collect();

                for order_pk in orders_to_delete {
                    self.peers[4]
                        .engine
                        .get_table("orders")
                        .rows
                        .remove(&order_pk);
                }

                self.log("[CLOUD HQ] Deleted c3 → CASCADE deleted orders");
            }
            16 => {
                // Mobile begins transaction
                let tx_id = self.peers[3].engine.tx_manager.begin();
                self.log(&format!("[MOBILE TX] Started transaction: {}", tx_id));
            }
            17 => {
                // Mobile commits transaction
                let tx_id = format!("tx_{}", self.peers[3].engine.tx_manager.transactions.len());
                let _ = self.peers[3].engine.tx_manager.add_operation(
                    &tx_id,
                    TransactionOp::Insert {
                        table: "customers".to_string(),
                        pk: "c11".to_string(),
                        columns: {
                            let mut cols = HashMap::new();
                            cols.insert("name".to_string(), "New Mobile Customer".to_string());
                            cols
                        },
                    },
                );
                let _ = self.peers[3].engine.tx_manager.add_operation(
                    &tx_id,
                    TransactionOp::Insert {
                        table: "orders".to_string(),
                        pk: "o11".to_string(),
                        columns: {
                            let mut cols = HashMap::new();
                            cols.insert("customer_id".to_string(), "c11".to_string());
                            cols.insert("product_id".to_string(), "p1".to_string());
                            cols
                        },
                    },
                );
                // Split borrow to avoid double mutable borrow
                let peer = &mut self.peers[3];
                let _ = peer
                    .engine
                    .tx_manager
                    .commit(&tx_id, &mut peer.engine.tables);
                self.log("[MOBILE TX] Committed: customer + order atomically");
            }
            18 => {
                // Final convergence
                for i in 0..4 {
                    self.sync_peers(i, 4);
                }
                for i in 0..4 {
                    self.sync_peers(4, i);
                }
                self.log("[CONVERGENCE] All peers synced to same state!");
                self.log("Production E-Commerce: Schema, FK, TX, Offline all working!");
            }
            _ => {
                self.log("Production E-Commerce scenario complete!");
            }
        }
    }

    fn sync_peers(&mut self, from_idx: usize, to_idx: usize) {
        if from_idx >= self.peers.len() || to_idx >= self.peers.len() {
            return;
        }

        // Check if peers are online
        if !self.peers[from_idx].is_online || !self.peers[to_idx].is_online {
            self.log("Sync failed: Peer offline");
            return;
        }

        // Simulate network delay
        let changeset = self.peers[from_idx].engine.crr_table.changeset();
        let changeset_size = changeset.len();

        // Add animation
        self.sync_animations.push(SyncAnimation {
            from_peer: from_idx,
            to_peer: to_idx,
            started_at: Instant::now(),
            duration_ms: self.peers[from_idx].network_delay_ms,
            changeset_size,
        });

        // Apply merge
        let report = self.peers[to_idx]
            .engine
            .crr_table
            .crr_merge(&changeset, self.tiebreak_policy);

        self.last_merge_report = Some(report.clone());
        self.peers[to_idx].last_sync_time = Some(Instant::now());

        // Highlight conflicts
        for (pk, col, _v, _, _) in &report.conflicts_equal_version {
            self.add_conflict_highlight(to_idx, pk, col, 2000);
        }
    }

    fn add_conflict_highlight(&mut self, peer_idx: usize, pk: &str, col: &str, duration_ms: u64) {
        self.conflict_highlights.push(ConflictHighlight {
            peer_index: peer_idx,
            row_pk: pk.to_string(),
            column: col.to_string(),
            until: Instant::now() + Duration::from_millis(duration_ms),
        });
    }

    fn tick_auto_play(&mut self) {
        if !self.is_auto_playing {
            return;
        }

        let now = Instant::now();
        if let Some(next_at) = self.auto_play_next_at {
            if now < next_at {
                return;
            }
        }

        let max_steps = self.current_scenario.steps().len();
        if self.current_step >= max_steps {
            // Current scenario complete, move to next
            let next_scenario = self.get_next_scenario();
            if let Some(next) = next_scenario {
                self.log(&format!("Scenario complete! Moving to: {}", next.title()));
                self.current_scenario = next;
                self.reset_scenario();
                self.auto_play_next_at =
                    Some(now + Duration::from_millis(self.auto_play_speed_ms * 2));
            } else {
                // All scenarios complete!
                self.is_auto_playing = false;
                self.log("All scenarios complete! Auto-play finished.");
            }
        } else {
            self.execute_step();
            self.auto_play_next_at = Some(now + Duration::from_millis(self.auto_play_speed_ms));
        }
    }

    fn get_next_scenario(&self) -> Option<DemoScenario> {
        match self.current_scenario {
            DemoScenario::CrrFundamentals => Some(DemoScenario::ConflictsAndOffline),
            DemoScenario::ConflictsAndOffline => Some(DemoScenario::DagRecovery),
            DemoScenario::DagRecovery => Some(DemoScenario::DatabaseFeatures),
            DemoScenario::DatabaseFeatures => Some(DemoScenario::StressTest),
            DemoScenario::StressTest => Some(DemoScenario::ProductionEcommerce),
            DemoScenario::ProductionEcommerce => None,
        }
    }

    fn log(&mut self, message: &str) {
        self.log_messages.push_front(message.to_string());
        if self.log_messages.len() > self.max_log_messages {
            self.log_messages.pop_back();
        }
    }

    fn cleanup_animations(&mut self) {
        let now = Instant::now();
        self.sync_animations.retain(|anim| {
            now.duration_since(anim.started_at).as_millis() < anim.duration_ms as u128
        });
        self.conflict_highlights.retain(|hl| now < hl.until);
    }

    fn render_peer_card(&self, peer_idx: usize, peer: &PeerState) -> impl IntoElement {
        let is_target = peer_idx == self.target_peer_index;
        let now = Instant::now();

        // Detect if this scenario uses multi-table format
        let use_multi_table = !peer.engine.tables.is_empty();

        // Collect rows from either crr_table or tables HashMap
        let mut rows: Vec<_> = peer.engine.crr_table.rows.values().collect();
        rows.sort_by_key(|r| &r.pk);

        // Colors
        let bg_color = if is_target {
            rgb(0x1a2a1a)
        } else if !peer.is_online {
            rgb(0x2a1a1a)
        } else {
            rgb(theme::PANEL_BACKGROUND)
        };

        let border_color = if is_target {
            rgb(0x00ff88)
        } else if !peer.is_online {
            rgb(0xff4444)
        } else {
            rgb(theme::BORDER_COLOR)
        };

        div()
            .flex()
            .flex_col()
            .gap_2()
            .bg(bg_color)
            .border_1()
            .border_color(border_color)
            .rounded_lg()
            .p_3()
            .min_w(px(280.))
            .child(
                // Header
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(gpui::FontWeight::BOLD)
                            .text_color(if is_target {
                                rgb(0x00ff88)
                            } else {
                                rgb(theme::TEXT_COLOR)
                            })
                            .child(format!(
                                "{}{}",
                                peer.name,
                                if is_target { " [TARGET]" } else { "" }
                            )),
                    )
                    .child(
                        div()
                            .px_2()
                            .py_1()
                            .rounded_md()
                            .bg(if peer.is_online {
                                rgb(0x0e7a0d)
                            } else {
                                rgb(0x7a0d0d)
                            })
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(white())
                                    .child(if peer.is_online { "ONLINE" } else { "OFFLINE" }),
                            ),
                    ),
            )
            .child(
                // Table - either single table or multi-table format
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .children(if use_multi_table {
                        // Multi-table rendering
                        self.render_multi_tables(peer_idx, peer, now)
                    } else if rows.is_empty() {
                        vec![div()
                            .text_xs()
                            .text_color(rgb(theme::MUTED_TEXT))
                            .child("(empty table)")
                            .into_any_element()]
                    } else {
                        // Single table rendering (existing code)
                        rows.iter()
                            .map(|row| {
                                let mut cols: Vec<_> = row.columns.iter().collect();
                                cols.sort_by_key(|(k, _)| *k);

                                div()
                                    .flex()
                                    .flex_col()
                                    .gap_1()
                                    .p_2()
                                    .bg(rgb(0x2a2a2a))
                                    .rounded_md()
                                    .child(
                                        div()
                                            .text_xs()
                                            .font_weight(gpui::FontWeight::SEMIBOLD)
                                            .text_color(rgb(0x9cdcfe))
                                            .child(format!("PK: {}", row.pk)),
                                    )
                                    .children(cols.iter().map(|(col_name, col_val)| {
                                        let version =
                                            row.versions.get(*col_name).copied().unwrap_or(0);

                                        // Check if highlighted
                                        let is_highlighted =
                                            self.conflict_highlights.iter().any(|hl| {
                                                hl.peer_index == peer_idx
                                                    && hl.row_pk == row.pk
                                                    && hl.column == **col_name
                                                    && now < hl.until
                                            });

                                        div()
                                            .flex()
                                            .flex_row()
                                            .gap_2()
                                            .items_center()
                                            .p_1()
                                            .rounded_sm()
                                            .bg(if is_highlighted {
                                                rgb(0x4a3a2a)
                                            } else {
                                                rgb(0x1a1a1a)
                                            })
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0xdcdcaa))
                                                    .child(format!("{}: ", col_name)),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0xce9178))
                                                    .child(format!("\"{}\"", col_val)),
                                            )
                                            .child(
                                                div().px_1().rounded_sm().bg(rgb(0x3a3a5a)).child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(rgb(0xaaaaff))
                                                        .child(format!("v{}", version)),
                                                ),
                                            )
                                    }))
                                    .into_any_element()
                            })
                            .collect()
                    }),
            )
            .when(peer.last_sync_time.is_some(), |d| {
                d.child(
                    div()
                        .text_xs()
                        .text_color(rgb(theme::MUTED_TEXT))
                        .child(format!(
                            "Last sync: {:?} ago",
                            now.duration_since(peer.last_sync_time.unwrap())
                        )),
                )
            })
    }

    fn render_multi_tables(
        &self,
        peer_idx: usize,
        peer: &PeerState,
        now: Instant,
    ) -> Vec<gpui::AnyElement> {
        let mut elements = Vec::new();

        // Sort table names for consistent display
        let mut table_names: Vec<_> = peer.engine.tables.keys().collect();
        table_names.sort();

        for table_name in table_names {
            if let Some(table) = peer.engine.tables.get(table_name.as_str()) {
                // Table header
                elements.push(
                    div()
                        .px_2()
                        .py_1()
                        .mt_2()
                        .rounded_sm()
                        .bg(rgb(0x3a3a5a))
                        .child(
                            div()
                                .text_xs()
                                .font_weight(gpui::FontWeight::BOLD)
                                .text_color(rgb(0xddddff))
                                .child(format!("📊 {}", table_name)),
                        )
                        .into_any_element(),
                );

                // Table rows
                let mut rows: Vec<_> = table.rows.values().collect();
                rows.sort_by_key(|r| &r.pk);

                if rows.is_empty() {
                    elements.push(
                        div()
                            .text_xs()
                            .text_color(rgb(theme::MUTED_TEXT))
                            .pl_4()
                            .child("(empty)")
                            .into_any_element(),
                    );
                } else {
                    for row in rows {
                        let mut cols: Vec<_> = row.columns.iter().collect();
                        cols.sort_by_key(|(k, _)| *k);

                        elements.push(
                            div()
                                .flex()
                                .flex_col()
                                .gap_1()
                                .p_2()
                                .ml_2()
                                .bg(rgb(0x2a2a2a))
                                .rounded_md()
                                .child(
                                    div()
                                        .text_xs()
                                        .font_weight(gpui::FontWeight::SEMIBOLD)
                                        .text_color(rgb(0x9cdcfe))
                                        .child(format!("PK: {}", row.pk)),
                                )
                                .children(cols.iter().map(|(col_name, col_val)| {
                                    let version = row.versions.get(*col_name).copied().unwrap_or(0);

                                    // Check if highlighted
                                    let is_highlighted =
                                        self.conflict_highlights.iter().any(|hl| {
                                            hl.peer_index == peer_idx
                                                && hl.row_pk == row.pk
                                                && hl.column == **col_name
                                                && now < hl.until
                                        });

                                    div()
                                        .flex()
                                        .flex_row()
                                        .gap_2()
                                        .items_center()
                                        .p_1()
                                        .rounded_sm()
                                        .bg(if is_highlighted {
                                            rgb(0x4a3a2a)
                                        } else {
                                            rgb(0x1a1a1a)
                                        })
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(rgb(0xdcdcaa))
                                                .child(format!("{}: ", col_name)),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(rgb(0xce9178))
                                                .child(format!("\"{}\"", col_val)),
                                        )
                                        .child(
                                            div().px_1().rounded_sm().bg(rgb(0x3a3a5a)).child(
                                                div()
                                                    .text_xs()
                                                    .text_color(rgb(0xaaaaff))
                                                    .child(format!("v{}", version)),
                                            ),
                                        )
                                }))
                                .into_any_element(),
                        );
                    }
                }
            }
        }

        elements
    }

    fn render_scenario_selector(&self, cx: &mut gpui::prelude::Context<Self>) -> impl IntoElement {
        let scenarios = [
            DemoScenario::CrrFundamentals,
            DemoScenario::ConflictsAndOffline,
            DemoScenario::DagRecovery,
            DemoScenario::DatabaseFeatures,
            DemoScenario::StressTest,
            DemoScenario::ProductionEcommerce,
        ];

        div()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                div()
                    .flex()
                    .flex_row()
                    .gap_2()
                    .flex_wrap()
                    .children(scenarios.iter().map(|scenario| {
                        let is_current = scenario == &self.current_scenario;
                        let is_completed = self.is_scenario_completed(scenario);
                        let scenario_clone = scenario.clone();

                        div()
                        .px_3()
                        .py_2()
                        .rounded_md()
                        .bg(if is_current {
                            rgb(0x007acc)
                        } else if is_completed {
                            rgb(0x0e7a0d)
                        } else {
                            rgb(0x3a3a3a)
                        })
                        .cursor_pointer()
                        .hover(|s| {
                            s.bg(rgb(if is_current {
                                0x0088ee
                            } else if is_completed {
                                0x1e9a1d
                            } else {
                                0x4a4a4a
                            }))
                        })
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(
                                move |this,
                                      _: &MouseDownEvent,
                                      _: &mut Window,
                                      cx: &mut Context<Self>| {
                                    this.current_scenario = scenario_clone.clone();
                                    this.reset_scenario();
                                    cx.notify();
                                },
                            ),
                        )
                        .child(
                            div()
                                .flex()
                                .flex_row()
                                .gap_2()
                                .items_center()
                                .child(div().text_xs().text_color(white()).child(scenario.title()))
                                .when(is_completed && !is_current, |d| {
                                    d.child(div().text_xs().child("Done"))
                                })
                                .when(is_current && self.is_auto_playing, |d| {
                                    d.child(div().text_xs().child("Playing"))
                                }),
                        )
                    })),
            )
            .when(self.is_auto_playing, |d| {
                d.child(div().px_3().py_1().bg(rgb(0x2a2a3a)).rounded_md().child(
                    div().text_xs().text_color(rgb(0xaaaaff)).child(format!(
                        "Auto-playing: Step {}/{} in {}",
                        self.current_step.min(self.current_scenario.steps().len()),
                        self.current_scenario.steps().len(),
                        self.current_scenario.title()
                    )),
                ))
            })
    }

    fn is_scenario_completed(&self, scenario: &DemoScenario) -> bool {
        if !self.is_auto_playing {
            return false;
        }

        let scenario_order = [
            DemoScenario::CrrFundamentals,
            DemoScenario::ConflictsAndOffline,
            DemoScenario::DagRecovery,
            DemoScenario::DatabaseFeatures,
            DemoScenario::StressTest,
            DemoScenario::ProductionEcommerce,
        ];

        if let (Some(scenario_idx), Some(current_idx)) = (
            scenario_order.iter().position(|s| s == scenario),
            scenario_order
                .iter()
                .position(|s| s == &self.current_scenario),
        ) {
            scenario_idx < current_idx
        } else {
            false
        }
    }
}

impl Render for ProfessionalDemo {
    fn render(
        &mut self,
        _window: &mut Window,
        cx: &mut gpui::prelude::Context<Self>,
    ) -> impl IntoElement {
        self.tick_auto_play();
        self.cleanup_animations();

        if self.is_auto_playing
            || !self.sync_animations.is_empty()
            || !self.conflict_highlights.is_empty()
        {
            cx.notify();
        }

        div()
            .id("main-scroll-container")
            .flex()
            .flex_col()
            .size_full()
            .overflow_y_scroll()
            .bg(rgb(theme::BACKGROUND))
            .text_color(rgb(theme::TEXT_COLOR))
            .p_4()
            .gap_4()
            .child(
                // Header
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .justify_between()
                    .pb_3()
                    .border_b_1()
                    .border_color(rgb(theme::BORDER_COLOR))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_xl()
                                    .font_weight(gpui::FontWeight::BOLD)
                                    .child("CRR Database Synchronization Demo")
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(rgb(theme::MUTED_TEXT))
                                    .child("Conflict-free Replicated Relations with Per-Column Versioning")
                            )
                    )
                    .child(
                        div()
                            .flex()
                            .flex_row()
                            .gap_2()
                            .child(
                                div()
                                    .px_3()
                                    .py_1()
                                    .rounded_md()
                                    .bg(rgb(if self.is_auto_playing { 0x7a0d0d } else { 0x0e7a0d }))
                                    .cursor_pointer()
                                    .hover(|s| s.bg(rgb(if self.is_auto_playing { 0x9a1d1d } else { 0x1e9a1d })))
                                    .on_mouse_down(MouseButton::Left, cx.listener(|this, _: &MouseDownEvent, _: &mut Window, cx: &mut Context<Self>| {
                                        if this.is_auto_playing {
                                            this.is_auto_playing = false;
                                            this.auto_play_next_at = None;
                                            this.log("Auto-play stopped");
                                        } else {
                                            this.is_auto_playing = true;
                                            this.auto_play_next_at = Some(Instant::now());
                                            this.log("Auto-play started");
                                        }
                                        cx.notify();
                                    }))
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(white())
                                            .child(if self.is_auto_playing { "Pause" } else { "Auto-Play" })
                                    )
                            )
                    )
            )
            .child(
                // Scenario selector
                self.render_scenario_selector(cx)
            )
            .child(
                // Current scenario info
                div()
                    .flex()
                    .flex_col()
                    .gap_2()
                    .p_3()
                    .bg(rgb(0x252526))
                    .rounded_md()
                    .child(
                        div()
                            .text_base()
                            .font_weight(gpui::FontWeight::SEMIBOLD)
                            .text_color(rgb(0x4ec9b0))
                            .child(self.current_scenario.title())
                    )
                    .child(
                        div()
                            .text_sm()
                            .text_color(rgb(theme::TEXT_COLOR))
                            .child(self.current_scenario.description())
                    )
                    .child(
                        div()
                            .flex()
                            .flex_row()
                            .gap_2()
                            .mt_2()
                            .child(
                                div()
                                    .px_3()
                                    .py_1()
                                    .rounded_md()
                                    .bg(rgb(0x0066cc))
                                    .cursor_pointer()
                                    .hover(|s| s.bg(rgb(0x0088ee)))
                                    .on_mouse_down(MouseButton::Left, cx.listener(|this, _: &MouseDownEvent, _: &mut Window, cx: &mut Context<Self>| {
                                        this.execute_step();
                                        cx.notify();
                                    }))
                                    .child(div().text_sm().text_color(white()).child("Next Step"))
                            )
                            .child(
                                div()
                                    .px_3()
                                    .py_1()
                                    .rounded_md()
                                    .bg(rgb(0x4a4a4a))
                                    .cursor_pointer()
                                    .hover(|s| s.bg(rgb(0x5a5a5a)))
                                    .on_mouse_down(MouseButton::Left, cx.listener(|this, _: &MouseDownEvent, _: &mut Window, cx: &mut Context<Self>| {
                                        this.reset_scenario();
                                        cx.notify();
                                    }))
                                    .child(div().text_sm().text_color(white()).child("Reset"))
                            )
                            .child(
                                div()
                                    .px_3()
                                    .py_1()
                                    .rounded_md()
                                    .bg(rgb(0x6a4a6a))
                                    .cursor_pointer()
                                    .hover(|s| s.bg(rgb(0x7a5a7a)))
                                    .on_mouse_down(MouseButton::Left, cx.listener(|this, _: &MouseDownEvent, _: &mut Window, cx: &mut Context<Self>| {
                                        this.tiebreak_policy = match this.tiebreak_policy {
                                            TieBreakPolicy::PreferExisting => TieBreakPolicy::PreferIncoming,
                                            TieBreakPolicy::PreferIncoming => TieBreakPolicy::LexicographicMin,
                                            TieBreakPolicy::LexicographicMin => TieBreakPolicy::PreferExisting,
                                        };
                                        this.log(&format!("Tiebreak policy: {:?}", this.tiebreak_policy));
                                        cx.notify();
                                    }))
                                    .child(div().text_sm().text_color(white()).child(format!("Policy: {:?}", self.tiebreak_policy)))
                            )
                            .child(
                                div()
                                    .px_2()
                                    .py_1()
                                    .bg(rgb(0x2a2a3a))
                                    .rounded_md()
                                    .child(div().text_sm().child(format!("Step {}/{}",
                                        self.current_step.min(self.current_scenario.steps().len()),
                                        self.current_scenario.steps().len())))
                            )
                    )
            )
            .child(
                // Peers grid
                div()
                    .flex()
                    .flex_row()
                    .gap_4()
                    .flex_wrap()
                    .children(self.peers.iter().enumerate().map(|(idx, peer)| {
                        self.render_peer_card(idx, peer)
                    }))
            )
            .when(self.show_merge_details && self.last_merge_report.is_some(), |d| {
                let report = self.last_merge_report.as_ref().unwrap();
                d.child(
                    div()
                        .flex()
                        .flex_row()
                        .gap_4()
                        .p_3()
                        .bg(rgb(0x1a1a2a))
                        .rounded_md()
                        .child(div().text_sm().font_weight(gpui::FontWeight::SEMIBOLD).child("Last Merge Report:"))
                        .child(div().text_sm().text_color(rgb(0x77dd77)).child(format!("Inserted: {}", report.inserted.len())))
                        .child(div().text_sm().text_color(rgb(0x77aadd)).child(format!("Updated: {}", report.updated.len())))
                        .child(div().text_sm().text_color(rgb(0xdd7777)).child(format!("Skipped: {}", report.skipped_older.len())))
                        .child(div().text_sm().text_color(rgb(0xdddd77)).child(format!("Conflicts: {}", report.conflicts_equal_version.len())))
                )
            })
            .child(
                // Log
                div()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .p_3()
                    .bg(rgb(0x1e1e1e))
                    .rounded_md()
                    .max_h(px(200.))
                    .child(div().text_sm().font_weight(gpui::FontWeight::SEMIBOLD).child("Event Log:"))
                    .children(self.log_messages.iter().map(|msg| {
                        div().text_xs().text_color(rgb(theme::TEXT_COLOR_SECONDARY)).child(msg.clone())
                    }))
            )
    }
}
