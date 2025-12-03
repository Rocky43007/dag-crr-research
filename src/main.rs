//! DAG+CRR Demo Application
//!
//! Interactive visualization of CRR database synchronization with:
//! - Per-column versioning demonstration
//! - Conflict resolution with different tiebreaker policies
//! - Network partition simulation
//! - DAG history visualization and recovery

use gpui::{Application, WindowOptions};
use ui::demo::ProfessionalDemo;

mod ui;

fn main() {
    Application::new().run(|cx| {
        cx.open_window(WindowOptions::default(), |_, cx| ProfessionalDemo::new(cx))
            .ok();
    });
}
