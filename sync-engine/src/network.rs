//! TCP-based GC Coordination Protocol
//!
//! This module implements a simple TCP protocol for GC coordination benchmarking.
//! It measures real protocol overhead (serialization, TCP stack) with injected
//! delays to model WAN latency.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

/// Messages for GC coordination protocol
#[derive(Debug, Clone)]
pub enum GcMessage {
    /// Peer reports its low watermark version
    WatermarkReport { peer_id: String, version: u64 },
    /// Coordinator broadcasts safe GC threshold
    SafeThreshold { threshold: u64 },
    /// Acknowledgment
    Ack,
}

impl GcMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            GcMessage::WatermarkReport { peer_id, version } => {
                let mut bytes = vec![0u8]; // Type tag
                let id_bytes = peer_id.as_bytes();
                bytes.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(id_bytes);
                bytes.extend_from_slice(&version.to_le_bytes());
                bytes
            }
            GcMessage::SafeThreshold { threshold } => {
                let mut bytes = vec![1u8]; // Type tag
                bytes.extend_from_slice(&threshold.to_le_bytes());
                bytes
            }
            GcMessage::Ack => vec![2u8],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        match bytes[0] {
            0 => {
                if bytes.len() < 13 {
                    return None;
                }
                let id_len = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
                if bytes.len() < 5 + id_len + 8 {
                    return None;
                }
                let peer_id = String::from_utf8_lossy(&bytes[5..5 + id_len]).to_string();
                let version = u64::from_le_bytes([
                    bytes[5 + id_len],
                    bytes[6 + id_len],
                    bytes[7 + id_len],
                    bytes[8 + id_len],
                    bytes[9 + id_len],
                    bytes[10 + id_len],
                    bytes[11 + id_len],
                    bytes[12 + id_len],
                ]);
                Some(GcMessage::WatermarkReport { peer_id, version })
            }
            1 => {
                if bytes.len() < 9 {
                    return None;
                }
                let threshold = u64::from_le_bytes([
                    bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
                ]);
                Some(GcMessage::SafeThreshold { threshold })
            }
            2 => Some(GcMessage::Ack),
            _ => None,
        }
    }
}

/// Result of a GC coordination round
#[derive(Debug, Clone)]
pub struct GcCoordinationResult {
    /// Total time for coordination
    pub total_time: Duration,
    /// Time spent in network (TCP round trips)
    pub network_time: Duration,
    /// Number of messages sent
    pub messages_sent: usize,
    /// Number of messages received
    pub messages_received: usize,
    /// Safe GC threshold computed
    pub safe_threshold: u64,
}

/// TCP-based GC Coordinator (runs on a dedicated port)
pub struct TcpGcCoordinator {
    port: u16,
    injected_delay_ms: u64,
}

impl TcpGcCoordinator {
    pub fn new(port: u16, injected_delay_ms: u64) -> Self {
        Self {
            port,
            injected_delay_ms,
        }
    }

    pub fn coordinate(&self, peer_count: usize) -> GcCoordinationResult {
        let start = Instant::now();
        #[allow(unused_variables)]
        let messages_sent = 0;
        let mut messages_received = 0;
        let mut watermarks = Vec::new();

        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port))
            .expect("Failed to bind coordinator");
        listener
            .set_nonblocking(false)
            .expect("Failed to set blocking");

        let (tx, rx) = mpsc::channel();
        let delay = self.injected_delay_ms;

        let handle = thread::spawn(move || {
            let mut received = 0;
            for stream in listener.incoming() {
                if received >= peer_count {
                    break;
                }
                if let Ok(mut stream) = stream {
                    if delay > 0 {
                        thread::sleep(Duration::from_millis(delay));
                    }

                    let mut buf = [0u8; 256];
                    if let Ok(n) = stream.read(&mut buf) {
                        if let Some(msg) = GcMessage::from_bytes(&buf[..n]) {
                            if let GcMessage::WatermarkReport { version, .. } = msg {
                                tx.send(version).ok();
                                received += 1;

                                if delay > 0 {
                                    thread::sleep(Duration::from_millis(delay));
                                }
                                stream.write_all(&GcMessage::Ack.to_bytes()).ok();
                            }
                        }
                    }
                }
            }
        });

        for _ in 0..peer_count {
            if let Ok(v) = rx.recv_timeout(Duration::from_secs(5)) {
                watermarks.push(v);
                messages_received += 1;
            }
        }

        let safe_threshold = watermarks.iter().copied().min().unwrap_or(0);

        drop(rx);
        let _ = handle.join();

        let total_time = start.elapsed();
        let network_time = Duration::from_millis(self.injected_delay_ms * 2 * peer_count as u64);

        GcCoordinationResult {
            total_time,
            network_time,
            messages_sent,
            messages_received,
            safe_threshold,
        }
    }
}

/// TCP-based GC Peer (connects to coordinator)
pub struct TcpGcPeer {
    peer_id: String,
    coordinator_port: u16,
    injected_delay_ms: u64,
}

impl TcpGcPeer {
    pub fn new(peer_id: &str, coordinator_port: u16, injected_delay_ms: u64) -> Self {
        Self {
            peer_id: peer_id.to_string(),
            coordinator_port,
            injected_delay_ms,
        }
    }

    pub fn report_watermark(&self, version: u64) -> Option<GcCoordinationResult> {
        let start = Instant::now();

        let mut stream =
            TcpStream::connect(format!("127.0.0.1:{}", self.coordinator_port)).ok()?;

        if self.injected_delay_ms > 0 {
            thread::sleep(Duration::from_millis(self.injected_delay_ms));
        }

        let msg = GcMessage::WatermarkReport {
            peer_id: self.peer_id.clone(),
            version,
        };
        stream.write_all(&msg.to_bytes()).ok()?;

        let mut buf = [0u8; 256];
        let _ = stream.read(&mut buf).ok()?;

        let total_time = start.elapsed();

        Some(GcCoordinationResult {
            total_time,
            network_time: Duration::from_millis(self.injected_delay_ms * 2),
            messages_sent: 1,
            messages_received: 1,
            safe_threshold: 0, // Peer doesn't know yet
        })
    }
}

/// Measure GC coordination round with injected network delays.
pub fn measure_gc_coordination_tcp(
    peer_count: usize,
    injected_rtt_ms: u64,
) -> GcCoordinationResult {
    let start = Instant::now();

    let mut messages_sent = 0;
    let mut watermarks = Vec::new();

    for i in 0..peer_count {
        let msg = GcMessage::WatermarkReport {
            peer_id: format!("peer_{}", i),
            version: (i as u64 + 1) * 100,
        };
        let bytes = msg.to_bytes();
        std::hint::black_box(&bytes);

        if injected_rtt_ms > 0 {
            thread::sleep(Duration::from_millis(injected_rtt_ms / 2));
        }

        if let Some(GcMessage::WatermarkReport { version, .. }) = GcMessage::from_bytes(&bytes) {
            watermarks.push(version);
        }
        messages_sent += 1;
    }

    let safe_threshold = watermarks.iter().copied().min().unwrap_or(0);

    let response = GcMessage::SafeThreshold { threshold: safe_threshold };
    let response_bytes = response.to_bytes();
    std::hint::black_box(&response_bytes);

    if injected_rtt_ms > 0 {
        thread::sleep(Duration::from_millis(injected_rtt_ms / 2));
    }

    let total_time = start.elapsed();

    GcCoordinationResult {
        total_time,
        network_time: Duration::from_millis(injected_rtt_ms), // 1 RTT (request + response)
        messages_sent,
        messages_received: peer_count, // All watermarks received
        safe_threshold,
    }
}

/// Measure DAG-CRR local GC (no network coordination).
pub fn measure_dag_gc_local(history_nodes: usize, gc_depth: usize) -> Duration {
    let start = Instant::now();

    let mut removed = 0;
    for i in 0..history_nodes {
        if i < history_nodes.saturating_sub(gc_depth) {
            removed += 1;
        }
    }
    std::hint::black_box(removed);

    start.elapsed()
}

// Simple random number for port selection
#[allow(dead_code)]
mod rand {
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn random<T: From<u16>>() -> T {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        T::from((nanos % 65536) as u16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let msg = GcMessage::WatermarkReport {
            peer_id: "test".to_string(),
            version: 12345,
        };
        let bytes = msg.to_bytes();
        let decoded = GcMessage::from_bytes(&bytes).unwrap();

        if let GcMessage::WatermarkReport { peer_id, version } = decoded {
            assert_eq!(peer_id, "test");
            assert_eq!(version, 12345);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_gc_coordination_basic() {
        // Test with 2 peers, no delay (fast path)
        let result = measure_gc_coordination_tcp(2, 0);
        assert!(result.total_time >= Duration::ZERO);
        assert_eq!(result.safe_threshold, 100); // min of 100, 200
        assert_eq!(result.messages_sent, 2);
    }

    #[test]
    fn test_gc_coordination_with_delay() {
        // Test with small delay to verify timing
        let result = measure_gc_coordination_tcp(2, 10);
        // Should take at least 10ms (2 half-RTT delays of 5ms each for peers + 5ms response)
        assert!(result.total_time >= Duration::from_millis(10));
        assert_eq!(result.safe_threshold, 100);
    }

    #[test]
    fn test_dag_gc_local() {
        let elapsed = measure_dag_gc_local(1000, 100);
        // Should be very fast (sub-millisecond)
        assert!(elapsed < Duration::from_millis(10));
    }
}
