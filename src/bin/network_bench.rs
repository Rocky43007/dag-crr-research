//! Network benchmark for real RTT measurements across GCP regions.
//!
//! Usage:
//!   network_bench server --bind 0.0.0.0:9000
//!   network_bench client --peers 10.0.0.1:9000,10.0.0.2:9000 --samples 100 --output results.json

use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    Ping { seq: u64 },
    Pong { seq: u64 },
    WatermarkRequest { gc_id: u64 },
    WatermarkResponse { gc_id: u64, watermark: u64 },
    GcThreshold { gc_id: u64, threshold: u64 },
    GcAck { gc_id: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchmarkResult {
    peer: String,
    rtt_samples_us: Vec<u64>,
    rtt_mean_us: u64,
    rtt_p50_us: u64,
    rtt_p95_us: u64,
    rtt_p99_us: u64,
    coordinated_gc_us: u64,
    local_gc_us: u64,
    speedup: f64,
}

fn send_msg(stream: &mut TcpStream, msg: &Message) -> std::io::Result<()> {
    let data = serde_json::to_vec(msg)?;
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len)?;
    stream.write_all(&data)?;
    stream.flush()
}

fn recv_msg(stream: &mut TcpStream) -> std::io::Result<Message> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    serde_json::from_slice(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn measure_rtt(stream: &mut TcpStream, samples: usize) -> Vec<Duration> {
    (0..samples as u64)
        .map(|seq| {
            let start = Instant::now();
            send_msg(stream, &Message::Ping { seq }).unwrap();
            let _ = recv_msg(stream).unwrap();
            start.elapsed()
        })
        .collect()
}

fn measure_coordinated_gc(streams: &mut [TcpStream], gc_id: u64) -> Duration {
    let start = Instant::now();

    // Phase 1: request watermarks (parallel)
    for stream in streams.iter_mut() {
        send_msg(stream, &Message::WatermarkRequest { gc_id }).unwrap();
    }

    let mut watermarks = Vec::new();
    for stream in streams.iter_mut() {
        if let Message::WatermarkResponse { watermark, .. } = recv_msg(stream).unwrap() {
            watermarks.push(watermark);
        }
    }

    let threshold = watermarks.iter().min().copied().unwrap_or(0);

    // Phase 2: broadcast threshold
    for stream in streams.iter_mut() {
        send_msg(stream, &Message::GcThreshold { gc_id, threshold }).unwrap();
    }

    for stream in streams.iter_mut() {
        let _ = recv_msg(stream).unwrap();
    }

    start.elapsed()
}

fn measure_local_gc(entries: usize) -> Duration {
    use sync_engine::{CrrTable, run_gc, GcPolicy};

    let mut table = CrrTable::open_in_memory().unwrap();
    for i in 0..100 {
        table.insert(&format!("row_{}", i))
            .column_str("val", "v1", 1)
            .commit().unwrap();
    }

    for v in 2..=(entries / 100).max(10) {
        for i in 0..100 {
            table.update(&format!("row_{}", i))
                .column_str("val", &format!("v{}", v))
                .commit().unwrap();
        }
    }

    let start = Instant::now();
    let _ = run_gc(&mut table, GcPolicy::KeepLast(5));
    start.elapsed()
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let idx = ((sorted.len() as f64) * p / 100.0) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn run_server(bind: &str) {
    use std::net::TcpListener;

    let listener = TcpListener::bind(bind).expect("Failed to bind");
    println!("Server listening on {}", bind);

    for stream in listener.incoming() {
        let mut stream = stream.expect("Connection failed");
        std::thread::spawn(move || {
            loop {
                match recv_msg(&mut stream) {
                    Ok(Message::Ping { seq }) => {
                        send_msg(&mut stream, &Message::Pong { seq }).ok();
                    }
                    Ok(Message::WatermarkRequest { gc_id }) => {
                        send_msg(&mut stream, &Message::WatermarkResponse { gc_id, watermark: 1000 }).ok();
                    }
                    Ok(Message::GcThreshold { gc_id, .. }) => {
                        send_msg(&mut stream, &Message::GcAck { gc_id }).ok();
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
    }
}

fn run_client(peers: Vec<String>, samples: usize, output: Option<String>) {
    let mut results = Vec::new();

    let mut streams: Vec<TcpStream> = peers.iter()
        .map(|p| {
            println!("Connecting to {}...", p);
            TcpStream::connect(p).unwrap_or_else(|_| panic!("Failed to connect to {}", p))
        })
        .collect();

    for (i, stream) in streams.iter_mut().enumerate() {
        println!("Measuring RTT to {}...", peers[i]);
        let rtts = measure_rtt(stream, samples);
        let mut rtt_us: Vec<u64> = rtts.iter().map(|d| d.as_micros() as u64).collect();
        rtt_us.sort();

        let mean = rtt_us.iter().sum::<u64>() / rtt_us.len() as u64;
        let p50 = percentile(&rtt_us, 50.0);
        let p95 = percentile(&rtt_us, 95.0);
        let p99 = percentile(&rtt_us, 99.0);

        println!("  RTT mean: {}us, p50: {}us, p95: {}us, p99: {}us", mean, p50, p95, p99);

        results.push(BenchmarkResult {
            peer: peers[i].clone(),
            rtt_samples_us: rtt_us,
            rtt_mean_us: mean,
            rtt_p50_us: p50,
            rtt_p95_us: p95,
            rtt_p99_us: p99,
            coordinated_gc_us: 0,
            local_gc_us: 0,
            speedup: 0.0,
        });
    }

    println!("\nMeasuring coordinated GC latency ({} peers)...", peers.len());
    let mut gc_latencies: Vec<u64> = (0..samples as u64)
        .map(|gc_id| measure_coordinated_gc(&mut streams, gc_id).as_micros() as u64)
        .collect();
    gc_latencies.sort();
    let coord_gc_mean = gc_latencies.iter().sum::<u64>() / gc_latencies.len() as u64;
    println!("  Coordinated GC mean: {}us", coord_gc_mean);

    println!("\nMeasuring local GC latency...");
    let mut local_gc_latencies: Vec<u64> = (0..samples)
        .map(|_| measure_local_gc(1000).as_micros() as u64)
        .collect();
    local_gc_latencies.sort();
    let local_gc_mean = local_gc_latencies.iter().sum::<u64>() / local_gc_latencies.len() as u64;
    println!("  Local GC mean: {}us", local_gc_mean);

    let speedup = coord_gc_mean as f64 / local_gc_mean.max(1) as f64;
    println!("\nSpeedup: {:.1}x", speedup);

    for result in &mut results {
        result.coordinated_gc_us = coord_gc_mean;
        result.local_gc_us = local_gc_mean;
        result.speedup = speedup;
    }

    if let Some(path) = output {
        let json = serde_json::to_string_pretty(&results).unwrap();
        std::fs::write(&path, &json).expect("Failed to write results");
        println!("\nResults written to {}", path);
    }

    println!("\n=== Summary ===");
    println!("| Peer | RTT | Coord GC | Local GC | Speedup |");
    println!("|------|-----|----------|----------|---------|");
    for r in &results {
        println!("| {} | {}us | {}us | {}us | {:.0}x |",
            r.peer, r.rtt_mean_us, r.coordinated_gc_us, r.local_gc_us, r.speedup);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!("  network_bench server --bind 0.0.0.0:9000");
        eprintln!("  network_bench client --peers IP:PORT,IP:PORT --samples 100 --output results.json");
        return;
    }

    match args[1].as_str() {
        "server" => {
            let bind = args.iter()
                .position(|a| a == "--bind")
                .and_then(|i| args.get(i + 1).map(|s| s.as_str()))
                .unwrap_or("0.0.0.0:9000");
            run_server(bind);
        }
        "client" | "bench" => {
            let peers: Vec<String> = args.iter()
                .position(|a| a == "--peers")
                .and_then(|i| args.get(i + 1))
                .map(|s| s.split(',').map(String::from).collect())
                .unwrap_or_default();

            if peers.is_empty() {
                eprintln!("Error: --peers required");
                return;
            }

            let samples: usize = args.iter()
                .position(|a| a == "--samples")
                .and_then(|i| args.get(i + 1))
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);

            let output = args.iter()
                .position(|a| a == "--output")
                .and_then(|i| args.get(i + 1).cloned());

            run_client(peers, samples, output);
        }
        _ => eprintln!("Unknown command: {}", args[1]),
    }
}
