//! Precog's counterpart of live-ingestion/benchmarks/redpanda-dedup/loadgen (franz-go): the
//! same JSON trade records (~230 bytes), producers x symbols, per-event dedup ids, a resend rate
//! (a producer resends one of its 64 latest records byte for byte) and a cap on records in
//! flight. Records are timed from creation to their batch's ack. `--rate` paces the offered load
//! (records/s over all producers) for latency at a fixed rate; without it, it saturates.
//!
//!   loadgen --nodes tcp://a:7000,tcp://b:7000,tcp://c:7000 [--rate 100000] [--dup-rate 0.2]
use precog_client::dedup_key;
use precog_mesh::client::MeshClient;
use precog_mesh::proto::{Batch, Control, ControlResp, LogId};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

fn arg<T: std::str::FromStr>(args: &[String], k: &str, d: T) -> T {
    args.iter()
        .position(|a| a == k)
        .map_or(d, |i| args[i + 1].parse().ok().unwrap())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let nodes: String = arg(&args, "--nodes", String::new());
    let stream: String = arg(&args, "--stream", "raw.bench.trades".to_string());
    let duration = Duration::from_secs(arg(&args, "--duration", 30));
    let warmup = Duration::from_secs(arg(&args, "--warmup", 5));
    let producers: usize = arg(&args, "--producers", 8);
    let symbols: usize = arg(&args, "--symbols", 3);
    let dup_rate: f64 = arg(&args, "--dup-rate", 0.0);
    let in_flight: usize = arg(&args, "--in-flight", 2000);
    let batch: usize = arg(&args, "--batch", 50);
    let rate: f64 = arg(&args, "--rate", 0.0);
    let label: String = arg(&args, "--label", "run".to_string());
    // like franz-go's default snappy on the Redpanda side: payloads compressed per batch
    let compress: u8 = arg(&args, "--compress", 1);
    let addrs = nodes
        .split(',')
        .map(|a| a.parse())
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mc = MeshClient::new(addrs, arg(&args, "--token", "in-cluster".to_string()))?;
    let preferred =
        (xxhash_rust::xxh3::xxh3_64(stream.as_bytes()) % mc.node_count() as u64) as usize;
    let copies = mc.node_count().min(2) as u8;
    let permits = Arc::new(tokio::sync::Semaphore::new(in_flight));
    let (acked, bytes, errors) = (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
    );
    let lat = Arc::new(parking_lot::Mutex::new(Vec::<f64>::new()));
    let start = Instant::now();
    let (measure_from, end) = (start + warmup, start + warmup + duration);
    let mut tasks = vec![];
    for w in 0..producers {
        let (mc, stream, permits) = (mc.clone(), stream.clone(), permits.clone());
        let (acked, bytes, errors, lat) =
            (acked.clone(), bytes.clone(), errors.clone(), lat.clone());
        tasks.push(tokio::spawn(async move {
            let name = format!("live-producer-bench-{w}");
            let log = LogId { stream, writer: name.clone(), epoch: start.elapsed().as_nanos() as u64 + w as u64 };
            let syms: Vec<String> = (0..symbols).map(|i| format!("SYM{w}-{i}")).collect();
            let (mut seq, mut trade, mut rng) = (0u64, 0u64, 0x9e37_79b9_7f4a_7c15u64 ^ w as u64);
            let mut ring: Vec<(Vec<u8>, Vec<u8>)> = vec![];
            let per_batch = if rate > 0.0 { Duration::from_secs_f64(batch as f64 * producers as f64 / rate) } else { Duration::ZERO };
            let mut next = Instant::now();
            while Instant::now() < end {
                if rate > 0.0 {
                    tokio::time::sleep_until(next.into()).await;
                    next += per_batch;
                }
                let mut evs: Vec<(u64, Vec<u8>, Vec<u8>)> = Vec::with_capacity(batch);
                for _ in 0..batch {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let resend = ring.len() >= 8 && (rng % 1000) as f64 / 1000.0 < dup_rate;
                    let (id, value) = if resend {
                        ring[(rng >> 32) as usize % ring.len()].clone()
                    } else {
                        trade += 1;
                        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as u64;
                        let v = serde_json::json!({"exchange": "bench", "symbol": syms[(rng % symbols as u64) as usize],
                            "trade_id": trade, "price": 20000.0 + (rng % 40000) as f64 * 1.000_1, "qty": (rng % 5000) as f64 / 1000.0,
                            "side": if rng % 2 == 0 { "buy" } else { "sell" }, "time_us": now, "local_timestamp_us": now, "producer": name});
                        let e = (format!("{name}-{trade}").into_bytes(), serde_json::to_vec(&v).unwrap());
                        if ring.len() == 64 {
                            ring.remove(0);
                        }
                        ring.push(e.clone());
                        e
                    };
                    evs.push((0, dedup_key(&id), value));
                }
                let n = evs.len();
                let size: usize = evs.iter().map(|(_, k, v)| k.len() + v.len()).sum();
                let refs: Vec<(u64, &[u8], &[u8])> = evs.iter().map(|(t, k, v)| (*t, &k[..], &v[..])).collect();
                let b = if compress == 1 {
                    Batch::encode_compressed(&log, seq, &refs).unwrap()
                } else {
                    Batch::encode_keyed(&log, seq, &refs).unwrap()
                };
                seq += n as u64;
                let permit = permits.clone().acquire_many_owned(n as u32).await.unwrap();
                let (mc, acked, bytes, errors, lat) = (mc.clone(), acked.clone(), bytes.clone(), errors.clone(), lat.clone());
                let sent = Instant::now();
                tokio::spawn(async move {
                    let r = mc.append(preferred, &b, copies).await;
                    drop(permit);
                    match r {
                        Ok(_) if sent >= measure_from && Instant::now() <= end => {
                            acked.fetch_add(n as u64, Relaxed);
                            bytes.fetch_add(size as u64, Relaxed);
                            let ms = sent.elapsed().as_secs_f64() * 1000.0;
                            lat.lock().extend(std::iter::repeat_n(ms, n));
                        }
                        Ok(_) => {}
                        Err(_) => {
                            errors.fetch_add(n as u64, Relaxed);
                        }
                    }
                });
            }
        }));
    }
    for t in tasks {
        t.await?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut l = std::mem::take(&mut *lat.lock());
    l.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |p: f64| {
        l.get(((l.len() as f64 * p) as usize).min(l.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0.0)
    };
    let mut dropped = 0;
    for n in 0..mc.node_count() {
        if let Ok(ControlResp::Status(s)) = mc.control(n, &Control::Status).await {
            dropped += s.dedup_dropped;
        }
    }
    let secs = duration.as_secs_f64();
    println!(
        "{}",
        serde_json::json!({"label": label, "throughput_records_per_sec": acked.load(Relaxed) as f64 / secs,
            "throughput_mb_per_sec": bytes.load(Relaxed) as f64 / secs / 1e6, "latency_p50_ms": q(0.50),
            "latency_p99_ms": q(0.99), "errors": errors.load(Relaxed), "dedup_dropped_total": dropped,
            "producers": producers, "batch": batch, "compress": compress, "in_flight": in_flight, "dup_rate": dup_rate, "rate": rate})
    );
    Ok(())
}
