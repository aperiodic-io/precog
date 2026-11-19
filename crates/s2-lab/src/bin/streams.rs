//! Many streams, one small record each: how fast streams are created and (measured outside, by
//! the harness) what they cost the server in memory and disk.
//!
//!   streams --endpoint http://h:8080 --count 1000000 [--concurrency 64]   # S2 REST
//!   streams --nodes tcp://h:7000 --count 1000000                           # Precog, native
//!
//! Over S2 REST each stream is created by its first append (`create_stream_on_append`). Natively,
//! each stream is one Precog writer log with one 1-event batch. Prints one JSON line.

use anyhow::Result;
use precog_mesh::client::MeshClient;
use precog_mesh::proto::{Batch, LogId};
use precog_s2_lab::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

fn arg<T: std::str::FromStr>(k: &str, d: T) -> T {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == k)
        .and_then(|i| a[i + 1].parse().ok())
        .unwrap_or(d)
}

#[tokio::main]
async fn main() -> Result<()> {
    let ep: String = arg("--endpoint", String::new());
    let nodes: String = arg("--nodes", String::new());
    let count: u64 = arg("--count", 100_000);
    let conc: u64 = arg("--concurrency", 64);
    let size: usize = arg("--size", 64);
    let basin: String = arg("--basin", "many-basin".to_string());
    let label: String = arg("--label", "run".to_string());
    let next = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let body = "x".repeat(size);
    let t0 = Instant::now();
    let mut tasks = vec![];
    if !ep.is_empty() {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(1024)
            .build()?;
        let _ = http
            .post(format!("{ep}/v1/basins"))
            .json(&serde_json::json!({"basin": basin,
                "config": {"create_stream_on_append": true}}))
            .send()
            .await;
        for _ in 0..conc {
            let (http, ep, basin, next, errors, body) = (
                http.clone(),
                ep.clone(),
                basin.clone(),
                next.clone(),
                errors.clone(),
                body.clone(),
            );
            tasks.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= count {
                        return;
                    }
                    let input = AppendInput {
                        records: vec![AppendRecord {
                            body: body.clone(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    };
                    let r = http
                        .post(format!("{ep}/v1/streams/m{i:08}/records"))
                        .header("s2-basin", &basin)
                        .json(&input)
                        .send()
                        .await;
                    if !matches!(r, Ok(ref x) if x.status().is_success()) {
                        errors.fetch_add(1, Relaxed);
                    }
                }
            }));
        }
    } else {
        let addrs = nodes
            .split(',')
            .map(|a| a.parse())
            .collect::<Result<Vec<_>>>()?;
        let mc = MeshClient::new(addrs, arg("--token", "in-cluster".to_string()))?;
        let copies = mc.node_count().min(2) as u8;
        for _ in 0..conc {
            let (mc, next, errors, body) = (mc.clone(), next.clone(), errors.clone(), body.clone());
            tasks.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= count {
                        return;
                    }
                    let log = LogId {
                        stream: format!("many/m{i:08}"),
                        writer: "s2".into(),
                        epoch: 0,
                    };
                    let b = Batch::encode_keyed(&log, 0, &[(now_us(), &[][..], body.as_bytes())])
                        .unwrap();
                    if mc
                        .append((i % mc.node_count() as u64) as usize, &b, copies)
                        .await
                        .is_err()
                    {
                        errors.fetch_add(1, Relaxed);
                    }
                }
            }));
        }
    }
    for t in tasks {
        t.await?;
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::json!({"label": label, "streams": count, "secs": (secs * 10.0).round() / 10.0,
            "streams_per_s": (count as f64 / secs).round(), "errors": errors.load(Relaxed)})
    );
    Ok(())
}
