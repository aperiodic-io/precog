//! One S2 REST workload against any endpoint (s2-lite or the Precog gateway): unary JSON
//! appends from concurrent writers, SSE tail readers, then latency probes. Prints one JSON line.
//!
//!   s2bench --endpoint http://h:8080 --streams 4 --writers 4 --batch 100 --size 256 \
//!           --readers 1 --duration 20 --warmup 5 [--rate 0] [--label x]
//!
//! Each record body starts with its creation time (16 digits, µs): readers derive end-to-end
//! latency from it. Probes after the load, on the first stream: idle 1-record append, check-tail,
//! unary read of 100 records 1000 back (time to first byte and to the full body), and SSE time
//! to the first `batch` event from `tail_offset=10`.

use anyhow::Result;
use futures::StreamExt;
use precog_s2_lab::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

fn arg<T: std::str::FromStr>(k: &str, d: T) -> T {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == k)
        .and_then(|i| a[i + 1].parse().ok())
        .unwrap_or(d)
}

fn body(size: usize) -> String {
    let mut s = format!("{:016}", now_us());
    s.extend(std::iter::repeat_n('x', size.saturating_sub(16)));
    s
}

fn sorted(mut v: Vec<f64>) -> Vec<f64> {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v
}

#[tokio::main]
async fn main() -> Result<()> {
    let ep: String = arg("--endpoint", "http://127.0.0.1:8080".to_string());
    let basin: String = arg("--basin", "bench-basin".to_string());
    let prefix: String = arg("--prefix", format!("s{}", now_us() % 1_000_000));
    let streams: usize = arg("--streams", 4);
    let writers: usize = arg("--writers", 4);
    let batch: usize = arg("--batch", 100);
    let size: usize = arg("--size", 256);
    let readers: usize = arg("--readers", 1);
    let rate: f64 = arg("--rate", 0.0);
    let dur = Duration::from_secs(arg("--duration", 20));
    let warm = Duration::from_secs(arg("--warmup", 5));
    let label: String = arg("--label", "run".to_string());
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(1024)
        .tcp_nodelay(true)
        .build()?;
    let _ = http
        .post(format!("{ep}/v1/basins"))
        .json(&serde_json::json!({"basin": basin,
            "config": {"create_stream_on_append": true, "create_stream_on_read": true}}))
        .send()
        .await;
    let names: Vec<String> = (0..streams).map(|i| format!("{prefix}-{i}")).collect();
    // create every stream before the clock starts
    for n in &names {
        let input = AppendInput {
            records: vec![AppendRecord {
                body: body(size),
                ..Default::default()
            }],
            ..Default::default()
        };
        http.post(format!("{ep}/v1/streams/{n}/records"))
            .header("s2-basin", &basin)
            .json(&input)
            .send()
            .await?
            .error_for_status()?;
    }
    let start = Instant::now();
    let (from, end) = (start + warm, start + warm + dur);
    let (acked, errors) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let e2e = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let mut tasks = vec![];
    for r in 0..readers {
        let (http, ep, basin, e2e) = (http.clone(), ep.clone(), basin.clone(), e2e.clone());
        let name = names[r % streams].clone();
        tasks.push(tokio::spawn(async move {
            let Ok(resp) = http
                .get(format!("{ep}/v1/streams/{name}/records"))
                .header("s2-basin", &basin)
                .header("accept", "text/event-stream")
                .send()
                .await
            else {
                return;
            };
            let mut s = resp.bytes_stream();
            let mut buf = Vec::<u8>::new();
            loop {
                let chunk = match tokio::time::timeout_at(end.into(), s.next()).await {
                    Ok(Some(Ok(c))) => c,
                    _ => return,
                };
                buf.extend_from_slice(&chunk);
                while let Some(i) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=i).collect();
                    let Some(data) = line.strip_prefix(b"data:") else {
                        continue;
                    };
                    let Ok(b) = serde_json::from_slice::<ReadBatch>(data) else {
                        continue;
                    };
                    let (now, t) = (now_us(), Instant::now());
                    if t < from {
                        continue;
                    }
                    let mut g = e2e.lock().unwrap();
                    for rec in &b.records {
                        if let Some(sent) = rec.body.get(..16).and_then(|s| s.parse::<u64>().ok()) {
                            g.push(now.saturating_sub(sent) as f64 / 1000.0);
                        }
                    }
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    for w in 0..writers * streams {
        let (http, ep, basin) = (http.clone(), ep.clone(), basin.clone());
        let (acked, errors, lat) = (acked.clone(), errors.clone(), lat.clone());
        let name = names[w % streams].clone();
        let gap = if rate > 0.0 {
            Duration::from_secs_f64(batch as f64 * (writers * streams) as f64 / rate)
        } else {
            Duration::ZERO
        };
        tasks.push(tokio::spawn(async move {
            let mut next = Instant::now();
            while Instant::now() < end {
                if rate > 0.0 {
                    tokio::time::sleep_until(next.into()).await;
                    next += gap;
                }
                let input = AppendInput {
                    records: (0..batch)
                        .map(|_| AppendRecord {
                            body: body(size),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                };
                let sent = Instant::now();
                let r = http
                    .post(format!("{ep}/v1/streams/{name}/records"))
                    .header("s2-basin", &basin)
                    .json(&input)
                    .send()
                    .await;
                let ok = matches!(&r, Ok(x) if x.status().is_success());
                if let Ok(x) = r {
                    let _ = x.bytes().await;
                }
                let done = Instant::now();
                if sent >= from && done <= end {
                    if ok {
                        acked.fetch_add(batch as u64, Relaxed);
                        lat.lock()
                            .unwrap()
                            .push((done - sent).as_secs_f64() * 1000.0);
                    } else {
                        errors.fetch_add(1, Relaxed);
                    }
                }
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let lat = sorted(lat.lock().unwrap().clone());
    let e2e = sorted(e2e.lock().unwrap().clone());
    let secs = dur.as_secs_f64();
    let recs = acked.load(Relaxed) as f64;

    // probes on an idle system
    let s0 = &names[0];
    let url = format!("{ep}/v1/streams/{s0}/records");
    let mut idle = vec![];
    for _ in 0..50 {
        let t = Instant::now();
        let input = AppendInput {
            records: vec![AppendRecord {
                body: body(size),
                ..Default::default()
            }],
            ..Default::default()
        };
        let r = http
            .post(&url)
            .header("s2-basin", &basin)
            .json(&input)
            .send()
            .await?;
        r.bytes().await?;
        idle.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let (mut tail_ms, mut ttfb, mut full) = (vec![], vec![], vec![]);
    let mut sse_first = vec![];
    for _ in 0..30 {
        let t = Instant::now();
        let r = http
            .get(format!("{url}/tail"))
            .header("s2-basin", &basin)
            .send()
            .await?;
        let tail: serde_json::Value = r.json().await?;
        tail_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        let at = tail["tail"]["seq_num"]
            .as_u64()
            .unwrap_or(0)
            .saturating_sub(1000);
        let t = Instant::now();
        let r = http
            .get(format!("{url}?seq_num={at}&count=100"))
            .header("s2-basin", &basin)
            .send()
            .await?;
        let mut s = r.bytes_stream();
        s.next().await;
        ttfb.push(t.elapsed().as_secs_f64() * 1000.0);
        while s.next().await.is_some() {}
        full.push(t.elapsed().as_secs_f64() * 1000.0);
        let t = Instant::now();
        let r = http
            .get(format!("{url}?tail_offset=10"))
            .header("s2-basin", &basin)
            .header("accept", "text/event-stream")
            .send()
            .await?;
        let mut s = r.bytes_stream();
        let mut seen = Vec::new();
        while let Ok(Some(Ok(c))) = tokio::time::timeout(Duration::from_secs(5), s.next()).await {
            seen.extend_from_slice(&c);
            if seen.windows(12).any(|w| w == b"event: batch") && seen.contains(&b'}') {
                break;
            }
        }
        sse_first.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let (idle, tail_ms, ttfb, full, sse_first) = (
        sorted(idle),
        sorted(tail_ms),
        sorted(ttfb),
        sorted(full),
        sorted(sse_first),
    );
    let p = |v: &[f64], q| (pct(v, q) * 1000.0).round() / 1000.0;
    println!(
        "{}",
        serde_json::json!({
            "label": label, "streams": streams, "writers_per_stream": writers, "batch": batch,
            "size": size, "readers": readers, "rate": rate,
            "records_per_s": (recs / secs).round(),
            "mib_per_s": (recs * size as f64 / secs / 1048576.0 * 100.0).round() / 100.0,
            "append_p50_ms": p(&lat, 0.5), "append_p99_ms": p(&lat, 0.99),
            "e2e_p50_ms": p(&e2e, 0.5), "e2e_p99_ms": p(&e2e, 0.99), "e2e_samples": e2e.len(),
            "errors": errors.load(Relaxed),
            "idle_append_p50_ms": p(&idle, 0.5), "idle_append_p99_ms": p(&idle, 0.99),
            "check_tail_p50_ms": p(&tail_ms, 0.5),
            "read_ttfb_p50_ms": p(&ttfb, 0.5), "read_full_p50_ms": p(&full, 0.5),
            "sse_first_batch_p50_ms": p(&sse_first, 0.5), "sse_first_batch_p99_ms": p(&sse_first, 0.99),
        })
    );
    Ok(())
}
