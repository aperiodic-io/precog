//! Driving the Redpanda dedup fork the way live-ingestion's producers do: idempotence off, one
//! `redpanda-dedup-key` header per record, acks=all; producer `n` writes partition `n % 3`.

use crate::check::{Gen, Rec};
use crate::docker::{Cluster, ip};
use crate::precog::{Load, Sent};
use anyhow::Result;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const PARTITIONS: i32 = 3;

pub async fn client() -> Result<Client> {
    let brokers = (0..3).map(|i| format!("{}:9092", ip(i))).collect();
    Ok(ClientBuilder::new(brokers)
        .backoff_config(rskafka::BackoffConfig {
            deadline: Some(Duration::from_secs(20)),
            ..Default::default()
        })
        .build()
        .await?)
}

/// raw-internal's topic config, with a dedup window of `window_ms` (none: no dedup).
pub async fn topic(c: &Cluster, name: &str, window_ms: Option<u64>) -> Result<()> {
    let _ = c.rpk(&["topic", "delete", name]).await;
    let mut a = vec![
        "topic",
        "create",
        name,
        "-p",
        "3",
        "-r",
        "3",
        "-c",
        "retention.bytes=1252698794",
        "-c",
        "cleanup.policy=delete",
    ];
    let w;
    if let Some(ms) = window_ms {
        w = format!("redpanda.dedup.window.ms={ms}");
        a.extend([
            "-c",
            &w,
            "-c",
            "redpanda.dedup.key.header=redpanda-dedup-key",
        ]);
    }
    c.rpk(&a).await.map(drop)
}

async fn partition(c: &Client, topic: &str, p: i32) -> Result<Arc<PartitionClient>> {
    Ok(Arc::new(
        c.partition_client(topic, p, UnknownTopicHandling::Retry)
            .await?,
    ))
}

pub async fn run(l: Load) -> Result<Sent> {
    let c = client().await?;
    let start = Instant::now();
    let end = start + l.duration;
    let mut tasks = vec![];
    for p in 0..l.producers {
        let pc = partition(&c, &l.stream, p as i32 % PARTITIONS).await?;
        let l = l.clone();
        tasks.push(tokio::spawn(async move {
            let name = l
                .gen_prefix
                .as_ref()
                .map_or(format!("{}{p}", l.prefix), |g| format!("{g}{p}"));
            let mut g = Gen::new(&name, l.symbols, l.dup_rate);
            g.empty_ids = l.empty_ids;
            g.big_every = l.big_every;
            let pace = if l.rate > 0.0 {
                Duration::from_secs_f64(l.batch as f64 * l.producers as f64 / l.rate)
            } else {
                Duration::ZERO
            };
            let mut next = Instant::now();
            let mut out = Sent::default();
            let mut inflight = tokio::task::JoinSet::new();
            while Instant::now() < end {
                if l.rate > 0.0 {
                    tokio::time::sleep_until(next.into()).await;
                    next += pace;
                }
                let mut evs = vec![];
                let mut resends = 0;
                for _ in 0..l.batch {
                    let (e, r) = g.next();
                    resends += r as u64;
                    out.sent.insert(e.uid.clone());
                    evs.push(e);
                }
                let records = evs
                    .iter()
                    .map(|e| Record {
                        key: None,
                        value: Some(e.body.clone()),
                        headers: if e.id.is_empty() {
                            BTreeMap::new()
                        } else {
                            BTreeMap::from([("redpanda-dedup-key".to_string(), e.id.clone())])
                        },
                        timestamp: rskafka::chrono::DateTime::from_timestamp_millis(
                            precog_client::now_ms() as i64,
                        )
                        .unwrap(),
                    })
                    .collect();
                while inflight.len() >= l.in_flight {
                    absorb(&mut out, inflight.join_next().await.unwrap().unwrap());
                }
                let pc = pc.clone();
                inflight.spawn(async move {
                    let t = Instant::now();
                    let r = pc.produce(records, Compression::NoCompression).await;
                    (
                        r.map_err(anyhow::Error::from),
                        evs,
                        resends,
                        t.elapsed().as_secs_f64() * 1000.0,
                    )
                });
            }
            while let Some(r) = inflight.join_next().await {
                absorb(&mut out, r.unwrap());
            }
            out
        }));
    }
    let mut all = Sent::default();
    for t in tasks {
        all.merge(t.await?);
    }
    all.secs = l.duration.as_secs_f64();
    Ok(all)
}

type Done = (Result<Vec<i64>>, Vec<crate::check::Ev>, u64, f64);

fn absorb(out: &mut Sent, (r, evs, resends, ms): Done) {
    match r {
        Ok(_) => {
            out.acked_events += evs.len() as u64;
            out.resends += resends;
            out.lat_ms.push(ms);
            out.acked.extend(evs.into_iter().map(|e| e.uid));
        }
        Err(e) => {
            out.failed_batches += 1;
            if out.errors.len() < 10 {
                out.errors.push(e.to_string());
            }
        }
    }
}

/// Every record of `topic`, partition by partition, in offset order.
pub async fn read_all(topic: &str) -> Result<Vec<Rec>> {
    let c = client().await?;
    let mut out = vec![];
    for p in 0..PARTITIONS {
        let pc = partition(&c, topic, p).await?;
        let mut off = 0;
        loop {
            let (recs, high) = pc.fetch_records(off, 1..8_000_000, 500).await?;
            for r in &recs {
                let uid = r
                    .record
                    .value
                    .as_deref()
                    .and_then(crate::check::uid_of)
                    .unwrap_or("?");
                out.push(Rec {
                    log: format!("p{p}"),
                    pos: r.offset as u64,
                    uid: uid.to_string(),
                });
                off = r.offset + 1;
            }
            if off >= high || (recs.is_empty() && off >= high) {
                break;
            }
            if recs.is_empty() {
                // compacted-away or aborted ranges: step past them
                off += 1;
            }
        }
    }
    Ok(out)
}
