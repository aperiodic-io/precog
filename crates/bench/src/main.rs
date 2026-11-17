//! Same-harness append benchmark for Apache Iggy (official Rust SDK) and the
//! precog mesh.
//!
//! Workload (identical for both): P producers, each in a closed loop with one
//! batch in flight: build B messages of S random bytes, send, wait for the
//! acknowledgement. Latency is measured from send start to ack. Every Iggy
//! producer owns one partition; every mesh producer owns one writer log.
//!
//! `--rate R` turns each producer open-loop at R batches/s (still at most one
//! in flight), so both systems carry the same offered load.
//!
//! `--subscribers S` adds S live-feed readers, each receiving *every* message
//! of the stream: Iggy standalone `PartitionReader`s over all partitions
//! (long-poll fetch), mesh `Subscription`s (server push). Producers stamp
//! the first 8 payload bytes with wall-clock µs, so each delivered message
//! yields an end-to-end (produce -> subscriber) latency.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use iggy::prelude::*;
use precog_mesh::client::{MeshClient, Writer};
use precog_mesh::proto::{Control, ControlResp};
use precog_mesh::transport::Addr;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, ValueEnum, Serialize, PartialEq)]
enum Target {
    Iggy,
    Mesh,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
enum IggyDurability {
    Persisted,
    Replicated,
}

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(value_enum)]
    target: Target,
    #[arg(long, default_value = "run")]
    label: String,
    #[arg(long, default_value_t = 8)]
    producers: usize,
    #[arg(long, default_value_t = 100)]
    batch: usize,
    #[arg(long, default_value_t = 256)]
    size: usize,
    #[arg(long, default_value_t = 15)]
    duration: u64,
    #[arg(long, default_value_t = 2)]
    warmup: u64,
    /// Timeline bucket width (ms); written to the JSON output.
    #[arg(long, default_value_t = 100)]
    bucket_ms: u64,
    #[arg(long)]
    json: Option<std::path::PathBuf>,
    /// Batches per second per producer (0 = closed loop, as fast as acks allow).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Live subscribers, each reading the whole stream (spread over nodes).
    #[arg(long, default_value_t = 0)]
    subscribers: usize,

    // --- iggy
    /// Comma-separated iggy connection strings (one per node; producer i uses i % n).
    #[arg(long, default_value = "iggy://iggy:iggy@127.0.0.1:8090")]
    iggy: String,
    #[arg(long, value_enum, default_value = "persisted")]
    durability: IggyDurability,
    /// Partitions (default: one per producer; producer i writes partition i % n).
    #[arg(long)]
    partitions: Option<u32>,
    /// Topic replication factor (cluster only).
    #[arg(long)]
    replication: Option<u32>,

    // --- mesh
    #[arg(long, default_value = "run/mesh/cluster.json")]
    cluster: std::path::PathBuf,
    #[arg(long, default_value_t = 2)]
    copies: u8,
    /// Mesh client RPC timeout (ms) before failing over to another node.
    #[arg(long, default_value_t = 1000)]
    rpc_timeout_ms: u64,
}

#[derive(Serialize)]
struct Report {
    label: String,
    target: Target,
    producers: usize,
    batch: usize,
    size: usize,
    seconds: f64,
    batches: u64,
    messages: u64,
    errors: u64,
    msgs_per_sec: f64,
    payload_mb_per_sec: f64,
    lat_us_p50: u64,
    lat_us_p90: u64,
    lat_us_p99: u64,
    lat_us_p999: u64,
    lat_us_max: u64,
    /// Longest interval with zero acknowledgements across all producers (ms).
    longest_ack_gap_ms: u64,
    /// Per producer: longest time without an acknowledgement (ms).
    producer_max_stall_ms: Vec<u64>,
    timeline: Vec<Bucket>,
    stored_check: String,
    subscribers: usize,
    /// Messages per second delivered to each subscriber (median subscriber).
    sub_msgs_per_sec_each: f64,
    /// Sum over subscribers.
    sub_msgs_per_sec_total: f64,
    e2e_us_p50: u64,
    e2e_us_p99: u64,
    e2e_us_p999: u64,
    e2e_us_max: u64,
    /// Subscribers that received every acknowledged message.
    subs_complete: String,
    args: String,
}

#[derive(Serialize, Clone, Default)]
struct Bucket {
    t_ms: u64,
    msgs: u64,
    max_lat_us: u64,
}

struct Shared {
    start: Instant,
    measure_from: Instant,
    stop: AtomicBool,
    buckets: Vec<AtomicU64>,
    bucket_max: Vec<AtomicU64>,
    bucket_ms: u64,
    errors: AtomicU64,
    acked_total: AtomicU64,
    /// Wall-clock µs at which the measured window starts.
    measure_from_wall_us: u64,
    subs: parking_lot_free::SubStats,
}

mod parking_lot_free {
    use hdrhistogram::Histogram;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    #[derive(Default)]
    pub struct SubStats {
        pub hist: Mutex<Option<Histogram<u64>>>,
        /// Per subscriber: messages delivered in total / stamped inside the window.
        pub total: Mutex<Vec<u64>>,
        pub window: Mutex<Vec<u64>>,
        pub delivered_total: AtomicU64,
    }
}

fn wall_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as u64
}

/// Copies the template payload with the send timestamp in its first 8 bytes.
fn stamped(template: &Bytes, ts: u64) -> Bytes {
    let mut b = bytes::BytesMut::from(&template[..]);
    let n = b.len().min(8);
    b[..n].copy_from_slice(&ts.to_le_bytes()[..n]);
    b.freeze()
}

fn stamp_of(payload: &[u8]) -> Option<u64> {
    payload.get(..8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
}

/// Records one received batch of stamped payloads.
struct SubRecorder {
    hist: Histogram<u64>,
    total: u64,
    window: u64,
    from_us: u64,
}

impl SubRecorder {
    fn new(sh: &Shared) -> SubRecorder {
        SubRecorder { hist: Histogram::new_with_bounds(1, 600_000_000, 3).unwrap(), total: 0, window: 0, from_us: sh.measure_from_wall_us }
    }
    fn record<'a>(&mut self, payloads: impl Iterator<Item = &'a [u8]>) {
        let now = wall_us();
        for p in payloads {
            self.total += 1;
            if let Some(ts) = stamp_of(p)
                && ts >= self.from_us
            {
                self.window += 1;
                let _ = self.hist.record(now.saturating_sub(ts).max(1));
            }
        }
    }
    fn done(self, sh: &Shared) {
        let mut h = sh.subs.hist.lock().unwrap();
        match h.as_mut() {
            Some(x) => x.add(&self.hist).unwrap(),
            None => *h = Some(self.hist),
        }
        sh.subs.total.lock().unwrap().push(self.total);
        sh.subs.window.lock().unwrap().push(self.window);
    }
}

impl Shared {
    fn record(&self, done: Instant, lat: Duration, msgs: u64) {
        self.acked_total.fetch_add(msgs, Ordering::Relaxed);
        let b = (done - self.start).as_millis() as u64 / self.bucket_ms;
        if let Some(c) = self.buckets.get(b as usize) {
            c.fetch_add(msgs, Ordering::Relaxed);
            self.bucket_max[b as usize].fetch_max(lat.as_micros() as u64, Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let payloads: Arc<Vec<Bytes>> = Arc::new(
        (0..1024)
            .map(|_| {
                Bytes::from(
                    (0..args.size)
                        .map(|_| rand::random::<u8>())
                        .collect::<Vec<_>>(),
                )
            })
            .collect(),
    );
    let total = args.warmup + args.duration;
    let nb = (total * 1000 / args.bucket_ms + 2) as usize;
    let start = Instant::now();
    let shared = Arc::new(Shared {
        start,
        measure_from: start + Duration::from_secs(args.warmup),
        stop: AtomicBool::new(false),
        buckets: (0..nb).map(|_| AtomicU64::new(0)).collect(),
        bucket_max: (0..nb).map(|_| AtomicU64::new(0)).collect(),
        bucket_ms: args.bucket_ms,
        errors: AtomicU64::new(0),
        acked_total: AtomicU64::new(0),
        measure_from_wall_us: wall_us() + args.warmup * 1_000_000,
        subs: Default::default(),
    });
    let sub_stop = Arc::new(AtomicBool::new(false));
    let mut sub_handles = Vec::new();

    let mut handles = Vec::new();
    let stored_check;
    match args.target {
        Target::Iggy => {
            let conns: Vec<String> = args.iggy.split(',').map(str::to_owned).collect();
            let admin = IggyClient::from_connection_string(&conns[0])?;
            admin.connect().await?;
            setup_iggy(&admin, &args).await?;
            let parts = args.partitions.unwrap_or(args.producers as u32);
            for s in 0..args.subscribers {
                let c = IggyClient::from_connection_string(&conns[s % conns.len()])?;
                c.connect().await?;
                sub_handles.push(tokio::spawn(iggy_subscriber(c, s, parts, shared.clone(), sub_stop.clone())));
            }
            if args.subscribers > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            for p in 0..args.producers {
                let c = IggyClient::from_connection_string(&conns[p % conns.len()])?;
                c.connect().await?;
                let part = p as u32 % args.partitions.unwrap_or(args.producers as u32);
                handles.push(tokio::spawn(iggy_producer(
                    c,
                    part,
                    args.clone(),
                    payloads.clone(),
                    shared.clone(),
                )));
            }
            let s = start.elapsed();
            tokio::time::sleep(Duration::from_secs(total).saturating_sub(s)).await;
            shared.stop.store(true, Ordering::Release);
            let hs = join(handles).await?;
            drain_subs(&shared, &sub_stop, sub_handles).await;
            tokio::time::sleep(Duration::from_millis(600)).await; // commit broadcast reaches all nodes
            let t = admin
                .get_topic(&Identifier::named("bench")?, &Identifier::named("t")?)
                .await?;
            let acked = shared.acked_total.load(Ordering::Relaxed);
            stored_check = format!(
                "topic reports {} messages; producers acked {}",
                t.map(|t| t.messages_count).unwrap_or(0),
                acked
            );
            finish(&args, &shared, hs, stored_check).await
        }
        Target::Mesh => {
            let cf: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&args.cluster).context("cluster file")?)?;
            let token = cf["token"].as_str().context("token")?.to_owned();
            let addrs: Vec<Addr> = cf["nodes"]
                .as_array()
                .context("nodes")?
                .iter()
                .map(|v| v.as_str().unwrap().parse())
                .collect::<Result<_>>()?;
            let epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_micros() as u64;
            // One iroh endpoint for all producers of this process (if needed).
            let probe = MeshClient::new(addrs.clone(), token.clone()).await?;
            let mut before = 0;
            for n in 0..addrs.len() {
                if let Ok(ControlResp::Status(s)) = probe.control(n, &Control::Status).await {
                    before = before.max(s.events);
                }
            }
            for s in 0..args.subscribers {
                let mc = MeshClient::new(addrs.clone(), token.clone()).await?;
                let sub = mc.subscribe(s % addrs.len(), Some("bench"), false).await?;
                sub_handles.push(tokio::spawn(mesh_subscriber(sub, mc, shared.clone(), sub_stop.clone())));
            }
            if args.subscribers > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            for p in 0..args.producers {
                let mut mc =
                    Arc::into_inner(MeshClient::new(addrs.clone(), token.clone()).await?).unwrap();
                mc.rpc_timeout = Duration::from_millis(args.rpc_timeout_ms);
                let mc = Arc::new(mc);
                let w = Writer::new(
                    mc,
                    "bench",
                    &format!("p{p}"),
                    epoch,
                    p % addrs.len(),
                    args.copies,
                );
                handles.push(tokio::spawn(mesh_producer(
                    w,
                    args.clone(),
                    payloads.clone(),
                    shared.clone(),
                )));
            }
            let s = start.elapsed();
            tokio::time::sleep(Duration::from_secs(total).saturating_sub(s)).await;
            shared.stop.store(true, Ordering::Release);
            let hs = join(handles).await?;
            drain_subs(&shared, &sub_stop, sub_handles).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let acked = shared.acked_total.load(Ordering::Relaxed);
            let mut per_node = Vec::new();
            for n in 0..addrs.len() {
                match probe.control(n, &Control::Status).await {
                    Ok(ControlResp::Status(s)) => {
                        per_node.push(format!("n{n}={}", s.events - before.min(s.events)))
                    }
                    _ => per_node.push(format!("n{n}=down")),
                }
            }
            stored_check = format!(
                "new events per node [{}]; producers acked {}",
                per_node.join(" "),
                acked
            );
            finish(&args, &shared, hs, stored_check).await
        }
    }
}

/// (latency hist, batches, msgs) in measured window, longest gap between acks (ms) incl. run end
type ProducerResult = (Histogram<u64>, u64, u64, u64);

async fn join(
    handles: Vec<tokio::task::JoinHandle<Result<ProducerResult>>>,
) -> Result<Vec<ProducerResult>> {
    let mut out = Vec::new();
    for h in handles {
        out.push(h.await??);
    }
    Ok(out)
}

async fn setup_iggy(admin: &IggyClient, args: &Args) -> Result<()> {
    let stream = Identifier::named("bench")?;
    if admin.get_stream(&stream).await?.is_some() {
        admin.delete_stream(&stream).await?;
    }
    admin.create_stream("bench", &Default::default()).await?;
    let opts = TopicCreateOptions {
        partitions_count: Some(args.partitions.unwrap_or(args.producers as u32)),
        durability: match args.durability {
            IggyDurability::Persisted => Durability::Persisted,
            IggyDurability::Replicated => Durability::Replicated,
        },
        replication_factor: args.replication,
        ..Default::default()
    };
    admin.create_topic(&stream, "t", &opts).await?;
    Ok(())
}

async fn iggy_producer(
    client: IggyClient,
    partition: u32,
    args: Args,
    payloads: Arc<Vec<Bytes>>,
    sh: Arc<Shared>,
) -> Result<ProducerResult> {
    let stream = Identifier::named("bench")?;
    let topic = Identifier::named("t")?;
    let part = Partitioning::partition_id(partition);
    let mut hist = Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?;
    let (mut batches, mut msgs) = (0, 0);
    let mut stall = Stall::new(&sh);
    let mut k = partition as usize;
    let mut pace = Pace::new(args.rate);
    while !sh.stop.load(Ordering::Acquire) {
        pace.wait().await;
        let ts = wall_us();
        let mut m: Vec<IggyMessage> = (0..args.batch)
            .map(|i| {
                IggyMessage::builder()
                    .payload(stamped(&payloads[(k + i) % payloads.len()], ts))
                    .build()
            })
            .collect::<Result<_, _>>()?;
        k += args.batch;
        let t0 = Instant::now();
        let r = client.send_messages(&stream, &topic, &part, &mut m).await;
        let t1 = Instant::now();
        match r {
            Ok(_) => {
                stall.ack(t1);
                sh.record(t1, t1 - t0, args.batch as u64);
                if t0 >= sh.measure_from {
                    hist.record((t1 - t0).as_micros().max(1) as u64)?;
                    batches += 1;
                    msgs += args.batch as u64;
                }
            }
            Err(e) => {
                if sh.errors.fetch_add(1, Ordering::Relaxed) < 5 {
                    eprintln!("iggy send error (partition {partition}): {e}");
                }
                if matches!(e, IggyError::PartitionNotFound(..)) {
                    bail!("partition {partition} not found: {e}");
                }
            }
        }
    }
    Ok((hist, batches, msgs, stall.finish()))
}

async fn mesh_producer(
    mut w: Writer,
    args: Args,
    payloads: Arc<Vec<Bytes>>,
    sh: Arc<Shared>,
) -> Result<ProducerResult> {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?;
    let (mut batches, mut msgs) = (0, 0);
    let mut stall = Stall::new(&sh);
    let mut k = w.preferred;
    let mut pace = Pace::new(args.rate);
    while !sh.stop.load(Ordering::Acquire) {
        pace.wait().await;
        let now = wall_us();
        let owned: Vec<Bytes> = (0..args.batch).map(|i| stamped(&payloads[(k + i) % payloads.len()], now)).collect();
        let evs: Vec<(u64, &[u8])> = owned.iter().map(|p| (now, &p[..])).collect();
        k += args.batch;
        let t0 = Instant::now();
        let bytes = w.encode(&evs)?;
        let r = w.append_encoded(&bytes, evs.len()).await;
        let t1 = Instant::now();
        match r {
            Ok(_) => {
                stall.ack(t1);
                sh.record(t1, t1 - t0, args.batch as u64);
                if t0 >= sh.measure_from {
                    hist.record((t1 - t0).as_micros().max(1) as u64)?;
                    batches += 1;
                    msgs += args.batch as u64;
                }
            }
            Err(e) => {
                if sh.errors.fetch_add(1, Ordering::Relaxed) < 5 {
                    eprintln!("mesh append error ({}): {e}", w.log.writer);
                }
            }
        }
    }
    Ok((hist, batches, msgs, stall.finish()))
}

/// Open-loop pacing: batch n is due at start + n/rate (no catch-up bursts
/// beyond one in flight; a system that falls behind simply delivers less).
struct Pace {
    every: Option<Duration>,
    next: tokio::time::Instant,
}

impl Pace {
    fn new(rate: u64) -> Pace {
        Pace {
            every: (rate > 0).then(|| Duration::from_secs_f64(1.0 / rate as f64)),
            next: tokio::time::Instant::now() + Duration::from_micros(rand::random::<u64>() % 10_000),
        }
    }
    async fn wait(&mut self) {
        if let Some(e) = self.every {
            tokio::time::sleep_until(self.next).await;
            self.next = (self.next + e).max(tokio::time::Instant::now() - e);
        }
    }
}

async fn iggy_subscriber(client: IggyClient, s: usize, parts: u32, sh: Arc<Shared>, stop: Arc<AtomicBool>) -> Result<()> {
    let stream = Identifier::named("bench")?;
    let topic = Identifier::named("t")?;
    let consumer = Consumer::new(Identifier::numeric(1000 + s as u32)?);
    let mut reader = client
        .reader(&stream, &topic, &consumer)
        .partitions(0..parts)
        .start(PollingStrategy::first())
        .options(PollOptions::wait(IggyDuration::from(Duration::from_millis(500))))
        .build()
        .await?;
    let mut rec = SubRecorder::new(&sh);
    while !stop.load(Ordering::Acquire) {
        match tokio::time::timeout(Duration::from_millis(200), reader.next()).await {
            Ok(Ok(batch)) => {
                let n = batch.messages.len() as u64;
                rec.record(batch.messages.iter().map(|m| &m.payload[..]));
                sh.subs.delivered_total.fetch_add(n, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                if sh.errors.fetch_add(1, Ordering::Relaxed) < 5 {
                    eprintln!("iggy subscriber {s}: {e:?}");
                }
            }
            Err(_) => {}
        }
    }
    rec.done(&sh);
    let _ = reader.close().await;
    Ok(())
}

async fn mesh_subscriber(
    mut sub: precog_mesh::client::Subscription,
    _mc: Arc<MeshClient>,
    sh: Arc<Shared>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    let mut rec = SubRecorder::new(&sh);
    while !stop.load(Ordering::Acquire) {
        match tokio::time::timeout(Duration::from_millis(200), sub.next()).await {
            Ok(Some(Ok(b))) => {
                let evs: Vec<_> = b.events().collect();
                rec.record(evs.iter().map(|e| &e.payload[..]));
                sh.subs.delivered_total.fetch_add(b.count as u64, Ordering::Relaxed);
            }
            Ok(Some(Err(e))) => {
                if sh.errors.fetch_add(1, Ordering::Relaxed) < 5 {
                    eprintln!("mesh subscriber: {e}");
                }
            }
            Ok(None) => bail!("subscription closed"),
            Err(_) => {}
        }
    }
    rec.done(&sh);
    Ok(())
}

/// After producers stop, give subscribers up to 15 s to receive everything
/// that was acknowledged, then stop them.
async fn drain_subs(sh: &Shared, stop: &AtomicBool, handles: Vec<tokio::task::JoinHandle<Result<()>>>) {
    if handles.is_empty() {
        return;
    }
    let want = sh.acked_total.load(Ordering::Relaxed) * handles.len() as u64;
    let deadline = Instant::now() + Duration::from_secs(15);
    while sh.subs.delivered_total.load(Ordering::Relaxed) < want && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::Release);
    for h in handles {
        match h.await {
            Ok(Err(e)) => eprintln!("subscriber ended: {e}"),
            Err(e) => eprintln!("subscriber panicked: {e}"),
            _ => {}
        }
    }
}

struct Stall {
    from: Instant,
    last: Instant,
    max: Duration,
}

impl Stall {
    fn new(sh: &Shared) -> Stall {
        Stall {
            from: sh.measure_from,
            last: sh.measure_from,
            max: Duration::ZERO,
        }
    }
    fn ack(&mut self, t: Instant) {
        if t > self.from {
            self.max = self.max.max(t - self.last.max(self.from));
        }
        self.last = t;
    }
    fn finish(mut self) -> u64 {
        self.ack(Instant::now());
        self.max.as_millis() as u64
    }
}

async fn finish(
    args: &Args,
    sh: &Shared,
    hs: Vec<ProducerResult>,
    stored_check: String,
) -> Result<()> {
    let mut hist = Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?;
    let (mut batches, mut msgs) = (0, 0);
    for (h, b, m, _) in &hs {
        hist.add(h)?;
        batches += b;
        msgs += m;
    }
    let secs = args.duration as f64;
    let timeline: Vec<Bucket> = sh
        .buckets
        .iter()
        .zip(&sh.bucket_max)
        .enumerate()
        .map(|(i, (c, m))| Bucket {
            t_ms: i as u64 * args.bucket_ms,
            msgs: c.load(Ordering::Relaxed),
            max_lat_us: m.load(Ordering::Relaxed),
        })
        .take(((args.warmup + args.duration) * 1000 / args.bucket_ms) as usize)
        .collect();
    let first = (args.warmup * 1000 / args.bucket_ms) as usize;
    let (mut gap, mut longest) = (0u64, 0u64);
    for b in &timeline[first.min(timeline.len())..] {
        if b.msgs == 0 {
            gap += args.bucket_ms;
            longest = longest.max(gap);
        } else {
            gap = 0;
        }
    }
    let r = Report {
        label: args.label.clone(),
        target: args.target,
        producers: args.producers,
        batch: args.batch,
        size: args.size,
        seconds: secs,
        batches,
        messages: msgs,
        errors: sh.errors.load(Ordering::Relaxed),
        msgs_per_sec: msgs as f64 / secs,
        payload_mb_per_sec: (msgs as f64 * args.size as f64) / secs / 1e6,
        lat_us_p50: hist.value_at_quantile(0.5),
        lat_us_p90: hist.value_at_quantile(0.9),
        lat_us_p99: hist.value_at_quantile(0.99),
        lat_us_p999: hist.value_at_quantile(0.999),
        lat_us_max: hist.max(),
        longest_ack_gap_ms: longest,
        producer_max_stall_ms: hs.iter().map(|h| h.3).collect(),
        timeline,
        stored_check,
        subscribers: args.subscribers,
        sub_msgs_per_sec_each: 0.0,
        sub_msgs_per_sec_total: 0.0,
        e2e_us_p50: 0,
        e2e_us_p99: 0,
        e2e_us_p999: 0,
        e2e_us_max: 0,
        subs_complete: String::new(),
        args: format!("{args:?}"),
    };
    let mut r = r;
    if args.subscribers > 0 {
        let mut win = sh.subs.window.lock().unwrap().clone();
        win.sort();
        let acked = sh.acked_total.load(Ordering::Relaxed);
        let totals = sh.subs.total.lock().unwrap().clone();
        r.sub_msgs_per_sec_each = win.get(win.len() / 2).copied().unwrap_or(0) as f64 / secs;
        r.sub_msgs_per_sec_total = win.iter().sum::<u64>() as f64 / secs;
        if let Some(h) = sh.subs.hist.lock().unwrap().as_ref() {
            r.e2e_us_p50 = h.value_at_quantile(0.5);
            r.e2e_us_p99 = h.value_at_quantile(0.99);
            r.e2e_us_p999 = h.value_at_quantile(0.999);
            r.e2e_us_max = h.max();
        }
        r.subs_complete = format!(
            "{}/{} got all {} acked (min {})",
            totals.iter().filter(|&&t| t >= acked).count(),
            args.subscribers,
            acked,
            totals.iter().min().unwrap_or(&0)
        );
        println!(
            "    subs {:>3}: {:>10.0} msg/s each, {:>11.0} total  e2e p50 {:>7}us p99 {:>8}us p999 {:>8}us max {:>9}us  {}",
            r.subscribers, r.sub_msgs_per_sec_each, r.sub_msgs_per_sec_total, r.e2e_us_p50, r.e2e_us_p99, r.e2e_us_p999, r.e2e_us_max, r.subs_complete
        );
    }
    println!(
        "{:<28} {:>5} prod x{:>4} x{:>5}B  {:>10.0} msg/s {:>8.1} MB/s  p50 {:>7}us p99 {:>7}us p999 {:>7}us max {:>8}us  gap {:>5}ms err {} stalls {:?} | {}",
        r.label,
        r.producers,
        r.batch,
        r.size,
        r.msgs_per_sec,
        r.payload_mb_per_sec,
        r.lat_us_p50,
        r.lat_us_p99,
        r.lat_us_p999,
        r.lat_us_max,
        r.longest_ack_gap_ms,
        r.errors,
        r.producer_max_stall_ms,
        r.stored_check
    );
    if let Some(p) = &args.json {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)?;
        writeln!(f, "{}", serde_json::to_string(&r)?)?;
    }
    Ok(())
}
