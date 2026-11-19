//! The scenarios. Each brings up its own cluster, runs a workload with its faults, heals, reads
//! everything back and asserts. `both` scenarios also run against the Redpanda dedup fork.

use crate::check::{Ev, Gen, Rec, divergence, verify};
use crate::docker::{Cluster, System, TOKEN};
use crate::precog::{self, Load, Sent, mesh, the_preferred};
use crate::redpanda;
use anyhow::{Result, bail};
use bytes::Bytes;
use precog_client::{Client, Start};
use precog_mesh::client::MeshClient;
use precog_mesh::proto::{AckStatus, Batch, Control, ControlResp, LogId};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct Scenario {
    pub name: &'static str,
    pub both: bool,
    pub about: &'static str,
}

const fn s(name: &'static str, both: bool, about: &'static str) -> Scenario {
    Scenario { name, both, about }
}

pub const ALL: &[Scenario] = &[
    s(
        "baseline",
        true,
        "loadgen workload (8 producers x 3 symbols, ~230 B JSON, 20k/s), no faults",
    ),
    s(
        "dedup-resend",
        true,
        "20% byte-identical resends of recent records: every one dropped, nothing lost",
    ),
    s(
        "dedup-replicas",
        true,
        "two producer replicas send the same events, to one node and to different nodes",
    ),
    s(
        "dedup-window",
        true,
        "a resend inside the window is dropped, one after it is appended",
    ),
    s(
        "dedup-ids",
        true,
        "empty ids are never deduplicated; one id with two bodies keeps the first; two ids with one body keep both",
    ),
    s(
        "dedup-restart",
        true,
        "the dedup index survives SIGKILL of every node: acked ids resent after restart are dropped, fresh ids appended",
    ),
    s(
        "dedup-capacity",
        false,
        "a full dedup index shortens the window (the newest ids still dropped, the oldest evicted) and says so",
    ),
    s(
        "big-records",
        true,
        "every 50th record ~256 KiB, mixed with 20% resends: batches rebuilt and recompressed",
    ),
    s(
        "kill-mid-append",
        true,
        "SIGKILL a node under load, restart it: acked implies readable on every node after repair",
    ),
    s(
        "freeze-mid-append",
        true,
        "SIGSTOP the preferred node under load for 4 s, then SIGCONT",
    ),
    s(
        "rolling-kill",
        true,
        "SIGKILL each node in turn under load, 3 s down each",
    ),
    s(
        "full-outage",
        true,
        "SIGKILL all three nodes at once under load (fsync per ack on Precog)",
    ),
    s(
        "full-outage-write-caching",
        false,
        "all three nodes killed with flush_delay_ms=100: loss is possible by design, reported",
    ),
    s(
        "isolate-node",
        true,
        "cut a node from its peers and from clients for 5 s",
    ),
    s(
        "partition-peers",
        true,
        "cut the preferred node from its peers only: clients still reach it",
    ),
    s(
        "clock-skew",
        false,
        "node clocks +10 min / -10 min (libfaketime) and client event times skewed",
    ),
    s(
        "retry-idempotent",
        false,
        "the same batch on every node; a retry after its first node dropped duplicates; a retry after a timeout",
    ),
    s(
        "conflict",
        false,
        "a batch identity reused with other bytes, including at a node that missed the original",
    ),
    s(
        "disk-full",
        false,
        "one node's disk fills under load: it fences, the others keep acking, it recovers with room",
    ),
    s(
        "fsync-failure",
        false,
        "an fsync error fences the node (needs a faulty block device)",
    ),
    s(
        "retention-under-load",
        false,
        "a 48 MiB retention budget under ~25 MB/s: disk stays bounded, the newest data stays readable",
    ),
    s(
        "compaction-under-load",
        false,
        "a compacted stream keeps every key's newest value through retention and restarts",
    ),
    s(
        "consumer-failover",
        false,
        "a consumer fails over to a node that is still repairing: no gap, no repeat",
    ),
    s(
        "commit-resume",
        false,
        "commit, resume from the committed position; commits on different nodes; the newest commit wins everywhere",
    ),
    s(
        "start-positions",
        false,
        "start at the beginning, the end, a time; Client::end covers every ack with a node down or lagging",
    ),
    s(
        "subscribers",
        false,
        "48 live subscribers across nodes and one slow one: every acked batch, in writer order, no repeat",
    ),
    s(
        "limits",
        false,
        "records, keys, names and event counts at and past the protocol limits",
    ),
    s(
        "many-streams",
        false,
        "300 streams x 4 writers: every stream readable on every node",
    ),
    s(
        "backpressure",
        false,
        "64 producers x 8 batches in flight, unthrottled",
    ),
    s(
        "write-caching-vs-fsync",
        false,
        "the same load with fsync per ack and with write caching: throughput and latency",
    ),
];

#[derive(Serialize, Debug)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    /// Reported, never failed (loss that is expected by design, measurements).
    pub info: bool,
    pub detail: Value,
}

#[derive(Default)]
pub struct Outcome {
    pub checks: Vec<Check>,
    pub metrics: serde_json::Map<String, Value>,
    pub skipped: Option<String>,
}

impl Outcome {
    fn check(&mut self, name: impl Into<String>, ok: bool, detail: impl Serialize) {
        self.checks.push(Check {
            name: name.into(),
            ok,
            info: false,
            detail: json!(detail),
        });
    }
    fn info(&mut self, name: impl Into<String>, detail: impl Serialize) {
        self.checks.push(Check {
            name: name.into(),
            ok: true,
            info: true,
            detail: json!(detail),
        });
    }
    fn metric(&mut self, k: &str, v: impl Serialize) {
        self.metrics.insert(k.into(), json!(v));
    }
}

pub struct Ctx {
    pub system: System,
    pub work: PathBuf,
    pub c: Cluster,
}

impl Ctx {
    fn precog(&self) -> bool {
        self.system == System::Precog
    }

    /// A fresh cluster, configured by `f`.
    async fn up(&mut self, f: impl FnOnce(&mut Cluster)) -> Result<()> {
        self.c.down().await?;
        self.c = Cluster::new(self.system, &self.work);
        f(&mut self.c);
        self.c.up().await?;
        if self.precog() {
            precog::ready(&mesh()).await?;
        }
        Ok(())
    }

    /// A stream (a topic on Redpanda, with the dedup window; Precog's window is node config).
    async fn stream(&self, name: &str, window_ms: Option<u64>) -> Result<()> {
        if !self.precog() {
            redpanda::topic(&self.c, name, window_ms).await?;
        }
        Ok(())
    }

    async fn load(&self, l: Load) -> Result<Sent> {
        if self.precog() {
            Ok(precog::run(mesh(), l).await)
        } else {
            redpanda::run(l).await
        }
    }

    /// Precog: repair until the nodes agree and return every node's read; Redpanda: one read.
    async fn read(&mut self, stream: &str) -> Result<Vec<Vec<Rec>>> {
        self.c.heal().await?;
        if self.precog() {
            let mc = mesh();
            precog::ready(&mc).await?;
            Ok(precog::converge(&mc, &[stream], Duration::from_secs(90))
                .await?
                .remove(0))
        } else {
            redpanda_ready(&self.c).await?;
            Ok(vec![redpanda::read_all(stream).await?])
        }
    }

    /// The acked-implies-readable, no-extra-copies, order and no-phantom checks on every node,
    /// and (Precog) that the nodes agree.
    fn verify(
        &self,
        o: &mut Outcome,
        tag: &str,
        reads: &[Vec<Rec>],
        sent: &Sent,
        copies: &HashMap<String, usize>,
    ) {
        for (i, r) in reads.iter().enumerate() {
            let v = verify(r, &sent.acked, &sent.sent, copies);
            let who = if self.precog() {
                format!("node {i}")
            } else {
                "cluster".into()
            };
            o.check(
                format!("{tag}: acked readable, no extra copies, in order ({who})"),
                v.clean(),
                &v,
            );
        }
        if reads.len() == 3 {
            let d = divergence(&reads[0], &reads[1]) + divergence(&reads[1], &reads[2]);
            o.check(
                format!("{tag}: nodes agree after repair"),
                d == 0,
                json!({"differing_records": d}),
            );
        }
    }
}

async fn redpanda_ready(c: &Cluster) -> Result<()> {
    for _ in 0..120 {
        if c.rpk(&["cluster", "health"]).await.is_ok_and(|h| {
            h.contains("Healthy:") && h.contains("true") && h.contains("Leaderless partitions (0)")
        }) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    bail!("redpanda not healthy after 120 s")
}

fn none() -> HashMap<String, usize> {
    HashMap::new()
}

/// Sends hand-made batches to one writer log (Precog) or partition 0 (Redpanda).
enum Sender {
    Precog {
        mc: Arc<MeshClient>,
        log: LogId,
        seq: u64,
        node: usize,
    },
    Redpanda {
        pc: rskafka::client::partition::PartitionClient,
    },
}

impl Sender {
    async fn new(ctx: &Ctx, stream: &str, writer: &str, node: usize) -> Result<Sender> {
        Ok(if ctx.precog() {
            Sender::Precog {
                mc: mesh(),
                log: LogId {
                    stream: stream.into(),
                    writer: writer.into(),
                    epoch: precog_client::now_ms(),
                },
                seq: 0,
                node,
            }
        } else {
            let c = redpanda::client().await?;
            Sender::Redpanda {
                pc: c
                    .partition_client(
                        stream,
                        0,
                        rskafka::client::partition::UnknownTopicHandling::Retry,
                    )
                    .await?,
            }
        })
    }

    async fn send(&mut self, evs: &[Ev]) -> Result<()> {
        match self {
            Sender::Precog { mc, log, seq, node } => {
                let b = precog::encode(log, *seq, evs);
                *seq += evs.len() as u64;
                mc.append(*node, &b, 2).await.map(drop)
            }
            Sender::Redpanda { pc } => {
                let recs = evs
                    .iter()
                    .map(|e| rskafka::record::Record {
                        key: None,
                        value: Some(e.body.clone()),
                        headers: if e.id.is_empty() {
                            Default::default()
                        } else {
                            [("redpanda-dedup-key".to_string(), e.id.clone())].into()
                        },
                        timestamp: rskafka::chrono::DateTime::from_timestamp_millis(
                            precog_client::now_ms() as i64,
                        )
                        .unwrap(),
                    })
                    .collect();
                pc.produce(recs, rskafka::client::partition::Compression::NoCompression)
                    .await?;
                Ok(())
            }
        }
    }
}

fn evs(g: &mut Gen, n: usize) -> Vec<Ev> {
    (0..n).map(|_| g.next().0).collect()
}

fn count(read: &[Rec]) -> HashMap<String, usize> {
    let mut m = HashMap::new();
    for r in read {
        *m.entry(r.uid.clone()).or_default() += 1;
    }
    m
}

fn copies_of(read: &[Rec], uids: &[Ev]) -> (usize, usize, usize) {
    let c = count(read);
    let mut zero_one_more = (0, 0, 0);
    for e in uids {
        match c.get(&e.uid).copied().unwrap_or(0) {
            0 => zero_one_more.0 += 1,
            1 => zero_one_more.1 += 1,
            _ => zero_one_more.2 += 1,
        }
    }
    zero_one_more
}

pub async fn run(name: &str, ctx: &mut Ctx) -> Result<Outcome> {
    let mut o = Outcome::default();
    match name {
        "baseline" => {
            load_and_verify(ctx, &mut o, Load::default(), |_| {}, |_| async { Ok(()) }).await?;
        }
        "dedup-resend" => {
            let l = Load { dup_rate: 0.2, ..Load::default() };
            let sent = load_and_verify(ctx, &mut o, l, |_| {}, |_| async { Ok(()) }).await?;
            if ctx.precog() {
                let dropped = precog::dedup_dropped(&mesh()).await;
                o.check("every acked resend dropped (status dedup_dropped)", dropped == sent.resends,
                    json!({"dropped": dropped, "resends_acked": sent.resends}));
            }
        }
        "dedup-replicas" => dedup_replicas(ctx, &mut o).await?,
        "dedup-window" => dedup_window(ctx, &mut o).await?,
        "dedup-ids" => dedup_ids(ctx, &mut o).await?,
        "dedup-restart" => dedup_restart(ctx, &mut o).await?,
        "dedup-capacity" => dedup_capacity(ctx, &mut o).await?,
        "big-records" => {
            let l = Load { dup_rate: 0.2, big_every: 50, rate: 5_000.0, ..Load::default() };
            load_and_verify(ctx, &mut o, l, |_| {}, |_| async { Ok(()) }).await?;
        }
        "kill-mid-append" => {
            let l = Load { duration: Duration::from_secs(12), ..Load::default() };
            fault_load(ctx, &mut o, l, |c| async move {
                sleep(3).await;
                c.kill(1).await?;
                sleep(4).await;
                c.start(1).await
            })
            .await?;
        }
        "freeze-mid-append" => {
            let l = Load { duration: Duration::from_secs(12), ..Load::default() };
            let p = if ctx.precog() { the_preferred(&l.stream) } else { 0 };
            fault_load(ctx, &mut o, l, move |c| async move {
                sleep(3).await;
                c.freeze(p).await?;
                sleep(4).await;
                c.thaw(p).await
            })
            .await?;
        }
        "rolling-kill" => {
            let l = Load { duration: Duration::from_secs(20), ..Load::default() };
            fault_load(ctx, &mut o, l, |c| async move {
                for i in 0..3 {
                    sleep(2).await;
                    c.kill(i).await?;
                    sleep(3).await;
                    c.start(i).await?;
                    wait_running(&c, i).await;
                }
                Ok(())
            })
            .await?;
        }
        "full-outage" => full_outage(ctx, &mut o, 0).await?,
        "full-outage-write-caching" => full_outage(ctx, &mut o, 100).await?,
        "isolate-node" => partition(ctx, &mut o, true).await?,
        "partition-peers" => partition(ctx, &mut o, false).await?,
        "clock-skew" => clock_skew(ctx, &mut o).await?,
        "retry-idempotent" => retry_idempotent(ctx, &mut o).await?,
        "conflict" => conflict(ctx, &mut o).await?,
        "disk-full" => disk_full(ctx, &mut o).await?,
        "fsync-failure" => {
            o.skipped = Some(
                "needs a block device that fails fsync on demand (dm-flakey/dm-error, privileged); \
                 covered in-process by crates/storage/tests/log.rs a_failed_fsync_is_fatal and the node's \
                 storage failpoints"
                    .into(),
            )
        }
        "retention-under-load" => retention(ctx, &mut o).await?,
        "compaction-under-load" => compaction(ctx, &mut o).await?,
        "consumer-failover" => consumer_failover(ctx, &mut o).await?,
        "commit-resume" => commit_resume(ctx, &mut o).await?,
        "start-positions" => start_positions(ctx, &mut o).await?,
        "subscribers" => subscribers(ctx, &mut o).await?,
        "limits" => limits(ctx, &mut o).await?,
        "many-streams" => many_streams(ctx, &mut o).await?,
        "backpressure" => {
            let l = Load { producers: 64, rate: 0.0, in_flight: 8, duration: Duration::from_secs(5), ..Load::default() };
            let s = load_and_verify(ctx, &mut o, l, |_| {}, |_| async { Ok(()) }).await?;
            o.check("no batch failed under backpressure", s.failed_batches == 0, json!({"failed": s.failed_batches}));
        }
        "write-caching-vs-fsync" => {
            for (label, ms) in [("fsync_per_ack", 0u64), ("write_caching_100ms", 100)] {
                let l = Load { rate: 0.0, duration: Duration::from_secs(5), ..Load::default() };
                let s = load_and_verify(ctx, &mut o, l, move |c| c.cfg["flush_delay_ms"] = json!(ms), |_| async { Ok(()) })
                    .await?;
                let mut s = s;
                o.metric(label, s.metrics());
            }
        }
        other => bail!("no scenario {other}"),
    }
    Ok(o)
}

async fn sleep(s: u64) {
    tokio::time::sleep(Duration::from_secs(s)).await
}

async fn wait_running(c: &Cluster, i: usize) {
    for _ in 0..40 {
        if c.running(i).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Brings up a cluster, runs `l` while `fault` acts on a copy of the cluster handle, heals,
/// reads back and verifies.
async fn load_and_verify<F, Fut>(
    ctx: &mut Ctx,
    o: &mut Outcome,
    l: Load,
    cfg: impl FnOnce(&mut Cluster),
    fault: F,
) -> Result<Sent>
where
    F: FnOnce(Arc<Cluster>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    ctx.up(cfg).await?;
    ctx.stream(&l.stream, Some(300_000)).await?;
    let handle = Arc::new(Cluster::shadow(&ctx.c));
    let f = tokio::spawn(fault(handle));
    let mut sent = ctx.load(l.clone()).await?;
    f.await??;
    o.metric("load", sent.metrics());
    let reads = ctx.read(&l.stream).await?;
    ctx.verify(o, "load", &reads, &sent, &none());
    Ok(std::mem::take(&mut sent))
}

async fn fault_load<F, Fut>(ctx: &mut Ctx, o: &mut Outcome, l: Load, fault: F) -> Result<Sent>
where
    F: FnOnce(Arc<Cluster>) -> Fut,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    load_and_verify(ctx, o, l, |_| {}, fault).await
}

async fn dedup_replicas(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    // Precog: the same node (as the SDK routes a stream) and different nodes. Redpanda: one
    // partition per producer pair, as keyed records land.
    let cases: &[(&str, Option<usize>, Option<usize>)] = if ctx.precog() {
        &[
            ("raw.replicas.same", None, None),
            ("raw.replicas.across", Some(0), Some(1)),
        ]
    } else {
        &[("raw.replicas.same", None, None)]
    };
    for &(stream, pa, pb) in cases {
        ctx.stream(stream, Some(300_000)).await?;
        let base = Load {
            stream: stream.into(),
            gen_prefix: Some("live-producer-bench-".into()),
            ..Load::default()
        };
        let a = Load {
            prefix: "replica-a-".into(),
            preferred: pa,
            ..base.clone()
        };
        let b = Load {
            prefix: "replica-b-".into(),
            preferred: pb,
            ..base
        };
        let (ra, rb) = tokio::join!(ctx.load(a), ctx.load(b));
        let mut sent = ra?;
        sent.merge(rb?);
        let reads = ctx.read(stream).await?;
        if pa == pb {
            ctx.verify(o, stream, &reads, &sent, &none());
            continue;
        }
        // Dedup is per node: replicas that write through different nodes at once race the
        // replication of each other's ids. The SDK sends a stream's producers to one node for
        // this reason; what slips through here is measured, not failed.
        for (i, r) in reads.iter().enumerate() {
            let v = verify(r, &sent.acked, &sent.sent, &none());
            o.check(
                format!("{stream}: acked readable, in order, no phantoms (node {i})"),
                v.lost == 0 && v.out_of_order == 0 && v.phantom == 0,
                &v,
            );
            o.info(
                format!("{stream}: copies that slipped past per-node dedup (node {i})"),
                json!({"extra_copies": v.extra_copies, "unique": v.unique}),
            );
        }
    }
    Ok(())
}

async fn dedup_window(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    let window = 4_000;
    ctx.up(|c| c.cfg["dedup_window_ms"] = json!(window)).await?;
    let stream = "raw.window";
    ctx.stream(stream, Some(window)).await?;
    let mut g = Gen::new("live-producer-window-0", 3, 0.0);
    let batch = evs(&mut g, 200);
    let mut s = Sender::new(ctx, stream, "w", 0).await?;
    s.send(&batch).await?;
    sleep(1).await;
    s.send(&batch).await?; // inside the window
    let r1 = ctx.read(stream).await?;
    let in_window = copies_of(&r1[0], &batch);
    o.check(
        "a resend 1 s later (window 4 s) is dropped",
        in_window == (0, 200, 0),
        json!({"zero_one_more": in_window}),
    );
    sleep(window / 1000 + 3).await;
    let mut s = Sender::new(ctx, stream, "w2", 1).await?;
    s.send(&batch).await?; // past the window
    let r2 = ctx.read(stream).await?;
    for (i, r) in r2.iter().enumerate() {
        let after = copies_of(r, &batch);
        o.check(
            format!("a resend 7 s later is appended again (view {i})"),
            after == (0, 0, 200),
            json!({"zero_one_more": after}),
        );
    }
    Ok(())
}

async fn dedup_ids(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    let stream = "raw.ids";
    ctx.stream(stream, Some(300_000)).await?;
    let mut s = Sender::new(ctx, stream, "ids", 0).await?;
    // empty ids, each sent twice
    let mut g = Gen::new("live-producer-empty-0", 3, 0.0);
    g.empty_ids = true;
    let empty = evs(&mut g, 100);
    s.send(&empty).await?;
    s.send(&empty).await?;
    // one id, two different bodies (a collision, or a producer bug): the id decides
    let mut g = Gen::new("live-producer-coll-0", 3, 0.0);
    let first = evs(&mut g, 100);
    let mut g2 = Gen::new("live-producer-coll2-0", 3, 0.0);
    let second: Vec<Ev> = first
        .iter()
        .map(|e| Ev {
            id: e.id.clone(),
            ..g2.next().0
        })
        .collect();
    s.send(&first).await?;
    s.send(&second).await?;
    // two ids, one body: both kept
    let mut g3 = Gen::new("live-producer-twin-0", 3, 0.0);
    let twins: Vec<Ev> = evs(&mut g3, 100);
    let twins2: Vec<Ev> = twins
        .iter()
        .map(|e| Ev {
            id: [&e.id[..], b"-twin"].concat(),
            ..e.clone()
        })
        .collect();
    s.send(&twins).await?;
    s.send(&twins2).await?;
    for (i, r) in ctx.read(stream).await?.iter().enumerate() {
        o.check(
            format!("empty ids are never deduplicated (view {i})"),
            copies_of(r, &empty) == (0, 0, 100),
            json!({"zero_one_more": copies_of(r, &empty)}),
        );
        o.check(
            format!("one id with two bodies keeps the first (view {i})"),
            copies_of(r, &first) == (0, 100, 0) && copies_of(r, &second) == (100, 0, 0),
            json!({"first": copies_of(r, &first), "second": copies_of(r, &second)}),
        );
        // twins share a uid (same body), so both copies count against the same uid
        o.check(
            format!("two ids with one body are both kept (view {i})"),
            copies_of(r, &twins) == (0, 0, 100),
            json!({"zero_one_more": copies_of(r, &twins)}),
        );
    }
    Ok(())
}

async fn dedup_restart(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    let stream = "raw.restart";
    ctx.stream(stream, Some(300_000)).await?;
    let mut g = Gen::new("live-producer-restart-0", 3, 0.0);
    let acked = evs(&mut g, 2000);
    let mut s = Sender::new(ctx, stream, "r", 0).await?;
    for c in acked.chunks(100) {
        s.send(c).await?;
    }
    for i in 0..3 {
        ctx.c.kill(i).await?;
    }
    sleep(1).await;
    for i in 0..3 {
        ctx.c.start(i).await?;
    }
    if ctx.precog() {
        precog::ready(&mesh()).await?;
    } else {
        redpanda_ready(&ctx.c).await?;
    }
    let fresh = evs(&mut g, 2000);
    let mut s = Sender::new(ctx, stream, "r2", 1).await?;
    for (a, f) in acked.chunks(100).zip(fresh.chunks(100)) {
        s.send(&[a, f].concat()).await?;
    }
    for (i, r) in ctx.read(stream).await?.iter().enumerate() {
        o.check(
            format!("acked ids resent after a full restart are dropped (view {i})"),
            copies_of(r, &acked) == (0, 2000, 0),
            json!({"zero_one_more": copies_of(r, &acked)}),
        );
        o.check(
            format!("fresh ids after the restart are appended (view {i})"),
            copies_of(r, &fresh) == (0, 2000, 0),
            json!({"zero_one_more": copies_of(r, &fresh)}),
        );
    }
    Ok(())
}

async fn dedup_capacity(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    let cap = 20_000;
    ctx.up(|c| c.cfg["dedup_max_entries"] = json!(cap)).await?;
    let stream = "raw.capacity";
    let mut g = Gen::new("live-producer-cap-0", 3, 0.0);
    let all = evs(&mut g, 60_000);
    // 5k new ids a second for 12 s: the cap holds about 4 s of them
    let mut s = Sender::new(ctx, stream, "fill", 0).await?;
    let mut next = Instant::now();
    for c in all.chunks(500) {
        tokio::time::sleep_until(next.into()).await;
        next += Duration::from_millis(100);
        s.send(c).await?;
    }
    let st = precog::status(&mesh(), 0).await?;
    o.check(
        "the index stays at its cap and reports a shorter window",
        st.dedup_entries <= cap as u64 && st.dedup_window_s < 300,
        json!({"entries": st.dedup_entries, "window_s": st.dedup_window_s}),
    );
    // probe age bands: the newest ids must still be caught, the oldest are evicted
    let (oldest, newest) = (&all[..1000], &all[all.len() - 1000..]);
    let mut p = Sender::new(ctx, stream, "probe", 0).await?;
    p.send(oldest).await?;
    p.send(newest).await?;
    let r = ctx.read(stream).await?;
    for (i, r) in r.iter().enumerate() {
        o.check(
            format!("the newest 1000 ids are still dropped (node {i})"),
            copies_of(r, newest) == (0, 1000, 0),
            json!({"zero_one_more": copies_of(r, newest)}),
        );
        o.info(
            format!("the oldest 1000 ids, evicted at the cap (node {i})"),
            json!({"zero_one_more": copies_of(r, oldest)}),
        );
    }
    Ok(())
}

async fn full_outage(ctx: &mut Ctx, o: &mut Outcome, flush_ms: u64) -> Result<()> {
    let l = Load {
        duration: Duration::from_secs(12),
        ..Load::default()
    };
    ctx.up(|c| c.cfg["flush_delay_ms"] = json!(flush_ms))
        .await?;
    ctx.stream(&l.stream, Some(300_000)).await?;
    let c = Arc::new(Cluster::shadow(&ctx.c));
    let f = tokio::spawn(async move {
        sleep(4).await;
        for i in 0..3 {
            c.kill(i).await?;
        }
        sleep(2).await;
        for i in 0..3 {
            c.start(i).await?;
        }
        anyhow::Ok(())
    });
    let mut sent = ctx.load(l.clone()).await?;
    f.await??;
    o.metric("load", sent.metrics());
    let reads = ctx.read(&l.stream).await?;
    // fsync per ack (Precog's default) must lose nothing; write caching (Redpanda as
    // live-ingestion runs it, or Precog's flush_delay_ms) may lose what was not yet synced
    let strict = ctx.precog() && flush_ms == 0;
    for (i, r) in reads.iter().enumerate() {
        let v = verify(r, &sent.acked, &sent.sent, &none());
        let name = format!("acked readable after all nodes were killed (view {i})");
        if strict {
            o.check(name, v.clean(), &v);
        } else {
            o.check(
                format!("no extra copies, in order, no phantoms (view {i})"),
                v.extra_copies == 0 && v.out_of_order == 0 && v.phantom == 0,
                &v,
            );
            o.info(
                format!("{name}: write caching, loss expected by design"),
                json!({"lost": v.lost, "acked": sent.acked.len()}),
            );
        }
    }
    if reads.len() == 3 {
        let d = divergence(&reads[0], &reads[1]) + divergence(&reads[1], &reads[2]);
        o.check(
            "nodes agree after repair",
            d == 0,
            json!({"differing_records": d}),
        );
    }
    Ok(())
}

async fn partition(ctx: &mut Ctx, o: &mut Outcome, clients: bool) -> Result<()> {
    let l = Load {
        duration: Duration::from_secs(12),
        ..Load::default()
    };
    let victim = if ctx.precog() {
        the_preferred(&l.stream)
    } else {
        2
    };
    ctx.up(|_| {}).await?;
    ctx.stream(&l.stream, Some(300_000)).await?;
    // the cut needs &mut Cluster: apply it from here while the load runs in a task
    let load = {
        let (precog, l) = (ctx.precog(), l.clone());
        tokio::spawn(async move {
            if precog {
                Ok(precog::run(mesh(), l).await)
            } else {
                redpanda::run(l).await
            }
        })
    };
    sleep(3).await;
    ctx.c.isolate(victim, clients).await?;
    sleep(5).await;
    ctx.c.heal_network().await;
    let mut sent = load.await??;
    o.metric("load", sent.metrics());
    o.metric("victim", victim);
    let reads = ctx.read(&l.stream).await?;
    ctx.verify(o, "load", &reads, &sent, &none());
    Ok(())
}

async fn clock_skew(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    let l = Load {
        dup_rate: 0.2,
        ..Load::default()
    };
    ctx.up(|c| {
        c.skew[1] = Some("+600s".into());
        c.skew[2] = Some("-600s".into());
    })
    .await?;
    let mc = mesh();
    let mut sent = precog::run(mc.clone(), l.clone()).await;
    o.metric("load", sent.metrics());
    let reads = ctx.read(&l.stream).await?;
    ctx.verify(o, "skewed nodes", &reads, &sent, &none());
    let dropped = precog::dedup_dropped(&mc).await;
    o.check(
        "every acked resend dropped with skewed node clocks",
        dropped == sent.resends,
        json!({"dropped": dropped, "resends": sent.resends}),
    );
    // start at a time: each node positions by its own clock
    let stream = "raw.skew.time";
    let mut g = Gen::new("live-producer-skew-0", 3, 0.0);
    let before = evs(&mut g, 100);
    let mut s = Sender::new(ctx, stream, "t", 0).await?;
    s.send(&before).await?;
    sleep(2).await;
    let t = precog_client::now_ms();
    sleep(1).await;
    let after = evs(&mut g, 100);
    s.send(&after).await?;
    precog::converge(&mc, &[stream], Duration::from_secs(30)).await?;
    for i in 0..3 {
        let got = read_from_time(&mc, i, stream, t).await?;
        let has_after = after.iter().all(|e| got.contains(&e.uid));
        let skew = ["none", "+600s", "-600s"][i];
        let detail =
            json!({"node_clock": skew, "returned": got.len(), "has_every_later_event": has_after});
        if i == 2 {
            // a slow clock stamps new data in the past: a time cursor from a correct clock lands
            // after it (positioning by time trusts the node's clock; nothing else does)
            o.info(
                format!("start at a time on node {i} (clock {skew})"),
                detail,
            );
        } else {
            o.check(
                format!("start at a time on node {i} returns every later event (clock {skew})"),
                has_after,
                detail,
            );
        }
    }
    // client event times far off: read order is per writer sequence, event time decides nothing
    let stream = "raw.skew.events";
    let log = LogId {
        stream: stream.into(),
        writer: "skewed".into(),
        epoch: 1,
    };
    let times = [0u64, 1, u64::MAX / 2, 4_102_444_800_000_000, 5];
    let refs: Vec<(u64, &[u8], &[u8])> = times.iter().map(|&t| (t, &b""[..], &b"x"[..])).collect();
    let b = Batch::encode_keyed(&log, 0, &refs)?;
    mc.append(0, &b, 2).await?;
    let r = precog::converge(&mc, &[stream], Duration::from_secs(30))
        .await?
        .remove(0);
    for (i, n) in r.iter().enumerate() {
        let seqs: Vec<u64> = n.iter().map(|r| r.pos).collect();
        o.check(
            format!("events with skewed event times read in sequence order (node {i})"),
            seqs == [0, 1, 2, 3, 4],
            &seqs,
        );
    }
    Ok(())
}

async fn read_from_time(
    mc: &MeshClient,
    i: usize,
    stream: &str,
    t: u64,
) -> Result<HashSet<String>> {
    let c = match mc
        .control(
            i,
            &Control::CursorAt {
                stream: stream.into(),
                time_ms: t,
            },
        )
        .await?
    {
        ControlResp::Cursor { cursor: Some(w) } => precog_mesh::store::cursor_from_wire(w, stream),
        r => bail!("unexpected {r:?}"),
    };
    let all = precog::read_node(mc, i, stream).await?;
    Ok(all
        .into_iter()
        .filter(|r| {
            let (w, e) = r.log.rsplit_once('#').unwrap();
            let log = LogId {
                stream: stream.into(),
                writer: w.into(),
                epoch: e.parse().unwrap(),
            };
            r.pos >= c.get(&log).copied().unwrap_or(0)
        })
        .map(|r| r.uid)
        .collect())
}

async fn retry_idempotent(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    let mc = mesh();
    let stream = "raw.retry";
    let log = LogId {
        stream: stream.into(),
        writer: "retry".into(),
        epoch: 7,
    };
    let mut g = Gen::new("live-producer-retry-0", 3, 0.0);
    let b1evs = evs(&mut g, 10);
    let b1 = precog::encode(&log, 0, &b1evs);
    let mut st = vec![];
    for n in 0..3 {
        st.push(mc.append(n, &b1, 2).await?.ack.status);
    }
    o.check(
        "the same batch on every node is acknowledged everywhere (Ok or Duplicate)",
        st.iter()
            .all(|s| matches!(s, AckStatus::Ok | AckStatus::Duplicate)),
        format!("{st:?}"),
    );
    // the first node drops a resent event and stores the rewritten batch; retrying the original
    // bytes on another node is still the same batch
    let mut b2evs = vec![b1evs[0].clone()];
    b2evs.extend(evs(&mut g, 9));
    let b2 = precog::encode(&log, 10, &b2evs);
    let r0 = mc.append(0, &b2, 2).await?.ack.status;
    let r1 = mc.append(1, &b2, 2).await.map(|r| r.ack.status);
    o.check(
        "a retry of a batch whose first node dropped a duplicate is acknowledged, not a Conflict",
        r0 == AckStatus::Ok && matches!(r1, Ok(AckStatus::Ok | AckStatus::Duplicate)),
        format!("{r0:?} then {r1:?}"),
    );
    // a retry after a timeout: the first node is frozen, the client fails over
    let b3evs = evs(&mut g, 10);
    let b3 = precog::encode(&log, 20, &b3evs);
    ctx.c.freeze(0).await?;
    let t = Instant::now();
    let r = mc.append(0, &b3, 2).await;
    let ms = t.elapsed().as_millis();
    ctx.c.thaw(0).await?;
    let again = mc.append(0, &b3, 2).await.map(|r| r.ack.status);
    o.check(
        "an append to a frozen node fails over and is acked",
        r.as_ref().is_ok_and(|r| r.node != 0),
        json!({"result": format!("{:?}", r.map(|r| (r.node, r.attempts, r.ack.status))), "ms": ms}),
    );
    o.check(
        "the same batch sent to the thawed node afterwards is acknowledged again",
        matches!(again, Ok(AckStatus::Ok | AckStatus::Duplicate)),
        format!("{again:?}"),
    );
    let reads = precog::converge(&mc, &[stream], Duration::from_secs(30))
        .await?
        .remove(0);
    let all: Vec<Ev> = [b1evs, b2evs[1..].to_vec(), b3evs].concat();
    for (i, r) in reads.iter().enumerate() {
        o.check(
            format!("every event exactly once (node {i})"),
            copies_of(r, &all) == (0, all.len(), 0) && r.len() == all.len(),
            json!({"zero_one_more": copies_of(r, &all), "records": r.len()}),
        );
    }
    Ok(())
}

async fn conflict(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|c| c.cfg["repair_interval_ms"] = json!(600_000))
        .await?;
    let mc = mesh();
    let stream = "raw.conflict";
    let log = LogId {
        stream: stream.into(),
        writer: "c".into(),
        epoch: 1,
    };
    let mut g = Gen::new("live-producer-conflict-0", 3, 0.0);
    let x = evs(&mut g, 5);
    let y = evs(&mut g, 5);
    mc.append(0, &precog::encode(&log, 0, &x), 2).await?;
    let yb = precog::encode(&log, 0, &y);
    let mut res = vec![];
    for n in 0..3 {
        res.push(
            mc.append(n, &yb, 2)
                .await
                .map(|r| r.ack.status)
                .map_err(|e| e.to_string()),
        );
    }
    o.check(
        "other bytes under an acked identity are refused as a Conflict on every node",
        res.iter()
            .all(|r| r.as_ref().is_err_and(|e| e.contains("Conflict"))),
        format!("{res:?}"),
    );
    // a node that missed the original: down while it was acked on the other two
    ctx.c.kill(2).await?;
    let x2 = evs(&mut g, 5);
    let r = mc.append(0, &precog::encode(&log, 5, &x2), 2).await?;
    o.metric("x2_ack_nodes", r.ack.nodes);
    ctx.c.start(2).await?;
    precog::ready(&mc).await?;
    let y2 = evs(&mut g, 5);
    let r2 = mc.append(2, &precog::encode(&log, 5, &y2), 2).await;
    let r2s = format!(
        "{:?}",
        r2.as_ref().map(|r| (r.node, r.ack.status, r.ack.nodes))
    );
    o.check(
        "other bytes under that identity at the node that missed it are not acknowledged",
        r2.is_err(),
        &r2s,
    );
    let reads = precog::converge(&mc, &[stream], Duration::from_secs(30))
        .await?
        .remove(0);
    let holds: Vec<(usize, usize, usize)> = reads
        .iter()
        .map(|r| (copies_of(r, &x2).1, copies_of(r, &y2).1, r.len()))
        .collect();
    let agree = reads
        .iter()
        .all(|r| copies_of(r, &x2) == (0, 5, 0) && copies_of(r, &y2) == (5, 0, 0));
    o.check(
        "after repair every node holds the acknowledged bytes, and only them",
        agree,
        json!({"per_node_x2_y2_records": holds}),
    );
    Ok(())
}

async fn disk_full(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    let small = 48;
    ctx.up(|c| {
        c.cfg["segment_size"] = json!(8u64 << 20);
        c.tmpfs_mb[2] = Some(small);
    })
    .await?;
    let l = Load {
        duration: Duration::from_secs(20),
        big_every: 100,
        rate: 10_000.0,
        ..Load::default()
    };
    let c = Arc::new(Cluster::shadow(&ctx.c));
    let probe = tokio::spawn(async move {
        let mut seen = vec![];
        for _ in 0..40 {
            sleep(1).await;
            let h = match c.health(2).await {
                Ok(h) => h,
                Err(_) if !c.running(2).await => "exited".into(),
                Err(e) => format!("unreachable: {e}"),
            };
            seen.push(h);
        }
        seen
    });
    let mut sent = precog::run(mesh(), l.clone()).await;
    let health = probe.await?;
    o.metric("load", sent.metrics());
    // fenced: /health answers 503, or the node exits so its supervisor restarts it into recovery
    let log = crate::docker::sh(&["docker", "logs", &ctx.c.name(2)])
        .await
        .unwrap_or_default();
    let exited = health.iter().any(|h| h == "exited");
    let fenced = health.iter().any(|h| h.contains("503"))
        || (exited && log.contains("No space left on device"));
    o.check(
        "the node with the full disk fences (503, or exits after ENOSPC)",
        fenced,
        json!({"health": health, "exited": exited}),
    );
    o.check(
        "the others keep acknowledging",
        sent.failed_batches == 0,
        json!({"failed": sent.failed_batches}),
    );
    let mc = mesh();
    for i in 0..2 {
        let r = precog::read_node(&mc, i, &l.stream).await?;
        let v = verify(&r, &sent.acked, &sent.sent, &none());
        o.check(
            format!("acked readable on node {i} while node 2 is fenced"),
            v.clean(),
            &v,
        );
    }
    let st = precog::status(&mc, 2).await;
    o.metric(
        "fenced_status",
        format!("{:?}", st.map(|s| (s.batches, s.wal_bytes))),
    );
    // room again: restart the node, it recovers its log and repairs
    ctx.c.grow_tmpfs(2, 2048).await?;
    let _ = ctx.c.kill(2).await; // it may have exited already
    ctx.c.start(2).await?;
    let reads = ctx.read(&l.stream).await?;
    ctx.verify(
        o,
        "after the disk has room and the node restarted",
        &reads,
        &sent,
        &none(),
    );
    o.check(
        "the recovered node is healthy",
        ctx.c.health(2).await.is_ok_and(|h| h.contains("200")),
        ctx.c.health(2).await.unwrap_or_default(),
    );
    Ok(())
}

async fn retention(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    let (budget, seg) = (48u64 << 20, 8u64 << 20);
    ctx.up(|c| {
        c.cfg["segment_size"] = json!(seg);
        c.cfg["retention_bytes"] = json!(budget);
        c.cfg["retention_interval_ms"] = json!(500);
    })
    .await?;
    let l = Load {
        duration: Duration::from_secs(20),
        big_every: 200,
        rate: 20_000.0,
        ..Load::default()
    };
    let c = Arc::new(Cluster::shadow(&ctx.c));
    let sampler = tokio::spawn(async move {
        let mut max = [0u64; 3];
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            for (i, m) in max.iter_mut().enumerate() {
                *m = (*m).max(c.disk_bytes(i));
            }
        }
        max
    });
    let mut sent = precog::run(mesh(), l.clone()).await;
    // a marker per writer, written last: the newest data must survive
    let mut g = Gen::new("live-producer-marker-0", 1, 0.0);
    let marker = evs(&mut g, 50);
    Sender::new(ctx, &l.stream, "marker", 0)
        .await?
        .send(&marker)
        .await?;
    let max = sampler.await?;
    o.metric("load", sent.metrics());
    o.metric("disk_max_bytes", max);
    let limit = budget + 4 * seg;
    o.check(
        "disk stays within the budget plus four segments",
        max.iter().all(|&m| m <= limit),
        json!({"max": max, "limit": limit}),
    );
    let mc = mesh();
    let reads = precog::converge(&mc, &[&l.stream], Duration::from_secs(60))
        .await?
        .remove(0);
    for (i, r) in reads.iter().enumerate() {
        let v = verify(
            r,
            &HashSet::new(),
            &sent
                .sent
                .iter()
                .cloned()
                .chain(marker.iter().map(|e| e.uid.clone()))
                .collect(),
            &none(),
        );
        o.check(
            format!("what retention kept reads in order with no copies or phantoms (node {i})"),
            v.extra_copies == 0 && v.out_of_order == 0 && v.phantom == 0,
            &v,
        );
        o.check(format!("the newest data survives retention (node {i})"), copies_of(r, &marker) == (0, 50, 0),
            json!({"zero_one_more": copies_of(r, &marker), "kept_records": r.len(), "acked": sent.acked.len()}));
    }
    // a consumer from the beginning starts at the retention floor
    let client = Client::connect(&precog::nodes(), TOKEN)?;
    let mut cons = client.consumer(&l.stream, "ret", Start::Beginning).await?;
    let first = cons.poll(1 << 20).await;
    o.check(
        "a consumer from the beginning reads from the retention floor",
        first.as_ref().is_ok_and(|e| !e.is_empty()),
        format!("{:?}", first.map(|e| e.len())),
    );
    Ok(())
}

async fn compaction(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    let state = "state.bench";
    ctx.up(|c| {
        c.cfg["segment_size"] = json!(4u64 << 20);
        c.cfg["retention_bytes"] = json!(24u64 << 20);
        c.cfg["retention_interval_ms"] = json!(500);
        c.cfg["streams"] = json!({ state: {"compacted": true} });
    })
    .await?;
    let mc = mesh();
    let load = tokio::spawn(precog::run(
        mc.clone(),
        Load {
            duration: Duration::from_secs(15),
            big_every: 200,
            ..Load::default()
        },
    ));
    let log = LogId {
        stream: state.into(),
        writer: "state-writer".into(),
        epoch: 1,
    };
    let (keys, rounds) = (500usize, 30usize);
    let mut seq = 0;
    for round in 0..rounds {
        let vals: Vec<(Vec<u8>, Vec<u8>)> = (0..keys)
            .map(|k| {
                (
                    format!("k{k}").into_bytes(),
                    format!("v{round}-{k}").into_bytes(),
                )
            })
            .collect();
        let refs: Vec<(u64, &[u8], &[u8])> =
            vals.iter().map(|(k, v)| (0, &k[..], &v[..])).collect();
        mc.append(0, &Batch::encode_keyed(&log, seq, &refs)?, 2)
            .await?;
        seq += keys as u64;
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    load.await?;
    let check_latest = |o: &mut Outcome, when: &str, per_node: Vec<Vec<(Vec<u8>, Vec<u8>)>>| {
        for (i, e) in per_node.iter().enumerate() {
            let m: HashMap<_, _> = e.iter().cloned().collect();
            let bad = (0..keys)
                .filter(|k| {
                    m.get(format!("k{k}").as_bytes()).map(|v| v.as_slice())
                        != Some(format!("v{}-{k}", rounds - 1).as_bytes())
                })
                .count();
            o.check(
                format!("every key's newest value on node {i} {when}"),
                bad == 0 && m.len() == keys,
                json!({"keys": m.len(), "wrong_or_missing": bad}),
            );
        }
    };
    let latest = |mc: Arc<MeshClient>| async move {
        let mut out = vec![];
        for i in 0..3 {
            out.push(
                match mc
                    .control(
                        i,
                        &Control::Latest {
                            stream: state.into(),
                        },
                    )
                    .await?
                {
                    ControlResp::Latest { entries } => entries,
                    r => bail!("unexpected {r:?}"),
                },
            );
        }
        anyhow::Ok(out)
    };
    let disk: Vec<u64> = (0..3).map(|i| ctx.c.disk_bytes(i)).collect();
    o.metric("disk_bytes", disk);
    check_latest(o, "under retention", latest(mc.clone()).await?);
    for i in 0..3 {
        ctx.c.kill(i).await?;
        ctx.c.start(i).await?;
    }
    precog::ready(&mc).await?;
    check_latest(
        o,
        "after every node was killed and restarted",
        latest(mc.clone()).await?,
    );
    Ok(())
}

/// A stream name whose preferred node is `n`.
fn stream_on(prefix: &str, n: usize) -> String {
    (0..)
        .map(|i| format!("{prefix}.{i}"))
        .find(|s| the_preferred(s) == n)
        .unwrap()
}

async fn consumer_failover(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    // repair only on demand: a restarted node stays behind while the consumer reads from it
    ctx.up(|c| c.cfg["repair_interval_ms"] = json!(600_000))
        .await?;
    let stream = stream_on("raw.failover", 2);
    let l = Load {
        stream: stream.clone(),
        duration: Duration::from_secs(14),
        ..Load::default()
    };
    let c = Arc::new(Cluster::shadow(&ctx.c));
    let faults = tokio::spawn(async move {
        sleep(2).await;
        c.kill(2).await?; // the consumer's node; it misses ~4 s of appends
        sleep(4).await;
        c.start(2).await?;
        sleep(3).await;
        c.kill(0).await?; // the consumer failed over to node 0: now it must go back to the laggard
        sleep(3).await;
        c.start(0).await
    });
    let client = Client::connect(&precog::nodes(), TOKEN)?;
    let reader = tokio::spawn(async move {
        let mut cons = client
            .consumer(&l.stream, "failover", Start::Beginning)
            .await?;
        let (mut got, mut errs) = (Vec::<(String, u64, String)>::new(), 0);
        let end = Instant::now() + Duration::from_secs(60);
        while Instant::now() < end {
            match cons.poll(1 << 20).await {
                Ok(evs) if evs.is_empty() => tokio::time::sleep(Duration::from_millis(50)).await,
                Ok(evs) => got.extend(evs.into_iter().map(|e| {
                    (
                        format!("{}#{}", e.writer, e.epoch),
                        e.seq,
                        crate::check::uid_of(&e.payload).unwrap_or("?").to_string(),
                    )
                })),
                Err(_) => errs += 1,
            }
        }
        anyhow::Ok((got, errs))
    });
    let mut sent = precog::run(
        mesh(),
        Load {
            stream: stream.clone(),
            duration: Duration::from_secs(14),
            ..Load::default()
        },
    )
    .await;
    faults.await??;
    // let the reader drain what is there, with repair on demand once everything is up
    sleep(5).await;
    let mc = mesh();
    precog::ready(&mc).await?;
    precog::converge(&mc, &[&stream], Duration::from_secs(60)).await?;
    let (got, errs) = reader.await??;
    o.metric("load", sent.metrics());
    o.metric("poll_errors", errs);
    let recs: Vec<Rec> = got
        .into_iter()
        .map(|(log, pos, uid)| Rec { log, pos, uid })
        .collect();
    let v = verify(&recs, &sent.acked, &sent.sent, &none());
    o.check(
        "the consumer saw every acked event once, in writer order, across failovers",
        v.clean(),
        &v,
    );
    Ok(())
}

async fn commit_resume(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    let stream = "raw.commit";
    let l = Load {
        stream: stream.into(),
        duration: Duration::from_secs(5),
        ..Load::default()
    };
    let mut sent = precog::run(mesh(), l).await;
    o.metric("load", sent.metrics());
    let client = Client::connect(&precog::nodes(), TOKEN)?;
    let uid =
        |e: &precog_client::Event| crate::check::uid_of(&e.payload).unwrap_or("?").to_string();
    let mut seen: Vec<String> = vec![];
    // reader 1: about a third, commit (on the preferred node), stop
    let mut c1 = client
        .consumer(
            stream,
            "g",
            Start::Committed {
                or: Box::new(Start::Beginning),
            },
        )
        .await?;
    while seen.len() < sent.acked.len() / 3 {
        seen.extend(c1.poll(64 << 10).await?.iter().map(uid));
    }
    c1.commit().await?;
    let first_commit = c1.cursor().clone();
    // reader 2 resumes; the preferred node dies before its commit, which lands elsewhere
    let mut c2 = client
        .consumer(
            stream,
            "g",
            Start::Committed {
                or: Box::new(Start::Beginning),
            },
        )
        .await?;
    let p = the_preferred(stream);
    while seen.len() < 2 * sent.acked.len() / 3 {
        seen.extend(c2.poll(64 << 10).await?.iter().map(uid));
    }
    ctx.c.kill(p).await?;
    c2.commit().await?;
    let second = c2.cursor().clone();
    ctx.c.start(p).await?;
    let mc = mesh();
    precog::ready(&mc).await?;
    for i in 0..3 {
        let _ = mc.control(i, &Control::Repair).await;
    }
    sleep(2).await;
    let mut pos = vec![];
    for i in 0..3 {
        pos.push(
            match mc
                .control(
                    i,
                    &Control::Position {
                        group: "g".into(),
                        stream: stream.into(),
                    },
                )
                .await?
            {
                ControlResp::Cursor { cursor: Some(w) } => {
                    Some(precog_mesh::store::cursor_from_wire(w, stream))
                }
                _ => None,
            },
        );
    }
    let newest_everywhere = pos.iter().all(|p| p.as_ref() == Some(&second));
    o.check("the newest commit (made on another node) is the position on every node", newest_everywhere,
        json!({"nodes_with_newest": pos.iter().filter(|p| p.as_ref() == Some(&second)).count(),
               "nodes_with_older": pos.iter().filter(|p| p.as_ref() == Some(&first_commit)).count()}));
    // reader 3 resumes from the newest commit and reads to the end
    let mut c3 = client
        .consumer(
            stream,
            "g",
            Start::Committed {
                or: Box::new(Start::Beginning),
            },
        )
        .await?;
    loop {
        let evs = c3.poll(1 << 20).await?;
        if evs.is_empty() {
            break;
        }
        seen.extend(evs.iter().map(uid));
    }
    let set: HashSet<&String> = seen.iter().collect();
    let lost = sent.acked.iter().filter(|u| !set.contains(u)).count();
    o.check(
        "commit and resume: no loss, no repeat",
        lost == 0 && set.len() == seen.len(),
        json!({"read": seen.len(), "unique": set.len(), "lost": lost}),
    );
    Ok(())
}

async fn start_positions(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|c| c.cfg["repair_interval_ms"] = json!(600_000))
        .await?;
    let stream = "raw.start";
    let mc = mesh();
    let client = Client::connect(&precog::nodes(), TOKEN)?;
    let uid =
        |e: &precog_client::Event| crate::check::uid_of(&e.payload).unwrap_or("?").to_string();
    let drain = |mut c: precog_client::Consumer| async move {
        let mut out = vec![];
        loop {
            let evs = c.poll(1 << 20).await?;
            if evs.is_empty() {
                return anyhow::Ok(out);
            }
            out.extend(evs.iter().map(uid));
        }
    };
    let mut g = Gen::new("live-producer-start-0", 3, 0.0);
    let a = evs(&mut g, 300);
    let mut s = Sender::new(ctx, stream, "s", 0).await?;
    s.send(&a).await?;
    sleep(1).await;
    let end = client.consumer(stream, "e", Start::End).await?;
    let t = precog_client::now_ms();
    sleep(1).await;
    let b = evs(&mut g, 300);
    s.send(&b).await?;
    let ids = |v: &[Ev]| v.iter().map(|e| e.uid.clone()).collect::<Vec<_>>();
    let from_end = drain(end).await?;
    o.check(
        "start at the end: only what came after",
        from_end == ids(&b),
        json!({"read": from_end.len()}),
    );
    let from_t = drain(client.consumer(stream, "t", Start::Time(t)).await?).await?;
    o.check(
        "start at a time: what was logged after it",
        from_t == ids(&b),
        json!({"read": from_t.len()}),
    );
    let from_0 = drain(client.consumer(stream, "b", Start::Beginning).await?).await?;
    o.check(
        "start at the beginning: everything",
        from_0 == [ids(&a), ids(&b)].concat(),
        json!({"read": from_0.len()}),
    );
    // Client::end proofs: a node down, then that node back but behind (repair is on demand)
    let down = the_preferred(stream);
    ctx.c.kill(down).await?;
    let mut w = Sender::new(ctx, stream, "s2", (down + 1) % 3).await?;
    let mut acked: Vec<(LogId, u64)> = vec![];
    for _ in 0..20 {
        w.send(&evs(&mut g, 50)).await?;
    }
    if let Sender::Precog { log, seq, .. } = &w {
        acked.push((log.clone(), *seq));
    }
    acked.push((
        LogId {
            stream: stream.into(),
            writer: "s".into(),
            epoch: 0,
        },
        0,
    ));
    let covers = |e: &precog_client::Cursor| {
        acked
            .iter()
            .all(|(l, s)| e.get(l).copied().unwrap_or(0) >= *s)
    };
    let e1 = client.end(stream).await;
    o.check(
        "Client::end covers every ack with a node down",
        e1.as_ref().is_ok_and(covers),
        format!("{:?}", e1.as_ref().map(|e| e.len())),
    );
    ctx.c.start(down).await?;
    precog::ready(&mc).await?;
    let e2 = client.end(stream).await;
    o.check(
        "Client::end covers every ack with a node back but behind",
        e2.as_ref().is_ok_and(covers),
        format!("{:?}", e2.as_ref().map(|e| e.len())),
    );
    Ok(())
}

async fn subscribers(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    let stream = "raw.live";
    let (n_subs, slow_ms) = (48usize, 5u64);
    let mut tasks = vec![];
    for k in 0..=n_subs {
        let slow = k == n_subs;
        let node = k % 3;
        let mc = mesh();
        let mut sub = mc.subscribe(node, Some(stream), false).await?;
        tasks.push(tokio::spawn(async move {
            let _keep = mc;
            let mut got = vec![];
            loop {
                match tokio::time::timeout(
                    Duration::from_secs(if slow { 60 } else { 15 }),
                    sub.next(),
                )
                .await
                {
                    Ok(Some(Ok(b))) => {
                        let log = format!("{}#{}", b.id.log.writer, b.id.log.epoch);
                        for e in b.try_events().unwrap_or_default() {
                            got.push(Rec {
                                log: log.clone(),
                                pos: e.seq,
                                uid: crate::check::uid_of(&e.payload).unwrap_or("?").into(),
                            });
                        }
                        if slow {
                            tokio::time::sleep(Duration::from_millis(slow_ms)).await;
                        }
                    }
                    _ => return (node, slow, got),
                }
            }
        }));
    }
    let mut sent = precog::run(
        mesh(),
        Load {
            stream: stream.into(),
            ..Load::default()
        },
    )
    .await;
    o.metric("load", sent.metrics());
    let mut bad = vec![];
    let mut slow_detail = Value::Null;
    for t in tasks {
        let (node, slow, got) = t.await?;
        let v = verify(&got, &sent.acked, &sent.sent, &none());
        if slow {
            o.check(
                "a slow subscriber still gets every acked event in writer order",
                v.clean(),
                &v,
            );
            slow_detail = json!(got.len());
        } else if !v.clean() {
            bad.push(json!({"node": node, "verdict": v}));
        }
    }
    o.metric("slow_subscriber_records", slow_detail);
    o.check(
        format!("{n_subs} subscribers: every acked event once, in writer order"),
        bad.is_empty(),
        json!({"failing": bad.iter().take(3).collect::<Vec<_>>(), "failing_count": bad.len()}),
    );
    Ok(())
}

async fn limits(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    use precog_mesh::proto::{MAX_BATCH_BYTES, MAX_EVENTS_PER_BATCH, MAX_KEY, MAX_NAME};
    ctx.up(|_| {}).await?;
    let mc = mesh();
    let stream = "raw.limits";
    let mut seq = 0u64;
    let mut try_batch = |name: &str, writer: String, evs: Vec<(Vec<u8>, Vec<u8>)>| {
        let log = LogId {
            stream: stream.into(),
            writer: if writer.is_empty() {
                name.into()
            } else {
                writer
            },
            epoch: 1,
        };
        let refs: Vec<(u64, &[u8], &[u8])> = evs.iter().map(|(k, v)| (0, &k[..], &v[..])).collect();
        let r = Batch::encode_keyed(&log, seq, &refs);
        seq += 1_000_000;
        r
    };
    let mut cases: Vec<(&str, bool, Result<Bytes>)> = vec![];
    let big = MAX_BATCH_BYTES - 4096;
    cases.push((
        "one record just under the batch limit",
        true,
        try_batch("big-ok", String::new(), vec![(vec![], vec![b'x'; big])]),
    ));
    cases.push((
        "one record past the batch limit",
        false,
        try_batch(
            "big-no",
            String::new(),
            vec![(vec![], vec![b'x'; MAX_BATCH_BYTES + 1])],
        ),
    ));
    cases.push((
        "a key at the limit",
        true,
        try_batch(
            "key-ok",
            String::new(),
            vec![(vec![b'k'; MAX_KEY], b"v".to_vec())],
        ),
    ));
    cases.push((
        "a key past the limit",
        false,
        try_batch(
            "key-no",
            String::new(),
            vec![(vec![b'k'; MAX_KEY + 1], b"v".to_vec())],
        ),
    ));
    cases.push((
        "a writer name at the limit",
        true,
        try_batch("", "w".repeat(MAX_NAME), vec![(vec![], b"v".to_vec())]),
    ));
    cases.push((
        "a writer name past the limit",
        false,
        try_batch("", "w".repeat(MAX_NAME + 1), vec![(vec![], b"v".to_vec())]),
    ));
    let many = |n: u32| (0..n).map(|_| (vec![], b"e".to_vec())).collect::<Vec<_>>();
    cases.push((
        "events per batch at the limit",
        true,
        try_batch("events-ok", String::new(), many(MAX_EVENTS_PER_BATCH)),
    ));
    cases.push((
        "events per batch past the limit",
        false,
        try_batch("events-no", String::new(), many(MAX_EVENTS_PER_BATCH + 1)),
    ));
    for (name, accept, enc) in cases {
        let res = match enc {
            Err(e) => Err(format!("refused by the encoder: {e}")),
            Ok(b) => mc
                .append(0, &b, 2)
                .await
                .map(|r| format!("{:?}", r.ack.status))
                .map_err(|e| e.to_string()),
        };
        o.check(
            format!("{name}: {}", if accept { "accepted" } else { "refused" }),
            res.is_ok() == accept,
            format!("{res:?}"),
        );
    }
    // a raw frame past the limits never reaches a node through the encoder; send one anyway
    let mut raw = precog::encode(
        &LogId {
            stream: stream.into(),
            writer: "raw".into(),
            epoch: 1,
        },
        0,
        &[Ev {
            uid: String::new(),
            id: vec![],
            body: b"x".to_vec(),
        }],
    )
    .to_vec();
    raw.resize(MAX_BATCH_BYTES + 64, 0);
    let r = mc.append(0, &Bytes::from(raw), 2).await;
    o.check(
        "a malformed oversized batch is refused",
        r.is_err(),
        format!("{:?}", r.map(|r| r.ack.status)),
    );
    let mut ok = true;
    for i in 0..3 {
        ok &= ctx.c.health(i).await.is_ok_and(|h| h.contains("200"));
    }
    let mut g = Gen::new("live-producer-after-limits-0", 3, 0.0);
    let after = evs(&mut g, 10);
    Sender::new(ctx, stream, "after", 1)
        .await?
        .send(&after)
        .await?;
    let reads = precog::converge(&mc, &[stream], Duration::from_secs(60))
        .await?
        .remove(0);
    let have = reads.iter().all(|r| copies_of(r, &after) == (0, 10, 0));
    o.check(
        "every node stays healthy and keeps accepting after the refusals",
        ok && have,
        json!({"healthy": ok, "readable": have}),
    );
    Ok(())
}

async fn many_streams(ctx: &mut Ctx, o: &mut Outcome) -> Result<()> {
    ctx.up(|_| {}).await?;
    let mc = mesh();
    let (streams, writers, batches, per) = (300usize, 4usize, 10usize, 10usize);
    let sem = Arc::new(tokio::sync::Semaphore::new(64));
    let mut js = tokio::task::JoinSet::new();
    for s in 0..streams {
        for w in 0..writers {
            let (mc, sem) = (mc.clone(), sem.clone());
            js.spawn(async move {
                let _p = sem.acquire().await.unwrap();
                let stream = format!("raw.many.{s}");
                let log = LogId {
                    stream: stream.clone(),
                    writer: format!("w{w}"),
                    epoch: 1,
                };
                let mut g = Gen::new(&format!("live-producer-many-{s}-{w}"), 3, 0.0);
                let mut sent = Sent::default();
                for b in 0..batches {
                    let e = evs(&mut g, per);
                    sent.sent.extend(e.iter().map(|e| e.uid.clone()));
                    if mc
                        .append(
                            the_preferred(&stream),
                            &precog::encode(&log, (b * per) as u64, &e),
                            2,
                        )
                        .await
                        .is_ok()
                    {
                        sent.acked.extend(e.iter().map(|e| e.uid.clone()));
                    } else {
                        sent.failed_batches += 1;
                    }
                }
                (s, sent)
            });
        }
    }
    let mut by_stream: HashMap<usize, Sent> = HashMap::new();
    while let Some(r) = js.join_next().await {
        let (s, sent) = r?;
        by_stream.entry(s).or_default().merge(sent);
    }
    let names: Vec<String> = (0..streams).map(|s| format!("raw.many.{s}")).collect();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let all = precog::converge(&mc, &refs, Duration::from_secs(60)).await?;
    let (mut bad, mut failed) = (0, 0);
    for (s, per_node) in all.iter().enumerate() {
        let sent = &by_stream[&s];
        failed += sent.failed_batches;
        bad += per_node
            .iter()
            .filter(|r| !verify(r, &sent.acked, &sent.sent, &none()).clean())
            .count();
    }
    o.check(
        format!("{streams} streams x {writers} writers readable on every node"),
        bad == 0 && failed == 0,
        json!({"stream_node_failures": bad, "failed_batches": failed}),
    );
    Ok(())
}

impl Cluster {
    /// A handle for fault tasks: same nodes, no network rules of its own.
    pub fn shadow(c: &Cluster) -> Cluster {
        let mut s = Cluster::new(c.system, &c.work);
        s.cfg = c.cfg.clone();
        s
    }
}
