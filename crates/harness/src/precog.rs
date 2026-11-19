//! Driving Precog: the loadgen workload over writer logs, per-node read-back, repair until the
//! nodes agree.

use crate::check::{Ev, Gen, Rec, quantile};
use crate::docker::{TOKEN, ip};
use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
use precog_client::dedup_key;
use precog_mesh::client::MeshClient;
use precog_mesh::proto::{Ack, Batch, Control, ControlResp, Kind, LogId, Status};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn nodes() -> Vec<String> {
    (0..3).map(|i| format!("tcp://{}:7000", ip(i))).collect()
}

pub fn mesh() -> Arc<MeshClient> {
    let mut mc = MeshClient::new(
        nodes().iter().map(|a| a.parse().unwrap()).collect(),
        TOKEN.into(),
    )
    .unwrap();
    let m = Arc::get_mut(&mut mc).unwrap();
    m.rpc_timeout = Duration::from_millis(1000);
    m.append_deadline = Duration::from_secs(20);
    mc
}

pub async fn status(mc: &MeshClient, i: usize) -> Result<Status> {
    match mc.control(i, &Control::Status).await? {
        ControlResp::Status(s) => Ok(s),
        r => bail!("unexpected {r:?}"),
    }
}

/// Waits until every node answers.
pub async fn ready(mc: &MeshClient) -> Result<()> {
    let t = Instant::now();
    loop {
        let mut ok = 0;
        for i in 0..mc.node_count() {
            ok += status(mc, i).await.is_ok() as usize;
        }
        if ok == mc.node_count() {
            return Ok(());
        }
        if t.elapsed() > Duration::from_secs(60) {
            bail!("only {ok} nodes answer after 60 s");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The loadgen workload, as a scenario sets it.
#[derive(Clone)]
pub struct Load {
    pub stream: String,
    pub producers: usize,
    pub symbols: usize,
    pub batch: usize,
    /// Events per second over all producers (0: as fast as acks allow).
    pub rate: f64,
    pub in_flight: usize,
    pub dup_rate: f64,
    pub duration: Duration,
    /// The node every producer tries first (default: the stream's preferred node, as the SDK).
    pub preferred: Option<usize>,
    pub empty_ids: bool,
    pub big_every: u64,
    pub copies: u8,
    /// Writer names: `<prefix><n>`.
    pub prefix: String,
    /// Producer names the events carry (default: the writer's): two loads with the same names
    /// send the same events, as two producer replicas do.
    pub gen_prefix: Option<String>,
}

impl Default for Load {
    fn default() -> Load {
        Load {
            stream: "raw.bench.trades".into(),
            producers: 8,
            symbols: 3,
            batch: 50,
            rate: 20_000.0,
            in_flight: 8,
            dup_rate: 0.0,
            duration: Duration::from_secs(10),
            preferred: None,
            empty_ids: false,
            big_every: 0,
            copies: 2,
            prefix: "live-producer-bench-".into(),
            gen_prefix: None,
        }
    }
}

/// What the producers sent and were told.
#[derive(Default, Debug)]
pub struct Sent {
    pub sent: HashSet<String>,
    pub acked: HashSet<String>,
    pub acked_events: u64,
    /// Resent events inside acknowledged batches.
    pub resends: u64,
    pub failed_batches: u64,
    pub errors: Vec<String>,
    pub lat_ms: Vec<f64>,
    pub secs: f64,
}

impl Sent {
    pub fn merge(&mut self, o: Sent) {
        self.sent.extend(o.sent);
        self.acked.extend(o.acked);
        self.acked_events += o.acked_events;
        self.resends += o.resends;
        self.failed_batches += o.failed_batches;
        self.errors.extend(o.errors);
        self.lat_ms.extend(o.lat_ms);
        self.secs = self.secs.max(o.secs);
    }

    pub fn metrics(&mut self) -> serde_json::Value {
        let secs = self.secs.max(1e-9);
        serde_json::json!({
            "acked_events": self.acked_events, "acked_per_sec": self.acked_events as f64 / secs,
            "resends_acked": self.resends, "failed_batches": self.failed_batches,
            "ack_p50_ms": quantile(&mut self.lat_ms, 0.5), "ack_p99_ms": quantile(&mut self.lat_ms, 0.99),
            "ack_max_ms": quantile(&mut self.lat_ms, 1.0), "errors": self.errors.iter().take(3).collect::<Vec<_>>(),
        })
    }
}

pub fn the_preferred(stream: &str) -> usize {
    (xxhash_rust::xxh3::xxh3_64(stream.as_bytes()) % 3) as usize
}

pub fn encode(log: &LogId, seq: u64, evs: &[Ev]) -> Bytes {
    let keys: Vec<Vec<u8>> = evs.iter().map(|e| dedup_key(&e.id)).collect();
    let refs: Vec<(u64, &[u8], &[u8])> = evs
        .iter()
        .zip(&keys)
        .map(|(e, k)| (precog_client::now_ms() * 1000, &k[..], &e.body[..]))
        .collect();
    Batch::encode_compressed(log, seq, &refs).unwrap()
}

/// Runs the workload for `l.duration`; each producer owns a writer log.
pub async fn run(mc: Arc<MeshClient>, l: Load) -> Sent {
    let start = Instant::now();
    let end = start + l.duration;
    let mut tasks = vec![];
    for p in 0..l.producers {
        let (mc, l) = (mc.clone(), l.clone());
        tasks.push(tokio::spawn(async move {
            let writer = format!("{}{p}", l.prefix);
            let gen_name = l
                .gen_prefix
                .as_ref()
                .map_or(writer.clone(), |g| format!("{g}{p}"));
            let mut g = Gen::new(&gen_name, l.symbols, l.dup_rate);
            g.empty_ids = l.empty_ids;
            g.big_every = l.big_every;
            let log = LogId {
                stream: l.stream.clone(),
                writer,
                epoch: precog_client::now_ms() * 1000 + p as u64,
            };
            let preferred = l.preferred.unwrap_or_else(|| the_preferred(&l.stream));
            let pace = if l.rate > 0.0 {
                Duration::from_secs_f64(l.batch as f64 * l.producers as f64 / l.rate)
            } else {
                Duration::ZERO
            };
            let (mut seq, mut next) = (0u64, Instant::now());
            let mut out = Sent::default();
            let mut inflight = tokio::task::JoinSet::new();
            while Instant::now() < end {
                if l.rate > 0.0 {
                    tokio::time::sleep_until(next.into()).await;
                    next += pace;
                }
                let mut evs = Vec::with_capacity(l.batch);
                let mut resends = 0;
                for _ in 0..l.batch {
                    let (e, r) = g.next();
                    resends += r as u64;
                    out.sent.insert(e.uid.clone());
                    evs.push(e);
                }
                let b = encode(&log, seq, &evs);
                seq += evs.len() as u64;
                while inflight.len() >= l.in_flight {
                    absorb(&mut out, inflight.join_next().await.unwrap().unwrap());
                }
                let (mc, copies) = (mc.clone(), l.copies);
                inflight.spawn(async move {
                    let t = Instant::now();
                    let r = mc.append(preferred, &b, copies).await.map(|r| r.ack);
                    (r, evs, resends, t.elapsed().as_secs_f64() * 1000.0)
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
        all.merge(t.await.unwrap());
    }
    all.secs = l.duration.as_secs_f64();
    all
}

type Done = (Result<Ack>, Vec<Ev>, u64, f64);

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

/// Every batch of `stream` node `i` holds, as records in its read order.
pub async fn read_node(mc: &MeshClient, i: usize, stream: &str) -> Result<Vec<Rec>> {
    let mut cursor = precog_client::Cursor::new();
    let mut out = vec![];
    loop {
        let req = Control::ReadCursor {
            stream: stream.into(),
            cursor: precog_mesh::store::cursor_to_wire(&cursor, stream),
            max_bytes: 4 << 20,
        };
        let f = mc
            .call(i, Kind::Control, Bytes::from(serde_json::to_vec(&req)?))
            .await
            .with_context(|| format!("read node {i}"))?;
        anyhow::ensure!(f.kind == Kind::Batches, "node {i}: {:?}", f.kind);
        let mut body = f.body;
        if body.is_empty() {
            return Ok(out);
        }
        while body.remaining() >= 4 {
            let len = body.get_u32_le() as usize;
            let b = Batch::parse(body.split_to(len))?;
            let log = format!("{}#{}", b.id.log.writer, b.id.log.epoch);
            for e in b.try_events()? {
                let uid = crate::check::uid_of(&e.payload).unwrap_or("?").to_string();
                out.push(Rec {
                    log: log.clone(),
                    pos: e.seq,
                    uid,
                });
            }
            cursor.insert(b.id.log.clone(), b.end_seq());
        }
    }
}

/// Repairs until every node holds the same records of `streams` (or `limit` passes).
pub async fn converge(
    mc: &MeshClient,
    streams: &[&str],
    limit: Duration,
) -> Result<Vec<Vec<Vec<Rec>>>> {
    // repair until every node reports the same frontiers for these streams (cheap), then read
    let t = Instant::now();
    let frontiers = |s: &Status| {
        let mut f: Vec<_> = s
            .logs
            .iter()
            .filter(|f| streams.contains(&f.log.stream.as_str()))
            .map(|f| (f.log.clone(), f.contiguous, f.high))
            .collect();
        f.sort();
        f
    };
    let mut last = None;
    while t.elapsed() < limit {
        for i in 0..3 {
            let _ = mc.control(i, &Control::Repair).await;
        }
        let mut per_node = vec![];
        for i in 0..3 {
            if let Ok(s) = status(mc, i).await {
                per_node.push(frontiers(&s));
            }
        }
        let same = per_node.len() == 3 && per_node[0] == per_node[1] && per_node[1] == per_node[2];
        // equal twice in a row: nothing was still in flight
        if same && last.as_ref() == Some(&per_node[0]) {
            break;
        }
        last = same.then(|| per_node.swap_remove(0));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let mut out = vec![];
    for s in streams {
        let mut per_node = vec![];
        for i in 0..3 {
            per_node.push(read_node(mc, i, s).await?);
        }
        out.push(per_node);
    }
    Ok(out)
}

pub async fn dedup_dropped(mc: &MeshClient) -> u64 {
    let mut n = 0;
    for i in 0..3 {
        n += status(mc, i).await.map_or(0, |s| s.dedup_dropped);
    }
    n
}
