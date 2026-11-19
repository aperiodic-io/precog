//! A mesh node: accepts appends for *any* writer log, stores them in its
//! group-commit WAL, replicates directly to peers in other failure domains
//! and acknowledges once `copies` distinct domains hold a durable copy.
//!
//! There is no leader and no per-partition consensus on the data path:
//! every writer log has exactly one writer, so the order inside a log is fixed
//! by the writer's sequence numbers, and logs never have to be ordered relative
//! to each other at write time. Readers merge logs by event time.

use crate::dedup::{self, Dedup};
use crate::proto::*;
use crate::rpc::{BoxRead, BoxWrite, RpcClient, read_frame, write_loop};
use crate::store::{Reserve, State, Store};
use crate::transport::Addr;
use crate::wal::Wal;
use anyhow::{Context, Result, anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc, oneshot};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerConfig {
    pub id: u32,
    pub domain: String,
    pub addr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub id: u32,
    pub domain: String,
    pub data_dir: PathBuf,
    pub token: String,
    pub listen_tcp: Option<SocketAddr>,
    pub peers: Vec<PeerConfig>,
    /// Copies the node tries to create for every append (including its own).
    #[serde(default = "three")]
    pub target_copies: u8,
    #[serde(default = "rpc_ms")]
    pub rpc_timeout_ms: u64,
    #[serde(default = "repair_ms")]
    pub repair_interval_ms: u64,
    /// Segment file size of the node's log (header page included).
    #[serde(default = "segment_size")]
    pub segment_size: u64,
    /// Keep at most this many bytes of log (oldest sealed segments go first).
    #[serde(default)]
    pub retention_bytes: Option<u64>,
    /// Drop sealed segments whose newest batch is older than this.
    #[serde(default)]
    pub retention_ms: Option<u64>,
    /// How often retention runs.
    #[serde(default = "retention_interval_ms")]
    pub retention_interval_ms: u64,
    /// Recent batches kept in memory for subscribers.
    #[serde(default = "tail_cache_bytes")]
    pub tail_cache_bytes: usize,
    /// Drop an event whose dedup key this node saw within this window (live-ingestion: 5
    /// minutes); `None` turns dedup off. Events without a key are never dropped.
    #[serde(default = "dedup_window_ms")]
    pub dedup_window_ms: Option<u64>,
    /// At most this many dedup ids (~15 bytes each); past it the window shortens.
    #[serde(default = "dedup_max_entries")]
    pub dedup_max_entries: usize,
    /// Fault injection for tests.
    #[serde(skip)]
    pub storage_failpoints: precog_storage::Failpoints,
}

impl NodeConfig {
    /// A node with no peers and default settings.
    pub fn single(id: u32, domain: &str, data_dir: PathBuf, token: String) -> NodeConfig {
        NodeConfig {
            id,
            domain: domain.into(),
            data_dir,
            token,
            listen_tcp: None,
            peers: vec![],
            target_copies: three(),
            rpc_timeout_ms: rpc_ms(),
            repair_interval_ms: repair_ms(),
            segment_size: segment_size(),
            retention_bytes: None,
            retention_ms: None,
            retention_interval_ms: retention_interval_ms(),
            dedup_window_ms: dedup_window_ms(),
            dedup_max_entries: dedup_max_entries(),
            tail_cache_bytes: tail_cache_bytes(),
            storage_failpoints: Default::default(),
        }
    }

    fn storage(&self) -> precog_storage::Config {
        let mut c = precog_storage::Config::new(self.data_dir.join("log"));
        c.segment_size = self.segment_size;
        c.tail_cache_bytes = self.tail_cache_bytes;
        c.failpoints = self.storage_failpoints.clone();
        c
    }
}

fn segment_size() -> u64 {
    1 << 30
}
fn dedup_window_ms() -> Option<u64> {
    Some(300_000)
}
fn dedup_max_entries() -> usize {
    16_000_000
}
fn retention_interval_ms() -> u64 {
    10_000
}
fn tail_cache_bytes() -> usize {
    64 << 20
}
fn three() -> u8 {
    3
}
fn rpc_ms() -> u64 {
    1000
}
fn repair_ms() -> u64 {
    1000
}

struct Peer {
    cfg: PeerConfig,
    addr: Addr,
    link: parking_lot::Mutex<Option<Arc<RpcClient>>>,
    connecting: tokio::sync::Mutex<()>,
    down_until: parking_lot::Mutex<Option<Instant>>,
}

pub struct Node {
    pub cfg: NodeConfig,
    pub store: Store,
    wal: Wal,
    peers: Vec<Arc<Peer>>,
    repair_cursor: parking_lot::Mutex<HashMap<u32, u64>>,
    repair_lock: tokio::sync::Mutex<()>,
    inflight: Semaphore,
    /// Held while a client batch is deduplicated and queued to the log, so ids are claimed in
    /// log order: a batch whose duplicates were dropped is durable only after the batches that
    /// claimed them (the log commits in order and fences on any write error).
    dedup: parking_lot::Mutex<Option<Dedup>>,
    dedup_dropped: AtomicU64,
}

/// The digests of a batch's keyed events, with their positions.
fn digests(batch: &Batch) -> Vec<(usize, u64)> {
    let seed = dedup::stream_seed(&batch.id.log.stream);
    batch
        .keys()
        .enumerate()
        .filter_map(|(i, k)| Some((i, dedup::digest(seed, k?))))
        .collect()
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A batch queued to the log by `begin_local`; `claimed`: it took dedup ids.
struct Queued {
    rx: oneshot::Receiver<Result<u64, String>>,
    claimed: bool,
}

enum Local {
    Stored,
    Duplicate,
    Conflict(String),
}

#[derive(Clone, Copy, Debug)]
enum Ident {
    Client,
    Peer(u32),
}

impl Node {
    pub async fn open(cfg: NodeConfig) -> Result<Arc<Node>> {
        anyhow::ensure!(cfg.id < 32, "node id must be < 32");
        anyhow::ensure!(cfg.peers.len() <= 31, "too many peers");
        std::fs::create_dir_all(&cfg.data_dir)?;
        let (wal, entries) = Wal::open(cfg.storage())?;
        let n = entries.len();
        let mut dedup = cfg
            .dedup_window_ms
            .map(|w| Dedup::new(w.div_ceil(1000).max(1), 3, cfg.dedup_max_entries));
        // the last window of the log rebuilds the index (bounded: the window, not the log)
        let recent: Vec<_> = match cfg.dedup_window_ms {
            Some(w) => {
                let since = (now_s() * 1000).saturating_sub(w);
                entries
                    .iter()
                    .filter(|e| e.meta.time_ms >= since)
                    .map(|e| e.lsn)
                    .collect()
            }
            None => vec![],
        };
        let store = Store::load(wal.reader.clone(), entries)?;
        if let Some(d) = dedup.as_mut() {
            for lsn in recent {
                if let Some(r) = wal.reader.read(lsn)? {
                    let b = Batch::parse(Bytes::copy_from_slice(&r.payload))?;
                    let ds: Vec<u64> = digests(&b).into_iter().map(|(_, h)| h).collect();
                    d.record(&ds, now_s());
                }
            }
        }
        tracing::info!(node = cfg.id, batches = n, "recovered log");
        let mut peers = Vec::new();
        for p in &cfg.peers {
            anyhow::ensure!(p.id != cfg.id && p.id < 32, "bad peer id {}", p.id);
            let addr: Addr = p.addr.parse()?;
            peers.push(Arc::new(Peer {
                cfg: p.clone(),
                addr,
                link: Default::default(),
                connecting: Default::default(),
                down_until: Default::default(),
            }));
        }
        let node = Arc::new(Node {
            cfg,
            store,
            wal,
            peers,
            repair_cursor: Default::default(),
            repair_lock: Default::default(),
            inflight: Semaphore::new(16_384),
            dedup: parking_lot::Mutex::new(dedup),
            dedup_dropped: AtomicU64::new(0),
        });
        Ok(node)
    }

    /// Opens the node and serves until the process is stopped.
    pub async fn run(cfg: NodeConfig) -> Result<()> {
        let node = Node::open(cfg).await?;
        node.spawn_listeners().await?;
        let n = node.clone();
        tokio::spawn(async move {
            let every = Duration::from_millis(n.cfg.repair_interval_ms.max(50));
            loop {
                tokio::time::sleep(every).await;
                let r = n.repair_pass().await;
                if r.0 > 0 {
                    tracing::info!(fetched = r.0, "repair pulled missing batches");
                }
            }
        });
        let n = node.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(n.cfg.retention_interval_ms.max(10)))
                    .await;
                if let Err(e) = n.apply_retention().await {
                    tracing::error!("retention: {e}");
                }
            }
        });
        // A storage fault fences the log (fsyncgate): restart and recover rather than run on.
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if node.is_fatal() {
                tracing::error!("storage fault: exiting so the node restarts and recovers");
                std::process::exit(70);
            }
        }
    }

    pub fn status(&self) -> Status {
        let (dedup_entries, dedup_window_s) = self
            .dedup
            .lock()
            .as_ref()
            .map_or((0, 0), |d| (d.len() as u64, d.effective_window_s(now_s())));
        use std::sync::atomic::Ordering::Relaxed;
        let (batches, events, bytes) = self.store.totals();
        let w = &self.wal.stats;
        Status {
            node_id: self.cfg.id,
            domain: self.cfg.domain.clone(),
            batches,
            events,
            bytes,
            wal_bytes: w.bytes.load(Relaxed),
            fsyncs: w.syncs.load(Relaxed),
            segments: w.segments.load(Relaxed),
            recovery_scanned_bytes: w.recovery_scanned_bytes.load(Relaxed),
            dedup_dropped: self.dedup_dropped.load(Relaxed),
            dedup_entries,
            dedup_window_s,
            logs: self.store.frontiers(None),
        }
    }

    /// The storage layer hit a fatal error; nothing more is acknowledged until restart.
    pub fn is_fatal(&self) -> bool {
        self.wal.fatal.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Applies the configured retention to the log and forgets trimmed batches.
    pub async fn apply_retention(&self) -> Result<()> {
        if self.cfg.retention_bytes.is_none() && self.cfg.retention_ms.is_none() {
            return Ok(());
        }
        let floor = self
            .wal
            .retain(self.cfg.retention_bytes, self.cfg.retention_ms)
            .await?;
        self.store.trim(floor);
        Ok(())
    }

    pub async fn spawn_listeners(self: &Arc<Self>) -> Result<()> {
        if let Some(addr) = self.cfg.listen_tcp {
            let l = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind {addr}"))?;
            tracing::info!(node = self.cfg.id, "tcp listening on {addr}");
            let node = self.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((s, _)) = l.accept().await else {
                        continue;
                    };
                    let _ = s.set_nodelay(true);
                    let (r, w) = s.into_split();
                    let node = node.clone();
                    tokio::spawn(async move { node.serve_conn(Box::new(r), Box::new(w)).await });
                }
            });
        }
        Ok(())
    }

    async fn serve_conn(self: Arc<Self>, mut r: BoxRead, w: BoxWrite) {
        let mut buf = BytesMut::new();
        let hello = match tokio::time::timeout(Duration::from_secs(5), read_frame(&mut r, &mut buf))
            .await
        {
            Ok(Ok(Some(f))) if f.kind == Kind::Hello => f,
            _ => return,
        };
        let ident = match self.authenticate(&hello.body) {
            Ok(i) => i,
            Err(e) => {
                tracing::warn!("rejected connection: {e}");
                return;
            }
        };
        let (tx, rx) = mpsc::channel::<Frame>(4096);
        tokio::spawn(async move {
            let _ = write_loop(w, rx).await;
        });
        let _ = tx
            .send(Frame::new(Kind::Ack, hello.req_id, Bytes::new()))
            .await;
        let per_conn = Arc::new(Semaphore::new(4096));
        loop {
            let f = match read_frame(&mut r, &mut buf).await {
                Ok(Some(f)) => f,
                _ => break,
            };
            if f.kind == Kind::Subscribe {
                tokio::spawn(self.clone().push_loop(f, tx.clone()));
                continue;
            }
            let Ok(permit) = per_conn.clone().acquire_owned().await else {
                break;
            };
            let node = self.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let id = f.req_id;
                let resp = match node.handle(f, ident).await {
                    Ok(r) => r,
                    Err(e) => Frame::new(Kind::Error, id, Bytes::from(e.to_string())),
                };
                let _ = tx.send(resp).await;
                drop(permit);
            });
        }
    }

    /// Live feed: pushes every batch that becomes durable on this node, in
    /// local durable order, starting at the oldest one or at "now". Each
    /// subscription keeps its own cursor into the node's log, so a slow
    /// subscriber only slows its own connection (bounded channel + transport
    /// flow control) and never drops data or blocks writers.
    async fn push_loop(self: Arc<Self>, req: Frame, tx: mpsc::Sender<Frame>) {
        let sub: Subscribe = match serde_json::from_slice(&req.body) {
            Ok(s) => s,
            Err(e) => {
                let _ = tx
                    .send(Frame::new(
                        Kind::Error,
                        req.req_id,
                        Bytes::from(e.to_string()),
                    ))
                    .await;
                return;
            }
        };
        // A live feed sends each log in sequence from where it stood when the feed began (a
        // log new to this node: from its start), holding a batch that lands before its
        // predecessor: the client joins a log at the first batch it sees.
        let (mut pos, mut live) = if sub.from_start {
            (0, None)
        } else {
            let (pos, next) = self.store.live_start(sub.stream.as_deref());
            (
                pos,
                Some((pos, next, HashMap::<LogId, BTreeMap<u64, Batch>>::new())),
            )
        };
        loop {
            let notified = self.store.durable_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable(); // register before checking: no lost wakeups
            let (batches, next) = self.store.tail(pos, sub.stream.as_deref(), 1 << 20);
            pos = next;
            if batches.is_empty() {
                tokio::select! {
                    _ = notified => {}
                    _ = tx.closed() => return,
                }
                continue;
            }
            for b in batches {
                let out = match live.as_mut() {
                    None => vec![b],
                    Some((start, next, held)) => self.in_log_order(*start, next, held, b),
                };
                for b in out {
                    if tx
                        .send(Frame::new(Kind::Push, req.req_id, b.bytes))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }

    /// A live feed's batches in log order: `b` and whatever it releases, or nothing while its
    /// predecessor is missing. A batch already below the feed's start was never due.
    fn in_log_order(
        &self,
        start: precog_storage::Lsn,
        next: &mut HashMap<LogId, u64>,
        held: &mut HashMap<LogId, BTreeMap<u64, Batch>>,
        b: Batch,
    ) -> Vec<Batch> {
        let log = b.id.log.clone();
        let n = next.entry(log.clone()).or_insert(0);
        match b.id.first_seq.cmp(n) {
            std::cmp::Ordering::Greater => {
                held.entry(log).or_default().insert(b.id.first_seq, b);
                return vec![];
            }
            std::cmp::Ordering::Less => return vec![], // below where the feed joined the log
            std::cmp::Ordering::Equal => {}
        }
        *n = b.end_seq();
        let mut out = vec![b];
        let h = held.entry(log.clone()).or_default();
        loop {
            if let Some(end) = self.store.durable_below(&log, *n, start) {
                *n = end;
            } else if let Some(b) = h.remove(n) {
                *n = b.end_seq();
                out.push(b);
            } else {
                break;
            }
        }
        if h.is_empty() {
            held.remove(&log);
        }
        out
    }

    fn authenticate(&self, body: &[u8]) -> Result<Ident> {
        let h: Hello = serde_json::from_slice(body)?;
        if !constant_time_eq(h.token.as_bytes(), self.cfg.token.as_bytes()) {
            bail!("bad token");
        }
        match h.node_id {
            None => Ok(Ident::Client),
            Some(id) => {
                // A token holder may claim any configured peer id.
                anyhow::ensure!(
                    self.peers.iter().any(|p| p.cfg.id == id),
                    "unknown peer {id}"
                );
                Ok(Ident::Peer(id))
            }
        }
    }

    async fn handle(self: &Arc<Self>, f: Frame, ident: Ident) -> Result<Frame> {
        let id = f.req_id;
        match f.kind {
            Kind::Append => {
                let (copies, batch) = decode_append(f.body)?;
                let ack = self.append(copies, batch).await;
                Ok(Frame::new(Kind::Ack, id, ack.encode()))
            }
            Kind::Replicate => {
                let Ident::Peer(pid) = ident else {
                    bail!("replicate from non-peer connection")
                };
                let (origin, batch) = decode_replicate(f.body)?;
                anyhow::ensure!(
                    origin == pid,
                    "replicate origin {origin} != authenticated peer {pid}"
                );
                let ack = self.replicate(batch).await;
                Ok(Frame::new(Kind::Ack, id, ack.encode()))
            }
            Kind::Control => {
                let c: Control = serde_json::from_slice(&f.body)?;
                if let Control::Fetch { ids } = &c {
                    let mut out = BytesMut::new();
                    for bid in ids {
                        if let Some(b) = self.store.get(bid) {
                            if out.len() + b.bytes.len() > MAX_FRAME_BYTES - 1024 {
                                break;
                            }
                            out.put_u32_le(b.bytes.len() as u32);
                            out.put_slice(&b.bytes);
                        }
                    }
                    return Ok(Frame::new(Kind::Batches, id, out.freeze()));
                }
                let resp = self.control(c, ident).await?;
                Ok(Frame::new(
                    Kind::ControlResp,
                    id,
                    Bytes::from(serde_json::to_vec(&resp)?),
                ))
            }
            k => bail!("unexpected frame {k:?}"),
        }
    }

    async fn control(self: &Arc<Self>, c: Control, _ident: Ident) -> Result<ControlResp> {
        Ok(match c {
            Control::Status => ControlResp::Status(self.status()),
            Control::Read {
                stream,
                from_time_us,
                limit,
            } => {
                let (events, coverage) =
                    self.store
                        .read(&stream, from_time_us, limit.min(10_000) as usize);
                ControlResp::Read { events, coverage }
            }
            Control::Inventory { after, limit } => {
                let (items, next, more) = self.store.inventory(after, limit.min(16_384));
                ControlResp::Inventory { items, next, more }
            }
            Control::Repair => {
                let (fetched, errors) = self.repair_pass().await;
                ControlResp::Repair { fetched, errors }
            }
            Control::Fetch { .. } => unreachable!(),
        })
    }

    /// Writes the batch to the local WAL unless an identical copy exists.
    /// A client batch: if it is new, drops its duplicate events and queues it to the log, both
    /// under the dedup lock (ids are claimed in log order). Returns the batch to store and
    /// replicate, and its queued write; `None` leaves it to `store_local` (a retry, a conflict).
    fn begin_local(&self, batch: Batch) -> Result<(Batch, Option<Queued>)> {
        let mut g = self.dedup.lock();
        if !matches!(self.store.reserve(&batch), Reserve::New) {
            return Ok((batch, None)); // reserve only peeked: store_local reserves again
        }
        let mut batch = batch;
        let mut claimed = false;
        if let Some(d) = g.as_mut() {
            let ds = digests(&batch);
            if !ds.is_empty() {
                let hs: Vec<u64> = ds.iter().map(|&(_, h)| h).collect();
                let mut dup = vec![];
                d.check(&hs, now_s(), &mut dup);
                claimed = dup.contains(&false);
                if dup.contains(&true) {
                    let n = dup.iter().filter(|&&x| x).count();
                    let mut drop = vec![false; batch.count as usize];
                    for (&(i, _), &x) in ds.iter().zip(&dup) {
                        drop[i] = x;
                    }
                    let ident = batch.id.clone();
                    batch = Batch::parse(batch.without(&drop)?)?;
                    debug_assert_eq!(batch.id, ident);
                    self.dedup_dropped.fetch_add(n as u64, Ordering::Relaxed);
                }
            }
        }
        match self.wal.submit(batch.clone()) {
            Ok(rx) => Ok((batch, Some(Queued { rx, claimed }))),
            Err(e) => {
                self.store.mark_failed(&batch.id);
                if claimed {
                    self.wal.fatal.store(true, Ordering::Release);
                }
                Err(e)
            }
        }
    }

    /// Waits for `begin_local`'s write, or stores the batch the ordinary way.
    async fn finish_local(&self, batch: &Batch, submitted: Option<Queued>) -> Result<Local> {
        let Some(Queued { rx, claimed }) = submitted else {
            return self.store_local(batch).await;
        };
        match Wal::durable(rx).await {
            Ok(lsn) => {
                self.store.mark_durable(&batch.id, lsn);
                Ok(Local::Stored)
            }
            Err(e) => {
                self.store.mark_failed(&batch.id);
                if claimed {
                    // its ids stay claimed in the index: fence, and recover the index from the log
                    self.wal.fatal.store(true, Ordering::Release);
                }
                Err(e)
            }
        }
    }

    async fn store_local(&self, batch: &Batch) -> Result<Local> {
        loop {
            match self.store.reserve(batch) {
                Reserve::New => {
                    return match self.wal.append(batch.clone()).await {
                        Ok(lsn) => {
                            self.store.mark_durable(&batch.id, lsn);
                            Ok(Local::Stored)
                        }
                        Err(e) => {
                            self.store.mark_failed(&batch.id);
                            Err(e)
                        }
                    };
                }
                Reserve::Duplicate(mut rx) => loop {
                    let s = *rx.borrow_and_update();
                    match s {
                        State::Durable => return Ok(Local::Duplicate),
                        State::Failed => break, // the earlier attempt failed: try again
                        State::Pending => {
                            if rx.changed().await.is_err() {
                                break;
                            }
                        }
                    }
                },
                Reserve::Conflict(e) => return Ok(Local::Conflict(e)),
                Reserve::Expired => {
                    return Ok(Local::Conflict(format!(
                        "{:?} seq {} is below the retention floor",
                        batch.id.log, batch.id.first_seq
                    )));
                }
            }
        }
    }

    async fn replicate(&self, bytes: Bytes) -> Ack {
        let crc = if bytes.len() >= 4 {
            batch_crc(&bytes)
        } else {
            0
        };
        let mut ack = Ack {
            status: AckStatus::Rejected,
            copies: 0,
            nodes: 0,
            crc,
        };
        let Ok(batch) = Batch::parse(bytes) else {
            return ack;
        };
        // the origin decided; record the ids it kept, so this node dedups too after a failover
        if let Some(d) = self.dedup.lock().as_mut() {
            let ds: Vec<u64> = digests(&batch).into_iter().map(|(_, h)| h).collect();
            d.record(&ds, now_s());
        }
        ack.crc = batch.ident; // the origin compares identities, not the rewritten bytes' CRC
        match self.store_local(&batch).await {
            Ok(Local::Stored) => ack.status = AckStatus::Ok,
            Ok(Local::Duplicate) => ack.status = AckStatus::Duplicate,
            Ok(Local::Conflict(e)) => {
                tracing::warn!("replica conflict: {e}");
                ack.status = AckStatus::Conflict;
                return ack;
            }
            Err(e) => {
                tracing::warn!("replica store failed: {e}");
                ack.status = AckStatus::Unknown;
                return ack;
            }
        }
        ack.copies = 1;
        ack.nodes = 1 << self.cfg.id;
        ack
    }

    /// One peer per distinct failure domain other than ours, up to
    /// `target_copies - 1` peers.
    fn targets(&self) -> Vec<Arc<Peer>> {
        let mut seen = HashSet::from([self.cfg.domain.clone()]);
        let mut out = Vec::new();
        for p in &self.peers {
            if out.len() + 1 >= self.cfg.target_copies as usize {
                break;
            }
            if seen.insert(p.cfg.domain.clone()) {
                out.push(p.clone());
            }
        }
        out
    }

    pub async fn append(self: &Arc<Self>, copies: u8, bytes: Bytes) -> Ack {
        let crc = if bytes.len() >= 4 {
            batch_crc(&bytes)
        } else {
            0
        };
        let reject = Ack {
            status: AckStatus::Rejected,
            copies: 0,
            nodes: 0,
            crc,
        };
        let Ok(_permit) = self.inflight.acquire().await else {
            return reject;
        };
        let Ok(batch) = Batch::parse(bytes) else {
            return reject;
        };
        let targets = self.targets();
        if copies == 0 || copies as usize > targets.len() + 1 {
            return reject; // cannot possibly satisfy: no side effects yet
        }
        let (batch, submitted) = match self.begin_local(batch) {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("local store failed: {e}");
                return Ack {
                    status: AckStatus::Unknown,
                    ..reject
                };
            }
        };
        let (tx, rx) = oneshot::channel::<Ack>();
        let node = self.clone();
        tokio::spawn(async move {
            enum R {
                Local(Result<Local>),
                Remote(u32, String, Result<Ack>),
            }
            let mut futs = FuturesUnordered::new();
            {
                let node = node.clone();
                let batch = batch.clone();
                futs.push(Box::pin(
                    async move { R::Local(node.finish_local(&batch, submitted).await) },
                )
                    as std::pin::Pin<Box<dyn std::future::Future<Output = R> + Send>>);
            }
            let body = encode_replicate(node.cfg.id, &batch.bytes);
            for p in targets {
                let node = node.clone();
                let body = body.clone();
                futs.push(Box::pin(async move {
                    let r = node
                        .peer_call(&p, Kind::Replicate, body)
                        .await
                        .and_then(|f| Ack::decode(f.body));
                    R::Remote(p.cfg.id, p.cfg.domain.clone(), r)
                }));
            }
            let mut domains = HashSet::new();
            let mut nodes = 0u32;
            let mut tx = Some(tx);
            while let Some(r) = futs.next().await {
                match r {
                    R::Local(Ok(Local::Stored | Local::Duplicate)) => {
                        domains.insert(node.cfg.domain.clone());
                        nodes |= 1 << node.cfg.id;
                    }
                    R::Local(Ok(Local::Conflict(e))) => {
                        tracing::debug!("append conflict: {e}");
                        if let Some(tx) = tx.take() {
                            let _ = tx.send(Ack {
                                status: AckStatus::Conflict,
                                copies: 0,
                                nodes: 0,
                                crc,
                            });
                        }
                    }
                    R::Local(Err(e)) => tracing::warn!("local store failed: {e}"),
                    R::Remote(pid, domain, Ok(a))
                        if a.is_success() && a.crc == crc && a.nodes == 1 << pid =>
                    {
                        domains.insert(domain);
                        nodes |= 1 << pid;
                    }
                    R::Remote(pid, _, Ok(a)) => {
                        tracing::debug!("peer {pid} did not confirm: {a:?}")
                    }
                    R::Remote(pid, _, Err(e)) => {
                        tracing::debug!("peer {pid} replicate failed: {e}")
                    }
                }
                if domains.len() >= copies as usize
                    && let Some(tx) = tx.take()
                {
                    // Early completion; remaining copies finish in this task.
                    let _ = tx.send(Ack {
                        status: AckStatus::Ok,
                        copies: domains.len() as u8,
                        nodes,
                        crc,
                    });
                }
            }
            if let Some(tx) = tx.take() {
                let _ = tx.send(Ack {
                    status: AckStatus::Unknown,
                    copies: domains.len() as u8,
                    nodes,
                    crc,
                });
            }
        });
        rx.await.unwrap_or(Ack {
            status: AckStatus::Unknown,
            copies: 0,
            nodes: 0,
            crc,
        })
    }

    async fn peer_client(&self, p: &Peer) -> Result<Arc<RpcClient>> {
        if let Some(c) = p.link.lock().as_ref()
            && !c.is_closed()
        {
            return Ok(c.clone());
        }
        let _g = p.connecting.lock().await;
        if let Some(c) = p.link.lock().as_ref()
            && !c.is_closed()
        {
            return Ok(c.clone());
        }
        if let Some(t) = *p.down_until.lock()
            && Instant::now() < t
        {
            bail!("peer {} marked down", p.cfg.id);
        }
        let timeout = Duration::from_millis(self.cfg.rpc_timeout_ms);
        let res: Result<Arc<RpcClient>> = async {
            let c = crate::transport::connect(&p.addr, timeout).await?;
            let hello = Hello {
                token: self.cfg.token.clone(),
                node_id: Some(self.cfg.id),
            };
            c.call(
                Kind::Hello,
                Bytes::from(serde_json::to_vec(&hello)?),
                timeout,
            )
            .await?;
            Ok(Arc::new(c))
        }
        .await;
        match res {
            Ok(c) => {
                *p.link.lock() = Some(c.clone());
                *p.down_until.lock() = None;
                Ok(c)
            }
            Err(e) => {
                *p.down_until.lock() = Some(Instant::now() + Duration::from_millis(250));
                Err(e)
            }
        }
    }

    async fn peer_call(&self, p: &Peer, kind: Kind, body: Bytes) -> Result<Frame> {
        let c = self.peer_client(p).await?;
        let r = c
            .call(kind, body, Duration::from_millis(self.cfg.rpc_timeout_ms))
            .await;
        if r.is_err() && c.is_closed() {
            let mut g = p.link.lock();
            if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) {
                *g = None;
            }
        }
        r
    }

    async fn peer_control(&self, p: &Peer, c: &Control) -> Result<Frame> {
        self.peer_call(p, Kind::Control, Bytes::from(serde_json::to_vec(c)?))
            .await
    }

    /// Anti-entropy: pull every batch a peer made durable since our cursor that
    /// we lack. Peers' inventories are their local WAL order, so after a
    /// restart (cursor 0) this is a full scan, otherwise an incremental tail.
    pub async fn repair_pass(&self) -> (u64, Vec<String>) {
        let _g = self.repair_lock.lock().await;
        let mut fetched = 0;
        let mut errors = Vec::new();
        for p in &self.peers {
            match self.repair_peer(p).await {
                Ok(n) => fetched += n,
                Err(e) => errors.push(format!("peer {}: {e}", p.cfg.id)),
            }
        }
        (fetched, errors)
    }

    async fn repair_peer(&self, p: &Peer) -> Result<u64> {
        let mut fetched = 0;
        for _ in 0..64 {
            let after = *self.repair_cursor.lock().get(&p.cfg.id).unwrap_or(&0);
            let f = self
                .peer_control(p, &Control::Inventory { after, limit: 8192 })
                .await?;
            let ControlResp::Inventory { items, next, more } = serde_json::from_slice(&f.body)?
            else {
                bail!("unexpected inventory response");
            };
            let missing: Vec<BatchId> = items
                .into_iter()
                .filter(|(_, id, _, crc)| !self.store.has(id, *crc))
                .map(|(_, id, _, _)| id)
                .collect();
            for chunk in missing.chunks(512) {
                let mut want: Vec<BatchId> = chunk.to_vec();
                while !want.is_empty() {
                    let f = self
                        .peer_control(p, &Control::Fetch { ids: want.clone() })
                        .await?;
                    anyhow::ensure!(f.kind == Kind::Batches, "unexpected fetch response");
                    let mut got = HashSet::new();
                    let mut body = &f.body[..];
                    while body.len() >= 4 {
                        let n = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
                        let b = Batch::parse(f.body.slice_ref(&body[4..4 + n]))?;
                        body = &body[4 + n..];
                        match self.store_local(&b).await? {
                            Local::Stored => fetched += 1,
                            Local::Duplicate => {}
                            Local::Conflict(e) => {
                                tracing::error!("repair conflict from peer {}: {e}", p.cfg.id)
                            }
                        }
                        got.insert(b.id);
                    }
                    if got.is_empty() {
                        break; // peer no longer serves them (cannot happen without deletion)
                    }
                    want.retain(|id| !got.contains(id));
                }
            }
            self.repair_cursor.lock().insert(p.cfg.id, next);
            if !more {
                break;
            }
        }
        Ok(fetched)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn parse_frame_ack(f: &Frame) -> Result<Ack> {
    if f.kind != Kind::Ack {
        return Err(anyhow!("expected ack, got {:?}", f.kind));
    }
    Ack::decode(f.body.clone())
}
