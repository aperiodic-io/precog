//! A mesh node: accepts appends for *any* writer log, stores them in its
//! group-commit WAL, replicates directly to peers in other failure domains
//! and acknowledges once `copies` distinct domains hold a durable copy.
//!
//! There is no leader and no per-partition consensus on the data path:
//! every writer log has exactly one writer, so the order inside a log is fixed
//! by the writer's sequence numbers, and logs never have to be ordered relative
//! to each other at write time. Readers merge logs by event time.

use crate::dedup::{self, Streams};
use crate::proto::*;
use crate::rpc::{BoxRead, BoxWrite, RpcClient, read_frame, write_loop};
use crate::store::{Cursor, Kept, Reserve, State, Store};
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
    /// Prometheus `/metrics` and `/health` (503 once storage is fenced) over plain HTTP.
    #[serde(default)]
    pub listen_metrics: Option<SocketAddr>,
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
    /// Write caching, as live-ingestion's Redpanda runs it (`write_caching_default`): 0 fsyncs
    /// every group commit before acknowledging; otherwise a batch is acknowledged once written
    /// (on `target_copies` nodes, durability then rests on the replicas) and the fsync follows
    /// within this many ms, or once `flush_bytes` are unsynced.
    #[serde(default)]
    pub flush_delay_ms: u64,
    #[serde(default = "flush_bytes")]
    pub flush_bytes: u64,
    /// Recent batches kept in memory for subscribers.
    #[serde(default = "tail_cache_bytes")]
    pub tail_cache_bytes: usize,
    /// Drop an event whose dedup key this node saw within this window (live-ingestion: 5
    /// minutes); `None` turns dedup off. Events without a key are never dropped.
    #[serde(default = "dedup_window_ms")]
    pub dedup_window_ms: Option<u64>,
    /// At most this many dedup ids per stream (~28 bytes each where the stream is accepted, 8
    /// where it is only replicated); past it the stream's window shortens.
    #[serde(default = "dedup_max_entries")]
    pub dedup_max_entries: usize,
    /// Streams with a retention of their own (topicsync's `retention.bytes` / `retention.ms`);
    /// others keep what the node-level budget leaves.
    #[serde(default)]
    pub streams: HashMap<String, StreamConfig>,
    /// A stream with no appends, reads or subscriptions for this long leaves RAM once none of
    /// its batches is in RAM, and is a summary on disk until it is used again (docs/ARCHITECTURE.md,
    /// "Dormant streams"). Past `dedup_window_ms`, so its dedup ids have expired by then.
    #[serde(default = "dormant_after_ms")]
    pub dormant_after_ms: u64,
    /// What the streams in RAM may cost (their metadata, about): past it the least recently used
    /// ones leave RAM before their idle time.
    #[serde(default = "hot_streams_bytes")]
    pub hot_streams_bytes: u64,
    /// The page cache of the dormant streams' database (`<data_dir>/streams.redb`).
    #[serde(default = "summary_cache_bytes")]
    pub summary_cache_bytes: u64,
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
            listen_metrics: None,
            peers: vec![],
            target_copies: three(),
            rpc_timeout_ms: rpc_ms(),
            repair_interval_ms: repair_ms(),
            segment_size: segment_size(),
            retention_bytes: None,
            retention_ms: None,
            retention_interval_ms: retention_interval_ms(),
            dedup_window_ms: dedup_window_ms(),
            streams: HashMap::new(),
            dormant_after_ms: dormant_after_ms(),
            hot_streams_bytes: hot_streams_bytes(),
            summary_cache_bytes: summary_cache_bytes(),
            dedup_max_entries: dedup_max_entries(),
            tail_cache_bytes: tail_cache_bytes(),
            flush_delay_ms: 0,
            flush_bytes: flush_bytes(),
            storage_failpoints: Default::default(),
        }
    }

    /// This config for member `id` of a cluster whose `peers` list every node, itself included:
    /// takes its id and failure domain, and keeps the others as peers (one ConfigMap for a
    /// whole StatefulSet).
    pub fn for_member(mut self, id: u32) -> Result<NodeConfig> {
        let me = self
            .peers
            .iter()
            .position(|p| p.id == id)
            .with_context(|| format!("node {id} is not in the peer list"))?;
        let me = self.peers.remove(me);
        self.id = id;
        self.domain = me.domain;
        Ok(self)
    }

    fn storage(&self) -> precog_storage::Config {
        let mut c = precog_storage::Config::new(self.data_dir.join("log"));
        c.segment_size = self.segment_size;
        c.tail_cache_bytes = self.tail_cache_bytes;
        c.flush_delay_ms = self.flush_delay_ms;
        c.flush_bytes = self.flush_bytes;
        c.failpoints = self.storage_failpoints.clone();
        c
    }
}

fn flush_bytes() -> u64 {
    256 << 10 // Redpanda's raft_replica_max_pending_flush_bytes
}

fn segment_size() -> u64 {
    1 << 30
}
/// A StatefulSet pod's ordinal from its hostname (`precog-2` -> 2).
pub fn ordinal(hostname: &str) -> Result<u32> {
    let (_, n) = hostname
        .rsplit_once('-')
        .with_context(|| format!("no ordinal in hostname {hostname:?}"))?;
    n.parse()
        .with_context(|| format!("no ordinal in hostname {hostname:?}"))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Keep at most this many bytes of the stream (its oldest batches go first).
    #[serde(default)]
    pub retention_bytes: Option<u64>,
    /// Drop the stream's batches older than this.
    #[serde(default)]
    pub retention_ms: Option<u64>,
    /// Keep the newest event of every key (`cleanup.policy=compact`), through retention and
    /// restarts; the event key is the compaction key, and the stream is never deduplicated.
    #[serde(default)]
    pub compacted: bool,
}

fn dedup_window_ms() -> Option<u64> {
    Some(300_000)
}
fn dedup_max_entries() -> usize {
    8_000_000
}
fn dormant_after_ms() -> u64 {
    600_000
}
fn hot_streams_bytes() -> u64 {
    256 << 20
}
fn summary_cache_bytes() -> u64 {
    64 << 20
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
    repair_lock: tokio::sync::Mutex<()>,
    inflight: Semaphore,
    /// Held while a client batch is deduplicated and queued to the log, so ids are claimed in
    /// log order: a batch whose duplicates were dropped is durable only after the batches that
    /// claimed them (the log commits in order and fences on any write error).
    dedup: parking_lot::Mutex<Option<Streams>>,
    dedup_dropped: AtomicU64,
    /// Compacted streams: key -> its newest event (see `Compacted`).
    compact: parking_lot::Mutex<HashMap<String, Compacted>>,
    /// This node's writer log for consumer-position commits (a new epoch per boot).
    pos_log: LogId,
    pos_seq: AtomicU64,
    /// The event time (us) of this node's last position commit: it never goes back.
    pos_time: AtomicU64,
    /// Held while the cleaner copies batches forward and while a replacement takes a batch's
    /// place, so a copy never carries bytes a replacement superseded.
    clean: tokio::sync::Mutex<()>,
    copied_bytes: AtomicU64,
    copied_batches: AtomicU64,
    /// Batches the node cap cut from streams that still retained them, by stream.
    forced: parking_lot::Mutex<BTreeMap<String, u64>>,
    /// This boot, for peers following its log (`Control::Inventory`; `Store::boot`).
    incarnation: u64,
}

/// Records of a peer's log per inventory request.
const INVENTORY_PAGE: u32 = 16 << 10;

/// Most bytes the cleaner copies forward in one group (one write to the log, as a client group).
const COPY_CHUNK: u64 = 4 << 20;

/// What copying `cost` bytes of records in groups of at most `limit` adds to the log at most: a
/// new segment's header for each group (`groups`: any two groups in a row hold more than
/// `limit`).
fn copy_bytes(cost: u64, limit: u64) -> u64 {
    if cost == 0 {
        return 0;
    }
    cost + precog_storage::SEG_HEADER * (2 * cost / limit + 1)
}

/// Whether copying `cost` bytes in groups of at most `limit` fits in `room` bytes.
fn fits(cost: u64, limit: u64, room: u64) -> bool {
    copy_bytes(cost, limit) <= room
}

/// The kept batches, in order, in groups of at most `limit` bytes (a larger one alone).
fn groups(kept: &[Kept], limit: u64) -> Vec<Vec<u64>> {
    let mut out: Vec<(u64, Vec<u64>)> = vec![];
    for k in kept {
        match out.last_mut() {
            Some((size, g)) if *size + k.cost <= limit => {
                *size += k.cost;
                g.push(k.lsn);
            }
            _ => out.push((k.cost, vec![k.lsn])),
        }
    }
    out.into_iter().map(|g| g.1).collect()
}

/// The internal compacted stream of consumer positions, keyed by group and stream.
pub const POSITIONS: &str = "__positions";

fn position_key(group: &str, stream: &str) -> Vec<u8> {
    [group.as_bytes(), b"\0", stream.as_bytes()].concat()
}

/// Where an event stands in read order: (event time, writer, epoch, seq).
type Order = (u64, String, u64, u64);

/// A compacted stream: key -> (its newest event's place in read order, LSN here, payload).
/// The newest is the last in read order, not the last logged here: replication and repair bring
/// batches to each node in its own order, and every node must pick the same value (spec/consumers).
type Compacted = BTreeMap<Vec<u8>, (Order, u64, Bytes)>;

const SNAP_MAGIC: &[u8; 4] = b"PCS2";
/// Format 1 (no read order: LSN only) still reads, its entries older than any event.
const SNAP_MAGIC_V1: &[u8; 4] = b"PCS1";

/// A compacted stream's snapshot: magic, count, then (key len u16, key, time u64, writer len
/// u16, writer, epoch u64, seq u64, lsn u64, payload len u32, payload) per key, then CRC-32C of
/// everything before it.
fn encode_snapshot(m: &Compacted) -> Vec<u8> {
    let mut b = SNAP_MAGIC.to_vec();
    b.extend_from_slice(&(m.len() as u64).to_le_bytes());
    for (k, ((time, writer, epoch, seq), lsn, p)) in m {
        b.extend_from_slice(&(k.len() as u16).to_le_bytes());
        b.extend_from_slice(k);
        b.extend_from_slice(&time.to_le_bytes());
        b.extend_from_slice(&(writer.len() as u16).to_le_bytes());
        b.extend_from_slice(writer.as_bytes());
        for v in [epoch, seq, lsn] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b.extend_from_slice(&(p.len() as u32).to_le_bytes());
        b.extend_from_slice(p);
    }
    let c = crc32c::crc32c(&b);
    b.extend_from_slice(&c.to_le_bytes());
    b
}

fn decode_snapshot(b: &[u8]) -> Result<Compacted> {
    let fail = || anyhow!("compacted-stream snapshot is damaged or of an unknown version");
    let (body, crc) = b
        .split_at_checked(b.len().checked_sub(4).ok_or_else(fail)?)
        .ok_or_else(fail)?;
    let v1 = body.get(..4) == Some(&SNAP_MAGIC_V1[..]);
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into()?)
        || !(v1 || body.get(..4) == Some(&SNAP_MAGIC[..]))
    {
        return Err(fail());
    }
    let mut c = &body[4..];
    let mut take = |n: usize| -> Result<&[u8]> {
        let (a, rest) = c.split_at_checked(n).ok_or_else(fail)?;
        c = rest;
        Ok(a)
    };
    let n = u64::from_le_bytes(take(8)?.try_into()?);
    let mut m = Compacted::new();
    for _ in 0..n {
        let kl = u16::from_le_bytes(take(2)?.try_into()?) as usize;
        let k = take(kl)?.to_vec();
        let order = if v1 {
            (0, String::new(), 0, 0)
        } else {
            let time = u64::from_le_bytes(take(8)?.try_into()?);
            let wl = u16::from_le_bytes(take(2)?.try_into()?) as usize;
            let writer = String::from_utf8(take(wl)?.to_vec()).map_err(|_| fail())?;
            let epoch = u64::from_le_bytes(take(8)?.try_into()?);
            let seq = u64::from_le_bytes(take(8)?.try_into()?);
            (time, writer, epoch, seq)
        };
        let lsn = u64::from_le_bytes(take(8)?.try_into()?);
        let pl = u32::from_le_bytes(take(4)?.try_into()?) as usize;
        m.insert(k, (order, lsn, Bytes::copy_from_slice(take(pl)?)));
    }
    if !c.is_empty() {
        return Err(fail());
    }
    Ok(m)
}

const FLOORS_MAGIC: &[u8; 4] = b"PCF1";

/// Each stream's own retention floor, as an older node kept them (`<data_dir>/floors`, read only
/// to rebuild its streams' summaries): magic, count, then (name len u16, name, append time u64,
/// first sequence u64, LSN u64) per stream, then CRC-32C of everything before it.
fn decode_floors(b: &[u8]) -> Result<BTreeMap<String, crate::store::Age>> {
    let fail = || anyhow!("retention floors are damaged or of an unknown version");
    let (body, crc) = b
        .split_at_checked(b.len().checked_sub(4).ok_or_else(fail)?)
        .ok_or_else(fail)?;
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into()?)
        || body.get(..4) != Some(&FLOORS_MAGIC[..])
    {
        return Err(fail());
    }
    let mut c = &body[4..];
    let mut take = |n: usize| -> Result<&[u8]> {
        let (a, rest) = c.split_at_checked(n).ok_or_else(fail)?;
        c = rest;
        Ok(a)
    };
    let n = u64::from_le_bytes(take(8)?.try_into()?);
    let mut m = BTreeMap::new();
    for _ in 0..n {
        let nl = u16::from_le_bytes(take(2)?.try_into()?) as usize;
        let name = String::from_utf8(take(nl)?.to_vec()).map_err(|_| fail())?;
        let mut age = [0u64; 3];
        for v in &mut age {
            *v = u64::from_le_bytes(take(8)?.try_into()?);
        }
        m.insert(name, (age[0], age[1], age[2]));
    }
    if !c.is_empty() {
        return Err(fail());
    }
    Ok(m)
}

/// Writes each file in `dir` durably: a temporary file, fsynced, renamed, then the directory.
fn write_durably(dir: &std::path::Path, files: Vec<(PathBuf, Vec<u8>)>) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    for (p, bytes) in files {
        let tmp = p.with_extension("tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &p)?;
    }
    // the renames are durable only once the directory is
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

fn snapshot_path(dir: &std::path::Path, stream: &str) -> PathBuf {
    dir.join("compact")
        .join(format!("{}.snap", hex_encode(stream.as_bytes())))
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

/// Applies a durable batch at `lsn` to a compacted stream's newest values (by read order).
fn apply_compacted(m: &mut Compacted, batch: &Batch, lsn: u64) {
    let kvs = match batch.keyed() {
        Ok(kvs) => kvs,
        Err(e) => {
            // the CRC held, so its producer wrote it this way; the log keeps it as written
            tracing::error!(
                "compacted stream {}: batch {:?} unreadable: {e}",
                batch.id.log.stream,
                batch.id
            );
            return;
        }
    };
    let log = &batch.id.log;
    for (seq, time, k, p) in kvs {
        let order = (time, log.writer.clone(), log.epoch, seq);
        if m.get(&k).is_none_or(|(o, ..)| *o <= order) {
            m.insert(k, (order, lsn, p));
        }
    }
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
        let wal = Wal::open(cfg.storage())?;
        let mut dedup = cfg
            .dedup_window_ms
            .map(|w| Streams::new(w.div_ceil(1000).max(1), 3, cfg.dedup_max_entries));
        let since = cfg
            .dedup_window_ms
            .map(|w| (now_s() * 1000).saturating_sub(w));
        // compacted streams: their snapshots, then whatever the log holds that is newer
        let mut compact: HashMap<String, Compacted> = HashMap::new();
        let compacted = cfg
            .streams
            .iter()
            .filter(|(_, s)| s.compacted)
            .map(|(n, _)| n.as_str());
        for name in compacted.chain([POSITIONS]) {
            let p = snapshot_path(&cfg.data_dir, name);
            let m = match std::fs::read(&p) {
                Ok(b) => decode_snapshot(&b).with_context(|| format!("{}", p.display()))?,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Compacted::new(),
                Err(e) => return Err(e.into()),
            };
            compact.insert(name.to_string(), m);
        }
        // the store: back to its checkpoint, then the log past it (dormant streams stay on disk)
        let db = crate::summary::Db::open(
            &cfg.data_dir.join("streams.redb"),
            cfg.summary_cache_bytes as usize,
        )
        .map_err(anyhow::Error::msg)?;
        let rebuilt = db.checkpoint().map_err(anyhow::Error::msg)?.is_none();
        let store = Store::open(wal.reader.clone(), db)?;
        // an older node's data directory: its streams' floors were in this file
        let p = cfg.data_dir.join("floors");
        if rebuilt && p.exists() {
            let floors = decode_floors(&std::fs::read(&p)?);
            store.restore_floors(&floors.with_context(|| format!("{}", p.display()))?);
        }
        let n = store.totals().0;
        // the compacted streams' replay (bounded: those, not the log)
        for (name, m) in compact.iter_mut() {
            for lsn in store.lsns(name) {
                if let Some(r) = wal.reader.read(lsn)? {
                    let b = Batch::parse(Bytes::copy_from_slice(&r.payload))?;
                    apply_compacted(m, &b, lsn);
                }
            }
        }
        // the dedup window's batches rebuild its index: the segments logged within it
        if let (Some(d), Some(since)) = (dedup.as_mut(), since) {
            // append times never go back: a segment's newest is no older than any before it
            let at = window_start(&wal.segments(), wal.reader.first_lsn(), since);
            // LSNs are dense: each chunk is the next 64k of them
            for from in (at..wal.reader.next_lsn()).step_by(1 << 16) {
                let es = wal.reader.entries(from, 1 << 16)?;
                for e in es.iter().filter(|e| e.meta.time_ms >= since) {
                    if let Some(r) = wal.reader.read(e.lsn)? {
                        let b = Batch::parse(Bytes::copy_from_slice(&r.payload))?;
                        let ds: Vec<u64> = digests(&b).into_iter().map(|(_, h)| h).collect();
                        d.record(&b.id.log.stream, &ds, now_s());
                    }
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
        let cfg_id = cfg.id;
        // this boot, on disk before any peer follows it
        let incarnation = rand::random::<u64>().max(1);
        store.boot(incarnation, wal.reader.next_lsn());
        let p = store.begin_checkpoint();
        let synced = wal.sync().await.map(|_| ());
        store.end_checkpoint(p, synced)?;
        store.set_failpoint(cfg.storage_failpoints.fail_at); // tests: faults past the open
        let node = Arc::new(Node {
            cfg,
            store,
            wal,
            peers,
            repair_lock: Default::default(),
            inflight: Semaphore::new(16_384),
            dedup: parking_lot::Mutex::new(dedup),
            dedup_dropped: AtomicU64::new(0),
            compact: parking_lot::Mutex::new(compact),
            pos_log: LogId {
                stream: POSITIONS.into(),
                writer: format!("node-{cfg_id}"),
                epoch: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_nanos() as u64,
            },
            pos_seq: AtomicU64::new(0),
            pos_time: AtomicU64::new(0),
            clean: Default::default(),
            copied_bytes: AtomicU64::new(0),
            copied_batches: AtomicU64::new(0),
            forced: Default::default(),
            incarnation,
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

    /// The node's status, with every log's frontier (a read of every dormant stream's summary).
    pub fn status(&self) -> Status {
        Status {
            logs: self.store.frontiers(None),
            ..self.counters()
        }
    }

    /// `status` without the frontiers.
    fn counters(&self) -> Status {
        let (dedup_entries, dedup_window_s) = self
            .dedup
            .lock()
            .as_ref()
            .map_or((0, 0), |d| (d.len() as u64, d.effective_window_s(now_s())));
        use std::sync::atomic::Ordering::Relaxed;
        let (batches, events, bytes) = self.store.totals();
        let w = &self.wal.stats;
        let h = self.store.hot_stats();
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
            logs: vec![],
            streams_hot: h.hot,
            streams_dormant: h.dormant,
        }
    }

    /// The storage layer hit a fatal error; nothing more is acknowledged until restart.
    pub fn is_fatal(&self) -> bool {
        self.wal.fatal.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Applies the configured retention to the log and forgets trimmed batches.
    /// Commits a reader group's position in a stream, durably on as many nodes as possible
    /// (up to 2); the newest commit wins.
    pub async fn commit_position(
        self: &Arc<Self>,
        group: &str,
        stream: &str,
        cursor: &Cursor,
    ) -> Result<()> {
        let entries: Vec<(&str, u64, u64)> = cursor
            .iter()
            .filter(|(l, _)| l.stream == stream)
            .map(|(l, s)| (l.writer.as_str(), l.epoch, *s))
            .collect();
        let payload = serde_json::to_vec(&entries)?;
        let seq = self.pos_seq.fetch_add(1, Ordering::Relaxed);
        // the commit time orders commits across nodes (the newest wins everywhere); on this node
        // it never goes back (equal times fall to the sequence). ponytail: commits through two
        // nodes closer together than their clock skew may keep the earlier one; an order from
        // the reader if that matters.
        let now = crate::store::now_ms() * 1000;
        let time = self.pos_time.fetch_max(now, Ordering::Relaxed).max(now);
        let b = Batch::encode_keyed(
            &self.pos_log,
            seq,
            &[(time, &position_key(group, stream), &payload)],
        )?;
        let copies = (1 + self.targets().len()).min(2) as u8;
        let ack = self.append(copies, b).await;
        anyhow::ensure!(
            ack.is_success(),
            "position commit not acknowledged: {ack:?}"
        );
        Ok(())
    }

    /// A reader group's committed position in a stream, if it has one.
    pub fn position(&self, group: &str, stream: &str) -> Option<Cursor> {
        let g = self.compact.lock();
        let (_, _, p) = g.get(POSITIONS)?.get(&position_key(group, stream))?;
        let entries: Vec<(String, u64, u64)> = serde_json::from_slice(p).ok()?;
        Some(
            entries
                .into_iter()
                .map(|(writer, epoch, s)| {
                    (
                        LogId {
                            stream: stream.into(),
                            writer,
                            epoch,
                        },
                        s,
                    )
                })
                .collect(),
        )
    }

    /// A compacted stream's newest value of every key, by key.
    pub fn latest(&self, stream: &str) -> Vec<(Vec<u8>, Bytes)> {
        self.compact.lock().get(stream).map_or(vec![], |m| {
            m.iter()
                .map(|(k, (_, _, p))| (k.clone(), p.clone()))
                .collect()
        })
    }

    fn compacted_durable(&self, batch: &Batch, lsn: u64) {
        if let Some(m) = self.compact.lock().get_mut(&batch.id.log.stream) {
            apply_compacted(m, batch, lsn);
        }
    }

    /// Writes every compacted stream's snapshot durably (before retention frees segments).
    fn write_snapshots(&self) -> Result<()> {
        let snaps: Vec<(PathBuf, Vec<u8>)> = self
            .compact
            .lock()
            .iter()
            .map(|(s, m)| (snapshot_path(&self.cfg.data_dir, s), encode_snapshot(m)))
            .collect();
        if snaps.is_empty() {
            return Ok(());
        }
        write_durably(&self.cfg.data_dir.join("compact"), snaps)
    }

    /// Applies each stream's own retention, then the node's bounds. Before the node drops its
    /// oldest segments, the cleaner copies forward the batches in them that streams still retain
    /// under their own retention (docs/ARCHITECTURE.md, "The segment cleaner").
    pub async fn apply_retention(&self) -> Result<()> {
        // sealed segments' batches leave RAM (their index is on disk)
        self.store.absorb();
        self.write_snapshots()?;
        let now = crate::store::now_ms();
        for (name, s) in &self.cfg.streams {
            self.store
                .trim_stream(name, s.retention_bytes, s.retention_ms, now);
        }
        self.retain_segments(now).await?;
        // what changed goes to disk; then unused streams leave RAM
        self.checkpoint().await?;
        let d = crate::store::Dormancy {
            idle_ms: self.cfg.dormant_after_ms,
            budget: self.cfg.hot_streams_bytes as usize,
        };
        self.store.evict(d, now);
        if let Some(d) = self.dedup.lock().as_mut() {
            d.sweep(now / 1000);
        }
        Ok(())
    }

    /// Writes the store's checkpoint once the log has synced what it covers.
    async fn checkpoint(&self) -> Result<()> {
        let p = self.store.begin_checkpoint();
        let synced = match self.store.unchanged(&p) {
            true => Ok(()),
            false => self.wal.sync().await.map(|_| ()),
        };
        self.store.end_checkpoint(p, synced)
    }

    /// The node's bounds: drops the oldest segments, after the cleaner copies forward what
    /// streams with a retention of their own still retain in them.
    async fn retain_segments(&self, now: u64) -> Result<()> {
        if self.cfg.retention_bytes.is_none() && self.cfg.retention_ms.is_none() {
            return Ok(());
        }
        let (segs, want, total) = self
            .wal
            .retirable(self.cfg.retention_bytes, self.cfg.retention_ms);
        // absorbed ones only: a segment with a batch not marked durable yet waits for a pass
        let absorbed = self.store.absorbed_end();
        let n = segs.iter().take_while(|s| s.end <= absorbed).count();
        // the later ones' bytes take no room from the copies, or a segment waiting a pass would
        // force a cut of what streams retain; the log can be over the cap by them for that pass
        let waiting: u64 = segs[n..].iter().map(|s| s.bytes).sum();
        let segs = &segs[..n];
        // the node's age limit is a ceiling: nothing older than it is copied
        let min_time = self.cfg.retention_ms.map_or(0, |a| now.saturating_sub(a));
        let cap = self.cfg.retention_bytes.unwrap_or(u64::MAX);
        let limit = self.copy_limit();
        // the segments the bounds drop, and as many more as the copies of what streams retain in
        // them need room for
        let (mut n, mut left, mut keep) = (0, total - waiting, HashMap::<String, Vec<Kept>>::new());
        let mut cost = 0;
        let own: Vec<&str> = (self.cfg.streams.keys())
            .filter(|s| self.retains(s))
            .map(|s| s.as_str())
            .collect();
        while let Some(s) = segs.get(n)
            && (n < want || !fits(cost, limit, cap.saturating_sub(left)))
        {
            for (stream, v) in self.store.retained(&[s.base], &own, min_time) {
                cost += v.iter().map(|k| k.cost).sum::<u64>();
                keep.entry(stream).or_default().extend(v);
            }
            left -= s.bytes;
            n += 1;
        }
        let Some(end) = segs[..n].last().map(|s| s.end) else {
            return Ok(());
        };
        let bases: Vec<u64> = segs[..n].iter().map(|s| s.base).collect();
        let budget = cap.saturating_sub(left);
        self.fit(&mut keep, budget);
        self.copy_forward(keep).await?;
        self.store.vacate(&bases);
        // before the drop: what streams forgot stays forgotten across a restart (copies of it
        // outlive the segments about to be dropped), and the checkpoint then covers the log past
        // them, so a restart never needs what they held
        self.checkpoint().await?;
        let floor = self.wal.drop_until(end).await?;
        self.store.trim(floor);
        Ok(())
    }

    /// The stream has a retention of its own, kept through the node cap (a compacted stream
    /// keeps its newest values in its snapshot instead).
    fn retains(&self, stream: &str) -> bool {
        self.cfg.streams.get(stream).is_some_and(|c| {
            !c.compacted && (c.retention_bytes.is_some() || c.retention_ms.is_some())
        })
    }

    fn copy_limit(&self) -> u64 {
        COPY_CHUNK.min(self.cfg.segment_size - precog_storage::SEG_HEADER)
    }

    /// The node cap wins over what streams retain: while copying `keep` would take the log past
    /// `budget` bytes, cuts the oldest batches of the stream furthest over its share of the cap.
    /// Every stream with a retention of its own has an equal share, so that is the one
    /// retaining the most bytes; cut it down to the next one, then both, and so on. Counts them.
    fn fit(&self, keep: &mut HashMap<String, Vec<Kept>>, budget: u64) {
        let limit = self.copy_limit();
        let mut cost: u64 = keep.values().flatten().map(|k| k.cost).sum();
        while !fits(cost, limit, budget) {
            let mut most: Vec<(u64, String)> = keep
                .iter()
                .filter(|(_, v)| !v.is_empty())
                .map(|(s, _)| (self.store.stream_bytes(s), s.clone()))
                .collect();
            most.sort_unstable_by(|a, b| b.cmp(a));
            let next = most.get(1).map_or(0, |x| x.0);
            // cost counts only what `keep` holds, so while it does not fit something is kept
            let (_, stream) = most.swap_remove(0);
            let mut kept: HashMap<u64, Kept> = keep[&stream].iter().map(|k| (k.lsn, *k)).collect();
            let cut = self.store.cut(&stream, |bytes, lsn| {
                if fits(cost, limit, budget) || bytes < next {
                    return false;
                }
                if let Some(k) = kept.remove(&lsn) {
                    cost -= k.cost;
                }
                true
            });
            keep.insert(stream.clone(), kept.into_values().collect());
            if cut == 0 {
                tracing::error!("retention: stream {stream} could not be cut");
                break;
            }
            *self.forced.lock().entry(stream).or_default() += cut as u64;
        }
    }

    /// Logs the kept batches again, oldest first, in groups of at most `copy_limit` bytes, each
    /// its own write to the log (appends go on between them) and each taking its batches'
    /// place in the store once written.
    async fn copy_forward(&self, keep: HashMap<String, Vec<Kept>>) -> Result<()> {
        let mut all: Vec<Kept> = keep.into_values().flatten().collect();
        all.sort_unstable_by_key(|k| (k.time, k.lsn));
        for lsns in groups(&all, self.copy_limit()) {
            let _g = self.clean.lock().await;
            // a record that does not read back goes with its segment, as before copying: failing
            // the pass would fail every later one at it, and the cap would drop nothing again
            let recs: Vec<_> = lsns
                .iter()
                .filter_map(|&l| match self.wal.reader.read(l) {
                    Ok(Some(r)) => Some(r),
                    r => {
                        tracing::error!("retention: lsn {l} to copy does not read: {:?}", r.err());
                        None
                    }
                })
                // a replacement may have superseded it since it was picked (the lock keeps
                // replacements out from here to `moved`)
                .filter(|r| self.store.retains(r.lsn, &r.meta))
                .collect();
            if !recs.is_empty() {
                let first = self
                    .wal
                    .copy(
                        recs.iter()
                            .map(|r| (r.meta.clone(), r.payload.clone()))
                            .collect(),
                    )
                    .await?;
                self.store.moved(
                    recs.iter()
                        .zip(first..)
                        .map(|(r, lsn)| (r.lsn, lsn, &r.meta, r.payload.len() as u32)),
                );
                let bytes: usize = recs
                    .iter()
                    .map(|r| precog_storage::record::encoded_len(r.meta.key.len(), r.payload.len()))
                    .sum();
                self.copied_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
                self.copied_batches
                    .fetch_add(recs.len() as u64, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Prometheus text exposition of `status()`.
    pub fn metrics(&self) -> String {
        let s = self.counters();
        let w = &self.wal.stats;
        let h = self.store.hot_stats();
        let rows: [(&str, &str, u64); 19] = [
            ("precog_batches", "gauge", s.batches),
            ("precog_events", "gauge", s.events),
            ("precog_bytes", "gauge", s.bytes),
            ("precog_wal_bytes", "gauge", s.wal_bytes),
            ("precog_fsyncs_total", "counter", s.fsyncs),
            ("precog_segments", "gauge", s.segments),
            (
                "precog_recovery_scanned_bytes",
                "gauge",
                s.recovery_scanned_bytes,
            ),
            ("precog_dedup_dropped_total", "counter", s.dedup_dropped),
            ("precog_dedup_entries", "gauge", s.dedup_entries),
            ("precog_dedup_window_seconds", "gauge", s.dedup_window_s),
            ("precog_fatal", "gauge", self.is_fatal() as u64),
            (
                "precog_log_bytes",
                "gauge",
                w.log_bytes.load(Ordering::Relaxed),
            ),
            (
                "precog_retention_copied_bytes_total",
                "counter",
                self.copied_bytes.load(Ordering::Relaxed),
            ),
            (
                "precog_retention_copied_batches_total",
                "counter",
                self.copied_batches.load(Ordering::Relaxed),
            ),
            ("precog_streams_hot", "gauge", h.hot),
            ("precog_streams_dormant", "gauge", h.dormant),
            ("precog_stream_loads_total", "counter", h.loads),
            ("precog_stream_evictions_total", "counter", h.evictions),
            // the time of the loads, in µs (a rate of it over the loads' is their mean)
            (
                "precog_stream_load_microseconds_total",
                "counter",
                h.load_ns / 1000,
            ),
        ];
        let mut out = String::new();
        for (name, kind, v) in rows {
            out.push_str(&format!("# TYPE {name} {kind}\n{name} {v}\n"));
        }
        out.push_str("# TYPE precog_retention_forced_total counter\n");
        for (stream, n) in self.forced.lock().iter() {
            out.push_str(&format!(
                "precog_retention_forced_total{{stream=\"{stream}\"}} {n}\n"
            ));
        }
        // per reader group and stream: events not yet covered by its committed position, and how
        // long ago it committed (what kafka-exporter gives live-ingestion's alerts)
        let positions: Vec<(Vec<u8>, u64, Bytes)> =
            self.compact.lock().get(POSITIONS).map_or(vec![], |m| {
                m.iter()
                    .map(|(k, (_, lsn, p))| (k.clone(), *lsn, p.clone()))
                    .collect()
            });
        out.push_str(
            "# TYPE precog_group_lag_events gauge\n# TYPE precog_group_commit_age_seconds gauge\n",
        );
        let now = crate::store::now_ms();
        for (key, lsn, payload) in positions {
            let Some((group, stream)) = std::str::from_utf8(&key)
                .ok()
                .and_then(|k| k.split_once('\0'))
            else {
                continue;
            };
            let Ok(entries) = serde_json::from_slice::<Vec<(String, u64, u64)>>(&payload) else {
                continue;
            };
            let at: HashMap<(String, u64), u64> =
                entries.into_iter().map(|(w, e, s)| ((w, e), s)).collect();
            let lag: u64 = self
                .store
                .frontiers(Some(stream))
                .iter()
                .map(|f| {
                    f.high.saturating_sub(
                        at.get(&(f.log.writer.clone(), f.log.epoch))
                            .copied()
                            .unwrap_or(0),
                    )
                })
                .sum();
            let labels = format!("group=\"{group}\",stream=\"{stream}\"");
            out.push_str(&format!("precog_group_lag_events{{{labels}}} {lag}\n"));
            let at = self.wal.reader.entries(lsn, 1).unwrap_or_else(|e| {
                tracing::error!("reading the log's index at lsn {lsn}: {e}");
                vec![]
            });
            if let Some(e) = at.first().filter(|e| e.lsn == lsn) {
                let age = now.saturating_sub(e.meta.time_ms) / 1000;
                out.push_str(&format!(
                    "precog_group_commit_age_seconds{{{labels}}} {age}\n"
                ));
            }
        }
        out
    }

    async fn serve_http(self: Arc<Self>, mut s: tokio::net::TcpStream) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 8192];
        let mut n = 0;
        while !buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
            anyhow::ensure!(n < buf.len(), "request too large");
            let k = s.read(&mut buf[n..]).await?;
            anyhow::ensure!(k > 0, "closed");
            n += k;
        }
        let path = std::str::from_utf8(&buf[..n])?
            .split(' ')
            .nth(1)
            .unwrap_or("");
        let (code, body) = match path {
            "/metrics" => ("200 OK", self.metrics()),
            "/health" if self.is_fatal() => ("503 Service Unavailable", "storage fenced\n".into()),
            "/health" => ("200 OK", "ok\n".into()),
            _ => ("404 Not Found", String::new()),
        };
        let head = format!(
            "HTTP/1.1 {code}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        s.write_all(head.as_bytes()).await?;
        s.write_all(body.as_bytes()).await?;
        Ok(())
    }

    pub async fn spawn_listeners(self: &Arc<Self>) -> Result<()> {
        if let Some(addr) = self.cfg.listen_metrics {
            let l = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind metrics {addr}"))?;
            let node = self.clone();
            tokio::spawn(async move {
                while let Ok((s, _)) = l.accept().await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        if let Err(e) = node.serve_http(s).await {
                            tracing::debug!("metrics request: {e}");
                        }
                    });
                }
            });
        }
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
        // An empty push confirms the starting point: the client's subscribe waits for it.
        if tx
            .send(Frame::new(Kind::Push, req.req_id, Bytes::new()))
            .await
            .is_err()
        {
            return;
        }
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

    /// A live feed's batches in log order: `b` and whatever it releases (held batches, and ones
    /// durable before the feed began that follow it), or nothing while its predecessor is
    /// missing. A batch below where the feed joined its log was never due.
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
                // durable before the feed began, but past the gap the feed joined the log at:
                // the client takes the log in sequence from the first batch it saw, so it is
                // due (skipping it stalled the log on the client for good: spec/livefeed)
                let id = BatchId {
                    log: log.clone(),
                    first_seq: *n,
                };
                out.extend(self.store.get(&id));
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
                if let Control::FetchLog {
                    log,
                    from_seq,
                    max_bytes,
                } = &c
                {
                    let max = (*max_bytes as usize).min(MAX_FRAME_BYTES / 2);
                    let mut out = BytesMut::new();
                    for b in self.store.log_batches(log, *from_seq, max) {
                        out.put_u32_le(b.len() as u32);
                        out.put_slice(&b);
                    }
                    return Ok(Frame::new(Kind::Batches, id, out.freeze()));
                }
                if let Control::ReadCursor {
                    stream,
                    cursor,
                    max_bytes,
                } = &c
                {
                    let cursor = crate::store::cursor_from_wire(cursor.clone(), stream);
                    let max = (*max_bytes as usize).min(MAX_FRAME_BYTES / 2);
                    let (batches, _) = self.store.read_cursor(stream, &cursor, max);
                    let mut out = BytesMut::new();
                    for b in batches {
                        out.put_u32_le(b.len() as u32);
                        out.put_slice(&b);
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
            Control::Repair => {
                let (fetched, errors) = self.repair_pass().await;
                ControlResp::Repair { fetched, errors }
            }
            Control::Fetch { .. } | Control::ReadCursor { .. } | Control::FetchLog { .. } => {
                unreachable!()
            }
            Control::Inventory {
                from,
                max,
                incarnation,
            } => {
                let from = self.store.rejoin(from, incarnation);
                let max = max.min(INVENTORY_PAGE) as usize;
                let (next, end, logs) = self.store.inventory(from, max);
                ControlResp::Inventory(Inventory {
                    incarnation: self.incarnation,
                    next,
                    end,
                    logs,
                })
            }
            Control::CursorAt { stream, time_ms } => {
                let c = self.store.cursor_at(&stream, time_ms);
                ControlResp::Cursor {
                    cursor: Some(crate::store::cursor_to_wire(&c, &stream)),
                }
            }
            Control::Commit {
                group,
                stream,
                cursor,
            } => {
                self.commit_position(
                    &group,
                    &stream,
                    &crate::store::cursor_from_wire(cursor, &stream),
                )
                .await?;
                ControlResp::Committed
            }
            Control::Position { group, stream } => ControlResp::Cursor {
                cursor: self
                    .position(&group, &stream)
                    .map(|c| crate::store::cursor_to_wire(&c, &stream)),
            },
            Control::Latest { stream } => ControlResp::Latest {
                entries: self
                    .latest(&stream)
                    .into_iter()
                    .map(|(k, v)| (k, v.to_vec()))
                    .collect(),
            },
        })
    }

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
        let compacted = self.compact.lock().contains_key(&batch.id.log.stream);
        if let Some(d) = g.as_mut().filter(|_| !compacted) {
            let ds = digests(&batch);
            if !ds.is_empty() {
                let hs: Vec<u64> = ds.iter().map(|&(_, h)| h).collect();
                let mut dup = vec![];
                d.check(&batch.id.log.stream, &hs, now_s(), &mut dup);
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
                self.compacted_durable(batch, lsn);
                self.store.mark_durable(batch, lsn);
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
                            self.compacted_durable(batch, lsn);
                            self.store.mark_durable(batch, lsn);
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
                Reserve::Error(e) => bail!(e),
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
            d.record(&batch.id.log.stream, &ds, now_s());
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
            // a peer holds other bytes under the batch's identity
            let mut peer_conflict = false;
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
                        peer_conflict |= a.status == AckStatus::Conflict;
                        tracing::debug!("peer {pid} did not confirm: {a:?}")
                    }
                    R::Remote(pid, _, Err(e)) => {
                        tracing::debug!("peer {pid} replicate failed: {e}")
                    }
                }
                // the accepting node's own copy is always one of them: reads here see the batch
                // right after the ack, and dedup's ordering argument holds (see begin_local)
                if domains.len() >= copies as usize
                    && nodes & (1 << node.cfg.id) != 0
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
            if peer_conflict && let Err(e) = node.yield_to_majority(&batch).await {
                tracing::warn!("conflict at {:?} unresolved: {e}", batch.id);
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

    /// A peer holds other bytes than `ours` under its identity (this node was down while they
    /// were acknowledged, then a writer reused the identity here). Repair cannot
    /// see this: the frontiers agree. If a strict majority of the nodes hold one other batch
    /// there, that batch is logged as a replacement and takes ours' place.
    /// ponytail: a majority of nodes, so an ack at `copies` > n/2 always wins (2 of 3); a crash
    /// before this runs leaves the conflict, which a per-log content digest in repair would find.
    async fn yield_to_majority(&self, ours: &Batch) -> Result<()> {
        let ask = Control::Fetch {
            ids: vec![ours.id.clone()],
        };
        let mut held: Vec<Batch> = vec![];
        for p in &self.peers {
            if let Ok(f) = self.peer_control(p, &ask).await
                && f.kind == Kind::Batches
                && !f.body.is_empty()
            {
                held.push(Batch::parse(f.body.slice(4..))?);
            }
        }
        let n = self.peers.len() + 1;
        let Some(theirs) = held
            .iter()
            .find(|b| 2 * held.iter().filter(|x| x.ident == b.ident).count() > n)
        else {
            bail!("no batch is held by a majority of {n} nodes");
        };
        self.replace_with(theirs).await?;
        tracing::warn!(
            "{:?}: replaced bytes a majority does not hold with theirs",
            ours.id
        );
        Ok(())
    }

    /// Logs `theirs` in place of the batch this node holds under its identity (other bytes over
    /// the same range).
    async fn replace_with(&self, theirs: &Batch) -> Result<()> {
        let _g = self.clean.lock().await;
        anyhow::ensure!(
            self.store.replaceable(theirs),
            "the majority's batch has another range"
        );
        let lsn = self.wal.append_replacing(theirs.clone()).await?;
        self.store.replace(theirs, lsn);
        Ok(())
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

    /// Follows the peer's log from where the last pass stopped (`Control::Inventory`): compares
    /// the frontiers of the logs it wrote since with ours and fetches only where it holds
    /// sequences we lack. A pass costs O(what the peer logged since), not O(logs): a dormant
    /// stream is not read unless the peer wrote to it. After a peer's restart its log may have
    /// lost an unsynced tail (write caching) and logged other records at those LSNs: the peer
    /// answers from where its log ended when it opened (`rejoin`). This node's position in each
    /// peer's log is in its checkpoint: a new data directory follows every peer from its start.
    async fn repair_peer(&self, p: &Peer) -> Result<u64> {
        let id = p.cfg.id;
        let (mut incarnation, mut from) = self.store.peer(id).unwrap_or((0, 0));
        let mut fetched = 0;
        loop {
            let req = Control::Inventory {
                from,
                max: INVENTORY_PAGE,
                incarnation,
            };
            let f = self.peer_control(p, &req).await?;
            let inv: Inventory = serde_json::from_slice(&f.body)?;
            fetched += self.fetch_missing(p, inv.logs).await?;
            (incarnation, from) = (inv.incarnation, inv.next);
            self.store.set_peer(id, incarnation, from);
            if from >= inv.end {
                return Ok(fetched);
            }
        }
    }

    /// Fetches from the peer what its frontiers `theirs` hold that ours lack (from our
    /// contiguous end on).
    async fn fetch_missing(&self, p: &Peer, theirs: Vec<Frontier>) -> Result<u64> {
        let mut fetched = 0;
        for t in theirs {
            // from the first sequence we lack up to the peer's high (nothing, if we lack none)
            let mut from = self.store.frontier(&t.log).contiguous;
            while from < t.high {
                let req = Control::FetchLog {
                    log: t.log.clone(),
                    from_seq: from,
                    max_bytes: 4 << 20,
                };
                let f = self.peer_control(p, &req).await?;
                anyhow::ensure!(f.kind == Kind::Batches, "unexpected fetch response");
                let mut body = &f.body[..];
                let mut end = from;
                while body.len() >= 4 {
                    let n = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
                    let b = Batch::parse(f.body.slice_ref(&body[4..4 + n]))?;
                    body = &body[4 + n..];
                    end = end.max(b.end_seq());
                    match self.store_local(&b).await? {
                        Local::Stored => {
                            fetched += 1;
                            // as a replica records the ids the origin kept: a node that got the
                            // batch only by repair dedups its events too. ponytail: its ids count
                            // from the repair, so an old batch's are kept up to a window too long
                            if let Some(d) = self.dedup.lock().as_mut() {
                                let ds: Vec<u64> =
                                    digests(&b).into_iter().map(|(_, h)| h).collect();
                                d.record(&b.id.log.stream, &ds, now_s());
                            }
                        }
                        Local::Duplicate => {}
                        Local::Conflict(e) => {
                            tracing::error!("repair conflict from peer {}: {e}", p.cfg.id)
                        }
                    }
                }
                if end == from {
                    break; // the peer has nothing more from here (trimmed)
                }
                from = end;
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

/// Where the dedup window's batches start in the log: past the last sealed segment whose newest
/// batch is older than `since` (append times never go back, so none before it is newer).
fn window_start(segs: &[precog_storage::Sealed], first: u64, since: u64) -> u64 {
    segs.iter()
        .rev()
        .find(|s| s.max_time < since)
        .map_or(first, |s| s.end)
}

#[cfg(test)]
mod props_cleaner {
    use super::*;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Op {
        /// batches of (stream, writer, payload bytes) appended in this order
        Write(Vec<(usize, usize, usize)>),
        /// a batch logged but not marked durable until the next `Retain`: absorption waits
        Slow,
        /// a held batch of (stream, writer, which) replaced by other bytes (a majority's)
        Replace(usize, usize, prop::sample::Index),
        Retain,
        Reopen,
        /// a pass that crashes at a step (a failpoint), then a restart without it
        Crash(prop::sample::Index),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => prop::collection::vec((0usize..4, 0usize..2, 200usize..3000), 1..40)
                    .prop_map(Op::Write),
                1 => Just(Op::Slow),
                1 => (0usize..4, 0usize..2, any::<prop::sample::Index>())
                    .prop_map(|(s, w, i)| Op::Replace(s, w, i)),
                3 => Just(Op::Retain),
                1 => Just(Op::Reopen),
                1 => any::<prop::sample::Index>().prop_map(Op::Crash),
            ],
            1..16,
        )
    }

    const STREAMS: [&str; 4] = ["low", "mid", "high", "bare"];

    fn log(stream: usize, writer: usize) -> LogId {
        LogId {
            stream: STREAMS[stream].into(),
            writer: ["w1", "w2"][writer].into(),
            epoch: 1,
        }
    }

    /// The stream's batches a cursor from its oldest (a seek to time 0) reads, per log, by
    /// sequence. Not from an empty cursor: after a restart a log whose first batches went with
    /// dropped segments starts at sequence 0 and reads as a gap there (as on main: the store's
    /// trimmed floors are not kept on disk).
    fn cursor(node: &Node, stream: &str) -> BTreeMap<LogId, Vec<u64>> {
        let (mut c, mut out) = (
            node.store.cursor_at(stream, 0),
            BTreeMap::<LogId, Vec<u64>>::new(),
        );
        loop {
            let (bs, next) = node.store.read_cursor(stream, &c, 16 << 10);
            if bs.is_empty() {
                return out;
            }
            for b in bs {
                let b = Batch::parse(b).unwrap();
                out.entry(b.id.log).or_default().push(b.id.first_seq);
            }
            c = next;
        }
    }

    /// Why: the cleaner is what keeps each stream's own retention under the node cap. Whatever
    /// the writes, passes, batches slow to be marked and restarts: after a pass the log is within
    /// the cap, every stream holds the newest suffix of what was written to each of its logs
    /// (no holes, each batch once and byte for byte), and a stream with a retention of its own
    /// that the cap did not cut holds all of its retention.
    fn run(caching: bool, doze: bool, ops: Vec<Op>) -> Result<(), TestCaseError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let d = tempfile::tempdir().unwrap();
        let mut c = NodeConfig::single(0, "a", d.path().into(), "tok".into());
        c.segment_size = 16 << 10;
        let cap = 6 * (16 << 10);
        c.retention_bytes = Some(cap);
        if caching {
            c.flush_delay_ms = 60_000;
        }
        if doze {
            // every stream with no batch in RAM leaves it at each pass: what follows reads them
            // back from their summaries
            c.dormant_after_ms = 0;
            c.hot_streams_bytes = 0;
        }
        let own = [20_000u64, 50_000];
        for (s, m) in own.iter().enumerate() {
            c.streams.insert(
                STREAMS[s].into(),
                StreamConfig {
                    retention_bytes: Some(*m),
                    ..Default::default()
                },
            );
        }
        c.streams.insert("bare".into(), Default::default());
        rt.block_on(async {
            let mut node = Node::open(c.clone()).await.unwrap();
            // per stream, what was written in order: (log, seq, bytes)
            let mut written: Vec<Vec<(LogId, u64, Bytes)>> = vec![vec![]; 4];
            let mut next: BTreeMap<LogId, u64> = BTreeMap::new();
            let mut slow: Option<(Batch, u64)> = None;
            let mut slow_seq = 0;
            let mut cut = std::collections::BTreeSet::new();
            // logs with a replacement: its record is later than its neighbours' and so is its
            // append time, so it may outlive them (ponytail: a replacement does not keep the
            // batch's age); for those, only that what is served is what was written last
            let mut replaced = std::collections::BTreeSet::new();
            for op in ops {
                let pass = match op {
                    Op::Write(bs) => {
                        for (s, w, size) in bs {
                            let l = log(s, w);
                            let seq = next.get(&l).copied().unwrap_or(0);
                            next.insert(l.clone(), seq + 1);
                            let b = Batch::encode(&l, seq, &[(seq, &vec![seq as u8; size][..])])
                                .unwrap();
                            let ack = node.append(1, b.clone()).await;
                            prop_assert!(ack.is_success(), "{:?} {:?} {}", ack, l, seq);
                            written[s].push((l, seq, b));
                        }
                        false
                    }
                    Op::Slow => {
                        if slow.is_none() {
                            let l = LogId {
                                stream: "slow".into(),
                                writer: "w".into(),
                                epoch: 1,
                            };
                            let b = Batch::parse(
                                Batch::encode(&l, slow_seq, &[(slow_seq, b"x")]).unwrap(),
                            )
                            .unwrap();
                            slow_seq += 1;
                            prop_assert!(matches!(node.store.reserve(&b), Reserve::New));
                            let lsn = node.wal.append(b.clone()).await.unwrap();
                            slow = Some((b, lsn));
                        }
                        false
                    }
                    Op::Replace(s, w, i) => {
                        let l = log(s, w);
                        let of: Vec<usize> = (0..written[s].len())
                            .filter(|&j| written[s][j].0 == l)
                            .collect();
                        if !of.is_empty() {
                            let j = of[i.index(of.len())];
                            let (seq, old) = (written[s][j].1, written[s][j].2.len());
                            // other bytes, as many of them
                            let size =
                                old - Batch::encode(&l, seq, &[(seq, &[][..])]).unwrap().len();
                            let theirs =
                                Batch::encode(&l, seq, &[(seq, &vec![!(seq as u8); size][..])])
                                    .unwrap();
                            if node
                                .replace_with(&Batch::parse(theirs.clone()).unwrap())
                                .await
                                .is_ok()
                            {
                                written[s][j].2 = theirs;
                                replaced.insert(l);
                            }
                        }
                        false
                    }
                    Op::Retain => true,
                    Op::Reopen => {
                        slow = None; // logged: recovery finds it
                        drop(node);
                        node = Node::open(c.clone()).await.unwrap();
                        true
                    }
                    Op::Crash(i) => {
                        slow = None;
                        drop(node);
                        let at = [
                            "summary:before_commit",
                            "summary:after_commit",
                            "retain:before_manifest",
                            "retain:after_manifest",
                        ];
                        let mut f = c.clone();
                        f.storage_failpoints.fail_at = Some(at[i.index(at.len())]);
                        let faulty = Node::open(f).await.unwrap();
                        let _ = faulty.apply_retention().await; // fails, if it gets there
                        // what the cap cut before the fault stays cut
                        cut.extend(faulty.forced.lock().keys().cloned());
                        drop(faulty);
                        node = Node::open(c.clone()).await.unwrap();
                        true
                    }
                };
                if !pass {
                    continue;
                }
                let waiting = slow.is_some();
                node.apply_retention().await.unwrap();
                if let Some((b, lsn)) = slow.take() {
                    node.store.mark_durable(&b, lsn);
                    node.apply_retention().await.unwrap();
                }
                let bytes = node.wal.stats.log_bytes.load(Ordering::Relaxed);
                // a segment waiting to be absorbed may keep the log over the cap for a pass
                prop_assert!(waiting || bytes <= cap, "log {bytes} over the cap {cap}");
                let metrics = node.metrics();
                prop_assert!(metrics.contains("precog_retention_copied_batches_total"));
                prop_assert_eq!(node.status().logs, node.store.frontiers(None));
                // (the metric starts again with the process)
                cut.extend(node.forced.lock().keys().cloned());
                for (s, w) in written.iter().enumerate() {
                    let got = cursor(&node, STREAMS[s]);
                    let (mut held, mut held_bytes) = (0, 0u64);
                    let whole = !replaced.iter().any(|l: &LogId| l.stream == STREAMS[s]);
                    for (l, seqs) in &got {
                        if replaced.contains(l) {
                            for &seq in seqs {
                                let id = BatchId {
                                    log: l.clone(),
                                    first_seq: seq,
                                };
                                let x = w
                                    .iter()
                                    .find(|x| &x.0 == l && x.1 == seq)
                                    .map(|x| x.2.clone());
                                prop_assert_eq!(node.store.get(&id).map(|x| x.bytes), x);
                            }
                            continue;
                        }
                        let all = next[l];
                        let first = all - seqs.len() as u64;
                        prop_assert_eq!(
                            seqs,
                            &(first..all).collect::<Vec<_>>(),
                            "{:?}: the newest, no hole",
                            l
                        );
                        for (wl, seq, b) in w.iter().filter(|x| &x.0 == l && x.1 >= first) {
                            let id = BatchId {
                                log: wl.clone(),
                                first_seq: *seq,
                            };
                            prop_assert_eq!(node.store.get(&id).map(|x| x.bytes), Some(b.clone()));
                        }
                        held += seqs.len();
                        held_bytes += w
                            .iter()
                            .filter(|x| &x.0 == l && x.1 >= first)
                            .map(|x| x.2.len() as u64)
                            .sum::<u64>();
                    }
                    if !whole {
                        continue;
                    }
                    prop_assert_eq!(
                        node.store.read(STREAMS[s], 0, 1 << 20).0.len(),
                        held,
                        "once each"
                    );
                    if s < 2 {
                        // within the stream's own retention, and (unless the cap ever cut it)
                        // within one batch of it: it forgets oldest first only while over
                        let total: u64 = w.iter().map(|x| x.2.len() as u64).sum();
                        let most = w.iter().map(|x| x.2.len() as u64).max().unwrap_or(0);
                        prop_assert!(
                            held_bytes <= own[s].max(total.min(own[s])),
                            "{} over its own",
                            STREAMS[s]
                        );
                        if !cut.contains(STREAMS[s]) {
                            if total <= own[s] {
                                prop_assert_eq!(held, w.len(), "{} keeps all", STREAMS[s]);
                            } else {
                                prop_assert!(
                                    held_bytes + most > own[s],
                                    "{} keeps its own retention",
                                    STREAMS[s]
                                );
                            }
                        }
                    }
                }
                for (s, w) in written.iter().enumerate() {
                    // a live feed from the start: what it is served is what was written
                    let (bs, _) = node.store.tail(0, Some(STREAMS[s]), 1 << 20);
                    for b in bs {
                        let x = w.iter().find(|x| x.0 == b.id.log && x.1 == b.id.first_seq);
                        prop_assert_eq!(x.map(|x| &x.2), Some(&b.bytes), "{:?}", b.id);
                    }
                }
                let (_, live) = node.store.live_start(None);
                // (a log reading as a gap at its start after a restart joins at the gap, as above)
                let whole: Vec<LogId> = node
                    .store
                    .frontiers(None)
                    .into_iter()
                    .filter(|f| f.contiguous == f.high)
                    .map(|f| f.log)
                    .collect();
                for (l, n) in &next {
                    if live.contains_key(l) && whole.contains(l) {
                        prop_assert_eq!(live[l], *n, "a feed joins {:?} after its last batch", l);
                    }
                }
            }
            Ok(())
        })
    }

    #[derive(Clone, Debug)]
    enum Hurt {
        /// batches of 1000 bytes appended to a stream
        Write(usize, usize),
        /// a sealed segment's index file cut short, or one byte of its records flipped
        Damage(bool, prop::sample::Index, prop::sample::Index),
        Retain,
    }

    /// Why: the cap protects the disk. Damage to sealed segments (a bad sector, a stray write)
    /// makes reads of them fail; that must not stop the cap: every pass succeeds, the log stays
    /// within the cap, and readers get errors or less, never a panic.
    fn hurt(ops: Vec<Hurt>) -> Result<(), TestCaseError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let d = tempfile::tempdir().unwrap();
        let mut c = NodeConfig::single(0, "a", d.path().into(), "tok".into());
        c.segment_size = 16 << 10;
        c.tail_cache_bytes = 0; // reads go to the disk
        let cap = 6 * (16 << 10);
        c.retention_bytes = Some(cap);
        for (s, m) in [(0, 20_000u64), (1, 50_000)] {
            c.streams.insert(
                STREAMS[s].into(),
                StreamConfig {
                    retention_bytes: Some(m),
                    ..Default::default()
                },
            );
        }
        rt.block_on(async {
            let node = Node::open(c.clone()).await.unwrap();
            let mut next = [0u64; 4];
            let mut damaged: Vec<PathBuf> = vec![];
            for op in ops {
                match op {
                    Hurt::Write(s, n) => {
                        for _ in 0..n {
                            let seq = next[s];
                            next[s] += 1;
                            let b =
                                Batch::encode(&log(s, 0), seq, &[(seq, &[7u8; 1000][..])]).unwrap();
                            let ack = node.append(1, b).await;
                            prop_assert!(ack.is_success(), "{:?} fatal {}", ack, node.is_fatal());
                        }
                    }
                    Hurt::Damage(index, file, at) => {
                        let ext = if index { "idx" } else { "log" };
                        let mut fs: Vec<_> = std::fs::read_dir(d.path().join("log"))
                            .unwrap()
                            .map(|e| e.unwrap().path())
                            .filter(|p| p.extension().is_some_and(|x| x == ext))
                            .collect();
                        fs.sort();
                        if !index {
                            fs.pop(); // the active segment's: appends go on there
                        }
                        if fs.is_empty() {
                            continue;
                        }
                        let p = &fs[file.index(fs.len())];
                        damaged.push(p.clone());
                        let f = std::fs::OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(p)
                            .unwrap();
                        let len = f.metadata().unwrap().len();
                        if len == 0 {
                            continue;
                        }
                        let off = at.index(len as usize) as u64;
                        if index {
                            // cut short: its reads fail (ponytail: an index file has no checksum
                            // of its entries, so a flipped byte there reads as wrong entries)
                            f.set_len(off).unwrap();
                        } else {
                            use std::os::unix::fs::FileExt;
                            let mut b = [0u8];
                            f.read_exact_at(&mut b, off).unwrap();
                            f.write_all_at(&[!b[0]], off).unwrap();
                        }
                    }
                    Hurt::Retain => {
                        let r = node.apply_retention().await;
                        prop_assert!(r.is_ok(), "a pass fails: {:?}", r);
                        // a damaged segment can keep the cap from cutting what it holds until it
                        // goes itself (the oldest go first): then the next pass is within it
                        damaged.retain(|p| p.exists());
                        if damaged.is_empty() {
                            prop_assert!(node.apply_retention().await.is_ok());
                            let bytes = node.wal.stats.log_bytes.load(Ordering::Relaxed);
                            prop_assert!(bytes <= cap, "log {} over the cap {}", bytes, cap);
                        }
                    }
                }
                for s in STREAMS {
                    let mut cur = node.store.cursor_at(s, 0);
                    for _ in 0..64 {
                        let (bs, n) = node.store.read_cursor(s, &cur, 16 << 10);
                        if bs.is_empty() {
                            break;
                        }
                        cur = n;
                    }
                    node.store.tail(0, Some(s), 1 << 20);
                    node.store.read(s, 0, 1 << 20);
                    node.store.lsns(s);
                    node.store.inventory(0, 1 << 16);
                }
            }
            Ok(())
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]

        #[test]
        fn the_cleaner_keeps_each_stream_s_retention_within_the_cap(caching: bool, doze: bool, ops in ops()) {
            run(caching, doze, ops)?;
        }

        #[test]
        fn the_cap_holds_whatever_damage_sealed_segments_take(ops in prop::collection::vec(
            prop_oneof![
                // mostly streams the cleaner copies: damage then hits what it reads
                3 => (0usize..2, 1usize..40).prop_map(|(s, n)| Hurt::Write(s, n)),
                1 => (1usize..40).prop_map(|n| Hurt::Write(2, n)),
                2 => (any::<bool>(), any::<prop::sample::Index>(), any::<prop::sample::Index>())
                    .prop_map(|(i, f, a)| Hurt::Damage(i, f, a)),
                3 => Just(Hurt::Retain),
            ],
            1..24,
        )) {
            hurt(ops)?;
        }
    }

    proptest! {
        /// Why: the node never exceeds its cap because of copying, so what it reserves for a
        /// copy must cover what the copy adds: its records and a segment header for each group
        /// (each may roll the log). Records of just over half a group are the worst case.
        #[test]
        fn what_a_copy_reserves_covers_its_records_and_a_header_per_group(
            costs in prop::collection::vec(1u64..3000, 0..40), limit in 1000u64..4000,
        ) {
            let kept: Vec<Kept> = costs.iter().zip(0..).map(|(&cost, lsn)| Kept { lsn, time: 0, cost }).collect();
            let gs = groups(&kept, limit);
            prop_assert_eq!(gs.concat(), (0..kept.len() as u64).collect::<Vec<_>>(), "all, in order");
            let size = |g: &Vec<u64>| g.iter().map(|&l| costs[l as usize]).sum::<u64>();
            for g in &gs {
                prop_assert!(g.len() == 1 || size(g) <= limit, "a group holds at most `limit`");
            }
            for w in gs.windows(2) {
                let next = costs[w[1][0] as usize];
                prop_assert!(size(&w[0]) + next > limit, "as few groups as fit: the next did not");
            }
            let cost: u64 = costs.iter().sum();
            prop_assert!(copy_bytes(cost, limit) >= cost + precog_storage::SEG_HEADER * gs.len() as u64);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_does_not_bring_back_what_a_stream_s_retention_forgot() {
        // Why: a copy forward puts a stream's older batches in a newer segment than its next
        // ones. When its own retention then forgets both and the cap drops the older segment,
        // only the copies are left on disk. Unless the stream's retention floor survives a
        // restart, they come back below a hole, and a reader from the stream's start stops there.
        let d = tempfile::tempdir().unwrap();
        let mut c = NodeConfig::single(0, "a", d.path().into(), "tok".into());
        c.segment_size = 64 << 10;
        c.retention_bytes = Some(4 * (64 << 10));
        let at = |stream: &str, seq: u64| {
            let log = LogId {
                stream: stream.into(),
                writer: "w".into(),
                epoch: 1,
            };
            Batch::encode(&log, seq, &[(seq, &[1u8; 1000][..])]).unwrap()
        };
        let one = at("low", 0).len() as u64;
        c.streams.insert(
            "low".into(),
            StreamConfig {
                retention_bytes: Some(10 * one),
                ..Default::default()
            },
        );
        let node = Node::open(c.clone()).await.unwrap();
        let mut hs = 0..;
        let mut low = 0..;
        let mut write = async |stream: &str, n: usize| {
            for _ in 0..n {
                let seq = if stream == "low" {
                    low.next()
                } else {
                    hs.next()
                }
                .unwrap();
                assert!(node.append(1, at(stream, seq)).await.is_success());
            }
        };
        write("low", 5).await;
        write("high", 70).await; // low 0..5 in the oldest segment
        write("low", 5).await;
        write("high", 150).await;
        node.apply_retention().await.unwrap(); // copies low 0..5 forward, past low 5..10
        write("low", 8).await;
        for _ in 0..2 {
            write("high", 60).await;
            // low forgets 0..8; the cap drops the segment of 5..10 (copying 8 and 9)
            node.apply_retention().await.unwrap();
        }
        let read = |node: &Node| {
            let (mut c, mut seqs) = (node.store.cursor_at("low", 0), vec![]);
            loop {
                let (bs, next) = node.store.read_cursor("low", &c, 1 << 20);
                if bs.is_empty() {
                    return seqs;
                }
                seqs.extend(
                    bs.into_iter()
                        .map(|b| Batch::parse(b).unwrap().id.first_seq),
                );
                c = next;
            }
        };
        assert_eq!(read(&node), (8..18).collect::<Vec<_>>());
        drop(node);
        let node = Node::open(c).await.unwrap();
        assert_eq!(read(&node), (8..18).collect::<Vec<_>>(), "after a restart");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_batch_replaced_after_the_cleaner_picked_it_is_not_copied() {
        // Why: a replacement can land between the cleaner picking a batch and copying it. A copy
        // of the superseded bytes logged after the replacement would conflict with it on disk:
        // the next start refuses the log. The copy must take only what is still retained.
        let d = tempfile::tempdir().unwrap();
        let mut c = NodeConfig::single(0, "a", d.path().into(), "tok".into());
        c.segment_size = 64 << 10;
        c.retention_bytes = Some(4 * (64 << 10));
        c.streams.insert(
            "low".into(),
            StreamConfig {
                retention_bytes: Some(1 << 20),
                ..Default::default()
            },
        );
        let node = Node::open(c.clone()).await.unwrap();
        let log = |stream: &str| LogId {
            stream: stream.into(),
            writer: "w".into(),
            epoch: 1,
        };
        let at = |stream: &str, seq: u64, v: u8| {
            Batch::encode(&log(stream), seq, &[(seq, &[v; 1000][..])]).unwrap()
        };
        for seq in 0..5 {
            assert!(node.append(1, at("low", seq, 1)).await.is_success());
        }
        for seq in 0..300 {
            assert!(node.append(1, at("high", seq, 1)).await.is_success());
        }
        node.store.absorb();
        let base = node.wal.retirable(None, None).0[0].base;
        let own: Vec<&str> = (node.cfg.streams.keys())
            .filter(|s| node.retains(s))
            .map(|s| s.as_str())
            .collect();
        let keep = node.store.retained(&[base], &own, 0);
        assert!(!keep.is_empty());
        let theirs = Batch::parse(at("low", 0, 2)).unwrap();
        node.replace_with(&theirs).await.unwrap();
        node.copy_forward(keep).await.unwrap();
        let id = BatchId {
            log: log("low"),
            first_seq: 0,
        };
        assert_eq!(
            node.store.get(&id).map(|b| b.bytes),
            Some(theirs.bytes.clone())
        );
        drop(node);
        let node = Node::open(c).await.expect("the log reopens");
        assert_eq!(node.store.get(&id).map(|b| b.bytes), Some(theirs.bytes));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_segment_waiting_to_be_absorbed_does_not_force_a_cut() {
        // Why: the cleaner drops only absorbed segments; a sealed one holding a batch not marked
        // durable yet waits a pass. Its bytes are not what streams retain: counting them against
        // the copies' room cut a stream within its retention, the loss the cleaner prevents.
        let d = tempfile::tempdir().unwrap();
        let mut c = NodeConfig::single(0, "a", d.path().into(), "tok".into());
        c.segment_size = 64 << 10;
        let cap = 4 * (64 << 10);
        c.retention_bytes = Some(cap);
        c.streams.insert(
            "low".into(),
            StreamConfig {
                retention_bytes: Some(1 << 20),
                ..Default::default()
            },
        );
        let node = Node::open(c).await.unwrap();
        let at = |stream: &str, seq: u64| {
            let log = LogId {
                stream: stream.into(),
                writer: "w".into(),
                epoch: 1,
            };
            Batch::encode(&log, seq, &[(seq, &[1u8; 1000][..])]).unwrap()
        };
        for seq in 0..20 {
            assert!(node.append(1, at("low", seq)).await.is_success());
        }
        for seq in 0..150 {
            assert!(node.append(1, at("high", seq)).await.is_success());
        }
        // written to the log, not marked durable yet: absorption waits for it
        let slow = Batch::parse(at("slow", 0)).unwrap();
        assert!(matches!(node.store.reserve(&slow), Reserve::New));
        let lsn = node.wal.append(slow.clone()).await.unwrap();
        for seq in 150..450 {
            assert!(node.append(1, at("high", seq)).await.is_success());
        }
        node.apply_retention().await.unwrap();
        assert!(node.forced.lock().is_empty(), "{:?}", node.forced.lock());
        node.store.mark_durable(&slow, lsn);
        node.apply_retention().await.unwrap();
        assert!(node.forced.lock().is_empty(), "{:?}", node.forced.lock());
        assert!(node.wal.stats.log_bytes.load(Ordering::Relaxed) <= cap);
        for seq in 0..20 {
            let id = BatchId {
                log: LogId {
                    stream: "low".into(),
                    writer: "w".into(),
                    epoch: 1,
                },
                first_seq: seq,
            };
            assert!(node.store.get(&id).is_some(), "low {seq}");
        }
    }

    #[test]
    fn copies_of_just_over_half_a_group_each_take_a_group_and_the_reserve_holds() {
        let kept: Vec<Kept> = (0..10)
            .map(|lsn| Kept {
                lsn,
                time: 0,
                cost: 501,
            })
            .collect();
        assert_eq!(groups(&kept, 1000).len(), 10);
        assert_eq!(
            copy_bytes(5010, 1000),
            5010 + precog_storage::SEG_HEADER * 11
        );
        assert_eq!(copy_bytes(0, 1000), 0, "nothing to copy reserves nothing");
        assert_eq!(copy_bytes(1, 1000), 1 + precog_storage::SEG_HEADER);
        // a copy fits exactly when the room is what it reserves
        assert!(fits(1, 1000, 1 + precog_storage::SEG_HEADER));
        assert!(!fits(1, 1000, precog_storage::SEG_HEADER));
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn a_format_1_snapshot_still_reads_and_any_newer_event_replaces_its_values() {
        // Why: every node has a __positions snapshot written before read-order compaction; a
        // node must start from it (formats read or are refused loudly, never silently).
        let mut b = SNAP_MAGIC_V1.to_vec();
        b.extend_from_slice(&1u64.to_le_bytes());
        b.extend_from_slice(&1u16.to_le_bytes());
        b.extend_from_slice(b"k");
        b.extend_from_slice(&42u64.to_le_bytes()); // lsn
        b.extend_from_slice(&3u32.to_le_bytes());
        b.extend_from_slice(b"old");
        let c = crc32c::crc32c(&b);
        b.extend_from_slice(&c.to_le_bytes());
        let mut m = decode_snapshot(&b).unwrap();
        assert_eq!(
            m[&b"k"[..]],
            ((0, String::new(), 0, 0), 42, Bytes::from("old"))
        );
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let batch = Batch::parse(Batch::encode_keyed(&log, 0, &[(0, b"k", b"new")]).unwrap());
        apply_compacted(&mut m, &batch.unwrap(), 1);
        assert_eq!(m[&b"k"[..]].2, Bytes::from("new"));
        let mut bad = b.clone();
        bad[0] = b'X';
        assert!(
            decode_snapshot(&bad).is_err(),
            "an unknown format is refused"
        );
    }
}

#[cfg(test)]
mod props_restart {
    use super::*;
    use proptest::prelude::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    fn config(dir: &std::path::Path) -> NodeConfig {
        let mut c = NodeConfig::single(0, "a", dir.into(), "tok".into());
        c.segment_size = 8 << 10;
        c.target_copies = 1;
        c.dormant_after_ms = 0;
        c
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]

        /// Why: a compacted stream keeps its newest value per key through restarts: its snapshot,
        /// then what the log holds of it, read back through the store even while it is dormant.
        #[test]
        fn a_compacted_stream_keeps_its_newest_values_through_restarts(
            writes in prop::collection::vec((0u8..4, any::<u8>(), any::<bool>()), 1..24),
        ) {
            let d = tempfile::tempdir().unwrap();
            let mut c = config(d.path());
            c.streams.insert("c".into(), StreamConfig { compacted: true, ..Default::default() });
            let log = LogId { stream: "c".into(), writer: "w".into(), epoch: 1 };
            rt().block_on(async {
                let mut node = Node::open(c.clone()).await.unwrap();
                let mut newest = BTreeMap::new();
                for (seq, (k, v, pass)) in writes.into_iter().enumerate() {
                    let seq = seq as u64;
                    let b = Batch::encode_keyed(&log, seq, &[(seq, &[k][..], &[v; 300][..])]).unwrap();
                    prop_assert!(node.append(1, b).await.is_success());
                    newest.insert(vec![k], vec![v; 300]);
                    if pass {
                        node.apply_retention().await.unwrap();
                        drop(node);
                        node = Node::open(c.clone()).await.unwrap();
                    }
                }
                drop(node);
                let node = Node::open(c.clone()).await.unwrap();
                let got: BTreeMap<Vec<u8>, Vec<u8>> =
                    node.latest("c").into_iter().map(|(k, v)| (k, v.to_vec())).collect();
                prop_assert_eq!(got, newest);
                Ok(())
            })?;
        }

        /// Why: an older node's data directory has no streams database, only `<data_dir>/floors`:
        /// its streams are rebuilt from the log, and what their own retention forgot stays
        /// forgotten.
        #[test]
        fn an_older_data_directory_is_rebuilt_and_keeps_what_retention_forgot(n in 2u64..10, keep in 1u64..4) {
            let d = tempfile::tempdir().unwrap();
            let c = config(d.path());
            let log = LogId { stream: "r".into(), writer: "w".into(), epoch: 1 };
            rt().block_on(async {
                let node = Node::open(c.clone()).await.unwrap();
                for seq in 0..n {
                    let b = Batch::encode(&log, seq, &[(seq, &[1u8; 100][..])]).unwrap();
                    prop_assert!(node.append(1, b).await.is_success());
                }
                let one = node.store.stream_bytes("r") / n;
                let gone = node.store.trim_stream("r", Some(keep * one), None, 0) as u64;
                prop_assert_eq!(gone, n.saturating_sub(keep));
                // the floor an older node wrote: past the last batch forgotten
                let es = node.wal.reader.entries(0, 64).unwrap();
                let last = es.iter().filter(|e| decode_log_key(&e.meta.key).is_ok_and(|l| l == log)).nth(gone.max(1) as usize - 1).unwrap();
                let floor = (last.meta.time_ms, last.meta.first_seq, last.lsn + gone.min(1));
                // the last reference: the database closes with it (an append's task may hold one)
                while Arc::strong_count(&node) > 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                drop(node);
                std::fs::remove_file(d.path().join("streams.redb")).unwrap();
                let floors = BTreeMap::from([("r".to_string(), floor)]);
                std::fs::write(d.path().join("floors"), props::encode_floors(&floors)).unwrap();
                let node = Node::open(c.clone()).await.unwrap();
                let c0 = node.store.cursor_at("r", 0);
                let (bs, _) = node.store.read_cursor("r", &c0, 1 << 20);
                prop_assert_eq!(bs.len() as u64, n - gone);
                Ok(())
            })?;
        }
    }
}

#[cfg(test)]
mod props {
    /// The older node's writer of `<data_dir>/floors`.
    pub(super) fn encode_floors(m: &BTreeMap<String, crate::store::Age>) -> Vec<u8> {
        let mut b = FLOORS_MAGIC.to_vec();
        b.extend_from_slice(&(m.len() as u64).to_le_bytes());
        for (name, &(time, seq, lsn)) in m {
            b.extend_from_slice(&(name.len() as u16).to_le_bytes());
            b.extend_from_slice(name.as_bytes());
            for v in [time, seq, lsn] {
                b.extend_from_slice(&v.to_le_bytes());
            }
        }
        let c = crc32c::crc32c(&b);
        b.extend_from_slice(&c.to_le_bytes());
        b
    }

    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Every segment with a batch at or past `since` is scanned, and nothing before the last
        /// that is wholly older.
        #[test]
        fn the_dedup_window_starts_after_the_last_wholly_older_segment(
            gaps in proptest::collection::vec(0u64..3, 0..8),
            since in 0u64..12,
        ) {
            let (mut t, mut segs) = (0, vec![]);
            for (i, g) in gaps.iter().enumerate() {
                t += g;
                let base = 10 + 5 * i as u64;
                segs.push(precog_storage::Sealed { base, end: base + 5, bytes: 1, max_time: t });
            }
            let at = window_start(&segs, 10, since);
            for s in &segs {
                prop_assert!(s.max_time < since || s.base >= at, "{s:?} skipped at {at}");
            }
            let last_older = segs.iter().filter(|s| s.max_time < since).map(|s| s.end).max();
            prop_assert_eq!(at, last_older.unwrap_or(10));
        }
    }

    proptest! {
        /// The streams' retention floors (read at every start) round-trip, and any bytes under a
        /// matching checksum decode or are refused, never a panic.
        #[test]
        fn retention_floors_round_trip_and_damaged_ones_are_refused(
            m in prop::collection::btree_map("[a-z0-9_-]{0,8}", any::<(u64, u64, u64)>(), 0..5),
            at in any::<prop::sample::Index>(), v: u8, cut in any::<prop::sample::Index>(),
        ) {
            let b = encode_floors(&m);
            prop_assert_eq!(decode_floors(&b).unwrap(), m);
            prop_assert!(decode_floors(&b[..b.len() - 1]).is_err(), "a short file");
            let mut more = b[..b.len() - 4].to_vec();
            more.push(0);
            let c = crc32c::crc32c(&more);
            more.extend_from_slice(&c.to_le_bytes());
            prop_assert!(decode_floors(&more).is_err(), "bytes past its entries");
            let mut body = b[..b.len() - 4].to_vec();
            let i = at.index(body.len());
            body[i] = v;
            body.truncate(cut.index(body.len() + 1));
            let c = crc32c::crc32c(&body);
            body.extend_from_slice(&c.to_le_bytes());
            let _ = decode_floors(&body);
        }

        /// A compacted-stream snapshot (read at every start) round-trips, and any bytes under a
        /// matching checksum decode or are refused, never a panic.
        #[test]
        fn snapshots_round_trip_and_damaged_ones_are_refused(
            m in prop::collection::btree_map(
                prop::collection::vec(any::<u8>(), 0..4),
                ((any::<u64>(), "[a-z0-9-]{0,6}", any::<u64>(), any::<u64>()), any::<u64>(),
                 prop::collection::vec(any::<u8>(), 0..4)),
                0..4,
            ),
            at in any::<prop::sample::Index>(), v: u8, cut in any::<prop::sample::Index>(),
        ) {
            let m: Compacted = m.into_iter().map(|(k, (o, l, p))| (k, (o, l, Bytes::from(p)))).collect();
            let b = encode_snapshot(&m);
            prop_assert_eq!(decode_snapshot(&b).unwrap(), m);
            let mut body = b[..b.len() - 4].to_vec();
            let i = at.index(body.len());
            body[i] = v;
            body.truncate(cut.index(body.len() + 1));
            let c = crc32c::crc32c(&body);
            body.extend_from_slice(&c.to_le_bytes());
            let _ = decode_snapshot(&body);
        }
    }
}
