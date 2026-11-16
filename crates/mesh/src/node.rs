//! A mesh node: accepts appends for *any* writer log, stores them in its
//! group-commit WAL, replicates directly to peers in other failure domains
//! and acknowledges once `copies` distinct domains hold a durable copy.
//!
//! There is no leader and no per-partition consensus on the data path:
//! every writer log has exactly one writer, so the order inside a log is fixed
//! by the writer's sequence numbers, and logs never have to be ordered relative
//! to each other at write time. Readers merge logs by event time.

use crate::proto::*;
use crate::rpc::{BoxRead, BoxWrite, RpcClient, read_frame, write_loop};
use crate::store::{Reserve, State, Store};
use crate::transport::{Addr, Dialer, iroh_endpoint, load_or_create_secret};
use crate::wal::Wal;
use anyhow::{Context, Result, anyhow, bail};
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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
    /// fdatasync before acknowledging (false = page-cache durability only).
    #[serde(default = "yes")]
    pub sync: bool,
    pub listen_tcp: Option<SocketAddr>,
    pub listen_iroh: Option<SocketAddr>,
    #[serde(default)]
    pub iroh_relay: bool,
    pub peers: Vec<PeerConfig>,
    /// Copies the node tries to create for every append (including its own).
    #[serde(default = "three")]
    pub target_copies: u8,
    #[serde(default = "rpc_ms")]
    pub rpc_timeout_ms: u64,
    #[serde(default = "repair_ms")]
    pub repair_interval_ms: u64,
}

fn yes() -> bool {
    true
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
    dialer: Dialer,
    peers: Vec<Arc<Peer>>,
    /// Peer ids keyed by pinned Iroh endpoint id.
    iroh_peers: HashMap<iroh::EndpointId, u32>,
    repair_cursor: parking_lot::Mutex<HashMap<u32, u64>>,
    repair_lock: tokio::sync::Mutex<()>,
    inflight: Semaphore,
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
    pub async fn open(cfg: NodeConfig) -> Result<(Arc<Node>, Option<iroh::Endpoint>)> {
        anyhow::ensure!(cfg.id < 32, "node id must be < 32");
        anyhow::ensure!(cfg.peers.len() <= 31, "too many peers");
        let (wal, rec) = Wal::open(&cfg.data_dir, cfg.sync)?;
        let n = rec.batches.len();
        let store = Store::load(rec.batches)?;
        tracing::info!(
            node = cfg.id,
            batches = n,
            truncated = rec.truncated_bytes,
            "recovered WAL"
        );
        let endpoint = match cfg.listen_iroh {
            Some(bind) => {
                let sk = load_or_create_secret(&cfg.data_dir.join("iroh.secret"))?;
                let ep = iroh_endpoint(Some(sk), Some(bind), cfg.iroh_relay).await?;
                tracing::info!(node = cfg.id, iroh_id = %ep.id(), "iroh endpoint bound on {bind}");
                Some(ep)
            }
            None => None,
        };
        let mut peers = Vec::new();
        let mut iroh_peers = HashMap::new();
        for p in &cfg.peers {
            anyhow::ensure!(p.id != cfg.id && p.id < 32, "bad peer id {}", p.id);
            let addr: Addr = p.addr.parse()?;
            if let Addr::Iroh { id, .. } = &addr {
                iroh_peers.insert(*id, p.id);
            }
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
            dialer: Dialer {
                iroh: endpoint.clone(),
            },
            peers,
            iroh_peers,
            repair_cursor: Default::default(),
            repair_lock: Default::default(),
            inflight: Semaphore::new(16_384),
        });
        Ok((node, endpoint))
    }

    /// Opens the node and serves until the process is stopped.
    pub async fn run(cfg: NodeConfig) -> Result<()> {
        let (node, endpoint) = Node::open(cfg).await?;
        node.spawn_listeners(endpoint).await?;
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
        std::future::pending::<()>().await;
        Ok(())
    }

    pub async fn spawn_listeners(self: &Arc<Self>, endpoint: Option<iroh::Endpoint>) -> Result<()> {
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
                    tokio::spawn(
                        async move { node.serve_conn(Box::new(r), Box::new(w), None).await },
                    );
                }
            });
        }
        if let Some(ep) = endpoint {
            let node = self.clone();
            tokio::spawn(async move {
                while let Some(incoming) = ep.accept().await {
                    let node = node.clone();
                    tokio::spawn(async move {
                        let conn = match incoming.await {
                            Ok(c) => c,
                            Err(e) => return tracing::debug!("iroh incoming failed: {e}"),
                        };
                        let remote = conn.remote_id();
                        while let Ok((send, recv)) = conn.accept_bi().await {
                            let node = node.clone();
                            tokio::spawn(async move {
                                node.serve_conn(Box::new(recv), Box::new(send), Some(remote))
                                    .await
                            });
                        }
                    });
                }
            });
        }
        Ok(())
    }

    async fn serve_conn(
        self: Arc<Self>,
        mut r: BoxRead,
        w: BoxWrite,
        iroh_remote: Option<iroh::EndpointId>,
    ) {
        let mut buf = BytesMut::new();
        let hello = match tokio::time::timeout(Duration::from_secs(5), read_frame(&mut r, &mut buf))
            .await
        {
            Ok(Ok(Some(f))) if f.kind == Kind::Hello => f,
            _ => return,
        };
        let ident = match self.authenticate(&hello.body, iroh_remote) {
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

    fn authenticate(&self, body: &[u8], iroh_remote: Option<iroh::EndpointId>) -> Result<Ident> {
        let h: Hello = serde_json::from_slice(body)?;
        if !constant_time_eq(h.token.as_bytes(), self.cfg.token.as_bytes()) {
            bail!("bad token");
        }
        match (h.node_id, iroh_remote) {
            (None, _) => Ok(Ident::Client),
            (Some(id), Some(remote)) => {
                // Over Iroh the peer identity is proven by its endpoint key.
                match self.iroh_peers.get(&remote) {
                    Some(&pid) if pid == id => Ok(Ident::Peer(id)),
                    _ => bail!("iroh endpoint {remote} is not configured as peer {id}"),
                }
            }
            (Some(id), None) => {
                // TCP test transport: a token holder may claim any peer id.
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
            Control::Status => {
                let (batches, events, bytes) = self.store.totals();
                ControlResp::Status(Status {
                    node_id: self.cfg.id,
                    domain: self.cfg.domain.clone(),
                    batches,
                    events,
                    bytes,
                    wal_bytes: self
                        .wal
                        .stats
                        .bytes
                        .load(std::sync::atomic::Ordering::Relaxed),
                    fsyncs: self
                        .wal
                        .stats
                        .syncs
                        .load(std::sync::atomic::Ordering::Relaxed),
                    logs: self.store.frontiers(None),
                })
            }
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
    async fn store_local(&self, batch: &Batch) -> Result<Local> {
        loop {
            match self.store.reserve(batch) {
                Reserve::New => {
                    return match self.wal.append(batch.bytes.clone()).await {
                        Ok(()) => {
                            self.store.mark_durable(&batch.id);
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
                futs.push(
                    Box::pin(async move { R::Local(node.store_local(&batch).await) })
                        as std::pin::Pin<Box<dyn std::future::Future<Output = R> + Send>>,
                );
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
            let c = self.dialer.connect(&p.addr, timeout).await?;
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
