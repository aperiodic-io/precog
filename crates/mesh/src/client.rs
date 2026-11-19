//! Client: any node accepts any append. A `Writer` owns one writer log
//! `(stream, writer, epoch)` and assigns sequence numbers itself, so retrying
//! the *same bytes* on another node after a timeout is always safe: the mesh
//! either already has them (Duplicate) or stores them now.

use crate::proto::*;
use crate::rpc::RpcClient;
use crate::transport::Addr;
use anyhow::{Result, bail};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct MeshClient {
    nodes: Vec<Addr>,
    links: Vec<tokio::sync::Mutex<Option<Arc<RpcClient>>>>,
    token: String,
    pub rpc_timeout: Duration,
    /// Total time an append keeps retrying across nodes.
    pub append_deadline: Duration,
}

#[derive(Debug, Clone)]
pub struct Receipt {
    pub ack: Ack,
    pub node: usize,
    pub attempts: u32,
}

impl MeshClient {
    pub fn new(nodes: Vec<Addr>, token: String) -> Result<Arc<MeshClient>> {
        anyhow::ensure!(!nodes.is_empty(), "no nodes");
        Ok(Arc::new(MeshClient {
            links: nodes.iter().map(|_| Default::default()).collect(),
            nodes,
            token,
            rpc_timeout: Duration::from_millis(1500),
            append_deadline: Duration::from_secs(30),
        }))
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    async fn link(&self, i: usize) -> Result<Arc<RpcClient>> {
        let mut g = self.links[i].lock().await;
        if let Some(c) = g.as_ref()
            && !c.is_closed()
        {
            return Ok(c.clone());
        }
        let c = crate::transport::connect(&self.nodes[i], self.rpc_timeout).await?;
        let hello = Hello {
            token: self.token.clone(),
            node_id: None,
        };
        c.call(
            Kind::Hello,
            Bytes::from(serde_json::to_vec(&hello)?),
            self.rpc_timeout,
        )
        .await?;
        let c = Arc::new(c);
        *g = Some(c.clone());
        Ok(c)
    }

    pub async fn call(&self, node: usize, kind: Kind, body: Bytes) -> Result<Frame> {
        let c = self.link(node).await?;
        let r = c.call(kind, body, self.rpc_timeout).await;
        if r.is_err() {
            // Drop the link on any failure so a hung connection is not reused.
            let mut g = self.links[node].lock().await;
            if g.as_ref().is_some_and(|x| Arc::ptr_eq(x, &c)) {
                *g = None;
            }
        }
        r
    }

    pub async fn control(&self, node: usize, c: &Control) -> Result<ControlResp> {
        let f = self
            .call(node, Kind::Control, Bytes::from(serde_json::to_vec(c)?))
            .await?;
        anyhow::ensure!(f.kind == Kind::ControlResp, "unexpected {:?}", f.kind);
        Ok(serde_json::from_slice(&f.body)?)
    }

    /// Appends pre-encoded batch bytes, starting at `preferred` node and
    /// failing over round-robin on transport errors, timeouts or
    /// `Unknown` outcomes until `append_deadline`.
    pub async fn append(&self, preferred: usize, batch: &Bytes, copies: u8) -> Result<Receipt> {
        let body = encode_append(copies, batch);
        let start = Instant::now();
        let mut node = preferred % self.nodes.len();
        let mut attempts = 0;
        let mut last_err;
        loop {
            attempts += 1;
            match self
                .call(node, Kind::Append, body.clone())
                .await
                .and_then(|f| crate::node::parse_frame_ack(&f))
            {
                Ok(ack) if ack.is_success() => {
                    return Ok(Receipt {
                        ack,
                        node,
                        attempts,
                    });
                }
                Ok(ack) if matches!(ack.status, AckStatus::Conflict | AckStatus::Rejected) => {
                    bail!("append refused: {ack:?}")
                }
                Ok(ack) => last_err = format!("{ack:?}"),
                Err(e) => last_err = e.to_string(),
            }
            if start.elapsed() > self.append_deadline {
                bail!("append outcome unknown after {attempts} attempts: {last_err}");
            }
            node = (node + 1) % self.nodes.len();
            if (attempts as usize).is_multiple_of(self.nodes.len()) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

impl MeshClient {
    /// Opens a live subscription on one node. Delivers every batch that
    /// becomes durable on that node, per writer log in sequence order.
    pub async fn subscribe(
        &self,
        node: usize,
        stream: Option<&str>,
        from_start: bool,
    ) -> Result<Subscription> {
        let c = self.link(node).await?;
        let body = serde_json::to_vec(&Subscribe {
            stream: stream.map(str::to_owned),
            from_start,
        })?;
        let mut rx = c
            .subscribe(Kind::Subscribe, Bytes::from(body), 1024)
            .await?;
        // The node's empty push: the feed starts no later than this.
        let start = tokio::time::timeout(self.rpc_timeout, rx.recv()).await;
        anyhow::ensure!(
            matches!(&start, Ok(Some(f)) if f.body.is_empty()),
            "node {node} did not confirm the subscription"
        );
        Ok(Subscription {
            rx,
            from_start,
            next: Default::default(),
            held: Default::default(),
            ready: Default::default(),
            _conn: c,
        })
    }
}

/// Client end of a live feed. Restores per-log order if a node received a
/// log's batches out of order (possible after repair), and drops batches it
/// has already delivered.
pub struct Subscription {
    rx: tokio::sync::mpsc::Receiver<Frame>,
    /// A replay must see every log from sequence 0; a live feed joins each
    /// log at the first batch it sees.
    from_start: bool,
    next: std::collections::HashMap<LogId, u64>,
    held: std::collections::HashMap<LogId, std::collections::BTreeMap<u64, Batch>>,
    ready: std::collections::VecDeque<Batch>,
    _conn: Arc<RpcClient>,
}

impl Subscription {
    /// Next in-order batch; `None` when the connection closed.
    pub async fn next(&mut self) -> Option<Result<Batch>> {
        loop {
            if let Some(b) = self.ready.pop_front() {
                return Some(Ok(b));
            }
            let f = self.rx.recv().await?;
            if f.kind == Kind::Error {
                return Some(Err(anyhow::anyhow!(
                    "subscription refused: {}",
                    String::from_utf8_lossy(&f.body)
                )));
            }
            let b = match Batch::parse(f.body) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            self.accept(b);
        }
    }

    fn accept(&mut self, b: Batch) {
        let log = b.id.log.clone();
        let start = if self.from_start { 0 } else { b.id.first_seq };
        let next = self.next.entry(log.clone()).or_insert(start);
        if b.end_seq() <= *next {
            return; // already delivered
        }
        if b.id.first_seq != *next {
            // A writer's new epoch may start anywhere, but within an epoch
            // a gap means an earlier batch is still on its way.
            self.held.entry(log).or_default().insert(b.id.first_seq, b);
            return;
        }
        *next = b.end_seq();
        self.ready.push_back(b);
        if let Some(h) = self.held.get_mut(&log) {
            while let Some(e) = h.first_entry() {
                let n = self.next[&log];
                if *e.key() < n && e.get().end_seq() <= n {
                    e.remove();
                } else if *e.key() == n {
                    let b = e.remove();
                    self.next.insert(log.clone(), b.end_seq());
                    self.ready.push_back(b);
                } else {
                    break;
                }
            }
        }
    }
}

/// Single-writer handle for one writer log.
pub struct Writer {
    pub log: LogId,
    pub next_seq: u64,
    pub preferred: usize,
    pub copies: u8,
    client: Arc<MeshClient>,
}

impl Writer {
    pub fn new(
        client: Arc<MeshClient>,
        stream: &str,
        writer: &str,
        epoch: u64,
        preferred: usize,
        copies: u8,
    ) -> Writer {
        Writer {
            log: LogId {
                stream: stream.into(),
                writer: writer.into(),
                epoch,
            },
            next_seq: 0,
            preferred,
            copies,
            client,
        }
    }

    pub fn encode(&self, events: &[(u64, &[u8])]) -> Result<Bytes> {
        Batch::encode(&self.log, self.next_seq, events)
    }

    /// Appends one batch; on success advances the sequence and sticks to the
    /// node that answered (so failover is sticky).
    pub async fn append_encoded(&mut self, bytes: &Bytes, count: usize) -> Result<Receipt> {
        let r = self
            .client
            .append(self.preferred, bytes, self.copies)
            .await?;
        self.preferred = r.node;
        self.next_seq += count as u64;
        Ok(r)
    }

    pub async fn append(&mut self, events: &[(u64, &[u8])]) -> Result<Receipt> {
        let b = self.encode(events)?;
        self.append_encoded(&b, events.len()).await
    }
}
