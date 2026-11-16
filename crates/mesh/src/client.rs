//! Client: any node accepts any append. A `Writer` owns one writer log
//! `(stream, writer, epoch)` and assigns sequence numbers itself, so retrying
//! the *same bytes* on another node after a timeout is always safe: the mesh
//! either already has them (Duplicate) or stores them now.

use crate::proto::*;
use crate::rpc::RpcClient;
use crate::transport::{Addr, Dialer, iroh_endpoint};
use anyhow::{Result, bail};
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct MeshClient {
    nodes: Vec<Addr>,
    links: Vec<tokio::sync::Mutex<Option<Arc<RpcClient>>>>,
    dialer: Dialer,
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
    pub async fn new(nodes: Vec<Addr>, token: String) -> Result<Arc<MeshClient>> {
        let needs_iroh = nodes.iter().any(|a| matches!(a, Addr::Iroh { .. }));
        let iroh = if needs_iroh { Some(iroh_endpoint(None, Some("127.0.0.1:0".parse()?), false).await?) } else { None };
        Self::with_dialer(nodes, token, Dialer { iroh })
    }

    pub fn with_dialer(nodes: Vec<Addr>, token: String, dialer: Dialer) -> Result<Arc<MeshClient>> {
        anyhow::ensure!(!nodes.is_empty(), "no nodes");
        Ok(Arc::new(MeshClient {
            links: nodes.iter().map(|_| Default::default()).collect(),
            nodes,
            dialer,
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
        let c = self.dialer.connect(&self.nodes[i], self.rpc_timeout).await?;
        let hello = Hello { token: self.token.clone(), node_id: None };
        c.call(Kind::Hello, Bytes::from(serde_json::to_vec(&hello)?), self.rpc_timeout).await?;
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
        let f = self.call(node, Kind::Control, Bytes::from(serde_json::to_vec(c)?)).await?;
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
            match self.call(node, Kind::Append, body.clone()).await.and_then(|f| crate::node::parse_frame_ack(&f)) {
                Ok(ack) if ack.is_success() => return Ok(Receipt { ack, node, attempts }),
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
            if attempts as usize % self.nodes.len() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
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
    pub fn new(client: Arc<MeshClient>, stream: &str, writer: &str, epoch: u64, preferred: usize, copies: u8) -> Writer {
        Writer {
            log: LogId { stream: stream.into(), writer: writer.into(), epoch },
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
        let r = self.client.append(self.preferred, bytes, self.copies).await?;
        self.preferred = r.node;
        self.next_seq += count as u64;
        Ok(r)
    }

    pub async fn append(&mut self, events: &[(u64, &[u8])]) -> Result<Receipt> {
        let b = self.encode(events)?;
        self.append_encoded(&b, events.len()).await
    }
}
