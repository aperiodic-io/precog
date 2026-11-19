//! Precog's Rust SDK.
//!
//! - [`Producer`]: batches events of one writer log, each with a dedup key (live-ingestion's
//!   `redpanda-dedup-key`), and appends them durably on 2 nodes. A stream's producers all prefer
//!   the same node, so two producer replicas' copies of an event meet one dedup index. A failed
//!   append is retried with the same bytes on the next node, which is idempotent.
//! - [`Consumer`]: reads a stream from a cursor that is valid on every node, failing over between
//!   nodes; starts at a committed position, the beginning, the end or a time; commits.
//! - [`Client::latest`]: a compacted stream's newest value of every key.

use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
use precog_mesh::client::MeshClient;
use precog_mesh::proto::{Batch, Control, ControlResp, Kind, LogId};
use precog_mesh::store::{cursor_from_wire, cursor_to_wire};
use std::sync::Arc;

pub use precog_mesh::proto::Event;
pub use precog_mesh::store::{Cursor, now_ms};

/// Batches a producer keeps waiting for their acks (the protocol POCs: pipelining 1.8x).
pub const MAX_IN_FLIGHT: usize = 8;

/// A dedup id as it travels: its 8-byte xxh3 (an empty id stays empty: never deduplicated).
pub fn dedup_key(id: &[u8]) -> Vec<u8> {
    if id.is_empty() {
        vec![]
    } else {
        xxhash_rust::xxh3::xxh3_64(id).to_le_bytes().to_vec()
    }
}

/// Events per batch before `send` asks for a flush (`Producer::is_full`).
pub const MAX_BATCH_EVENTS: usize = 4096;
/// Bytes per batch before `send` asks for a flush.
pub const MAX_BATCH_BYTES: usize = 1 << 20;

#[derive(Clone)]
pub struct Client {
    mc: Arc<MeshClient>,
}

/// Where a new consumer starts.
#[derive(Clone, Debug)]
pub enum Start {
    Beginning,
    End,
    /// The first batch logged at or after this time (ms since the epoch).
    Time(u64),
    /// The group's committed position, or `or` if it has none.
    Committed {
        or: Box<Start>,
    },
}

impl Client {
    /// `nodes`: `tcp://host:port` of every node.
    pub fn connect(nodes: &[impl AsRef<str>], token: &str) -> Result<Client> {
        let addrs = nodes
            .iter()
            .map(|a| a.as_ref().parse())
            .collect::<Result<Vec<_>>>()?;
        Ok(Client {
            mc: MeshClient::new(addrs, token.into())?,
        })
    }

    /// The node a stream's clients try first.
    fn preferred(&self, stream: &str) -> usize {
        (xxhash_rust::xxh3::xxh3_64(stream.as_bytes()) % self.mc.node_count() as u64) as usize
    }

    /// A control request to the first node that answers, from `start` round-robin.
    async fn control(&self, start: usize, c: &Control) -> Result<(usize, ControlResp)> {
        let n = self.mc.node_count();
        let mut last = None;
        for i in 0..n {
            let node = (start + i) % n;
            match self.mc.control(node, c).await {
                Ok(r) => return Ok((node, r)),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap()).context("no node answered")
    }

    /// A writer log of `stream` named `writer`, with a new epoch (time-based) per producer.
    pub fn producer(&self, stream: &str, writer: &str) -> Producer {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64);
        Producer {
            client: self.clone(),
            log: LogId {
                stream: stream.into(),
                writer: writer.into(),
                epoch,
            },
            next_seq: 0,
            events: vec![],
            bytes: 0,
            copies: self.mc.node_count().min(2) as u8,
            in_flight: tokio::task::JoinSet::new(),
            compress: true,
        }
    }

    pub async fn consumer(&self, stream: &str, group: &str, start: Start) -> Result<Consumer> {
        let cursor = self.resolve(stream, group, start).await?;
        let mut c = self.consumer_at(stream, cursor);
        c.group = Some(group.into());
        Ok(c)
    }

    /// A consumer from a cursor the caller kept (brrrrr keeps it in its own checkpoints).
    pub fn consumer_at(&self, stream: &str, cursor: Cursor) -> Consumer {
        Consumer {
            client: self.clone(),
            stream: stream.into(),
            group: None,
            cursor,
            node: self.preferred(stream),
        }
    }

    async fn resolve(&self, stream: &str, group: &str, start: Start) -> Result<Cursor> {
        let at = |time_ms| Control::CursorAt {
            stream: stream.into(),
            time_ms,
        };
        let c = match start {
            Start::Beginning => return Ok(Cursor::new()),
            Start::End => return self.end(stream).await,
            Start::Time(t) => at(t),
            Start::Committed { or } => {
                let q = Control::Position {
                    group: group.into(),
                    stream: stream.into(),
                };
                match self.control(self.preferred(stream), &q).await?.1 {
                    ControlResp::Cursor { cursor: Some(w) } => {
                        return Ok(cursor_from_wire(w, stream));
                    }
                    _ => return Box::pin(self.resolve(stream, group, *or)).await,
                }
            }
        };
        match self.control(self.preferred(stream), &c).await?.1 {
            ControlResp::Cursor { cursor: Some(w) } => Ok(cursor_from_wire(w, stream)),
            r => bail!("unexpected {r:?}"),
        }
    }

    /// Stores `cursor` as `group`'s position on `stream`: a reader that reads ahead of what it
    /// has made durable (the archiver, until its upload is acknowledged) commits the cursor it
    /// kept for the durable part instead of its consumer's.
    pub async fn commit(&self, group: &str, stream: &str, cursor: &Cursor) -> Result<()> {
        let c = Control::Commit {
            group: group.into(),
            stream: stream.into(),
            cursor: cursor_to_wire(cursor, stream),
        };
        match self.control(self.preferred(stream), &c).await?.1 {
            ControlResp::Committed => Ok(()),
            r => bail!("unexpected {r:?}"),
        }
    }

    /// Where `stream` ends: every acknowledged event is before this cursor. An acked batch is on
    /// `copies` (2) nodes, so one node's end can be short (a node just restarted, or repairing);
    /// the ends of any nodes - copies + 1 nodes, merged per writer log, include one of its copies
    /// (2 of 3 nodes, 3 of 4). A node that lost its disk can make the end short until repair:
    /// a reader starting there then reads a few acked events again (spec/mesh, EndCoversAcked).
    pub async fn end(&self, stream: &str) -> Result<Cursor> {
        let n = self.mc.node_count();
        let need = n - n.min(2) + 1; // the producers' copies: n.min(2)
        let q = Control::CursorAt {
            stream: stream.into(),
            time_ms: u64::MAX,
        };
        let start = self.preferred(stream);
        let (mut end, mut answered, mut last) = (Cursor::new(), 0, None);
        for i in 0..n {
            match self.mc.control((start + i) % n, &q).await {
                Ok(ControlResp::Cursor { cursor: Some(w) }) => {
                    for (log, seq) in cursor_from_wire(w, stream) {
                        let e = end.entry(log).or_insert(0);
                        *e = (*e).max(seq);
                    }
                    answered += 1;
                    if answered == need {
                        return Ok(end);
                    }
                }
                Ok(r) => last = Some(anyhow::anyhow!("unexpected {r:?}")),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("no nodes")))
            .with_context(|| format!("the end of {stream} needs {need} nodes, {answered} answered"))
    }

    /// A compacted stream's newest value of every key, by key.
    pub async fn latest(&self, stream: &str) -> Result<Vec<(Vec<u8>, Bytes)>> {
        match self
            .control(
                self.preferred(stream),
                &Control::Latest {
                    stream: stream.into(),
                },
            )
            .await?
            .1
        {
            ControlResp::Latest { entries } => Ok(entries
                .into_iter()
                .map(|(k, v)| (k, Bytes::from(v)))
                .collect()),
            r => bail!("unexpected {r:?}"),
        }
    }
}

/// Events of one writer log, appended in batches.
pub struct Producer {
    client: Client,
    log: LogId,
    next_seq: u64,
    events: Vec<(u64, Vec<u8>, Vec<u8>)>,
    bytes: usize,
    copies: u8,
    in_flight: tokio::task::JoinSet<Result<()>>,
    compress: bool,
}

impl Producer {
    /// Payloads go as one lz4 block per batch (the default: about a third of the bytes to send,
    /// store and replicate, at ~0.2 us/record of client CPU); the broker never decompresses.
    pub fn set_compression(&mut self, on: bool) {
        self.compress = on;
    }

    /// Buffers an event with its dedup id (live-ingestion's `redpanda-dedup-key`; empty: never
    /// deduplicated), sent as its 8-byte hash. `flush` sends it; flush when `is_full`.
    pub fn send(&mut self, time_us: u64, dedup_id: &[u8], payload: &[u8]) {
        let key = dedup_key(dedup_id);
        self.put(time_us, &key, payload);
    }

    /// Buffers an event of a compacted stream: `key` is its compaction key, kept as is.
    pub fn put(&mut self, time_us: u64, key: &[u8], payload: &[u8]) {
        self.bytes += 14 + key.len() + payload.len();
        self.events.push((time_us, key.to_vec(), payload.to_vec()));
    }

    /// Batches sent and not yet acknowledged.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }

    /// Waits until every batch sent so far is acknowledged (durable on `copies` nodes).
    pub async fn sync(&mut self) -> Result<()> {
        self.flush().await?;
        while let Some(r) = self.in_flight.join_next().await {
            r??;
        }
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.events.len() >= MAX_BATCH_EVENTS || self.bytes >= MAX_BATCH_BYTES
    }

    /// Sends the buffered events as one batch, keeping up to `MAX_IN_FLIGHT` batches waiting
    /// for their acks (it waits for the oldest beyond that); `sync` waits for all of them. A
    /// failed append is retried with the same bytes, which is idempotent.
    pub async fn flush(&mut self) -> Result<()> {
        if self.events.is_empty() {
            return Ok(());
        }
        while self.in_flight.len() >= MAX_IN_FLIGHT {
            self.in_flight.join_next().await.unwrap()??;
        }
        let evs: Vec<(u64, &[u8], &[u8])> = self
            .events
            .iter()
            .map(|(t, k, p)| (*t, &k[..], &p[..]))
            .collect();
        let b = if self.compress {
            Batch::encode_compressed(&self.log, self.next_seq, &evs)?
        } else {
            Batch::encode_keyed(&self.log, self.next_seq, &evs)?
        };
        let (mc, copies) = (self.client.mc.clone(), self.copies);
        let preferred = self.client.preferred(&self.log.stream);
        self.in_flight
            .spawn(async move { mc.append(preferred, &b, copies).await.map(|_| ()) });
        self.next_seq += self.events.len() as u64;
        self.events.clear();
        self.bytes = 0;
        Ok(())
    }
}

/// A reader of one stream.
pub struct Consumer {
    client: Client,
    stream: String,
    group: Option<String>,
    cursor: Cursor,
    node: usize,
}

impl Consumer {
    /// The next events (about `max_bytes` of batches), per writer in sequence order; empty when
    /// caught up. Fails over to the next node on an error.
    pub async fn poll(&mut self, max_bytes: u32) -> Result<Vec<Event>> {
        let n = self.client.mc.node_count();
        let req = Control::ReadCursor {
            stream: self.stream.clone(),
            cursor: cursor_to_wire(&self.cursor, &self.stream),
            max_bytes,
        };
        let body = Bytes::from(serde_json::to_vec(&req)?);
        let mut last = None;
        for i in 0..n {
            let node = (self.node + i) % n;
            match self.client.mc.call(node, Kind::Control, body.clone()).await {
                Ok(f) if f.kind == Kind::Batches => {
                    self.node = node;
                    return self.take(f.body);
                }
                Ok(f) => last = Some(anyhow::anyhow!("unexpected {:?}", f.kind)),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap()).context("no node answered")
    }

    fn take(&mut self, mut body: Bytes) -> Result<Vec<Event>> {
        let mut out = vec![];
        while body.remaining() >= 4 {
            let len = body.get_u32_le() as usize;
            anyhow::ensure!(body.remaining() >= len, "truncated batches");
            let b = Batch::parse(body.split_to(len))?;
            let from = self.cursor.get(&b.id.log).copied().unwrap_or(0);
            out.extend(b.try_events()?.into_iter().filter(|e| e.seq >= from));
            self.cursor.insert(b.id.log.clone(), b.end_seq());
        }
        Ok(out)
    }

    /// Where the consumer is: a position valid on every node.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    /// Stores the position as the group's (after the caller has made what it read durable).
    pub async fn commit(&self) -> Result<()> {
        let group = self
            .group
            .as_deref()
            .context("a consumer_at has no group to commit to")?;
        self.client.commit(group, &self.stream, &self.cursor).await
    }
}
