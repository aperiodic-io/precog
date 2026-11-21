//! Precog's Rust SDK.
//!
//! - [`Producer`]: batches events of one writer log, each with a dedup key (live-ingestion's
//!   `redpanda-dedup-key`), and appends them durably on 2 nodes. A stream's producers all prefer
//!   the same node, so two producer replicas' copies of an event meet one dedup index. A failed
//!   append is retried with the same bytes on the next node, which is idempotent.
//! - [`Consumer`]: reads a stream from a cursor that is valid on every node, failing over between
//!   nodes; starts at a committed position, the beginning, the end or a time; commits.
//! - [`Client::latest`]: a compacted stream's newest value of every key.
//! - Events larger than a batch: a producer and a consumer that are both `set_chunked` carry an
//!   event of any size up to [`MAX_EVENT_BYTES`] as several events of the stream, each with a
//!   one-byte tag; the broker stores them as any others.

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

/// A chunked producer cuts an event into chunks of at most this many bytes: two fill a batch.
pub const CHUNK_BYTES: usize = MAX_BATCH_BYTES / 2;
/// The largest event a chunked consumer puts back together: it holds an event's chunks in
/// memory until its last one, and a writer that never ends an event must not grow it for ever.
pub const MAX_EVENT_BYTES: usize = 256 << 20;

/// The first byte of every event of a chunked stream: the event whole, or its first, a middle
/// or its last chunk.
const WHOLE: u8 = 0;
const FIRST: u8 = 1;
const MIDDLE: u8 = 2;
const LAST: u8 = 3;

/// An event's payload as a chunked producer sends it: tagged chunks of at most `max` bytes.
fn chunks(payload: &[u8], max: usize) -> impl Iterator<Item = Vec<u8>> + '_ {
    let n = payload.len().div_ceil(max).max(1);
    (0..n).map(move |i| {
        let tag = match i {
            _ if n == 1 => WHOLE,
            0 => FIRST,
            i if i + 1 == n => LAST,
            _ => MIDDLE,
        };
        let part = &payload[i * max..((i + 1) * max).min(payload.len())];
        [&[tag][..], part].concat()
    })
}

/// A chunked consumer's reassembly of one writer log: the event whose chunks it holds.
#[derive(Default)]
struct Assembly {
    /// The first chunk (its sequence and time are the event's), the bytes so far, and the
    /// sequence its next chunk has.
    building: Option<(Event, Vec<u8>, u64)>,
}

impl Assembly {
    /// The next event of the log, in sequence order: the event it completes, if any. An event
    /// is yielded only whole: chunks whose first was not seen are passed over (the reader
    /// started inside the event, the broker dropped its first chunk as a duplicate, retention
    /// cut it), and an event with a chunk missing is dropped. An error leaves it at the next
    /// event's start.
    fn push(&mut self, e: Event) -> Result<Option<Event>> {
        // (a gap in the sequence: what was being built never ends)
        let building = self.building.take().filter(|b| b.2 == e.seq);
        let Some((&tag, part)) = e.payload.split_first() else {
            bail!(
                "{}/{} seq {}: an event without a chunk tag",
                e.stream,
                e.writer,
                e.seq
            );
        };
        match (tag, building) {
            (WHOLE, _) => Ok(Some(Event {
                payload: e.payload.slice(1..),
                ..e
            })),
            (FIRST, _) => {
                self.building = Some((e.clone(), part.to_vec(), e.seq + 1));
                Ok(None)
            }
            (MIDDLE | LAST, None) => Ok(None),
            (MIDDLE | LAST, Some((first, mut bytes, _))) => {
                anyhow::ensure!(
                    bytes.len() + part.len() <= MAX_EVENT_BYTES,
                    "{}/{} seq {}: an event over {MAX_EVENT_BYTES} bytes is passed over",
                    first.stream,
                    first.writer,
                    first.seq
                );
                bytes.extend_from_slice(part);
                if tag == MIDDLE {
                    self.building = Some((first, bytes, e.seq + 1));
                    return Ok(None);
                }
                Ok(Some(Event {
                    payload: bytes.into(),
                    ..first
                }))
            }
            _ => bail!("{}/{} seq {}: chunk tag {tag}", e.stream, e.writer, e.seq),
        }
    }

    /// Where the event being built starts: a reader that resumes must start there.
    fn boundary(&self) -> Option<u64> {
        self.building.as_ref().map(|b| b.0.seq)
    }
}

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

/// Trusts the CA certificates in a PEM file for every `tls://` node this process connects to:
/// call it before `Client::connect` to a cluster that serves TLS.
pub use precog_mesh::transport::trust as trust_ca;

impl Client {
    /// `nodes`: `tcp://host:port` of every node, or `tls://host:port` (see `trust_ca`).
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
            chunked: false,
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
            resume: cursor.clone(),
            cursor,
            node: self.preferred(stream),
            chunked: None,
            refused: None,
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

    /// Deletes the stream: every writer log of it there is, on every node, for good. Returns
    /// the epoch it is deleted below (0: there was nothing of it to delete). A producer created
    /// after this (its epoch is the clock, above any before it) writes a new stream of the
    /// name; one created before is refused.
    pub async fn delete(&self, stream: &str) -> Result<u64> {
        let c = Control::Delete {
            stream: stream.into(),
        };
        let (_, r) = self.control(self.preferred(stream), &c).await?;
        r.deleted().with_context(|| format!("unexpected {r:?}"))
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
    chunked: bool,
}

impl Producer {
    /// Payloads go as one lz4 block per batch (the default: about a third of the bytes to send,
    /// store and replicate, at ~0.2 us/record of client CPU); the broker never decompresses.
    pub fn set_compression(&mut self, on: bool) {
        self.compress = on;
    }

    /// Sends events of any size up to `MAX_EVENT_BYTES`: each event carries a one-byte tag,
    /// and one larger than `CHUNK_BYTES` goes as several events of the stream, which a
    /// consumer that is `set_chunked` too puts back together (one that is not sees the tagged
    /// chunks as events). Every producer and consumer of the stream must agree; set it before
    /// the first `send`. Not for compacted streams: a `put` is tagged and never cut.
    pub fn set_chunked(&mut self, on: bool) {
        self.chunked = on;
    }

    /// Buffers an event with its dedup id (live-ingestion's `redpanda-dedup-key`; empty: never
    /// deduplicated), sent as its 8-byte hash. `flush` sends it; flush when `is_full`.
    pub fn send(&mut self, time_us: u64, dedup_id: &[u8], payload: &[u8]) {
        let key = dedup_key(dedup_id);
        if !self.chunked {
            return self.push(time_us, &key, payload.to_vec());
        }
        // the id is its first chunk's: the others have none, so the broker's dedup does not
        // take them for copies of the first (a duplicate event loses its first chunk, and a
        // consumer passes over the rest)
        for (i, chunk) in chunks(payload, CHUNK_BYTES).enumerate() {
            self.push(time_us, if i == 0 { &key } else { &[] }, chunk);
        }
    }

    /// Buffers an event of a compacted stream: `key` is its compaction key, kept as is.
    pub fn put(&mut self, time_us: u64, key: &[u8], payload: &[u8]) {
        match self.chunked {
            true => self.push(time_us, key, [&[WHOLE][..], payload].concat()),
            false => self.push(time_us, key, payload.to_vec()),
        }
    }

    fn push(&mut self, time_us: u64, key: &[u8], payload: Vec<u8>) {
        self.bytes += 14 + key.len() + payload.len();
        self.events.push((time_us, key.to_vec(), payload));
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
    ///
    /// A chunked producer sends what is buffered as batches of about `MAX_BATCH_BYTES` (one
    /// event's chunks fill several), in order; the others send it as one batch, as before.
    pub async fn flush(&mut self) -> Result<()> {
        while !self.events.is_empty() {
            let mut n = self.events.len();
            if self.chunked {
                let mut bytes = 0;
                let sizes = self.events.iter().map(|e| 14 + e.1.len() + e.2.len());
                let fits = sizes.take_while(|size| {
                    bytes += size;
                    bytes <= MAX_BATCH_BYTES
                });
                n = fits.count().max(1);
            }
            while self.in_flight.len() >= MAX_IN_FLIGHT {
                self.in_flight.join_next().await.unwrap()??;
            }
            let evs: Vec<(u64, &[u8], &[u8])> = (self.events[..n].iter())
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
            self.next_seq += n as u64;
            self.events.drain(..n);
        }
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
    /// A chunked consumer: each writer log's event in the making, the position a reader may
    /// resume from (no later than the start of an event not yet yielded), and an error to
    /// report at the next poll.
    chunked: Option<std::collections::HashMap<LogId, Assembly>>,
    resume: Cursor,
    refused: Option<anyhow::Error>,
}

impl Consumer {
    /// Puts back together the events a chunked producer cut (`Producer::set_chunked`): set it
    /// before the first `poll`. `cursor` and `commit` then give a position at the start of an
    /// event, so a consumer that resumes reads the event it was inside from its first chunk.
    /// An event over `MAX_EVENT_BYTES`, or one without a chunk tag (the stream is not chunked),
    /// is passed over and reported as an error by the next `poll`.
    pub fn set_chunked(&mut self, on: bool) {
        self.chunked = on.then(Default::default);
        self.resume = self.cursor.clone();
    }

    /// The next events (about `max_bytes` of batches), per writer in sequence order; empty when
    /// caught up. Fails over to the next node on an error.
    pub async fn poll(&mut self, max_bytes: u32) -> Result<Vec<Event>> {
        // a batch can hold no event for this reader (dedup dropped them all, or the cursor is
        // past them): read on, so an empty poll still means caught up
        if let Some(e) = self.refused.take() {
            return Err(e);
        }
        loop {
            match self.poll_batches(max_bytes).await? {
                Some(evs) if evs.is_empty() => continue,
                evs => return Ok(evs.unwrap_or_default()),
            }
        }
    }

    /// One read: None when no node had a batch past the cursor.
    async fn poll_batches(&mut self, max_bytes: u32) -> Result<Option<Vec<Event>>> {
        let n = self.client.mc.node_count();
        let req = Control::ReadCursor {
            stream: self.stream.clone(),
            cursor: cursor_to_wire(&self.cursor, &self.stream),
            max_bytes,
        };
        let body = Bytes::from(serde_json::to_vec(&req)?);
        let (mut last, mut answered) = (None, false);
        // a node with nothing past the cursor may be behind (an acked batch is on 2 of 3): caught
        // up only when no node has more. ponytail: an empty poll asks every node; ask the merged
        // end (`Client::end`) first if tailing readers' empty polls cost too much
        for i in 0..n {
            let node = (self.node + i) % n;
            match self.client.mc.call(node, Kind::Control, body.clone()).await {
                Ok(f) if f.kind == Kind::Batches => {
                    answered = true;
                    if let Some(evs) = self.take(f.body)? {
                        self.node = node;
                        return Ok(Some(evs));
                    }
                }
                Ok(f) => last = Some(anyhow::anyhow!("unexpected {:?}", f.kind)),
                Err(e) => last = Some(e),
            }
        }
        if answered {
            return Ok(None);
        }
        Err(last.unwrap()).context("no node answered")
    }

    fn take(&mut self, mut body: Bytes) -> Result<Option<Vec<Event>>> {
        if body.remaining() < 4 {
            return Ok(None);
        }
        let mut out = vec![];
        while body.remaining() >= 4 {
            let len = body.get_u32_le() as usize;
            anyhow::ensure!(body.remaining() >= len, "truncated batches");
            let b = Batch::parse(body.split_to(len))?;
            let from = self.cursor.get(&b.id.log).copied().unwrap_or(0);
            let events = b.try_events()?.into_iter().filter(|e| e.seq >= from);
            self.cursor.insert(b.id.log.clone(), b.end_seq());
            let Some(logs) = self.chunked.as_mut() else {
                out.extend(events);
                continue;
            };
            let a = logs.entry(b.id.log.clone()).or_default();
            for e in events {
                match a.push(e) {
                    Ok(whole) => out.extend(whole),
                    Err(e) => self.refused = self.refused.take().or(Some(e)),
                }
            }
            let at = a.boundary().unwrap_or(b.end_seq());
            self.resume.insert(b.id.log.clone(), at);
        }
        Ok(Some(out))
    }

    /// Where the consumer is: a position valid on every node.
    pub fn cursor(&self) -> &Cursor {
        match self.chunked {
            Some(_) => &self.resume,
            None => &self.cursor,
        }
    }

    /// Stores the position as the group's (after the caller has made what it read durable).
    pub async fn commit(&self) -> Result<()> {
        let group = self
            .group
            .as_deref()
            .context("a consumer_at has no group to commit to")?;
        self.client.commit(group, &self.stream, self.cursor()).await
    }
}

/// The chunk codec and the reader's reassembly, as properties: whatever the events' sizes,
/// wherever a reader starts and whatever chunks it never sees, it yields only events that were
/// sent, whole, in order.
#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    /// The tagged events of `payloads` as one writer log's, chunked at `max` bytes.
    fn log_of(payloads: &[Vec<u8>], max: usize) -> Vec<Event> {
        let tagged = payloads.iter().flat_map(|p| chunks(p, max));
        (tagged.enumerate())
            .map(|(seq, payload)| Event {
                stream: "s".into(),
                writer: "w".into(),
                epoch: 1,
                seq: seq as u64,
                event_time_us: 7,
                payload: payload.into(),
            })
            .collect()
    }

    /// What a reader yields of the events it is handed, and where it could resume after each.
    fn read(events: impl IntoIterator<Item = Event>) -> (Vec<Vec<u8>>, Vec<u64>) {
        let (mut a, mut out, mut at) = (Assembly::default(), vec![], vec![]);
        for e in events {
            let next = e.seq + 1;
            out.extend(a.push(e).unwrap().map(|e| e.payload.to_vec()));
            at.push(a.boundary().unwrap_or(next));
        }
        (out, at)
    }

    fn payloads() -> impl Strategy<Value = Vec<Vec<u8>>> {
        let one = (any::<u8>(), 0usize..40).prop_map(|(b, n)| vec![b; n]);
        prop::collection::vec(one, 0..12)
    }

    proptest! {
        /// Why: an event of any size must come back as it was sent, byte for byte, whether it
        /// went as one chunk or many, and each chunk must fit the size it was cut for.
        #[test]
        fn any_events_round_trip_in_chunks_of_any_size(payloads in payloads(), max in 1usize..17) {
            let log = log_of(&payloads, max);
            prop_assert!(log.iter().all(|e| e.payload.len() <= max + 1), "a chunk and its tag");
            let (got, at) = read(log.clone());
            prop_assert_eq!(&got, &payloads);
            prop_assert_eq!(at.last().copied().unwrap_or(0), log.len() as u64, "all read");
        }

        /// Why: a reader resumes from a position it committed, or starts where retention left
        /// the stream, which may be inside an event. It must never yield a tail without its
        /// head: from any start it yields exactly the events that begin at or after it. And
        /// wherever it stops, the position it would commit is no later than the start of an
        /// event it has not yielded: resuming there loses nothing.
        #[test]
        fn a_reader_from_anywhere_yields_whole_events_and_resumes_at_an_event_s_start(
            payloads in payloads(), max in 1usize..17, start: prop::sample::Index, stop: prop::sample::Index,
        ) {
            let log = log_of(&payloads, max);
            // where each event begins in the log, and how many chunks it is
            let begins: Vec<(usize, usize)> = payloads.iter().scan(0, |at, p| {
                let n = chunks(p, max).count();
                *at += n;
                Some((*at - n, n))
            }).collect();
            let (start, stop) = (start.index(log.len() + 1), stop.index(log.len() + 1));
            let (from, to) = (start.min(stop), start.max(stop));
            let (got, at) = read(log[from..to].iter().cloned());
            let want: Vec<Vec<u8>> = (begins.iter().zip(&payloads))
                .filter(|((b, n), _)| *b >= from && b + n <= to)
                .map(|(_, p)| p.clone())
                .collect();
            prop_assert_eq!(&got, &want);
            // resuming at the committed position reads the rest, and no event twice
            let resume = at.last().copied().unwrap_or(from as u64) as usize;
            prop_assert!(resume <= to);
            let (rest, _) = read(log[resume..].iter().cloned());
            let after: Vec<Vec<u8>> = (begins.iter().zip(&payloads))
                .filter(|((b, _), _)| *b >= from)
                .map(|(_, p)| p.clone())
                .collect();
            prop_assert_eq!([got, rest].concat(), after);
        }

        /// Why: a reader does not see every chunk a producer sent: the broker drops an event's
        /// first chunk when its dedup id was seen (the others have none), retention cuts a
        /// stream inside an event, a producer dies between two chunks. Whatever is missing, the
        /// reader yields only events that were sent, each whole and in order: never the head
        /// of one with the tail of another.
        #[test]
        fn whatever_chunks_are_missing_a_reader_yields_only_events_that_were_sent(
            payloads in prop::collection::vec((1u8..250, 0usize..40), 0..12), max in 1usize..17,
            missing in prop::collection::vec(any::<bool>(), 0..200),
        ) {
            // (each event's bytes are its own: a mix of two would not equal either)
            let payloads: Vec<Vec<u8>> = (payloads.iter().enumerate())
                .map(|(i, (b, n))| [vec![i as u8; 1], vec![*b; *n]].concat())
                .collect();
            let log = log_of(&payloads, max);
            let seen = (log.into_iter().enumerate())
                .filter(|(i, _)| !missing.get(*i).copied().unwrap_or(false))
                .map(|x| x.1);
            let (got, _) = read(seen);
            let mut sent = payloads.iter();
            for e in &got {
                prop_assert!(sent.any(|p| p == e), "not sent, or out of order: {:?}", e);
            }
        }
    }

    #[test]
    fn an_event_past_the_readers_bound_is_refused_and_the_next_one_is_read() {
        // Why: a reader holds an event's chunks in memory until its last one. A writer that
        // never ends an event must not grow a reader without bound: past `MAX_EVENT_BYTES` the
        // event is refused (an error, once) and the reader goes on at the next event.
        let mut a = Assembly::default();
        let chunk = |seq: u64, tag: u8, n: usize| Event {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
            seq,
            event_time_us: 0,
            payload: [vec![tag], vec![0u8; n]].concat().into(),
        };
        let half = MAX_EVENT_BYTES / 2 + 1;
        assert!(a.push(chunk(0, FIRST, half)).unwrap().is_none());
        assert!(a.push(chunk(1, MIDDLE, half)).is_err());
        assert!(
            a.push(chunk(2, LAST, 1)).unwrap().is_none(),
            "its tail is passed over"
        );
        assert_eq!(a.boundary(), None);
        let next = a.push(chunk(3, WHOLE, 3)).unwrap().unwrap();
        assert_eq!((next.seq, next.payload.len()), (3, 3));
        // a tag no version of this reader wrote is an error too
        assert!(a.push(chunk(4, 9, 1)).is_err());
        assert!(
            a.push(Event {
                payload: Bytes::new(),
                ..chunk(5, 0, 0)
            })
            .is_err()
        );
    }
}
