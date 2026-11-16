//! Wire and on-disk formats.
//!
//! A *batch* is the unit of append, replication, storage and repair. It is
//! immutable once created by a writer and is identified by
//! `(stream, writer, epoch, first_seq)`; it covers sequences
//! `first_seq .. first_seq + count`. The same bytes are sent by the client,
//! replicated to peers and written to every node's WAL, so no node re-encodes
//! anything on the hot path.
//!
//! ```text
//! batch := magic "PCB1" | u16 stream_len | stream | u16 writer_len | writer
//!        | u64 epoch | u64 first_seq | u32 count
//!        | count * ( u64 event_time_us | u32 payload_len | payload )
//!        | u32 crc32c(everything before)
//! ```
//!
//! Event time is carried per event and is *not* used for identity,
//! deduplication or durability. Readers sort by `(event_time_us, writer,
//! epoch, seq)`.

use anyhow::{Context, Result, bail, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub const BATCH_MAGIC: &[u8; 4] = b"PCB1";
pub const MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_EVENTS_PER_BATCH: u32 = 65_536;
pub const MAX_NAME: usize = 255;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogId {
    pub stream: String,
    pub writer: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BatchId {
    pub log: LogId,
    pub first_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub stream: String,
    pub writer: String,
    pub epoch: u64,
    pub seq: u64,
    pub event_time_us: u64,
    #[serde(with = "payload_str")]
    pub payload: Bytes,
}

mod payload_str {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(b: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&String::from_utf8_lossy(b))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        Ok(Bytes::from(String::deserialize(d)?))
    }
}

/// A parsed, checksum-verified batch. Holds the original bytes.
#[derive(Clone, Debug)]
pub struct Batch {
    pub id: BatchId,
    pub count: u32,
    pub bytes: Bytes,
    /// Offset of the first event record inside `bytes`.
    events_off: usize,
}

impl Batch {
    pub fn encode(log: &LogId, first_seq: u64, events: &[(u64, &[u8])]) -> Result<Bytes> {
        ensure!(!events.is_empty(), "empty batch");
        ensure!(events.len() as u64 <= MAX_EVENTS_PER_BATCH as u64, "too many events");
        ensure!(log.stream.len() <= MAX_NAME && log.writer.len() <= MAX_NAME, "name too long");
        ensure!(!log.stream.is_empty() && !log.writer.is_empty(), "empty name");
        first_seq
            .checked_add(events.len() as u64)
            .context("sequence overflow")?;
        let payload: usize = events.iter().map(|(_, p)| 12 + p.len()).sum();
        let len = 4 + 2 + log.stream.len() + 2 + log.writer.len() + 8 + 8 + 4 + payload + 4;
        ensure!(len <= MAX_BATCH_BYTES, "batch too large ({len} bytes)");
        let mut b = BytesMut::with_capacity(len);
        b.put_slice(BATCH_MAGIC);
        b.put_u16_le(log.stream.len() as u16);
        b.put_slice(log.stream.as_bytes());
        b.put_u16_le(log.writer.len() as u16);
        b.put_slice(log.writer.as_bytes());
        b.put_u64_le(log.epoch);
        b.put_u64_le(first_seq);
        b.put_u32_le(events.len() as u32);
        for (t, p) in events {
            b.put_u64_le(*t);
            b.put_u32_le(p.len() as u32);
            b.put_slice(p);
        }
        let crc = crc32c::crc32c(&b);
        b.put_u32_le(crc);
        debug_assert_eq!(b.len(), len);
        Ok(b.freeze())
    }

    /// Parses and fully validates (checksum, structure, bounds) a batch.
    pub fn parse(bytes: Bytes) -> Result<Batch> {
        ensure!(bytes.len() <= MAX_BATCH_BYTES, "batch too large");
        ensure!(bytes.len() >= 4 + 2 + 2 + 20 + 4, "batch too short");
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        let crc = u32::from_le_bytes(crc.try_into().unwrap());
        ensure!(crc32c::crc32c(body) == crc, "batch checksum mismatch");
        let mut c = body;
        ensure!(&c[..4] == BATCH_MAGIC, "bad batch magic");
        c.advance(4);
        let stream = take_str(&mut c)?;
        let writer = take_str(&mut c)?;
        ensure!(c.remaining() >= 20, "truncated header");
        let epoch = c.get_u64_le();
        let first_seq = c.get_u64_le();
        let count = c.get_u32_le();
        ensure!(count > 0 && count <= MAX_EVENTS_PER_BATCH, "bad event count");
        first_seq.checked_add(count as u64).context("sequence overflow")?;
        let events_off = body.len() - c.remaining();
        for _ in 0..count {
            ensure!(c.remaining() >= 12, "truncated event");
            c.advance(8);
            let n = c.get_u32_le() as usize;
            ensure!(c.remaining() >= n, "truncated payload");
            c.advance(n);
        }
        ensure!(c.remaining() == 0, "trailing bytes in batch");
        Ok(Batch {
            id: BatchId { log: LogId { stream, writer, epoch }, first_seq },
            count,
            bytes,
            events_off,
        })
    }

    pub fn end_seq(&self) -> u64 {
        self.id.first_seq + self.count as u64
    }

    pub fn events(&self) -> impl Iterator<Item = Event> + '_ {
        let mut off = self.events_off;
        (0..self.count).map(move |i| {
            let t = u64::from_le_bytes(self.bytes[off..off + 8].try_into().unwrap());
            let n = u32::from_le_bytes(self.bytes[off + 8..off + 12].try_into().unwrap()) as usize;
            let payload = self.bytes.slice(off + 12..off + 12 + n);
            off += 12 + n;
            Event {
                stream: self.id.log.stream.clone(),
                writer: self.id.log.writer.clone(),
                epoch: self.id.log.epoch,
                seq: self.id.first_seq + i as u64,
                event_time_us: t,
                payload,
            }
        })
    }
}

fn take_str(c: &mut &[u8]) -> Result<String> {
    ensure!(c.remaining() >= 2, "truncated name");
    let n = c.get_u16_le() as usize;
    ensure!(n > 0 && n <= MAX_NAME && c.remaining() >= n, "bad name length");
    let s = std::str::from_utf8(&c[..n]).context("name not utf8")?.to_owned();
    c.advance(n);
    Ok(s)
}

// ---------------------------------------------------------------------------
// RPC frames
// ---------------------------------------------------------------------------
//
// frame := u32 len | u8 kind | u64 req_id | body          (len covers kind..body)
//
// Hot-path bodies (Append / Replicate / Ack) are hand-encoded; the rare
// control messages use JSON.

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Hello = 1,
    Append = 2,
    Replicate = 3,
    Ack = 4,
    Control = 5,
    ControlResp = 6,
    Error = 7,
    /// Response to `Control::Fetch`: repeated `u32 len | batch`.
    Batches = 8,
}

impl Kind {
    pub fn from_u8(v: u8) -> Result<Kind> {
        Ok(match v {
            1 => Kind::Hello,
            2 => Kind::Append,
            3 => Kind::Replicate,
            4 => Kind::Ack,
            5 => Kind::Control,
            6 => Kind::ControlResp,
            7 => Kind::Error,
            8 => Kind::Batches,
            _ => bail!("unknown frame kind {v}"),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Frame {
    pub kind: Kind,
    pub req_id: u64,
    pub body: Bytes,
}

impl Frame {
    pub fn new(kind: Kind, req_id: u64, body: Bytes) -> Self {
        Frame { kind, req_id, body }
    }
    pub fn encode_into(&self, out: &mut BytesMut) {
        out.reserve(13 + self.body.len());
        out.put_u32_le((9 + self.body.len()) as u32);
        out.put_u8(self.kind as u8);
        out.put_u64_le(self.req_id);
        out.put_slice(&self.body);
    }
}

/// Append body: u8 copies | batch
pub fn encode_append(copies: u8, batch: &Bytes) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + batch.len());
    b.put_u8(copies);
    b.put_slice(batch);
    b.freeze()
}

pub fn decode_append(mut body: Bytes) -> Result<(u8, Bytes)> {
    ensure!(!body.is_empty(), "empty append");
    let copies = body.get_u8();
    Ok((copies, body))
}

/// Replicate body: u32 origin node | batch
pub fn encode_replicate(origin: u32, batch: &Bytes) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + batch.len());
    b.put_u32_le(origin);
    b.put_slice(batch);
    b.freeze()
}

pub fn decode_replicate(mut body: Bytes) -> Result<(u32, Bytes)> {
    ensure!(body.len() >= 4, "short replicate");
    let origin = body.get_u32_le();
    Ok((origin, body))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckStatus {
    /// Stored durably by `copies` distinct failure domains.
    Ok = 0,
    /// Identical batch already present (idempotent retry). Same guarantees as Ok.
    Duplicate = 1,
    /// The batch identity is already used with different content.
    Conflict = 2,
    /// Fewer copies than requested were confirmed; the batch may or may not
    /// survive. Retrying the exact same batch is safe.
    Unknown = 3,
    /// Rejected before any side effect.
    Rejected = 4,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub status: AckStatus,
    /// Number of distinct failure domains with a durable copy at reply time.
    pub copies: u8,
    /// Bitmask of node ids (< 32) that confirmed a durable copy.
    pub nodes: u32,
    /// crc32c of the batch the replying node stored.
    pub crc: u32,
}

impl Ack {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(10);
        b.put_u8(self.status as u8);
        b.put_u8(self.copies);
        b.put_u32_le(self.nodes);
        b.put_u32_le(self.crc);
        b.freeze()
    }
    pub fn decode(mut b: Bytes) -> Result<Ack> {
        ensure!(b.len() == 10, "bad ack length");
        let status = match b.get_u8() {
            0 => AckStatus::Ok,
            1 => AckStatus::Duplicate,
            2 => AckStatus::Conflict,
            3 => AckStatus::Unknown,
            4 => AckStatus::Rejected,
            v => bail!("bad ack status {v}"),
        };
        Ok(Ack { status, copies: b.get_u8(), nodes: b.get_u32_le(), crc: b.get_u32_le() })
    }
    pub fn is_success(&self) -> bool {
        matches!(self.status, AckStatus::Ok | AckStatus::Duplicate)
    }
}

pub fn batch_crc(batch: &Bytes) -> u32 {
    u32::from_le_bytes(batch[batch.len() - 4..].try_into().unwrap())
}

/// Hello body: JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hello {
    pub token: String,
    /// Set when the connecting side is a mesh node (peer link).
    pub node_id: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Control {
    Status,
    /// Event-time ordered snapshot read of one stream from local durable state.
    Read { stream: String, from_time_us: u64, limit: u32 },
    /// Batch ids this node made durable after local index `after`.
    Inventory { after: u64, limit: u32 },
    /// Raw batch bytes for the given ids (missing ones are skipped).
    Fetch { ids: Vec<BatchId> },
    /// Run one anti-entropy pass against every peer now.
    Repair,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlResp {
    Status(Status),
    Read { events: Vec<Event>, coverage: Vec<Frontier> },
    Inventory { items: Vec<(u64, BatchId, u32, u32)>, next: u64, more: bool },
    Repair { fetched: u64, errors: Vec<String> },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Status {
    pub node_id: u32,
    pub domain: String,
    pub batches: u64,
    pub events: u64,
    pub bytes: u64,
    pub wal_bytes: u64,
    pub fsyncs: u64,
    pub logs: Vec<Frontier>,
}

/// Per writer-log coverage on one node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    pub log: LogId,
    /// All sequences `< contiguous` are present locally.
    pub contiguous: u64,
    /// One past the highest sequence present locally.
    pub high: u64,
}

pub fn hex_encode(b: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(H[(x >> 4) as usize] as char);
        s.push(H[(x & 15) as usize] as char);
    }
    s
}

pub fn hex_decode(s: &str) -> Result<Vec<u8>> {
    ensure!(s.len() % 2 == 0, "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).context("bad hex"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> LogId {
        LogId { stream: "s".into(), writer: "w".into(), epoch: 7 }
    }

    #[test]
    fn batch_roundtrip() {
        let b = Batch::encode(&log(), 10, &[(5, b"a"), (3, b"bc")]).unwrap();
        let p = Batch::parse(b).unwrap();
        assert_eq!(p.id.first_seq, 10);
        assert_eq!(p.end_seq(), 12);
        let ev: Vec<_> = p.events().collect();
        assert_eq!(ev[1].seq, 11);
        assert_eq!(ev[1].event_time_us, 3);
        assert_eq!(&ev[1].payload[..], b"bc");
    }

    #[test]
    fn batch_corruption_detected() {
        let b = Batch::encode(&log(), 0, &[(1, b"hello")]).unwrap();
        for i in 0..b.len() {
            let mut v = b.to_vec();
            v[i] ^= 0x40;
            assert!(Batch::parse(Bytes::from(v)).is_err(), "flip at {i} undetected");
        }
        assert!(Batch::parse(b.slice(..b.len() - 1)).is_err());
    }

    #[test]
    fn ack_roundtrip() {
        let a = Ack { status: AckStatus::Unknown, copies: 1, nodes: 0b101, crc: 99 };
        assert_eq!(Ack::decode(a.encode()).unwrap(), a);
    }
}
