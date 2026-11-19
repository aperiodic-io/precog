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

/// Batch format 2: every event has an optional dedup key, and a dropped event is a tombstone.
pub const BATCH_MAGIC: &[u8; 4] = b"PCB2";
/// Format 1 (no keys) still parses.
const BATCH_MAGIC_V1: &[u8; 4] = b"PCB1";
/// A payload length that marks a dropped event (no key, no payload).
const TOMBSTONE: u32 = u32::MAX;
/// `(time, Some((key, payload)))`, or `(time, None)` for a tombstone.
type RawEvent<'a> = (u64, Option<(&'a [u8], &'a [u8])>);
pub const MAX_KEY: usize = 1024;
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
    /// Format 1: events without keys.
    v1: bool,
    /// The batch's identity checksum: its own CRC, or the CRC of the batch it was rewritten from
    /// (dedup tombstones), so a producer's retry of the original bytes still matches.
    pub ident: u32,
}

impl Batch {
    /// Events without dedup keys: format 1, which has no key or tombstone fields to write or check.
    pub fn encode(log: &LogId, first_seq: u64, events: &[(u64, &[u8])]) -> Result<Bytes> {
        ensure!(!events.is_empty(), "empty batch");
        ensure!(
            events.len() as u64 <= MAX_EVENTS_PER_BATCH as u64,
            "too many events"
        );
        ensure!(
            log.stream.len() <= MAX_NAME && log.writer.len() <= MAX_NAME,
            "name too long"
        );
        ensure!(
            !log.stream.is_empty() && !log.writer.is_empty(),
            "empty name"
        );
        first_seq
            .checked_add(events.len() as u64)
            .context("sequence overflow")?;
        let payload: usize = events.iter().map(|(_, p)| 12 + p.len()).sum();
        let len = 4 + 2 + log.stream.len() + 2 + log.writer.len() + 8 + 8 + 4 + payload + 4;
        ensure!(len <= MAX_BATCH_BYTES, "batch too large ({len} bytes)");
        let mut b = BytesMut::with_capacity(len);
        b.put_slice(BATCH_MAGIC_V1);
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

    /// Events of `(time, dedup key, payload)`; an empty key is never deduplicated.
    pub fn encode_keyed(
        log: &LogId,
        first_seq: u64,
        events: &[(u64, &[u8], &[u8])],
    ) -> Result<Bytes> {
        let events: Vec<RawEvent<'_>> = events.iter().map(|&(t, k, p)| (t, Some((k, p)))).collect();
        Self::encode_events(log, first_seq, 0, &events)
    }

    /// `None` is a tombstone.
    /// `ident` 0: the batch is its own identity.
    fn encode_events(
        log: &LogId,
        first_seq: u64,
        ident: u32,
        events: &[RawEvent<'_>],
    ) -> Result<Bytes> {
        ensure!(!events.is_empty(), "empty batch");
        ensure!(
            events.len() as u64 <= MAX_EVENTS_PER_BATCH as u64,
            "too many events"
        );
        ensure!(
            log.stream.len() <= MAX_NAME && log.writer.len() <= MAX_NAME,
            "name too long"
        );
        ensure!(
            !log.stream.is_empty() && !log.writer.is_empty(),
            "empty name"
        );
        first_seq
            .checked_add(events.len() as u64)
            .context("sequence overflow")?;
        for (_, e) in events {
            if let Some((k, p)) = e {
                ensure!(k.len() <= MAX_KEY, "dedup key too long");
                ensure!(p.len() < TOMBSTONE as usize, "payload too large");
            }
        }
        let payload: usize = events
            .iter()
            .map(|(_, e)| 14 + e.map_or(0, |(k, p)| k.len() + p.len()))
            .sum();
        let len = 4 + 2 + log.stream.len() + 2 + log.writer.len() + 8 + 8 + 4 + 4 + payload + 4;
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
        b.put_u32_le(ident);
        for (t, e) in events {
            b.put_u64_le(*t);
            let (k, p) = e.unwrap_or((b"", b""));
            b.put_u16_le(k.len() as u16);
            b.put_u32_le(if e.is_some() {
                p.len() as u32
            } else {
                TOMBSTONE
            });
            b.put_slice(k);
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
        let v1 = &c[..4] == BATCH_MAGIC_V1;
        ensure!(v1 || &c[..4] == BATCH_MAGIC, "bad batch magic");
        c.advance(4);
        let stream = take_str(&mut c)?;
        let writer = take_str(&mut c)?;
        ensure!(c.remaining() >= 20, "truncated header");
        let epoch = c.get_u64_le();
        let first_seq = c.get_u64_le();
        let count = c.get_u32_le();
        let ident = if v1 {
            0
        } else {
            ensure!(c.remaining() >= 4, "truncated header");
            c.get_u32_le()
        };
        // ponytail: a batch whose own CRC is 0 (1 in 2^32) cannot be named as an identity, so a
        // retry of it after a dedup rewrite is refused as a conflict; widen ident if that matters
        let ident = if ident == 0 { crc } else { ident };
        ensure!(
            count > 0 && count <= MAX_EVENTS_PER_BATCH,
            "bad event count"
        );
        first_seq
            .checked_add(count as u64)
            .context("sequence overflow")?;
        let events_off = body.len() - c.remaining();
        for _ in 0..count {
            let n = if v1 {
                // format 1: time and payload length only
                ensure!(c.remaining() >= 12, "truncated event");
                c.advance(8);
                let n = c.get_u32_le();
                ensure!(n != TOMBSTONE, "bad payload length");
                n as usize
            } else {
                let (k, n) = take_event(&mut c, false)?;
                k + n.unwrap_or(0)
            };
            ensure!(c.remaining() >= n, "truncated payload");
            c.advance(n);
        }
        ensure!(c.remaining() == 0, "trailing bytes in batch");
        Ok(Batch {
            id: BatchId {
                log: LogId {
                    stream,
                    writer,
                    epoch,
                },
                first_seq,
            },
            count,
            bytes,
            events_off,
            v1,
            ident,
        })
    }

    /// `(time, key, payload)` of every event, `None` for a tombstone.
    fn raw(&self) -> impl Iterator<Item = RawEvent<'_>> + '_ {
        let mut c = &self.bytes[self.events_off..self.bytes.len() - 4];
        (0..self.count).map(move |_| {
            let t = u64::from_le_bytes(c[..8].try_into().unwrap());
            let (k, n) = take_event(&mut c, self.v1).expect("validated by parse");
            let e = n.map(|n| (&c[..k], &c[k..k + n]));
            c.advance(k + n.unwrap_or(0));
            (t, e)
        })
    }

    /// Each event's dedup key; `None` for no key or a tombstone.
    pub fn keys(&self) -> impl Iterator<Item = Option<&[u8]>> + '_ {
        self.raw()
            .map(|(_, e)| e.map(|(k, _)| k).filter(|k| !k.is_empty()))
    }

    /// This batch with the flagged events replaced by tombstones (same identity and sequence).
    pub fn without(&self, drop: &[bool]) -> Result<Bytes> {
        ensure!(drop.len() == self.count as usize, "one flag per event");
        if !drop.contains(&true) {
            return Ok(self.bytes.clone());
        }
        let events: Vec<_> = self
            .raw()
            .zip(drop)
            .map(|((t, e), &d)| (t, e.filter(|_| !d)))
            .collect();
        Self::encode_events(&self.id.log, self.id.first_seq, self.ident, &events)
    }

    pub fn end_seq(&self) -> u64 {
        self.id.first_seq + self.count as u64
    }

    /// The batch's events, without tombstones.
    pub fn events(&self) -> impl Iterator<Item = Event> + '_ {
        self.raw().enumerate().filter_map(move |(i, (t, e))| {
            let (_, p) = e?;
            Some(Event {
                stream: self.id.log.stream.clone(),
                writer: self.id.log.writer.clone(),
                epoch: self.id.log.epoch,
                seq: self.id.first_seq + i as u64,
                event_time_us: t,
                payload: self.bytes.slice_ref(p),
            })
        })
    }
}

/// Skips the event time; returns (key length, payload length), `None` for a tombstone.
fn take_event(c: &mut &[u8], v1: bool) -> Result<(usize, Option<usize>)> {
    if v1 {
        ensure!(c.remaining() >= 12, "truncated event");
        c.advance(8);
        let n = c.get_u32_le();
        ensure!(n != TOMBSTONE, "bad payload length");
        return Ok((0, Some(n as usize)));
    }
    ensure!(c.remaining() >= 14, "truncated event");
    c.advance(8);
    let k = c.get_u16_le() as usize;
    let n = c.get_u32_le();
    ensure!(k <= MAX_KEY, "dedup key too long");
    if n == TOMBSTONE {
        ensure!(k == 0, "tombstone with a key");
        return Ok((0, None));
    }
    Ok((k, Some(n as usize)))
}

fn take_str(c: &mut &[u8]) -> Result<String> {
    ensure!(c.remaining() >= 2, "truncated name");
    let n = c.get_u16_le() as usize;
    ensure!(
        n > 0 && n <= MAX_NAME && c.remaining() >= n,
        "bad name length"
    );
    let s = std::str::from_utf8(&c[..n])
        .context("name not utf8")?
        .to_owned();
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
    /// Open a live subscription (JSON `Subscribe`); answered by `Push` frames
    /// carrying the subscription's req_id, each one raw batch.
    Subscribe = 9,
    Push = 10,
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
            9 => Kind::Subscribe,
            10 => Kind::Push,
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
        Ok(Ack {
            status,
            copies: b.get_u8(),
            nodes: b.get_u32_le(),
            crc: b.get_u32_le(),
        })
    }
    pub fn is_success(&self) -> bool {
        matches!(self.status, AckStatus::Ok | AckStatus::Duplicate)
    }
}

/// Storage key of a writer log: `u16 len | stream | u16 len | writer | u64 epoch`.
pub fn encode_log_key(log: &LogId) -> Vec<u8> {
    let mut v = Vec::with_capacity(12 + log.stream.len() + log.writer.len());
    v.extend_from_slice(&(log.stream.len() as u16).to_le_bytes());
    v.extend_from_slice(log.stream.as_bytes());
    v.extend_from_slice(&(log.writer.len() as u16).to_le_bytes());
    v.extend_from_slice(log.writer.as_bytes());
    v.extend_from_slice(&log.epoch.to_le_bytes());
    v
}

pub fn decode_log_key(mut b: &[u8]) -> Result<LogId> {
    let stream = take_str(&mut b)?;
    let writer = take_str(&mut b)?;
    ensure!(b.len() == 8, "bad log key");
    Ok(LogId {
        stream,
        writer,
        epoch: u64::from_le_bytes(b.try_into().unwrap()),
    })
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

/// Live subscription request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subscribe {
    /// Only this stream (all streams if None).
    pub stream: Option<String>,
    /// Start from the node's first durable batch (`true`) or from now.
    pub from_start: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Control {
    Status,
    /// Event-time ordered snapshot read of one stream from local durable state.
    Read {
        stream: String,
        from_time_us: u64,
        limit: u32,
    },
    /// Batch ids this node made durable after local index `after`.
    Inventory {
        after: u64,
        limit: u32,
    },
    /// Raw batch bytes for the given ids (missing ones are skipped).
    Fetch {
        ids: Vec<BatchId>,
    },
    /// Run one anti-entropy pass against every peer now.
    Repair,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlResp {
    Status(Status),
    Read {
        events: Vec<Event>,
        coverage: Vec<Frontier>,
    },
    Inventory {
        items: Vec<(u64, BatchId, u32, u32)>,
        next: u64,
        more: bool,
    },
    Repair {
        fetched: u64,
        errors: Vec<String>,
    },
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
    /// Segment files in the node's log.
    #[serde(default)]
    pub segments: u64,
    /// Bytes the last startup had to scan to recover (only the active segment).
    #[serde(default)]
    pub recovery_scanned_bytes: u64,
    /// Events dropped as duplicates of an event seen within the dedup window.
    #[serde(default)]
    pub dedup_dropped: u64,
    /// Dedup ids held, and how far back they reach (the window, or less at the entry cap).
    #[serde(default)]
    pub dedup_entries: u64,
    #[serde(default)]
    pub dedup_window_s: u64,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> LogId {
        LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 7,
        }
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
            assert!(
                Batch::parse(Bytes::from(v)).is_err(),
                "flip at {i} undetected"
            );
        }
        assert!(Batch::parse(b.slice(..b.len() - 1)).is_err());
    }

    #[test]
    fn keyed_batches_carry_a_dedup_key_per_event() {
        let b = Batch::encode_keyed(
            &log(),
            4,
            &[(1, b"k1", b"a"), (2, b"", b"bb"), (3, b"k3", b"")],
        )
        .unwrap();
        let p = Batch::parse(b).unwrap();
        let keys: Vec<Option<&[u8]>> = p.keys().collect();
        assert_eq!(keys, vec![Some(&b"k1"[..]), None, Some(&b"k3"[..])]);
        let ev: Vec<_> = p.events().collect();
        assert_eq!(ev.len(), 3);
        assert_eq!((&ev[1].payload[..], ev[2].seq), (&b"bb"[..], 6));
    }

    #[test]
    fn dropping_events_keeps_their_sequence_numbers_and_shrinks_the_batch() {
        // Dedup replaces a duplicate with a tombstone: readers skip it, but the writer's sequence
        // stays contiguous, so a cursor past it is exact on every node.
        let big = vec![7u8; 1000];
        let b = Batch::encode_keyed(
            &log(),
            10,
            &[(1, b"a", &big), (2, b"b", b"keep"), (3, b"c", &big)],
        )
        .unwrap();
        let p = Batch::parse(b.clone()).unwrap();
        let f = Batch::parse(p.without(&[true, false, true]).unwrap()).unwrap();
        assert_eq!((f.id.clone(), f.count, f.end_seq()), (p.id.clone(), 3, 13));
        assert!(
            f.bytes.len() < b.len() - 1900,
            "tombstones carry no payload"
        );
        let ev: Vec<_> = f.events().collect();
        assert_eq!(ev.len(), 1);
        assert_eq!((ev[0].seq, &ev[0].payload[..]), (11, &b"keep"[..]));
        assert_eq!(
            f.keys().collect::<Vec<_>>(),
            vec![None, Some(&b"b"[..]), None]
        );
        // dropping nothing returns the batch as it was
        assert_eq!(p.without(&[false, false, false]).unwrap(), b);
        assert!(p.without(&[true]).is_err(), "one flag per event");
    }

    #[test]
    fn a_batch_keeps_its_identity_when_events_are_dropped() {
        // A producer's retry of the bytes it sent must match what the broker stored after dedup,
        // on every node: the rewritten batch carries the original's checksum as its identity.
        let b = Batch::encode_keyed(&log(), 0, &[(1, b"a", b"x"), (2, b"b", b"y")]).unwrap();
        let p = Batch::parse(b.clone()).unwrap();
        assert_eq!(p.ident, batch_crc(&b));
        let f = Batch::parse(p.without(&[true, false]).unwrap()).unwrap();
        assert_ne!(batch_crc(&f.bytes), batch_crc(&b));
        assert_eq!(f.ident, p.ident);
        // dropping more from an already rewritten batch keeps the first identity
        let g = Batch::parse(f.without(&[false, true]).unwrap()).unwrap();
        assert_eq!(g.ident, p.ident);
    }

    #[test]
    fn version_1_batches_still_read_as_keyless() {
        // PCB1: magic, names, epoch, first_seq, count, then (time u64, len u32, payload) per event
        let l = log();
        let mut v = b"PCB1".to_vec();
        for n in [&l.stream, &l.writer] {
            v.extend_from_slice(&(n.len() as u16).to_le_bytes());
            v.extend_from_slice(n.as_bytes());
        }
        v.extend_from_slice(&l.epoch.to_le_bytes());
        v.extend_from_slice(&3u64.to_le_bytes());
        v.extend_from_slice(&2u32.to_le_bytes());
        for (t, p) in [(9u64, &b"x"[..]), (8, b"yz")] {
            v.extend_from_slice(&t.to_le_bytes());
            v.extend_from_slice(&(p.len() as u32).to_le_bytes());
            v.extend_from_slice(p);
        }
        let c = crc32c::crc32c(&v);
        v.extend_from_slice(&c.to_le_bytes());
        let p = Batch::parse(Bytes::from(v)).unwrap();
        assert_eq!(p.keys().collect::<Vec<_>>(), vec![None, None]);
        let ev: Vec<_> = p.events().collect();
        assert_eq!((ev[1].seq, &ev[1].payload[..]), (4, &b"yz"[..]));
    }

    #[test]
    fn keyed_batch_corruption_detected() {
        let b = Batch::encode_keyed(&log(), 0, &[(1, b"id", b"hello")]).unwrap();
        let t = Batch::parse(b.clone()).unwrap().without(&[true]).unwrap();
        for x in [b, t] {
            for i in 0..x.len() {
                let mut v = x.to_vec();
                v[i] ^= 0x40;
                assert!(
                    Batch::parse(Bytes::from(v)).is_err(),
                    "flip at {i} undetected"
                );
            }
        }
    }

    #[test]
    fn log_key_roundtrip() {
        let l = log();
        assert_eq!(decode_log_key(&encode_log_key(&l)).unwrap(), l);
        assert!(decode_log_key(&encode_log_key(&l)[..5]).is_err());
    }

    #[test]
    fn ack_roundtrip() {
        let a = Ack {
            status: AckStatus::Unknown,
            copies: 1,
            nodes: 0b101,
            crc: 99,
        };
        assert_eq!(Ack::decode(a.encode()).unwrap(), a);
    }
}
