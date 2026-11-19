//! The node's index over its segment log.
//!
//! Every writer log `(stream, writer, epoch)` is an independent sequence space. Batches occupy
//! non-overlapping sequence ranges; an exact retry is idempotent, any other overlap is a
//! conflict. A batch becomes visible (reads, inventory, subscriptions) only once the segment
//! log has made it durable. Only metadata lives in RAM (range, checksum, length, LSN); payloads
//! are read back through the log's index and tail cache.
//!
//! ponytail: ~150 bytes of RAM per retained batch (tests/memory.rs), on top of the segment log's
//! own index; move the per-log index to disk if retention keeps more than ~10M batches per node.

use crate::proto::{Batch, BatchId, Event, Frontier, LogId, decode_log_key};
use parking_lot::Mutex;
use precog_storage::{Lsn, Reader};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Durable,
    Failed,
}

/// An entry's LSN until the segment log has made it durable.
const PENDING: Lsn = Lsn::MAX;

/// A batch, keyed by its writer log and first sequence. Its length, count and checksum decide
/// duplicates; its LSN is where its payload is.
#[derive(Clone, Copy, Debug)]
struct Entry {
    lsn: Lsn,
    count: u32,
    crc: u32,
    len: u32,
}

impl Entry {
    fn durable(&self) -> bool {
        self.lsn != PENDING
    }
}

/// One writer log. Kept for good once seen: its frontier outlives its batches.
struct Log {
    id: LogId,
    batches: BTreeMap<u64, Entry>,
    /// Sequences below this were trimmed by retention.
    trimmed: u64,
    /// Every sequence below this is durable here or trimmed (kept as batches arrive, so
    /// frontiers, and with them repair, cost O(logs) instead of O(batches)).
    contig: u64,
}

impl Log {
    /// The end of its last durable batch.
    fn high(&self) -> Option<u64> {
        self.batches
            .iter()
            .rev()
            .find(|(_, e)| e.durable())
            .map(|(&s, e)| s + e.count as u64)
    }
}

pub enum Reserve {
    /// New range reserved; the caller writes it to the log, then calls `mark_durable` or
    /// `mark_failed`.
    New,
    /// Identical batch already present or pending; wait for its state.
    Duplicate(watch::Receiver<State>),
    /// Range overlaps different content.
    Conflict(String),
    /// The range lies below this node's retention floor for that writer log.
    Expired,
}

/// Writer logs are interned: a batch refers to its log by index, so no map holds a copy of its
/// stream and writer names per batch.
#[derive(Default)]
struct Inner {
    logs: Vec<Log>,
    ix: HashMap<LogId, u32>,
    streams: HashMap<String, StreamLog>,
    /// Durable batches by LSN: (log, len, first seq).
    order: BTreeMap<Lsn, (u32, u32, u64)>,
    /// What waits on a batch not yet durable (a concurrent duplicate); dropped once it is
    /// durable or failed, as a later duplicate needs no wait.
    pending: HashMap<(u32, u64), watch::Sender<State>>,
    /// Every LSN below this is in `order` (or trimmed). Subscriptions read only below it:
    /// batches are marked durable in any order, but a reader must never skip one.
    contiguous: Lsn,
    events: u64,
    bytes: u64,
}

#[derive(Default)]
struct StreamLog {
    /// The stream's writer logs.
    logs: BTreeSet<u32>,
    /// Its durable batches by LSN: append time ms, for its own retention.
    order: BTreeMap<Lsn, u64>,
    bytes: u64,
}

/// A reader's position in a stream, valid on every node: per writer log, the next sequence to
/// read. A log missing from it is read from its start.
pub type Cursor = BTreeMap<LogId, u64>;

pub fn cursor_to_wire(c: &Cursor, stream: &str) -> crate::proto::WireCursor {
    c.iter()
        .filter(|(l, _)| l.stream == stream)
        .map(|(l, s)| (l.writer.clone(), l.epoch, *s))
        .collect()
}

pub fn cursor_from_wire(w: crate::proto::WireCursor, stream: &str) -> Cursor {
    w.into_iter()
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
        .collect()
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

impl Inner {
    fn intern(&mut self, log: &LogId) -> u32 {
        if let Some(&i) = self.ix.get(log) {
            return i;
        }
        let i = self.logs.len() as u32;
        self.logs.push(Log {
            id: log.clone(),
            batches: BTreeMap::new(),
            trimmed: 0,
            contig: 0,
        });
        self.ix.insert(log.clone(), i);
        i
    }

    fn log(&self, log: &LogId) -> Option<&Log> {
        self.ix.get(log).map(|&i| &self.logs[i as usize])
    }

    /// Adds the log to its stream's.
    fn join(&mut self, ix: u32) {
        let stream = &self.logs[ix as usize].id.stream;
        match self.streams.get_mut(stream) {
            Some(s) => {
                s.logs.insert(ix);
            }
            None => {
                let s = StreamLog {
                    logs: BTreeSet::from([ix]),
                    ..Default::default()
                };
                self.streams.insert(stream.clone(), s);
            }
        }
    }

    fn index(&mut self, lsn: Lsn, ix: u32, first: u64, e: Entry, time_ms: u64) {
        self.order.insert(lsn, (ix, e.len, first));
        self.events += e.count as u64;
        self.bytes += e.len as u64;
        let s = self
            .streams
            .get_mut(&self.logs[ix as usize].id.stream)
            .expect("a log joins its stream when reserved");
        s.order.insert(lsn, time_ms);
        s.bytes += e.len as u64;
        self.advance(ix);
    }

    /// Moves the log's contiguous end past every durable batch that now follows it.
    fn advance(&mut self, ix: u32) {
        let l = &mut self.logs[ix as usize];
        let mut c = l.contig.max(l.trimmed);
        while let Some(e) = l.batches.get(&c).filter(|e| e.durable()) {
            c += e.count as u64;
        }
        l.contig = c;
    }

    /// Forgets the durable batch at `lsn` (retention): later copies of it are `Expired`.
    fn forget(&mut self, lsn: Lsn) {
        let Some((ix, len, first)) = self.order.remove(&lsn) else {
            return;
        };
        let l = &mut self.logs[ix as usize];
        let count = l.batches.remove(&first).map_or(0, |e| e.count);
        l.trimmed = l.trimmed.max(first + count as u64);
        self.events -= count as u64;
        self.bytes -= len as u64;
        if let Some(s) = self.streams.get_mut(&l.id.stream)
            && s.order.remove(&lsn).is_some()
        {
            s.bytes -= len as u64;
        }
        self.advance(ix);
    }
}

pub struct Store {
    inner: Mutex<Inner>,
    reader: Reader,
    /// Woken whenever a batch becomes durable (live subscriptions).
    pub durable_notify: tokio::sync::Notify,
}

impl Store {
    /// Rebuilds the index from the segment log's entries.
    pub fn load(reader: Reader, entries: Vec<precog_storage::Entry>) -> anyhow::Result<Store> {
        let s = Store {
            inner: Mutex::default(),
            reader,
            durable_notify: tokio::sync::Notify::new(),
        };
        {
            let mut g = s.inner.lock();
            let inner = &mut *g;
            inner.contiguous = s.reader.next_lsn(); // everything recovered is durable
            for e in entries {
                let log = decode_log_key(&e.meta.key)?;
                let ix = inner.intern(&log);
                let first = e.meta.first_seq;
                let new = Entry {
                    lsn: e.lsn,
                    count: e.meta.count,
                    crc: e.meta.tag,
                    len: e.payload_len,
                };
                let map = &mut inner.logs[ix as usize].batches;
                if let Some((&s0, x)) = map.range(..=first).next_back()
                    && s0 + x.count as u64 > first
                {
                    if s0 == first && x.crc == new.crc && x.count == new.count {
                        continue; // logged twice (a repair raced a replicate): keep the first
                    }
                    anyhow::bail!(
                        "conflicting batches in the log at lsn {}: {:?} seq {}",
                        e.lsn,
                        log,
                        first
                    );
                }
                map.insert(first, new);
                inner.join(ix);
                inner.index(e.lsn, ix, first, new, e.meta.time_ms);
            }
        }
        Ok(s)
    }

    pub fn reserve(&self, b: &Batch) -> Reserve {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let first = b.id.first_seq;
        let end = b.end_seq();
        if inner.log(&b.id.log).is_some_and(|l| first < l.trimmed) {
            return Reserve::Expired;
        }
        let ix = inner.intern(&b.id.log);
        let log = &mut inner.logs[ix as usize].batches;
        if let Some((&s, e)) = log.range(..=first).next_back()
            && s + e.count as u64 > first
        {
            if s == first && e.crc == b.ident && e.count == b.count {
                return Reserve::Duplicate(match inner.pending.get(&(ix, first)) {
                    Some(tx) => tx.subscribe(),
                    None => watch::channel(State::Durable).1,
                });
            }
            return Reserve::Conflict(format!(
                "{:?} seq {first}..{end} overlaps stored {s}..{}",
                b.id.log,
                s + e.count as u64
            ));
        }
        if let Some((&s, _)) = log.range(first + 1..).next()
            && s < end
        {
            return Reserve::Conflict(format!(
                "{:?} seq {first}..{end} overlaps stored batch at {s}",
                b.id.log
            ));
        }
        log.insert(
            first,
            Entry {
                lsn: PENDING,
                count: b.count,
                crc: b.ident,
                len: b.bytes.len() as u32,
            },
        );
        inner
            .pending
            .insert((ix, first), watch::channel(State::Pending).0);
        inner.join(ix);
        Reserve::New
    }

    pub fn mark_durable(&self, id: &BatchId, lsn: Lsn) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let Some(&ix) = inner.ix.get(&id.log) else {
            return;
        };
        let first = id.first_seq;
        let Some(e) = inner.logs[ix as usize].batches.get_mut(&first) else {
            return;
        };
        if e.durable() {
            return;
        }
        e.lsn = lsn;
        let e = *e;
        if let Some(tx) = inner.pending.remove(&(ix, first)) {
            tx.send_replace(State::Durable);
        }
        inner.index(lsn, ix, first, e, now_ms());
        while inner.order.contains_key(&inner.contiguous) {
            inner.contiguous += 1;
        }
        drop(g);
        self.durable_notify.notify_waiters();
    }

    pub fn mark_failed(&self, id: &BatchId) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        if let Some(&ix) = inner.ix.get(&id.log)
            && let Some(tx) = inner.pending.remove(&(ix, id.first_seq))
        {
            inner.logs[ix as usize].batches.remove(&id.first_seq);
            tx.send_replace(State::Failed);
        }
    }

    /// Forgets every batch below the log's new first LSN (retention).
    pub fn trim(&self, floor: Lsn) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        inner.contiguous = inner.contiguous.max(floor);
        let gone: Vec<Lsn> = inner.order.range(..floor).map(|(&l, _)| l).collect();
        for lsn in gone {
            inner.forget(lsn);
        }
    }

    /// One stream's own retention: drops its oldest batches while it holds more than
    /// `max_bytes`, or while they are older than `max_age_ms` at `now_ms`. Returns how many.
    pub fn trim_stream(
        &self,
        stream: &str,
        max_bytes: Option<u64>,
        max_age_ms: Option<u64>,
        now_ms: u64,
    ) -> usize {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let mut n = 0;
        while let Some(s) = inner.streams.get(stream)
            && let Some((&lsn, &time)) = s.order.first_key_value()
            && (max_bytes.is_some_and(|m| s.bytes > m)
                || max_age_ms.is_some_and(|a| now_ms.saturating_sub(time) > a))
        {
            inner.forget(lsn);
            n += 1;
        }
        n
    }

    /// Durable copy with this exact checksum present (or aged out by retention)?
    pub fn has(&self, id: &BatchId, crc: u32) -> bool {
        let g = self.inner.lock();
        let Some(l) = g.log(&id.log) else {
            return false;
        };
        id.first_seq < l.trimmed
            || l.batches
                .get(&id.first_seq)
                .is_some_and(|e| e.durable() && e.crc == crc)
    }

    fn lsn_of(&self, id: &BatchId) -> Option<Lsn> {
        let g = self.inner.lock();
        let e = g.log(&id.log)?.batches.get(&id.first_seq)?;
        e.durable().then_some(e.lsn)
    }

    fn read_batch(&self, lsn: Lsn) -> Option<Batch> {
        match self.reader.read(lsn) {
            Ok(Some(r)) => Batch::parse(bytes::Bytes::from(r.payload.to_vec())).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::error!("reading lsn {lsn}: {e}");
                None
            }
        }
    }

    pub fn get(&self, id: &BatchId) -> Option<Batch> {
        self.read_batch(self.lsn_of(id)?)
    }

    /// Durable batches at LSNs `from..`, optionally only one stream, up to about `max_bytes`.
    /// Returns the batches and the next LSN to ask for.
    pub fn tail(&self, from: u64, stream: Option<&str>, max_bytes: usize) -> (Vec<Batch>, u64) {
        let picks: Vec<(Lsn, bool)> = {
            let g = self.inner.lock();
            let mut size = 0usize;
            let mut v = vec![];
            for (&lsn, &(ix, len, _)) in g.order.range(from..g.contiguous.max(from)) {
                if size >= max_bytes {
                    break;
                }
                let want = stream.is_none_or(|s| g.logs[ix as usize].id.stream == s);
                if want {
                    size += len as usize;
                }
                v.push((lsn, want));
            }
            v
        };
        let next = picks.last().map_or(from, |x| x.0 + 1);
        let out = picks
            .into_iter()
            .filter(|x| x.1)
            .filter_map(|(lsn, _)| self.read_batch(lsn))
            .collect();
        (out, next)
    }

    /// The stream's durable batches the cursor has not covered, in this node's order, up to about
    /// `max_bytes`, and the cursor past them. A writer's batch waits until its predecessor is
    /// here (per-writer order); nothing is skipped (only LSNs below `contiguous` are read).
    pub fn read_cursor(
        &self,
        stream: &str,
        cursor: &Cursor,
        max_bytes: usize,
    ) -> (Vec<bytes::Bytes>, Cursor) {
        let mut next = cursor.clone();
        let lsns: Vec<Lsn> = {
            let g = self.inner.lock();
            let Some(sl) = g.streams.get(stream) else {
                return (vec![], next);
            };
            let from = |l: &Log| cursor.get(&l.id).copied().unwrap_or(l.trimmed);
            // start at the earliest LSN of an unread batch of any of the stream's logs
            let start = sl
                .logs
                .iter()
                .filter_map(|&ix| {
                    let l = &g.logs[ix as usize];
                    let c = from(l);
                    let (_, e) = l
                        .batches
                        .range(..=c)
                        .next_back()
                        .filter(|(s, e)| **s + e.count as u64 > c)
                        .or_else(|| l.batches.range(c..).next())?;
                    e.durable().then_some(e.lsn)
                })
                .min();
            let Some(start) = start else {
                return (vec![], next);
            };
            let (mut size, mut out, mut blocked) = (0usize, vec![], BTreeSet::new());
            for &lsn in sl.order.range(start..g.contiguous.max(start)).map(|x| x.0) {
                if size >= max_bytes {
                    break;
                }
                let (ix, len, first) = g.order[&lsn];
                let l = &g.logs[ix as usize];
                let c = next.get(&l.id).copied().unwrap_or_else(|| from(l));
                let end = first + l.batches[&first].count as u64;
                if blocked.contains(&ix) || end <= c {
                    continue;
                }
                if first > c {
                    blocked.insert(ix); // its predecessor is not here yet
                    continue;
                }
                next.insert(l.id.clone(), end);
                size += len as usize;
                out.push(lsn);
            }
            out
        };
        let bytes = lsns
            .into_iter()
            .filter_map(|lsn| self.read_batch(lsn).map(|b| b.bytes))
            .collect();
        (bytes, next)
    }

    /// A cursor at the first batch this node logged at or after `time_ms`; `u64::MAX`: the end.
    pub fn cursor_at(&self, stream: &str, time_ms: u64) -> Cursor {
        let g = self.inner.lock();
        let mut c = Cursor::new();
        let Some(sl) = g.streams.get(stream) else {
            return c;
        };
        // the end (u64::MAX) is polled often: no batch is at or after it, so skip the scan
        if time_ms != u64::MAX {
            for (lsn, t) in &sl.order {
                let (ix, _, first) = g.order[lsn];
                let log = &g.logs[ix as usize].id;
                if *t >= time_ms && !c.contains_key(log) {
                    c.insert(log.clone(), first);
                }
            }
        }
        for &ix in &sl.logs {
            let l = &g.logs[ix as usize];
            if !c.contains_key(&l.id) {
                // a log's batches are keyed by first seq and do not overlap: its last durable
                // one ends furthest
                c.insert(l.id.clone(), l.high().unwrap_or(0).max(l.trimmed));
            }
        }
        c
    }

    /// Where a live feed starting now joins: the LSN its tail starts at and, per log of the
    /// stream, the sequence it is next due (the end of the log's contiguous run below that
    /// LSN). Read under one lock, so no batch falls between the two.
    pub fn live_start(&self, stream: Option<&str>) -> (Lsn, HashMap<LogId, u64>) {
        let g = self.inner.lock();
        let pos = g.contiguous;
        let next = g
            .logs
            .iter()
            .filter(|l| stream.is_none_or(|s| l.id.stream == s))
            .map(|l| {
                let mut c = l.trimmed;
                for (&s, e) in &l.batches {
                    // durable out of LSN order at or past `pos`: the tail still returns it
                    if s == c && e.durable() && e.lsn < pos {
                        c = s + e.count as u64;
                    }
                }
                (l.id.clone(), c)
            })
            .collect();
        (pos, next)
    }

    /// The end of the log's durable batch starting at `seq`, if its LSN is below `pos` (a tail
    /// from `pos` does not return it).
    pub fn durable_below(&self, log: &LogId, seq: u64, pos: Lsn) -> Option<u64> {
        let g = self.inner.lock();
        let e = g.log(log)?.batches.get(&seq)?;
        // an LSN is set once durable
        (e.durable() && e.lsn < pos).then_some(seq + e.count as u64)
    }

    /// The LSN a subscription "from now" starts at.
    pub fn durable_len(&self) -> u64 {
        self.inner.lock().contiguous
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        let g = self.inner.lock();
        (g.order.len() as u64, g.events, g.bytes)
    }

    fn frontier_of(l: &Log) -> Frontier {
        let contiguous = l.contig.max(l.trimmed);
        Frontier {
            log: l.id.clone(),
            contiguous,
            high: l.high().unwrap_or(0).max(contiguous),
        }
    }

    /// A log's durable batches from sequence `from` (one that starts before it included), up to
    /// about `max_bytes` (repair).
    pub fn log_batches(&self, log: &LogId, from: u64, max_bytes: usize) -> Vec<bytes::Bytes> {
        let lsns: Vec<Lsn> = {
            let g = self.inner.lock();
            let Some(l) = g.log(log) else {
                return vec![];
            };
            let first = l
                .batches
                .range(..=from)
                .next_back()
                .filter(|(s, e)| **s + e.count as u64 > from)
                .map_or(from, |(&s, _)| s);
            let mut size = 0usize;
            let mut out = vec![];
            for e in l.batches.range(first..).map(|(_, e)| e) {
                if size >= max_bytes {
                    break;
                }
                if e.durable() {
                    size += e.len as usize;
                    out.push(e.lsn);
                }
            }
            out
        };
        lsns.into_iter()
            .filter_map(|lsn| self.read_batch(lsn).map(|b| b.bytes))
            .collect()
    }

    pub fn frontiers(&self, stream: Option<&str>) -> Vec<Frontier> {
        let g = self.inner.lock();
        let mut v: Vec<_> = g
            .logs
            .iter()
            .filter(|l| stream.is_none_or(|s| l.id.stream == s))
            .map(Self::frontier_of)
            .filter(|f| f.high > 0)
            .collect();
        v.sort_by(|a, b| a.log.cmp(&b.log));
        v
    }

    /// Event-time ordered read of locally durable events of one stream with
    /// `event_time_us >= from`. Ties break on (writer, epoch, seq), so every node with the same
    /// content returns the same order.
    ///
    /// ponytail: reads the whole stream from the log; an event-time index per sealed segment
    /// replaces this when streams outgrow a scan.
    pub fn read(&self, stream: &str, from: u64, limit: usize) -> (Vec<Event>, Vec<Frontier>) {
        let (lsns, cov) = {
            let g = self.inner.lock();
            let mut lsns = vec![];
            let mut cov = vec![];
            for &ix in g.streams.get(stream).into_iter().flat_map(|s| &s.logs) {
                let l = &g.logs[ix as usize];
                cov.push(Self::frontier_of(l));
                lsns.extend(l.batches.values().filter(|e| e.durable()).map(|e| e.lsn));
            }
            cov.sort_by(|a, b| a.log.cmp(&b.log));
            (lsns, cov)
        };
        let mut out: Vec<Event> = lsns
            .into_iter()
            .filter_map(|l| self.read_batch(l))
            .flat_map(|b| {
                b.events()
                    .filter(|ev| ev.event_time_us >= from)
                    .collect::<Vec<_>>()
            })
            .collect();
        out.sort_by(|a, b| {
            (a.event_time_us, &a.writer, a.epoch, a.seq).cmp(&(
                b.event_time_us,
                &b.writer,
                b.epoch,
                b.seq,
            ))
        });
        out.truncate(limit);
        (out, cov)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{batch_crc, encode_log_key};
    use bytes::Bytes;
    use precog_storage::record::Meta;

    struct T {
        _dir: tempfile::TempDir,
        log: precog_storage::Log,
        s: Store,
    }

    fn store() -> T {
        let dir = tempfile::tempdir().unwrap();
        let mut c = precog_storage::Config::new(dir.path());
        c.segment_size = 1 << 20;
        let log = precog_storage::Log::open(c).unwrap();
        let s = Store::load(log.reader(), vec![]).unwrap();
        T { _dir: dir, log, s }
    }

    impl T {
        fn durable(&mut self, b: &Batch) {
            let m = Meta {
                key: encode_log_key(&b.id.log),
                first_seq: b.id.first_seq,
                count: b.count,
                tag: batch_crc(&b.bytes),
                time_ms: 0,
            };
            let lsn = self.log.append(&[(m, &b.bytes[..])]).unwrap();
            self.s.mark_durable(&b.id, lsn);
        }
    }

    fn b(writer: &str, epoch: u64, seq: u64, evs: &[(u64, &[u8])]) -> Batch {
        let log = LogId {
            stream: "s".into(),
            writer: writer.into(),
            epoch,
        };
        Batch::parse(Batch::encode(&log, seq, evs).unwrap()).unwrap()
    }

    #[test]
    fn duplicate_and_conflict() {
        let mut t = store();
        let a = b("w", 1, 0, &[(1, b"a"), (2, b"b")]);
        assert!(matches!(t.s.reserve(&a), Reserve::New));
        t.durable(&a);
        match t.s.reserve(&a) {
            Reserve::Duplicate(rx) => assert_eq!(*rx.borrow(), State::Durable),
            _ => panic!("expected duplicate"),
        }
        // same identity, different payload
        assert!(matches!(
            t.s.reserve(&b("w", 1, 0, &[(1, b"a"), (2, b"X")])),
            Reserve::Conflict(_)
        ));
        // same identity, different timestamp only: still a conflict
        assert!(matches!(
            t.s.reserve(&b("w", 1, 0, &[(9, b"a"), (2, b"b")])),
            Reserve::Conflict(_)
        ));
        // partial overlap from either side
        assert!(matches!(
            t.s.reserve(&b("w", 1, 1, &[(1, b"b")])),
            Reserve::Conflict(_)
        ));
        let c = b("w", 1, 5, &[(1, b"c")]);
        assert!(matches!(t.s.reserve(&c), Reserve::New));
        assert!(matches!(
            t.s.reserve(&b("w", 1, 3, &[(1, b"x"), (1, b"y"), (1, b"z")])),
            Reserve::Conflict(_)
        ));
        // different epoch is a different sequence space
        assert!(matches!(
            t.s.reserve(&b("w", 2, 0, &[(1, b"zz")])),
            Reserve::New
        ));
    }

    #[test]
    fn a_duplicate_of_a_pending_batch_learns_how_it_ends() {
        // a retry arriving while the first copy is still being written waits for that write:
        // it must hear the outcome, durable or failed, not hang or assume
        let mut t = store();
        let a = b("w", 1, 0, &[(1, b"a")]);
        let c = b("w", 1, 1, &[(1, b"c")]);
        for x in [&a, &c] {
            assert!(matches!(t.s.reserve(x), Reserve::New));
        }
        let (Reserve::Duplicate(mut ra), Reserve::Duplicate(mut rc)) =
            (t.s.reserve(&a), t.s.reserve(&c))
        else {
            panic!("expected duplicates");
        };
        assert_eq!(*ra.borrow_and_update(), State::Pending);
        t.durable(&a);
        assert_eq!(*ra.borrow(), State::Durable);
        t.s.mark_failed(&c.id);
        assert_eq!(*rc.borrow_and_update(), State::Failed);
        assert!(
            matches!(t.s.reserve(&c), Reserve::New),
            "a failed write is retried"
        );
        t.s.mark_failed(&a.id);
        assert!(
            t.s.has(&a.id, batch_crc(&a.bytes)),
            "a durable batch never fails"
        );
    }

    #[test]
    fn frontier_gaps() {
        let mut t = store();
        for seq in [0u64, 1, 3, 4] {
            let x = b("w", 1, seq, &[(seq, b"x")]);
            assert!(matches!(t.s.reserve(&x), Reserve::New));
            t.durable(&x);
        }
        let f = &t.s.frontiers(None)[0];
        assert_eq!((f.contiguous, f.high), (2, 5));
        let gap = b("w", 1, 2, &[(2, b"x")]);
        assert!(matches!(t.s.reserve(&gap), Reserve::New));
        assert_eq!(
            t.s.frontiers(None)[0].contiguous,
            2,
            "pending batch must not advance frontier"
        );
        t.durable(&gap);
        assert_eq!(t.s.frontiers(None)[0].contiguous, 5);
    }

    #[test]
    fn read_orders_by_event_time_with_deterministic_ties() {
        let mut t = store();
        for x in [
            b("w2", 1, 0, &[(30, b"c"), (10, b"a")]),
            b("w1", 1, 0, &[(10, b"tie"), (20, b"b")]),
        ] {
            t.s.reserve(&x);
            t.durable(&x);
        }
        let pending = b("w3", 1, 0, &[(0, b"invisible")]);
        t.s.reserve(&pending);
        let (ev, cov) = t.s.read("s", 0, 100);
        let got: Vec<_> = ev
            .iter()
            .map(|e| (e.event_time_us, e.writer.as_str()))
            .collect();
        assert_eq!(got, vec![(10, "w1"), (10, "w2"), (20, "w1"), (30, "w2")]);
        assert_eq!(cov.len(), 3);
        let (ev, _) = t.s.read("s", 15, 1);
        assert_eq!(ev[0].payload, Bytes::from_static(b"b"));
    }

    #[test]
    fn failed_reservation_is_released() {
        let t = store();
        let a = b("w", 1, 0, &[(1, b"a")]);
        t.s.reserve(&a);
        t.s.mark_failed(&a.id);
        assert!(matches!(t.s.reserve(&a), Reserve::New));
    }

    #[test]
    fn trim_forgets_old_batches_but_keeps_frontiers_and_refuses_reingest() {
        let mut t = store();
        let xs: Vec<Batch> = (0..6).map(|i| b("w", 1, i, &[(i, b"x")])).collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        t.s.trim(3);
        assert_eq!(t.s.totals().0, 3);
        assert!(t.s.get(&xs[1].id).is_none());
        assert!(t.s.get(&xs[4].id).is_some());
        let f = &t.s.frontiers(None)[0];
        assert_eq!((f.contiguous, f.high), (6, 6));
        assert!(matches!(t.s.reserve(&xs[0]), Reserve::Expired));
        assert!(
            t.s.has(&xs[0].id, 0),
            "repair must not fetch expired batches back"
        );
        // repair is served from after the trimmed prefix
        let served = t.s.log_batches(&xs[0].id.log, 0, 1 << 20);
        assert_eq!(Batch::parse(served[0].clone()).unwrap().id.first_seq, 3);
    }

    #[test]
    fn tail_never_skips_a_batch_marked_durable_out_of_lsn_order() {
        // LSNs come from the log in order, but tasks mark batches durable in any order
        let mut t = store();
        let xs: Vec<Batch> = (0..3).map(|i| b("w", 1, i, &[(i, b"x")])).collect();
        let mut lsns = vec![];
        for x in &xs {
            t.s.reserve(x);
            let m = Meta {
                key: encode_log_key(&x.id.log),
                first_seq: x.id.first_seq,
                count: 1,
                tag: batch_crc(&x.bytes),
                time_ms: 0,
            };
            lsns.push(t.log.append(&[(m, &x.bytes[..])]).unwrap());
        }
        t.s.mark_durable(&xs[0].id, lsns[0]);
        t.s.mark_durable(&xs[2].id, lsns[2]); // lsn 2 before lsn 1
        let (got, next) = t.s.tail(0, None, 1 << 20);
        assert_eq!(
            got.len(),
            1,
            "only the contiguous prefix is visible to subscribers"
        );
        t.s.mark_durable(&xs[1].id, lsns[1]);
        let (got, next2) = t.s.tail(next, None, 1 << 20);
        assert_eq!(got.len(), 2);
        assert_eq!(next2, 3);
        assert_eq!(t.s.durable_len(), 3);
    }

    #[test]
    fn a_live_feed_joins_each_log_after_its_run_below_the_start() {
        // the feed's start LSN and each log's next sequence are one snapshot: the log's run
        // stops at its gap (seq 2), even though seq 3 is already durable below the start
        let mut t = store();
        let xs: Vec<Batch> = (0..4).map(|i| b("w", 1, i, &[(i, b"x")])).collect();
        let mut lsns = vec![];
        for i in [0, 1, 3, 2] {
            let x = &xs[i];
            t.s.reserve(x);
            let m = Meta {
                key: encode_log_key(&x.id.log),
                first_seq: x.id.first_seq,
                count: 1,
                tag: batch_crc(&x.bytes),
                time_ms: 0,
            };
            lsns.push((i, t.log.append(&[(m, &x.bytes[..])]).unwrap()));
        }
        for &(i, lsn) in &lsns[..3] {
            t.s.mark_durable(&xs[i].id, lsn); // seq 2 (lsn 3) is not durable yet
        }
        let (pos, next) = t.s.live_start(Some("s"));
        assert_eq!(pos, 3);
        assert_eq!(next, HashMap::from([(xs[0].id.log.clone(), 2)]));
        assert!(t.s.live_start(Some("other")).1.is_empty());
        let log = &xs[0].id.log;
        assert_eq!(
            t.s.durable_below(log, 3, pos),
            Some(4),
            "above the gap, below the start"
        );
        assert_eq!(
            t.s.durable_below(log, 3, 2),
            None,
            "the tail from 2 returns it"
        );
        assert_eq!(t.s.durable_below(log, 2, pos), None, "not durable");
    }

    #[test]
    fn load_rebuilds_from_the_log() {
        let mut t = store();
        let xs: Vec<Batch> = (0..4)
            .map(|i| b(&format!("w{}", i % 2), 1, i / 2, &[(i, b"payload")]))
            .collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        let r = t.log.reader();
        let s2 = Store::load(r.clone(), r.entries(0, 100).unwrap()).unwrap();
        assert_eq!(s2.totals(), t.s.totals());
        assert_eq!(s2.frontiers(None), t.s.frontiers(None));
        assert_eq!(s2.get(&xs[3].id).unwrap().bytes, xs[3].bytes);
    }

    impl T {
        fn log_only(&mut self, x: &Batch, crc: u32) {
            let m = Meta {
                key: encode_log_key(&x.id.log),
                first_seq: x.id.first_seq,
                count: x.count,
                tag: crc,
                time_ms: 0,
            };
            self.log.append(&[(m, &x.bytes[..])]).unwrap();
        }
        fn reload(&self) -> anyhow::Result<Store> {
            let r = self.log.reader();
            Store::load(r.clone(), r.entries(0, 1000)?)
        }
    }

    #[test]
    fn load_keeps_a_batch_logged_twice_once_and_refuses_conflicting_ones() {
        let mut t = store();
        let a = b("w", 1, 0, &[(1, b"a"), (2, b"bb")]);
        let crc = batch_crc(&a.bytes);
        t.log_only(&a, crc);
        t.log_only(&a, crc); // a repair raced a replicate
        let s = t.reload().unwrap();
        assert_eq!(s.totals(), (1, 2, a.bytes.len() as u64));
        assert!(
            matches!(s.reserve(&a), Reserve::Duplicate(_)),
            "retry after restart"
        );
        assert!(s.has(&a.id, crc));
        // same first_seq, other content
        let mut t = store();
        t.log_only(&a, crc);
        t.log_only(&a, crc ^ 1);
        assert!(t.reload().is_err());
        // a later first_seq inside the stored batch
        let mut t = store();
        t.log_only(&a, crc);
        t.log_only(&b("w", 1, 1, &[(1, b"x")]), 7);
        assert!(t.reload().is_err());
        // adjacent batches are fine
        let mut t = store();
        t.log_only(&a, crc);
        let c = b("w", 1, 2, &[(1, b"x")]);
        t.log_only(&c, batch_crc(&c.bytes));
        assert_eq!(t.reload().unwrap().totals().0, 2);
    }

    #[test]
    fn trim_keeps_totals_and_the_boundary_batch_exact() {
        let mut t = store();
        let xs: Vec<Batch> = (0..6).map(|i| b("w", 1, i, &[(i, b"xyz")])).collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        t.s.trim(3);
        let len = xs[0].bytes.len() as u64;
        assert_eq!(t.s.totals(), (3, 3, 3 * len));
        // the first kept batch is a duplicate, not expired, and needs its own checksum
        assert!(matches!(t.s.reserve(&xs[3]), Reserve::Duplicate(_)));
        assert!(t.s.has(&xs[3].id, batch_crc(&xs[3].bytes)));
        assert!(!t.s.has(&xs[3].id, 0));
    }

    #[test]
    fn has_needs_a_durable_copy_with_the_same_checksum() {
        let mut t = store();
        let a = b("w", 1, 0, &[(1, b"a")]);
        let crc = batch_crc(&a.bytes);
        t.s.reserve(&a);
        assert!(!t.s.has(&a.id, crc), "reserved is not durable");
        t.durable(&a);
        assert!(t.s.has(&a.id, crc));
        assert!(!t.s.has(&a.id, crc ^ 1));
        let other = b("v", 1, 0, &[(1, b"a")]);
        assert!(!t.s.has(&other.id, crc));
    }

    #[test]
    fn tail_stops_near_max_bytes_and_frontiers_filter_by_stream() {
        let mut t = store();
        let xs: Vec<Batch> = (0..10).map(|i| b("w", 1, i, &[(i, &[7; 100])])).collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        let one = xs[0].bytes.len();
        let (got, next) = t.s.tail(0, None, 3 * one);
        assert_eq!((got.len(), next), (3, 3));
        assert_eq!(t.s.frontiers(Some("s")).len(), 1);
        assert!(t.s.frontiers(Some("other")).is_empty());
    }

    fn on(stream: &str, writer: &str, seq: u64) -> Batch {
        let log = LogId {
            stream: stream.into(),
            writer: writer.into(),
            epoch: 1,
        };
        Batch::parse(Batch::encode(&log, seq, &[(seq, &[9u8; 100][..])]).unwrap()).unwrap()
    }

    #[test]
    fn a_stream_trims_to_its_own_byte_budget_and_leaves_others_alone() {
        let mut t = store();
        let a: Vec<Batch> = (0..10).map(|i| on("a", "w", i)).collect();
        let b: Vec<Batch> = (0..10).map(|i| on("b", "w", i)).collect();
        for i in 0..10 {
            for x in [&a[i], &b[i]] {
                t.s.reserve(x);
                t.durable(x);
            }
        }
        let one = a[0].bytes.len() as u64;
        assert_eq!(t.s.trim_stream("a", Some(3 * one), None, 0), 7);
        assert!(t.s.get(&a[6].id).is_none() && t.s.get(&a[7].id).is_some());
        assert!(t.s.get(&b[0].id).is_some(), "stream b keeps everything");
        assert_eq!(t.s.totals().0, 13);
        assert_eq!(t.s.totals().2, 13 * one);
        assert!(
            matches!(t.s.reserve(&a[2]), Reserve::Expired),
            "a trimmed batch is not re-ingested"
        );
        let f = t.s.frontiers(Some("a"));
        assert_eq!((f[0].contiguous, f[0].high), (10, 10));
        // already within budget: nothing more goes
        assert_eq!(t.s.trim_stream("a", Some(3 * one), None, 0), 0);
    }

    #[test]
    fn a_stream_trims_batches_older_than_its_age_limit() {
        let mut t = store();
        let a: Vec<Batch> = (0..4).map(|i| on("a", "w", i)).collect();
        for x in &a {
            t.s.reserve(x);
            t.durable(x);
        }
        let now = now_ms();
        assert_eq!(
            t.s.trim_stream("a", None, Some(60_000), now),
            0,
            "all are seconds old"
        );
        assert_eq!(t.s.trim_stream("a", None, Some(60_000), now + 61_000), 4);
        assert_eq!(t.s.totals().0, 0);
    }

    fn drain(s: &Store, stream: &str, mut c: Cursor, max: usize) -> (Vec<BatchId>, Cursor) {
        let mut got = vec![];
        loop {
            let (bs, next) = s.read_cursor(stream, &c, max);
            if bs.is_empty() {
                return (got, c);
            }
            got.extend(bs.into_iter().map(|b| Batch::parse(b).unwrap().id));
            c = next;
        }
    }

    #[test]
    fn a_cursor_reads_every_batch_once_in_writer_order_in_small_steps() {
        let mut t = store();
        for i in 0..6 {
            for w in ["w1", "w2"] {
                let x = on("s", w, i);
                t.s.reserve(&x);
                t.durable(&x);
            }
        }
        let one = on("s", "w1", 0).bytes.len();
        let (got, c) = drain(&t.s, "s", Cursor::new(), 3 * one);
        assert_eq!(got.len(), 12);
        for w in ["w1", "w2"] {
            let seqs: Vec<u64> = got
                .iter()
                .filter(|id| id.log.writer == w)
                .map(|id| id.first_seq)
                .collect();
            assert_eq!(seqs, (0..6).collect::<Vec<_>>(), "{w}");
        }
        assert_eq!(c.values().copied().collect::<Vec<_>>(), [6, 6]);
        assert!(
            t.s.read_cursor("s", &c, 1 << 20).0.is_empty(),
            "nothing more"
        );
    }

    #[test]
    fn a_cursor_from_one_node_resumes_exactly_on_another() {
        // two nodes hold the same batches in different LSN orders
        let (mut a, mut b) = (store(), store());
        let xs: Vec<Batch> = (0..8)
            .flat_map(|i| [on("s", "w1", i), on("s", "w2", i)])
            .collect();
        for x in &xs {
            a.s.reserve(x);
            a.durable(x);
        }
        for x in xs
            .iter()
            .rev()
            .filter(|x| x.id.log.writer == "w2")
            .chain(xs.iter().filter(|x| x.id.log.writer == "w1"))
        {
            b.s.reserve(x);
            b.durable(x);
        }
        let one = xs[0].bytes.len();
        let (first, c) = a.s.read_cursor("s", &Cursor::new(), 5 * one);
        let (rest, _) = drain(&b.s, "s", c, 3 * one);
        let mut all: Vec<BatchId> = first
            .into_iter()
            .map(|b| Batch::parse(b).unwrap().id)
            .chain(rest)
            .collect();
        assert_eq!(all.len(), 16, "nothing lost or repeated");
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 16);
    }

    #[test]
    fn a_writers_batch_waits_until_its_predecessor_is_here() {
        let mut t = store();
        let xs: Vec<Batch> = (0..3).map(|i| on("s", "w", i)).collect();
        for x in [&xs[0], &xs[2]] {
            t.s.reserve(x);
            t.durable(x);
        }
        let (got, c) = drain(&t.s, "s", Cursor::new(), 1 << 20);
        assert_eq!(
            got.iter().map(|i| i.first_seq).collect::<Vec<_>>(),
            [0],
            "seq 2 waits for 1"
        );
        t.s.reserve(&xs[1]);
        t.durable(&xs[1]);
        let (got, _) = drain(&t.s, "s", c, 1 << 20);
        assert_eq!(got.iter().map(|i| i.first_seq).collect::<Vec<_>>(), [1, 2]);
    }

    #[test]
    fn the_end_is_past_the_last_durable_batch_of_every_log_not_a_reserved_one() {
        // Why: the end is polled often (brrrrr asks for it every 200 ms per caught-up stream), so
        // it is read from each log's last batches instead of the whole stream; it must still be
        // past every durable batch, gaps included, and never past one only reserved.
        let mut t = store();
        let xs: Vec<_> = (0..4).map(|i| on("s", "w", i)).collect();
        for i in [0, 1, 3] {
            t.s.reserve(&xs[i]);
            t.durable(&xs[i]);
        }
        t.s.reserve(&xs[2]); // a gap: reserved, not durable
        let y = on("s", "v", 0);
        t.s.reserve(&y); // a log with nothing durable yet
        let reserved_last = on("s", "w", 4);
        t.s.reserve(&reserved_last);
        let end = t.s.cursor_at("s", u64::MAX);
        let w = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: xs[0].id.log.epoch,
        };
        assert_eq!(
            end.get(&w),
            Some(&4),
            "past seq 3, not past the reserved 4: {end:?}"
        );
        assert_eq!(end.len(), 2, "{end:?}");
        assert!(
            end.values().any(|&v| v == 0),
            "the log with nothing durable starts at 0"
        );
    }

    #[test]
    fn a_cursor_can_start_at_the_end_or_at_a_time() {
        let mut t = store();
        for i in 0..3 {
            let x = on("s", "w", i);
            t.s.reserve(&x);
            t.durable(&x);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        let since = now_ms();
        for i in 3..5 {
            let x = on("s", "w", i);
            t.s.reserve(&x);
            t.durable(&x);
        }
        let at = t.s.cursor_at("s", since);
        assert_eq!(
            drain(&t.s, "s", at, 1 << 20)
                .0
                .iter()
                .map(|i| i.first_seq)
                .collect::<Vec<_>>(),
            [3, 4]
        );
        let end = t.s.cursor_at("s", u64::MAX);
        assert!(drain(&t.s, "s", end.clone(), 1 << 20).0.is_empty());
        let x = on("s", "w", 5);
        t.s.reserve(&x);
        t.durable(&x);
        assert_eq!(
            drain(&t.s, "s", end, 1 << 20).0.len(),
            1,
            "the end cursor sees what comes next"
        );
    }

    #[test]
    fn a_logs_batches_from_a_sequence_serve_repair_and_frontiers_track_gaps() {
        let mut t = store();
        let xs: Vec<Batch> = (0..6).map(|i| on("s", "w", i)).collect();
        for x in xs.iter().filter(|x| x.id.first_seq != 2) {
            t.s.reserve(x);
            t.durable(x);
        }
        let f = &t.s.frontiers(None)[0];
        assert_eq!((f.contiguous, f.high), (2, 6), "a gap at 2");
        let got: Vec<u64> =
            t.s.log_batches(&xs[0].id.log, 1, 1 << 20)
                .into_iter()
                .map(|b| Batch::parse(b).unwrap().id.first_seq)
                .collect();
        assert_eq!(got, [1, 3, 4, 5]);
        let one = xs[0].bytes.len();
        assert_eq!(
            t.s.log_batches(&xs[0].id.log, 3, one).len(),
            1,
            "bounded by max_bytes"
        );
        t.s.reserve(&xs[2]);
        t.durable(&xs[2]);
        let f = &t.s.frontiers(None)[0];
        assert_eq!((f.contiguous, f.high), (6, 6), "the gap filled");
        t.s.trim_stream("s", Some(0), None, 0);
        let f = &t.s.frontiers(None)[0];
        assert_eq!(
            (f.contiguous, f.high),
            (6, 6),
            "trimmed sequences count as present"
        );
    }

    #[test]
    fn a_cursor_read_stops_at_about_max_bytes() {
        // a subscriber's response is bounded: a long backlog comes in steps, not one frame
        let mut t = store();
        for i in 0..6 {
            let x = on("s", "w", i);
            t.s.reserve(&x);
            t.durable(&x);
        }
        let one = on("s", "w", 0).bytes.len();
        let (bs, c) = t.s.read_cursor("s", &Cursor::new(), 3 * one);
        assert_eq!(bs.len(), 3);
        assert_eq!(c.values().copied().collect::<Vec<_>>(), [3]);
    }

    #[test]
    fn a_cursor_inside_a_batch_still_gets_that_batch() {
        // nothing after the cursor is skipped, even where it falls inside a batch
        let mut t = store();
        let xs = [
            b("w", 1, 0, &[(1, b"a"), (2, b"b"), (3, b"c")]),
            b("w", 1, 3, &[(4, b"d"), (5, b"e"), (6, b"f")]),
        ];
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        let c = Cursor::from([(xs[0].id.log.clone(), 1)]);
        let (got, c) = drain(&t.s, "s", c, 1 << 20);
        assert_eq!(got.iter().map(|i| i.first_seq).collect::<Vec<_>>(), [0, 3]);
        assert_eq!(c.values().copied().collect::<Vec<_>>(), [6]);
    }

    #[test]
    fn a_writer_idle_at_its_cursor_does_not_stall_the_others() {
        // the scan starts at the earliest unread batch. Starting at w1's last read batch
        // instead would meet w2's early seq 2 first, block w2, and return nothing, forever
        let mut t = store();
        let (w1, w2) = (|i| on("s", "w1", i), |i| on("s", "w2", i));
        for x in [w1(0), w2(0), w2(2), w2(1)] {
            t.s.reserve(&x);
            t.durable(&x);
        }
        let c = Cursor::from([(w1(0).id.log, 1), (w2(0).id.log, 1)]);
        let (got, _) = drain(&t.s, "s", c, 1 << 20);
        let got: Vec<_> = got
            .iter()
            .map(|i| (i.log.writer.as_str(), i.first_seq))
            .collect();
        assert_eq!(got, [("w2", 1), ("w2", 2)]);
    }

    #[test]
    fn a_batch_exactly_its_streams_age_limit_old_is_kept() {
        // retention.ms keeps data for at least that long (as in Kafka): only older goes
        let mut t = store();
        let a = on("a", "w", 0);
        let m = Meta {
            key: encode_log_key(&a.id.log),
            first_seq: 0,
            count: a.count,
            tag: batch_crc(&a.bytes),
            time_ms: 1_000,
        };
        t.log.append(&[(m, &a.bytes[..])]).unwrap();
        let r = t.log.reader();
        let s = Store::load(r.clone(), r.entries(0, 10).unwrap()).unwrap();
        assert_eq!(
            s.trim_stream("a", None, Some(500), 1_500),
            0,
            "exactly 500 ms old"
        );
        assert_eq!(s.trim_stream("a", None, Some(500), 1_501), 1, "older");
    }

    #[test]
    fn a_logs_batches_start_at_the_batch_holding_the_sequence() {
        // repair asks from the first sequence it lacks: it gets the batch holding it (even from
        // inside a batch), and not the batch before, which it has
        let mut t = store();
        let xs = [
            b("w", 1, 0, &[(1, b"a"), (2, b"b")]),
            b("w", 1, 2, &[(3, b"c"), (4, b"d")]),
            b("w", 1, 4, &[(5, b"e"), (6, b"f")]),
        ];
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        let firsts = |from| -> Vec<u64> {
            t.s.log_batches(&xs[0].id.log, from, 1 << 20)
                .into_iter()
                .map(|b| Batch::parse(b).unwrap().id.first_seq)
                .collect()
        };
        assert_eq!(firsts(2), [2, 4], "from a batch's start");
        assert_eq!(firsts(3), [2, 4], "from inside a batch");
        // a gap at 2: the batch ending at 2 is not resent
        let ys = [
            b("v", 1, 0, &[(1, b"a"), (2, b"b")]),
            b("v", 1, 4, &[(5, b"e")]),
        ];
        for y in &ys {
            t.s.reserve(y);
            t.durable(y);
        }
        let got: Vec<u64> =
            t.s.log_batches(&ys[0].id.log, 2, 1 << 20)
                .into_iter()
                .map(|b| Batch::parse(b).unwrap().id.first_seq)
                .collect();
        assert_eq!(got, [4]);
    }
}
