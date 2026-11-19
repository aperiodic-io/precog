//! The node's index over its segment log.
//!
//! Every writer log `(stream, writer, epoch)` is an independent sequence space. Batches occupy
//! non-overlapping sequence ranges; an exact retry is idempotent, any other overlap is a
//! conflict. A batch becomes visible (reads, inventory, subscriptions) only once the segment
//! log has made it durable. Payloads are read back through the log's index and tail cache.
//!
//! Two tiers keep RAM independent of retention (a node retains tens of millions of small
//! batches). Batches in the active segment (and pending ones) are in RAM. Once a segment is
//! sealed and every batch in it durable, `absorb` drops its batches from RAM: what stays is per
//! writer log its frontier and, per sealed segment holding its batches, their sequence range;
//! per stream and segment its totals. Their batches are then read from the segment's index file
//! (`Reader::by_key`: one read of the log's run in it), only for the segments whose range
//! covers what is asked.
//!
//! ponytail: index reads of sealed segments run under the store's lock; move them out if old
//! reads (a consumer replaying retention) measurably stall appends.

use crate::proto::{Batch, BatchId, Event, Frontier, LogId, decode_log_key, encode_log_key};
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

/// A durable batch as a stream read sees it.
#[derive(Clone, Copy)]
struct Rec {
    lsn: Lsn,
    ix: u32,
    first: u64,
    e: Entry,
    time: u64,
}

/// A log's batches in one sealed segment: their sequences lie in `lo..hi`.
#[derive(Clone, Copy, Debug)]
struct Cold {
    base: Lsn,
    lo: u64,
    hi: u64,
}

/// One writer log. Kept for good once seen: its frontier outlives its batches.
struct Log {
    id: LogId,
    /// Its batches in RAM: pending ones and durable ones not yet absorbed.
    batches: BTreeMap<u64, Entry>,
    /// The sealed segments holding its other batches, oldest first.
    cold: Vec<Cold>,
    /// Sequences below this were trimmed by retention.
    trimmed: u64,
    /// Every sequence below this is durable here or trimmed (kept as batches arrive, so
    /// frontiers, and with them repair, cost O(logs) instead of O(batches)).
    contig: u64,
    /// The end of its furthest durable batch.
    high: u64,
}

/// Writer logs are interned: a batch refers to its log by index, so no map holds a copy of its
/// stream and writer names per batch.
#[derive(Default)]
struct Inner {
    logs: Vec<Log>,
    ix: HashMap<LogId, u32>,
    streams: HashMap<String, StreamLog>,
    /// Durable batches in RAM by LSN: (log, len, first seq).
    order: BTreeMap<Lsn, (u32, u32, u64)>,
    /// What waits on a batch not yet durable (a concurrent duplicate); dropped once it is
    /// durable or failed, as a later duplicate needs no wait.
    pending: HashMap<(u32, u64), watch::Sender<State>>,
    /// Every LSN below this is durable and indexed (or trimmed). Subscriptions read only below
    /// it: batches are marked durable in any order, but a reader must never skip one.
    contiguous: Lsn,
    /// LSNs below this are in absorbed segments: their batches are read from disk.
    cold_end: Lsn,
    /// `trim` forgot every batch below this.
    floor: Lsn,
    /// Second copies of a batch logged twice (a repair raced a replicate): never served.
    dead: BTreeSet<Lsn>,
    /// Index reads of absorbed segments (`Reader::by_key`) so far.
    reads: std::cell::Cell<u64>,
    batches: u64,
    events: u64,
    bytes: u64,
}

#[derive(Default)]
struct StreamLog {
    /// The stream's writer logs.
    logs: BTreeSet<u32>,
    /// Its durable batches in RAM by LSN: append time ms, for its own retention.
    order: BTreeMap<Lsn, u64>,
    /// Its batches per absorbed segment.
    cold: BTreeMap<Lsn, ColdStream>,
    /// Its own retention forgot every one of its batches below this.
    floor: Lsn,
    bytes: u64,
}

/// A stream's batches still retained in one absorbed segment.
#[derive(Clone, Copy, Default)]
struct ColdStream {
    end: Lsn,
    batches: u64,
    events: u64,
    bytes: u64,
    max_time: u64,
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
    /// The index could not be read: the outcome is unknown (retry).
    Error(String),
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

type Res<T> = precog_storage::Result<T>;

impl Inner {
    fn intern(&mut self, log: &LogId) -> u32 {
        if let Some(&i) = self.ix.get(log) {
            return i;
        }
        let i = self.logs.len() as u32;
        self.logs.push(Log {
            id: log.clone(),
            batches: BTreeMap::new(),
            cold: vec![],
            trimmed: 0,
            contig: 0,
            high: 0,
        });
        self.ix.insert(log.clone(), i);
        i
    }

    fn log(&self, log: &LogId) -> Option<(u32, &Log)> {
        self.ix.get(log).map(|&i| (i, &self.logs[i as usize]))
    }

    fn stream_of(&self, ix: u32) -> &StreamLog {
        &self.streams[&self.logs[ix as usize].id.stream]
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

    /// The log's batches retained in the absorbed segment at `base`, by sequence.
    fn run(&self, r: &Reader, ix: u32, base: Lsn) -> Res<Vec<Rec>> {
        let l = &self.logs[ix as usize];
        let floor = self.floor.max(self.stream_of(ix).floor);
        self.reads.set(self.reads.get() + 1);
        Ok(r.by_key(base, &encode_log_key(&l.id))?
            .into_iter()
            .filter(|e| e.lsn >= floor && !self.dead.contains(&e.lsn))
            .map(|e| Rec {
                lsn: e.lsn,
                ix,
                first: e.meta.first_seq,
                e: Entry {
                    lsn: e.lsn,
                    count: e.meta.count,
                    crc: e.meta.tag,
                    len: e.payload_len,
                },
                time: e.meta.time_ms,
            })
            .collect())
    }

    /// Visits the log's batches (pending ones included) that end past `from`, by sequence,
    /// until `f` returns false. Reads only the absorbed segments whose range reaches past
    /// `from`.
    fn log_from(
        &self,
        r: &Reader,
        ix: u32,
        from: u64,
        mut f: impl FnMut(u64, Entry) -> bool,
    ) -> Res<()> {
        let l = &self.logs[ix as usize];
        // the RAM part from the batch before `from` (it may hold it)
        let start = l
            .batches
            .range(..=from)
            .next_back()
            .map_or(from, |(&s, _)| s);
        let hot: Vec<(u64, Entry)> = l.batches.range(start..).map(|(&s, &e)| (s, e)).collect();
        // by first sequence: the absorbed segments whose range reaches past `from`, and RAM
        // (which may hold a late batch below any of them: it overlaps all after it)
        let mut parts: Vec<(u64, u64, Option<Lsn>)> = l
            .cold
            .iter()
            .filter(|c| c.hi > from)
            .map(|c| (c.lo, c.hi, Some(c.base)))
            .collect();
        if let Some(&(lo, _)) = hot.first() {
            parts.push((lo, u64::MAX, None));
        }
        parts.sort_by_key(|p| p.0);
        let mut i = 0;
        while i < parts.len() {
            // parts whose ranges overlap are read before any of them is visited; the next one
            // is read only if the visit gets that far
            let (mut j, mut hi, mut v) = (i, 0, vec![]);
            while j < parts.len() && (j == i || parts[j].0 < hi) {
                hi = hi.max(parts[j].1);
                match parts[j].2 {
                    Some(base) => v.extend(self.run(r, ix, base)?.iter().map(|x| (x.first, x.e))),
                    None => v.extend_from_slice(&hot),
                }
                j += 1;
            }
            v.sort_by_key(|x| x.0);
            for (s, e) in v {
                if s + e.count as u64 > from && !f(s, e) {
                    return Ok(());
                }
            }
            i = j;
        }
        Ok(())
    }

    /// The log's batch starting at `seq`.
    fn find(&self, r: &Reader, ix: u32, seq: u64) -> Res<Option<Entry>> {
        let mut hit = None;
        self.log_from(r, ix, seq, |s, e| {
            hit = (s == seq).then_some(e);
            false
        })?;
        Ok(hit)
    }

    /// The first batch overlapping `first..end`: the one holding `first`, else the first one
    /// starting inside.
    fn overlap(&self, r: &Reader, ix: u32, first: u64, end: u64) -> Res<Option<(u64, Entry)>> {
        let mut hit = None;
        self.log_from(r, ix, first, |s, e| {
            hit = (s < end).then_some((s, e));
            false
        })?;
        Ok(hit)
    }

    /// The stream's retained batches in the absorbed segment at `base`, by LSN.
    fn seg_recs(&self, r: &Reader, stream: &StreamLog, base: Lsn) -> Res<Vec<Rec>> {
        let mut v = vec![];
        for &ix in &stream.logs {
            let l = &self.logs[ix as usize];
            if l.cold.binary_search_by_key(&base, |c| c.base).is_ok() {
                v.extend(self.run(r, ix, base)?);
            }
        }
        v.sort_by_key(|x| x.lsn);
        Ok(v)
    }

    /// Visits the stream's durable batches at LSNs `from..upto`, in LSN order, until `f`
    /// returns false. Skips absorbed segments with nothing logged at or after `min_time`.
    fn stream_from(
        &self,
        r: &Reader,
        stream: &str,
        (from, upto): (Lsn, Lsn),
        min_time: u64,
        mut f: impl FnMut(Rec) -> bool,
    ) -> Res<()> {
        let Some(s) = self.streams.get(stream) else {
            return Ok(());
        };
        for (&base, cs) in &s.cold {
            if cs.end <= from || cs.batches == 0 || cs.max_time < min_time {
                continue;
            }
            for x in self.seg_recs(r, s, base)? {
                if x.lsn >= from && (x.lsn >= upto || !f(x)) {
                    return Ok(());
                }
            }
        }
        let lo = from.max(self.cold_end);
        for (&lsn, &time) in s.order.range(lo..upto.max(lo)) {
            let (ix, _, first) = self.order[&lsn];
            let e = self.logs[ix as usize].batches[&first];
            if !f(Rec {
                lsn,
                ix,
                first,
                e,
                time,
            }) {
                return Ok(());
            }
        }
        Ok(())
    }

    fn index(&mut self, r: &Reader, lsn: Lsn, ix: u32, first: u64, e: Entry, time_ms: u64) {
        self.order.insert(lsn, (ix, e.len, first));
        self.batches += 1;
        self.events += e.count as u64;
        self.bytes += e.len as u64;
        let l = &mut self.logs[ix as usize];
        l.high = l.high.max(first + e.count as u64);
        let at_contig = first == l.contig.max(l.trimmed);
        let s = self
            .streams
            .get_mut(&l.id.stream)
            .expect("a log joins its stream when reserved");
        s.order.insert(lsn, time_ms);
        s.bytes += e.len as u64;
        // only a batch at the log's contiguous end can move it
        if at_contig {
            self.advance(r, ix);
        }
    }

    /// Moves the log's contiguous end past every durable batch that now follows it.
    fn advance(&mut self, r: &Reader, ix: u32) {
        let l = &self.logs[ix as usize];
        let mut c = l.contig.max(l.trimmed);
        let walked = self.log_from(r, ix, c, |s, e| {
            let next = s == c && e.durable();
            if next {
                c += e.count as u64;
            }
            next
        });
        if let Err(e) = walked {
            tracing::error!("reading the index of {:?}: {e}", self.logs[ix as usize].id);
        }
        self.logs[ix as usize].contig = c;
    }

    /// Raises the log's trimmed floor to `end` (retention forgot a batch ending there).
    fn trim_log(&mut self, r: &Reader, ix: u32, end: u64) {
        let l = &mut self.logs[ix as usize];
        l.trimmed = l.trimmed.max(end);
        if l.trimmed > l.contig {
            self.advance(r, ix);
        }
    }

    /// Forgets the durable batch at `lsn` in RAM (retention): later copies of it are `Expired`.
    fn forget(&mut self, r: &Reader, lsn: Lsn) {
        let Some((ix, len, first)) = self.order.remove(&lsn) else {
            return;
        };
        let l = &mut self.logs[ix as usize];
        let count = l.batches.remove(&first).map_or(0, |e| e.count);
        self.batches -= 1;
        self.events -= count as u64;
        self.bytes -= len as u64;
        if let Some(s) = self.streams.get_mut(&l.id.stream)
            && s.order.remove(&lsn).is_some()
        {
            s.bytes -= len as u64;
            s.floor = s.floor.max(lsn + 1);
        }
        self.trim_log(r, ix, first + count as u64);
    }

    /// Forgets an absorbed batch (its stream's retention).
    fn forget_cold(&mut self, r: &Reader, x: Rec) {
        let s = self
            .streams
            .get_mut(&self.logs[x.ix as usize].id.stream)
            .unwrap();
        s.floor = s.floor.max(x.lsn + 1);
        s.bytes -= x.e.len as u64;
        if let Some((_, cs)) = s.cold.range_mut(..=x.lsn).next_back() {
            cs.batches -= 1;
            cs.events -= x.e.count as u64;
            cs.bytes -= x.e.len as u64;
        }
        self.batches -= 1;
        self.events -= x.e.count as u64;
        self.bytes -= x.e.len as u64;
        self.trim_log(r, x.ix, x.first + x.e.count as u64);
    }

    /// Forgets the absorbed segment at `base` whole (the log dropped it).
    fn drop_segment(&mut self, r: &Reader, base: Lsn) {
        for s in self.streams.values_mut() {
            if let Some(cs) = s.cold.remove(&base) {
                s.bytes -= cs.bytes;
                self.batches -= cs.batches;
                self.events -= cs.events;
                self.bytes -= cs.bytes;
            }
        }
        for ix in 0..self.logs.len() as u32 {
            let l = &mut self.logs[ix as usize];
            if let Ok(i) = l.cold.binary_search_by_key(&base, |c| c.base) {
                let hi = l.cold.remove(i).hi;
                self.trim_log(r, ix, hi);
            }
        }
    }

    /// Moves the batches of every sealed segment that ends at or below `upto` out of RAM.
    fn absorb(&mut self, r: &Reader, upto: Lsn) {
        for (base, end) in r.sealed() {
            if end > upto {
                break;
            }
            if end <= self.cold_end {
                continue;
            }
            let moved: Vec<(Lsn, (u32, u32, u64))> =
                self.order.range(base..end).map(|(&l, &v)| (l, v)).collect();
            for (lsn, (ix, len, first)) in moved {
                self.order.remove(&lsn);
                let l = &mut self.logs[ix as usize];
                let e = l
                    .batches
                    .remove(&first)
                    .expect("an indexed batch is in its log");
                let hi = first + e.count as u64;
                match l.cold.last_mut().filter(|c| c.base == base) {
                    Some(c) => (c.lo, c.hi) = (c.lo.min(first), c.hi.max(hi)),
                    None => l.cold.push(Cold {
                        base,
                        lo: first,
                        hi,
                    }),
                }
                let s = self.streams.get_mut(&l.id.stream).unwrap();
                let time = s.order.remove(&lsn).unwrap_or(0);
                let cs = s.cold.entry(base).or_insert(ColdStream {
                    end,
                    ..Default::default()
                });
                cs.batches += 1;
                cs.events += e.count as u64;
                cs.bytes += len as u64;
                cs.max_time = cs.max_time.max(time);
            }
            self.cold_end = end;
        }
    }
}

pub struct Store {
    inner: Mutex<Inner>,
    reader: Reader,
    /// Woken whenever a batch becomes durable (live subscriptions).
    pub durable_notify: tokio::sync::Notify,
}

impl Store {
    /// Rebuilds the index from the segment log, one chunk of its index at a time: the batches
    /// of sealed segments end up on disk, not in RAM.
    pub fn load(reader: Reader) -> anyhow::Result<Store> {
        let s = Store {
            inner: Mutex::default(),
            reader,
            durable_notify: tokio::sync::Notify::new(),
        };
        {
            let r = &s.reader;
            let mut g = s.inner.lock();
            let inner = &mut *g;
            let end = r.next_lsn();
            inner.contiguous = end; // everything recovered is durable
            inner.cold_end = r.first_lsn();
            let mut from = r.first_lsn();
            while from < end {
                let es = r.entries(from, 1 << 16)?;
                anyhow::ensure!(
                    !es.is_empty(),
                    "the log's index ends at lsn {from} of {end}"
                );
                for e in es {
                    from = e.lsn + 1;
                    let log = decode_log_key(&e.meta.key)?;
                    let ix = inner.intern(&log);
                    let first = e.meta.first_seq;
                    let new = Entry {
                        lsn: e.lsn,
                        count: e.meta.count,
                        crc: e.meta.tag,
                        len: e.payload_len,
                    };
                    if let Some((s0, x)) = inner.overlap(r, ix, first, first + new.count as u64)? {
                        if s0 == first && x.crc == new.crc && x.count == new.count {
                            inner.dead.insert(e.lsn); // logged twice (a repair raced a replicate): keep the first
                            continue;
                        }
                        anyhow::bail!(
                            "conflicting batches in the log at lsn {}: {:?} seq {}",
                            e.lsn,
                            log,
                            first
                        );
                    }
                    inner.logs[ix as usize].batches.insert(first, new);
                    inner.join(ix);
                    inner.index(r, e.lsn, ix, first, new, e.meta.time_ms);
                }
                inner.absorb(r, from);
            }
        }
        Ok(s)
    }

    /// Moves the batches of sealed segments out of RAM once each of them is durable.
    pub fn absorb(&self) {
        let mut g = self.inner.lock();
        let upto = g.contiguous;
        g.absorb(&self.reader, upto);
    }

    pub fn reserve(&self, b: &Batch) -> Reserve {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let first = b.id.first_seq;
        let end = b.end_seq();
        if inner.log(&b.id.log).is_some_and(|l| first < l.1.trimmed) {
            return Reserve::Expired;
        }
        let ix = inner.intern(&b.id.log);
        match inner.overlap(&self.reader, ix, first, end) {
            Err(e) => return Reserve::Error(format!("{:?}: reading the index: {e}", b.id.log)),
            Ok(Some((s, e))) => {
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
            Ok(None) => {}
        }
        inner.logs[ix as usize].batches.insert(
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

    /// `b` as written to the log at `lsn` (its bytes may be fewer than reserved: dedup).
    pub fn mark_durable(&self, b: &Batch, lsn: Lsn) {
        let id = &b.id;
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
        e.len = b.bytes.len() as u32;
        let e = *e;
        if let Some(tx) = inner.pending.remove(&(ix, first)) {
            tx.send_replace(State::Durable);
        }
        inner.index(&self.reader, lsn, ix, first, e, now_ms());
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
        let r = &self.reader;
        inner.contiguous = inner.contiguous.max(floor);
        let gone: Vec<Lsn> = inner.order.range(..floor).map(|(&l, _)| l).collect();
        for lsn in gone {
            inner.forget(r, lsn);
        }
        let mut bases: Vec<(Lsn, Lsn)> = inner
            .streams
            .values()
            .flat_map(|s| s.cold.iter().map(|(&b, cs)| (b, cs.end)))
            .filter(|&(b, _)| b < floor)
            .collect();
        bases.sort_unstable();
        bases.dedup();
        for (base, end) in bases {
            if end <= floor {
                inner.drop_segment(r, base);
                continue;
            }
            // a segment the floor cuts through: forget its batches below the floor one by one
            let streams: Vec<String> = inner.streams.keys().cloned().collect();
            for name in streams {
                let s = &inner.streams[&name];
                if !s.cold.contains_key(&base) {
                    continue;
                }
                match inner.seg_recs(r, s, base) {
                    Ok(v) => {
                        for x in v.into_iter().filter(|x| x.lsn < floor) {
                            inner.forget_cold(r, x);
                        }
                    }
                    Err(e) => tracing::error!("reading the index of segment at {base}: {e}"),
                }
            }
        }
        inner.floor = inner.floor.max(floor);
        let kept = inner.dead.split_off(&floor);
        inner.dead = kept;
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
        let r = &self.reader;
        let over = |s: &StreamLog, time: u64| {
            max_bytes.is_some_and(|m| s.bytes > m)
                || max_age_ms.is_some_and(|a| now_ms.saturating_sub(time) > a)
        };
        let mut n = 0;
        while let Some(s) = inner.streams.get(stream) {
            // its oldest absorbed segment with batches left, read once
            if let Some((&base, _)) = s.cold.iter().find(|(_, cs)| cs.batches > 0) {
                let v = match inner.seg_recs(r, s, base) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("reading the index of segment at {base}: {e}");
                        break;
                    }
                };
                let before = n;
                for x in v {
                    if !over(&inner.streams[stream], x.time) {
                        return n;
                    }
                    inner.forget_cold(r, x);
                    n += 1;
                }
                if n == before {
                    break; // its totals disagree with its index: do not spin
                }
                continue;
            }
            let Some((&lsn, &time)) = s.order.first_key_value() else {
                break;
            };
            if !over(s, time) {
                break;
            }
            inner.forget(r, lsn);
            n += 1;
        }
        n
    }

    /// The log's durable batch starting at `seq`; an index read error counts as absent.
    fn durable_at(&self, g: &Inner, log: &LogId, seq: u64) -> Option<Entry> {
        let (ix, _) = g.log(log)?;
        match g.find(&self.reader, ix, seq) {
            Ok(e) => e.filter(|e| e.durable()),
            Err(e) => {
                tracing::error!("reading the index of {log:?}: {e}");
                None
            }
        }
    }

    /// Durable copy with this exact checksum present (or aged out by retention)?
    pub fn has(&self, id: &BatchId, crc: u32) -> bool {
        let g = self.inner.lock();
        g.log(&id.log).is_some_and(|l| id.first_seq < l.1.trimmed)
            || self
                .durable_at(&g, &id.log, id.first_seq)
                .is_some_and(|e| e.crc == crc)
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
        let lsn = self
            .durable_at(&self.inner.lock(), &id.log, id.first_seq)?
            .lsn;
        self.read_batch(lsn)
    }

    /// Durable batches at LSNs `from..`, optionally only one stream, up to about `max_bytes`.
    /// Returns the batches and the next LSN to ask for.
    pub fn tail(&self, from: u64, stream: Option<&str>, max_bytes: usize) -> (Vec<Batch>, u64) {
        let picks: Vec<(Lsn, bool)> = {
            let g = self.inner.lock();
            let mut size = 0usize;
            let mut v = vec![];
            // absorbed segments first (a subscriber far behind), from their LSN index: what the
            // store does not serve is passed over (a pick not wanted)
            let mut at = from.max(self.reader.first_lsn());
            let n = g.cold_end.saturating_sub(at).min(1024) as usize;
            let es = match self.reader.entries(at, n) {
                Ok(es) => es,
                Err(e) => {
                    tracing::error!("reading the log's index at lsn {at}: {e}");
                    return (vec![], from);
                }
            };
            for e in es {
                if size >= max_bytes {
                    break;
                }
                at = e.lsn + 1;
                let log = decode_log_key(&e.meta.key).ok();
                let want = log.and_then(|l| g.log(&l)).is_some_and(|(ix, l)| {
                    e.lsn >= g.floor.max(g.stream_of(ix).floor)
                        && !g.dead.contains(&e.lsn)
                        && stream.is_none_or(|s| l.id.stream == s)
                });
                if want {
                    size += e.payload_len as usize;
                }
                v.push((e.lsn, want));
            }
            // then RAM's, once past them
            let upto = if at >= g.cold_end { g.contiguous } else { at };
            for (&lsn, &(ix, len, _)) in g.order.range(at..upto.max(at)) {
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
            let r = &self.reader;
            let Some(sl) = g.streams.get(stream) else {
                return (vec![], next);
            };
            let from = |l: &Log| cursor.get(&l.id).copied().unwrap_or(l.trimmed);
            // start at the earliest LSN of an unread batch of any of the stream's logs
            let mut start: Option<Lsn> = None;
            for &ix in &sl.logs {
                let mut lsn = None;
                let read = g.log_from(r, ix, from(&g.logs[ix as usize]), |_, e| {
                    lsn = e.durable().then_some(e.lsn);
                    false
                });
                if let Err(e) = read {
                    tracing::error!("reading the index of stream {stream}: {e}");
                    return (vec![], next);
                }
                if let Some(l) = lsn {
                    start = Some(start.map_or(l, |s| s.min(l)));
                }
            }
            let Some(start) = start else {
                return (vec![], next);
            };
            let (mut size, mut out, mut blocked) = (0usize, vec![], BTreeSet::new());
            let read = g.stream_from(r, stream, (start, g.contiguous), 0, |x| {
                if size >= max_bytes {
                    return false;
                }
                let l = &g.logs[x.ix as usize];
                let c = next.get(&l.id).copied().unwrap_or_else(|| from(l));
                let end = x.first + x.e.count as u64;
                if blocked.contains(&x.ix) || end <= c {
                    return true;
                }
                if x.first > c {
                    blocked.insert(x.ix); // its predecessor is not here yet
                    return true;
                }
                next.insert(l.id.clone(), end);
                size += x.e.len as usize;
                out.push(x.lsn);
                true
            });
            if let Err(e) = read {
                tracing::error!("reading the index of stream {stream}: {e}");
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
            let read = g.stream_from(&self.reader, stream, (0, u64::MAX), time_ms, |x| {
                let log = &g.logs[x.ix as usize].id;
                if x.time >= time_ms && !c.contains_key(log) {
                    c.insert(log.clone(), x.first);
                }
                c.len() < sl.logs.len()
            });
            if let Err(e) = read {
                tracing::error!("reading the index of stream {stream}: {e}");
            }
        }
        for &ix in &sl.logs {
            let l = &g.logs[ix as usize];
            if !c.contains_key(&l.id) {
                c.insert(l.id.clone(), l.high.max(l.trimmed));
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
        // batches durable at or past `pos` (out of LSN order; all in RAM) cut a log's run
        let mut cut: HashMap<u32, u64> = HashMap::new();
        for &(ix, _, first) in g.order.range(pos..).map(|x| x.1) {
            let l = &g.logs[ix as usize];
            // the run from `trimmed` is the contiguous one
            if (l.trimmed..l.contig.max(l.trimmed)).contains(&first) {
                let c = cut.entry(ix).or_insert(first);
                *c = (*c).min(first);
            }
        }
        let next = g
            .logs
            .iter()
            .enumerate()
            .filter(|(_, l)| stream.is_none_or(|s| l.id.stream == s))
            .map(|(ix, l)| {
                let end = l.contig.max(l.trimmed);
                (l.id.clone(), cut.get(&(ix as u32)).copied().unwrap_or(end))
            })
            .collect();
        (pos, next)
    }

    /// The end of the log's durable batch starting at `seq`, if its LSN is below `pos` (a tail
    /// from `pos` does not return it).
    pub fn durable_below(&self, log: &LogId, seq: u64, pos: Lsn) -> Option<u64> {
        let g = self.inner.lock();
        let e = self.durable_at(&g, log, seq)?;
        (e.lsn < pos).then_some(seq + e.count as u64)
    }

    /// The LSN a subscription "from now" starts at.
    pub fn durable_len(&self) -> u64 {
        self.inner.lock().contiguous
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        let g = self.inner.lock();
        (g.batches, g.events, g.bytes)
    }

    fn frontier_of(l: &Log) -> Frontier {
        let contiguous = l.contig.max(l.trimmed);
        Frontier {
            log: l.id.clone(),
            contiguous,
            high: l.high.max(contiguous),
        }
    }

    /// A log's durable batches from sequence `from` (one that starts before it included), up to
    /// about `max_bytes` (repair).
    pub fn log_batches(&self, log: &LogId, from: u64, max_bytes: usize) -> Vec<bytes::Bytes> {
        let lsns: Vec<Lsn> = {
            let g = self.inner.lock();
            let Some((ix, _)) = g.log(log) else {
                return vec![];
            };
            let mut size = 0usize;
            let mut out = vec![];
            let read = g.log_from(&self.reader, ix, from, |_, e| {
                if size >= max_bytes {
                    return false;
                }
                if e.durable() {
                    size += e.len as usize;
                    out.push(e.lsn);
                }
                true
            });
            if let Err(e) = read {
                tracing::error!("reading the index of {log:?}: {e}");
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
                cov.push(Self::frontier_of(&g.logs[ix as usize]));
            }
            let read = g.stream_from(&self.reader, stream, (0, u64::MAX), 0, |x| {
                lsns.push(x.lsn);
                true
            });
            if let Err(e) = read {
                tracing::error!("reading the index of stream {stream}: {e}");
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

    thread_local! {
        /// Run a test with tiny segments, absorbing after each batch: most of its batches are
        /// then read from disk (the `cold` module reruns every test so).
        static COLD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    fn store() -> T {
        store_with(if COLD.get() { TINY } else { 1 << 20 })
    }

    impl T {
        fn durable(&mut self, b: &Batch) {
            let m = Meta {
                key: encode_log_key(&b.id.log),
                first_seq: b.id.first_seq,
                count: b.count,
                tag: batch_crc(&b.bytes),
                time_ms: now_ms(),
            };
            let lsn = self.log.append(&[(m, &b.bytes[..])]).unwrap();
            self.s.mark_durable(b, lsn);
            if COLD.get() {
                self.s.absorb();
            }
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
        // and a subscriber from the start gets the first kept batch, the one at the floor
        let (got, _) = t.s.tail(0, None, 1 << 20);
        let seqs: Vec<u64> = got.iter().map(|b| b.id.first_seq).collect();
        assert_eq!(seqs, [3, 4, 5]);
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
        t.s.mark_durable(&xs[0], lsns[0]);
        t.s.mark_durable(&xs[2], lsns[2]); // lsn 2 before lsn 1
        let (got, next) = t.s.tail(0, None, 1 << 20);
        assert_eq!(
            got.len(),
            1,
            "only the contiguous prefix is visible to subscribers"
        );
        t.s.mark_durable(&xs[1], lsns[1]);
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
            t.s.mark_durable(&xs[i], lsn); // seq 2 (lsn 3) is not durable yet
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
        let s2 = Store::load(r.clone()).unwrap();
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
            Store::load(r.clone())
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
        assert!(
            matches!(t.s.reserve(&a[6]), Reserve::Expired),
            "the last trimmed one neither"
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
        let s = Store::load(r.clone()).unwrap();
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
        assert_eq!(firsts(1), [0, 2, 4], "from inside the first batch");
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

    /// A segment of a few small batches: most of a test's batches end up absorbed.
    const TINY: u64 = 4096 + 600;

    fn store_with(segment_size: u64) -> T {
        let dir = tempfile::tempdir().unwrap();
        let mut c = precog_storage::Config::new(dir.path());
        c.segment_size = segment_size;
        c.spares = 0;
        let log = precog_storage::Log::open(c).unwrap();
        let s = Store::load(log.reader()).unwrap();
        T { _dir: dir, log, s }
    }

    fn kind(r: Reserve) -> &'static str {
        match r {
            Reserve::New => "new",
            Reserve::Duplicate(rx) => match *rx.borrow() {
                State::Durable => "duplicate",
                _ => "pending duplicate",
            },
            Reserve::Conflict(_) => "conflict",
            Reserve::Expired => "expired",
            Reserve::Error(_) => "error",
        }
    }

    /// Every query a node serves, as one comparable value.
    fn answers(s: &Store, logs: &[LogId], probes: &[Batch]) -> Vec<String> {
        let mut v = vec![
            format!("{:?}", s.totals()),
            format!("{:?}", s.frontiers(None)),
        ];
        let pos = s.durable_len();
        let mut live: Vec<_> = s.live_start(None).1.into_iter().collect();
        live.sort();
        v.push(format!("{pos} {live:?}"));
        for log in logs {
            for seq in 0..90 {
                let id = BatchId {
                    log: log.clone(),
                    first_seq: seq,
                };
                v.push(format!(
                    "{seq} {} {} {:?} {:?}",
                    s.has(&id, 0),
                    s.get(&id).map_or(0, |b| b.ident),
                    s.durable_below(log, seq, pos / 2),
                    s.durable_below(log, seq, pos),
                ));
                for max in [1, 400, 1 << 20] {
                    let got: Vec<u64> = s
                        .log_batches(log, seq, max)
                        .into_iter()
                        .map(|b| Batch::parse(b).unwrap().id.first_seq)
                        .collect();
                    v.push(format!("{seq} {max} {got:?}"));
                }
            }
        }
        for stream in ["s", "t", "none"] {
            for max in [1, 700, 1 << 20] {
                let (got, c) = drain(s, stream, Cursor::new(), max);
                v.push(format!("{stream} {max} {got:?} {c:?}"));
                for start in [3, 30, 60] {
                    let c: Cursor = logs.iter().map(|l| (l.clone(), start)).collect();
                    let (got, c) = drain(s, stream, c, max);
                    v.push(format!("{stream} {start} {got:?} {c:?}"));
                }
                let (got, next) = s.tail(5, Some(stream), max);
                let ids: Vec<_> = got.iter().map(|b| b.id.clone()).collect();
                v.push(format!("{stream} tail {ids:?} {next}"));
            }
            for at in [0, 1, 2, u64::MAX] {
                v.push(format!("{stream} at {at} {:?}", s.cursor_at(stream, at)));
            }
            let (ev, cov) = s.read(stream, 0, 1000);
            let ev: Vec<_> = ev.iter().map(|e| (e.event_time_us, e.seq)).collect();
            v.push(format!("{stream} read {ev:?} {cov:?}"));
        }
        for p in probes {
            let k = kind(s.reserve(p));
            s.mark_failed(&p.id);
            v.push(format!("{:?} {}", p.id, k));
        }
        v
    }

    #[test]
    fn absorbed_segments_answer_every_query_as_ram_does() {
        // Why: batches of sealed segments leave RAM and are read back from the segments' index
        // files. Every query must answer exactly as it did from RAM, or a seal or a restart
        // changes what clients and peers are told.
        let (mut hot, mut cold) = (store_with(1 << 20), store_with(TINY));
        let log = |stream: &str, writer: &str| LogId {
            stream: stream.into(),
            writer: writer.into(),
            epoch: 1,
        };
        let logs = [log("s", "w1"), log("s", "w2"), log("t", "w1")];
        let batch = |l: &LogId, seq: u64, n: u64| {
            let evs: Vec<(u64, &[u8])> = (0..n).map(|i| (seq + i, &[7u8; 40][..])).collect();
            Batch::parse(Batch::encode(l, seq, &evs).unwrap()).unwrap()
        };
        // two-event batches, a gap at 14 in s/w2, and its 20 arriving last (out of order)
        let mut xs: Vec<Batch> = (0..40u64)
            .flat_map(|i| logs.iter().map(move |l| (l.clone(), i * 2)))
            .filter(|(l, s)| !(l.writer == "w2" && (*s == 14 || *s == 20)))
            .map(|(l, s)| batch(&l, s, 2))
            .collect();
        xs.push(batch(&logs[1], 20, 2));
        for x in &xs {
            for t in [&mut hot, &mut cold] {
                assert!(matches!(t.s.reserve(x), Reserve::New));
                t.durable(x);
            }
        }
        cold.s.absorb();
        {
            let g = cold.s.inner.lock();
            assert!(
                g.order.len() < xs.len() / 10,
                "{} of {} batches still in RAM",
                g.order.len(),
                xs.len()
            );
        }
        let probes = [
            xs[5].clone(),               // a duplicate of an absorbed batch
            batch(&logs[0], 4, 1),       // same start, other content
            batch(&logs[0], 5, 2),       // inside one absorbed batch, across the next
            batch(&logs[1], 14, 2),      // the gap
            batch(&logs[1], 13, 2),      // into the gap from the batch before it
            batch(&logs[2], 200, 1),     // past everything
            batch(&log("u", "x"), 0, 1), // a new log
        ];
        let same = |hot: &T, cold: &T, when: &str| {
            let (a, b) = (
                answers(&hot.s, &logs, &probes),
                answers(&cold.s, &logs, &probes),
            );
            for (x, y) in a.iter().zip(&b) {
                assert_eq!(x, y, "{when}");
            }
            assert_eq!(a.len(), b.len());
        };
        same(&hot, &cold, "retained");
        // (the probes made the new log known to both: compare restarts with restarts)
        let reload = |t: &T| T {
            s: Store::load(t.log.reader()).unwrap(),
            ..store_with(TINY)
        };
        same(&reload(&hot), &reload(&cold), "after a restart");
        let one = xs[0].bytes.len() as u64;
        for t in [&hot, &cold] {
            assert_eq!(t.s.trim_stream("t", Some(15 * one), None, 0), 25);
        }
        same(&hot, &cold, "stream t trimmed to its budget");
        for t in [&hot, &cold] {
            t.s.trim(37);
        }
        same(&hot, &cold, "trimmed below lsn 37");
        for t in [&hot, &cold] {
            t.s.trim(70);
            t.s.absorb();
        }
        same(&hot, &cold, "trimmed below lsn 70");
        let n = hot.s.trim_stream("s", Some(10 * one), None, 0);
        assert!(n > 0);
        assert_eq!(cold.s.trim_stream("s", Some(10 * one), None, 0), n);
        same(
            &hot,
            &cold,
            "stream s trimmed to its budget after whole segments went",
        );
        // batches forgotten while still in RAM stay forgotten once their segment is absorbed
        for i in 0..8 {
            let x = batch(&logs[2], 200 + 2 * i, 2);
            for t in [&mut hot, &mut cold] {
                t.s.reserve(&x);
                t.durable(&x);
            }
        }
        let n = hot.s.trim_stream("t", Some(4 * one), None, 0);
        assert_eq!(cold.s.trim_stream("t", Some(4 * one), None, 0), n);
        for t in [&hot, &cold] {
            t.s.absorb();
        }
        same(&hot, &cold, "stream t trimmed in RAM, then absorbed");
        for t in [&hot, &cold] {
            assert!(t.s.trim_stream("s", None, Some(1), now_ms() + 60_000) > 0);
        }
        same(&hot, &cold, "stream s aged out");
    }

    #[test]
    fn trimming_past_a_gap_moves_the_frontier_over_what_follows() {
        // retention may forget a batch above a gap (it trims by LSN): the sequences below its end
        // count as present, so the contiguous run continues over the durable batches after it
        let mut t = store();
        for seq in [6, 0, 1, 2, 3, 4, 7, 8, 9] {
            let x = on("g", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        let f = t.s.frontiers(Some("g"));
        assert_eq!((f[0].contiguous, f[0].high), (5, 10), "a gap at 5");
        let one = on("g", "w", 0).bytes.len() as u64;
        assert_eq!(
            t.s.trim_stream("g", Some(8 * one), None, 0),
            1,
            "seq 6 goes"
        );
        let f = t.s.frontiers(Some("g"));
        assert_eq!((f[0].contiguous, f[0].high), (10, 10));
    }

    #[test]
    fn old_batches_cost_index_reads_only_where_they_are() {
        // Why: with batches on disk, what bounds a lookup is how many segment indexes it reads.
        // A lookup reads the segments holding what it asks for: none for what lies past the
        // absorbed batches, in RAM, trimmed or older than asked for.
        let mut t = store_with(TINY);
        for (stream, w) in [("s", "w1"), ("s", "w2"), ("t", "w1")] {
            for seq in 0..30 {
                let x = on(stream, w, seq);
                t.s.reserve(&x);
                t.durable(&x);
            }
        }
        t.s.absorb();
        let reads = |t: &T| t.s.inner.lock().reads.get();
        let cost = |t: &T, f: &dyn Fn(&Store)| {
            let r0 = reads(t);
            f(&t.s);
            reads(t) - r0
        };
        assert_eq!(
            cost(&t, &|s| {
                let x = on("s", "w1", 30);
                assert!(matches!(s.reserve(&x), Reserve::New));
                s.mark_failed(&x.id);
            }),
            0,
            "the next batch after a log's absorbed ones"
        );
        let w = |w: &str| LogId {
            stream: "s".into(),
            writer: w.into(),
            epoch: 1,
        };
        let id = |seq| BatchId {
            log: w("w1"),
            first_seq: seq,
        };
        assert_eq!(
            cost(&t, &|s| assert!(s.get(&id(0)).is_some())),
            1,
            "its segment only"
        );
        let recent = Cursor::from([(w("w1"), 30), (w("w2"), 27)]);
        assert!(
            cost(&t, &|s| assert_eq!(
                s.read_cursor("s", &recent, 1 << 20).0.len(),
                3
            )) <= 4,
            "a cursor near the end reads the last segments, not the stream's"
        );
        let later = now_ms() + 3_600_000;
        assert_eq!(cost(&t, &|s| drop(s.cursor_at("t", later))), 0, "all older");
        assert_eq!(
            cost(&t, &|s| drop(s.cursor_at("t", 0))),
            1,
            "found in the first"
        );
        let base = t.log.reader().sealed()[3].0;
        assert_eq!(cost(&t, &|s| s.trim(base)), 0, "whole segments go unread");
        assert!(t.s.trim_stream("s", Some(0), None, 0) > 0);
        assert_eq!(
            cost(&t, &|s| assert!(s.read("s", 0, 10).0.is_empty())),
            0,
            "all trimmed"
        );
        // a trim up to (not past) a gap leaves the frontier where it is, unread
        let mut t = store_with(TINY);
        for seq in [4, 0, 1, 2, 3, 6, 7, 8] {
            let x = on("g", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        let one = on("g", "w", 0).bytes.len() as u64;
        assert_eq!(
            cost(&t, &|s| assert_eq!(
                s.trim_stream("g", Some(7 * one), None, 0),
                1
            )),
            1,
            "the oldest segment, nothing past the gap"
        );
        assert_eq!(t.s.frontiers(Some("g"))[0].contiguous, 5);
    }

    #[test]
    fn an_unreadable_index_is_no_conflict() {
        // Why: a conflict is final (the producer drops the batch); an index that cannot be read
        // says nothing about the batch, so a retry checked against it must be retryable
        let mut t = store_with(TINY);
        let xs: Vec<Batch> = (0..6).map(|i| on("s", "w", i)).collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        t.s.absorb();
        for f in std::fs::read_dir(t._dir.path()).unwrap() {
            let p = f.unwrap().path();
            if p.extension().is_some_and(|e| e == "idx") {
                std::fs::File::options()
                    .write(true)
                    .open(&p)
                    .unwrap()
                    .set_len(0)
                    .unwrap();
            }
        }
        assert!(matches!(t.s.reserve(&xs[0]), Reserve::Error(_)));
    }

    #[test]
    fn a_batch_rewritten_before_its_write_counts_the_bytes_it_logged() {
        // Why: dedup drops events between reserve and write. The totals must be the logged
        // bytes, what a restart and an absorbed batch's retention read back; else a stream's
        // byte count drifts up and its retention by bytes forgets more than its budget
        let mut t = store();
        let full = b("w", 1, 0, &[(1, &[7; 300]), (2, &[8; 300])]);
        let kept = Batch::parse(full.without(&[false, true]).unwrap()).unwrap();
        assert!(matches!(t.s.reserve(&full), Reserve::New));
        t.durable(&kept);
        let one = kept.bytes.len() as u64;
        assert_eq!(t.s.totals(), (1, 2, one));
        assert_eq!(Store::load(t.log.reader()).unwrap().totals(), (1, 2, one));
        assert_eq!(t.s.trim_stream("s", Some(one), None, 0), 0, "within budget");
        assert_eq!(t.s.trim_stream("s", Some(0), None, 0), 1);
        assert_eq!(t.s.totals(), (0, 0, 0));
    }

    #[test]
    fn a_segment_leaves_ram_once_every_batch_in_it_is_durable() {
        // absorbing a segment with a batch still being written would hide that batch from every
        // reader once it is durable; a segment whose batches all are durable goes, even while the
        // next one's are not
        let mut t = store_with(TINY);
        let xs: Vec<Batch> = (0..12).map(|i| on("s", "w", i)).collect();
        let mut lsns = vec![];
        for x in &xs {
            t.s.reserve(x);
            let m = Meta {
                key: encode_log_key(&x.id.log),
                first_seq: x.id.first_seq,
                count: 1,
                tag: batch_crc(&x.bytes),
                time_ms: now_ms(),
            };
            lsns.push(t.log.append(&[(m, &x.bytes[..])]).unwrap());
        }
        let sealed = t.log.reader().sealed();
        assert!(sealed.len() >= 3);
        let edge = sealed[1].1; // the first LSN of the third segment
        for (x, &lsn) in xs.iter().zip(&lsns).filter(|p| *p.1 != 1 && *p.1 != edge) {
            t.s.mark_durable(x, lsn);
        }
        t.s.absorb();
        assert_eq!(
            t.s.inner.lock().order.range(..1).count(),
            1,
            "nothing absorbed: lsn 1 is pending"
        );
        t.s.mark_durable(&xs[1], 1);
        t.s.absorb();
        assert_eq!(
            t.s.inner.lock().order.range(..edge).count(),
            0,
            "the segments before the pending batch's"
        );
        t.s.mark_durable(&xs[edge as usize], edge);
        let (got, _) = drain(&t.s, "s", Cursor::new(), 1 << 20);
        let seqs: Vec<u64> = got.iter().map(|i| i.first_seq).collect();
        assert_eq!(seqs, (0..12).collect::<Vec<_>>());
        let (got, _) = t.s.tail(0, None, 1 << 20);
        assert_eq!(got.len(), 12);
    }

    /// Every test above again, its batches mostly absorbed (read from segment index files).
    mod cold {
        macro_rules! cold {
            ($($t:ident),* $(,)?) => {$(
                #[test]
                fn $t() {
                    super::COLD.set(true);
                    super::$t();
                }
            )*};
        }
        cold!(
            a_batch_rewritten_before_its_write_counts_the_bytes_it_logged,
            trimming_past_a_gap_moves_the_frontier_over_what_follows,
            duplicate_and_conflict,
            a_duplicate_of_a_pending_batch_learns_how_it_ends,
            frontier_gaps,
            read_orders_by_event_time_with_deterministic_ties,
            failed_reservation_is_released,
            trim_forgets_old_batches_but_keeps_frontiers_and_refuses_reingest,
            tail_never_skips_a_batch_marked_durable_out_of_lsn_order,
            a_live_feed_joins_each_log_after_its_run_below_the_start,
            load_rebuilds_from_the_log,
            load_keeps_a_batch_logged_twice_once_and_refuses_conflicting_ones,
            trim_keeps_totals_and_the_boundary_batch_exact,
            has_needs_a_durable_copy_with_the_same_checksum,
            tail_stops_near_max_bytes_and_frontiers_filter_by_stream,
            a_stream_trims_to_its_own_byte_budget_and_leaves_others_alone,
            a_stream_trims_batches_older_than_its_age_limit,
            a_cursor_reads_every_batch_once_in_writer_order_in_small_steps,
            a_cursor_from_one_node_resumes_exactly_on_another,
            a_writers_batch_waits_until_its_predecessor_is_here,
            the_end_is_past_the_last_durable_batch_of_every_log_not_a_reserved_one,
            a_cursor_can_start_at_the_end_or_at_a_time,
            a_logs_batches_from_a_sequence_serve_repair_and_frontiers_track_gaps,
            a_cursor_read_stops_at_about_max_bytes,
            a_cursor_inside_a_batch_still_gets_that_batch,
            a_writer_idle_at_its_cursor_does_not_stall_the_others,
            a_batch_exactly_its_streams_age_limit_old_is_kept,
            a_logs_batches_start_at_the_batch_holding_the_sequence,
        );
    }
}
