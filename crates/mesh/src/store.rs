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
//! A consumer replaying retention (`read_cursor`, `cursor_at`, the event-time `read`) reads
//! absorbed segments' indexes on a copy of their metadata, without the store's lock, which
//! every append takes.
//!
//! ponytail: other old lookups (a late retry's `reserve`, `has`/`get`, repair's `log_batches`,
//! retention, `tail`'s chunk of at most 1024 entries) read under the lock: each reads a few
//! segments' runs. Move them onto a view too if they measurably stall appends.

use crate::proto::{
    Batch, BatchId, Event, Frontier, LogId, decode_log_key, decode_record_key, encode_log_key,
    encode_replacing_key,
};
use parking_lot::Mutex;
use precog_storage::record::Meta;
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
    /// Some of its batches are later in this node's order than ones after them in sequence
    /// (copied forward, or landed late: a repair, a recovered log holding either).
    ooo: bool,
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
    /// Second copies of a batch logged twice (a repair raced a replicate), and absorbed batches
    /// a replacement took the place of: never served.
    dead: BTreeSet<Lsn>,
    /// LSNs past `contiguous` of batches replaced or forgotten in RAM: durable, so
    /// `contiguous` steps over them.
    superseded: BTreeSet<Lsn>,
    /// Batches copied forward (`Store::moved`) to LSNs at or past `contiguous`: not new, so a
    /// live feed starting now does not join their logs at them.
    copied: BTreeSet<Lsn>,
    /// Index reads of absorbed segments (`Reader::by_key`) so far, made under the store's lock.
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
    /// Its own retention forgot every one of its batches below this: it trims oldest first.
    floor: Age,
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
    /// At most the age of its oldest batch (exact when absorbed).
    min: Age,
}

/// The order a stream trims in, oldest first: (append time, first sequence, LSN). A copy forward
/// keeps its time and sequence; append times tie (milliseconds), and the sequence then keeps a
/// log's batches in order where the copy's later LSN would not. The LSN only orders other logs.
pub type Age = (u64, u64, Lsn);

/// A batch the cleaner may copy forward (`Store::retained`): where it is, its append time and
/// at most the bytes it adds to the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Kept {
    pub lsn: Lsn,
    pub time: u64,
    pub cost: u64,
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
            ooo: false,
        });
        self.ix.insert(log.clone(), i);
        i
    }

    fn log(&self, log: &LogId) -> Option<(u32, &Log)> {
        self.ix.get(log).map(|&i| (i, &self.logs[i as usize]))
    }

    /// Whether `seq` of the log is below its trimmed floor: raised by `trim`, or by a segment
    /// the log already dropped (retention runs before `trim` hears of it).
    fn trimmed(&self, r: &Reader, log: &LogId, seq: u64) -> bool {
        let Some((_, l)) = self.log(log) else {
            return false;
        };
        let first = r.first_lsn();
        let gone = l.cold.iter().take_while(|c| c.base < first);
        seq < gone.fold(l.trimmed, |t, c| t.max(c.hi))
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
        let own = self.stream_of(ix).floor;
        self.reads.set(self.reads.get() + 1);
        // a replacement is logged under the log's replacing key (its own run)
        let mut v = r.by_key(base, &encode_log_key(&l.id))?;
        v.extend(r.by_key(base, &encode_replacing_key(&l.id))?);
        Ok(v.into_iter()
            .filter(|e| {
                e.lsn >= self.floor
                    && (e.meta.time_ms, e.meta.first_seq, e.lsn) >= own
                    && !self.dead.contains(&e.lsn)
            })
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
        // the RAM part from the batch before `from` (it may hold it), walked lazily
        let start = l
            .batches
            .range(..=from)
            .next_back()
            .map_or(from, |(&s, _)| s);
        let mut hot = l.batches.range(start..).map(|(&s, &e)| (s, e)).peekable();
        // by first sequence: the absorbed segments whose range reaches past `from`, and RAM
        // (which may hold a late batch below any of them: it overlaps all after it)
        let mut parts: Vec<(u64, u64, Option<Lsn>)> = l
            .cold
            .iter()
            .filter(|c| c.hi > from)
            .map(|c| (c.lo, c.hi, Some(c.base)))
            .collect();
        if let Some(&(lo, _)) = hot.peek() {
            parts.push((lo, u64::MAX, None));
        }
        parts.sort_by_key(|p| p.0);
        let mut i = 0;
        while i < parts.len() {
            // parts whose ranges overlap are read before any of them is visited; the next one
            // is read only if the visit gets that far
            let (mut j, mut hi, mut v, mut with_hot) = (i, 0, vec![], false);
            while j < parts.len() && (j == i || parts[j].0 < hi) {
                hi = hi.max(parts[j].1);
                match parts[j].2 {
                    Some(base) => v.extend(self.run(r, ix, base)?.iter().map(|x| (x.first, x.e))),
                    None => with_hot = true,
                }
                j += 1;
            }
            v.sort_by_key(|x| x.0);
            let mut cold = v.into_iter().peekable();
            loop {
                // the lower first sequence of the two (RAM's only in its own group: the last)
                let next = match (cold.peek(), hot.peek().filter(|_| with_hot)) {
                    (Some(c), Some(h)) => match c.0.cmp(&h.0) {
                        std::cmp::Ordering::Less => cold.next(),
                        _ => hot.next(),
                    },
                    (Some(_), None) => cold.next(),
                    (None, Some(_)) => hot.next(),
                    (None, None) => None,
                };
                let Some((s, e)) = next else { break };
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
        l.ooo |= first < l.high;
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

    /// Drops the log's durable batch at `first` (a replacement takes its place): its LSN is
    /// never served again (its record stays in its segment's index file), and it leaves RAM.
    fn unindex(&mut self, r: &Reader, ix: u32, first: u64) -> Res<()> {
        let Some(e) = self.find(r, ix, first)?.filter(|e| e.durable()) else {
            return Ok(());
        };
        self.dead.insert(e.lsn);
        let l = &mut self.logs[ix as usize];
        let s = self.streams.get_mut(&l.id.stream).unwrap();
        if self.order.remove(&e.lsn).is_some() {
            l.batches.remove(&first);
            if s.order.remove(&e.lsn).is_some() {
                s.bytes -= e.len as u64;
            }
            // ponytail: one stays behind per replacement below `contiguous` (rare: a reused
            // identity)
            self.superseded.insert(e.lsn);
        } else {
            s.bytes -= e.len as u64;
            if let Some((_, cs)) = s.cold.range_mut(..=e.lsn).next_back() {
                cs.batches -= 1;
                cs.events -= e.count as u64;
                cs.bytes -= e.len as u64;
            }
        }
        self.batches -= 1;
        self.events -= e.count as u64;
        self.bytes -= e.len as u64;
        Ok(())
    }

    fn advance_contiguous(&mut self) {
        while self.order.contains_key(&self.contiguous) || self.superseded.remove(&self.contiguous)
        {
            self.contiguous += 1;
        }
        self.copied = self.copied.split_off(&self.contiguous);
    }

    /// Forgets the durable batch at `lsn` in RAM (retention): later copies of it are `Expired`.
    fn forget(&mut self, r: &Reader, lsn: Lsn) {
        let Some((ix, len, first)) = self.order.remove(&lsn) else {
            return;
        };
        if lsn >= self.contiguous {
            self.superseded.insert(lsn); // in the log all the same: `contiguous` steps over it
        }
        let l = &mut self.logs[ix as usize];
        let count = l.batches.remove(&first).map_or(0, |e| e.count);
        self.batches -= 1;
        self.events -= count as u64;
        self.bytes -= len as u64;
        if let Some(s) = self.streams.get_mut(&l.id.stream)
            && s.order.remove(&lsn).is_some()
        {
            s.bytes -= len as u64;
        }
        self.trim_log(r, ix, first + count as u64);
    }

    /// Forgets an absorbed batch.
    fn forget_cold(&mut self, r: &Reader, x: Rec) {
        let s = self
            .streams
            .get_mut(&self.logs[x.ix as usize].id.stream)
            .unwrap();
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

    /// A copy of what reading the stream's absorbed segments takes (O(its logs and segments)):
    /// its logs, with only `hot(log)` of their RAM batches, their absorbed runs, the floors and
    /// the dead copies. Its index reads then need no lock.
    fn view(&self, stream: &str, hot: impl Fn(&Log) -> Vec<(u64, Entry)>) -> Inner {
        let mut v = Inner {
            cold_end: self.cold_end,
            dead: self.dead.clone(),
            floor: self.floor,
            ..Default::default()
        };
        let Some(s) = self.streams.get(stream) else {
            return v;
        };
        let mut sl = StreamLog {
            cold: s.cold.clone(),
            // what `run` filters by, with the store's floor
            floor: s.floor,
            ..Default::default()
        };
        for &ix in &s.logs {
            let l = &self.logs[ix as usize];
            sl.logs.insert(v.logs.len() as u32);
            v.logs.push(Log {
                id: l.id.clone(),
                batches: hot(l).into_iter().collect(),
                cold: l.cold.clone(),
                ..*l
            });
        }
        v.streams.insert(stream.into(), sl);
        v
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
                    min: (u64::MAX, u64::MAX, Lsn::MAX),
                    ..Default::default()
                });
                cs.batches += 1;
                cs.events += e.count as u64;
                cs.bytes += len as u64;
                cs.max_time = cs.max_time.max(time);
                cs.min = cs.min.min((time, first, lsn));
            }
            self.cold_end = end;
        }
    }

    /// The batch logged at `lsn` (`m`), if it is still retained here: its log.
    fn live(&self, lsn: Lsn, m: &Meta) -> Option<u32> {
        let (l, _) = decode_record_key(&m.key).ok()?;
        let ix = *self.ix.get(&l)?;
        let age = (m.time_ms, m.first_seq, lsn);
        (lsn >= self.floor && age >= self.stream_of(ix).floor && !self.dead.contains(&lsn))
            .then_some(ix)
    }

    /// Forgets the stream's oldest batches while `over` (its bytes, the batch) says so, oldest
    /// first by `Age`: a batch copied forward keeps its append time, so it still
    /// goes first. Returns how many. Reads one absorbed segment's runs at a time: the one whose
    /// oldest may be the oldest (`ColdStream::min`), as long as it is.
    /// ponytail: scans the stream's RAM batches per segment visited; keep them by time if a
    /// stream with a large active segment trims often enough to matter.
    fn trim_while(
        &mut self,
        r: &Reader,
        stream: &str,
        mut over: impl FnMut(u64, &Rec) -> bool,
    ) -> usize {
        // a part that yields nothing (its totals disagree with its index) ends it: no spinning
        let (mut n, mut went) = (0, true);
        while went {
            let Some(s) = self.streams.get(stream) else {
                return n;
            };
            // by the oldest each may hold: RAM (exact; None) and the absorbed segments
            let ram = s
                .order
                .iter()
                .map(|(&lsn, &time)| (time, self.order[&lsn].2, lsn))
                .min();
            let mut parts: Vec<(Age, Option<Lsn>)> = s
                .cold
                .iter()
                .filter(|(_, cs)| cs.batches > 0)
                .map(|(&base, cs)| (cs.min, Some(base)))
                .chain(ram.map(|x| (x, None)))
                .collect();
            parts.sort_unstable();
            // the part to read, and the oldest any other part may hold
            let Some(&(_, base)) = parts.first() else {
                return n;
            };
            let bound = parts.get(1).map(|p| p.0);
            let mut recs = match base {
                Some(base) => match self.seg_recs(r, s, base) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("reading the index of segment at {base}: {e}");
                        return n;
                    }
                },
                None => s
                    .order
                    .iter()
                    .map(|(&lsn, &time)| {
                        let (ix, _, first) = self.order[&lsn];
                        let e = self.logs[ix as usize].batches[&first];
                        Rec {
                            lsn,
                            ix,
                            first,
                            e,
                            time,
                        }
                    })
                    .collect(),
            };
            recs.sort_unstable_by_key(|x| (x.time, x.first, x.lsn));
            let before = n;
            let mut raised = false;
            for x in recs {
                if bound.is_some_and(|b| (x.time, x.first, x.lsn) >= b) {
                    if let Some(base) = base {
                        let s = self.streams.get_mut(stream).unwrap();
                        s.cold.get_mut(&base).unwrap().min = (x.time, x.first, x.lsn);
                        raised = true;
                    }
                    break;
                }
                if !over(self.streams[stream].bytes, &x) {
                    return n;
                }
                // forgotten oldest first: everything older is gone too
                let s = self.streams.get_mut(stream).unwrap();
                s.floor = s.floor.max((x.time, x.first, x.lsn + 1));
                match base {
                    Some(_) => self.forget_cold(r, x),
                    None => self.forget(r, x.lsn),
                }
                n += 1;
            }
            went = n > before || raised;
        }
        n
    }
}

pub struct Store {
    inner: Mutex<Inner>,
    reader: Reader,
    /// Index reads of absorbed segments made without the lock (`Inner::reads`: under it).
    unlocked_reads: std::sync::atomic::AtomicU64,
    /// Woken whenever a batch becomes durable (live subscriptions).
    pub durable_notify: tokio::sync::Notify,
}

impl Store {
    /// Rebuilds the index from the segment log, one chunk of its index at a time: the batches
    /// of sealed segments end up on disk, not in RAM.
    pub fn load(reader: Reader) -> anyhow::Result<Store> {
        Self::load_with(reader, |_| {})
    }

    /// `load`, showing `each` every index entry of the log in LSN order (one pass for the
    /// node's other indexes too).
    pub fn load_with(
        reader: Reader,
        mut each: impl FnMut(&precog_storage::Entry),
    ) -> anyhow::Result<Store> {
        let s = Store {
            inner: Mutex::default(),
            reader,
            unlocked_reads: Default::default(),
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
                    each(&e);
                    let (log, replaces) = decode_record_key(&e.meta.key)?;
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
                        if !(replaces && s0 == first && x.count == new.count) {
                            anyhow::bail!(
                                "conflicting batches in the log at lsn {}: {:?} seq {}",
                                e.lsn,
                                log,
                                first
                            );
                        }
                        // a node's bytes a majority did not hold, replaced with theirs
                        inner.unindex(r, ix, first)?;
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
        if inner.trimmed(&self.reader, &b.id.log, first) {
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
        let time = self.time_of(lsn);
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
        inner.index(&self.reader, lsn, ix, first, e, time);
        inner.advance_contiguous();
        drop(g);
        self.durable_notify.notify_waiters();
    }

    /// The append time the log gave the record at `lsn`: what retention by age, a seek by time
    /// and the stream's trim order go by, the same in RAM as once absorbed.
    fn time_of(&self, lsn: Lsn) -> u64 {
        // ponytail: `lsn` was just written (the active segment's index, in RAM); were it not
        // readable, now stands in, unlogged
        self.reader
            .time_ms(lsn)
            .ok()
            .flatten()
            .unwrap_or_else(now_ms)
    }

    /// Whether `b` can replace what this node holds under its identity: a durable batch with
    /// other bytes over exactly the same sequence range. ponytail: an overlap of another shape
    /// stays a logged conflict; replace those too if writers ever reuse identities that way.
    pub fn replaceable(&self, b: &Batch) -> bool {
        let g = self.inner.lock();
        self.durable_at(&g, &b.id.log, b.id.first_seq)
            .is_some_and(|e| e.count == b.count && e.crc != b.ident)
    }

    /// `b`, logged at `lsn` with a replacing key, takes the place of the `replaceable` batch.
    pub fn replace(&self, b: &Batch, lsn: Lsn) {
        let time = self.time_of(lsn);
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let ix = inner.intern(&b.id.log);
        let first = b.id.first_seq;
        if let Err(e) = inner.unindex(&self.reader, ix, first) {
            tracing::error!("reading the index of {:?}: {e}", b.id.log);
        }
        let e = Entry {
            lsn,
            count: b.count,
            crc: b.ident,
            len: b.bytes.len() as u32,
        };
        inner.logs[ix as usize].batches.insert(first, e);
        inner.join(ix);
        inner.index(&self.reader, lsn, ix, first, e, time);
        inner.advance_contiguous();
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
        self.inner
            .lock()
            .trim_while(&self.reader, stream, |bytes, x| {
                max_bytes.is_some_and(|m| bytes > m)
                    || max_age_ms.is_some_and(|a| now_ms.saturating_sub(x.time) > a)
            })
    }

    /// Each stream's own retention floor, where it has one: below it the stream forgot every
    /// batch. Kept on disk by the node so that a restart does not bring those back.
    pub fn floors(&self) -> BTreeMap<String, Age> {
        let g = self.inner.lock();
        let set = g.streams.iter().filter(|(_, s)| s.floor > Age::default());
        set.map(|(n, s)| (n.clone(), s.floor)).collect()
    }

    /// After a restart: forgets again what each stream's own retention had forgotten (copies of
    /// it may still be on disk, in segments newer than its next batches).
    pub fn restore_floors(&self, floors: &BTreeMap<String, Age>) {
        let mut g = self.inner.lock();
        for (stream, &floor) in floors {
            g.trim_while(&self.reader, stream, |_, x| {
                (x.time, x.first, x.lsn) < floor
            });
            if let Some(s) = g.streams.get_mut(stream) {
                s.floor = s.floor.max(floor);
            }
        }
    }

    /// The end of the absorbed segments: the cleaner works only on those.
    pub fn absorbed_end(&self) -> Lsn {
        self.inner.lock().cold_end
    }

    /// The bytes of the stream's batches this node retains.
    pub fn stream_bytes(&self, stream: &str) -> u64 {
        self.inner.lock().streams.get(stream).map_or(0, |s| s.bytes)
    }

    /// What streams `keeps` still retain (not forgotten, not a dead copy) in the absorbed
    /// segments at `bases`, logged at or after `min_time`, by stream: what the cleaner copies
    /// forward before the log drops those segments.
    pub fn retained(
        &self,
        bases: &[Lsn],
        keeps: impl Fn(&str) -> bool,
        min_time: u64,
    ) -> HashMap<String, Vec<Kept>> {
        let g = self.inner.lock();
        let mut out: HashMap<String, Vec<Kept>> = HashMap::new();
        for (name, s) in g.streams.iter().filter(|(n, _)| keeps(n)) {
            for base in bases {
                let v = match g.seg_recs(&self.reader, s, *base) {
                    Ok(v) => v,
                    Err(e) => {
                        // not copied: retention then drops it, as it did before copying
                        tracing::error!("reading the index of segment at {base}: {e}");
                        continue;
                    }
                };
                out.entry(name.clone()).or_default().extend(
                    v.into_iter().filter(|x| x.time >= min_time).map(|x| {
                        // a replacement's key is the longer one
                        let k = encode_replacing_key(&g.logs[x.ix as usize].id).len();
                        Kept {
                            lsn: x.lsn,
                            time: x.time,
                            cost: precog_storage::record::encoded_len(k, x.e.len as usize) as u64,
                        }
                    }),
                );
            }
        }
        out
    }

    /// Cuts the stream's oldest batches (as its own retention does, oldest first) while `f`
    /// (its bytes, the batch's LSN) says so. Returns how many.
    pub fn cut(&self, stream: &str, mut f: impl FnMut(u64, Lsn) -> bool) -> usize {
        self.inner
            .lock()
            .trim_while(&self.reader, stream, |bytes, x| f(bytes, x.lsn))
    }

    /// Whether the batch logged at `lsn` (`m`) is still retained here (not forgotten, replaced
    /// or copied since): what the cleaner may copy.
    pub fn retains(&self, lsn: Lsn, m: &Meta) -> bool {
        self.inner.lock().live(lsn, m).is_some()
    }

    /// Batches the cleaner copied forward: each (old LSN, new LSN, its record's metadata,
    /// payload length), each of a batch it `retains`. A copy keeps the batch's identity and
    /// bytes and takes its place; the old record is never served again.
    pub fn moved<'a>(&self, copies: impl IntoIterator<Item = (Lsn, Lsn, &'a Meta, u32)>) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let mut new = vec![];
        for (old, lsn, m, len) in copies {
            let ix = inner
                .live(old, m)
                .expect("a copy is of a retained batch: Node::copy_forward checks, under its lock");
            inner.dead.insert(old);
            let l = &mut inner.logs[ix as usize];
            let s = inner.streams.get_mut(&l.id.stream).unwrap();
            if let Some((_, cs)) = s.cold.range_mut(..=old).next_back() {
                cs.batches -= 1;
                cs.events -= m.count as u64;
                cs.bytes -= len as u64;
            }
            s.order.insert(lsn, m.time_ms);
            let e = Entry {
                lsn,
                count: m.count,
                crc: m.tag,
                len,
            };
            l.batches.insert(m.first_seq, e);
            l.ooo = true;
            inner.order.insert(lsn, (ix, len, m.first_seq));
            new.push(lsn);
        }
        inner.advance_contiguous();
        let c = inner.contiguous;
        inner.copied.extend(new.into_iter().filter(|&l| l >= c));
        drop(g);
        // `contiguous` may have moved past batches logged after the copies
        self.durable_notify.notify_waiters();
    }

    /// Before the log drops the absorbed segments at `bases`: a log keeps a segment's range only
    /// up to the end of the batches of it still retained there (they go with it), so dropping
    /// it does not trim the log past the batches copied out of it.
    pub fn vacate(&self, bases: &[Lsn]) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        for ix in 0..inner.logs.len() as u32 {
            for base in bases {
                let cold = &inner.logs[ix as usize].cold;
                let Ok(i) = cold.binary_search_by_key(base, |c| c.base) else {
                    continue;
                };
                let left = match inner.run(&self.reader, ix, *base) {
                    Ok(v) => v.iter().map(|x| x.first + x.e.count as u64).max(),
                    Err(e) => {
                        tracing::error!("reading the index of segment at {base}: {e}");
                        continue;
                    }
                };
                let cold = &mut inner.logs[ix as usize].cold;
                match left {
                    Some(end) => cold[i].hi = end,
                    None => {
                        cold.remove(i);
                    }
                }
            }
        }
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
        g.trimmed(&self.reader, &id.log, id.first_seq)
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
                    e.lsn >= g.floor
                        && (e.meta.time_ms, e.meta.first_seq, e.lsn) >= g.stream_of(ix).floor
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

    /// Visits the stream's durable batches in LSN order from the LSN `start` picks (on the
    /// view; None: nothing to visit), until `f` returns false: those in absorbed segments on a
    /// view of them, their index reads without the store's lock, then the rest under it (up to
    /// `contiguous` for a reader that must never skip one). Skips absorbed segments with nothing
    /// logged at or after `min_time`.
    fn visit_stream(
        &self,
        stream: &str,
        hot: impl Fn(&Log) -> Vec<(u64, Entry)>,
        start: impl FnOnce(&Inner) -> Res<Option<Lsn>>,
        (contiguous, min_time): (bool, u64),
        mut f: impl FnMut(&Inner, Rec) -> bool,
    ) -> Res<()> {
        let r = &self.reader;
        let v = self.inner.lock().view(stream, hot);
        let mut done = false;
        let read = start(&v).and_then(|from| {
            let Some(from) = from else {
                done = true;
                return Ok(from);
            };
            v.stream_from(r, stream, (from, v.cold_end), min_time, |x| {
                done = !f(&v, x);
                !done
            })?;
            Ok(Some(from))
        });
        self.unlocked_reads
            .fetch_add(v.reads.get(), std::sync::atomic::Ordering::Relaxed);
        let from = read?;
        let Some(from) = from.filter(|_| !done) else {
            return Ok(());
        };
        let g = self.inner.lock();
        let upto = if contiguous { g.contiguous } else { Lsn::MAX };
        g.stream_from(r, stream, (from.max(v.cold_end), upto), min_time, |x| {
            f(&g, x)
        })
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
        let r = &self.reader;
        let mut next = cursor.clone();
        let from = |l: &Log| cursor.get(&l.id).copied().unwrap_or(l.trimmed);
        // each log's first RAM batch past its cursor (the one holding it, else the next): an
        // unread batch in RAM may precede the log's absorbed ones
        let hot = |l: &Log| {
            use std::ops::Bound::{Excluded, Unbounded};
            let c = from(l);
            let at = l.batches.range(..=c).next_back();
            at.into_iter()
                .chain(l.batches.range((Excluded(c), Unbounded)).next())
                .map(|(&s, &e)| (s, e))
                .collect()
        };
        // start at the earliest LSN of an unread batch of any of the stream's logs
        let start = |v: &Inner| {
            let mut start: Option<Lsn> = None;
            for ix in 0..v.logs.len() as u32 {
                let mut lsn = None;
                v.log_from(r, ix, from(&v.logs[ix as usize]), |_, e| {
                    lsn = e.durable().then_some(e.lsn);
                    false
                })?;
                if let Some(l) = lsn {
                    start = Some(start.map_or(l, |s| s.min(l)));
                }
            }
            Ok(start)
        };
        let (mut size, mut out, mut blocked) = (0usize, vec![], BTreeSet::new());
        let mut walk = Ok(());
        let read = self.visit_stream(stream, hot, start, (true, 0), |g, x| {
            if size >= max_bytes {
                return false;
            }
            let l = &g.logs[x.ix as usize];
            let c = next.get(&l.id).copied().unwrap_or_else(|| from(l));
            if blocked.contains(&l.id) || x.first + x.e.count as u64 <= c {
                return true;
            }
            if x.first > c {
                // its next batches may be later in this node's order (copied forward): take
                // them by sequence now, as far as the log goes on without a gap. That is all of
                // it this read takes: what follows lies past a gap (or the read is full).
                let mut at = c;
                let walked = g.log_from(r, x.ix, c, |s, e| {
                    let due = size < max_bytes && s == at && e.durable();
                    if due {
                        at = s + e.count as u64;
                        size += e.len as usize;
                        out.push(e.lsn);
                    }
                    due
                });
                walk = std::mem::replace(&mut walk, Ok(())).and(walked); // logged with the read's
                if at > c {
                    next.insert(l.id.clone(), at);
                }
                blocked.insert(l.id.clone());
                return true;
            }
            next.insert(l.id.clone(), x.first + x.e.count as u64);
            size += x.e.len as usize;
            out.push(x.lsn);
            true
        });
        let read = read.and(walk);
        if let Err(e) = read {
            tracing::error!("reading the index of stream {stream}: {e}");
        }
        let bytes = out
            .into_iter()
            .filter_map(|lsn| self.read_batch(lsn).map(|b| b.bytes))
            .collect();
        (bytes, next)
    }

    /// A cursor at the first batch this node logged at or after `time_ms`; `u64::MAX`: the end.
    pub fn cursor_at(&self, stream: &str, time_ms: u64) -> Cursor {
        let mut c = Cursor::new();
        // the end (u64::MAX) is polled often: no batch is at or after it, so skip the scan
        if time_ms != u64::MAX {
            // per log the lowest sequence logged at or after it: the first found, unless a log
            // has batches later in this node's order than ones after them (copied forward)
            let (logs, mut ooo) = (std::cell::Cell::new(0), false);
            let start = |v: &Inner| {
                logs.set(v.logs.len());
                Ok(Some(0))
            };
            let read = self.visit_stream(
                stream,
                |_| vec![],
                start,
                (false, time_ms),
                |g, x| {
                    let l = &g.logs[x.ix as usize];
                    if x.time >= time_ms {
                        let at = c.entry(l.id.clone()).or_insert(x.first);
                        *at = (*at).min(x.first);
                        ooo |= l.ooo;
                    }
                    ooo || c.len() < logs.get()
                },
            );
            if let Err(e) = read {
                tracing::error!("reading the index of stream {stream}: {e}");
            }
        }
        let g = self.inner.lock();
        for &ix in g.streams.get(stream).into_iter().flat_map(|s| &s.logs) {
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
        for (_, &(ix, _, first)) in g.order.range(pos..).filter(|x| !g.copied.contains(x.0)) {
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
        // what the read covers, taken before it: batches durable meanwhile may be read too, but
        // are never claimed without being read
        let mut cov: Vec<Frontier> = {
            let g = self.inner.lock();
            let logs = g.streams.get(stream).into_iter().flat_map(|s| &s.logs);
            logs.map(|&ix| Self::frontier_of(&g.logs[ix as usize]))
                .collect()
        };
        cov.sort_by(|a, b| a.log.cmp(&b.log));
        let mut lsns = vec![];
        let read = self.visit_stream(
            stream,
            |_| vec![],
            |_| Ok(Some(0)),
            (false, 0),
            |_, x| {
                lsns.push(x.lsn);
                true
            },
        );
        if let Err(e) = read {
            tracing::error!("reading the index of stream {stream}: {e}");
        }
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
            self.durable_at(b, now_ms());
        }

        /// `b` logged with append time `time_ms`.
        fn durable_at(&mut self, b: &Batch, time_ms: u64) {
            let m = Meta {
                key: encode_log_key(&b.id.log),
                first_seq: b.id.first_seq,
                count: b.count,
                tag: batch_crc(&b.bytes),
                time_ms,
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
    fn a_replacing_record_takes_a_conflicting_batchs_place_at_load() {
        // Why: a node that held bytes a majority does not logs the majority's batch with a
        // replacing key; a restart must keep the replacement, not refuse the log
        let replacing = |t: &mut T, x: &Batch| {
            let m = Meta {
                key: crate::proto::encode_replacing_key(&x.id.log),
                first_seq: x.id.first_seq,
                count: x.count,
                tag: x.ident,
                time_ms: 0,
            };
            t.log.append(&[(m, &x.bytes[..])]).unwrap();
        };
        let y = b("w", 1, 0, &[(1, b"y"), (2, b"yy")]);
        let x = b("w", 1, 0, &[(1, b"x"), (2, b"xx")]);
        let mut t = store();
        t.log_only(&y, y.ident);
        replacing(&mut t, &x);
        let s = t.reload().unwrap();
        assert_eq!(s.get(&x.id).unwrap().bytes, x.bytes);
        assert_eq!(s.totals(), (1, 2, x.bytes.len() as u64));
        assert!(matches!(s.reserve(&x), Reserve::Duplicate(_)));
        // a replacement over another range is still a conflict
        let mut t = store();
        t.log_only(&y, y.ident);
        replacing(&mut t, &b("w", 1, 0, &[(1, b"x")]));
        assert!(t.reload().is_err());
        // one whose original retention already dropped is an ordinary batch
        let mut t = store();
        replacing(&mut t, &x);
        assert_eq!(t.reload().unwrap().totals().0, 1);
    }

    #[test]
    fn a_replacement_takes_the_batchs_place_and_the_feed_steps_over_it() {
        let mut t = store();
        // an earlier batch still on its way to disk holds the feed back
        let p = b("v", 1, 0, &[(1, b"p")]);
        assert!(matches!(t.s.reserve(&p), Reserve::New));
        let p_lsn = t.log.append(&[(Meta::default(), &p.bytes[..])]).unwrap();
        let y = b("w", 1, 0, &[(1, b"y")]);
        t.s.reserve(&y);
        t.durable(&y);
        assert_eq!(t.s.durable_len(), 0);
        let x = b("w", 1, 0, &[(1, b"x")]);
        assert!(t.s.replaceable(&x));
        assert!(!t.s.replaceable(&y), "the same bytes");
        assert!(
            !t.s.replaceable(&b("w", 1, 0, &[(1, b"x"), (2, b"z")])),
            "another range"
        );
        assert!(
            !t.s.replaceable(&b("v", 1, 0, &[(1, b"q")])),
            "still pending"
        );
        assert!(
            !t.s.replaceable(&b("u", 1, 0, &[(1, b"q")])),
            "nothing there"
        );
        let lsn = t.log.append(&[(Meta::default(), &x.bytes[..])]).unwrap();
        t.s.replace(&x, lsn);
        assert_eq!(t.s.get(&x.id).unwrap().bytes, x.bytes);
        assert_eq!(t.s.totals(), (1, 1, x.bytes.len() as u64));
        t.s.mark_durable(&p, p_lsn);
        assert_eq!(t.s.durable_len(), lsn + 1, "past the replaced batch's LSN");
        let budget = (p.bytes.len() + x.bytes.len()) as u64;
        assert_eq!(
            t.s.trim_stream("s", Some(budget), None, 0),
            0,
            "the stream counts the replacement's bytes, not the old batch's"
        );
        let (fed, _) = t.s.tail(0, None, 1 << 20);
        let fed: Vec<_> = fed.iter().map(|b| b.bytes.clone()).collect();
        assert_eq!(fed, [p.bytes.clone(), x.bytes.clone()]);
    }

    #[test]
    fn a_replacement_of_an_absorbed_batch_hides_it_on_disk_too() {
        // Why: an absorbed batch is read from its segment's index file, where its record stays.
        // Its replacement must hide it from every read and keep the totals exact, also after a
        // restart and once the replacement is absorbed too
        let mut t = store_with(TINY);
        let xs: Vec<Batch> = (0..6).map(|i| b("w", 1, i, &[(1, b"y")])).collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        t.s.absorb();
        assert!(!t.s.inner.lock().order.contains_key(&0), "absorbed");
        let before = t.s.totals();
        let x = b("w", 1, 0, &[(1, b"x")]);
        assert!(t.s.replaceable(&x));
        let m = Meta {
            key: crate::proto::encode_replacing_key(&x.id.log),
            first_seq: 0,
            count: 1,
            tag: x.ident,
            time_ms: now_ms(),
        };
        let lsn = t.log.append(&[(m, &x.bytes[..])]).unwrap();
        t.s.replace(&x, lsn);
        let check = |s: &Store, when: &str| {
            assert_eq!(s.get(&x.id).unwrap().bytes, x.bytes, "{when}");
            assert_eq!(s.totals(), before, "{when}");
            assert_eq!(drain(s, "s", Cursor::new(), 1 << 20).0.len(), 6, "{when}");
            let (first, _) = s.read_cursor("s", &Cursor::new(), 1 << 20);
            assert_eq!(
                first,
                std::slice::from_ref(&x.bytes),
                "{when}: seq 0 first, its replacement"
            );
            let (fed, _) = s.tail(0, None, 1 << 20);
            assert_eq!(fed.len(), 6, "{when}");
            assert!(fed.iter().all(|b| b.bytes != xs[0].bytes), "{when}");
            assert_eq!(s.read("s", 0, 100).0.len(), 6, "{when}");
            assert!(matches!(s.reserve(&x), Reserve::Duplicate(_)), "{when}");
        };
        check(&t.s, "replaced");
        check(&t.reload().unwrap(), "after a restart");
        // the stream's bytes are exact: one byte under its total trims its oldest batch only
        let one = xs[1].bytes.len() as u64;
        assert_eq!(t.s.trim_stream("s", Some(before.2 - 1), None, 0), 1);
        assert_eq!(t.s.totals(), (5, 5, before.2 - one));
        for i in 6..12 {
            let z = b("w", 1, i, &[(1, b"z")]);
            t.s.reserve(&z);
            t.durable(&z);
        }
        t.s.absorb();
        assert_eq!(t.s.trim_stream("s", Some(t.s.totals().2), None, 0), 0);
        // (seq 1 forgotten: a cursor starts past it, so past seq 0 too)
        assert_eq!(drain(&t.s, "s", Cursor::new(), 1 << 20).0.len(), 10);
        assert!(t.s.get(&x.id).is_some_and(|b| b.bytes == x.bytes));
        // dropping the replaced batch's segment subtracts its segment's exact totals
        let floor = t.log.retain(Some(1), None).unwrap();
        assert!(floor > 1, "the replaced batch's segment dropped");
        t.s.trim(floor);
        assert_eq!(t.s.totals(), t.reload().unwrap().totals());
    }

    #[test]
    fn a_seek_finds_a_batch_logged_at_exactly_the_time_asked() {
        // Why: a seek skips an absorbed segment whose newest batch is older than the time asked;
        // a batch logged at exactly that time (its segment's newest) is where the cursor starts
        let mut t = store_with(TINY);
        let xs: Vec<Batch> = (0..8).map(|i| on("s", "w", i)).collect();
        for (i, x) in xs.iter().enumerate() {
            let m = Meta {
                key: encode_log_key(&x.id.log),
                first_seq: x.id.first_seq,
                count: 1,
                tag: batch_crc(&x.bytes),
                time_ms: 1000 * (i as u64 + 1),
            };
            t.log.append(&[(m, &x.bytes[..])]).unwrap();
        }
        let s = t.reload().unwrap();
        assert!(s.inner.lock().order.len() < xs.len(), "mostly absorbed");
        for (i, x) in xs.iter().enumerate() {
            let c = s.cursor_at("s", 1000 * (i as u64 + 1));
            assert_eq!(c[&x.id.log], x.id.first_seq, "seek to batch {i}'s time");
        }
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
        // retention may forget a batch above a gap (it trims by append time): the sequences below
        // its end count as present, so the contiguous run continues over the durable batches
        // after it
        let mut t = store();
        for (i, seq) in [6, 0, 1, 2, 3, 4, 7, 8, 9].into_iter().enumerate() {
            let x = on("g", "w", seq);
            t.s.reserve(&x);
            t.durable_at(&x, 1_000 + i as u64);
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
        let reads = |t: &T| {
            t.s.inner.lock().reads.get()
                + t.s
                    .unlocked_reads
                    .load(std::sync::atomic::Ordering::Relaxed)
        };
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
        for (i, seq) in [4, 0, 1, 2, 3, 6, 7, 8].into_iter().enumerate() {
            let x = on("g", "w", seq);
            t.s.reserve(&x);
            t.durable_at(&x, 1_000 + i as u64);
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
    fn replaying_old_batches_reads_their_index_without_the_stores_lock() {
        // Why: appends take the store's lock (reserve, mark_durable). Consumers replaying
        // retention (live-ingestion's archivers start 2 h back) read many segment indexes; under
        // that lock they would stall every producer's append
        let mut t = store_with(TINY);
        for (stream, w) in [("s", "w1"), ("s", "w2")] {
            for seq in 0..20 {
                let x = on(stream, w, seq);
                t.s.reserve(&x);
                t.durable(&x);
            }
        }
        t.s.absorb();
        let locked = |t: &T| t.s.inner.lock().reads.get();
        let r0 = locked(&t);
        let (got, _) = drain(&t.s, "s", Cursor::new(), 300);
        assert_eq!(got.len(), 40);
        assert_eq!(t.s.read("s", 0, 100).0.len(), 40);
        let c = t.s.cursor_at("s", 0);
        assert_eq!(c.values().collect::<Vec<_>>(), [&0, &0]);
        assert_eq!(locked(&t), r0, "index reads under the lock");
    }

    #[test]
    fn a_segment_retention_dropped_is_trimmed_before_the_store_hears_of_it() {
        // Why: the node drops segments from the log, then tells the store (`trim`). In between,
        // a retry of a batch in a dropped segment must not be taken as new (appended again),
        // and repair must not think the batch missing
        let mut t = store_with(TINY);
        let xs: Vec<Batch> = (0..8).map(|i| on("s", "w", i)).collect();
        for x in &xs {
            t.s.reserve(x);
            t.durable(x);
        }
        t.s.absorb();
        let floor = t.log.retain(Some(3 * TINY), None).unwrap();
        let sealed = t.log.reader().sealed();
        assert!(
            floor > 1 && sealed[0].0 == floor,
            "segments dropped, a sealed one kept"
        );
        let kinds = |t: &T| -> Vec<&str> { xs.iter().map(|x| kind(t.s.reserve(x))).collect() };
        let want: Vec<&str> = (0..8)
            .map(|lsn| if lsn < floor { "expired" } else { "duplicate" })
            .collect();
        assert_eq!(kinds(&t), want);
        assert!(t.s.has(&xs[0].id, batch_crc(&xs[0].bytes)));
        t.s.trim(floor);
        assert_eq!(kinds(&t), want);
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
    /// What a node tells clients and peers about batches by identity and sequence, not by LSN:
    /// totals, frontiers, has/get, repair's reads, cursors read to the end (each log's batches
    /// in order), seeks by time, the event-time read, and what a retry is.
    fn by_identity(s: &Store, logs: &[LogId], probes: &[Batch]) -> Vec<String> {
        let mut v = vec![
            format!("{:?}", s.totals()),
            format!("{:?}", s.frontiers(None)),
        ];
        for log in logs {
            for seq in 0..90 {
                let id = BatchId {
                    log: log.clone(),
                    first_seq: seq,
                };
                v.push(format!(
                    "{seq} {} {}",
                    s.has(&id, 0),
                    s.get(&id).map_or(0, |b| b.ident)
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
        for stream in ["s", "t"] {
            for max in [1, 700, 1 << 20] {
                for start in [None, Some(3), Some(30), Some(60)] {
                    let c: Cursor = start.map_or(Cursor::new(), |at| {
                        logs.iter().map(|l| (l.clone(), at)).collect()
                    });
                    let (got, c) = drain(s, stream, c, max);
                    let mut per: BTreeMap<LogId, Vec<u64>> = BTreeMap::new();
                    for id in got {
                        per.entry(id.log).or_default().push(id.first_seq);
                    }
                    v.push(format!("{stream} {max} {start:?} {per:?} {c:?}"));
                }
            }
            for at in [0, 1_100, 1_500, 1_799, 5_000, u64::MAX] {
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

    /// The cleaner's steps (as `Node::apply_retention` takes them) on the oldest `n` sealed
    /// segments: copy forward what streams `keeps` retain there, one record at a time (tiny
    /// segments), and with `drop`, drop the segments. Returns how many batches it copied.
    fn clean(t: &mut T, n: usize, keeps: impl Fn(&str) -> bool, drop: bool) -> usize {
        t.s.absorb();
        let segs = t.log.segments().0;
        let segs = &segs[..n];
        assert!(segs.last().unwrap().end <= t.s.absorbed_end());
        let bases: Vec<Lsn> = segs.iter().map(|s| s.base).collect();
        let mut all: Vec<Kept> =
            t.s.retained(&bases, keeps, 0)
                .into_values()
                .flatten()
                .collect();
        all.sort_unstable_by_key(|k| (k.time, k.lsn));
        for k in &all {
            let r = t.log.reader().read(k.lsn).unwrap().unwrap();
            assert!(
                k.cost
                    >= precog_storage::record::encoded_len(r.meta.key.len(), r.payload.len())
                        as u64
            );
            let lsn = t.log.append(&[(r.meta.clone(), &r.payload[..])]).unwrap();
            t.s.moved([(k.lsn, lsn, &r.meta, r.payload.len() as u32)]);
        }
        if drop {
            t.s.vacate(&bases);
            let floor = t.log.drop_until(segs.last().unwrap().end).unwrap();
            t.s.trim(floor);
        }
        all.len()
    }

    #[test]
    fn a_batch_copied_forward_answers_every_query_as_before() {
        // Why: the cleaner moves batches that streams retain to the head of the log before it
        // drops their segment. Readers, cursors and peers key on identity and sequence, so a
        // copy must change nothing anyone is told: compared with a store that never copied,
        // after the copy, after the drop, after a restart, and through each stream's own
        // retention (oldest first by append time, which a copy keeps).
        let (mut plain, mut moved) = (store_with(TINY), store_with(TINY));
        let log = |stream: &str, writer: &str| LogId {
            stream: stream.into(),
            writer: writer.into(),
            epoch: 1,
        };
        let logs = [log("s", "w1"), log("s", "w2"), log("t", "w1")];
        let batch = |l: &LogId, seq: u64| {
            let evs: Vec<(u64, &[u8])> = (0..2).map(|i| (seq + i, &[7u8; 40][..])).collect();
            Batch::parse(Batch::encode(l, seq, &evs).unwrap()).unwrap()
        };
        // two-event batches logged 10 ms apart, a gap at 14 in s/w2 that 20 lands after
        let mut xs: Vec<Batch> = (0..30u64)
            .flat_map(|i| logs.iter().map(move |l| (l.clone(), i * 2)))
            .filter(|(l, s)| !(l.writer == "w2" && (*s == 14 || *s == 20)))
            .map(|(l, s)| batch(&l, s))
            .collect();
        xs.push(batch(&logs[1], 20));
        for (i, x) in xs.iter().enumerate() {
            for t in [&mut plain, &mut moved] {
                assert!(matches!(t.s.reserve(x), Reserve::New));
                t.durable_at(x, 1_000 + 10 * i as u64);
            }
        }
        plain.s.absorb();
        let probes = [
            xs[5].clone(),        // a retry of a batch that will be copied
            batch(&logs[0], 4),   // same start, other content (an event time differs)
            batch(&logs[1], 14),  // the gap
            batch(&logs[2], 200), // past everything
        ];
        let same = |a: &T, b: &T, when: &str| {
            let (x, y) = (
                by_identity(&a.s, &logs, &probes),
                by_identity(&b.s, &logs, &probes),
            );
            for (x, y) in x.iter().zip(&y) {
                assert_eq!(x, y, "{when}");
            }
            assert_eq!(x.len(), y.len(), "{when}");
        };
        let n = moved.log.segments().0.len() / 2;
        assert!(clean(&mut moved, n, |_| true, false) > 0);
        same(&plain, &moved, "copied, its old segments still there");
        let reload = |t: &T| T {
            s: Store::load(t.log.reader()).unwrap(),
            ..store_with(TINY)
        };
        same(
            &plain,
            &reload(&moved),
            "a restart before the drop: both copies on disk",
        );
        moved.s.vacate(&[]);
        clean(&mut moved, n, |_| true, true);
        assert!(moved.log.first_lsn() > 0, "the old segments went");
        same(&plain, &moved, "copied again and the old segments dropped");
        same(&plain, &reload(&moved), "after a restart");
        // and again: copies of copies, interleaved with what came after them
        let m = moved.log.segments().0.len() / 2;
        clean(&mut moved, m, |_| true, true);
        same(&plain, &moved, "copied twice");
        let one = xs[0].bytes.len() as u64;
        for t in [&plain, &moved] {
            assert_eq!(t.s.trim_stream("t", Some(15 * one), None, 0), 15);
            assert!(t.s.trim_stream("s", None, Some(100), 1_500) > 0);
        }
        same(&plain, &moved, "each stream trimmed by its own retention");
        // (a stream's own retention is applied again after a restart: compare restarts)
        same(&reload(&plain), &reload(&moved), "trimmed, after a restart");
    }

    /// Logs a copy of each kept batch, as `Node::copy_forward` does, and hands them to `moved`.
    fn copy(t: &mut T, kept: &[Kept]) {
        for k in kept {
            let r = t.log.reader().read(k.lsn).unwrap().unwrap();
            if !t.s.retains(k.lsn, &r.meta) {
                continue; // as Node::copy_forward: forgotten or replaced since picked
            }
            let lsn = t.log.append(&[(r.meta.clone(), &r.payload[..])]).unwrap();
            t.s.moved([(k.lsn, lsn, &r.meta, r.payload.len() as u32)]);
        }
    }

    #[test]
    fn a_batch_forgotten_after_the_cleaner_picked_it_is_not_copied() {
        // Why: the cleaner picks what streams retain, then copies it; the stream's retention can
        // forget a batch in between. A copy of it would be dead on arrival with its LSN in the
        // log, holding `contiguous` there for good: live feeds would stall and nothing would be
        // absorbed again (so the node cap would stop dropping segments). It is not copied.
        let mut t = store_with(TINY);
        for seq in 0..4 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        let base = t.log.segments().0[0].base;
        let kept: Vec<Kept> =
            t.s.retained(&[base], |_| true, 0)
                .into_values()
                .flatten()
                .collect();
        assert!(!kept.is_empty());
        assert_eq!(t.s.trim_stream("s", Some(0), None, 0), 4, "all forgotten");
        copy(&mut t, &kept);
        let x = on("s", "w", 4);
        t.s.reserve(&x);
        t.durable(&x);
        assert_eq!(t.s.live_start(None).0, t.log.next_lsn(), "contiguous");
        let (got, _) = t.s.tail(0, Some("s"), 1 << 20);
        assert_eq!(got.iter().map(|b| b.id.first_seq).collect::<Vec<_>>(), [4]);
    }

    #[test]
    fn a_copy_wakes_live_feeds_waiting_on_what_was_logged_after_it() {
        // Why: a batch logged after a copy is marked durable while `contiguous` still waits at
        // the copy, so its wake-up finds nothing to serve. Taking the copy in moves `contiguous`
        // past it: feeds must be woken then, or a quiet stream's subscriber waits for the next
        // unrelated append.
        use futures::FutureExt;
        let mut t = store_with(TINY);
        for seq in 0..4 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        let base = t.log.segments().0[0].base;
        let kept: Vec<Kept> =
            t.s.retained(&[base], |_| true, 0)
                .into_values()
                .flatten()
                .collect();
        let r = t.log.reader().read(kept[0].lsn).unwrap().unwrap();
        let at = t.log.append(&[(r.meta.clone(), &r.payload[..])]).unwrap();
        let x = on("s", "other", 0);
        t.s.reserve(&x);
        t.durable(&x);
        assert_eq!(t.s.live_start(None).0, at, "held at the copy");
        let woke = t.s.durable_notify.notified();
        t.s.moved([(kept[0].lsn, at, &r.meta, r.payload.len() as u32)]);
        assert_eq!(t.s.live_start(None).0, t.log.next_lsn());
        assert!(woke.now_or_never().is_some(), "feeds woken");
    }

    #[test]
    fn a_stream_trims_a_log_in_sequence_order_when_a_copy_ties_on_append_time() {
        // Why: append times are milliseconds, so batches tie, and a copy forward keeps its time
        // but takes a later LSN. Ordered by (time, LSN), a log's copied batch would sort after
        // its next one, and the stream would forget the next one first: a hole, hidden while
        // its trimmed floor lasts and visible after a restart. A log trims in sequence order.
        let mut t = store_with(TINY);
        for seq in 0..6 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable_at(&x, 1_000);
        }
        t.s.absorb();
        let base = t.log.segments().0[0].base;
        let kept: Vec<Kept> =
            t.s.retained(&[base], |_| true, 0)
                .into_values()
                .flatten()
                .collect();
        let n = kept.len() as u64;
        assert!(n > 0 && n < 6);
        copy(&mut t, &kept);
        let one = on("s", "w", 0).bytes.len() as u64;
        assert_eq!(
            t.s.trim_stream("s", Some((6 - n) * one), None, 0),
            n as usize
        );
        let (got, _) = drain(&t.s, "s", Cursor::new(), 1 << 20);
        let seqs: Vec<u64> = got.iter().map(|b| b.first_seq).collect();
        assert_eq!(seqs, (n..6).collect::<Vec<_>>(), "the oldest went");
        let reloaded = Store::load(t.log.reader()).unwrap();
        for seq in n..6 {
            let id = BatchId {
                log: w("s", "w"),
                first_seq: seq,
            };
            assert!(reloaded.get(&id).is_some(), "{seq} is on disk");
        }
    }

    #[test]
    fn a_batch_retention_forgets_past_contiguous_does_not_hold_it_back() {
        // Why: a stream's own retention may forget a batch logged after one not marked durable
        // yet. Its LSN is in the log all the same: once the earlier one is marked, `contiguous`
        // must step over it, or live feeds stall and nothing is absorbed again.
        let mut t = store();
        let slow = on("t", "w", 0);
        t.s.reserve(&slow);
        let m = Meta {
            key: encode_log_key(&slow.id.log),
            first_seq: 0,
            count: 1,
            tag: batch_crc(&slow.bytes),
            time_ms: now_ms(),
        };
        let at = t.log.append(&[(m, &slow.bytes[..])]).unwrap();
        let x = on("s", "w", 0);
        t.s.reserve(&x);
        t.durable(&x);
        assert_eq!(t.s.trim_stream("s", Some(0), None, 0), 1);
        t.s.mark_durable(&slow, at);
        assert_eq!(t.s.live_start(None).0, t.log.next_lsn(), "contiguous");
    }

    #[test]
    fn a_live_feed_starting_now_does_not_join_a_log_at_a_copied_batch() {
        // Why: a copy is logged at a new LSN but is not new. A feed starting while the copy is
        // still past `contiguous` (an earlier LSN not marked durable yet) must not take it for
        // the log's next batch, or it would push the log's history again.
        let mut t = store_with(TINY);
        for seq in 0..6 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        let late = on("s", "other", 0);
        t.s.reserve(&late);
        let m = Meta {
            key: encode_log_key(&late.id.log),
            first_seq: 0,
            count: 1,
            tag: batch_crc(&late.bytes),
            time_ms: now_ms(),
        };
        let at = t.log.append(&[(m, &late.bytes[..])]).unwrap(); // written, not marked yet
        assert!(clean(&mut t, 1, |_| true, false) > 0);
        let w = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let (pos, next) = t.s.live_start(None);
        assert_eq!((pos, next[&w]), (at, 6), "joins w after its last batch");
        t.s.mark_durable(&late, at);
        let (pos, next) = t.s.live_start(None);
        assert_eq!((pos, next[&w]), (t.log.next_lsn(), 6));
        assert!(
            t.s.inner.lock().copied.is_empty(),
            "forgotten once contiguous"
        );
    }

    #[test]
    fn a_batch_s_age_is_its_append_time_in_the_log() {
        // Why: retention by age, a seek by time and a stream's trim order go by the time the
        // log gave a batch, the same in RAM as once on disk, and a copy forward keeps it.
        let mut t = store();
        let x = on("s", "w", 0);
        t.s.reserve(&x);
        t.durable_at(&x, 1_000);
        assert_eq!(t.s.cursor_at("s", 1_000)[&x.id.log], 0);
        assert_eq!(t.s.trim_stream("s", None, Some(60_000), 61_000), 0);
        assert_eq!(t.s.trim_stream("s", None, Some(60_000), 61_001), 1);
    }

    fn w(stream: &str, writer: &str) -> LogId {
        LogId {
            stream: stream.into(),
            writer: writer.into(),
            epoch: 1,
        }
    }

    #[test]
    fn a_seek_by_time_takes_the_lowest_sequence_logged_after_it_even_one_that_landed_late() {
        // Why: per log, a cursor is the next sequence to read. A batch that landed out of
        // order after the time asked (a repair, a copy forward) must not be skipped by a seek.
        let mut t = store();
        for (seq, time) in [(0, 1_000), (2, 1_010), (1, 1_020)] {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable_at(&x, time);
        }
        assert_eq!(t.s.cursor_at("s", 1_005)[&w("s", "w")], 1);
    }

    #[test]
    fn a_cursor_read_serves_logs_in_this_nodes_order() {
        // Why: a consumer interleaves a stream's writers as the node logged them; only a log
        // whose next batch is later in that order is taken by sequence.
        let mut t = store();
        for (wr, seq) in [("A", 0), ("B", 0), ("A", 1), ("B", 1)] {
            let x = on("s", wr, seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        let (bs, _) = t.s.read_cursor("s", &Cursor::new(), 1 << 20);
        let got: Vec<(String, u64)> = bs
            .into_iter()
            .map(|b| {
                let id = Batch::parse(b).unwrap().id;
                (id.log.writer, id.first_seq)
            })
            .collect();
        let want = [("A", 0), ("B", 0), ("A", 1), ("B", 1)];
        assert_eq!(got, want.map(|(w, s)| (w.to_string(), s)));
    }

    #[test]
    fn a_seek_by_time_stops_at_the_first_batch_of_every_log_that_arrived_in_order() {
        // Why: a seek reads segment indexes until it has every log; only a log with batches
        // that landed out of order needs all of them. A gap alone is no such thing.
        let mut t = store_with(TINY);
        for seq in [0, 1, 3, 4, 5, 6, 7, 8] {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        let reads = || {
            t.s.unlocked_reads
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let r0 = reads();
        assert_eq!(t.s.cursor_at("s", 0)[&w("s", "w")], 0);
        assert_eq!(reads() - r0, 1, "found in the first segment");
    }

    #[test]
    fn a_floor_inside_a_segment_hides_what_lies_below_it_from_every_read() {
        // Why: retention may cut through a segment; a replay reads absorbed segments on a view
        // of the store, which must carry the store's floor too.
        let mut t = store_with(TINY);
        for seq in 0..12 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        let (base, end) = t.log.reader().sealed()[1];
        assert!(end - base >= 2);
        t.s.trim(base + 1);
        let (ev, _) = t.s.read("s", 0, 1000);
        assert_eq!(ev.len() as u64, t.s.totals().1);
        assert_eq!(ev[0].seq, base + 1);
        assert_eq!(t.s.cursor_at("s", 0)[&w("s", "w")], base + 1);
    }

    #[test]
    fn a_copy_still_in_ram_is_trimmed_before_the_newer_batches_on_disk() {
        // Why: a copy keeps its batch's append time, so its stream's retention takes it first
        // even while it sits in RAM after batches logged later.
        let mut t = store_with(TINY);
        for seq in 0..9 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable_at(&x, 1_000 + 10 * seq);
        }
        assert!(clean(&mut t, 1, |_| true, true) > 0);
        assert!(!t.s.inner.lock().order.is_empty(), "the copies are in RAM");
        let one = on("s", "w", 0).bytes.len() as u64;
        let keep = t.s.stream_bytes("s") - 2 * one;
        assert_eq!(t.s.trim_stream("s", Some(keep), None, 0), 2);
        let (got, _) = drain(&t.s, "s", Cursor::new(), 1 << 20);
        let seqs: Vec<u64> = got.iter().map(|id| id.first_seq).collect();
        assert_eq!(seqs, (2..9).collect::<Vec<_>>());
    }

    #[test]
    fn a_stream_trims_oldest_first_by_append_time_across_segments() {
        // Why: append times need not rise with LSN (a copy, a clock step); the trim reads the
        // segment that may hold the oldest, and one whose oldest is gone yields to the next.
        let mut t = store_with(TINY);
        for (x, time) in [
            (on("s", "a", 0), 1_000),
            (on("s", "b", 0), 1_030),
            (on("s", "a", 1), 1_020),
            (on("s", "b", 1), 1_040),
        ] {
            t.s.reserve(&x);
            t.durable_at(&x, time);
        }
        t.s.absorb();
        assert_eq!(
            t.log.reader().sealed()[0],
            (0, 2),
            "a/0 and b/0 share a segment"
        );
        // a/0 goes with the store's floor: its segment's oldest is gone, not what it says
        t.s.trim(1);
        let one = on("s", "a", 0).bytes.len() as u64;
        assert_eq!(t.s.trim_stream("s", Some(2 * one), None, 0), 1);
        let id = |wr: &str, seq| BatchId {
            log: w("s", wr),
            first_seq: seq,
        };
        assert!(t.s.get(&id("a", 1)).is_none(), "a/1 (1020) went second");
        assert!(t.s.get(&id("b", 0)).is_some(), "b/0 (1030) stays");
    }

    /// A store with one sealed segment of four three-event batches of s/w (seq 0, 3, 6, 9,
    /// logged at 1000, 1010, 1020, 1030), absorbed, and more after it.
    fn four_in_a_segment() -> (T, Vec<Batch>) {
        let mut t = store_with(4096 + 1100);
        let xs: Vec<Batch> = (0..8u64)
            .map(|i| {
                let evs: Vec<(u64, &[u8])> = (0..3).map(|j| (3 * i + j, &[5u8; 40][..])).collect();
                Batch::parse(Batch::encode(&w("s", "w"), 3 * i, &evs).unwrap()).unwrap()
            })
            .collect();
        for (i, x) in xs.iter().enumerate() {
            t.s.reserve(x);
            t.durable_at(x, 1_000 + 10 * i as u64);
        }
        t.s.absorb();
        assert_eq!(t.log.reader().sealed()[0], (0, 4));
        (t, xs)
    }

    #[test]
    fn what_the_nodes_age_limit_drops_goes_with_its_segment_and_the_copies_keep_their_place() {
        // Why: the node's age limit is a ceiling: a batch logged before it is not copied and
        // goes with its segment (expired, never re-ingested), one logged at it is copied.
        let (mut t, xs) = four_in_a_segment();
        let keep = t.s.retained(&[0], |_| true, 1_010);
        let lsns: Vec<Lsn> = keep["s"].iter().map(|k| k.lsn).collect();
        assert_eq!(lsns, [1, 2, 3]);
        for k in &keep["s"] {
            let r = t.log.reader().read(k.lsn).unwrap().unwrap();
            if !t.s.retains(k.lsn, &r.meta) {
                continue; // as Node::copy_forward: forgotten or replaced since picked
            }
            let lsn = t.log.append(&[(r.meta.clone(), &r.payload[..])]).unwrap();
            t.s.moved([(k.lsn, lsn, &r.meta, r.payload.len() as u32)]);
        }
        t.s.vacate(&[0]);
        let floor = t.log.drop_until(4).unwrap();
        t.s.trim(floor);
        assert!(matches!(t.s.reserve(&xs[0]), Reserve::Expired));
        assert!(t.s.has(&xs[0].id, 0));
        let (got, _) = drain(&t.s, "s", Cursor::new(), 1 << 20);
        let seqs: Vec<u64> = got.iter().map(|id| id.first_seq).collect();
        assert_eq!(seqs, (1..8).map(|i| 3 * i).collect::<Vec<_>>());
    }

    #[test]
    fn a_copy_of_a_batch_retention_forgot_meanwhile_is_never_served() {
        // Why: what to copy is read before the copy is written; a batch its stream's retention
        // forgot in between must stay forgotten, not come back through its copy.
        let (mut t, xs) = four_in_a_segment();
        let keep = t.s.retained(&[0], |_| true, 0);
        let one = xs[0].bytes.len() as u64;
        assert_eq!(t.s.trim_stream("s", Some(6 * one), None, 0), 2);
        for k in &keep["s"] {
            let r = t.log.reader().read(k.lsn).unwrap().unwrap();
            if !t.s.retains(k.lsn, &r.meta) {
                continue; // as Node::copy_forward: forgotten or replaced since picked
            }
            let lsn = t.log.append(&[(r.meta.clone(), &r.payload[..])]).unwrap();
            t.s.moved([(k.lsn, lsn, &r.meta, r.payload.len() as u32)]);
        }
        assert_eq!(t.s.totals().0, 6);
        assert_eq!(t.s.read("s", 0, 1000).0.len(), 18);
        assert!(t.s.get(&xs[1].id).is_none());
        assert!(t.s.get(&xs[2].id).is_some());
    }

    #[test]
    fn a_cursor_read_takes_a_log_whose_next_batch_was_copied_forward_by_sequence() {
        // Why: a copy forward puts a log's oldest batches after its later ones in this node's
        // order. Meeting a later one first, a read takes the log from its next batch by
        // sequence (as far as `max_bytes` goes), so neither the copy nor a stuck log elsewhere
        // in the stream (G, behind a gap) keeps it from being read.
        let (mut t, mut lens) = (store_with(TINY), vec![]);
        for x in [
            on("s", "L", 0),
            on("t", "x", 0),
            on("s", "G", 1),
            on("s", "M", 0),
        ]
        .into_iter()
        .chain((1..4).map(|i| on("s", "L", i)))
        {
            t.s.reserve(&x);
            t.durable(&x);
            lens.push(x.bytes.len());
        }
        assert!(lens.iter().all(|&l| l == lens[0]));
        assert_eq!(t.log.reader().sealed()[0], (0, 2), "L/0 and t/x/0");
        assert_eq!(clean(&mut t, 1, |s| s == "s", false), 1);
        let ids = |max| {
            let (bs, _) = t.s.read_cursor("s", &Cursor::new(), max);
            bs.into_iter()
                .map(|b| {
                    let id = Batch::parse(b).unwrap().id;
                    (id.log.writer, id.first_seq)
                })
                .collect::<Vec<_>>()
        };
        let all = [("M", 0), ("L", 0), ("L", 1), ("L", 2), ("L", 3)];
        let all: Vec<(String, u64)> = all.iter().map(|&(w, s)| (w.into(), s)).collect();
        assert_eq!(ids(1 << 20), all, "everything in order, in one read");
        // a batch reserved, not yet durable, is not taken
        assert!(matches!(t.s.reserve(&on("s", "L", 4)), Reserve::New));
        let (_, c) = t.s.read_cursor("s", &Cursor::new(), 1 << 20);
        assert_eq!(c[&w("s", "L")], 4);
        assert!(
            !c.contains_key(&w("s", "G")),
            "G, behind its gap, is where it was"
        );
        assert_eq!(ids(2 * lens[0]), all[..2], "only what fits");
        let (got, _) = drain(&t.s, "s", Cursor::new(), 2 * lens[0]);
        assert_eq!(got.len(), 5, "and the rest after");
    }

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
            a_replacing_record_takes_a_conflicting_batchs_place_at_load,
            a_replacement_takes_the_batchs_place_and_the_feed_steps_over_it,
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

/// Model-based test: random reserves (retries, overlaps, conflicting contents), durable marks
/// and failures against a reference model of the per-node rules spec/mesh/Replication.tla
/// assumes (identity, overlap = conflict, durable-only visibility) and of frontier arithmetic.
#[cfg(test)]
mod model {
    use super::*;
    use crate::proto::{batch_crc, encode_log_key};
    use precog_storage::record::Meta;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    #[derive(Clone, Debug)]
    enum Op {
        /// first_seq, count, content variant
        Reserve(u64, u32, u8),
        Durable(prop::sample::Index),
        Fail(prop::sample::Index),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                3 => (0u64..8, 1u32..4, 0u8..2).prop_map(|(s, n, v)| Op::Reserve(s, n, v)),
                2 => any::<prop::sample::Index>().prop_map(Op::Durable),
                1 => any::<prop::sample::Index>().prop_map(Op::Fail),
            ],
            1..30,
        )
    }

    /// The reference: first_seq -> (end, variant, count, durable).
    type Ref = BTreeMap<u64, (u64, u8, u32, bool)>;

    fn batch(seq: u64, n: u32, v: u8) -> Batch {
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let p = [v];
        let evs: Vec<(u64, &[u8])> = (0..n).map(|_| (0, &p[..])).collect();
        Batch::parse(Batch::encode(&log, seq, &evs).unwrap()).unwrap()
    }

    proptest! {
        // each case opens a log: 64 cases of up to 30 operations
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn the_store_follows_the_identity_rules_and_frontier_arithmetic(ops in ops()) {
            let dir = tempfile::tempdir().unwrap();
            let mut c = precog_storage::Config::new(dir.path());
            c.segment_size = 1 << 20;
            let mut log = precog_storage::Log::open(c).unwrap();
            let s = Store::load(log.reader()).unwrap();
            let mut r = Ref::new();
            let mut pending: Vec<Batch> = vec![];
            for op in ops {
                match op {
                    Op::Reserve(seq, n, v) => {
                        let b = batch(seq, n, v);
                        let end = seq + n as u64;
                        let overlap = r.iter().find(|&(&f, &(e, ..))| f < end && seq < e);
                        let want = match overlap {
                            None => "new",
                            Some((&f, &(_, rv, rn, _))) if f == seq && rv == v && rn == n => "dup",
                            Some(_) => "conflict",
                        };
                        let got = match s.reserve(&b) {
                            Reserve::New => "new",
                            Reserve::Duplicate(_) => "dup",
                            Reserve::Conflict(_) => "conflict",
                            Reserve::Expired => "expired",
                            Reserve::Error(_) => "error",
                        };
                        prop_assert_eq!(got, want, "reserve {}..{} variant {}", seq, end, v);
                        if want == "new" {
                            r.insert(seq, (end, v, n, false));
                            pending.push(b);
                        }
                    }
                    Op::Durable(i) if !pending.is_empty() => {
                        let b = pending.remove(i.index(pending.len()));
                        let m = Meta {
                            key: encode_log_key(&b.id.log),
                            first_seq: b.id.first_seq,
                            count: b.count,
                            tag: batch_crc(&b.bytes),
                            time_ms: 0,
                        };
                        let lsn = log.append(&[(m, &b.bytes[..])]).unwrap();
                        s.mark_durable(&b, lsn);
                        r.get_mut(&b.id.first_seq).unwrap().3 = true;
                    }
                    Op::Fail(i) if !pending.is_empty() => {
                        let b = pending.remove(i.index(pending.len()));
                        s.mark_failed(&b.id);
                        r.remove(&b.id.first_seq);
                    }
                    _ => {}
                }
                // frontiers: the durable prefix from 0, and one past the highest durable sequence
                let mut contiguous = 0;
                while let Some(&(e, .., true)) = r.get(&contiguous) {
                    contiguous = e;
                }
                let high = r.values().filter(|x| x.3).map(|x| x.0).max().unwrap_or(0).max(contiguous);
                let f = s.frontiers(None);
                if high == 0 {
                    prop_assert!(f.is_empty());
                } else {
                    prop_assert_eq!((f[0].contiguous, f[0].high), (contiguous, high));
                }
                // durable-only visibility: exactly the durable batches are served
                let durable = r.values().filter(|x| x.3).count();
                prop_assert_eq!(s.totals().0 as usize, durable);
            }
        }
    }
}
