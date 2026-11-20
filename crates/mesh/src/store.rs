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
//! A third keeps RAM independent of the number of streams: a stream with no batch in RAM, unused
//! for a while (`Store::evict`), leaves RAM whole and is a summary on disk (`crate::summary`).
//! Any operation on it puts it back (`Inner::wake`: one read of the summary, no index read).
//! The summaries and a checkpoint are written once per retention pass (`Store::begin_checkpoint`),
//! and a restart replays only the log past the checkpoint (`Store::open`).
//!
//! A consumer replaying retention (`read_cursor`, `cursor_at`, the event-time `read`) reads
//! absorbed segments' indexes on a copy of their metadata, without the store's lock, which
//! every append takes.
//!
//! ponytail: other old lookups (a late retry's `reserve`, `has`/`get`, repair's `log_batches`,
//! retention, `tail`'s chunk of at most 1024 entries) read under the lock: each reads a few
//! segments' runs; waking a dormant stream reads its summary under it. Move them onto a view too
//! if they measurably stall appends.

use crate::proto::{
    Batch, BatchId, Event, Frontier, LogId, decode_log_key, decode_record_key, encode_log_key,
    encode_replacing_key,
};
use crate::summary::{Checkpoint, Db, LogSum, StreamSum};
use parking_lot::Mutex;
use precog_storage::record::Meta;
use precog_storage::{Lsn, Reader};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cold {
    pub base: Lsn,
    pub lo: u64,
    pub hi: u64,
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

/// A log's index is its own hash, spread over the high bits SwissTable reads.
#[derive(Default)]
struct Spread(u64);

impl Hasher for Spread {
    fn finish(&self) -> u64 {
        self.0.wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = self.0 << 8 | x as u64;
        }
    }
}

/// The logs in RAM by index: an index is free again once its stream is dormant (`Inner::put`).
#[derive(Default)]
struct Logs(HashMap<u32, Log, BuildHasherDefault<Spread>>);

impl std::ops::Index<usize> for Logs {
    type Output = Log;
    fn index(&self, i: usize) -> &Log {
        &self.0[&(i as u32)]
    }
}

impl std::ops::IndexMut<usize> for Logs {
    fn index_mut(&mut self, i: usize) -> &mut Log {
        self.0.get_mut(&(i as u32)).expect("a log in RAM")
    }
}

impl Logs {
    /// Adds a log at the next index (a view's logs, from 0).
    fn push(&mut self, l: Log) {
        self.0.insert(self.0.len() as u32, l);
    }
}

impl std::ops::Deref for Logs {
    type Target = HashMap<u32, Log, BuildHasherDefault<Spread>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Logs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Writer logs are interned: a batch refers to its log by index, so no map holds a copy of its
/// stream and writer names per batch.
#[derive(Default)]
struct Inner {
    logs: Logs,
    /// Indexes freed by dormant streams, and the next never used.
    free: Vec<u32>,
    next_ix: u32,
    ix: HashMap<LogId, u32>,
    /// The streams in RAM; the others are dormant, summaries in the database.
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
    /// Per absorbed segment (base): its end, and what every stream, dormant ones too, retains in
    /// it. Dropping it subtracts these from the totals without waking anyone.
    segs: BTreeMap<Lsn, (Lsn, Totals)>,
    /// Repair's position in each peer's log: (its incarnation, the next LSN to ask for).
    peers: BTreeMap<u32, (u64, u64)>,
    /// This node's boots, oldest first: (incarnation, where its log ended at the open).
    boots: Vec<(u64, Lsn)>,
    /// The clock of the last eviction pass (ms): a use marks a stream with it.
    now: u64,
    /// Dormant streams woken, and the time it took.
    loads: u64,
    load_ns: u64,
    evictions: u64,
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
    /// When it was last used (`Inner::now`); 0 if only retention or repair woke it.
    used: u64,
    /// Changed since its summary was last written.
    dirty: bool,
}

/// A stream's batches still retained in one absorbed segment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ColdStream {
    pub end: Lsn,
    pub batches: u64,
    pub events: u64,
    pub bytes: u64,
    pub max_time: u64,
    /// At most the age of its oldest batch (exact when absorbed).
    pub min: Age,
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

/// Retained (batches, events, bytes).
type Totals = (u64, u64, u64);

/// Boots a node keeps: a follower of an older one follows its log from the start again.
const BOOTS: usize = 64;

/// Where a peer that followed this node's log up to `from` in the boot `theirs` goes on from,
/// given its `boots` (oldest first; the last is this one): no later than where the log ended at
/// the open of any boot after `theirs` (a crash may have lost an unsynced tail, and other
/// records were logged at those LSNs since); from the start for a boot it no longer knows
/// (spec/mesh/Inventory.tla: one boot back is not enough, `MC_inventory_lastboot`).
pub fn rejoin(from: Lsn, theirs: u64, boots: &[(u64, Lsn)]) -> Lsn {
    match boots.iter().rposition(|b| b.0 == theirs) {
        Some(i) => boots[i + 1..].iter().fold(from, |m, b| m.min(b.1)),
        None => 0,
    }
}

fn corrupt(e: String) -> precog_storage::Error {
    precog_storage::Error::Corrupt(e)
}

/// Takes an absorbed batch of `n` events and `len` bytes off its segment's totals.
fn untotal(segs: &mut BTreeMap<Lsn, (Lsn, Totals)>, base: Lsn, n: u64, len: u64) {
    if let Some((_, t)) = segs.get_mut(&base) {
        (t.0, t.1, t.2) = (t.0 - 1, t.1 - n, t.2 - len);
    }
}

/// What one stream in RAM costs, about (bytes): what `Store::evict` holds to its budget.
fn cost(name: &str, s: &StreamLog, logs: &Logs) -> usize {
    let per_log: usize = s
        .logs
        .iter()
        .filter_map(|ix| logs.get(ix))
        .map(|l| {
            std::mem::size_of::<Log>()
                + 64
                + 2 * (l.id.stream.len() + l.id.writer.len())
                + 32 * l.cold.len()
                + 64 * l.batches.len()
        })
        .sum();
    std::mem::size_of::<StreamLog>()
        + 64
        + 2 * name.len()
        + 96 * s.cold.len()
        + 48 * s.order.len()
        + per_log
}

/// A dormant stream's log as RAM holds it (no batch in RAM).
fn log_of(stream: &str, l: LogSum) -> Log {
    Log {
        id: LogId {
            stream: stream.into(),
            writer: l.writer,
            epoch: l.epoch,
        },
        batches: BTreeMap::new(),
        cold: l.cold,
        trimmed: l.trimmed,
        contig: l.contig,
        high: l.high,
        ooo: l.ooo,
    }
}

impl Inner {
    /// The log's index, a new one if it is new to RAM. Its stream must be awake (`wake`).
    fn intern(&mut self, log: &LogId) -> u32 {
        if let Some(&i) = self.ix.get(log) {
            return i;
        }
        let i = self.put(Log {
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

    /// A free index for `l`.
    fn put(&mut self, l: Log) -> u32 {
        let i = match self.free.pop() {
            Some(i) => i,
            None => {
                self.next_ix += 1;
                self.next_ix - 1
            }
        };
        self.logs.insert(i, l);
        i
    }

    fn log(&self, log: &LogId) -> Option<(u32, &Log)> {
        self.ix.get(log).map(|&i| (i, &self.logs[i as usize]))
    }

    /// Puts a dormant stream back in RAM from its summary (one read of it; nothing if it is
    /// awake or unknown). `touch`: a use (an append, a read, a subscription), which keeps it
    /// in RAM for the idle time; retention and repair wake it without one.
    fn wake(&mut self, db: &Db, r: &Reader, stream: &str, touch: bool) -> Res<()> {
        if let Some(s) = self.streams.get_mut(stream) {
            if touch {
                s.used = self.now;
            }
            return Ok(());
        }
        let t = std::time::Instant::now();
        let Some(sum) = db.stream(stream).map_err(corrupt)? else {
            return Ok(());
        };
        let mut s = StreamLog {
            cold: sum.cold.into_iter().collect(),
            floor: sum.floor,
            bytes: sum.bytes,
            used: if touch { self.now } else { 0 },
            ..Default::default()
        };
        for l in sum.logs {
            let l = log_of(stream, l);
            let id = l.id.clone();
            let ix = self.put(l);
            self.ix.insert(id, ix);
            s.logs.insert(ix);
        }
        self.streams.insert(stream.into(), s);
        self.reconcile(r, stream);
        self.loads += 1;
        self.load_ns += t.elapsed().as_nanos() as u64;
        Ok(())
    }

    /// A batch in RAM at the checkpoint, back in RAM (it is counted in the summaries already).
    fn put_back(&mut self, db: &Db, r: &Reader, e: &precog_storage::Entry) -> Res<()> {
        let (log, _) = decode_record_key(&e.meta.key).map_err(|x| corrupt(x.to_string()))?;
        self.wake(db, r, &log.stream, false)?;
        let ix = self.intern(&log);
        self.join(ix);
        let first = e.meta.first_seq;
        let l = &mut self.logs[ix as usize];
        let new = Entry {
            lsn: e.lsn,
            count: e.meta.count,
            crc: e.meta.tag,
            len: e.payload_len,
        };
        l.batches.insert(first, new);
        // a segment dropped since may have trimmed the log up to it
        let at_contig = first == l.contig.max(l.trimmed);
        self.order.insert(e.lsn, (ix, new.len, first));
        self.stream_mut(ix).order.insert(e.lsn, e.meta.time_ms);
        if at_contig {
            self.advance(r, ix);
        }
        Ok(())
    }

    /// What `trim` did to the streams in RAM while this one was dormant: the absorbed segments
    /// below the floor went (a floor inside one wakes every stream first: `Store::trim`).
    fn reconcile(&mut self, r: &Reader, stream: &str) {
        let floor = self.floor;
        let s = self.streams.get_mut(stream).expect("woken");
        let logs: Vec<u32> = s.logs.iter().copied().collect();
        // (one the floor cuts through was cut while the stream was in RAM: `Store::trim`)
        let gone = s.cold.range(..floor).filter(|(_, c)| c.end <= floor);
        let dropped: Vec<Lsn> = gone.map(|x| *x.0).collect();
        for base in &dropped {
            let cs = s.cold.remove(base).expect("listed");
            s.bytes -= cs.bytes;
            s.dirty = true;
        }
        for ix in logs {
            while let Some(c) = self.logs[ix as usize].cold.first().copied() {
                if !dropped.contains(&c.base) {
                    break;
                }
                self.logs[ix as usize].cold.remove(0);
                self.trim_log(r, ix, c.hi);
            }
        }
    }

    /// The stream's summary: everything RAM holds of it but its batches in RAM.
    fn summary(&self, name: &str) -> StreamSum {
        let s = &self.streams[name];
        StreamSum {
            floor: s.floor,
            bytes: s.bytes,
            cold: s.cold.iter().map(|(&b, &c)| (b, c)).collect(),
            logs: s
                .logs
                .iter()
                .map(|&ix| {
                    let l = &self.logs[ix as usize];
                    LogSum {
                        writer: l.id.writer.clone(),
                        epoch: l.id.epoch,
                        cold: l.cold.clone(),
                        trimmed: l.trimmed,
                        contig: l.contig,
                        high: l.high,
                        ooo: l.ooo,
                    }
                })
                .collect(),
        }
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

    fn stream_mut(&mut self, ix: u32) -> &mut StreamLog {
        let name = &self.logs[ix as usize].id.stream;
        self.streams
            .get_mut(name)
            .expect("a log in RAM has its stream in RAM")
    }

    /// Adds the log to its stream's.
    fn join(&mut self, ix: u32) {
        let stream = &self.logs[ix as usize].id.stream;
        match self.streams.get_mut(stream) {
            Some(s) => {
                if s.logs.insert(ix) {
                    s.dirty = true;
                }
            }
            None => {
                let s = StreamLog {
                    logs: BTreeSet::from([ix]),
                    used: self.now,
                    dirty: true,
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
        // or its predecessor is later in the log: marked durable first (any earlier batch later
        // in the log than this one is later than its predecessor, or the flag is up already)
        let pred = l.batches.range(..first).next_back();
        l.ooo |= pred.is_some_and(|(_, p)| p.durable() && p.lsn > lsn);
        l.high = l.high.max(first + e.count as u64);
        let at_contig = first == l.contig.max(l.trimmed);
        let s = self
            .streams
            .get_mut(&l.id.stream)
            .expect("a log joins its stream when reserved");
        s.order.insert(lsn, time_ms);
        s.bytes += e.len as u64;
        s.dirty = true;
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
        self.stream_mut(ix).dirty = true;
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
        s.dirty = true;
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
            if let Some((&base, cs)) = s.cold.range_mut(..=e.lsn).next_back() {
                cs.batches -= 1;
                cs.events -= e.count as u64;
                cs.bytes -= e.len as u64;
                untotal(&mut self.segs, base, e.count as u64, e.len as u64);
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
        if let Some((&base, cs)) = s.cold.range_mut(..=x.lsn).next_back() {
            cs.batches -= 1;
            cs.events -= x.e.count as u64;
            cs.bytes -= x.e.len as u64;
            untotal(&mut self.segs, base, x.e.count as u64, x.e.len as u64);
        }
        self.batches -= 1;
        self.events -= x.e.count as u64;
        self.bytes -= x.e.len as u64;
        self.trim_log(r, x.ix, x.first + x.e.count as u64);
    }

    /// Forgets the absorbed segment at `base` whole (the log dropped it): its totals, and what
    /// the streams in RAM hold of it (a dormant stream catches up when it wakes: `reconcile`).
    fn drop_segment(&mut self, r: &Reader, base: Lsn) {
        if let Some((_, t)) = self.segs.remove(&base) {
            self.batches -= t.0;
            self.events -= t.1;
            self.bytes -= t.2;
        }
        for s in self.streams.values_mut() {
            if let Some(cs) = s.cold.remove(&base) {
                s.bytes -= cs.bytes;
                s.dirty = true;
            }
        }
        let ixs: Vec<u32> = self.logs.keys().copied().collect();
        for ix in ixs {
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
                let (_, t) = self.segs.entry(base).or_insert((end, (0, 0, 0)));
                (t.0, t.1, t.2) = (t.0 + 1, t.1 + e.count as u64, t.2 + len as u64);
                let s = self.streams.get_mut(&l.id.stream).unwrap();
                s.dirty = true;
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

    /// The batch logged at `lsn` (`m`), if it is still retained here: its log. Its stream must
    /// be awake.
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
        'parts: while went {
            let Some(s) = self.streams.get(stream) else {
                break 'parts;
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
                break 'parts;
            };
            let bound = parts.get(1).map(|p| p.0);
            let mut recs = match base {
                Some(base) => match self.seg_recs(r, s, base) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!("reading the index of segment at {base}: {e}");
                        break 'parts;
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
                        s.dirty = true;
                        raised = true;
                    }
                    break;
                }
                if !over(self.streams[stream].bytes, &x) {
                    break 'parts;
                }
                // forgotten oldest first: everything older is gone too
                let s = self.streams.get_mut(stream).unwrap();
                s.floor = s.floor.max((x.time, x.first, x.lsn + 1));
                s.dirty = true;
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

    /// Puts dormant streams that are not used for `idle_ms` out of RAM, and the least recently
    /// used ones while the streams in RAM cost more than `budget` bytes. Only a stream whose
    /// summary is written (not `dirty`) and with no batch in RAM leaves.
    fn evict(&mut self, idle_ms: u64, budget: usize) -> usize {
        let mut held: usize = self
            .streams
            .iter()
            .map(|(n, s)| cost(n, s, &self.logs))
            .sum();
        let mut out: Vec<(u64, String)> = self
            .streams
            .iter()
            .filter(|(_, s)| {
                !s.dirty
                    && s.order.is_empty()
                    && s.logs
                        .iter()
                        .all(|&ix| self.logs[ix as usize].batches.is_empty())
            })
            .map(|(n, s)| (s.used, n.clone()))
            .collect();
        out.sort_unstable();
        let mut n = 0;
        for (used, name) in out {
            if self.now.saturating_sub(used) < idle_ms && held <= budget {
                break;
            }
            let s = self.streams.remove(&name).expect("listed");
            held = held.saturating_sub(cost(&name, &s, &self.logs));
            for ix in s.logs {
                let l = self.logs.remove(&ix).expect("a stream's log is in RAM");
                self.ix.remove(&l.id);
                self.free.push(ix);
            }
            n += 1;
        }
        // the maps keep their capacity: give it back once most of it is free
        if self.streams.capacity() > 2 * self.streams.len() {
            self.streams.shrink_to_fit();
            self.ix.shrink_to_fit();
            self.logs.shrink_to_fit();
        }
        self.evictions += n as u64;
        n
    }
}

/// How long a stream may go unused before it leaves RAM, and what the streams in RAM may cost.
#[derive(Clone, Copy, Debug)]
pub struct Dormancy {
    pub idle_ms: u64,
    pub budget: usize,
}

/// Counters of the hot set (metrics).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HotStats {
    /// Streams in RAM.
    pub hot: u64,
    /// Streams with a summary on disk and not in RAM.
    pub dormant: u64,
    pub loads: u64,
    pub load_ns: u64,
    pub evictions: u64,
}

/// A checkpoint taken under the store's lock, to be written once the log has synced what it
/// covers (`Store::end_checkpoint`).
pub struct Pending {
    cp: Checkpoint,
    streams: Vec<(String, Vec<u8>)>,
}

pub struct Store {
    inner: Mutex<Inner>,
    reader: Reader,
    db: Db,
    /// One checkpoint at a time: the database's view moves only forward.
    writing: Mutex<()>,
    /// Checkpoints taken and not yet written: their streams count as written, so none leaves
    /// RAM meanwhile (spec/store/Dormant.tla, `MC_dormant_evict_in_flight`).
    in_flight: AtomicU64,
    /// Streams with a summary on disk (after the last checkpoint).
    known: AtomicU64,
    /// Index reads of absorbed segments made without the lock (`Inner::reads`: under it).
    unlocked_reads: std::sync::atomic::AtomicU64,
    /// Woken whenever a batch becomes durable (live subscriptions).
    pub durable_notify: tokio::sync::Notify,
}

impl Store {
    /// Rebuilds the index from the segment log, one chunk of its index at a time: the batches
    /// of sealed segments end up on disk, not in RAM. Its summaries are in RAM (no data
    /// directory: tests and tools).
    pub fn load(reader: Reader) -> anyhow::Result<Store> {
        Self::open(reader, Db::memory())
    }

    /// Opens the store over the segment log with the summaries in `db`: back to the last
    /// checkpoint, then the log past it replayed. Without a checkpoint (a new database) the whole
    /// log is replayed. Streams the replay does not touch stay dormant.
    pub fn open(reader: Reader, db: Db) -> anyhow::Result<Store> {
        let cp = db.checkpoint().map_err(anyhow::Error::msg)?;
        let known = AtomicU64::new(0);
        let s = Store {
            inner: Mutex::default(),
            reader,
            db,
            writing: Mutex::new(()),
            in_flight: AtomicU64::new(0),
            known,
            unlocked_reads: Default::default(),
            durable_notify: tokio::sync::Notify::new(),
        };
        {
            let r = &s.reader;
            let mut g = s.inner.lock();
            let inner = &mut *g;
            inner.now = now_ms();
            let (first, end) = (r.first_lsn(), r.next_lsn());
            let cp = cp.unwrap_or(Checkpoint {
                cold_end: first,
                contiguous: first,
                ..Default::default()
            });
            anyhow::ensure!(
                cp.contiguous <= end,
                "the streams database covers the log up to lsn {}, but the log ends at {end}: \
                 remove streams.redb to rebuild it from the log",
                cp.contiguous
            );
            inner.floor = cp.floor.max(first);
            (inner.batches, inner.events, inner.bytes) = cp.totals;
            inner.segs = cp.segs.into_iter().map(|(b, e, t)| (b, (e, t))).collect();
            inner.dead = cp.dead.into_iter().filter(|&l| l >= first).collect();
            inner.peers = cp.peers.into_iter().map(|(p, i, n)| (p, (i, n))).collect();
            inner.boots = cp.boots;
            // segments retention dropped after the checkpoint
            let gone: Vec<Lsn> = (inner.segs.iter())
                .filter(|(_, (end, _))| *end <= first)
                .map(|x| *x.0)
                .collect();
            for base in gone {
                inner.drop_segment(r, base);
            }
            let in_ram = |lsn: Lsn| {
                let i = cp.ram.partition_point(|&(a, _)| a <= lsn);
                i > 0 && lsn < cp.ram[i - 1].1
            };
            inner.contiguous = end; // everything recovered is durable
            let mut from = cp.cold_end.max(first);
            inner.cold_end = from;
            // the batches in RAM at the checkpoint first (counted already): a batch replayed as
            // new after them sees those later in the log than it
            for &(a, b) in &cp.ram {
                let mut at = a.max(from);
                while at < b.min(end) {
                    let es = r.entries(at, (b.min(end) - at).min(1 << 16) as usize)?;
                    anyhow::ensure!(!es.is_empty(), "the log's index ends at lsn {at}");
                    for e in es {
                        at = e.lsn + 1;
                        inner.put_back(&s.db, r, &e)?;
                    }
                }
            }
            while from < end {
                let es = r.entries(from, 1 << 16)?;
                anyhow::ensure!(
                    !es.is_empty(),
                    "the log's index ends at lsn {from} of {end}"
                );
                for e in es {
                    from = e.lsn + 1;
                    let (log, replaces) = decode_record_key(&e.meta.key)?;
                    inner.wake(&s.db, r, &log.stream, false)?;
                    let ix = inner.intern(&log);
                    let first = e.meta.first_seq;
                    let new = Entry {
                        lsn: e.lsn,
                        count: e.meta.count,
                        crc: e.meta.tag,
                        len: e.payload_len,
                    };
                    if e.lsn < cp.contiguous || in_ram(e.lsn) {
                        continue; // indexed at the checkpoint (and back in RAM if it was there)
                    }
                    // forgotten in RAM past `contiguous` (retention), or superseded (`dead`)
                    if inner.dead.contains(&e.lsn) {
                        continue;
                    }
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
        s.count_known()?;
        Ok(s)
    }

    fn count_known(&self) -> anyhow::Result<()> {
        let n = self.db.streams().map_err(anyhow::Error::msg)?;
        self.known.store(n, Relaxed);
        Ok(())
    }

    /// Moves the batches of sealed segments out of RAM once each of them is durable.
    pub fn absorb(&self) {
        let mut g = self.inner.lock();
        let upto = g.contiguous;
        g.absorb(&self.reader, upto);
    }

    /// Wakes the stream (see `Inner::wake`), logging what fails.
    fn wake(&self, g: &mut Inner, stream: &str, touch: bool) -> bool {
        match g.wake(&self.db, &self.reader, stream, touch) {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("waking stream {stream:?}: {e}");
                false
            }
        }
    }

    pub fn reserve(&self, b: &Batch) -> Reserve {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        if let Err(e) = inner.wake(&self.db, &self.reader, &b.id.log.stream, true) {
            return Reserve::Error(format!("{:?}: waking its stream: {e}", b.id.log));
        }
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
        let mut g = self.inner.lock();
        self.wake(&mut g, &b.id.log.stream, false);
        self.durable_at(&g, &b.id.log, b.id.first_seq)
            .is_some_and(|e| e.count == b.count && e.crc != b.ident)
    }

    /// `b`, logged at `lsn` with a replacing key, takes the place of the `replaceable` batch.
    pub fn replace(&self, b: &Batch, lsn: Lsn) {
        let time = self.time_of(lsn);
        let mut g = self.inner.lock();
        let inner = &mut *g;
        self.wake(inner, &b.id.log.stream, false);
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
        // whole segments of dormant streams too: their totals go now
        bases.extend(inner.segs.range(..floor).map(|(&b, &(end, _))| (b, end)));
        bases.sort_unstable();
        bases.dedup();
        // a floor inside a segment (the node drops whole ones; tests) cuts dormant streams too
        if bases.iter().any(|&(_, end)| end > floor) {
            let mut names = vec![];
            let read = self.db.each(|n, _| names.push(n.to_string()));
            read.unwrap_or_else(|e| tracing::error!("{e}"));
            for name in names {
                self.wake(inner, &name, false);
            }
        }
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
        self.wake(&mut g, stream, false);
        g.trim_while(&self.reader, stream, |bytes, x| {
            max_bytes.is_some_and(|m| bytes > m)
                || max_age_ms.is_some_and(|a| now_ms.saturating_sub(x.time) > a)
        })
    }

    /// After a rebuild from the log (an older node's data directory, `<data_dir>/floors`):
    /// forgets again what each stream's own retention had forgotten (copies of it may still be
    /// on disk, in segments newer than its next batches).
    pub fn restore_floors(&self, floors: &BTreeMap<String, Age>) {
        let mut g = self.inner.lock();
        for (stream, &floor) in floors {
            self.wake(&mut g, stream, false);
            g.trim_while(&self.reader, stream, |_, x| {
                (x.time, x.first, x.lsn) < floor
            });
            if let Some(s) = g.streams.get_mut(stream) {
                s.floor = s.floor.max(floor);
                s.dirty = true;
            }
        }
    }

    /// The end of the absorbed segments: the cleaner works only on those.
    pub fn absorbed_end(&self) -> Lsn {
        self.inner.lock().cold_end
    }

    /// The bytes of the stream's batches this node retains.
    pub fn stream_bytes(&self, stream: &str) -> u64 {
        let mut g = self.inner.lock();
        self.wake(&mut g, stream, false);
        g.streams.get(stream).map_or(0, |s| s.bytes)
    }

    /// What `streams` still retain (not forgotten, not a dead copy) in the absorbed segments at
    /// `bases`, logged at or after `min_time`, by stream: what the cleaner copies forward before
    /// the log drops those segments.
    pub fn retained(
        &self,
        bases: &[Lsn],
        streams: &[&str],
        min_time: u64,
    ) -> HashMap<String, Vec<Kept>> {
        let mut g = self.inner.lock();
        let mut out: HashMap<String, Vec<Kept>> = HashMap::new();
        for &name in streams {
            self.wake(&mut g, name, false);
            let Some(s) = g.streams.get(name) else {
                continue;
            };
            for base in bases {
                let v = match g.seg_recs(&self.reader, s, *base) {
                    Ok(v) => v,
                    Err(e) => {
                        // not copied: retention then drops it, as it did before copying
                        tracing::error!("reading the index of segment at {base}: {e}");
                        continue;
                    }
                };
                out.entry(name.to_string()).or_default().extend(
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
        let mut g = self.inner.lock();
        self.wake(&mut g, stream, false);
        g.trim_while(&self.reader, stream, |bytes, x| f(bytes, x.lsn))
    }

    /// The stream the record with key `key` belongs to, awake.
    fn wake_key(&self, g: &mut Inner, key: &[u8]) {
        if let Ok((l, _)) = decode_record_key(key) {
            self.wake(g, &l.stream, false);
        }
    }

    /// Whether the batch logged at `lsn` (`m`) is still retained here (not forgotten, replaced
    /// or copied since): what the cleaner may copy.
    pub fn retains(&self, lsn: Lsn, m: &Meta) -> bool {
        let mut g = self.inner.lock();
        self.wake_key(&mut g, &m.key);
        g.live(lsn, m).is_some()
    }

    /// Batches the cleaner copied forward: each (old LSN, new LSN, its record's metadata,
    /// payload length), each of a batch it `retains`. A copy keeps the batch's identity and
    /// bytes and takes its place; the old record is never served again.
    pub fn moved<'a>(&self, copies: impl IntoIterator<Item = (Lsn, Lsn, &'a Meta, u32)>) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let mut new = vec![];
        for (old, lsn, m, len) in copies {
            self.wake_key(inner, &m.key);
            let ix = inner
                .live(old, m)
                .expect("a copy is of a retained batch: Node::copy_forward checks, under its lock");
            inner.dead.insert(old);
            let l = &mut inner.logs[ix as usize];
            let s = inner.streams.get_mut(&l.id.stream).unwrap();
            s.dirty = true;
            if let Some((&base, cs)) = s.cold.range_mut(..=old).next_back() {
                cs.batches -= 1;
                cs.events -= m.count as u64;
                cs.bytes -= len as u64;
                untotal(&mut inner.segs, base, m.count as u64, len as u64);
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
    /// it does not trim the log past the batches copied out of it. Only logs in RAM: copies are
    /// of streams `retained` woke, and for any other log the range's end is what it retains
    /// there or below its trimmed floor already.
    pub fn vacate(&self, bases: &[Lsn]) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let ixs: Vec<u32> = inner.logs.keys().copied().collect();
        for ix in ixs {
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
                inner.stream_mut(ix).dirty = true;
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
        let mut g = self.inner.lock();
        self.wake(&mut g, &id.log.stream, false);
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
        let mut g = self.inner.lock();
        self.wake(&mut g, &id.log.stream, true);
        let lsn = self.durable_at(&g, &id.log, id.first_seq)?.lsn;
        drop(g);
        self.read_batch(lsn)
    }

    /// A stream's own floor, in RAM or from its summary, remembered in `seen` for the call.
    fn floor_of(&self, g: &Inner, stream: &str, seen: &mut HashMap<String, Age>) -> Age {
        if let Some(s) = g.streams.get(stream) {
            return s.floor;
        }
        if let Some(&f) = seen.get(stream) {
            return f;
        }
        let f = self
            .db
            .stream(stream)
            .inspect_err(|e| tracing::error!("{e}"));
        let f = f.ok().flatten().map_or((0, 0, 0), |s| s.floor);
        seen.insert(stream.to_string(), f);
        f
    }

    /// Durable batches at LSNs `from..`, optionally only one stream, up to about `max_bytes`.
    /// Returns the batches and the next LSN to ask for.
    pub fn tail(&self, from: u64, stream: Option<&str>, max_bytes: usize) -> (Vec<Batch>, u64) {
        let picks: Vec<(Lsn, bool)> = {
            let mut g = self.inner.lock();
            if let Some(s) = stream {
                self.wake(&mut g, s, true);
            }
            let mut size = 0usize;
            let mut v = vec![];
            // absorbed segments first (a subscriber far behind), from their LSN index: what the
            // store does not serve is passed over (a pick not wanted). A dormant stream's floor
            // comes from its summary.
            let mut at = from.max(self.reader.first_lsn());
            let n = g.cold_end.saturating_sub(at).min(1024) as usize;
            let es = match self.reader.entries(at, n) {
                Ok(es) => es,
                Err(e) => {
                    tracing::error!("reading the log's index at lsn {at}: {e}");
                    return (vec![], from);
                }
            };
            let mut floors = HashMap::new();
            for e in es {
                if size >= max_bytes {
                    break;
                }
                at = e.lsn + 1;
                let want = decode_log_key(&e.meta.key).is_ok_and(|l| {
                    stream.is_none_or(|s| l.stream == s)
                        && e.lsn >= g.floor
                        && !g.dead.contains(&e.lsn)
                        && (e.meta.time_ms, e.meta.first_seq, e.lsn)
                            >= self.floor_of(&g, &l.stream, &mut floors)
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
        let v = {
            let mut g = self.inner.lock();
            g.wake(&self.db, r, stream, true)?;
            g.view(stream, hot)
        };
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
        let mut g = self.inner.lock();
        // it may have left RAM since: back for the rest
        g.wake(&self.db, r, stream, true)?;
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
        let mut g = self.inner.lock();
        self.wake(&mut g, stream, true);
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
    /// LSN). Read under one lock, so no batch falls between the two. A feed of every stream
    /// (`None`) reads every dormant stream's summary for it.
    pub fn live_start(&self, stream: Option<&str>) -> (Lsn, HashMap<LogId, u64>) {
        let mut g = self.inner.lock();
        if let Some(s) = stream {
            self.wake(&mut g, s, true);
        }
        let dormant = self.dormant_logs(&mut g, stream);
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
        let mut next: HashMap<LogId, u64> = g
            .logs
            .iter()
            .filter(|(_, l)| stream.is_none_or(|s| l.id.stream == s))
            .map(|(ix, l)| {
                let end = l.contig.max(l.trimmed);
                (l.id.clone(), cut.get(ix).copied().unwrap_or(end))
            })
            .collect();
        // a dormant log has no batch in RAM: its run ends at its contiguous end
        for l in dormant {
            next.insert(l.id, l.contig.max(l.trimmed));
        }
        (pos, next)
    }

    /// The logs of every dormant stream (from their summaries): `stream` None. A stream whose
    /// summary predates segments retention dropped since is woken instead (`reconcile`): its
    /// logs are then in RAM. Any other `stream`: nothing.
    fn dormant_logs(&self, g: &mut Inner, stream: Option<&str>) -> Vec<Log> {
        let (mut out, mut stale) = (vec![], vec![]);
        let floor = g.floor;
        let read = self.db.each(|name, s| {
            if stream.is_some() || g.streams.contains_key(name) {
                return;
            }
            match s.cold.first().is_some_and(|c| c.0 < floor) {
                true => stale.push(name.to_string()),
                false => out.extend(s.logs.into_iter().map(|l| log_of(name, l))),
            }
        });
        if let Err(e) = read {
            tracing::error!("{e}");
        }
        for name in stale {
            self.wake(g, &name, false);
        }
        out
    }

    /// The end of the log's durable batch starting at `seq`, if its LSN is below `pos` (a tail
    /// from `pos` does not return it).
    pub fn durable_below(&self, log: &LogId, seq: u64, pos: Lsn) -> Option<u64> {
        let mut g = self.inner.lock();
        self.wake(&mut g, &log.stream, true);
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
            let mut g = self.inner.lock();
            self.wake(&mut g, &log.stream, false);
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

    /// The frontiers of the stream's logs, or of every log (`None`: dormant streams' from their
    /// summaries, a read of every one of them).
    pub fn frontiers(&self, stream: Option<&str>) -> Vec<Frontier> {
        let mut g = self.inner.lock();
        if let Some(s) = stream {
            self.wake(&mut g, s, false);
        }
        let dormant = self.dormant_logs(&mut g, stream);
        let mut v: Vec<_> = g
            .logs
            .values()
            .filter(|l| stream.is_none_or(|s| l.id.stream == s))
            .map(Self::frontier_of)
            .collect();
        v.extend(dormant.iter().map(Self::frontier_of));
        v.retain(|f| f.high > 0);
        v.sort_by(|a, b| a.log.cmp(&b.log));
        v
    }

    /// Repair's view of this node's log from LSN `from`: the next LSN to ask for, the end of what
    /// is durable here now, and the frontier of every log with a batch logged in between (at most
    /// `max` records of it). A peer that follows it compares only those logs, not every one.
    pub fn inventory(&self, from: Lsn, max: usize) -> (Lsn, Lsn, Vec<Frontier>) {
        let end = self.durable_len();
        let from = from.max(self.reader.first_lsn());
        let n = end.saturating_sub(from).min(max as u64) as usize;
        let es = match self.reader.entries(from, n) {
            Ok(es) => es,
            Err(e) => {
                tracing::error!("reading the log's index at lsn {from}: {e}");
                return (from, end, vec![]);
            }
        };
        let next = es.last().map_or(from.max(end.min(from)), |e| e.lsn + 1);
        let logs: BTreeSet<LogId> = es
            .iter()
            .filter_map(|e| decode_record_key(&e.meta.key).ok().map(|x| x.0))
            .collect();
        let mut g = self.inner.lock();
        let mut out = vec![];
        for log in logs {
            self.wake(&mut g, &log.stream, false);
            if let Some((_, l)) = g.log(&log) {
                out.push(Self::frontier_of(l));
            }
        }
        (next, end, out)
    }

    /// The log's frontier here (contiguous 0 for a log this node has never seen).
    pub fn frontier(&self, log: &LogId) -> Frontier {
        let mut g = self.inner.lock();
        self.wake(&mut g, &log.stream, false);
        g.log(log).map_or(
            Frontier {
                log: log.clone(),
                contiguous: 0,
                high: 0,
            },
            |(_, l)| Self::frontier_of(l),
        )
    }

    /// Repair's position in a peer's log (kept with the checkpoint): its incarnation and the next
    /// LSN to ask for.
    pub fn peer(&self, id: u32) -> Option<(u64, Lsn)> {
        self.inner.lock().peers.get(&id).copied()
    }

    pub fn set_peer(&self, id: u32, incarnation: u64, next: Lsn) {
        self.inner.lock().peers.insert(id, (incarnation, next));
    }

    /// Sets the fault the summaries' database injects (tests: `summary::Db`).
    pub fn set_failpoint(&self, at: Option<&'static str>) {
        self.db.set_failpoint(at);
    }

    /// A boot of this node, `incarnation`, whose log ended at `recovered` when it opened. Kept
    /// with the checkpoint, at most `BOOTS` of them.
    pub fn boot(&self, incarnation: u64, recovered: Lsn) {
        let mut g = self.inner.lock();
        g.boots.push((incarnation, recovered));
        let n = g.boots.len().saturating_sub(BOOTS);
        g.boots.drain(..n);
    }

    /// Where a peer that followed this node's log up to `from` in its boot `theirs` goes on from
    /// (`rejoin`).
    pub fn rejoin(&self, from: Lsn, theirs: u64) -> Lsn {
        rejoin(from, theirs, &self.inner.lock().boots)
    }

    /// Every durable batch of the stream, by LSN (a compacted stream's replay at start).
    pub fn lsns(&self, stream: &str) -> Vec<Lsn> {
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
        lsns
    }

    /// Takes a checkpoint under the lock: every changed stream's summary and the store's other
    /// state. The records it covers must be on stable storage before `end_checkpoint` writes it
    /// (write caching: sync the log in between), or a power loss could leave summaries counting
    /// batches the log lost.
    pub fn begin_checkpoint(&self) -> Pending {
        let mut g = self.inner.lock();
        self.in_flight.fetch_add(1, Relaxed);
        let inner = &mut *g;
        let names: Vec<String> = inner
            .streams
            .iter()
            .filter(|(_, s)| s.dirty)
            .map(|(n, _)| n.clone())
            .collect();
        let streams = names
            .into_iter()
            .map(|n| {
                let v = crate::summary::encode_stream(&inner.summary(&n));
                inner.streams.get_mut(&n).expect("listed").dirty = false;
                (n, v)
            })
            .collect();
        // the LSNs of the batches in RAM, as runs
        let mut ram: Vec<(Lsn, Lsn)> = vec![];
        for &lsn in inner.order.keys() {
            match ram.last_mut() {
                Some(r) if r.1 == lsn => r.1 += 1,
                _ => ram.push((lsn, lsn + 1)),
            }
        }
        let cp = Checkpoint {
            cold_end: inner.cold_end,
            contiguous: inner.contiguous,
            ram,
            floor: inner.floor,
            totals: (inner.batches, inner.events, inner.bytes),
            segs: inner.segs.iter().map(|(&b, &(e, t))| (b, e, t)).collect(),
            // what was forgotten in RAM past `contiguous` is never served again either
            dead: inner.dead.union(&inner.superseded).copied().collect(),
            peers: inner.peers.iter().map(|(&p, &(i, n))| (p, i, n)).collect(),
            boots: inner.boots.clone(),
        };
        Pending { cp, streams }
    }

    /// Writes a checkpoint durably once the log `synced` what it covers. On a failure (of either)
    /// its streams count as changed again.
    pub fn end_checkpoint(&self, p: Pending, synced: anyhow::Result<()>) -> anyhow::Result<()> {
        let _w = self.writing.lock();
        let r = synced.and_then(|()| {
            self.db
                .commit(&p.cp, &p.streams)
                .map_err(anyhow::Error::msg)
        });
        // (under the lock: an eviction after this sees the streams changed again, or written)
        let mut g = self.inner.lock();
        self.in_flight.fetch_sub(1, Relaxed);
        if let Err(e) = r {
            for (n, _) in &p.streams {
                if let Some(s) = g.streams.get_mut(n) {
                    s.dirty = true;
                }
            }
            anyhow::bail!(e);
        }
        drop(g);
        self.count_known()
    }

    /// `begin_checkpoint` and `end_checkpoint` at once, for a log that syncs every write.
    pub fn checkpoint(&self) -> anyhow::Result<()> {
        self.end_checkpoint(self.begin_checkpoint(), Ok(()))
    }

    /// Puts streams out of RAM (see `Inner::evict`), at time `now_ms`. Returns how many.
    pub fn evict(&self, d: Dormancy, now_ms: u64) -> usize {
        let mut g = self.inner.lock();
        let n = match self.in_flight.load(Relaxed) {
            0 => g.evict(d.idle_ms, d.budget),
            _ => 0, // a checkpoint in flight counts its streams as written already
        };
        g.now = now_ms;
        n
    }

    pub fn hot_stats(&self) -> HotStats {
        let g = self.inner.lock();
        HotStats {
            hot: g.streams.len() as u64,
            dormant: self
                .known
                .load(Relaxed)
                .saturating_sub(g.streams.len() as u64),
            loads: g.loads,
            load_ns: g.load_ns,
            evictions: g.evictions,
        }
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
            let mut g = self.inner.lock();
            self.wake(&mut g, stream, true);
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
        /// And put every stream that can leave RAM out of it after each batch and before each
        /// query (the `dormant` module reruns every test so): its state is then the summary.
        static DOZE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// Under `DOZE`: a checkpoint, then every stream with no batch in RAM leaves it.
    fn doze(s: &Store) {
        if DOZE.get() {
            s.checkpoint().unwrap();
            s.evict(
                Dormancy {
                    idle_ms: 0,
                    budget: 0,
                },
                now_ms(),
            );
        }
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
            doze(&self.s);
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

    /// The store as a restart finds it: its checkpoint, and the log past it replayed.
    fn reopen(t: &T) -> Store {
        t.s.checkpoint().unwrap();
        Store::open(t.s.reader.clone(), t.s.db.share()).unwrap()
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

    /// Every query a node serves, as one comparable value (each on a dozing store under `DOZE`).
    fn answers(s: &Store, logs: &[LogId], probes: &[Batch]) -> Vec<String> {
        doze(s);
        let mut v = vec![format!("{:?}", s.totals())];
        doze(s);
        v.push(format!("{:?}", s.frontiers(None)));
        doze(s);
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
                doze(s);
                v.push(format!(
                    "{seq} {} {} {:?} {:?}",
                    s.has(&id, 0),
                    s.get(&id).map_or(0, |b| b.ident),
                    s.durable_below(log, seq, pos / 2),
                    s.durable_below(log, seq, pos),
                ));
                for max in [1, 400, 1 << 20] {
                    doze(s);
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
                doze(s);
                let (got, c) = drain(s, stream, Cursor::new(), max);
                v.push(format!("{stream} {max} {got:?} {c:?}"));
                for start in [3, 30, 60] {
                    let c: Cursor = logs.iter().map(|l| (l.clone(), start)).collect();
                    doze(s);
                    let (got, c) = drain(s, stream, c, max);
                    v.push(format!("{stream} {start} {got:?} {c:?}"));
                }
                doze(s);
                let (got, next) = s.tail(5, Some(stream), max);
                let ids: Vec<_> = got.iter().map(|b| b.id.clone()).collect();
                v.push(format!("{stream} tail {ids:?} {next}"));
            }
            for at in [0, 1, 2, u64::MAX] {
                doze(s);
                v.push(format!("{stream} at {at} {:?}", s.cursor_at(stream, at)));
            }
            doze(s);
            let (ev, cov) = s.read(stream, 0, 1000);
            let ev: Vec<_> = ev.iter().map(|e| (e.event_time_us, e.seq)).collect();
            v.push(format!("{stream} read {ev:?} {cov:?}"));
        }
        for p in probes {
            doze(s);
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
            // the absorbed store as it is, and with its streams out of RAM before every query
            for doze in [false, true] {
                let a = answers(&hot.s, &logs, &probes);
                DOZE.set(doze);
                let b = answers(&cold.s, &logs, &probes);
                DOZE.set(false);
                for (x, y) in a.iter().zip(&b) {
                    assert_eq!(x, y, "{when} (dozing {doze})");
                }
                assert_eq!(a.len(), b.len());
            }
            // and restarted from its checkpoint: nothing a restart could not know differs
            let a = answers(&hot.s, &logs, &probes);
            let b = answers(&reopen(cold), &logs, &probes);
            for (x, y) in a.iter().zip(&b) {
                assert_eq!(x, y, "{when} (restarted from its checkpoint)");
            }
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
        doze(s);
        let mut v = vec![format!("{:?}", s.totals())];
        doze(s);
        v.push(format!("{:?}", s.frontiers(None)));
        for log in logs {
            for seq in 0..90 {
                let id = BatchId {
                    log: log.clone(),
                    first_seq: seq,
                };
                doze(s);
                v.push(format!(
                    "{seq} {} {}",
                    s.has(&id, 0),
                    s.get(&id).map_or(0, |b| b.ident)
                ));
                for max in [1, 400, 1 << 20] {
                    doze(s);
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
                    doze(s);
                    let (got, c) = drain(s, stream, c, max);
                    let mut per: BTreeMap<LogId, Vec<u64>> = BTreeMap::new();
                    for id in got {
                        per.entry(id.log).or_default().push(id.first_seq);
                    }
                    v.push(format!("{stream} {max} {start:?} {per:?} {c:?}"));
                }
            }
            for at in [0, 1_100, 1_500, 1_799, 5_000, u64::MAX] {
                doze(s);
                v.push(format!("{stream} at {at} {:?}", s.cursor_at(stream, at)));
            }
            doze(s);
            let (ev, cov) = s.read(stream, 0, 1000);
            let ev: Vec<_> = ev.iter().map(|e| (e.event_time_us, e.seq)).collect();
            v.push(format!("{stream} read {ev:?} {cov:?}"));
        }
        for p in probes {
            doze(s);
            let k = kind(s.reserve(p));
            s.mark_failed(&p.id);
            v.push(format!("{:?} {}", p.id, k));
        }
        v
    }

    /// The cleaner's steps (as `Node::apply_retention` takes them) on the oldest `n` sealed
    /// segments: copy forward what streams `keeps` retain there, one record at a time (tiny
    /// segments), and with `drop`, drop the segments. Returns how many batches it copied.
    /// Every stream the tests write to.
    const ALL: &[&str] = &["s", "t", "u", "none"];

    /// Each stream's own retention floor (what an older node kept in `<data_dir>/floors`).
    fn floors(s: &Store) -> BTreeMap<String, Age> {
        let g = s.inner.lock();
        g.streams
            .iter()
            .map(|(n, x)| (n.clone(), x.floor))
            .collect()
    }

    fn clean(t: &mut T, n: usize, keeps: &[&str], drop: bool) -> usize {
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
            // as it is, with its streams out of RAM before every query, restarted from its
            // checkpoint
            for how in ["", " dozing", " restarted from its checkpoint"] {
                let x = by_identity(&a.s, &logs, &probes);
                DOZE.set(how == " dozing");
                let y = match how {
                    " restarted from its checkpoint" => by_identity(&reopen(b), &logs, &probes),
                    _ => by_identity(&b.s, &logs, &probes),
                };
                DOZE.set(false);
                for (x, y) in x.iter().zip(&y) {
                    assert_eq!(x, y, "{when}{how}");
                }
                assert_eq!(x.len(), y.len(), "{when}{how}");
            }
        };
        let n = moved.log.segments().0.len() / 2;
        assert!(clean(&mut moved, n, ALL, false) > 0);
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
        clean(&mut moved, n, ALL, true);
        assert!(moved.log.first_lsn() > 0, "the old segments went");
        same(&plain, &moved, "copied again and the old segments dropped");
        same(&plain, &reload(&moved), "after a restart");
        // and again: copies of copies, interleaved with what came after them
        let m = moved.log.segments().0.len() / 2;
        clean(&mut moved, m, ALL, true);
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
            t.s.retained(&[base], ALL, 0)
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
            t.s.retained(&[base], ALL, 0)
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
            t.s.retained(&[base], ALL, 0)
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
    fn a_stream_whose_totals_disagree_with_its_index_trims_what_it_can_and_stops() {
        // Why: retention runs under the store's lock; totals that claim batches the segment's
        // index does not hold (a bug, damage) must end the trim, not spin it forever.
        let mut t = store_with(TINY);
        for seq in 0..6 {
            let x = on("s", "w", seq);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        assert_eq!(t.s.trim_stream("s", Some(0), None, 0), 6, "all forgotten");
        {
            let mut g = t.s.inner.lock();
            let cs = g
                .streams
                .get_mut("s")
                .unwrap()
                .cold
                .values_mut()
                .next()
                .unwrap();
            cs.batches = 1; // claims one its index no longer holds
            g.streams.get_mut("s").unwrap().bytes = 1 << 20;
        }
        assert_eq!(t.s.trim_stream("s", Some(0), None, 0), 0);
    }

    #[test]
    fn restored_floors_forget_again_what_a_stream_forgot_and_nothing_at_the_floor() {
        // Why: a restart reloads every record still on disk. What the stream's retention forgot
        // must be forgotten again (also in RAM: the active segment, which no floor filters), and
        // the batch of another log at the very next LSN with the same time and sequence, the
        // floor itself, must stay.
        let mut t = store();
        let (a, b) = (on("s", "A", 0), on("s", "B", 0));
        for x in [&a, &b] {
            t.s.reserve(x);
            t.durable_at(x, 1_000);
        }
        let one = a.bytes.len() as u64;
        assert_eq!(t.s.trim_stream("s", Some(one), None, 0), 1, "A goes");
        let floors = floors(&t.s);
        let back = Store::load(t.log.reader()).unwrap();
        assert!(back.get(&a.id).is_some(), "a restart brings it back");
        back.restore_floors(&floors);
        assert!(back.get(&a.id).is_none(), "forgotten again");
        assert!(back.get(&b.id).is_some(), "the batch at the floor stays");
        assert_eq!(back.totals().0, 1);
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
        assert!(clean(&mut t, 1, ALL, false) > 0);
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
        assert!(clean(&mut t, 1, ALL, true) > 0);
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
        let keep = t.s.retained(&[0], ALL, 1_010);
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
        let keep = t.s.retained(&[0], ALL, 0);
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
        assert_eq!(clean(&mut t, 1, &["s"], false), 1);
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

    #[test]
    fn idle_streams_leave_ram_and_past_its_budget_the_least_recently_used_go_first() {
        // Why: the idle time keeps a stream in use in RAM, and the budget bounds RAM whatever
        // the idle time: past it the streams used longest ago leave first.
        let mut t = store_with(TINY);
        for name in ["s", "t", "u"] {
            let x = on(name, "w", 0);
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        t.s.checkpoint().unwrap();
        let hot = |t: &T| {
            let mut v: Vec<String> = t.s.inner.lock().streams.keys().cloned().collect();
            v.sort();
            v
        };
        let keep = Dormancy {
            idle_ms: u64::MAX,
            budget: usize::MAX,
        };
        // used at 10, 20 and 30 (a use marks a stream with the last pass's clock)
        for (now, name) in [(10, "s"), (20, "t"), (30, "u")] {
            assert_eq!(t.s.evict(keep, now), 0);
            t.s.cursor_at(name, 0);
        }
        assert_eq!(
            t.s.evict(keep, 30),
            0,
            "within their idle time and the budget"
        );
        // idle for exactly 20 at 30: s goes, t (10) stays
        let idle = Dormancy {
            idle_ms: 20,
            budget: usize::MAX,
        };
        assert_eq!(t.s.evict(idle, 30), 1);
        assert_eq!(hot(&t), ["t", "u"]);
        assert_eq!(t.s.hot_stats().evictions, 1);
        // a budget for one stream: the older of t and u goes
        let one = {
            let g = t.s.inner.lock();
            cost("u", &g.streams["u"], &g.logs)
        };
        let tight = Dormancy {
            idle_ms: u64::MAX,
            budget: one,
        };
        assert_eq!(t.s.evict(tight, 30), 1);
        assert_eq!(hot(&t), ["u"]);
        assert_eq!(t.s.evict(tight, 30), 0, "within the budget now");
        // a stream changed since its checkpoint stays until the next one has it
        for x in [on("u", "w", 1), on("f", "w", 0), on("f", "w", 1)] {
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb(); // u's segment is sealed now, f's batch the active one's
        let none = Dormancy {
            idle_ms: 0,
            budget: 0,
        };
        assert_eq!(
            t.s.evict(none, 40),
            0,
            "changed: not out before it is written"
        );
        t.s.checkpoint().unwrap();
        assert_eq!(t.s.evict(none, 40), 1);
        let h = t.s.hot_stats();
        assert_eq!((h.hot, h.evictions), (1, 3), "f has a batch in RAM");
        // and all of them answer from their summaries
        for name in ["s", "t", "u"] {
            assert!(t.s.get(&on(name, "w", 0).id).is_some(), "{name}");
        }
        assert_eq!(t.s.hot_stats().loads, 3);
    }

    #[test]
    fn a_peer_is_followed_again_from_the_first_open_after_the_boot_it_followed() {
        // Why: a peer under write caching can lose its unsynced tail in a crash and log other
        // records at those LSNs; after two crashes the boot in between did too
        // (spec/mesh/Inventory.tla, `MC_inventory_lastboot`). Following on would skip them.
        let boots = [(3, 0), (5, 400), (7, 100), (9, 300)];
        assert_eq!(rejoin(500, 9, &boots), 500, "this boot: on from 500");
        assert_eq!(rejoin(500, 7, &boots), 300, "back to this boot's open");
        assert_eq!(rejoin(500, 5, &boots), 100, "back to the lowest open since");
        assert_eq!(rejoin(50, 5, &boots), 50, "below it nothing changed");
        assert_eq!(
            rejoin(500, 11, &boots),
            0,
            "a boot it does not know: from the start"
        );
        let t = store();
        for i in 0..70 {
            t.s.boot(i, 10 * i);
        }
        assert_eq!(t.s.rejoin(1000, 69), 1000);
        assert_eq!(t.s.rejoin(1000, 10), 110, "the boots after it");
        assert_eq!(
            t.s.rejoin(1000, 5),
            0,
            "older than the boots kept: from the start"
        );
        let back = reopen(&t);
        assert_eq!(back.rejoin(1000, 10), 110, "kept with the checkpoint");
    }

    #[test]
    fn no_stream_leaves_ram_while_a_checkpoint_of_it_is_in_flight() {
        // Why: a checkpoint is taken under the lock and written without it; a stream it took
        // counts as written already. Out of RAM before the write, it would wake from its older
        // summary; and a write that fails must leave it changed (spec/store/Dormant.tla).
        let mut t = store_with(TINY);
        for x in [on("s", "w", 0), on("f", "w", 0), on("f", "w", 1)] {
            t.s.reserve(&x);
            t.durable(&x);
        }
        t.s.absorb();
        let all = Dormancy {
            idle_ms: 0,
            budget: 0,
        };
        let p = t.s.begin_checkpoint();
        assert_eq!(t.s.evict(all, 1), 0, "in flight");
        assert!(
            t.s.end_checkpoint(p, Err(anyhow::anyhow!("sync failed")))
                .is_err()
        );
        assert_eq!(t.s.evict(all, 1), 0, "not written: changed still");
        let p = t.s.begin_checkpoint();
        t.s.end_checkpoint(p, Ok(())).unwrap();
        assert_eq!(t.s.evict(all, 1), 1, "s is on disk now");
        assert!(t.s.get(&on("s", "w", 0).id).is_some());
    }

    mod dormant {
        macro_rules! dormant {
            ($($t:ident),* $(,)?) => {$(
                #[test]
                fn $t() {
                    super::COLD.set(true);
                    super::DOZE.set(true);
                    super::$t();
                }
            )*};
        }
        dormant!(
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
mod props_rejoin {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Why: a follower of a peer's log must not skip a record the peer logged at an LSN a
        /// crash freed (spec/mesh/Inventory.tla): it goes back no later than where the log ended
        /// at every open since the boot it followed, and never past where it was.
        #[test]
        fn a_follower_goes_back_to_the_lowest_open_since_its_boot(
            from in 0u64..1000,
            boots in prop::collection::vec((0u64..6, 0u64..1000), 0..8),
            theirs in 0u64..7,
        ) {
            let got = rejoin(from, theirs, &boots);
            match boots.iter().rposition(|b| b.0 == theirs) {
                None => prop_assert_eq!(got, 0, "unknown: from the start"),
                Some(i) => {
                    prop_assert!(got <= from);
                    for b in &boots[i + 1..] {
                        prop_assert!(got <= b.1, "past the open of a boot since");
                    }
                    let low = boots[i + 1..].iter().map(|b| b.1).min();
                    prop_assert_eq!(got, low.map_or(from, |l| l.min(from)), "no further back");
                }
            }
        }
    }
}

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
        /// A checkpoint, then every stream that can leave RAM leaves it.
        Doze,
        /// A checkpoint only.
        Checkpoint,
        /// A crash and a restart: back to the last checkpoint, the log past it replayed; what
        /// was only reserved is gone.
        Crash,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                3 => (0u64..8, 1u32..4, 0u8..2).prop_map(|(s, n, v)| Op::Reserve(s, n, v)),
                2 => any::<prop::sample::Index>().prop_map(Op::Durable),
                1 => any::<prop::sample::Index>().prop_map(Op::Fail),
                1 => Just(Op::Doze),
                1 => Just(Op::Checkpoint),
                1 => Just(Op::Crash),
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
        fn the_store_follows_the_identity_rules_and_frontier_arithmetic(small: bool, ops in ops()) {
            let dir = tempfile::tempdir().unwrap();
            let mut c = precog_storage::Config::new(dir.path());
            // tiny segments seal every few batches: absorbed, then out of RAM when dozing
            c.segment_size = if small { 4096 + 200 } else { 1 << 20 };
            let mut log = precog_storage::Log::open(c).unwrap();
            let mut s = Store::load(log.reader()).unwrap();
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
                    Op::Doze => {
                        s.absorb();
                        s.checkpoint().unwrap();
                        s.evict(Dormancy { idle_ms: 0, budget: 0 }, 0);
                    }
                    Op::Checkpoint => s.checkpoint().unwrap(),
                    Op::Crash => {
                        let db = s.db.share();
                        drop(s);
                        s = Store::open(log.reader(), db).unwrap();
                        for b in pending.drain(..) {
                            r.remove(&b.id.first_seq);
                        }
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

/// Model-based test of dormant streams: two stores over two logs written alike, one of them
/// putting its streams out of RAM, failing checkpoints and reads, and crashing back to its last
/// checkpoint; after every step both answer every query alike (docs/ARCHITECTURE.md, "Dormant
/// streams"; spec/store/Dormant.tla).
#[cfg(test)]
mod model_dormant {
    use super::*;
    use crate::proto::{batch_crc, encode_log_key};
    use precog_storage::record::Meta;
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Op {
        /// stream, writer, first_seq, count, content variant
        Reserve(usize, usize, u64, u32, u8),
        Durable(usize),
        /// a reservation written to both logs, not yet marked durable
        Write(usize),
        /// a written one marked durable (in any order: past `contiguous` until the earlier are)
        Mark(usize),
        Fail(usize),
        Absorb,
        /// retention as the node runs it: the second store's checkpoint, then the oldest sealed
        /// segments dropped from both logs, then the stores trimmed to the new first LSN
        Retain(usize),
        /// a floor inside a segment (no drop), and the second store's checkpoint after it; with
        /// `true` its streams leave RAM first (they catch up when they wake: `reconcile`)
        TrimMid(usize, bool),
        /// a stream's own retention to this many batches' bytes, and the second store's
        /// checkpoint after it (as a retention pass)
        TrimStream(usize, u64),
        /// the second store: a checkpoint, then every stream that can leaves RAM
        Doze,
        /// the second store: a checkpoint that fails before or after it is durable
        FailCheckpoint(bool),
        /// the second store: an eviction while a checkpoint is in flight (none leaves), then
        /// the checkpoint written, or failed in its sync
        InFlight(bool),
        /// the second store: reads of its summaries fail meanwhile
        FailRead(usize, usize),
        /// the second store: a crash, back to its checkpoint and the log past it
        Crash,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => (0usize..2, 0usize..2, 0u64..8, 1u32..3, 0u8..2)
                    .prop_map(|(s, w, q, n, v)| Op::Reserve(s, w, q, n, v)),
                5 => any::<usize>().prop_map(Op::Durable),
                1 => any::<usize>().prop_map(Op::Fail),
                2 => Just(Op::Absorb),
                1 => any::<usize>().prop_map(Op::Retain),
                1 => (any::<usize>(), any::<bool>()).prop_map(|(i, d)| Op::TrimMid(i, d)),
                2 => any::<usize>().prop_map(Op::Write),
                2 => any::<usize>().prop_map(Op::Mark),
                1 => (0usize..2, 0u64..6).prop_map(|(s, n)| Op::TrimStream(s, n)),
                3 => Just(Op::Doze),
                1 => any::<bool>().prop_map(Op::FailCheckpoint),
                1 => any::<bool>().prop_map(Op::InFlight),
                1 => (0usize..2, 0usize..2).prop_map(|(s, w)| Op::FailRead(s, w)),
                2 => Just(Op::Crash),
            ],
            1..40,
        )
    }

    fn log(s: usize, w: usize) -> LogId {
        LogId {
            stream: ["s", "t"][s].into(),
            writer: ["a", "b"][w].into(),
            epoch: 1,
        }
    }

    fn batch(l: &LogId, seq: u64, n: u32, v: u8) -> Batch {
        let p = [v; 40];
        let evs: Vec<(u64, &[u8])> = (0..n).map(|i| (seq + i as u64, &p[..])).collect();
        Batch::parse(Batch::encode(l, seq, &evs).unwrap()).unwrap()
    }

    fn kind(r: Reserve) -> &'static str {
        match r {
            Reserve::New => "new",
            Reserve::Duplicate(_) => "dup",
            Reserve::Conflict(_) => "conflict",
            Reserve::Expired => "expired",
            Reserve::Error(_) => "error",
        }
    }

    /// What every query answers.
    fn answers(s: &Store) -> Vec<String> {
        // first, while dormant streams are dormant: the floors of their absorbed batches
        let all: Vec<BatchId> = s
            .tail(0, None, 1 << 20)
            .0
            .into_iter()
            .map(|b| b.id)
            .collect();
        let f = s.frontiers(None);
        let known: BTreeSet<LogId> = f.iter().map(|f| f.log.clone()).collect();
        // (a log only a failed reservation made is in RAM until a crash forgets it)
        let mut live: Vec<_> = s.live_start(None).1.into_iter().collect();
        live.retain(|x| known.contains(&x.0));
        live.sort();
        let mut v = vec![
            format!("{:?} {f:?}", s.totals()),
            format!("{} {live:?}", s.durable_len()),
            format!("{:?}", s.inventory(0, 1 << 10)),
        ];
        for l in (0..2).flat_map(|s| (0..2).map(move |w| log(s, w))) {
            for seq in 0..10 {
                let id = BatchId {
                    log: l.clone(),
                    first_seq: seq,
                };
                let got: Vec<u64> = (s.log_batches(&l, seq, 1 << 20).into_iter())
                    .map(|b| Batch::parse(b).unwrap().id.first_seq)
                    .collect();
                v.push(format!(
                    "{seq} {} {:?} {:?} {got:?}",
                    s.has(&id, 0),
                    s.get(&id).map(|b| b.ident),
                    s.durable_below(&l, seq, s.durable_len()),
                ));
            }
            v.push(format!("{:?}", s.frontier(&l)));
        }
        let only = |mut c: Cursor| {
            c.retain(|l, _| known.contains(l));
            c
        };
        for stream in ["s", "t"] {
            let (bs, c) = s.read_cursor(stream, &Cursor::new(), 1 << 20);
            let c = only(c);
            let ids: Vec<BatchId> = bs
                .into_iter()
                .map(|b| Batch::parse(b).unwrap().id)
                .collect();
            let tail: Vec<BatchId> = (s.tail(0, Some(stream), 1 << 20).0.into_iter())
                .map(|b| b.id)
                .collect();
            v.push(format!("{stream} {ids:?} {c:?} {tail:?}"));
            v.push(format!(
                "{stream} {:?} {:?} {}",
                only(s.cursor_at(stream, 0)),
                only(s.cursor_at(stream, u64::MAX)),
                s.stream_bytes(stream)
            ));
            let mut live: Vec<_> = s.live_start(Some(stream)).1.into_iter().collect();
            live.retain(|x| known.contains(&x.0));
            live.sort();
            v.push(format!("{stream} {live:?} {:?}", s.frontiers(Some(stream))));
            let (ev, mut cov) = s.read(stream, 0, 1000);
            cov.retain(|f| known.contains(&f.log));
            v.push(format!(
                "{stream} {} {cov:?} {:?}",
                ev.len(),
                s.lsns(stream)
            ));
        }
        v.push(format!("{all:?}"));
        v
    }

    fn open(dir: &tempfile::TempDir, name: &str) -> (precog_storage::Log, Store) {
        let mut c = precog_storage::Config::new(dir.path().join(name));
        c.segment_size = 4096 + 700; // sealed every few batches
        c.spares = 0;
        let log = precog_storage::Log::open(c).unwrap();
        let db = Db::open(&dir.path().join(format!("{name}.redb")), 1 << 20).unwrap();
        let s = Store::open(log.reader(), db).unwrap();
        (log, s)
    }

    /// Why: a stream out of RAM must come back exactly as it was, whatever happened while it
    /// was out, and a crash at any point must come back to what the log and the last checkpoint
    /// say: an answer that differs is a duplicate accepted, a batch lost, a reader skipping or
    /// stalling.
    fn run(ops: Vec<Op>) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let ((mut la, a), (mut lb, mut b)) = (open(&dir, "a"), open(&dir, "b"));
        let mut pending: Vec<Batch> = vec![];
        let mut written: Vec<(Batch, Lsn)> = vec![];
        for op in ops {
            match op {
                Op::Reserve(s, w, seq, n, v) => {
                    let x = batch(&log(s, w), seq, n, v);
                    let (ka, kb) = (kind(a.reserve(&x)), kind(b.reserve(&x)));
                    prop_assert_eq!(ka, kb, "{:?}", x.id);
                    if ka == "new" {
                        pending.push(x);
                    }
                }
                Op::Durable(i) | Op::Write(i) if !pending.is_empty() => {
                    let x = pending.remove(i % pending.len());
                    // append times as the WAL gives them, but never equal (a floor of a
                    // stream's own retention is not compared across logs at one time)
                    let m = Meta {
                        key: encode_log_key(&x.id.log),
                        first_seq: x.id.first_seq,
                        count: x.count,
                        tag: batch_crc(&x.bytes),
                        time_ms: la.next_lsn(),
                    };
                    let lsn = la.append(&[(m.clone(), &x.bytes[..])]).unwrap();
                    prop_assert_eq!(lb.append(&[(m, &x.bytes[..])]).unwrap(), lsn);
                    if matches!(op, Op::Write(_)) {
                        written.push((x, lsn));
                    } else {
                        a.mark_durable(&x, lsn);
                        b.mark_durable(&x, lsn);
                    }
                }
                Op::Mark(i) if !written.is_empty() => {
                    let (x, lsn) = written.remove(i % written.len());
                    a.mark_durable(&x, lsn);
                    b.mark_durable(&x, lsn);
                }
                Op::Fail(i) if !pending.is_empty() => {
                    let x = pending.remove(i % pending.len());
                    a.mark_failed(&x.id);
                    b.mark_failed(&x.id);
                }
                Op::Absorb => {
                    a.absorb();
                    b.absorb();
                }
                Op::Retain(i) => {
                    // only absorbed segments, as the node: their batches are all durable
                    let sealed: Vec<Lsn> = (la.segments().0.into_iter())
                        .map(|x| x.end)
                        .filter(|&e| e <= a.absorbed_end().min(b.absorbed_end()))
                        .collect();
                    if !sealed.is_empty() {
                        let end = sealed[i % sealed.len()];
                        b.checkpoint().unwrap();
                        let floor = la.drop_until(end).unwrap();
                        prop_assert_eq!(lb.drop_until(end).unwrap(), floor);
                        a.trim(floor);
                        b.trim(floor);
                    }
                }
                Op::TrimMid(i, doze) => {
                    if doze {
                        b.checkpoint().unwrap();
                        b.evict(
                            Dormancy {
                                idle_ms: 0,
                                budget: 0,
                            },
                            0,
                        );
                    }
                    // (the node's floor never passes what is durable: it drops absorbed segments)
                    let floor = (i % (a.durable_len() as usize + 1)) as Lsn;
                    a.trim(floor);
                    b.trim(floor);
                    b.checkpoint().unwrap();
                }
                Op::TrimStream(s, n) => {
                    let max = Some(n * 300);
                    let stream = ["s", "t"][s];
                    prop_assert_eq!(
                        a.trim_stream(stream, max, None, 0),
                        b.trim_stream(stream, max, None, 0)
                    );
                    // the pass ends with a checkpoint (before it, a crash brings them back
                    // and the next pass forgets them again)
                    b.checkpoint().unwrap();
                }
                Op::Doze => {
                    b.checkpoint().unwrap();
                    b.evict(
                        Dormancy {
                            idle_ms: 0,
                            budget: 0,
                        },
                        0,
                    );
                }
                Op::InFlight(ok) => {
                    let p = b.begin_checkpoint();
                    prop_assert_eq!(
                        b.evict(
                            Dormancy {
                                idle_ms: 0,
                                budget: 0
                            },
                            0
                        ),
                        0
                    );
                    let synced = if ok {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("sync"))
                    };
                    prop_assert_eq!(b.end_checkpoint(p, synced).is_ok(), ok);
                }
                Op::FailCheckpoint(after) => {
                    let at = if after {
                        "summary:after_commit"
                    } else {
                        "summary:before_commit"
                    };
                    b.db.set_failpoint(Some(at));
                    prop_assert!(b.checkpoint().is_err());
                    b.db.set_failpoint(None);
                }
                Op::FailRead(s, w) => {
                    // a reservation past everything: refused (a dormant stream does not
                    // read) or new, and failed again on both
                    let x = batch(&log(s, w), 100, 1, 0);
                    b.db.set_failpoint(Some("summary:read"));
                    let _ = (b.tail(0, None, 1 << 20), b.get(&x.id), b.cursor_at("s", 1));
                    let kb = kind(b.reserve(&x));
                    let _ = (b.frontiers(None), b.live_start(None));
                    b.db.set_failpoint(None);
                    prop_assert!(kb == "error" || kb == kind(a.reserve(&x)), "{}", kb);
                    a.mark_failed(&x.id);
                    b.mark_failed(&x.id);
                }
                Op::Crash => {
                    let db = b.db.share();
                    drop(b);
                    b = Store::open(lb.reader(), db).unwrap();
                    // what was only reserved is gone; what was written, recovered
                    for x in pending.drain(..) {
                        a.mark_failed(&x.id);
                    }
                    for (x, lsn) in written.drain(..) {
                        a.mark_durable(&x, lsn);
                    }
                    a.absorb(); // as the restart does
                    b.absorb();
                }
                _ => {}
            }
            let (x, y) = (answers(&a), answers(&b));
            for (x, y) in x.iter().zip(&y) {
                prop_assert_eq!(x, y);
            }
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        #[test]
        fn a_store_whose_streams_leave_ram_answers_as_one_that_keeps_them(ops in ops()) {
            run(ops)?;
        }
    }

    /// What the model found, and the paths it reaches only now and then.
    #[test]
    fn the_cases_the_model_found_and_its_rare_paths() {
        use Op::*;
        let cases = [
            // a dormant log's frontier after the segment of its batches went (5 to a segment)
            vec![
                Reserve(0, 0, 1, 1, 0),
                Durable(0),
                Reserve(1, 0, 0, 1, 0),
                Durable(0),
                Reserve(1, 1, 0, 1, 0),
                Durable(0),
                Reserve(1, 1, 1, 1, 0),
                Durable(0),
                Reserve(1, 1, 2, 1, 0),
                Durable(0),
                Reserve(1, 1, 3, 1, 0),
                Durable(0),
                Absorb,
                Retain(0),
                Crash,
            ],
            // a restart: the segment that went trims a log up to a batch put back in RAM
            vec![
                Reserve(1, 0, 5, 1, 0),
                Durable(0),
                Reserve(0, 0, 0, 1, 0),
                Durable(0),
                Reserve(0, 1, 0, 1, 0),
                Durable(0),
                Reserve(0, 1, 1, 1, 0),
                Durable(0),
                Reserve(0, 1, 2, 1, 0),
                Durable(0),
                Reserve(1, 0, 6, 1, 0),
                Absorb,
                Durable(0),
                Retain(0),
                Crash,
            ],
            // a floor inside a segment while its streams are out of RAM
            vec![
                Reserve(0, 0, 0, 1, 0),
                Durable(0),
                Reserve(0, 0, 1, 1, 0),
                Durable(0),
                Reserve(0, 0, 2, 1, 0),
                Durable(0),
                Reserve(1, 0, 0, 1, 0),
                Durable(0),
                Reserve(1, 0, 1, 1, 0),
                Durable(0),
                Reserve(1, 0, 2, 1, 0),
                Durable(0),
                Absorb,
                TrimMid(2, true),
            ],
            // marked past `contiguous`, forgotten, then a crash: the replay leaves it forgotten,
            // and a feed starting meanwhile joins the log before it
            vec![
                Reserve(0, 0, 0, 1, 0),
                Reserve(0, 0, 1, 1, 0),
                Write(0),
                Write(0),
                Mark(1),
                TrimStream(0, 0),
                Crash,
            ],
            vec![
                Reserve(0, 0, 0, 1, 0),
                Reserve(0, 0, 1, 1, 0),
                Reserve(0, 0, 2, 1, 0),
                Durable(0),
                Write(0),
                Durable(0),
                Mark(0),
                Reserve(1, 0, 0, 2, 0),
                Write(340),
                TrimMid(3918277648458073569, true),
                Absorb,
                Doze,
            ],
            // reads that fail while streams are out of RAM
            vec![
                Reserve(0, 0, 0, 1, 0),
                Durable(0),
                Reserve(1, 0, 0, 1, 0),
                Durable(0),
                Reserve(1, 1, 0, 1, 0),
                Durable(0),
                Reserve(1, 1, 1, 1, 0),
                Durable(0),
                Reserve(1, 1, 2, 1, 0),
                Durable(0),
                Reserve(1, 1, 3, 1, 0),
                Durable(0),
                Absorb,
                Doze,
                FailRead(0, 0),
            ],
        ];
        for ops in cases {
            run(ops.clone()).unwrap_or_else(|e| panic!("{ops:?}: {e}"));
        }
    }
}
