//! The node's index over its segment log.
//!
//! Every writer log `(stream, writer, epoch)` is an independent sequence space. Batches occupy
//! non-overlapping sequence ranges; an exact retry is idempotent, any other overlap is a
//! conflict. A batch becomes visible (reads, inventory, subscriptions) only once the segment
//! log has made it durable. Only metadata lives in RAM (range, checksum, length, LSN); payloads
//! are read back through the log's index and tail cache.
//!
//! ponytail: ~150 bytes of RAM per retained batch; move the per-log index to disk (redb or the
//! segment index files) if retention keeps more than ~10M batches per node.

use crate::proto::{Batch, BatchId, Event, Frontier, LogId, decode_log_key};
use parking_lot::Mutex;
use precog_storage::{Lsn, Reader};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::sync::watch;

/// (lsn, batch id, event count, batch checksum)
pub type InventoryItem = (u64, BatchId, u32, u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Durable,
    Failed,
}

/// What identifies a batch's content for duplicate detection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Shape {
    end: u64,
    count: u32,
    crc: u32,
    len: u32,
}

impl Shape {
    fn of(b: &Batch) -> Shape {
        Shape {
            end: b.end_seq(),
            count: b.count,
            crc: b.ident,
            len: b.bytes.len() as u32,
        }
    }
}

struct Entry {
    shape: Shape,
    lsn: Option<Lsn>,
    state: watch::Sender<State>,
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

#[derive(Default)]
struct Inner {
    logs: HashMap<LogId, BTreeMap<u64, Entry>>,
    streams: HashMap<String, BTreeSet<LogId>>,
    /// Durable batches by LSN: (id, count, crc, len).
    order: BTreeMap<Lsn, (BatchId, u32, u32, u32)>,
    /// Per writer log: sequences below this were trimmed by retention.
    trimmed: HashMap<LogId, u64>,
    /// Every LSN below this is in `order` (or trimmed). Subscriptions read only below it:
    /// batches are marked durable in any order, but a reader must never skip one.
    contiguous: Lsn,
    events: u64,
    bytes: u64,
    /// Per stream, its durable batches by LSN, for its own retention.
    by_stream: HashMap<String, StreamLog>,
}

#[derive(Default)]
struct StreamLog {
    /// (first seq's log, append time ms, bytes) by LSN
    order: BTreeMap<Lsn, (LogId, u64, u32)>,
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
    fn index(&mut self, lsn: Lsn, id: &BatchId, count: u32, crc: u32, len: u32, time_ms: u64) {
        self.order.insert(lsn, (id.clone(), count, crc, len));
        self.events += count as u64;
        self.bytes += len as u64;
        let s = self.by_stream.entry(id.log.stream.clone()).or_default();
        s.order.insert(lsn, (id.log.clone(), time_ms, len));
        s.bytes += len as u64;
    }

    /// Forgets the durable batch at `lsn` (retention): later copies of it are `Expired`.
    fn forget(&mut self, lsn: Lsn) {
        let Some((id, count, _, len)) = self.order.remove(&lsn) else {
            return;
        };
        self.events -= count as u64;
        self.bytes -= len as u64;
        if let Some(s) = self.by_stream.get_mut(&id.log.stream)
            && s.order.remove(&lsn).is_some()
        {
            s.bytes -= len as u64;
        }
        if let Some(map) = self.logs.get_mut(&id.log)
            && let Some(e) = map.remove(&id.first_seq)
        {
            let t = self.trimmed.entry(id.log.clone()).or_default();
            *t = (*t).max(e.shape.end);
            if map.is_empty() {
                self.logs.remove(&id.log);
            }
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
                let id = BatchId {
                    log: log.clone(),
                    first_seq: e.meta.first_seq,
                };
                let shape = Shape {
                    end: e.meta.first_seq + e.meta.count as u64,
                    count: e.meta.count,
                    crc: e.meta.tag,
                    len: e.payload_len,
                };
                let map = inner.logs.entry(log.clone()).or_default();
                if let Some((&s0, x)) = map.range(..=id.first_seq).next_back()
                    && x.shape.end > id.first_seq
                {
                    if s0 == id.first_seq
                        && x.shape.crc == shape.crc
                        && x.shape.count == shape.count
                    {
                        continue; // logged twice (a repair raced a replicate): keep the first
                    }
                    anyhow::bail!(
                        "conflicting batches in the log at lsn {}: {:?} seq {}",
                        e.lsn,
                        log,
                        id.first_seq
                    );
                }
                let len = shape.len;
                let (tx, _) = watch::channel(State::Durable);
                map.insert(
                    id.first_seq,
                    Entry {
                        shape,
                        lsn: Some(e.lsn),
                        state: tx,
                    },
                );
                inner
                    .streams
                    .entry(log.stream.clone())
                    .or_default()
                    .insert(log);
                inner.index(e.lsn, &id, shape.count, shape.crc, len, e.meta.time_ms);
            }
        }
        Ok(s)
    }

    pub fn reserve(&self, b: &Batch) -> Reserve {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let first = b.id.first_seq;
        let end = b.end_seq();
        if inner.trimmed.get(&b.id.log).is_some_and(|&t| first < t) {
            return Reserve::Expired;
        }
        let log = inner.logs.entry(b.id.log.clone()).or_default();
        if let Some((&s, e)) = log.range(..=first).next_back()
            && e.shape.end > first
        {
            let new = Shape::of(b);
            if s == first
                && e.shape.crc == new.crc
                && e.shape.count == new.count
                && e.shape.end == new.end
            {
                return Reserve::Duplicate(e.state.subscribe());
            }
            return Reserve::Conflict(format!(
                "{:?} seq {first}..{end} overlaps stored {s}..{}",
                b.id.log, e.shape.end
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
        let (tx, _) = watch::channel(State::Pending);
        log.insert(
            first,
            Entry {
                shape: Shape::of(b),
                lsn: None,
                state: tx,
            },
        );
        inner
            .streams
            .entry(b.id.log.stream.clone())
            .or_default()
            .insert(b.id.log.clone());
        Reserve::New
    }

    pub fn mark_durable(&self, id: &BatchId, lsn: Lsn) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let Some(e) = inner
            .logs
            .get_mut(&id.log)
            .and_then(|l| l.get_mut(&id.first_seq))
        else {
            return;
        };
        if *e.state.borrow() == State::Durable {
            return;
        }
        e.lsn = Some(lsn);
        e.state.send_replace(State::Durable);
        let sh = e.shape;
        inner.index(lsn, id, sh.count, sh.crc, sh.len, now_ms());
        while inner.order.contains_key(&inner.contiguous) {
            inner.contiguous += 1;
        }
        drop(g);
        self.durable_notify.notify_waiters();
    }

    pub fn mark_failed(&self, id: &BatchId) {
        let mut g = self.inner.lock();
        if let Some(log) = g.logs.get_mut(&id.log)
            && let Some(e) = log.remove(&id.first_seq)
        {
            e.state.send_replace(State::Failed);
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
        while let Some(s) = inner.by_stream.get(stream)
            && let Some((&lsn, &(_, time, _))) = s.order.first_key_value()
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
        if g.trimmed.get(&id.log).is_some_and(|&t| id.first_seq < t) {
            return true;
        }
        g.logs
            .get(&id.log)
            .and_then(|l| l.get(&id.first_seq))
            .is_some_and(|e| *e.state.borrow() == State::Durable && e.shape.crc == crc)
    }

    fn lsn_of(&self, id: &BatchId) -> Option<Lsn> {
        let g = self.inner.lock();
        g.logs
            .get(&id.log)?
            .get(&id.first_seq)
            .filter(|e| *e.state.borrow() == State::Durable)?
            .lsn
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

    /// Durable batches with LSN >= `after`: (lsn, id, count, crc), the next cursor, and
    /// whether more remain.
    pub fn inventory(&self, after: u64, limit: u32) -> (Vec<InventoryItem>, u64, bool) {
        let g = self.inner.lock();
        let items: Vec<_> = g
            .order
            .range(after..)
            .take(limit as usize)
            .map(|(&l, (id, n, crc, _))| (l, id.clone(), *n, *crc))
            .collect();
        let next = items.last().map_or(after, |x| x.0 + 1);
        let more = g.order.range(next..).next().is_some();
        (items, next, more)
    }

    /// Durable batches at LSNs `from..`, optionally only one stream, up to about `max_bytes`.
    /// Returns the batches and the next LSN to ask for.
    pub fn tail(
        &self,
        from: u64,
        stream: Option<&str>,
        max_bytes: usize,
    ) -> (Vec<bytes::Bytes>, u64) {
        let picks: Vec<(Lsn, bool)> = {
            let g = self.inner.lock();
            let mut size = 0usize;
            let mut v = vec![];
            for (&lsn, (id, _, _, len)) in g.order.range(from..g.contiguous.max(from)) {
                if size >= max_bytes {
                    break;
                }
                let want = stream.is_none_or(|s| id.log.stream == s);
                if want {
                    size += *len as usize;
                }
                v.push((lsn, want));
            }
            v
        };
        let next = picks.last().map_or(from, |x| x.0 + 1);
        let out = picks
            .into_iter()
            .filter(|x| x.1)
            .filter_map(|(lsn, _)| self.read_batch(lsn).map(|b| b.bytes))
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
            let (Some(sl), Some(logs)) = (g.by_stream.get(stream), g.streams.get(stream)) else {
                return (vec![], next);
            };
            let from = |log: &LogId| {
                cursor
                    .get(log)
                    .copied()
                    .unwrap_or_else(|| g.trimmed.get(log).copied().unwrap_or(0))
            };
            // start at the earliest LSN of an unread batch of any of the stream's logs
            let start = logs
                .iter()
                .filter_map(|log| {
                    let (map, c) = (g.logs.get(log)?, from(log));
                    let e = map
                        .range(..=c)
                        .next_back()
                        .filter(|(_, e)| e.shape.end > c)
                        .or_else(|| map.range(c..).next());
                    e?.1.lsn
                })
                .min();
            let Some(start) = start else {
                return (vec![], next);
            };
            let (mut size, mut out, mut blocked) = (0usize, vec![], BTreeSet::new());
            for (&lsn, (log, _, len)) in sl.order.range(start..g.contiguous.max(start)) {
                if size >= max_bytes {
                    break;
                }
                let id = &g.order[&lsn].0;
                let c = next.get(log).copied().unwrap_or_else(|| from(log));
                let end = g.logs[log][&id.first_seq].shape.end;
                if blocked.contains(log) || end <= c {
                    continue;
                }
                if id.first_seq > c {
                    blocked.insert(log.clone()); // its predecessor is not here yet
                    continue;
                }
                next.insert(log.clone(), end);
                size += *len as usize;
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
        if let Some(sl) = g.by_stream.get(stream) {
            for (lsn, (log, t, _)) in &sl.order {
                if *t >= time_ms && !c.contains_key(log) {
                    c.insert(log.clone(), g.order[lsn].0.first_seq);
                }
            }
        }
        for log in g.streams.get(stream).into_iter().flatten() {
            if !c.contains_key(log) {
                let end = g
                    .logs
                    .get(log)
                    .and_then(|m| m.values().filter_map(|e| e.lsn.map(|_| e.shape.end)).max());
                c.insert(
                    log.clone(),
                    end.unwrap_or(0)
                        .max(g.trimmed.get(log).copied().unwrap_or(0)),
                );
            }
        }
        c
    }

    /// The LSN a subscription "from now" starts at.
    pub fn durable_len(&self) -> u64 {
        self.inner.lock().contiguous
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        let g = self.inner.lock();
        (g.order.len() as u64, g.events, g.bytes)
    }

    fn frontier_of(log: &LogId, entries: Option<&BTreeMap<u64, Entry>>, trimmed: u64) -> Frontier {
        let mut contiguous = trimmed;
        let mut high = trimmed;
        for (&s, e) in entries.into_iter().flatten() {
            if *e.state.borrow() != State::Durable {
                continue;
            }
            if s == contiguous {
                contiguous = e.shape.end;
            }
            high = high.max(e.shape.end);
        }
        Frontier {
            log: log.clone(),
            contiguous,
            high,
        }
    }

    pub fn frontiers(&self, stream: Option<&str>) -> Vec<Frontier> {
        let g = self.inner.lock();
        let mut keys: BTreeSet<&LogId> = g.logs.keys().collect();
        keys.extend(g.trimmed.keys());
        let mut v: Vec<_> = keys
            .into_iter()
            .filter(|l| stream.is_none_or(|s| l.stream == s))
            .map(|l| Self::frontier_of(l, g.logs.get(l), g.trimmed.get(l).copied().unwrap_or(0)))
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
            if let Some(logs) = g.streams.get(stream) {
                for l in logs {
                    cov.push(Self::frontier_of(
                        l,
                        g.logs.get(l),
                        g.trimmed.get(l).copied().unwrap_or(0),
                    ));
                    for e in g.logs.get(l).into_iter().flat_map(|m| m.values()) {
                        if let (State::Durable, Some(lsn)) = (*e.state.borrow(), e.lsn) {
                            lsns.push(lsn);
                        }
                    }
                }
            }
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
        let (items, next, more) = t.s.inventory(0, 10);
        assert_eq!((items[0].0, next, more), (3, 6, false));
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
        let s2 = Store::load(r.clone(), r.entries(0, 100)).unwrap();
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
            Store::load(r.clone(), r.entries(0, 1000))
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
        let s = Store::load(r.clone(), r.entries(0, 10)).unwrap();
        assert_eq!(
            s.trim_stream("a", None, Some(500), 1_500),
            0,
            "exactly 500 ms old"
        );
        assert_eq!(s.trim_stream("a", None, Some(500), 1_501), 1, "older");
    }
}
