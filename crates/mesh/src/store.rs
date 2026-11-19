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
                inner.order.insert(e.lsn, (id, shape.count, shape.crc, len));
                inner.events += shape.count as u64;
                inner.bytes += len as u64;
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
        inner
            .order
            .insert(lsn, (id.clone(), sh.count, sh.crc, sh.len));
        inner.events += sh.count as u64;
        inner.bytes += sh.len as u64;
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
        let keep = inner.order.split_off(&floor);
        let gone = std::mem::replace(&mut inner.order, keep);
        for (_, (id, count, _, len)) in gone {
            inner.events -= count as u64;
            inner.bytes -= len as u64;
            if let Some(map) = inner.logs.get_mut(&id.log)
                && let Some(e) = map.remove(&id.first_seq)
            {
                let t = inner.trimmed.entry(id.log.clone()).or_default();
                *t = (*t).max(e.shape.end);
                if map.is_empty() {
                    inner.logs.remove(&id.log);
                }
            }
        }
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
    pub fn tail(&self, from: u64, stream: Option<&str>, max_bytes: usize) -> (Vec<Batch>, u64) {
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
            .filter_map(|(lsn, _)| self.read_batch(lsn))
            .collect();
        (out, next)
    }

    /// Where a live feed starting now joins: the LSN its tail starts at and, per log of the
    /// stream, the sequence it is next due (the end of the log's contiguous run below that
    /// LSN). Read under one lock, so no batch falls between the two.
    pub fn live_start(&self, stream: Option<&str>) -> (Lsn, HashMap<LogId, u64>) {
        let g = self.inner.lock();
        let pos = g.contiguous;
        let mut keys: BTreeSet<&LogId> = g.logs.keys().collect();
        keys.extend(g.trimmed.keys());
        let next = keys
            .into_iter()
            .filter(|l| stream.is_none_or(|s| l.stream == s))
            .map(|l| {
                let mut c = g.trimmed.get(l).copied().unwrap_or(0);
                for (&s, e) in g.logs.get(l).into_iter().flatten() {
                    // durable out of LSN order at or past `pos`: the tail still returns it
                    if s == c && e.lsn.is_some_and(|x| (..pos).contains(&x)) {
                        c = e.shape.end;
                    }
                }
                (l.clone(), c)
            })
            .collect();
        (pos, next)
    }

    /// The end of the log's durable batch starting at `seq`, if its LSN is below `pos` (a tail
    /// from `pos` does not return it).
    pub fn durable_below(&self, log: &LogId, seq: u64, pos: Lsn) -> Option<u64> {
        let g = self.inner.lock();
        let e = g.logs.get(log)?.get(&seq)?;
        // an LSN is set once durable
        e.lsn.filter(|x| (..pos).contains(x)).map(|_| e.shape.end)
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
    fn a_live_feed_joins_each_log_after_its_run_below_the_start() {
        // the feed's start LSN and each log's next sequence are one snapshot: a batch durable
        // out of LSN order past the start is still due (the tail returns it), not counted
        let mut t = store();
        let xs: Vec<Batch> = (0..4).map(|i| b("w", 1, i, &[(i, b"x")])).collect();
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
        for i in [0, 1, 3] {
            t.s.mark_durable(&xs[i].id, lsns[i]); // lsn 2 is not durable yet
        }
        let (pos, next) = t.s.live_start(Some("s"));
        assert_eq!(pos, 2);
        assert_eq!(next, HashMap::from([(xs[0].id.log.clone(), 2)]));
        assert!(t.s.live_start(Some("other")).1.is_empty());
        let log = &xs[0].id.log;
        assert_eq!(t.s.durable_below(log, 1, pos), Some(2), "below the start");
        assert_eq!(t.s.durable_below(log, 3, pos), None, "the tail returns it");
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
}
