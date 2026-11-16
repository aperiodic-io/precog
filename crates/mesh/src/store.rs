//! In-memory index over the WAL contents of one node.
//!
//! Every writer log `(stream, writer, epoch)` is an independent sequence
//! space. Batches occupy non-overlapping sequence ranges; an exact retry of a
//! batch is idempotent and any other overlap is a conflict. Batches become
//! visible to reads and inventory only after the WAL reports them durable.
//!
//! POC simplification: all data stays in RAM (and in the WAL). The production
//! design would index sealed segments on disk.

use crate::proto::{Batch, BatchId, Event, Frontier, LogId, batch_crc};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Durable,
    Failed,
}

struct Entry {
    batch: Batch,
    state: watch::Sender<State>,
}

pub enum Reserve {
    /// New range reserved; caller must write it to the WAL then call
    /// `mark_durable` or `mark_failed`.
    New,
    /// Identical batch already present/pending; wait for its state.
    Duplicate(watch::Receiver<State>),
    /// Range overlaps different content.
    Conflict(String),
}

#[derive(Default)]
struct Inner {
    logs: HashMap<LogId, BTreeMap<u64, Entry>>,
    streams: HashMap<String, BTreeSet<LogId>>,
    /// Local durable order; inventory index i+1 refers to order[i].
    order: Vec<(BatchId, u32, u32)>,
    events: u64,
    bytes: u64,
}

#[derive(Default)]
pub struct Store {
    inner: Mutex<Inner>,
}

impl Store {
    /// Loads batches recovered from the WAL (already durable).
    pub fn load(recovered: Vec<Batch>) -> anyhow::Result<Store> {
        let s = Store::default();
        for b in recovered {
            match s.reserve(&b) {
                Reserve::New => s.mark_durable(&b.id),
                Reserve::Duplicate(_) => {} // same batch logged twice (e.g. repair race)
                Reserve::Conflict(e) => anyhow::bail!("conflicting batches in WAL: {e}"),
            }
        }
        Ok(s)
    }

    pub fn reserve(&self, b: &Batch) -> Reserve {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let log = inner.logs.entry(b.id.log.clone()).or_default();
        let first = b.id.first_seq;
        let end = b.end_seq();
        if let Some((&s, e)) = log.range(..=first).next_back()
            && e.batch.end_seq() > first
        {
            if s == first && e.batch.bytes == b.bytes {
                return Reserve::Duplicate(e.state.subscribe());
            }
            return Reserve::Conflict(format!(
                "{:?} seq {first}..{end} overlaps stored {s}..{}",
                b.id.log,
                e.batch.end_seq()
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
                batch: b.clone(),
                state: tx,
            },
        );
        if !inner.streams.contains_key(&b.id.log.stream) {
            inner
                .streams
                .insert(b.id.log.stream.clone(), BTreeSet::new());
        }
        inner
            .streams
            .get_mut(&b.id.log.stream)
            .unwrap()
            .insert(b.id.log.clone());
        Reserve::New
    }

    pub fn mark_durable(&self, id: &BatchId) {
        let mut g = self.inner.lock();
        let inner = &mut *g;
        let Some(e) = inner.logs.get(&id.log).and_then(|l| l.get(&id.first_seq)) else {
            return;
        };
        if *e.state.borrow() == State::Durable {
            return;
        }
        let (count, len, crc) = (
            e.batch.count,
            e.batch.bytes.len(),
            batch_crc(&e.batch.bytes),
        );
        e.state.send_replace(State::Durable);
        inner.order.push((id.clone(), count, crc));
        inner.events += count as u64;
        inner.bytes += len as u64;
    }

    pub fn mark_failed(&self, id: &BatchId) {
        let mut g = self.inner.lock();
        if let Some(log) = g.logs.get_mut(&id.log)
            && let Some(e) = log.remove(&id.first_seq)
        {
            e.state.send_replace(State::Failed);
        }
    }

    /// Durable copy with this exact crc present?
    pub fn has(&self, id: &BatchId, crc: u32) -> bool {
        let g = self.inner.lock();
        g.logs
            .get(&id.log)
            .and_then(|l| l.get(&id.first_seq))
            .is_some_and(|e| {
                *e.state.borrow() == State::Durable && batch_crc(&e.batch.bytes) == crc
            })
    }

    pub fn get(&self, id: &BatchId) -> Option<Batch> {
        let g = self.inner.lock();
        g.logs
            .get(&id.log)
            .and_then(|l| l.get(&id.first_seq))
            .filter(|e| *e.state.borrow() == State::Durable)
            .map(|e| e.batch.clone())
    }

    pub fn inventory(&self, after: u64, limit: u32) -> (Vec<(u64, BatchId, u32, u32)>, u64, bool) {
        let g = self.inner.lock();
        let start = (after as usize).min(g.order.len());
        let end = (start + limit as usize).min(g.order.len());
        let items = g.order[start..end]
            .iter()
            .enumerate()
            .map(|(i, (id, n, crc))| ((start + i + 1) as u64, id.clone(), *n, *crc))
            .collect();
        (items, end as u64, end < g.order.len())
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        let g = self.inner.lock();
        (g.order.len() as u64, g.events, g.bytes)
    }

    fn frontier_of(log: &LogId, entries: &BTreeMap<u64, Entry>) -> Frontier {
        let mut contiguous = 0;
        let mut high = 0;
        for (&s, e) in entries {
            if *e.state.borrow() != State::Durable {
                continue;
            }
            if s == contiguous {
                contiguous = e.batch.end_seq();
            }
            high = high.max(e.batch.end_seq());
        }
        Frontier {
            log: log.clone(),
            contiguous,
            high,
        }
    }

    pub fn frontiers(&self, stream: Option<&str>) -> Vec<Frontier> {
        let g = self.inner.lock();
        let mut v: Vec<_> = g
            .logs
            .iter()
            .filter(|(l, _)| stream.is_none_or(|s| l.stream == s))
            .map(|(l, e)| Self::frontier_of(l, e))
            .filter(|f| f.high > 0)
            .collect();
        v.sort_by(|a, b| a.log.cmp(&b.log));
        v
    }

    /// Event-time ordered read of locally durable events of one stream with
    /// `event_time_us >= from`. Ties break on (writer, epoch, seq) so every
    /// node with the same content returns the same order.
    ///
    /// POC: scans the whole stream. A production version keeps an event-time
    /// index per sealed segment.
    pub fn read(&self, stream: &str, from: u64, limit: usize) -> (Vec<Event>, Vec<Frontier>) {
        let g = self.inner.lock();
        let mut out = Vec::new();
        let mut cov = Vec::new();
        if let Some(logs) = g.streams.get(stream) {
            for l in logs {
                let entries = &g.logs[l];
                cov.push(Self::frontier_of(l, entries));
                for e in entries.values() {
                    if *e.state.borrow() == State::Durable {
                        out.extend(e.batch.events().filter(|ev| ev.event_time_us >= from));
                    }
                }
            }
        }
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
    use bytes::Bytes;

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
        let s = Store::default();
        let a = b("w", 1, 0, &[(1, b"a"), (2, b"b")]);
        assert!(matches!(s.reserve(&a), Reserve::New));
        s.mark_durable(&a.id);
        match s.reserve(&a) {
            Reserve::Duplicate(rx) => assert_eq!(*rx.borrow(), State::Durable),
            _ => panic!("expected duplicate"),
        }
        // same identity, different payload
        assert!(matches!(
            s.reserve(&b("w", 1, 0, &[(1, b"a"), (2, b"X")])),
            Reserve::Conflict(_)
        ));
        // same identity, different timestamp only: still a conflict
        assert!(matches!(
            s.reserve(&b("w", 1, 0, &[(9, b"a"), (2, b"b")])),
            Reserve::Conflict(_)
        ));
        // partial overlap from either side
        assert!(matches!(
            s.reserve(&b("w", 1, 1, &[(1, b"b")])),
            Reserve::Conflict(_)
        ));
        let c = b("w", 1, 5, &[(1, b"c")]);
        assert!(matches!(s.reserve(&c), Reserve::New));
        assert!(matches!(
            s.reserve(&b("w", 1, 3, &[(1, b"x"), (1, b"y"), (1, b"z")])),
            Reserve::Conflict(_)
        ));
        // different epoch is a different sequence space
        assert!(matches!(
            s.reserve(&b("w", 2, 0, &[(1, b"zz")])),
            Reserve::New
        ));
    }

    #[test]
    fn frontier_gaps() {
        let s = Store::default();
        for seq in [0u64, 1, 3, 4] {
            let x = b("w", 1, seq, &[(seq, b"x")]);
            assert!(matches!(s.reserve(&x), Reserve::New));
            s.mark_durable(&x.id);
        }
        let f = &s.frontiers(None)[0];
        assert_eq!((f.contiguous, f.high), (2, 5));
        let gap = b("w", 1, 2, &[(2, b"x")]);
        assert!(matches!(s.reserve(&gap), Reserve::New));
        assert_eq!(
            s.frontiers(None)[0].contiguous,
            2,
            "pending batch must not advance frontier"
        );
        s.mark_durable(&gap.id);
        assert_eq!(s.frontiers(None)[0].contiguous, 5);
    }

    #[test]
    fn read_orders_by_event_time_with_deterministic_ties() {
        let s = Store::default();
        for x in [
            b("w2", 1, 0, &[(30, b"c"), (10, b"a")]),
            b("w1", 1, 0, &[(10, b"tie"), (20, b"b")]),
        ] {
            s.reserve(&x);
            s.mark_durable(&x.id);
        }
        let pending = b("w3", 1, 0, &[(0, b"invisible")]);
        s.reserve(&pending);
        let (ev, cov) = s.read("s", 0, 100);
        let got: Vec<_> = ev
            .iter()
            .map(|e| (e.event_time_us, e.writer.as_str()))
            .collect();
        assert_eq!(got, vec![(10, "w1"), (10, "w2"), (20, "w1"), (30, "w2")]);
        assert_eq!(cov.len(), 3);
        let (ev, _) = s.read("s", 15, 1);
        assert_eq!(ev[0].payload, Bytes::from_static(b"b"));
    }

    #[test]
    fn failed_reservation_is_released() {
        let s = Store::default();
        let a = b("w", 1, 0, &[(1, b"a")]);
        s.reserve(&a);
        s.mark_failed(&a.id);
        assert!(matches!(s.reserve(&a), Reserve::New));
    }
}
