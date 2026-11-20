//! Group commit on top of `precog-storage`.
//!
//! One OS thread owns the segment log. Every append queued while the previous group's
//! `fdatasync` runs joins the next group, so syncs per second are bounded by the disk, not
//! by the request rate. With write caching (`Config::flush_delay_ms`) the caller writes its batch
//! under the log's lock and has its LSN at once; the thread only runs the deferred fsync when it
//! falls due (idle or not), without the lock. Retention (the cleaner's copies and the drop of
//! old segments) runs on the same thread. A caller never waits for a slow step: a batch that
//! rolls the segment, and every batch submitted while the thread rolls or runs retention, is
//! queued to the thread (`Wal::gate`), in submit order. A storage fault
//! (`precog_storage::Error::Fatal`) sets `fatal`: every later append fails, and the node
//! must restart and recover.

use crate::proto::{Batch, encode_log_key, encode_replacing_key};
use anyhow::{Result, bail};
use parking_lot::Mutex;
use precog_storage::record::Meta;
use precog_storage::{Config, Lsn, Reader, Sealed};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::oneshot;

/// Largest group handed to the log at once.
const MAX_GROUP_BYTES: usize = 16 << 20;

#[derive(Default, Debug)]
pub struct WalStats {
    pub bytes: AtomicU64,
    pub syncs: AtomicU64,
    pub segments: AtomicU64,
    pub recovery_scanned_bytes: AtomicU64,
    /// Bytes of records in the log now (what the node cap bounds).
    pub log_bytes: AtomicU64,
    /// The newest append time given (ms): no record is stamped before it.
    pub newest_ms: AtomicU64,
}

type Done = oneshot::Sender<Result<Lsn, String>>;

enum Req {
    Append {
        batch: Batch,
        /// It replaces a conflicting batch under its identity (`encode_replacing_key`).
        replaces: bool,
        done: Done,
    },
    /// The cleaner's steps, on this thread between groups.
    Retain { job: Job, done: Done },
    /// Write caching: the flush deadline moved (`Log::flush_due`).
    Wake,
}

enum Job {
    /// Records copied forward as they are (one group), synced as appends are.
    Copy(Vec<(Meta, Arc<[u8]>)>),
    /// `Log::drop_until`: syncs what is unsynced (the copies), then drops.
    Drop(Lsn),
    /// Write caching: syncs what is unsynced now (before a checkpoint covers it).
    Sync,
}

#[derive(Clone)]
pub struct Wal {
    tx: std::sync::mpsc::Sender<Req>,
    log: Arc<Mutex<precog_storage::Log>>,
    /// Write caching: set while the WAL thread runs a slow step (a roll, retention), during which
    /// callers queue their batches to it instead of waiting for the log. Taken before `log`.
    gate: Arc<Mutex<bool>>,
    caching: bool,
    pub stats: Arc<WalStats>,
    pub fatal: Arc<AtomicBool>,
    pub reader: Reader,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The append time for a new record: now, but never before one already given (the wall clock
/// can step back), so a stream's trim order (`store::Age`) is the log's order for new records.
fn stamp(stats: &WalStats) -> u64 {
    let t = now_ms();
    stats.newest_ms.fetch_max(t, Ordering::Relaxed).max(t)
}

impl Wal {
    /// Opens (and recovers) the segment log and starts the writer thread.
    pub fn open(cfg: Config) -> Result<Wal> {
        let caching = cfg.flush_delay_ms > 0;
        let log = precog_storage::Log::open(cfg)?;
        let reader = log.reader();
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        let stats = Arc::new(WalStats::default());
        let s = log.stats();
        stats.segments.store(s.segments as u64, Ordering::Relaxed);
        stats.log_bytes.store(s.log_bytes, Ordering::Relaxed);
        stats.newest_ms.store(log.max_time(), Ordering::Relaxed);
        stats
            .recovery_scanned_bytes
            .store(s.recovery_scanned_bytes, Ordering::Relaxed);
        let fatal = Arc::new(AtomicBool::new(false));
        let log = Arc::new(Mutex::new(log));
        let gate = Arc::new(Mutex::new(false));
        {
            let log = log.clone();
            let gate = gate.clone();
            let stats = stats.clone();
            let fatal = fatal.clone();
            std::thread::Builder::new()
                .name("precog-wal".into())
                .spawn(move || writer(log, gate, rx, stats, fatal))?;
        }
        Ok(Wal {
            tx,
            log,
            gate,
            caching,
            stats,
            fatal,
            reader,
        })
    }

    /// Resolves with the batch's LSN once it is on stable storage.
    pub async fn append(&self, batch: Batch) -> Result<Lsn> {
        Self::durable(self.submit(batch)?).await
    }

    /// Queues the batch (under write caching: writes it on the caller): batches reach the log in
    /// the order they are submitted.
    pub fn submit(&self, batch: Batch) -> Result<oneshot::Receiver<Result<Lsn, String>>> {
        self.submit_as(batch, false)
    }

    /// Logs `batch` in place of the conflicting batch this node holds under its identity.
    pub async fn append_replacing(&self, batch: Batch) -> Result<Lsn> {
        Self::durable(self.submit_as(batch, true)?).await
    }

    fn submit_as(
        &self,
        batch: Batch,
        replaces: bool,
    ) -> Result<oneshot::Receiver<Result<Lsn, String>>> {
        if self.fatal.load(Ordering::Acquire) {
            bail!("storage is fenced after a fatal error");
        }
        let (done, rx) = oneshot::channel();
        // held until the batch is written or queued: no batch submitted after it goes first
        let mut gate = self.caching.then(|| self.gate.lock());
        if let Some(g) = &mut gate
            && !**g
        {
            let log = self.log.lock();
            let item = [(meta(&batch, replaces, stamp(&self.stats)), &batch.bytes[..])];
            if !log.rolls(&item) {
                drop(gate);
                let _ = done.send(self.write(log, &item));
                return Ok(rx);
            }
            **g = true; // the WAL thread rolls; the batches after this one queue behind it
        }
        self.tx
            .send(Req::Append {
                batch,
                replaces,
                done,
            })
            .map_err(|_| anyhow::anyhow!("wal writer stopped"))?;
        Ok(rx)
    }

    /// Write caching: appends a batch that does not roll on the calling thread, and wakes the WAL
    /// thread only when the flush deadline moves (the first unsynced write, or `flush_bytes`
    /// crossed). ponytail: past twice `flush_bytes` unsynced the append syncs here, on the caller
    /// (an fsync of at most that much); queue it too if p99 shows it.
    fn write(
        &self,
        mut log: parking_lot::MutexGuard<'_, precog_storage::Log>,
        item: &[(Meta, &[u8])],
    ) -> Result<Lsn, String> {
        let due = log.flush_due();
        let r = log.append(item);
        publish(&log, &self.stats, &self.fatal);
        let wake = log.flush_due() != due;
        drop(log);
        if wake {
            let _ = self.tx.send(Req::Wake);
        }
        r.map_err(|e| {
            tracing::error!("storage append failed: {e}");
            e.to_string()
        })
    }

    /// Waits for a submitted batch's LSN.
    pub async fn durable(rx: oneshot::Receiver<Result<Lsn, String>>) -> Result<Lsn> {
        match rx.await {
            Ok(Ok(lsn)) => Ok(lsn),
            Ok(Err(e)) => bail!("storage: {e}"),
            Err(_) => bail!("wal writer dropped request"),
        }
    }

    /// The sealed segments, oldest first, how many of them these bounds drop (`max_age_ms`
    /// before now), and the log's bytes.
    pub fn retirable(
        &self,
        max_bytes: Option<u64>,
        max_age_ms: Option<u64>,
    ) -> (Vec<Sealed>, usize, u64) {
        let cutoff = max_age_ms.map(|a| now_ms().saturating_sub(a));
        let log = self.log.lock();
        let (segs, total) = log.segments();
        (segs, log.retirable(max_bytes, cutoff), total)
    }

    /// Logs `records` again, unchanged, as one group; resolves with the first one's new LSN.
    pub async fn copy(&self, records: Vec<(Meta, Arc<[u8]>)>) -> Result<Lsn> {
        self.request(Job::Copy(records)).await
    }

    /// Makes everything written durable, then drops the sealed segments that end at or below
    /// `end`; returns the new first LSN.
    pub async fn drop_until(&self, end: Lsn) -> Result<Lsn> {
        self.request(Job::Drop(end)).await
    }

    /// Makes everything written so far durable (a no-op without write caching).
    pub async fn sync(&self) -> Result<Lsn> {
        self.request(Job::Sync).await
    }

    async fn request(&self, job: Job) -> Result<Lsn> {
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Req::Retain { job, done })
            .map_err(|_| anyhow::anyhow!("wal writer stopped"))?;
        Self::durable(rx).await
    }
}

fn meta(b: &Batch, replaces: bool, time_ms: u64) -> Meta {
    Meta {
        key: if replaces {
            encode_replacing_key(&b.id.log)
        } else {
            encode_log_key(&b.id.log)
        },
        first_seq: b.id.first_seq,
        count: b.count,
        tag: b.ident,
        time_ms,
    }
}

fn publish(log: &precog_storage::Log, stats: &WalStats, fatal: &AtomicBool) {
    // a fatal error fences the log for good; the node stops acknowledging until restart. Only
    // ever raise it: the node also fences on its own (a claimed batch that failed to store).
    if log.is_fenced() {
        fatal.store(true, Ordering::Release);
    }
    let s = log.stats();
    stats.syncs.store(s.syncs, Ordering::Relaxed);
    stats.bytes.store(s.bytes, Ordering::Relaxed);
    stats.segments.store(s.segments as u64, Ordering::Relaxed);
    stats.log_bytes.store(s.log_bytes, Ordering::Relaxed);
}

/// Runs the due deferred fsync without holding the log, so writers go on meanwhile.
fn flush(log: &Mutex<precog_storage::Log>, stats: &WalStats, fatal: &AtomicBool) {
    let f = log.lock().begin_flush();
    let r = match f {
        Ok(Some(f)) => {
            let r = f.sync();
            let mut l = log.lock();
            let r = l.end_flush(f, r);
            publish(&l, stats, fatal);
            r
        }
        Ok(None) => Ok(()),
        Err(e) => Err(e),
    };
    if let Err(e) = r {
        tracing::error!("storage flush failed: {e}");
    }
}

fn writer(
    log: Arc<Mutex<precog_storage::Log>>,
    gate: Arc<Mutex<bool>>,
    rx: std::sync::mpsc::Receiver<Req>,
    stats: Arc<WalStats>,
    fatal: Arc<AtomicBool>,
) {
    let mut group: Vec<(Batch, bool, Done)> = Vec::with_capacity(256);
    let mut pending: Option<Req> = None;
    loop {
        let first = match pending.take() {
            Some(r) => r,
            None => {
                {
                    // the slow step that diverted callers is over once nothing is queued behind it
                    let mut g = gate.lock();
                    if *g {
                        match rx.try_recv() {
                            Ok(r) => {
                                pending = Some(r);
                                continue;
                            }
                            Err(_) => *g = false,
                        }
                    }
                }
                let due = log.lock().flush_due();
                let next = match due {
                    None => rx.recv().map_err(|_| true),
                    Some(due) => rx
                        .recv_timeout(due.saturating_duration_since(std::time::Instant::now()))
                        .map_err(|e| e == std::sync::mpsc::RecvTimeoutError::Disconnected),
                };
                match next {
                    Ok(r) => r,
                    Err(true) => {
                        let _ = log.lock().flush();
                        return;
                    }
                    Err(false) => {
                        flush(&log, &stats, &fatal);
                        continue;
                    }
                }
            }
        };
        match first {
            Req::Retain { job, done } => {
                *gate.lock() = true; // callers queue meanwhile rather than wait for the log
                let mut log = log.lock();
                let r = match job {
                    Job::Copy(records) => {
                        let items: Vec<(Meta, &[u8])> =
                            records.iter().map(|(m, p)| (m.clone(), &p[..])).collect();
                        log.append(&items)
                    }
                    Job::Drop(end) => log.drop_until(end),
                    Job::Sync => log.flush().map(|_| log.next_lsn()),
                };
                publish(&log, &stats, &fatal);
                let _ = done.send(r.map_err(|e| e.to_string()));
                continue;
            }
            Req::Append {
                batch,
                replaces,
                done,
            } => group.push((batch, replaces, done)),
            Req::Wake => continue,
        }
        let mut size = group[0].0.bytes.len();
        while size < MAX_GROUP_BYTES {
            match rx.try_recv() {
                Ok(Req::Append {
                    batch,
                    replaces,
                    done,
                }) => {
                    size += batch.bytes.len();
                    group.push((batch, replaces, done));
                }
                Ok(r @ Req::Retain { .. }) => {
                    pending = Some(r);
                    break;
                }
                Ok(Req::Wake) | Err(_) => break,
            }
        }
        let t = stamp(&stats);
        let items: Vec<(Meta, &[u8])> = group
            .iter()
            .map(|(b, replaces, _)| (meta(b, *replaces, t), &b.bytes[..]))
            .collect();
        let r = {
            let mut log = log.lock();
            let r = log.append(&items);
            publish(&log, &stats, &fatal);
            r
        };
        match r {
            Ok(first) => {
                for (i, (_, _, done)) in group.drain(..).enumerate() {
                    let _ = done.send(Ok(first + i as u64));
                }
            }
            Err(e) => {
                tracing::error!("storage append failed: {e}");
                let msg = e.to_string();
                for (_, _, done) in group.drain(..) {
                    let _ = done.send(Err(msg.clone()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{LogId, decode_record_key};

    #[tokio::test]
    async fn only_a_replacement_is_logged_with_a_replacing_key() {
        // through the WAL thread (fsync per ack) and on the caller (write caching)
        for flush_delay_ms in [0, 100] {
            replacing_key(flush_delay_ms).await;
        }
    }

    #[tokio::test]
    async fn append_times_never_go_back_when_the_clock_does() {
        // Why: a stream trims oldest first by append time (`store::Age`) and forgets everything below
        // the last batch it trimmed in that order. A batch stamped earlier than one already in
        // the log (the wall clock stepped back, here an hour, across a restart) would sort below
        // that floor: hidden from readers as soon as it is written.
        for flush_delay_ms in [0, 100] {
            let dir = tempfile::tempdir().unwrap();
            let mut c = Config::new(dir.path());
            c.flush_delay_ms = flush_delay_ms;
            let log = LogId {
                stream: "s".into(),
                writer: "w".into(),
                epoch: 1,
            };
            let at = |seq| Batch::parse(Batch::encode(&log, seq, &[(1, b"e")]).unwrap()).unwrap();
            let ahead = now_ms() + 3_600_000;
            {
                let mut l = precog_storage::Log::open(c.clone()).unwrap();
                let b = at(0);
                l.append(&[(meta(&b, false, ahead), &b.bytes[..])]).unwrap();
                l.flush().unwrap();
            }
            let wal = Wal::open(c).unwrap();
            let lsn = wal.append(at(1)).await.unwrap();
            let t = wal.reader.time_ms(lsn).unwrap().unwrap();
            assert!(t >= ahead, "flush_delay_ms {flush_delay_ms}: {t} < {ahead}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn under_write_caching_a_submit_never_waits_for_a_roll() {
        // Why: under write caching the caller is a network worker. A roll seals and syncs the
        // segment and, with no spare ready, zero-fills a whole new one: done on the worker under
        // the log's lock, it stalls that worker and every other one that submits meanwhile.
        let dir = tempfile::tempdir().unwrap();
        let mut c = Config::new(dir.path());
        c.segment_size = 64 << 20;
        c.spares = 0; // every roll zero-fills its segment: slow
        c.flush_delay_ms = 60_000;
        c.flush_bytes = 1 << 30;
        let wal = Wal::open(c).unwrap();
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let p = vec![7u8; 4 << 20];
        let mut rolled = false;
        for seq in 0..20 {
            let b = Batch::parse(Batch::encode(&log, seq, &[(1, &p[..])]).unwrap()).unwrap();
            let segments = wal.stats.segments.load(Ordering::Relaxed);
            let t = std::time::Instant::now();
            let rx = wal.submit(b).unwrap();
            let submitted = t.elapsed();
            assert_eq!(Wal::durable(rx).await.unwrap(), seq, "in submit order");
            if wal.stats.segments.load(Ordering::Relaxed) > segments {
                rolled = true;
                let written = t.elapsed();
                assert!(
                    submitted * 4 < written,
                    "the submit waited for the roll: {submitted:?} of {written:?}"
                );
            }
        }
        assert!(rolled, "never rolled");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn under_write_caching_a_submit_never_waits_for_retention() {
        // Why: retention runs on the WAL thread under the log's lock: a manifest write, file
        // renames, and the cleaner's copies, which can roll. A caller arriving meanwhile is a
        // network worker; it queues its batch behind the step instead of blocking on the lock.
        let dir = tempfile::tempdir().unwrap();
        let mut c = Config::new(dir.path());
        c.segment_size = 64 << 20;
        c.spares = 0; // the copy's roll zero-fills its segment: slow
        c.flush_delay_ms = 60_000;
        c.flush_bytes = 1 << 30;
        let wal = Wal::open(c).unwrap();
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let p = vec![7u8; 4 << 20];
        let at = |seq| Batch::parse(Batch::encode(&log, seq, &[(1, &p[..])]).unwrap()).unwrap();
        let mut seq = 0;
        let copy = loop {
            let b = at(seq);
            let item = [(meta(&b, false, 1), &b.bytes[..])];
            if wal.log.lock().rolls(&item) {
                break vec![(item[0].0.clone(), Arc::<[u8]>::from(&b.bytes[..]))];
            }
            wal.append(b).await.unwrap();
            seq += 1;
        };
        let w = wal.clone();
        let t = std::time::Instant::now();
        let copied = tokio::spawn(async move { w.copy(copy).await });
        while !*wal.gate.lock() {
            assert!(
                t.elapsed() < std::time::Duration::from_secs(5),
                "never gated"
            );
            tokio::task::yield_now().await;
        }
        let t = std::time::Instant::now();
        let rx = wal.submit(at(seq + 1)).unwrap();
        let submitted = t.elapsed();
        let first = copied.await.unwrap().unwrap();
        let copying = t.elapsed();
        assert!(
            submitted * 4 < copying,
            "the submit waited for retention: {submitted:?} of {copying:?}"
        );
        assert_eq!(first, seq, "the copy first");
        assert_eq!(
            Wal::durable(rx).await.unwrap(),
            seq + 1,
            "then the queued batch"
        );
    }

    async fn replacing_key(flush_delay_ms: u64) {
        let dir = tempfile::tempdir().unwrap();
        let mut c = Config::new(dir.path());
        c.segment_size = 1 << 20;
        c.flush_delay_ms = flush_delay_ms;
        let wal = Wal::open(c).unwrap();
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let at = |seq| Batch::parse(Batch::encode(&log, seq, &[(1, b"e")]).unwrap()).unwrap();
        wal.append(at(0)).await.unwrap();
        wal.append_replacing(at(1)).await.unwrap();
        let flags: Vec<bool> = wal
            .reader
            .entries(0, 10)
            .unwrap()
            .iter()
            .map(|e| decode_record_key(&e.meta.key).unwrap().1)
            .collect();
        assert_eq!(flags, [false, true], "flush_delay_ms {flush_delay_ms}");
    }
}

/// Model-based test: random bursts of appends (plain and replacing, queued together so the
/// writer groups them), retention, and an fsync that fails after a random number of syncs.
/// Acks arrive in submit order with consecutive LSNs, each acked batch reads back as logged,
/// and from the first failure on nothing is acknowledged again (Error::Fatal: ack means durable).
#[cfg(test)]
mod model {
    use super::*;
    use crate::proto::{LogId, decode_record_key};
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    enum Op {
        /// a burst of (payload bytes, replaces) submitted before any is awaited
        Burst(Vec<(usize, bool)>),
        Retain(u64),
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => prop::collection::vec((0usize..3000, any::<bool>()), 1..6).prop_map(Op::Burst),
                1 => (16u64..128).prop_map(|k| Op::Retain(k << 10)),
            ],
            1..24,
        )
    }

    fn run(caching: bool, fail_after: Option<u64>, ops: Vec<Op>) -> Result<(), TestCaseError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut c = Config::new(dir.path());
        c.segment_size = 32 << 10;
        c.failpoints.fail_data_sync_after = fail_after;
        if caching {
            // flush_bytes small enough that appends sync inline: the fsyncs happen in the test's order
            c.flush_delay_ms = 60_000;
            c.flush_bytes = 4 << 10;
        }
        let wal = Wal::open(c).unwrap();
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        let mut logged: Vec<(bytes::Bytes, bool)> = vec![];
        let mut first = 0;
        let mut failed = false;
        let mut seq = 0u64;
        rt.block_on(async {
            for op in ops {
                match op {
                    Op::Burst(bs) => {
                        let mut rxs = vec![];
                        for (n, replaces) in bs {
                            let p: Vec<u8> = (0..n).map(|i| (seq as usize + i) as u8).collect();
                            let b =
                                Batch::parse(Batch::encode(&log, seq, &[(seq, &p[..])]).unwrap())
                                    .unwrap();
                            seq += 1;
                            let bytes = b.bytes.clone();
                            // under write caching a deferred fsync can fail on the WAL thread
                            failed |= wal.fatal.load(Ordering::Acquire);
                            match wal.submit_as(b, replaces) {
                                Ok(rx) => rxs.push((rx, bytes, replaces, failed)),
                                // under write caching an append fails at once, fencing the rest
                                Err(_) => prop_assert!(
                                    failed || wal.fatal.load(Ordering::Acquire),
                                    "refused before any failure"
                                ),
                            }
                        }
                        for (rx, bytes, replaces, after) in rxs {
                            match Wal::durable(rx).await {
                                Ok(lsn) => {
                                    prop_assert!(!after, "acked after an fsync failed");
                                    prop_assert_eq!(
                                        lsn,
                                        logged.len() as u64,
                                        "acks in submit order"
                                    );
                                    logged.push((bytes, replaces));
                                }
                                Err(_) => {
                                    prop_assert!(
                                        fail_after.is_some(),
                                        "an append failed with no fault"
                                    );
                                    failed = true;
                                }
                            }
                        }
                        if failed {
                            prop_assert!(
                                wal.fatal.load(Ordering::Acquire),
                                "a failed fsync fences the wal"
                            );
                        }
                    }
                    Op::Retain(max) => {
                        failed |= wal.fatal.load(Ordering::Acquire);
                        let (segs, n, _) = wal.retirable(Some(max), None);
                        let end = segs[..n].last().map_or(0, |s| s.end);
                        match wal.drop_until(end).await {
                            Ok(f) => {
                                prop_assert!(!failed && f >= first);
                                first = f;
                            }
                            Err(e) => prop_assert!(
                                fail_after.is_some(),
                                "retention failed with no fault: {e}"
                            ),
                        }
                    }
                }
                for (lsn, (bytes, replaces)) in logged.iter().enumerate().skip(first as usize) {
                    let r = wal.reader.read(lsn as u64).unwrap().unwrap();
                    prop_assert_eq!(&r.payload[..], &bytes[..]);
                    prop_assert_eq!(
                        decode_record_key(&r.meta.key).unwrap(),
                        (log.clone(), *replaces)
                    );
                }
            }
            Ok(())
        })
    }

    proptest! {

        #[test]
        fn the_wal_acks_in_order_and_never_after_a_failed_fsync(
            fail_after in prop::option::of(0u64..12), ops in ops(),
        ) {
            run(false, fail_after, ops)?;
        }

        #[test]
        fn under_write_caching_the_wal_acks_in_order_and_never_after_a_failed_fsync(
            fail_after in prop::option::of(0u64..12), ops in ops(),
        ) {
            run(true, fail_after, ops)?;
        }
    }
}
