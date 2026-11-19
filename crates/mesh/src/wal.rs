//! Group commit on top of `precog-storage`.
//!
//! One OS thread owns the segment log. Every append queued while the previous group's
//! `fdatasync` runs joins the next group, so syncs per second are bounded by the disk, not
//! by the request rate. With write caching (`Config::flush_delay_ms`) the caller writes its batch
//! under the log's lock and has its LSN at once; the thread only runs the deferred fsync when it
//! falls due (idle or not), without the lock. Retention runs on the same thread. A storage fault
//! (`precog_storage::Error::Fatal`) sets `fatal`: every later append fails, and the node
//! must restart and recover.

use crate::proto::{Batch, encode_log_key, encode_replacing_key};
use anyhow::{Result, bail};
use parking_lot::Mutex;
use precog_storage::record::Meta;
use precog_storage::{Config, Lsn, Reader};
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
}

type Done = oneshot::Sender<Result<Lsn, String>>;

enum Req {
    Append {
        batch: Batch,
        /// It replaces a conflicting batch under its identity (`encode_replacing_key`).
        replaces: bool,
        done: Done,
    },
    Retain {
        max_bytes: Option<u64>,
        max_age_ms: Option<u64>,
        done: oneshot::Sender<Result<Lsn, String>>,
    },
    /// Write caching: the flush deadline moved (`Log::flush_due`).
    Wake,
}

#[derive(Clone)]
pub struct Wal {
    tx: std::sync::mpsc::Sender<Req>,
    log: Arc<Mutex<precog_storage::Log>>,
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

impl Wal {
    /// Opens (and recovers) the segment log and starts the writer thread. Returns the index
    /// entries of every retained record, for rebuilding the node's metadata.
    pub fn open(cfg: Config) -> Result<(Wal, Vec<precog_storage::Entry>)> {
        let caching = cfg.flush_delay_ms > 0;
        let log = precog_storage::Log::open(cfg)?;
        let reader = log.reader();
        let entries = reader.entries(reader.first_lsn(), usize::MAX);
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        let stats = Arc::new(WalStats::default());
        let s = log.stats();
        stats.segments.store(s.segments as u64, Ordering::Relaxed);
        stats
            .recovery_scanned_bytes
            .store(s.recovery_scanned_bytes, Ordering::Relaxed);
        let fatal = Arc::new(AtomicBool::new(false));
        let log = Arc::new(Mutex::new(log));
        {
            let log = log.clone();
            let stats = stats.clone();
            let fatal = fatal.clone();
            std::thread::Builder::new()
                .name("precog-wal".into())
                .spawn(move || writer(log, rx, stats, fatal))?;
        }
        Ok((
            Wal {
                tx,
                log,
                caching,
                stats,
                fatal,
                reader,
            },
            entries,
        ))
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
        if self.caching {
            let _ = done.send(self.write(&batch, replaces));
            return Ok(rx);
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

    /// Write caching: appends the batch on the calling thread, and wakes the WAL thread only when
    /// the flush deadline moves (the first unsynced write, or `flush_bytes` crossed).
    /// ponytail: a roll (its fsyncs, a zero-fill when no spare is ready) or the WAL thread's
    /// `retain` holds the lock and stalls every submitting worker; move rolls off the worker if
    /// `roll_stalls` or p99 say so.
    fn write(&self, batch: &Batch, replaces: bool) -> Result<Lsn, String> {
        let mut log = self.log.lock();
        let due = log.flush_due();
        let r = log.append(&[(meta(batch, replaces, now_ms()), &batch.bytes[..])]);
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

    /// Applies retention; returns the new first LSN.
    pub async fn retain(&self, max_bytes: Option<u64>, max_age_ms: Option<u64>) -> Result<Lsn> {
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Req::Retain {
                max_bytes,
                max_age_ms,
                done,
            })
            .map_err(|_| anyhow::anyhow!("wal writer stopped"))?;
        match rx.await {
            Ok(Ok(lsn)) => Ok(lsn),
            Ok(Err(e)) => bail!("storage: {e}"),
            Err(_) => bail!("wal writer dropped request"),
        }
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
            Req::Retain {
                max_bytes,
                max_age_ms,
                done,
            } => {
                let cutoff = max_age_ms.map(|a| now_ms().saturating_sub(a));
                let mut log = log.lock();
                let r = log.retain(max_bytes, cutoff).map_err(|e| e.to_string());
                stats
                    .segments
                    .store(log.stats().segments as u64, Ordering::Relaxed);
                fatal.store(log.is_fenced(), Ordering::Release);
                let _ = done.send(r);
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
        let t = now_ms();
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

    async fn replacing_key(flush_delay_ms: u64) {
        let dir = tempfile::tempdir().unwrap();
        let mut c = Config::new(dir.path());
        c.segment_size = 1 << 20;
        c.flush_delay_ms = flush_delay_ms;
        let (wal, _) = Wal::open(c).unwrap();
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
            .iter()
            .map(|e| decode_record_key(&e.meta.key).unwrap().1)
            .collect();
        assert_eq!(flags, [false, true], "flush_delay_ms {flush_delay_ms}");
    }
}
