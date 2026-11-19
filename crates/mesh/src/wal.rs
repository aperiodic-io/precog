//! Group commit on top of `precog-storage`.
//!
//! One OS thread owns the segment log. Every append queued while the previous group's
//! `fdatasync` runs joins the next group, so syncs per second are bounded by the disk, not
//! by the request rate. With write caching (`Config::flush_delay_ms`) a group is acknowledged
//! once written, and the thread runs the deferred fsync when it falls due, idle or not. Retention runs on the same thread between groups. A storage fault
//! (`precog_storage::Error::Fatal`) sets `fatal`: every later append fails, and the node
//! must restart and recover.

use crate::proto::{Batch, encode_log_key};
use anyhow::{Result, bail};
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

enum Req {
    Append {
        batch: Batch,
        done: oneshot::Sender<Result<Lsn, String>>,
    },
    Retain {
        max_bytes: Option<u64>,
        max_age_ms: Option<u64>,
        done: oneshot::Sender<Result<Lsn, String>>,
    },
}

#[derive(Clone)]
pub struct Wal {
    tx: std::sync::mpsc::Sender<Req>,
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
    /// Opens (and recovers) the segment log and starts the writer thread.
    pub fn open(cfg: Config) -> Result<Wal> {
        let log = precog_storage::Log::open(cfg)?;
        let reader = log.reader();
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        let stats = Arc::new(WalStats::default());
        let s = log.stats();
        stats.segments.store(s.segments as u64, Ordering::Relaxed);
        stats
            .recovery_scanned_bytes
            .store(s.recovery_scanned_bytes, Ordering::Relaxed);
        let fatal = Arc::new(AtomicBool::new(false));
        {
            let stats = stats.clone();
            let fatal = fatal.clone();
            std::thread::Builder::new()
                .name("precog-wal".into())
                .spawn(move || writer(log, rx, stats, fatal))?;
        }
        Ok(Wal {
            tx,
            stats,
            fatal,
            reader,
        })
    }

    /// Resolves with the batch's LSN once it is on stable storage.
    pub async fn append(&self, batch: Batch) -> Result<Lsn> {
        Self::durable(self.submit(batch)?).await
    }

    /// Queues the batch: batches reach the log in the order they are submitted.
    pub fn submit(&self, batch: Batch) -> Result<oneshot::Receiver<Result<Lsn, String>>> {
        if self.fatal.load(Ordering::Acquire) {
            bail!("storage is fenced after a fatal error");
        }
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Req::Append { batch, done })
            .map_err(|_| anyhow::anyhow!("wal writer stopped"))?;
        Ok(rx)
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

fn writer(
    mut log: precog_storage::Log,
    rx: std::sync::mpsc::Receiver<Req>,
    stats: Arc<WalStats>,
    fatal: Arc<AtomicBool>,
) {
    let mut group: Vec<(Batch, oneshot::Sender<Result<Lsn, String>>)> = Vec::with_capacity(256);
    let mut pending: Option<Req> = None;
    loop {
        let first = match pending.take() {
            Some(r) => r,
            None => {
                let next = match log.flush_due() {
                    None => rx.recv().map_err(|_| true),
                    Some(due) => rx
                        .recv_timeout(due.saturating_duration_since(std::time::Instant::now()))
                        .map_err(|e| e == std::sync::mpsc::RecvTimeoutError::Disconnected),
                };
                match next {
                    Ok(r) => r,
                    Err(true) => {
                        let _ = log.flush();
                        return;
                    }
                    Err(false) => {
                        if let Err(e) = log.flush() {
                            tracing::error!("storage flush failed: {e}");
                        }
                        fatal.store(log.is_fenced(), Ordering::Release);
                        stats.syncs.store(log.stats().syncs, Ordering::Relaxed);
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
                let r = log.retain(max_bytes, cutoff).map_err(|e| e.to_string());
                stats
                    .segments
                    .store(log.stats().segments as u64, Ordering::Relaxed);
                fatal.store(log.is_fenced(), Ordering::Release);
                let _ = done.send(r);
                continue;
            }
            Req::Append { batch, done } => group.push((batch, done)),
        }
        let mut size = group[0].0.bytes.len();
        while size < MAX_GROUP_BYTES {
            match rx.try_recv() {
                Ok(Req::Append { batch, done }) => {
                    size += batch.bytes.len();
                    group.push((batch, done));
                }
                Ok(r @ Req::Retain { .. }) => {
                    pending = Some(r);
                    break;
                }
                Err(_) => break,
            }
        }
        let t = now_ms();
        let items: Vec<(Meta, &[u8])> = group
            .iter()
            .map(|(b, _)| {
                let meta = Meta {
                    key: encode_log_key(&b.id.log),
                    first_seq: b.id.first_seq,
                    count: b.count,
                    tag: b.ident,
                    time_ms: t,
                };
                (meta, &b.bytes[..])
            })
            .collect();
        let r = log.append(&items);
        // a fatal error fences the log for good; the node stops acknowledging until restart
        fatal.store(log.is_fenced(), Ordering::Release);
        match r {
            Ok(first) => {
                let s = log.stats();
                stats.syncs.store(s.syncs, Ordering::Relaxed);
                stats.bytes.store(s.bytes, Ordering::Relaxed);
                stats.segments.store(s.segments as u64, Ordering::Relaxed);
                for (i, (_, done)) in group.drain(..).enumerate() {
                    let _ = done.send(Ok(first + i as u64));
                }
            }
            Err(e) => {
                tracing::error!("storage append failed: {e}");
                let msg = e.to_string();
                for (_, done) in group.drain(..) {
                    let _ = done.send(Err(msg.clone()));
                }
            }
        }
    }
}
