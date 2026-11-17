//! Group commit on top of `precog-storage`.
//!
//! One OS thread owns the segment log. Every append queued while the previous group's
//! `fdatasync` runs joins the next group, so syncs per second are bounded by the disk, not
//! by the request rate. Retention runs on the same thread between groups. A storage fault
//! (`precog_storage::Error::Fatal`) sets `fatal`: every later append fails, and the node
//! must restart and recover.

use crate::proto::{Batch, batch_crc, encode_log_key};
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
    /// Opens (and recovers) the segment log and starts the writer thread. Returns the index
    /// entries of every retained record, for rebuilding the node's metadata.
    pub fn open(cfg: Config) -> Result<(Wal, Vec<precog_storage::Entry>)> {
        let log = precog_storage::Log::open(cfg)?;
        let reader = log.reader();
        let entries = reader.entries(reader.first_lsn(), usize::MAX);
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        let stats = Arc::new(WalStats::default());
        let fatal = Arc::new(AtomicBool::new(false));
        {
            let stats = stats.clone();
            let fatal = fatal.clone();
            std::thread::Builder::new()
                .name("precog-wal".into())
                .spawn(move || writer(log, rx, stats, fatal))?;
        }
        Ok((
            Wal {
                tx,
                stats,
                fatal,
                reader,
            },
            entries,
        ))
    }

    /// Resolves with the batch's LSN once it is on stable storage.
    pub async fn append(&self, batch: Batch) -> Result<Lsn> {
        if self.fatal.load(Ordering::Acquire) {
            bail!("storage is fenced after a fatal error");
        }
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Req::Append { batch, done })
            .map_err(|_| anyhow::anyhow!("wal writer stopped"))?;
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
            None => match rx.recv() {
                Ok(r) => r,
                Err(_) => return,
            },
        };
        match first {
            Req::Retain {
                max_bytes,
                max_age_ms,
                done,
            } => {
                let cutoff = max_age_ms.map(|a| now_ms().saturating_sub(a));
                let r = log.retain(max_bytes, cutoff).map_err(|e| e.to_string());
                if matches!(r, Err(_)) && log_is_fatal(&r) {
                    fatal.store(true, Ordering::Release);
                }
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
                    tag: batch_crc(&b.bytes),
                    time_ms: t,
                };
                (meta, &b.bytes[..])
            })
            .collect();
        match log.append(&items) {
            Ok(first) => {
                let s = log.stats();
                stats.syncs.store(s.syncs, Ordering::Relaxed);
                stats.bytes.store(s.bytes, Ordering::Relaxed);
                for (i, (_, done)) in group.drain(..).enumerate() {
                    let _ = done.send(Ok(first + i as u64));
                }
            }
            Err(e) => {
                if matches!(e, precog_storage::Error::Fatal(_)) {
                    fatal.store(true, Ordering::Release);
                    tracing::error!("storage fault, node fenced until restart: {e}");
                }
                let msg = e.to_string();
                for (_, done) in group.drain(..) {
                    let _ = done.send(Err(msg.clone()));
                }
            }
        }
    }
}

fn log_is_fatal(r: &Result<Lsn, String>) -> bool {
    matches!(r, Err(e) if e.starts_with("fatal"))
}
