//! Append-only write-ahead log with group commit.
//!
//! ```text
//! file   := magic "PCMWAL2\n" | record*
//! record := u32 batch_len | batch            (batch carries its own crc32c)
//! ```
//!
//! One dedicated OS thread owns the file. Every append request that arrives
//! while the previous `fdatasync` is running joins the next group, so the
//! number of syncs per second is bounded by disk latency, not by request rate.
//! A request is answered only after the sync that covers it returns.
//!
//! Recovery truncates a torn tail (incomplete header/body, or a final record
//! whose checksum fails) and fails closed on damage followed by more records.
//! An I/O error fences the WAL: every later append fails until reopen.

use crate::proto::{Batch, MAX_BATCH_BYTES};
use anyhow::{Context, Result, bail};
use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::oneshot;

const MAGIC: &[u8; 8] = b"PCMWAL2\n";
const MAX_GROUP_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default, Debug)]
pub struct WalStats {
    pub bytes: AtomicU64,
    pub syncs: AtomicU64,
    pub records: AtomicU64,
}

struct Req {
    batch: Bytes,
    done: oneshot::Sender<Result<(), String>>,
}

#[derive(Clone)]
pub struct Wal {
    tx: std::sync::mpsc::Sender<Req>,
    pub stats: Arc<WalStats>,
    fenced: Arc<AtomicBool>,
}

pub struct Recovered {
    pub batches: Vec<Batch>,
    pub truncated_bytes: u64,
}

impl Wal {
    /// Opens (creating if needed) `dir/wal.log`, takes an exclusive lock,
    /// recovers existing records and starts the writer thread.
    pub fn open(dir: &Path, sync: bool) -> Result<(Wal, Recovered)> {
        std::fs::create_dir_all(dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("LOCK"))?;
        lock.try_lock()
            .map_err(|e| anyhow::anyhow!("data dir {} is locked: {e}", dir.display()))?;
        let path = dir.join("wal.log");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        let recovered = recover(&mut file, &path)?;
        let stats = Arc::new(WalStats::default());
        stats.bytes.store(file.metadata()?.len(), Ordering::Relaxed);
        stats
            .records
            .store(recovered.batches.len() as u64, Ordering::Relaxed);
        let (tx, rx) = std::sync::mpsc::channel::<Req>();
        let fenced = Arc::new(AtomicBool::new(false));
        {
            let stats = stats.clone();
            let fenced = fenced.clone();
            std::thread::Builder::new()
                .name("wal-writer".into())
                .spawn(move || writer_loop(file, lock, rx, sync, stats, fenced))?;
        }
        Ok((Wal { tx, stats, fenced }, recovered))
    }

    /// Appends a validated batch; resolves after it is on stable storage
    /// (or after the write, if the WAL was opened with `sync = false`).
    pub async fn append(&self, batch: Bytes) -> Result<()> {
        if self.fenced.load(Ordering::Acquire) {
            bail!("wal fenced after I/O error");
        }
        let (done, rx) = oneshot::channel();
        self.tx
            .send(Req { batch, done })
            .map_err(|_| anyhow::anyhow!("wal writer stopped"))?;
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => bail!("wal: {e}"),
            Err(_) => bail!("wal writer dropped request"),
        }
    }
}

fn writer_loop(
    mut file: File,
    _lock: File,
    rx: std::sync::mpsc::Receiver<Req>,
    sync: bool,
    stats: Arc<WalStats>,
    fenced: Arc<AtomicBool>,
) {
    let mut buf = BytesMut::with_capacity(1 << 20);
    let mut group: Vec<Req> = Vec::with_capacity(256);
    while let Ok(first) = rx.recv() {
        group.push(first);
        let mut size = group[0].batch.len();
        while size < MAX_GROUP_BYTES {
            match rx.try_recv() {
                Ok(r) => {
                    size += r.batch.len();
                    group.push(r);
                }
                Err(_) => break,
            }
        }
        if fenced.load(Ordering::Acquire) {
            for r in group.drain(..) {
                let _ = r.done.send(Err("wal fenced".into()));
            }
            continue;
        }
        buf.clear();
        for r in &group {
            buf.put_u32_le(r.batch.len() as u32);
            buf.put_slice(&r.batch);
        }
        let res = file
            .write_all(&buf)
            .and_then(|_| if sync { file.sync_data() } else { Ok(()) });
        match res {
            Ok(()) => {
                stats.bytes.fetch_add(buf.len() as u64, Ordering::Relaxed);
                stats
                    .records
                    .fetch_add(group.len() as u64, Ordering::Relaxed);
                if sync {
                    stats.syncs.fetch_add(1, Ordering::Relaxed);
                }
                for r in group.drain(..) {
                    let _ = r.done.send(Ok(()));
                }
            }
            Err(e) => {
                // We no longer know what reached the disk: fence until reopen,
                // where recovery decides.
                fenced.store(true, Ordering::Release);
                let msg = format!("write/sync failed: {e}");
                tracing::error!("{msg}; WAL fenced");
                for r in group.drain(..) {
                    let _ = r.done.send(Err(msg.clone()));
                }
            }
        }
    }
}

fn recover(file: &mut File, path: &PathBuf) -> Result<Recovered> {
    let mut data = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut data)?;
    if data.is_empty() {
        file.write_all(MAGIC)?;
        file.sync_all()?;
        return Ok(Recovered {
            batches: vec![],
            truncated_bytes: 0,
        });
    }
    if data.len() < MAGIC.len() {
        // A crash while writing the header of a fresh file.
        if MAGIC.starts_with(&data) {
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            return Ok(Recovered {
                batches: vec![],
                truncated_bytes: data.len() as u64,
            });
        }
        bail!("{}: not a WAL file", path.display());
    }
    if &data[..MAGIC.len()] != MAGIC {
        bail!("{}: bad WAL magic", path.display());
    }
    let data = Bytes::from(data);
    let mut off = MAGIC.len();
    let mut batches = Vec::new();
    let mut good_end = off;
    while off < data.len() {
        if data.len() - off < 4 {
            break; // torn length prefix
        }
        let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        let start = off + 4;
        if len > MAX_BATCH_BYTES || start + len > data.len() {
            if len > MAX_BATCH_BYTES && start + len <= data.len() {
                bail!("{}: corrupt record length at offset {off}", path.display());
            }
            break; // torn body
        }
        match Batch::parse(data.slice(start..start + len)) {
            Ok(b) => {
                batches.push(b);
                off = start + len;
                good_end = off;
            }
            Err(e) => {
                if start + len == data.len() {
                    break; // damaged final record: never acknowledged, drop it
                }
                bail!(
                    "{}: corrupt record at offset {off} followed by more data: {e}",
                    path.display()
                );
            }
        }
    }
    let truncated = (data.len() - good_end) as u64;
    if truncated > 0 {
        tracing::warn!("WAL recovery truncating {truncated} torn tail bytes");
        file.set_len(good_end as u64)?;
    }
    file.seek(SeekFrom::End(0))?;
    // Recovered-but-maybe-unsynced records become durable now, so a retry of a
    // batch whose earlier outcome was unknown can be answered as a duplicate.
    file.sync_all()?;
    Ok(Recovered {
        batches,
        truncated_bytes: truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::LogId;

    fn batch(seq: u64) -> Bytes {
        let log = LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        };
        Batch::encode(&log, seq, &[(seq, b"payload")]).unwrap()
    }

    #[tokio::test]
    async fn persist_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, rec) = Wal::open(dir.path(), true).unwrap();
            assert!(rec.batches.is_empty());
            for i in 0..10 {
                wal.append(batch(i)).await.unwrap();
            }
        }
        // writer thread holds the lock until its channel closes
        std::thread::sleep(std::time::Duration::from_millis(50));
        let (_wal, rec) = Wal::open(dir.path(), true).unwrap();
        assert_eq!(rec.batches.len(), 10);
        assert_eq!(rec.batches[9].id.first_seq, 9);
    }

    #[tokio::test]
    async fn lock_excludes_second_opener() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, _) = Wal::open(dir.path(), true).unwrap();
        assert!(Wal::open(dir.path(), true).is_err());
    }

    fn write_raw(dir: &Path, tail: &[u8]) {
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.join("wal.log"))
            .unwrap();
        f.write_all(tail).unwrap();
    }

    async fn make(dir: &Path, n: u64) {
        let (wal, _) = Wal::open(dir, true).unwrap();
        for i in 0..n {
            wal.append(batch(i)).await.unwrap();
        }
        drop(wal);
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn torn_tail_truncated() {
        for tail in [&[1u8, 0][..], &[200, 0, 0, 0, 1, 2, 3][..]] {
            let dir = tempfile::tempdir().unwrap();
            make(dir.path(), 3).await;
            let before = std::fs::metadata(dir.path().join("wal.log")).unwrap().len();
            write_raw(dir.path(), tail);
            let (_w, rec) = Wal::open(dir.path(), true).unwrap();
            assert_eq!(rec.batches.len(), 3);
            assert_eq!(rec.truncated_bytes, tail.len() as u64);
            assert_eq!(
                std::fs::metadata(dir.path().join("wal.log")).unwrap().len(),
                before
            );
        }
    }

    #[tokio::test]
    async fn corrupt_final_record_dropped_interior_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 3).await;
        let p = dir.path().join("wal.log");
        let mut v = std::fs::read(&p).unwrap();
        let last = v.len() - 3;
        v[last] ^= 0xff;
        std::fs::write(&p, &v).unwrap();
        let (w, rec) = Wal::open(dir.path(), true).unwrap();
        assert_eq!(rec.batches.len(), 2);
        drop(w);
        std::thread::sleep(std::time::Duration::from_millis(50));

        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 3).await;
        let p = dir.path().join("wal.log");
        let mut v = std::fs::read(&p).unwrap();
        v[MAGIC.len() + 10] ^= 0xff; // inside the first record
        std::fs::write(&p, &v).unwrap();
        assert!(Wal::open(dir.path(), true).is_err());
    }
}
