//! The segment log: one per node.
//!
//! Files in `Config::dir`:
//! - `seg-<id>.log`: a segment. A 4 KiB header page (magic, id, generation, base LSN, CRC)
//!   followed by records (see `record`). Segments are written once, front to back.
//! - `seg-<id>.idx`: the index of a sealed segment, one entry per record. Rebuilt from the
//!   segment if missing or damaged.
//! - `spare-<n>.log`: preallocated (zero-filled) or recycled segment files, ready to be renamed
//!   into place on a roll. A background thread keeps `Config::spares` of them.
//! - `MANIFEST`: two checksummed slots, written alternately: segments, generations, the retention
//!   floor. The manifest is a cache of what the segment headers already say: a successor segment
//!   with a valid header and contiguous LSNs is adopted even if the manifest missed it.
//!
//! Durability: `append` returns after `fdatasync`, or, with a deferred flush
//! (`Config::flush_delay_ms`), once written: the fsync then follows within the delay or once
//! `flush_bytes` are unsynced, as Redpanda's write caching does (the replicas carry durability in
//! between). Any I/O error on the write path is fatal (fsyncgate): the log refuses further writes
//! and the caller must restart and recover.

use crate::record::{self, Decoded, Meta};
use crate::{Error, Lsn, Result};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

const SEG_HEADER: u64 = 4096;
const SEG_MAGIC: u32 = u32::from_le_bytes(*b"PCSG");
const IDX_MAGIC: u32 = u32::from_le_bytes(*b"PCIX");
const MF_MAGIC: u32 = u32::from_le_bytes(*b"PCMF");
const FORMAT_VERSION: u32 = 1;
/// ponytail: 64 KiB manifest slots hold ~1,300 segments (1.3 TB at 1 GiB); grow the slot when needed.
const MF_SLOT: usize = 64 << 10;
/// Largest write group. Bounds the recovery search window (2x this) on restart.
pub const MAX_GROUP: usize = 32 << 20;

#[derive(Clone, Debug)]
pub struct Config {
    pub dir: PathBuf,
    /// Size of each segment file, header page included.
    pub segment_size: u64,
    /// Preallocated spare segments to keep ready.
    pub spares: usize,
    /// Bytes of recent records kept in memory for tail readers.
    pub tail_cache_bytes: usize,
    /// How fast the background preallocator zero-fills a spare (0: unpaced). Full speed
    /// saturates the disk and the log's fsyncs queue behind it; 64 MiB/s readies a 1 GiB spare
    /// in 16 s. A roll that finds no spare fills one at full speed (counted in `roll_stalls`).
    pub prealloc_bytes_per_s: u64,
    /// 0: `append` fsyncs every group. Otherwise a group is acknowledged once written and the
    /// fsync is deferred to the caller's `flush` (or `begin_flush`), due once the oldest
    /// unsynced group is this old or `flush_bytes` are unsynced (`flush_due` says when).
    pub flush_delay_ms: u64,
    pub flush_bytes: u64,
    pub failpoints: Failpoints,
}

/// Fault injection for tests. Leave at `Default` in production.
#[derive(Clone, Debug, Default)]
pub struct Failpoints {
    /// The data sync after this many successful ones fails.
    pub fail_data_sync_after: Option<u64>,
    /// Fail at a named step ("roll:after_spare_rename", "roll:after_header",
    /// "roll:after_manifest", "roll:after_index").
    pub fail_at: Option<&'static str>,
}

impl Config {
    pub fn new(dir: impl AsRef<Path>) -> Config {
        Config {
            dir: dir.as_ref().into(),
            segment_size: 1 << 30,
            spares: 1,
            tail_cache_bytes: 64 << 20,
            prealloc_bytes_per_s: 64 << 20,
            flush_delay_ms: 0,
            flush_bytes: 256 << 10,
            failpoints: Failpoints::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub segments: usize,
    /// Bytes of records in the log (sealed segments' ends plus the active segment's offset).
    pub log_bytes: u64,
    /// Sealed segments whose index file was missing or invalid at startup and was rebuilt.
    pub index_rebuilds: u64,
    pub bytes: u64,
    pub recovery_scanned_bytes: u64,
    pub spare_hits: u64,
    pub roll_stalls: u64,
    pub recycled: u64,
    /// Spares zero-filled (by the preallocator or on a roll that found none).
    pub spares_made: u64,
    pub cache_hits: u64,
    pub syncs: u64,
}

#[derive(Clone, Debug)]
pub struct Record {
    pub lsn: Lsn,
    pub meta: Meta,
    pub payload: Arc<[u8]>,
}

/// Index entry of one record.
#[derive(Clone, Debug)]
pub struct Entry {
    pub lsn: Lsn,
    pub segment: u64,
    pub offset: u64,
    pub payload_len: u32,
    pub meta: Meta,
}

#[derive(Clone, Debug)]
struct IdxEntry {
    offset: u64,
    len: u32,
    meta: Meta,
}

struct Seg {
    id: u64,
    generation: u64,
    base_lsn: Lsn,
    file: File,
    entries: RwLock<Vec<IdxEntry>>,
}

impl Seg {
    fn end_lsn(&self) -> Lsn {
        self.base_lsn + self.entries.read().unwrap().len() as u64
    }
}

struct Shared {
    segs: RwLock<Vec<Arc<Seg>>>,
    first_lsn: AtomicU64,
    /// Published after the group that contains it is durable.
    next_lsn: AtomicU64,
    cache: Mutex<TailCache>,
    cache_hits: AtomicU64,
}

#[derive(Default)]
struct TailCache {
    recs: VecDeque<Record>,
    bytes: usize,
    cap: usize,
}

impl TailCache {
    fn push(&mut self, r: Record) {
        if self.cap == 0 {
            return;
        }
        self.bytes += r.payload.len();
        self.recs.push_back(r);
        while self.bytes > self.cap {
            let x = self.recs.pop_front().unwrap();
            self.bytes -= x.payload.len();
        }
    }
    fn get(&self, lsn: Lsn) -> Option<Record> {
        let first = self.recs.front()?.lsn;
        if lsn < first {
            return None;
        }
        // appends push contiguous LSNs; checking costs nothing and keeps a wrong slot impossible
        self.recs
            .get((lsn - first) as usize)
            .filter(|r| r.lsn == lsn)
            .cloned()
    }
    fn clear(&mut self) {
        self.recs.clear();
        self.bytes = 0;
    }
}

/// Cheap, cloneable read handle; safe to use from any thread while the log appends.
#[derive(Clone)]
pub struct Reader {
    shared: Arc<Shared>,
}

impl Reader {
    /// The record at `lsn`, or `None` if it is not (or no longer) in the log.
    /// Damaged records are an error, never returned.
    pub fn read(&self, lsn: Lsn) -> Result<Option<Record>> {
        let s = &self.shared;
        if lsn >= s.next_lsn.load(Ordering::Acquire) || self.trimmed(lsn) {
            return Ok(None);
        }
        if let Some(r) = s.cache.lock().unwrap().get(lsn) {
            s.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(r));
        }
        let seg = {
            let segs = s.segs.read().unwrap();
            let i = segs.partition_point(|g| g.base_lsn <= lsn);
            if i == 0 {
                return Ok(None);
            }
            segs[i - 1].clone()
        };
        let e = {
            let ents = seg.entries.read().unwrap();
            match ents.get((lsn - seg.base_lsn) as usize) {
                Some(e) => e.clone(),
                None => return Ok(None),
            }
        };
        let mut buf = vec![0u8; e.len as usize];
        seg.file.read_exact_at(&mut buf, e.offset)?;
        match record::decode(&buf, 0, seg.generation) {
            Decoded::Record {
                header,
                key,
                payload,
                ..
            } if header.lsn == lsn => Ok(Some(Record {
                lsn,
                meta: meta_of(&header, key),
                payload: payload.into(),
            })),
            // retention may have recycled the file under this read
            _ if self.trimmed(lsn) => Ok(None),
            _ => Err(Error::Corrupt(format!(
                "segment {} offset {}: record {lsn} fails validation",
                seg.id, e.offset
            ))),
        }
    }

    /// Below the retention floor.
    fn trimmed(&self, lsn: Lsn) -> bool {
        lsn < self.shared.first_lsn.load(Ordering::Acquire)
    }

    /// Index entries from `from`, at most `limit`, of durable records only.
    pub fn entries(&self, from: Lsn, limit: usize) -> Vec<Entry> {
        let s = &self.shared;
        let end = s.next_lsn.load(Ordering::Acquire);
        let from = from.max(s.first_lsn.load(Ordering::Acquire));
        let segs = s.segs.read().unwrap().clone();
        let mut out = Vec::new();
        for seg in segs {
            let ents = seg.entries.read().unwrap();
            let seg_end = seg.base_lsn + ents.len() as u64;
            if seg_end <= from {
                continue;
            }
            let mut lsn = from.max(seg.base_lsn);
            let stop = seg_end.min(end);
            while lsn < stop && out.len() < limit {
                let e = &ents[(lsn - seg.base_lsn) as usize];
                out.push(Entry {
                    lsn,
                    segment: seg.id,
                    offset: e.offset,
                    payload_len: e.len - (record::HEADER + e.meta.key.len()) as u32,
                    meta: e.meta.clone(),
                });
                lsn += 1;
            }
        }
        out
    }

    pub fn next_lsn(&self) -> Lsn {
        self.shared.next_lsn.load(Ordering::Acquire)
    }

    pub fn first_lsn(&self) -> Lsn {
        self.shared.first_lsn.load(Ordering::Acquire)
    }

    pub fn clear_cache(&self) {
        self.shared.cache.lock().unwrap().clear();
    }
}

fn meta_of(h: &record::Header, key: &[u8]) -> Meta {
    Meta {
        key: key.to_vec(),
        first_seq: h.first_seq,
        count: h.count,
        tag: h.tag,
        time_ms: h.time_ms,
    }
}

// ---------------------------------------------------------------------------
// manifest

#[derive(Clone, Debug, Default, PartialEq)]
struct MfSeg {
    id: u64,
    generation: u64,
    base_lsn: Lsn,
    sealed: bool,
    end_offset: u64,
    end_lsn: Lsn,
    max_time: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Manifest {
    seq: u64,
    next_generation: u64,
    next_segment_id: u64,
    first_lsn: Lsn,
    segs: Vec<MfSeg>,
}

impl Manifest {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        for v in [self.next_generation, self.next_segment_id, self.first_lsn] {
            body.extend_from_slice(&v.to_le_bytes());
        }
        body.extend_from_slice(&(self.segs.len() as u32).to_le_bytes());
        for s in &self.segs {
            for v in [s.id, s.generation, s.base_lsn] {
                body.extend_from_slice(&v.to_le_bytes());
            }
            body.push(s.sealed as u8);
            for v in [s.end_offset, s.end_lsn, s.max_time] {
                body.extend_from_slice(&v.to_le_bytes());
            }
        }
        if body.len() + 24 > MF_SLOT {
            return Err(Error::Invalid(format!(
                "manifest exceeds {MF_SLOT} bytes ({} segments)",
                self.segs.len()
            )));
        }
        let mut out = Vec::with_capacity(MF_SLOT);
        out.extend_from_slice(&MF_MAGIC.to_le_bytes());
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        let mut h = crc32c::crc32c(&out[..20]);
        h = crc32c::crc32c_append(h, &body);
        out.extend_from_slice(&h.to_le_bytes());
        out.extend_from_slice(&body);
        out.resize(MF_SLOT, 0);
        Ok(out)
    }

    fn decode(b: &[u8]) -> Option<Manifest> {
        let h = b.get(..24)?;
        if u32_at(h, 0) != MF_MAGIC || u32_at(h, 4) != FORMAT_VERSION {
            return None;
        }
        let body = b.get(24..24usize.checked_add(u32_at(h, 16) as usize)?)?;
        if body.len() < 28 || crc32c::crc32c_append(crc32c::crc32c(&b[..20]), body) != u32_at(b, 20)
        {
            return None;
        }
        let mut m = Manifest {
            seq: u64_at(b, 8),
            next_generation: u64_at(body, 0),
            next_segment_id: u64_at(body, 8),
            first_lsn: u64_at(body, 16),
            segs: vec![],
        };
        let n = u32_at(body, 24) as usize;
        let mut o = 28;
        for _ in 0..n {
            if o + 49 > body.len() {
                return None;
            }
            m.segs.push(MfSeg {
                id: u64_at(body, o),
                generation: u64_at(body, o + 8),
                base_lsn: u64_at(body, o + 16),
                sealed: body[o + 24] == 1,
                end_offset: u64_at(body, o + 25),
                end_lsn: u64_at(body, o + 33),
                max_time: u64_at(body, o + 41),
            });
            o += 49;
        }
        Some(m)
    }
}

fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("seg-{id:016}.log"))
}
fn idx_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("seg-{id:016}.idx"))
}

fn seg_header(id: u64, generation: u64, base_lsn: Lsn) -> Vec<u8> {
    let mut b = Vec::with_capacity(SEG_HEADER as usize);
    b.extend_from_slice(&SEG_MAGIC.to_le_bytes());
    b.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    for v in [id, generation, base_lsn] {
        b.extend_from_slice(&v.to_le_bytes());
    }
    let c = crc32c::crc32c(&b);
    b.extend_from_slice(&c.to_le_bytes());
    b.resize(SEG_HEADER as usize, 0);
    b
}

/// (id, generation, base_lsn) of a segment file, if its header is valid.
fn read_seg_header(f: &File) -> Option<(u64, u64, Lsn)> {
    let mut b = [0u8; 36];
    f.read_exact_at(&mut b, 0).ok()?;
    if u32_at(&b, 0) != SEG_MAGIC
        || u32_at(&b, 4) != FORMAT_VERSION
        || crc32c::crc32c(&b[..32]) != u32_at(&b, 32)
    {
        return None;
    }
    Some((u64_at(&b, 8), u64_at(&b, 16), u64_at(&b, 24)))
}

fn encode_index(seg: &Seg) -> Vec<u8> {
    let ents = seg.entries.read().unwrap();
    let mut b = Vec::with_capacity(40 + ents.len() * 48);
    b.extend_from_slice(&IDX_MAGIC.to_le_bytes());
    b.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    for v in [seg.id, seg.generation, seg.base_lsn] {
        b.extend_from_slice(&v.to_le_bytes());
    }
    b.extend_from_slice(&(ents.len() as u32).to_le_bytes());
    for e in ents.iter() {
        b.extend_from_slice(&e.offset.to_le_bytes());
        b.extend_from_slice(&e.len.to_le_bytes());
        b.extend_from_slice(&e.meta.first_seq.to_le_bytes());
        b.extend_from_slice(&e.meta.count.to_le_bytes());
        b.extend_from_slice(&e.meta.tag.to_le_bytes());
        b.extend_from_slice(&e.meta.time_ms.to_le_bytes());
        b.extend_from_slice(&(e.meta.key.len() as u16).to_le_bytes());
        b.extend_from_slice(&e.meta.key);
    }
    let c = crc32c::crc32c(&b);
    b.extend_from_slice(&c.to_le_bytes());
    b
}

fn decode_index(b: &[u8], id: u64, generation: u64, base_lsn: Lsn) -> Option<Vec<IdxEntry>> {
    let (body, crc) = b.split_at_checked(b.len().checked_sub(4)?)?;
    if crc32c::crc32c(body) != u32_at(crc, 0) {
        return None;
    }
    let h = body.get(..36)?;
    if u32_at(h, 0) != IDX_MAGIC || u32_at(h, 4) != FORMAT_VERSION {
        return None;
    }
    if (u64_at(h, 8), u64_at(h, 16), u64_at(h, 24)) != (id, generation, base_lsn) {
        return None;
    }
    let mut rest = &body[36..];
    let mut v = Vec::with_capacity(u32_at(h, 32) as usize);
    for _ in 0..u32_at(h, 32) {
        let e = rest.get(..38)?;
        let key = rest.get(38..38 + u16::from_le_bytes(e[36..38].try_into().unwrap()) as usize)?;
        v.push(IdxEntry {
            offset: u64_at(e, 0),
            len: u32_at(e, 8),
            meta: Meta {
                first_seq: u64_at(e, 12),
                count: u32_at(e, 20),
                tag: u32_at(e, 24),
                time_ms: u64_at(e, 28),
                key: key.to_vec(),
            },
        });
        rest = &rest[38 + key.len()..];
    }
    rest.is_empty().then_some(v)
}

// ---------------------------------------------------------------------------
// spare preallocation

struct Spares {
    pool: Mutex<Vec<PathBuf>>,
    cv: Condvar,
    stop: AtomicBool,
    target: usize,
    size: u64,
    rate: u64,
    dir: PathBuf,
    counter: AtomicU64,
    made: AtomicU64,
    /// Until when the log counts as recycling: `RECYCLING` after retention last recycled a
    /// segment into the pool.
    recycling_until: Mutex<Option<std::time::Instant>>,
}

/// A log that recycled a segment this recently is at its retention budget: each roll is followed
/// by a recycle, so the preallocator waits up to `RECYCLE_WAIT` for one before zero-filling.
const RECYCLING: std::time::Duration = std::time::Duration::from_secs(60);
const RECYCLE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

impl Spares {
    /// Waits (pool lock held) while the pool is short but a recycle is due; true when the pool
    /// filled up meanwhile.
    fn wait_for_recycle<'a>(
        &self,
        mut pool: std::sync::MutexGuard<'a, Vec<PathBuf>>,
    ) -> (std::sync::MutexGuard<'a, Vec<PathBuf>>, bool) {
        let recycling = self.recycling_until.lock().unwrap().is_some_and(|u| {
            u.checked_duration_since(std::time::Instant::now())
                .is_some()
        });
        if !recycling {
            return (pool, false);
        }
        let until = std::time::Instant::now() + RECYCLE_WAIT;
        while pool.len() < self.target && !self.stop.load(Ordering::Acquire) {
            let Some(left) = until.checked_duration_since(std::time::Instant::now()) else {
                return (pool, false);
            };
            pool = self.cv.wait_timeout(pool, left).unwrap().0;
        }
        let filled = pool.len() >= self.target;
        (pool, filled)
    }

    /// Creates one zero-filled spare file, durably named; `paced`: at `rate`, synced every
    /// 4 MiB, so it never floods the disk the log fsyncs on.
    fn make(&self, paced: bool) -> std::io::Result<PathBuf> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let tmp = self.dir.join(format!("spare-tmp-{n}"));
        let path = self.dir.join(format!("spare-{n:08}.log"));
        let mut f = File::create(&tmp)?;
        self.made.fetch_add(1, Ordering::Relaxed);
        let zeros = vec![0u8; 4 << 20]; // paced, each chunk is synced
        let (start, mut done) = (std::time::Instant::now(), 0u64);
        while done < self.size {
            let k = (self.size - done).min(zeros.len() as u64) as usize;
            f.write_all(&zeros[..k])?;
            done += k as u64;
            if paced && self.rate > 0 {
                f.sync_data()?;
                let due = std::time::Duration::from_secs_f64(done as f64 / self.rate as f64);
                if let Some(wait) = due.checked_sub(start.elapsed()) {
                    std::thread::sleep(wait);
                }
                if self.stop.load(Ordering::Acquire) {
                    drop(f);
                    let _ = std::fs::remove_file(&tmp);
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "log closed",
                    ));
                }
            }
        }
        f.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        sync_dir(&self.dir)?;
        Ok(path)
    }

    fn run(self: Arc<Self>) {
        loop {
            {
                let mut pool = self.pool.lock().unwrap();
                while pool.len() >= self.target && !self.stop.load(Ordering::Acquire) {
                    pool = self.cv.wait(pool).unwrap();
                }
                let (_pool, filled) = self.wait_for_recycle(pool);
                if self.stop.load(Ordering::Acquire) {
                    return;
                }
                if filled {
                    continue;
                }
            }
            match self.make(true) {
                Ok(p) => self.pool.lock().unwrap().push(p),
                Err(_) => {
                    // the roll path creates its own when none is ready; retry later
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// the log

pub struct Log {
    cfg: Config,
    shared: Arc<Shared>,
    manifest: Manifest,
    /// Writable handle and state of the active segment.
    active: Option<Active>,
    failed: Option<String>,
    spares: Arc<Spares>,
    prealloc: Option<std::thread::JoinHandle<()>>,
    stats: Stats,
    buf: Vec<u8>,
    /// Bytes written to the active segment since its last fsync, and when the first of them was.
    unsynced: u64,
    unsynced_since: Option<std::time::Instant>,
    /// Bytes an outstanding `Flush` (`begin_flush`) covers and has not synced yet (0: none out).
    inflight: u64,
}

/// A deferred fsync taken out of the log (`Log::begin_flush`), so appends go on while it runs.
pub struct Flush {
    seg: Arc<Seg>,
    fail: bool,
}

impl Flush {
    /// Runs the fsync; hand the result to `Log::end_flush`.
    pub fn sync(&self) -> std::io::Result<()> {
        if self.fail {
            return Err(std::io::Error::other("failpoint: data sync"));
        }
        self.seg.file.sync_data()
    }
}

struct Active {
    seg: Arc<Seg>,
    file: File,
    offset: u64,
    capacity: u64,
    max_time: u64,
}

/// Result of scanning one segment.
struct Scan {
    entries: Vec<IdxEntry>,
    end: u64,
    max_time: u64,
    /// The scan stopped at damage (torn tail), not at clean zeroes/stale data.
    damaged: bool,
    /// Bytes past `end` that may hold stale same-generation data (zero them before reuse).
    dirty_end: u64,
    scanned: u64,
}

impl Log {
    pub fn open(cfg: Config) -> Result<Log> {
        std::fs::create_dir_all(&cfg.dir)?;
        let dir = cfg.dir.clone();
        let mf_path = dir.join("MANIFEST");
        let mut manifest = if mf_path.exists() {
            let b = std::fs::read(&mf_path)?;
            let a = b.get(..MF_SLOT).and_then(Manifest::decode);
            let c = b.get(MF_SLOT..2 * MF_SLOT).and_then(Manifest::decode);
            match (a, c) {
                (Some(x), Some(y)) => {
                    if x.seq >= y.seq {
                        x
                    } else {
                        y
                    }
                }
                (Some(x), None) | (None, Some(x)) => x,
                (None, None) => {
                    return Err(Error::Corrupt("both manifest slots are invalid".into()));
                }
            }
        } else {
            let has_segments = std::fs::read_dir(&dir)?
                .flatten()
                .any(|e| e.file_name().to_string_lossy().starts_with("seg-"));
            if has_segments {
                return Err(Error::Corrupt(
                    "segments exist but MANIFEST is missing".into(),
                ));
            }
            let m = Manifest {
                seq: 0,
                next_generation: 1,
                next_segment_id: 1,
                first_lsn: 0,
                segs: vec![],
            };
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&mf_path)?;
            let mut m1 = m.clone();
            m1.seq = 1;
            f.write_all(&m1.encode()?)?;
            let mut m2 = m.clone();
            m2.seq = 2;
            f.write_all(&m2.encode()?)?;
            f.sync_all()?;
            sync_dir(&dir)?;
            m2
        };

        // Leftover temporary spares are discarded; complete spares of the right size are pooled.
        let mut pool = vec![];
        let mut counter = 0u64;
        for e in std::fs::read_dir(&dir)?.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            // a closing log's paced preallocator may still rename or remove its own spare while
            // this lists them (a quick restart in one process)
            if name.starts_with("spare-tmp-") {
                let _ = std::fs::remove_file(e.path()); // best effort: gone is as good
            } else if let Some(n) = name
                .strip_prefix("spare-")
                .and_then(|s| s.strip_suffix(".log"))
            {
                counter = counter.max(n.trim_start_matches('r').parse::<u64>().unwrap_or(0) + 1);
                match std::fs::metadata(e.path()) {
                    Ok(m) if m.len() == cfg.segment_size => pool.push(e.path()),
                    Ok(_) => std::fs::remove_file(e.path())?,
                    Err(x) if x.kind() == std::io::ErrorKind::NotFound => {} // gone since listed
                    Err(x) => return Err(x.into()),
                }
            }
        }

        let shared = Arc::new(Shared {
            segs: RwLock::new(vec![]),
            first_lsn: AtomicU64::new(manifest.first_lsn),
            next_lsn: AtomicU64::new(manifest.first_lsn),
            cache: Mutex::new(TailCache {
                cap: cfg.tail_cache_bytes,
                ..Default::default()
            }),
            cache_hits: AtomicU64::new(0),
        });
        let spares = Arc::new(Spares {
            pool: Mutex::new(pool),
            cv: Condvar::new(),
            stop: AtomicBool::new(false),
            target: cfg.spares,
            size: cfg.segment_size,
            rate: cfg.prealloc_bytes_per_s,
            dir: dir.clone(),
            counter: AtomicU64::new(counter),
            made: AtomicU64::new(0),
            recycling_until: Mutex::new(None),
        });
        let mut log = Log {
            cfg,
            shared,
            manifest: manifest.clone(),
            active: None,
            failed: None,
            spares,
            prealloc: None,
            stats: Stats::default(),
            buf: Vec::with_capacity(1 << 20),
            unsynced: 0,
            unsynced_since: None,
            inflight: 0,
        };

        let unsynced = (log.cfg.flush_delay_ms > 0)
            .then(|| log.cfg.flush_bytes.saturating_add(MAX_GROUP as u64));
        // Sealed segments: drop ones missing at the head (retention whose manifest write was lost),
        // load their indexes (or rebuild them).
        let mut segs: Vec<Arc<Seg>> = vec![];
        let mut first_lsn = manifest.first_lsn;
        let listed = std::mem::take(&mut manifest.segs);
        let mut kept = vec![];
        for (i, m) in listed.iter().enumerate() {
            let path = seg_path(&dir, m.id);
            let file = match OpenOptions::new().read(true).write(!m.sealed).open(&path) {
                Ok(f) => f,
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound && segs.is_empty() && m.sealed =>
                {
                    first_lsn = listed.get(i + 1).map_or(m.end_lsn, |n| n.base_lsn);
                    continue;
                }
                Err(e) => {
                    return Err(Error::Corrupt(format!(
                        "segment {} listed in MANIFEST: {e}",
                        m.id
                    )));
                }
            };
            match read_seg_header(&file) {
                Some((id, g, b)) if id == m.id && g == m.generation && b == m.base_lsn => {}
                _ if segs.is_empty() && m.sealed => {
                    // recycled after a retention whose manifest write was lost
                    first_lsn = listed.get(i + 1).map_or(m.end_lsn, |n| n.base_lsn);
                    continue;
                }
                _ => {
                    return Err(Error::Corrupt(format!(
                        "segment {} header does not match MANIFEST",
                        m.id
                    )));
                }
            }
            if m.sealed {
                let index = std::fs::read(idx_path(&dir, m.id))
                    .ok()
                    .and_then(|b| decode_index(&b, m.id, m.generation, m.base_lsn))
                    .filter(|e| m.base_lsn + e.len() as u64 == m.end_lsn);
                let rebuilt = index.is_none();
                let entries = match index {
                    Some(e) => e,
                    None => {
                        let s = scan(&file, m.generation, m.base_lsn, Some(m.end_offset), None)?;
                        log.stats.index_rebuilds += 1;
                        log.stats.recovery_scanned_bytes += s.scanned;
                        if s.damaged {
                            return Err(Error::Corrupt(format!(
                                "sealed segment {} is damaged",
                                m.id
                            )));
                        }
                        if m.base_lsn + s.entries.len() as u64 != m.end_lsn {
                            return Err(Error::Corrupt(format!(
                                "sealed segment {} disagrees with MANIFEST",
                                m.id
                            )));
                        }
                        s.entries
                    }
                };
                let seg = Arc::new(Seg {
                    id: m.id,
                    generation: m.generation,
                    base_lsn: m.base_lsn,
                    file,
                    entries: RwLock::new(entries),
                });
                if rebuilt {
                    write_index(&dir, &seg)?;
                }
                segs.push(seg);
                kept.push(m.clone());
            } else {
                segs.push(Arc::new(Seg {
                    id: m.id,
                    generation: m.generation,
                    base_lsn: m.base_lsn,
                    file,
                    entries: RwLock::new(vec![]),
                }));
                kept.push(m.clone());
            }
        }
        manifest.segs = kept;
        manifest.first_lsn = first_lsn;
        log.shared.first_lsn.store(first_lsn, Ordering::Release);

        // The active segment (if any), then any successors the manifest missed.
        let mut next_lsn = segs.last().map_or(first_lsn, |s| {
            if manifest.segs.last().unwrap().sealed {
                s.end_lsn()
            } else {
                s.base_lsn
            }
        });
        let mut active: Option<(Arc<Seg>, Scan)> = None;
        if let Some(last) = manifest.segs.last()
            && !last.sealed
        {
            let seg = segs.pop().unwrap();
            let s = scan(&seg.file, seg.generation, seg.base_lsn, None, unsynced)?;
            log.stats.recovery_scanned_bytes += s.scanned;
            next_lsn = seg.base_lsn + s.entries.len() as u64;
            active = Some((seg, s));
        }
        loop {
            let id = manifest.next_segment_id;
            let path = seg_path(&dir, id);
            if !path.exists() {
                break;
            }
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            match read_seg_header(&file) {
                Some((hid, g, b))
                    if hid == id
                        && b == next_lsn
                        && g >= manifest.next_generation.saturating_sub(1) =>
                {
                    let s = scan(&file, g, b, None, unsynced)?;
                    log.stats.recovery_scanned_bytes += s.scanned;
                    if let Some((prev, ps)) = active.take() {
                        // Lost records would make the successor's base LSN mismatch (refused
                        // below); bytes past the last record of a full segment hold nothing.
                        // seal the previous active segment
                        *prev.entries.write().unwrap() = ps.entries;
                        let mf = manifest.segs.last_mut().unwrap();
                        mf.sealed = true;
                        mf.end_offset = ps.end;
                        mf.end_lsn = prev.end_lsn();
                        mf.max_time = ps.max_time;
                        write_index(&dir, &prev)?;
                        segs.push(prev);
                    }
                    manifest.segs.push(MfSeg {
                        id,
                        generation: g,
                        base_lsn: b,
                        ..Default::default()
                    });
                    manifest.next_segment_id = id + 1;
                    manifest.next_generation = manifest.next_generation.max(g + 1);
                    next_lsn = b + s.entries.len() as u64;
                    let seg = Arc::new(Seg {
                        id,
                        generation: g,
                        base_lsn: b,
                        file,
                        entries: RwLock::new(vec![]),
                    });
                    active = Some((seg, s));
                    log.write_manifest(&mut manifest)?;
                }
                Some((hid, g, b)) if hid == id => {
                    // A durable successor that does not continue the log: something before it
                    // is damaged or missing. Recycling it would discard acknowledged records.
                    return Err(Error::Corrupt(format!(
                        "segment {id} (generation {g}) starts at lsn {b} but the log before it ends at lsn {next_lsn}"
                    )));
                }
                _ => {
                    // a roll that crashed before its header was durable: return the file to the pool
                    drop(file);
                    log.recycle_file(&path)?;
                    break;
                }
            }
        }
        // a sealed tail with no active segment is fine: the next append rolls
        if let Some((seg, s)) = active {
            // zero the torn group's leftovers so a shorter write can never be followed by them
            let zeros = vec![0u8; s.dirty_end.saturating_sub(s.end) as usize];
            if !zeros.is_empty() {
                seg.file.write_all_at(&zeros, s.end)?;
                seg.file.sync_data()?;
            }
            *seg.entries.write().unwrap() = s.entries;
            let file = seg.file.try_clone()?;
            let capacity = file.metadata()?.len();
            log.active = Some(Active {
                seg: seg.clone(),
                file,
                offset: s.end,
                capacity,
                max_time: s.max_time,
            });
            segs.push(seg);
        }
        log.stats.segments = segs.len();
        *log.shared.segs.write().unwrap() = segs;
        log.shared.next_lsn.store(next_lsn, Ordering::Release);
        log.manifest = manifest;

        // Start with an active segment so no append ever waits for a spare to be zero-filled.
        if log.active.is_none() {
            log.roll()?;
        }
        let sp = log.spares.clone();
        log.prealloc = Some(
            std::thread::Builder::new()
                .name("precog-prealloc".into())
                .spawn(move || sp.run())?,
        );
        Ok(log)
    }

    pub fn reader(&self) -> Reader {
        Reader {
            shared: self.shared.clone(),
        }
    }

    pub fn next_lsn(&self) -> Lsn {
        self.shared.next_lsn.load(Ordering::Acquire)
    }

    pub fn first_lsn(&self) -> Lsn {
        self.shared.first_lsn.load(Ordering::Acquire)
    }

    /// When the deferred flush is due (`None`: nothing unsynced, or every group is synced or
    /// covered by a `Flush` that is out). Past `flush_bytes` it is due at once.
    pub fn flush_due(&self) -> Option<std::time::Instant> {
        let t = self.unsynced_since?;
        if self.unsynced >= self.cfg.flush_bytes {
            return Some(t);
        }
        Some(t + std::time::Duration::from_millis(self.cfg.flush_delay_ms))
    }

    /// The most bytes a crash can leave unsynced under write caching: what an outstanding flush
    /// covers plus what was appended meanwhile stays below twice `flush_bytes` (an append past it
    /// syncs), plus the group that crossed it. Recovery bounds a lost unsynced tail by this.
    pub fn unsynced_bound(cfg: &Config) -> u64 {
        cfg.flush_bytes
            .saturating_mul(2)
            .saturating_add(MAX_GROUP as u64)
    }

    /// Fsyncs what was written since the last fsync (nothing to do: free).
    pub fn flush(&mut self) -> Result<()> {
        let Some(f) = self.begin_flush()? else {
            return Ok(());
        };
        let r = f.sync();
        self.end_flush(f, r)
    }

    /// Takes the deferred fsync out of the log: run `Flush::sync` without holding the log, then
    /// `end_flush`. Groups appended meanwhile are not covered and keep their own deadline.
    pub fn begin_flush(&mut self) -> Result<Option<Flush>> {
        if let Some(f) = &self.failed {
            return Err(Error::Fatal(f.clone()));
        }
        if self.unsynced_since.is_none() {
            return Ok(None);
        }
        let fail = self
            .cfg
            .failpoints
            .fail_data_sync_after
            .is_some_and(|n| self.stats.syncs >= n);
        let seg = self.active.as_ref().unwrap().seg.clone();
        self.inflight = self.unsynced;
        self.unsynced = 0;
        self.unsynced_since = None;
        Ok(Some(Flush { seg, fail }))
    }

    /// Ends a `begin_flush`: a failed fsync is fatal, as under the lock.
    pub fn end_flush(&mut self, _flush: Flush, r: std::io::Result<()>) -> Result<()> {
        self.inflight = 0;
        if let Err(e) = r {
            return self.fail(format!("deferred flush: {e}"));
        }
        self.stats.syncs += 1;
        match &self.failed {
            Some(f) => Err(Error::Fatal(f.clone())),
            None => Ok(()),
        }
    }

    fn sync_active(&mut self) -> std::io::Result<()> {
        if let Some(n) = self.cfg.failpoints.fail_data_sync_after
            && self.stats.syncs >= n
        {
            return Err(std::io::Error::other("failpoint: data sync"));
        }
        if let Some(a) = &self.active {
            a.file.sync_data()?;
        }
        self.stats.syncs += 1;
        self.unsynced = 0;
        self.inflight = 0; // the whole file is synced, what an outstanding flush covers included
        self.unsynced_since = None;
        Ok(())
    }

    /// A fatal error refused all further writes; reopen to recover.
    pub fn is_fenced(&self) -> bool {
        self.failed.is_some()
    }

    pub fn stats(&self) -> Stats {
        let mut s = self.stats.clone();
        s.cache_hits = self.shared.cache_hits.load(Ordering::Relaxed);
        s.spares_made = self.spares.made.load(Ordering::Relaxed);
        s.segments = self.shared.segs.read().unwrap().len();
        s.log_bytes = self
            .manifest
            .segs
            .iter()
            .filter(|m| m.sealed)
            .map(|m| m.end_offset)
            .sum::<u64>()
            + self.active.as_ref().map_or(0, |a| a.offset);
        s
    }

    fn fail<T>(&mut self, what: impl Into<String>) -> Result<T> {
        let w = what.into();
        self.failed = Some(w.clone());
        Err(Error::Fatal(w))
    }

    /// Fails at a named step; only on rolls that seal a segment, so the crash leaves data to recover.
    fn failpoint(&mut self, step: &'static str, sealing: bool) -> Result<()> {
        if sealing && self.cfg.failpoints.fail_at == Some(step) {
            return self.fail(format!("failpoint {step}"));
        }
        Ok(())
    }

    /// Appends one write group. Returns the LSN of its first record after the whole group is
    /// on stable storage. On an I/O error the log becomes unusable (`Error::Fatal`).
    pub fn append(&mut self, group: &[(Meta, &[u8])]) -> Result<Lsn> {
        if let Some(f) = &self.failed {
            return Err(Error::Fatal(f.clone()));
        }
        if group.is_empty() {
            return Err(Error::Invalid("empty group".into()));
        }
        let mut total = 0usize;
        for (m, p) in group {
            if m.key.len() > record::MAX_KEY {
                return Err(Error::Invalid(format!(
                    "key of {} bytes exceeds {}",
                    m.key.len(),
                    record::MAX_KEY
                )));
            }
            if record::HEADER + m.key.len() + p.len() > record::MAX_RECORD {
                return Err(Error::Invalid(format!(
                    "record of {} bytes exceeds {}",
                    p.len(),
                    record::MAX_RECORD
                )));
            }
            total += record::encoded_len(m.key.len(), p.len());
        }
        let room = (self.cfg.segment_size - SEG_HEADER) as usize;
        if total > MAX_GROUP.min(room) {
            return Err(Error::Invalid(format!(
                "group of {total} bytes exceeds {}",
                MAX_GROUP.min(room)
            )));
        }
        if self
            .active
            .as_ref()
            .is_none_or(|a| a.offset + total as u64 > a.capacity)
        {
            self.roll()?;
        }
        let first = self.next_lsn();
        let a = self.active.as_mut().unwrap();
        let group_start = a.offset;
        self.buf.clear();
        let mut new = Vec::with_capacity(group.len());
        for (i, (m, p)) in group.iter().enumerate() {
            let off = a.offset + self.buf.len() as u64;
            record::encode(
                &mut self.buf,
                a.seg.generation,
                group_start,
                first + i as u64,
                m,
                p,
            );
            new.push(IdxEntry {
                offset: off,
                len: (record::HEADER + m.key.len() + p.len()) as u32,
                meta: m.clone(),
            });
        }
        let res = a.file.write_all_at(&self.buf, group_start).and_then(|_| {
            self.unsynced += self.buf.len() as u64;
            self.unsynced_since
                .get_or_insert_with(std::time::Instant::now);
            if self.cfg.flush_delay_ms == 0
                || self.unsynced + self.inflight >= self.cfg.flush_bytes.saturating_mul(2)
            {
                self.sync_active()
            } else {
                Ok(())
            }
        });
        if let Err(e) = res {
            return self.fail(format!("write/sync of group at lsn {first}: {e}"));
        }
        let a = self.active.as_mut().unwrap();
        a.offset += self.buf.len() as u64;
        for (m, _) in group {
            a.max_time = a.max_time.max(m.time_ms);
        }
        a.seg.entries.write().unwrap().extend(new);
        self.stats.bytes += self.buf.len() as u64;
        {
            let mut c = self.shared.cache.lock().unwrap();
            for (i, (m, p)) in group.iter().enumerate() {
                c.push(Record {
                    lsn: first + i as u64,
                    meta: m.clone(),
                    payload: (*p).into(),
                });
            }
        }
        self.shared
            .next_lsn
            .store(first + group.len() as u64, Ordering::Release);
        Ok(first)
    }

    fn write_manifest(&mut self, m: &mut Manifest) -> Result<()> {
        m.seq += 1;
        let bytes = m.encode()?;
        let f = OpenOptions::new()
            .write(true)
            .open(self.cfg.dir.join("MANIFEST"))?;
        f.write_all_at(&bytes, (m.seq % 2) * MF_SLOT as u64)?;
        f.sync_data()?;
        Ok(())
    }

    fn take_spare(&mut self) -> Result<PathBuf> {
        let p = {
            let mut pool = self.spares.pool.lock().unwrap();
            let p = pool.pop();
            self.spares.cv.notify_all(); // under the lock, so the preallocator cannot miss it
            p
        };
        Ok(match p {
            Some(p) => {
                self.stats.spare_hits += 1;
                p
            }
            None => {
                self.stats.roll_stalls += 1;
                self.spares.make(false)?
            }
        })
    }

    /// Seals the active segment (if any) and starts a new one from a spare.
    fn roll(&mut self) -> Result<()> {
        let dir = self.cfg.dir.clone();
        let id = self.manifest.next_segment_id;
        let generation = self.manifest.next_generation;
        let base = self.next_lsn();
        let sealing = self.active.is_some();
        // a sealed segment is never written again: its unsynced tail is synced now or never (a
        // `Flush` that is out may not have run yet)
        if (self.unsynced_since.is_some() || self.inflight > 0)
            && let Err(e) = self.sync_active()
        {
            return self.fail(format!("sync of the segment being sealed: {e}"));
        }
        let res: Result<()> = (|| {
            let spare = self.take_spare()?;
            let path = seg_path(&dir, id);
            std::fs::rename(&spare, &path)?;
            sync_dir(&dir)?;
            self.failpoint("roll:after_spare_rename", sealing)?;
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            file.write_all_at(&seg_header(id, generation, base), 0)?;
            file.sync_data()?;
            self.failpoint("roll:after_header", sealing)?;

            let mut m = self.manifest.clone();
            if let Some(a) = &self.active {
                let last = m.segs.last_mut().unwrap();
                last.sealed = true;
                last.end_offset = a.offset;
                last.end_lsn = base;
                last.max_time = a.max_time;
            }
            m.segs.push(MfSeg {
                id,
                generation,
                base_lsn: base,
                ..Default::default()
            });
            m.next_segment_id = id + 1;
            m.next_generation = generation + 1;
            self.write_manifest(&mut m)?;
            self.manifest = m;
            self.failpoint("roll:after_manifest", sealing)?;

            let seg = Arc::new(Seg {
                id,
                generation,
                base_lsn: base,
                file: file.try_clone()?,
                entries: RwLock::new(vec![]),
            });
            let capacity = file.metadata()?.len();
            let prev = self.active.replace(Active {
                seg: seg.clone(),
                file,
                offset: SEG_HEADER,
                capacity,
                max_time: 0,
            });
            self.shared.segs.write().unwrap().push(seg);
            if let Some(p) = prev {
                write_index(&dir, &p.seg)?;
            }
            self.failpoint("roll:after_index", sealing)?;
            Ok(())
        })();
        match res {
            Ok(()) => Ok(()),
            Err(Error::Fatal(e)) => Err(Error::Fatal(e)),
            Err(e) => self.fail(format!("segment roll: {e}")),
        }
    }

    /// Returns a segment file to the spare pool (or deletes it when the pool is full).
    fn recycle_file(&mut self, path: &Path) -> Result<()> {
        let pool_len = self.spares.pool.lock().unwrap().len();
        let size_ok = std::fs::metadata(path)
            .map(|m| m.len() == self.cfg.segment_size)
            .unwrap_or(false);
        if pool_len < self.cfg.spares.max(1) * 2 && size_ok {
            let n = self.spares.counter.fetch_add(1, Ordering::Relaxed);
            let to = self.cfg.dir.join(format!("spare-{n:08}.log"));
            std::fs::rename(path, &to)?;
            sync_dir(&self.cfg.dir)?;
            *self.spares.recycling_until.lock().unwrap() =
                Some(std::time::Instant::now() + RECYCLING);
            let mut pool = self.spares.pool.lock().unwrap();
            pool.push(to);
            self.spares.cv.notify_all();
            drop(pool);
            self.stats.recycled += 1;
        } else {
            std::fs::remove_file(path)?;
            sync_dir(&self.cfg.dir)?;
        }
        Ok(())
    }

    /// Drops the oldest sealed segments while the log holds more than `max_bytes` (total of
    /// segment bytes written), or while a segment's newest record is older than
    /// `min_time_ms`. The active segment is never dropped. Returns the new first LSN.
    pub fn retain(&mut self, max_bytes: Option<u64>, min_time_ms: Option<u64>) -> Result<Lsn> {
        if let Some(f) = &self.failed {
            return Err(Error::Fatal(f.clone()));
        }
        let active_bytes = self.active.as_ref().map_or(0, |a| a.offset);
        let mut total: u64 = self
            .manifest
            .segs
            .iter()
            .filter(|s| s.sealed)
            .map(|s| s.end_offset)
            .sum::<u64>()
            + active_bytes;
        let mut drop_n = 0;
        for s in self.manifest.segs.iter().filter(|s| s.sealed) {
            let by_size = max_bytes.is_some_and(|m| total > m);
            let by_age = min_time_ms.is_some_and(|t| s.max_time < t);
            if !(by_size || by_age) {
                break;
            }
            total -= s.end_offset;
            drop_n += 1;
        }
        if drop_n == 0 {
            return Ok(self.first_lsn());
        }
        let mut m = self.manifest.clone();
        let dropped: Vec<MfSeg> = m.segs.drain(..drop_n).collect();
        m.first_lsn = m.segs.first().map_or(self.next_lsn(), |s| s.base_lsn);
        if let Err(e) = self.write_manifest(&mut m) {
            return self.fail(format!("manifest write during retention: {e}"));
        }
        self.manifest = m;
        let floor = self.manifest.first_lsn;
        self.shared.first_lsn.store(floor, Ordering::Release);
        self.shared.segs.write().unwrap().drain(..drop_n);
        for s in dropped {
            let _ = std::fs::remove_file(idx_path(&self.cfg.dir, s.id));
            self.recycle_file(&seg_path(&self.cfg.dir, s.id))?;
        }
        Ok(floor)
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        {
            // under the pool lock: the preallocator checks `stop` under it before waiting
            let _g = self.spares.pool.lock().unwrap();
            self.spares.stop.store(true, Ordering::Release);
            self.spares.cv.notify_all();
        }
        if let Some(t) = self.prealloc.take() {
            let _ = t.join();
        }
    }
}

fn write_index(dir: &Path, seg: &Seg) -> Result<()> {
    let tmp = dir.join(format!("seg-{:016}.idx.tmp", seg.id));
    let mut f = File::create(&tmp)?;
    f.write_all(&encode_index(seg))?;
    f.sync_all()?;
    std::fs::rename(&tmp, idx_path(dir, seg.id))?;
    sync_dir(dir)?;
    Ok(())
}

/// Scans a segment's records. For a sealed segment `end` bounds the scan and any invalid
/// record before it is damage. For the active segment the scan stops at the first invalid
/// record and decides between a torn tail (truncate) and interior damage (error).
///
/// `unsynced`: with write caching, at most this many bytes at the end can have been unsynced at
/// a crash (`flush_bytes` + one group), and the kernel writes them back in any order. Damage
/// followed only by records within that bound is that tail, partly lost: it is dropped as a
/// whole (none of it was acknowledged on this node's fsync). Found by spec/storage.
fn scan(
    file: &File,
    generation: u64,
    base_lsn: Lsn,
    end: Option<u64>,
    unsynced: Option<u64>,
) -> Result<Scan> {
    let len = file.metadata()?.len();
    let limit = end.unwrap_or(len).min(len);
    let mut s = Scan {
        entries: vec![],
        end: SEG_HEADER,
        max_time: 0,
        damaged: false,
        dirty_end: 0,
        scanned: 0,
    };
    let mut w = Window {
        file,
        len,
        buf: vec![],
        start: SEG_HEADER,
        scanned: 0,
    };
    let mut off = SEG_HEADER;
    let mut group = SEG_HEADER;
    let mut stopped_on_written = false;
    while off < limit {
        let rel = w.cover_ahead(off, limit)?;
        match record::decode(&w.buf, rel, generation) {
            Decoded::Record {
                header, key, next, ..
            } => {
                let lsn_ok = header.lsn == base_lsn + s.entries.len() as u64;
                let group_ok = header.group_start == off || header.group_start == group;
                if !lsn_ok || !group_ok {
                    stopped_on_written = true;
                    break;
                }
                group = header.group_start;
                s.entries.push(IdxEntry {
                    offset: off,
                    len: header.len,
                    meta: meta_of(&header, key),
                });
                s.max_time = s.max_time.max(header.time_ms);
                off += (next - rel) as u64;
                s.end = off;
            }
            // Short: the record runs past the end of the file
            Decoded::Short => break,
            Decoded::Invalid => {
                stopped_on_written = w.written_at(rel, generation);
                break;
            }
        }
    }
    s.scanned = w.scanned;
    if let Some(e) = end {
        // sealed: the scan must reach the recorded end exactly
        s.damaged = s.end != e;
        return Ok(s);
    }
    if s.end >= limit {
        return Ok(s);
    }
    // The active segment stopped at x. Look for a later valid record of this generation within
    // the window a torn group can span: one of a later group means interior damage; ones of the
    // torn group itself mean the group reached disk in part.
    let x = s.end;
    let window_end = x.saturating_add(SEARCH_WINDOW).min(len);
    let mut later = None;
    let mut torn: Option<u64> = None;
    // a record of this generation was written at x but fails validation: damage, not a clean end
    let mut dirty = x + u64::from(stopped_on_written);
    let mut pos = x + 8;
    while pos < window_end {
        let rel = w.cover_ahead(pos, window_end)?;
        if w.written_at(rel, generation) {
            // zeroing a record's header is enough to make it unreadable for good
            dirty = dirty.max(pos + record::HEADER as u64);
            if let Decoded::Record { header, next, .. } = record::decode(&w.buf, rel, generation) {
                if header.group_start > x {
                    later = Some(header.group_start);
                    break;
                }
                torn = Some(torn.map_or(header.group_start, |t| t.min(header.group_start)));
                pos += (next - rel) as u64;
                continue;
            }
        }
        pos += 8;
    }
    // With write caching, a later group may be part of the unsynced tail: everything written
    // past x must then lie within the bound. `dirty` ends at or before the end of the furthest
    // record (a record is at least its header).
    let lost_tail = match (later, unsynced) {
        (Some(_), Some(u)) => {
            dirty = dirty.max(written_end(&mut w, pos, window_end, generation)?);
            dirty <= x.saturating_add(u)
        }
        _ => false,
    };
    s.scanned = w.scanned;
    if let Some(g) = later.filter(|_| !lost_tail) {
        return Err(Error::Corrupt(format!(
            "damage at offset {x} of the active segment is followed by a later group at offset {g}"
        )));
    }
    match torn.or(later.map(|_| x)) {
        Some(g) => {
            // drop the whole torn group (it was never acknowledged), and with write caching
            // the unsynced groups after it
            s.entries
                .truncate(s.entries.iter().take_while(|e| e.offset < g).count());
            s.end = g;
            s.max_time = s.entries.iter().map(|e| e.meta.time_ms).max().unwrap_or(0);
            s.damaged = true;
        }
        // nothing valid follows: a clean end, or a torn tail whose valid prefix is kept
        None => s.damaged = dirty > x,
    }
    if s.damaged {
        // zero at least one record header past the end, so a torn group's remains are gone
        s.dirty_end = dirty.max(x + record::HEADER as u64).min(len);
    }
    Ok(s)
}

/// The end of the header of the last record of this generation written in `[pos, end)`
/// (`pos` if none), skipping over the records that decode.
fn written_end(w: &mut Window<'_>, mut pos: u64, end: u64, generation: u64) -> Result<u64> {
    let mut last = pos;
    while pos < end {
        let rel = w.cover_ahead(pos, end)?;
        if w.written_at(rel, generation) {
            last = pos + record::HEADER as u64;
            if let Decoded::Record { next, .. } = record::decode(&w.buf, rel, generation) {
                pos += (next - rel) as u64;
                continue;
            }
        }
        pos += 8;
    }
    Ok(last)
}

/// How far past a torn write recovery looks for later records: a torn group spans at most
/// MAX_GROUP bytes, so a later group must start within this window.
const SEARCH_WINDOW: u64 = (2 * MAX_GROUP + record::MAX_RECORD) as u64;

/// A buffered view of a file region for scanning, read ahead in large chunks.
struct Window<'a> {
    file: &'a File,
    len: u64,
    buf: Vec<u8>,
    start: u64,
    scanned: u64,
}

impl Window<'_> {
    /// Makes the buffer hold `[from, to)` (reading ahead by at least 8 MiB, within the file)
    /// and returns `from`'s offset in it.
    fn cover(&mut self, from: u64, to: u64) -> Result<usize> {
        const CHUNK: u64 = 8 << 20;
        let end = self.start + self.buf.len() as u64;
        if from < self.start || from > end {
            self.buf.clear();
            self.start = from;
        } else if to > end && from - self.start >= CHUNK {
            // drop what was consumed only once it is large, so the shift is amortized
            self.buf.drain(..(from - self.start) as usize);
            self.start = from;
        }
        let have = self.start + self.buf.len() as u64;
        if to > have {
            let upto = to.max(have + CHUNK).min(self.len).max(to);
            let at = self.buf.len();
            self.buf.resize(at + (upto - have) as usize, 0);
            self.file.read_exact_at(&mut self.buf[at..], have)?;
            self.scanned += upto - have;
        }
        Ok((from - self.start) as usize)
    }

    /// Covers `[from, min(from + one maximal record, limit))`: enough to decode any record there.
    fn cover_ahead(&mut self, from: u64, limit: u64) -> Result<usize> {
        const MAX_SCAN: u64 = (record::MAX_RECORD + record::HEADER) as u64;
        self.cover(from, from.saturating_add(MAX_SCAN).min(limit))
    }

    /// Bytes at `rel` start with this generation's record magic: something was written there.
    fn written_at(&self, rel: usize, generation: u64) -> bool {
        let b = &self.buf[rel..];
        b.len() >= 24 && u32_at(b, 0) == record::MAGIC && u64_at(b, 16) == generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(lsn: Lsn, n: usize) -> Record {
        Record {
            lsn,
            meta: Meta::default(),
            payload: vec![0u8; n].into(),
        }
    }

    #[test]
    fn tail_cache_evicts_oldest_beyond_its_byte_budget() {
        let mut c = TailCache {
            cap: 80,
            ..Default::default()
        };
        c.push(rec(1, 40));
        c.push(rec(2, 40));
        assert_eq!(
            (c.recs.len(), c.bytes),
            (2, 80),
            "exactly at the budget: nothing evicted"
        );
        c.push(rec(3, 40));
        assert_eq!((c.recs.len(), c.bytes), (2, 80));
        assert!(c.get(1).is_none());
        assert_eq!(
            c.get(2).unwrap().lsn,
            2,
            "the oldest cached record is a hit"
        );
        assert_eq!(c.get(3).unwrap().lsn, 3);
        assert!(c.get(4).is_none());
    }

    fn mf(n: usize) -> Manifest {
        Manifest {
            seq: 9,
            next_generation: 5,
            next_segment_id: 7,
            first_lsn: 100,
            segs: (0..n as u64)
                .map(|i| MfSeg {
                    id: i,
                    generation: i + 1,
                    base_lsn: 100 + i * 10,
                    sealed: i + 1 < n as u64,
                    end_offset: 4096 + i,
                    end_lsn: 110 + i * 10,
                    max_time: i,
                })
                .collect(),
        }
    }

    #[test]
    fn manifest_roundtrip_and_bounds() {
        let m = mf(3);
        let b = m.encode().unwrap();
        assert_eq!(b.len(), MF_SLOT);
        assert_eq!(Manifest::decode(&b).unwrap(), m);
        // the slot's capacity: the largest manifest that fits encodes, one more segment does not
        let fits = (MF_SLOT - 24 - 28) / 49;
        assert!(mf(fits).encode().is_ok());
        assert!(matches!(mf(fits + 1).encode(), Err(Error::Invalid(_))));
        // too short, a length past the end, or a segment list cut short: rejected
        assert!(Manifest::decode(&b[..23]).is_none());
        assert!(Manifest::decode(&b[..24]).is_none());
        let mut long = b.clone();
        long[16..20].copy_from_slice(&((MF_SLOT - 23) as u32).to_le_bytes());
        assert!(Manifest::decode(&long).is_none());
        // a wrong magic or version is refused even under a valid checksum
        for (at, bad) in [(0, 0xdead_beef_u32), (4, 9)] {
            let mut v = b.clone();
            v[at..at + 4].copy_from_slice(&bad.to_le_bytes());
            let len = u32_at(&v, 16) as usize;
            let c = crc32c::crc32c_append(crc32c::crc32c(&v[..20]), &v[24..24 + len]);
            v[20..24].copy_from_slice(&c.to_le_bytes());
            assert!(Manifest::decode(&v).is_none(), "field at {at}");
        }
    }

    #[test]
    fn a_manifest_body_too_short_for_its_fields_is_refused_not_a_panic() {
        // Why: a slot is refused unless it decodes; a body shorter than its fixed fields under a
        // valid checksum (a torn or foreign write that happens to check) must read as invalid,
        // so the other slot is used. Found by the Kani proof `manifest_decode_never_panics`.
        for n in 0..28usize {
            let mut b = Vec::new();
            b.extend_from_slice(&MF_MAGIC.to_le_bytes());
            b.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
            b.extend_from_slice(&1u64.to_le_bytes());
            b.extend_from_slice(&(n as u32).to_le_bytes());
            let body = vec![0u8; n];
            let c = crc32c::crc32c_append(crc32c::crc32c(&b), &body);
            b.extend_from_slice(&c.to_le_bytes());
            b.extend_from_slice(&body);
            assert!(Manifest::decode(&b).is_none(), "body of {n} bytes");
        }
        // exactly the fixed fields: a manifest of no segments (a fresh log's first slots)
        assert_eq!(Manifest::decode(&mf(0).encode().unwrap()), Some(mf(0)));
    }

    fn header_with(magic: u32, version: u32, id: u64, generation: u64, base: Lsn) -> Vec<u8> {
        let mut b = vec![];
        b.extend_from_slice(&magic.to_le_bytes());
        b.extend_from_slice(&version.to_le_bytes());
        for v in [id, generation, base] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        let c = crc32c::crc32c(&b);
        b.extend_from_slice(&c.to_le_bytes());
        b.resize(SEG_HEADER as usize, 0);
        b
    }

    #[test]
    fn segment_headers_need_magic_version_and_checksum() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("h");
        let check = |b: &[u8]| {
            std::fs::write(&p, b).unwrap();
            read_seg_header(&File::open(&p).unwrap())
        };
        assert_eq!(check(&seg_header(3, 4, 5)), Some((3, 4, 5)));
        assert_eq!(
            check(&header_with(SEG_MAGIC, FORMAT_VERSION, 3, 4, 5)),
            Some((3, 4, 5))
        );
        // each check on its own, with a valid checksum
        assert_eq!(
            check(&header_with(SEG_MAGIC ^ 1, FORMAT_VERSION, 3, 4, 5)),
            None
        );
        assert_eq!(
            check(&header_with(SEG_MAGIC, FORMAT_VERSION + 1, 3, 4, 5)),
            None
        );
        let mut bad_crc = seg_header(3, 4, 5);
        bad_crc[32] ^= 1;
        assert_eq!(check(&bad_crc), None);
        assert_eq!(check(&[0u8; 10]), None);
    }

    #[test]
    fn index_files_roundtrip_and_reject_damage() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s");
        std::fs::write(&p, b"").unwrap();
        let entries: Vec<IdxEntry> = (0..5u64)
            .map(|i| IdxEntry {
                offset: 4096 + i * 100,
                len: 90,
                meta: Meta {
                    key: vec![b'k'; i as usize],
                    first_seq: i,
                    count: 2,
                    tag: 7,
                    time_ms: 9,
                },
            })
            .collect();
        let seg = Seg {
            id: 3,
            generation: 4,
            base_lsn: 50,
            file: File::open(&p).unwrap(),
            entries: RwLock::new(entries.clone()),
        };
        let b = encode_index(&seg);
        let got = decode_index(&b, 3, 4, 50).unwrap();
        assert_eq!(got.len(), 5);
        for (a, e) in got.iter().zip(&entries) {
            assert_eq!((a.offset, a.len, &a.meta), (e.offset, e.len, &e.meta));
        }
        // another segment's identity, any flipped byte, or a cut: rejected
        assert!(decode_index(&b, 3, 5, 50).is_none());
        assert!(decode_index(&b, 2, 4, 50).is_none());
        assert!(decode_index(&b, 3, 4, 51).is_none());
        for i in 0..b.len() {
            let mut v = b.clone();
            v[i] ^= 0x10;
            assert!(decode_index(&v, 3, 4, 50).is_none(), "flip at {i}");
        }
        for cut in [0, 10, 39, b.len() - 1] {
            assert!(decode_index(&b[..cut], 3, 4, 50).is_none(), "cut at {cut}");
        }
        // a wrong magic or version is refused even under a valid checksum
        for (at, bad) in [(0, 0xdead_beef_u32), (4, 9)] {
            let mut v = b[..b.len() - 4].to_vec();
            v[at..at + 4].copy_from_slice(&bad.to_le_bytes());
            let c = crc32c::crc32c(&v);
            v.extend_from_slice(&c.to_le_bytes());
            assert!(decode_index(&v, 3, 4, 50).is_none(), "field at {at}");
        }
        // a valid checksum over a body that lies about its entry count
        let mut lie = b[..b.len() - 4].to_vec();
        lie[32..36].copy_from_slice(&6u32.to_le_bytes());
        let c = crc32c::crc32c(&lie);
        lie.extend_from_slice(&c.to_le_bytes());
        assert!(decode_index(&lie, 3, 4, 50).is_none());
        let mut short = b[..b.len() - 4].to_vec();
        short[32..36].copy_from_slice(&4u32.to_le_bytes());
        let c = crc32c::crc32c(&short);
        short.extend_from_slice(&c.to_le_bytes());
        assert!(
            decode_index(&short, 3, 4, 50).is_none(),
            "trailing bytes after the entries"
        );
    }

    #[test]
    fn a_zero_budget_cache_holds_nothing() {
        let mut c = TailCache::default();
        c.push(rec(1, 10));
        assert!(c.get(1).is_none());
    }
}

/// Bounded proofs (Kani): the decoders of the files recovery reads never panic, whatever the
/// bytes. CRC-32C is stubbed with an unconstrained value, so every checksum may or may not
/// match: the proofs cover torn or foreign writes that happen to check.
#[cfg(kani)]
mod proofs {
    use super::*;

    fn crc(_b: &[u8]) -> u32 {
        kani::any()
    }
    fn crc_append(_c: u32, _b: &[u8]) -> u32 {
        kani::any()
    }

    #[kani::proof]
    #[kani::stub(crc32c::crc32c, crc)]
    #[kani::stub(crc32c::crc32c_append, crc_append)]
    #[kani::unwind(3)]
    fn manifest_decode_never_panics() {
        let b: [u8; 24 + 28 + 49] = kani::any();
        let n: usize = kani::any();
        kani::assume(n <= b.len());
        let _ = Manifest::decode(&b[..n]);
    }

    /// An index file never panics the decoder, whatever its bytes and length (up to one entry
    /// with an 8-byte key, or two with empty keys), and an accepted one has exactly the entries
    /// it declares.
    #[kani::proof]
    #[kani::stub(crc32c::crc32c, crc)]
    #[kani::unwind(3)]
    fn index_decode_never_panics() {
        let b: [u8; 36 + 2 * 38 + 4] = kani::any();
        let n: usize = kani::any();
        kani::assume(n <= b.len());
        if let Some(v) = decode_index(&b[..n], kani::any(), kani::any(), kani::any()) {
            assert_eq!(v.len(), u32_at(&b, 32) as usize);
        }
    }
}
