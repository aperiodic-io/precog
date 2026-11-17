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
//! Durability: `append` returns after `fdatasync`. Any I/O error on the write path is fatal
//! (fsyncgate): the log refuses further writes and the caller must restart and recover.

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
            failpoints: Failpoints::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub segments: usize,
    pub bytes: u64,
    pub recovery_scanned_bytes: u64,
    pub spare_hits: u64,
    pub roll_stalls: u64,
    pub recycled: u64,
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
        if let Some(last) = self.recs.back()
            && last.lsn + 1 != r.lsn
        {
            self.clear();
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
        self.recs.get((lsn - first) as usize).cloned()
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
        if lsn >= s.next_lsn.load(Ordering::Acquire) || lsn < s.first_lsn.load(Ordering::Acquire) {
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
            _ if lsn < s.first_lsn.load(Ordering::Acquire) => Ok(None),
            _ => Err(Error::Corrupt(format!(
                "segment {} offset {}: record {lsn} fails validation",
                seg.id, e.offset
            ))),
        }
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
            while lsn < seg_end && lsn < end && out.len() < limit {
                let e = &ents[(lsn - seg.base_lsn) as usize];
                out.push(Entry {
                    lsn,
                    segment: seg.id,
                    offset: e.offset,
                    meta: e.meta.clone(),
                });
                lsn += 1;
            }
            if out.len() >= limit || lsn >= end {
                break;
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
        if b.len() < 24 || u32_at(b, 0) != MF_MAGIC {
            return None;
        }
        if u32_at(b, 4) != FORMAT_VERSION {
            return None;
        }
        let len = u32_at(b, 16) as usize;
        if 24 + len > b.len() {
            return None;
        }
        let body = &b[24..24 + len];
        if crc32c::crc32c_append(crc32c::crc32c(&b[..20]), body) != u32_at(b, 20) {
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
    if b.len() < 40 || crc32c::crc32c(&b[..b.len() - 4]) != u32_at(b, b.len() - 4) {
        return None;
    }
    if u32_at(b, 0) != IDX_MAGIC || u32_at(b, 4) != FORMAT_VERSION {
        return None;
    }
    if (u64_at(b, 8), u64_at(b, 16), u64_at(b, 24)) != (id, generation, base_lsn) {
        return None;
    }
    let n = u32_at(b, 32) as usize;
    let mut o = 36;
    let end = b.len() - 4;
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        if o + 38 > end {
            return None;
        }
        let kl = u16::from_le_bytes(b[o + 36..o + 38].try_into().unwrap()) as usize;
        if o + 38 + kl > end {
            return None;
        }
        v.push(IdxEntry {
            offset: u64_at(b, o),
            len: u32_at(b, o + 8),
            meta: Meta {
                first_seq: u64_at(b, o + 12),
                count: u32_at(b, o + 20),
                tag: u32_at(b, o + 24),
                time_ms: u64_at(b, o + 28),
                key: b[o + 38..o + 38 + kl].to_vec(),
            },
        });
        o += 38 + kl;
    }
    (o == end).then_some(v)
}

// ---------------------------------------------------------------------------
// spare preallocation

struct Spares {
    pool: Mutex<Vec<PathBuf>>,
    cv: Condvar,
    stop: AtomicBool,
    target: usize,
    size: u64,
    dir: PathBuf,
    counter: AtomicU64,
}

impl Spares {
    /// Creates one zero-filled spare file, durably named.
    fn make(&self) -> std::io::Result<PathBuf> {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let tmp = self.dir.join(format!("spare-tmp-{n}"));
        let path = self.dir.join(format!("spare-{n:08}.log"));
        let mut f = File::create(&tmp)?;
        let zeros = vec![0u8; 1 << 20];
        let mut left = self.size;
        while left > 0 {
            let k = left.min(zeros.len() as u64) as usize;
            f.write_all(&zeros[..k])?;
            left -= k as u64;
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
            }
            if self.stop.load(Ordering::Acquire) {
                return;
            }
            match self.make() {
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
            if name.starts_with("spare-tmp-") {
                std::fs::remove_file(e.path())?;
            } else if let Some(n) = name
                .strip_prefix("spare-")
                .and_then(|s| s.strip_suffix(".log"))
            {
                counter = counter.max(n.trim_start_matches('r').parse::<u64>().unwrap_or(0) + 1);
                if e.metadata()?.len() == cfg.segment_size {
                    pool.push(e.path());
                } else {
                    std::fs::remove_file(e.path())?;
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
            dir: dir.clone(),
            counter: AtomicU64::new(counter),
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
        };

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
                let entries = match std::fs::read(idx_path(&dir, m.id))
                    .ok()
                    .and_then(|b| decode_index(&b, m.id, m.generation, m.base_lsn))
                {
                    Some(e) if m.base_lsn + e.len() as u64 == m.end_lsn => e,
                    _ => {
                        let s = scan(&file, m.generation, m.base_lsn, Some(m.end_offset))?;
                        if s.damaged || m.base_lsn + s.entries.len() as u64 != m.end_lsn {
                            return Err(Error::Corrupt(format!(
                                "sealed segment {} is damaged",
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
                if !idx_path(&dir, m.id).exists()
                    || std::fs::read(idx_path(&dir, m.id))
                        .ok()
                        .and_then(|b| decode_index(&b, m.id, m.generation, m.base_lsn))
                        .is_none()
                {
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
            let s = scan(&seg.file, seg.generation, seg.base_lsn, None)?;
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
                    let s = scan(&file, g, b, None)?;
                    log.stats.recovery_scanned_bytes += s.scanned;
                    if let Some((prev, ps)) = active.take() {
                        if ps.damaged && !s.entries.is_empty() {
                            return Err(Error::Corrupt(format!(
                                "segment {} is damaged at offset {} and a later segment {} holds records",
                                prev.id, ps.end, id
                            )));
                        }
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
            if s.dirty_end > s.end {
                // zero the torn group's leftovers so a shorter write can never be followed by them
                let zeros = vec![0u8; (s.dirty_end - s.end) as usize];
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

    pub fn stats(&self) -> Stats {
        let mut s = self.stats.clone();
        s.cache_hits = self.shared.cache_hits.load(Ordering::Relaxed);
        s.segments = self.shared.segs.read().unwrap().len();
        s
    }

    fn fail<T>(&mut self, what: impl Into<String>) -> Result<T> {
        let w = what.into();
        self.failed = Some(w.clone());
        Err(Error::Fatal(w))
    }

    fn failpoint(&mut self, step: &'static str) -> Result<()> {
        // only rolls that seal a segment, so the crash leaves data behind to recover
        if self.cfg.failpoints.fail_at == Some(step) && self.manifest.segs.iter().any(|s| !s.sealed)
        {
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
            if let Some(n) = self.cfg.failpoints.fail_data_sync_after
                && self.stats.syncs >= n
            {
                return Err(std::io::Error::other("failpoint: data sync"));
            }
            a.file.sync_data()
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
        self.stats.syncs += 1;
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
        let p = self.spares.pool.lock().unwrap().pop();
        self.spares.cv.notify_all();
        Ok(match p {
            Some(p) => {
                self.stats.spare_hits += 1;
                p
            }
            None => {
                self.stats.roll_stalls += 1;
                self.spares.make()?
            }
        })
    }

    /// Seals the active segment (if any) and starts a new one from a spare.
    fn roll(&mut self) -> Result<()> {
        let dir = self.cfg.dir.clone();
        let id = self.manifest.next_segment_id;
        let generation = self.manifest.next_generation;
        let base = self.next_lsn();
        let res: Result<()> = (|| {
            let spare = self.take_spare()?;
            let path = seg_path(&dir, id);
            std::fs::rename(&spare, &path)?;
            sync_dir(&dir)?;
            self.failpoint("roll:after_spare_rename")?;
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            file.write_all_at(&seg_header(id, generation, base), 0)?;
            file.sync_data()?;
            self.failpoint("roll:after_header")?;

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
            self.failpoint("roll:after_manifest")?;

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
            self.failpoint("roll:after_index")?;
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
            self.spares.pool.lock().unwrap().push(to);
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
        self.spares.stop.store(true, Ordering::Release);
        self.spares.cv.notify_all();
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
fn scan(file: &File, generation: u64, base_lsn: Lsn, end: Option<u64>) -> Result<Scan> {
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
    let mut buf: Vec<u8> = Vec::new();
    let mut buf_start = SEG_HEADER;
    let chunk = 8 << 20;
    let mut off = SEG_HEADER;
    let mut group = SEG_HEADER;
    let fill = |buf: &mut Vec<u8>,
                buf_start: &mut u64,
                off: u64,
                need: usize,
                scanned: &mut u64|
     -> Result<bool> {
        // keep [off..] and read more
        let rel = (off - *buf_start) as usize;
        buf.drain(..rel.min(buf.len()));
        *buf_start = off;
        let have_to = *buf_start + buf.len() as u64;
        if have_to >= limit {
            return Ok(false);
        }
        let want = (need.max(chunk) as u64).min(limit - have_to) as usize;
        let at = buf.len();
        buf.resize(at + want, 0);
        file.read_exact_at(&mut buf[at..], have_to)?;
        *scanned += want as u64;
        Ok(true)
    };
    loop {
        if off >= limit {
            break;
        }
        let rel = (off - buf_start) as usize;
        let d = record::decode(&buf[..], rel.min(buf.len()), generation);
        match d {
            Decoded::Short => {
                if !fill(
                    &mut buf,
                    &mut buf_start,
                    off,
                    record::MAX_RECORD + record::HEADER,
                    &mut s.scanned,
                )? {
                    break; // ends inside a record at the end of the file: torn
                }
                continue;
            }
            Decoded::Record {
                header, key, next, ..
            } => {
                let lsn_ok = header.lsn == base_lsn + s.entries.len() as u64;
                let group_ok = header.group_start == off || header.group_start == group;
                if !lsn_ok || !group_ok {
                    break;
                }
                group = header.group_start;
                s.entries.push(IdxEntry {
                    offset: off,
                    len: header.len,
                    meta: meta_of(&header, key),
                });
                s.max_time = s.max_time.max(header.time_ms);
                off = buf_start + next as u64;
                s.end = off;
            }
            Decoded::Invalid => break,
        }
    }
    if let Some(e) = end {
        // sealed: the scan must reach the recorded end exactly
        s.damaged = s.end != e;
        return Ok(s);
    }
    if s.end >= limit {
        return Ok(s);
    }
    // Active segment stopped at s.end. Look for a later valid record of this generation within
    // the window a torn group can span.
    let x = s.end;
    let window_end = (x + 2 * MAX_GROUP as u64 + record::MAX_RECORD as u64).min(len);
    let mut pos = x + 8;
    let mut later: Option<u64> = None; // group_start of a valid record from a later group
    let mut torn: Option<u64> = None; // group_start of valid records of the torn group itself
    let mut dirty = x;
    // continue in the buffer the main scan already read (no second read of the same bytes)
    while pos < window_end {
        let rel = (pos - buf_start) as usize;
        if rel + record::HEADER > buf.len() {
            let keep = (pos - buf_start) as usize;
            buf.drain(..keep.min(buf.len()));
            buf_start = pos;
            let have_to = buf_start + buf.len() as u64;
            if have_to >= window_end {
                break;
            }
            let want = (chunk as u64).min(window_end - have_to) as usize;
            let at = buf.len();
            buf.resize(at + want, 0);
            file.read_exact_at(&mut buf[at..], have_to)?;
            s.scanned += want as u64;
            continue;
        }
        let b = &buf[rel..];
        if u32_at(b, 0) == record::MAGIC && u64_at(b, 16) == generation {
            let rec_len = u32_at(b, 4) as u64;
            dirty = dirty.max(pos + rec_len.min(record::MAX_RECORD as u64));
            // validate fully (may need more bytes)
            let need = rec_len as usize;
            if rel + need > buf.len() && pos + rec_len <= window_end {
                let at = buf.len();
                let more = rel + need - at;
                buf.resize(at + more, 0);
                file.read_exact_at(&mut buf[at..], buf_start + at as u64)?;
                s.scanned += more as u64;
            }
            if let Decoded::Record { header, next, .. } = record::decode(&buf, rel, generation) {
                if header.group_start > x {
                    later = Some(header.group_start);
                    break;
                }
                torn = Some(torn.map_or(header.group_start, |t: u64| t.min(header.group_start)));
                pos = buf_start + next as u64;
                continue;
            }
        }
        pos += 8;
    }
    if let Some(g) = later {
        return Err(Error::Corrupt(format!(
            "damage at offset {x} of the active segment is followed by a later group at offset {g}"
        )));
    }
    match torn {
        Some(g) => {
            // the torn group itself reached disk in part: drop the whole group
            s.damaged = true;
            let keep = s.entries.iter().take_while(|e| e.offset < g).count();
            s.entries.truncate(keep);
            s.end = g.max(SEG_HEADER);
            s.max_time = s.entries.iter().map(|e| e.meta.time_ms).max().unwrap_or(0);
        }
        None => {
            // nothing valid follows: a clean end, or a torn tail with its valid prefix kept
            s.damaged = dirty > x;
        }
    }
    s.dirty_end = dirty.max(if s.damaged { x } else { 0 });
    // zero at least the first record slot after the end so a torn group's remains are gone
    if s.damaged {
        s.dirty_end = s.dirty_end.max(x + record::HEADER as u64).min(len);
    }
    Ok(s)
}
