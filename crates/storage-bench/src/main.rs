//! Durable group-commit append benchmark across Rust storage options.
//!
//! Every backend sits behind the same front end, modelled on precog's WAL:
//! P producer threads each append one batch (value of `--size` bytes, key
//! (log=producer, seq)) and wait until it is durable; one writer thread drains
//! every queued append into one `append_group` call that must return only
//! when the whole group is on stable storage. The only variable is the
//! backend.
//!
//! Phases: write (throughput, ack latency, CPU, bytes written to the device),
//! read (random point gets, full scan; page cache warm), reopen (time to open
//! and be able to serve reads again, incl. any index rebuild / replay).

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossbeam_channel::{Receiver, Sender, bounded};
use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

type Key = [u8; 16];

fn key(log: u64, seq: u64) -> Key {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&log.to_be_bytes());
    k[8..].copy_from_slice(&seq.to_be_bytes());
    k
}

trait Store: Send {
    /// Durable on return.
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()>;
    /// Length of the value, if present.
    fn get(&mut self, k: &Key) -> Result<Option<usize>>;
    /// (records, bytes) over everything.
    fn scan(&mut self) -> Result<(u64, u64)>;
}

// ---------------------------------------------------------------------------
// Raw-file backends: a record log + in-memory index (precog's current design)
// record := u32 len | key[16] | u32 crc32(value) | u32 generation | value
// (generation guards against stale-but-valid records in recycled segments)
// ---------------------------------------------------------------------------

const SEG_SIZE: u64 = 1 << 30;
const HDR: usize = 28; // u32 len | key[16] | u32 crc | u32 generation

#[derive(Clone, Copy, PartialEq, Debug)]
enum RawMode {
    /// O_APPEND file, write + fdatasync (precog today).
    Append,
    /// pwrite into pre-written (recycled) segments, fdatasync.
    Prealloc,
    /// pwrite into fallocate()d segments (unwritten extents), fdatasync.
    Falloc,
    /// O_DIRECT | O_DSYNC pwrite into pre-written segments, tail block rewrite.
    Direct,
    /// io_uring linked WRITE + FSYNC(DATASYNC) into pre-written segments.
    Uring,
}

struct RawStore {
    mode: RawMode,
    dir: PathBuf,
    pool: PathBuf,
    segs: Vec<File>,
    off: u64,
    index: HashMap<Key, (u32, u64, u32)>,
    buf: Vec<u8>,
    // O_DIRECT: aligned staging buffer and current partial tail block
    abuf: AlignedBuf,
    tail: Vec<u8>,
    ring: Option<io_uring::IoUring>,
    generation: u32,
    readers: Vec<File>,
}

struct AlignedBuf {
    ptr: *mut u8,
    cap: usize,
}
unsafe impl Send for AlignedBuf {}
impl AlignedBuf {
    fn new(cap: usize) -> AlignedBuf {
        let layout = std::alloc::Layout::from_size_align(cap, 4096).unwrap();
        AlignedBuf {
            ptr: unsafe { std::alloc::alloc_zeroed(layout) },
            cap,
        }
    }
    fn slice(&mut self, n: usize) -> &mut [u8] {
        assert!(n <= self.cap);
        unsafe { std::slice::from_raw_parts_mut(self.ptr, n) }
    }
}
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(
                self.ptr,
                std::alloc::Layout::from_size_align(self.cap, 4096).unwrap(),
            )
        }
    }
}

/// Pre-written segment files reused across runs ("recycled segments").
fn pool_segment(pool: &Path, i: usize) -> Result<PathBuf> {
    std::fs::create_dir_all(pool)?;
    let p = pool.join(format!("seg-{i:04}"));
    let ok = std::fs::metadata(&p)
        .map(|m| m.len() == SEG_SIZE)
        .unwrap_or(false);
    if !ok {
        let mut f = File::create(&p)?;
        let z = vec![0u8; 8 << 20];
        for _ in 0..(SEG_SIZE / z.len() as u64) {
            f.write_all(&z)?;
        }
        f.sync_all()?;
        File::open(pool)?.sync_all()?;
    }
    Ok(p)
}

impl RawStore {
    fn open(mode: RawMode, dir: &Path, pool: &Path) -> Result<RawStore> {
        std::fs::create_dir_all(dir)?;
        let ring = if mode == RawMode::Uring {
            Some(io_uring::IoUring::new(64)?)
        } else {
            None
        };
        let mut s = RawStore {
            mode,
            dir: dir.into(),
            pool: pool.into(),
            segs: vec![],
            off: 0,
            index: HashMap::new(),
            buf: Vec::with_capacity(32 << 20),
            abuf: AlignedBuf::new(48 << 20),
            tail: vec![],
            ring,
            generation: std::fs::read_to_string(dir.join("GENERATION"))
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or_else(|| {
                    let g = rand::random::<u32>() | 1;
                    let _ = std::fs::write(dir.join("GENERATION"), g.to_string());
                    g
                }),
            readers: vec![],
        };
        s.add_segment()?;
        Ok(s)
    }

    fn add_segment(&mut self) -> Result<()> {
        let i = self.segs.len();
        let f = match self.mode {
            RawMode::Append => OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(self.dir.join(format!("seg-{i:04}")))?,
            RawMode::Falloc => {
                let path = self.dir.join(format!("seg-{i:04}"));
                let existed = path.exists();
                let f = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)?;
                if existed {
                    self.segs.push(f.try_clone()?);
                    self.readers.push(f);
                    self.off = 0;
                    return Ok(()); // reopen: keep existing data (replay rebuilds the index)
                }
                let r = unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, SEG_SIZE as i64) };
                if r != 0 {
                    bail!("fallocate: {}", std::io::Error::last_os_error());
                }
                f.sync_all()?;
                f
            }
            RawMode::Prealloc | RawMode::Uring => OpenOptions::new()
                .read(true)
                .write(true)
                .open(pool_segment(&self.pool, i)?)?,
            RawMode::Direct => OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
                .open(pool_segment(&self.pool, i)?)?,
        };
        let rf = match self.mode {
            RawMode::Direct => File::open(pool_segment(&self.pool, i)?)?,
            _ => f.try_clone()?,
        };
        self.readers.push(rf);
        self.segs.push(f);
        self.off = 0;
        self.tail.clear();
        Ok(())
    }

    fn write_group(&mut self) -> Result<()> {
        let seg = self.segs.last().unwrap();
        match self.mode {
            RawMode::Append => {
                (&*seg).write_all(&self.buf)?;
                seg.sync_data()?;
            }
            RawMode::Prealloc | RawMode::Falloc => {
                seg.write_all_at(&self.buf, self.off)?;
                seg.sync_data()?;
            }
            RawMode::Direct => {
                // Rewrite the partial tail block together with the new bytes,
                // padded to a 4 KiB multiple; O_DSYNC makes the write durable.
                let start = self.off & !4095;
                let total = self.tail.len() + self.buf.len();
                let padded = total.div_ceil(4096) * 4096;
                let tl = self.tail.len();
                let a = self.abuf.slice(padded);
                a[..tl].copy_from_slice(&self.tail);
                a[tl..total].copy_from_slice(&self.buf);
                a[total..].fill(0);
                seg.write_all_at(a, start)?;
                let new_end = self.off + self.buf.len() as u64;
                let keep = (new_end & 4095) as usize;
                let from = total - keep;
                self.tail.clear();
                self.tail
                    .extend_from_slice(&self.abuf.slice(padded)[from..total]);
            }
            RawMode::Uring => {
                use io_uring::{opcode, squeue, types};
                let ring = self.ring.as_mut().unwrap();
                let fd = types::Fd(seg.as_raw_fd());
                let w = opcode::Write::new(fd, self.buf.as_ptr(), self.buf.len() as u32)
                    .offset(self.off)
                    .build()
                    .flags(squeue::Flags::IO_LINK)
                    .user_data(1);
                let s = opcode::Fsync::new(fd)
                    .flags(types::FsyncFlags::DATASYNC)
                    .build()
                    .user_data(2);
                unsafe {
                    let mut sq = ring.submission();
                    sq.push(&w).map_err(|_| anyhow::anyhow!("sq full"))?;
                    sq.push(&s).map_err(|_| anyhow::anyhow!("sq full"))?;
                }
                ring.submit_and_wait(2)?;
                for c in ring.completion() {
                    if c.result() < 0 {
                        bail!(
                            "io_uring op {} failed: {}",
                            c.user_data(),
                            std::io::Error::from_raw_os_error(-c.result())
                        );
                    }
                    if c.user_data() == 1 && c.result() as usize != self.buf.len() {
                        bail!("short io_uring write");
                    }
                }
            }
        }
        Ok(())
    }

    fn rebuild(&mut self) -> Result<()> {
        // Replay every segment to rebuild the index (what a WAL-only design does at start).
        self.index.clear();
        for (si, f) in self.segs.iter().enumerate() {
            // Read in 8 MiB chunks so an unused recycled segment costs one read.
            let mut data = Vec::new();
            let mut r = f.try_clone()?;
            use std::io::Seek;
            r.seek(std::io::SeekFrom::Start(0))?;
            let mut chunk = vec![0u8; 8 << 20];
            let mut o = 0usize;
            let mut found_any = false;
            loop {
                let n = r.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&chunk[..n]);
                let mut stop = false;
                while o + HDR <= data.len() {
                    let len = u32::from_le_bytes(data[o..o + 4].try_into().unwrap()) as usize;
                    let gen_ = u32::from_le_bytes(data[o + 24..o + 28].try_into().unwrap());
                    if len == 0 || gen_ != self.generation || len > 64 << 20 {
                        stop = true;
                        break;
                    }
                    if o + HDR + len > data.len() {
                        break; // need more bytes
                    }
                    let k: Key = data[o + 4..o + 20].try_into().unwrap();
                    let crc = u32::from_le_bytes(data[o + 20..o + 24].try_into().unwrap());
                    if crc32(&data[o + HDR..o + HDR + len]) != crc {
                        stop = true;
                        break;
                    }
                    self.index
                        .insert(k, (si as u32, (o + HDR) as u64, len as u32));
                    found_any = true;
                    o += HDR + len;
                }
                if stop {
                    break;
                }
            }
            if !found_any {
                break;
            }
            let _ = &data;
            continue;
            #[allow(unreachable_code)]
            while o + HDR <= data.len() {
                let len = u32::from_le_bytes(data[o..o + 4].try_into().unwrap()) as usize;
                if len == 0 || o + HDR + len > data.len() {
                    break;
                }
                let k: Key = data[o + 4..o + 20].try_into().unwrap();
                let crc = u32::from_le_bytes(data[o + 20..o + 24].try_into().unwrap());
                if crc32(&data[o + HDR..o + HDR + len]) != crc {
                    break;
                }
                self.index
                    .insert(k, (si as u32, (o + HDR) as u64, len as u32));
                o += HDR + len;
            }
        }
        Ok(())
    }
}

/// Reads one byte per 4 KiB so memory-mapped engines actually fault pages in.
fn touch(v: &[u8]) -> u64 {
    let mut x = 0u8;
    for i in (0..v.len()).step_by(4096) {
        x ^= unsafe { std::ptr::read_volatile(&v[i]) };
    }
    std::hint::black_box(x);
    v.len() as u64
}

fn crc32(b: &[u8]) -> u32 {
    // cheap non-cryptographic checksum (FNV-1a folded) – same cost class as crc32c
    let mut h: u32 = 0x811c9dc5;
    for c in b.chunks(8) {
        let mut v = 0u64;
        for (i, x) in c.iter().enumerate() {
            v |= (*x as u64) << (i * 8);
        }
        h = (h ^ (v as u32) ^ ((v >> 32) as u32)).wrapping_mul(0x01000193);
    }
    h
}

impl Store for RawStore {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let size: usize = items.iter().map(|(_, v)| HDR + v.len()).sum();
        if self.off + size as u64 > SEG_SIZE {
            self.add_segment()?;
        }
        self.buf.clear();
        let si = (self.segs.len() - 1) as u32;
        let mut pos = self.off;
        for (k, v) in items {
            self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            self.buf.extend_from_slice(k);
            self.buf.extend_from_slice(&crc32(v).to_le_bytes());
            self.buf.extend_from_slice(&self.generation.to_le_bytes());
            self.buf.extend_from_slice(v);
            self.index
                .insert(*k, (si, pos + HDR as u64, v.len() as u32));
            pos += (HDR + v.len()) as u64;
        }
        self.write_group()?;
        self.off = pos;
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        let Some(&(s, o, l)) = self.index.get(k) else {
            return Ok(None);
        };
        let mut v = vec![0u8; l as usize];
        self.readers[s as usize].read_exact_at(&mut v, o)?;
        Ok(Some(v.len()))
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        let (mut n, mut b) = (0, 0);
        let mut v = vec![0u8; 1 << 20];
        let mut locs: Vec<_> = self.index.values().copied().collect();
        locs.sort();
        let ro = &self.readers;
        for (s, o, l) in locs {
            if v.len() < l as usize {
                v.resize(l as usize, 0);
            }
            ro[s as usize].read_exact_at(&mut v[..l as usize], o)?;
            n += 1;
            b += touch(&v[..l as usize]);
        }
        Ok((n, b))
    }
}

// ---------------------------------------------------------------------------
// Embedded engines
// ---------------------------------------------------------------------------

struct Rocks {
    db: rocksdb::DB,
}
impl Rocks {
    fn open(dir: &Path, blob: bool) -> Result<Rocks> {
        let mut o = rocksdb::Options::default();
        o.create_if_missing(true);
        o.set_compression_type(rocksdb::DBCompressionType::None);
        o.set_write_buffer_size(64 << 20);
        o.increase_parallelism(2);
        if blob {
            o.set_enable_blob_files(true);
            o.set_min_blob_size(1024);
            o.set_blob_compression_type(rocksdb::DBCompressionType::None);
        }
        Ok(Rocks {
            db: rocksdb::DB::open(&o, dir)?,
        })
    }
}
impl Store for Rocks {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let mut b = rocksdb::WriteBatch::default();
        for (k, v) in items {
            b.put(k, v);
        }
        let mut w = rocksdb::WriteOptions::default();
        w.set_sync(true);
        self.db.write_opt(b, &w)?;
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        Ok(self.db.get_pinned(k)?.map(|v| v.len()))
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        let (mut n, mut b) = (0, 0);
        for kv in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (_, v) = kv?;
            n += 1;
            b += touch(&v);
        }
        Ok((n, b))
    }
}

struct Fjall {
    db: fjall::Database,
    ks: fjall::Keyspace,
}
impl Fjall {
    fn open(dir: &Path, kv_sep: bool) -> Result<Fjall> {
        let db = fjall::Database::builder(dir).open()?;
        let ks = db.keyspace("data", || {
            let o = fjall::KeyspaceCreateOptions::default().max_memtable_size(64 << 20);
            if kv_sep {
                o.with_kv_separation(Some(fjall::KvSeparationOptions::default()))
            } else {
                o
            }
        })?;
        Ok(Fjall { db, ks })
    }
}
impl Store for Fjall {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let mut b = self.db.batch();
        for (k, v) in items {
            b.insert(&self.ks, k.to_vec(), v.clone());
        }
        b.commit()?;
        self.db.persist(fjall::PersistMode::SyncData)?;
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        Ok(self.ks.get(k)?.map(|v| v.len()))
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        let (mut n, mut b) = (0, 0);
        for kv in self.ks.iter() {
            let v = kv.value()?;
            n += 1;
            b += touch(&v);
        }
        Ok((n, b))
    }
}

const RTABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("data");
struct Redb {
    db: redb::Database,
}
impl Store for Redb {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let tx = self.db.begin_write()?;
        {
            let mut t = tx.open_table(RTABLE)?;
            for (k, v) in items {
                t.insert(&k[..], &v[..])?;
            }
        }
        tx.commit()?; // Durability::Immediate (default)
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        use redb::ReadableDatabase;
        let tx = self.db.begin_read()?;
        let t = tx.open_table(RTABLE)?;
        Ok(t.get(&k[..])?.map(|v| v.value().len()))
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        use redb::{ReadableDatabase, ReadableTable};
        let tx = self.db.begin_read()?;
        let t = tx.open_table(RTABLE)?;
        let (mut n, mut b) = (0, 0);
        for kv in t.iter()? {
            let (_, v) = kv?;
            n += 1;
            b += touch(v.value());
        }
        Ok((n, b))
    }
}

struct Lmdb {
    // field order: db (just a dbi handle) is dropped before env
    db: heed::Database<'static, heed::types::Bytes, heed::types::Bytes>,
    env: heed::Env,
}
impl Lmdb {
    fn open(dir: &Path) -> Result<Lmdb> {
        std::fs::create_dir_all(dir)?;
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(64 << 30)
                .max_dbs(1)
                .open(dir)?
        };
        let w = env.write_txn()?;
        let db = env
            .create_databases_and_commit(w, |wdo, wtxn| {
                let d: heed::Database<heed::types::Bytes, heed::types::Bytes> =
                    wdo.create_database(wtxn, Some("data"))?;
                // The handle is only a dbi number; extend its borrow of env to 'static.
                let d: heed::Database<'static, heed::types::Bytes, heed::types::Bytes> =
                    unsafe { std::mem::transmute(d) };
                heed::Result::Ok(heed::CommitOrAbort::<_, ()>::Commit(d))
            })?
            .unwrap_commit();
        Ok(Lmdb { db, env })
    }
}
impl Store for Lmdb {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let mut w = self.env.write_txn()?;
        for (k, v) in items {
            self.db.put(&mut w, k, v)?;
        }
        w.commit()?; // synchronous by default (fdatasync of data + meta)
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        let r = self.env.read_txn()?;
        Ok(self.db.get(&r, k)?.map(|v| v.len()))
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        let r = self.env.read_txn()?;
        let (mut n, mut b) = (0, 0);
        for kv in self.db.iter(&r)? {
            let (_, v) = kv?;
            n += 1;
            b += touch(&v);
        }
        Ok((n, b))
    }
}

struct Sqlite {
    c: rusqlite::Connection,
}
impl Sqlite {
    fn open(dir: &Path) -> Result<Sqlite> {
        std::fs::create_dir_all(dir)?;
        let c = rusqlite::Connection::open(dir.join("db.sqlite"))?;
        c.pragma_update(None, "journal_mode", "WAL")?;
        c.pragma_update(None, "synchronous", "FULL")?;
        c.pragma_update(None, "wal_autocheckpoint", 10000)?;
        c.execute(
            "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID",
            [],
        )?;
        Ok(Sqlite { c })
    }
}
impl Store for Sqlite {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let tx = self.c.transaction()?;
        {
            let mut st = tx.prepare_cached("INSERT OR REPLACE INTO kv (k, v) VALUES (?1, ?2)")?;
            for (k, v) in items {
                st.execute(rusqlite::params![&k[..], &v[..]])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        let mut st = self
            .c
            .prepare_cached("SELECT length(v) FROM kv WHERE k = ?1")?;
        let mut rows = st.query([&k[..]])?;
        Ok(match rows.next()? {
            Some(r) => Some(r.get::<_, i64>(0)? as usize),
            None => None,
        })
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        let mut st = self.c.prepare("SELECT v FROM kv")?;
        let mut rows = st.query([])?;
        let (mut n, mut b) = (0, 0);
        while let Some(r) = rows.next()? {
            let v: Vec<u8> = r.get(0)?;
            n += 1;
            b += touch(&v);
        }
        Ok((n, b))
    }
}

#[derive(Clone)]
struct RaftMsg;
impl raft_engine::MessageExt for RaftMsg {
    type Entry = raft::eraftpb::Entry;
    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}
struct RaftEng {
    e: raft_engine::Engine,
    logs: HashMap<u64, (u64, u64)>, // region -> (first, last)
}
impl RaftEng {
    fn open(dir: &Path) -> Result<RaftEng> {
        let cfg = raft_engine::Config {
            dir: dir.to_string_lossy().into(),
            // values are incompressible; disable batch compression
            batch_compression_threshold: raft_engine::ReadableSize(u64::MAX / 2),
            ..Default::default()
        };
        let e = raft_engine::Engine::open(cfg)?;
        let mut logs = HashMap::new();
        for r in e.raft_groups() {
            if let (Some(f), Some(l)) = (e.first_index(r), e.last_index(r)) {
                logs.insert(r, (f, l));
            }
        }
        Ok(RaftEng { e, logs })
    }
}
impl Store for RaftEng {
    fn append_group(&mut self, items: &[(Key, Vec<u8>)]) -> Result<()> {
        let mut b = raft_engine::LogBatch::with_capacity(items.len());
        for (k, v) in items {
            let region = u64::from_be_bytes(k[..8].try_into().unwrap());
            let idx = u64::from_be_bytes(k[8..].try_into().unwrap()) + 1; // raft indexes start at 1
            let mut en = raft::eraftpb::Entry::default();
            en.index = idx;
            en.data = v.clone().into();
            b.add_entries::<RaftMsg>(region, &[en])?;
            let e = self.logs.entry(region).or_insert((idx, idx));
            e.1 = idx;
        }
        self.e.write(&mut b, true)?;
        Ok(())
    }
    fn get(&mut self, k: &Key) -> Result<Option<usize>> {
        let region = u64::from_be_bytes(k[..8].try_into().unwrap());
        let idx = u64::from_be_bytes(k[8..].try_into().unwrap()) + 1;
        Ok(self
            .e
            .get_entry::<RaftMsg>(region, idx)?
            .map(|e| e.data.len()))
    }
    fn scan(&mut self) -> Result<(u64, u64)> {
        let (mut n, mut b) = (0, 0);
        let mut v = Vec::new();
        for (&r, &(f, l)) in &self.logs {
            let mut i = f;
            while i <= l {
                let end = (i + 256).min(l + 1);
                v.clear();
                self.e
                    .fetch_entries_to::<RaftMsg>(r, i, end, None, &mut v)?;
                for e in &v {
                    n += 1;
                    b += touch(&e.data);
                }
                i = end;
            }
        }
        Ok((n, b))
    }
}

// ---------------------------------------------------------------------------

const BACKENDS: &[&str] = &[
    "raw-append",
    "raw-prealloc",
    "raw-falloc",
    "raw-odirect",
    "raw-uring",
    "rocksdb",
    "rocksdb-blob",
    "fjall",
    "fjall-kvsep",
    "redb",
    "lmdb",
    "sqlite",
    "raft-engine",
];

fn open_store(name: &str, dir: &Path, pool: &Path) -> Result<Box<dyn Store>> {
    Ok(match name {
        "raw-append" => Box::new(RawStore::open(RawMode::Append, dir, pool)?),
        "raw-prealloc" => Box::new(RawStore::open(RawMode::Prealloc, dir, pool)?),
        "raw-falloc" => Box::new(RawStore::open(RawMode::Falloc, dir, pool)?),
        "raw-odirect" => Box::new(RawStore::open(RawMode::Direct, dir, pool)?),
        "raw-uring" => Box::new(RawStore::open(RawMode::Uring, dir, pool)?),
        "rocksdb" => Box::new(Rocks::open(dir, false)?),
        "rocksdb-blob" => Box::new(Rocks::open(dir, true)?),
        "fjall" => Box::new(Fjall::open(dir, false)?),
        "fjall-kvsep" => Box::new(Fjall::open(dir, true)?),
        "redb" => {
            std::fs::create_dir_all(dir)?;
            Box::new(Redb {
                db: redb::Database::create(dir.join("db.redb"))?,
            })
        }
        "lmdb" => Box::new(Lmdb::open(dir)?),
        "sqlite" => Box::new(Sqlite::open(dir)?),
        "raft-engine" => Box::new(RaftEng::open(dir)?),
        n => bail!("unknown backend {n}; one of {BACKENDS:?}"),
    })
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    backend: String,
    #[arg(long, default_value = "/home/user/precog/run/storage")]
    root: PathBuf,
    #[arg(long, default_value_t = 4)]
    producers: usize,
    /// Value (batch) size in bytes.
    #[arg(long, default_value_t = 27_000)]
    size: usize,
    #[arg(long, default_value_t = 8)]
    secs: u64,
    #[arg(long, default_value_t = 50_000)]
    gets: usize,
    #[arg(long)]
    json: Option<PathBuf>,
    #[arg(long, default_value = "")]
    label: String,
    /// Dormant-stream summaries instead (docs/S2-PLAN.md C7): this many streams.
    #[arg(long, default_value_t = 0)]
    summaries: u64,
}

struct Req {
    key: Key,
    val: Vec<u8>,
    done: Sender<()>,
}

/// Bytes written to the block device holding the data (sectors written, all
/// writers on the device: data, journal, metadata, compaction).
fn io_write_bytes() -> u64 {
    std::fs::read_to_string("/sys/block/vda/stat")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .nth(6)
                .and_then(|x| x.parse::<u64>().ok())
        })
        .map(|sectors| sectors * 512)
        .unwrap_or(0)
}

fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(u.ru_utime) + t(u.ru_stime)
}

fn dir_size(p: &Path) -> u64 {
    let mut s = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let m = e.metadata().unwrap();
            if m.is_dir() {
                s += dir_size(&e.path());
            } else {
                use std::os::unix::fs::MetadataExt;
                s += m.blocks() * 512;
            }
        }
    }
    s
}

fn drop_caches() -> bool {
    unsafe { libc::sync() };
    std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok()
}

fn main() -> Result<()> {
    let a = Args::parse();
    let dir = a.root.join("data").join(&a.backend);
    let pool = a.root.join("pool");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;
    // Pre-create recycled segments outside the timed region.
    if matches!(
        a.backend.as_str(),
        "raw-prealloc" | "raw-odirect" | "raw-uring"
    ) {
        for i in 0..6 {
            pool_segment(&pool, i)?;
        }
    }
    let store = open_store(&a.backend, &dir, &pool).context("open")?;
    if a.summaries > 0 {
        return summaries(&a, store, &dir, &pool);
    }

    // ---------------- write phase
    let (tx, rx): (Sender<Req>, Receiver<Req>) = bounded(4096);
    let writer = std::thread::spawn(move || -> Result<(Box<dyn Store>, u64, u64)> {
        let mut store = store;
        let (mut groups, mut items_n) = (0u64, 0u64);
        let mut items: Vec<(Key, Vec<u8>)> = Vec::with_capacity(256);
        let mut dones: Vec<Sender<()>> = Vec::with_capacity(256);
        while let Ok(r) = rx.recv() {
            items.push((r.key, r.val));
            dones.push(r.done);
            let mut bytes = items[0].1.len();
            while bytes < 16 << 20 {
                match rx.try_recv() {
                    Ok(r) => {
                        bytes += r.val.len();
                        items.push((r.key, r.val));
                        dones.push(r.done);
                    }
                    Err(_) => break,
                }
            }
            store.append_group(&items)?;
            groups += 1;
            items_n += items.len() as u64;
            items.clear();
            for d in dones.drain(..) {
                let _ = d.send(());
            }
        }
        Ok((store, groups, items_n))
    });

    unsafe { libc::sync() };
    let w0 = io_write_bytes();
    let c0 = cpu_secs();
    let t0 = Instant::now();
    let deadline = t0 + Duration::from_secs(a.secs);
    let mut handles = vec![];
    for p in 0..a.producers {
        let tx = tx.clone();
        let size = a.size;
        handles.push(std::thread::spawn(move || -> (Histogram<u64>, u64) {
            let mut h = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            let base: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
            let (dtx, drx) = bounded(1);
            let mut seq = 0u64;
            while Instant::now() < deadline {
                let mut v = base.clone();
                v[..8].copy_from_slice(&seq.to_le_bytes());
                let s = Instant::now();
                tx.send(Req {
                    key: key(p as u64, seq),
                    val: v,
                    done: dtx.clone(),
                })
                .unwrap();
                drx.recv().unwrap();
                h.record(s.elapsed().as_micros().max(1) as u64).unwrap();
                seq += 1;
            }
            (h, seq)
        }));
    }
    drop(tx);
    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut appends = 0u64;
    let mut written: Vec<u64> = vec![];
    for h in handles {
        let (x, n) = h.join().unwrap();
        hist.add(&x)?;
        appends += n;
        written.push(n);
    }
    let (mut store, groups, _items) = writer.join().unwrap()?;
    let elapsed = t0.elapsed().as_secs_f64();
    let cpu_w = cpu_secs() - c0;
    unsafe { libc::sync() }; // count writeback still pending (compaction output, page cache)
    std::thread::sleep(Duration::from_millis(500));
    let dev_bytes = io_write_bytes() - w0;
    let user_bytes = appends * a.size as u64;

    // ---------------- read phase (warm page cache)
    let mut gh = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let tg = Instant::now();
    let mut found = 0;
    for i in 0..a.gets {
        let p = (rand::random::<u64>() as usize) % a.producers;
        if written[p] == 0 {
            continue;
        }
        let s = rand::random::<u64>() % written[p];
        let st = Instant::now();
        if store.get(&key(p as u64, s))?.is_some() {
            found += 1;
        }
        gh.record(st.elapsed().as_micros().max(1) as u64)?;
        let _ = i;
    }
    let get_secs = tg.elapsed().as_secs_f64();
    let ts = Instant::now();
    let (sn, sb) = store.scan()?;
    let scan_secs = ts.elapsed().as_secs_f64();

    // ---------------- reopen / recovery
    drop(store);
    std::thread::sleep(Duration::from_millis(200));
    let cold = drop_caches();
    let tr = Instant::now();
    let mut store = open_store(&a.backend, &dir, &pool)?;
    let reopen_open = tr.elapsed().as_secs_f64();
    // Raw stores need a replay to serve reads again; others are ready.
    let replay = raw_replay(&mut store, &a.backend, &dir, &pool)?;
    let reopen_total = tr.elapsed().as_secs_f64();
    let ok_after = store.get(&key(0, written[0].saturating_sub(1)))?.is_some();
    let disk = if a.backend == "raw-falloc" {
        0
    } else {
        dir_size(&dir)
    } + if a.backend.starts_with("raw-") && a.backend != "raw-append" {
        user_bytes
    } else {
        0
    };

    let r = serde_json::json!({
        "label": a.label, "backend": a.backend, "producers": a.producers, "size": a.size, "secs": elapsed,
        "appends": appends, "appends_per_s": appends as f64 / elapsed, "mb_per_s": user_bytes as f64 / elapsed / 1e6,
        "avg_group": appends as f64 / groups.max(1) as f64, "groups_per_s": groups as f64 / elapsed,
        "lat_p50_us": hist.value_at_quantile(0.5), "lat_p99_us": hist.value_at_quantile(0.99), "lat_p999_us": hist.value_at_quantile(0.999),
        "cpu_us_per_append": cpu_w * 1e6 / appends.max(1) as f64,
        "write_amp": dev_bytes as f64 / user_bytes.max(1) as f64,
        "disk_amp": disk as f64 / user_bytes.max(1) as f64,
        "get_per_s": a.gets as f64 / get_secs, "get_p99_us": gh.value_at_quantile(0.99), "get_found": found,
        "scan_mb_per_s": sb as f64 / scan_secs / 1e6, "scan_records": sn,
        "reopen_ms": reopen_total * 1e3, "reopen_open_ms": reopen_open * 1e3, "replayed_records": replay, "reopen_cold": cold, "readable_after_reopen": ok_after,
    });
    println!(
        "{:<13} p{:<2} {:>6}B  {:>8.0} app/s {:>7.1} MB/s  grp {:>5.1}  p50 {:>6}us p99 {:>7}us  cpu {:>6.1}us/app  wamp {:>4.2} disk {:>4.2}  get {:>8.0}/s  scan {:>7.0} MB/s  reopen {:>7.0} ms{}",
        a.backend,
        a.producers,
        a.size,
        r["appends_per_s"].as_f64().unwrap(),
        r["mb_per_s"].as_f64().unwrap(),
        r["avg_group"].as_f64().unwrap(),
        r["lat_p50_us"],
        r["lat_p99_us"],
        r["cpu_us_per_append"].as_f64().unwrap(),
        r["write_amp"].as_f64().unwrap(),
        r["disk_amp"].as_f64().unwrap(),
        r["get_per_s"].as_f64().unwrap(),
        r["scan_mb_per_s"].as_f64().unwrap(),
        r["reopen_ms"].as_f64().unwrap(),
        if cold { " (cold)" } else { "" }
    );
    if let Some(p) = &a.json {
        let mut f = OpenOptions::new().create(true).append(true).open(p)?;
        writeln!(f, "{r}")?;
    }
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// For raw stores: re-attach existing segments and rebuild the index by replay.
fn raw_replay(store: &mut Box<dyn Store>, backend: &str, dir: &Path, pool: &Path) -> Result<u64> {
    if !backend.starts_with("raw-") {
        return Ok(0);
    }
    let mode = match backend {
        "raw-append" => RawMode::Append,
        "raw-prealloc" => RawMode::Prealloc,
        "raw-falloc" => RawMode::Falloc,
        "raw-odirect" => RawMode::Direct,
        _ => RawMode::Uring,
    };
    // Re-open existing segments (open_store created a fresh first segment; rebuild from disk instead)
    let mut segs = vec![];
    let mut i = 0;
    loop {
        let p = match mode {
            RawMode::Append | RawMode::Falloc => dir.join(format!("seg-{i:04}")),
            _ => pool.join(format!("seg-{i:04}")),
        };
        if !p.exists() || i >= 6 {
            break;
        }
        segs.push(File::open(&p)?);
        i += 1;
    }
    let mut rs = RawStore::open(RawMode::Append, &dir.join("replay-scratch"), pool)?;
    rs.generation = std::fs::read_to_string(dir.join("GENERATION"))?
        .trim()
        .parse()?;
    rs.readers = segs
        .iter()
        .map(|f| f.try_clone())
        .collect::<std::io::Result<_>>()?;
    rs.segs = segs;
    rs.rebuild()?;
    let n = rs.index.len() as u64;
    *store = Box::new(rs);
    Ok(n)
}

/// A stream's key: its name's hash, in no order (names arrive in any order).
fn skey(i: u64) -> Key {
    let mut z = i.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    key(z ^ (z >> 31), i)
}

fn rss_mib() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse::<f64>().ok())
        .unwrap_or(0.0)
        / 1024.0
}

/// The dormant-stream summary workload: N summaries of ~120 B written by checkpoints of 10k,
/// then checkpoints of 1k changed streams, a reopen, and point reads of streams that exist
/// (a wake) and that do not (a new stream's first append), one at a time as the store does.
fn summaries(a: &Args, mut store: Box<dyn Store>, dir: &Path, pool: &Path) -> Result<()> {
    let n = a.summaries;
    let val = |i: u64, v: u64| -> Vec<u8> {
        let mut b = vec![0u8; 120];
        b[..8].copy_from_slice(&i.to_le_bytes());
        b[8..16].copy_from_slice(&v.to_le_bytes());
        b
    };
    let hist = || Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    let (dev0, t) = (io_write_bytes(), Instant::now());
    let mut load = hist();
    let mut i = 0;
    while i < n {
        let items: Vec<(Key, Vec<u8>)> = (i..(i + 10_000).min(n))
            .map(|j| (skey(j), val(j, 0)))
            .collect();
        let tc = Instant::now();
        store.append_group(&items)?;
        load.record(tc.elapsed().as_micros().max(1) as u64)?;
        i += items.len() as u64;
    }
    let load_s = t.elapsed().as_secs_f64();
    let mut upd = hist();
    let (dev1, tu) = (io_write_bytes(), Instant::now());
    for r in 0..200u64 {
        let items: Vec<(Key, Vec<u8>)> = (0..1000)
            .map(|_| {
                let j = rand::random::<u64>() % n;
                (skey(j), val(j, r + 1))
            })
            .collect();
        let tc = Instant::now();
        store.append_group(&items)?;
        upd.record(tc.elapsed().as_micros().max(1) as u64)?;
    }
    let upd_s = tu.elapsed().as_secs_f64();
    let upd_dev = io_write_bytes() - dev1;
    drop(store);
    std::thread::sleep(Duration::from_millis(200));
    let cold = drop_caches();
    let rss0 = rss_mib();
    let tr = Instant::now();
    let mut store = open_store(&a.backend, dir, pool)?;
    raw_replay(&mut store, &a.backend, dir, pool)?;
    let reopen_ms = tr.elapsed().as_secs_f64() * 1e3;
    let (mut hit, mut miss) = (hist(), hist());
    let tg = Instant::now();
    for _ in 0..a.gets {
        let j = rand::random::<u64>() % n;
        let tc = Instant::now();
        let got = store.get(&skey(j))?;
        hit.record(tc.elapsed().as_nanos().max(1) as u64)?;
        anyhow::ensure!(got == Some(120), "stream {j} lost");
    }
    let hit_s = tg.elapsed().as_secs_f64();
    for _ in 0..a.gets {
        let j = n + rand::random::<u64>() % (n * 10);
        let tc = Instant::now();
        let got = store.get(&skey(j))?;
        miss.record(tc.elapsed().as_nanos().max(1) as u64)?;
        anyhow::ensure!(got.is_none());
    }
    let r = serde_json::json!({
        "label": a.label, "backend": a.backend, "streams": n,
        "load_per_s": n as f64 / load_s, "load_commit_p50_ms": load.value_at_quantile(0.5) as f64 / 1e3,
        "load_write_amp": (dev1 - dev0) as f64 / (n * 136) as f64,
        "ckpt1k_p50_ms": upd.value_at_quantile(0.5) as f64 / 1e3, "ckpt1k_p99_ms": upd.value_at_quantile(0.99) as f64 / 1e3,
        "ckpt1k_per_s": 200.0 / upd_s, "ckpt1k_dev_kib": upd_dev as f64 / 200.0 / 1024.0,
        "reopen_ms": reopen_ms, "cold": cold,
        "wake_p50_us": hit.value_at_quantile(0.5) as f64 / 1e3, "wake_p99_us": hit.value_at_quantile(0.99) as f64 / 1e3,
        "wake_per_s": a.gets as f64 / hit_s,
        "miss_p50_us": miss.value_at_quantile(0.5) as f64 / 1e3, "miss_p99_us": miss.value_at_quantile(0.99) as f64 / 1e3,
        "rss_after_mib": rss_mib(), "rss_before_open_mib": rss0, "disk_mib": dir_size(dir) as f64 / (1 << 20) as f64,
    });
    println!("{r}");
    if let Some(p) = &a.json {
        let mut f = OpenOptions::new().create(true).append(true).open(p)?;
        writeln!(f, "{r}")?;
    }
    drop(store);
    let _ = std::fs::remove_dir_all(dir);
    Ok(())
}
