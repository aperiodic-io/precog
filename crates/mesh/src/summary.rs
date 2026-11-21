//! Dormant streams on disk (docs/ARCHITECTURE.md, "Dormant streams").
//!
//! The store keeps a stream in RAM only while it is used; the rest are summaries in one redb
//! database (`<data_dir>/streams.redb`, already a dependency of the workspace, pure Rust, crash
//! safe by copy-on-write pages and a two-phase commit). Per stream one value: its retention
//! floor and totals, per absorbed segment its totals, and per writer log its frontier and its
//! sequence range per absorbed segment. One more value, the checkpoint, says up to where the
//! summaries cover the segment log: a restart replays only the log past it.
//!
//! Values are this module's own format (versioned, CRC-32C), not redb's: a newer node refuses an
//! older one loudly instead of misreading it.

use crate::store::{Age, Cold, ColdStream};
use precog_storage::Lsn;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::sync::{Arc, Mutex};

const STREAMS: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const CHECKPOINT: &str = "checkpoint";

/// The format of a stream's summary and of the checkpoint; bumped on any change.
/// 3: a stream's summary has its epoch floor (`Store::set_floor`: a deleted stream).
pub const VERSION: u8 = 3;
/// The oldest version still read: a version 2 summary is a stream never deleted, and the
/// checkpoint did not change.
const OLDEST: u8 = 2;

pub type Result<T> = std::result::Result<T, String>;

/// One writer log of a dormant stream.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogSum {
    pub writer: String,
    pub epoch: u64,
    pub cold: Vec<Cold>,
    pub trimmed: u64,
    pub contig: u64,
    pub high: u64,
    pub ooo: bool,
}

/// A stream as the store holds it, less its batches in RAM (a stream leaves RAM only once it has
/// none).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamSum {
    pub floor: Age,
    pub bytes: u64,
    /// Its writer logs with a lower epoch are deleted (0: never deleted).
    pub epoch_floor: u64,
    pub cold: Vec<(Lsn, ColdStream)>,
    pub logs: Vec<LogSum>,
}

/// Up to where the summaries cover the log, and the store's other state then. Every record below
/// `cold_end` is in a summary. The records in `ram` (runs of LSNs: the store's batches in RAM)
/// were indexed and counted in the summaries and totals: a restart puts them back in RAM without
/// counting them again. Other records below `contiguous` were indexed and forgotten (retention, a
/// replacement, a second copy): a restart skips them. Records past those are replayed as new.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Checkpoint {
    pub cold_end: Lsn,
    pub contiguous: Lsn,
    pub ram: Vec<(Lsn, Lsn)>,
    pub floor: Lsn,
    /// Retained (batches, events, bytes), hot and dormant streams together.
    pub totals: (u64, u64, u64),
    /// Per absorbed segment (base, end), what every stream retains in it: dropping it subtracts
    /// these.
    pub segs: Vec<(Lsn, Lsn, (u64, u64, u64))>,
    pub dead: Vec<Lsn>,
    /// Repair's position in each peer's log: (peer, its incarnation, next LSN).
    pub peers: Vec<(u32, u64, u64)>,
    /// This node's boots, oldest first: (incarnation, where its log ended when it opened).
    pub boots: Vec<(u64, u64)>,
}

struct W(Vec<u8>);

impl W {
    /// LEB128: seven bits a byte, low first, the top bit set on all but the last.
    fn u64(&mut self, mut x: u64) {
        // at most nine bytes before the last (a bound, not a condition: any change to the loop
        // still ends)
        for _ in 0..9 {
            if x < 0x80 {
                break;
            }
            self.0.push(x as u8 | 0x80);
            x >>= 7;
        }
        self.0.push(x as u8);
    }
    fn age(&mut self, a: Age) {
        for x in [a.0, a.1, a.2] {
            self.u64(x);
        }
    }
    fn str(&mut self, s: &str) {
        self.u64(s.len() as u64);
        self.0.extend_from_slice(s.as_bytes());
    }
    /// Seals the value: a CRC-32C of everything before it.
    fn done(mut self) -> Vec<u8> {
        let c = crc32c::crc32c(&self.0);
        self.0.extend_from_slice(&c.to_le_bytes());
        self.0
    }
}

/// A reader: a count is a u64 read like any other, nothing is allocated for it, and a count
/// past the bytes left fails its first short read.
struct R<'a>(&'a [u8]);

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (a, b) = self.0.split_at_checked(n)?;
        self.0 = b;
        Some(a)
    }
    /// A number in its one LEB128 form: no longer than it needs, at most 64 bits.
    fn u64(&mut self) -> Option<u64> {
        // (the tenth byte ends it: past 1 it is refused, and 1 or less has no top bit)
        let (mut x, mut i) = (0, 0);
        loop {
            let b = self.take(1)?[0];
            if i == 9 && b > 1 || i > 0 && b == 0 {
                return None;
            }
            x |= u64::from(b & 0x7f) << (7 * i);
            if b < 0x80 {
                return Some(x);
            }
            i += 1;
        }
    }
    fn age(&mut self) -> Option<Age> {
        Some((self.u64()?, self.u64()?, self.u64()?))
    }
    fn str(&mut self) -> Option<String> {
        let n = usize::try_from(self.u64()?).ok()?;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }
}

/// The value's version and its body, after its checksum and version are checked.
fn open(b: &[u8]) -> Option<(u8, R<'_>)> {
    let (body, crc) = b.split_at_checked(b.len().checked_sub(4)?)?;
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().ok()?) {
        return None;
    }
    let (&v, rest) = body.split_first()?;
    (OLDEST..=VERSION).contains(&v).then_some((v, R(rest)))
}

pub fn encode_stream(s: &StreamSum) -> Vec<u8> {
    // room for a stream of one log in one segment: no regrowth
    let mut w = W(Vec::with_capacity(256));
    w.0.push(VERSION);
    w.age(s.floor);
    w.u64(s.bytes);
    w.u64(s.epoch_floor);
    w.u64(s.cold.len() as u64);
    for (base, c) in &s.cold {
        for x in [*base, c.end, c.batches, c.events, c.bytes, c.max_time] {
            w.u64(x);
        }
        w.age(c.min);
    }
    w.u64(s.logs.len() as u64);
    for l in &s.logs {
        w.str(&l.writer);
        for x in [l.epoch, l.trimmed, l.contig, l.high, l.ooo as u64] {
            w.u64(x);
        }
        w.u64(l.cold.len() as u64);
        for c in &l.cold {
            for x in [c.base, c.lo, c.hi] {
                w.u64(x);
            }
        }
    }
    w.done()
}

pub fn decode_stream(b: &[u8]) -> Option<StreamSum> {
    let (version, mut r) = open(b)?;
    let mut s = StreamSum {
        floor: r.age()?,
        bytes: r.u64()?,
        epoch_floor: if version == OLDEST { 0 } else { r.u64()? },
        ..Default::default()
    };
    for _ in 0..r.u64()? {
        let base = r.u64()?;
        let c = ColdStream {
            end: r.u64()?,
            batches: r.u64()?,
            events: r.u64()?,
            bytes: r.u64()?,
            max_time: r.u64()?,
            min: r.age()?,
        };
        s.cold.push((base, c));
    }
    for _ in 0..r.u64()? {
        let mut l = LogSum {
            writer: r.str()?,
            epoch: r.u64()?,
            trimmed: r.u64()?,
            contig: r.u64()?,
            high: r.u64()?,
            ooo: r.u64()? != 0,
            cold: vec![],
        };
        for _ in 0..r.u64()? {
            l.cold.push(Cold {
                base: r.u64()?,
                lo: r.u64()?,
                hi: r.u64()?,
            });
        }
        s.logs.push(l);
    }
    r.0.is_empty().then_some(s)
}

pub fn encode_checkpoint(c: &Checkpoint) -> Vec<u8> {
    let mut w = W(vec![VERSION]);
    for x in [
        c.cold_end,
        c.contiguous,
        c.floor,
        c.totals.0,
        c.totals.1,
        c.totals.2,
    ] {
        w.u64(x);
    }
    w.u64(c.ram.len() as u64);
    for &(a, b) in &c.ram {
        w.u64(a);
        w.u64(b);
    }
    w.u64(c.dead.len() as u64);
    for &x in &c.dead {
        w.u64(x);
    }
    w.u64(c.segs.len() as u64);
    for &(base, end, (b, e, n)) in &c.segs {
        for x in [base, end, b, e, n] {
            w.u64(x);
        }
    }
    w.u64(c.peers.len() as u64);
    for &(p, inc, next) in &c.peers {
        for x in [p as u64, inc, next] {
            w.u64(x);
        }
    }
    w.u64(c.boots.len() as u64);
    for &(inc, lsn) in &c.boots {
        w.u64(inc);
        w.u64(lsn);
    }
    w.done()
}

pub fn decode_checkpoint(b: &[u8]) -> Option<Checkpoint> {
    let (_, mut r) = open(b)?;
    let mut c = Checkpoint {
        cold_end: r.u64()?,
        contiguous: r.u64()?,
        floor: r.u64()?,
        totals: (r.u64()?, r.u64()?, r.u64()?),
        ..Default::default()
    };
    for _ in 0..r.u64()? {
        c.ram.push((r.u64()?, r.u64()?));
    }
    for _ in 0..r.u64()? {
        c.dead.push(r.u64()?);
    }
    for _ in 0..r.u64()? {
        c.segs
            .push((r.u64()?, r.u64()?, (r.u64()?, r.u64()?, r.u64()?)));
    }
    for _ in 0..r.u64()? {
        let p = u32::try_from(r.u64()?).ok()?;
        c.peers.push((p, r.u64()?, r.u64()?));
    }
    for _ in 0..r.u64()? {
        c.boots.push((r.u64()?, r.u64()?));
    }
    r.0.is_empty().then_some(c)
}

type Table = redb::ReadOnlyTable<&'static str, &'static [u8]>;

/// What the handles of one database share.
struct Shared {
    db: redb::Database,
    /// The streams table as of the last commit, kept open between reads (a read transaction
    /// per wake costs more than the read); dropped at each commit, so old pages are freed.
    table: Mutex<Option<Table>>,
    /// Test fault injection: "summary:before_commit" fails a commit before it writes anything,
    /// "summary:after_commit" right after it is durable, "summary:read" every read.
    fail_at: Mutex<Option<&'static str>>,
}

/// The summaries of dormant streams and the checkpoint.
pub struct Db(Option<Arc<Shared>>);

/// The databases open in this process, by path.
static OPEN: Mutex<Vec<(std::path::PathBuf, Arc<Shared>)>> = Mutex::new(vec![]);

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn err(e: impl std::fmt::Display) -> String {
    format!("streams database: {e}")
}

impl Drop for Db {
    /// The last handle closes the database, under the lock of the open ones: an open after it
    /// finds the file free.
    fn drop(&mut self) {
        let mut open = lock(&OPEN);
        let s = self.0.take().expect("held until dropped");
        if Arc::strong_count(&s) == 2 {
            open.retain(|(_, x)| !Arc::ptr_eq(x, &s));
        }
        drop(s);
    }
}

impl Db {
    /// Opens (or creates) the database at `path`, with a page cache of at most `cache` bytes.
    /// The file is locked while open, against other processes; in this process an open shares
    /// the database a node still closing holds (its last tasks keep it), as its segment log does.
    pub fn open(path: &std::path::Path, cache: usize) -> Result<Db> {
        let mut open = lock(&OPEN);
        if let Some((_, s)) = open.iter().find(|(p, _)| p == path) {
            return Ok(Db(Some(s.clone())));
        }
        let mut b = redb::Database::builder();
        let s = Db::shared(b.set_cache_size(cache).create(path).map_err(err)?)?;
        open.push((path.to_path_buf(), s.clone()));
        Ok(Db(Some(s)))
    }

    /// Both tables exist from the start: no read meets a missing one.
    fn shared(db: redb::Database) -> Result<Arc<Shared>> {
        let tx = db.begin_write().map_err(err)?;
        tx.open_table(STREAMS).map_err(err)?;
        tx.open_table(META).map_err(err)?;
        tx.commit().map_err(err)?;
        let (table, fail_at) = Default::default();
        Ok(Arc::new(Shared { db, table, fail_at }))
    }

    /// A database in RAM (a store without a data directory: tests and tools).
    pub fn memory() -> Db {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .expect("an in-memory database opens");
        Db(Some(Db::shared(db).expect("its tables")))
    }

    fn s(&self) -> &Shared {
        self.0.as_ref().expect("held until dropped")
    }

    /// Another handle to the same database (a store reopened over it in this process).
    pub fn share(&self) -> Db {
        Db(self.0.clone())
    }

    /// Sets the fault every handle of this database injects (tests).
    pub fn set_failpoint(&self, at: Option<&'static str>) {
        *lock(&self.s().fail_at) = at;
    }

    pub fn with_failpoint(self, at: Option<&'static str>) -> Db {
        self.set_failpoint(at);
        self
    }

    fn fault(&self, at: &'static str) -> Result<()> {
        match *lock(&self.s().fail_at) == Some(at) {
            true => Err(err(format!("failpoint {at}"))),
            false => Ok(()),
        }
    }

    /// The stream's summary, if it has one.
    pub fn stream(&self, stream: &str) -> Result<Option<StreamSum>> {
        self.fault("summary:read")?;
        let mut t = lock(&self.s().table);
        if t.is_none() {
            *t = Some(
                self.s()
                    .db
                    .begin_read()
                    .map_err(err)?
                    .open_table(STREAMS)
                    .map_err(err)?,
            );
        }
        let got = t.as_ref().expect("opened").get(stream).map_err(err)?;
        let refused = || err(format!("the summary of stream {stream:?} does not decode"));
        got.map(|v| decode_stream(v.value()).ok_or_else(refused))
            .transpose()
    }

    pub fn checkpoint(&self) -> Result<Option<Checkpoint>> {
        let tx = self.s().db.begin_read().map_err(err)?;
        let got = tx
            .open_table(META)
            .map_err(err)?
            .get(CHECKPOINT)
            .map_err(err)?;
        let refused = || err("the checkpoint does not decode (a newer format?)");
        got.map(|v| decode_checkpoint(v.value()).ok_or_else(refused))
            .transpose()
    }

    /// Visits every stream's summary.
    pub fn each(&self, mut f: impl FnMut(&str, StreamSum)) -> Result<()> {
        self.fault("summary:read")?;
        let tx = self.s().db.begin_read().map_err(err)?;
        for kv in tx.open_table(STREAMS).map_err(err)?.iter().map_err(err)? {
            let (k, v) = kv.map_err(err)?;
            let refused = || err(format!("the summary of {:?} does not decode", k.value()));
            f(k.value(), decode_stream(v.value()).ok_or_else(refused)?);
        }
        Ok(())
    }

    /// How many streams have a summary.
    pub fn streams(&self) -> Result<u64> {
        let tx = self.s().db.begin_read().map_err(err)?;
        tx.open_table(STREAMS).map_err(err)?.len().map_err(err)
    }

    /// Writes the summaries (encoded) and the checkpoint in one durable transaction.
    pub fn commit(&self, c: &Checkpoint, streams: &[(String, Vec<u8>)]) -> Result<()> {
        self.fault("summary:before_commit")?;
        let mut tx = self.s().db.begin_write().map_err(err)?;
        // the allocator state goes with each commit: a crash costs no walk of the whole file
        tx.set_quick_repair(true);
        {
            let mut t = tx.open_table(STREAMS).map_err(err)?;
            for (k, v) in streams {
                t.insert(k.as_str(), v.as_slice()).map_err(err)?;
            }
            let mut m = tx.open_table(META).map_err(err)?;
            m.insert(CHECKPOINT, encode_checkpoint(c).as_slice())
                .map_err(err)?;
        }
        tx.commit().map_err(err)?;
        *lock(&self.s().table) = None;
        self.fault("summary:after_commit")
    }
}

#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;

    fn age() -> impl Strategy<Value = Age> {
        (any::<u64>(), any::<u64>(), any::<u64>())
    }

    fn stream() -> impl Strategy<Value = StreamSum> {
        let cold = (any::<u64>(), prop::array::uniform5(any::<u64>()), age()).prop_map(
            |(base, x, min)| {
                let c = ColdStream {
                    end: x[0],
                    batches: x[1],
                    events: x[2],
                    bytes: x[3],
                    max_time: x[4],
                    min,
                };
                (base, c)
            },
        );
        let log = (
            ".{0,12}",
            prop::array::uniform4(any::<u64>()),
            any::<bool>(),
            prop::collection::vec(prop::array::uniform3(any::<u64>()), 0..4),
        )
            .prop_map(|(writer, x, ooo, cold)| LogSum {
                writer,
                epoch: x[0],
                trimmed: x[1],
                contig: x[2],
                high: x[3],
                ooo,
                cold: cold
                    .into_iter()
                    .map(|c| Cold {
                        base: c[0],
                        lo: c[1],
                        hi: c[2],
                    })
                    .collect(),
            });
        (
            age(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(cold, 0..4),
            prop::collection::vec(log, 0..4),
        )
            .prop_map(|(floor, bytes, epoch_floor, cold, logs)| StreamSum {
                floor,
                bytes,
                epoch_floor,
                cold,
                logs,
            })
    }

    fn checkpoint() -> impl Strategy<Value = Checkpoint> {
        let v = || prop::collection::vec(any::<u64>(), 0..5);
        (
            prop::array::uniform6(any::<u64>()),
            prop::collection::vec((any::<u64>(), any::<u64>()), 0..4),
            v(),
            prop::collection::vec(
                (
                    any::<u64>(),
                    any::<u64>(),
                    prop::array::uniform3(any::<u64>()),
                ),
                0..4,
            ),
            prop::collection::vec((any::<u32>(), any::<u64>(), any::<u64>()), 0..4),
            prop::collection::vec((any::<u64>(), any::<u64>()), 0..4),
        )
            .prop_map(|(x, ram, dead, segs, peers, boots)| Checkpoint {
                cold_end: x[0],
                contiguous: x[1],
                floor: x[2],
                totals: (x[3], x[4], x[5]),
                ram,
                dead,
                segs: segs
                    .into_iter()
                    .map(|(b, e, t)| (b, e, (t[0], t[1], t[2])))
                    .collect(),
                peers,
                boots,
            })
    }

    /// Every way of damaging `b`: each byte flipped, cut at each length, a byte more.
    fn damaged(b: &[u8]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = (0..b.len())
            .map(|i| {
                let mut v = b.to_vec();
                v[i] ^= 0x41;
                v
            })
            .collect();
        out.extend((0..b.len()).map(|n| b[..n].to_vec()));
        out.push([b, &[0]].concat());
        out
    }

    proptest! {
        /// Why: a dormant stream comes back from its summary, so the summary must say exactly
        /// what RAM held, and a damaged one (a bad sector, another version) must be refused
        /// rather than read as some other stream's state.
        #[test]
        fn a_summary_round_trips_and_damage_is_refused(s in stream()) {
            let b = encode_stream(&s);
            prop_assert_eq!(decode_stream(&b), Some(s));
            for d in damaged(&b) {
                prop_assert_eq!(decode_stream(&d), None);
            }
        }

        #[test]
        fn a_checkpoint_round_trips_and_damage_is_refused(c in checkpoint()) {
            let b = encode_checkpoint(&c);
            prop_assert_eq!(decode_checkpoint(&b), Some(c));
            for d in damaged(&b) {
                prop_assert_eq!(decode_checkpoint(&d), None);
            }
        }

        /// Why: a node upgraded in place opens the summaries its older version wrote. A version 2
        /// summary (no epoch floor) must read as that stream, never deleted; any other version
        /// is refused, loudly, not read as something else.
        #[test]
        fn a_version_2_summary_still_reads_and_no_other_version_does(s in stream(), other: u8) {
            let s = StreamSum { epoch_floor: 0, ..s };
            let v3 = encode_stream(&s);
            // as version 2 wrote it: the same, without the floor's byte after the bytes
            let mut at = W(vec![]);
            at.age(s.floor);
            at.u64(s.bytes);
            let at = 1 + at.0.len();
            let seal = |version: u8, body: &[u8]| {
                let mut v = [&[version][..], body].concat();
                let c = crc32c::crc32c(&v);
                v.extend_from_slice(&c.to_le_bytes());
                v
            };
            let body = &v3[1..v3.len() - 4];
            prop_assert_eq!(body[at - 1], 0, "the floor");
            let old = [&body[..at - 1], &body[at..]].concat();
            prop_assert_eq!(decode_stream(&seal(2, &old)), Some(s.clone()));
            prop_assert_eq!(decode_stream(&seal(3, body)), Some(s));
            // the checkpoint did not change between them
            let c = encode_checkpoint(&Checkpoint::default());
            let c = &c[1..c.len() - 4];
            prop_assert_eq!(decode_checkpoint(&seal(2, c)), Some(Checkpoint::default()));
            if other != 2 && other != 3 {
                prop_assert_eq!(decode_stream(&seal(other, &old)), None);
                prop_assert_eq!(decode_stream(&seal(other, body)), None);
                prop_assert_eq!(decode_checkpoint(&seal(other, c)), None);
            }
        }

        /// Why: a node opens values another version wrote. Whatever the bytes, the decoders
        /// answer or refuse; they never panic or allocate what the bytes merely claim.
        #[test]
        fn any_bytes_decode_or_are_refused(b in prop::collection::vec(any::<u8>(), 0..200)) {
            let _ = decode_stream(&b);
            let _ = decode_checkpoint(&b);
            // the same bytes behind a valid checksum and version: past the outer checks
            let mut v = vec![VERSION];
            v.extend_from_slice(&b);
            let c = crc32c::crc32c(&v);
            v.extend_from_slice(&c.to_le_bytes());
            let _ = decode_stream(&v);
            let _ = decode_checkpoint(&v);
        }

        /// Why: what a commit wrote is what a reopened database reads, streams and checkpoint.
        #[test]
        fn a_commit_is_read_back_after_a_reopen(ss in prop::collection::vec(stream(), 1..4), c in checkpoint()) {
            let d = tempfile::tempdir().unwrap();
            let p = d.path().join("s.redb");
            let names: Vec<String> = (0..ss.len()).map(|i| format!("s{i}")).collect();
            {
                let db = Db::open(&p, 1 << 20).unwrap();
                prop_assert_eq!(db.checkpoint().unwrap(), None);
                prop_assert_eq!(db.stream("s0").unwrap(), None);
                let vals: Vec<(String, Vec<u8>)> =
                    names.iter().cloned().zip(ss.iter().map(encode_stream)).collect();
                db.commit(&c, &vals).unwrap();
            }
            let db = Db::open(&p, 1 << 20).unwrap();
            prop_assert_eq!(db.checkpoint().unwrap(), Some(c));
            let mut seen = vec![];
            db.each(|k, s| seen.push((k.to_string(), s))).unwrap();
            prop_assert_eq!(seen, names.into_iter().zip(ss).collect::<Vec<_>>());
        }
    }

    /// A value behind a valid version and checksum, as the decoders see it past those checks.
    fn sealed(body: &[u8]) -> Vec<u8> {
        let mut v = vec![VERSION];
        v.extend_from_slice(body);
        let c = crc32c::crc32c(&v);
        v.extend_from_slice(&c.to_le_bytes());
        v
    }

    #[test]
    fn a_stream_of_one_log_in_one_segment_has_a_summary_of_under_a_hundred_bytes() {
        // Why: each checkpoint writes the summary of every stream that changed, and each new
        // stream changed. Their size sets what the database writes per checkpoint (B-tree
        // pages, copied on write) and its size on disk per dormant stream. Its numbers are mostly
        // small: counts, sequences, an LSN, a time in milliseconds.
        let (lsn, t) = (40_000_000, 1_790_000_000_000);
        let s = StreamSum {
            floor: (t, 0, lsn),
            bytes: 64,
            epoch_floor: 0,
            cold: vec![(
                lsn - 1000,
                ColdStream {
                    end: lsn + 70_000,
                    batches: 1,
                    events: 1,
                    bytes: 64,
                    max_time: t,
                    min: (t, 0, lsn),
                },
            )],
            logs: vec![LogSum {
                writer: "s2".into(),
                epoch: 1,
                cold: vec![Cold {
                    base: lsn - 1000,
                    lo: 0,
                    hi: 1,
                }],
                trimmed: 0,
                contig: 1,
                high: 1,
                ooo: false,
            }],
        };
        let n = encode_stream(&s).len();
        assert!(n < 100, "{n} bytes");
    }

    #[test]
    fn a_number_longer_than_it_needs_or_past_64_bits_is_refused() {
        // Why: one number has one encoding, so a value reads back only as what was written: a
        // longer form of a number, or one past 64 bits, is damage, not some other number.
        let ok = |b: &[u8]| R(b).u64();
        assert_eq!(ok(&[0x7f]), Some(127));
        assert_eq!(ok(&[0x80, 0x01]), Some(128));
        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(ok(&max), Some(u64::MAX));
        assert_eq!(ok(&[0x80, 0x00]), None, "a longer form of 0");
        assert_eq!(ok(&[0xff, 0x80, 0x00]), None, "a longer form of 127");
        let mut past = max;
        past[9] = 0x02;
        assert_eq!(ok(&past), None, "65 bits");
        assert_eq!(ok(&[0x80; 11]), None, "11 bytes");
        assert_eq!(ok(&[0x80]), None, "cut short");
        // the same behind a valid checksum: a checkpoint of that number is refused
        assert_eq!(decode_checkpoint(&sealed(&[0x80, 0x00])), None);
    }

    #[test]
    fn failpoints_fail_a_commit_before_or_after_it_is_durable() {
        let c = Checkpoint {
            cold_end: 7,
            ..Default::default()
        };
        let db = Db::memory().with_failpoint(Some("summary:before_commit"));
        // an operator reading the node's error can tell which store failed, and where
        let e = db.commit(&c, &[]).unwrap_err().to_string();
        assert_eq!(e, "streams database: failpoint summary:before_commit");
        assert_eq!(db.checkpoint().unwrap(), None, "nothing written");
        let db = Db::memory().with_failpoint(Some("summary:after_commit"));
        assert!(db.commit(&c, &[]).is_err());
        assert_eq!(db.checkpoint().unwrap(), Some(c), "durable, then the fault");
        db.share().set_failpoint(Some("summary:read"));
        assert!(
            db.stream("s").is_err() && db.each(|_, _| ()).is_err(),
            "shared by handles"
        );
    }
}
