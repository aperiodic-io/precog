//! Behaviour of the segment log through its public API, including crash and
//! damage scenarios produced by editing the files between runs.

use precog_storage::record::{self, Meta};
use precog_storage::{Config, Error, Log};
use std::path::{Path, PathBuf};

const KB: u64 = 1024;

fn cfg(dir: &Path, segment_size: u64) -> Config {
    let mut c = Config::new(dir);
    c.segment_size = segment_size;
    c.spares = 1;
    c
}

fn meta(i: u64) -> Meta {
    Meta {
        key: format!("log-{}", i % 3).into_bytes(),
        first_seq: i,
        count: 1,
        tag: i as u32,
        time_ms: 1_000 + i,
    }
}

fn payload(i: u64, n: usize) -> Vec<u8> {
    (0..n).map(|j| (i as usize * 31 + j) as u8).collect()
}

/// Appends `groups` groups of `per` records of `size` bytes; returns the number of records.
fn fill(log: &mut Log, start: u64, groups: u64, per: u64, size: usize) -> u64 {
    let mut i = start;
    for _ in 0..groups {
        let items: Vec<(Meta, Vec<u8>)> = (0..per)
            .map(|k| (meta(i + k), payload(i + k, size)))
            .collect();
        let refs: Vec<(Meta, &[u8])> = items.iter().map(|(m, p)| (m.clone(), &p[..])).collect();
        let first = log.append(&refs).unwrap();
        assert_eq!(first, i, "lsns are contiguous");
        i += per;
    }
    i - start
}

fn check_all(log: &Log, from: u64, to: u64, size: usize) {
    let r = log.reader();
    for i in from..to {
        let rec = r
            .read(i)
            .unwrap()
            .unwrap_or_else(|| panic!("lsn {i} missing"));
        assert_eq!(rec.lsn, i);
        assert_eq!(rec.meta, meta(i));
        assert_eq!(
            &rec.payload[..],
            &payload(i, size)[..],
            "payload of lsn {i}"
        );
    }
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            let n = p.file_name().unwrap().to_string_lossy();
            n.starts_with("seg-") && n.ends_with(".log")
        })
        .collect();
    v.sort();
    v
}

fn patch(path: &Path, offset: u64, bytes: &[u8]) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(bytes, offset).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn appends_survive_reopen() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        assert_eq!(log.next_lsn(), 0);
        assert_eq!(fill(&mut log, 0, 10, 3, 500), 30);
        check_all(&log, 0, 30, 500);
        // no clean shutdown: dropping is like a crash after the last ack
    }
    let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 30);
    check_all(&log, 0, 30, 500);
    fill(&mut log, 30, 2, 2, 500);
    check_all(&log, 0, 34, 500);
    assert!(log.reader().read(34).unwrap().is_none());
}

#[test]
fn rolls_segments_and_restart_scans_only_the_active_one() {
    let d = tempfile::tempdir().unwrap();
    let seg = 64 * KB;
    {
        let mut log = Log::open(cfg(d.path(), seg)).unwrap();
        fill(&mut log, 0, 100, 4, 1000); // ~400 KB across ~7 segments
        assert!(log.stats().segments >= 6, "{:?}", log.stats());
    }
    assert!(segment_files(d.path()).len() >= 6);
    let log = Log::open(cfg(d.path(), seg)).unwrap();
    assert_eq!(log.next_lsn(), 400);
    assert!(
        log.stats().recovery_scanned_bytes <= seg,
        "{:?}",
        log.stats()
    );
    check_all(&log, 0, 400, 1000);
}

#[test]
fn torn_final_group_is_dropped_even_if_later_pages_reached_disk() {
    let d = tempfile::tempdir().unwrap();
    let last_group_offset;
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 5, 3, 700);
        let e = log.reader().entries(12, 3);
        last_group_offset = e[0].offset;
        assert_eq!(e.len(), 3);
    }
    // the first record of the last group never made it; the other two did
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, last_group_offset + 8, &[0xFF; 4]);
    let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(
        log.next_lsn(),
        12,
        "the whole unacknowledged group is dropped"
    );
    check_all(&log, 0, 12, 700);
    // A shorter write in its place must not let the old group's tail reappear.
    fill(&mut log, 12, 1, 1, 10);
    drop(log);
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 13);
}

#[test]
fn torn_tail_keeps_the_valid_prefix_of_the_last_group() {
    let d = tempfile::tempdir().unwrap();
    let third;
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 4, 3, 700);
        third = log.reader().entries(11, 1)[0].offset;
    }
    // only the first two records of the last group reached disk
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, third, &vec![0u8; 2000]);
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 11);
    check_all(&log, 0, 11, 700);
}

#[test]
fn damage_followed_by_a_later_group_fails_closed() {
    let d = tempfile::tempdir().unwrap();
    let early;
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 6, 2, 700);
        early = log.reader().entries(4, 1)[0].offset;
    }
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, early + record::HEADER as u64 + 3, &[0x5A]);
    match Log::open(cfg(d.path(), 1 << 20)) {
        Err(Error::Corrupt(msg)) => assert!(msg.contains("later"), "{msg}"),
        Err(e) => panic!("wrong error: {e}"),
        Ok(_) => panic!("opened a log with interior damage"),
    }
}

#[test]
fn sealed_segment_damage_is_refused_on_read() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    fill(&mut log, 0, 100, 2, 1000);
    let e = log.reader().entries(3, 1)[0].clone();
    let first = segment_files(d.path())[0].clone();
    patch(
        &first,
        e.offset + record::HEADER as u64 + e.meta.key.len() as u64 + 10,
        &[0x00, 0x01],
    );
    log.reader().clear_cache();
    assert!(matches!(log.reader().read(3), Err(Error::Corrupt(_))));
    assert!(log.reader().read(150).unwrap().is_some());
}

#[test]
fn missing_index_file_is_rebuilt() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 60, 2, 1000);
    }
    let idx: Vec<_> = std::fs::read_dir(d.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "idx"))
        .collect();
    assert!(idx.len() >= 2);
    std::fs::remove_file(&idx[0]).unwrap();
    std::fs::write(&idx[1], b"garbage").unwrap();
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    check_all(&log, 0, 120, 1000);
    assert!(
        idx[0].exists() && std::fs::metadata(&idx[1]).unwrap().len() > 7,
        "indexes rewritten"
    );
}

#[test]
fn corrupt_newest_manifest_slot_loses_nothing() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 60, 2, 1000);
    }
    // Corrupt whichever slot is newer: try both, one at a time.
    let m = d.path().join("MANIFEST");
    let orig = std::fs::read(&m).unwrap();
    let slot = orig.len() / 2;
    for s in [0, slot] {
        let mut v = orig.clone();
        v[s + 20] ^= 0xFF;
        std::fs::write(&m, &v).unwrap();
        let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        assert_eq!(log.next_lsn(), 120);
        check_all(&log, 0, 120, 1000);
        drop(log);
        std::fs::write(&m, &orig).unwrap();
    }
    let mut v = orig.clone();
    v[20] ^= 0xFF;
    v[slot + 20] ^= 0xFF;
    std::fs::write(&m, &v).unwrap();
    assert!(matches!(
        Log::open(cfg(d.path(), 64 * KB)),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn retention_recycles_segments_without_resurrecting_old_records() {
    let d = tempfile::tempdir().unwrap();
    let seg = 64 * KB;
    let floor;
    {
        let mut log = Log::open(cfg(d.path(), seg)).unwrap();
        fill(&mut log, 0, 150, 2, 1000); // ~300 KB
        let before = log.stats().segments;
        floor = log.retain(Some(3 * seg), None).unwrap();
        assert!(
            floor > 0 && log.stats().segments < before,
            "{:?}",
            log.stats()
        );
        assert!(log.reader().read(floor - 1).unwrap().is_none());
        assert!(log.reader().read(floor).unwrap().is_some());
        assert!(log.stats().recycled >= 1, "{:?}", log.stats());
        // recycled files are reused with fewer, smaller records than they held before
        fill(&mut log, 300, 40, 1, 300);
    }
    let log = Log::open(cfg(d.path(), seg)).unwrap();
    assert_eq!(log.first_lsn(), floor);
    assert_eq!(log.next_lsn(), 340);
    check_all(&log, floor, 300, 1000);
    let r = log.reader();
    for i in 300..340 {
        assert_eq!(r.read(i).unwrap().unwrap().payload.len(), 300);
    }
}

#[test]
fn retention_by_age_drops_only_old_sealed_segments() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    fill(&mut log, 0, 100, 2, 1000); // time_ms = 1000 + lsn
    let floor = log.retain(None, Some(1_000 + 100)).unwrap();
    assert!(floor > 0 && floor <= 100, "floor {floor}");
    assert!(log.reader().read(floor).unwrap().is_some());
    // the active segment is never dropped, however old
    let floor2 = log.retain(None, Some(u64::MAX)).unwrap();
    assert!(log.reader().read(199).unwrap().is_some());
    assert!(floor2 <= 199);
}

#[test]
fn a_failed_fsync_is_fatal() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 1 << 20);
    c.failpoints.fail_data_sync_after = Some(3);
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 3, 1, 100);
    let m = meta(3);
    let p = payload(3, 100);
    assert!(matches!(
        log.append(&[(m.clone(), &p[..])]),
        Err(Error::Fatal(_))
    ));
    assert!(
        matches!(log.append(&[(m, &p[..])]), Err(Error::Fatal(_))),
        "stays fatal"
    );
    drop(log);
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert!(log.next_lsn() >= 3, "acknowledged records survive");
    check_all(&log, 0, 3, 100);
}

#[test]
fn a_crash_at_any_step_of_a_segment_roll_recovers() {
    for step in [
        "roll:after_spare_rename",
        "roll:after_header",
        "roll:after_manifest",
        "roll:after_index",
    ] {
        let d = tempfile::tempdir().unwrap();
        let mut c = cfg(d.path(), 64 * KB);
        c.failpoints.fail_at = Some(step);
        let mut log = Log::open(c).unwrap();
        let mut acked = 0u64;
        for i in 0..200u64 {
            let m = meta(i);
            let p = payload(i, 1000);
            match log.append(&[(m, &p[..])]) {
                Ok(_) => acked += 1,
                Err(_) => break,
            }
        }
        assert!(acked < 200, "{step}: failpoint never fired");
        drop(log);
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap_or_else(|e| panic!("{step}: {e}"));
        assert!(log.next_lsn() >= acked, "{step}");
        check_all(&log, 0, acked, 1000);
        let n = log.next_lsn();
        fill(&mut log, n, 80, 1, 1000);
        drop(log);
        let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        assert_eq!(log.next_lsn(), n + 80, "{step}");
    }
}

#[test]
fn rolls_use_preallocated_spares() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    for _ in 0..10 {
        let n = log.next_lsn();
        fill(&mut log, n, 10, 1, 1000);
        std::thread::sleep(std::time::Duration::from_millis(20)); // let the preallocator catch up
    }
    let s = log.stats();
    assert!(s.segments >= 2 && s.spare_hits >= 1, "{s:?}");
}

#[test]
fn invalid_appends_are_refused_without_side_effects() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert!(matches!(log.append(&[]), Err(Error::Invalid(_))));
    let big = vec![0u8; 70 * 1024];
    assert!(matches!(
        log.append(&[(meta(0), &big[..])]),
        Err(Error::Invalid(_))
    ));
    assert_eq!(log.next_lsn(), 0);
    fill(&mut log, 0, 1, 1, 10);
}

#[test]
fn readers_see_appends_concurrently_and_hit_the_tail_cache() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    let r = log.reader();
    let t = std::thread::spawn(move || {
        let mut seen = 0;
        while seen < 200 {
            if let Some(rec) = r.read(seen).unwrap() {
                assert_eq!(&rec.payload[..], &payload(seen, 500)[..]);
                seen += 1;
            } else {
                std::thread::yield_now();
            }
        }
    });
    fill(&mut log, 0, 200, 1, 500);
    t.join().unwrap();
    assert!(log.stats().cache_hits > 0, "{:?}", log.stats());
}

mod property {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

        /// A crash leaves an arbitrary prefix of the last write on disk. Recovery
        /// keeps exactly the records that are complete, never errors, and the log
        /// stays appendable.
        #[test]
        fn crash_inside_the_last_write_recovers_a_prefix(
            groups in proptest::collection::vec(1usize..5, 1..12),
            size in 1usize..3000,
            cut_frac in 0.0f64..1.0,
        ) {
            let d = tempfile::tempdir().unwrap();
            let mut ends = vec![];
            let (last_start, last_end);
            {
                let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
                let mut i = 0u64;
                for g in &groups {
                    fill(&mut log, i, 1, *g as u64, size);
                    i += *g as u64;
                }
                let n = log.next_lsn();
                let e = log.reader().entries(0, n as usize);
                // a record is complete once its bytes (not its zero padding) are on disk
                for x in &e { ends.push(x.offset + (record::HEADER + x.meta.key.len() + size) as u64); }
                let last_g = *groups.last().unwrap() as u64;
                last_start = e[(n - last_g) as usize].offset;
                last_end = *ends.last().unwrap();
            }
            let cut = last_start + ((last_end - last_start) as f64 * cut_frac) as u64;
            let active = segment_files(d.path()).pop().unwrap();
            patch(&active, cut, &vec![0u8; (last_end - cut) as usize]);
            let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
            let expect = ends.iter().filter(|&&e| e <= cut).count() as u64;
            prop_assert_eq!(log.next_lsn(), expect);
            check_all(&log, 0, expect, size);
            fill(&mut log, expect, 1, 1, 5);
        }
    }
}

#[test]
fn damaged_sealed_segment_without_index_fails_closed() {
    let d = tempfile::tempdir().unwrap();
    let off;
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
        off = log.reader().entries(5, 1)[0].offset;
    }
    let first = segment_files(d.path())[0].clone();
    std::fs::remove_file(first.with_extension("idx")).unwrap();
    patch(&first, off + record::HEADER as u64 + 20, &[0x77]);
    assert!(matches!(
        Log::open(cfg(d.path(), 64 * KB)),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn segments_missing_at_the_head_count_as_retained() {
    // a retention that recycled files but crashed before its manifest write was durable
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 150, 2, 1000);
    }
    let segs = segment_files(d.path());
    std::fs::remove_file(&segs[0]).unwrap();
    std::fs::remove_file(&segs[1]).unwrap();
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert!(log.first_lsn() > 0);
    assert_eq!(log.next_lsn(), 300);
    check_all(&log, log.first_lsn(), 300, 1000);
    // a hole in the middle is not retention
    drop(log);
    let segs = segment_files(d.path());
    std::fs::remove_file(&segs[1]).unwrap();
    assert!(matches!(
        Log::open(cfg(d.path(), 64 * KB)),
        Err(Error::Corrupt(_))
    ));
}

mod flips {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

        /// Arbitrary damage confined to the last write never stops recovery, and what is
        /// recovered is a prefix of what was written, byte for byte.
        #[test]
        fn damage_in_the_last_write_recovers_a_prefix(
            groups in proptest::collection::vec(1usize..4, 1..8),
            flips in proptest::collection::vec((0.0f64..1.0, any::<u8>()), 1..6),
        ) {
            let d = tempfile::tempdir().unwrap();
            let (last_start, last_end, total);
            {
                let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
                let mut i = 0u64;
                for g in &groups {
                    fill(&mut log, i, 1, *g as u64, 400);
                    i += *g as u64;
                }
                total = log.next_lsn();
                let e = log.reader().entries(0, total as usize);
                let last_g = *groups.last().unwrap() as u64;
                last_start = e[(total - last_g) as usize].offset;
                let l = e.last().unwrap();
                last_end = l.offset + (record::HEADER + l.meta.key.len() + 400) as u64;
            }
            let active = segment_files(d.path()).pop().unwrap();
            for (f, x) in flips {
                let at = last_start + ((last_end - last_start - 1) as f64 * f) as u64;
                patch(&active, at, &[x ^ 0xA5]);
            }
            let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
            let n = log.next_lsn();
            prop_assert!(n <= total);
            prop_assert!(n >= total - *groups.last().unwrap() as u64);
            check_all(&log, 0, n, 400);
        }
    }
}
