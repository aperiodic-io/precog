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
fn entries_report_each_payload_length_without_reading_it() {
    // lengths that are not multiples of the 8-byte padding, from sealed and active segments,
    // before and after a restart (index files and the recovery scan)
    let d = tempfile::tempdir().unwrap();
    let lens: Vec<usize> = (0..120).map(|i| 1 + i * 13 % 997).collect();
    let mut log = Log::open(cfg(d.path(), 16 * KB)).unwrap();
    for (i, &n) in lens.iter().enumerate() {
        log.append(&[(meta(i as u64), &payload(i as u64, n)[..])])
            .unwrap();
    }
    for pass in 0..2 {
        let got: Vec<usize> = log
            .reader()
            .entries(0, 1000)
            .unwrap()
            .iter()
            .map(|e| e.payload_len as usize)
            .collect();
        assert_eq!(got, lens, "pass {pass}");
        assert!(log.stats().segments > 2);
        drop(log);
        log = Log::open(cfg(d.path(), 16 * KB)).unwrap();
    }
}

#[test]
fn torn_final_group_is_dropped_even_if_later_pages_reached_disk() {
    let d = tempfile::tempdir().unwrap();
    let last_group_offset;
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 5, 3, 700);
        let e = log.reader().entries(12, 3).unwrap();
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
        third = log.reader().entries(11, 1).unwrap()[0].offset;
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
        early = log.reader().entries(4, 1).unwrap()[0].offset;
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
    let e = log.reader().entries(3, 1).unwrap()[0].clone();
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
    assert!(!log.is_fenced());
    let m = meta(3);
    let p = payload(3, 100);
    assert!(matches!(
        log.append(&[(m.clone(), &p[..])]),
        Err(Error::Fatal(_))
    ));
    assert!(log.is_fenced());
    assert!(matches!(log.retain(Some(0), None), Err(Error::Fatal(_))));
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
                let e = log.reader().entries(0, n as usize).unwrap();
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
        off = log.reader().entries(5, 1).unwrap()[0].offset;
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
                let e = log.reader().entries(0, total as usize).unwrap();
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

#[test]
fn a_fresh_log_is_ready_to_append_without_a_roll() {
    // The first segment is created while opening, so the first append never waits for a
    // spare to be zero-filled (seconds for a 1 GiB segment).
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.stats().segments, 1, "{:?}", log.stats());
    let stalls = log.stats().roll_stalls + log.stats().spare_hits;
    fill(&mut log, 0, 1, 1, 10);
    assert_eq!(
        log.stats().roll_stalls + log.stats().spare_hits,
        stalls,
        "first append rolled"
    );
}

#[test]
fn without_spares_every_new_segment_is_a_counted_stall() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 64 * KB);
    c.spares = 0;
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 200, 1, 1000);
    let s = log.stats();
    assert!(s.segments >= 3, "{s:?}");
    assert_eq!(
        (s.roll_stalls, s.spare_hits),
        (s.segments as u64, 0),
        "{s:?}"
    );
}

#[test]
fn the_tail_cache_is_bounded_and_older_reads_go_to_disk() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 1 << 20);
    c.tail_cache_bytes = 10 * 1024;
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 100, 1, 1000);
    let r = log.reader();
    let before = log.stats().cache_hits;
    r.read(99).unwrap().unwrap();
    assert_eq!(
        log.stats().cache_hits,
        before + 1,
        "recent record served from memory"
    );
    r.read(0).unwrap().unwrap();
    assert_eq!(
        log.stats().cache_hits,
        before + 1,
        "old record read from disk"
    );
    check_all(&log, 0, 100, 1000);
}

#[test]
fn oversized_keys_and_records_are_refused() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 << 20)).unwrap();
    let mut m = meta(0);
    m.key = vec![b'k'; record::MAX_KEY + 1];
    assert!(matches!(log.append(&[(m, b"x")]), Err(Error::Invalid(_))));
    let big = vec![0u8; record::MAX_RECORD];
    assert!(matches!(
        log.append(&[(meta(0), &big[..])]),
        Err(Error::Invalid(_))
    ));
    assert_eq!(log.next_lsn(), 0);
}

#[test]
fn segments_without_a_manifest_are_refused() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 3, 1, 100);
    }
    std::fs::remove_file(d.path().join("MANIFEST")).unwrap();
    assert!(matches!(
        Log::open(cfg(d.path(), 1 << 20)),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn a_segment_header_that_contradicts_the_manifest_is_refused() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    let segs = segment_files(d.path());
    patch(&segs[1], 8, &[0xEE]); // id field of the second segment's header
    assert!(matches!(
        Log::open(cfg(d.path(), 64 * KB)),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn damage_before_a_successor_segment_with_records_fails_closed() {
    // The manifest's newest slot is lost, so the older slot makes the previous segment look
    // active. That segment's last record is damaged, but its successor holds records: the
    // damaged record was acknowledged, so this is interior damage, never a torn tail.
    let d = tempfile::tempdir().unwrap();
    let last_of_prev;
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 70, 2, 1000);
        let e = log.reader().entries(0, 1000).unwrap();
        let last_seg = e.last().unwrap().segment;
        last_of_prev = e
            .iter()
            .rev()
            .find(|x| x.segment != last_seg)
            .unwrap()
            .offset;
    }
    let m = d.path().join("MANIFEST");
    let mut mf = std::fs::read(&m).unwrap();
    let slot = mf.len() / 2;
    let seq = |o: usize| u64::from_le_bytes(mf[o + 8..o + 16].try_into().unwrap());
    let newest = if seq(0) > seq(slot) { 0 } else { slot };
    mf[newest + 20] ^= 0xFF;
    std::fs::write(&m, &mf).unwrap();
    let segs = segment_files(d.path());
    patch(&segs[segs.len() - 2], last_of_prev + 8, &[0xFF; 4]);
    assert!(matches!(
        Log::open(cfg(d.path(), 64 * KB)),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn damage_that_cuts_a_segment_short_never_discards_its_successor() {
    // regression: a successor whose base LSN no longer matched was recycled as a leftover
    let d = tempfile::tempdir().unwrap();
    let mid;
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 70, 2, 1000);
        let e = log.reader().entries(0, 1000).unwrap();
        let last_seg = e.last().unwrap().segment;
        mid = e
            .iter()
            .filter(|x| x.segment != last_seg)
            .rev()
            .nth(5)
            .unwrap()
            .offset;
    }
    let m = d.path().join("MANIFEST");
    let mut mf = std::fs::read(&m).unwrap();
    let slot = mf.len() / 2;
    let seq = |o: usize| u64::from_le_bytes(mf[o + 8..o + 16].try_into().unwrap());
    let newest = if seq(0) > seq(slot) { 0 } else { slot };
    mf[newest + 20] ^= 0xFF;
    std::fs::write(&m, &mf).unwrap();
    let segs = segment_files(d.path());
    patch(&segs[segs.len() - 2], mid + 8, &[0xFF; 4]);
    assert!(matches!(
        Log::open(cfg(d.path(), 64 * KB)),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(
        segment_files(d.path()).len(),
        segs.len(),
        "no segment was recycled"
    );
}

#[test]
fn recovery_handles_torn_groups_beyond_the_first_read_chunk() {
    // 12 MiB of data in a 24 MiB segment: the scan crosses its 8 MiB read chunks.
    let d = tempfile::tempdir().unwrap();
    let seg = 24 << 20;
    let last;
    {
        let mut log = Log::open(cfg(d.path(), seg)).unwrap();
        fill(&mut log, 0, 200, 4, 15_000);
        last = log.reader().entries(796, 1).unwrap()[0].offset;
    }
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, last + 8, &[0xFF; 4]);
    let log = Log::open(cfg(d.path(), seg)).unwrap();
    assert_eq!(log.next_lsn(), 796);
    check_all(&log, 790, 796, 15_000);
}

#[test]
fn opening_and_dropping_never_hangs() {
    // regression: a lost wakeup let the preallocator sleep through shutdown, hanging drop
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let d = tempfile::tempdir().unwrap();
        for i in 0..300u64 {
            let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
            let n = log.next_lsn();
            fill(&mut log, n, 1, 1, 100 + (i as usize % 7) * 100);
        }
        tx.send(()).unwrap();
    });
    rx.recv_timeout(std::time::Duration::from_secs(60))
        .expect("open/drop cycle hung");
}

// ---------------------------------------------------------------------------
// Targeted tests from mutation testing: each pins a check whose mutation survived.

fn seg_header_bytes(id: u64, generation: u64, base: u64) -> Vec<u8> {
    let mut b = vec![];
    b.extend_from_slice(b"PCSG");
    b.extend_from_slice(&1u32.to_le_bytes());
    for v in [id, generation, base] {
        b.extend_from_slice(&v.to_le_bytes());
    }
    let c = crc32c::crc32c(&b);
    b.extend_from_slice(&c.to_le_bytes());
    b
}

fn header_of(seg: &Path) -> (u64, u64, u64) {
    let b = std::fs::read(seg).unwrap();
    let u = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    (u(8), u(16), u(24))
}

#[test]
fn a_clean_restart_uses_the_index_files_and_rebuilds_none() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    let s = log.stats();
    assert_eq!(s.index_rebuilds, 0, "{s:?}");
    assert!(
        s.recovery_scanned_bytes > 0 && s.recovery_scanned_bytes <= 64 * KB,
        "{s:?}"
    );
    drop(log);
    // drop one index: exactly one rebuild, then the rewritten index is used again
    let segs = segment_files(d.path());
    std::fs::remove_file(segs[1].with_extension("idx")).unwrap();
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(log.stats().index_rebuilds, 1);
    // the rebuild read the whole sealed segment on top of the active one
    assert!(
        log.stats().recovery_scanned_bytes >= s.recovery_scanned_bytes + 32 * KB,
        "{:?}",
        log.stats()
    );
    drop(log);
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(
        log.stats().index_rebuilds,
        0,
        "the rebuilt index was written back"
    );
    check_all(&log, 0, 200, 1000);
}

#[test]
fn reader_entries_and_bounds_across_segments() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    fill(&mut log, 0, 100, 2, 1000);
    let r = log.reader();
    assert_eq!((r.first_lsn(), r.next_lsn()), (0, 200));
    let all = r.entries(0, usize::MAX).unwrap();
    assert_eq!(
        all.iter().map(|e| e.lsn).collect::<Vec<_>>(),
        (0..200).collect::<Vec<_>>()
    );
    assert!(
        all.first().unwrap().segment != all.last().unwrap().segment,
        "spans segments"
    );
    for (from, limit) in [(0, 1), (37, 50), (59, 120), (150, 1000), (199, 5)] {
        let e = r.entries(from, limit).unwrap();
        let want: Vec<u64> = (from..(from + limit as u64).min(200)).collect();
        assert_eq!(
            e.iter().map(|x| x.lsn).collect::<Vec<_>>(),
            want,
            "from {from} limit {limit}"
        );
    }
    assert!(r.entries(200, 10).unwrap().is_empty());
    assert!(r.entries(0, 0).unwrap().is_empty());
    let floor = log.retain(Some(2 * 64 * KB), None).unwrap();
    let r = log.reader();
    assert_eq!(r.first_lsn(), floor);
    assert_eq!(
        r.entries(0, 3).unwrap()[0].lsn,
        floor,
        "entries start at the retention floor"
    );
}

#[test]
fn appends_at_the_exact_size_limits() {
    let d = tempfile::tempdir().unwrap();
    let seg = 20 << 20;
    let mut log = Log::open(cfg(d.path(), seg)).unwrap();
    // a key of exactly MAX_KEY and a record of exactly MAX_RECORD bytes are accepted
    let mut m = meta(0);
    m.key = vec![b'k'; record::MAX_KEY];
    log.append(&[(m.clone(), b"x")]).unwrap();
    let big = vec![1u8; record::MAX_RECORD - record::HEADER - 5];
    let mut m5 = meta(1);
    m5.key = b"12345".to_vec();
    log.append(&[(m5, &big[..])]).unwrap();
    assert_eq!(log.next_lsn(), 2);
    let mut c = Config::new(d.path().join("small"));
    c.segment_size = 64 * KB;
    c.tail_cache_bytes = 0; // reads below go to the file, through the in-memory index
    let mut log = Log::open(c.clone()).unwrap();
    let room = 64 * KB as usize - 4096;
    let key = 0usize;
    // one record whose padded size is exactly the room of a segment: fits, no roll
    let exact = vec![2u8; room - record::HEADER - key];
    log.append(&[(Meta::default(), &exact[..])]).unwrap();
    assert_eq!(log.stats().segments, 1, "{:?}", log.stats());
    let too_big = vec![2u8; room - record::HEADER - key + 8];
    assert!(matches!(
        log.append(&[(Meta::default(), &too_big[..])]),
        Err(Error::Invalid(_))
    ));
    // the next append rolls into a new segment
    log.append(&[(Meta::default(), b"next")]).unwrap();
    assert_eq!(log.stats().segments, 2);
    let r = log.reader();
    assert_eq!(r.read(0).unwrap().unwrap().payload.len(), exact.len());
    assert_eq!(&r.read(1).unwrap().unwrap().payload[..], b"next");
    let s = log.stats();
    assert_eq!(s.bytes, (room + record::encoded_len(0, 4)) as u64, "{s:?}");
    assert_eq!(
        s.log_bytes,
        64 * KB + 4096 + record::encoded_len(0, 4) as u64
    );
}

#[test]
fn retention_boundaries_are_exact() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 64 * KB);
    c.spares = 0;
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 100, 2, 1000);
    let total = log.stats().log_bytes;
    assert_eq!(
        log.retain(Some(total), None).unwrap(),
        0,
        "at the limit: nothing dropped"
    );
    let segs_before = log.stats().segments;
    let floor = log.retain(Some(total - 1), None).unwrap();
    assert!(floor > 0);
    assert_eq!(
        log.stats().segments,
        segs_before - 1,
        "one byte over: exactly one segment dropped"
    );
    // by age: the next segment's newest record has time 1000 + (its last lsn)
    let e = log.reader().entries(floor, 1000).unwrap();
    let first_seg = e[0].segment;
    let last_lsn = e.iter().rfind(|x| x.segment == first_seg).unwrap().lsn;
    assert_eq!(
        log.retain(None, Some(1000 + last_lsn)).unwrap(),
        floor,
        "not older than the cutoff"
    );
    let f2 = log.retain(None, Some(1000 + last_lsn + 1)).unwrap();
    assert_eq!(f2, last_lsn + 1, "older by one: that segment goes");
}

#[test]
fn retention_recycles_at_most_two_files_per_spare_slot() {
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 64 * KB);
    c.spares = 0; // no preallocator: the pool holds only recycled files
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 200, 2, 1000);
    let segs = log.stats().segments;
    assert!(segs >= 7);
    log.retain(Some(64 * KB), None).unwrap();
    assert_eq!(log.stats().recycled, 2, "{:?}", log.stats());
    let spares = std::fs::read_dir(d.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("spare-")
        })
        .count();
    assert_eq!(spares, 2);
}

#[test]
fn wrong_sized_spares_are_discarded_on_open() {
    let d = tempfile::tempdir().unwrap();
    drop(Log::open(cfg(d.path(), 64 * KB)).unwrap());
    std::fs::write(d.path().join("spare-00000099.log"), b"short").unwrap();
    std::fs::write(d.path().join("spare-tmp-5"), b"partial").unwrap();
    drop(Log::open(cfg(d.path(), 64 * KB)).unwrap());
    assert!(!d.path().join("spare-00000099.log").exists());
    assert!(!d.path().join("spare-tmp-5").exists());
}

#[test]
fn a_header_with_a_valid_checksum_but_the_wrong_identity_is_refused() {
    for field in ["generation", "base"] {
        let d = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
            fill(&mut log, 0, 100, 2, 1000);
        }
        let segs = segment_files(d.path());
        let (id, g, b) = header_of(&segs[1]);
        let h = match field {
            "generation" => seg_header_bytes(id, g + 1, b),
            _ => seg_header_bytes(id, g, b + 1),
        };
        patch(&segs[1], 0, &h);
        assert!(
            matches!(Log::open(cfg(d.path(), 64 * KB)), Err(Error::Corrupt(_))),
            "{field}"
        );
    }
}

#[test]
fn a_recycled_head_segment_moves_the_floor_exactly() {
    // retention renamed the oldest segment's file away but its manifest write was lost: the
    // file now holds another header (here: zeros)
    let d = tempfile::tempdir().unwrap();
    let second_base;
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
        let e = log.reader().entries(0, 1000).unwrap();
        second_base = e.iter().find(|x| x.segment != e[0].segment).unwrap().lsn;
    }
    let segs = segment_files(d.path());
    patch(&segs[0], 0, &[0u8; 64]);
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(log.first_lsn(), second_base);
    assert!(log.reader().read(second_base - 1).unwrap().is_none());
    check_all(&log, second_base, 200, 1000);
    drop(log);
    // a missing head file (not just a recycled one) gives the same floor
    let d2 = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d2.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    std::fs::remove_file(&segment_files(d2.path())[0]).unwrap();
    assert_eq!(
        Log::open(cfg(d2.path(), 64 * KB)).unwrap().first_lsn(),
        second_base
    );
}

/// Writes a well-formed record of the active segment's generation right after its last record.
fn plant(d: &Path, lsn_delta: i64, group_delta: i64) -> u64 {
    let active = segment_files(d).pop().unwrap();
    let (_, generation, base) = header_of(&active);
    let data = std::fs::read(&active).unwrap();
    let mut off = 4096usize;
    let mut n = 0u64;
    while let record::Decoded::Record { next, .. } = record::decode(&data, off, generation) {
        off = next;
        n += 1;
    }
    let mut buf = vec![];
    let lsn = (base + n) as i64 + lsn_delta;
    let group = off as i64 + group_delta;
    record::encode(
        &mut buf,
        generation,
        group as u64,
        lsn as u64,
        &meta(9),
        b"planted",
    );
    patch(&active, off as u64, &buf);
    off as u64
}

#[test]
fn records_out_of_sequence_or_outside_their_group_end_the_log() {
    for (lsn_delta, group_delta) in [(5, 0), (0, 8)] {
        let d = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
            fill(&mut log, 0, 10, 1, 100);
        }
        let at = plant(d.path(), lsn_delta, group_delta);
        let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        assert_eq!(log.next_lsn(), 10, "lsn {lsn_delta} group {group_delta}");
        drop(log);
        // it was written (same generation): recovery zeroes it so it can never come back
        let active = segment_files(d.path()).pop().unwrap();
        let b = std::fs::read(&active).unwrap();
        assert!(
            b[at as usize..at as usize + 64].iter().all(|&x| x == 0),
            "planted record left on disk"
        );
    }
}

/// The active segment's end after `fill`: (path, generation, next lsn, offset).
fn active_end(d: &Path) -> (PathBuf, u64, u64, u64) {
    let active = segment_files(d).pop().unwrap();
    let (_, generation, base) = header_of(&active);
    let data = std::fs::read(&active).unwrap();
    let (mut off, mut n) = (4096usize, 0u64);
    while let record::Decoded::Record { next, .. } = record::decode(&data, off, generation) {
        off = next;
        n += 1;
    }
    (active, generation, base + n, off as u64)
}

#[test]
fn a_torn_group_is_zeroed_no_further_than_its_last_record() {
    // A two-record group whose first record is damaged: both records go, and recovery writes
    // nothing past the second record's header (it must not zero-fill the rest of the segment).
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 10, 1, 100);
    }
    let (active, generation, lsn, x) = active_end(d.path());
    let mut group = vec![];
    record::encode(&mut group, generation, x, lsn, &meta(10), &[1; 200]);
    let second = x + group.len() as u64;
    record::encode(&mut group, generation, x, lsn + 1, &meta(11), &[2; 200]);
    group[100] ^= 0xff; // first record's payload
    patch(&active, x, &group);
    let marker = second + 128 * KB;
    patch(&active, marker, &[0xab; 8]);
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 10);
    drop(log);
    let b = std::fs::read(&active).unwrap();
    let at = |o: u64, n: usize| &b[o as usize..o as usize + n];
    assert!(at(second, 64).iter().all(|&v| v == 0), "torn record left");
    assert_eq!(at(marker, 8), &[0xab; 8], "zeroed past the torn group");
}

#[test]
fn recovery_looks_for_later_groups_within_two_groups_and_a_record_of_a_torn_write() {
    // Past a torn write at x, a valid record of a later group within 2 * 32 MiB + 16 MiB means
    // interior damage; further out it cannot belong to this log's history and is ignored.
    const MIB: u64 = 1 << 20;
    for (distance, corrupt) in [(78 * MIB, true), (82 * MIB, false)] {
        let d = tempfile::tempdir().unwrap();
        let mut c = cfg(d.path(), 128 * MIB);
        c.spares = 0;
        {
            let mut log = Log::open(c.clone()).unwrap();
            fill(&mut log, 0, 10, 1, 100);
        }
        let (active, generation, lsn, x) = active_end(d.path());
        let mut torn = vec![];
        record::encode(&mut torn, generation, x, lsn, &meta(10), &[1; 200]);
        torn[100] ^= 0xff;
        patch(&active, x, &torn);
        let at = x + distance;
        let mut later = vec![];
        record::encode(&mut later, generation, at, lsn + 5, &meta(15), &[3; 200]);
        patch(&active, at, &later);
        match Log::open(c) {
            Err(Error::Corrupt(_)) => assert!(corrupt, "{distance}: refused"),
            Ok(log) => {
                assert!(!corrupt, "{distance}: opened");
                assert_eq!(log.next_lsn(), 10);
            }
            Err(e) => panic!("{e}"),
        }
    }
}

#[test]
fn stale_records_of_another_generation_are_left_alone() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 10, 1, 100);
    }
    let active = segment_files(d.path()).pop().unwrap();
    let (_, generation, _) = header_of(&active);
    let data = std::fs::read(&active).unwrap();
    let mut off = 4096usize;
    while let record::Decoded::Record { next, .. } = record::decode(&data, off, generation) {
        off = next;
    }
    let mut buf = vec![];
    record::encode(&mut buf, generation + 7, off as u64, 10, &meta(9), b"stale");
    patch(&active, off as u64, &buf);
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 10);
    drop(log);
    let b = std::fs::read(&active).unwrap();
    assert_eq!(
        &b[off..off + buf.len()],
        &buf[..],
        "a clean end: nothing to zero"
    );
}

#[test]
fn adopted_segments_keep_generations_unique() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 70, 2, 1000);
    }
    let m = d.path().join("MANIFEST");
    let mut mf = std::fs::read(&m).unwrap();
    let slot = mf.len() / 2;
    let seq = |b: &[u8], o: usize| u64::from_le_bytes(b[o + 8..o + 16].try_into().unwrap());
    let newest = if seq(&mf, 0) > seq(&mf, slot) {
        0
    } else {
        slot
    };
    mf[newest + 20] ^= 0xFF;
    std::fs::write(&m, &mf).unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert!(
        log.stats().recovery_scanned_bytes > 64 * KB
            && log.stats().recovery_scanned_bytes <= 2 * 64 * KB,
        "two segments scanned, one adopted: {}",
        log.stats().recovery_scanned_bytes
    );
    fill(&mut log, 140, 60, 2, 1000); // more rolls after the adoption
    drop(log);
    let gens: Vec<u64> = segment_files(d.path())
        .iter()
        .map(|p| header_of(p).1)
        .collect();
    let mut u = gens.clone();
    u.sort();
    u.dedup();
    assert_eq!(u.len(), gens.len(), "generations {gens:?}");
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    check_all(&log, 0, 260, 1000);
}

#[test]
fn recovery_of_an_active_segment_larger_than_its_read_buffer() {
    // ~26 MiB in the active segment: the scan refills past its first 16 MiB read, and the
    // torn-group search continues past what the scan had buffered
    let d = tempfile::tempdir().unwrap();
    let seg = 48 << 20;
    let last;
    {
        let mut log = Log::open(cfg(d.path(), seg)).unwrap();
        fill(&mut log, 0, 440, 4, 15_000);
        last = log.reader().entries(1756, 4).unwrap();
    }
    let active = segment_files(d.path()).pop().unwrap();
    // the first record of the last group is damaged, the rest of the group reached disk
    patch(&active, last[0].offset + 8, &[0xFF; 4]);
    let log = Log::open(cfg(d.path(), seg)).unwrap();
    assert_eq!(log.next_lsn(), 1756);
    check_all(&log, 1700, 1756, 15_000);
    let s = log.stats();
    assert!(s.recovery_scanned_bytes > 16 << 20, "{s:?}");
}

#[test]
fn interior_damage_far_from_the_end_of_a_large_segment_fails_closed() {
    let d = tempfile::tempdir().unwrap();
    let seg = 48 << 20;
    let early;
    {
        let mut log = Log::open(cfg(d.path(), seg)).unwrap();
        fill(&mut log, 0, 400, 4, 15_000);
        early = log.reader().entries(1200, 1).unwrap()[0].offset; // ~18 MiB in
    }
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, early + 8, &[0xFF; 4]);
    assert!(matches!(
        Log::open(cfg(d.path(), seg)),
        Err(Error::Corrupt(_))
    ));
}

/// Rewrites an index file's body (header, then 40-byte entries by LSN) under a fixed CRC.
fn rewrite_index(path: &Path, f: impl FnOnce(&mut Vec<u8>)) {
    let b = std::fs::read(path).unwrap();
    let mut body = b[..b.len() - 4].to_vec();
    f(&mut body);
    let c = crc32c::crc32c(&body);
    body.extend_from_slice(&c.to_le_bytes());
    std::fs::write(path, body).unwrap();
}

#[test]
fn an_index_pointing_at_the_wrong_record_is_caught_on_read() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    let idx = segment_files(d.path())[0].with_extension("idx");
    // swap the offsets of records 0 and 1
    rewrite_index(&idx, |b| {
        let (x, y) = (b[40..48].to_vec(), b[80..88].to_vec());
        b[40..48].copy_from_slice(&y);
        b[80..88].copy_from_slice(&x);
    });
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(
        log.stats().index_rebuilds,
        0,
        "the index is well-formed, so it is used"
    );
    assert!(
        matches!(log.reader().read(0), Err(Error::Corrupt(_))),
        "record 1 is not served as record 0"
    );
}

#[test]
fn an_index_with_the_wrong_number_of_entries_is_rebuilt() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    let idx = segment_files(d.path())[0].with_extension("idx");
    // one entry fewer than the segment holds, consistently
    rewrite_index(&idx, |b| {
        let n = u32::from_le_bytes(b[32..36].try_into().unwrap());
        b[32..36].copy_from_slice(&(n - 1).to_le_bytes());
    });
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(log.stats().index_rebuilds, 1);
    check_all(&log, 0, 200, 1000);
}

#[test]
fn a_crash_right_after_renaming_a_recycled_spare_recovers() {
    // A recycled spare still holds its old segment's valid header. If the roll dies before the
    // new header is written, the renamed file carries another segment's identity: recycle it.
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 64 * KB);
    c.spares = 0; // the pool holds only recycled files
    {
        let mut log = Log::open(c.clone()).unwrap();
        fill(&mut log, 0, 300, 1, 1000);
        log.retain(Some(2 * 64 * KB), None).unwrap();
        assert!(log.stats().recycled >= 1);
    }
    c.failpoints.fail_at = Some("roll:after_spare_rename");
    let mut log = Log::open(c.clone()).unwrap();
    let mut i = log.next_lsn();
    loop {
        let (m, p) = (meta(i), payload(i, 1000));
        if log.append(&[(m, &p[..])]).is_err() {
            break;
        }
        i += 1;
    }
    let acked = i;
    drop(log);
    c.failpoints.fail_at = None;
    let mut log = Log::open(c).unwrap();
    assert_eq!(log.next_lsn(), acked);
    let first = log.first_lsn();
    check_all(&log, first, acked, 1000);
    fill(&mut log, acked, 100, 1, 1000);
}

#[test]
fn both_manifest_slots_are_written_alternately() {
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    let mf = std::fs::read(d.path().join("MANIFEST")).unwrap();
    let slot = mf.len() / 2;
    let seq = |o: usize| u64::from_le_bytes(mf[o + 8..o + 16].try_into().unwrap());
    let (a, b) = (seq(0), seq(slot));
    assert!(
        a > 2 && b > 2 && a.abs_diff(b) == 1,
        "slots at seq {a} and {b}"
    );
}

#[test]
fn a_maximal_record_reads_back_after_a_restart() {
    let d = tempfile::tempdir().unwrap();
    let big = vec![9u8; record::MAX_RECORD - record::HEADER];
    {
        let mut log = Log::open(cfg(d.path(), 20 << 20)).unwrap();
        log.append(&[(Meta::default(), &big[..])]).unwrap();
    }
    let log = Log::open(cfg(d.path(), 20 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 1);
    assert_eq!(
        log.reader().read(0).unwrap().unwrap().payload.len(),
        big.len()
    );
}

#[test]
fn a_damaged_final_record_is_dropped_and_zeroed() {
    let d = tempfile::tempdir().unwrap();
    let off;
    {
        let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
        fill(&mut log, 0, 10, 1, 100);
        off = log.reader().entries(9, 1).unwrap()[0].offset;
    }
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, off + record::HEADER as u64 + 30, &[0xEE]); // payload: the checksum fails
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 9);
    drop(log);
    let b = std::fs::read(&active).unwrap();
    assert!(
        b[off as usize..off as usize + 64].iter().all(|&x| x == 0),
        "its header was zeroed"
    );
}

#[test]
fn spares_are_filled_at_a_paced_rate_in_the_background() {
    // A spare zero-filled at full speed saturates the disk and the log's own fsyncs queue
    // behind it (p99 acks of 400 ms in bench/compare at each roll); the preallocator paces itself.
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 4 << 20);
    c.prealloc_bytes_per_s = 4 << 20;
    let _log = Log::open(c).unwrap();
    let spares = || {
        std::fs::read_dir(d.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("spare-0")
            })
            .count()
    };
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(
        spares(),
        0,
        "a 4 MiB spare at 4 MiB/s is not ready after 300 ms"
    );
    let t = std::time::Instant::now();
    while spares() == 0 && t.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(spares(), 1, "ready within a few seconds");
}

fn deferred(dir: &Path, segment_size: u64, bytes: u64) -> Config {
    let mut c = cfg(dir, segment_size);
    c.flush_delay_ms = 100;
    c.flush_bytes = bytes;
    c
}

#[test]
fn deferred_flush_never_fsyncs_in_append_and_falls_due_at_once_past_the_bytes_threshold() {
    // Why: Redpanda's write caching (live-ingestion runs it) acks after replication and fsyncs on a
    // 100 ms / 256 KiB cadence; matching it means a group is acknowledged once written, and the
    // fsync cost is paid per threshold, not per group. Under write caching the append runs on the
    // caller's network thread under the log's lock, so the fsync is never in it: past the
    // threshold the flush is due at once and the caller's flusher runs it.
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(deferred(d.path(), 1 << 20, 64 * KB)).unwrap();
    fill(&mut log, 0, 10, 1, 1000); // ~10 KiB: under the threshold
    check_all(&log, 0, 10, 1000); // readable once written
    let due = log.flush_due().expect("unsynced data has a flush deadline");
    assert!(due > std::time::Instant::now(), "not due before the delay");
    fill(&mut log, 10, 60, 1, 1000); // past 64 KiB
    assert_eq!(log.stats().syncs, 0, "no fsync in an append");
    assert!(
        log.flush_due().unwrap() <= std::time::Instant::now(),
        "due at once past the threshold"
    );
    log.flush().unwrap();
    assert_eq!(log.stats().syncs, 1);
    assert!(log.flush_due().is_none(), "nothing left to flush");
    log.flush().unwrap();
    assert_eq!(log.stats().syncs, 1, "flushing a synced log is free");
}

#[test]
fn a_flush_run_outside_the_log_lets_appends_go_on_and_leaves_them_due() {
    // Why: the deferred fsync runs on the flusher thread without the log's lock, so the writers
    // appending meanwhile never wait for the disk; what they write during it is not covered and
    // keeps its own deadline.
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(deferred(d.path(), 1 << 20, 1 << 20)).unwrap();
    assert!(log.begin_flush().unwrap().is_none(), "nothing to flush");
    fill(&mut log, 0, 5, 1, 100);
    let f = log.begin_flush().unwrap().expect("unsynced groups");
    assert!(
        log.flush_due().is_none(),
        "the flush covers what was written"
    );
    let before = std::time::Instant::now();
    fill(&mut log, 5, 5, 1, 100); // while the fsync runs
    let r = f.sync();
    log.end_flush(f, r).unwrap();
    assert_eq!(log.stats().syncs, 1);
    let due = log.flush_due().expect("groups written during the flush");
    assert!(due >= before + std::time::Duration::from_millis(100));
    check_all(&log, 0, 10, 100);
}

#[test]
fn unsynced_bytes_stay_bounded_while_a_flush_is_out() {
    // Why: recovery tells a lost unsynced tail from interior damage by its size (the kernel writes
    // unsynced pages back in any order), so a crash must never leave more than
    // `Log::unsynced_bound` unsynced. Appends go on while the flusher's fsync runs outside the
    // log, so past twice `flush_bytes` (what that fsync covers plus as much again) an append
    // syncs itself.
    let d = tempfile::tempdir().unwrap();
    let c = deferred(d.path(), 1 << 20, 4 * KB);
    assert_eq!(
        Log::unsynced_bound(&c),
        8 * KB + precog_storage::log::MAX_GROUP as u64
    );
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 5, 1, 1000); // past flush_bytes: the flush is due, the append does not sync
    assert_eq!(log.stats().syncs, 0);
    let f = log.begin_flush().unwrap().unwrap();
    fill(&mut log, 5, 2, 1, 1000); // ~7 KiB not yet synced, the fsync still out
    assert_eq!(log.stats().syncs, 0, "below twice flush_bytes");
    fill(&mut log, 7, 2, 1, 1000); // past twice flush_bytes
    assert_eq!(log.stats().syncs, 1, "the append synced the bound back");
    let r = f.sync();
    log.end_flush(f, r).unwrap();
    check_all(&log, 0, 9, 1000);
}

#[test]
fn a_failed_flush_run_outside_the_log_fences_it() {
    // Why: fsyncgate, as for a flush under the lock: whatever was acknowledged since rests on the
    // replicas, and the log must refuse everything after the failure is known.
    let d = tempfile::tempdir().unwrap();
    let mut c = deferred(d.path(), 1 << 20, 1 << 20);
    c.failpoints.fail_data_sync_after = Some(0);
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 2, 1, 100);
    let f = log.begin_flush().unwrap().unwrap();
    let r = f.sync();
    assert!(r.is_err());
    fill(&mut log, 2, 1, 1, 100); // not known yet
    assert!(matches!(log.end_flush(f, r), Err(Error::Fatal(_))));
    assert!(log.is_fenced());
    let m = meta(3);
    assert!(log.append(&[(m, &payload(3, 100)[..])]).is_err());
    assert!(matches!(log.begin_flush(), Err(Error::Fatal(_))));
}

#[test]
fn a_roll_while_a_flush_is_out_syncs_the_segment_it_seals() {
    // Why: the flush outside the lock may still be running (or fail) when a writer rolls; the
    // roll cannot count on it, and a sealed segment is never written again.
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(deferred(d.path(), 64 * KB, 1 << 30)).unwrap();
    fill(&mut log, 0, 40, 1, 1000);
    let f = log.begin_flush().unwrap().unwrap();
    assert_eq!(log.stats().segments, 1);
    let big = payload(40, 30_000); // does not fit: rolls
    log.append(&[(meta(40), &big[..])]).unwrap();
    assert_eq!(log.stats().segments, 2);
    assert_eq!(
        log.stats().syncs,
        1,
        "the roll synced the segment it sealed"
    );
    let r = f.sync();
    log.end_flush(f, r).unwrap();
    assert_eq!(log.stats().syncs, 2);
}

#[test]
fn deferred_flush_keeps_written_groups_across_a_process_crash() {
    // Why: an unsynced group is in the page cache; a process crash (not a power loss) loses nothing.
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(deferred(d.path(), 1 << 20, 1 << 20)).unwrap();
        fill(&mut log, 0, 20, 2, 300);
        assert_eq!(log.stats().syncs, 0);
    }
    let log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    assert_eq!(log.next_lsn(), 40);
    check_all(&log, 0, 40, 300);
}

#[test]
fn a_roll_syncs_the_segment_it_seals() {
    // Why: a sealed segment is never written again, so nothing would ever flush its unsynced tail.
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(deferred(d.path(), 64 * KB, 1 << 30)).unwrap();
    fill(&mut log, 0, 200, 1, 1000); // ~3 segments
    assert!(log.stats().segments >= 3);
    assert!(
        log.stats().syncs >= log.stats().segments as u64 - 1,
        "each sealed segment synced"
    );
}

#[test]
fn a_failed_deferred_flush_fences_the_log() {
    // Why: fsyncgate: after a failed fsync the page cache may have dropped the data; acking more on
    // top of it would hide the loss.
    let d = tempfile::tempdir().unwrap();
    let mut c = deferred(d.path(), 1 << 20, 1 << 20);
    c.failpoints.fail_data_sync_after = Some(0);
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 2, 1, 100);
    assert!(matches!(log.flush(), Err(Error::Fatal(_))));
    assert!(log.is_fenced());
    let m = meta(2);
    assert!(log.append(&[(m, &payload(2, 100)[..])]).is_err());
}

#[test]
fn at_its_retention_budget_the_log_reuses_recycled_segments_instead_of_zero_filling() {
    // Why: once retention recycles a segment per roll, a zero-filled spare made meanwhile is
    // deleted unused: the preallocator wrote a whole segment of zeros for nothing (11% of a
    // broker's CPU at 100 MB/s). It must wait for the recycle instead.
    let d = tempfile::tempdir().unwrap();
    let seg = 64 * KB;
    let mut c = cfg(d.path(), seg);
    c.prealloc_bytes_per_s = 0;
    let mut log = Log::open(c).unwrap();
    let mut i = 0;
    for _ in 0..10 {
        i += fill(&mut log, i, 30, 2, 1000); // ~1 segment per round
        log.retain(Some(3 * seg), None).unwrap();
    }
    assert!(log.stats().recycled > 0, "{:?}", log.stats());
    let (made, stalls) = (log.stats().spares_made, log.stats().roll_stalls);
    for _ in 0..20 {
        i += fill(&mut log, i, 30, 2, 1000);
        log.retain(Some(3 * seg), None).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let s = log.stats();
    assert_eq!(
        s.spares_made, made,
        "zero-filled spares at steady state: {s:?}"
    );
    assert_eq!(
        s.roll_stalls, stalls,
        "every roll found a recycled segment: {s:?}"
    );
}

#[test]
fn a_growing_log_keeps_a_zero_filled_spare_ready() {
    // Why: before retention recycles anything, a roll without a spare fills one on the write path.
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 64 * KB);
    c.prealloc_bytes_per_s = 0;
    let mut log = Log::open(c).unwrap();
    for k in 0..5u64 {
        fill(&mut log, k * 60, 30, 2, 1000);
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let s = log.stats();
    assert!(s.spares_made >= 4, "{s:?}");
    // only the first segment, made at open before any spare exists
    assert!(s.roll_stalls <= 1, "{s:?}");
}

#[test]
fn a_spare_gone_since_the_directory_was_listed_is_skipped_but_other_errors_stop_the_open() {
    // A closing log's paced preallocator can rename or remove a spare while the next open lists
    // the directory (a quick restart in one process). A dangling link stands in for a spare
    // listed and then gone; a link loop for a spare that cannot be examined at all.
    let d = tempfile::tempdir().unwrap();
    drop(Log::open(cfg(d.path(), 1 << 20)).unwrap());
    let link = d.path().join("spare-00000099.log");
    std::os::unix::fs::symlink(d.path().join("gone"), &link).unwrap();
    let mut log = Log::open(cfg(d.path(), 1 << 20)).unwrap();
    log.append(&[(meta(0), &b"x"[..])]).unwrap();
    drop(log);
    std::fs::remove_file(&link).unwrap();
    std::os::unix::fs::symlink(&link, &link).unwrap();
    assert!(
        Log::open(cfg(d.path(), 1 << 20)).is_err(),
        "not a vanished spare"
    );
}

fn spare_files(dir: &Path) -> Vec<std::fs::Metadata> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_name().to_string_lossy().starts_with("spare-0"))
        .map(|e| e.metadata().unwrap())
        .collect()
}

#[test]
fn an_unpaced_spare_is_ready_at_once_and_exactly_a_segment_long() {
    // rate 0 turns pacing off; a spare becomes a segment, so it is a segment's size, also when
    // that is not a whole number of fill chunks
    let d = tempfile::tempdir().unwrap();
    let size = (4 << 20) + (1 << 19);
    let mut c = cfg(d.path(), size);
    c.prealloc_bytes_per_s = 0;
    let _log = Log::open(c).unwrap();
    let t = std::time::Instant::now();
    while spare_files(d.path()).is_empty() && t.elapsed() < std::time::Duration::from_secs(5) {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let s = spare_files(d.path());
    assert_eq!(s.len(), 1, "ready within seconds");
    assert_eq!(s[0].len(), size);
}

#[test]
fn a_roll_that_finds_no_spare_fills_one_at_full_speed() {
    // the append path waits for this spare: pacing it would stall writes for seconds
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 4 << 20);
    c.spares = 0; // nothing prepared: every segment is made when it is needed
    c.prealloc_bytes_per_s = 1 << 20; // a paced 4 MiB spare would take 4 s
    let t = std::time::Instant::now();
    let mut log = Log::open(c).unwrap();
    let rec = vec![1u8; 64 << 10];
    for i in 0..80 {
        log.append(&[(meta(i), &rec[..])]).unwrap(); // 5 MiB: one roll
    }
    assert!(log.stats().segments >= 2, "{:?}", log.stats());
    assert!(
        t.elapsed() < std::time::Duration::from_secs(3),
        "{:?}",
        t.elapsed()
    );
}

#[test]
fn the_deferred_flush_is_due_its_delay_after_the_first_unsynced_write() {
    // the ack-before-fsync window is bounded by flush_delay_ms: never longer, and not shorter
    // (an earlier flush would pay the fsync per group that write caching exists to avoid)
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(deferred(d.path(), 1 << 20, 1 << 20)).unwrap();
    let before = std::time::Instant::now();
    fill(&mut log, 0, 1, 1, 100);
    let after = std::time::Instant::now();
    let due = log.flush_due().unwrap();
    let delay = std::time::Duration::from_millis(100);
    assert!(due >= before + delay && due <= after + delay);
}

#[test]
fn at_its_retention_budget_the_preallocator_waits_out_recycles_and_still_stops_promptly() {
    // Why: a zero-filled spare made while recycles keep the pool full is deleted unused, also
    // once the preallocator's recycle wait (5 s) has run out; and a closing log must not sit out
    // that wait.
    let d = tempfile::tempdir().unwrap();
    let seg = 64 * KB;
    let mut c = cfg(d.path(), seg);
    c.prealloc_bytes_per_s = 0;
    let mut log = Log::open(c).unwrap();
    let mut i = 0;
    for _ in 0..10 {
        i += fill(&mut log, i, 30, 2, 1000);
        log.retain(Some(3 * seg), None).unwrap();
    }
    // rolls take every spare: the preallocator waits for a recycle
    let drain = |log: &mut Log, i: &mut u64| {
        while !spare_files(d.path()).is_empty() {
            *i += fill(log, *i, 5, 1, 1000);
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    drain(&mut log, &mut i);
    let made = log.stats().spares_made;
    // exactly one recycle fills the pool to its target
    log.retain(Some(log.stats().log_bytes - seg / 2), None)
        .unwrap();
    assert_eq!(spare_files(d.path()).len(), 1);
    std::thread::sleep(std::time::Duration::from_millis(5_500));
    assert_eq!(log.stats().spares_made, made, "{:?}", log.stats());
    drain(&mut log, &mut i);
    let t = std::time::Instant::now();
    drop(log);
    assert!(
        t.elapsed() < std::time::Duration::from_secs(2),
        "{:?}",
        t.elapsed()
    );
}

#[test]
fn a_spare_of_the_right_size_is_reused_after_a_restart() {
    // a restart must not throw away a ready spare and zero-fill another
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 1 << 20);
    c.prealloc_bytes_per_s = 0;
    let names = || -> Vec<String> {
        std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .filter(|n| n.starts_with("spare-0"))
            .collect()
    };
    {
        let _log = Log::open(c.clone()).unwrap();
        let t = std::time::Instant::now();
        while names().is_empty() && t.elapsed() < std::time::Duration::from_secs(5) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    let before = names();
    assert_eq!(before.len(), 1);
    let _log = Log::open(c).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(names(), before, "the same spare, not a new one");
}

#[test]
fn spare_names_keep_counting_up_across_a_restart() {
    // a name below one already on disk could be reused and its rename overwrite a pooled spare
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 64 * 1024);
    c.prealloc_bytes_per_s = 0;
    let numbers = || -> Vec<u64> {
        let mut v: Vec<u64> = std::fs::read_dir(d.path())
            .unwrap()
            .filter_map(|e| {
                let n = e.unwrap().file_name().to_string_lossy().to_string();
                n.strip_prefix("spare-")?.strip_suffix(".log")?.parse().ok()
            })
            .collect();
        v.sort();
        v
    };
    let wait = |want: usize| {
        let t = std::time::Instant::now();
        while numbers().len() < want && t.elapsed() < std::time::Duration::from_secs(5) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };
    {
        let _log = Log::open(c.clone()).unwrap();
        wait(1);
    }
    let old = numbers();
    assert_eq!(old.len(), 1);
    let mut log = Log::open(c).unwrap();
    fill(&mut log, 0, 40, 2, 1000); // ~80 KiB: a roll takes the old spare
    std::thread::sleep(std::time::Duration::from_millis(100));
    wait(1);
    let new = numbers();
    assert!(
        !new.is_empty() && new.iter().all(|&n| n > old[0]),
        "{old:?} then {new:?}"
    );
}

#[test]
fn sealed_segments_serve_each_keys_records_from_their_index_file() {
    // Why: the node looks up a writer log's older batches here (one read of that key's run in a
    // segment's index file) instead of holding every record's index entry in RAM. The answer
    // must match the records exactly, survive a restart, and drop with the segment.
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    fill(&mut log, 0, 300, 1, 1000);
    let keys: Vec<Vec<u8>> = (0..3).map(|i| format!("log-{i}").into_bytes()).collect();
    let check = |log: &Log| {
        let r = log.reader();
        let sealed = r.sealed();
        assert!(sealed.len() >= 3, "{sealed:?}");
        assert_eq!(sealed[0].0, log.first_lsn());
        for w in sealed.windows(2) {
            assert_eq!(w[0].1, w[1].0, "contiguous");
        }
        let active = sealed.last().unwrap().1;
        assert!(active < log.next_lsn(), "the active segment is not sealed");
        for &(base, end) in &sealed {
            let all = r.entries(base, (end - base) as usize).unwrap();
            for key in &keys {
                let got = r.by_key(base, key).unwrap();
                let want: Vec<_> = all.iter().filter(|e| &e.meta.key == key).collect();
                assert!(!got.is_empty());
                assert_eq!(got.len(), want.len());
                for (g, w) in got.iter().zip(want) {
                    assert_eq!(
                        (g.lsn, g.segment, g.offset, g.payload_len, &g.meta),
                        (w.lsn, w.segment, w.offset, w.payload_len, &w.meta)
                    );
                }
            }
            assert!(r.by_key(base, b"log-9").unwrap().is_empty(), "no such key");
            assert!(
                r.by_key(base + 1, &keys[0]).unwrap().is_empty(),
                "not a segment"
            );
        }
        assert!(
            r.by_key(active, &keys[0]).unwrap().is_empty(),
            "the active one"
        );
        sealed
    };
    let sealed = check(&log);
    drop(log);
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(log.stats().index_rebuilds, 0);
    assert_eq!(check(&log), sealed, "after a restart, from the files");
    let floor = log.retain(Some(4 * 64 * KB), None).unwrap();
    assert!(floor > sealed[0].0);
    let r = log.reader();
    assert!(
        r.by_key(sealed[0].0, &keys[0]).unwrap().is_empty(),
        "dropped"
    );
    assert_eq!(r.sealed()[0].0, floor);
}

#[test]
fn an_index_file_of_the_old_format_is_rebuilt() {
    // the index format is versioned: version 1 files (entries held in RAM, no by-key section)
    // are not misread, their segments are rescanned
    let d = tempfile::tempdir().unwrap();
    {
        let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        fill(&mut log, 0, 100, 2, 1000);
    }
    let idx = segment_files(d.path())[0].with_extension("idx");
    rewrite_index(&idx, |b| b[4..8].copy_from_slice(&1u32.to_le_bytes()));
    let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    assert_eq!(log.stats().index_rebuilds, 1);
    check_all(&log, 0, 200, 1000);
    let base = log.reader().sealed()[0].0;
    assert_eq!(log.reader().by_key(base, b"log-1").unwrap()[0].lsn, 1);
}

#[test]
fn a_record_stays_readable_while_its_segment_is_sealed() {
    // Why: a roll seals the previous segment (its index moves from RAM to its file) while
    // readers read it. A read racing the seal must still find the record: a `None` for a
    // durable record makes a consumer skip it
    let d = tempfile::tempdir().unwrap();
    let mut c = cfg(d.path(), 16 * KB);
    c.tail_cache_bytes = 0;
    let mut log = Log::open(c).unwrap();
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (r, done) = (log.reader(), done.clone());
            std::thread::spawn(move || {
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    let n = r.next_lsn();
                    if n > 0 {
                        assert!(r.read(n - 1).unwrap().is_some(), "lsn {} missing", n - 1);
                    }
                }
            })
        })
        .collect();
    fill(&mut log, 0, 5_000, 1, 300);
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in readers {
        t.join().unwrap();
    }
}

/// Appends `groups` groups of `per` records without asking for a sync; returns the offset of
/// the first record.
fn unsynced_groups(log: &mut Log, start: u64, groups: u64, per: u64, size: usize) -> u64 {
    fill(log, start, groups, per, size);
    log.reader().entries(start, 1).unwrap()[0].offset
}

#[test]
fn a_power_loss_under_write_caching_drops_the_unsynced_tail_whatever_reached_disk() {
    // Why: with write caching (live-ingestion's setting) several groups are unsynced at once and
    // the kernel writes their pages back in any order, so a power loss can keep a later group and
    // lose an earlier one. None of them was relied on locally (acks rested on the replicas), so
    // the node must come back with its synced prefix and repair the rest, not refuse to start.
    // Found by spec/storage (MC_caching_unfixed.cfg).
    let d = tempfile::tempdir().unwrap();
    let c = || deferred(d.path(), 1 << 20, 1 << 20);
    let lost;
    {
        let mut log = Log::open(c()).unwrap();
        fill(&mut log, 0, 4, 2, 300);
        log.flush().unwrap();
        lost = unsynced_groups(&mut log, 8, 3, 2, 300);
        assert_eq!(log.stats().syncs, 1, "groups 8.. are unsynced");
    }
    // the first unsynced group never reached disk; the two after it did
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, lost, &[0u8; 64]);
    let mut log = Log::open(c()).unwrap();
    assert_eq!(
        log.next_lsn(),
        8,
        "the synced prefix, nothing after the hole"
    );
    check_all(&log, 0, 8, 300);
    // the dropped groups' records were zeroed: a shorter write cannot be followed by them
    fill(&mut log, 8, 1, 1, 10);
    log.flush().unwrap();
    drop(log);
    assert_eq!(Log::open(c()).unwrap().next_lsn(), 9);
}

#[test]
fn under_write_caching_damage_before_more_than_the_unsynced_bound_still_fails_closed() {
    // Why: at most flush_bytes plus one group (MAX_GROUP) can be unsynced, so a hole followed by
    // more than that was synced: damage, not a lost tail. Dropping it would discard data the
    // log had made durable.
    let d = tempfile::tempdir().unwrap();
    let c = || deferred(d.path(), 64 << 20, 64 * KB);
    let early;
    {
        let mut log = Log::open(c()).unwrap();
        fill(&mut log, 0, 4, 1, 300);
        early = log.reader().entries(1, 1).unwrap()[0].offset;
        // past MAX_GROUP + flush_bytes of synced groups after the damage
        let big = (record::MAX_RECORD - 4096) as u64;
        let n = (precog_storage::log::MAX_GROUP as u64 + 64 * KB) / big + 2;
        for i in 0..n {
            let m = meta(4 + i);
            log.append(&[(m, &vec![7u8; big as usize][..])]).unwrap();
        }
        log.flush().unwrap();
    }
    let active = segment_files(d.path()).pop().unwrap();
    patch(&active, early + record::HEADER as u64 + 3, &[0x5A]);
    match Log::open(c()) {
        Err(Error::Corrupt(msg)) => assert!(msg.contains("later"), "{msg}"),
        Err(e) => panic!("wrong error: {e}"),
        Ok(_) => panic!("opened a log with interior damage"),
    }
}

#[test]
fn under_write_caching_the_unsynced_bound_is_exact_to_the_last_record_written() {
    // Why: the bound decides between dropping an unsynced tail and failing closed on damage; a
    // record that ends past x + Log::unsynced_bound was written after an fsync covered x.
    // Two later records: the second, found by stepping over the first, decides.
    for (past, opens) in [(0u64, true), (8, false)] {
        let d = tempfile::tempdir().unwrap();
        let c = || deferred(d.path(), 64 << 20, 4096);
        {
            let mut log = Log::open(c()).unwrap();
            fill(&mut log, 0, 2, 1, 100);
            log.flush().unwrap();
        }
        let (active, generation, lsn, x) = active_end(d.path());
        let mut torn = vec![];
        record::encode(&mut torn, generation, x, lsn, &meta(lsn), &[1; 100]);
        torn[80] ^= 0xff;
        patch(&active, x, &torn);
        let bound = Log::unsynced_bound(&c());
        let first = x + 1024;
        let second = x + bound - record::HEADER as u64 + past;
        for (at, l) in [(first, lsn + 1), (second, lsn + 2)] {
            let mut r = vec![];
            record::encode(&mut r, generation, at, l, &meta(l), &[2; 100]);
            patch(&active, at, &r);
        }
        match Log::open(c()) {
            Ok(log) => {
                assert!(opens, "{past} bytes past the bound must fail closed");
                assert_eq!(log.next_lsn(), 2, "the unsynced tail is dropped");
            }
            Err(Error::Corrupt(msg)) => assert!(!opens, "within the bound: {msg}"),
            Err(e) => panic!("wrong error: {e}"),
        }
    }
}

#[test]
fn retention_names_the_segments_it_would_drop_and_drops_up_to_a_boundary() {
    // Why: a node copies forward what its streams still retain from exactly the segments the
    // cap drops, then drops those and no more: what it is told must be what retention does.
    let d = tempfile::tempdir().unwrap();
    let seg = 64 * KB;
    let mut log = Log::open(cfg(d.path(), seg)).unwrap();
    fill(&mut log, 0, 150, 2, 1000);
    let total = log.stats().log_bytes;
    let (segs, t) = log.segments();
    assert_eq!(t, total);
    assert_eq!(
        segs.len() + 1,
        log.stats().segments,
        "all but the active one"
    );
    let gone = &segs[..log.retirable(Some(3 * seg), None)];
    assert!(gone.len() >= 2, "{gone:?}");
    let freed: u64 = gone.iter().map(|s| s.bytes).sum();
    assert!(total - freed <= 3 * seg && total - freed + gone.last().unwrap().bytes > 3 * seg);
    let sealed = log.reader().sealed();
    assert_eq!(sealed.len(), segs.len());
    for (s, (base, end)) in segs.iter().zip(&sealed) {
        assert_eq!((s.base, s.end), (*base, *end));
        assert_eq!(s.max_time, 1_000 + end - 1, "time_ms of its newest record");
    }
    // age: only segments whose newest record is older than the limit
    assert_eq!(log.retirable(None, Some(1_000 + sealed[1].1)), 2);
    assert_eq!(log.retirable(None, None), 0);
    // the first one only, whatever the cap says
    let floor = log.drop_until(gone[0].end).unwrap();
    assert_eq!(floor, gone[0].end);
    assert_eq!(log.stats().log_bytes, total - gone[0].bytes);
    assert!(log.reader().read(floor - 1).unwrap().is_none());
    check_all(&log, floor, 300, 1000);
    assert_eq!(
        log.drop_until(floor).unwrap(),
        floor,
        "nothing below it is left"
    );
    assert_eq!(
        log.retain(Some(3 * seg), None).unwrap(),
        gone.last().unwrap().end
    );
    // the active segment is never dropped
    let next = log.next_lsn();
    let floor = log.drop_until(next).unwrap();
    assert!(floor < next && log.reader().read(next - 1).unwrap().is_some());
}

#[test]
fn a_crash_while_retention_drops_segments_keeps_the_log_consistent() {
    // Why: the cleaner deletes a segment only after copying what streams retain from it; a
    // crash before the manifest names the drop keeps the old copy, one after keeps the new.
    for (step, dropped) in [
        ("retain:before_manifest", false),
        ("retain:after_manifest", true),
    ] {
        let d = tempfile::tempdir().unwrap();
        let mut c = cfg(d.path(), 64 * KB);
        c.failpoints.fail_at = Some(step);
        let mut log = Log::open(c).unwrap();
        fill(&mut log, 0, 150, 2, 1000);
        let end = log.reader().sealed()[0].1;
        assert!(
            matches!(log.drop_until(end), Err(Error::Fatal(_))),
            "{step}"
        );
        assert!(log.is_fenced(), "{step}");
        drop(log);
        let log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
        let first = if dropped { end } else { 0 };
        assert_eq!(log.first_lsn(), first, "{step}");
        check_all(&log, first, 300, 1000);
    }
}

#[test]
fn a_record_s_append_time_is_read_from_its_index() {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path(), 64 * KB)).unwrap();
    fill(&mut log, 0, 150, 2, 1000);
    let r = log.reader();
    for lsn in [0, 1, 150, 299] {
        assert_eq!(r.time_ms(lsn).unwrap(), Some(1_000 + lsn), "lsn {lsn}");
    }
    assert_eq!(r.time_ms(300).unwrap(), None, "not written");
    log.retain(Some(3 * 64 * KB), None).unwrap();
    assert_eq!(r.time_ms(0).unwrap(), None, "dropped");
}
