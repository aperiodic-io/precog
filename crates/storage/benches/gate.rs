//! Instruction-count gate for the storage hot paths. CI fails a PR that adds more than 2%
//! instructions to any of these against its merge base (callgrind via gungraun).
use gungraun::{library_benchmark, library_benchmark_group, main};
use precog_storage::record::{self, Meta};
use precog_storage::{Config, Log};
use std::hint::black_box;

fn meta(i: u64) -> Meta {
    Meta {
        key: b"market/writer-7/1".to_vec(),
        first_seq: i,
        count: 100,
        tag: i as u32,
        time_ms: 1_700_000_000_000,
    }
}

fn payloads() -> Vec<Vec<u8>> {
    (0..100u64)
        .map(|i| (0..2_700).map(|j| (i * 31 + j) as u8).collect())
        .collect()
}

fn encoded_group() -> Vec<u8> {
    let p = payloads();
    let mut buf = vec![];
    for (i, x) in p.iter().enumerate() {
        record::encode(&mut buf, 7, 4096, i as u64, &meta(i as u64), x);
    }
    buf
}

#[library_benchmark]
#[bench::hundred_2700b(payloads())]
fn encode_group(p: Vec<Vec<u8>>) -> usize {
    let mut buf = Vec::with_capacity(300_000);
    for (i, x) in p.iter().enumerate() {
        record::encode(&mut buf, 7, 4096, i as u64, &meta(i as u64), x);
    }
    black_box(buf.len())
}

#[library_benchmark]
#[bench::hundred_2700b(encoded_group())]
fn decode_group(buf: Vec<u8>) -> u64 {
    let mut off = 0;
    let mut n = 0;
    while let record::Decoded::Record { header, next, .. } = record::decode(&buf, off, 7) {
        n += header.count as u64;
        off = next;
    }
    black_box(n)
}

fn cfg(dir: &std::path::Path) -> Config {
    let mut c = Config::new(dir);
    c.segment_size = 8 << 20;
    c.spares = 0; // no background thread competing with the measurement
    c
}

/// A log directory holding ~5 MiB of records in its active segment.
fn written_log() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    let mut log = Log::open(cfg(d.path())).unwrap();
    let p = payloads();
    for g in 0..20u64 {
        let items: Vec<(Meta, &[u8])> = p
            .iter()
            .enumerate()
            .map(|(i, x)| (meta(g * 100 + i as u64), &x[..]))
            .collect();
        log.append(&items).unwrap();
    }
    d
}

#[library_benchmark]
#[bench::active_segment_5mib(written_log())]
fn recover(d: tempfile::TempDir) -> u64 {
    let log = Log::open(cfg(d.path())).unwrap();
    let n = log.next_lsn();
    std::mem::forget(log);
    std::mem::forget(d);
    black_box(n)
}

fn empty_log() -> (tempfile::TempDir, Log, Vec<Vec<u8>>) {
    let d = tempfile::tempdir().unwrap();
    let log = Log::open(cfg(d.path())).unwrap();
    (d, log, payloads())
}

#[library_benchmark]
#[bench::hundred_2700b(empty_log())]
fn append_group((d, mut log, p): (tempfile::TempDir, Log, Vec<Vec<u8>>)) -> u64 {
    let items: Vec<(Meta, &[u8])> = p
        .iter()
        .enumerate()
        .map(|(i, x)| (meta(i as u64), &x[..]))
        .collect();
    let first = log.append(&items).unwrap();
    std::mem::forget(log);
    std::mem::forget(d);
    black_box(first)
}

library_benchmark_group!(name = gate; benchmarks = encode_group, decode_group, recover, append_group);
main!(library_benchmark_groups = gate);
