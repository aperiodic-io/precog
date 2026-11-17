//! Instruction-count gate for the mesh's per-batch CPU work (encode on the client, full
//! validation on every node). CI fails a PR that adds more than 2% instructions.
use gungraun::{library_benchmark, library_benchmark_group, main};
use precog_mesh::proto::{Batch, LogId};
use std::hint::black_box;

fn events() -> Vec<(u64, Vec<u8>)> {
    (0..100u64)
        .map(|i| {
            (
                1_700_000_000_000_000 + i,
                (0..256).map(|j| (i * 7 + j) as u8).collect(),
            )
        })
        .collect()
}

fn log() -> LogId {
    LogId {
        stream: "market".into(),
        writer: "writer-7".into(),
        epoch: 1,
    }
}

#[library_benchmark]
#[bench::hundred_256b(events())]
fn encode_batch(evs: Vec<(u64, Vec<u8>)>) -> usize {
    let refs: Vec<(u64, &[u8])> = evs.iter().map(|(t, p)| (*t, &p[..])).collect();
    black_box(Batch::encode(&log(), 0, &refs).unwrap().len())
}

fn encoded() -> bytes::Bytes {
    let evs = events();
    let refs: Vec<(u64, &[u8])> = evs.iter().map(|(t, p)| (*t, &p[..])).collect();
    Batch::encode(&log(), 0, &refs).unwrap()
}

#[library_benchmark]
#[bench::hundred_256b(encoded())]
fn parse_batch(b: bytes::Bytes) -> u32 {
    black_box(Batch::parse(b).unwrap().count)
}

library_benchmark_group!(name = gate; benchmarks = encode_batch, parse_batch);
main!(library_benchmark_groups = gate);
