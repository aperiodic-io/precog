//! Wall-clock throughput of the segment log under the group-commit workload precog runs:
//! P producer threads each append one record and wait until it is durable; one writer thread
//! drains every queued record into one `Log::append`. Prints one JSON line.
//!
//! cargo run --release -p precog-storage --example throughput -- <dir> [producers] [size] [secs]
use precog_storage::record::Meta;
use precog_storage::{Config, Log};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(
        a.get(1)
            .expect("usage: throughput <dir> [producers] [size] [secs]"),
    );
    let producers: usize = a.get(2).map_or(4, |x| x.parse().unwrap());
    let size: usize = a.get(3).map_or(27_000, |x| x.parse().unwrap());
    let secs: u64 = a.get(4).map_or(5, |x| x.parse().unwrap());
    let mut cfg = Config::new(&dir);
    cfg.segment_size = 256 << 20;
    let mut log = Log::open(cfg).unwrap();
    let (tx, rx) = mpsc::channel::<(Vec<u8>, mpsc::SyncSender<()>)>();
    let writer = std::thread::spawn(move || {
        let mut groups = 0u64;
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            while let Ok(x) = rx.try_recv() {
                batch.push(x);
            }
            let items: Vec<(Meta, &[u8])> = batch
                .iter()
                .map(|(p, _)| {
                    (
                        Meta {
                            key: b"bench".to_vec(),
                            first_seq: 0,
                            count: 1,
                            tag: 0,
                            time_ms: 0,
                        },
                        &p[..],
                    )
                })
                .collect();
            log.append(&items).unwrap();
            groups += 1;
            for (_, done) in batch {
                let _ = done.send(());
            }
        }
        groups
    });
    let deadline = Instant::now() + Duration::from_secs(secs);
    let start = Instant::now();
    let handles: Vec<_> = (0..producers)
        .map(|_| {
            let tx = tx.clone();
            std::thread::spawn(move || {
                let (dtx, drx) = mpsc::sync_channel(1);
                let mut lat = vec![];
                while Instant::now() < deadline {
                    let t = Instant::now();
                    tx.send((vec![7u8; size], dtx.clone())).unwrap();
                    drx.recv().unwrap();
                    lat.push(t.elapsed().as_micros() as u64);
                }
                lat
            })
        })
        .collect();
    drop(tx);
    let mut lat: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let groups = writer.join().unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    lat.sort_unstable();
    let n = lat.len();
    println!(
        "{{\"appends_per_s\":{:.1},\"mb_per_s\":{:.2},\"p50_us\":{},\"p99_us\":{},\"avg_group\":{:.2}}}",
        n as f64 / elapsed,
        (n * size) as f64 / elapsed / 1e6,
        lat[n / 2],
        lat[n * 99 / 100],
        n as f64 / groups as f64
    );
}
