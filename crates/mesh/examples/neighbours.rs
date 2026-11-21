//! The neighbour test of `results/session-log.md`, in one process: three nodes on loopback, each
//! on a runtime thread of its own (as `precog-node serve` runs by default), and a small writer
//! (10 x 256-byte events every 5 ms, one append in flight, 2 copies) beside `large` tenants that
//! append events of `large_kib` KiB back to back through one token, with or without an append
//! rate on it. One JSON line: the small writer's acks, and what the large writers got through.
//!
//!   cargo run --release -p precog-mesh --example neighbours -- <dir> <seconds> <large> <large_kib> <rate_kib>
//!
//! `large` 0: the small writer alone. `rate_kib` 0: no rate. The rate is per node, and a client
//! refused on one node retries on the next, so the tenants get up to three times it.
//! `bench/session/neighbours.sh` runs the cases of the results file.

use bytes::Bytes;
use precog_mesh::client::MeshClient;
use precog_mesh::node::{Node, NodeConfig, PeerConfig, TokenConfig};
use precog_mesh::proto::{Batch, LogId};
use std::time::{Duration, Instant};

fn log(stream: &str) -> LogId {
    LogId {
        stream: stream.into(),
        writer: "w".into(),
        epoch: 1,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [dir, secs, large, large_kib, rate_kib] = &args[..] else {
        panic!("usage: neighbours <dir> <seconds> <large> <large_kib> <rate_kib>");
    };
    let n = |s: &String| s.parse::<u64>().expect("a number");
    let (secs, large, large_kib, rate_kib) = (n(secs), n(large), n(large_kib), n(rate_kib));
    // three free ports (bound, then released for the nodes)
    let ports: Vec<u16> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>()
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let addr = |i: usize| format!("tcp://127.0.0.1:{}", ports[i]);
    for (i, port) in ports.iter().enumerate() {
        let d = std::path::Path::new(dir).join(format!("n{i}"));
        let mut c = NodeConfig::single(i as u32, &format!("d{i}"), d, "tok".into());
        c.listen_tcp = Some(format!("127.0.0.1:{port}").parse().unwrap());
        c.peers = (0..3)
            .filter(|&j| j != i)
            .map(|j| PeerConfig {
                id: j as u32,
                domain: format!("d{j}"),
                addr: addr(j),
            })
            .collect();
        (c.segment_size, c.peer_token) = (256 << 20, Some("peers".into()));
        c.tokens = vec![TokenConfig {
            token: "big".into(),
            prefixes: vec!["big/".into()],
            read: true,
            write: true,
            append_bytes_per_s: (rate_kib > 0).then_some(rate_kib << 10),
        }];
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(Node::run(c)).unwrap();
        });
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let addrs: Vec<_> = (0..3).map(|i| addr(i).parse().unwrap()).collect();
        let small = MeshClient::new(addrs.clone(), "tok".into()).unwrap();
        let big = MeshClient::new(addrs, "big".into()).unwrap();
        // (until the nodes listen and have found each other)
        let first = Batch::encode(&log("warmup"), 0, &[(0, b"x")]).unwrap();
        while small.append(0, &first, 2).await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let end = Instant::now() + Duration::from_secs(secs);
        let writers: Vec<_> = (0..large)
            .map(|w| {
                let big = big.clone();
                tokio::spawn(async move {
                    let mut payload = vec![0u8; (large_kib as usize) << 10];
                    rand::RngCore::fill_bytes(&mut rand::rng(), &mut payload);
                    let (stream, mut seq, mut bytes) = (format!("big/{w}"), 0, 0u64);
                    while Instant::now() < end {
                        let b = Batch::encode(&log(&stream), seq, &[(seq, &payload[..])]).unwrap();
                        if big.append(0, &b, 2).await.is_ok() {
                            (seq, bytes) = (seq + 1, bytes + b.len() as u64);
                        }
                    }
                    bytes
                })
            })
            .collect();
        let event = [7u8; 256];
        let events: Vec<(u64, &[u8])> = (0..10).map(|i| (i, &event[..])).collect();
        let mut acks_us: Vec<u64> = vec![];
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        while Instant::now() < end {
            tick.tick().await;
            let seq = 10 * acks_us.len() as u64; // (ten events a batch)
            let b: Bytes = Batch::encode(&log("small"), seq, &events).unwrap();
            let start = Instant::now();
            small
                .append(0, &b, 2)
                .await
                .expect("the small writer's append");
            acks_us.push(start.elapsed().as_micros() as u64);
        }
        let mut large_bytes = 0;
        for w in writers {
            large_bytes += w.await.unwrap();
        }
        acks_us.sort_unstable();
        let at = |q: f64| acks_us[((acks_us.len() - 1) as f64 * q) as usize] as f64 / 1000.0;
        println!(
            "{{\"large\":{large},\"large_kib\":{large_kib},\"rate_kib\":{rate_kib},\
             \"seconds\":{secs},\"small_appends\":{},\"ack_p50_ms\":{:.2},\"ack_p99_ms\":{:.2},\
             \"ack_max_ms\":{:.2},\"large_mib_s\":{:.1}}}",
            acks_us.len(),
            at(0.5),
            at(0.99),
            at(1.0),
            large_bytes as f64 / (1 << 20) as f64 / secs as f64
        );
    });
    std::process::exit(0); // the nodes serve until the process ends
}
