//! Dedup indexes at production scale: a 5-minute window and ~26.7k new ids/s (8M live ids, the
//! fork's per-partition cap), over 15 simulated minutes; measured over the last 5 (steady state).
//! Ids look like the producers' (`<producer>-<counter>`) and are hashed with xxh3 here, so the
//! cost per event includes the hash. A resend repeats one of the producer's 64 latest ids.
//!
//!   cargo run --release -p precog-lab --bin dedup [-- --rate N --window S]
use precog_lab::dedup::SwissGenerations;
use precog_lab::dedup::{
    Dedup, Generations, GenerationsPrefetch, MapFifo, Nothing, Packed, Stamped, StampedPrefetch,
};
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |k: &str, d: u64| {
        args.iter()
            .position(|a| a == k)
            .map_or(d, |i| args[i + 1].parse().unwrap())
    };
    let rate = arg("--rate", 26_667);
    let window = arg("--window", 300) as u32;
    let secs = 3 * window as u64;
    println!("| index | resends | ns/event | MiB at end | bytes/live id | wrong |");
    println!("|---|---:|---:|---:|---:|---:|");
    for dup in [0.0, 0.2, 0.5] {
        let make: Vec<Box<dyn Fn() -> Box<dyn Dedup>>> = vec![
            Box::new(move || Box::new(Nothing)),
            Box::new(move || Box::new(Packed::new(window))),
            Box::new(move || Box::new(SwissGenerations::new(2, window))),
            Box::new(move || Box::new(SwissGenerations::new(3, window))),
            Box::new(move || Box::new(Generations::new(2, window))),
            Box::new(move || Box::new(Generations::new(4, window))),
            Box::new(move || Box::new(Generations::new(8, window))),
            Box::new(move || Box::new(Stamped::new(window))),
            Box::new(move || Box::new(MapFifo::new(window))),
            Box::new(move || Box::new(StampedPrefetch(Stamped::new(window)))),
            Box::new(move || Box::new(GenerationsPrefetch(Generations::new(2, window)))),
            Box::new(move || Box::new(GenerationsPrefetch(Generations::new(3, window)))),
            Box::new(move || Box::new(GenerationsPrefetch(Generations::new(4, window)))),
        ];
        for m in make {
            let mut d = m();
            let (mut rng, mut ring, mut id) = (0x9e37_79b9_7f4a_7c15u64, [0u64; 64], 0u64);
            let (mut wrong, mut n, mut t0) = (0u64, 0u64, None);
            let mut buf = String::new();
            let (mut hs, mut expect, mut out) = (Vec::new(), Vec::new(), Vec::new());
            for s in 0..secs {
                if s == 2 * window as u64 {
                    t0 = Some(Instant::now());
                    n = 0;
                }
                let mut fresh = 0;
                while fresh < rate {
                    // one producer batch of 100 events
                    hs.clear();
                    expect.clear();
                    for _ in 0..100 {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let resend = (rng % 1000) as f64 / 1000.0 < dup && id > 64;
                        let x = if resend {
                            ring[(rng >> 32) as usize % 64]
                        } else {
                            id
                        };
                        buf.clear();
                        use std::fmt::Write;
                        let _ = write!(buf, "binance-futures-p{}-{x}", x % 16);
                        hs.push(xxhash_rust::xxh3::xxh3_64(buf.as_bytes()));
                        expect.push(resend);
                        if !resend {
                            ring[id as usize % 64] = id;
                            id += 1;
                            fresh += 1;
                        }
                    }
                    d.seen_batch(&hs, s as u32, &mut out);
                    wrong += out.iter().zip(&expect).filter(|(a, b)| a != b).count() as u64;
                    n += hs.len() as u64;
                }
            }
            let ns = t0.unwrap().elapsed().as_nanos() as f64 / n as f64;
            let live = rate * window as u64;
            println!(
                "| {} | {:.0}% | {ns:.1} | {:.0} | {:.1} | {wrong} |",
                d.name(),
                dup * 100.0,
                d.bytes() as f64 / (1 << 20) as f64,
                d.bytes() as f64 / live as f64
            );
        }
    }
}
