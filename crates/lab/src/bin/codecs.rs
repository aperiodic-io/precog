//! Round 3, POC 9: payload-column codecs on round 1's market-data records. Each batch's payload
//! column is compressed as one block (as the compact layout's codec flag would carry it); the
//! broker never decompresses, so the two ends are the producer (compress) and a consumer
//! (decompress). CPU is `getrusage(RUSAGE_THREAD)` on one pinned thread.
//!
//!   cargo run --release -p precog-lab --bin codecs [-- --reps 3]
use precog_lab::net::{Meter, pin};
use precog_lab::proto::{Record, records};

#[derive(Clone, Copy)]
enum Codec {
    None,
    Lz4,
    Zstd(i32),
    ZstdDict(i32),
}

#[derive(Clone)]
struct Out {
    comp_us: f64,
    decomp_us: f64,
    bytes: f64,
}

fn column(b: &[Record]) -> Vec<u8> {
    b.iter().flat_map(|r| r.payload.iter().copied()).collect()
}

fn bench(c: Codec, recs: &[Record], batch: usize, dict: &[u8]) -> Out {
    let cols: Vec<Vec<u8>> = recs.chunks(batch).map(column).collect();
    let n = recs.len() as f64;
    let mut zc = match c {
        Codec::Zstd(l) => Some(zstd::bulk::Compressor::new(l).unwrap()),
        Codec::ZstdDict(l) => Some(zstd::bulk::Compressor::with_dictionary(l, dict).unwrap()),
        _ => None,
    };
    let mut zd = match c {
        Codec::Zstd(_) => Some(zstd::bulk::Decompressor::new().unwrap()),
        Codec::ZstdDict(_) => Some(zstd::bulk::Decompressor::with_dictionary(dict).unwrap()),
        _ => None,
    };
    let mut out = Vec::new();
    let m = Meter::start();
    for col in &cols {
        out.push(match c {
            Codec::None => col.clone(),
            Codec::Lz4 => lz4_flex::block::compress(col),
            _ => zc.as_mut().unwrap().compress(col).unwrap(),
        });
    }
    let comp = m.stop().cpu;
    let m = Meter::start();
    let mut total = 0usize;
    for (z, col) in out.iter().zip(&cols) {
        let d = match c {
            Codec::None => z.clone(),
            Codec::Lz4 => lz4_flex::block::decompress(z, col.len()).unwrap(),
            _ => zd.as_mut().unwrap().decompress(z, col.len()).unwrap(),
        };
        total += d.len();
        debug_assert_eq!(&d, col);
    }
    let decomp = m.stop().cpu;
    assert_eq!(total, cols.iter().map(Vec::len).sum::<usize>());
    // the compact layout: a 60 B header per batch and 14 B of fixed columns per record
    let wire: usize = out.iter().map(|z| z.len() + 60).sum::<usize>() + 14 * recs.len();
    Out {
        comp_us: comp * 1e6 / n,
        decomp_us: decomp * 1e6 / n,
        bytes: wire as f64 / n,
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let reps = args
        .iter()
        .position(|a| a == "--reps")
        .map_or(3, |i| args[i + 1].parse().unwrap());
    pin(2);
    let recs = records(1 << 18, 42);
    // trained on other records than the ones measured (a producer ships the dictionary once)
    let sample = records(20_000, 7);
    let t = Meter::start();
    let dict = zstd::dict::from_samples(
        &sample.iter().map(|r| r.payload.clone()).collect::<Vec<_>>(),
        16 << 10,
    )
    .unwrap();
    let train = t.stop().cpu;
    eprintln!(
        "dictionary: {} B, trained in {:.0} ms",
        dict.len(),
        train * 1e3
    );
    let codecs = [
        ("none", Codec::None),
        ("lz4 (round 1)", Codec::Lz4),
        ("zstd 1", Codec::Zstd(1)),
        ("zstd 3", Codec::Zstd(3)),
        ("zstd 1 + 16 KiB dictionary", Codec::ZstdDict(1)),
        ("zstd 3 + 16 KiB dictionary", Codec::ZstdDict(3)),
    ];
    // ~1 ms of one producer at 100k/s, and a 1 MiB backlog frame
    let batches = [100usize, 4096];
    let mut res = vec![vec![Vec::new(); batches.len()]; codecs.len()];
    bench(Codec::Lz4, &recs, 4096, &dict); // warm-up
    for _ in 0..reps {
        for (i, &(_, c)) in codecs.iter().enumerate() {
            for (j, &b) in batches.iter().enumerate() {
                res[i][j].push(bench(c, &recs, b, &dict));
            }
        }
    }
    let med = |v: &[Out], f: fn(&Out) -> f64| {
        let mut x: Vec<f64> = v.iter().map(f).collect();
        x.sort_by(f64::total_cmp);
        x[x.len() / 2]
    };
    let rng = |v: &[Out], f: fn(&Out) -> f64| {
        let x: Vec<f64> = v.iter().map(f).collect();
        format!(
            "{:.3}-{:.3}",
            x.iter().copied().fold(f64::MAX, f64::min),
            x.iter().copied().fold(0.0, f64::max)
        )
    };
    println!(
        "| codec | B/rec @100 | compress µs/rec @100 | decompress µs/rec @100 | B/rec @4096 | compress µs/rec @4096 | decompress µs/rec @4096 | compress range @4096 |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");
    for (i, (name, _)) in codecs.iter().enumerate() {
        let (a, b) = (&res[i][0], &res[i][1]);
        println!(
            "| {name} | {:.1} | {:.3} | {:.3} | {:.1} | {:.3} | {:.3} | {} |",
            med(a, |o| o.bytes),
            med(a, |o| o.comp_us),
            med(a, |o| o.decomp_us),
            med(b, |o| o.bytes),
            med(b, |o| o.comp_us),
            med(b, |o| o.decomp_us),
            rng(b, |o| o.comp_us),
        );
    }
}
