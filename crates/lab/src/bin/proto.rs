//! Ten native-protocol proofs of concept: one client-to-server append path each, over loopback
//! TCP with std threads. The server parses, validates (CRC-32C, structure, bounds, sequence
//! contiguity, the dedup hash of every record) and acknowledges; nothing touches a disk.
//! Each POC changes one idea against the row above it (`results/protocol.md`).
//!
//! Threads are pinned: server connection threads on CPUs 0 and 1, the client's writer on 2 and
//! its ack reader on 3. CPU time is `getrusage(RUSAGE_THREAD)` of exactly those threads.
//!
//!   cargo run --release -p precog-lab --bin proto [-- --reps 3 --n 1000000 --rate 100000 --secs 2 --linger-ms 1 --only <prefix>]
use precog_lab::proto::codec::{Enc, Key, Layout, Log, decode_hello, encode_hello, visit};
use precog_lab::proto::{Record, records};
use std::collections::VecDeque;
use std::io::{IoSlice, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use xxhash_rust::xxh3::xxh3_64;

/// `u32 len | u8 kind | u64 req_id | body`, `len` covering kind..body (today's `rpc.rs` frame).
const FRAME: usize = 13;
const HELLO: u8 = 1;
const APPEND: u8 = 2;
const ACK: u8 = 3;
const MAX_FRAME: usize = 1 << 20;
const STREAM: &str = "raw.trades.binance-futures";
const WRITERS: [&str; 2] = [
    "live-producer-binance-futures-0",
    "live-producer-binance-futures-1",
];

#[derive(Clone, Copy, PartialEq)]
enum Pair {
    One,
    /// Two producer replicas, both sending every event in full.
    TwoFull,
    /// The second replica sends only the dedup hashes (`Layout::Shadow`).
    Standby,
}

#[derive(Clone, Copy)]
struct Poc {
    name: &'static str,
    /// Batches (frames) in flight per connection.
    inflight: usize,
    /// Send whatever has queued, up to 1 MiB per frame; otherwise exactly 100 records.
    backlog: bool,
    /// One ack per read loop with the highest contiguous sequence, not one per batch.
    cumulative: bool,
    layout: Layout,
    /// Server parses in place in one reusable buffer; client `writev`s head and payload column.
    zero_copy: bool,
    /// With `backlog`: hold a frame until its oldest record has waited this long (or ~1 MiB).
    linger: Duration,
    pair: Pair,
}

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

/// CPU seconds (user + system) of the calling thread.
fn thread_cpu() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut u) }, 0);
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 * 1e-6;
    tv(u.ru_utime) + tv(u.ru_stime)
}

fn frame_header(out: &mut Vec<u8>, kind: u8, req_id: u64, body_len: usize) {
    out.extend_from_slice(&((9 + body_len) as u32).to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&req_id.to_le_bytes());
}

fn uses_ids(l: Layout) -> bool {
    l != Layout::Pcb2Names
}

// ---------------------------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------------------------

struct Srv {
    layout: Layout,
    cumulative: bool,
    names: Vec<Option<String>>,
    expect: u64,
    records: u64,
    /// Folded dedup hashes and times, so the compiler cannot skip the per-record work.
    fold: u64,
    acked: u64,
    acks: Vec<u8>,
}

impl Srv {
    fn frame(&mut self, kind: u8, req_id: u64, body: &[u8]) {
        if kind == HELLO {
            for (id, name) in decode_hello(body).expect("hello") {
                let i = id as usize;
                if self.names.len() <= i {
                    self.names.resize(i + 1, None);
                }
                self.names[i] = Some(name);
            }
            return;
        }
        assert_eq!(kind, APPEND);
        let _copies = body[0];
        let mut fold = self.fold;
        let b = visit(self.layout, &body[1..], |r| {
            let h = match r.key {
                Key::Id(k) => xxh3_64(k),
                Key::Hash(h) => h,
            };
            fold = fold.rotate_left(5) ^ h ^ r.time_us ^ r.len as u64;
        })
        .expect("valid batch");
        self.fold = fold;
        match &b.names {
            Some((s, w)) => assert!(s == STREAM && WRITERS.contains(&w.as_str())),
            None => assert!(
                [b.stream_id, b.writer_id]
                    .iter()
                    .all(|&i| self.names.get(i as usize).is_some_and(Option::is_some)),
                "unknown stream or writer id"
            ),
        }
        assert_eq!(b.first_seq, self.expect, "sequence gap");
        self.expect += b.count as u64;
        self.records += b.count as u64;
        if !self.cumulative {
            self.ack(req_id, b.writer_id);
        }
    }

    fn ack(&mut self, req_id: u64, writer_id: u32) {
        frame_header(&mut self.acks, ACK, req_id, 12);
        self.acks.extend_from_slice(&writer_id.to_le_bytes());
        self.acks.extend_from_slice(&self.expect.to_le_bytes());
        self.acked = self.expect;
    }

    /// After a read: the cumulative ack, then every queued ack in one write.
    fn flush(&mut self, s: &mut TcpStream) -> u64 {
        if self.cumulative && self.acked < self.expect {
            self.ack(0, 0);
        }
        let n = self.acks.len() as u64;
        if n > 0 {
            s.write_all(&self.acks).unwrap();
            self.acks.clear();
        }
        n
    }
}

struct SrvOut {
    cpu: f64,
    records: u64,
    down: u64,
    fold: u64,
}

/// Today's path: read into a `BytesMut` as `rpc::read_frame` does, split each frame off as its
/// own `Bytes`, allocate the batch's names while parsing.
fn serve_copying(s: &mut TcpStream, srv: &mut Srv) -> u64 {
    let mut buf = bytes::BytesMut::new();
    let mut down = 0;
    loop {
        while buf.len() >= 4 {
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            assert!((9..=MAX_FRAME + 64).contains(&len), "bad frame length");
            if buf.len() < 4 + len {
                break;
            }
            let _ = buf.split_to(4);
            let f = buf.split_to(len).freeze();
            srv.frame(
                f[0],
                u64::from_le_bytes(f[1..9].try_into().unwrap()),
                &f[9..],
            );
        }
        down += srv.flush(s);
        if buf.len() >= 4 {
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            buf.reserve(4 + len - buf.len());
        } else {
            buf.reserve(64 * 1024);
        }
        // Read straight into the spare capacity, as tokio's `read_buf` does.
        let spare = buf.spare_capacity_mut();
        let n = unsafe { libc::read(s.as_raw_fd(), spare.as_mut_ptr().cast(), spare.len()) };
        assert!(n >= 0, "read: {}", std::io::Error::last_os_error());
        if n == 0 {
            assert!(buf.is_empty(), "closed mid-frame");
            return down;
        }
        unsafe { buf.set_len(buf.len() + n as usize) };
    }
}

/// One 8 MiB buffer for the connection's life; frames are parsed where they landed.
fn serve_in_place(s: &mut TcpStream, srv: &mut Srv) -> u64 {
    let mut buf = vec![0u8; 8 << 20];
    let (mut start, mut end, mut down) = (0, 0, 0);
    loop {
        while end - start >= 4 {
            let len = u32::from_le_bytes(buf[start..start + 4].try_into().unwrap()) as usize;
            assert!((9..=MAX_FRAME + 64).contains(&len), "bad frame length");
            if end - start < 4 + len {
                break;
            }
            let f = &buf[start + 4..start + 4 + len];
            srv.frame(
                f[0],
                u64::from_le_bytes(f[1..9].try_into().unwrap()),
                &f[9..],
            );
            start += 4 + len;
        }
        down += srv.flush(s);
        if start == end {
            (start, end) = (0, 0);
        } else if buf.len() - end < MAX_FRAME + 128 {
            buf.copy_within(start..end, 0); // rare: once per ~7 MiB
            (start, end) = (0, end - start);
        }
        let n = s.read(&mut buf[end..]).unwrap();
        if n == 0 {
            assert_eq!(start, end, "closed mid-frame");
            return down;
        }
        end += n;
    }
}

fn serve(mut s: TcpStream, p: Poc, layout: Layout, cpu: usize) -> SrvOut {
    pin(cpu);
    let t0 = thread_cpu();
    let mut srv = Srv {
        layout,
        cumulative: p.cumulative,
        names: Vec::new(),
        expect: 0,
        records: 0,
        fold: 0,
        acked: 0,
        acks: Vec::with_capacity(64 * 1024),
    };
    let down = if p.zero_copy {
        serve_in_place(&mut s, &mut srv)
    } else {
        serve_copying(&mut s, &mut srv)
    };
    SrvOut {
        cpu: thread_cpu() - t0,
        records: srv.records,
        down,
        fold: srv.fold,
    }
}

// ---------------------------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------------------------

struct CliOut {
    cpu: f64,
    up: u64,
    frames: u64,
    done: Instant,
    /// Per-record ack latency in ns (rate runs only), from the record's scheduled arrival.
    lat: Vec<u64>,
}

fn write_all_vectored(s: &mut TcpStream, mut bufs: &mut [IoSlice<'_>]) {
    while !bufs.is_empty() {
        let n = s.write_vectored(bufs).unwrap();
        assert!(n > 0);
        IoSlice::advance_slices(&mut bufs, n);
    }
}

/// Reads acks; publishes the highest acked sequence and wakes the writer.
fn read_acks(
    mut s: TcpStream,
    n: u64,
    rate: Option<f64>,
    start: Instant,
    acked: &AtomicU64,
    writer: std::thread::Thread,
) -> (f64, Instant, Vec<u64>) {
    pin(3);
    let t0 = thread_cpu();
    let mut lat = Vec::with_capacity(if rate.is_some() { n as usize } else { 0 });
    let mut buf = vec![0u8; 256 * 1024];
    let (mut fill, mut done) = (0, 0u64);
    while done < n {
        let r = s.read(&mut buf[fill..]).unwrap();
        assert!(r > 0, "server closed");
        fill += r;
        let mut off = 0;
        while fill - off >= FRAME {
            let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            if fill - off < 4 + len {
                break;
            }
            assert_eq!(buf[off + 4], ACK);
            let end = u64::from_le_bytes(buf[off + 17..off + 25].try_into().unwrap());
            if let Some(rate) = rate {
                let now = start.elapsed().as_nanos() as u64;
                for seq in done..end {
                    lat.push(now.saturating_sub((seq as f64 * 1e9 / rate) as u64));
                }
            }
            done = done.max(end);
            off += 4 + len;
        }
        buf.copy_within(off..fill, 0);
        fill -= off;
        acked.store(done, Ordering::Release);
        writer.unpark();
    }
    (thread_cpu() - t0, Instant::now(), lat)
}

#[allow(clippy::too_many_arguments)]
fn client(
    mut s: TcpStream,
    p: Poc,
    layout: Layout,
    writer_id: u32,
    recs: Arc<Vec<Record>>,
    n: u64,
    rate: Option<f64>,
    start: Instant,
) -> CliOut {
    let acked = Arc::new(AtomicU64::new(0));
    let reader = {
        let (s, acked, me) = (
            s.try_clone().unwrap(),
            acked.clone(),
            std::thread::current(),
        );
        std::thread::spawn(move || read_acks(s, n, rate, start, &acked, me))
    };
    pin(2);
    let t0 = thread_cpu();
    let log = Log {
        stream: STREAM,
        writer: WRITERS[writer_id as usize - 1],
        stream_id: 0,
        writer_id,
        epoch: 1,
    };
    let (mut up, mut frames) = (0u64, 0u64);
    if uses_ids(layout) {
        let mut body = Vec::new();
        encode_hello(&[(0, log.stream), (writer_id, log.writer)], &mut body);
        let mut f = Vec::new();
        frame_header(&mut f, HELLO, 0, body.len());
        f.extend_from_slice(&body);
        s.write_all(&f).unwrap();
        up += f.len() as u64;
    }
    let arrival = |seq: u64| start + Duration::from_secs_f64(seq as f64 / rate.unwrap());
    let avail = || match rate {
        None => n,
        Some(r) => n.min((start.elapsed().as_secs_f64() * r) as u64),
    };
    let mut inflight: VecDeque<u64> = VecDeque::new();
    let (mut sent, mut enc, mut out) = (0u64, Enc::default(), Vec::new());
    while sent < n {
        let need = if p.backlog { 1 } else { 100.min(n - sent) };
        if avail() < sent + need {
            std::thread::sleep(arrival(sent + need - 1).saturating_duration_since(Instant::now()));
            continue;
        }
        if rate.is_some() && p.linger > Duration::ZERO && avail() - sent < 4096 {
            let ready = arrival(sent) + p.linger;
            if Instant::now() < ready {
                std::thread::sleep(ready - Instant::now());
                continue;
            }
        }
        loop {
            let a = acked.load(Ordering::Acquire);
            while inflight.front().is_some_and(|&e| e <= a) {
                inflight.pop_front();
            }
            if inflight.len() < p.inflight {
                break;
            }
            std::thread::park();
        }
        let first = (sent % recs.len() as u64) as usize;
        let mut count = if p.backlog { avail() - sent } else { need } as usize;
        count = count.min(recs.len() - first).min(65_536); // a batch never wraps the record set
        let mut est = 0;
        for (i, r) in recs[first..first + count].iter().enumerate() {
            est += 14 + r.id.len() + r.payload.len();
            if p.backlog && est > MAX_FRAME - 4096 {
                count = i.max(1);
                break;
            }
        }
        let batch = &recs[first..first + count];
        if p.zero_copy {
            enc.head.clear();
            enc.blob.clear();
            enc.head.resize(FRAME + 1, 0);
            enc.encode(layout, &log, sent, batch);
            let len = enc.head.len() + enc.blob.len();
            let mut h = Vec::with_capacity(FRAME);
            frame_header(&mut h, APPEND, sent, len - FRAME);
            enc.head[..FRAME].copy_from_slice(&h);
            enc.head[FRAME] = 2; // copies
            write_all_vectored(
                &mut s,
                &mut [IoSlice::new(&enc.head), IoSlice::new(&enc.blob)],
            );
            up += len as u64;
        } else {
            // Today: the batch is built in its own buffer, `encode_append` copies it behind the
            // copies byte, and `write_loop` copies the frame into its send buffer.
            let mut e = Enc::default();
            e.head.reserve(est + 64);
            e.encode(layout, &log, sent, batch);
            let mut body = Vec::with_capacity(1 + e.head.len() + e.blob.len());
            body.push(2);
            body.extend_from_slice(&e.head);
            body.extend_from_slice(&e.blob);
            out.clear();
            frame_header(&mut out, APPEND, sent, body.len());
            out.extend_from_slice(&body);
            s.write_all(&out).unwrap();
            up += out.len() as u64;
        }
        frames += 1;
        sent += count as u64;
        inflight.push_back(sent);
    }
    let cpu = thread_cpu() - t0;
    let (rcpu, done, lat) = reader.join().unwrap();
    s.shutdown(Shutdown::Write).unwrap();
    CliOut {
        cpu: cpu + rcpu,
        up,
        frames,
        done,
        lat,
    }
}

// ---------------------------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Run {
    rps: f64,
    srv_us: f64,
    cli_us: f64,
    up_b: f64,
    down_b: f64,
    per_frame: f64,
    p50_us: f64,
    p99_us: f64,
}

/// `n` unique events per replica; at saturation (`rate` None) or offered at `rate`/s.
fn run(p: Poc, recs: &Arc<Vec<Record>>, n: u64, rate: Option<f64>) -> Run {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let layouts = match p.pair {
        Pair::One => vec![p.layout],
        Pair::TwoFull => vec![p.layout, p.layout],
        Pair::Standby => vec![p.layout, Layout::Shadow],
    };
    let servers = {
        let layouts = layouts.clone();
        std::thread::spawn(move || {
            let hs: Vec<_> = layouts
                .into_iter()
                .enumerate()
                .map(|(i, layout)| {
                    let (s, _) = l.accept().unwrap();
                    s.set_nodelay(true).unwrap();
                    std::thread::spawn(move || serve(s, p, layout, i))
                })
                .collect();
            hs.into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        })
    };
    let socks: Vec<_> = layouts
        .iter()
        .map(|_| {
            let s = TcpStream::connect(addr).unwrap();
            s.set_nodelay(true).unwrap();
            s
        })
        .collect();
    let start = Instant::now();
    let clients: Vec<_> = socks
        .into_iter()
        .zip(&layouts)
        .enumerate()
        .map(|(i, (s, &layout))| {
            let recs = recs.clone();
            std::thread::spawn(move || client(s, p, layout, i as u32 + 1, recs, n, rate, start))
        })
        .collect();
    let cs: Vec<CliOut> = clients.into_iter().map(|h| h.join().unwrap()).collect();
    let ss = servers.join().unwrap();
    assert!(ss.iter().all(|s| s.records == n && s.fold != 0));
    let wall = cs
        .iter()
        .map(|c| c.done - start)
        .max()
        .unwrap()
        .as_secs_f64();
    let n = n as f64;
    let mut lat = cs[0].lat.clone();
    lat.sort_unstable();
    let q = |f: f64| {
        lat.get((lat.len() as f64 * f) as usize)
            .map_or(0.0, |&v| v as f64 / 1e3)
    };
    Run {
        rps: n / wall,
        srv_us: ss.iter().map(|s| s.cpu).sum::<f64>() * 1e6 / n,
        cli_us: cs.iter().map(|c| c.cpu).sum::<f64>() * 1e6 / n,
        up_b: cs.iter().map(|c| c.up).sum::<u64>() as f64 / n,
        down_b: ss.iter().map(|s| s.down).sum::<u64>() as f64 / n,
        per_frame: n / cs[0].frames as f64,
        p50_us: q(0.50),
        p99_us: q(0.99),
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |k: &str, d: f64| {
        args.iter()
            .position(|a| a == k)
            .map_or(d, |i| args[i + 1].parse().unwrap())
    };
    let (reps, n, rate, secs) = (
        arg("--reps", 3.0) as usize,
        arg("--n", 1_000_000.0) as u64,
        arg("--rate", 100_000.0),
        arg("--secs", 2.0),
    );
    let only = args
        .iter()
        .position(|a| a == "--only")
        .map(|i| args[i + 1].clone());
    let recs = Arc::new(records(1 << 18, 42));
    let mean = recs
        .iter()
        .map(|r| r.payload.len() + r.id.len())
        .sum::<usize>()
        / recs.len();
    eprintln!("records: {} distinct, mean payload+id {mean} B", recs.len());

    let base = Poc {
        name: "1 baseline: PCB2 x100, 1 in flight",
        inflight: 1,
        backlog: false,
        cumulative: false,
        layout: Layout::Pcb2Names,
        zero_copy: false,
        linger: Duration::ZERO,
        pair: Pair::One,
    };
    let p2 = Poc {
        name: "2 pipelining: 8 in flight",
        inflight: 8,
        ..base
    };
    let p3 = Poc {
        name: "3 batch-on-backlog (<=1 MiB)",
        backlog: true,
        ..p2
    };
    let ms = Duration::from_secs_f64(arg("--linger-ms", 1.0) / 1e3);
    let p3b = Poc {
        name: "3b (side branch) 3 + linger",
        linger: ms,
        ..p3
    };
    let p4 = Poc {
        name: "4 cumulative acks",
        cumulative: true,
        ..p3
    };
    let p5 = Poc {
        name: "5 session-interned names",
        layout: Layout::Pcb2Ids,
        ..p4
    };
    let p6 = Poc {
        name: "6 compact 14 B/record, hash column",
        layout: Layout::Compact,
        ..p5
    };
    let p7 = Poc {
        name: "7 columnar: varint deltas, symbol dict",
        layout: Layout::Columnar,
        ..p6
    };
    let p8 = Poc {
        name: "8 lz4 payload column",
        layout: Layout::ColumnarLz4,
        ..p7
    };
    let p9 = Poc {
        name: "9 zero-copy: in-place parse, writev",
        zero_copy: true,
        ..p8
    };
    let p10a = Poc {
        name: "10a two replicas, both full",
        pair: Pair::TwoFull,
        ..p9
    };
    let p10b = Poc {
        name: "10b active full + standby hashes",
        pair: Pair::Standby,
        ..p9
    };
    let best = Poc {
        name: "11 best: 2+3b+4+5+6+9+10b",
        layout: Layout::Compact,
        linger: ms,
        ..p10b
    };
    let best_lz4 = Poc {
        name: "11b best + lz4 (8)",
        layout: Layout::ColumnarLz4,
        ..best
    };
    let pocs = [
        base, p2, p3, p3b, p4, p5, p6, p7, p8, p9, p10a, p10b, best, best_lz4,
    ];

    let pocs: Vec<Poc> = pocs
        .into_iter()
        .filter(|p| only.as_ref().is_none_or(|o| p.name.starts_with(o.as_str())))
        .collect();
    let at_n = (rate * secs) as u64;
    run(pocs[0], &recs, n / 4, None); // warm-up, discarded
    // Repetitions interleave the POCs, so a noisy stretch of this VM hits every row alike.
    let (mut sat, mut at) = (vec![Vec::new(); pocs.len()], vec![Vec::new(); pocs.len()]);
    for _ in 0..reps {
        for (i, &p) in pocs.iter().enumerate() {
            sat[i].push(run(p, &recs, n, None));
            at[i].push(run(p, &recs, at_n, Some(rate)));
        }
    }
    let m = |v: &[Run], f: fn(&Run) -> f64| median(v.iter().map(f).collect());
    let range = |v: &[Run], f: fn(&Run) -> f64, d: usize| {
        let x: Vec<f64> = v.iter().map(f).collect();
        let (lo, hi) = (
            x.iter().copied().fold(f64::MAX, f64::min),
            x.iter().copied().fold(0.0, f64::max),
        );
        format!("{lo:.d$}-{hi:.d$}")
    };
    println!(
        "| POC | records/s | server µs/rec | client µs/rec | server µs/rec @100k | p50 µs @100k | p99 µs @100k | B/rec up | B/rec up @100k | B/rec acks | rec/frame | rec/frame @100k |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (i, p) in pocs.iter().enumerate() {
        let (sat, at) = (&sat[i], &at[i]);
        println!(
            "| {} | {:.0} | {:.3} | {:.3} | {:.3} | {:.0} | {:.0} | {:.1} | {:.1} | {:.2} | {:.0} | {:.1} |",
            p.name,
            m(sat, |r| r.rps),
            m(sat, |r| r.srv_us),
            m(sat, |r| r.cli_us),
            m(at, |r| r.srv_us),
            m(at, |r| r.p50_us),
            m(at, |r| r.p99_us),
            m(sat, |r| r.up_b),
            m(at, |r| r.up_b),
            m(sat, |r| r.down_b),
            m(sat, |r| r.per_frame),
            m(at, |r| r.per_frame),
        );
    }
    println!(
        "\n| POC | records/s range | server µs/rec range | client µs/rec range | server µs/rec @100k range | p99 µs @100k range |"
    );
    println!("|---|---:|---:|---:|---:|---:|");
    for (i, p) in pocs.iter().enumerate() {
        let (sat, at) = (&sat[i], &at[i]);
        println!(
            "| {} | {} | {} | {} | {} | {} |",
            p.name,
            range(sat, |r| r.rps, 0),
            range(sat, |r| r.srv_us, 3),
            range(sat, |r| r.cli_us, 3),
            range(at, |r| r.srv_us, 3),
            range(at, |r| r.p99_us, 0),
        );
    }
}
