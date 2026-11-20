//! Round 3 (transport and I/O POCs, `src/bin/transport.rs`): what every transport shares, so
//! only the I/O layer differs between rows. The frames are round 1's (`u32 len | u8 kind |
//! u64 req_id | body`), the batches its compact layout, the acks cumulative.

use crate::proto::Record;
use crate::proto::codec::{Enc, Key, Layout, Log, decode_hello, encode_hello, visit};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use xxhash_rust::xxh3::xxh3_64;

pub const FRAME: usize = 13;
pub const HELLO: u8 = 1;
pub const APPEND: u8 = 2;
pub const ACK: u8 = 3;
/// Largest frame body a sender builds (round 1: 1 MiB backlog frames).
pub const MAX_FRAME: usize = 1 << 20;
pub const ACK_BYTES: usize = FRAME + 12;
pub const STREAM: &str = "raw.trades.binance-futures";
pub const WRITER: &str = "live-producer-binance-futures-0";

pub fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

/// CPU time, context switches and syscalls of a set of threads.
#[derive(Default, Clone, Copy, Debug)]
pub struct Usage {
    pub cpu: f64,
    pub cs: u64,
    pub sys: u64,
}

impl std::ops::AddAssign for Usage {
    fn add_assign(&mut self, o: Usage) {
        self.cpu += o.cpu;
        self.cs += o.cs;
        self.sys += o.sys;
    }
}

fn rusage() -> (f64, u64) {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_THREAD, &mut u) }, 0);
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 * 1e-6;
    (
        tv(u.ru_utime) + tv(u.ru_stime),
        (u.ru_nvcsw + u.ru_nivcsw) as u64,
    )
}

/// The `raw_syscalls:sys_enter` tracepoint id, if tracefs is mounted.
fn sys_enter_id() -> Option<u64> {
    static ID: std::sync::OnceLock<Option<u64>> = std::sync::OnceLock::new();
    *ID.get_or_init(|| {
        ["/sys/kernel/tracing", "/sys/kernel/debug/tracing"]
            .iter()
            .find_map(|d| {
                std::fs::read_to_string(format!("{d}/events/raw_syscalls/sys_enter/id")).ok()
            })
            .and_then(|s| s.trim().parse().ok())
    })
}

/// `perf_event_attr` up to `PERF_ATTR_SIZE_VER7`; only type, size and config are set.
#[repr(C)]
struct PerfAttr {
    type_: u32,
    size: u32,
    config: u64,
    rest: [u64; 14],
}

/// Counts the calling thread's syscalls (a per-thread perf counter on `sys_enter`).
fn syscall_counter() -> Option<i32> {
    let attr = PerfAttr {
        type_: 2, // PERF_TYPE_TRACEPOINT
        size: size_of::<PerfAttr>() as u32,
        config: sys_enter_id()?,
        rest: [0; 14],
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_perf_event_open,
            &attr as *const PerfAttr,
            0,
            -1,
            -1,
            8u64, // PERF_FLAG_FD_CLOEXEC
        )
    };
    (fd >= 0).then_some(fd as i32)
}

fn read_counter(fd: i32) -> u64 {
    let mut v = 0u64;
    let n = unsafe { libc::read(fd, (&mut v as *mut u64).cast(), 8) };
    assert_eq!(n, 8);
    v
}

/// Started on the thread it measures, stopped on the same thread.
pub struct Meter {
    cpu: f64,
    cs: u64,
    fd: Option<i32>,
}

impl Meter {
    pub fn start() -> Meter {
        let fd = syscall_counter();
        let (cpu, cs) = rusage();
        Meter { cpu, cs, fd }
    }

    pub fn stop(self) -> Usage {
        let (cpu, cs) = rusage();
        let sys = self.fd.map_or(0, |fd| {
            let v = read_counter(fd);
            unsafe { libc::close(fd) };
            v
        });
        Usage {
            cpu: cpu - self.cpu,
            cs: cs - self.cs,
            sys,
        }
    }
}

/// CPU seconds of this process's kernel-side io_uring threads (`iou-sqp-*`, `iou-wrk-*`), which
/// no user thread's `getrusage` includes. 10 ms resolution (`/proc/.../stat` ticks).
pub fn uring_kernel_cpu() -> f64 {
    let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else {
        return 0.0;
    };
    dir.flatten()
        .filter_map(|t| {
            let comm = std::fs::read_to_string(t.path().join("comm")).ok()?;
            if !comm.starts_with("iou-") {
                return None;
            }
            let stat = std::fs::read_to_string(t.path().join("stat")).ok()?;
            let f: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
            Some((f[11].parse::<f64>().ok()? + f[12].parse::<f64>().ok()?) / tck)
        })
        .sum()
}

pub fn frame_header(out: &mut Vec<u8>, kind: u8, req_id: u64, body_len: usize) {
    out.extend_from_slice(&((9 + body_len) as u32).to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&req_id.to_le_bytes());
}

pub fn log() -> Log<'static> {
    Log {
        stream: STREAM,
        writer: WRITER,
        stream_id: 0,
        writer_id: 1,
        epoch: 1,
    }
}

pub fn hello_frame() -> Vec<u8> {
    let mut body = Vec::new();
    encode_hello(&[(0, STREAM), (1, WRITER)], &mut body);
    let mut f = Vec::new();
    frame_header(&mut f, HELLO, 0, body.len());
    f.extend_from_slice(&body);
    f
}

/// Appends one append frame (`copies` byte, then a compact batch) to `out`.
pub fn encode_frame(out: &mut Vec<u8>, enc: &mut Enc, first_seq: u64, batch: &[Record]) {
    let at = out.len();
    enc.head.clear();
    enc.blob.clear();
    enc.encode(Layout::Compact, &log(), first_seq, batch);
    frame_header(out, APPEND, first_seq, 1 + enc.head.len() + enc.blob.len());
    out.push(2);
    out.extend_from_slice(&enc.head);
    out.extend_from_slice(&enc.blob);
    debug_assert_eq!(
        u32::from_le_bytes(out[at..at + 4].try_into().unwrap()) as usize + 4,
        out.len() - at
    );
}

/// How many of `recs` fit one frame of at most `max` body bytes.
pub fn fit(recs: &[Record], max: usize) -> usize {
    let mut est = 64;
    for (i, r) in recs.iter().enumerate() {
        est += 14 + r.payload.len();
        if est > max {
            return i.max(1);
        }
    }
    recs.len()
}

pub fn ack_frame(out: &mut Vec<u8>, through: u64) {
    frame_header(out, ACK, 0, 12);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&through.to_le_bytes());
}

/// What a batch check found: the broker's per-batch work (CRC-32C, structure, bounds, known
/// ids, one dedup hash per record), without the ordering check, which needs connection state.
#[derive(Debug, PartialEq, Eq)]
pub struct Checked {
    pub first_seq: u64,
    pub count: u64,
    pub fold: u64,
}

pub fn check_append(body: &[u8], names: &[Option<String>]) -> Result<Checked, String> {
    let (&copies, batch) = body.split_first().ok_or("empty append")?;
    if copies == 0 {
        return Err("zero copies".into());
    }
    let mut fold = 0u64;
    let b = visit(Layout::Compact, batch, |r| {
        let h = match r.key {
            Key::Id(k) => xxh3_64(k),
            Key::Hash(h) => h,
        };
        fold = fold.rotate_left(5) ^ h ^ r.time_us ^ r.len as u64;
    })?;
    if ![b.stream_id, b.writer_id]
        .iter()
        .all(|&i| names.get(i as usize).is_some_and(Option::is_some))
    {
        return Err("unknown stream or writer id".into());
    }
    Ok(Checked {
        first_seq: b.first_seq,
        count: b.count as u64,
        fold,
    })
}

pub fn apply_hello(body: &[u8], names: &mut Vec<Option<String>>) -> Result<(), String> {
    for (id, name) in decode_hello(body)? {
        let i = id as usize;
        if i > 1024 {
            return Err("id too large".into());
        }
        if names.len() <= i {
            names.resize(i + 1, None);
        }
        names[i] = Some(name);
    }
    Ok(())
}

/// One connection's receive side: an 8 MiB buffer for the connection's life, frames parsed where
/// they landed (round 1's zero-copy server), sequences checked, one cumulative ack per call.
pub struct Conn {
    buf: Vec<u8>,
    start: usize,
    end: usize,
    pub names: Vec<Option<String>>,
    /// Next expected sequence (everything below is validated).
    pub expect: u64,
    acked: u64,
    pub fold: u64,
    pub frames: u64,
    /// Ack once per `filled` (false: someone else acks, e.g. a group commit or a quorum).
    pub ack: bool,
    pub acks: Vec<u8>,
}

impl Default for Conn {
    fn default() -> Self {
        Conn {
            buf: vec![0; 8 << 20],
            start: 0,
            end: 0,
            names: Vec::new(),
            expect: 0,
            acked: 0,
            fold: 0,
            frames: 0,
            ack: true,
            acks: Vec::with_capacity(4096),
        }
    }
}

impl Conn {
    /// Where the next read lands (at least one whole frame of room).
    pub fn spare(&mut self) -> &mut [u8] {
        if self.start == self.end {
            (self.start, self.end) = (0, 0);
        } else if self.buf.len() - self.end < MAX_FRAME + 128 {
            self.buf.copy_within(self.start..self.end, 0); // rare: once per ~7 MiB
            (self.start, self.end) = (0, self.end - self.start);
        }
        &mut self.buf[self.end..]
    }

    /// `n` bytes landed in `spare()`: process every whole frame, calling `each` with its raw
    /// bytes (header included) before checking it.
    pub fn filled_with(&mut self, n: usize, mut each: impl FnMut(&[u8])) -> Result<(), String> {
        self.end += n;
        while self.end - self.start >= 4 {
            let s = self.start;
            let len = u32::from_le_bytes(self.buf[s..s + 4].try_into().unwrap()) as usize;
            if !(9..=MAX_FRAME + 64).contains(&len) {
                return Err(format!("bad frame length {len}"));
            }
            if self.end - s < 4 + len {
                break;
            }
            each(&self.buf[s..s + 4 + len]);
            let (kind, body) = (self.buf[s + 4], &self.buf[s + FRAME..s + 4 + len]);
            match kind {
                HELLO => apply_hello(body, &mut self.names)?,
                APPEND => {
                    let c = check_append(body, &self.names)?;
                    if c.first_seq != self.expect {
                        return Err(format!("sequence gap at {}", self.expect));
                    }
                    self.expect += c.count;
                    self.fold = self.fold.rotate_left(7) ^ c.fold;
                }
                k => return Err(format!("unexpected frame kind {k}")),
            }
            self.frames += 1;
            self.start += 4 + len;
        }
        if self.ack && self.acked < self.expect {
            ack_frame(&mut self.acks, self.expect);
            self.acked = self.expect;
        }
        Ok(())
    }

    pub fn filled(&mut self, n: usize) -> Result<(), String> {
        self.filled_with(n, |_| {})
    }

    /// Copies `data` in (transports that receive into their own buffers).
    pub fn feed(&mut self, mut data: &[u8]) -> Result<(), String> {
        while !data.is_empty() {
            let spare = self.spare();
            let k = spare.len().min(data.len());
            spare[..k].copy_from_slice(&data[..k]);
            self.filled(k)?;
            data = &data[k..];
        }
        Ok(())
    }

    /// True when the stream ended at a frame boundary.
    pub fn clean_end(&self) -> bool {
        self.start == self.end
    }
}

/// Reads cumulative acks out of a byte stream; returns the highest one in `buf[..n]` and how many
/// bytes it consumed.
pub fn parse_acks(buf: &[u8]) -> (Option<u64>, usize) {
    let (mut off, mut top) = (0, None);
    while buf.len() - off >= ACK_BYTES {
        let len = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
        assert_eq!((len, buf[off + 4]), (ACK_BYTES - 4, ACK), "bad ack frame");
        top = Some(u64::from_le_bytes(
            buf[off + 17..off + 25].try_into().unwrap(),
        ));
        off += ACK_BYTES;
    }
    (top, off)
}

/// A single-producer single-consumer byte pipe in shared memory (POC 10, co-located clients):
/// spins a little, then parks. Threads of one process share it here; across processes the same
/// ring would live in a `memfd` mapping and park on a futex in it.
pub struct Pipe {
    buf: Box<[std::cell::UnsafeCell<u8>]>,
    head: AtomicU64,
    _pad: [u64; 7],
    tail: AtomicU64,
    reader_parked: AtomicBool,
    writer_parked: AtomicBool,
    closed: AtomicBool,
    reader: std::sync::Mutex<Option<std::thread::Thread>>,
    writer: std::sync::Mutex<Option<std::thread::Thread>>,
}

unsafe impl Sync for Pipe {}

const SPIN: u32 = 2000;

impl Pipe {
    pub fn new(cap: usize) -> Arc<Pipe> {
        assert!(cap.is_power_of_two());
        Arc::new(Pipe {
            buf: (0..cap).map(|_| std::cell::UnsafeCell::new(0)).collect(),
            head: AtomicU64::new(0),
            _pad: [0; 7],
            tail: AtomicU64::new(0),
            reader_parked: AtomicBool::new(false),
            writer_parked: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            reader: Default::default(),
            writer: Default::default(),
        })
    }

    fn ptr(&self) -> *mut u8 {
        self.buf.as_ptr() as *mut u8
    }

    fn wait(
        &self,
        parked: &AtomicBool,
        me: &std::sync::Mutex<Option<std::thread::Thread>>,
        ready: impl Fn() -> bool,
    ) {
        for _ in 0..SPIN {
            if ready() {
                return;
            }
            std::hint::spin_loop();
        }
        me.lock().unwrap().get_or_insert_with(std::thread::current);
        while !ready() {
            parked.store(true, Ordering::SeqCst);
            if ready() {
                parked.store(false, Ordering::SeqCst);
                return;
            }
            std::thread::park();
            parked.store(false, Ordering::SeqCst);
        }
    }

    fn wake(parked: &AtomicBool, who: &std::sync::Mutex<Option<std::thread::Thread>>) {
        std::sync::atomic::fence(Ordering::SeqCst);
        if parked.load(Ordering::SeqCst)
            && let Some(t) = who.lock().unwrap().as_ref()
        {
            t.unpark();
        }
    }

    pub fn write_all(&self, mut data: &[u8]) {
        let cap = self.buf.len() as u64;
        while !data.is_empty() {
            let head = self.head.load(Ordering::Relaxed);
            self.wait(&self.writer_parked, &self.writer, || {
                head - self.tail.load(Ordering::Acquire) < cap
            });
            let room = (cap - (head - self.tail.load(Ordering::Acquire))) as usize;
            let at = (head % cap) as usize;
            let k = data.len().min(room).min(cap as usize - at);
            unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr().add(at), k) };
            self.head.store(head + k as u64, Ordering::Release);
            Self::wake(&self.reader_parked, &self.reader);
            data = &data[k..];
        }
    }

    /// Blocks until some bytes are there; 0 once the writer closed and everything was read.
    pub fn read(&self, out: &mut [u8]) -> usize {
        let cap = self.buf.len() as u64;
        let tail = self.tail.load(Ordering::Relaxed);
        self.wait(&self.reader_parked, &self.reader, || {
            self.head.load(Ordering::Acquire) > tail || self.closed.load(Ordering::Acquire)
        });
        let head = self.head.load(Ordering::Acquire);
        let at = (tail % cap) as usize;
        let k = ((head - tail) as usize)
            .min(out.len())
            .min(cap as usize - at);
        unsafe { std::ptr::copy_nonoverlapping(self.ptr().add(at), out.as_mut_ptr(), k) };
        self.tail.store(tail + k as u64, Ordering::Release);
        Self::wake(&self.writer_parked, &self.writer);
        k
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        Self::wake(&self.reader_parked, &self.reader);
        if let Some(t) = self.reader.lock().unwrap().as_ref() {
            t.unpark();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::records;

    fn stream(n: usize) -> (Vec<u8>, u64) {
        let recs = records(n, 3);
        let (mut out, mut enc) = (hello_frame(), Enc::default());
        let mut seq = 0;
        while (seq as usize) < n {
            let k = fit(&recs[seq as usize..], 64 * 1024).min(700);
            encode_frame(
                &mut out,
                &mut enc,
                seq,
                &recs[seq as usize..seq as usize + k],
            );
            seq += k as u64;
        }
        (out, seq)
    }

    /// Every transport hands the connection bytes cut differently; the result must not depend on
    /// the cuts, and the ack must cover exactly what was validated.
    #[test]
    fn a_stream_validates_the_same_however_it_is_cut() {
        let (bytes, n) = stream(5000);
        let mut whole = Conn::default();
        whole.feed(&bytes).unwrap();
        assert_eq!(whole.expect, n);
        assert!(whole.clean_end());
        assert_eq!(parse_acks(&whole.acks).0, Some(n));
        for cut in [1usize, 7, 4096, 65_537] {
            let mut c = Conn::default();
            for chunk in bytes.chunks(cut) {
                c.feed(chunk).unwrap();
            }
            assert_eq!(
                (c.expect, c.fold, c.frames),
                (whole.expect, whole.fold, whole.frames)
            );
            assert_eq!(parse_acks(&c.acks).0, Some(n), "cut {cut}");
        }
        // a frame cut short is not acknowledged, and is not a clean end
        let mut c = Conn::default();
        c.feed(&bytes[..bytes.len() - 1]).unwrap();
        assert!(c.expect < n && !c.clean_end());
    }

    #[test]
    fn damage_is_refused() {
        let (mut bytes, _) = stream(2000);
        let at = bytes.len() - 100;
        bytes[at] ^= 1;
        assert!(Conn::default().feed(&bytes).is_err());
        let (bytes, _) = stream(2000);
        let hello = hello_frame().len();
        assert!(
            Conn::default().feed(&bytes[hello..]).is_err(),
            "unknown ids"
        );
    }

    #[test]
    fn the_pipe_delivers_bytes_in_order() {
        let p = Pipe::new(1 << 10);
        let w = {
            let p = p.clone();
            std::thread::spawn(move || {
                for i in 0..20_000u32 {
                    p.write_all(&i.to_le_bytes());
                }
                p.close();
            })
        };
        let (mut got, mut buf) = (Vec::new(), [0u8; 300]);
        loop {
            let n = p.read(&mut buf);
            if n == 0 {
                break;
            }
            got.extend_from_slice(&buf[..n]);
        }
        w.join().unwrap();
        let want: Vec<u8> = (0..20_000u32).flat_map(|i| i.to_le_bytes()).collect();
        assert_eq!(got, want);
    }
}
