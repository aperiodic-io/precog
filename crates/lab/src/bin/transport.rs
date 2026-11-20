//! Round 3: transport and I/O proofs of concept. Every row sends the same records in round 1's
//! frames and compact batches; the server parses, validates (CRC-32C, structure, bounds, ids,
//! sequence contiguity, one dedup hash per record) and sends cumulative acks. Only the I/O layer
//! changes between rows (`results/protocol.md`, round 3).
//!
//! Pinning: server threads on CPU 0 (SQPOLL kernel thread on 1), client writers on 2, ack
//! readers on 3. CPU is `getrusage(RUSAGE_THREAD)` of exactly those threads (plus `iou-*`
//! kernel threads), context switches from the same call, syscalls from a per-thread perf
//! counter on `raw_syscalls:sys_enter`. Latency runs from each record's scheduled arrival to
//! its ack (coordinated-omission-free).
//!
//!   cargo run --release -p precog-lab --bin transport [-- --reps 3 --n 3000000 --secs 2 --only <prefix>|--group <g>]
use io_uring::{IoUring, cqueue, opcode, types};
use precog_lab::net::*;
use precog_lab::proto::codec::Enc;
use precog_lab::proto::{Record, records};
use std::collections::{BinaryHeap, VecDeque};
use std::io::{IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------------------------
// What a POC is
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Spin {
    /// `epoll_wait(-1)`: sleep until ready.
    Park,
    /// `epoll_wait(0)` forever.
    Busy,
    /// `epoll_wait(0)` for up to this many µs after the last event, then sleep.
    Adaptive(u64),
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum UMode {
    /// One-shot `IORING_OP_RECV` into the connection buffer.
    Recv,
    /// `RECV` multishot with a provided-buffer ring, registered files.
    Multishot,
    /// `READ_FIXED` into the registered connection buffer, registered files.
    Fixed,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Srv {
    /// A blocking thread per connection (rounds 1-2).
    Std,
    /// One thread, nonblocking sockets, level-triggered epoll; acks written after every ready
    /// socket was read (coalesced across connections per wake-up).
    Epoll(Spin),
    /// One thread, io_uring: acks of every connection and the next receives in one
    /// `io_uring_enter` per wake-up. `bool`: SQPOLL.
    Uring(UMode, bool),
    /// Today's broker shape (`rpc.rs`, `node.rs`): `read_frame` splits `BytesMut`, a task per
    /// request, an mpsc to a writer task that coalesces. `bool`: multi-thread (2 workers).
    TokioRpc(bool),
    /// Tokio current-thread, one task per connection: read, validate, ack inline.
    TokioInline,
    /// monoio (thread-per-core, io_uring driver), one task per connection, inline.
    Monoio,
    /// A shared-memory byte pipe (co-located client upper bound).
    Shm,
    /// quinn, one bidirectional stream per connection.
    Quic,
    /// Group commit: per-connection readers, a commit thread that "fsyncs" (sleeps) this many µs
    /// and then releases acks. `bool`: the commit thread writes every connection's ack itself
    /// (coalesced) rather than waking one ack writer thread per connection.
    Commit(u64, bool),
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Send {
    /// One `write` per frame.
    Write,
    /// One `writev` of every frame of a call.
    Writev,
    /// One `sendmmsg`, a message per frame.
    Sendmmsg,
    /// `TCP_CORK` on, a write per frame, cork off.
    Cork,
    /// `send(MSG_ZEROCOPY)`, buffers reused after their completion on the error queue.
    MsgZc,
    /// io_uring `SEND_ZC`, buffers reused after their notification.
    UringZc,
}

#[derive(Clone, Copy, Debug)]
struct Cli {
    conns: usize,
    inflight: usize,
    linger_us: u64,
    /// Largest frame body (the backlog frame cap).
    max_frame: usize,
    /// Fixed records per frame instead of backlog frames (today's loadgen sends 250).
    fixed: Option<usize>,
    /// Frames handed to one send call.
    per_call: usize,
    send: Send,
    nodelay: bool,
    sndbuf: Option<usize>,
    /// CPUs of the writer and the ack reader.
    wcpu: usize,
    rcpu: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct Sock {
    quickack: bool,
    busy_poll_us: Option<i32>,
    rcvbuf: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Net {
    Lo,
    /// A veth pair into a network namespace (`plab`).
    Veth,
}

#[derive(Clone, Copy)]
struct Poc {
    group: &'static str,
    name: &'static str,
    srv: Srv,
    cli: Cli,
    sock: Sock,
    net: Net,
    /// Shell commands run before / after each run of this row (offload toggles).
    setup: &'static str,
    teardown: &'static str,
}

const CLI: Cli = Cli {
    conns: 1,
    inflight: 8,
    linger_us: 1000,
    max_frame: MAX_FRAME,
    fixed: None,
    per_call: 1,
    send: Send::Write,
    nodelay: true,
    sndbuf: None,
    wcpu: 2,
    rcpu: 3,
};

fn sh(cmd: &str) {
    if cmd.is_empty() {
        return;
    }
    let st = std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .status()
        .unwrap();
    assert!(st.success(), "{cmd}");
}

// ---------------------------------------------------------------------------------------------
// Sockets
// ---------------------------------------------------------------------------------------------

fn setsockopt(fd: RawFd, level: i32, opt: i32, v: i32) {
    let r = unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            (&v as *const i32).cast(),
            size_of::<i32>() as u32,
        )
    };
    assert_eq!(
        r,
        0,
        "setsockopt {opt}: {}",
        std::io::Error::last_os_error()
    );
}

fn getsockopt(fd: RawFd, level: i32, opt: i32) -> i32 {
    let (mut v, mut l) = (0i32, size_of::<i32>() as u32);
    unsafe { libc::getsockopt(fd, level, opt, (&mut v as *mut i32).cast(), &mut l) };
    v
}

const VETH_NS: &str = "plab";
const VETH_SRV: &str = "10.88.0.2";

fn veth_up() {
    sh(&format!(
        "ip netns del {VETH_NS} 2>/dev/null; ip link del plab0 2>/dev/null; \
         ip netns add {VETH_NS} && ip link add plab0 type veth peer name plab1 && \
         ip link set plab1 netns {VETH_NS} && ip addr add 10.88.0.1/24 dev plab0 && ip link set plab0 up && \
         ip netns exec {VETH_NS} ip addr add {VETH_SRV}/24 dev plab1 && \
         ip netns exec {VETH_NS} ip link set plab1 up && ip netns exec {VETH_NS} ip link set lo up"
    ));
}

/// A listener on loopback, or inside the veth namespace (bound from a thread that entered it;
/// the socket keeps its namespace).
fn listen(net: Net) -> TcpListener {
    match net {
        Net::Lo => TcpListener::bind("127.0.0.1:0").unwrap(),
        Net::Veth => std::thread::spawn(|| {
            let f = std::fs::File::open(format!("/var/run/netns/{VETH_NS}")).unwrap();
            assert_eq!(unsafe { libc::setns(f.as_raw_fd(), libc::CLONE_NEWNET) }, 0);
            TcpListener::bind(format!("{VETH_SRV}:0")).unwrap()
        })
        .join()
        .unwrap(),
    }
}

fn server_sock(s: &TcpStream, o: Sock) -> i32 {
    s.set_nodelay(true).unwrap();
    let fd = s.as_raw_fd();
    if let Some(b) = o.rcvbuf {
        setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, b as i32);
    }
    if let Some(us) = o.busy_poll_us {
        setsockopt(fd, libc::SOL_SOCKET, libc::SO_BUSY_POLL, us);
    }
    getsockopt(fd, libc::SOL_SOCKET, libc::SO_INCOMING_NAPI_ID)
}

fn client_sock(addr: SocketAddr, c: &Cli) -> TcpStream {
    let s = TcpStream::connect(addr).unwrap();
    s.set_nodelay(c.nodelay).unwrap();
    if let Some(b) = c.sndbuf {
        setsockopt(s.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, b as i32);
    }
    if c.send == Send::MsgZc {
        setsockopt(s.as_raw_fd(), libc::SOL_SOCKET, libc::SO_ZEROCOPY, 1);
    }
    s
}

fn write_all_fd(fd: RawFd, mut b: &[u8]) {
    while !b.is_empty() {
        let n = unsafe { libc::write(fd, b.as_ptr().cast(), b.len()) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "write: {e}");
            std::hint::spin_loop();
            continue;
        }
        b = &b[n as usize..];
    }
}

// ---------------------------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------------------------

/// The send side of one connection. Frames are owned buffers; `free` hands them back for reuse
/// once the transport no longer needs them (zero-copy sends hold them until completion).
trait Tx: std::marker::Send {
    fn send(&mut self, frames: Vec<Vec<u8>>);
    fn buf(&mut self) -> Vec<u8>;
    /// Wait for outstanding zero-copy completions; report whether the kernel copied anyway.
    fn finish(&mut self) -> Option<bool> {
        None
    }
}

struct TcpTx {
    s: TcpStream,
    mode: Send,
    free: Vec<Vec<u8>>,
    /// Zero-copy: (last completion id this frame needs, frame).
    pending: VecDeque<(u32, Vec<u8>)>,
    next_id: u32,
    done_through: Option<u32>,
    copied: bool,
    ring: Option<IoUring>,
}

impl TcpTx {
    fn new(s: TcpStream, mode: Send) -> TcpTx {
        let ring = (mode == Send::UringZc).then(|| IoUring::new(64).unwrap());
        TcpTx {
            s,
            mode,
            free: Vec::new(),
            pending: VecDeque::new(),
            next_id: 0,
            done_through: None,
            copied: false,
            ring,
        }
    }

    fn recycle(&mut self, mut b: Vec<u8>) {
        b.clear();
        self.free.push(b);
    }

    /// MSG_ZEROCOPY completions from the error queue; `block` waits for at least one.
    fn reap_errqueue(&mut self, block: bool) {
        let fd = self.s.as_raw_fd();
        loop {
            let mut ctrl = [0u64; 16];
            let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
            msg.msg_control = ctrl.as_mut_ptr().cast();
            msg.msg_controllen = size_of_val(&ctrl);
            let r = unsafe { libc::recvmsg(fd, &mut msg, libc::MSG_ERRQUEUE | libc::MSG_DONTWAIT) };
            if r < 0 {
                if block {
                    let mut p = libc::pollfd {
                        fd,
                        events: 0,
                        revents: 0,
                    };
                    unsafe { libc::poll(&mut p, 1, -1) };
                    continue;
                }
                return;
            }
            let cm = unsafe { libc::CMSG_FIRSTHDR(&msg) };
            assert!(!cm.is_null());
            let ee = unsafe { &*(libc::CMSG_DATA(cm) as *const libc::sock_extended_err) };
            assert_eq!(ee.ee_origin, 5 /* SO_EE_ORIGIN_ZEROCOPY */);
            if ee.ee_code & 1 /* SO_EE_CODE_ZEROCOPY_COPIED */ != 0 {
                self.copied = true;
            }
            self.done_through = Some(ee.ee_data);
            self.release();
            if !block {
                continue;
            }
            return;
        }
    }

    fn release(&mut self) {
        while let (Some((id, _)), Some(d)) = (self.pending.front(), self.done_through) {
            if *id > d {
                break;
            }
            let (_, b) = self.pending.pop_front().unwrap();
            self.recycle(b);
        }
    }

    /// `SEND_ZC` notifications (their `user_data` is the send's id); a frame is released once
    /// the notification of its last send arrived. Result CQEs were consumed in `send_zc`.
    fn reap_uring(&mut self, wait: usize) {
        let ring = self.ring.as_mut().unwrap();
        ring.submit_and_wait(wait).unwrap();
        let notifs: Vec<u32> = ring
            .completion()
            .filter(|c| cqueue::notif(c.flags()))
            .map(|c| c.user_data() as u32)
            .collect();
        for d in notifs {
            self.done_through = Some(self.done_through.map_or(d, |x| x.max(d)));
        }
        self.release();
    }

    fn send_zc(&mut self, f: &[u8]) {
        let fd = self.s.as_raw_fd();
        let mut off = 0;
        while off < f.len() {
            let n = match self.mode {
                Send::MsgZc => {
                    let r = unsafe {
                        libc::send(
                            fd,
                            f[off..].as_ptr().cast(),
                            f.len() - off,
                            libc::MSG_ZEROCOPY,
                        )
                    };
                    if r < 0 {
                        let e = std::io::Error::last_os_error();
                        assert_eq!(e.raw_os_error(), Some(libc::ENOBUFS), "send: {e}");
                        self.reap_errqueue(true);
                        continue;
                    }
                    self.next_id = self.next_id.wrapping_add(1);
                    r as usize
                }
                _ => {
                    let id = self.next_id;
                    let e = opcode::SendZc::new(
                        types::Fd(fd),
                        f[off..].as_ptr(),
                        (f.len() - off) as u32,
                    )
                    .build()
                    .user_data(id as u64);
                    let ring = self.ring.as_mut().unwrap();
                    unsafe { ring.submission().push(&e).unwrap() };
                    ring.submit_and_wait(1).unwrap();
                    let mut sent = None;
                    let mut notifs = Vec::new();
                    for c in ring.completion() {
                        if cqueue::notif(c.flags()) {
                            notifs.push(c.user_data() as u32);
                        } else {
                            assert!(c.result() >= 0, "send_zc: {}", c.result());
                            sent = Some(c.result() as usize);
                        }
                    }
                    for d in notifs {
                        self.done_through = Some(self.done_through.map_or(d, |x| x.max(d)));
                    }
                    self.next_id += 1;
                    match sent {
                        Some(n) => n,
                        None => {
                            // the result CQE comes first; wait for it
                            let ring = self.ring.as_mut().unwrap();
                            ring.submit_and_wait(1).unwrap();
                            let c = ring.completion().next().unwrap();
                            assert!(!cqueue::notif(c.flags()) && c.result() >= 0);
                            c.result() as usize
                        }
                    }
                }
            };
            off += n;
        }
    }
}

impl Tx for TcpTx {
    fn buf(&mut self) -> Vec<u8> {
        loop {
            if let Some(b) = self.free.pop() {
                return b;
            }
            match self.mode {
                Send::MsgZc if self.pending.len() >= 16 => self.reap_errqueue(true),
                Send::UringZc if self.pending.len() >= 16 => self.reap_uring(1),
                _ => return Vec::with_capacity(MAX_FRAME + 64),
            }
        }
    }

    fn send(&mut self, frames: Vec<Vec<u8>>) {
        let fd = self.s.as_raw_fd();
        match self.mode {
            Send::Write => frames.iter().for_each(|f| self.s.write_all(f).unwrap()),
            Send::Cork => {
                setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_CORK, 1);
                frames.iter().for_each(|f| self.s.write_all(f).unwrap());
                setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_CORK, 0);
            }
            Send::Writev => {
                let mut v: Vec<IoSlice> = frames.iter().map(|f| IoSlice::new(f)).collect();
                let mut bufs = &mut v[..];
                while !bufs.is_empty() {
                    let n = self.s.write_vectored(bufs).unwrap();
                    IoSlice::advance_slices(&mut bufs, n);
                }
            }
            Send::Sendmmsg => {
                let mut iov: Vec<libc::iovec> = frames
                    .iter()
                    .map(|f| libc::iovec {
                        iov_base: f.as_ptr() as *mut _,
                        iov_len: f.len(),
                    })
                    .collect();
                let mut msgs: Vec<libc::mmsghdr> = iov
                    .iter_mut()
                    .map(|v| {
                        let mut m: libc::mmsghdr = unsafe { std::mem::zeroed() };
                        m.msg_hdr.msg_iov = v;
                        m.msg_hdr.msg_iovlen = 1;
                        m
                    })
                    .collect();
                let mut at = 0;
                while at < msgs.len() {
                    let r = unsafe {
                        libc::sendmmsg(fd, msgs[at..].as_mut_ptr(), (msgs.len() - at) as u32, 0)
                    };
                    assert!(r > 0, "sendmmsg: {}", std::io::Error::last_os_error());
                    for m in &msgs[at..at + r as usize] {
                        let i = at;
                        let _ = i;
                        let len = unsafe { (*m.msg_hdr.msg_iov).iov_len };
                        if (m.msg_len as usize) < len {
                            let base = unsafe { (*m.msg_hdr.msg_iov).iov_base as *const u8 };
                            let rest = unsafe {
                                std::slice::from_raw_parts(
                                    base.add(m.msg_len as usize),
                                    len - m.msg_len as usize,
                                )
                            };
                            self.s.write_all(rest).unwrap();
                        }
                    }
                    at += r as usize;
                }
            }
            Send::MsgZc | Send::UringZc => {
                for f in frames {
                    self.send_zc(&f);
                    let id = self.next_id.wrapping_sub(1);
                    self.pending.push_back((id, f));
                }
                match self.mode {
                    Send::MsgZc => self.reap_errqueue(false),
                    _ => self.reap_uring(0),
                }
                return;
            }
        }
        for f in frames {
            self.recycle(f);
        }
    }

    fn finish(&mut self) -> Option<bool> {
        match self.mode {
            Send::MsgZc => {
                while !self.pending.is_empty() {
                    self.reap_errqueue(true);
                }
                Some(self.copied)
            }
            Send::UringZc => {
                while !self.pending.is_empty() {
                    self.reap_uring(1);
                }
                None // ponytail: copy usage not requested (IORING_SEND_ZC_REPORT_USAGE); lo/veth copy anyway
            }
            _ => None,
        }
    }
}

struct PipeTx(Arc<Pipe>, Vec<Vec<u8>>);

impl Tx for PipeTx {
    fn send(&mut self, frames: Vec<Vec<u8>>) {
        for mut f in frames {
            self.0.write_all(&f);
            f.clear();
            self.1.push(f);
        }
    }
    fn buf(&mut self) -> Vec<u8> {
        self.1
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(MAX_FRAME + 64))
    }
}

/// A quinn stream driven from a plain thread (the endpoint runs on the client's runtime).
struct QuicTx(tokio::runtime::Handle, quinn::SendStream, Vec<Vec<u8>>);

impl Tx for QuicTx {
    fn send(&mut self, frames: Vec<Vec<u8>>) {
        for mut f in frames {
            self.0.block_on(self.1.write_all(&f)).unwrap();
            f.clear();
            self.2.push(f);
        }
    }
    fn buf(&mut self) -> Vec<u8> {
        self.2
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(MAX_FRAME + 64))
    }
}

trait Rx: std::marker::Send {
    fn recv(&mut self, buf: &mut [u8]) -> usize;
}

impl Rx for TcpStream {
    fn recv(&mut self, buf: &mut [u8]) -> usize {
        Read::read(self, buf).unwrap()
    }
}

struct PipeRx(Arc<Pipe>);

impl Rx for PipeRx {
    fn recv(&mut self, buf: &mut [u8]) -> usize {
        self.0.read(buf)
    }
}

struct QuicRx(tokio::runtime::Handle, quinn::RecvStream);

impl Rx for QuicRx {
    fn recv(&mut self, buf: &mut [u8]) -> usize {
        self.0.block_on(self.1.read(buf)).unwrap().unwrap_or(0)
    }
}

struct CliOut {
    usage: Usage,
    up: u64,
    frames: u64,
    done: Instant,
    lat: Vec<u64>,
    zc_copied: Option<bool>,
}

/// Reads acks; publishes the highest acked sequence and wakes the writer.
fn read_acks(
    mut rx: Box<dyn Rx>,
    n: u64,
    rate: Option<f64>,
    start: Instant,
    acked: &AtomicU64,
    writer: std::thread::Thread,
    cpu: usize,
) -> (Usage, Instant, Vec<u64>) {
    pin(cpu);
    let m = Meter::start();
    let mut lat = Vec::with_capacity(if rate.is_some() { n as usize } else { 0 });
    let mut buf = vec![0u8; 64 * 1024];
    let (mut fill, mut done) = (0, 0u64);
    while done < n {
        let r = rx.recv(&mut buf[fill..]);
        assert!(r > 0, "server closed at {done} of {n}");
        fill += r;
        let (top, used) = parse_acks(&buf[..fill]);
        buf.copy_within(used..fill, 0);
        fill -= used;
        if let Some(end) = top.filter(|&e| e > done) {
            if let Some(rate) = rate {
                let now = start.elapsed().as_nanos() as u64;
                lat.extend((done..end).map(|s| now.saturating_sub((s as f64 * 1e9 / rate) as u64)));
            }
            done = end;
            acked.store(done, Ordering::Release);
            writer.unpark();
        }
    }
    (m.stop(), Instant::now(), lat)
}

/// One connection's writer (round 1's client): pipelined, backlog frames up to `max_frame`
/// with linger at a fixed rate, or fixed-size frames, `per_call` frames per send.
#[allow(clippy::too_many_arguments)]
fn client(
    mut tx: Box<dyn Tx>,
    rx: Box<dyn Rx>,
    c: Cli,
    recs: Arc<Vec<Record>>,
    n: u64,
    rate: Option<f64>,
    start: Instant,
    hello: bool,
) -> CliOut {
    let acked = Arc::new(AtomicU64::new(0));
    let reader = {
        let (acked, me) = (acked.clone(), std::thread::current());
        std::thread::spawn(move || read_acks(rx, n, rate, start, &acked, me, c.rcpu))
    };
    pin(c.wcpu);
    let m = Meter::start();
    let (mut up, mut frames) = (0u64, 0u64);
    if hello {
        let mut b = tx.buf();
        b.extend_from_slice(&hello_frame());
        up += b.len() as u64;
        tx.send(vec![b]);
    }
    let arrival = |seq: u64| start + Duration::from_secs_f64(seq as f64 / rate.unwrap());
    let avail = || match rate {
        None => n,
        Some(r) => n.min((start.elapsed().as_secs_f64() * r) as u64),
    };
    let linger = Duration::from_micros(c.linger_us);
    let full = c.fixed.unwrap_or(4096) as u64 * c.per_call as u64;
    let mut inflight: VecDeque<u64> = VecDeque::new();
    let (mut sent, mut enc) = (0u64, Enc::default());
    let len = recs.len() as u64;
    while sent < n {
        let need = c.fixed.map_or(1, |k| (k as u64).min(n - sent));
        if avail() < sent + need {
            std::thread::sleep(arrival(sent + need - 1).saturating_duration_since(Instant::now()));
            continue;
        }
        if rate.is_some() && !linger.is_zero() && avail() - sent < full && avail() < n {
            let ready = arrival(sent) + linger;
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
            if inflight.len() < c.inflight {
                break;
            }
            std::thread::park();
        }
        let mut out = Vec::with_capacity(c.per_call);
        let top = avail();
        while out.len() < c.per_call && sent < top {
            let first = (sent % len) as usize;
            let mut k = (top - sent).min(len - first as u64) as usize;
            if let Some(f) = c.fixed {
                if (k as u64) < (f as u64).min(n - sent).min(len - first as u64) {
                    break;
                }
                k = k.min(f);
            }
            k = k.min(fit(&recs[first..first + k], c.max_frame)).min(65_536);
            let mut b = tx.buf();
            encode_frame(&mut b, &mut enc, sent, &recs[first..first + k]);
            up += b.len() as u64;
            out.push(b);
            sent += k as u64;
        }
        frames += out.len() as u64;
        tx.send(out);
        inflight.push_back(sent);
    }
    let zc_copied = tx.finish();
    let mut usage = m.stop();
    let (ru, done, lat) = reader.join().unwrap();
    usage += ru;
    drop(tx);
    CliOut {
        usage,
        up,
        frames,
        done,
        lat,
        zc_copied,
    }
}

// ---------------------------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct SrvOut {
    usage: Usage,
    records: u64,
    down: u64,
    fold: u64,
    napi: i32,
}

fn add(o: &mut SrvOut, c: &Conn, down: u64) {
    o.records += c.expect;
    o.fold = o.fold.wrapping_add(c.fold);
    o.down += down;
}

fn serve_std(mut s: TcpStream, o: Sock) -> SrvOut {
    pin(0);
    let m = Meter::start();
    let (mut c, mut down) = (Conn::default(), 0);
    let fd = s.as_raw_fd();
    loop {
        let n = s.read(c.spare()).unwrap();
        if n == 0 {
            break;
        }
        c.filled(n).unwrap();
        if o.quickack {
            setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_QUICKACK, 1);
        }
        if !c.acks.is_empty() {
            s.write_all(&c.acks).unwrap();
            down += c.acks.len() as u64;
            c.acks.clear();
        }
    }
    assert!(c.clean_end());
    let mut out = SrvOut {
        usage: m.stop(),
        ..Default::default()
    };
    add(&mut out, &c, down);
    out
}

fn serve_epoll(socks: Vec<TcpStream>, spin: Spin) -> SrvOut {
    pin(0);
    let m = Meter::start();
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    let mut conns: Vec<(Conn, bool)> = Vec::new();
    for (i, s) in socks.iter().enumerate() {
        s.set_nonblocking(true).unwrap();
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: i as u64,
        };
        unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, s.as_raw_fd(), &mut ev) };
        conns.push((Conn::default(), true));
    }
    let (mut open, mut down) = (socks.len(), 0u64);
    let mut evs = vec![libc::epoll_event { events: 0, u64: 0 }; 64];
    let mut last = Instant::now();
    while open > 0 {
        let timeout = match spin {
            Spin::Park => -1,
            Spin::Busy => 0,
            Spin::Adaptive(us) => {
                if last.elapsed() < Duration::from_micros(us) {
                    0
                } else {
                    -1
                }
            }
        };
        let k = unsafe { libc::epoll_wait(ep, evs.as_mut_ptr(), evs.len() as i32, timeout) };
        if k <= 0 {
            continue;
        }
        last = Instant::now();
        for e in &evs[..k as usize] {
            let i = e.u64 as usize;
            let fd = socks[i].as_raw_fd();
            let (c, live) = &mut conns[i];
            loop {
                let sp = c.spare();
                let n = unsafe { libc::read(fd, sp.as_mut_ptr().cast(), sp.len()) };
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock, "read: {err}");
                    break;
                }
                if n == 0 {
                    *live = false;
                    open -= 1;
                    unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
                    break;
                }
                c.filled(n as usize).unwrap();
            }
        }
        // every connection's acks after the wake-up's reads
        for (i, (c, _)) in conns.iter_mut().enumerate() {
            if !c.acks.is_empty() {
                write_all_fd(socks[i].as_raw_fd(), &c.acks);
                down += c.acks.len() as u64;
                c.acks.clear();
            }
        }
    }
    unsafe { libc::close(ep) };
    let mut out = SrvOut {
        usage: m.stop(),
        ..Default::default()
    };
    for (c, _) in &conns {
        add(&mut out, c, down);
        down = 0;
    }
    out
}

const BGID: u16 = 7;
const RING_BUFS: u16 = 64;
const RING_BUF: usize = 64 * 1024;

/// A provided-buffer ring (`io_uring_buf` entries; the tail aliases entry 0's `resv`).
struct BufRing {
    ring: *mut u8,
    data: Vec<u8>,
    tail: u16,
}

impl BufRing {
    fn new(r: &IoUring) -> BufRing {
        let bytes = RING_BUFS as usize * 16;
        let ring = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        } as *mut u8;
        assert!(!ring.is_null());
        unsafe {
            r.submitter()
                .register_buf_ring_with_flags(ring as u64, RING_BUFS, BGID, 0)
                .unwrap()
        };
        let mut b = BufRing {
            ring,
            data: vec![0; RING_BUFS as usize * RING_BUF],
            tail: 0,
        };
        for bid in 0..RING_BUFS {
            b.put(bid);
        }
        b.publish();
        b
    }

    fn put(&mut self, bid: u16) {
        let e = unsafe { self.ring.add((self.tail & (RING_BUFS - 1)) as usize * 16) };
        unsafe {
            (e as *mut u64).write(self.data.as_ptr().add(bid as usize * RING_BUF) as u64);
            (e.add(8) as *mut u32).write(RING_BUF as u32);
            (e.add(12) as *mut u16).write(bid);
        }
        self.tail = self.tail.wrapping_add(1);
    }

    fn publish(&self) {
        let t = unsafe { &*(self.ring.add(14) as *const std::sync::atomic::AtomicU16) };
        t.store(self.tail, Ordering::Release);
    }

    fn get(&self, bid: u16, n: usize) -> &[u8] {
        &self.data[bid as usize * RING_BUF..bid as usize * RING_BUF + n]
    }
}

fn serve_uring(socks: Vec<TcpStream>, mode: UMode, sqpoll: bool) -> SrvOut {
    pin(0);
    let m = Meter::start();
    let mut b = IoUring::builder();
    if sqpoll {
        b.setup_sqpoll(1000).setup_sqpoll_cpu(1);
    } else {
        b.setup_coop_taskrun()
            .setup_single_issuer()
            .setup_defer_taskrun();
    }
    let mut ring: IoUring = b.build(256).unwrap();
    let fds: Vec<RawFd> = socks.iter().map(|s| s.as_raw_fd()).collect();
    if mode != UMode::Recv {
        ring.submitter().register_files(&fds).unwrap();
    }
    let mut conns: Vec<Conn> = (0..socks.len()).map(|_| Conn::default()).collect();
    if mode == UMode::Fixed {
        let iov: Vec<libc::iovec> = conns
            .iter_mut()
            .map(|c| {
                let s = c.spare();
                libc::iovec {
                    iov_base: s.as_mut_ptr().cast(),
                    iov_len: s.len(),
                }
            })
            .collect();
        unsafe { ring.submitter().register_buffers(&iov).unwrap() };
    }
    let mut bufs = (mode == UMode::Multishot).then(|| BufRing::new(&ring));
    let k = socks.len();
    let (mut armed, mut live) = (vec![false; k], vec![true; k]);
    let mut sending: Vec<Option<Vec<u8>>> = vec![None; k];
    let mut spare_bufs: Vec<Vec<u8>> = (0..k).map(|_| Vec::with_capacity(4096)).collect();
    let mut down = 0u64;
    loop {
        {
            let mut sq = ring.submission();
            for i in 0..k {
                if live[i] && !armed[i] {
                    let e = match mode {
                        UMode::Recv => {
                            let sp = conns[i].spare();
                            opcode::Recv::new(types::Fd(fds[i]), sp.as_mut_ptr(), sp.len() as u32)
                                .build()
                        }
                        UMode::Multishot => {
                            opcode::RecvMulti::new(types::Fixed(i as u32), BGID).build()
                        }
                        UMode::Fixed => {
                            let sp = conns[i].spare();
                            opcode::ReadFixed::new(
                                types::Fixed(i as u32),
                                sp.as_mut_ptr(),
                                sp.len() as u32,
                                i as u16,
                            )
                            .build()
                        }
                    };
                    unsafe { sq.push(&e.user_data((i as u64) << 8 | 1)).unwrap() };
                    armed[i] = true;
                }
                if sending[i].is_none() && !conns[i].acks.is_empty() {
                    let mut b = std::mem::take(&mut spare_bufs[i]);
                    std::mem::swap(&mut b, &mut conns[i].acks);
                    let e = if mode == UMode::Recv {
                        opcode::Send::new(types::Fd(fds[i]), b.as_ptr(), b.len() as u32).build()
                    } else {
                        opcode::Send::new(types::Fixed(i as u32), b.as_ptr(), b.len() as u32)
                            .build()
                    };
                    unsafe { sq.push(&e.user_data((i as u64) << 8 | 2)).unwrap() };
                    sending[i] = Some(b);
                }
            }
        }
        if !live.iter().any(|&l| l) && sending.iter().all(Option::is_none) {
            break;
        }
        ring.submit_and_wait(1).unwrap();
        let cqes: Vec<cqueue::Entry> = ring.completion().collect();
        let mut returned = false;
        for c in cqes {
            let (i, op, res) = (
                (c.user_data() >> 8) as usize,
                c.user_data() & 0xff,
                c.result(),
            );
            if op == 2 {
                let mut b = sending[i].take().unwrap();
                assert_eq!(res as usize, b.len(), "short ack send");
                down += b.len() as u64;
                b.clear();
                spare_bufs[i] = b;
                continue;
            }
            if res == -libc::ENOBUFS {
                armed[i] = false;
                continue;
            }
            assert!(
                res >= 0,
                "recv: {}",
                std::io::Error::from_raw_os_error(-res)
            );
            if mode != UMode::Multishot || !cqueue::more(c.flags()) {
                armed[i] = false;
            }
            if res == 0 {
                live[i] = false;
                continue;
            }
            match (mode, bufs.as_mut()) {
                (UMode::Multishot, Some(br)) => {
                    let bid = cqueue::buffer_select(c.flags()).unwrap();
                    conns[i].feed(br.get(bid, res as usize)).unwrap();
                    br.put(bid);
                    returned = true;
                }
                _ => conns[i].filled(res as usize).unwrap(),
            }
        }
        if returned {
            bufs.as_ref().unwrap().publish();
        }
    }
    let kcpu = uring_kernel_cpu();
    drop(ring);
    let mut out = SrvOut {
        usage: m.stop(),
        ..Default::default()
    };
    out.usage.cpu += kcpu;
    for c in &conns {
        add(&mut out, c, down);
        down = 0;
    }
    out
}

thread_local! {
    static TMETER: std::cell::RefCell<Option<Meter>> = const { std::cell::RefCell::new(None) };
}

/// A tokio runtime whose threads are pinned to `cpu` and metered into `sink` when they stop.
fn tokio_rt(mt: bool, cpu: usize, sink: Arc<Mutex<Usage>>) -> tokio::runtime::Runtime {
    let mut b = if mt {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(if cpu == 0 { 2 } else { 1 });
        b
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    b.enable_all()
        .on_thread_start(move || {
            pin(cpu);
            TMETER.with(|m| *m.borrow_mut() = Some(Meter::start()));
        })
        .on_thread_stop(move || {
            if let Some(m) = TMETER.with(|m| m.borrow_mut().take()) {
                *sink.lock().unwrap() += m.stop();
            }
        })
        .build()
        .unwrap()
}

async fn rpc_conn(s: TcpStream) -> (u64, u64, u64) {
    use bytes::{Buf, BytesMut};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    s.set_nonblocking(true).unwrap();
    let s = tokio::net::TcpStream::from_std(s).unwrap();
    let (mut r, mut w) = s.into_split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, u64, u64)>(4096);
    let writer = tokio::spawn(async move {
        let (mut heap, mut acked, mut fold, mut down) = (BinaryHeap::new(), 0u64, 0u64, 0u64);
        let mut out = Vec::with_capacity(4096);
        while let Some(x) = rx.recv().await {
            heap.push(std::cmp::Reverse(x));
            while let Ok(x) = rx.try_recv() {
                heap.push(std::cmp::Reverse(x));
            }
            let before = acked;
            while heap.peek().is_some_and(|x| x.0.0 == acked) {
                let (_, end, f) = heap.pop().unwrap().0;
                acked = end;
                fold = fold.rotate_left(7) ^ f;
            }
            if acked > before {
                ack_frame(&mut out, acked);
                w.write_all(&out).await.unwrap();
                w.flush().await.unwrap();
                down += out.len() as u64;
                out.clear();
            }
        }
        (acked, fold, down)
    });
    let mut buf = BytesMut::new();
    let mut names = Vec::new();
    let mut shared: Option<Arc<Vec<Option<String>>>> = None;
    let mut expect = 0u64;
    loop {
        // `rpc::read_frame`
        if buf.len() >= 4 {
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            assert!((9..=MAX_FRAME + 64).contains(&len));
            if buf.len() >= 4 + len {
                buf.advance(4);
                let mut f = buf.split_to(len);
                let kind = f[0];
                f.advance(9);
                let f = f.freeze();
                if kind == HELLO {
                    apply_hello(&f, &mut names).unwrap();
                    shared = Some(Arc::new(names.clone()));
                    continue;
                }
                let first = u64::from_le_bytes(f[25..33].try_into().unwrap());
                let count = u32::from_le_bytes(f[33..37].try_into().unwrap()) as u64;
                assert_eq!(first, expect, "sequence gap");
                expect += count;
                let (tx, names) = (tx.clone(), shared.clone().unwrap());
                tokio::spawn(async move {
                    let c = check_append(&f, &names).unwrap();
                    tx.send((c.first_seq, c.first_seq + c.count, c.fold))
                        .await
                        .unwrap();
                });
                continue;
            }
            buf.reserve(4 + len - buf.len());
        } else {
            buf.reserve(64 * 1024);
        }
        if r.read_buf(&mut buf).await.unwrap() == 0 {
            assert!(buf.is_empty());
            break;
        }
    }
    drop(tx);
    let (acked, fold, down) = writer.await.unwrap();
    assert_eq!(acked, expect);
    (acked, fold, down)
}

async fn inline_conn(s: TcpStream) -> (u64, u64, u64) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    s.set_nonblocking(true).unwrap();
    let mut s = tokio::net::TcpStream::from_std(s).unwrap();
    let (mut c, mut down) = (Conn::default(), 0);
    loop {
        let n = s.read(c.spare()).await.unwrap();
        if n == 0 {
            break;
        }
        c.filled(n).unwrap();
        if !c.acks.is_empty() {
            s.write_all(&c.acks).await.unwrap();
            down += c.acks.len() as u64;
            c.acks.clear();
        }
    }
    (c.expect, c.fold, down)
}

fn serve_tokio(socks: Vec<TcpStream>, mt: bool, inline: bool) -> SrvOut {
    pin(0);
    let sink = Arc::new(Mutex::new(Usage::default()));
    let rt = tokio_rt(mt, 0, sink.clone());
    let m = Meter::start();
    let res = rt.block_on(async {
        let hs: Vec<_> = socks
            .into_iter()
            .map(|s| {
                if inline {
                    tokio::spawn(inline_conn(s))
                } else {
                    tokio::spawn(rpc_conn(s))
                }
            })
            .collect();
        let mut v = Vec::new();
        for h in hs {
            v.push(h.await.unwrap());
        }
        v
    });
    let me = m.stop();
    drop(rt);
    let mut out = SrvOut {
        usage: *sink.lock().unwrap(),
        ..Default::default()
    };
    out.usage += me;
    for (e, f, d) in res {
        out.records += e;
        out.fold = out.fold.wrapping_add(f);
        out.down += d;
    }
    out
}

fn serve_monoio(socks: Vec<TcpStream>) -> SrvOut {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
    pin(0);
    let m = Meter::start();
    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .unwrap();
    let res = rt.block_on(async {
        let hs: Vec<_> = socks
            .into_iter()
            .map(|s| {
                monoio::spawn(async move {
                    s.set_nonblocking(true).unwrap();
                    let mut s = monoio::net::TcpStream::from_std(s).unwrap();
                    let (mut c, mut down) = (Conn::default(), 0u64);
                    let mut acks = Vec::with_capacity(4096);
                    loop {
                        let sp = c.spare();
                        let raw = unsafe { monoio::buf::RawBuf::new(sp.as_mut_ptr(), sp.len()) };
                        let (r, _) = s.read(raw).await;
                        let n = r.unwrap();
                        if n == 0 {
                            break;
                        }
                        c.filled(n).unwrap();
                        if !c.acks.is_empty() {
                            std::mem::swap(&mut acks, &mut c.acks);
                            let (r, b) = s.write_all(acks).await;
                            r.unwrap();
                            down += b.len() as u64;
                            acks = b;
                            acks.clear();
                        }
                    }
                    (c.expect, c.fold, down)
                })
            })
            .collect();
        let mut v = Vec::new();
        for h in hs {
            v.push(h.await);
        }
        v
    });
    let kcpu = uring_kernel_cpu();
    drop(rt);
    let mut out = SrvOut {
        usage: m.stop(),
        ..Default::default()
    };
    out.usage.cpu += kcpu;
    for (e, f, d) in res {
        out.records += e;
        out.fold = out.fold.wrapping_add(f);
        out.down += d;
    }
    out
}

fn serve_pipe(up: Arc<Pipe>, down_p: Arc<Pipe>) -> SrvOut {
    pin(0);
    let m = Meter::start();
    let (mut c, mut down) = (Conn::default(), 0);
    loop {
        let n = up.read(c.spare());
        if n == 0 {
            break;
        }
        c.filled(n).unwrap();
        if !c.acks.is_empty() {
            down_p.write_all(&c.acks);
            down += c.acks.len() as u64;
            c.acks.clear();
        }
    }
    let mut out = SrvOut {
        usage: m.stop(),
        ..Default::default()
    };
    add(&mut out, &c, down);
    out
}

/// Group commit: readers validate and publish; one commit thread "fsyncs" and releases acks.
fn serve_commit(socks: Vec<TcpStream>, fsync_us: u64, coalesce: bool) -> SrvOut {
    struct Shared {
        expect: Vec<AtomicU64>,
        /// Per connection: acked-through for its ack writer (non-coalesced mode).
        release: Vec<AtomicU64>,
        readers_done: AtomicU64,
        commit: Mutex<Option<std::thread::Thread>>,
    }
    let k = socks.len();
    let sh = Arc::new(Shared {
        expect: (0..k).map(|_| AtomicU64::new(0)).collect(),
        release: (0..k).map(|_| AtomicU64::new(0)).collect(),
        readers_done: AtomicU64::new(0),
        commit: Mutex::new(None),
    });
    let total = Arc::new(Mutex::new(SrvOut::default()));
    let commit_started = Arc::new(std::sync::Barrier::new(2));
    // ack writers (one per connection) when not coalescing
    let writers: Vec<_> = if coalesce {
        Vec::new()
    } else {
        socks
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let (mut s, sh, total) = (s.try_clone().unwrap(), sh.clone(), total.clone());
                std::thread::spawn(move || {
                    pin(0);
                    let m = Meter::start();
                    let (mut sent, mut out, mut down) = (0u64, Vec::new(), 0u64);
                    loop {
                        let r = sh.release[i].load(Ordering::Acquire);
                        if r == u64::MAX {
                            break;
                        }
                        if r > sent {
                            out.clear();
                            ack_frame(&mut out, r);
                            s.write_all(&out).unwrap();
                            down += out.len() as u64;
                            sent = r;
                        } else {
                            std::thread::park();
                        }
                    }
                    let mut t = total.lock().unwrap();
                    t.usage += m.stop();
                    t.down += down;
                    std::thread::current()
                })
            })
            .collect()
    };
    let writer_threads: Arc<Mutex<Vec<std::thread::Thread>>> = Arc::new(Mutex::new(Vec::new()));
    let commit = {
        let (sh, total, socks2, started, wt) = (
            sh.clone(),
            total.clone(),
            socks
                .iter()
                .map(|s| s.try_clone().unwrap())
                .collect::<Vec<_>>(),
            commit_started.clone(),
            writer_threads.clone(),
        );
        std::thread::spawn(move || {
            pin(0);
            *sh.commit.lock().unwrap() = Some(std::thread::current());
            started.wait();
            let m = Meter::start();
            let mut committed = vec![0u64; k];
            let (mut out, mut down) = (Vec::new(), 0u64);
            loop {
                let snap: Vec<u64> = sh
                    .expect
                    .iter()
                    .map(|e| e.load(Ordering::Acquire))
                    .collect();
                if snap.iter().zip(&committed).all(|(s, c)| s == c) {
                    if sh.readers_done.load(Ordering::Acquire) == k as u64
                        && snap
                            .iter()
                            .zip(sh.expect.iter())
                            .all(|(s, e)| *s == e.load(Ordering::Acquire))
                    {
                        break;
                    }
                    std::thread::park();
                    continue;
                }
                std::thread::sleep(Duration::from_micros(fsync_us)); // the fdatasync
                for i in 0..k {
                    if snap[i] > committed[i] {
                        committed[i] = snap[i];
                        if coalesce {
                            out.clear();
                            ack_frame(&mut out, snap[i]);
                            (&socks2[i]).write_all(&out).unwrap();
                            down += out.len() as u64;
                        } else {
                            sh.release[i].store(snap[i], Ordering::Release);
                            wt.lock().unwrap()[i].unpark();
                        }
                    }
                }
            }
            let mut t = total.lock().unwrap();
            t.usage += m.stop();
            t.down += down;
        })
    };
    if !coalesce {
        *writer_threads.lock().unwrap() = writers.iter().map(|h| h.thread().clone()).collect();
    }
    commit_started.wait();
    let readers: Vec<_> = socks
        .into_iter()
        .enumerate()
        .map(|(i, mut s)| {
            let (sh, total) = (sh.clone(), total.clone());
            std::thread::spawn(move || {
                pin(0);
                let m = Meter::start();
                let mut c = Conn::default();
                c.ack = false;
                loop {
                    let n = s.read(c.spare()).unwrap();
                    if n == 0 {
                        break;
                    }
                    c.filled(n).unwrap();
                    sh.expect[i].store(c.expect, Ordering::Release);
                    if let Some(t) = sh.commit.lock().unwrap().as_ref() {
                        t.unpark();
                    }
                }
                sh.readers_done.fetch_add(1, Ordering::AcqRel);
                if let Some(t) = sh.commit.lock().unwrap().as_ref() {
                    t.unpark();
                }
                let mut t = total.lock().unwrap();
                t.usage += m.stop();
                t.records += c.expect;
                t.fold = t.fold.wrapping_add(c.fold);
            })
        })
        .collect();
    for r in readers {
        r.join().unwrap();
    }
    commit.join().unwrap();
    for (i, w) in writers.into_iter().enumerate() {
        sh.release[i].store(u64::MAX, Ordering::Release);
        w.thread().unpark();
        w.join().unwrap();
    }
    std::mem::take(&mut *total.lock().unwrap())
}

// ---------------------------------------------------------------------------------------------
// QUIC
// ---------------------------------------------------------------------------------------------

fn quic_configs() -> (quinn::ServerConfig, quinn::ClientConfig) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let der = cert.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let mut t = quinn::TransportConfig::default();
    t.stream_receive_window((32u32 << 20).into())
        .receive_window((64u32 << 20).into())
        .send_window(64 << 20);
    let t = Arc::new(t);
    let mut sc = quinn::ServerConfig::with_single_cert(vec![der.clone()], key.into()).unwrap();
    sc.transport_config(t.clone());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(der).unwrap();
    let mut cc = quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    cc.transport_config(t);
    (sc, cc)
}

// ---------------------------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Run {
    rps: f64,
    srv_us: f64,
    cli_us: f64,
    srv_sys: f64,
    srv_cs: f64,
    cli_sys: f64,
    cli_cs: f64,
    up_b: f64,
    down_b: f64,
    per_frame: f64,
    p50: f64,
    p99: f64,
    p999: f64,
    napi: i32,
    zc_copied: Option<bool>,
}

fn summarize(n: u64, start: Instant, ss: SrvOut, cs: Vec<CliOut>, conns: usize) -> Run {
    assert_eq!(ss.records, n * conns as u64, "server validated everything");
    assert!(ss.fold != 0);
    let total = (n * conns as u64) as f64;
    let wall = cs
        .iter()
        .map(|c| c.done - start)
        .max()
        .unwrap()
        .as_secs_f64();
    let mut lat: Vec<u64> = cs.iter().flat_map(|c| c.lat.iter().copied()).collect();
    lat.sort_unstable();
    let q = |f: f64| {
        lat.get(((lat.len() as f64 * f) as usize).min(lat.len().saturating_sub(1)))
            .map_or(0.0, |&v| v as f64 / 1e3)
    };
    let mut cu = Usage::default();
    for c in &cs {
        cu += c.usage;
    }
    Run {
        rps: total / wall,
        srv_us: ss.usage.cpu * 1e6 / total,
        cli_us: cu.cpu * 1e6 / total,
        srv_sys: ss.usage.sys as f64 * 1e3 / total,
        srv_cs: ss.usage.cs as f64 * 1e3 / total,
        cli_sys: cu.sys as f64 * 1e3 / total,
        cli_cs: cu.cs as f64 * 1e3 / total,
        up_b: cs.iter().map(|c| c.up).sum::<u64>() as f64 / total,
        down_b: ss.down as f64 / total,
        per_frame: total / cs.iter().map(|c| c.frames).sum::<u64>() as f64,
        p50: q(0.50),
        p99: q(0.99),
        p999: q(0.999),
        napi: ss.napi,
        zc_copied: cs[0].zc_copied,
    }
}

/// `n` records per connection; at saturation (`rate` None) or offered at `rate`/s in total.
fn run(p: &Poc, recs: &Arc<Vec<Record>>, n: u64, rate: Option<f64>) -> Run {
    sh(p.setup);
    let c = p.cli;
    let rate_c = rate.map(|r| r / c.conns as f64);
    let r = match p.srv {
        Srv::Shm => {
            let (up, down) = (Pipe::new(16 << 20), Pipe::new(1 << 20));
            let srv = {
                let (up, down) = (up.clone(), down.clone());
                std::thread::spawn(move || serve_pipe(up, down))
            };
            let start = Instant::now();
            let tx = Box::new(PipeTx(up.clone(), Vec::new()));
            let rx = Box::new(PipeRx(down));
            let co = client(tx, rx, c, recs.clone(), n, rate_c, start, true);
            up.close();
            let ss = srv.join().unwrap();
            summarize(n, start, ss, vec![co], 1)
        }
        Srv::Quic => run_quic(c, recs, n, rate_c),
        _ => {
            let l = listen(p.net);
            let addr = l.local_addr().unwrap();
            let (srv, sock) = (p.srv, p.sock);
            let server = std::thread::spawn(move || {
                let socks: Vec<TcpStream> = (0..c.conns).map(|_| l.accept().unwrap().0).collect();
                let napi = socks.iter().map(|s| server_sock(s, sock)).max().unwrap();
                let mut o = match srv {
                    Srv::Std => {
                        let hs: Vec<_> = socks
                            .into_iter()
                            .map(|s| std::thread::spawn(move || serve_std(s, sock)))
                            .collect();
                        let mut o = SrvOut::default();
                        for h in hs {
                            let x = h.join().unwrap();
                            o.usage += x.usage;
                            o.records += x.records;
                            o.fold = o.fold.wrapping_add(x.fold);
                            o.down += x.down;
                        }
                        o
                    }
                    Srv::Epoll(spin) => serve_epoll(socks, spin),
                    Srv::Uring(m, sq) => serve_uring(socks, m, sq),
                    Srv::TokioRpc(mt) => serve_tokio(socks, mt, false),
                    Srv::TokioInline => serve_tokio(socks, false, true),
                    Srv::Monoio => serve_monoio(socks),
                    Srv::Commit(us, co) => serve_commit(socks, us, co),
                    Srv::Shm | Srv::Quic => unreachable!(),
                };
                o.napi = napi;
                o
            });
            let socks: Vec<TcpStream> = (0..c.conns).map(|_| client_sock(addr, &c)).collect();
            let start = Instant::now();
            let cs: Vec<_> = socks
                .into_iter()
                .map(|s| {
                    let recs = recs.clone();
                    std::thread::spawn(move || {
                        let rx: Box<dyn Rx> = Box::new(s.try_clone().unwrap());
                        let tx: Box<dyn Tx> = Box::new(TcpTx::new(s, c.send));
                        client(tx, rx, c, recs, n, rate_c, start, true)
                    })
                })
                .collect();
            let cs: Vec<CliOut> = cs.into_iter().map(|h| h.join().unwrap()).collect();
            let ss = server.join().unwrap();
            summarize(n, start, ss, cs, c.conns)
        }
    };
    sh(p.teardown);
    r
}

fn run_quic(c: Cli, recs: &Arc<Vec<Record>>, n: u64, rate: Option<f64>) -> Run {
    let (sc, cc) = quic_configs();
    let ssink = Arc::new(Mutex::new(Usage::default()));
    let srt = tokio_rt(true, 0, ssink.clone());
    let ep = srt
        .block_on(async { quinn::Endpoint::server(sc, "127.0.0.1:0".parse().unwrap()).unwrap() });
    let addr = ep.local_addr().unwrap();
    let server = srt.spawn(async move {
        let conn = ep.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = conn.accept_bi().await.unwrap();
        let (mut c, mut down) = (Conn::default(), 0u64);
        while let Some(k) = recv.read(c.spare()).await.unwrap() {
            c.filled(k).unwrap();
            if !c.acks.is_empty() {
                send.write_all(&c.acks).await.unwrap();
                down += c.acks.len() as u64;
                c.acks.clear();
            }
        }
        send.finish().unwrap();
        let _ = send.stopped().await;
        conn.close(0u32.into(), b"");
        ep.wait_idle().await;
        (c.expect, c.fold, down)
    });
    let csink = Arc::new(Mutex::new(Usage::default()));
    let crt = tokio_rt(true, 2, csink.clone());
    let (cep, conn, send, recv) = crt.block_on(async {
        let mut ep = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        ep.set_default_client_config(cc);
        let conn = ep.connect(addr, "localhost").unwrap().await.unwrap();
        let (send, recv) = conn.open_bi().await.unwrap();
        (ep, conn, send, recv)
    });
    let start = Instant::now();
    let h = crt.handle().clone();
    let co = {
        let recs = recs.clone();
        let h2 = h.clone();
        std::thread::spawn(move || {
            let tx = Box::new(QuicTx(h2.clone(), send, Vec::new()));
            let rx = Box::new(QuicRx(h2, recv));
            client(tx, rx, c, recs, n, rate, start, true)
        })
        .join()
        .unwrap()
    };
    let (e, f, d) = srt.block_on(server).unwrap();
    h.block_on(async {
        conn.close(0u32.into(), b"");
        cep.wait_idle().await;
    });
    drop(crt);
    drop(srt);
    let mut co = co;
    co.usage += *csink.lock().unwrap();
    let ss = SrvOut {
        usage: *ssink.lock().unwrap(),
        records: e,
        down: d,
        fold: f,
        napi: 0,
    };
    summarize(n, start, ss, vec![co], 1)
}

// ---------------------------------------------------------------------------------------------
// Replication topologies (POC 7): client -> leader -> two followers, ack at 2 copies
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Topo {
    /// The leader sends every frame to both followers (today).
    Star,
    /// The leader sends to f1; f1 stores, then forwards each whole frame to f2.
    Chain,
    /// As Chain, but f1 forwards every chunk as it arrives, before parsing it.
    PipeChain,
}

#[derive(Default, Clone)]
struct ReplRun {
    rps: f64,
    leader_us: f64,
    all_us: f64,
    bytes: f64,
    p50: f64,
    p99: f64,
    p999: f64,
}

struct Counted(TcpStream, u64);

impl Counted {
    fn write(&mut self, b: &[u8]) {
        self.0.write_all(b).unwrap();
        self.1 += b.len() as u64;
    }
}

/// A follower: validates, acks upstream; in a chain, forwards to `next`.
fn follower(
    mut up: TcpStream,
    next: Option<TcpStream>,
    cut_through: bool,
    cpu: usize,
) -> (Usage, u64) {
    pin(cpu);
    let m = Meter::start();
    let mut bytes = 0u64;
    let mut c = Conn::default();
    let mut next = next.map(|s| Counted(s, 0));
    // drain the downstream's acks (the third copy; nobody waits for it at ack-at-2)
    let drain = next.as_ref().map(|n| {
        let mut r = n.0.try_clone().unwrap();
        std::thread::spawn(move || {
            pin(cpu);
            let m = Meter::start();
            let mut b = vec![0u8; 64 * 1024];
            while r.read(&mut b).unwrap() > 0 {}
            m.stop()
        })
    });
    let mut upw = up.try_clone().unwrap();
    loop {
        let sp = c.spare();
        let n = up.read(sp).unwrap();
        if n == 0 {
            break;
        }
        if cut_through && let Some(nx) = next.as_mut() {
            let sp = c.spare();
            nx.write(&sp[..n]);
        }
        match (cut_through, next.as_mut()) {
            (false, Some(nx)) => c.filled_with(n, |f| nx.write(f)).unwrap(),
            _ => c.filled(n).unwrap(),
        }
        if !c.acks.is_empty() {
            upw.write_all(&c.acks).unwrap();
            bytes += c.acks.len() as u64;
            c.acks.clear();
        }
    }
    let mut u = m.stop();
    if let Some(nx) = next {
        bytes += nx.1;
        nx.0.shutdown(std::net::Shutdown::Write).unwrap();
        u += drain.unwrap().join().unwrap();
    }
    (u, bytes)
}

fn repl(topo: Topo, recs: &Arc<Vec<Record>>, n: u64, rate: Option<f64>) -> ReplRun {
    let pair = || {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let a = TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let b = l.accept().unwrap().0;
        a.set_nodelay(true).unwrap();
        b.set_nodelay(true).unwrap();
        (a, b)
    };
    let (cl, ls) = pair(); // client -> leader
    let (l1, f1u) = pair(); // leader -> f1
    let (x2, f2u) = pair(); // leader (star) or f1 (chain) -> f2
    let f2 = std::thread::spawn(move || follower(f2u, None, false, 2));
    let (f1, mut links) = match topo {
        Topo::Star => (
            std::thread::spawn(move || follower(f1u, None, false, 1)),
            vec![Counted(l1, 0), Counted(x2, 0)],
        ),
        _ => {
            let ct = topo == Topo::PipeChain;
            (
                std::thread::spawn(move || follower(f1u, Some(x2), ct, 1)),
                vec![Counted(l1, 0)],
            )
        }
    };
    struct Q {
        leader: AtomicU64,
        peers: Vec<AtomicU64>,
        sent: Mutex<(u64, u64)>, // (acked to client, ack bytes)
        client: Mutex<TcpStream>,
    }
    impl Q {
        /// Ack at 2 copies: the leader's own and the first follower's.
        fn try_ack(&self) {
            let best = self
                .peers
                .iter()
                .map(|p| p.load(Ordering::Acquire))
                .max()
                .unwrap();
            let q = best.min(self.leader.load(Ordering::Acquire));
            let mut s = self.sent.lock().unwrap();
            if q > s.0 {
                let mut out = Vec::new();
                ack_frame(&mut out, q);
                self.client.lock().unwrap().write_all(&out).unwrap();
                *s = (q, s.1 + out.len() as u64);
            }
        }
    }
    let q = Arc::new(Q {
        leader: AtomicU64::new(0),
        peers: links.iter().map(|_| AtomicU64::new(0)).collect(),
        sent: Mutex::new((0, 0)),
        client: Mutex::new(ls.try_clone().unwrap()),
    });
    let ackers: Vec<_> = links
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let (mut r, q) = (l.0.try_clone().unwrap(), q.clone());
            std::thread::spawn(move || {
                pin(0);
                let m = Meter::start();
                let (mut buf, mut fill) = (vec![0u8; 64 * 1024], 0);
                loop {
                    let k = r.read(&mut buf[fill..]).unwrap();
                    if k == 0 {
                        break;
                    }
                    fill += k;
                    let (top, used) = parse_acks(&buf[..fill]);
                    buf.copy_within(used..fill, 0);
                    fill -= used;
                    if let Some(t) = top {
                        q.peers[i].store(t, Ordering::Release);
                        q.try_ack();
                    }
                }
                m.stop()
            })
        })
        .collect();
    let leader = {
        let q = q.clone();
        let mut ls = ls;
        std::thread::spawn(move || {
            pin(0);
            let m = Meter::start();
            let mut c = Conn::default();
            c.ack = false;
            loop {
                let k = ls.read(c.spare()).unwrap();
                if k == 0 {
                    break;
                }
                c.filled_with(k, |f| links.iter_mut().for_each(|l| l.write(f)))
                    .unwrap();
                q.leader.store(c.expect, Ordering::Release);
                q.try_ack();
            }
            let bytes: u64 = links.iter().map(|l| l.1).sum();
            for l in &links {
                l.0.shutdown(std::net::Shutdown::Write).unwrap();
            }
            (m.stop(), bytes, c.expect)
        })
    };
    let start = Instant::now();
    let cli = Cli {
        wcpu: 3,
        rcpu: 3,
        ..CLI
    };
    let co = {
        let recs = recs.clone();
        let rx: Box<dyn Rx> = Box::new(cl.try_clone().unwrap());
        let tx: Box<dyn Tx> = Box::new(TcpTx::new(cl, Send::Write));
        client(tx, rx, cli, recs, n, rate, start, true)
    };
    let (mut lu, lbytes, validated) = leader.join().unwrap();
    assert_eq!(validated, n);
    for a in ackers {
        lu += a.join().unwrap();
    }
    let (u1, b1) = f1.join().unwrap();
    let (u2, b2) = f2.join().unwrap();
    let ack_bytes = q.sent.lock().unwrap().1;
    let nf = n as f64;
    let mut lat = co.lat;
    lat.sort_unstable();
    let qn = |f: f64| {
        lat.get(((lat.len() as f64 * f) as usize).min(lat.len().saturating_sub(1)))
            .map_or(0.0, |&v| v as f64 / 1e3)
    };
    ReplRun {
        rps: nf / (co.done - start).as_secs_f64(),
        leader_us: lu.cpu * 1e6 / nf,
        all_us: (lu.cpu + u1.cpu + u2.cpu) * 1e6 / nf,
        bytes: (co.up + lbytes + ack_bytes + b1 + b2) as f64 / nf,
        p50: qn(0.5),
        p99: qn(0.99),
        p999: qn(0.999),
    }
}

fn repl_main(recs: &Arc<Vec<Record>>, reps: usize, n: u64, secs: f64) {
    let topos = [
        ("star: leader sends to both followers (today)", Topo::Star),
        ("chain: leader -> f1 -> f2, store and forward", Topo::Chain),
        (
            "pipelined chain: f1 forwards each chunk before parsing",
            Topo::PipeChain,
        ),
    ];
    repl(Topo::Star, recs, n / 4, None); // warm-up
    let mut res = vec![vec![Vec::new(); 3]; topos.len()];
    for rep in 0..reps {
        for (i, (name, t)) in topos.iter().enumerate() {
            for (j, rate) in [None, Some(100_000.0), Some(1_000_000.0)]
                .into_iter()
                .enumerate()
            {
                let k = rate.map_or(n, |r| (r * secs) as u64);
                let r = repl(*t, recs, k, rate);
                eprintln!(
                    "rep {rep} {name} {rate:?}: {:.0}/s leader {:.3} all {:.3} µs/rec, {:.1} B/rec, p50 {:.0} p99 {:.0}",
                    r.rps, r.leader_us, r.all_us, r.bytes, r.p50, r.p99
                );
                res[i][j].push(r);
            }
        }
    }
    let m = |v: &[ReplRun], f: fn(&ReplRun) -> f64| median(v.iter().map(f).collect());
    let col = |v: &[ReplRun], f: fn(&ReplRun) -> f64| v.iter().map(f).collect::<Vec<f64>>();
    println!("\n### Replication topology, 3 nodes, ack at 2 (medians of {reps})\n");
    println!(
        "| topology | records/s | leader µs/rec | all nodes µs/rec | B/rec, all links | leader µs/rec @100k | all µs/rec @100k | p50 / p99 / p99.9 µs @100k | leader µs/rec @1M | all µs/rec @1M | p50 / p99 / p99.9 µs @1M |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (i, (name, _)) in topos.iter().enumerate() {
        let (s, a, b) = (&res[i][0], &res[i][1], &res[i][2]);
        println!(
            "| {name} | {:.0} | {:.3} | {:.3} | {:.1} | {:.3} | {:.3} | {:.0} / {:.0} / {:.0} | {:.3} | {:.3} | {:.0} / {:.0} / {:.0} |",
            m(s, |r| r.rps),
            m(s, |r| r.leader_us),
            m(s, |r| r.all_us),
            m(s, |r| r.bytes),
            m(a, |r| r.leader_us),
            m(a, |r| r.all_us),
            m(a, |r| r.p50),
            m(a, |r| r.p99),
            m(a, |r| r.p999),
            m(b, |r| r.leader_us),
            m(b, |r| r.all_us),
            m(b, |r| r.p50),
            m(b, |r| r.p99),
            m(b, |r| r.p999),
        );
    }
    println!(
        "\n| topology | records/s | leader µs/rec sat | leader µs/rec @100k | leader µs/rec @1M | p99 µs @100k | p99 µs @1M |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for (i, (name, _)) in topos.iter().enumerate() {
        let (s, a, b) = (&res[i][0], &res[i][1], &res[i][2]);
        println!(
            "| {name} | {} | {} | {} | {} | {} | {} |",
            range(&col(s, |r| r.rps), 0),
            range(&col(s, |r| r.leader_us), 3),
            range(&col(a, |r| r.leader_us), 3),
            range(&col(b, |r| r.leader_us), 3),
            range(&col(a, |r| r.p99), 0),
            range(&col(b, |r| r.p99), 0),
        );
    }
}

// ---------------------------------------------------------------------------------------------

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn range(v: &[f64], d: usize) -> String {
    let lo = v.iter().copied().fold(f64::MAX, f64::min);
    let hi = v.iter().copied().fold(f64::MIN, f64::max);
    format!("{lo:.d$}-{hi:.d$}")
}

fn pocs() -> Vec<Poc> {
    let p = |group, name, srv, cli| Poc {
        group,
        name,
        srv,
        cli,
        sock: Sock::default(),
        net: Net::Lo,
        setup: "",
        teardown: "",
    };
    let small = Cli {
        fixed: Some(250),
        inflight: 8,
        linger_us: 0,
        ..CLI
    };
    let zc = |k: usize| Cli {
        max_frame: k << 10,
        linger_us: 0,
        ..CLI
    };
    let veth = |mut x: Poc| {
        x.net = Net::Veth;
        x
    };
    let c8 = Cli { conns: 8, ..CLI };
    let mut v = vec![
        // 1, 4: event loops and runtimes, one connection, same work
        p("io", "std blocking thread (rounds 1-2)", Srv::Std, CLI),
        p("io", "epoll, one thread", Srv::Epoll(Spin::Park), CLI),
        p(
            "io",
            "io_uring recv, batched submit",
            Srv::Uring(UMode::Recv, false),
            CLI,
        ),
        p(
            "io",
            "io_uring multishot + buf ring + fixed files",
            Srv::Uring(UMode::Multishot, false),
            CLI,
        ),
        p(
            "io",
            "io_uring read_fixed (registered buffers)",
            Srv::Uring(UMode::Fixed, false),
            CLI,
        ),
        p(
            "io",
            "io_uring multishot, SQPOLL",
            Srv::Uring(UMode::Multishot, true),
            CLI,
        ),
        p(
            "io",
            "tokio multi-thread, rpc.rs shape (today)",
            Srv::TokioRpc(true),
            CLI,
        ),
        p(
            "io",
            "tokio current-thread, rpc.rs shape",
            Srv::TokioRpc(false),
            CLI,
        ),
        p("io", "tokio current-thread, inline", Srv::TokioInline, CLI),
        p("io", "monoio (thread-per-core, io_uring)", Srv::Monoio, CLI),
        p("io", "shared-memory ring (upper bound)", Srv::Shm, CLI),
        p("io", "QUIC (quinn), one stream", Srv::Quic, CLI),
        // 4 at 8 connections: one wake-up serves many sockets
        p("conns8", "8 conns: std thread per conn", Srv::Std, c8),
        p(
            "conns8",
            "8 conns: epoll, one thread",
            Srv::Epoll(Spin::Park),
            c8,
        ),
        p(
            "conns8",
            "8 conns: io_uring multishot, one submit",
            Srv::Uring(UMode::Multishot, false),
            c8,
        ),
        p(
            "conns8",
            "8 conns: tokio multi-thread, rpc.rs shape",
            Srv::TokioRpc(true),
            c8,
        ),
        p("conns8", "8 conns: monoio", Srv::Monoio, c8),
        // 5: latency knobs
        p("knobs", "epoll, busy spin", Srv::Epoll(Spin::Busy), CLI),
        p(
            "knobs",
            "epoll, adaptive spin 50 µs then park",
            Srv::Epoll(Spin::Adaptive(50)),
            CLI,
        ),
        Poc {
            sock: Sock {
                busy_poll_us: Some(50),
                ..Default::default()
            },
            ..p("knobs", "SO_BUSY_POLL 50 µs (std)", Srv::Std, CLI)
        },
        Poc {
            sock: Sock {
                quickack: true,
                ..Default::default()
            },
            ..p("knobs", "TCP_QUICKACK (std)", Srv::Std, CLI)
        },
        p(
            "knobs",
            "Nagle on (no TCP_NODELAY)",
            Srv::Std,
            Cli {
                nodelay: false,
                ..CLI
            },
        ),
        p(
            "knobs",
            "TCP_CORK around each send",
            Srv::Std,
            Cli {
                send: Send::Cork,
                ..CLI
            },
        ),
        Poc {
            sock: Sock {
                rcvbuf: Some(64 << 10),
                ..Default::default()
            },
            ..p(
                "knobs",
                "64 KiB socket buffers",
                Srv::Std,
                Cli {
                    sndbuf: Some(64 << 10),
                    ..CLI
                },
            )
        },
        Poc {
            sock: Sock {
                rcvbuf: Some(8 << 20),
                ..Default::default()
            },
            ..p(
                "knobs",
                "8 MiB socket buffers",
                Srv::Std,
                Cli {
                    sndbuf: Some(8 << 20),
                    ..CLI
                },
            )
        },
        // 3: batched syscalls, 250-record frames as today's loadgen sends
        p("batch", "250-rec frames, a write each", Srv::Std, small),
        p(
            "batch",
            "250-rec frames, writev of 8",
            Srv::Std,
            Cli {
                per_call: 8,
                send: Send::Writev,
                ..small
            },
        ),
        p(
            "batch",
            "250-rec frames, sendmmsg of 8",
            Srv::Std,
            Cli {
                per_call: 8,
                send: Send::Sendmmsg,
                ..small
            },
        ),
        // 2, 1: zero-copy sends at fixed frame sizes
        p("zc", "64 KiB frames, write", Srv::Std, zc(64)),
        p(
            "zc",
            "64 KiB frames, MSG_ZEROCOPY",
            Srv::Std,
            Cli {
                send: Send::MsgZc,
                ..zc(64)
            },
        ),
        p(
            "zc",
            "64 KiB frames, io_uring SEND_ZC",
            Srv::Std,
            Cli {
                send: Send::UringZc,
                ..zc(64)
            },
        ),
        p("zc", "256 KiB frames, write", Srv::Std, zc(256)),
        p(
            "zc",
            "256 KiB frames, MSG_ZEROCOPY",
            Srv::Std,
            Cli {
                send: Send::MsgZc,
                ..zc(256)
            },
        ),
        p(
            "zc",
            "256 KiB frames, io_uring SEND_ZC",
            Srv::Std,
            Cli {
                send: Send::UringZc,
                ..zc(256)
            },
        ),
        p("zc", "1 MiB frames, write", Srv::Std, zc(1024)),
        p(
            "zc",
            "1 MiB frames, MSG_ZEROCOPY",
            Srv::Std,
            Cli {
                send: Send::MsgZc,
                ..zc(1024)
            },
        ),
        p(
            "zc",
            "1 MiB frames, io_uring SEND_ZC",
            Srv::Std,
            Cli {
                send: Send::UringZc,
                ..zc(1024)
            },
        ),
        // 6: offloads
        Poc {
            setup: "ethtool -K lo tso off gso off gro off",
            teardown: "ethtool -K lo tso on gso on gro on",
            ..p("offload", "loopback, TSO/GSO/GRO off (std)", Srv::Std, CLI)
        },
        // 8: frames clocked by group commit; acks coalesced across connections
        p(
            "clock",
            "8 conns, linger 1 ms, ack writer per conn",
            Srv::Commit(250, false),
            c8,
        ),
        p(
            "clock",
            "8 conns, linger 1 ms, acks coalesced",
            Srv::Commit(250, true),
            c8,
        ),
        p(
            "clock",
            "8 conns, clocked by commit, ack writer per conn",
            Srv::Commit(250, false),
            Cli {
                inflight: 1,
                linger_us: 0,
                ..c8
            },
        ),
        p(
            "clock",
            "8 conns, clocked by commit, acks coalesced",
            Srv::Commit(250, true),
            Cli {
                inflight: 1,
                linger_us: 0,
                ..c8
            },
        ),
    ];
    // per-frame cost: no linger, so at a fixed rate frames hold a few records each
    let nl = Cli {
        linger_us: 0,
        ..CLI
    };
    v.extend([
        p("nolinger", "no linger: std blocking thread", Srv::Std, nl),
        p(
            "nolinger",
            "no linger: epoll, one thread",
            Srv::Epoll(Spin::Park),
            nl,
        ),
        p(
            "nolinger",
            "no linger: epoll, adaptive spin 50 µs",
            Srv::Epoll(Spin::Adaptive(50)),
            nl,
        ),
        p(
            "nolinger",
            "no linger: io_uring multishot",
            Srv::Uring(UMode::Multishot, false),
            nl,
        ),
        p(
            "nolinger",
            "no linger: io_uring read_fixed",
            Srv::Uring(UMode::Fixed, false),
            nl,
        ),
        p(
            "nolinger",
            "no linger: tokio multi-thread, rpc.rs shape",
            Srv::TokioRpc(true),
            nl,
        ),
        p(
            "nolinger",
            "no linger: tokio current-thread, rpc.rs shape",
            Srv::TokioRpc(false),
            nl,
        ),
        p(
            "nolinger",
            "no linger: tokio current-thread, inline",
            Srv::TokioInline,
            nl,
        ),
        p("nolinger", "no linger: monoio", Srv::Monoio, nl),
        p("nolinger", "no linger: shared-memory ring", Srv::Shm, nl),
    ]);
    // the same kernel path over a veth pair between namespaces
    v.extend([
        veth(p("veth", "veth: std blocking thread", Srv::Std, CLI)),
        veth(p(
            "veth",
            "veth: epoll, one thread",
            Srv::Epoll(Spin::Park),
            CLI,
        )),
        veth(p(
            "veth",
            "veth: io_uring multishot",
            Srv::Uring(UMode::Multishot, false),
            CLI,
        )),
        veth(p(
            "veth",
            "veth: tokio multi-thread, rpc.rs shape",
            Srv::TokioRpc(true),
            CLI,
        )),
        veth(p(
            "veth",
            "veth: 1 MiB frames, MSG_ZEROCOPY",
            Srv::Std,
            Cli {
                send: Send::MsgZc,
                ..zc(1024)
            },
        )),
        veth(Poc {
            sock: Sock {
                busy_poll_us: Some(50),
                ..Default::default()
            },
            setup: "ethtool -K plab0 gro on; ip netns exec plab ethtool -K plab1 gro on",
            teardown: "ethtool -K plab0 gro off; ip netns exec plab ethtool -K plab1 gro off",
            ..p(
                "veth",
                "veth: NAPI (GRO on) + SO_BUSY_POLL 50 µs",
                Srv::Std,
                CLI,
            )
        }),
        veth(Poc {
            setup: "ethtool -K plab0 tso off gso off; ip netns exec plab ethtool -K plab1 gro off",
            teardown: "ethtool -K plab0 tso on gso on",
            ..p("veth", "veth: TSO/GSO off", Srv::Std, CLI)
        }),
    ]);
    v
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |k: &str, d: f64| {
        args.iter()
            .position(|a| a == k)
            .map_or(d, |i| args[i + 1].parse().unwrap())
    };
    let sarg = |k: &str| {
        args.iter()
            .position(|a| a == k)
            .map(|i| args[i + 1].clone())
    };
    let (reps, n, secs) = (
        arg("--reps", 3.0) as usize,
        arg("--n", 3_000_000.0) as u64,
        arg("--secs", 2.0),
    );
    let (only, group) = (sarg("--only"), sarg("--group"));
    let recs = Arc::new(records(1 << 18, 42));
    if group.as_deref() == Some("repl") {
        return repl_main(&recs, reps, n, secs);
    }
    let pocs: Vec<Poc> = pocs()
        .into_iter()
        .filter(|p| only.as_ref().is_none_or(|o| p.name.starts_with(o.as_str())))
        .filter(|p| {
            group
                .as_ref()
                .is_none_or(|g| g.split(',').any(|g| g == p.group))
        })
        .collect();
    if pocs.iter().any(|p| p.net == Net::Veth) {
        veth_up();
    }
    eprintln!(
        "{} POCs, {reps} reps, sys_enter counter: {}",
        pocs.len(),
        Meter::start().stop().sys > 0
    );
    let rates = [100_000.0, 1_000_000.0];
    run(&pocs[0], &recs, n / 4, None); // warm-up, discarded
    let k = pocs.len();
    let (mut sat, mut at) = (vec![Vec::new(); k], vec![vec![Vec::new(); 2]; k]);
    for rep in 0..reps {
        for (i, p) in pocs.iter().enumerate() {
            let per = (n / p.cli.conns as u64).max(1);
            let r = run(p, &recs, per, None);
            eprintln!(
                "rep {rep} {}: sat {:.0}/s srv {:.3} cli {:.3} µs/rec",
                p.name, r.rps, r.srv_us, r.cli_us
            );
            sat[i].push(r);
            for (j, &rate) in rates.iter().enumerate() {
                let per = (rate * secs) as u64 / p.cli.conns as u64;
                let r = run(p, &recs, per, Some(rate));
                eprintln!(
                    "rep {rep} {} @{rate}: srv {:.3} µs/rec p50 {:.0} p99 {:.0} p99.9 {:.0} µs",
                    p.name, r.srv_us, r.p50, r.p99, r.p999
                );
                at[i][j].push(r);
            }
        }
    }
    let m = |v: &[Run], f: fn(&Run) -> f64| median(v.iter().map(f).collect());
    let col = |v: &[Run], f: fn(&Run) -> f64| v.iter().map(f).collect::<Vec<f64>>();
    println!("\n### Saturation (medians of {reps})\n");
    println!(
        "| POC | records/s | server µs/rec | client µs/rec | B/rec up | B/rec acks | rec/frame | server syscalls/krec | server cs/krec | client syscalls/krec | client cs/krec |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (i, p) in pocs.iter().enumerate() {
        let s = &sat[i];
        println!(
            "| {} | {:.0} | {:.3} | {:.3} | {:.1} | {:.2} | {:.0} | {:.2} | {:.2} | {:.2} | {:.2} |",
            p.name,
            m(s, |r| r.rps),
            m(s, |r| r.srv_us),
            m(s, |r| r.cli_us),
            m(s, |r| r.up_b),
            m(s, |r| r.down_b),
            m(s, |r| r.per_frame),
            m(s, |r| r.srv_sys),
            m(s, |r| r.srv_cs),
            m(s, |r| r.cli_sys),
            m(s, |r| r.cli_cs),
        );
    }
    for (j, label) in ["100k/s", "1M/s"].iter().enumerate() {
        println!("\n### Fixed load {label} (medians of {reps})\n");
        println!(
            "| POC | server µs/rec | client µs/rec | p50 µs | p99 µs | p99.9 µs | rec/frame | server syscalls/krec | server cs/krec | client syscalls/krec | client cs/krec |"
        );
        println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        for (i, p) in pocs.iter().enumerate() {
            let s = &at[i][j];
            println!(
                "| {} | {:.3} | {:.3} | {:.0} | {:.0} | {:.0} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} |",
                p.name,
                m(s, |r| r.srv_us),
                m(s, |r| r.cli_us),
                m(s, |r| r.p50),
                m(s, |r| r.p99),
                m(s, |r| r.p999),
                m(s, |r| r.per_frame),
                m(s, |r| r.srv_sys),
                m(s, |r| r.srv_cs),
                m(s, |r| r.cli_sys),
                m(s, |r| r.cli_cs),
            );
        }
    }
    println!("\n### Spread (min-max of {reps})\n");
    println!(
        "| POC | records/s | server µs/rec sat | server µs/rec @100k | server µs/rec @1M | p99 µs @100k | p99 µs @1M |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for (i, p) in pocs.iter().enumerate() {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            p.name,
            range(&col(&sat[i], |r| r.rps), 0),
            range(&col(&sat[i], |r| r.srv_us), 3),
            range(&col(&at[i][0], |r| r.srv_us), 3),
            range(&col(&at[i][1], |r| r.srv_us), 3),
            range(&col(&at[i][0], |r| r.p99), 0),
            range(&col(&at[i][1], |r| r.p99), 0),
        );
    }
    let notes: Vec<String> = pocs
        .iter()
        .enumerate()
        .filter_map(|(i, p)| {
            let r = &sat[i][0];
            (r.napi != 0 || r.zc_copied.is_some()).then(|| {
                format!(
                    "- {}: NAPI id {}, zero-copy completions reported copied: {:?}",
                    p.name, r.napi, r.zc_copied
                )
            })
        })
        .collect();
    for n in notes {
        println!("{n}");
    }
    let napi0: Vec<&str> = pocs
        .iter()
        .enumerate()
        .filter(|(i, p)| p.sock.busy_poll_us.is_some() && sat[*i][0].napi == 0)
        .map(|(_, p)| p.name)
        .collect();
    if !napi0.is_empty() {
        println!("- no NAPI id on the server socket (busy polling cannot apply): {napi0:?}");
    }
    if pocs.iter().any(|p| p.net == Net::Veth) {
        sh(&format!(
            "ip netns del {VETH_NS}; ip link del plab0 2>/dev/null || true"
        ));
    }
}
