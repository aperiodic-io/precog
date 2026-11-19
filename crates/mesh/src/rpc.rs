//! Multiplexed request/response over one ordered byte stream (a TCP socket).
//! Many requests can be in flight;
//! responses are matched by `req_id`. The writer task coalesces queued frames
//! into one write, so pipelined small requests cost few syscalls.

use crate::proto::{Frame, Kind, MAX_FRAME_BYTES};
use anyhow::{Result, anyhow, bail, ensure};
use bytes::{Buf, BytesMut};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

pub type BoxRead = Box<dyn AsyncRead + Unpin + Send>;
pub type BoxWrite = Box<dyn AsyncWrite + Unpin + Send>;

/// Reads one frame. `Ok(None)` on clean EOF at a frame boundary.
pub async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
) -> Result<Option<Frame>> {
    loop {
        if buf.len() >= 4 {
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            ensure!(
                (9..=MAX_FRAME_BYTES).contains(&len),
                "bad frame length {len}"
            );
            if buf.len() >= 4 + len {
                buf.advance(4);
                let mut f = buf.split_to(len);
                let kind = Kind::from_u8(f[0])?;
                let req_id = u64::from_le_bytes(f[1..9].try_into().unwrap());
                f.advance(9);
                return Ok(Some(Frame {
                    kind,
                    req_id,
                    body: f.freeze(),
                }));
            }
            buf.reserve(4 + len - buf.len());
        } else {
            buf.reserve(64 * 1024);
        }
        if r.read_buf(buf).await? == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            bail!("connection closed mid-frame");
        }
    }
}

/// Writer task: drains `rx`, coalescing all immediately available frames.
pub async fn write_loop<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<()> {
    let mut out = BytesMut::with_capacity(256 * 1024);
    while let Some(f) = rx.recv().await {
        f.encode_into(&mut out);
        while out.len() < 4 * 1024 * 1024 {
            match rx.try_recv() {
                Ok(f) => f.encode_into(&mut out),
                Err(_) => break,
            }
        }
        w.write_all(&out).await?;
        w.flush().await?;
        out.clear();
    }
    let _ = w.shutdown().await;
    Ok(())
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Frame>>>>;
type Subs = Arc<Mutex<HashMap<u64, mpsc::Sender<Frame>>>>;

/// Client side of a multiplexed connection.
pub struct RpcClient {
    tx: mpsc::Sender<Frame>,
    pending: Pending,
    subs: Subs,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
}

impl RpcClient {
    pub fn start(r: BoxRead, w: BoxWrite) -> RpcClient {
        let (tx, rx) = mpsc::channel::<Frame>(4096);
        let pending: Pending = Default::default();
        let subs: Subs = Default::default();
        let closed = Arc::new(AtomicBool::new(false));
        {
            let closed = closed.clone();
            tokio::spawn(async move {
                if let Err(e) = write_loop(w, rx).await {
                    tracing::debug!("rpc writer ended: {e}");
                }
                closed.store(true, Ordering::Release);
            });
        }
        {
            let pending = pending.clone();
            let subs = subs.clone();
            let closed = closed.clone();
            tokio::spawn(async move {
                let mut r = r;
                let mut buf = BytesMut::new();
                loop {
                    match read_frame(&mut r, &mut buf).await {
                        Ok(Some(f)) if f.kind == Kind::Push => {
                            let id = f.req_id;
                            let tx = subs.lock().get(&id).cloned();
                            if let Some(tx) = tx {
                                // Backpressure: a slow subscriber stalls this
                                // connection's reader, and TCP flow
                                // control then stalls the server's push.
                                if tx.send(f).await.is_err() {
                                    subs.lock().remove(&id);
                                }
                            }
                        }
                        Ok(Some(f)) => {
                            if let Some(tx) = pending.lock().remove(&f.req_id) {
                                let _ = tx.send(f);
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::debug!("rpc reader ended: {e}");
                            break;
                        }
                    }
                }
                closed.store(true, Ordering::Release);
                pending.lock().clear(); // wakes all waiters with an error
                subs.lock().clear(); // ends all subscriptions
            });
        }
        RpcClient {
            tx,
            pending,
            subs,
            next_id: AtomicU64::new(1),
            closed,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Opens a server-push stream: sends one request and returns the
    /// receiver of every `Push` frame carrying its req_id. The stream ends
    /// when the connection closes.
    pub async fn subscribe(
        &self,
        kind: Kind,
        body: bytes::Bytes,
        buffer: usize,
    ) -> Result<mpsc::Receiver<Frame>> {
        if self.is_closed() {
            bail!("connection closed");
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(buffer);
        self.subs.lock().insert(id, tx);
        self.tx
            .send(Frame::new(kind, id, body))
            .await
            .map_err(|_| anyhow!("connection closed"))?;
        Ok(rx)
    }

    pub async fn call(&self, kind: Kind, body: bytes::Bytes, timeout: Duration) -> Result<Frame> {
        if self.is_closed() {
            bail!("connection closed");
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(id, tx);
        let res = tokio::time::timeout(timeout, async {
            self.tx
                .send(Frame::new(kind, id, body))
                .await
                .map_err(|_| anyhow!("connection closed"))?;
            rx.await.map_err(|_| anyhow!("connection closed"))
        })
        .await;
        match res {
            Ok(Ok(f)) => {
                if f.kind == Kind::Error {
                    bail!("remote error: {}", String::from_utf8_lossy(&f.body));
                }
                Ok(f)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                self.pending.lock().remove(&id);
                bail!("rpc timeout after {timeout:?}")
            }
        }
    }
}

/// The frame reader faces the network: any byte stream, cut into any reads, yields the frames
/// written to it in order and then a clean end, or an error, never a panic.
#[cfg(test)]
mod props {
    use super::*;
    use proptest::prelude::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// Hands out the stream in the given read sizes.
    struct Chunks(Vec<u8>, Vec<usize>, usize);

    impl AsyncRead for Chunks {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            out: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let Chunks(b, sizes, at) = &mut *self;
            let n = sizes.get(*at).copied().unwrap_or(usize::MAX);
            *at += 1;
            let n = n.max(1).min(out.remaining()).min(b.len());
            out.put_slice(&b[..n]);
            b.drain(..n);
            Poll::Ready(Ok(()))
        }
    }

    fn read_all(bytes: Vec<u8>, sizes: Vec<usize>) -> (Vec<Frame>, Result<()>) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut r = Chunks(bytes, sizes, 0);
            let mut buf = BytesMut::new();
            let mut got = vec![];
            loop {
                match read_frame(&mut r, &mut buf).await {
                    Ok(Some(f)) => got.push(f),
                    Ok(None) => return (got, Ok(())),
                    Err(e) => return (got, Err(e)),
                }
            }
        })
    }

    proptest! {
        #[test]
        fn frames_read_back_in_order_however_the_stream_is_cut(
            frames in prop::collection::vec(
                (1u8..=10, any::<u64>(), prop::collection::vec(any::<u8>(), 0..64)), 0..8),
            sizes in prop::collection::vec(1usize..40, 0..64),
            cut in any::<prop::sample::Index>(),
        ) {
            let mut b = BytesMut::new();
            for (k, id, body) in &frames {
                Frame::new(Kind::from_u8(*k).unwrap(), *id, body.clone().into()).encode_into(&mut b);
            }
            let (got, end) = read_all(b.to_vec(), sizes.clone());
            prop_assert!(end.is_ok());
            prop_assert_eq!(got.len(), frames.len());
            for (f, (k, id, body)) in got.iter().zip(&frames) {
                prop_assert_eq!((f.kind as u8, f.req_id, &f.body[..]), (*k, *id, &body[..]));
            }
            // a stream cut inside a frame is an error after the frames before the cut
            let c = cut.index(b.len() + 1);
            let whole = frames.iter().scan(0, |at, f| { *at += 13 + f.2.len(); Some(*at) })
                .filter(|&e| e <= c).count();
            let (got, end) = read_all(b[..c].to_vec(), sizes);
            prop_assert_eq!(got.len(), whole);
            let boundary = c == 0 || frames.iter().scan(0, |at, f| { *at += 13 + f.2.len(); Some(*at) }).any(|e| e == c);
            prop_assert_eq!(end.is_ok(), boundary);
        }

        #[test]
        fn any_bytes_read_as_frames_or_an_error(
            bytes in prop::collection::vec(any::<u8>(), 0..256),
            sizes in prop::collection::vec(1usize..40, 0..16),
        ) {
            let _ = read_all(bytes, sizes);
        }
    }
}
