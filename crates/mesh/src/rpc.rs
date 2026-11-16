//! Multiplexed request/response over one ordered byte stream (a TCP socket or
//! one Iroh QUIC bidirectional stream). Many requests can be in flight;
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

/// Client side of a multiplexed connection.
pub struct RpcClient {
    tx: mpsc::Sender<Frame>,
    pending: Pending,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    /// Keeps the underlying transport (e.g. an Iroh connection) alive.
    _keep: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl RpcClient {
    pub fn start(
        r: BoxRead,
        w: BoxWrite,
        keep: Option<Box<dyn std::any::Any + Send + Sync>>,
    ) -> RpcClient {
        let (tx, rx) = mpsc::channel::<Frame>(4096);
        let pending: Pending = Default::default();
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
            let closed = closed.clone();
            tokio::spawn(async move {
                let mut r = r;
                let mut buf = BytesMut::new();
                loop {
                    match read_frame(&mut r, &mut buf).await {
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
            });
        }
        RpcClient {
            tx,
            pending,
            next_id: AtomicU64::new(1),
            closed,
            _keep: keep,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
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
