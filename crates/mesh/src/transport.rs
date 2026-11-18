//! Connectivity: plain TCP.
//!
//! Address syntax:
//!   tcp://127.0.0.1:7001
//!
//! QUIC/Iroh was removed: TCP measured 1.4-1.6x faster at every load.

use crate::rpc::RpcClient;
use anyhow::{Context, Result, anyhow, bail};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Addr {
    Tcp(SocketAddr),
}

impl FromStr for Addr {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Addr> {
        if let Some(rest) = s.strip_prefix("tcp://") {
            return Ok(Addr::Tcp(
                rest.parse().with_context(|| format!("bad tcp addr {s}"))?,
            ));
        }
        bail!("address must start with tcp://: {s}")
    }
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Addr::Tcp(a) => write!(f, "tcp://{a}"),
        }
    }
}

/// Outbound side shared by nodes (peer links) and clients.
pub async fn connect(addr: &Addr, timeout: Duration) -> Result<RpcClient> {
    let Addr::Tcp(a) = addr;
    let s = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(a))
        .await
        .map_err(|_| anyhow!("connect to {addr} timed out"))??;
    s.set_nodelay(true)?;
    let (r, w) = s.into_split();
    Ok(RpcClient::start(Box::new(r), Box::new(w)))
}
