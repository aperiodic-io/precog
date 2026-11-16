//! Connectivity: plain TCP (benchmark baseline, loopback/LAN) or Iroh QUIC
//! (authenticated by endpoint key, NAT traversal and relays available).
//!
//! Address syntax:
//!   tcp://127.0.0.1:7001
//!   iroh://<endpoint-id>@127.0.0.1:7101[,10.0.0.5:7101]
//!
//! Iroh choice for this POC: one long-lived connection per peer and a single
//! bidirectional stream carrying the multiplexed RPC frames. The storage
//! protocol, not Iroh, defines durability.

use crate::rpc::{BoxRead, BoxWrite, RpcClient};
use anyhow::{Context, Result, anyhow, bail};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

pub const ALPN: &[u8] = b"precog/mesh/2";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Addr {
    Tcp(SocketAddr),
    Iroh { id: EndpointId, addrs: Vec<SocketAddr> },
}

impl FromStr for Addr {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Addr> {
        if let Some(rest) = s.strip_prefix("tcp://") {
            return Ok(Addr::Tcp(rest.parse().with_context(|| format!("bad tcp addr {s}"))?));
        }
        if let Some(rest) = s.strip_prefix("iroh://") {
            let (id, addrs) = rest.split_once('@').unwrap_or((rest, ""));
            let id = EndpointId::from_str(id).map_err(|e| anyhow!("bad iroh endpoint id: {e}"))?;
            let addrs = addrs
                .split(',')
                .filter(|a| !a.is_empty())
                .map(|a| a.parse().with_context(|| format!("bad socket addr {a}")))
                .collect::<Result<_>>()?;
            return Ok(Addr::Iroh { id, addrs });
        }
        bail!("address must start with tcp:// or iroh://: {s}")
    }
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Addr::Tcp(a) => write!(f, "tcp://{a}"),
            Addr::Iroh { id, addrs } => {
                let a: Vec<String> = addrs.iter().map(|a| a.to_string()).collect();
                write!(f, "iroh://{id}@{}", a.join(","))
            }
        }
    }
}

/// Binds an Iroh endpoint in direct mode: no relay, no address lookup;
/// peers are dialed by pinned endpoint id plus configured socket addresses.
/// `relay = true` uses n0's public relays/discovery instead (for NAT'd
/// commodity hosts; not exercised by the benchmarks).
pub async fn iroh_endpoint(secret: Option<SecretKey>, bind: Option<SocketAddr>, relay: bool) -> Result<Endpoint> {
    let mut b = if relay { Endpoint::builder(presets::N0) } else { Endpoint::builder(presets::Minimal) };
    if let Some(sk) = secret {
        b = b.secret_key(sk);
    }
    if let Some(addr) = bind {
        b = b.clear_ip_transports().bind_addr(addr).map_err(|e| anyhow!("bind {addr}: {e}"))?;
    }
    b = b.alpns(vec![ALPN.to_vec()]);
    b.bind().await.map_err(|e| anyhow!("iroh bind: {e}"))
}

pub fn load_or_create_secret(path: &std::path::Path) -> Result<SecretKey> {
    if path.exists() {
        let hex = std::fs::read_to_string(path)?;
        let bytes = crate::proto::hex_decode(hex.trim())?;
        let arr: [u8; 32] = bytes.try_into().map_err(|_| anyhow!("secret must be 32 bytes"))?;
        return Ok(SecretKey::from_bytes(&arr));
    }
    let sk = SecretKey::generate();
    std::fs::write(path, crate::proto::hex_encode(&sk.to_bytes()))?;
    Ok(sk)
}

/// Outbound side shared by nodes (peer links) and clients.
#[derive(Clone)]
pub struct Dialer {
    pub iroh: Option<Endpoint>,
}

impl Dialer {
    pub async fn connect(&self, addr: &Addr, timeout: Duration) -> Result<RpcClient> {
        tokio::time::timeout(timeout, self.connect_inner(addr))
            .await
            .map_err(|_| anyhow!("connect to {addr} timed out"))?
    }

    async fn connect_inner(&self, addr: &Addr) -> Result<RpcClient> {
        match addr {
            Addr::Tcp(a) => {
                let s = tokio::net::TcpStream::connect(a).await?;
                s.set_nodelay(true)?;
                let (r, w) = s.into_split();
                Ok(RpcClient::start(Box::new(r) as BoxRead, Box::new(w) as BoxWrite, None))
            }
            Addr::Iroh { id, addrs } => {
                let ep = self.iroh.as_ref().context("iroh address but no iroh endpoint")?;
                let target = EndpointAddr::from_parts(*id, addrs.iter().map(|a| TransportAddr::Ip(*a)));
                let conn = ep.connect(target, ALPN).await.map_err(|e| anyhow!("iroh connect: {e}"))?;
                if conn.remote_id() != *id {
                    bail!("iroh peer identity mismatch");
                }
                let (send, recv) = conn.open_bi().await.map_err(|e| anyhow!("open_bi: {e}"))?;
                Ok(RpcClient::start(Box::new(recv), Box::new(send), Some(Box::new(conn))))
            }
        }
    }
}
