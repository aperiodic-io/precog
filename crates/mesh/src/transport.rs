//! Connectivity: plain TCP to `tcp://<host>:<port>`, where host is an IP address or a DNS name
//! (resolved at every dial, so a StatefulSet pod that moves keeps its name).

use crate::rpc::RpcClient;
use anyhow::{Result, anyhow, bail};
use std::str::FromStr;
use std::time::Duration;

/// `host:port`, checked when parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Addr(String);

impl FromStr for Addr {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Addr> {
        let Some(rest) = s.strip_prefix("tcp://") else {
            bail!("address must start with tcp://: {s}")
        };
        match rest.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {
                Ok(Addr(rest.to_string()))
            }
            _ => bail!("bad tcp address (want tcp://host:port): {s}"),
        }
    }
}

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tcp://{}", self.0)
    }
}

/// Outbound side shared by nodes (peer links) and clients.
pub async fn connect(addr: &Addr, timeout: Duration) -> Result<RpcClient> {
    let s = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr.0.as_str()))
        .await
        .map_err(|_| anyhow!("connect to {addr} timed out"))??;
    s.set_nodelay(true)?;
    let (r, w) = s.into_split();
    Ok(RpcClient::start(Box::new(r), Box::new(w)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_ip_or_dns_names_with_a_port() {
        for ok in [
            "tcp://127.0.0.1:7001",
            "tcp://precog-1.precog.live-ingestion.svc.cluster.local:7000",
            "tcp://[::1]:7000",
        ] {
            let a: Addr = ok.parse().unwrap();
            assert_eq!(a.to_string(), ok);
        }
        for bad in [
            "tcp://precog-1",
            "tcp://host:port",
            "tcp://:7000",
            "udp://1.2.3.4:5",
            "tcp://h:70000",
        ] {
            assert!(bad.parse::<Addr>().is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_dns_name_is_resolved_when_dialing() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move { while l.accept().await.is_ok() {} });
        let a: Addr = format!("tcp://localhost:{port}").parse().unwrap();
        connect(&a, Duration::from_secs(5)).await.unwrap();
    }
}
