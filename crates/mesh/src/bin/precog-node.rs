use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use precog_mesh::client::{MeshClient, Writer};
use precog_mesh::node::{Node, NodeConfig, PeerConfig};
use precog_mesh::proto::Control;
use precog_mesh::transport::Addr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Precog mesh node and admin CLI")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a node.
    Serve {
        #[arg(long)]
        config: PathBuf,
        /// The config lists every node in `peers`; this node is the one whose id is the
        /// hostname's ordinal (a StatefulSet pod `precog-2` is node 2).
        #[arg(long)]
        member_from_hostname: bool,
        /// Tokio worker threads (default: all cores).
        #[arg(long)]
        threads: Option<usize>,
        /// `current-thread` runs the node's network and replication on one thread: about 8%
        /// less CPU per record at a fixed rate, but lower peak throughput at a fixed number of
        /// appends in flight.
        #[arg(long, value_enum, default_value_t = Runtime::MultiThread)]
        runtime: Runtime,
    },
    /// Write configs for an N-node local cluster (one failure domain label per node).
    Init {
        #[arg(long, default_value_t = 3)]
        nodes: u32,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 7000)]
        base_port: u16,
        /// Segment file size in MiB.
        #[arg(long, default_value_t = 1024)]
        segment_mb: u64,
        /// Keep at most this many MiB of log per node (unbounded if unset).
        #[arg(long)]
        retention_mb: Option<u64>,
        #[arg(long, default_value_t = 1000)]
        rpc_timeout_ms: u64,
    },
    /// Query node status.
    Status {
        #[arg(long)]
        cluster: PathBuf,
        #[arg(long, default_value_t = 0)]
        node: usize,
    },
    /// Append N demo events (descending event time, ascending sequence).
    Demo {
        #[arg(long)]
        cluster: PathBuf,
        #[arg(long, default_value_t = 10)]
        count: u64,
        #[arg(long, default_value_t = 2)]
        copies: u8,
        #[arg(long, default_value = "market")]
        stream: String,
        #[arg(long, default_value = "demo-writer")]
        writer: String,
        #[arg(long, default_value_t = 0)]
        node: usize,
    },
    /// Event-time ordered read of a stream from one node.
    Read {
        #[arg(long)]
        cluster: PathBuf,
        #[arg(long, default_value = "market")]
        stream: String,
        #[arg(long, default_value_t = 0)]
        node: usize,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Trigger an anti-entropy pass on one node.
    Repair {
        #[arg(long)]
        cluster: PathBuf,
        #[arg(long, default_value_t = 0)]
        node: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Runtime {
    MultiThread,
    CurrentThread,
}

/// `cluster.json`: what clients need to reach the nodes.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ClusterFile {
    pub token: String,
    pub nodes: Vec<String>,
}

pub fn client_from(path: &PathBuf) -> Result<std::sync::Arc<MeshClient>> {
    let c: ClusterFile = serde_json::from_slice(&std::fs::read(path)?)?;
    let addrs = c
        .nodes
        .iter()
        .map(|s| s.parse())
        .collect::<Result<Vec<Addr>>>()?;
    MeshClient::new(addrs, c.token)
}

fn builder(runtime: Runtime, threads: Option<usize>) -> Result<tokio::runtime::Builder> {
    Ok(match (runtime, threads) {
        (Runtime::CurrentThread, Some(_)) => {
            anyhow::bail!("--threads does not apply to --runtime current-thread")
        }
        (Runtime::CurrentThread, None) => tokio::runtime::Builder::new_current_thread(),
        (Runtime::MultiThread, t) => {
            let mut b = tokio::runtime::Builder::new_multi_thread();
            if let Some(t) = t {
                b.worker_threads(t);
            }
            b
        }
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let (threads, runtime) = match &cli.cmd {
        Cmd::Serve {
            threads, runtime, ..
        } => (*threads, *runtime),
        _ => (Some(2), Runtime::MultiThread),
    };
    builder(runtime, threads)?
        .enable_all()
        .build()?
        .block_on(run(cli.cmd))
}

async fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Serve {
            config,
            member_from_hostname,
            ..
        } => {
            let mut cfg: NodeConfig =
                serde_json::from_slice(&std::fs::read(&config).context("read config")?)?;
            if member_from_hostname {
                let host = std::env::var("HOSTNAME").context("HOSTNAME")?;
                cfg = cfg.for_member(precog_mesh::node::ordinal(&host)?)?;
            }
            Node::run(cfg).await
        }
        Cmd::Init {
            nodes,
            dir,
            base_port,
            segment_mb,
            retention_mb,
            rpc_timeout_ms,
        } => {
            std::fs::create_dir_all(&dir)?;
            let token = precog_mesh::proto::hex_encode(&rand::random::<[u8; 16]>());
            let addrs: Vec<String> = (0..nodes)
                .map(|i| format!("tcp://127.0.0.1:{}", base_port + i as u16))
                .collect();
            for i in 0..nodes {
                let port = base_port + i as u16;
                let sock = format!("127.0.0.1:{port}").parse()?;
                let mut cfg = NodeConfig::single(
                    i,
                    &format!("domain-{i}"),
                    std::path::absolute(dir.join(format!("n{i}")))?,
                    token.clone(),
                );
                cfg.listen_tcp = Some(sock);
                cfg.peers = (0..nodes)
                    .filter(|&j| j != i)
                    .map(|j| PeerConfig {
                        id: j,
                        domain: format!("domain-{j}"),
                        addr: addrs[j as usize].clone(),
                    })
                    .collect();
                cfg.rpc_timeout_ms = rpc_timeout_ms;
                cfg.segment_size = segment_mb << 20;
                cfg.retention_bytes = retention_mb.map(|m| m << 20);
                std::fs::write(
                    dir.join(format!("node-{i}.json")),
                    serde_json::to_vec_pretty(&cfg)?,
                )?;
            }
            let cluster = ClusterFile {
                token,
                nodes: addrs,
            };
            std::fs::write(
                dir.join("cluster.json"),
                serde_json::to_vec_pretty(&cluster)?,
            )?;
            println!(
                "wrote {} node configs and cluster.json to {}",
                nodes,
                dir.display()
            );
            Ok(())
        }
        Cmd::Status { cluster, node } => {
            let c = client_from(&cluster)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&c.control(node, &Control::Status).await?)?
            );
            Ok(())
        }
        Cmd::Repair { cluster, node } => {
            let c = client_from(&cluster)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&c.control(node, &Control::Repair).await?)?
            );
            Ok(())
        }
        Cmd::Read {
            cluster,
            stream,
            node,
            limit,
        } => {
            let c = client_from(&cluster)?;
            let r = c
                .control(
                    node,
                    &Control::Read {
                        stream,
                        from_time_us: 0,
                        limit,
                    },
                )
                .await?;
            println!("{}", serde_json::to_string_pretty(&r)?);
            Ok(())
        }
        Cmd::Demo {
            cluster,
            count,
            copies,
            stream,
            writer,
            node,
        } => {
            let c = client_from(&cluster)?;
            let epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs();
            let mut w = Writer::new(c, &stream, &writer, epoch, node, copies);
            for i in 0..count {
                let payload = format!("event {i}");
                // Event time runs backwards to show that read order is event
                // time, not writer sequence.
                let r = w
                    .append(&[(1_000_000 - i * 1000, payload.as_bytes())])
                    .await?;
                println!(
                    "seq {} -> node {} copies {} nodes {:#b}",
                    w.next_seq - 1,
                    r.node,
                    r.ack.copies,
                    r.ack.nodes
                );
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Why: a node below saturation spends a fifth of its CPU on wake-ups between the runtime's
    /// threads; one thread for everything is the opt-in for that.
    #[test]
    fn serve_runs_on_a_multi_thread_runtime_unless_asked() {
        let parse = |extra: &[&str]| {
            let mut a = vec!["precog-node", "serve", "--config", "c.json"];
            a.extend_from_slice(extra);
            match Cli::try_parse_from(a).map(|c| c.cmd) {
                Ok(Cmd::Serve { runtime, .. }) => Ok(runtime),
                Ok(_) => unreachable!(),
                Err(e) => Err(e),
            }
        };
        assert_eq!(parse(&[]).unwrap(), Runtime::MultiThread);
        assert_eq!(
            parse(&["--runtime", "current-thread"]).unwrap(),
            Runtime::CurrentThread
        );
        assert!(parse(&["--runtime", "fibers"]).is_err());
        // worker threads mean nothing to a current-thread runtime
        let rt = parse(&["--runtime", "current-thread", "--threads", "2"]).unwrap();
        assert!(builder(rt, Some(2)).is_err());
        assert!(builder(Runtime::MultiThread, Some(2)).is_ok());
        let one = builder(Runtime::CurrentThread, None)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(one.metrics().num_workers(), 1);
    }
}
