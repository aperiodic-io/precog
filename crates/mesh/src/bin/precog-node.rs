use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use precog_mesh::client::{MeshClient, Writer};
use precog_mesh::node::{Node, NodeConfig, PeerConfig};
use precog_mesh::proto::Control;
use precog_mesh::transport::{Addr, load_or_create_secret};
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
        /// Tokio worker threads (default: all cores).
        #[arg(long)]
        threads: Option<usize>,
    },
    /// Write configs for an N-node local cluster (one failure domain label per node).
    Init {
        #[arg(long, default_value_t = 3)]
        nodes: u32,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value = "tcp")]
        transport: String,
        #[arg(long, default_value_t = 7000)]
        base_port: u16,
        /// Disable fdatasync (page-cache durability only).
        #[arg(long)]
        no_sync: bool,
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

/// `cluster.json`: what clients need to reach the nodes.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ClusterFile {
    pub token: String,
    pub nodes: Vec<String>,
}

pub async fn client_from(path: &PathBuf) -> Result<std::sync::Arc<MeshClient>> {
    let c: ClusterFile = serde_json::from_slice(&std::fs::read(path)?)?;
    let addrs = c
        .nodes
        .iter()
        .map(|s| s.parse())
        .collect::<Result<Vec<Addr>>>()?;
    MeshClient::new(addrs, c.token).await
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let threads = match &cli.cmd {
        Cmd::Serve { threads, .. } => *threads,
        _ => Some(2),
    };
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(t) = threads {
        rt.worker_threads(t);
    }
    rt.enable_all().build()?.block_on(run(cli.cmd))
}

async fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Serve { config, .. } => {
            let cfg: NodeConfig =
                serde_json::from_slice(&std::fs::read(&config).context("read config")?)?;
            Node::run(cfg).await
        }
        Cmd::Init {
            nodes,
            dir,
            transport,
            base_port,
            no_sync,
            rpc_timeout_ms,
        } => {
            std::fs::create_dir_all(&dir)?;
            let token = precog_mesh::proto::hex_encode(&rand::random::<[u8; 16]>());
            let mut addrs = Vec::new();
            for i in 0..nodes {
                let data = dir.join(format!("n{i}"));
                std::fs::create_dir_all(&data)?;
                let port = base_port + i as u16;
                let addr = match transport.as_str() {
                    "tcp" => format!("tcp://127.0.0.1:{port}"),
                    "iroh" => {
                        let sk = load_or_create_secret(&data.join("iroh.secret"))?;
                        format!("iroh://{}@127.0.0.1:{port}", sk.public())
                    }
                    t => anyhow::bail!("unknown transport {t}"),
                };
                addrs.push(addr);
            }
            for i in 0..nodes {
                let port = base_port + i as u16;
                let sock = format!("127.0.0.1:{port}").parse()?;
                let cfg = NodeConfig {
                    id: i,
                    domain: format!("domain-{i}"),
                    data_dir: std::path::absolute(dir.join(format!("n{i}")))?,
                    token: token.clone(),
                    sync: !no_sync,
                    listen_tcp: (transport == "tcp").then_some(sock),
                    listen_iroh: (transport == "iroh").then_some(sock),
                    iroh_relay: false,
                    peers: (0..nodes)
                        .filter(|&j| j != i)
                        .map(|j| PeerConfig {
                            id: j,
                            domain: format!("domain-{j}"),
                            addr: addrs[j as usize].clone(),
                        })
                        .collect(),
                    target_copies: 3,
                    rpc_timeout_ms,
                    repair_interval_ms: 1000,
                };
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
            let c = client_from(&cluster).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&c.control(node, &Control::Status).await?)?
            );
            Ok(())
        }
        Cmd::Repair { cluster, node } => {
            let c = client_from(&cluster).await?;
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
            let c = client_from(&cluster).await?;
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
            let c = client_from(&cluster).await?;
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
