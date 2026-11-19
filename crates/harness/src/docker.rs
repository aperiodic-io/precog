//! A 3-node cluster in Docker (Precog or the Redpanda dedup fork) and the faults the scenarios
//! inject: SIGKILL, SIGSTOP/SIGCONT, network cuts (iptables on the host's DOCKER-USER chain, so
//! no image needs iptables), a full disk (a host tmpfs under a node's data dir) and clock skew
//! (libfaketime). Needs root for iptables and mount.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const NET: &str = "precog-harness";
const SUBNET: &str = "172.31.79.0/24";
pub const TOKEN: &str = "harness";
pub const IMAGE_RP: &str = "ghcr.io/dream-faster/redpanda:71f80ccdf79917925ec1e3fb4e958017480a65fc";
const FAKETIME_LIB: &str = "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum System {
    Precog,
    Redpanda,
}

pub fn ip(i: usize) -> String {
    format!("172.31.79.{}", 10 + i)
}

pub async fn sh(args: &[&str]) -> Result<String> {
    let out = tokio::process::Command::new(args[0])
        .args(&args[1..])
        .output()
        .await
        .with_context(|| format!("run {args:?}"))?;
    if !out.status.success() {
        bail!("{args:?}: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Builds the Precog node image from a `precog-node` binary.
pub async fn build_precog_image(node_bin: &Path, work: &Path) -> Result<()> {
    let ctx = work.join("image");
    std::fs::create_dir_all(&ctx)?;
    std::fs::copy(node_bin, ctx.join("precog-node")).context("copy precog-node")?;
    // HARNESS_BASE: another debian image (a local one, when the registry rate-limits pulls)
    let base = std::env::var("HARNESS_BASE").unwrap_or_else(|_| "debian:trixie-slim".into());
    std::fs::write(
        ctx.join("Dockerfile"),
        format!(
            "FROM {base}\n\
             RUN apt-get update && apt-get install -y --no-install-recommends faketime && rm -rf /var/lib/apt/lists/*\n\
             COPY precog-node /usr/local/bin/\n"
        ),
    )?;
    sh(&[
        "docker",
        "build",
        "-q",
        "-t",
        "precog-harness",
        ctx.to_str().unwrap(),
    ])
    .await?;
    Ok(())
}

pub struct Cluster {
    pub system: System,
    pub n: usize,
    pub work: PathBuf,
    /// The Precog node config every node shares (scenarios edit it before `up`).
    pub cfg: Value,
    /// libfaketime offsets per node (e.g. "+600s").
    pub skew: Vec<Option<String>>,
    /// Redpanda: `write_caching_default`.
    pub rp_write_caching: bool,
    /// Data dirs mounted as a tmpfs of this many MiB (a disk that fills up).
    pub tmpfs_mb: Vec<Option<u64>>,
    rules: Vec<Vec<String>>,
}

impl Cluster {
    pub fn new(system: System, work: &Path) -> Cluster {
        let peers: Vec<Value> = (0..3)
            .map(|i| json!({"id": i, "domain": format!("d{i}"), "addr": format!("tcp://{}:7000", ip(i))}))
            .collect();
        Cluster {
            system,
            n: 3,
            work: work.to_path_buf(),
            cfg: json!({"id": 0, "domain": "", "data_dir": "/var/lib/precog", "token": TOKEN,
                "listen_tcp": "0.0.0.0:7000", "listen_metrics": "0.0.0.0:9100", "peers": peers,
                "target_copies": 3, "rpc_timeout_ms": 500, "repair_interval_ms": 1000,
                "segment_size": 64u64 << 20, "dedup_window_ms": 300000}),
            skew: vec![None; 3],
            rp_write_caching: true,
            tmpfs_mb: vec![None; 3],
            rules: vec![],
        }
    }

    pub fn name(&self, i: usize) -> String {
        match self.system {
            System::Precog => format!("ph-{i}"),
            System::Redpanda => format!("rh-{i}"),
        }
    }

    pub fn data_dir(&self, i: usize) -> PathBuf {
        self.work.join(format!("n{i}"))
    }

    pub async fn up(&mut self) -> Result<()> {
        self.down().await?;
        if sh(&["docker", "network", "inspect", NET]).await.is_err() {
            sh(&["docker", "network", "create", "--subnet", SUBNET, NET]).await?;
        }
        match self.system {
            System::Precog => self.up_precog().await,
            System::Redpanda => self.up_redpanda().await,
        }
    }

    async fn up_precog(&mut self) -> Result<()> {
        let cfg_path = self.work.join("cluster.json");
        std::fs::write(&cfg_path, serde_json::to_vec_pretty(&self.cfg)?)?;
        for i in 0..self.n {
            let dir = self.data_dir(i);
            std::fs::create_dir_all(&dir)?;
            if let Some(mb) = self.tmpfs_mb[i] {
                sh(&[
                    "mount",
                    "-t",
                    "tmpfs",
                    "-o",
                    &format!("size={mb}m"),
                    "tmpfs",
                    dir.to_str().unwrap(),
                ])
                .await?;
            }
            let (name, ip) = (self.name(i), ip(i));
            let vol_cfg = format!("{}:/etc/precog/cluster.json:ro", cfg_path.display());
            let vol_data = format!("{}:/var/lib/precog", dir.display());
            let mut args = vec![
                "docker",
                "run",
                "-d",
                "--name",
                &name,
                "--hostname",
                &name,
                "--network",
                NET,
                "--ip",
                &ip,
                "-v",
                &vol_cfg,
                "-v",
                &vol_data,
                "-e",
                "RUST_LOG=info",
            ];
            let (ft, pre);
            if let Some(s) = &self.skew[i] {
                ft = format!("FAKETIME={s}");
                pre = format!("LD_PRELOAD={FAKETIME_LIB}");
                args.extend([
                    "-e",
                    &ft,
                    "-e",
                    &pre,
                    "-e",
                    "FAKETIME_DONT_FAKE_MONOTONIC=1",
                ]);
            }
            args.extend([
                "--entrypoint",
                "precog-node",
                "precog-harness",
                "serve",
                "--config",
                "/etc/precog/cluster.json",
                "--member-from-hostname",
                "--threads",
                "2",
            ]);
            sh(&args).await?;
        }
        Ok(())
    }

    async fn up_redpanda(&mut self) -> Result<()> {
        let seeds = (0..3)
            .map(|i| format!("{}:33145", ip(i)))
            .collect::<Vec<_>>()
            .join(",");
        for i in 0..self.n {
            let (name, ip) = (self.name(i), ip(i));
            let id = i.to_string();
            let adv = format!("internal://{ip}:9092");
            let rpc = format!("{ip}:33145");
            sh(&[
                "docker",
                "run",
                "-d",
                "--name",
                &name,
                "--network",
                NET,
                "--ip",
                &ip,
                "--user",
                "root",
                IMAGE_RP,
                "redpanda",
                "start",
                "--smp=1",
                "--memory=2G",
                "--reserve-memory=0M",
                "--overprovisioned",
                &format!("--node-id={id}"),
                "--check=false",
                "--kafka-addr=internal://0.0.0.0:9092",
                &format!("--advertise-kafka-addr={adv}"),
                &format!("--rpc-addr={rpc}"),
                &format!("--advertise-rpc-addr={rpc}"),
                &format!("--seeds={seeds}"),
            ])
            .await?;
        }
        for _ in 0..90 {
            if self
                .rpk(&["cluster", "health"])
                .await
                .is_ok_and(|h| h.contains("Healthy:") && h.contains("true"))
            {
                self.rpk(&[
                    "cluster",
                    "config",
                    "set",
                    "auto_create_topics_enabled",
                    "false",
                ])
                .await?;
                let wc = self.rp_write_caching.to_string();
                self.rpk(&["cluster", "config", "set", "write_caching_default", &wc])
                    .await?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        bail!("redpanda never became healthy")
    }

    pub async fn rpk(&self, args: &[&str]) -> Result<String> {
        let mut a = vec!["docker", "exec", "rh-0", "/opt/redpanda/bin/rpk"];
        a.extend(args);
        sh(&a).await
    }

    pub async fn down(&mut self) -> Result<()> {
        self.heal_network().await;
        for i in 0..3 {
            let _ = sh(&["docker", "rm", "-f", "-v", &self.name(i)]).await;
            let d = self.data_dir(i);
            if d.exists() {
                let _ = sh(&["umount", d.to_str().unwrap()]).await;
                let _ = std::fs::remove_dir_all(&d);
            }
        }
        Ok(())
    }

    pub async fn kill(&self, i: usize) -> Result<()> {
        sh(&["docker", "kill", "-s", "KILL", &self.name(i)])
            .await
            .map(drop)
    }
    pub async fn freeze(&self, i: usize) -> Result<()> {
        sh(&["docker", "kill", "-s", "STOP", &self.name(i)])
            .await
            .map(drop)
    }
    pub async fn thaw(&self, i: usize) -> Result<()> {
        sh(&["docker", "kill", "-s", "CONT", &self.name(i)])
            .await
            .map(drop)
    }
    pub async fn start(&self, i: usize) -> Result<()> {
        sh(&["docker", "start", &self.name(i)]).await.map(drop)
    }
    pub async fn running(&self, i: usize) -> bool {
        sh(&[
            "docker",
            "inspect",
            "-f",
            "{{.State.Running}}",
            &self.name(i),
        ])
        .await
        .is_ok_and(|s| s.trim() == "true")
    }

    async fn rule(&mut self, r: Vec<String>) -> Result<()> {
        let mut a = vec!["iptables".to_string(), "-I".into()];
        a.extend(r.iter().cloned());
        sh(&a.iter().map(|s| s.as_str()).collect::<Vec<_>>()).await?;
        self.rules.push(r);
        Ok(())
    }

    /// Drops traffic between nodes `a` and `b`, both ways.
    pub async fn cut(&mut self, a: usize, b: usize) -> Result<()> {
        for (s, d) in [(a, b), (b, a)] {
            self.rule(vec![
                "DOCKER-USER".into(),
                "-s".into(),
                ip(s),
                "-d".into(),
                ip(d),
                "-j".into(),
                "DROP".into(),
            ])
            .await?;
        }
        Ok(())
    }

    /// Cuts node `i` from its peers and, with `clients`, from this host (the harness's clients).
    pub async fn isolate(&mut self, i: usize, clients: bool) -> Result<()> {
        for j in (0..self.n).filter(|&j| j != i) {
            self.cut(i, j).await?;
        }
        if clients {
            self.rule(vec![
                "OUTPUT".into(),
                "-d".into(),
                ip(i),
                "-j".into(),
                "DROP".into(),
            ])
            .await?;
            self.rule(vec![
                "INPUT".into(),
                "-s".into(),
                ip(i),
                "-j".into(),
                "DROP".into(),
            ])
            .await?;
        }
        Ok(())
    }

    pub async fn heal_network(&mut self) {
        for r in std::mem::take(&mut self.rules) {
            let mut a = vec!["iptables".to_string(), "-D".into()];
            a.extend(r);
            let _ = sh(&a.iter().map(|s| s.as_str()).collect::<Vec<_>>()).await;
        }
    }

    /// Every fault undone: network healed, frozen nodes resumed, stopped nodes started.
    pub async fn heal(&mut self) -> Result<()> {
        self.heal_network().await;
        for i in 0..self.n {
            let _ = self.thaw(i).await;
            if !self.running(i).await {
                self.start(i).await?;
            }
        }
        Ok(())
    }

    /// Saves every node's log under `dir` (kept with the results).
    pub async fn save_logs(&self, dir: &Path) {
        let _ = std::fs::create_dir_all(dir);
        for i in 0..self.n {
            if let Ok(o) = tokio::process::Command::new("docker")
                .args(["logs", &self.name(i)])
                .output()
                .await
            {
                let mut all = o.stdout;
                all.extend(o.stderr);
                let _ = std::fs::write(dir.join(format!("{}.log", self.name(i))), all);
            }
        }
    }

    /// A node's `/health` status line (Precog).
    pub async fn health(&self, i: usize) -> Result<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(format!("{}:9100", ip(i))),
        )
        .await??;
        s.write_all(b"GET /health HTTP/1.0\r\n\r\n").await?;
        let mut out = String::new();
        tokio::time::timeout(Duration::from_secs(2), s.read_to_string(&mut out)).await??;
        Ok(out.lines().next().unwrap_or("").to_string())
    }

    /// Bytes under a node's data dir.
    pub fn disk_bytes(&self, i: usize) -> u64 {
        fn walk(p: &Path) -> u64 {
            std::fs::read_dir(p).map_or(0, |rd| {
                rd.flatten()
                    .map(|e| match e.metadata() {
                        Ok(m) if m.is_dir() => walk(&e.path()),
                        Ok(m) => m.len(),
                        Err(_) => 0,
                    })
                    .sum()
            })
        }
        walk(&self.data_dir(i))
    }

    /// Grows a tmpfs data dir (the full disk gets room again).
    pub async fn grow_tmpfs(&self, i: usize, mb: u64) -> Result<()> {
        let d = self.data_dir(i);
        sh(&[
            "mount",
            "-o",
            &format!("remount,size={mb}m"),
            d.to_str().unwrap(),
        ])
        .await
        .map(drop)
    }
}
