//! Acceptance scenarios (tests/acceptance/features) against real `precog-node` processes:
//! SIGKILLs, files damaged on disk, restarts, retention, replication and subscriptions.
//!
//! cargo test -p precog-mesh --test acceptance -- --tags 'not @nightly'

use bytes::Bytes;
use cucumber::{World, given, then, when};
use precog_mesh::client::{MeshClient, Writer};
use precog_mesh::proto::*;
use precog_mesh::transport::Addr;
use precog_storage::record;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_precog-node");
static NEXT_PORT: AtomicU16 = AtomicU16::new(17_100);

struct WriterState {
    stream: String,
    name: String,
    epoch: u64,
    acked: u64,
    size: usize,
    copies: u8,
}

type SubLog = Arc<Mutex<Vec<(String, u64, u32)>>>;

#[derive(Default, World)]
pub struct Mesh {
    dir: Option<tempfile::TempDir>,
    n: usize,
    procs: Arc<Mutex<Vec<Option<Child>>>>,
    client: Option<Arc<MeshClient>>,
    writers: Vec<WriterState>,
    subs: Vec<(SubLog, Arc<AtomicBool>)>,
    /// Clients holding a tenant's scoped token, by tenant.
    tenants: std::collections::HashMap<String, Arc<MeshClient>>,
    /// The epoch each deleted stream was deleted below.
    floors: std::collections::HashMap<String, u64>,
    /// Where node 0's metrics listen, and a number of series a scenario noted.
    metrics_port: u16,
    noted: usize,
}

impl std::fmt::Debug for Mesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Mesh({} nodes)", self.n)
    }
}

impl Drop for Mesh {
    fn drop(&mut self) {
        for s in &self.subs {
            s.1.store(true, Ordering::Release);
        }
        for c in self.procs.lock().unwrap().iter_mut().flatten() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn payload(writer: &str, seq: u64, size: usize) -> Vec<u8> {
    let h = writer.bytes().fold(seq.wrapping_mul(2654435761), |a, b| {
        a.wrapping_mul(31).wrapping_add(b as u64)
    });
    (0..size)
        .map(|j| (h.wrapping_add(j as u64 * 7) % 251) as u8)
        .collect()
}

impl Mesh {
    fn root(&self) -> &Path {
        self.dir.as_ref().unwrap().path()
    }
    fn client(&self) -> Arc<MeshClient> {
        self.client.clone().unwrap()
    }

    fn start(&self, i: usize) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root().join(format!("n{i}.log")))
            .unwrap();
        let child = Command::new(BIN)
            .args(["serve", "--config"])
            .arg(self.root().join(format!("node-{i}.json")))
            .args(["--threads", "2"])
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("spawn precog-node");
        self.procs.lock().unwrap()[i] = Some(child);
    }

    fn kill(&self, i: usize) {
        if let Some(mut c) = self.procs.lock().unwrap()[i].take() {
            let _ = c.kill(); // SIGKILL
            let _ = c.wait();
        }
    }

    async fn wait_ready(&self, i: usize) {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if self.status(i).await.is_some() {
                return;
            }
            if let Some(c) = self.procs.lock().unwrap()[i].as_mut()
                && let Ok(Some(st)) = c.try_wait()
            {
                panic!(
                    "node {i} exited during startup ({st}): {}",
                    self.node_log(i)
                );
            }
            assert!(
                Instant::now() < deadline,
                "node {i} not ready: {}",
                self.node_log(i)
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn node_log(&self, i: usize) -> String {
        std::fs::read_to_string(self.root().join(format!("n{i}.log"))).unwrap_or_default()
    }

    async fn status(&self, i: usize) -> Option<Status> {
        match self.client().control(i, &Control::Status).await {
            Ok(ControlResp::Status(s)) => Some(s),
            _ => None,
        }
    }

    /// The batches node `i` holds among `seqs` of a writer.
    async fn fetch(&self, i: usize, w: &WriterState, seqs: &[u64]) -> Vec<Batch> {
        let mut out = vec![];
        for chunk in seqs.chunks(256) {
            let ids: Vec<BatchId> = chunk
                .iter()
                .map(|&s| BatchId {
                    log: LogId {
                        stream: w.stream.clone(),
                        writer: w.name.clone(),
                        epoch: w.epoch,
                    },
                    first_seq: s,
                })
                .collect();
            let body = Bytes::from(serde_json::to_vec(&Control::Fetch { ids }).unwrap());
            let f = self
                .client()
                .call(i, Kind::Control, body)
                .await
                .expect("fetch");
            assert_eq!(f.kind, Kind::Batches);
            let mut b = &f.body[..];
            while b.len() >= 4 {
                let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                out.push(Batch::parse(f.body.slice_ref(&b[4..4 + n])).expect("valid batch"));
                b = &b[4 + n..];
            }
        }
        out
    }

    fn active_segment(&self, i: usize) -> PathBuf {
        let mut v: Vec<PathBuf> = std::fs::read_dir(self.root().join(format!("n{i}/log")))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name().unwrap().to_string_lossy().starts_with("seg-")
                    && p.extension().is_some_and(|x| x == "log")
            })
            .collect();
        v.sort();
        v.pop().expect("an active segment")
    }

    async fn append(&mut self, w: usize, n: u64) {
        let c = self.client();
        let preferred = w % self.n;
        let ws = &mut self.writers[w];
        let mut writer = Writer::new(c, &ws.stream, &ws.name, ws.epoch, preferred, ws.copies);
        writer.next_seq = ws.acked;
        for _ in 0..n {
            let seq = writer.next_seq;
            let p = payload(&ws.name, seq, ws.size);
            writer
                .append(&[(seq, &p[..])])
                .await
                .unwrap_or_else(|e| panic!("append {seq}: {e}"));
            ws.acked = writer.next_seq;
        }
    }
}

/// Segment generation (header offset 16) and the offsets of its valid records.
fn records(seg: &Path) -> (u64, Vec<(u64, usize)>) {
    let data = std::fs::read(seg).unwrap();
    let generation = u64::from_le_bytes(data[16..24].try_into().unwrap());
    let mut off = 4096;
    let mut v = vec![];
    while let record::Decoded::Record { header, next, .. } = record::decode(&data, off, generation)
    {
        v.push((off as u64, header.len as usize));
        off = next;
    }
    (generation, v)
}

fn patch(path: &Path, offset: u64, bytes: &[u8]) {
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(bytes, offset).unwrap();
    f.sync_all().unwrap();
}

// ---------------------------------------------------------------------------

#[given(
    regex = r#"^a (\d+)-node cluster with (\d+) MiB segments(?: and (\d+) MiB retention)?(?:, where stream "([\w/*-]+)" (?:retains (\d+) MiB|keeps (\d+) seconds))?(, where idle streams leave RAM)?(?:, where tenant "(\w+)" holds a token for "([\w/]+)" and peers have their own token)?$"#
)]
#[allow(clippy::too_many_arguments)]
async fn cluster(
    w: &mut Mesh,
    n: usize,
    seg_mb: u64,
    retention_mb: String,
    stream: String,
    stream_mb: String,
    stream_s: String,
    dormant: String,
    tenant: String,
    prefix: String,
) {
    let dir = tempfile::tempdir().unwrap();
    let base = NEXT_PORT.fetch_add(10, Ordering::Relaxed);
    let mut cmd = Command::new(BIN);
    cmd.args(["init", "--nodes", &n.to_string()])
        .args([
            "--base-port",
            &base.to_string(),
            "--segment-mb",
            &seg_mb.to_string(),
        ])
        .arg("--dir")
        .arg(dir.path());
    if !retention_mb.is_empty() {
        cmd.args(["--retention-mb", &retention_mb]);
    }
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "init: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    for i in 0..n {
        // retention runs often enough for a scenario to see it
        let p = dir.path().join(format!("node-{i}.json"));
        let mut v: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        v["retention_interval_ms"] = 200.into();
        if !dormant.is_empty() {
            v["dormant_after_ms"] = 0.into();
        }
        // (scraped by the metrics scenarios; the nodes' own ports are the first of the ten)
        v["listen_metrics"] = format!("127.0.0.1:{}", base + 5 + i as u16).into();
        if let Ok(mb) = stream_mb.parse::<u64>() {
            v["streams"] = serde_json::json!({ stream.clone(): { "retention_bytes": mb << 20 } });
        }
        if let Ok(s) = stream_s.parse::<u64>() {
            v["streams"] = serde_json::json!({ stream.clone(): { "retention_ms": s * 1000 } });
        }
        if !tenant.is_empty() {
            // as an operator writes it: `read` and `write` left out
            v["tokens"] = serde_json::json!([{ "token": format!("tok-{tenant}"), "prefixes": [prefix.clone()] }]);
            v["peer_token"] = "peers-only".into();
        }
        std::fs::write(&p, serde_json::to_vec_pretty(&v).unwrap()).unwrap();
    }
    w.metrics_port = base + 5;
    let cf: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("cluster.json")).unwrap()).unwrap();
    let addrs: Vec<Addr> = cf["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().parse().unwrap())
        .collect();
    let mut mc = Arc::into_inner(
        MeshClient::new(addrs.clone(), cf["token"].as_str().unwrap().into()).unwrap(),
    )
    .unwrap();
    mc.rpc_timeout = Duration::from_millis(1500);
    mc.append_deadline = Duration::from_secs(60);
    if !tenant.is_empty() {
        let t = MeshClient::new(addrs, format!("tok-{tenant}")).unwrap();
        let mut t = Arc::into_inner(t).unwrap();
        (t.rpc_timeout, t.append_deadline) = (mc.rpc_timeout, Duration::from_secs(10));
        w.tenants.insert(tenant.clone(), Arc::new(t));
    }
    w.client = Some(Arc::new(mc));
    w.n = n;
    w.dir = Some(dir);
    *w.procs.lock().unwrap() = (0..n).map(|_| None).collect();
    for i in 0..n {
        w.start(i);
    }
    for i in 0..n {
        w.wait_ready(i).await;
    }
}

#[given(regex = r"^a subscriber on node (\d+)$")]
async fn subscriber(w: &mut Mesh, node: usize) {
    let mut sub = w.client().subscribe(node, Some("s"), false).await.unwrap();
    let log: SubLog = Default::default();
    let stop = Arc::new(AtomicBool::new(false));
    let (l, s) = (log.clone(), stop.clone());
    tokio::spawn(async move {
        while !s.load(Ordering::Acquire) {
            if let Ok(Some(Ok(b))) =
                tokio::time::timeout(Duration::from_millis(200), sub.next()).await
            {
                l.lock()
                    .unwrap()
                    .push((b.id.log.writer.clone(), b.id.first_seq, b.count));
            }
        }
    });
    w.subs.push((log, stop));
}

fn new_writer(w: &mut Mesh, size: usize, copies: u8) -> usize {
    let i = w.writers.len();
    w.writers.push(WriterState {
        stream: "s".into(),
        name: format!("writer-{i}"),
        epoch: 1,
        acked: 0,
        size,
        copies,
    });
    i
}

#[when(
    regex = r#"^a writer appends (\d+) batches of (\d+) bytes(?: to stream "([\w/*-]+)")? with (\d+) cop(?:y|ies)$"#
)]
async fn append(w: &mut Mesh, n: u64, size: usize, stream: String, copies: u8) {
    let i = new_writer(w, size, copies);
    if !stream.is_empty() {
        w.writers[i].stream = stream;
    }
    w.append(i, n).await;
}

#[when(regex = r"^(\d+) writers each append (\d+) batches of (\d+) bytes with (\d+) copies$")]
async fn append_many(w: &mut Mesh, k: usize, n: u64, size: usize, copies: u8) {
    let ids: Vec<usize> = (0..k).map(|_| new_writer(w, size, copies)).collect();
    let mut tasks = vec![];
    for &i in &ids {
        let c = w.client();
        let preferred = i % w.n;
        let (name, epoch) = (w.writers[i].name.clone(), w.writers[i].epoch);
        tasks.push(tokio::spawn(async move {
            let mut writer = Writer::new(c, "s", &name, epoch, preferred, copies);
            for seq in 0..n {
                let p = payload(&name, seq, size);
                writer.append(&[(seq, &p[..])]).await.unwrap();
            }
            n
        }));
    }
    for (t, &i) in tasks.into_iter().zip(&ids) {
        w.writers[i].acked = t.await.unwrap();
    }
}

#[when(regex = r"^(\d+) streams each get a batch of (\d+) bytes with (\d+) copies$")]
async fn many_streams(w: &mut Mesh, k: usize, size: usize, copies: u8) {
    let ids: Vec<usize> = (0..k)
        .map(|j| {
            let i = new_writer(w, size, copies);
            w.writers[i].stream = format!("d{j:05}");
            i
        })
        .collect();
    // 16 at a time
    for chunk in ids.chunks(16) {
        let mut tasks = vec![];
        for &i in chunk {
            let (c, preferred) = (w.client(), i % w.n);
            let ws = &w.writers[i];
            let (stream, name, epoch) = (ws.stream.clone(), ws.name.clone(), ws.epoch);
            tasks.push(tokio::spawn(async move {
                let mut writer = Writer::new(c, &stream, &name, epoch, preferred, copies);
                let p = payload(&name, 0, size);
                writer.append(&[(0, &p[..])]).await.unwrap();
            }));
        }
        for (t, &i) in tasks.into_iter().zip(chunk) {
            t.await.unwrap();
            w.writers[i].acked = 1;
        }
    }
}

#[when(regex = r"^node (\d+) is replaced by an empty one$")]
async fn replace_node(w: &mut Mesh, i: usize) {
    w.kill(i);
    std::fs::remove_dir_all(w.root().join(format!("n{i}"))).unwrap();
    w.start(i);
    w.wait_ready(i).await;
}

#[then(regex = r"^node (\d+) holds at least (\d+) streams only on disk$")]
async fn dormant_streams(w: &mut Mesh, i: usize, n: u64) {
    // the next retention pass puts them out of RAM
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let s = w.status(i).await.unwrap();
        if s.streams_dormant >= n {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {i}: {} dormant, {} in RAM",
            s.streams_dormant,
            s.streams_hot
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[when(regex = r"^the writer appends (\d+) more batches$")]
async fn append_more(w: &mut Mesh, n: u64) {
    let i = w.writers.len() - 1;
    w.append(i, n).await;
}

#[when(
    regex = r"^a writer appends (\d+) batches of (\d+) bytes with (\d+) cop(?:y|ies) while node (\d+) is killed (\d+) times$"
)]
async fn append_with_kills(w: &mut Mesh, n: u64, size: usize, copies: u8, node: usize, kills: u32) {
    let i = new_writer(w, size, copies);
    let stop = Arc::new(AtomicBool::new(false));
    let (procs, root, stop2) = (w.procs.clone(), w.root().to_path_buf(), stop.clone());
    // the killer: SIGKILL, restart, wait, repeat, spread over the run
    let killer = std::thread::spawn(move || {
        for _ in 0..kills {
            std::thread::sleep(Duration::from_millis(300 + rand::random::<u64>() % 700));
            if stop2.load(Ordering::Acquire) {
                break;
            }
            if let Some(mut c) = procs.lock().unwrap()[node].take() {
                let _ = c.kill();
                let _ = c.wait();
            }
            std::thread::sleep(Duration::from_millis(100));
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join(format!("n{node}.log")))
                .unwrap();
            let child = Command::new(BIN)
                .args(["serve", "--config"])
                .arg(root.join(format!("node-{node}.json")))
                .args(["--threads", "2"])
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap();
            procs.lock().unwrap()[node] = Some(child);
        }
    });
    w.append(i, n).await;
    stop.store(true, Ordering::Release);
    killer.join().unwrap();
    w.wait_ready(node).await;
}

#[when(regex = r"^node (\d+) is killed and restarted$")]
async fn kill_restart(w: &mut Mesh, i: usize) {
    w.kill(i);
    w.start(i);
    w.wait_ready(i).await;
}

#[when(regex = r"^node (\d+) is stopped$")]
async fn stop(w: &mut Mesh, i: usize) {
    w.kill(i);
}

#[when(regex = r"^node (\d+) is started$")]
async fn start(w: &mut Mesh, i: usize) {
    w.start(i);
    w.wait_ready(i).await;
}

#[when(
    regex = r"^a torn, unacknowledged write is left after the last record of node (\d+)'s active segment$"
)]
async fn torn_write(w: &mut Mesh, i: usize) {
    let seg = w.active_segment(i);
    let (generation, recs) = records(&seg);
    let (off, len) = *recs.last().unwrap();
    let end = off + len.next_multiple_of(8) as u64;
    // a group that was being written when the node died: header and part of the body only
    let mut buf = vec![];
    let meta = record::Meta {
        key: b"torn".to_vec(),
        first_seq: 9_999,
        count: 1,
        tag: 0,
        time_ms: 0,
    };
    record::encode(&mut buf, generation, end, u64::MAX / 2, &meta, &[7u8; 3000]);
    patch(&seg, end, &buf[..buf.len() / 2]);
}

#[when(regex = r"^an early record of node (\d+)'s active segment is damaged$")]
async fn damage(w: &mut Mesh, i: usize) {
    let seg = w.active_segment(i);
    let (_, recs) = records(&seg);
    let (off, _) = recs[10];
    patch(&seg, off + record::HEADER as u64 + 40, &[0x5A, 0xA5]);
}

#[when(regex = r"^retention has run on (?:node (\d+)|every node)$")]
async fn retention_ran(_w: &mut Mesh, _node: String) {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

#[when(regex = r"^repair has run on every node$")]
async fn repair(w: &mut Mesh) {
    // A peer that was just restarted stays marked down for its backoff, and a repair meanwhile
    // reports it: repair has run once it completes without errors, which must happen within the
    // deadline.
    // A pass that refills a replaced node takes longer than one request: the node goes on
    // with it (repair runs every second); ask again until a pass completes cleanly.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    for i in 0..w.n {
        loop {
            match w.client().control(i, &Control::Repair).await {
                Ok(ControlResp::Repair { errors, .. }) if errors.is_empty() => break,
                Ok(ControlResp::Repair { errors, .. }) => {
                    assert!(std::time::Instant::now() < deadline, "node {i}: {errors:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => assert!(std::time::Instant::now() < deadline, "node {i}: {e}"),
                Ok(r) => panic!("{r:?}"),
            }
        }
    }
}

#[then(regex = r"^every acknowledged batch is readable from node (\d+)$")]
async fn all_readable(w: &mut Mesh, i: usize) {
    for ws in &w.writers {
        let seqs: Vec<u64> = (0..ws.acked).collect();
        let got = w.fetch(i, ws, &seqs).await;
        assert_eq!(
            got.len() as u64,
            ws.acked,
            "{} of {} acknowledged batches on node {i}",
            got.len(),
            ws.acked
        );
        for b in got {
            let e = b.events().next().unwrap();
            assert_eq!(
                &e.payload[..],
                &payload(&ws.name, b.id.first_seq, ws.size)[..],
                "payload of {}",
                b.id.first_seq
            );
        }
    }
}

#[then(regex = r"^every acknowledged batch is held by (?:at least (\d+)|all (\d+)) nodes$")]
async fn held_by(w: &mut Mesh, at_least: String, all: String) {
    let k: usize = if at_least.is_empty() {
        all.parse().unwrap()
    } else {
        at_least.parse().unwrap()
    };
    for ws in &w.writers {
        let seqs: Vec<u64> = (0..ws.acked).collect();
        let mut count = vec![0usize; ws.acked as usize];
        for i in 0..w.n {
            for b in w.fetch(i, ws, &seqs).await {
                count[b.id.first_seq as usize] += 1;
            }
        }
        let short: Vec<usize> = count
            .iter()
            .enumerate()
            .filter(|(_, c)| **c < k)
            .map(|(s, _)| s)
            .take(5)
            .collect();
        assert!(
            short.is_empty(),
            "batches held by fewer than {k} nodes, e.g. seqs {short:?}"
        );
    }
}

#[then(regex = r"^node (\d+) reports at least (\d+) segments$")]
async fn segments_at_least(w: &mut Mesh, i: usize, n: u64) {
    let s = w.status(i).await.unwrap();
    assert!(s.segments >= n, "{} segments", s.segments);
}

#[then(regex = r"^node (\d+) keeps at most (\d+) segments$")]
async fn segments_at_most(w: &mut Mesh, i: usize, n: u64) {
    let s = w.status(i).await.unwrap();
    assert!(s.segments <= n, "{} segments", s.segments);
}

#[then(regex = r"^node (\d+) recovered by scanning at most (\d+) MiB$")]
async fn recovered_scanning(w: &mut Mesh, i: usize, mib: u64) {
    let s = w.status(i).await.unwrap();
    assert!(
        s.recovery_scanned_bytes > 0 && s.recovery_scanned_bytes <= mib << 20,
        "scanned {}",
        s.recovery_scanned_bytes
    );
}

#[then(regex = r"^the newest (\d+) batches are readable from node (\d+)$")]
async fn newest_readable(w: &mut Mesh, n: u64, i: usize) {
    let ws = w.writers.last().unwrap();
    let seqs: Vec<u64> = (ws.acked - n..ws.acked).collect();
    assert_eq!(w.fetch(i, ws, &seqs).await.len() as u64, n);
}

#[then(regex = r"^the oldest batch is gone from node (\d+)$")]
async fn oldest_gone(w: &mut Mesh, i: usize) {
    let ws = w.writers.last().unwrap();
    assert!(w.fetch(i, ws, &[0]).await.is_empty());
}

#[then(regex = r"^node (\d+) holds exactly the writer's (\d+) batches$")]
async fn holds_exactly(w: &mut Mesh, i: usize, n: u64) {
    let s = w.status(i).await.unwrap();
    let ws = w.writers.last().unwrap();
    let f = s
        .logs
        .iter()
        .find(|f| f.log.writer == ws.name)
        .expect("writer's log");
    assert_eq!((f.contiguous, f.high), (n, n));
}

#[then(regex = r#"^node (\d+) refuses to start, saying "([^"]+)"$"#)]
async fn refuses(w: &mut Mesh, i: usize, msg: String) {
    w.start(i);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let st = w.procs.lock().unwrap()[i]
            .as_mut()
            .unwrap()
            .try_wait()
            .unwrap();
        if let Some(st) = st {
            assert!(!st.success(), "node exited successfully");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node {i} started despite the damage"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let log = w.node_log(i).to_lowercase();
    assert!(
        log.contains(&msg.to_lowercase()),
        "log does not mention {msg:?}: {log}"
    );
}

#[then(regex = r"^every subscriber received every acknowledged batch in writer order$")]
async fn subscribers_complete(w: &mut Mesh) {
    let deadline = Instant::now() + Duration::from_secs(30);
    for (log, _) in &w.subs {
        loop {
            let done = {
                let l = log.lock().unwrap();
                w.writers
                    .iter()
                    .all(|ws| l.iter().filter(|x| x.0 == ws.name).count() as u64 == ws.acked)
            };
            if done {
                break;
            }
            assert!(Instant::now() < deadline, "a subscriber is missing batches");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let l = log.lock().unwrap();
        for ws in &w.writers {
            let seqs: Vec<u64> = l.iter().filter(|x| x.0 == ws.name).map(|x| x.1).collect();
            assert_eq!(
                seqs,
                (0..ws.acked).collect::<Vec<_>>(),
                "{}: out of order or duplicated",
                ws.name
            );
        }
    }
}

#[tokio::main]
async fn main() {
    Mesh::cucumber()
        .max_concurrent_scenarios(1)
        .fail_on_skipped()
        .run_and_exit("tests/acceptance/features")
        .await;
}

// ---------------------------------------------------------------------------
// Scoped tokens (features/tokens.feature)

fn tenant_batch(stream: &str, seq: u64, size: usize) -> Bytes {
    let log = LogId {
        stream: stream.into(),
        writer: "tenant-writer".into(),
        epoch: 1,
    };
    Batch::encode(
        &log,
        seq,
        &[(seq, &payload("tenant-writer", seq, size)[..])],
    )
    .unwrap()
}

/// The first sequences of the stream's batches a cursor from its start reads from node `i`,
/// or the node's refusal.
async fn tenant_read(mc: &MeshClient, i: usize, stream: &str) -> anyhow::Result<Vec<u64>> {
    let (mut cursor, mut seqs): (WireCursor, Vec<u64>) = (vec![], vec![]);
    loop {
        let req = Control::ReadCursor {
            stream: stream.into(),
            cursor: cursor.clone(),
            max_bytes: 1 << 20,
        };
        let f = mc
            .call(i, Kind::Control, Bytes::from(serde_json::to_vec(&req)?))
            .await?;
        anyhow::ensure!(f.kind == Kind::Batches, "unexpected {:?}", f.kind);
        if f.body.is_empty() {
            return Ok(seqs);
        }
        let mut b = &f.body[..];
        while b.len() >= 4 {
            let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
            let x = Batch::parse(f.body.slice_ref(&b[4..4 + n]))?;
            b = &b[4 + n..];
            seqs.push(x.id.first_seq);
            cursor = vec![(x.id.log.writer.clone(), x.id.log.epoch, x.end_seq())];
        }
    }
}

#[when(
    regex = r#"^the tenant "(\w+)" appends (\d+) batches of (\d+) bytes to stream "([\w/-]+)" with (\d+) copies$"#
)]
async fn tenant_appends(
    w: &mut Mesh,
    tenant: String,
    n: u64,
    size: usize,
    stream: String,
    copies: u8,
) {
    let mc = w.tenants[&tenant].clone();
    for seq in 0..n {
        let r = mc
            .append(
                seq as usize % w.n,
                &tenant_batch(&stream, seq, size),
                copies,
            )
            .await;
        r.unwrap_or_else(|e| panic!("append {seq}: {e}"));
    }
}

#[then(regex = r#"^the tenant "(\w+)" reads (\d+) batches of stream "([\w/-]+)" from every node$"#)]
async fn tenant_reads(w: &mut Mesh, tenant: String, n: u64, stream: String) {
    let mc = w.tenants[&tenant].clone();
    for i in 0..w.n {
        // (a node just restarted takes what it missed from its peers first)
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got = tenant_read(&mc, i, &stream).await;
            if got.as_ref().is_ok_and(|s| *s == (0..n).collect::<Vec<_>>()) {
                break;
            }
            assert!(Instant::now() < deadline, "node {i}: {got:?}");
            let _ = w.client().control(i, &Control::Repair).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[then(
    regex = r#"^the tenant "(\w+)" is refused an append to stream "([\w/-]+)" and nothing is stored$"#
)]
async fn tenant_refused_append(w: &mut Mesh, tenant: String, stream: String) {
    let mc = w.tenants[&tenant].clone();
    for i in 0..w.n {
        let e = mc.append(i, &tenant_batch(&stream, 0, 100), 1).await;
        let e = e.expect_err("refused").to_string();
        assert!(e.contains("refused"), "node {i}: {e}");
        // (the cluster's own token reads every stream)
        let held = tenant_read(&w.client(), i, &stream).await.unwrap();
        assert!(held.is_empty(), "node {i} stored {held:?}");
    }
}

#[then(regex = r#"^the tenant "(\w+)" is refused a read and a live feed of stream "([\w/-]+)"$"#)]
async fn tenant_refused_stream(w: &mut Mesh, tenant: String, stream: String) {
    let mc = w.tenants[&tenant].clone();
    for i in 0..w.n {
        let e = tenant_read(&mc, i, &stream).await.expect_err("refused");
        assert!(e.to_string().contains("forbidden"), "node {i}: {e}");
        assert!(
            mc.subscribe(i, Some(&stream), true).await.is_err(),
            "node {i}"
        );
    }
}

#[then(
    regex = r#"^the tenant "(\w+)" is refused the status of every node and a live feed of every stream$"#
)]
async fn tenant_refused_node(w: &mut Mesh, tenant: String) {
    let mc = w.tenants[&tenant].clone();
    for i in 0..w.n {
        let e = mc.control(i, &Control::Status).await.expect_err("refused");
        assert!(e.to_string().contains("forbidden"), "node {i}: {e}");
        assert!(mc.subscribe(i, None, true).await.is_err(), "node {i}");
    }
}

#[then(regex = r#"^no client token makes a peer of node (\d+)$"#)]
async fn no_client_peer(w: &mut Mesh, node: usize) {
    let cf: serde_json::Value =
        serde_json::from_slice(&std::fs::read(w.root().join("cluster.json")).unwrap()).unwrap();
    let addr: Addr = cf["nodes"][node].as_str().unwrap().parse().unwrap();
    let shared = cf["token"].as_str().unwrap().to_string();
    let t = Duration::from_secs(2);
    let mut tokens: Vec<(String, bool)> = vec![(shared, false), ("peers-only".into(), true)];
    tokens.extend(w.tenants.keys().map(|k| (format!("tok-{k}"), false)));
    for (token, ok) in tokens {
        let conn = precog_mesh::transport::connect(&addr, t).await.unwrap();
        let hello = Hello {
            token: token.clone(),
            node_id: Some(((node + 1) % w.n) as u32),
        };
        let r = conn
            .call(Kind::Hello, serde_json::to_vec(&hello).unwrap().into(), t)
            .await;
        assert_eq!(r.is_ok(), ok, "{token} as a peer");
    }
}

// ---------------------------------------------------------------------------
// Stream delete (features/delete.feature)

/// A delete through node `i`, asked again until it is acknowledged, as a caller does.
async fn delete_through(mc: &MeshClient, i: usize, stream: &str) -> anyhow::Result<u64> {
    let c = Control::Delete {
        stream: stream.into(),
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match mc.control(i, &c).await {
            Ok(r) => return r.deleted().ok_or_else(|| anyhow::anyhow!("{r:?}")),
            Err(e) if e.to_string().contains("forbidden") => return Err(e),
            Err(e) if Instant::now() > deadline => return Err(e),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
}

#[when(regex = r#"^stream "([\w/-]+)" is deleted through node (\d+)$"#)]
async fn delete_stream(w: &mut Mesh, stream: String, node: usize) {
    let floor = delete_through(&w.client(), node, &stream).await.unwrap();
    w.floors.insert(stream, floor);
}

#[when(regex = r#"^the tenant "(\w+)" deletes stream "([\w/-]+)" through node (\d+)$"#)]
async fn tenant_deletes(w: &mut Mesh, tenant: String, stream: String, node: usize) {
    let floor = delete_through(&w.tenants[&tenant], node, &stream)
        .await
        .unwrap();
    w.floors.insert(stream, floor);
}

#[then(regex = r#"^the tenant "(\w+)" is refused a delete of stream "([\w/-]+)"$"#)]
async fn tenant_refused_delete(w: &mut Mesh, tenant: String, stream: String) {
    for i in 0..w.n {
        let e = delete_through(&w.tenants[&tenant], i, &stream).await;
        let e = e.expect_err("refused").to_string();
        assert!(e.contains("forbidden"), "node {i}: {e}");
    }
}

#[then(regex = r#"^stream "([\w/-]+)" holds no batch on (?:node (\d+)|any node)$"#)]
async fn holds_nothing(w: &mut Mesh, stream: String, node: String) {
    let nodes: Vec<usize> = match node.parse() {
        Ok(i) => vec![i],
        Err(_) => (0..w.n).collect(),
    };
    for i in nodes {
        let held = tenant_read(&w.client(), i, &stream).await.unwrap();
        assert!(held.is_empty(), "node {i} serves {held:?}");
        // nor by identity: what a repairing peer would ask for
        for ws in w.writers.iter().filter(|x| x.stream == stream) {
            let seqs: Vec<u64> = (0..ws.acked).collect();
            assert!(w.fetch(i, ws, &seqs).await.is_empty(), "node {i}");
        }
    }
}

#[then(regex = r#"^node (\d+) still serves (\d+) batches of stream "([\w/-]+)"$"#)]
async fn still_serves(w: &mut Mesh, node: usize, n: usize, stream: String) {
    let held = tenant_read(&w.client(), node, &stream).await.unwrap();
    assert_eq!(held.len(), n, "node {node}");
}

#[then(
    regex = r#"^a retry of a batch of the deleted stream "([\w/-]+)" is refused by every node$"#
)]
async fn retry_refused(w: &mut Mesh, stream: String) {
    let ws = w.writers.iter().find(|x| x.stream == stream).unwrap();
    let log = LogId {
        stream: ws.stream.clone(),
        writer: ws.name.clone(),
        epoch: ws.epoch,
    };
    // what the writer sent first, byte for byte, and what it would send next
    for seq in [0, ws.acked] {
        let b = Batch::encode(&log, seq, &[(seq, &payload(&ws.name, seq, ws.size)[..])]).unwrap();
        for i in 0..w.n {
            let e = w.client().append(i, &b, 1).await;
            assert!(e.is_err(), "node {i} took sequence {seq}");
        }
    }
}

#[then(regex = r#"^a client's own delete record for stream "([\w/-]+)" is refused by every node$"#)]
async fn forged_delete_refused(w: &mut Mesh, stream: String) {
    // as a node writes a delete (the floors' stream), with the highest floor there is
    let log = LogId {
        stream: precog_mesh::store::FLOORS.into(),
        writer: "client".into(),
        epoch: 1,
    };
    let e = precog_mesh::store::encode_floor(&stream, u64::MAX);
    let b = Batch::encode(&log, 0, &[(0, &e[..])]).unwrap();
    for i in 0..w.n {
        let r = w.client().append(i, &b, 1).await;
        assert!(r.is_err(), "node {i} took a client's delete record: {r:?}");
    }
}

#[when(
    regex = r#"^a writer that starts after the delete appends (\d+) batches of (\d+) bytes to stream "([\w/-]+)" with (\d+) copies$"#
)]
async fn append_after_delete(w: &mut Mesh, n: u64, size: usize, stream: String, copies: u8) {
    let i = new_writer(w, size, copies);
    // an epoch at the delete's floor: the lowest a writer of the new stream can have
    (w.writers[i].epoch, w.writers[i].stream) = (w.floors[&stream], stream);
    w.append(i, n).await;
}

#[then(
    regex = r#"^every node serves exactly the new writer's (\d+) batches of stream "([\w/-]+)"$"#
)]
async fn serves_new(w: &mut Mesh, n: u64, stream: String) {
    for i in 0..w.n {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let got = tenant_read(&w.client(), i, &stream).await.unwrap();
            if got == (0..n).collect::<Vec<_>>() {
                break;
            }
            assert!(Instant::now() < deadline, "node {i}: {got:?}");
            let _ = w.client().control(i, &Control::Repair).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[then(regex = r#"^a cursor reads every batch of stream "([\w/*-]+)" from every node$"#)]
async fn cursor_reads_all(w: &mut Mesh, stream: String) {
    let ws = w.writers.iter().find(|x| x.stream == stream).unwrap();
    for i in 0..w.n {
        let mut cursor: WireCursor = vec![];
        let mut seqs = vec![];
        loop {
            let req = Control::ReadCursor {
                stream: stream.clone(),
                cursor: cursor.clone(),
                max_bytes: 1 << 20,
            };
            let body = Bytes::from(serde_json::to_vec(&req).unwrap());
            let f = w.client().call(i, Kind::Control, body).await.unwrap();
            assert_eq!(f.kind, Kind::Batches);
            if f.body.is_empty() {
                break;
            }
            let mut b = &f.body[..];
            while b.len() >= 4 {
                let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                let x = Batch::parse(f.body.slice_ref(&b[4..4 + n])).unwrap();
                b = &b[4 + n..];
                let e = x.events().next().unwrap();
                assert_eq!(
                    &e.payload[..],
                    &payload(&ws.name, x.id.first_seq, ws.size)[..]
                );
                seqs.push(x.id.first_seq);
                cursor = vec![(x.id.log.writer.clone(), x.id.log.epoch, x.end_seq())];
            }
        }
        assert_eq!(seqs, (0..ws.acked).collect::<Vec<_>>(), "node {i}");
    }
}

#[then(regex = r#"^the oldest batch of stream "([\w/*-]+)" is gone from every node$"#)]
async fn oldest_gone_everywhere(w: &mut Mesh, stream: String) {
    let ws = w.writers.iter().find(|x| x.stream == stream).unwrap();
    for i in 0..w.n {
        assert!(w.fetch(i, ws, &[0]).await.is_empty(), "node {i}");
    }
}

// ---------------------------------------------------------------------------
// Metrics and readers' positions (features/metrics.feature)

/// A node's `/metrics`, as a scraper reads it.
async fn scrape(w: &Mesh, node: usize) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("127.0.0.1:{}", w.metrics_port + node as u16);
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    s.write_all(b"GET /metrics HTTP/1.0\r\n\r\n").await.unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).await.unwrap();
    body
}

/// The series of a scrape: its lines that are not comments or headers.
fn series_of(m: &str) -> Vec<&str> {
    m.lines().filter(|l| l.starts_with("precog_")).collect()
}

#[when(
    regex = r#"^(\d+)(?: more)? sessions of (\d+) batches are written under "([\w/]+)" and a reader group commits a position in each through node (\d+)$"#
)]
async fn sessions(w: &mut Mesh, n: usize, batches: u64, prefix: String, node: usize) {
    for _ in 0..n {
        let i = new_writer(w, 500, 2);
        w.writers[i].stream = format!("{prefix}session-{i}");
        w.append(i, batches).await;
        let ws = &w.writers[i];
        let c = Control::Commit {
            group: "ui".into(),
            stream: ws.stream.clone(),
            cursor: vec![(ws.name.clone(), ws.epoch, ws.acked)],
        };
        w.client().control(node, &c).await.unwrap();
    }
}

#[then(regex = r#"^node (\d+)'s metrics count (\d+) positions under class "([\w/*-]+)"$"#)]
async fn class_positions(w: &mut Mesh, node: usize, n: usize, class: String) {
    let m = scrape(w, node).await;
    let line = format!("precog_class_positions{{stream=\"{class}\"}} {n}");
    assert!(
        series_of(&m).contains(&line.as_str()),
        "no {line:?} in\n{m}"
    );
    // and no series of a session's own
    let own: Vec<&str> = (series_of(&m).into_iter())
        .filter(|l| l.contains("session-"))
        .collect();
    assert!(own.is_empty(), "{own:?}");
}

#[then(regex = r"^the number of series in node (\d+)'s metrics is noted$")]
async fn note_series(w: &mut Mesh, node: usize) {
    w.noted = series_of(&scrape(w, node).await).len();
}

#[then(regex = r"^node (\d+)'s metrics have as many series as noted$")]
async fn same_series(w: &mut Mesh, node: usize) {
    let m = scrape(w, node).await;
    assert_eq!(series_of(&m).len(), w.noted, "{m}");
}

#[then(
    regex = r#"^node (\d+)'s metrics count the tenant's appends under "([\w/]+)" and name no token$"#
)]
async fn token_series(w: &mut Mesh, node: usize, prefix: String) {
    let m = scrape(w, node).await;
    let head = format!("precog_token_appends_total{{token=\"{prefix}\"}} ");
    let n: u64 = (series_of(&m).iter())
        .find_map(|l| l.strip_prefix(head.as_str()))
        .unwrap_or_else(|| panic!("no {head:?} in\n{m}"))
        .parse()
        .unwrap();
    assert!(n > 0, "{m}");
    for secret in ["tok-", "peers-only"] {
        assert!(!m.contains(secret), "a token in\n{m}");
    }
}

#[then(
    regex = r#"^within (\d+) seconds no node holds a position of the reader group in a stream under "([\w/]+)"$"#
)]
async fn positions_expire(w: &mut Mesh, secs: u64, prefix: String) {
    let streams: Vec<String> = (w.writers.iter())
        .filter(|x| x.stream.starts_with(&prefix))
        .map(|x| x.stream.clone())
        .collect();
    assert!(!streams.is_empty());
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let mut held = vec![];
        for i in 0..w.n {
            for stream in &streams {
                let q = Control::Position {
                    group: "ui".into(),
                    stream: stream.clone(),
                };
                match w.client().control(i, &q).await {
                    Ok(ControlResp::Cursor { cursor: None }) => {}
                    r => held.push((i, stream.clone(), format!("{r:?}"))),
                }
            }
        }
        if held.is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "still held: {held:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
