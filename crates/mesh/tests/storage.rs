//! The node on top of precog-storage: payloads come from disk, retention trims both the
//! segment log and the node's metadata, and a storage fault fences the node.

use bytes::Bytes;
use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::*;
use std::sync::Arc;

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut c = NodeConfig::single(0, "a", dir.to_path_buf(), "tok".into());
    c.segment_size = 64 * 1024;
    c
}

fn batch(writer: &str, seq: u64, size: usize) -> Bytes {
    let log = LogId {
        stream: "s".into(),
        writer: writer.into(),
        epoch: 1,
    };
    let p = vec![(seq % 251) as u8; size];
    Batch::encode(&log, seq, &[(seq, &p[..])]).unwrap()
}

async fn append_n(node: &Arc<Node>, writer: &str, n: u64, size: usize) -> Vec<Bytes> {
    let mut v = vec![];
    for seq in 0..n {
        let b = batch(writer, seq, size);
        let ack = node.append(1, b.clone()).await;
        assert!(ack.is_success(), "seq {seq}: {ack:?}");
        v.push(b);
    }
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_are_served_from_disk_after_restart() {
    let d = tempfile::tempdir().unwrap();
    let written = {
        let node = Node::open(config(d.path())).await.unwrap();
        append_n(&node, "w", 120, 1500).await
    };
    let node = Node::open(config(d.path())).await.unwrap();
    assert_eq!(node.store.totals().0, 120);
    for (seq, bytes) in written.iter().enumerate() {
        let id = BatchId {
            log: LogId {
                stream: "s".into(),
                writer: "w".into(),
                epoch: 1,
            },
            first_seq: seq as u64,
        };
        let got = node
            .store
            .get(&id)
            .unwrap_or_else(|| panic!("batch {seq} missing"));
        assert_eq!(&got.bytes, bytes, "batch {seq}");
    }
    // a retry of an old batch is still recognised as a duplicate after restart
    let ack = node.append(1, written[3].clone()).await;
    assert_eq!(ack.status, AckStatus::Ok);
    assert_eq!(node.store.totals().0, 120);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_retaining_many_small_batches_restarts_and_serves_them_from_disk() {
    // Why: a node keeps only the active segment's batches in RAM; the rest are read from the
    // segments' index files. After a restart, every retained batch must still be served: by id,
    // to a cursor reading the stream from its start, and to repair from any sequence.
    let d = tempfile::tempdir().unwrap();
    let writers = ["w1", "w2", "w3"];
    let mut written = vec![];
    {
        let node = Node::open(config(d.path())).await.unwrap();
        for seq in 0..500 {
            for w in writers {
                let b = batch(w, seq, 60);
                assert!(node.append(1, b.clone()).await.is_success());
                written.push(b);
            }
        }
        node.apply_retention().await.unwrap(); // moves sealed segments' batches out of RAM
        assert_eq!(node.store.totals().0, 1500);
    }
    let node = Node::open(config(d.path())).await.unwrap();
    assert_eq!(node.store.totals().0, 1500);
    for b in &written {
        let b = Batch::parse(b.clone()).unwrap();
        let got = node.store.get(&b.id).expect("served after restart");
        assert_eq!(got.bytes, b.bytes);
    }
    // a consumer reading from the start gets every batch once, each writer's in order
    let mut cursor = precog_mesh::store::Cursor::new();
    let mut got = vec![];
    loop {
        let (bs, next) = node.store.read_cursor("s", &cursor, 16 << 10);
        if bs.is_empty() {
            break;
        }
        got.extend(bs.into_iter().map(|b| Batch::parse(b).unwrap().id));
        cursor = next;
    }
    assert_eq!(got.len(), 1500);
    for w in writers {
        let seqs: Vec<u64> = got
            .iter()
            .filter(|id| id.log.writer == w)
            .map(|id| id.first_seq)
            .collect();
        assert_eq!(seqs, (0..500).collect::<Vec<_>>(), "{w}");
    }
    let log = LogId {
        stream: "s".into(),
        writer: "w2".into(),
        epoch: 1,
    };
    let repair: Vec<u64> = node
        .store
        .log_batches(&log, 123, 1 << 20)
        .into_iter()
        .map(|b| Batch::parse(b).unwrap().id.first_seq)
        .collect();
    assert_eq!(repair, (123..500).collect::<Vec<_>>());
    let f = node.store.frontiers(None);
    assert!(
        f.iter().all(|f| (f.contiguous, f.high) == (500, 500)),
        "{f:?}"
    );
    // an old retry is still a duplicate, not a new batch
    let ack = node.append(1, written[7].clone()).await;
    assert_eq!(ack.status, AckStatus::Ok);
    assert_eq!(node.store.totals().0, 1500);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_trims_the_log_and_the_metadata() {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.retention_bytes = Some(3 * 64 * 1024);
    let node = Node::open(c).await.unwrap();
    append_n(&node, "w", 400, 1000).await;
    node.apply_retention().await.unwrap();
    let (batches, _, _) = node.store.totals();
    assert!(batches < 400 && batches > 50, "kept {batches}");
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let served = node.store.log_batches(&log, 0, 1 << 20);
    assert!(
        Batch::parse(served[0].clone()).unwrap().id.first_seq > 0,
        "repair serves after the trimmed prefix"
    );
    let id = |seq| BatchId {
        log: LogId {
            stream: "s".into(),
            writer: "w".into(),
            epoch: 1,
        },
        first_seq: seq,
    };
    assert!(node.store.get(&id(0)).is_none());
    assert!(node.store.get(&id(399)).is_some());
    let f = node.store.frontiers(None);
    assert_eq!(f[0].high, 400);
    assert!(!node.is_fatal(), "retention does not fence a healthy node");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn age_retention_drops_sealed_segments_older_than_the_limit() {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.retention_ms = Some(60_000);
    let node = Node::open(c.clone()).await.unwrap();
    append_n(&node, "w", 200, 1000).await;
    node.apply_retention().await.unwrap();
    assert_eq!(node.store.totals().0, 200, "nothing is a minute old yet");
    drop(node);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    c.retention_ms = Some(10);
    let node = Node::open(c).await.unwrap();
    node.apply_retention().await.unwrap();
    let kept = node.store.totals().0;
    assert!(
        kept > 0 && kept < 200,
        "kept {kept}: only the active segment stays"
    );
}

#[test]
fn omitted_config_fields_take_production_defaults() {
    let c: NodeConfig = serde_json::from_str(
        r#"{"id":0,"domain":"a","data_dir":"/d","token":"t","listen_tcp":null,
            "peers":[]}"#,
    )
    .unwrap();
    assert_eq!(c.segment_size, 1 << 30);
    assert_eq!(c.tail_cache_bytes, 64 << 20);
    assert_eq!(c.retention_interval_ms, 10_000);
    assert_eq!((c.retention_bytes, c.retention_ms), (None, None));
    // write caching is opt-in; once on, it flushes at Redpanda's 256 KiB of unsynced data
    assert_eq!((c.flush_delay_ms, c.flush_bytes), (0, 256 << 10));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_storage_fault_fences_the_node() {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.storage_failpoints.fail_data_sync_after = Some(5);
    let node = Node::open(c).await.unwrap();
    append_n(&node, "w", 5, 100).await;
    assert!(!node.is_fatal());
    let ack = node.append(1, batch("w", 5, 100)).await;
    assert!(!ack.is_success(), "{ack:?}");
    assert!(node.is_fatal());
    let ack = node.append(1, batch("other", 0, 100)).await;
    assert!(!ack.is_success(), "a fenced node acknowledges nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reports_recovery_and_segments() {
    let d = tempfile::tempdir().unwrap();
    {
        let node = Node::open(config(d.path())).await.unwrap();
        append_n(&node, "w", 120, 1500).await;
    }
    let node = Node::open(config(d.path())).await.unwrap();
    let s = node.status();
    assert_eq!(s.batches, 120);
    assert!(s.segments >= 3, "{s:?}");
    assert!(
        s.recovery_scanned_bytes > 0 && s.recovery_scanned_bytes <= 64 * 1024,
        "{s:?}"
    );
    assert!(s.fsyncs == 0, "fsyncs count this process's group commits");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_declared_stream_keeps_its_own_retention() {
    // As topicsync declares retention.bytes / retention.ms per topic: one stream trims to its
    // budget, another to its age, an undeclared one keeps everything (node budget only).
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    let one = batch("w", 0, 1000).len() as u64;
    c.streams.insert(
        "small".into(),
        precog_mesh::node::StreamConfig {
            retention_bytes: Some(5 * one),
            ..Default::default()
        },
    );
    c.streams.insert(
        "young".into(),
        precog_mesh::node::StreamConfig {
            retention_ms: Some(0),
            ..Default::default()
        },
    );
    let node = Node::open(c).await.unwrap();
    for stream in ["small", "young", "other"] {
        let log = LogId {
            stream: stream.into(),
            writer: "w".into(),
            epoch: 1,
        };
        for seq in 0..20u64 {
            let b = Batch::encode(&log, seq, &[(seq, &[1u8; 1000 - 40][..])]).unwrap();
            assert!(node.append(1, b).await.is_success());
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    node.apply_retention().await.unwrap();
    let count = |s: &str| node.store.read(s, 0, 1000).0.len();
    assert!((4..=5).contains(&count("small")), "{}", count("small"));
    assert_eq!(count("young"), 0);
    assert_eq!(count("other"), 20);
}

#[test]
fn one_cluster_config_serves_every_statefulset_member() {
    // a ConfigMap lists every node; each pod takes its own id (from its hostname's ordinal)
    let mut c = NodeConfig::single(0, "unused", "/var/lib/precog".into(), "tok".into());
    c.peers = (0..3)
        .map(|i| precog_mesh::node::PeerConfig {
            id: i,
            domain: format!("zone-{i}"),
            addr: format!("tcp://precog-{i}.precog:7000"),
        })
        .collect();
    let m = c.clone().for_member(1).unwrap();
    assert_eq!((m.id, m.domain.as_str()), (1, "zone-1"));
    assert_eq!(m.peers.iter().map(|p| p.id).collect::<Vec<_>>(), [0, 2]);
    assert!(c.clone().for_member(7).is_err(), "not a member");
    assert_eq!(precog_mesh::node::ordinal("precog-12").unwrap(), 12);
    assert!(precog_mesh::node::ordinal("precog").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_caching_acks_before_the_fsync_and_flushes_an_idle_node_within_the_delay() {
    // Why: live-ingestion's Redpanda runs write caching (ack after replication, fsync on a 100 ms /
    // 256 KiB cadence); Precog configured the same way must ack without an fsync per batch, and
    // still sync a quiet node's last batches within the delay, not only when more arrive.
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.segment_size = 1 << 20;
    c.flush_delay_ms = 100;
    c.flush_bytes = 1 << 20;
    let node = Node::open(c).await.unwrap();
    let t = std::time::Instant::now();
    append_n(&node, "w", 50, 100).await;
    assert_eq!(
        node.status().fsyncs,
        0,
        "50 batches acknowledged without an fsync"
    );
    let deadline = t + std::time::Duration::from_secs(2);
    while node.status().fsyncs == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "an idle node never flushed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(t.elapsed() >= std::time::Duration::from_millis(100));
}

#[test]
fn under_write_caching_an_append_is_written_by_its_caller_not_the_wal_thread() {
    // Why: handing each batch to the WAL thread and back (a futex wake, its sleep, its wake of the
    // worker) was ~10% of a node's CPU under write caching, where nothing in an append waits for
    // the disk. The caller writes it and has the LSN as soon as `submit` returns.
    use precog_mesh::wal::Wal;
    let d = tempfile::tempdir().unwrap();
    let mut c = precog_storage::Config::new(d.path());
    c.segment_size = 1 << 20;
    c.flush_delay_ms = 100;
    let wal = Wal::open(c).unwrap();
    for seq in 0..100 {
        let mut rx = wal
            .submit(Batch::parse(batch("w", seq, 100)).unwrap())
            .unwrap();
        assert_eq!(
            rx.try_recv().unwrap().unwrap(),
            seq,
            "written when submit returns"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn under_write_caching_past_flush_bytes_the_fsync_runs_without_waiting_out_the_delay() {
    // Why: flush_bytes bounds how much an acknowledged but unsynced tail can hold; appends no longer
    // fsync themselves, so crossing it must wake the flusher instead of waiting for the delay.
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.segment_size = 1 << 20;
    c.flush_delay_ms = 60_000;
    c.flush_bytes = 4 << 10;
    let node = Node::open(c).await.unwrap();
    append_n(&node, "w", 1, 5000).await; // the first and only append crosses it
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while node.status().fsyncs == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "no fsync past flush_bytes"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn under_write_caching_a_failed_deferred_fsync_fences_the_node() {
    // Why: the acks given before it rest on the replicas; once the node knows its disk lost data it
    // must acknowledge nothing more until it restarts and recovers.
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.segment_size = 1 << 20;
    c.flush_delay_ms = 20;
    c.storage_failpoints.fail_data_sync_after = Some(0);
    let node = Node::open(c).await.unwrap();
    append_n(&node, "w", 3, 100).await; // acknowledged before the fsync
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !node.is_fatal() {
        assert!(std::time::Instant::now() < deadline, "never fenced");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let ack = node.append(1, batch("other", 0, 100)).await;
    assert!(!ack.is_success(), "a fenced node acknowledges nothing");
}

#[test]
fn without_write_caching_batches_queued_during_an_fsync_share_the_next_one() {
    // Why: group commit: fsyncs per second are bounded by the disk, not by the request rate, so the
    // per-ack fsync stays on the WAL thread, not on each caller.
    use precog_mesh::wal::Wal;
    let d = tempfile::tempdir().unwrap();
    let mut c = precog_storage::Config::new(d.path());
    c.segment_size = 1 << 20;
    let wal = Wal::open(c).unwrap();
    let rxs: Vec<_> = (0..200)
        .map(|seq| {
            wal.submit(Batch::parse(batch("w", seq, 100)).unwrap())
                .unwrap()
        })
        .collect();
    for (seq, rx) in rxs.into_iter().enumerate() {
        assert_eq!(rx.blocking_recv().unwrap().unwrap(), seq as u64);
    }
    let syncs = wal.stats.syncs.load(std::sync::atomic::Ordering::Relaxed);
    assert!(syncs < 100, "{syncs} fsyncs for 200 batches");
}

fn on(stream: &str, seq: u64, size: usize) -> Bytes {
    let log = LogId {
        stream: stream.into(),
        writer: "w".into(),
        epoch: 1,
    };
    let p = vec![(seq % 251) as u8; size];
    Batch::encode(&log, seq, &[(seq, &p[..])]).unwrap()
}

fn id(stream: &str, seq: u64) -> BatchId {
    BatchId {
        log: LogId {
            stream: stream.into(),
            writer: "w".into(),
            epoch: 1,
        },
        first_seq: seq,
    }
}

/// A counter or gauge of `node.metrics()` (summed over its labels).
fn metric(node: &Node, name: &str) -> u64 {
    node.metrics()
        .lines()
        .filter(|l| l.split([' ', '{']).next() == Some(name))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// Every batch of the stream a cursor from its start reads, by sequence.
fn cursor_seqs(node: &Node, stream: &str) -> Vec<u64> {
    let mut c = precog_mesh::store::Cursor::new();
    let mut seqs = vec![];
    loop {
        let (bs, next) = node.store.read_cursor(stream, &c, 1 << 20);
        if bs.is_empty() {
            return seqs;
        }
        seqs.extend(
            bs.into_iter()
                .map(|b| Batch::parse(b).unwrap().id.first_seq),
        );
        c = next;
    }
}

fn stream_cfg(retention_bytes: u64) -> precog_mesh::node::StreamConfig {
    precog_mesh::node::StreamConfig {
        retention_bytes: Some(retention_bytes),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_low_volume_stream_keeps_its_own_retention_while_the_node_cap_turns_over() {
    // Why: every stream shares one segment log, and the node cap drops its oldest segments. A
    // stream far below its own budget keeps its history, as a Redpanda topic keeps its own
    // retention; it must not lose it at the node's time horizon (cap / ingest rate).
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    let cap = 4 * 64 * 1024;
    c.retention_bytes = Some(cap);
    let one = on("low", 0, 1000).len() as u64;
    c.streams.insert("low".into(), stream_cfg(30 * one));
    // declared without a retention of its own, or compacted (its snapshot keeps it): not copied
    c.streams.insert("bare".into(), Default::default());
    c.streams.insert(
        "kv".into(),
        precog_mesh::node::StreamConfig {
            retention_bytes: Some(1 << 30),
            compacted: true,
            ..Default::default()
        },
    );
    let node = Node::open(c.clone()).await.unwrap();
    assert!(node.append(1, on("bare", 0, 1000)).await.is_success());
    let kv = LogId {
        stream: "kv".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let b = Batch::encode_keyed(&kv, 0, &[(0, b"k", b"v")]).unwrap();
    assert!(node.append(1, b).await.is_success());
    let mut low = vec![];
    for seq in 0..20 {
        let b = on("low", seq, 1000);
        assert!(node.append(1, b.clone()).await.is_success());
        low.push(b);
    }
    // the high-volume stream has no retention of its own: the node cap bounds it
    for round in 0..20u64 {
        for i in 0..100 {
            let seq = round * 100 + i;
            assert!(node.append(1, on("high", seq, 1000)).await.is_success());
        }
        node.apply_retention().await.unwrap();
        let bytes = metric(&node, "precog_log_bytes");
        assert!(
            bytes > 0 && bytes <= cap,
            "round {round}: the cap holds with the copies ({bytes})"
        );
    }
    assert!(node.store.get(&id("high", 0)).is_none(), "high turned over");
    assert!(
        node.store.get(&id("bare", 0)).is_none(),
        "no retention of its own: not kept"
    );
    assert!(
        node.store.read("kv", 0, 10).0.is_empty(),
        "compacted: not copied"
    );
    assert_eq!(
        node.latest("kv"),
        vec![(b"k".to_vec(), Bytes::from("v"))],
        "its snapshot keeps it"
    );
    assert!(node.store.get(&id("high", 1999)).is_some());
    let check = |node: &Node, when: &str| {
        for (seq, b) in low.iter().enumerate() {
            let got = node.store.get(&id("low", seq as u64));
            assert_eq!(got.map(|x| x.bytes), Some(b.clone()), "{when}: low {seq}");
        }
        assert_eq!(
            cursor_seqs(node, "low"),
            (0..20).collect::<Vec<_>>(),
            "{when}"
        );
        assert_eq!(
            node.store.read("low", 0, 1000).0.len(),
            20,
            "{when}: once each"
        );
    };
    check(&node, "after the turnover");
    let copied = metric(&node, "precog_retention_copied_batches_total");
    assert!(
        copied >= 20 * 4,
        "low was copied on every reclaim: {copied}"
    );
    assert!(metric(&node, "precog_retention_copied_bytes_total") >= copied * one);
    assert_eq!(metric(&node, "precog_retention_forced_total"), 0);
    drop(node);
    let node = Node::open(c).await.unwrap();
    check(&node, "after a restart");
    node.apply_retention().await.unwrap();
    check(&node, "after a restart and retention");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn when_streams_retain_more_than_the_cap_the_one_furthest_over_its_share_is_cut() {
    // Why: the node cap protects the disk, so it wins over what streams retain. The cut takes
    // the oldest batches of the stream holding most beyond its share (here half the cap each),
    // never from a stream within its share, and the metric names whom it cut.
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    let cap = 4 * 64 * 1024;
    c.retention_bytes = Some(cap);
    for s in ["big", "small"] {
        c.streams.insert(s.into(), stream_cfg(1 << 30));
    }
    let node = Node::open(c).await.unwrap();
    for seq in 0..20 {
        assert!(node.append(1, on("small", seq, 1000)).await.is_success());
    }
    for seq in 0..400 {
        assert!(node.append(1, on("big", seq, 1000)).await.is_success());
        if seq % 50 == 49 {
            node.apply_retention().await.unwrap();
            let bytes = metric(&node, "precog_log_bytes");
            assert!(bytes > 0 && bytes <= cap, "at big {seq}: {bytes}");
        }
    }
    assert_eq!(cursor_seqs(&node, "small"), (0..20).collect::<Vec<_>>());
    let big = cursor_seqs(&node, "big");
    let kept = big[0]..400;
    assert!(big[0] > 0, "its oldest went");
    assert!(
        big.len() > 150,
        "no more than the cap needs: kept {}",
        big.len()
    );
    assert_eq!(big, kept.collect::<Vec<_>>(), "only its oldest: no hole");
    let m = node.metrics();
    let forced = |s: &str| {
        m.lines()
            .find(|l| l.starts_with(&format!("precog_retention_forced_total{{stream=\"{s}\"}}")))
            .and_then(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
            .unwrap_or(0)
    };
    assert!(forced("big") >= big[0], "{m}");
    assert_eq!(forced("small"), 0, "{m}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_anywhere_between_copy_and_delete_keeps_every_retained_batch_once() {
    // Why: the cleaner writes the copies, makes them durable, then deletes the old segment. A
    // crash at any point must leave the old copy or the new one, never neither, and recovery
    // must keep each batch exactly once by its identity.
    let cases: [(&str, u64, precog_storage::Failpoints); 4] = [
        (
            "the copy's fsync fails",
            0,
            precog_storage::Failpoints {
                fail_data_sync_after: Some(0),
                fail_at: None,
            },
        ),
        (
            "write caching: the fsync before the delete fails",
            60_000,
            precog_storage::Failpoints {
                fail_data_sync_after: Some(0),
                fail_at: None,
            },
        ),
        (
            "copied, not deleted",
            0,
            precog_storage::Failpoints {
                fail_data_sync_after: None,
                fail_at: Some("retain:before_manifest"),
            },
        ),
        (
            "deleted, files left",
            0,
            precog_storage::Failpoints {
                fail_data_sync_after: None,
                fail_at: Some("retain:after_manifest"),
            },
        ),
    ];
    for (when, flush_delay_ms, fp) in cases {
        let d = tempfile::tempdir().unwrap();
        let mut c = config(d.path());
        c.retention_bytes = Some(4 * 64 * 1024);
        c.streams.insert("low".into(), stream_cfg(1 << 20));
        c.flush_delay_ms = flush_delay_ms;
        c.flush_bytes = 1 << 30;
        {
            let node = Node::open(c.clone()).await.unwrap();
            for seq in 0..40 {
                assert!(node.append(1, on("low", seq, 1000)).await.is_success());
            }
            for seq in 0..300 {
                assert!(node.append(1, on("high", seq, 1000)).await.is_success());
            }
        }
        // reopen with the fault armed: the first fsync of this process is the cleaner's
        let mut armed = c.clone();
        armed.storage_failpoints = fp;
        {
            let node = Node::open(armed).await.unwrap();
            assert!(
                node.apply_retention().await.is_err(),
                "{when}: the fault fired"
            );
            assert!(node.is_fatal(), "{when}");
        }
        for pass in ["after the crash", "after the next retention"] {
            let node = Node::open(c.clone()).await.unwrap();
            if pass != "after the crash" {
                node.apply_retention().await.unwrap();
            }
            assert_eq!(
                cursor_seqs(&node, "low"),
                (0..40).collect::<Vec<_>>(),
                "{when} {pass}"
            );
            assert_eq!(
                node.store.read("low", 0, 1000).0.len(),
                40,
                "{when} {pass}: once each"
            );
            for seq in 0..40 {
                assert!(
                    node.store.get(&id("low", seq)).is_some(),
                    "{when} {pass}: {seq}"
                );
            }
            let s = node.status();
            let f = s.logs.iter().find(|f| f.log.stream == "low").unwrap();
            assert_eq!((f.contiguous, f.high), (40, 40), "{when} {pass}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_over_the_cap_alike_are_cut_alike() {
    // Why: with equal shares, the cut takes from whichever retains most, down to the next
    // one, then from both: two streams written alike keep alike, each its newest.
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    let cap = 4 * 64 * 1024;
    c.retention_bytes = Some(cap);
    for s in ["a", "b"] {
        c.streams.insert(s.into(), stream_cfg(1 << 30));
    }
    let node = Node::open(c).await.unwrap();
    for seq in 0..300 {
        for s in ["a", "b"] {
            assert!(node.append(1, on(s, seq, 1000)).await.is_success());
        }
        if seq % 50 == 49 {
            node.apply_retention().await.unwrap();
            let bytes = metric(&node, "precog_log_bytes");
            assert!(bytes > 0 && bytes <= cap, "at {seq}: {bytes}");
        }
    }
    let (a, b) = (cursor_seqs(&node, "a"), cursor_seqs(&node, "b"));
    assert!(a.len().abs_diff(b.len()) <= 2, "{} vs {}", a.len(), b.len());
    for v in [&a, &b] {
        assert!(v[0] > 0 && v.len() > 50);
        assert_eq!(*v, (v[0]..300).collect::<Vec<_>>());
    }
    assert_eq!(
        metric(&node, "precog_retention_forced_total") as usize,
        600 - a.len() - b.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_the_cleaner_cannot_read_does_not_stop_the_node_cap() {
    // Why: the node cap protects the disk. A retained record that fails its checksum when the
    // cleaner reads it to copy it cannot be kept anyway; if that failed the whole pass, every
    // later pass would fail at it too, nothing would be dropped again, and the log would grow
    // until the disk is full.
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    let cap = 4 * 64 * 1024;
    c.retention_bytes = Some(cap);
    c.streams.insert("low".into(), stream_cfg(1 << 20));
    c.tail_cache_bytes = 0; // reads go to the disk
    let node = Node::open(c).await.unwrap();
    for seq in 0..20 {
        assert!(node.append(1, on("low", seq, 1000)).await.is_success());
    }
    for seq in 0..100 {
        assert!(node.append(1, on("high", seq, 1000)).await.is_success());
    }
    // the first record of the oldest segment (low 0): one payload byte flips on disk
    let seg = std::fs::read_dir(d.path().join("log"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .min()
        .unwrap();
    let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
    std::os::unix::fs::FileExt::write_all_at(&f, &[0xff], 4096 + 600).unwrap();
    for round in 1..6u64 {
        for i in 0..100 {
            let seq = round * 100 + i;
            assert!(node.append(1, on("high", seq, 1000)).await.is_success());
        }
        node.apply_retention().await.unwrap();
        let bytes = metric(&node, "precog_log_bytes");
        assert!(bytes <= cap, "round {round}: {bytes}");
    }
    assert_eq!(cursor_seqs(&node, "low"), (1..20).collect::<Vec<_>>());
}
