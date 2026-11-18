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
