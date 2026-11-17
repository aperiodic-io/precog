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
        let (node, _) = Node::open(config(d.path())).await.unwrap();
        append_n(&node, "w", 120, 1500).await
    };
    let (node, _) = Node::open(config(d.path())).await.unwrap();
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
    let (node, _) = Node::open(c).await.unwrap();
    append_n(&node, "w", 400, 1000).await;
    node.apply_retention().await.unwrap();
    let (batches, _, _) = node.store.totals();
    assert!(batches < 400 && batches > 50, "kept {batches}");
    let (items, _, _) = node.store.inventory(0, 10);
    assert!(items[0].0 > 0, "inventory starts after the trimmed prefix");
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
    let (node, _) = Node::open(c.clone()).await.unwrap();
    append_n(&node, "w", 200, 1000).await;
    node.apply_retention().await.unwrap();
    assert_eq!(node.store.totals().0, 200, "nothing is a minute old yet");
    drop(node);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    c.retention_ms = Some(10);
    let (node, _) = Node::open(c).await.unwrap();
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
            "listen_iroh":null,"peers":[]}"#,
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
    let (node, _) = Node::open(c).await.unwrap();
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
        let (node, _) = Node::open(config(d.path())).await.unwrap();
        append_n(&node, "w", 120, 1500).await;
    }
    let (node, _) = Node::open(config(d.path())).await.unwrap();
    let s = node.status();
    assert_eq!(s.batches, 120);
    assert!(s.segments >= 3, "{s:?}");
    assert!(
        s.recovery_scanned_bytes > 0 && s.recovery_scanned_bytes <= 64 * 1024,
        "{s:?}"
    );
    assert!(s.fsyncs == 0, "fsyncs count this process's group commits");
}
