//! Broker-side dedup, as live-ingestion's raw topics need it: two producer replicas send the same
//! events (same dedup key); the node keeps the first copy of each and drops later ones within
//! the window, while every writer's sequence stays contiguous and retries stay idempotent.

use bytes::Bytes;
use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::*;
use std::sync::Arc;

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut c = NodeConfig::single(0, "a", dir.to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    c
}

fn log(writer: &str) -> LogId {
    LogId {
        stream: "raw.trades.x".into(),
        writer: writer.into(),
        epoch: 1,
    }
}

/// A batch of events whose payload is their key (so a read shows which copy survived).
fn keyed(writer: &str, seq: u64, keys: &[&str]) -> Bytes {
    let evs: Vec<(u64, &[u8], &[u8])> = keys
        .iter()
        .map(|k| (1, k.as_bytes(), k.as_bytes()))
        .collect();
    Batch::encode_keyed(&log(writer), seq, &evs).unwrap()
}

async fn append(node: &Arc<Node>, b: &Bytes) -> AckStatus {
    let ack = node.append(1, b.clone()).await;
    assert!(ack.is_success(), "{ack:?}");
    assert_eq!(
        ack.crc,
        Batch::parse(b.clone()).unwrap().ident,
        "acks name the batch's identity"
    );
    ack.status
}

fn payloads(node: &Node) -> Vec<String> {
    let (evs, _) = node.store.read("raw.trades.x", 0, 1000);
    let mut v: Vec<String> = evs
        .iter()
        .map(|e| format!("{}:{}", e.writer, String::from_utf8_lossy(&e.payload)))
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_replicas_copy_is_dropped_and_its_own_events_kept() {
    let d = tempfile::tempdir().unwrap();
    let node = Node::open(config(d.path())).await.unwrap();
    append(&node, &keyed("a", 0, &["t1", "t2"])).await;
    let b = keyed("b", 0, &["t2", "t3"]);
    assert_eq!(append(&node, &b).await, AckStatus::Ok);
    assert_eq!(payloads(&node), ["a:t1", "a:t2", "b:t3"]);
    // b's sequence stays contiguous: its next batch starts at 2
    append(&node, &keyed("b", 2, &["t4"])).await;
    let f = node.store.frontiers(None);
    let fb = f.iter().find(|f| f.log.writer == "b").unwrap();
    assert_eq!((fb.contiguous, fb.high), (3, 3));
    // b's retry of the bytes it sent is acknowledged (a duplicate copy), not refused as a conflict
    append(&node, &b).await;
    assert_eq!(payloads(&node).len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_without_a_key_are_never_dropped() {
    let d = tempfile::tempdir().unwrap();
    let node = Node::open(config(d.path())).await.unwrap();
    for w in ["a", "b"] {
        append(
            &node,
            &Batch::encode(&log(w), 0, &[(1, b"same"), (2, b"same")]).unwrap(),
        )
        .await;
    }
    assert_eq!(payloads(&node).len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_can_be_switched_off() {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.dedup_window_ms = None;
    let node = Node::open(c).await.unwrap();
    append(&node, &keyed("a", 0, &["t1"])).await;
    append(&node, &keyed("b", 0, &["t1"])).await;
    assert_eq!(payloads(&node), ["a:t1", "b:t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_index_is_rebuilt_from_the_log_after_a_restart() {
    let d = tempfile::tempdir().unwrap();
    {
        let node = Node::open(config(d.path())).await.unwrap();
        append(&node, &keyed("a", 0, &["t1", "t2"])).await;
    }
    let node = Node::open(config(d.path())).await.unwrap();
    append(&node, &keyed("b", 0, &["t2", "t9"])).await;
    assert_eq!(payloads(&node), ["a:t1", "a:t2", "b:t9"]);
    assert!(node.status().dedup_dropped >= 1, "{:?}", node.status());
}

#[test]
fn an_omitted_dedup_config_takes_live_ingestion_s_five_minutes() {
    // the raw topics' replicas resend within minutes: a shorter default window would let
    // a second replica's copies through
    let c: NodeConfig = serde_json::from_str(
        r#"{"id":0,"domain":"a","data_dir":"/d","token":"t","listen_tcp":null,"peers":[]}"#,
    )
    .unwrap();
    assert_eq!(c.dedup_window_ms, Some(300_000));
    assert_eq!(c.dedup_max_entries, 16_000_000);
    let s = NodeConfig::single(0, "a", "/d".into(), "t".into());
    assert_eq!(
        (s.dedup_window_ms, s.dedup_max_entries),
        (Some(300_000), 16_000_000)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_rebuilds_only_the_last_window_of_the_log() {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.dedup_window_ms = Some(1_000);
    {
        let node = Node::open(c.clone()).await.unwrap();
        append(&node, &keyed("a", 0, &["t1"])).await;
    }
    // appended more than the window ago: the rebuilt index must not hold it, or a restart
    // would drop events the running node would have kept
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    let node = Node::open(c).await.unwrap();
    append(&node, &keyed("b", 0, &["t1"])).await;
    assert_eq!(payloads(&node), ["a:t1", "b:t1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_the_fenced_log_refuses_is_unknown_so_the_client_retries_it() {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.storage_failpoints.fail_data_sync_after = Some(1);
    let node = Node::open(c).await.unwrap();
    append(&node, &keyed("a", 0, &["t1"])).await;
    assert!(!node.append(1, keyed("a", 1, &["t2"])).await.is_success());
    assert!(node.is_fatal());
    // not Rejected ("no side effect, drop it"): the client must send it again elsewhere
    let ack = node.append(1, keyed("b", 0, &["t3"])).await;
    assert_eq!(ack.status, AckStatus::Unknown, "{ack:?}");
}
