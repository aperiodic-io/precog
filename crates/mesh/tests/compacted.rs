//! Compacted streams, as live-ingestion's producers keep their state in (`cleanup.policy=compact`):
//! the newest value of every key, through retention and restarts, never deduplicated.

use bytes::Bytes;
use precog_mesh::node::{Node, NodeConfig, StreamConfig};
use precog_mesh::proto::*;
use std::sync::Arc;

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut c = NodeConfig::single(0, "a", dir.to_path_buf(), "tok".into());
    c.segment_size = 64 * 1024;
    c.retention_bytes = Some(3 * 64 * 1024);
    c.streams.insert(
        "state".into(),
        StreamConfig {
            compacted: true,
            ..Default::default()
        },
    );
    c
}

async fn put(node: &Arc<Node>, stream: &str, seq: u64, kvs: &[(&str, &str)]) {
    let log = LogId {
        stream: stream.into(),
        writer: "w".into(),
        epoch: 1,
    };
    let evs: Vec<(u64, &[u8], &[u8])> = kvs
        .iter()
        .map(|(k, v)| (seq, k.as_bytes(), v.as_bytes()))
        .collect();
    let ack = node
        .append(1, Batch::encode_keyed(&log, seq, &evs).unwrap())
        .await;
    assert!(ack.is_success(), "{ack:?}");
}

fn latest(node: &Node) -> Vec<(String, String)> {
    node.latest("state")
        .into_iter()
        .map(|(k, v)| {
            (
                String::from_utf8(k).unwrap(),
                String::from_utf8(v.to_vec()).unwrap(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compacted_stream_serves_the_newest_value_of_every_key() {
    let d = tempfile::tempdir().unwrap();
    let node = Node::open(config(d.path())).await.unwrap();
    put(&node, "state", 0, &[("k1", "v1"), ("k2", "v2")]).await;
    // the same key again within the dedup window is a new state, not a duplicate
    put(&node, "state", 2, &[("k1", "v3")]).await;
    assert_eq!(
        latest(&node),
        [("k1".into(), "v3".into()), ("k2".into(), "v2".into())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newest_values_survive_retention_and_restarts() {
    let d = tempfile::tempdir().unwrap();
    {
        let node = Node::open(config(d.path())).await.unwrap();
        put(&node, "state", 0, &[("k1", "old"), ("k2", "v2")]).await;
        put(&node, "state", 2, &[("k1", "new")]).await;
        // enough other data that retention frees the segments holding the state
        let log = LogId {
            stream: "raw".into(),
            writer: "w".into(),
            epoch: 1,
        };
        for seq in 0..400u64 {
            let b = Batch::encode(&log, seq, &[(seq, &[7u8; 1000][..])]).unwrap();
            assert!(node.append(1, b).await.is_success());
        }
        node.apply_retention().await.unwrap();
        assert!(
            node.store.read("state", 0, 10).0.is_empty(),
            "the log no longer holds the state"
        );
        assert_eq!(
            latest(&node),
            [("k1".into(), "new".into()), ("k2".into(), "v2".into())]
        );
        put(&node, "state", 3, &[("k3", "after")]).await;
    }
    let node = Node::open(config(d.path())).await.unwrap();
    assert_eq!(
        latest(&node),
        [
            ("k1".into(), "new".into()),
            ("k2".into(), "v2".into()),
            ("k3".into(), "after".into())
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_damaged_snapshot_is_refused_loudly() {
    let d = tempfile::tempdir().unwrap();
    {
        let node = Node::open(config(d.path())).await.unwrap();
        put(&node, "state", 0, &[("k1", "v1")]).await;
        node.apply_retention().await.unwrap();
    }
    let snap = std::fs::read_dir(d.path().join("compact"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut b = std::fs::read(&snap).unwrap();
    let n = b.len();
    b[n / 2] ^= 1;
    std::fs::write(&snap, &b).unwrap();
    let e = Node::open(config(d.path()))
        .await
        .err()
        .expect("must not start on a damaged snapshot");
    assert!(format!("{e:#}").contains("snapshot"), "{e:#}");
    let _: Option<Bytes> = None;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_of_an_unknown_version_is_refused_even_with_a_valid_checksum() {
    let d = tempfile::tempdir().unwrap();
    {
        let node = Node::open(config(d.path())).await.unwrap();
        put(&node, "state", 0, &[("k1", "v1")]).await;
        node.apply_retention().await.unwrap();
    }
    let snap = std::fs::read_dir(d.path().join("compact"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    // a future format: other magic, consistent checksum. Reading it as this one would
    // serve made-up keys, so it must be refused, not trusted for its CRC
    let mut b = std::fs::read(&snap).unwrap();
    let n = b.len() - 4;
    b[3] = b'9';
    let c = crc32c::crc32c(&b[..n]);
    b[n..].copy_from_slice(&c.to_le_bytes());
    std::fs::write(&snap, &b).unwrap();
    let e = Node::open(config(d.path()))
        .await
        .err()
        .expect("must not start on a snapshot of another version");
    assert!(format!("{e:#}").contains("unknown version"), "{e:#}");
}
