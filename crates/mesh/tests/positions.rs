//! Named consumer positions (live-ingestion's archivers commit after each upload): a committed
//! cursor survives restarts and is the same on every node.

use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::*;
use precog_mesh::store::Cursor;

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut c = NodeConfig::single(0, "a", dir.to_path_buf(), "tok".into());
    c.segment_size = 1 << 20;
    c
}

fn cursor(pairs: &[(&str, u64)]) -> Cursor {
    pairs
        .iter()
        .map(|(w, s)| {
            (
                LogId {
                    stream: "raw.trades.x".into(),
                    writer: (*w).into(),
                    epoch: 1,
                },
                *s,
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_position_survives_a_restart_and_the_latest_commit_wins() {
    let d = tempfile::tempdir().unwrap();
    {
        let node = Node::open(config(d.path())).await.unwrap();
        assert_eq!(
            node.position("archiver", "raw.trades.x"),
            None,
            "a new group has none"
        );
        node.commit_position("archiver", "raw.trades.x", &cursor(&[("a", 5)]))
            .await
            .unwrap();
        node.commit_position("archiver", "raw.trades.x", &cursor(&[("a", 9), ("b", 2)]))
            .await
            .unwrap();
        node.commit_position("other", "raw.trades.x", &cursor(&[("a", 1)]))
            .await
            .unwrap();
        node.apply_retention().await.unwrap();
    }
    let node = Node::open(config(d.path())).await.unwrap();
    assert_eq!(
        node.position("archiver", "raw.trades.x"),
        Some(cursor(&[("a", 9), ("b", 2)]))
    );
    assert_eq!(
        node.position("other", "raw.trades.x"),
        Some(cursor(&[("a", 1)]))
    );
    assert_eq!(node.position("archiver", "raw.quotes.x"), None);
    // commits after the restart (a new writer epoch) still win over the older ones
    node.commit_position("archiver", "raw.trades.x", &cursor(&[("a", 11)]))
        .await
        .unwrap();
    assert_eq!(
        node.position("archiver", "raw.trades.x"),
        Some(cursor(&[("a", 11)]))
    );
}

/// A position commit as node `id` writes it to `__positions`: its `seq`-th, at `time_us`.
fn commit_batch(id: u32, seq: u64, time_us: u64, c: &[(&str, u64)]) -> bytes::Bytes {
    let log = LogId {
        stream: "__positions".into(),
        writer: format!("node-{id}"),
        epoch: 1,
    };
    let entries: Vec<(&str, u64, u64)> = c.iter().map(|&(w, s)| (w, 1, s)).collect();
    let payload = serde_json::to_vec(&entries).unwrap();
    Batch::encode_keyed(&log, seq, &[(time_us, b"archiver\0raw.trades.x", &payload)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_gets_two_commits_newest_first_serves_the_newest() {
    // Why: replication and repair bring commits to a node in any order. Picking the value
    // logged last on this node made it serve the older commit while it held the newer one, so
    // a reader failing over to it re-read what it had committed past. Found by spec/consumers
    // (MC_positions_lsn).
    let d = tempfile::tempdir().unwrap();
    let node = Node::open(config(d.path())).await.unwrap();
    // node 7 committed (a, 5) then (a, 9); node 8 later committed (a, 12)
    let older = commit_batch(7, 0, 1_000, &[("a", 5)]);
    let newer = commit_batch(7, 1, 2_000, &[("a", 9)]);
    assert!(node.append(1, newer).await.is_success());
    assert!(node.append(1, older).await.is_success());
    assert_eq!(
        node.position("archiver", "raw.trades.x"),
        Some(cursor(&[("a", 9)]))
    );
    let other = commit_batch(8, 0, 3_000, &[("a", 12)]);
    assert!(node.append(1, other).await.is_success());
    assert_eq!(
        node.position("archiver", "raw.trades.x"),
        Some(cursor(&[("a", 12)]))
    );
    // a commit through this node, a second after another node's, wins (times are in us)
    let now_us = precog_mesh::store::now_ms() * 1000;
    let before = commit_batch(9, 0, now_us - 1_000_000, &[("a", 20)]);
    assert!(node.append(1, before).await.is_success());
    node.commit_position("archiver", "raw.trades.x", &cursor(&[("a", 30)]))
        .await
        .unwrap();
    assert_eq!(
        node.position("archiver", "raw.trades.x"),
        Some(cursor(&[("a", 30)]))
    );
    node.apply_retention().await.unwrap(); // writes the snapshots
    drop(node);
    // and the same after a restart, from the snapshot and the log
    let node = Node::open(config(d.path())).await.unwrap();
    assert_eq!(
        node.position("archiver", "raw.trades.x"),
        Some(cursor(&[("a", 30)]))
    );
}
