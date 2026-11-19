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
