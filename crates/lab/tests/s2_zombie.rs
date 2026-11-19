//! The guard docs/S2-PLAN.md relies on for a stream-level sequence: two owners of one stream
//! (a zombie and its successor) that number the same seq_nums are told apart by the mesh's
//! conflict rule. With `copies` a majority of the replica set, at most one of two different
//! batches at one seq range is ever acknowledged, whichever nodes accept them.

use bytes::Bytes;
use precog_mesh::client::MeshClient;
use precog_mesh::node::{Node, NodeConfig, PeerConfig};
use precog_mesh::proto::{Batch, LogId};
use std::sync::Arc;

async fn cluster(dir: &std::path::Path) -> (Vec<Arc<Node>>, Vec<String>) {
    let ls: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let socks: Vec<_> = ls.iter().map(|l| l.local_addr().unwrap()).collect();
    let addrs: Vec<String> = socks.iter().map(|s| format!("tcp://{s}")).collect();
    drop(ls);
    let mut nodes = vec![];
    for i in 0..3u32 {
        let mut c = NodeConfig::single(i, &format!("d{i}"), dir.join(format!("n{i}")), "t".into());
        c.listen_tcp = Some(socks[i as usize]);
        c.peers = (0..3u32)
            .filter(|&j| j != i)
            .map(|j| PeerConfig {
                id: j,
                domain: format!("d{j}"),
                addr: addrs[j as usize].clone(),
            })
            .collect();
        c.segment_size = 1 << 20;
        c.repair_interval_ms = 1_000_000;
        let n = Node::open(c).await.unwrap();
        n.spawn_listeners().await.unwrap();
        nodes.push(n);
    }
    (nodes, addrs)
}

fn batch(stream: &str, seq: u64, owner: &str) -> Bytes {
    let log = LogId {
        stream: stream.into(),
        writer: "s2".into(),
        epoch: 0,
    };
    Batch::encode_keyed(&log, seq, &[(0, &[][..], owner.as_bytes())]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_owners_never_both_win_a_seq_num() {
    let dir = tempfile::tempdir().unwrap();
    let (_nodes, addrs) = cluster(dir.path()).await;
    let mc = MeshClient::new(
        addrs.iter().map(|a| a.parse().unwrap()).collect(),
        "t".into(),
    )
    .unwrap();
    let (mut both, mut none, mut one) = (0, 0, 0);
    for seq in 0..200u64 {
        // each owner enters through a different node, at the same moment
        let (a, b) = (batch("z", seq, "old-owner"), batch("z", seq, "new-owner"));
        let (ra, rb) = tokio::join!(mc.append(0, &a, 2), mc.append(2, &b, 2));
        match (ra.is_ok(), rb.is_ok()) {
            (true, true) => both += 1,
            (false, false) => none += 1,
            _ => one += 1,
        }
    }
    assert_eq!(both, 0, "two different batches acknowledged at one seq_num");
    eprintln!("200 races: one winner {one}, no winner {none}");
}

/// The counter-case: `copies = 1` is not a majority, so both owners can be acknowledged at the
/// same seq_num, each by the node it entered through. The guard is quorum intersection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_majority_both_owners_can_win() {
    let dir = tempfile::tempdir().unwrap();
    let (_nodes, addrs) = cluster(dir.path()).await;
    let mc = MeshClient::new(
        addrs.iter().map(|a| a.parse().unwrap()).collect(),
        "t".into(),
    )
    .unwrap();
    let mut both = 0;
    for seq in 0..200u64 {
        let (a, b) = (batch("z", seq, "old-owner"), batch("z", seq, "new-owner"));
        let (ra, rb) = tokio::join!(mc.append(0, &a, 1), mc.append(2, &b, 1));
        both += (ra.is_ok() && rb.is_ok()) as u32;
    }
    eprintln!("200 races at copies=1: both acknowledged {both}");
    assert!(both > 0);
}
