//! In-process multi-node tests over real loopback TCP sockets.

use bytes::Bytes;
use precog_mesh::client::{MeshClient, Writer};
use precog_mesh::node::{Node, NodeConfig, PeerConfig};
use precog_mesh::proto::*;
use precog_mesh::transport::Addr;
use std::sync::Arc;
use std::time::Duration;

struct Cluster {
    cfgs: Vec<NodeConfig>,
    addrs: Vec<Addr>,
    /// Each node's port, held until the node listens on it, so no other test takes it in
    /// between: bound with SO_REUSEADDR (as the node binds) but not listening, so a node not
    /// started yet still refuses connections.
    reserved: std::sync::Mutex<Vec<Option<tokio::net::TcpSocket>>>,
    _dir: tempfile::TempDir,
}

fn cluster(n: u32, domains: &[&str]) -> Cluster {
    let dir = tempfile::tempdir().unwrap();
    let mut addrs = Vec::new();
    let mut socks = Vec::new();
    let mut sockets = Vec::new();
    for _ in 0..n {
        let s = tokio::net::TcpSocket::new_v4().unwrap();
        s.set_reuseaddr(true).unwrap();
        s.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = s.local_addr().unwrap().port();
        sockets.push(Some(s));
        socks.push(format!("127.0.0.1:{port}").parse().unwrap());
        addrs.push(format!("tcp://127.0.0.1:{port}"));
    }
    let cfgs = (0..n)
        .map(|i| {
            let mut c = NodeConfig::single(
                i,
                domains[i as usize],
                dir.path().join(format!("n{i}")),
                "secret-token".into(),
            );
            c.listen_tcp = Some(socks[i as usize]);
            c.peers = (0..n)
                .filter(|&j| j != i)
                .map(|j| PeerConfig {
                    id: j,
                    domain: domains[j as usize].into(),
                    addr: addrs[j as usize].clone(),
                })
                .collect();
            c.rpc_timeout_ms = 500;
            c.repair_interval_ms = 1_000_000; // tests trigger repair explicitly
            c.segment_size = 1 << 20;
            c
        })
        .collect();
    Cluster {
        cfgs,
        addrs: addrs.iter().map(|a| a.parse().unwrap()).collect(),
        reserved: std::sync::Mutex::new(sockets),
        _dir: dir,
    }
}

async fn start(c: &Cluster, i: usize) -> Arc<Node> {
    let node = Node::open(c.cfgs[i].clone()).await.unwrap();
    node.spawn_listeners().await.unwrap();
    c.reserved.lock().unwrap()[i] = None; // the node holds the port now
    node
}

async fn client(c: &Cluster) -> Arc<MeshClient> {
    let mc = MeshClient::new(c.addrs.clone(), "secret-token".into()).unwrap();
    // keep failing tests fast
    let mc = Arc::into_inner(mc).unwrap();
    let mut mc = mc;
    mc.rpc_timeout = Duration::from_millis(2000);
    mc.append_deadline = Duration::from_secs(3);
    Arc::new(mc)
}

async fn status(mc: &MeshClient, node: usize) -> Status {
    match mc.control(node, &Control::Status).await.unwrap() {
        ControlResp::Status(s) => s,
        r => panic!("{r:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_replication_degraded_and_repair() {
    let c = cluster(3, &["a", "b", "c"]);
    let _n0 = start(&c, 0).await;
    let _n1 = start(&c, 1).await;
    let mc = client(&c).await;

    // Only two of three nodes up: durable-2 succeeds, durable-3 cannot.
    let mut w = Writer::new(mc.clone(), "s", "w1", 1, 0, 2);
    for i in 0..20u64 {
        let r = w
            .append(&[(100 - i, format!("e{i}").as_bytes())])
            .await
            .unwrap();
        assert!(r.ack.copies >= 2, "{r:?}");
        assert_eq!(r.ack.nodes & 0b11, 0b11);
    }
    let mut w3 = Writer::new(mc.clone(), "s", "w3", 1, 0, 3);
    let err = w3.append(&[(1, b"x")]).await.unwrap_err().to_string();
    assert!(err.contains("unknown"), "{err}");

    // Node 2 joins and repairs everything it missed.
    let n2 = start(&c, 2).await;
    let (fetched, errors) = n2.repair_pass().await;
    assert!(errors.is_empty(), "{errors:?}");
    // w1's 20 batches, plus w3's batch, which nodes 0 and 1 stored
    // even though its durable-3 append returned unknown.
    assert_eq!(fetched, 21);
    let s2 = status(&mc, 2).await;
    assert_eq!(s2.events, 21);
    let f = s2.logs.iter().find(|f| f.log.writer == "w1").unwrap();
    assert_eq!((f.contiguous, f.high), (20, 20));

    // Now durable-3 works and the same-bytes retry is idempotent.
    let r = w3.append(&[(1, b"x")]).await.unwrap();
    assert_eq!(r.ack.copies, 3);

    // Event-time ordered read from any node is identical.
    let read = |n| {
        let mc = mc.clone();
        async move {
            match mc
                .control(
                    n,
                    &Control::Read {
                        stream: "s".into(),
                        from_time_us: 0,
                        limit: 100,
                    },
                )
                .await
                .unwrap()
            {
                ControlResp::Read { events, .. } => events,
                r => panic!("{r:?}"),
            }
        }
    };
    let e0 = read(0).await;
    assert_eq!(e0.len(), 21);
    assert!(
        e0.windows(2)
            .all(|w| w[0].event_time_us <= w[1].event_time_us)
    );
    assert_eq!(e0[0].payload, Bytes::from_static(b"x")); // event time 1
    assert_eq!(read(2).await, e0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_client_fails_over_to_the_next_node_when_its_preferred_one_is_down() {
    let c = cluster(3, &["a", "b", "c"]);
    // node 2 is down: a client preferring it wraps around to node 0
    for i in 0..2 {
        start(&c, i).await;
    }
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let b = Batch::encode(&log, 0, &[(1, b"x")]).unwrap();
    let r = mc.append(2, &b, 1).await.unwrap();
    assert_eq!((r.node, r.attempts), (0, 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idempotent_retry_and_conflict() {
    let c = cluster(3, &["a", "b", "c"]);
    for i in 0..3 {
        start(&c, i).await;
    }
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 9,
    };
    let b = Batch::encode(&log, 0, &[(5, b"one"), (6, b"two")]).unwrap();
    let r1 = mc.append(0, &b, 2).await.unwrap();
    // Retry of the same bytes through a *different* node: duplicate, no new data.
    let r2 = mc.append(1, &b, 2).await.unwrap();
    assert!(r1.ack.is_success() && r2.ack.is_success());
    tokio::time::sleep(Duration::from_millis(300)).await; // let the third copy land
    for n in 0..3 {
        assert_eq!(status(&mc, n).await.events, 2, "node {n}");
    }
    // Same identity, different content: refused everywhere.
    let bad = Batch::encode(&log, 1, &[(6, b"TWO")]).unwrap();
    for n in 0..3 {
        let e = mc.append(n, &bad, 1).await.unwrap_err().to_string();
        assert!(e.contains("Conflict"), "{e}");
    }
    // Impossible copy count is rejected without side effects.
    let other = Batch::encode(&log, 2, &[(7, b"three")]).unwrap();
    let e = mc.append(0, &other, 4).await.unwrap_err().to_string();
    assert!(e.contains("Rejected"), "{e}");
    assert_eq!(status(&mc, 0).await.events, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_domain_labels_do_not_count_as_copies() {
    // Nodes 0 and 1 share domain "a": only two independent domains exist.
    let c = cluster(3, &["a", "a", "b"]);
    for i in 0..3 {
        start(&c, i).await;
    }
    let mc = client(&c).await;
    let mut w = Writer::new(mc.clone(), "s", "w", 1, 0, 2);
    let r = w.append(&[(1, b"x")]).await.unwrap();
    assert_eq!(r.ack.copies, 2);
    assert_eq!(
        r.ack.nodes & 0b010,
        0,
        "same-domain peer must not be a replication target"
    );
    let mut w3 = Writer::new(mc.clone(), "s", "w3", 1, 0, 3);
    assert!(
        w3.append(&[(1, b"x")])
            .await
            .unwrap_err()
            .to_string()
            .contains("Rejected")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_cannot_replicate_and_bad_tokens_are_refused() {
    let c = cluster(1, &["a"]);
    start(&c, 0).await;
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let b = Batch::encode(&log, 0, &[(1, b"x")]).unwrap();
    let e = mc
        .call(0, Kind::Replicate, encode_replicate(0, &b))
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("non-peer"), "{e}");
    let bad = MeshClient::new(c.addrs.clone(), "wrong".into()).unwrap();
    assert!(bad.control(0, &Control::Status).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_from_wal() {
    let c = cluster(1, &["a"]);
    let cfg = c.cfgs[0].clone();
    {
        let node = Node::open(cfg.clone()).await.unwrap();
        for i in 0..5u64 {
            let log = LogId {
                stream: "s".into(),
                writer: "w".into(),
                epoch: 1,
            };
            let a = node
                .append(1, Batch::encode(&log, i, &[(i, b"p")]).unwrap())
                .await;
            assert!(a.is_success());
        }
        drop(node);
    }
    tokio::time::sleep(Duration::from_millis(100)).await; // WAL thread releases lock
    let node = Node::open(cfg).await.unwrap();
    assert_eq!(node.store.totals().1, 5);
    assert_eq!(node.store.frontiers(None)[0].contiguous, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_subscribers_receive_everything_in_writer_order() {
    let c = cluster(3, &["a", "b", "c"]);
    for i in 0..3 {
        start(&c, i).await;
    }
    let mc = client(&c).await;
    // History written before anyone subscribes.
    let mut w1 = Writer::new(mc.clone(), "s", "w1", 1, 0, 2);
    for i in 0..10u64 {
        w1.append(&[(i, b"old")]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await; // third copies land
    let mut replay = mc.subscribe(2, Some("s"), true).await.unwrap();
    let mut live = mc.subscribe(1, Some("s"), false).await.unwrap();
    let mut other = mc.subscribe(1, Some("other-stream"), false).await.unwrap();
    // Live traffic from two writers through different nodes.
    let mut w2 = Writer::new(mc.clone(), "s", "w2", 1, 1, 2);
    for i in 0..50u64 {
        w1.append(&[(100 + i, b"new")]).await.unwrap();
        w2.append(&[(100 + i, b"new"), (101 + i, b"new2")])
            .await
            .unwrap();
    }
    async fn drain(
        s: &mut precog_mesh::client::Subscription,
        events: u64,
    ) -> std::collections::HashMap<String, Vec<u64>> {
        let mut got: std::collections::HashMap<String, Vec<u64>> = Default::default();
        let mut n = 0;
        while n < events {
            let b = tokio::time::timeout(Duration::from_secs(5), s.next())
                .await
                .expect("feed stalled")
                .unwrap()
                .unwrap();
            n += b.count as u64;
            got.entry(b.id.log.writer.clone())
                .or_default()
                .push(b.id.first_seq);
        }
        got
    }
    let r = drain(&mut replay, 10 + 50 + 100).await;
    assert_eq!(r["w1"], (0..60).collect::<Vec<_>>());
    assert_eq!(r["w2"], (0..50).map(|i| i * 2).collect::<Vec<_>>());
    let l = drain(&mut live, 50 + 100).await;
    assert_eq!(
        l["w1"],
        (10..60).collect::<Vec<_>>(),
        "live feed starts at subscribe time"
    );
    assert_eq!(l["w2"].len(), 50);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), other.next())
            .await
            .is_err(),
        "stream filter"
    );
}
