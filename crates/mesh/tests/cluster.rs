//! In-process multi-node tests over real loopback TCP sockets.

use bytes::Bytes;
use precog_mesh::client::{MeshClient, Writer};
use precog_mesh::node::{Node, NodeConfig, PeerConfig};
use precog_mesh::proto::*;
use precog_mesh::transport::Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;

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

/// A live feed starts when `subscribe` returns, not whenever the node gets round to it: the
/// node confirms the feed's starting point, so a batch written right after subscribing is never
/// taken for history and skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_node_confirms_a_feed_before_it_carries_anything() {
    let c = cluster(1, &["a"]);
    start(&c, 0).await;
    let t = Duration::from_secs(2);
    let conn = precog_mesh::transport::connect(&c.addrs[0], t)
        .await
        .unwrap();
    let hello = Hello {
        token: "secret-token".into(),
        node_id: None,
    };
    conn.call(Kind::Hello, serde_json::to_vec(&hello).unwrap().into(), t)
        .await
        .unwrap();
    for from_start in [false, true] {
        let sub = Subscribe {
            stream: None,
            from_start,
        };
        let mut rx = conn
            .subscribe(Kind::Subscribe, serde_json::to_vec(&sub).unwrap().into(), 8)
            .await
            .unwrap();
        let f = tokio::time::timeout(t, rx.recv())
            .await
            .expect("no confirmation with nothing written")
            .unwrap();
        assert!(f.body.is_empty(), "a start marker, not a batch");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_subscribers_receive_everything_in_writer_order() {
    let c = cluster(3, &["a", "b", "c"]);
    let mut nodes = vec![];
    for i in 0..3 {
        nodes.push(start(&c, i).await);
    }
    let mc = client(&c).await;
    // History written before anyone subscribes.
    let mut w1 = Writer::new(mc.clone(), "s", "w1", 1, 0, 2);
    for i in 0..10u64 {
        w1.append(&[(i, b"old")]).await.unwrap();
    }
    // An ack needs 2 copies and the third can land after it: repair (off in these tests, every
    // second in production) brings it in, so the subscribing nodes hold all the history.
    for n in &nodes[1..] {
        assert!(n.repair_pass().await.1.is_empty());
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replicas_dedup_index_is_warm_so_a_failover_still_drops_duplicates() {
    // Producer replica a's events land on node 0 and are replicated; replica b's copies then
    // reach node 1 (as after node 0 fails): node 1 drops them, having recorded a's ids.
    let c = cluster(3, &["a", "b", "c"]);
    let nodes: Vec<_> = futures::future::join_all((0..3).map(|i| start(&c, i))).await;
    let mc = client(&c).await;
    let log = |w: &str| LogId {
        stream: "raw.trades.x".into(),
        writer: w.into(),
        epoch: 1,
    };
    let a = Batch::encode_keyed(&log("a"), 0, &[(1, b"t1", b"x"), (2, b"t2", b"y")]).unwrap();
    mc.append(0, &a, 3).await.unwrap();
    let b = Batch::encode_keyed(&log("b"), 0, &[(1, b"t2", b"y"), (3, b"t3", b"z")]).unwrap();
    let r = mc.append(1, &b, 3).await.unwrap();
    assert_eq!(r.node, 1);
    let (evs, _) = nodes[1].store.read("raw.trades.x", 0, 100);
    let mut got: Vec<_> = evs
        .iter()
        .map(|e| (e.writer.clone(), e.payload.clone()))
        .collect();
    got.sort();
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(nodes[1].status().dedup_dropped >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_position_committed_through_one_node_is_read_on_another() {
    let c = cluster(3, &["a", "b", "c"]);
    let nodes: Vec<_> = futures::future::join_all((0..3).map(|i| start(&c, i))).await;
    let log = LogId {
        stream: "raw.trades.x".into(),
        writer: "p".into(),
        epoch: 1,
    };
    let pos: precog_mesh::store::Cursor = [(log, 42)].into_iter().collect();
    nodes[0]
        .commit_position("archiver", "raw.trades.x", &pos)
        .await
        .unwrap();
    // replicated to at least one peer before the commit returns; repair brings the third
    let held = nodes
        .iter()
        .filter(|n| n.position("archiver", "raw.trades.x") == Some(pos.clone()))
        .count();
    assert!(held >= 2, "{held}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_is_acknowledged_only_once_the_accepting_node_holds_it() {
    // Two peers holding a batch is not enough: the accepting node's own copy must be durable
    // (reads there right after the ack see it, and a batch whose duplicates were dropped is acked
    // only after the batches that claimed them, which the accepting node's log orders).
    let mut c = cluster(3, &["a", "b", "c"]);
    c.cfgs[0].storage_failpoints.fail_data_sync_after = Some(0);
    let _nodes: Vec<_> = futures::future::join_all((0..3).map(|i| start(&c, i))).await;
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let b = Batch::encode(&log, 0, &[(1, b"x")]).unwrap();
    let ack = mc
        .call(0, Kind::Append, encode_append(2, &b))
        .await
        .unwrap();
    let ack = precog_mesh::node::parse_frame_ack(&ack).unwrap();
    assert!(
        !ack.is_success(),
        "acknowledged without the accepting node's copy: {ack:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_read_over_rpc_answers_in_frames_that_fit() {
    // a reader asking for "everything" over a backlog larger than a frame gets it in steps:
    // one oversized answer would be refused by its own transport, and the reader would stall
    let mut c = cluster(1, &["a"]);
    c.cfgs[0].segment_size = 64 << 20;
    let _node = start(&c, 0).await;
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let big = vec![7u8; 1 << 20];
    for seq in 0..20 {
        let b = Batch::encode(&log, seq, &[(seq, &big[..])]).unwrap();
        mc.append(0, &b, 1).await.unwrap();
    }
    let ControlResp::Cursor {
        cursor: Some(start),
    } = mc
        .control(
            0,
            &Control::CursorAt {
                stream: "s".into(),
                time_ms: 0,
            },
        )
        .await
        .unwrap()
    else {
        panic!("no cursor")
    };
    assert_eq!(start, [("w".to_string(), 1, 0)], "the stream's start");
    let read = Control::ReadCursor {
        stream: "s".into(),
        cursor: start,
        max_bytes: u32::MAX,
    };
    let f = mc
        .call(
            0,
            Kind::Control,
            Bytes::from(serde_json::to_vec(&read).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(f.kind, Kind::Batches);
    let mut body = &f.body[..];
    let mut n = 0;
    while !body.is_empty() {
        let len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
        body = &body[4 + len..];
        n += 1;
    }
    assert!((1..20).contains(&n), "{n} of 20 batches in one answer");
    assert!(
        f.body.len() <= MAX_FRAME_BYTES / 2 + (1 << 21),
        "{}",
        f.body.len()
    );
    // a reader's cursor is honoured: from seq 19 only the last batch comes back
    let read = Control::ReadCursor {
        stream: "s".into(),
        cursor: vec![("w".to_string(), 1, 19)],
        max_bytes: u32::MAX,
    };
    let f = mc
        .call(
            0,
            Kind::Control,
            Bytes::from(serde_json::to_vec(&read).unwrap()),
        )
        .await
        .unwrap();
    let len = u32::from_le_bytes(f.body[..4].try_into().unwrap()) as usize;
    assert_eq!(f.body.len(), 4 + len, "one batch");
    assert_eq!(Batch::parse(f.body.slice(4..)).unwrap().id.first_seq, 19);
    // repair's FetchLog answers within a frame the same way
    let fetch = Control::FetchLog {
        log,
        from_seq: 0,
        max_bytes: u32::MAX,
    };
    let f = mc
        .call(
            0,
            Kind::Control,
            Bytes::from(serde_json::to_vec(&fetch).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(f.kind, Kind::Batches);
    assert!(
        (1..=MAX_FRAME_BYTES / 2 + (1 << 21)).contains(&f.body.len()),
        "{}",
        f.body.len()
    );
}

/// Forwards connections to `to`, counting the FetchLog requests that pass through.
async fn counting_proxy(to: std::net::SocketAddr) -> (String, Arc<AtomicU64>) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("tcp://{}", l.local_addr().unwrap());
    let n = Arc::new(AtomicU64::new(0));
    let count = n.clone();
    tokio::spawn(async move {
        while let Ok((inbound, _)) = l.accept().await {
            let out = tokio::net::TcpStream::connect(to).await.unwrap();
            let (mut ir, mut iw) = inbound.into_split();
            let (mut or, mut ow) = out.into_split();
            tokio::spawn(async move { tokio::io::copy(&mut or, &mut iw).await });
            let count = count.clone();
            tokio::spawn(async move {
                let mut buf = bytes::BytesMut::new();
                while let Ok(Some(f)) = precog_mesh::rpc::read_frame(&mut ir, &mut buf).await {
                    if f.kind == Kind::Control
                        && matches!(
                            serde_json::from_slice(&f.body),
                            Ok(Control::FetchLog { .. })
                        )
                    {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    let mut b = bytes::BytesMut::new();
                    f.encode_into(&mut b);
                    if ow.write_all(&b).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    (addr, n)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_pulls_what_a_node_lacks_in_as_few_requests_as_it_takes() {
    // anti-entropy runs on every node all the time: it pulls a lagging log in 4 MiB steps up to
    // the peer's high, and asks nothing about a log the node already holds
    let mut c = cluster(2, &["a", "b"]);
    for cfg in &mut c.cfgs {
        cfg.segment_size = 64 << 20;
        cfg.target_copies = 1; // no replication: node 0 gets the log by repair only
    }
    let n1 = start(&c, 1).await;
    let (proxy, fetches) = counting_proxy(c.cfgs[1].listen_tcp.unwrap()).await;
    c.cfgs[0].peers[0].addr = proxy;
    let n0 = start(&c, 0).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let big = vec![7u8; 1 << 20];
    for seq in 0..6 {
        let b = Batch::encode(&log, seq, &[(seq, &big[..])]).unwrap();
        assert!(n1.append(1, b).await.is_success());
    }
    let (fetched, errors) = n0.repair_pass().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(fetched, 6, "the whole log, not its first 4 MiB");
    assert_eq!(
        fetches.load(Ordering::Relaxed),
        2,
        "4 MiB, then the rest; none past the peer's high"
    );
    let (fetched, _) = n0.repair_pass().await;
    assert_eq!(fetched, 0);
    assert_eq!(
        fetches.load(Ordering::Relaxed),
        2,
        "nothing asked for a log it holds"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_subscriber_gets_a_logs_batches_that_land_out_of_order() {
    // A replica can store a log's seq 1 before seq 0 (its replicate requests run
    // concurrently). A subscriber that was already listening must still get seq 0: the feed
    // joins a log that is new to it at its start, not at the first batch that happens to land.
    let c = cluster(1, &["a"]);
    let n = start(&c, 0).await;
    let mc = client(&c).await;
    let mut live = mc.subscribe(0, Some("s"), false).await.unwrap();
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    for seq in [1, 0, 2] {
        let b = Batch::encode(&log, seq, &[(seq, b"x")]).unwrap();
        assert!(n.append(1, b).await.is_success());
    }
    let mut got = vec![];
    for _ in 0..3 {
        let b = tokio::time::timeout(Duration::from_secs(5), live.next())
            .await
            .expect("feed stalled")
            .unwrap()
            .unwrap();
        got.push(b.id.first_seq);
    }
    assert_eq!(got, [0, 1, 2]);
}

#[test]
fn an_address_prints_as_configured() {
    // errors and logs name peers by the address an operator wrote
    let a: Addr = "tcp://127.0.0.1:7001".parse().unwrap();
    assert_eq!(a.to_string(), "tcp://127.0.0.1:7001");
}

/// A live feed that joins a log behind a gap (a node made its batches durable out of order, as
/// pipelined appends and repair do) must carry the batches past the gap once it fills: the
/// client joins the log at the first batch it sees and waits for each next one in sequence.
/// The feed used to skip batches durable before it started, so the client held everything
/// after them for good. Found by spec/livefeed (MC_livefeed_skip).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_feed_that_joins_behind_a_gap_delivers_everything_after_it() {
    let c = cluster(1, &["a"]);
    let node = start(&c, 0).await;
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 1,
    };
    let b = |seq: u64| Batch::encode(&log, seq, &[(seq, b"x")]).unwrap();
    for seq in [0, 1, 3] {
        assert!(node.append(1, b(seq)).await.is_success());
    }
    let mut sub = mc.subscribe(0, Some("s"), false).await.unwrap();
    for seq in [2, 4] {
        assert!(node.append(1, b(seq)).await.is_success());
    }
    let mut got = vec![];
    while got.len() < 3 {
        let next = tokio::time::timeout(Duration::from_secs(2), sub.next()).await;
        let Ok(Some(Ok(batch))) = next else {
            panic!("the feed stalled after {got:?}");
        };
        got.push(batch.id.first_seq);
    }
    assert_eq!(got, vec![2, 3, 4]);
}

/// The bytes node `n` holds at batch identity `id`, if any.
async fn held(mc: &MeshClient, n: usize, id: &BatchId) -> Option<Bytes> {
    let c = Control::Fetch {
        ids: vec![id.clone()],
    };
    let f = mc
        .call(
            n,
            Kind::Control,
            Bytes::from(serde_json::to_vec(&c).unwrap()),
        )
        .await
        .unwrap();
    (f.body.len() > 4).then(|| f.body.slice(4..))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_missed_a_batch_gives_up_other_bytes_under_its_identity() {
    // Why: node 2 was down while X was acknowledged on nodes 0 and 1. A writer that reuses X's
    // identity with other bytes Y at node 2 is refused, but node 2 kept Y: repair compares
    // frontiers, which agree, so node 2 never served the acknowledged X, not even after repair.
    let c = cluster(3, &["a", "b", "c"]);
    let (_n0, _n1) = (start(&c, 0).await, start(&c, 1).await);
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 3,
    };
    let x = Batch::encode(&log, 0, &[(1, b"acknowledged")]).unwrap();
    let id = Batch::parse(x.clone()).unwrap().id;
    assert!(mc.append(0, &x, 2).await.unwrap().ack.is_success());
    let n2 = start(&c, 2).await;
    let y = Batch::encode(&log, 0, &[(1, b"other bytes")]).unwrap();
    let e = mc.append(2, &y, 2).await.unwrap_err().to_string();
    assert!(e.contains("Conflict"), "{e}");
    let mut ok = false;
    for _ in 0..50 {
        if held(&mc, 2, &id).await.as_ref() == Some(&x) {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ok, "node 2 holds {:?}", held(&mc, 2, &id).await);
    assert_eq!(status(&mc, 2).await.events, 1, "the other bytes are gone");
    // and it stays that way across a restart (recovery keeps the replacement)
    drop(n2);
    tokio::time::sleep(Duration::from_millis(300)).await; // WAL thread releases its lock
    let n2 = Node::open(c.cfgs[2].clone()).await.unwrap();
    assert_eq!(n2.store.get(&id).map(|b| b.bytes), Some(x));
    assert_eq!(n2.store.totals().1, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflict_without_a_majority_changes_nothing() {
    // Why: one node against one is no majority; neither may give up its bytes for the other's
    let c = cluster(2, &["a", "b"]);
    let _n0 = start(&c, 0).await;
    let mc = client(&c).await;
    let log = LogId {
        stream: "s".into(),
        writer: "w".into(),
        epoch: 4,
    };
    let x = Batch::encode(&log, 0, &[(1, b"first")]).unwrap();
    let id = Batch::parse(x.clone()).unwrap().id;
    assert!(mc.append(0, &x, 1).await.unwrap().ack.is_success());
    let _n1 = start(&c, 1).await;
    let y = Batch::encode(&log, 0, &[(1, b"second")]).unwrap();
    assert!(mc.append(1, &y, 2).await.is_err());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(held(&mc, 0, &id).await, Some(x));
    assert_eq!(held(&mc, 1, &id).await, Some(y));
}

/// The cleaner copies what streams retain forward before the node cap drops old segments. A
/// copy keeps its batch's identity and bytes: a live feed must not push it again, a replay
/// delivers each batch once in order, and repair finds nothing missing or conflicting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_copied_forward_is_not_new_to_feeds_or_peers() {
    let mut c = cluster(2, &["a", "b"]);
    for cfg in &mut c.cfgs {
        cfg.segment_size = 64 << 10;
        cfg.retention_bytes = Some(4 * (64 << 10));
        cfg.streams.insert(
            "low".into(),
            precog_mesh::node::StreamConfig {
                retention_bytes: Some(1 << 20),
                ..Default::default()
            },
        );
    }
    let n0 = start(&c, 0).await;
    let n1 = start(&c, 1).await;
    let mc = client(&c).await;
    let mut low = Writer::new(mc.clone(), "low", "w", 1, 0, 2);
    for i in 0..20u64 {
        low.append(&[(i, &[1u8; 900][..])]).await.unwrap();
    }
    // a raw live feed on node 0: every push it gets, as the node sends it
    let t = Duration::from_secs(2);
    let conn = precog_mesh::transport::connect(&c.addrs[0], t)
        .await
        .unwrap();
    let hello = Hello {
        token: "secret-token".into(),
        node_id: None,
    };
    conn.call(Kind::Hello, serde_json::to_vec(&hello).unwrap().into(), t)
        .await
        .unwrap();
    let sub = Subscribe {
        stream: Some("low".into()),
        from_start: false,
    };
    let mut raw = conn
        .subscribe(
            Kind::Subscribe,
            serde_json::to_vec(&sub).unwrap().into(),
            1024,
        )
        .await
        .unwrap();
    assert!(
        raw.recv().await.unwrap().body.is_empty(),
        "the start marker"
    );
    let mut high = Writer::new(mc.clone(), "high", "w", 1, 0, 2);
    for round in 0..10u64 {
        for i in 0..100u64 {
            high.append(&[(round * 100 + i, &[2u8; 900][..])])
                .await
                .unwrap();
        }
        for n in [&n0, &n1] {
            n.apply_retention().await.unwrap();
        }
    }
    let copied = n0
        .metrics()
        .lines()
        .find_map(|l| l.strip_prefix("precog_retention_copied_batches_total "))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap();
    assert!(
        copied >= 40,
        "low was copied forward, again and again: {copied}"
    );
    low.append(&[(20, b"new")]).await.unwrap();
    let f = tokio::time::timeout(t, raw.recv()).await.unwrap().unwrap();
    let b = Batch::parse(f.body).unwrap();
    assert_eq!(
        b.id.first_seq, 20,
        "the first push is the new batch, no copy"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), raw.recv())
            .await
            .is_err(),
        "and nothing else"
    );
    // a replay gets each batch once, in order
    let mut replay = mc.subscribe(0, Some("low"), true).await.unwrap();
    for seq in 0..21 {
        let b = tokio::time::timeout(t, replay.next())
            .await
            .expect("replay stalled")
            .unwrap()
            .unwrap();
        assert_eq!(b.id.first_seq, seq);
    }
    // repair: the same identities and bytes on both nodes, nothing to fetch
    for n in [&n0, &n1] {
        assert_eq!(n.repair_pass().await, (0, vec![]));
    }
    let log = LogId {
        stream: "low".into(),
        writer: "w".into(),
        epoch: 1,
    };
    assert_eq!(
        n0.store.log_batches(&log, 0, 1 << 20),
        n1.store.log_batches(&log, 0, 1 << 20)
    );
    assert_eq!(n0.store.log_batches(&log, 0, 1 << 20).len(), 21);
    assert_eq!(
        n0.store.frontiers(Some("low")),
        n1.store.frontiers(Some("low"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_got_a_batch_by_repair_drops_a_later_duplicate_of_its_events() {
    // node 2 was down while producer a's event went to nodes 0 and 1; once it has repaired, a
    // producer replica's copy of that event landing on node 2 (its preferred node, back) is a
    // duplicate there too
    let c = cluster(3, &["a", "b", "c"]);
    let _n0 = start(&c, 0).await;
    let _n1 = start(&c, 1).await;
    let mc = client(&c).await;
    let log = |w: &str| LogId {
        stream: "s".into(),
        writer: w.into(),
        epoch: 1,
    };
    let ev: [(u64, &[u8], &[u8]); 1] = [(1, b"k", b"from-a")];
    mc.append(0, &Batch::encode_keyed(&log("a"), 0, &ev).unwrap(), 2)
        .await
        .unwrap();
    let n2 = start(&c, 2).await;
    let (fetched, errors) = n2.repair_pass().await;
    assert!(errors.is_empty() && fetched == 1, "{fetched} {errors:?}");
    let ev: [(u64, &[u8], &[u8]); 1] = [(2, b"k", b"from-b")];
    assert!(
        n2.append(1, Batch::encode_keyed(&log("b"), 0, &ev).unwrap())
            .await
            .is_success()
    );
    let (evs, _) = n2.store.read("s", 0, 1000);
    let got: Vec<&[u8]> = evs.iter().map(|e| &e.payload[..]).collect();
    assert_eq!(got, [&b"from-a"[..]], "b's copy of k is a duplicate");
}

mod props_majority {
    use super::*;
    use proptest::prelude::*;

    /// Why: a node that missed an acknowledged batch, then took other bytes under its identity,
    /// gives them up for the majority's (`yield_to_majority`). Whatever the bytes and however
    /// the cleaner then moves the replacement (copied forward, its old segments dropped), the
    /// node keeps serving the majority's bytes and reopens on its log afterwards.
    fn run(x: Vec<u8>, y: Vec<u8>, clean: bool) -> Result<(), TestCaseError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = cluster(3, &["a", "b", "c"]);
            c.cfgs[2].segment_size = 64 << 10;
            c.cfgs[2].retention_bytes = Some(4 * (64 << 10));
            c.cfgs[2].streams.insert(
                "s".into(),
                precog_mesh::node::StreamConfig {
                    retention_bytes: Some(1 << 20),
                    ..Default::default()
                },
            );
            let (_n0, _n1) = (start(&c, 0).await, start(&c, 1).await);
            let mc = client(&c).await;
            let log = |stream: &str| LogId {
                stream: stream.into(),
                writer: "w".into(),
                epoch: 3,
            };
            let x = Batch::encode(&log("s"), 0, &[(1, &x[..])]).unwrap();
            let id = Batch::parse(x.clone()).unwrap().id;
            prop_assert!(mc.append(0, &x, 2).await.unwrap().ack.is_success());
            let n2 = start(&c, 2).await;
            let y = Batch::encode(&log("s"), 0, &[(1, &y[..])]).unwrap();
            prop_assert!(mc.append(2, &y, 2).await.is_err(), "a conflict");
            let mut yielded = false;
            for _ in 0..50 {
                if n2.store.get(&id).map(|b| b.bytes).as_ref() == Some(&x) {
                    yielded = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            prop_assert!(yielded, "node 2 holds the majority's bytes");
            if clean {
                for seq in 0..400 {
                    let b = Batch::encode(&log("t"), seq, &[(seq, &[0u8; 1000][..])]).unwrap();
                    prop_assert!(n2.append(1, b).await.is_success());
                }
                n2.apply_retention().await.unwrap();
                prop_assert_eq!(n2.store.get(&id).map(|b| b.bytes), Some(x.clone()));
            }
            drop(n2);
            tokio::time::sleep(Duration::from_millis(300)).await; // the WAL thread lets go
            let n2 = Node::open(c.cfgs[2].clone()).await.unwrap();
            prop_assert_eq!(n2.store.get(&id).map(|b| b.bytes), Some(x));
            Ok(())
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(6))]

        #[test]
        fn a_node_gives_up_other_bytes_for_the_majority_s_and_keeps_them_through_the_cleaner(
            x in prop::collection::vec(any::<u8>(), 1..64),
            y in prop::collection::vec(any::<u8>(), 1..64),
            clean: bool,
        ) {
            prop_assume!(x != y);
            run(x, y, clean)?;
        }
    }
}
