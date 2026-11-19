//! The Rust SDK end to end against an in-process 3-node cluster over loopback TCP, as
//! live-ingestion uses Redpanda today: two producer replicas send the same events with a dedup
//! id; readers resume from a committed position (the archiver) or a cursor they keep (brrrrr),
//! from a time (a new group's now - 2h), on any node.

use precog_client::{Client, Event, Start};
use precog_mesh::node::{Node, NodeConfig, PeerConfig, StreamConfig};
use std::collections::BTreeSet;
use std::sync::Arc;

/// A free port, held (bound with SO_REUSEADDR, as the node binds, but not listening, so a node
/// not started refuses connections) while the socket lives, so no test running in parallel
/// takes it before its node binds it.
fn reserve() -> tokio::net::TcpSocket {
    let s = tokio::net::TcpSocket::new_v4().unwrap();
    s.set_reuseaddr(true).unwrap();
    s.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    s
}

struct Cluster {
    addrs: Vec<String>,
    ports: Vec<u16>,
    /// held until each node listens; those of nodes not started stay dead
    reserved: Vec<Option<tokio::net::TcpSocket>>,
    _nodes: Vec<Arc<Node>>,
    _dir: tempfile::TempDir,
}

/// Starts node `i` of the cluster; `repair_ms` is how often it repairs from its peers.
async fn start(c: &mut Cluster, i: u32, repair_ms: u64) {
    let mut cfg = NodeConfig::single(
        i,
        &format!("d{i}"),
        c._dir.path().join(format!("n{i}")),
        "tok".into(),
    );
    cfg.listen_tcp = Some(
        format!("127.0.0.1:{}", c.ports[i as usize])
            .parse()
            .unwrap(),
    );
    cfg.peers = (0..c.ports.len() as u32)
        .filter(|&j| j != i)
        .map(|j| PeerConfig {
            id: j,
            domain: format!("d{j}"),
            addr: c.addrs[j as usize].clone(),
        })
        .collect();
    cfg.rpc_timeout_ms = 300;
    cfg.repair_interval_ms = repair_ms;
    cfg.segment_size = 1 << 20;
    cfg.streams.insert(
        "state".into(),
        StreamConfig {
            compacted: true,
            ..Default::default()
        },
    );
    let n = Node::open(cfg).await.unwrap();
    n.spawn_listeners().await.unwrap();
    c.reserved[i as usize] = None; // the node holds the port now
    c._nodes.push(n);
}

/// Nodes `up` of three are started; the others' addresses stay dead.
async fn cluster(up: &[u32]) -> Cluster {
    cluster_of(3, up).await
}

/// Nodes `up` of `n`, each in its own failure domain.
async fn cluster_of(n: usize, up: &[u32]) -> Cluster {
    let reserved: Vec<_> = (0..n).map(|_| Some(reserve())).collect();
    let ports: Vec<u16> = reserved
        .iter()
        .map(|s| s.as_ref().unwrap().local_addr().unwrap().port())
        .collect();
    let mut c = Cluster {
        reserved,
        addrs: ports
            .iter()
            .map(|p| format!("tcp://127.0.0.1:{p}"))
            .collect(),
        ports,
        _nodes: vec![],
        _dir: tempfile::tempdir().unwrap(),
    };
    for &i in up {
        start(
            &mut c,
            i,
            NodeConfig::single(0, "", "".into(), "".into()).repair_interval_ms,
        )
        .await;
    }
    c
}

fn names(evs: &[Event]) -> Vec<String> {
    evs.iter()
        .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicas_publish_the_same_events_and_a_reader_sees_each_once_in_writer_order() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    let mut a = client.producer("raw.trades.x", "producer-a");
    let mut b = client.producer("raw.trades.x", "producer-b");
    for i in 0..50u64 {
        let id = format!("trade-{i}");
        a.send(i, id.as_bytes(), format!("t{i}").as_bytes());
        b.send(i, id.as_bytes(), format!("t{i}").as_bytes());
        if i % 10 == 9 {
            a.sync().await.unwrap();
            b.sync().await.unwrap();
        }
    }
    let mut r = client
        .consumer("raw.trades.x", "archiver", Start::Beginning)
        .await
        .unwrap();
    let mut got = vec![];
    loop {
        let evs = r.poll(4096).await.unwrap();
        if evs.is_empty() {
            break;
        }
        got.extend(evs);
    }
    let set: BTreeSet<String> = names(&got).into_iter().collect();
    assert_eq!(set.len(), 50, "every event once");
    assert_eq!(got.len(), 50, "no duplicate of a replica's event");
    for w in ["producer-a", "producer-b"] {
        let seqs: Vec<u64> = got
            .iter()
            .filter(|e| e.writer == w)
            .map(|e| e.seq)
            .collect();
        assert!(
            seqs.windows(2).all(|p| p[0] < p[1]),
            "{w} in order: {seqs:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_committed_position_resumes_on_another_node_losing_and_repeating_nothing() {
    let c = cluster(&[1, 2]).await; // node 0 is down: every client fails over
    let client = Client::connect(&c.addrs, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    for i in 0..40u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("t{i}").as_bytes());
        if i % 5 == 4 {
            p.sync().await.unwrap(); // batches of 5: the read unit
        }
    }
    let mut r = client
        .consumer("raw.trades.x", "archiver", Start::Beginning)
        .await
        .unwrap();
    let first = r.poll(300).await.unwrap();
    assert!(!first.is_empty() && first.len() < 40, "{}", first.len());
    r.commit().await.unwrap();
    // a new archiver pod: resumes at the commit through whichever node answers
    let mut r2 = client
        .consumer(
            "raw.trades.x",
            "archiver",
            Start::Committed {
                or: Box::new(Start::Beginning),
            },
        )
        .await
        .unwrap();
    let mut rest = vec![];
    loop {
        let evs = r2.poll(1 << 20).await.unwrap();
        if evs.is_empty() {
            break;
        }
        rest.extend(evs);
    }
    let all: Vec<String> = names(&first).into_iter().chain(names(&rest)).collect();
    assert_eq!(all, (0..40).map(|i| format!("t{i}")).collect::<Vec<_>>());
    // brrrrr keeps its own cursor in its checkpoint and resumes from it
    let mut r3 = client.consumer_at("raw.trades.x", r.cursor().clone());
    assert_eq!(names(&r3.poll(1 << 20).await.unwrap()), names(&rest));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_commits_the_cursor_it_kept_not_how_far_its_reader_got() {
    // The archiver reads ahead of its uploads: it may commit only what is durable in the object
    // store, so it keeps that cursor itself and commits it, while its consumer is further on.
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    for i in 0..20u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("t{i}").as_bytes());
    }
    p.sync().await.unwrap();
    let mut r = client
        .consumer("raw.trades.x", "archiver", Start::Beginning)
        .await
        .unwrap();
    let read = r.poll(1 << 20).await.unwrap();
    assert_eq!(read.len(), 20);
    let mut uploaded = precog_client::Cursor::new();
    let log = r.cursor().keys().next().unwrap().clone();
    uploaded.insert(log.clone(), 10); // t0..t9 are durable; t10.. only read
    client
        .commit("archiver", "raw.trades.x", &uploaded)
        .await
        .unwrap();
    let mut resumed = client
        .consumer(
            "raw.trades.x",
            "archiver",
            Start::Committed {
                or: Box::new(Start::End),
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.cursor().get(&log), Some(&10));
    assert_eq!(
        names(&resumed.poll(1 << 20).await.unwrap()),
        (10..20).map(|i| format!("t{i}")).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_group_can_start_at_a_time_and_compacted_state_is_served() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    p.send(1, b"old", b"old");
    p.sync().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let since = precog_client::now_ms();
    p.send(2, b"new", b"new");
    p.sync().await.unwrap();
    let mut r = client
        .consumer(
            "raw.trades.x",
            "seeded",
            Start::Committed {
                or: Box::new(Start::Time(since)),
            },
        )
        .await
        .unwrap();
    assert_eq!(names(&r.poll(1 << 20).await.unwrap()), ["new"]);
    let mut s = client.producer("state", "collector");
    s.put(1, b"k1", b"v1");
    s.put(2, b"k1", b"v2");
    s.put(3, b"k2", b"w");
    s.sync().await.unwrap();
    let latest = client.latest("state").await.unwrap();
    assert_eq!(
        latest,
        vec![
            (b"k1".to_vec(), b"v2".to_vec().into()),
            (b"k2".to_vec(), b"w".to_vec().into())
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_producer_keeps_several_batches_in_flight_and_sync_waits_for_all() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    for i in 0..200u64 {
        p.send(i, format!("id{i}").as_bytes(), format!("t{i}").as_bytes());
        if i % 10 == 9 {
            p.flush().await.unwrap(); // queues the batch; at most 8 wait for their acks
            assert!(p.in_flight() <= 8);
        }
    }
    p.sync().await.unwrap();
    assert_eq!(p.in_flight(), 0);
    let mut r = client.consumer_at("raw.trades.x", Default::default());
    let mut got = vec![];
    loop {
        let evs = r.poll(1 << 20).await.unwrap();
        if evs.is_empty() {
            break;
        }
        got.extend(evs);
    }
    assert_eq!(
        names(&got),
        (0..200).map(|i| format!("t{i}")).collect::<Vec<_>>()
    );
}

#[test]
fn dedup_ids_travel_as_8_byte_hashes() {
    assert_eq!(
        precog_client::dedup_key(b"binance-futures-p3-123456").len(),
        8
    );
    assert_eq!(
        precog_client::dedup_key(b"a"),
        precog_client::dedup_key(b"a")
    );
    assert_ne!(
        precog_client::dedup_key(b"a"),
        precog_client::dedup_key(b"b")
    );
    assert!(
        precog_client::dedup_key(b"").is_empty(),
        "no id: never deduplicated"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producers_compress_by_default_and_readers_decompress() {
    let c = cluster(&[0, 1, 2]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    let mut p = client.producer("raw.trades.x", "producer-a");
    let mut plain = client.producer("raw.trades.x", "producer-b");
    plain.set_compression(false);
    for i in 0..100u64 {
        let payload =
            format!(r#"{{"exchange":"binance","symbol":"BTCUSDT","trade_id":{i},"side":"buy"}}"#);
        p.send(i, format!("a{i}").as_bytes(), payload.as_bytes());
        plain.send(i, format!("b{i}").as_bytes(), payload.as_bytes());
    }
    p.sync().await.unwrap();
    plain.sync().await.unwrap();
    let st = |n: usize| c._nodes[n].status();
    let bytes = st(0).bytes;
    let mut r = client.consumer_at("raw.trades.x", Default::default());
    let evs = r.poll(1 << 20).await.unwrap();
    assert_eq!(evs.len(), 200);
    assert_eq!(
        evs.iter()
            .filter(|e| e.payload.starts_with(b"{\"exchange\""))
            .count(),
        200
    );
    // the compressed writer's batch is the smaller of the two
    let a: u64 = evs
        .iter()
        .filter(|e| e.writer == "producer-a")
        .map(|e| e.payload.len() as u64)
        .sum();
    assert!(
        bytes < 2 * a,
        "stored {bytes} bytes for {a} + {a} payload bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_end_of_a_stream_is_every_acked_event_even_when_one_node_is_behind() {
    // Why: an acked batch is on 2 of 3 nodes, not on all 3. A reader that takes one node's end
    // as the stream's end (brrrrr's replay, a new group at Start::End) would stop short, or skip
    // events, whenever that node is behind: just restarted, or repairing.
    let mut c = cluster(&[1, 2]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    // a stream whose clients ask node 0 first: the node that will be behind
    let stream = (0..)
        .map(|i| format!("raw.trades.{i}"))
        .find(|s| xxhash_rust::xxh3::xxh3_64(s.as_bytes()).is_multiple_of(3))
        .unwrap();
    let mut p = client.producer(&stream, "w");
    for i in 0..40u64 {
        p.send(i, format!("id{i}").as_bytes(), b"x");
        if i % 10 == 9 {
            p.flush().await.unwrap();
        }
    }
    p.sync().await.unwrap();
    start(&mut c, 0, 3_600_000).await; // empty, and no repair within the test

    let end = client.end(&stream).await.unwrap();
    assert_eq!(end.values().sum::<u64>(), 40, "every acked event: {end:?}");
    let at_end = client.consumer(&stream, "g", Start::End).await.unwrap();
    assert_eq!(
        at_end.cursor(),
        &end,
        "a new group at the end skips nothing it should not"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_end_of_a_stream_on_four_nodes_merges_enough_ends_to_meet_every_acked_copy() {
    // Why: an acked batch is on `copies` nodes, so only the ends of nodes - copies + 1 nodes are
    // sure to include one of them. On four nodes with 2 copies, two ends can both miss it, and a
    // reader starting at the end would read acked events as new. Found by spec/mesh (MC_end4).
    let mut c = cluster_of(4, &[0, 1]).await;
    let client = Client::connect(&c.addrs, "tok").unwrap();
    // clients of this stream ask node 2 first, then 3: the two nodes without the batches
    let stream = (0..)
        .map(|i| format!("raw.trades.{i}"))
        .find(|s| xxhash_rust::xxh3::xxh3_64(s.as_bytes()) % 4 == 2)
        .unwrap();
    let mut p = client.producer(&stream, "w");
    for i in 0..10u64 {
        p.send(i, format!("id{i}").as_bytes(), b"x");
    }
    p.sync().await.unwrap();
    for i in [2, 3] {
        start(&mut c, i, 3_600_000).await; // empty, and no repair within the test
    }
    let end = client.end(&stream).await.unwrap();
    assert_eq!(end.values().sum::<u64>(), 10, "every acked event: {end:?}");
}
