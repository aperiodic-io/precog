//! The Rust SDK end to end against an in-process 3-node cluster over loopback TCP, as
//! live-ingestion uses Redpanda today: two producer replicas send the same events with a dedup
//! id; readers resume from a committed position (the archiver) or a cursor they keep (brrrrr),
//! from a time (a new group's now - 2h), on any node.

use precog_client::{Client, Event, Start};
use precog_mesh::node::{Node, NodeConfig, PeerConfig, StreamConfig};
use std::collections::BTreeSet;
use std::sync::Arc;

struct Cluster {
    addrs: Vec<String>,
    _nodes: Vec<Arc<Node>>,
    _dir: tempfile::TempDir,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Nodes `up` of three are started; the others' addresses stay dead.
async fn cluster(up: &[u32]) -> Cluster {
    let dir = tempfile::tempdir().unwrap();
    let ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let addrs: Vec<String> = ports
        .iter()
        .map(|p| format!("tcp://127.0.0.1:{p}"))
        .collect();
    let mut nodes = vec![];
    for &i in up {
        let mut c = NodeConfig::single(
            i,
            &format!("d{i}"),
            dir.path().join(format!("n{i}")),
            "tok".into(),
        );
        c.listen_tcp = Some(format!("127.0.0.1:{}", ports[i as usize]).parse().unwrap());
        c.peers = (0..3)
            .filter(|&j| j != i)
            .map(|j| PeerConfig {
                id: j,
                domain: format!("d{j}"),
                addr: addrs[j as usize].clone(),
            })
            .collect();
        c.rpc_timeout_ms = 300;
        c.segment_size = 1 << 20;
        c.streams.insert(
            "state".into(),
            StreamConfig {
                compacted: true,
                ..Default::default()
            },
        );
        let n = Node::open(c).await.unwrap();
        n.spawn_listeners().await.unwrap();
        nodes.push(n);
    }
    Cluster {
        addrs,
        _nodes: nodes,
        _dir: dir,
    }
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
