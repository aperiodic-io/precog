//! Model-based test of the whole path (SDK, mesh client, RPC, node, store, WAL) on an in-process
//! 3-node cluster over loopback TCP: two producer replicas send the same events with dedup ids,
//! a compacted stream takes puts, and readers read from the beginning, resume from a committed
//! position, start at the end and ask for the latest values, against a model of what was acked,
//! with one node down from the start until it joins and repairs.

mod model {
    use precog_client::{Client, Event, Start};
    use precog_mesh::node::{Node, NodeConfig, PeerConfig, StreamConfig};
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    enum Op {
        /// producer replica 0 or 1 sends these dedup ids (flushed, not awaited)
        Send(u8, Vec<u8>),
        /// a compacted stream's key gets a value
        Put(u8, u8),
        ReadAll,
        /// read part, commit, and resume from the committed position on a new consumer
        Resume(u32),
        End,
        Latest,
        /// the node that was down starts and repairs from its peers
        Join,
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        prop::collection::vec(
            prop_oneof![
                6 => (0u8..2, prop::collection::vec(0u8..24, 1..6)).prop_map(|(p, ids)| Op::Send(p, ids)),
                2 => (0u8..4, any::<u8>()).prop_map(|(k, v)| Op::Put(k, v)),
                1 => Just(Op::ReadAll),
                1 => (64u32..512).prop_map(Op::Resume),
                1 => Just(Op::End),
                1 => Just(Op::Latest),
                1 => Just(Op::Join),
            ],
            1..24,
        )
    }

    /// Three nodes on loopback; `down` is not started (its port is held, so it refuses
    /// connections) until `Op::Join`.
    struct Cluster {
        addrs: Vec<String>,
        cfgs: Vec<NodeConfig>,
        held: Vec<Option<tokio::net::TcpSocket>>,
        nodes: Vec<Arc<Node>>,
    }

    impl Cluster {
        async fn new(dir: &std::path::Path, down: Option<usize>) -> Cluster {
            let held: Vec<Option<tokio::net::TcpSocket>> = (0..3)
                .map(|_| {
                    let s = tokio::net::TcpSocket::new_v4().unwrap();
                    s.set_reuseaddr(true).unwrap();
                    s.bind("127.0.0.1:0".parse().unwrap()).unwrap();
                    Some(s)
                })
                .collect();
            let ports: Vec<u16> = held
                .iter()
                .map(|s| s.as_ref().unwrap().local_addr().unwrap().port())
                .collect();
            let addrs: Vec<String> = ports
                .iter()
                .map(|p| format!("tcp://127.0.0.1:{p}"))
                .collect();
            let cfgs = (0..3u32)
                .map(|i| {
                    let mut c = NodeConfig::single(
                        i,
                        &format!("d{i}"),
                        dir.join(format!("n{i}")),
                        "tok".into(),
                    );
                    c.listen_tcp =
                        Some(format!("127.0.0.1:{}", ports[i as usize]).parse().unwrap());
                    c.peers = (0..3u32)
                        .filter(|&j| j != i)
                        .map(|j| PeerConfig {
                            id: j,
                            domain: format!("d{j}"),
                            addr: addrs[j as usize].clone(),
                        })
                        .collect();
                    c.rpc_timeout_ms = 300;
                    c.repair_interval_ms = 1_000_000; // Op::Join repairs explicitly
                    c.segment_size = 1 << 20;
                    c.streams.insert(
                        "state".into(),
                        StreamConfig {
                            compacted: true,
                            ..Default::default()
                        },
                    );
                    c
                })
                .collect();
            let mut c = Cluster {
                addrs,
                cfgs,
                held,
                nodes: vec![],
            };
            for i in (0..3).filter(|&i| Some(i) != down) {
                c.start(i).await;
            }
            c
        }

        async fn start(&mut self, i: usize) -> Arc<Node> {
            let n = Node::open(self.cfgs[i].clone()).await.unwrap();
            n.spawn_listeners().await.unwrap();
            self.held[i] = None; // the node holds the port now
            self.nodes.push(n.clone());
            n
        }
    }

    /// A commit, asked again until it is acknowledged, as a caller does: the test's peers
    /// time out after 300 ms, and on a busy machine a replicate of the position can take
    /// longer (the commit is then not acknowledged; committing the same position again is).
    async fn commit(c: &precog_client::Consumer) {
        let mut r = c.commit().await;
        for _ in 0..50 {
            if r.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            r = c.commit().await;
        }
        r.unwrap();
    }

    async fn drain(c: &mut precog_client::Consumer) -> Vec<Event> {
        let mut out = vec![];
        loop {
            let evs = c.poll(4096).await.unwrap();
            if evs.is_empty() {
                return out;
            }
            out.extend(evs);
        }
    }

    fn ids(evs: &[Event]) -> Vec<String> {
        evs.iter()
            .map(|e| String::from_utf8(e.payload.to_vec()).unwrap())
            .collect()
    }

    /// Each acked id once, nothing that was never sent, each writer's events in sequence.
    fn exactly_once(evs: &[Event], acked: &BTreeSet<String>) -> Result<(), TestCaseError> {
        let got = ids(evs);
        let set: BTreeSet<String> = got.iter().cloned().collect();
        prop_assert_eq!(got.len(), set.len(), "an id read twice: {:?}", got);
        prop_assert_eq!(&set, acked);
        for w in ["a", "b"] {
            let seqs: Vec<u64> = evs
                .iter()
                .filter(|e| e.writer == w)
                .map(|e| e.seq)
                .collect();
            prop_assert!(
                seqs.windows(2).all(|p| p[0] < p[1]),
                "writer {} out of order: {:?}",
                w,
                seqs
            );
        }
        Ok(())
    }

    async fn run(mut down: Option<usize>, ops: Vec<Op>) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let mut cluster = Cluster::new(dir.path(), down).await;
        let client = Client::connect(&cluster.addrs, "tok").unwrap();
        let mut ps = [client.producer("raw", "a"), client.producer("raw", "b")];
        let mut state = client.producer("state", "s");
        let mut acked: BTreeSet<String> = BTreeSet::new();
        let mut latest: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut t = 1u64;
        for op in ops {
            match op {
                Op::Send(p, evs) => {
                    for id in evs {
                        t += 1;
                        let id = format!("e{id}");
                        ps[p as usize].send(t, id.as_bytes(), id.as_bytes());
                        acked.insert(id);
                    }
                    ps[p as usize].flush().await.unwrap();
                    continue;
                }
                Op::Put(k, v) => {
                    t += 1;
                    state.put(t, &[k], &[v]);
                    state.sync().await.unwrap();
                    latest.insert(vec![k], vec![v]);
                    continue;
                }
                _ => {}
            }
            for p in &mut ps {
                p.sync().await.unwrap();
            }
            match op {
                Op::ReadAll => {
                    let mut c = client
                        .consumer("raw", "all", Start::Beginning)
                        .await
                        .unwrap();
                    exactly_once(&drain(&mut c).await, &acked)?;
                }
                Op::Resume(max) => {
                    let g = format!("g{t}");
                    let mut c = client.consumer("raw", &g, Start::Beginning).await.unwrap();
                    let mut got = c.poll(max).await.unwrap();
                    commit(&c).await;
                    let mut r = client
                        .consumer(
                            "raw",
                            &g,
                            Start::Committed {
                                or: Box::new(Start::End),
                            },
                        )
                        .await
                        .unwrap();
                    got.extend(drain(&mut r).await);
                    exactly_once(&got, &acked)?;
                }
                Op::End => {
                    let mut c = client.consumer("raw", "end", Start::End).await.unwrap();
                    prop_assert!(drain(&mut c).await.is_empty(), "events past the end");
                }
                Op::Join => {
                    if let Some(i) = down.take() {
                        let n = cluster.start(i).await;
                        let (_, errors) = n.repair_pass().await;
                        prop_assert!(errors.is_empty(), "repair: {:?}", errors);
                    }
                }
                Op::Latest => {
                    let got: BTreeMap<Vec<u8>, Vec<u8>> = client
                        .latest("state")
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|(k, v)| (k, v.to_vec()))
                        .collect();
                    prop_assert_eq!(&got, &latest);
                }
                Op::Send(..) | Op::Put(..) => unreachable!(),
            }
        }
        Ok(())
    }

    /// Why: a deleted stream must read as empty through the SDK on a cluster, with a node down
    /// or not, while the name stays usable: what a producer created after the delete sends is
    /// read, what one that wrote before it sends is refused.
    async fn delete(down: Option<usize>, before: u8, after: u8) -> Result<(), TestCaseError> {
        let dir = tempfile::tempdir().unwrap();
        let c = Cluster::new(dir.path(), down).await;
        let client = Client::connect(&c.addrs, "tok").unwrap();
        let mut old = client.producer("sessions/1", "w");
        let mut kept = client.producer("sessions/2", "w");
        for i in 0..before {
            old.send(i as u64, &[], &[i]);
            kept.send(i as u64, &[], &[i]);
        }
        old.sync().await.unwrap();
        kept.sync().await.unwrap();
        // (asked again until acknowledged: a replica can be slower than the RPC timeout)
        let mut floor = client.delete("sessions/1").await;
        for _ in 0..50 {
            if floor.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            floor = client.delete("sessions/1").await;
        }
        let floor = floor.unwrap();
        let all = |stream: &'static str| {
            let client = client.clone();
            async move { drain(&mut client.consumer_at(stream, Default::default())).await }
        };
        prop_assert_eq!(all("sessions/1").await.len(), 0);
        prop_assert_eq!(all("sessions/2").await.len(), before as usize);
        // the producer from before the delete is refused, if the stream held anything of it
        // (a writer no node had seen yet writes the new stream); one created now is not
        old.send(99, &[], b"late");
        prop_assert_eq!(old.sync().await.is_err(), before > 0);
        prop_assert_eq!(floor > 0, before > 0);
        let mut new = client.producer("sessions/1", "w");
        for i in 0..after {
            new.send(i as u64, &[], &[i]);
        }
        new.sync().await.unwrap();
        let got = all("sessions/1").await;
        prop_assert_eq!(got.len(), after as usize + (before == 0) as usize);
        prop_assert!(got.iter().all(|e| e.epoch >= floor));
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(8))]

        #[test]
        fn a_deleted_stream_reads_as_empty_and_its_name_works_for_new_producers(
            down in prop::option::of(0usize..3), before in 0u8..6, after in 0u8..6,
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(delete(down, before, after))?;
        }
    }

    proptest! {

        #[test]
        fn a_cluster_delivers_every_acked_event_once_and_the_newest_values(
            down in prop::option::of(0usize..3), ops in ops(),
        ) {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            rt.block_on(run(down, ops))?;
        }
    }
}
