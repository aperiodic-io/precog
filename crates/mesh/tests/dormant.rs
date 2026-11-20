//! Dormant streams (docs/ARCHITECTURE.md, "Dormant streams"): a stream nothing used for its idle
//! time leaves RAM and is a summary on disk. Any use wakes it, and it must answer as it did:
//! appends and their identity rules, reads, a live feed's start, through crashes at any step of
//! a retention pass and appends racing the pass that puts it out.

use bytes::Bytes;
use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::*;
use precog_mesh::store::Cursor;
use std::sync::Arc;

fn config(dir: &std::path::Path) -> NodeConfig {
    let mut c = NodeConfig::single(0, "a", dir.to_path_buf(), "tok".into());
    c.segment_size = 16 << 10;
    c.target_copies = 1;
    c.dormant_after_ms = 0; // every stream with no batch in RAM leaves it at a pass
    c
}

fn log(stream: &str) -> LogId {
    LogId {
        stream: stream.into(),
        writer: "w".into(),
        epoch: 1,
    }
}

fn batch(stream: &str, seq: u64, v: u8) -> Bytes {
    Batch::encode(&log(stream), seq, &[(seq, &[v; 500][..])]).unwrap()
}

/// Enough of another stream to seal every segment the others are in, then a pass.
async fn settle(node: &Arc<Node>, round: u64) {
    let l = LogId {
        stream: "filler".into(),
        writer: "f".into(),
        epoch: round,
    };
    for seq in 0..3 {
        let b = Batch::encode(&l, seq, &[(seq, &[0u8; 9000][..])]).unwrap();
        assert!(node.append(1, b).await.is_success());
    }
    node.apply_retention().await.unwrap();
}

/// The stream's batches from its start, by sequence.
fn read_all(node: &Node, stream: &str) -> Vec<Bytes> {
    let (mut c, mut out) = (Cursor::new(), vec![]);
    loop {
        let (bs, next) = node.store.read_cursor(stream, &c, 1 << 20);
        if bs.is_empty() {
            return out;
        }
        out.extend(bs);
        c = next;
    }
}

/// Whether the stream was out of RAM: asking for its frontier had to load it.
fn dormant(node: &Node, stream: &str) -> bool {
    let before = node.store.hot_stats().loads;
    node.store.frontier(&log(stream));
    node.store.hot_stats().loads > before
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dormant_stream_wakes_for_an_append_a_read_and_a_subscription_and_answers_as_before() {
    let d = tempfile::tempdir().unwrap();
    let node = Node::open(config(d.path())).await.unwrap();
    let written: Vec<Bytes> = (0..6).map(|s| batch("a", s, s as u8)).collect();
    for b in &written {
        assert!(node.append(1, b.clone()).await.is_success());
    }
    settle(&node, 1).await;
    let h = node.store.hot_stats();
    assert!(h.dormant >= 1 && h.evictions >= 1, "{h:?}");
    // a read
    assert!(dormant(&node, "a"), "out of RAM after the pass");
    assert_eq!(read_all(&node, "a"), written);
    // appends: a retry is idempotent, other bytes a conflict, the next one new
    settle(&node, 2).await;
    let before = node.store.totals();
    let ack = node.append(1, written[3].clone()).await;
    assert!(ack.is_success(), "{ack:?}");
    assert_eq!(
        node.store.totals(),
        before,
        "an idempotent retry: nothing new"
    );
    settle(&node, 3).await;
    let ack = node.append(1, batch("a", 3, 99)).await;
    assert_eq!(ack.status, AckStatus::Conflict, "{ack:?}");
    settle(&node, 4).await;
    assert!(node.append(1, batch("a", 6, 6)).await.is_success());
    let f = node.store.frontier(&log("a"));
    assert_eq!((f.contiguous, f.high), (7, 7));
    // a live feed from now joins the log after its last batch
    settle(&node, 5).await;
    let (_, next) = node.store.live_start(Some("a"));
    assert_eq!(next[&log("a")], 7);
    let (_, all) = node.store.live_start(None);
    assert_eq!(all[&log("a")], 7, "a feed of every stream too");
    // and after a restart, still as it was
    drop(node);
    let node = Node::open(config(d.path())).await.unwrap();
    let mut want = written.clone();
    want.push(batch("a", 6, 6));
    assert_eq!(read_all(&node, "a"), want);
    assert!(node.metrics().contains("precog_streams_dormant"));
}

/// Writes streams, runs a pass that fails at `fail_at`, restarts without the fault: every
/// acknowledged batch is there, once, and a retry of each adds nothing.
async fn crash_at(fail_at: &'static str, caching: bool) {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.retention_bytes = Some(6 * (16 << 10));
    if caching {
        c.flush_delay_ms = 60_000;
    }
    let node = Node::open(c.clone()).await.unwrap();
    let mut written: Vec<(String, Vec<Bytes>)> = vec![];
    for (i, s) in ["p", "q", "r"].iter().enumerate() {
        let bs: Vec<Bytes> = (0..4).map(|seq| batch(s, seq, i as u8)).collect();
        for b in &bs {
            assert!(node.append(1, b.clone()).await.is_success());
        }
        written.push((s.to_string(), bs));
        settle(&node, i as u64).await;
    }
    drop(node);
    let mut faulty = c.clone();
    faulty.storage_failpoints.fail_at = Some(fail_at);
    let node = Node::open(faulty).await.unwrap();
    // the later streams leave the earlier segments for the cap to drop
    for seq in 0..4 {
        let b = Batch::encode(&log("late"), seq, &[(seq, &[1u8; 9000][..])]).unwrap();
        assert!(node.append(1, b).await.is_success(), "{fail_at}");
    }
    assert!(
        node.apply_retention().await.is_err(),
        "{fail_at} fails the pass"
    );
    drop(node);
    let node = Node::open(c).await.unwrap();
    for (s, bs) in &written {
        let got = read_all(&node, s);
        // what the cap dropped is gone from the start; the rest is all there, in order
        assert!(bs.ends_with(&got), "{fail_at}: stream {s}");
        let before = node.store.totals();
        for b in bs {
            // idempotent, or below what the cap dropped (refused as expired)
            let ack = node.append(1, b.clone()).await;
            assert_ne!(ack.status, AckStatus::Unknown, "{fail_at}: {s}");
        }
        assert_eq!(
            node.store.totals(),
            before,
            "{fail_at}: a retry of {s} is not new"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_at_any_step_of_a_pass_that_puts_streams_out_of_ram_loses_nothing() {
    for caching in [false, true] {
        for at in [
            "summary:before_commit",
            "summary:after_commit",
            "retain:before_manifest",
            "retain:after_manifest",
        ] {
            crash_at(at, caching).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_racing_the_pass_that_puts_their_stream_out_of_ram_are_all_kept() {
    let d = tempfile::tempdir().unwrap();
    let node = Node::open(config(d.path())).await.unwrap();
    let n = 300u64;
    let writer = {
        let node = node.clone();
        tokio::spawn(async move {
            for seq in 0..n {
                assert!(node.append(1, batch("race", seq, 7)).await.is_success());
                if seq % 7 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    };
    let passes = {
        let node = node.clone();
        tokio::spawn(async move {
            while node.store.frontier(&log("race")).contiguous < n {
                node.apply_retention().await.unwrap();
            }
        })
    };
    writer.await.unwrap();
    passes.await.unwrap();
    settle(&node, 1).await;
    let f = node.store.frontier(&log("race"));
    assert_eq!((f.contiguous, f.high), (n, n));
    assert_eq!(read_all(&node, "race").len() as u64, n);
    assert!(node.store.hot_stats().evictions > 0);
    drop(node);
    let node = Node::open(config(d.path())).await.unwrap();
    assert_eq!(read_all(&node, "race").len() as u64, n, "after a restart");
}

/// A restart's work with `n` dormant streams: (summaries it read, streams in RAM after it,
/// seconds).
async fn restart_with(n: u64) -> (u64, u64, f64) {
    let d = tempfile::tempdir().unwrap();
    let mut c = config(d.path());
    c.segment_size = 256 << 10;
    let node = Node::open(c.clone()).await.unwrap();
    for i in 0..n {
        let b = batch(&format!("x{i:06}"), 0, 1);
        assert!(node.append(1, b).await.is_success());
    }
    // the same tail for every n: a sealed segment's worth of one stream after them
    for seq in 0..30 {
        let l = LogId {
            stream: "tail".into(),
            writer: "t".into(),
            epoch: 1,
        };
        let b = Batch::encode(&l, seq, &[(seq, &[0u8; 9000][..])]).unwrap();
        assert!(node.append(1, b).await.is_success());
    }
    node.apply_retention().await.unwrap();
    assert!(
        node.store.hot_stats().dormant >= n,
        "out of RAM before the restart"
    );
    drop(node);
    let t = std::time::Instant::now();
    let node = Node::open(c).await.unwrap();
    let secs = t.elapsed().as_secs_f64();
    let h = node.store.hot_stats();
    // and every stream is still there
    assert_eq!(read_all(&node, &format!("x{:06}", n - 1)).len(), 1);
    (h.loads, h.hot, secs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_does_not_rebuild_dormant_streams_so_it_costs_the_same_for_more_of_them() {
    // Why: a node of millions of streams must restart in time independent of their number
    // (docs/S2-PLAN.md, S5): it reads its checkpoint and replays the log past it, and a dormant
    // stream stays a summary on disk until something uses it.
    let (few, many) = (restart_with(500).await, restart_with(4000).await);
    eprintln!("restart: 500 streams {few:?}, 4000 streams {many:?}");
    assert_eq!(
        (few.0, few.1),
        (many.0, many.1),
        "the same summaries read, the same in RAM"
    );
    assert!(many.1 <= 2, "only the log's tail in RAM: {many:?}");
    // the log's own open reads each sealed index (a few bytes per stream): well under 8x
    assert!(many.2 < 4.0 * few.2.max(0.05), "{few:?} {many:?}");
}
