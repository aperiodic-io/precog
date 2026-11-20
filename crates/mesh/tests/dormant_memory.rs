//! RAM per dormant stream (docs/S2-PLAN.md, C7). A node of many small streams, most of them
//! idle, must not hold each one in RAM: 10M streams within 2 GiB is ~200 B per stream for
//! everything. This creates streams with one keyed event each, lets them go idle, runs the
//! node's retention pass (absorb, checkpoint, evict) and counts the live heap they still cost,
//! as the slope between two stream counts (so fixed costs, caches bounded by config, cancel).

use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::{Batch, LogId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

struct Counting;
static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        LIVE.fetch_add(new as isize - l.size() as isize, Ordering::Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static A: Counting = Counting;

/// Heap bytes per dormant stream: the stream's summary is on disk; what may stay is a few bytes
/// of a sealed segment's key table and of free slots. 2,649 B per stream on main before dormant
/// streams left RAM; 37 B since; 10 B with summaries in LEB128 (measured over 64k streams, with a
/// page cache full at both measurements).
const BUDGET: f64 = 64.0;

/// Streams `from..to`, one keyed event each, 16 appends at a time.
async fn create(node: &Arc<Node>, from: usize, to: usize) {
    let mut tasks = tokio::task::JoinSet::new();
    for t in 0..16 {
        let node = node.clone();
        tasks.spawn(async move {
            for i in (from + t..to).step_by(16) {
                let log = LogId {
                    stream: format!("many/m{i:08}"),
                    writer: "s2".into(),
                    epoch: 1,
                };
                let key = format!("k{i}");
                let ev = [(0, key.as_bytes(), &[7u8; 64][..])];
                let b = Batch::encode_keyed(&log, 0, &ev).unwrap();
                assert!(node.append(1, b).await.is_success());
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

/// Fills the active segment with one stream's batches, so every stream before it is in a
/// sealed segment, then runs retention passes until those leave RAM.
async fn settle(node: &Arc<Node>, round: u64) {
    let log = LogId {
        stream: "filler".into(),
        writer: "f".into(),
        epoch: round,
    };
    for seq in 0..24 {
        let b = Batch::encode(&log, seq, &[(seq, &[1u8; 60_000][..])]).unwrap();
        assert!(node.append(1, b).await.is_success());
    }
    // past the dedup window, so the streams' ids expire
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    for _ in 0..2 {
        node.apply_retention().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ram_per_dormant_stream_stays_within_budget() {
    let dir = tempfile::tempdir().unwrap();
    // unknown fields are ignored by serde: this test compiles on a node without these knobs
    let cfg: NodeConfig = serde_json::from_value(serde_json::json!({
        "id": 0,
        "domain": "a",
        "data_dir": dir.path(),
        "token": "tok",
        "listen_tcp": null,
        "peers": [],
        "target_copies": 1,
        "segment_size": 1 << 20,
        "dedup_window_ms": 1000,
        "tail_cache_bytes": 0,
        "dormant_after_ms": 0,
        // small enough to be full at both measurements: the cache is bounded, not per stream
        "summary_cache_bytes": 1 << 16,
    }))
    .unwrap();
    let node = Node::open(cfg).await.unwrap();
    const N: usize = 32_000;
    create(&node, 0, N).await;
    settle(&node, 1).await;
    let at_n = LIVE.load(Ordering::Relaxed);
    create(&node, N, 3 * N).await;
    settle(&node, 2).await;
    let at_3n = LIVE.load(Ordering::Relaxed);
    let per = (at_3n - at_n) as f64 / (2 * N) as f64;
    eprintln!("heap per dormant stream: {per:.0} B");
    assert!(
        per <= BUDGET,
        "{per:.0} B of heap per dormant stream, over the {BUDGET} B budget"
    );
    // still served: a read wakes the stream from disk
    let last = format!("many/m{:08}", 3 * N - 1);
    let c = node.store.cursor_at(&last, 0);
    assert_eq!(node.store.read_cursor(&last, &c, 1 << 20).0.len(), 1);
}
