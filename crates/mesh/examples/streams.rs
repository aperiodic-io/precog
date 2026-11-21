//! Many-streams footprint, in process (`bench/s2/streams.sh` without Docker): creates COUNT
//! streams with one 64-byte record each through `Node::append` (1 copy, dedup off), then reports
//! the process's RSS and live heap, restarts the node and reports the restart time (until it
//! accepts writes) and RSS and heap after it. One JSON line.
//!
//!   cargo run --release -p precog-mesh --example streams -- <count> [segment_size] [dormant]
//!
//! `dormant` (1): streams leave RAM at once (`dormant_after_ms` 0) and a retention pass runs
//! every 100k streams, as the node's every 10 s; the numbers are then the dormant streams' cost.
//! Without it the node keeps its defaults (a stream leaves RAM after 10 minutes unused, or
//! sooner past its 256 MiB budget), and a retention pass runs every `retention_interval_ms` (10
//! s) during the load, as `serve` runs them.

use precog_mesh::node::{Node, NodeConfig};
use precog_mesh::proto::{Batch, LogId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

struct Counting;
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let n = LIVE.fetch_add(l.size() as isize, Ordering::Relaxed) + l.size() as isize;
        PEAK.fetch_max(n, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let n = LIVE.fetch_add(new as isize - l.size() as isize, Ordering::Relaxed);
        PEAK.fetch_max(n + new as isize - l.size() as isize, Ordering::Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static A: Counting = Counting;

fn rss_mib() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kb: f64 = s
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0.0);
    kb / 1024.0
}

fn mib(x: isize) -> f64 {
    x as f64 / (1 << 20) as f64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let count: usize = args.get(1).map_or(Ok(100_000), |a| a.parse())?;
    let segment: u64 = args.get(2).map_or(Ok(256 << 20), |a| a.parse())?;
    let dormant = args.get(3).is_some_and(|a| a == "1");
    let dir = tempfile::tempdir_in(std::env::var("DATA").unwrap_or("/tmp".into()))?;
    let mut cfg = NodeConfig::single(0, "d0", dir.path().into(), "tok".into());
    cfg.target_copies = 1;
    cfg.segment_size = segment;
    cfg.dedup_window_ms = None;
    if dormant {
        cfg.dormant_after_ms = 0;
    }
    let node = Node::open(cfg.clone()).await?;
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let t = std::time::Instant::now();
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (mut done, mut pass) = (0, 0.0);
    // what `serve` runs: a retention pass every interval (dormant runs its own, below)
    let stop = Arc::new(tokio::sync::Notify::new());
    let passes = (!dormant).then(|| {
        let (node, stop) = (node.clone(), stop.clone());
        tokio::spawn(async move {
            let every = std::time::Duration::from_millis(node.cfg.retention_interval_ms);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(every) => node.apply_retention().await.unwrap(),
                    _ = stop.notified() => return, // between passes, never inside one
                }
            }
        })
    });
    while done < count {
        let upto = if dormant {
            (done + 100_000).min(count)
        } else {
            count
        };
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let (node, next) = (node.clone(), next.clone());
            tasks.spawn(async move {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= upto {
                        next.fetch_sub(1, Ordering::Relaxed);
                        return;
                    }
                    let log = LogId {
                        stream: format!("many/m{i:08}"),
                        writer: "s2".into(),
                        epoch: 1,
                    };
                    let b = Batch::encode(&log, 0, &[(0, &[7u8; 64][..])]).unwrap();
                    assert!(node.append(1, b).await.is_success());
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        done = upto;
        if dormant {
            let t = std::time::Instant::now();
            node.apply_retention().await?;
            pass += t.elapsed().as_secs_f64();
        }
    }
    let rate = count as f64 / t.elapsed().as_secs_f64();
    if let Some(p) = passes {
        stop.notify_one();
        p.await?;
    }
    node.apply_retention().await?; // what the node's retention pass does every 10 s
    let (rss, heap, peak) = (
        rss_mib(),
        mib(LIVE.load(Ordering::Relaxed) - base),
        mib(PEAK.load(Ordering::Relaxed) - base),
    );
    let hot = node.store.hot_stats();
    drop(node);
    let t = std::time::Instant::now();
    let node = Node::open(cfg).await?;
    let restart = t.elapsed().as_secs_f64();
    let (rss2, heap2) = (rss_mib(), mib(LIVE.load(Ordering::Relaxed) - base));
    // a read of the last stream answers
    let last = format!("many/m{:08}", count - 1);
    let c = node.store.cursor_at(&last, 0);
    assert_eq!(node.store.read_cursor(&last, &c, 1 << 20).0.len(), 1);
    println!(
        "{{\"streams\":{count},\"segment_size\":{segment},\"dormant\":{dormant},\"create_per_s\":{rate:.0},\"passes_s\":{pass:.2},\"rss_mib\":{rss:.1},\"heap_mib\":{heap:.1},\"heap_peak_mib\":{peak:.1},\"heap_per_stream\":{:.0},\"hot\":{},\"restart_s\":{restart:.2},\"rss_after_restart_mib\":{rss2:.1},\"heap_after_restart_mib\":{heap2:.1}}}",
        heap * (1 << 20) as f64 / count as f64,
        hot.hot
    );
    Ok(())
}
