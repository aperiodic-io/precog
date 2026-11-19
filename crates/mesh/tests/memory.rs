//! RAM per retained batch. A node retains tens of millions of small batches (live-ingestion's
//! producers flush one per writer log every 100 ms, and a node keeps ~74 GiB), so what the node
//! holds in RAM per batch decides whether it fits its pod: at 500 B a batch, 50M batches are
//! 25 GB. This counts live heap bytes with a counting allocator and fails past a budget.

use precog_mesh::proto::{Batch, LogId, batch_crc, encode_log_key};
use precog_mesh::store::{Reserve, Store};
use precog_storage::record::Meta;
use std::alloc::{GlobalAlloc, Layout, System};
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

fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

const BATCHES: usize = 200_000;
/// Bytes of heap per retained batch, the store's index and the segment log's together (requested
/// sizes: malloc's own rounding and headers come on top). Today: 151 B (store 147 B).
const BUDGET: f64 = 200.0;

#[test]
fn ram_per_retained_batch_stays_within_budget() {
    // live-ingestion's shapes: stream and writer names of its length, a few streams, a few
    // writer logs each, one small batch per log in turn
    let logs: Vec<LogId> = (0..4)
        .flat_map(|s| {
            (0..8).map(move |w| LogId {
                stream: format!("trade_size.v1.binance-futures.{s}"),
                writer: format!("live-producers-binance-futures-{w:04x}"),
                epoch: 1_700_000_000_000 + w,
            })
        })
        .collect();
    let batches: Vec<Batch> = (0..BATCHES)
        .map(|i| {
            let log = &logs[i % logs.len()];
            let seq = (i / logs.len()) as u64 * 10;
            let evs: Vec<(u64, &[u8])> = (0..10).map(|e| (seq + e, &b"0123456789"[..])).collect();
            Batch::parse(Batch::encode(log, seq, &evs).unwrap()).unwrap()
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = precog_storage::Config::new(dir.path());
    cfg.segment_size = 1 << 20;
    cfg.spares = 0;
    cfg.tail_cache_bytes = 0;
    let mut log = precog_storage::Log::open(cfg).unwrap();
    let store = Store::load(log.reader(), vec![]).unwrap();

    let base = live();
    // (store, segment log): the group's own Vec and keys are freed before the next count
    let (mut store_bytes, mut log_bytes) = (0, 0);
    for chunk in batches.chunks(1000) {
        let group: Vec<(Meta, &[u8])> = chunk
            .iter()
            .map(|b| {
                let m = Meta {
                    key: encode_log_key(&b.id.log),
                    first_seq: b.id.first_seq,
                    count: b.count,
                    tag: batch_crc(&b.bytes),
                    time_ms: 0,
                };
                (m, &b.bytes[..])
            })
            .collect();
        let t0 = live();
        for b in chunk {
            assert!(matches!(store.reserve(b), Reserve::New));
        }
        let t1 = live();
        let first = log.append(&group).unwrap();
        let t2 = live();
        for (i, b) in chunk.iter().enumerate() {
            store.mark_durable(&b.id, first + i as u64);
        }
        store_bytes += live() - t2 + (t1 - t0);
        log_bytes += t2 - t1;
    }
    assert_eq!(store.totals().0, BATCHES as u64);
    let per = |x: isize| x as f64 / BATCHES as f64;
    let total = per(live() - base);
    eprintln!(
        "heap per retained batch: {total:.0} B (store {:.0} B, segment log index {:.0} B)",
        per(store_bytes),
        per(log_bytes)
    );
    assert!(
        total <= BUDGET,
        "{total:.0} B of heap per retained batch, over the {BUDGET} B budget"
    );
}
