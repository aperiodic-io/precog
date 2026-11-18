//! Exact dedup indexes over 64-bit digests of event ids, each forgetting ids older than a window.
//! `seen` answers "was this id seen within the window?" and records it if not.
//! Probabilistic filters (Bloom, cuckoo, xor) are left out on purpose: a false positive drops a
//! distinct trade.

use std::collections::VecDeque;

pub trait Dedup {
    fn name(&self) -> String;
    /// True if `h` was seen within the window (a duplicate); otherwise records it at `now_s`.
    fn seen(&mut self, h: u64, now_s: u32) -> bool;
    /// `seen` for a whole batch, in order (later events see earlier ones).
    fn seen_batch(&mut self, hs: &[u64], now_s: u32, out: &mut Vec<bool>) {
        out.clear();
        out.extend(hs.iter().map(|&h| self.seen(h, now_s)));
    }
    /// Heap bytes held.
    fn bytes(&self) -> usize;
}

/// 0 marks an empty slot.
fn nz(h: u64) -> u64 {
    h | (h == 0) as u64
}

/// Open addressing with linear probing over bare digests.
struct Table {
    slots: Vec<u64>,
    len: usize,
    start_s: u32,
}

impl Table {
    fn new(cap: usize, start_s: u32) -> Table {
        Table {
            slots: vec![0; cap.max(1024)],
            len: 0,
            start_s,
        }
    }
    /// Any capacity: multiply-shift instead of a power-of-two mask.
    fn home(&self, h: u64) -> usize {
        ((h as u128 * self.slots.len() as u128) >> 64) as usize
    }
    fn next(&self, i: usize) -> usize {
        if i + 1 == self.slots.len() { 0 } else { i + 1 }
    }
    fn contains(&self, h: u64) -> bool {
        let mut i = self.home(h);
        loop {
            match self.slots[i] {
                0 => return false,
                x if x == h => return true,
                _ => i = self.next(i),
            }
        }
    }
    fn insert(&mut self, h: u64) {
        if (self.len + 1) * 8 > self.slots.len() * 7 {
            let cap = self.slots.len() + self.slots.len() / 2;
            let old = std::mem::replace(&mut self.slots, vec![0; cap]);
            self.len = 0;
            for x in old.into_iter().filter(|&x| x != 0) {
                self.insert(x);
            }
        }
        let mut i = self.home(h);
        while self.slots[i] != 0 {
            i = self.next(i);
        }
        self.slots[i] = h;
        self.len += 1;
    }
}

/// A: `k` tables of digests; a new one every `window / (k - 1)`, the oldest dropped whole. Remembers
/// between `window` and `window * k / (k - 1)`; a lookup probes up to `k` tables.
pub struct Generations {
    k: usize,
    span_s: u32,
    tables: VecDeque<Table>,
    sized: usize,
}

impl Generations {
    pub fn new(k: usize, window_s: u32) -> Generations {
        assert!(k >= 2);
        Generations {
            k,
            span_s: window_s.div_ceil(k as u32 - 1),
            tables: VecDeque::new(),
            sized: 1024,
        }
    }
}

impl Dedup for Generations {
    fn name(&self) -> String {
        format!("A generations k={}", self.k)
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        let h = nz(h);
        if self
            .tables
            .back()
            .is_none_or(|t| now_s >= t.start_s + self.span_s)
        {
            // size the new table like the last full one, so it rarely grows
            // a new generation sized for the last one's count at 7/8 load, plus 10%
            self.sized = self.tables.back().map_or(self.sized, |t| t.len * 8 / 7 * 11 / 10 + 1);
            self.tables.push_back(Table::new(self.sized, now_s));
            // by count, and by age after an idle gap
            while self.tables.len() > self.k
                || self
                    .tables
                    .front()
                    .is_some_and(|t| now_s >= t.start_s + self.span_s * self.k as u32)
            {
                self.tables.pop_front();
            }
        }
        if self.tables.iter().rev().any(|t| t.contains(h)) {
            return true;
        }
        self.tables.back_mut().unwrap().insert(h);
        false
    }
    fn bytes(&self) -> usize {
        self.tables.iter().map(|t| t.slots.len() * 8).sum()
    }
}

/// B: one table of (digest, seconds). An expired entry is reused in place; when the table fills
/// it is rebuilt without its expired entries (and grown only if the live ones need it).
pub struct Stamped {
    window_s: u32,
    h: Vec<u64>,
    t: Vec<u32>,
    used: usize,
}

impl Stamped {
    pub fn new(window_s: u32) -> Stamped {
        Stamped {
            window_s,
            h: vec![0; 1024],
            t: vec![0; 1024],
            used: 0,
        }
    }
    fn rebuild(&mut self, now_s: u32) {
        let live = self
            .h
            .iter()
            .zip(&self.t)
            .filter(|&(&h, &t)| h != 0 && now_s - t <= self.window_s)
            .count();
        let mut cap = self.h.len();
        while (live + 1) * 2 > cap {
            cap *= 2;
        }
        let h = std::mem::replace(&mut self.h, vec![0; cap]);
        let t = std::mem::replace(&mut self.t, vec![0; cap]);
        self.used = 0;
        let m = cap - 1;
        for (x, s) in h
            .into_iter()
            .zip(t)
            .filter(|&(x, s)| x != 0 && now_s - s <= self.window_s)
        {
            let mut i = x as usize & m;
            while self.h[i] != 0 {
                i = (i + 1) & m;
            }
            (self.h[i], self.t[i]) = (x, s);
            self.used += 1;
        }
    }
}

impl Dedup for Stamped {
    fn name(&self) -> String {
        "B stamped table".into()
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        let h = nz(h);
        let m = self.h.len() - 1;
        let mut i = h as usize & m;
        let mut reuse = None;
        loop {
            let x = self.h[i];
            if x == 0 {
                break;
            }
            let fresh = now_s - self.t[i] <= self.window_s;
            if x == h && fresh {
                return true;
            }
            if x == h || (!fresh && reuse.is_none()) {
                reuse = Some(i);
                if x == h {
                    break;
                }
            }
            i = (i + 1) & m;
        }
        match reuse {
            Some(j) => (self.h[j], self.t[j]) = (h, now_s),
            None => {
                (self.h[i], self.t[i]) = (h, now_s);
                self.used += 1;
                if self.used * 8 > self.h.len() * 7 {
                    self.rebuild(now_s);
                }
            }
        }
        false
    }
    fn bytes(&self) -> usize {
        self.h.len() * 12
    }
}

fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0)
    }
}

/// D: B, with every slot a batch will probe prefetched first, so the cache misses overlap.
pub struct StampedPrefetch(pub Stamped);

impl Dedup for StampedPrefetch {
    fn name(&self) -> String {
        "D stamped + batch prefetch".into()
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        self.0.seen(h, now_s)
    }
    fn seen_batch(&mut self, hs: &[u64], now_s: u32, out: &mut Vec<bool>) {
        let m = self.0.h.len() - 1;
        for &h in hs {
            let i = nz(h) as usize & m;
            prefetch(&self.0.h[i]);
            prefetch(&self.0.t[i]);
        }
        out.clear();
        out.extend(hs.iter().map(|&h| self.0.seen(h, now_s)));
    }
    fn bytes(&self) -> usize {
        self.0.bytes()
    }
}

/// E: A with k = 2, prefetching the slot in both tables for a whole batch.
pub struct GenerationsPrefetch(pub Generations);

impl Dedup for GenerationsPrefetch {
    fn name(&self) -> String {
        format!("E generations k={} + batch prefetch", self.0.k)
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        self.0.seen(h, now_s)
    }
    fn seen_batch(&mut self, hs: &[u64], now_s: u32, out: &mut Vec<bool>) {
        for t in &self.0.tables {
            for &h in hs {
                prefetch(&t.slots[t.home(nz(h))]);
            }
        }
        out.clear();
        out.extend(hs.iter().map(|&h| self.0.seen(h, now_s)));
    }
    fn bytes(&self) -> usize {
        self.0.bytes()
    }
}

/// The cost of everything but the index (the driver's id generation and hashing).
pub struct Nothing;

impl Dedup for Nothing {
    fn name(&self) -> String {
        "0 no index (driver cost)".into()
    }
    fn seen(&mut self, _: u64, _: u32) -> bool {
        false
    }
    fn bytes(&self) -> usize {
        0
    }
}

/// One slot: digest and seconds side by side, 12 bytes, one cache line touched per probe.
#[derive(Clone, Copy, Default)]
#[repr(C, packed(4))]
struct Slot {
    h: u64,
    t: u32,
}

/// F: B with packed 12-byte slots, any capacity (multiply-shift instead of a power-of-two
/// mask, growing by half), and batch prefetch.
pub struct Packed {
    window_s: u32,
    slots: Vec<Slot>,
    used: usize,
}

impl Packed {
    pub fn new(window_s: u32) -> Packed {
        Packed {
            window_s,
            slots: vec![Slot::default(); 1024],
            used: 0,
        }
    }
    fn home(&self, h: u64) -> usize {
        ((h as u128 * self.slots.len() as u128) >> 64) as usize
    }
    fn rebuild(&mut self, now_s: u32) {
        let w = self.window_s;
        let live: Vec<Slot> = self
            .slots
            .iter()
            .copied()
            .filter(|s| s.h != 0 && now_s - s.t <= w)
            .collect();
        let mut cap = self.slots.len();
        while (live.len() + 1) * 4 > cap * 3 {
            cap += cap / 2;
        }
        self.slots = vec![Slot::default(); cap];
        self.used = live.len();
        for s in live {
            let mut i = self.home(s.h);
            while self.slots[i].h != 0 {
                i = if i + 1 == cap { 0 } else { i + 1 };
            }
            self.slots[i] = s;
        }
    }
}

impl Dedup for Packed {
    fn name(&self) -> String {
        "F packed 12-byte slots + prefetch".into()
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        let h = nz(h);
        let cap = self.slots.len();
        let mut i = self.home(h);
        let mut reuse = None;
        loop {
            let s = self.slots[i];
            if s.h == 0 {
                break;
            }
            let fresh = now_s - s.t <= self.window_s;
            if s.h == h {
                if fresh {
                    return true;
                }
                reuse = Some(i);
                break;
            }
            if !fresh && reuse.is_none() {
                reuse = Some(i);
            }
            i = if i + 1 == cap { 0 } else { i + 1 };
        }
        match reuse {
            Some(j) => self.slots[j] = Slot { h, t: now_s },
            None => {
                self.slots[i] = Slot { h, t: now_s };
                self.used += 1;
                if self.used * 8 > cap * 7 {
                    self.rebuild(now_s);
                }
            }
        }
        false
    }
    fn seen_batch(&mut self, hs: &[u64], now_s: u32, out: &mut Vec<bool>) {
        for &h in hs {
            prefetch(&self.slots[self.home(nz(h))]);
        }
        out.clear();
        out.extend(hs.iter().map(|&h| self.seen(h, now_s)));
    }
    fn bytes(&self) -> usize {
        self.slots.len() * 12
    }
}

/// The digest is already xxh3: hash it no further.
#[derive(Clone, Copy, Default)]
pub struct Pass(u64);

impl std::hash::Hasher for Pass {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("only u64 keys")
    }
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
}

type Set = hashbrown::HashSet<u64, std::hash::BuildHasherDefault<Pass>>;

/// H: generations of SwissTables (hashbrown): 16 tags compared per SIMD probe, so a new id is
/// rejected fast even at 7/8 load; 9 bytes a slot.
pub struct SwissGenerations {
    k: usize,
    span_s: u32,
    sets: VecDeque<(u32, Set)>,
    sized: usize,
}

impl SwissGenerations {
    pub fn new(k: usize, window_s: u32) -> SwissGenerations {
        assert!(k >= 2);
        SwissGenerations { k, span_s: window_s.div_ceil(k as u32 - 1), sets: VecDeque::new(), sized: 1024 }
    }
}

impl Dedup for SwissGenerations {
    fn name(&self) -> String {
        format!("H swiss generations k={}", self.k)
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        if self.sets.back().is_none_or(|(t, _)| now_s >= t + self.span_s) {
            self.sized = self.sets.back().map_or(self.sized, |(_, s)| s.len() * 11 / 10 + 1);
            self.sets.push_back((now_s, Set::with_capacity_and_hasher(self.sized, Default::default())));
            while self.sets.len() > self.k || self.sets.front().is_some_and(|(t, _)| now_s >= t + self.span_s * self.k as u32) {
                self.sets.pop_front();
            }
        }
        if self.sets.iter().rev().any(|(_, s)| s.contains(&h)) {
            return true;
        }
        self.sets.back_mut().unwrap().1.insert(h);
        false
    }
    fn bytes(&self) -> usize {
        self.sets.iter().map(|(_, s)| s.allocation_size()).sum()
    }
}

/// C: the obvious one, a hash map with a FIFO of insertions for expiry.
pub struct MapFifo {
    window_s: u32,
    map: hashbrown::HashMap<u64, u32>,
    fifo: VecDeque<(u32, u64)>,
}

impl MapFifo {
    pub fn new(window_s: u32) -> MapFifo {
        MapFifo {
            window_s,
            map: Default::default(),
            fifo: VecDeque::new(),
        }
    }
}

impl Dedup for MapFifo {
    fn name(&self) -> String {
        "C hashmap + fifo".into()
    }
    fn seen(&mut self, h: u64, now_s: u32) -> bool {
        while let Some(&(t, x)) = self.fifo.front() {
            if now_s - t <= self.window_s {
                break;
            }
            self.fifo.pop_front();
            if self.map.get(&x) == Some(&t) {
                self.map.remove(&x);
            }
        }
        if self.map.contains_key(&h) {
            return true;
        }
        self.map.insert(h, now_s);
        self.fifo.push_back((now_s, h));
        false
    }
    fn bytes(&self) -> usize {
        self.map.capacity() * 13 + self.fifo.capacity() * 16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all(w: u32) -> Vec<Box<dyn Dedup>> {
        vec![
            Box::new(Generations::new(2, w)),
            Box::new(Generations::new(4, w)),
            Box::new(Stamped::new(w)),
            Box::new(MapFifo::new(w)),
            Box::new(StampedPrefetch(Stamped::new(w))),
            Box::new(GenerationsPrefetch(Generations::new(2, w))),
            Box::new(Packed::new(w)),
            Box::new(SwissGenerations::new(2, w)),
            Box::new(SwissGenerations::new(3, w)),
        ]
    }

    #[test]
    fn every_index_drops_a_resend_within_the_window_and_forgets_one_far_past_it() {
        for mut d in all(300) {
            let n = d.name();
            assert!(!d.seen(42, 10), "{n}");
            assert!(d.seen(42, 10), "{n}: same second");
            assert!(d.seen(42, 300), "{n}: inside the window");
            assert!(!d.seen(43, 300), "{n}");
            // generations remember up to window * k / (k - 1); far past that everything is gone
            assert!(!d.seen(42, 10 + 700), "{n}: long expired");
        }
    }

    #[test]
    fn batches_see_their_own_earlier_events() {
        for mut d in all(300) {
            let mut out = vec![];
            d.seen_batch(&[1, 2, 1, 3, 2], 5, &mut out);
            assert_eq!(out, [false, false, true, false, true], "{}", d.name());
            d.seen_batch(&[3, 4], 6, &mut out);
            assert_eq!(out, [true, false], "{}", d.name());
        }
    }

    #[test]
    fn every_index_is_exact_through_growth_and_expiry() {
        // many distinct ids over several windows: none is called a duplicate, every resend is
        for mut d in all(10) {
            let n = d.name();
            for i in 0..200_000u64 {
                let now = (i / 5_000) as u32;
                let h = xxhash_rust::xxh3::xxh3_64(&i.to_le_bytes());
                assert!(!d.seen(h, now), "{n}: {i} is new");
                if i % 7 == 0 {
                    assert!(d.seen(h, now), "{n}: {i} resent");
                }
            }
        }
    }
}
